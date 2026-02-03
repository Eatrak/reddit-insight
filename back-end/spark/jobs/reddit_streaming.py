import json
import os
import re
import sqlite3
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS_DB_PATH = os.getenv("TOPICS_DB_PATH", "/data/topics.db")
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_BASE", "/checkpoints")

# Topics
RAW_POSTS_TOPIC = "reddit.raw.posts"
RAW_COMMENTS_TOPIC = "reddit.raw.comments"
MATCHED_TOPIC = "reddit.topic.matched"
METRICS_TOPIC = "reddit.topic.metrics"

# Watermark
WATERMARK_DURATION = "35 days"


from typing import Any, Dict, List, Optional

# -----------------------------------------------------------------------------
# 1. Utils & UDFs
# -----------------------------------------------------------------------------

WORD_RE = re.compile(r"[a-zA-Z0-9_]{2,}")

@F.udf(returnType=T.ArrayType(T.StringType()))
def tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]

import shutil

def load_topics() -> List[Dict[str, Any]]:
    if not os.path.exists(TOPICS_DB_PATH):
        return []
        
    temp_db = "/tmp/topics_copy.db"
    try:
        shutil.copyfile(TOPICS_DB_PATH, temp_db)
    except:
        temp_db = TOPICS_DB_PATH

    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM topics WHERE is_active = 1").fetchall()
    except Exception as e:
        sys.stderr.write(f"ERROR: load_topics failed: {e}\n")
        return []
    finally:
        conn.close()

    def split_csv(s: str) -> List[str]:
        s = (s or "").strip()
        if not s:
            return []
        return [x.strip().lower() for x in s.split(",") if x.strip()]

    out: List[Dict[str, Any]] = []
    for r in rows:
        keywords_raw = r["keywords"]
        keywords = []
        try:
            keywords = json.loads(keywords_raw) if keywords_raw.startswith("[") else split_csv(keywords_raw)
        except:
            keywords = split_csv(keywords_raw)

        out.append(
            {
                "topic_id": r["id"],
                "keywords": keywords,
                "subreddits": split_csv(r["subreddits"]),
                "min_score": int(json.loads(r["filters_json"]).get("min_score", 0)) if r["filters_json"] else 0,
            }
        )
    return out


# -----------------------------------------------------------------------------
# 2. Main Job
# -----------------------------------------------------------------------------
def log_msg(msg):
    with open("/opt/spark-apps/debug.log", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")

def main() -> None:
    # Ensure log file exists
    with open("/opt/spark-apps/debug.log", "w") as f:
        f.write("Script started.\n")

    try:
        log_msg("Initializing Spark Session...")
        spark = (
            SparkSession.builder.appName("trend-insight-streaming")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        log_msg("Spark Session created.")

        # Strict Schema
        schema = T.StructType(
            [
                T.StructField("event_id", T.StringType(), True),
                T.StructField("event_type", T.StringType(), True),
                T.StructField("subreddit", T.StringType(), True),
                T.StructField("author", T.StringType(), True),
                T.StructField("created_utc", T.StringType(), True),
                T.StructField("text", T.StringType(), True),
                T.StructField("score", T.IntegerType(), True),
                T.StructField("num_comments", T.IntegerType(), True),
                T.StructField("ingested_at", T.StringType(), True),
            ]
        )

        log_msg("Creating DataStreamReader...")
        reader = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", f"{RAW_POSTS_TOPIC},{RAW_COMMENTS_TOPIC}") \
            .option("startingOffsets", "earliest") # Catch all data

        df_raw = reader.load()
        log_msg("df_raw loaded.")

        log_msg("Applying transformations to df...")
        df = (
            df_raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
            .select("v.*")
            .withColumn("event_time", F.to_timestamp(F.col("created_utc"))) 
            .withColumn("event_time", F.col("created_utc").cast("timestamp"))
            .withColumn("tokens", tokenize(F.col("text")))
            .withWatermark("event_time", WATERMARK_DURATION)
        )

        # ---------------------------------------------------------------------
        # JOB 1: Topic Matching (Stateless, foreachBatch)
        # ---------------------------------------------------------------------
        log_msg("Defining write_matched (Job 1)...")
        def write_matched(batch_df, batch_id: int) -> None:
            topics = load_topics()
            if not topics: return
            rows = []
            for t in topics:
                for kw in (t["keywords"] or []):
                    if isinstance(kw, list):
                        for k in kw: rows.append((t["topic_id"], k.lower()))
                    else:
                        rows.append((t["topic_id"], kw.lower()))
            if not rows: return
            
            map_df = spark.createDataFrame(rows, schema=["topic_id", "term"])
            exploded = batch_df.select("*", F.explode_outer("tokens").alias("token")).withColumn("token", F.lower(F.col("token")))
            
            joined = exploded.join(map_df, exploded["token"] == map_df["term"], "inner")
            matched = joined.dropDuplicates(["event_id", "topic_id"])
            
            (matched.select(F.to_json(F.struct(*[F.col(c) for c in matched.columns])).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", MATCHED_TOPIC).save())

        matched_query = (
            df.writeStream.foreachBatch(write_matched)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/matched_v3")
            .start()
        )
        log_msg(f"matched_query started. Active: {matched_query.isActive}")

        # ---------------------------------------------------------------------
        # JOB 2: Simple Daily Aggregation (Windowed)
        # ---------------------------------------------------------------------
        log_msg("Setting up Job 2 (Simple Daily Aggregations)...")
        
        # Schema for reading back 'matched' topic
        matched_json_schema = schema.add("topic_id", T.StringType()).add("tokens", T.ArrayType(T.StringType()))
        
        df_matched_in = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", MATCHED_TOPIC)
            .option("startingOffsets", "earliest") # Reads backfill from Job 1
            .load()
            .select(F.from_json(F.col("value").cast("string"), matched_json_schema).alias("data"))
            .select("data.*")
            .withColumn("event_time", F.to_timestamp(F.col("created_utc")))
            .withColumn("event_time", F.col("created_utc").cast("timestamp"))
            .withWatermark("event_time", WATERMARK_DURATION)
        )

        # Simple 1-Day Tumbling Window
        # Group by Topic + Day
        df_aggs = (
            df_matched_in.groupBy("topic_id", F.window("event_time", "1 day").alias("window"))
            .agg(
                F.count("*").alias("mentions"),
                F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
            )
            .select(
                "topic_id",
                F.col("window.start").alias("start"),
                F.col("window.end").alias("end"),
                "mentions",
                "engagement"
            )
            .withColumn("window_type", F.lit("1d")) # Tag it so API knows resolution
        )

        metrics_query = (
            df_aggs.select(F.to_json(F.struct(*df_aggs.columns)).alias("value"))
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", METRICS_TOPIC)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics_simple_daily_v1")
            .outputMode("update")
            .start()
        )
        log_msg(f"metrics_query started. Active: {metrics_query.isActive}")

        log_msg("Waiting for termination...")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        log_msg(f"CRITICAL ERROR IN MAIN: {e}")
        import traceback
        with open("/opt/spark-apps/debug.log", "a") as f:
            traceback.print_exc(file=f)
        sys.exit(1)

if __name__ == "__main__":
    main()
