import json
import os
import re
import sqlite3
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import SparkSession, functions as F, types as T

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
# These tell Spark where to find Kafka and the database
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_PATH = os.getenv("TOPICS_DB_PATH", "/data/topics.db")
CHECKPOINTS   = os.getenv("SPARK_CHECKPOINT_BASE", "/checkpoints")

# Data Topics (Channels)
RAW_POSTS_TOPIC    = "reddit.raw.posts"
RAW_COMMENTS_TOPIC = "reddit.raw.comments"
MATCHED_TOPIC      = "reddit.topic.matched.v2"
METRICS_TOPIC      = "reddit.topic.metrics"

# How long to keep data in memory (35 days)
DATA_HISTORY_WINDOW = "35 days"

# =============================================================================
# 2. HELPER FUNCTIONS
# =============================================================================

def get_active_topics() -> List[Dict[str, Any]]:
    """
    Reads the 'topics' table from the database and prepares the list of keywords.
    """
    if not os.path.exists(DATABASE_PATH): return []
        
    # Copy DB to /tmp to avoid locking issues while the API is using it
    temp_db = "/tmp/topics_copy.db"
    try:
        shutil.copyfile(DATABASE_PATH, temp_db)
    except:
        temp_db = DATABASE_PATH

    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    try:
        # Only load topics that are currently 'active'
        rows = conn.execute("SELECT id, keywords FROM topics WHERE is_active = 1").fetchall()
    except Exception as e:
        print(f"ERROR loading topics: {e}")
        return []
    finally:
        conn.close()

    def parse_keywords(raw: str) -> List[str]:
        raw = (raw or "").strip()
        if not raw: return []
        # Support both JSON arrays ["keyword1"] and simple CSV "keyword1, keyword2"
        try:
            if raw.startswith("["):
                return json.loads(raw)
            return [x.strip().lower() for x in raw.split(",") if x.strip()]
        except:
            return [x.strip().lower() for x in raw.split(",") if x.strip()]

    topics = []
    for r in rows:
        topics.append({
            "id": r["id"],
            "keywords": parse_keywords(r["keywords"]),
        })
    return topics

def log(msg: str):
    """Simple logger to track progress in the debug.log file."""
    with open("/opt/spark-apps/debug.log", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")

# =============================================================================
# 3. CORE PROCESSING LOGIC
# =============================================================================

def process_matching_batch(batch_df, batch_id: int):
    """
    JOB 1: Scans every new Reddit post for your topic keywords.
    """
    active_topics = get_active_topics()
    if not active_topics: return

    # A. Build a matching 'filter' for each topic
    topic_patterns = []
    for t in active_topics:
        kws = t["keywords"]
        if not kws: continue
        # Create a combined regex pattern (keyword1|keyword2|...)
        pattern_str = "|".join([re.escape(str(k)) for k in kws])
        pattern = re.compile(f"(?i)({pattern_str})") # (?i) makes it case-insensitive
        topic_patterns.append((t["id"], pattern))

    # B. The 'Scanner' function
    @F.udf(returnType=T.ArrayType(T.StructType([
        T.StructField("topic_id", T.StringType(), False),
        T.StructField("term", T.StringType(), False)
    ])))
    def scan_for_keywords(text: str):
        if not text: return []
        results = []
        for topic_id, regex in topic_patterns:
            if regex.search(text):
                # If we find a match, record the topic and the specific word found
                found_words = regex.findall(text)
                for word in set([w.lower() for w in found_words]):
                    results.append((topic_id, word))
        return results

    # C. Apply the scanner and create one row per topic match
    matched_df = (
        batch_df
        .withColumn("matches", scan_for_keywords(F.col("text")))
        .filter(F.size("matches") > 0)
        .select("*", F.explode("matches").alias("match_info"))
        .select(
            "*", 
            F.col("match_info.topic_id").alias("topic_id"),
            F.col("match_info.term").alias("term")
        )
        .drop("matches", "match_info")
    )

    # D. Push the results to the 'matched' topic for Job 2 to pick up
    (matched_df.select(F.to_json(F.struct("*")).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", KAFKA_SERVERS)
     .option("topic", MATCHED_TOPIC)
     .save())

def main():
    # 1. Initialize Spark
    spark = SparkSession.builder.appName("TrendInsight-Streaming").getOrCreate()
    log("Spark Session Started.")

    # 2. Define the 'Shape' of a Reddit Post
    post_schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("subreddit", T.StringType()),
        T.StructField("created_utc", T.StringType()),
        T.StructField("text", T.StringType()),
        T.StructField("score", T.IntegerType()),
        T.StructField("num_comments", T.IntegerType()),
    ])

    # -------------------------------------------------------------------------
    # PART A: THE MATCHER (Job 1)
    # -------------------------------------------------------------------------
    # Goal: Read raw posts -> Tag with Topic IDs -> Save
    raw_posts_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", f"{RAW_POSTS_TOPIC},{RAW_COMMENTS_TOPIC}")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), post_schema).alias("data"))
        .select("data.*")
    )

    matcher_query = (
        raw_posts_stream.writeStream
        .foreachBatch(process_matching_batch)
        .option("checkpointLocation", f"{CHECKPOINTS}/matched_v5")
        .start()
    )

    # -------------------------------------------------------------------------
    # PART B: THE COUNTER (Job 2)
    # -------------------------------------------------------------------------
    # Goal: Read tagged matches -> Group by Day -> Calculate Mentions & Growth
    matched_schema = post_schema.add("topic_id", T.StringType()).add("term", T.StringType())

    daily_metrics_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", MATCHED_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), matched_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.col("created_utc").cast("timestamp"))
        .withWatermark("event_time", DATA_HISTORY_WINDOW)
        
        # Avoid counting the same post twice (Deduplication)
        .dropDuplicates(["event_id", "topic_id"])
        
        # Group data into 1-Day buckets
        .groupBy("topic_id", F.window("event_time", "1 day").alias("time_window"))
        .agg(
            F.count("*").alias("mentions"),
            F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
        )
        
        # Prepare for output to the API
        .select(
            "topic_id", "mentions", "engagement",
            F.col("time_window.start").alias("start"),
            F.col("time_window.end").alias("end"),
            F.lit("1d").alias("window_type")
        )
    )

    metrics_query = (
        daily_metrics_stream.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", METRICS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINTS}/metrics_v3")
        .outputMode("update")
        .start()
    )

    log("Both Jobs are active. Processing data...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
