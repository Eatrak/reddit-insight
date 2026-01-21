import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS_DB_PATH = os.getenv("TOPICS_DB_PATH", "/data/topics.db")
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_BASE", "/checkpoints")


RAW_TOPICS = ["reddit.posts.raw", "reddit.comments.raw"]
MATCHED_TOPIC = "reddit.topic.matched"
METRICS_TOPIC = "reddit.topic.metrics"
TRENDS_TOPIC = "reddit.topic.trends"
SENTIMENT_TOPIC = "reddit.topic.sentiment"
GLOBAL_TRENDS_TOPIC = "reddit.global.trends"


WORD_RE = re.compile(r"[a-zA-Z0-9_]{2,}")

POS_WORDS = {
    "good",
    "great",
    "love",
    "awesome",
    "nice",
    "amazing",
    "excellent",
    "win",
    "bullish",
    "happy",
}
NEG_WORDS = {
    "bad",
    "terrible",
    "hate",
    "awful",
    "sad",
    "angry",
    "worse",
    "worst",
    "bearish",
    "loss",
}


def load_topics() -> List[Dict[str, Any]]:
    """
    Reads user-defined topics from the API sqlite DB.
    This keeps runtime tracking deterministic (no LLMs), while still allowing CRUD from the REST API.
    """
    if not os.path.exists(TOPICS_DB_PATH):
        return []
    conn = sqlite3.connect(TOPICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM topics WHERE is_active = 1").fetchall()
    conn.close()

    def split_csv(s: str) -> List[str]:
        s = (s or "").strip()
        if not s:
            return []
        return [x.strip().lower() for x in s.split(",") if x.strip()]

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "topic_id": r["id"],
                "keywords": split_csv(r["keywords"]),
                "subreddits": split_csv(r["subreddits"]),
                "min_score": int(json.loads(r["filters_json"]).get("min_score", 0)),
            }
        )
    return out


@F.udf(returnType=T.ArrayType(T.StringType()))
def tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


@F.udf(returnType=T.DoubleType())
def simple_sentiment(tokens: List[str]) -> float:
    if not tokens:
        return 0.0
    pos = sum(1 for t in tokens if t in POS_WORDS)
    neg = sum(1 for t in tokens if t in NEG_WORDS)
    return float(pos - neg) / float(max(len(tokens), 1))


def main() -> None:
    spark = (
        SparkSession.builder.appName("reddit-insight-streaming")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )

    # Read raw posts/comments from Kafka
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", ",".join(RAW_TOPICS))
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payload
    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("subreddit", T.StringType(), True),
            T.StructField("author", T.StringType(), True),
            T.StructField("created_utc", T.StringType(), True),  # ISO string
            T.StructField("score", T.IntegerType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("type", T.StringType(), True),
            T.StructField("permalink", T.StringType(), True),
            T.StructField("url", T.StringType(), True),
            T.StructField("link_id", T.StringType(), True),
        ]
    )

    df = (
        df_raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*")
        .withColumn("event_time", F.to_timestamp("created_utc"))
        .withColumn("tokens", tokenize(F.col("text")))
        .withWatermark("event_time", "10 minutes")
    )

    # ---------------------------------------------------------------------
    # 1) Topic matching: foreachBatch so we can reload topics deterministically
    # ---------------------------------------------------------------------
    matched_schema = T.StructType(
        [
            T.StructField("topic_id", T.StringType(), False),
            T.StructField("id", T.StringType(), True),
            T.StructField("subreddit", T.StringType(), True),
            T.StructField("author", T.StringType(), True),
            T.StructField("created_utc", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("score", T.IntegerType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("type", T.StringType(), True),
            T.StructField("tokens", T.ArrayType(T.StringType()), True),
        ]
    )

    def write_matched(batch_df, batch_id: int) -> None:  # noqa: ANN001
        topics = load_topics()
        if not topics:
            return

        # Build a small mapping table of (topic_id, keyword) and (topic_id, subreddit)
        rows: List[tuple] = []
        for t in topics:
            for kw in (t["keywords"] or []):
                rows.append((t["topic_id"], kw, "kw"))
            for sr in (t["subreddits"] or []):
                rows.append((t["topic_id"], sr, "sr"))

        # If a topic has no keywords, we cannot match it deterministically
        rows = [r for r in rows if r[2] == "kw"]
        if not rows:
            return

        map_df = spark.createDataFrame(rows, schema=T.StructType(
            [T.StructField("topic_id", T.StringType(), False),
             T.StructField("term", T.StringType(), False),
             T.StructField("kind", T.StringType(), False)]
        ))

        # explode tokens and join on keyword
        exploded = (
            batch_df.select(
                "id",
                "subreddit",
                "author",
                "created_utc",
                "event_time",
                "score",
                "text",
                "type",
                "tokens",
                F.explode_outer("tokens").alias("token"),
            )
            .withColumn("token", F.lower(F.col("token")))
        )

        joined = exploded.join(map_df, exploded["token"] == map_df["term"], "inner")

        matched = (
            joined.groupBy(
                "topic_id",
                "id",
                "subreddit",
                "author",
                "created_utc",
                "event_time",
                "score",
                "text",
                "type",
                "tokens",
            )
            .agg(F.count("*").alias("keyword_hits"))
            .withColumn("timestamp", F.date_format(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
            .select(
                "topic_id",
                "id",
                "subreddit",
                "author",
                "created_utc",
                "timestamp",
                "score",
                "text",
                "type",
                "tokens",
            )
        )

        out = matched.select(F.to_json(F.struct(*[F.col(c) for c in matched.columns])).alias("value"))
        (
            out.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", MATCHED_TOPIC)
            .save()
        )

    matched_query = (
        df.writeStream.foreachBatch(write_matched)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/matched")
        .start()
    )

    # ---------------------------------------------------------------------
    # 2) Downstream: read matched stream back from Kafka (decouples stages)
    # ---------------------------------------------------------------------
    df_matched_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", MATCHED_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    df_matched = (
        df_matched_raw.select(F.from_json(F.col("value").cast("string"), matched_schema).alias("m"))
        .select("m.*")
        .withColumn("event_time", F.to_timestamp("created_utc"))
        .withWatermark("event_time", "10 minutes")
    )

    # ---------------------------------------------------------------------
    # 3) Metrics: mentions per topic per 5m window
    # ---------------------------------------------------------------------
    metrics = (
        df_matched.groupBy("topic_id", F.window("event_time", "5 minutes").alias("w"))
        .agg(F.count("*").alias("mentions"))
        .withColumn("timestamp", F.date_format(F.col("w").getField("end"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("event_kind", F.lit("metrics"))
        .select("topic_id", "timestamp", "mentions", "event_kind")
    )

    metrics_out = metrics.select(F.to_json(F.struct(*metrics.columns)).alias("value"))
    metrics_query = (
        metrics_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", METRICS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics")
        .outputMode("update")
        .start()
    )

    # ---------------------------------------------------------------------
    # 4) Trends: velocity = short(5m) / long(60m) per topic, computed per 5m tick
    # ---------------------------------------------------------------------
    short = df_matched.groupBy("topic_id", F.window("event_time", "5 minutes").alias("w")).agg(
        F.count("*").alias("mentions_5m")
    )
    long = df_matched.groupBy("topic_id", F.window("event_time", "60 minutes", "5 minutes").alias("w")).agg(
        F.count("*").alias("mentions_60m")
    )
    trends = (
        short.join(long, on=["topic_id", "w"], how="inner")
        .withColumn("velocity", (F.col("mentions_5m") / F.greatest(F.col("mentions_60m"), F.lit(1))).cast("double"))
        .withColumn("timestamp", F.date_format(F.col("w").getField("end"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("event_kind", F.lit("trends"))
        .select("topic_id", "timestamp", F.col("mentions_5m").alias("mentions"), "velocity", "event_kind")
    )

    trends_out = trends.select(F.to_json(F.struct(*trends.columns)).alias("value"))
    trends_query = (
        trends_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TRENDS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trends")
        .outputMode("update")
        .start()
    )

    # ---------------------------------------------------------------------
    # 5) Sentiment: lexicon-based average sentiment per topic per 5m window
    # ---------------------------------------------------------------------
    df_sent = df_matched.withColumn("sent", simple_sentiment(F.col("tokens")))
    sentiment = (
        df_sent.groupBy("topic_id", F.window("event_time", "5 minutes").alias("w"))
        .agg(F.avg("sent").alias("sentiment_avg"))
        .withColumn("timestamp", F.date_format(F.col("w").getField("end"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("event_kind", F.lit("sentiment"))
        .select("topic_id", "timestamp", "sentiment_avg", "event_kind")
    )

    sentiment_out = sentiment.select(F.to_json(F.struct(*sentiment.columns)).alias("value"))
    sentiment_query = (
        sentiment_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", SENTIMENT_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/sentiment")
        .outputMode("update")
        .start()
    )

    # ---------------------------------------------------------------------
    # 6) Global viral topics: top-3 terms by growth short(5m) vs long(60m)
    # ---------------------------------------------------------------------
    df_terms = df.select(F.col("event_time"), F.explode_outer("tokens").alias("term")).where(F.col("term").isNotNull())

    term_short = df_terms.groupBy(F.window("event_time", "5 minutes").alias("w"), "term").agg(
        F.count("*").alias("count_5m")
    )
    term_long = df_terms.groupBy(F.window("event_time", "60 minutes", "5 minutes").alias("w"), "term").agg(
        F.count("*").alias("count_60m")
    )

    term_growth = (
        term_short.join(term_long, on=["w", "term"], how="inner")
        .withColumn("growth_pct", ((F.col("count_5m") - F.col("count_60m")) / F.greatest(F.col("count_60m"), F.lit(1))).cast("double"))
        .withColumn("timestamp", F.date_format(F.col("w").getField("end"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("event_kind", F.lit("global_trends"))
    )

    # rank within each window and keep top-3
    from pyspark.sql.window import Window  # noqa: PLC0415

    w = Window.partitionBy("w").orderBy(F.col("growth_pct").desc(), F.col("count_5m").desc())
    top3 = (
        term_growth.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= 3)
        .select(
            "timestamp",
            "term",
            F.col("count_5m").alias("mentions_5m"),
            F.col("count_60m").alias("mentions_60m"),
            "growth_pct",
            "rank",
            "event_kind",
        )
    )

    global_out = top3.select(F.to_json(F.struct(*top3.columns)).alias("value"))
    global_query = (
        global_out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", GLOBAL_TRENDS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/global_trends")
        .outputMode("update")
        .start()
    )

    print(f"[spark] started at {datetime.utcnow().isoformat()} bootstrap={KAFKA_BOOTSTRAP_SERVERS}")
    spark.streams.awaitAnyTermination()

    # keep references so they don't get GC'd (clarity)
    _ = (matched_query, metrics_query, trends_query, sentiment_query, global_query)


if __name__ == "__main__":
    main()

