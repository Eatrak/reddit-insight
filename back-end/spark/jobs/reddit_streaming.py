import json
import os
import re
import time
import requests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

"""
REDDIT STREAMING JOB
====================

This Spark Structured Streaming job is responsible for:
1.  Ingesting raw Reddit data (posts/comments) from Kafka.
2.  Scanning the text content against a dynamic list of Topics (keywords) fetched from an API.
3.  Aggregating metrics (mentions, engagement scores) per Topic per Day.
4.  Writing the aggregated metrics back to Kafka for downstream consumption.

Concepts:
-   Structured Streaming: Processing data as an infinite table.
-   UDF (User Defined Function): Custom Python logic for text scanning.
-   Watermarking: Handling late data and state cleanup.
"""

# =============================================================================
# 1. SETTINGS & CONFIGURATION
# =============================================================================
# Kafka Bootstrap Servers: The address of the Kafka cluster.
KAFKA_URL         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# API Endpoint: The internal API to fetch topics from.
# In production, this should be an internal DNS/Service Discovery name.
API_TOPICS_URL    = os.getenv("API_TOPICS_URL", "http://trend-api:8000/topics")

# Checkpoint Directory: Critical for Structured Streaming fault tolerance.
CHECKPOINT_DIR    = "/checkpoints"

# Output Topic: Where we send the calculated metrics.
DAILY_METRICS_OUT = "reddit.topic.metrics"

# Input Schema: Defines the structure of the JSON data coming from Reddit.
POST_SCHEMA       = "event_id STRING, created_utc STRING, text STRING, score INT, num_comments INT"


# =============================================================================
# 2. INTELLIGENT SCANNER (Stateful UDF)
# =============================================================================
# Global cache for workers. Spark recycles Python worker processes, so this global
# persists across multiple tasks/micro-batches processed by the same worker.
_TOPIC_CACHE = {"regex": None, "lookup": {}, "last_updated": 0}

def get_topics_cached():
    """
    Worker-side caching mechanism.
    
    This function ensures that the topic definitions (keywords) are loaded
    into memory. It fetches from the API at most once every 60 seconds.
    """
    now = time.time()
    
    # 1. Check refresh interval (Throttle API calls to avoid N+1 / DDoS)
    if now - _TOPIC_CACHE["last_updated"] > 10:
        try:
            # 2. Fetch from API
            # Timeout is critical. If API is slow, don't block the stream forever.
            response = requests.get(API_TOPICS_URL, timeout=5)
            
            if response.status_code == 200:
                topics_list = response.json()
                
                lookup = {} # Mapping: "keyword" -> [Topic ID 1, Topic ID 2]
                all_terms = set()
                
                for r in topics_list:
                    # Skip inactive topics if the API returns them
                    if not r.get("is_active", True):
                        continue
                        
                    param_value = r.get("keywords")
                    
                    kws = []
                    if isinstance(param_value, list):
                        kws = [str(x).strip().lower() for x in param_value if x]
                    elif isinstance(param_value, str):
                        raw = param_value.strip()
                        if raw:
                             kws = json.loads(raw) if raw.startswith("[") else [x.strip().lower() for x in raw.split(",") if x]
                    
                    # Build the lookup table
                    for k in kws:
                        if k not in lookup: lookup[k] = []
                        lookup[k].append(r["id"])
                        all_terms.add(k)
                
                # 3. Compile Regex
                if all_terms:
                    # Sort by length descending. This is CRITICAL for regex correctness.
                    sorted_terms = sorted(list(all_terms), key=len, reverse=True)
                    
                    # Create one giant pattern: (term1|term2|term3)
                    pattern = "|".join([re.escape(t) for t in sorted_terms])
                    _TOPIC_CACHE["regex"] = re.compile(f"(?i)({pattern})")
                else:
                    _TOPIC_CACHE["regex"] = None
                    
                _TOPIC_CACHE["lookup"] = lookup
                _TOPIC_CACHE["last_updated"] = now
        except Exception as e:
            # Fail Open: If API is down, we just keep using the old cache.
            # Only print/log if you have a logging framework setup
            pass
            
    return _TOPIC_CACHE

def main():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("SimpleStreamer").getOrCreate()

    # Define the UDF (User Defined Function)
    @F.udf(returnType="array<struct<topic_id:string, term:string>>")
    def scan_text(text):
        """
        Scans a text string identifying all matching topics.
        """
        if not text: return []
        
        # Retrieve the cached regex/lookup table (lazy loading)
        cache = get_topics_cached()
        regex = cache["regex"]
        lookup = cache["lookup"]
        
        if not regex: return []

        matches = []
        # Single Pass Scan: regex.findall returns all non-overlapping matches
        found_terms = set(regex.findall(text))
        
        for term in found_terms:
            lower_term = term.lower()
            if lower_term in lookup:
                for tid in lookup[lower_term]:
                    matches.append((tid, lower_term))
                    
        return list(set(matches))
    
    # =========================================================================
    # PIPELINE STEP 1: READ KAFKA
    # =========================================================================
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", "reddit.raw.posts,reddit.raw.comments")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
    )

    # =========================================================================
    # PIPELINE STEP 2: TRANSFORM & AGGREGATE
    # =========================================================================
    metrics_stream = (
        raw_stream
        .withColumn("matches", scan_text(F.col("text")))
        .filter("size(matches) > 0")
        .selectExpr("*", "explode(matches) as m")
        .selectExpr("*", "m.topic_id", "m.term")
        .drop("matches", "m")
        .withColumn("time", F.col("created_utc").cast("timestamp"))
        .withWatermark("time", "35 days")
        .dropDuplicates(["event_id", "topic_id"])
        .groupBy("topic_id", F.window("time", "1 day").alias("day"))
        .agg(
            F.count("*").alias("mentions"),
            F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
        )
        .selectExpr(
            "topic_id", "mentions", "engagement",
            "day.start as start", "day.end as end",
            "'1d' as window_type"
        )
    )

    # =========================================================================
    # PIPELINE STEP 3: WRITE TO KAFKA
    # =========================================================================
    (metrics_stream.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", DAILY_METRICS_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/metrics_simple_v3")
        .outputMode("update")
        .start()
        .awaitTermination())

if __name__ == "__main__":
    main()
