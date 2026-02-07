import os
import re
import time
import requests
from pyspark.sql import SparkSession, functions as F

# Kafka Bootstrap Servers: The address of the Kafka cluster.
KAFKA_URL         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# API Endpoint: The internal API to fetch topics from.
# In production, this should be an internal DNS/Service Discovery name.
API_TOPICS_URL    = os.getenv("API_TOPICS_URL", "http://trend-api:8000/topics")

# Checkpoint Directory: Critical for Structured Streaming fault tolerance.
CHECKPOINT_DIR    = "/checkpoints"



# Input Schema: Defines the structure of the JSON data coming from Reddit.
# We now include 'topic_id' because the Ingestion service matches at source.
POST_SCHEMA       = "topic_id STRING, event_id STRING, created_utc STRING, text STRING, score INT, num_comments INT"



def main():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("SimpleStreamer").getOrCreate()

    
    # READ KAFKA
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", "reddit.raw.posts")  # User Request: Ignore comments for strict consistency
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
    )

    # TRANSFORM & AGGREGATE
    # Ingestion now associates topic_id at the source. We no longer need to scan text in Spark.
    metrics_stream = (
        raw_stream
        .withColumn("timestamp", F.col("created_utc").cast("timestamp"))
        .withColumn("engagement", F.col("score") + F.col("num_comments") + 1)
        .selectExpr(
            "topic_id", 
            "event_id", 
            "created_utc",
            "timestamp", 
            "text", 
            "score", 
            "num_comments", 
            "engagement",
            "'matched_post' as event_type"
        )
    )

    # WRITE TO KAFKA
    # This topic will be consumed by Logstash to UPSERT into Elasticsearch.
    (metrics_stream.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", "reddit.topic.matches")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/matches_granular")
        .outputMode("append")
        .start()
        .awaitTermination())

if __name__ == "__main__":
    main()
