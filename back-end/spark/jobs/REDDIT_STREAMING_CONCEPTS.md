# Reddit Streaming Job Concepts

This document explains the core concepts and architecture used in the `reddit_streaming.py` Spark job. This job is a real-time data processing pipeline that ingests Reddit posts/comments, identifies mentions of specific topics, aggregates metrics, and attempts to output insights.

## Architecture Overview

The pipeline follows a standard "Kappa Architecture" stream processing pattern:

1.  **Source**: Raw data (Reddit posts/comments) arrives in Kafka.
2.  **Process**: Spark Structured Streaming reads the data, processes it, and aggregates it.
3.  **Sink**: Processed metrics are written back to a different Kafka topic for downstream consumption (e.g., by an indexer or API).

## Key Concepts

### 1. Spark Structured Streaming

Unlike traditional "batch" processing (running a job on a fixed dataset once), **Structured Streaming** treats a live data stream as a table that is being continuously appended to.

- **"Infinite Table"**: You write queries as if you were querying a static table, and Spark runs them incrementally as new data arrives.
- **Micro-batches**: Spark processes data in small chunks (micro-batches) to achieve low latency (seconds to minutes).

### 2. Kafka Integration

**Kafka** is the central nervous system here.

- **Source (`readStream`)**: Spark acts as a Kafka consumer. It subscribes to `reddit.raw.posts` and `reddit.raw.comments`.
- **Sink (`writeStream`)**: After processing, Spark writes the results to `reddit.topic.metrics`. This decouples the processing from the storage/serving layer.
- **Offsets**: Spark tracks Kafka offsets (positions in the stream) in a **Checkpoint** directory. If the job crashes, it restarts reading exactly where it left off.

### 3. User Defined Functions (UDFs) & Text Scanning

Standard SQL functions often aren't enough for complex logic like "find all keywords from a dynamic list in this text."

- **UDF (`@F.udf`)**: We wrap a Python function `scan_text` so Spark can use it in SQL queries.
- **The Challenge**: Distributing the "Topic Dictionary" to thousands of worker nodes.
- **The Solution (Worker-Side Caching)**:
  - Each Spark worker node gets a copy of the SQLite database (`topics.db`).
  - The `scan_text` function checks a global cache `_TOPIC_CACHE` on the worker.
  - If the cache is stale (older than 60s), it reloads the topics from the DB and compiles a generic **Regular Expression**.
  - **Optimization**: Instead of looping through thousands of keywords for every post (O(N\*M)), we compile all keywords into one massive Regex `(term1|term2|...)`. This allows the C-based regex engine to scan the text in a single pass (roughly O(1)).

### 4. Event Time vs. Processing Time

- **Event Time**: The time the event actually happened (`created_utc` from Reddit).
- **Processing Time**: The time the system processed the event.
- **Watermarking (`.withWatermark`)**: This tells Spark how long to wait for "late" data.
  - `withWatermark("time", "35 days")` means "If data arrives with a timestamp older than 35 days ago, ignore it; otherwise, update the old counts."
  - This is crucial for managing state. Without a watermark, Spark would have to keep intermediate aggregation state for _all time_ in memory, eventually crashing. Watermarks allow Spark to drop old state.

### 5. Windowing & Aggregation

We want to know "How many mentions per day?".

- `groupBy(F.window("time", "1 day"))`: This buckets data into 1-day windows based on the **Event Time**.
- **Update Mode**: Since we allow late data (up to 35 days), the count for "Yesterday" might change today if a late event arrives. Spark handles this by emitting an _update_ for that window.

### 6. Output Modes

When writing to the sink (Kafka), we choose how to output data:

- **Append**: Only output rows when they are finalized (i.e., watermark has passed). Good for "immutable" logs.
- **Update**: Output rows as soon as they are updated. If the count for "2023-10-27" changes from 5 to 6, emit the new row. This is what we use.
- **Complete**: Output the entire state every time. Too heavy for infinite streams.

### 7. Checkpointing

`option("checkpointLocation", ...)` is mandatory for production streams. It saves:

- **Metadata**: Which files/offsets have been processed.
- **State**: Intermediate aggregation counts (e.g., "current count for topic X on day Y").
- **Fault Tolerance**: If the job dies, clearing the checkpoint directory essentially resets the job to zero.

## Summary of the Data Flow

1.  **Read** JSON strings from Kafka.
2.  **Parse** JSON into columns (text, created_utc, etc.).
3.  **Scan** text using the optimized Regex UDF to find Topic IDs.
4.  **Explode** rows: One post might match 3 topics -> becomes 3 rows.
5.  **Watermark** & Deduplicate to handle replays/late data.
6.  **Aggregate** counts and scores by `(topic_id, 1_day_window)`.
7.  **Write** updated metrics back to Kafka.
