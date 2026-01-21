# Reddit Insight – Backend

## Part I: Functional Requirements

### 1. Purpose of the System

Reddit Insight is a backend-heavy data processing system designed to ingest, process, and analyze high‑volume, fast‑changing Reddit data in near real time. The system focuses on **topic tracking**, **trend detection**, and **sentiment/statistical analysis** without relying on LLMs at runtime.

The backend is explicitly designed to **justify and require Apache Kafka and Apache Spark**, both from an architectural and data‑volume perspective.

### 2. Core Functional Features

The system provides value through two main mechanisms: *user-defined topic insights* and *automatic detection of globally viral topics*.

#### 2.1 Topic Management (User-Defined)

**Description**
Users define topics to track using a **natural language description**. The backend converts this description into a structured tracking configuration.

**Topic Configuration Includes**
* Topic ID
* Description (free text)
* Keywords list
* Subreddits list
* Filters (score, time window, post type)
* Update frequency
* Activation status

**Backend Responsibilities**
* Store user-defined topics
* Maintain topic configuration metadata
* Associate topics with tracking rules

*Note: Topic expansion (keyword/subreddit inference) may use AI offline or rule‑based logic, but runtime tracking is deterministic.*

#### 2.2 Global Viral Topic Detection (Automatic)

**Description**
The system continuously detects the **top 3 viral topics** across all ingested Reddit data, independently of any user-defined topics. This ensures the system delivers immediate insight value without requiring user configuration.

A viral topic is defined as a term or keyword cluster that shows a statistically significant increase in activity compared to its historical baseline.

**Backend Responsibilities**
* Continuously monitor all incoming Reddit events
* Rank topics by growth velocity
* Persist the current Top 3 viral topics

---

## Part II: Technical Architecture & Processing

### 1. Introduction

This section provides a **technical, implementation-level explanation** of how RedditInsight works internally. It explains how data flows through the system, how Apache Kafka and Apache Spark are used, and how precise topic metrics are computed **without relying on LLMs**.

The document is intended for:
* academic evaluation (TAP project)
* backend implementation guidance
* architectural justification

### 2. System Overview

RedditInsight is a **real-time data analytics platform** designed to process large volumes of Reddit activity and extract statistically meaningful insights.

Core principles:
* Event-driven architecture
* Stream-first processing
* Deterministic analytics
* Horizontal scalability

The system produces two main outputs:
1. **User-defined topic analytics**
2. **Global viral topic detection (Top 3)**

### 3. Data Model

#### 3.1 Event Definition
Each Reddit post or comment is modeled as an immutable event.

Core event fields:
* `event_id`
* `type` (post | comment)
* `subreddit`
* `author`
* `created_utc`
* `score`
* `text`

Events are never mutated once ingested.

### 4. Data Ingestion Layer (Kafka)

#### 4.1 Ingestion Strategy
Because Reddit does not provide push-based streaming APIs, ingestion is performed via **high-frequency polling**.

The ingestion service:
* periodically queries Reddit APIs
* normalizes responses
* publishes each post/comment as a Kafka message

#### 4.2 Kafka Topics

| Topic                  | Purpose                    |
| ---------------------- | -------------------------- |
| `reddit.posts.raw`     | Raw post events            |
| `reddit.comments.raw`  | Raw comment events         |
| `reddit.topic.matched` | Events matched to topics   |
| `reddit.global.trends` | Global viral topic metrics |

Kafka acts as a **durable event log**, enabling replay and decoupled processing.

### 5. Stream Processing Layer (Apache Spark)

Spark Structured Streaming consumes events from Kafka and performs **stateful, window-based analytics**.

#### 5.1 Text Preprocessing
Each event text undergoes deterministic preprocessing:
* lowercasing
* punctuation removal
* tokenization
* optional stopword filtering

No semantic inference is performed.

#### 5.2 Topic Matching (User-Defined Topics)
Each user-defined topic contains:
* keyword sets
* allowed subreddits
* score thresholds

Spark performs:
* keyword or n-gram matching
* contextual filtering by subreddit
* assignment of `topic_id` to events

Matched events are emitted to `reddit.topic.matched`.

#### 5.3 Windowed Aggregations
Spark maintains **sliding and tumbling windows** for each topic.

Computed metrics:
* mention count
* engagement-weighted count
* subreddit distribution
* temporal velocity

Velocity is computed as:
`velocity = short_window_count / long_window_average`

This allows detection of abnormal increases in activity.

#### 5.4 Trend Detection Logic
A topic is considered trending if:
* velocity exceeds a configurable threshold
* activity persists across multiple windows

This logic is purely statistical and deterministic.

#### 5.5 Global Viral Topic Detection
Spark continuously analyzes **all incoming events**, independent of user topics.

Process:
1. extract candidate tokens and n-grams
2. aggregate frequencies over short and long windows
3. compute growth deltas
4. rank candidates by velocity
5. select top 3

This ensures system value without user configuration.

#### 5.6 Sentiment Analysis (Optional, Non-AI)
Sentiment scoring uses **lexicon-based methods** (e.g. VADER):
* deterministic
* explainable
* lightweight

Sentiment trends are aggregated per topic over time.

### 6. Storage Layer

#### 6.1 Raw Data Storage
Raw Kafka data may be persisted to object storage for:
* debugging
* replay
* offline experiments

#### 6.2 Processed Metrics Storage
Aggregated metrics are stored in:
* relational DB or
* time-series database

Spark writes only **final aggregates**, never raw text.

### 7. Backend API Layer

The backend exposes read-only APIs:

#### 7.1 Topic APIs
* create/update/delete topic definitions

#### 7.2 Analytics APIs
* retrieve metrics
* retrieve trends
* retrieve sentiment summaries
* retrieve global viral topics

All APIs serve **precomputed results only**.

### 8. Fault Tolerance & Scalability

#### Kafka
* partitioned topics
* durable logs
* replay support

#### Spark
* checkpointing
* exactly-once processing
* horizontal scaling

No centralized coordinator (e.g. ZooKeeper) is used.

### 9. Optional Local LLM Layer (Out of Core System)

A local LLM may optionally consume Spark outputs to:
* generate summaries
* explain trends
* compare topics

The LLM never accesses raw data or streaming logic.

### 10. Why the System Is Precise Without AI

Precision arises from:
* contextual constraints
* multi-word patterns
* temporal deltas
* statistical filtering

The system detects **changes in behavior**, not word meanings.

### 11. Conclusion

RedditInsight is a fully stream-based analytics platform where:
* Kafka handles ingestion and buffering
* Spark performs deterministic intelligence
* insights emerge from temporal statistics

The architecture is scalable, explainable, and academically defensible.
