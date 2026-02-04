import axios from "axios";
import { Kafka, logLevel } from "kafkajs";
import dotenv from "dotenv";
import dayjs from "dayjs";
import { Utils } from "./utils.js";
import { RedditService } from "./reddit.service.js";

// Initialize environment variables
dotenv.config();

/**
 * Service Configuration
 */
const CONFIG = {
  KAFKA_SERVERS: (
    process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"
  ).split(","),
  USER_AGENT: process.env.REDDIT_USER_AGENT || "trend-insight/0.1",
  POLL_INTERVAL: parseInt(process.env.REDDIT_POLL_INTERVAL_SECONDS || "15"),
  POST_LIMIT: parseInt(process.env.REDDIT_LIMIT || "50"),
  API_BASE_URL: process.env.API_BASE_URL || "http://trend-api:8000",
};

const kafka = new Kafka({
  clientId: "ingestion-service",
  brokers: CONFIG.KAFKA_SERVERS,
  logLevel: logLevel.ERROR,
});

/**
 * Worker for real-time data ingestion.
 */
async function startRealTimeIngestion() {
  // Initialize Kafka producer for publishing events
  const producer = await Utils.getProducer(kafka);

  // Maintain local state to deduplicate posts and comments within the ingestion cycle
  const lastSeen = { post: new Set<string>(), comment: new Set<string>() };

  while (true) {
    try {
      // Fetch the list of subreddits to monitor from the system API
      const res = await axios.get(`${CONFIG.API_BASE_URL}/subreddits`, {
        timeout: 5000,
      });
      const subreddits =
        (res.data as { subreddits: string[] }).subreddits || [];

      if (subreddits.length > 0) {
        // Poll Reddit for new activity in the retrieved subreddits
        // We limit to 100 subreddits per batch to avoid rate limiting or memory issues
        await RedditService.poll(
          producer,
          subreddits.slice(0, 100),
          lastSeen,
          CONFIG,
        );
        // Brief pause between batches to respect Reddit API guidelines
        await Utils.sleep(2000);
      }

      // Memory Management: Prune deduplication sets if they grow too large
      // This prevents the ingestion worker from exhausting memory over long periods
      for (const key in lastSeen) {
        const set = (lastSeen as { [key: string]: Set<string> })[key];
        if (set.size > 20000) {
          (lastSeen as { [key: string]: Set<string> })[key] = new Set(
            Array.from(set).slice(-10000),
          );
        }
      }
    } catch (e: any) {
      console.error(`[ingestion] Poll error:`, e.message);
    }

    // Wait for the configured interval before the next polling cycle
    await Utils.sleep(CONFIG.POLL_INTERVAL * 1000);
  }
}

/**
 * Worker to handle historical backfill requests.
 */
async function startBackfilling() {
  const consumer = kafka.consumer({
    groupId: "ingestion-backfill-worker",
    // 2 mins. How long Kafka is willing to wait before it gives up on the worker.
    // By default it's 30 seconds, but if a request to Reddit takes longer than that, Kafka will consider the worker as failed.
    sessionTimeout: 120000,
    // 30s. How often the worker intends to check in.
    // We also use manual heartbeats during active backfilling,
    // because during backfilling the worker is busy and doesn't want to check in.
    heartbeatInterval: 30000,
    // 2 mins. To allow long-running backfills to complete gracefully before Kafka reorders workers
    rebalanceTimeout: 120000,
  });
  const producer = await Utils.getProducer(kafka);

  // Establish connection and subscribe to the backfill task queue
  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  // Start the consumer loop to process backfill requests
  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        // Exit if the consumer is stopping or the batch has become stale
        if (!isRunning() || isStale()) break;

        try {
          const rawValue = message.value?.toString();
          if (!rawValue) continue;

          const payload = JSON.parse(rawValue);
          const { topic_id, subreddits = [] } = payload;

          console.log(
            `[backfill] Topic: ${topic_id} | Subreddits: ${subreddits.join(", ")}`,
          );

          // Warm-up Delay: Wait 15s to ensure Spark workers (10s cache TTL) pick up the new topic
          console.log(
            `[backfill] Waiting 15s for Spark to sync topic cache...`,
          );
          await Utils.sleep(15000);

          // If no subreddits are provided, mark the task as completed immediately
          if (!Array.isArray(subreddits) || !subreddits.length) {
            await axios.patch(
              `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
              { status: "COMPLETED", percentage: 100 },
            );
            resolveOffset(message.offset);
            continue;
          }

          // Backfill constraints: limit total posts and look back only 7 days
          const MAX_FETCH_LIMIT = 5000;
          const LOOKBACK_SECONDS = 7 * 24 * 3600;
          const cutoff = dayjs().unix() - LOOKBACK_SECONDS;
          const subredditsQuery = subreddits.slice(0, 100).join("+");

          let after: string | null = null;
          let fetchedCount = 0;
          let isFinished = false;
          const seenInSession = new Set<string>();

          // Pagination loop to fetch historical data from Reddit
          while (!isFinished && fetchedCount < MAX_FETCH_LIMIT) {
            try {
              // Send heartbeat to Kafka to prevent session timeout during long-running HTTP requests
              await heartbeat();

              const response = await RedditService.request(
                `https://www.reddit.com/r/${subredditsQuery}/new.json`,
                { limit: 100, after },
                CONFIG.USER_AGENT,
              );

              if (!response || !response.data) break;

              const posts = response.data.children || [];
              if (!posts.length) break;

              for (const post of posts) {
                const postToStore = Utils.extractContent(post, "post");
                if (!postToStore) continue;

                const isPastCutoff =
                  dayjs(postToStore.created_utc).unix() < cutoff;

                // Stop fetching if we've reached the historical age limit
                if (isPastCutoff) {
                  isFinished = true;
                  break;
                }

                // Local deduplication to avoid redundant Kafka messages
                if (seenInSession.has(postToStore.event_id)) continue;
                seenInSession.add(postToStore.event_id);

                // Publish the raw post data to the internal processing pipeline
                await producer.send({
                  topic: "reddit.raw.posts",
                  messages: [
                    {
                      key: postToStore.event_id,
                      value: JSON.stringify(postToStore),
                    },
                  ],
                });
              }

              fetchedCount += posts.length;
              let lastPost = posts[posts.length - 1];
              after = lastPost.data.name;

              // Calculate progress based on the age of the last fetched post relative to the cutoff
              const progress = Utils.clamp(
                ((dayjs().unix() - lastPost.data.created_utc) /
                  LOOKBACK_SECONDS) *
                  100,
                0,
                100,
              );
              console.log(`[backfill] Progress for ${topic_id}: ${progress}%`);

              await axios.patch(
                `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
                { percentage: progress },
              );

              // Rate limiting: sleep between pagination requests
              await Utils.sleep(1500);
            } catch (error: any) {
              console.error(
                `[backfill] Error in batch for ${topic_id}:`,
                error.message,
              );
              break;
            }
          }

          // Finalize task status in the API
          await axios.patch(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
            { status: "COMPLETED", percentage: 100 },
          );

          // Mark the message as processed in Kafka
          resolveOffset(message.offset);
          await heartbeat();
        } catch (err: any) {
          console.error(
            `[backfill] Critical error processing message:`,
            err.message,
          );
        }
      }
    },
  });
}

startRealTimeIngestion();
startBackfilling();
