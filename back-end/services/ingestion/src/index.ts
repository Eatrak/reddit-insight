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
 * Periodic worker that syncs recent activity for all topics.
 */
async function syncTopicsPosts() {
  console.log("[ingestion] Starting Topic-Based Sync Worker...");
  const producer = await Utils.getProducer(kafka);

  while (true) {
    try {
      // Fetch the latest topic definitions from the API
      const res = await axios.get(`${CONFIG.API_BASE_URL}/topics`);
      const topics = (res.data as any[]) || [];
      const activeTopics = topics.filter((t: any) => t.is_active);

      if (activeTopics.length > 0) {
        console.log(
          `[ingestion] Syncing ${activeTopics.length} active topics...`,
        );

        for (const topic of activeTopics) {
          try {
            console.log(
              `[ingestion] Syncing topic: ${topic.title || topic.id}`,
            );

            // Sync logic: Fetch posts from the last 48 hours
            const cutoff = Math.floor(Date.now() / 1000) - 48 * 60 * 60;

            await RedditService.ingestTopicPosts(producer, topic, CONFIG, {
              cutoffTimestamp: cutoff,
            });
          } catch (e: any) {
            console.error(
              `[ingestion] Error syncing topic ${topic.id}:`,
              e.message,
            );
          }
        }
      }
    } catch (e: any) {
      console.error(`[ingestion] Sync cycle error:`, e.message);
    }

    // Wait 1 minute between sync cycles
    await Utils.sleep(60 * 1000);
  }
}

/**
 * Worker to handle historical backfill requests.
 */
async function startBackfilling() {
  console.log(
    "[backfill] Starting worker. Waiting 3s for system stabilization...",
  );
  await Utils.sleep(3000);

  const consumer = kafka.consumer({
    groupId: "ingestion-backfill-worker",
    sessionTimeout: 120000,
    heartbeatInterval: 30000,
    rebalanceTimeout: 120000,
  });
  const producer = await Utils.getProducer(kafka);

  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        try {
          const rawValue = message.value?.toString();
          if (!rawValue) continue;

          const payload = JSON.parse(rawValue);
          const { topic_id, subreddits = [], lookback_seconds } = payload;

          const res = await axios.get(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}`,
          );
          const topic = res.data as any;
          const query = Utils.buildQuery(topic.keywords);

          if (!query || !subreddits.length) {
            await axios.patch(
              `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
              {
                status: "COMPLETED",
                percentage: 100,
              },
            );
            resolveOffset(message.offset);
            continue;
          }

          // Fetch topic posts of the last 30 days
          const cutoff = dayjs().unix() - (lookback_seconds || 30 * 24 * 3600);

          await RedditService.ingestTopicPosts(producer, topic, CONFIG, {
            cutoffTimestamp: cutoff,
            heartbeat: async () => await heartbeat(),
            onProgress: async (pct: number) => {
              await axios.patch(
                `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
                {
                  percentage: Math.round(pct),
                },
              );
            },
          });

          await axios.patch(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
            {
              status: "COMPLETED",
              percentage: 100,
            },
          );

          resolveOffset(message.offset);
          await heartbeat();
        } catch (err: any) {
          console.error(`[backfill] Task error:`, err.message);
        }
      }
    },
  });
}

syncTopicsPosts();
startBackfilling();
