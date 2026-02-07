import axios from "axios";
import { Utils } from "./utils.js";

/**
 * Service for interacting with the Reddit API.
 */
export class RedditService {
  /**
   * Executes a GET request to the Reddit API with rate-limit handling.
   */
  static async request(
    url: string,
    params: any,
    userAgent: string,
  ): Promise<any> {
    try {
      // Execute the GET request with the provided user agent and parameters
      const response = await axios.get(url, {
        headers: { "User-Agent": userAgent },
        params,
        timeout: 20000,
      });
      return response.data;
    } catch (error: any) {
      // If rate limited (429), wait for the duration specified by Reddit and retry
      if (error.response?.status === 429) {
        const retryAfter = parseInt(
          error.response.headers["retry-after"] || "5",
        );
        console.warn(
          `[ingestion] Rate limited (429). Waiting ${retryAfter}s...`,
        );

        await Utils.sleep(retryAfter * 1000);
        return this.request(url, params, userAgent);
      }

      // Propagate other errors to the caller
      throw error;
    }
  }

  /**
   * Universal ingestion method using Reddit Search API for a specific topic.
   */
  static async ingestTopicPosts(
    producer: any,
    topic: any,
    config: any,
    options: {
      cutoffTimestamp: number;
      onProgress?: (percentage: number) => Promise<void>;
      heartbeat?: () => Promise<void>;
      maxLimit?: number;
    },
  ) {
    const {
      cutoffTimestamp,
      onProgress,
      heartbeat,
      maxLimit = 20000,
    } = options;

    const query = Utils.buildQuery(topic.keywords);
    if (!query) return;

    const subreddits = topic.subreddits || [];
    const subStr = subreddits.slice(0, 30).join("+");

    let after: string | null = null;
    let fetchedCount = 0;
    let isFinished = false;

    try {
      while (!isFinished && fetchedCount < maxLimit) {
        if (heartbeat) await heartbeat();

        const url = `https://www.reddit.com/r/${subStr}/search.json`;
        const params: any = {
          q: query,
          sort: "new",
          restrict_sr: "on",
          t: "all",
          limit: 100,
        };
        if (after) params.after = after;

        const data = await this.request(url, params, config.USER_AGENT);
        const children = data.data?.children || [];

        if (children.length === 0) break;

        for (const child of children) {
          const msg = Utils.extractContent(child);
          if (!msg) continue;

          const createdUnix = Math.floor(
            new Date(msg.created_utc).getTime() / 1000,
          );
          if (createdUnix < cutoffTimestamp) {
            isFinished = true;
            break;
          }

          await producer.send({
            topic: "reddit.raw.posts",
            messages: [
              {
                key: msg.event_id,
                value: JSON.stringify({ ...msg, topic_id: topic.id }),
              },
            ],
          });
        }

        fetchedCount += children.length;
        const lastChild = children[children.length - 1];
        after = lastChild.data.name;

        if (onProgress && children.length > 0) {
          const lastCreated = lastChild.data.created_utc;
          const now = Math.floor(Date.now() / 1000);
          const totalRange = now - cutoffTimestamp;
          const elapsedRange = now - lastCreated;
          const progress = Utils.clamp(
            (elapsedRange / totalRange) * 100,
            0,
            100,
          );
          await onProgress(progress);
        }

        await Utils.sleep(1000);
      }
    } catch (e: any) {
      console.error(
        `[ingestion] Ingest error for topic ${topic.id}:`,
        e.message,
      );
      throw e;
    }
  }
}
