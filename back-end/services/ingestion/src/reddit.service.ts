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
   * Polls Reddit for new posts and comments across a list of subreddits.
   */
  static async poll(producer: any, subs: string[], lastSeen: any, config: any) {
    // Join subreddits with '+' to fetch from multiple subreddits in a single request
    const subStr = subs.join("+");

    // Iterate through both posts and comments to fetch new content
    for (const type of ["post", "comment"] as const) {
      try {
        // Construct the Reddit API URL for either new posts or recent comments
        const url = `https://www.reddit.com/r/${subStr}/${type === "post" ? "new" : "comments"}.json`;

        // Fetch data from Reddit with configured limit and user agent
        const data = await this.request(
          url,
          { limit: config.POST_LIMIT },
          config.USER_AGENT,
        );

        const children = data.data?.children || [];

        // Process each item returned by the API
        for (const child of children) {
          const msg = Utils.extractContent(child, type);

          // Check if the item is valid and hasn't been processed in the current session
          if (msg && !lastSeen[type].has(msg.event_id)) {
            // Send the raw content to the respective Kafka topic
            await producer.send({
              topic: `reddit.raw.${type}s`,
              messages: [{ key: msg.event_id, value: JSON.stringify(msg) }],
            });

            // Track the event ID to prevent duplicate processing
            lastSeen[type].add(msg.event_id);
          }
        }

        // Wait briefly between requests to respect API etiquette
        await Utils.sleep(2000);
      } catch (e: any) {
        console.error(`[ingestion] Error polling ${type}s:`, e.message);
      }
    }
  }
}
