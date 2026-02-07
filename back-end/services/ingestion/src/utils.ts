import { Kafka, Producer } from "kafkajs";
import dayjs from "dayjs";

/**
 * Shared utility methods for the ingestion service.
 */
export class Utils {
  /**
   * Pauses the execution for a given number of milliseconds.
   */
  static sleep = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  /**
   * Connects and returns a Kafka Producer. Retries indefinitely if the broker is unavailable.
   */
  static async getProducer(kafka: Kafka): Promise<Producer> {
    const producer = kafka.producer();
    while (true) {
      try {
        await producer.connect();
        return producer;
      } catch (e: any) {
        console.error(
          `[ingestion] Kafka connection failed: ${e.message}. Retrying...`,
        );
        await Utils.sleep(5000);
      }
    }
  }

  /**
   * Normalizes Reddit API data (posts) into our unified schema.
   * Ensures consistent fields for downstream processing (Spark/Frontend).
   */
  static extractContent(child: any): any {
    const d = child.data || {};
    if (!d.id) return null;

    return {
      event_id: d.name || d.id,
      event_type: "post",
      subreddit: d.subreddit || "",
      author: d.author || null,
      created_utc: dayjs
        .unix(parseFloat(d.created_utc || dayjs().unix().toString()))
        .toISOString(),
      score: parseInt(d.score || "0"),
      ingested_at: dayjs().toISOString(),
      text: `${d.title || ""}\n${d.selftext || ""}`,
      num_comments: parseInt(d.num_comments || "0"),
    };
  }

  /**
   * Build Reddit Search Query (CNF: [[A, B], [C]] -> (A OR B) AND (C))
   */
  static buildQuery(keywords: any): string {
    if (!Array.isArray(keywords) || keywords.length === 0) return "";

    const quote = (s: string) => (s.includes(" ") ? `"${s}"` : s);

    if (Array.isArray(keywords[0])) {
      // CNF Logic: list of lists
      return (keywords as string[][])
        .map((group) => `(${group.map(quote).join(" OR ")})`)
        .join(" AND ");
    } else {
      // Legacy Flat List
      return (keywords as string[]).map(quote).join(" OR ");
    }
  }

  /**
   * Clamps a number between a minimum and maximum value.
   */
  static clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
  }
}
