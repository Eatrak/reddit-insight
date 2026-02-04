import OpenAI from "openai";
import express, { Request, Response } from "express";
import Database from "better-sqlite3";
import { Client } from "@elastic/elasticsearch";
import dotenv from "dotenv";
import { Kafka } from "kafkajs";
import path from "path";

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8000;
const TOPICS_DB_PATH = process.env.TOPICS_DB_PATH || "/data/topics.db";
const ELASTICSEARCH_URL =
  process.env.ELASTICSEARCH_URL || "http://elasticsearch:9200";

// ----------------------------------------------------------------------------
// OpenAI Client
// ----------------------------------------------------------------------------
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const DEFAULT_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

// ----------------------------------------------------------------------------
// Storage: SQLite (Topics)
// ----------------------------------------------------------------------------
const db = new Database(TOPICS_DB_PATH);

// Initialize DB Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS topics (
    id TEXT PRIMARY KEY,
    description TEXT,
    keywords TEXT, -- JSON array or CSV
    subreddits TEXT, -- CSV
    filters_json TEXT, -- JSON string
    update_frequency_seconds INTEGER DEFAULT 60,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    backfill_status TEXT DEFAULT 'IDLE', -- IDLE, PENDING, COMPLETED, ERROR
    backfill_percentage REAL DEFAULT 0.0
  );
`);

// Migration: Add backfill_status if missing (for existing DBs)
try {
  db.exec("ALTER TABLE topics ADD COLUMN backfill_status TEXT DEFAULT 'IDLE'");
} catch (e: any) {
  // Ignore
}

// Migration: Add backfill_percentage if missing
try {
  db.exec("ALTER TABLE topics ADD COLUMN backfill_percentage REAL DEFAULT 0.0");
} catch (e: any) {
  // Ignore
}

// ... (Rest of code)

// 9. PATCH /topics/:id/status
app.patch("/topics/:id/status", (req: Request, res: Response) => {
  try {
    const { status, percentage } = req.body;
    const id = req.params.id;

    if (status) {
      db.prepare("UPDATE topics SET backfill_status = ? WHERE id = ?").run(
        status,
        id,
      );
    }

    if (percentage !== undefined) {
      db.prepare("UPDATE topics SET backfill_percentage = ? WHERE id = ?").run(
        percentage,
        id,
      );
    }

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// ----------------------------------------------------------------------------
// Kafka Producer (for backfill tasks)
// ----------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: "api-service",
  brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS || "kafka:9092").split(","),
});
const producer = kafka.producer();

const connectProducer = async () => {
  let retries = 5;
  while (retries > 0) {
    try {
      await producer.connect();
      console.log("Kafka Producer connected");
      return;
    } catch (error) {
      console.error("Kafka Producer connection failed, retrying...", error);
      retries--;
      await new Promise((res) => setTimeout(res, 5000));
    }
  }
  console.error("Could not connect to Kafka after multiple retries.");
};

connectProducer();

// ----------------------------------------------------------------------------
// Storage: Elasticsearch (Metrics, Trends)
// ----------------------------------------------------------------------------
const esClient = new Client({
  node: ELASTICSEARCH_URL,
  auth: {
    username: process.env.BASIC_AUTH_USER || "admin",
    password: process.env.BASIC_AUTH_PASS || "admin",
  },
  tls: { rejectUnauthorized: false },
});

// ----------------------------------------------------------------------------
// API Endpoints
// ----------------------------------------------------------------------------

app.get("/subreddits", (req: Request, res: Response) => {
  const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  res.json({ subreddits: allowedSubreddits });
});

// 2. GET /topics
app.get("/topics", (req: Request, res: Response) => {
  try {
    const stmt = db.prepare("SELECT * FROM topics");
    const rows = stmt.all();
    // Parse JSON fields
    const topics = rows.map((r: any) => ({
      ...r,
      keywords: isJson(r.keywords) ? JSON.parse(r.keywords) : r.keywords,
      subreddits: (r.subreddits || "").split(","),
      filters: JSON.parse(r.filters_json || "{}"),
    }));
    res.json(topics);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 1. POST /topics
app.post("/topics", (req: Request, res: Response) => {
  try {
    const {
      id,
      description,
      keywords,
      subreddits,
      filters,
      update_frequency,
      is_active,
    } = req.body;

    // Basic validation
    if (!id || !keywords || !subreddits) {
      res
        .status(400)
        .json({ error: "Missing required fields: id, keywords, subreddits" });
      return;
    }

    const stmt = db.prepare(`
      INSERT INTO topics (id, description, keywords, subreddits, filters_json, update_frequency_seconds, is_active, created_at, backfill_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'IDLE')
    `);

    stmt.run(
      id,
      description || "",
      Array.isArray(keywords) ? JSON.stringify(keywords) : keywords,
      Array.isArray(subreddits) ? subreddits.join(",") : subreddits,
      JSON.stringify(filters || {}),
      update_frequency || 60, // Default to 60s
      is_active ? 1 : 0,
      new Date().toISOString(),
    );

    res.status(201).json({ message: "Topic created", id });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 3. GET /topics/{id}
app.get("/topics/:id", (req: Request, res: Response) => {
  try {
    const stmt = db.prepare("SELECT * FROM topics WHERE id = ?");
    const topic = stmt.get(req.params.id) as any;

    if (!topic) {
      res.status(404).json({ error: "Topic not found" });
      return;
    }

    res.json({
      ...topic,
      keywords: isJson(topic.keywords)
        ? JSON.parse(topic.keywords)
        : topic.keywords,
      subreddits: (topic.subreddits || "").split(","),
      filters: JSON.parse(topic.filters_json || "{}"),
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 8. POST /topics/:id/backfill
app.post("/topics/:id/backfill", async (req: Request, res: Response) => {
  try {
    const id = req.params.id;
    // 1. Get Topic
    const stmt = db.prepare("SELECT * FROM topics WHERE id = ?");
    const topic = stmt.get(id) as any;
    if (!topic) {
      res.status(404).json({ error: "Topic not found" });
      return;
    }

    // 2. Update Status -> PENDING
    db.prepare(
      "UPDATE topics SET backfill_status = 'PENDING' WHERE id = ?",
    ).run(id);

    // 3. Send Task to Kafka
    const subreddits = (topic.subreddits || "").split(",");
    const message = {
      topic_id: id,
      subreddits: subreddits,
    };

    await producer.send({
      topic: "reddit.tasks.backfill",
      messages: [{ value: JSON.stringify(message) }],
    });

    res.json({ message: "Backfill task queued", status: "PENDING" });
  } catch (error: any) {
    console.error("Backfill Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// 9. PATCH /topics/:id/status
app.patch("/topics/:id/status", (req: Request, res: Response) => {
  try {
    const { status, percentage } = req.body;
    const id = req.params.id;

    if (status) {
      db.prepare("UPDATE topics SET backfill_status = ? WHERE id = ?").run(
        status,
        id,
      );
    }

    if (percentage !== undefined) {
      db.prepare("UPDATE topics SET backfill_percentage = ? WHERE id = ?").run(
        percentage,
        id,
      );
    }

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});
// 4. GET /topics/{id}/report
// 4. GET /topics/:id/report
// Reads metrics from Elasticsearch (reddit-topic-metrics*)
app.get("/topics/:id/report", async (req: Request, res: Response) => {
  try {
    const topicId = req.params.id;

    // Use Aggregations to deduplicate streaming updates
    // Group by: window_type -> start time -> max(mentions) or latest record
    // Simplified query: Fetch all 1d metrics for this topic
    // Since Spark now only outputs 1d 'data bricks', we perform 7d/30d aggregation here.
    const result = await esClient.search({
      index: "reddit-topic-metrics*",
      size: 0,
      query: {
        bool: {
          must: [
            { term: { "topic_id.keyword": topicId } },
            { term: { "window_type.keyword": "1d" } },
          ],
        },
      },
      aggs: {
        by_start_time: {
          date_histogram: {
            field: "start",
            fixed_interval: "1d",
            order: { _key: "asc" },
          },
          aggs: {
            latest_update: {
              top_hits: {
                size: 1,
                sort: [{ mentions: { order: "desc" } }],
              },
            },
          },
        },
      },
    });

    const dailyMetrics: any[] = [];
    const buckets = (result.aggregations as any)?.by_start_time?.buckets || [];

    for (const bucket of buckets) {
      const hit = bucket.latest_update.hits.hits[0];
      if (hit) {
        dailyMetrics.push(hit._source);
      }
    }

    // --- Synthesis: 1d -> 1w, 1m ---
    const allWindows = aggregateDailyToWindows(dailyMetrics);

    // --- Metric Enrichment (Compute-on-Read) ---
    const finalMetrics = enrichMetrics(allWindows);

    // Sort descending by end time for the report
    finalMetrics.sort(
      (a, b) => new Date(b.end).getTime() - new Date(a.end).getTime(),
    );

    res.json({ topic_id: topicId, metrics: finalMetrics });
  } catch (error: any) {
    console.error("ES Error:", error);
    if (error.meta && error.meta.body) {
      console.error(
        "ES Error Details:",
        JSON.stringify(error.meta.body, null, 2),
      );
    }
    // If index doesn't exist yet, return empty
    res.json({ topic_id: req.params.id, metrics: [] });
  }
});

// Helper: Synthesize 7d and 30d views from daily metrics
function aggregateDailyToWindows(dailyRecords: any[]): any[] {
  // 1. Ensure sorted by date ASC
  const sorted = [...dailyRecords].sort(
    (a, b) => new Date(a.start).getTime() - new Date(b.start).getTime(),
  );
  const result: any[] = [];

  // Helper to sum previous N days
  const getWindowSum = (index: number, days: number, label: string) => {
    let mentions = 0;
    let engagement = 0;
    const currentEnd = new Date(sorted[index].end);
    // Start date of the window is (end - days)
    const windowStart = new Date(
      currentEnd.getTime() - days * 24 * 60 * 60 * 1000,
    );

    // Scan backwards for records that fall within [windowStart, currentEnd]
    for (let j = index; j >= 0; j--) {
      const recStart = new Date(sorted[j].start);
      if (recStart >= windowStart) {
        mentions += sorted[j].mentions || 0;
        engagement += sorted[j].engagement || 0;
      } else {
        break; // Out of range
      }
    }

    return {
      ...sorted[index],
      window_type: label,
      mentions,
      engagement,
      start: windowStart.toISOString(),
      // end remains the same as daily record
    };
  };

  for (let i = 0; i < sorted.length; i++) {
    // Add original 1d record
    result.push({ ...sorted[i], window_type: "1d" });

    // Synthesize 1w (7 days)
    result.push(getWindowSum(i, 7, "1w"));

    // Synthesize 1m (30 days)
    result.push(getWindowSum(i, 30, "1m"));
  }

  return result;
}

// Helper: Compute Velocity & Acceleration
function enrichMetrics(rawDocs: any[]) {
  // 1. Group by window_type (30m, 60m, 120m)
  const groups: Record<string, any[]> = {};
  rawDocs.forEach((d) => {
    const type = d.window_type || "1d";
    if (!groups[type]) groups[type] = [];
    groups[type].push(d);
  });

  const output: any[] = [];
  const W1 = parseFloat(process.env.WEIGHT_VELOCITY || "0.5");
  const W2 = parseFloat(process.env.WEIGHT_ACCELERATION || "0.3");
  const W3 = parseFloat(process.env.WEIGHT_ENGAGEMENT || "0.2");

  // Duration map in hours
  const durationMap: Record<string, number> = {
    "1d": 24.0,
    "1w": 168.0,
    "1m": 720.0,
  };

  // 2. Process each group
  for (const type of Object.keys(groups)) {
    // Sort by start time ASC to find previous window
    const docs = groups[type].sort(
      (a, b) => new Date(a.start).getTime() - new Date(b.start).getTime(),
    );
    const durationHrs = durationMap[type] || 1.0;

    // We need to map start_time -> doc to find strict previous window
    // But for simplicity/robustness, we can just use the immediately preceding record
    // IF the gap matches the slide. Spark slide is: 30m->15m slide, 60m->30m slide, 120m->60m slide.
    // Let's rely on sorting.

    for (let i = 0; i < docs.length; i++) {
      const curr = docs[i];
      const prev = docs[i - 1]; // Simple predecessor check

      let velocity = 0;
      let acceleration = 0;

      // Check if prev is valid predecessor (contiguous or sliding overlap)
      // For now, simple diff with previous available record in strict time order
      if (prev) {
        const m_curr = curr.mentions || 0;
        const m_prev = prev.mentions || 0;
        velocity = (m_curr - m_prev) / durationHrs;

        // For acceleration, we need prev_velocity.
        // We can look at the ALREADY calculated prev record in 'output'
        // but 'prev' here is the raw doc. We need to store computed state.
        // Let's attach computed metrics to the doc object temporarily.
        const v_prev_val = (prev as any)._computed_velocity || 0;
        acceleration = velocity - v_prev_val;
      }

      (curr as any)._computed_velocity = velocity;

      const trend_score =
        W1 * velocity + W2 * acceleration + W3 * (curr.engagement || 0);

      output.push({
        ...curr,
        velocity,
        acceleration,
        trend_score,
        _computed_velocity: undefined, // clean up
      });
    }
  }

  return output;
}

// 6. POST /generate-config
app.post("/generate-config", async (req: Request, res: Response) => {
  try {
    const { description } = req.body;
    if (!description) {
      res.status(400).json({ error: "Description is required" });
      return;
    }

    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
      return;
    }

    const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
      .split(",")
      .map((s) => s.trim());
    const allowedListString = allowedSubreddits.join(", ");

    const completion = await openai.chat.completions.create({
      model: DEFAULT_MODEL,
      messages: [
        {
          role: "system",
          content: `You are a configuration generator for a trend monitoring tool.
          Based on the user description, extract a JSON object with:
          - id: a strictly kebab-case identifier (max 30 chars).
          - keywords: array of 3-10 relevant keywords/phrases.
          - subreddits: array of 3-5 relevant subreddits selected ONLY from the provided allowed list.
          - description: a polished version of the user's description.

          ALLOWED SUBREDDITS: [${allowedListString}]

          IMPORTANT: You must ONLY choose subreddits from the ALLOWED SUBREDDITS list. Do NOT invent new ones.`,
        },
        {
          role: "user",
          content: description,
        },
      ],
      response_format: { type: "json_object" },
    });

    const responseContent = completion.choices[0].message?.content;
    if (!responseContent) throw new Error("Empty response from OpenAI");

    let config = JSON.parse(responseContent);

    // FALLBACK: Ensure ID is kebab-case and not generic
    if (
      !config.id ||
      config.id.includes("topic-id") ||
      config.id.match(/[A-Z\s]/)
    ) {
      const source = (config.keywords && config.keywords[0]) || description;
      config.id = source
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "")
        .slice(0, 30);
    }

    // SANITIZER: Ensure subreddits is array of strings
    if (config.subreddits && Array.isArray(config.subreddits)) {
      config.subreddits = config.subreddits.map((s: any) =>
        typeof s === "string" ? s : s.name || s.id || JSON.stringify(s),
      );
    }

    res.json(config);
  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res
      .status(500)
      .json({ error: error.message || "Failed to generate config" });
  }
});

// 7. POST /generate-random-prompt
app.post("/generate-random-prompt", async (req: Request, res: Response) => {
  try {
    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
      return;
    }

    const categories = [
      "Technology & Gadgets",
      "Gaming & Esports",
      "Movies & TV Shows",
      "Cryptocurrency & Finance",
      "Health & Fitness",
      "Travel & Digital Nomad",
      "Programming & AI",
      "Politics & World News",
      "Home Improvement & DIY",
      "Music & Concerts",
    ];
    const randomCategory =
      categories[Math.floor(Math.random() * categories.length)];

    const completion = await openai.chat.completions.create({
      model: DEFAULT_MODEL,
      messages: [
        {
          role: "system",
          content: `You are an assistant for a Trend Intelligence platform. 
          Generate a SINGLE, short user intent (max 15 words) that focuses on **analyzing trends**, **tracking volume**, or **monitoring sentiment shifts** for a topic in the given category.
          
          NEGATIVE CONSTRAINTS: 
          - Do NOT use passive phrases like "stay updated", "keep up with", "learn about", or "follow". 
          - Do NOT mention specific subreddits or "reddit.com".
          
          Output ONLY the sentence.`,
        },
        {
          role: "user",
          content: `Category: ${randomCategory}
          
          Target Style Examples:
          - "Analyze the spike in negative sentiment around the new iPhone."
          - "Track the discussion volume regarding the upcoming election."
          - "Monitor the trend trajectory of sustainable fashion brands."`,
        },
      ],
    });

    const result = completion.choices[0].message?.content?.trim() || "";

    const cleaned = result
      .replace(/^(Sentence|Output|Response|Prompt|Here is a sentence):\s*/i, "")
      .replace(/^["']|["']$/g, "")
      .trim();

    res.json({ prompt: cleaned });
  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res
      .status(500)
      .json({ error: error.message || "Failed to generate prompt" });
  }
});

function isJson(str: string) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}

app.listen(PORT, () => {
  console.log(`[API] Listening on ${PORT}`);
  console.log(`[API] DB Path: ${TOPICS_DB_PATH}`);
});
