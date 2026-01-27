import OpenAI from 'openai';
import express, { Request, Response } from 'express';
import Database from 'better-sqlite3';
import { Client } from '@elastic/elasticsearch';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8000;
const TOPICS_DB_PATH = process.env.TOPICS_DB_PATH || '/data/topics.db';
const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || 'http://elasticsearch:9200';

// ----------------------------------------------------------------------------
// OpenAI Client
// ----------------------------------------------------------------------------
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const DEFAULT_MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';

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
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`);

// ----------------------------------------------------------------------------
// Storage: Elasticsearch (Metrics, Trends)
// ----------------------------------------------------------------------------
const esClient = new Client({
  node: ELASTICSEARCH_URL,
  auth: {
    username: process.env.BASIC_AUTH_USER || 'admin',
    password: process.env.BASIC_AUTH_PASS || 'admin'
  },
  tls: { rejectUnauthorized: false }
});

// ----------------------------------------------------------------------------
// API Endpoints
// ----------------------------------------------------------------------------

app.get('/subreddits', (req: Request, res: Response) => {
  const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "").split(',').map(s => s.trim()).filter(s => s.length > 0);
  res.json({ subreddits: allowedSubreddits });
});

// 2. GET /topics
app.get('/topics', (req: Request, res: Response) => {
  try {
    const stmt = db.prepare('SELECT * FROM topics');
    const rows = stmt.all();
    // Parse JSON fields
    const topics = rows.map((r: any) => ({
      ...r,
      keywords: isJson(r.keywords) ? JSON.parse(r.keywords) : r.keywords,
      subreddits: (r.subreddits || "").split(','),
      filters: JSON.parse(r.filters_json || '{}')
    }));
    res.json(topics);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 1. POST /topics
app.post('/topics', (req: Request, res: Response) => {
  try {
    const { id, description, keywords, subreddits, filters, update_frequency, is_active } = req.body;
    
    // Basic validation
    if (!id || !keywords || !subreddits) {
       res.status(400).json({ error: 'Missing required fields: id, keywords, subreddits' });
       return;
    }

    const stmt = db.prepare(`
      INSERT INTO topics (id, description, keywords, subreddits, filters_json, update_frequency_seconds, is_active, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    stmt.run(
      id, 
      description || '', 
      Array.isArray(keywords) ? JSON.stringify(keywords) : keywords, 
      Array.isArray(subreddits) ? subreddits.join(',') : subreddits, 
      JSON.stringify(filters || {}), 
      update_frequency || 60, // Default to 60s
      is_active ? 1 : 0,
      new Date().toISOString()
    );

    res.status(201).json({ message: 'Topic created', id });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 3. GET /topics/{id}
app.get('/topics/:id', (req: Request, res: Response) => {
  try {
    const stmt = db.prepare('SELECT * FROM topics WHERE id = ?');
    const topic = stmt.get(req.params.id) as any;
    
    if (!topic) {
      res.status(404).json({ error: 'Topic not found' });
      return;
    }

    res.json({
      ...topic,
       keywords: isJson(topic.keywords) ? JSON.parse(topic.keywords) : topic.keywords,
       subreddits: (topic.subreddits || "").split(','),
       filters: JSON.parse(topic.filters_json || '{}')
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 4. GET /topics/{id}/report
// 4. GET /topics/:id/report
// Reads metrics from Elasticsearch (reddit-topic-metrics*)
app.get('/topics/:id/report', async (req: Request, res: Response) => {
  try {
    const topicId = req.params.id;
    
    // Use Aggregations to deduplicate streaming updates
    // Group by: window_type -> start time -> max(mentions) or latest record
    const result = await esClient.search({
      index: 'reddit-topic-metrics*', 
      size: 0, // We only care about aggregations
      body: {
        query: {
          bool: {
            must: [
              { match: { topic_id: topicId } }
            ]
          }
        },
        aggs: {
          by_window_type: {
            terms: { field: "window_type.keyword", size: 10 },
            aggs: {
              by_start_time: {
                terms: { field: "start", size: 500, order: { "_key": "desc" } }, // Last 500 windows per type
                aggs: {
                  latest_update: {
                    top_hits: {
                      size: 1,
                      sort: [{ "mentions": { "order": "desc" } }] // Take the one with most mentions (safest for cumulative)
                    }
                  }
                }
              }
            }
          }
        }
      }
    });

    const enriched: any[] = [];
    const buckets = (result.aggregations as any)?.by_window_type?.buckets || [];
    
    // Flatten aggregation structure
    for (const typeBucket of buckets) {
        const startBuckets = typeBucket.by_start_time.buckets;
        for (const startBucket of startBuckets) {
            const hit = startBucket.latest_update.hits.hits[0];
            if (hit) {
                enriched.push(hit._source);
            }
        }
    }

    // --- Metric Enrichment (Compute-on-Read) ---
    // Spark outputs raw 'mentions' and 'engagement'. We calculate Velocity/Acceleration here.
    const finalMetrics = enrichMetrics(enriched);
    
    // Sort descending by end time for the report
    finalMetrics.sort((a, b) => new Date(b.end).getTime() - new Date(a.end).getTime());

    res.json({ topic_id: topicId, metrics: finalMetrics });
  } catch (error: any) {
     console.error("ES Error:", error);
    // If index doesn't exist yet, return empty
    res.json({ topic_id: req.params.id, metrics: [] });
  }
});

// Helper: Compute Velocity & Acceleration
function enrichMetrics(rawDocs: any[]) {
    // 1. Group by window_type (30m, 60m, 120m)
    const groups: Record<string, any[]> = {};
    rawDocs.forEach(d => {
        const type = d.window_type || '1d';
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
        "1m": 720.0
    };

    // 2. Process each group
    for (const type of Object.keys(groups)) {
        // Sort by start time ASC to find previous window
        const docs = groups[type].sort((a, b) => new Date(a.start).getTime() - new Date(b.start).getTime());
        const durationHrs = durationMap[type] || 1.0;
        
        // We need to map start_time -> doc to find strict previous window
        // But for simplicity/robustness, we can just use the immediately preceding record 
        // IF the gap matches the slide. Spark slide is: 30m->15m slide, 60m->30m slide, 120m->60m slide.
        // Let's rely on sorting. 
        
        for (let i = 0; i < docs.length; i++) {
            const curr = docs[i];
            const prev = docs[i-1]; // Simple predecessor check
            
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
             
             const trend_score = (W1 * velocity) + (W2 * acceleration) + (W3 * (curr.engagement || 0));
             
             output.push({
                 ...curr,
                 velocity,
                 acceleration,
                 trend_score,
                 _computed_velocity: undefined // clean up
             });
        }
    }
    
    return output;
}

// 5. GET /trending/global
// Reads global trends from Elasticsearch (reddit-global-trends*)
app.get('/trending/global', async (req: Request, res: Response) => {
  try {
    // Get latest top 3 via ES aggregation or just filtering recent records
    // Since Spark writes Top 3 explicitly, we can just fetch the latest records.
    const result = await esClient.search({
      index: 'reddit-global-trends*',
      body: {
        sort: [{ timestamp: { order: 'desc' } }],
        size: 20 
      }
    });

    const hits = result.hits.hits.map(h => h._source);
    res.json({ global_trends: hits });
  } catch (error: any) {
    console.error("ES Error:", error);
    res.json({ global_trends: [] });
  }
});

// 6. POST /generate-config
app.post('/generate-config', async (req: Request, res: Response) => {
  try {
    const { description } = req.body;
    if (!description) {
      res.status(400).json({ error: 'Description is required' });
      return;
    }

    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: 'OPENAI_API_KEY is not configured' });
      return;
    }

    const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "").split(',').map(s => s.trim());
    const allowedListString = allowedSubreddits.join(', ');

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

          IMPORTANT: You must ONLY choose subreddits from the ALLOWED SUBREDDITS list. Do NOT invent new ones.`
        },
        {
          role: "user",
          content: description
        }
      ],
      response_format: { type: "json_object" }
    });

    const responseContent = completion.choices[0].message?.content;
    if (!responseContent) throw new Error("Empty response from OpenAI");

    let config = JSON.parse(responseContent);

    // FALLBACK: Ensure ID is kebab-case and not generic
    if (!config.id || config.id.includes('topic-id') || config.id.match(/[A-Z\s]/)) {
        const source = (config.keywords && config.keywords[0]) || description;
        config.id = source
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/(^-|-$)/g, '')
          .slice(0, 30);
    }

    // SANITIZER: Ensure subreddits is array of strings
    if (config.subreddits && Array.isArray(config.subreddits)) {
        config.subreddits = config.subreddits.map((s: any) => 
            typeof s === 'string' ? s : (s.name || s.id || JSON.stringify(s))
        );
    }

    res.json(config);

  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res.status(500).json({ error: error.message || "Failed to generate config" });
  }
});

// 7. POST /generate-random-prompt
app.post('/generate-random-prompt', async (req: Request, res: Response) => {
  try {
    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: 'OPENAI_API_KEY is not configured' });
      return;
    }

    const categories = [
      "Technology & Gadgets", "Gaming & Esports", "Movies & TV Shows",
      "Cryptocurrency & Finance", "Health & Fitness", "Travel & Digital Nomad",
      "Programming & AI", "Politics & World News", "Home Improvement & DIY", "Music & Concerts"
    ];
    const randomCategory = categories[Math.floor(Math.random() * categories.length)];

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
          
          Output ONLY the sentence.`
        },
        {
          role: "user",
          content: `Category: ${randomCategory}
          
          Target Style Examples:
          - "Analyze the spike in negative sentiment around the new iPhone."
          - "Track the discussion volume regarding the upcoming election."
          - "Monitor the trend trajectory of sustainable fashion brands."`
        }
      ]
    });

    const result = completion.choices[0].message?.content?.trim() || "";
    
    const cleaned = result
      .replace(/^(Sentence|Output|Response|Prompt|Here is a sentence):\s*/i, '')
      .replace(/^["']|["']$/g, '')
      .trim();

    res.json({ prompt: cleaned });

  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res.status(500).json({ error: error.message || "Failed to generate prompt" });
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
