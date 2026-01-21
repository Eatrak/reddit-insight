"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const dotenv_1 = __importDefault(require("dotenv"));
const elasticsearch_1 = require("@elastic/elasticsearch");
const better_sqlite3_1 = __importDefault(require("better-sqlite3"));
const path_1 = __importDefault(require("path"));
dotenv_1.default.config();
// Env helpers
const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || "http://localhost:9200";
const TOPICS_DB_PATH = process.env.TOPICS_DB_PATH || path_1.default.join(process.cwd(), "topics.db");
const BASIC_AUTH_USER = process.env.BASIC_AUTH_USER || "admin";
const BASIC_AUTH_PASS = process.env.BASIC_AUTH_PASS || "admin";
const PORT = parseInt(process.env.PORT || "8000", 10);
// Elasticsearch client
const es = new elasticsearch_1.Client({ node: ELASTICSEARCH_URL });
// SQLite (better-sqlite3 is synchronous and simple)
const db = new better_sqlite3_1.default(TOPICS_DB_PATH);
db.prepare(`
  CREATE TABLE IF NOT EXISTS topics (
    id TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    keywords TEXT NOT NULL,
    subreddits TEXT NOT NULL,
    filters_json TEXT NOT NULL,
    update_frequency_seconds INTEGER NOT NULL,
    is_active INTEGER NOT NULL,
    created_at TEXT NOT NULL
  )
`).run();
// Helpers
function splitCsv(s) {
    if (!s)
        return [];
    return s
        .split(",")
        .map((x) => x.trim())
        .filter((x) => x.length > 0);
}
function joinCsv(xs) {
    if (!xs)
        return "";
    return xs
        .map((x) => x.trim())
        .filter((x) => x.length > 0)
        .join(",");
}
function decodeBasicAuth(authHeader) {
    if (!authHeader || !authHeader.toLowerCase().startsWith("basic "))
        return null;
    const b64 = authHeader.split(" ", 2)[1];
    try {
        const raw = Buffer.from(b64, "base64").toString("utf8");
        const [user, pass] = raw.split(":", 2);
        if (!user || pass === undefined)
            return null;
        return { user, pass };
    }
    catch {
        return null;
    }
}
// Express app
const app = (0, express_1.default)();
app.use(express_1.default.json());
// Basic auth middleware
function requireBasicAuth(req, res, next) {
    const creds = decodeBasicAuth(req.headers["authorization"]);
    if (!creds || creds.user !== BASIC_AUTH_USER || creds.pass !== BASIC_AUTH_PASS) {
        res.setHeader("WWW-Authenticate", 'Basic realm="reddit-insight"');
        return res.status(401).json({ detail: "Unauthorized" });
    }
    return next();
}
// Routes
app.get("/health", async (_req, res) => {
    try {
        await es.info();
        res.json({ ok: true, elasticsearch: ELASTICSEARCH_URL });
    }
    catch {
        res.json({ ok: false, elasticsearch: ELASTICSEARCH_URL });
    }
});
// Create topic
app.post("/topics", requireBasicAuth, (req, res) => {
    const body = req.body;
    if (!body.description) {
        return res.status(400).json({ detail: "description is required" });
    }
    const filters = {
        min_score: body.filters?.min_score ?? 0,
        post_type: body.filters?.post_type || "any",
        time_window_minutes: body.filters?.time_window_minutes ?? 60,
    };
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const stmt = db.prepare(`
    INSERT INTO topics (
      id, description, keywords, subreddits, filters_json,
      update_frequency_seconds, is_active, created_at
    )
    VALUES (@id, @description, @keywords, @subreddits, @filters_json,
            @update_frequency_seconds, @is_active, @created_at)
  `);
    stmt.run({
        id,
        description: body.description,
        keywords: joinCsv(body.keywords || []),
        subreddits: joinCsv(body.subreddits || []),
        filters_json: JSON.stringify(filters),
        update_frequency_seconds: body.update_frequency_seconds ?? 60,
        is_active: body.is_active === false ? 0 : 1,
        created_at: createdAt,
    });
    const topic = {
        id,
        description: body.description,
        keywords: body.keywords || [],
        subreddits: body.subreddits || [],
        filters,
        update_frequency_seconds: body.update_frequency_seconds ?? 60,
        is_active: body.is_active !== false,
        created_at: createdAt,
    };
    return res.status(201).json(topic);
});
// List topics
app.get("/topics", requireBasicAuth, (_req, res) => {
    const rows = db.prepare("SELECT * FROM topics ORDER BY created_at DESC").all();
    const topics = rows.map((r) => {
        const filters = JSON.parse(r.filters_json || "{}");
        return {
            id: r.id,
            description: r.description,
            keywords: splitCsv(r.keywords),
            subreddits: splitCsv(r.subreddits),
            filters: {
                min_score: filters.min_score ?? 0,
                post_type: filters.post_type || "any",
                time_window_minutes: filters.time_window_minutes ?? 60,
            },
            update_frequency_seconds: Number(r.update_frequency_seconds),
            is_active: Boolean(Number(r.is_active)),
            created_at: r.created_at,
        };
    });
    return res.json(topics);
});
// Get topic
app.get("/topics/:id", requireBasicAuth, (req, res) => {
    const row = db.prepare("SELECT * FROM topics WHERE id = ?").get(req.params.id);
    if (!row) {
        return res.status(404).json({ detail: "Topic not found" });
    }
    const filters = JSON.parse(row.filters_json || "{}");
    const topic = {
        id: row.id,
        description: row.description,
        keywords: splitCsv(row.keywords),
        subreddits: splitCsv(row.subreddits),
        filters: {
            min_score: filters.min_score ?? 0,
            post_type: filters.post_type || "any",
            time_window_minutes: filters.time_window_minutes ?? 60,
        },
        update_frequency_seconds: Number(row.update_frequency_seconds),
        is_active: Boolean(Number(row.is_active)),
        created_at: row.created_at,
    };
    return res.json(topic);
});
// Update topic
app.put("/topics/:id", requireBasicAuth, (req, res) => {
    const id = req.params.id;
    const body = req.body;
    const existing = db.prepare("SELECT * FROM topics WHERE id = ?").get(id);
    if (!existing) {
        return res.status(404).json({ detail: "Topic not found" });
    }
    const filters = {
        min_score: body.filters?.min_score ?? 0,
        post_type: body.filters?.post_type || "any",
        time_window_minutes: body.filters?.time_window_minutes ?? 60,
    };
    db.prepare(`
    UPDATE topics
    SET description = @description,
        keywords = @keywords,
        subreddits = @subreddits,
        filters_json = @filters_json,
        update_frequency_seconds = @update_frequency_seconds,
        is_active = @is_active
    WHERE id = @id
  `).run({
        id,
        description: body.description ?? existing.description,
        keywords: joinCsv(body.keywords || splitCsv(existing.keywords)),
        subreddits: joinCsv(body.subreddits || splitCsv(existing.subreddits)),
        filters_json: JSON.stringify(filters),
        update_frequency_seconds: body.update_frequency_seconds ?? existing.update_frequency_seconds,
        is_active: body.is_active === false ? 0 : 1,
    });
    const updatedRow = db.prepare("SELECT * FROM topics WHERE id = ?").get(id);
    const updFilters = JSON.parse(updatedRow.filters_json || "{}");
    const topic = {
        id: updatedRow.id,
        description: updatedRow.description,
        keywords: splitCsv(updatedRow.keywords),
        subreddits: splitCsv(updatedRow.subreddits),
        filters: {
            min_score: updFilters.min_score ?? 0,
            post_type: updFilters.post_type || "any",
            time_window_minutes: updFilters.time_window_minutes ?? 60,
        },
        update_frequency_seconds: Number(updatedRow.update_frequency_seconds),
        is_active: Boolean(Number(updatedRow.is_active)),
        created_at: updatedRow.created_at,
    };
    return res.json(topic);
});
// Delete topic
app.delete("/topics/:id", requireBasicAuth, (req, res) => {
    const info = db.prepare("DELETE FROM topics WHERE id = ?").run(req.params.id);
    if (info.changes === 0) {
        return res.status(404).json({ detail: "Topic not found" });
    }
    return res.status(204).send();
});
// Helper to query Elasticsearch
async function esSearch(index, topicId) {
    const must = [];
    if (topicId) {
        must.push({ term: { topic_id: topicId } });
    }
    const resp = await es.search({
        index,
        size: 200,
        sort: [{ "@timestamp": { order: "desc" } }],
        query: { bool: { must } },
    });
    return (resp.hits.hits || []).map((h) => h._source);
}
// Metrics / trends / sentiment / global viral
app.get("/topics/:id/metrics", requireBasicAuth, async (req, res) => {
    const items = await esSearch("reddit-topic-metrics", req.params.id);
    return res.json({ topic_id: req.params.id, items });
});
app.get("/topics/:id/trends", requireBasicAuth, async (req, res) => {
    const items = await esSearch("reddit-topic-trends", req.params.id);
    return res.json({ topic_id: req.params.id, items });
});
app.get("/topics/:id/sentiment", requireBasicAuth, async (req, res) => {
    const items = await esSearch("reddit-topic-sentiment", req.params.id);
    return res.json({ topic_id: req.params.id, items });
});
app.get("/global/viral", requireBasicAuth, async (_req, res) => {
    const items = await esSearch("reddit-global-trends");
    return res.json({ items });
});
// Start server
app.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`Reddit Insight API (Express + TypeScript) listening on port ${PORT}`);
});
