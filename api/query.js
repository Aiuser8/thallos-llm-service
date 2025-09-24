// api/query.js — Vercel Node serverless handler (ESM)
import OpenAI from "openai";
import pg from "pg";
import { planQuery, retryPlan, generateAnswerFromResults, isQuestionInDataScope, handleGeneralKnowledgeQuestion } from "../lib/instructions.js";

export const config = { runtime: "nodejs" }; // optionally: { runtime: "nodejs", regions: ["iad1"] }

// Create and cache the database connection pool
let dbPool = null;

function getDbPool() {
  if (!dbPool) {
    const url = new URL(process.env.DATABASE_URL);
    dbPool = new pg.Pool({
      host: url.hostname, // e.g., aws-1-us-east-2.pooler.supabase.com
      port: Number(url.port || 5432),
      user: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      database: url.pathname.replace(/^\//, ""), // "postgres"
      // Force TLS but relax CA validation to avoid SELF_SIGNED_CERT_IN_CHAIN in Vercel
      ssl: { rejectUnauthorized: false },
      // Connection pool configuration for better performance
      max: 20, // Maximum number of clients in the pool
      idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
      connectionTimeoutMillis: 2000, // Return an error after 2 seconds if connection could not be established
    });
  }
  return dbPool;
}

// Read JSON safely whether req.body exists or not
async function readJson(req) {
  if (req.body && typeof req.body === "object") return req.body;
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try {
    return JSON.parse(raw);
  } catch {
    // Non-JSON caller (e.g., GET or webhook without body)
    return {};
  }
}

export default async function handler(req, res) {
  try {
    // Accept POST (JSON body) and GET (?q=...) to support webhooks/cron
    if (req.method !== "POST" && req.method !== "GET") {
      res.setHeader("Allow", "POST, GET");
      return res.status(405).json({ error: "Method Not Allowed" });
    }

    // Accept from JSON body OR query string (?q= / ?question=)
    const urlObj = new URL(req.url, `https://${req.headers.host}`);
    const qsQuestion = urlObj.searchParams.get("q") || urlObj.searchParams.get("question");

    const body = await readJson(req);
    const question = body?.question || qsQuestion;
    const minimal =
      body?.minimal === true || urlObj.searchParams.get("minimal") === "true";
    const presentationHint = body?.presentationHint;

    if (!question) {
      return res.status(400).json({
        error:
          "Missing 'question'. Provide JSON body {\"question\":\"...\"} or use ?q= in the URL.",
        exampleCurl:
          `curl -H "content-type: application/json" -d '{"question":"What was Ethereum TVL on the most recent date?"}' ` +
          `${urlObj.origin}${urlObj.pathname}`,
      });
    }

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // Check if question is within data scope or needs general knowledge handling
    const inDataScope = await isQuestionInDataScope(question);
    
    if (!inDataScope) {
      // Handle as general knowledge question
      const answer = await handleGeneralKnowledgeQuestion(openai, question);
      return res.status(200).json({ 
        answer, 
        source: "general_knowledge",
        note: "This question was answered using general knowledge rather than database queries."
      });
    }

    // 1) Plan SQL
    let { sql } = await planQuery(openai, question);

    // 2) Execute SQL (with optional statement timeout)
    const pool = getDbPool();
    let rows = [];
    let sqlTried = sql;

    const client = await pool.connect();
    try {
      const ms = Number(process.env.DB_QUERY_TIMEOUT_MS || 30000); // Reduced from 180000 to 30000 (30 seconds)
      await client.query(`SET statement_timeout TO ${ms}`);

      // First attempt
      try {
        const r = await client.query(sql);
        rows = r.rows || [];
      } catch (e1) {
        // Retry with error-aware planning
        const retry = await retryPlan(openai, question, sql, String(e1));
        sql = retry.sql;
        sqlTried = sql;

        // Second attempt
        try {
          const r2 = await client.query(sql);
          rows = r2.rows || [];
        } catch (e2) {
          // Bubble precise DB error + which SQL failed
          const err = new Error(`Query failed after retry: ${e2.message}`);
          err.code = e2.code;
          err.detail = e2.detail;
          err.hint = e2.hint;
          err.position = e2.position;
          err.sql = sqlTried;
          throw err;
        }
      }
    } finally {
      client.release();
      // Note: We no longer call pool.end() - the pool stays alive for reuse
    }

    // 3) Respond (JSON only)
    if (minimal) return res.status(200).json({ sql, rows, source: "database_query" });

    // Use optimized answer generation (simplified prompt, faster response)
    const answer = await generateAnswerFromResults(openai, question, rows, presentationHint);
    return res.status(200).json({ sql, rows, answer, source: "database_query" });
  } catch (err) {
    // Clear, JSON-only error surface for Vercel & clients
    const message = err?.message || String(err);
    const payload = {
      error: message,
      db: {
        code: err?.code,
        detail: err?.detail,
        hint: err?.hint,
        position: err?.position,
      },
      sql: err?.sql, // which SQL failed (if any)
      hint:
        "Check DATABASE_URL (host/db/role), ?sslmode=require (optional), schema/table privileges, and request method/body.",
    };
    res.setHeader("content-type", "application/json");
    return res.status(500).end(JSON.stringify(payload));
  }
}
