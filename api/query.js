// api/query.js — Vercel Node serverless handler (ESM)
import OpenAI from "openai";
import pg from "pg";
import { planQuery, retryPlan, generateAnswer } from "../lib/instructions.js";

export const config = { runtime: "nodejs" }; // optionally: { runtime: "nodejs", regions: ["iad1"] }

// Read JSON safely whether req.body exists or not
async function readJson(req) {
  if (req.body && typeof req.body === "object") return req.body;
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try {
    return JSON.parse(raw);
  } catch (e) {
    throw new Error(`Invalid JSON body: ${e.message}`);
  }
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.setHeader("Allow", "POST");
      return res.status(405).json({ error: "Method Not Allowed" });
    }

    const { question, minimal = false, presentationHint } = await readJson(req);
    if (!question) return res.status(400).json({ error: "Missing 'question'" });

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // 1) Plan SQL
    let { sql } = await planQuery(openai, question);

    // 2) Execute SQL (with optional statement timeout)
// Build the pool from discrete params (avoids URL parsing quirks)
const url = new URL(process.env.DATABASE_URL);

const pool = new pg.Pool({
  host: url.hostname,                                   // e.g. aws-1-us-east-2.pooler.supabase.com
  port: Number(url.port || 5432),
  user: decodeURIComponent(url.username),
  password: decodeURIComponent(url.password),
  database: url.pathname.replace(/^\//, ""),            // "postgres"
  // Force TLS but skip CA validation to avoid SELF_SIGNED_CERT_IN_CHAIN
  ssl: { rejectUnauthorized: false },
});

    let rows = [];
    let sqlTried = sql;

    const client = await pool.connect();
    try {
      const ms = Number(process.env.DB_QUERY_TIMEOUT_MS || 180000);
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
      await pool.end();
    }

    // 3) Respond (JSON only)
    if (minimal) return res.status(200).json({ rows });

    const answer = await generateAnswer(openai, question, rows, presentationHint);
    return res.status(200).json({ sql, rows, answer });

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
        "Check DATABASE_URL (host/db/role), ?sslmode=require, schema/table privileges, and request method/body.",
    };
    res.setHeader("content-type", "application/json");
    return res.status(500).end(JSON.stringify(payload));
  }
}