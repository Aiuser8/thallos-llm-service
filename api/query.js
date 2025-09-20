// api/query.js — Vercel Node serverless handler (ESM)
import OpenAI from "openai";
import pg from "pg";
import { planQuery, retryPlan, generateAnswer } from "../lib/instructions.js";

export const config = { runtime: "nodejs" };

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

    // 2) Execute SQL
    const pool = new pg.Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    });

    let rows = [];
    try {
      const r = await pool.query(sql);
      rows = r.rows || [];
    } catch (e) {
      // 3) Retry once with error-aware planning
      const retry = await retryPlan(openai, question, sql, String(e));
      const r2 = await pool.query(retry.sql);
      sql = retry.sql;
      rows = r2.rows || [];
    } finally {
      await pool.end();
    }

    // 4) Respond (JSON only)
    if (minimal) return res.status(200).json({ rows });

    const answer = await generateAnswer(openai, question, rows, presentationHint);
    return res.status(200).json({ sql, rows, answer });
  } catch (err) {
    return res.status(500).json({ error: err?.message || String(err) });
  }
}