// pages/api/query.js
// Force Node runtime (pg won't run on Edge)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

// small helper
function sendJson(res, status, obj) {
  res.status(status).setHeader('content-type', 'application/json');
  res.end(JSON.stringify(obj));
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,   // use pooled 6543 URL with sslmode=require
  ssl: { rejectUnauthorized: false },           // avoid self-signed cert errors on Vercel
});

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return sendJson(res, 405, { ok: false, error: 'Use POST' });
  }

  // Simple shared-secret header (optional but recommended)
  const incomingKey = req.headers['x-service-key'];
  if ((process.env.SERVICE_API_KEY || '') && incomingKey !== process.env.SERVICE_API_KEY) {
    return sendJson(res, 401, { ok: false, error: 'Unauthorized' });
  }

  try {
    // Parse body
    let body;
    try {
      body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    } catch {
      return sendJson(res, 400, { ok: false, error: 'Invalid JSON body' });
    }

    const question = (body?.question || '').trim();
    if (!question) return sendJson(res, 400, { ok: false, error: 'Missing "question"' });

    // Quick DB probe so errors are clearer than FUNCTION_INVOCATION_FAILED
    const clientProbe = await pool.connect();
    try {
      await clientProbe.query('SET statement_timeout = 15000');
      await clientProbe.query('SELECT 1');
    } finally {
      clientProbe.release();
    }

    // OpenAI
    const openaiKey = process.env.OPENAI_API_KEY;
    if (!openaiKey) return sendJson(res, 500, { ok: false, error: 'OPENAI_API_KEY not set' });
    const openai = new OpenAI({ apiKey: openaiKey });

    // Ask model for SQL (STRICT JSON)
    const gen = await openai.chat.completions.create({
      model: 'gpt-4o-mini', // broadly available; uses default params
      messages: [
        {
          role: 'system',
          content:
            'You write safe Postgres SQL against a single view public.market_data ' +
            '(columns: protocol text, symbol text, supply numeric, borrows numeric, available numeric, ' +
            'supply_apy numeric, borrow_apy numeric, utilization numeric, ts timestamptz). ' +
            "Always include protocol='aave'. Treat ETH as WETH. " +
            'Return STRICT JSON: {"sql":"..."}; one SELECT only; no comments.'
        },
        { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing "sql".` }
      ],
    });

    const raw = gen.choices?.[0]?.message?.content || '{}';
    let sql;
    try {
      sql = String(JSON.parse(raw).sql || '').trim();
    } catch (e) {
      return sendJson(res, 500, { ok: false, error: 'LLM returned non-JSON', raw });
    }

    // Minimal guard
    const lower = sql.toLowerCase();
    if (!lower.startsWith('select')) {
      return sendJson(res, 400, { ok: false, error: 'Only SELECT allowed', sql });
    }
    if (sql.includes(';')) {
      return sendJson(res, 400, { ok: false, error: 'Multiple statements not allowed', sql });
    }
    if (/(--|\/\*)/.test(sql)) {
      return sendJson(res, 400, { ok: false, error: 'SQL comments not allowed', sql });
    }
    if (!/\blimit\s+\d+\b/i.test(sql)) {
      sql = `${sql}\nLIMIT 500`;
    }

    // Execute SQL
    const client = await pool.connect();
    let rows = [];
    try {
      await client.query('SET statement_timeout = 15000');
      const r = await client.query(sql);
      rows = r.rows || [];
    } finally {
      client.release();
    }

    // Summarize
    const sum = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Answer in 1–2 sentences. Use only numbers from the rows. No tables.' },
        { role: 'user', content: `Question: ${question}` },
        { role: 'assistant', content: `Rows: ${JSON.stringify(rows).slice(0, 100000)}` }
      ],
    });

    return sendJson(res, 200, {
      ok: true,
      answer: sum.choices?.[0]?.message?.content?.trim() || '',
      sql,
      rows, // UI shows first 10
    });
  } catch (err) {
    console.error('QUERY_ERROR:', err);
    return sendJson(res, 500, { ok: false, error: err?.message || String(err) });
  }
}