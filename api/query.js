// pages/api/query.js
// Force Node runtime (pg won't run on Edge)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function send(res, status, obj) {
  res.status(status).setHeader('content-type', 'application/json');
  res.end(JSON.stringify(obj));
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return send(res, 405, { ok: false, error: 'Use POST' });
  }

  // ---- Same-origin bypass for browser UI ----
  const host = req.headers.host || '';
  const referer = req.headers.referer || req.headers.origin || '';
  let sameOrigin = false;
  try { sameOrigin = !!referer && new URL(referer).host === host; } catch { sameOrigin = false; }

  // Require key if NOT same-origin
  const svcKey = req.headers['x-service-key'] || '';
  const requiredKey = process.env.SERVICE_API_KEY || '';
  if (!sameOrigin && (!svcKey || svcKey !== requiredKey)) {
    return send(res, 401, { ok: false, error: 'unauthorized' });
  }

  // parse body
  let body = req.body;
  if (typeof body === 'string') {
    try { body = JSON.parse(body); }
    catch { return send(res, 400, { ok: false, error: 'Invalid JSON body' }); }
  }
  const question = (body?.question || '').trim();
  if (!question) return send(res, 400, { ok: false, error: 'Missing "question"' });

  // DB probe
  try {
    const c = await pool.connect();
    try { await c.query('SET statement_timeout = 15000'); await c.query('SELECT 1'); }
    finally { c.release(); }
  } catch (e) {
    return send(res, 500, { ok: false, error: `DB error: ${e.message}` });
  }

  if (!process.env.OPENAI_API_KEY) {
    return send(res, 500, { ok: false, error: 'OPENAI_API_KEY not set' });
  }
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  // LLM → SQL
  let raw;
  try {
    const gen = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content:
            'You write safe Postgres SQL against public.market_data ' +
            '(protocol text, symbol text, supply numeric, borrows numeric, available numeric, ' +
            'supply_apy numeric, borrow_apy numeric, utilization numeric, ts timestamptz). ' +
            "Always include protocol='aave'. Treat ETH as WETH. Return STRICT JSON {\"sql\":\"...\"}; one statement; no semicolon."
        },
        { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing "sql".` }
      ],
    });
    raw = gen.choices?.[0]?.message?.content || '{}';
  } catch (e) {
    return send(res, 500, { ok: false, error: `LLM error: ${e.message}` });
  }

  let sql;
  try { sql = String(JSON.parse(raw).sql || '').trim(); }
  catch { return send(res, 500, { ok: false, error: 'LLM returned non-JSON', raw }); }

  if (!/^\s*(select|with)\b/i.test(sql)) return send(res, 400, { ok: false, error: 'Only SELECT/CTE allowed', sql });
  if (sql.includes(';')) return send(res, 400, { ok: false, error: 'Multiple statements not allowed', sql });
  if (!/\blimit\s+\d+\b/i.test(sql)) sql = `${sql}\nLIMIT 500`;

  // Run SQL
  let rows;
  try {
    const c2 = await pool.connect();
    try { await c2.query('SET statement_timeout = 15000'); rows = (await c2.query(sql)).rows; }
    finally { c2.release(); }
  } catch (e) {
    return send(res, 500, { ok: false, error: `SQL error: ${e.message}`, sql });
  }

  // Summarize
  let answer = '';
  try {
    const sum = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Answer in 1–2 sentences. Use only numbers from the rows. No tables.' },
        { role: 'user', content: `Question: ${question}` },
        { role: 'assistant', content: `Rows: ${JSON.stringify(rows).slice(0, 100000)}` }
      ],
    });
    answer = sum.choices?.[0]?.message?.content?.trim() || '';
  } catch (_) {}

  return send(res, 200, { ok: true, answer, sql, rows });
}