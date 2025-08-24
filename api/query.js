// /api/query.js  (Next.js pages router)
// If you’re using the App Router: export const runtime='nodejs' and adjust to Request/Response.
export const config = { runtime: 'nodejs' }; // Force Node (pg won't run on Edge)

import { Pool } from 'pg';
import OpenAI from 'openai';

// Stronger error surface
function json(res, status, obj) {
  res.status(status).setHeader('content-type', 'application/json');
  res.end(JSON.stringify(obj));
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // use the pooled 6543 URL with sslmode=require
  ssl: { rejectUnauthorized: false },        // avoids “self-signed cert” errors
});

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return json(res, 405, { ok: false, error: 'Use POST' });
  }
  try {
    // simple auth
    const key = req.headers['x-service-key'];
    if (!key || key !== process.env.SERVICE_API_KEY) {
      return json(res, 401, { ok: false, error: 'Unauthorized' });
    }

    // parse body
    let body;
    try { body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body; }
    catch { return json(res, 400, { ok: false, error: 'Invalid JSON body' }); }

    const question = (body?.question || '').trim();
    if (!question) return json(res, 400, { ok: false, error: 'Missing "question"' });

    // Basic DB ping (helps distinguish DB vs LLM issues)
    const client = await pool.connect();
    try {
      await client.query('SET statement_timeout = 15000');
      // tiny probe so we know DB works
      const probe = await client.query('SELECT 1 as ok');
      if (!probe?.rows?.length) throw new Error('DB probe failed');
    } finally { client.release(); }

    // LLM call
    const openaiKey = process.env.OPENAI_API_KEY;
    if (!openaiKey) {
      return json(res, 500, { ok: false, error: 'OPENAI_API_KEY not set on server' });
    }
    const openai = new OpenAI({ apiKey: openaiKey });

    // Keep to a widely-available model & default params
    const gen = await openai.chat.completions.create({
      model: 'gpt-4o-mini',     // avoid custom/unsupported models or non-default temps
      messages: [
        {
          role: 'system',
          content:
            'You write safe Postgres SQL against a single view public.market_data ' +
            '(columns: protocol text, symbol text, supply numeric, borrows numeric, available numeric, ' +
            'supply_apy numeric, borrow_apy numeric, utilization numeric, ts timestamptz). ' +
            'Always include protocol=\'aave\'. USDC/WETH symbols are uppercase; treat ETH as WETH. ' +
            'Return STRICT JSON: {"sql":"..."}; one SELECT only.'
        },
        { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing "sql".` }
      ],
    });

    const raw = gen.choices?.[0]?.message?.content || '{}';
    let sql;
    try {
      sql = String(JSON.parse(raw).sql || '').trim();
    } catch (e) {
      return json(res, 500, { ok: false, error: 'LLM returned non-JSON', raw });
    }
    if (!sql.toLowerCase().startsWith('select')) {
      return json(res, 400, { ok: false, error: 'Not a SELECT', sql });
    }

    // Minimal guard + LIMIT
    if (sql.includes(';')) return json(res, 400, { ok: false, error: 'Multiple statements not allowed', sql });
    if (!/\blimit\s+\d+\b/i.test(sql)) sql = `${sql}\nLIMIT 500`;

    // Run SQL
    const client2 = await pool.connect();
    let rows;
    try {
      await client2.query('SET statement_timeout = 15000');
      rows = (await client2.query(sql)).rows;
    } finally { client2.release(); }

    // Summarize
    const sum = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Answer in 1–2 sentences. Use only numbers from the rows. No tables.' },
        { role: 'user', content: `Question: ${question}` },
        { role: 'assistant', content: `Rows: ${JSON.stringify(rows).slice(0, 100000)}` }
      ],
    });

    return json(res, 200, {
      ok: true,
      answer: sum.choices?.[0]?.message?.content?.trim() || '',
      debug: process.env.DEBUG_SQL ? { sql, rows: rows?.length ?? 0 } : undefined,
    });

  } catch (err) {
    // CRUCIAL: surface the real error so you don’t see just FUNCTION_INVOCATION_FAILED
    console.error('QUERY_ERROR:', err);
    return json(res, 500, { ok: false, error: String(err?.message || err) });
  }
}