// api/query.js  — Hosted LLM→SQL engine (hard‑prompt version)
// Force Node runtime (pg won't run on Edge)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

// ---------- config ----------
const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 600000);
const MAX_LIMIT = 500;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function send(res, status, obj) {
  res.status(status).setHeader('content-type', 'application/json');
  res.end(JSON.stringify(obj));
}

// ---------- tiny utils ----------
const log = (...args) => {
  if (process.env.DEBUG_SQL) console.log('[debug]', ...args);
};
function ensureLimit(sql, n = MAX_LIMIT) {
  return /\blimit\s+\d+\b/i.test(sql) ? sql : `${sql.trim()}\nLIMIT ${n}`;
}
function normalizeQuestion(q = '') {
  let out = String(q);
  out = out.replace(/\butlizaiton\b|\butliza?tion\b|\butl?ization\b/gi, 'utilization');
  out = out.replace(/\butilisation\b/gi, 'utilization');
  out = out.replace(/\bbtc-?e?t?h?\b/gi, 'WETH');
  return out.trim();
}
function normalizeSymbol(sym = '') {
  const s = String(sym).trim().toUpperCase();
  if (s === 'ETH') return 'WETH';
  return s;
}
function extractSymbolFromText(q = '') {
  const m = q.match(/\b(USDC|USDBC|WETH|ETH|CBETH|CBBTC|GHO|EURC|WEETH)\b/i);
  return m ? normalizeSymbol(m[0]) : null;
}
function looksLikeLatest(q = '') {
  return /\b(latest|most\s+recent|current)\b/i.test(q);
}
function stripTimeFilterIfAny(sql) {
  let out = sql;
  out = out.replace(/AND\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, '');
  out = out.replace(
    /WHERE\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is,
    (m) => (/\bAND\b/i.test(m) ? m : 'WHERE 1=1 ')
  );
  return out;
}

// --- heuristic fixer to patch common LLM SQL slips ---
function tweakSqlHeuristics(sql, question = '') {
  let s = String(sql || '');

  // utilization is 0..1; convert obvious % thresholds (>= 85 -> >= 0.85)
  s = s.replace(/(utilization\s*[<>]=?\s*)(\d+(\.\d+)?)/ig, (m, left, num) => {
    const n = parseFloat(num);
    if (isFinite(n) && n >= 1) return `${left}${(n / 100).toFixed(4)}`;
    return m;
  });

  // Add hourly pre-agg if user talks about streak/hours but query lacks date_trunc('hour')
  if (/\b(consecutive|streak|hours?)\b/i.test(question) && !/date_trunc\s*\(\s*'hour'/i.test(s)) {
    if (/from\s+public\.market_data\b/i.test(s) && /utilization\b/i.test(s)) {
      s = s.replace(
        /from\s+public\.market_data\b/i,
        "FROM (\n  SELECT date_trunc('hour', ts) AS hour, AVG(utilization) AS utilization,\n         protocol, symbol\n  FROM public.market_data\n  WHERE protocol='aave'\n  GROUP BY 1, protocol, symbol\n) h"
      );
      s = s.replace(/\bts\b/gi, 'hour');
    }
  }

  // "at least 24 hours" guard
  if (/\bat\s+least\s+24\b/i.test(question)) {
    s = s.replace(/\bstreak_count\s*=\s*24\b/gi, 'streak_count >= 24');
    s = s.replace(/\bhours\s*=\s*24\b/gi,        'hours >= 24');
  }

  // Rewrite illegal percentile_cont(... ) OVER(...) into correlated subquery (rolling p75)
  const aliasMatch = s.match(/from\s*\(\s*select\s*date_trunc\(\s*'hour'\s*,\s*ts\)\s+as\s+hour[\s\S]+?\)\s+([a-z_][a-z0-9_]*)/i);
  const baseAlias = aliasMatch?.[1] || 'h';
  const hasSymbolRef = new RegExp(`\\b${baseAlias}\\s*\\.\\s*symbol\\b`, 'i').test(s) || /\bsymbol\b/i.test(s);

  s = s.replace(
    /percentile_cont\s*\(\s*0\s*\.\s*?75\s*\)\s*within\s+group\s*\(\s*order\s+by\s+([a-z_][a-z0-9_\.]*)\s*\)\s*over\s*\([^)]*\)/ig,
    (_m, orderCol) => {
      const colOnly = orderCol.includes('.') ? orderCol.split('.').pop() : orderCol;
      const symPred = hasSymbolRef ? `AND h2.symbol = ${baseAlias}.symbol` : '';
      return `(
  SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY h2.${colOnly})
  FROM (
    SELECT date_trunc('hour', ts) AS hour, symbol, protocol, AVG(utilization) AS util
    FROM public.market_data
    WHERE protocol='aave'
      AND ts >= NOW() - INTERVAL '6 months'
    GROUP BY 1,2,3
  ) h2
  WHERE h2.hour BETWEEN ${baseAlias}.hour - INTERVAL '30 days' AND ${baseAlias}.hour
  ${symPred}
)`;
    }
  );

  return s;
}

// ---- SAFE SQL GUARD (CTE-aware + strings-aware + SRF allowlist + alias aware) ----
function guardSql(sql, whitelistTables, whitelistColsByTable) {
  let s = String(sql || '').trim();
  if (s.endsWith(';')) s = s.slice(0, -1).trim();

  const stripStrings = (text) => text.replace(/'(?:''|[^'])*'/g, (m) => ' '.repeat(m.length));
  const sNoStrings = stripStrings(s);

  // Single statement; allow WITH ... SELECT
  if (!/^\s*(select|with)\b/i.test(sNoStrings)) {
    throw new Error('Only SELECT (or WITH ... SELECT) statements are allowed.');
  }
  if (sNoStrings.includes(';')) throw new Error('Multiple statements are not allowed.');
  const forbidden = /\b(update|insert|delete|drop|alter|truncate|create|grant|revoke|copy|vacuum|analyze)\b/i;
  if (forbidden.test(sNoStrings)) throw new Error('Write/DDL statements are not allowed.');
  if (/(--|\/\*)/.test(sNoStrings)) throw new Error('SQL comments are not allowed.');

  const normalizeTable = (t) => t.replace(/^public\./i, '').toLowerCase();

  // collect derived aliases and CTE names so we don't treat them as "real tables"
  const aliasSet = new Set();
  {
    const aliasRegex = /\)\s+([a-z_][a-z0-9_]*)/gi; // ") alias"
    let m; while ((m = aliasRegex.exec(sNoStrings))) aliasSet.add(m[1].toLowerCase());
  }
  {
    const cteRegex = /(?:^|\bwith|,)\s*([a-z_][a-z0-9_]*)\s*(?:\([^)]+\))?\s+as\s*\(/gi;
    let m; while ((m = cteRegex.exec(sNoStrings))) aliasSet.add(m[1].toLowerCase());
  }

  const allowedSrfs = new Set(['generate_series', 'unnest']);
  const tableCandidates = new Set();
  {
    const fromJoinRegex = /\b(?:from|join)\s+([a-z_][a-z0-9_]*(?:\s*\.\s*[a-z_][a-z0-9_]*)?)(\s*\(|\s|$)/gi;
    let m;
    while ((m = fromJoinRegex.exec(sNoStrings))) {
      const raw = m[1].replace(/\s+/g, '');
      const looksFunc = /\(/.test(m[2] || '');
      const base = raw.split('.')[0].toLowerCase();
      if (looksFunc && allowedSrfs.has(base)) continue;
      const candidate = normalizeTable(raw);
      if (!aliasSet.has(candidate)) tableCandidates.add(candidate);
    }
  }
  {
    const qualColRegex = /\b([a-z_][a-z0-9_]*(?:\s*\.\s*[a-z_][a-z0-9_]*)?)\s*\.\s*[a-z_][a-z0-9_]*/gi;
    let m;
    while ((m = qualColRegex.exec(sNoStrings))) {
      const qualifier = m[1].replace(/\s+/g, '');
      if (qualifier.includes('.')) {
        const parts = qualifier.split('.');
        const tbl = normalizeTable(
          `${parts.length === 2 ? parts[0] : parts[parts.length - 2]}.${parts[parts.length - 1]}`
        );
        if (!aliasSet.has(tbl)) tableCandidates.add(tbl);
      }
    }
  }

  for (const t of tableCandidates) {
    if (t && !whitelistTables.has(t)) {
      throw new Error(`Table not allowed: ${t}`);
    }
  }

  // fully-qualified column allowlisting
  {
    const fqCols = sNoStrings.match(/\b([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)\s*\.\s*([a-z_][a-z0-9_]*)/gi) || [];
    for (const ref of fqCols) {
      const parts = ref.split('.');
      let tbl, col;
      if (parts.length >= 3) { // schema.table.col
        tbl = parts[parts.length - 2].toLowerCase();
        col = parts[parts.length - 1].toLowerCase();
      } else {
        tbl = parts[0].toLowerCase();
        col = parts[1].toLowerCase();
      }
      tbl = tbl.replace(/^public\./i, '');
      if (allowedSrfs.has(tbl)) continue;
      if (aliasSet.has(tbl)) continue;

      const allowedCols = whitelistColsByTable.get(tbl);
      if (allowedCols && !allowedCols.has(col)) {
        throw new Error(`Column not allowed: ${tbl}.${col}`);
      }
    }
  }

  return ensureLimit(s);
}

// Build schema allowlist once per cold start
let schemaCache = null;
async function fetchSchema() {
  if (schemaCache) return schemaCache;
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `SELECT table_name, column_name
       FROM information_schema.columns
       WHERE table_schema='public' AND table_name IN ('market_data')
       ORDER BY table_name, ordinal_position;`
    );
    const byTable = new Map();
    for (const r of rows) {
      const t = r.table_name.toLowerCase();
      if (!byTable.has(t)) byTable.set(t, new Set());
      byTable.get(t).add(r.column_name.toLowerCase());
    }
    const tables = new Set(Array.from(byTable.keys()));
    const doc = Array.from(byTable.entries())
      .map(([t]) =>
        `${t}(protocol text, symbol text, supply numeric, borrows numeric, available numeric, ` +
        `supply_apy numeric, borrow_apy numeric, utilization numeric, ts timestamptz)`
      ).join('\n');

    schemaCache = { tables, colsByTable: byTable, doc };
    return schemaCache;
  } finally {
    client.release();
  }
}

// ---------- handler ----------
export default async function handler(req, res) {
  if (req.method !== 'POST') return send(res, 405, { ok: false, error: 'Use POST' });

  // same-origin bypass (UI) else require x-service-key
  const host = req.headers.host || '';
  const referer = req.headers.referer || req.headers.origin || '';
  let sameOrigin = false;
  try { sameOrigin = !!referer && new URL(referer).host === host; } catch { sameOrigin = false; }

  const svcKey = req.headers['x-service-key'] || '';
  const requiredKey = process.env.SERVICE_API_KEY || '';
  if (!sameOrigin && (!svcKey || svcKey !== requiredKey)) {
    return send(res, 401, { ok: false, error: 'unauthorized' });
  }

  // parse body
  let body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch { return send(res, 400, { ok:false, error:'Invalid JSON body' }); } }
  const rawQuestion = (body?.question || '').trim();
  if (!rawQuestion) return send(res, 400, { ok:false, error:'Missing "question"' });
  const question = normalizeQuestion(rawQuestion);

  // DB probe
  try {
    const c = await pool.connect();
    try { await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); await c.query('SELECT 1'); }
    finally { c.release(); }
  } catch (e) {
    return send(res, 500, { ok:false, error:`DB error: ${e.message}` });
  }

  // fast path for "latest"
  if (looksLikeLatest(question)) {
    const sym = extractSymbolFromText(question) || 'USDC';
    const symbol = normalizeSymbol(sym);
    const latestSql = `
      SELECT ts, utilization, ROUND(utilization*100,2) AS utilization_pct
      FROM public.market_data
      WHERE protocol='aave' AND symbol='${symbol}'
      ORDER BY ts DESC
      LIMIT 1
    `;
    try {
      const c = await pool.connect();
      let rows;
      try { await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c.query(latestSql)).rows; }
      finally { c.release(); }
      const r = rows[0];
      return send(res, 200, {
        ok: true,
        answer: r ? `Latest ${symbol} utilization is ${Number(r.utilization).toFixed(8)}.` : 'No results for that query.',
        sql: latestSql.trim(),
        rows
      });
    } catch (e) {
      return send(res, 500, { ok:false, error:`SQL error: ${e.message}`, sql: latestSql.trim() });
    }
  }

  // LLM → SQL (hard‑prompt system prompt)
  if (!process.env.OPENAI_API_KEY) return send(res, 500, { ok:false, error:'OPENAI_API_KEY not set' });
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  const { tables, colsByTable, doc: schemaDoc } = await fetchSchema();

  let rawGen;
  try {
    const gen = await openai.chat.completions.create({
      model: 'gpt-5',
      messages: [
        {
          role: 'system',
          content:
            `Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the schema below.\n` +
            `Rules:\n` +
            `• You MAY use CTEs (WITH ...) but only ONE statement total (no trailing semicolons).\n` +
            `• Use public.market_data and exact columns; always include protocol='aave'.\n` +
            `• Asset symbols must be UPPERCASE; if user says ETH, use symbol='WETH'.\n` +
            `• If user asks for "latest"/"most recent", use ORDER BY ts DESC LIMIT 1 without a time filter.\n` +
            `• For windows ('7 days', etc.), add ts >= NOW() - INTERVAL '<window>'.\n` +
            `• utilization is 0..1; interpret percentages (85% -> 0.85).\n` +
            `• Avoid percentile_cont/disc with OVER(); for rolling percentiles, pre-aggregate hourly and compute via correlated subquery.\n` +
            `Return STRICT JSON only.\n\n` +
            `Whitelisted schema:\n${schemaDoc}`
        },
        { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing a single key "sql".` }
      ]
    });
    rawGen = gen.choices?.[0]?.message?.content || '{}';
  } catch (e) {
    return send(res, 500, { ok:false, error:`LLM error: ${e.message}` });
  }

  let sql;
  try {
    const obj = JSON.parse(rawGen);
    sql = String(obj.sql || '').trim();
  } catch {
    return send(res, 500, { ok:false, error:'LLM returned non-JSON', raw: rawGen });
  }
  if (!/^\s*(select|with)\b/i.test(sql)) return send(res, 400, { ok:false, error:'Only SELECT/CTE allowed', sql });

  sql = tweakSqlHeuristics(sql, question);

  // Guard & run (with retry for specific syntax issues)
  let safeSql;
  try {
    safeSql = guardSql(sql, tables, colsByTable);
  } catch (e) {
    log('guard blocked:', sql);
    return send(res, 400, { ok:false, error: e.message, sql });
  }

  let rows;
  try {
    const c2 = await pool.connect();
    try { await c2.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c2.query(safeSql)).rows; }
    finally { c2.release(); }
  } catch (err) {
    const msg = String(err?.message || '');
    const needsRetry =
      /syntax error/i.test(msg) ||
      /OVER is not supported for ordered-set aggregate/i.test(msg) ||
      /percentile_(cont|disc).*OVER/i.test(msg);

    if (!needsRetry) return send(res, 500, { ok:false, error:`SQL error: ${msg}`, sql: safeSql });

    // Retry: ask model to avoid the failing construct
    let raw2;
    try {
      const regen = await openai.chat.completions.create({
        model: 'gpt-5',
        messages: [
          {
            role: 'system',
            content:
              `Regenerate a SINGLE Postgres query as JSON {"sql":"..."} that avoids the failing construct.\n` +
              `Hard rules:\n` +
              `• You MAY use CTEs; one statement only; no semicolons.\n` +
              `• DO NOT use percentile_cont/disc with OVER(). For rolling 30-day p75, use a correlated subquery over an hourly pre-agg.\n` +
              `• You MAY use generate_series for lead–lag grids.\n` +
              `• Always include protocol='aave' and uppercase symbols (ETH -> WETH).\n` +
              `Return STRICT JSON only.`
          },
          { role: 'user', content: `Original question:\n${question}` },
          { role: 'assistant', content: `Previous SQL:\n${safeSql}\n\nError:\n${msg}` }
        ]
      });
      raw2 = regen.choices?.[0]?.message?.content || '{}';
    } catch (e2) {
      return send(res, 500, { ok:false, error:`Retry LLM error: ${e2.message}`, sql: safeSql });
    }

    let sql2;
    try { sql2 = String(JSON.parse(raw2).sql || '').trim(); }
    catch { return send(res, 500, { ok:false, error:'Retry model returned non-JSON', raw: raw2 }); }

    const patched2 = tweakSqlHeuristics(sql2, question);
    try {
      safeSql = guardSql(patched2, tables, colsByTable);
    } catch (e) {
      return send(res, 400, { ok:false, error: e.message, sql: patched2 });
    }

    const c3 = await pool.connect();
    try { await c3.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c3.query(safeSql)).rows; }
    finally { c3.release(); }
  }

  // If empty with a time filter, retry without it
  if (rows.length === 0 && /ts\s*>=/i.test(safeSql)) {
    const fallback = stripTimeFilterIfAny(safeSql);
    const c4 = await pool.connect();
    try { await c4.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c4.query(fallback)).rows; }
    finally { c4.release(); }
  }

  // Summarize (short)
  let answer = '';
  try {
    const sum = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Answer in 1–2 sentences. Use only numbers from the rows. No tables.' },
        { role: 'user', content: `Question: ${rawQuestion}` },
        { role: 'assistant', content: `Rows: ${JSON.stringify(rows).slice(0, 100000)}` }
      ]
    });
    answer = sum.choices?.[0]?.message?.content?.trim() || '';
  } catch (_) {}

  return send(res, 200, { ok: true, answer, sql: safeSql, rows });
}