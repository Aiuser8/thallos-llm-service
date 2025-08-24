// Force Node runtime (pg needs Node, not Edge)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

// --- simple helpers ---
function send(res, status, obj) {
  res.status(status).setHeader('content-type', 'application/json');
  res.end(JSON.stringify(obj));
}
const log = (...a) => { if (process.env.DEBUG_SQL) console.log(...a); };

const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 60000);
const MAX_LIMIT = 500;

// --- PG pool (Supabase pooled URL recommended) ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// --- light CLI-to-HTTP shims from your improved script ---
function ensureLimit(sql, n = MAX_LIMIT) {
  const hasLimit = /\blimit\s+\d+\b/i.test(sql);
  return hasLimit ? sql : `${sql.trim()}\nLIMIT ${n}`;
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
  return s === 'ETH' ? 'WETH' : s;
}
function extractSymbolFromText(q = '') {
  const m = String(q).match(/\b(USDC|USDBC|WETH|ETH|CBETH|CBBTC|GHO|EURC|WEETH)\b/i);
  return m ? normalizeSymbol(m[0]) : null;
}
function looksLikeLatest(q = '') {
  return /\b(latest|most\s+recent|current)\b/i.test(q);
}

// --- heuristic fixer (percent → fraction, hourly buckets, rolling p75 rewrite) ---
function tweakSqlHeuristics(sql, question = '') {
  let s = String(sql || '');

  // 1) utilization 0..1; convert ">= 85" to ">= 0.85"
  s = s.replace(/(utilization\s*[<>]=?\s*)(\d+(\.\d+)?)/ig, (m, left, num) => {
    const n = parseFloat(num);
    return (isFinite(n) && n >= 1) ? `${left}${(n / 100).toFixed(4)}` : m;
    });

  // 2) streak/consecutive → ensure hourly base
  if (/\b(consecutive|streak|hours?)\b/i.test(question) && !/date_trunc\s*\(\s*'hour'/i.test(s)) {
    if (/from\s+public\.market_data\b/i.test(s) && /utilization\b/i.test(s)) {
      s = s.replace(
        /from\s+public\.market_data\b/i,
        "FROM (\n  SELECT date_trunc('hour', ts) AS hour, AVG(utilization) AS utilization,\n         protocol, symbol\n  FROM public.market_data\n  WHERE protocol='aave'\n  GROUP BY 1, protocol, symbol\n) h"
      );
      s = s.replace(/\bts\b/gi, 'hour');
    }
  }

  // 3) "at least 24 hours" → >= 24
  if (/\bat\s+least\s+24\b/i.test(question)) {
    s = s.replace(/\bstreak_count\s*=\s*24\b/gi, 'streak_count >= 24');
    s = s.replace(/\bhours\s*=\s*24\b/gi,        'hours >= 24');
  }

  // 4) rewrite illegal percentile_cont ... OVER (...) to correlated subquery
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

// --- SAFE SQL GUARD (CTE-aware + strings-aware + SRF allowlist + alias aware) ---
function guardSql(sql, whitelistTables, whitelistColsByTable) {
  let s = String(sql || '').trim();
  if (s.endsWith(';')) s = s.slice(0, -1).trim();

  const stripStrings = (text) => text.replace(/'(?:''|[^'])*'/g, (m) => ' '.repeat(m.length));
  const sNoStrings = stripStrings(s);

  if (!/^\s*(select|with)\b/i.test(sNoStrings)) throw new Error('Only SELECT (or WITH ... SELECT) statements are allowed.');
  if (sNoStrings.includes(';')) throw new Error('Multiple statements are not allowed.');
  const forbidden = /\b(update|insert|delete|drop|alter|truncate|create|grant|revoke|copy|vacuum|analyze)\b/i;
  if (forbidden.test(sNoStrings)) throw new Error('Write/DDL statements are not allowed.');
  if (/(--|\/\*)/.test(sNoStrings)) throw new Error('SQL comments are not allowed.');

  const normalizeTable = (t) => t.replace(/^public\./i, '').toLowerCase();
  const aliasSet = new Set();

  // derived-table aliases ") alias"
  {
    const aliasRegex = /\)\s+([a-z_][a-z0-9_]*)/gi;
    let m;
    while ((m = aliasRegex.exec(sNoStrings))) aliasSet.add(m[1].toLowerCase());
  }
  // CTE names in WITH
  {
    const cteRegex = /(?:^|\bwith|,)\s*([a-z_][a-z0-9_]*)\s*(?:\([^)]+\))?\s+as\s*\(/gi;
    let m;
    while ((m = cteRegex.exec(sNoStrings))) aliasSet.add(m[1].toLowerCase());
  }

  const allowedSrfs = new Set(['generate_series', 'unnest']);
  const tableCandidates = new Set();

  // FROM / JOIN targets (skip SRFs & derived/CTE aliases)
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
  // Also pick up schema.table from qualified column refs
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

// ---- schema fetch from information_schema ----
async function fetchSchema() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(`
      SELECT table_schema, table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name IN ('market_data')
      ORDER BY table_name, ordinal_position
    `);
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
      )
      .join('\n');
    return { tables, colsByTable: byTable, doc };
  } finally {
    client.release();
  }
}

async function runQuery(sql, params = []) {
  const client = await pool.connect();
  try {
    await client.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
    log('[sql]', sql, params || []);
    const { rows } = await client.query(sql, params);
    return rows;
  } finally { client.release(); }
}

function stripTimeFilterIfAny(sql) {
  let out = sql;
  out = out.replace(/AND\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, '');
  out = out.replace(/WHERE\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is,
    (m) => (/\bAND\b/i.test(m) ? m : 'WHERE 1=1 '));
  return out;
}

// =================== API HANDLER ===================
export default async function handler(req, res) {
  if (req.method !== 'POST') return send(res, 405, { ok: false, error: 'Use POST' });
  if (!process.env.SERVICE_API_KEY || req.headers['x-service-key'] !== process.env.SERVICE_API_KEY) {
    return send(res, 401, { ok: false, error: 'Unauthorized' });
  }

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const rawQuestion = String(body?.question || '').trim();
    if (!rawQuestion) return send(res, 400, { ok: false, error: 'Missing "question"' });

    const question = normalizeQuestion(rawQuestion);

    // 1) schema whitelist
    const { tables, colsByTable, doc: schemaDoc } = await fetchSchema();

    // 2) fast path "latest"
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
      const rows = await runQuery(latestSql);
      if (!rows.length) return send(res, 200, { ok: true, answer: 'No results for that query.', sql: latestSql, rows: [] });
      const r = rows[0];
      const pct = Number(r.utilization_pct).toFixed(2);
      return send(res, 200, {
        ok: true,
        answer: `Latest ${symbol} utilization is ${pct}% at ${r.ts?.toISOString?.() || r.ts}.`,
        sql: latestSql,
        rows,
      });
    }

    // 3) ask model for a single SQL (WITH allowed)
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    const gen = await openai.chat.completions.create({
      model: 'gpt-5',
      messages: [
        {
          role: 'system',
          content:
            `Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the schema below.\n` +
            `Rules:\n` +
            `• You MAY use CTEs (WITH ...) but only ONE statement total (no trailing semicolons).\n` +
            `• Use public.market_data (or market_data) and exact columns; always include protocol='aave'.\n` +
            `• Asset symbols must be UPPERCASE; if user says ETH, use symbol='WETH'.\n` +
            `• If user asks for "latest"/"most recent", use ORDER BY ts DESC LIMIT 1 without a time filter.\n` +
            `• For windows ('7 days', etc.), add ts >= NOW() - INTERVAL '<window>'.\n` +
            `• utilization is 0..1; interpret percentages accordingly (e.g., 85% -> 0.85).\n` +
            `• Avoid percentile_cont/disc with OVER(); for rolling percentiles, use correlated subquery on hourly pre-agg.\n` +
            `Return STRICT JSON only.\n\n` +
            `Whitelisted schema:\n${schemaDoc}\n`
        },
        { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing a single key "sql".` }
      ]
    });

    const raw = gen.choices?.[0]?.message?.content || '{}';
    let sql;
    try {
      const obj = JSON.parse(raw);
      sql = String(obj.sql || '').trim();
    } catch {
      return send(res, 500, { ok: false, error: 'LLM returned non-JSON', raw });
    }
    if (!sql) return send(res, 500, { ok: false, error: 'Empty SQL from model' });

    sql = tweakSqlHeuristics(sql, question);

    // 4) guard
    let safeSql;
    try {
      safeSql = guardSql(sql, tables, colsByTable);
    } catch (e) {
      return send(res, 400, { ok: false, error: e.message || String(e), sql });
    }

    // 5) run (with “syntax-aware retry” if needed)
    let rows;
    try {
      rows = await runQuery(safeSql);
    } catch (err) {
      const msg = String(err?.message || '');
      const needsRetry =
        /syntax error/i.test(msg) ||
        /OVER is not supported for ordered-set aggregate/i.test(msg) ||
        /percentile_(cont|disc).*OVER/i.test(msg);

      if (!needsRetry) return send(res, 500, { ok: false, error: msg, sql: safeSql });

      const regen = await openai.chat.completions.create({
        model: 'gpt-5',
        messages: [
          {
            role: 'system',
            content:
              `Regenerate a SINGLE Postgres query as JSON {"sql":"..."} that avoids the failing construct.\n` +
              `Hard rules:\n` +
              `• You MAY use CTEs; one statement only; no semicolons.\n` +
              `• DO NOT use percentile_cont/disc with OVER(). Use correlated subquery on hourly pre-agg for rolling percentiles.\n` +
              `• You MAY use generate_series for lead–lag grids.\n` +
              `• Always include protocol='aave' and uppercase symbols (ETH -> WETH).\n` +
              `Return STRICT JSON only.`
          },
          { role: 'user', content: `Original question:\n${question}` },
          { role: 'assistant', content: `Previous SQL:\n${safeSql}\n\nError:\n${msg}` }
        ]
      });

      const raw2 = regen.choices?.[0]?.message?.content || '{}';
      let sql2;
      try {
        const obj2 = JSON.parse(raw2);
        sql2 = String(obj2.sql || '').trim();
      } catch {
        return send(res, 500, { ok: false, error: 'Retry LLM returned non-JSON', raw: raw2 });
      }

      const patched2 = tweakSqlHeuristics(sql2, question);
      safeSql = guardSql(patched2, tables, colsByTable);
      rows = await runQuery(safeSql);
    }

    // 6) if empty & had time filter, retry without
    if (rows.length === 0 && /ts\s*>=/i.test(safeSql)) {
      const fallback = stripTimeFilterIfAny(safeSql);
      log('[sql-fallback]', fallback);
      rows = await runQuery(fallback);
    }

    // 7) summarize
    const sum = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: 'Answer in 1–2 sentences. Use only numbers present in the rows. No tables.' },
        { role: 'user', content: `Question: ${rawQuestion}` },
        { role: 'assistant', content: `Rows (JSON): ${JSON.stringify(rows).slice(0, 100000)}` }
      ]
    });

    return send(res, 200, {
      ok: true,
      answer: sum.choices?.[0]?.message?.content?.trim() || '',
      sql: safeSql,
      rows,
    });

  } catch (err) {
    console.error('QUERY_ERROR:', err);
    return send(res, 500, { ok: false, error: err.message || String(err) });
  }
}