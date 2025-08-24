// src/llm/core.js
import 'dotenv/config';
import OpenAI from 'openai';
import pg from 'pg';

const { Pool } = pg;

export const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 15000);
export const MAX_LIMIT = 500;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: true },
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ---------- helpers ----------
const log = (...args) => { if (process.env.DEBUG_SQL) console.log(...args); };

const ensureLimit = (sql, n = MAX_LIMIT) =>
  /\blimit\s+\d+\b/i.test(sql) ? sql : `${sql.trim()}\nLIMIT ${n}`;

function normalizeQuestion(q = '') {
  return q
    .replace(/\butlizaiton\b|\butliza?tion\b|\butl?ization\b/gi, 'utilization')
    .replace(/\butilisation\b/gi, 'utilization')
    .replace(/\bbtc-?e?t?h?\b/gi, 'WETH')
    .trim();
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

// --- heuristic fixer (same as your local tool) ---
function tweakSqlHeuristics(sql, question = '') {
  let s = String(sql || '');

  // percent thresholds -> fractions for utilization (0..1)
  s = s.replace(/(utilization\s*[<>]=?\s*)(\d+(\.\d+)?)/ig, (m, left, num) => {
    const n = parseFloat(num);
    if (isFinite(n) && n >= 1) return `${left}${(n / 100).toFixed(4)}`;
    return m;
  });

  // Hourly compression if asking about consecutive/streak/hours and missing date_trunc('hour')
  if (/\b(consecutive|streak|hours?)\b/i.test(question) && !/date_trunc\s*\(\s*'hour'/i.test(s)) {
    if (/from\s+public\.aave_v1\b/i.test(s) && /utilization\b/i.test(s)) {
      s = s.replace(
        /from\s+public\.aave_v1\b/i,
        "FROM (\n  SELECT date_trunc('hour', ts) AS hour, AVG(utilization) AS utilization,\n         protocol, symbol\n  FROM public.aave_v1\n  GROUP BY 1, protocol, symbol\n) h"
      );
      s = s.replace(/\bts\b/gi, 'hour');
    }
  }

  if (/\bat\s+least\s+24\b/i.test(question)) {
    s = s.replace(/\bstreak_count\s*=\s*24\b/gi, 'streak_count >= 24');
    s = s.replace(/\bhours\s*=\s*24\b/gi,        'hours >= 24');
  }

  // Rewrite illegal percentile_cont(...) OVER(...) into correlated subquery
  const aliasMatch = s.match(/from\s*\(\s*select\s*date_trunc\(\s*'hour'\s*,\s*ts\)\s+as\s+hour[\s\S]+?\)\s+([a-z_][a-z0-9_]*)/i);
  const baseAlias = aliasMatch?.[1] || 'h';
  const hasSymbolRef = new RegExp(`\\b${baseAlias}\\s*\\.\\s*symbol\\b`, 'i').test(s) || /\bsymbol\b/i.test(s);

  s = s.replace(
    /percentile_cont\s*\(\s*0\s*\.\s*?75\s*\)\s*within\s+group\s*\(\s*order\s+by\s+([a-z_][a-z0-9_\.]*)\s*\)\s*over\s*\([^)]*\)/ig,
    (_m, orderCol) => {
      const colOnly = orderCol.includes('.') ? orderCol.split('.').pop() : orderCol;
      const symbolPredicate = hasSymbolRef ? `AND h2.symbol = ${baseAlias}.symbol` : '';
      return `(
  SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY h2.${colOnly})
  FROM (
    SELECT date_trunc('hour', ts) AS hour, symbol, protocol, AVG(utilization) AS util
    FROM public.aave_v1
    WHERE protocol='aave'
      AND ts >= NOW() - INTERVAL '6 months'
    GROUP BY 1,2,3
  ) h2
  WHERE h2.hour BETWEEN ${baseAlias}.hour - INTERVAL '30 days' AND ${baseAlias}.hour
  ${symbolPredicate}
)`;
    }
  );

  return s;
}

// ---- Guard (strings-aware + SRF allowlist + alias aware) ----
function guardSql(sql, whitelistTables, whitelistColsByTable) {
  let s = String(sql || '').trim();
  if (s.endsWith(';')) s = s.slice(0, -1).trim();

  const stripStrings = (text) => text.replace(/'(?:''|[^'])*'/g, (m) => ' '.repeat(m.length));
  const sNoStrings = stripStrings(s);

  if (!/^\s*select\b/i.test(sNoStrings)) throw new Error('Only SELECT queries are allowed.');
  if (sNoStrings.includes(';')) throw new Error('Multiple statements are not allowed.');
  if (/(--|\/\*)/.test(sNoStrings)) throw new Error('SQL comments are not allowed.');
  if (/\b(update|insert|delete|drop|alter|truncate|create|grant|revoke|copy|vacuum|analyze)\b/i.test(sNoStrings)) {
    throw new Error('Write/DDL statements are not allowed.');
  }

  // collect derived-table aliases
  const aliasSet = new Set();
  const aliasRegex = /\)\s+([a-z_][a-z0-9_]*)/gi;
  let m;
  while ((m = aliasRegex.exec(s))) aliasSet.add(m[1].toLowerCase());

  // allow SRFs
  const allowedSrfs = new Set(['generate_series', 'unnest']);
  const normalizeTable = (t) => t.replace(/^public\./i, '').toLowerCase();
  const tableCandidates = new Set();

  const fromJoinRegex = /\b(?:from|join)\s+([a-z_][a-z0-9_]*(?:\s*\.\s*[a-z_][a-z0-9_]*)?)(\s*\(|\s|$)/gi;
  while ((m = fromJoinRegex.exec(s))) {
    const raw = m[1].replace(/\s+/g, '');
    const looksLikeFunc = /\(/.test(m[2] || '');
    const base = raw.split('.')[0].toLowerCase();
    if (looksLikeFunc && allowedSrfs.has(base)) continue;
    const cand = normalizeTable(raw);
    if (!aliasSet.has(cand)) tableCandidates.add(cand);
  }

  for (const t of tableCandidates) {
    if (t && !whitelistTables.has(t)) {
      throw new Error(`Table not allowed: ${t}`);
    }
  }

  // qualified column whitelist
  const fqCols = s.match(/\b([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)\s*\.\s*([a-z_][a-z0-9_]*)/gi) || [];
  for (const ref of fqCols) {
    const parts = ref.split('.');
    let tbl, col;
    if (parts.length >= 3) {
      tbl = parts[parts.length - 2].toLowerCase();
      col = parts[parts.length - 1].toLowerCase();
    } else {
      tbl = parts[0].toLowerCase();
      col = parts[1].toLowerCase();
    }
    tbl = tbl.replace(/^public\./i, '');
    if (allowedSrfs.has(tbl)) continue;
    const allowedCols = whitelistColsByTable.get(tbl);
    if (allowedCols && !allowedCols.has(col)) {
      throw new Error(`Column not allowed: ${tbl}.${col}`);
    }
  }

  return ensureLimit(s);
}

async function fetchSchema() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(`
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema='public'
        AND table_name IN ('aave_v1')
      ORDER BY table_name, ordinal_position;
    `);
    const byTable = new Map();
    for (const r of rows) {
      const t = r.table_name.toLowerCase();
      if (!byTable.has(t)) byTable.set(t, new Set());
      byTable.get(t).add(r.column_name.toLowerCase());
    }
    const tables = new Set(Array.from(byTable.keys()));
    const doc = Array.from(byTable.entries())
      .map(([t, cols]) => `${t}(${Array.from(cols).join(', ')})`)
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
    log('[sql]', sql);
    const { rows } = await client.query(sql, params);
    return rows;
  } finally {
    client.release();
  }
}

function stripTimeFilterIfAny(sql) {
  let out = sql;
  out = out.replace(/AND\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, '');
  out = out.replace(/WHERE\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, (m) =>
    (/\bAND\b/i.test(m) ? m : 'WHERE 1=1 ')
  );
  return out;
}

// ---------- main exported function ----------
export async function ask(questionRaw) {
  const rawQuestion = questionRaw || 'What is the most recent USDC utilization rate?';
  const question = normalizeQuestion(rawQuestion);

  const { tables, colsByTable, doc: schemaDoc } = await fetchSchema();

  // Fast path for "latest"
  if (looksLikeLatest(question)) {
    const sym = extractSymbolFromText(question) || 'USDC';
    const symbol = normalizeSymbol(sym);
    const latestSql = `
      SELECT ts, utilization, ROUND(utilization*100,2) AS utilization_pct
      FROM public.aave_v1
      WHERE protocol='aave' AND symbol='${symbol}'
      ORDER BY ts DESC
      LIMIT 1
    `;
    const rows = await runQuery(latestSql);
    if (!rows.length) {
      return { ok: true, sql: latestSql, rows: [], answer: 'No results for that query.' };
    }
    const r = rows[0];
    const pct = Number(r.utilization_pct).toFixed(2);
    return {
      ok: true,
      sql: latestSql,
      rows,
      answer: `Latest ${symbol} utilization is ${pct}% at ${r.ts?.toISOString?.() || r.ts}.`,
    };
  }

  // Ask the model for a SINGLE SELECT (same rules you used locally)
  const gen = await openai.chat.completions.create({
    model: 'gpt-5',
    messages: [
      {
        role: 'system',
        content:
          `You write safe SQL for Postgres using ONLY the whitelisted schema below.\n` +
          `Return STRICT JSON: {"sql":"..."} and nothing else.\n` +
          `Rules:\n` +
          `• Use table public.aave_v1 (or aave_v1) and exact columns.\n` +
          `• Always include protocol='aave'.\n` +
          `• Asset symbols must be UPPERCASE; if the user says ETH, use 'WETH'.\n` +
          `• If "latest"/"most recent": ORDER BY ts DESC LIMIT 1 (no time filter).\n` +
          `• For windows: ts >= NOW() - INTERVAL '<window>'.\n` +
          `• utilization is a 0..1 fraction (85% => 0.85).\n` +
          `• For "consecutive hours"/"streaks": aggregate hourly and use gaps-and-islands.\n` +
          `• Do NOT use percentile_cont/disc with OVER(); for rolling percentiles, use a correlated subquery.\n` +
          `• Single SELECT only; no comments; no CTEs; no multiple statements.\n\n` +
          `Whitelisted schema:\n${schemaDoc}\n`
      },
      { role: 'user', content: `Question: ${question}\nRespond ONLY with JSON containing a single key "sql".` }
    ]
  });

  let sql;
  try {
    const raw = gen.choices?.[0]?.message?.content || '{}';
    sql = String(JSON.parse(raw).sql || '').trim();
  } catch (e) {
    throw new Error(`Model did not return valid JSON {sql}`);
  }
  if (!sql) throw new Error('Empty SQL from model');

  // Heuristic patches (percent→fraction, hourly, >=24, percentile rewrite)
  sql = tweakSqlHeuristics(sql, question);

  // Guard + run
  let safeSql;
  try {
    safeSql = guardSql(sql, tables, colsByTable);
  } catch (e) {
    // surface guard error
    return { ok: false, sql, rows: [], answer: '', error: `[guard] ${e.message}` };
  }

  let rows = await runQuery(safeSql);
  if (rows.length === 0 && /ts\s*>=/i.test(safeSql)) {
    const fallback = stripTimeFilterIfAny(safeSql);
    log('[sql-fallback]', fallback);
    rows = await runQuery(fallback);
  }

  // Summarize in 1–2 sentences
  const sum = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [
      {
        role: 'system',
        content:
          'You are a precise analyst. Answer in at most 1–2 sentences (no tables). ' +
          'Use only numbers present in rows. If a value is 0.0167, render it as 1.67%. ' +
          'If rows are empty, say "No results for that query."'
      },
      { role: 'user', content: `Question: ${rawQuestion}` },
      { role: 'assistant', content: `Rows: ${JSON.stringify(rows).slice(0, 100000)}` }
    ]
  });

  const answer = sum.choices?.[0]?.message?.content?.trim() || 'No results for that query.';
  return { ok: true, sql: safeSql, rows, answer };
}