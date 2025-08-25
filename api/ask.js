// Vercel serverless endpoint: POST /api/ask  { q: "your question", debug?: true }

import OpenAI from "openai";
import pg from "pg";

const { Pool } = pg;

// ---------- config ----------
const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 10000);

// Reuse pool across invocations (important on serverless)
const pool =
  globalThis.__THALLOS_POOL__ ||
  new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 10000
  });
if (!globalThis.__THALLOS_POOL__) globalThis.__THALLOS_POOL__ = pool;

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || process.env.LLM_KEY
});

// ---------- tiny utils ----------
const log = (...args) => {
  if (process.env.DEBUG_SQL) console.log(...args);
};
const ensureLimit = (sql, n = 500) =>
  /\blimit\s+\d+\b/i.test(sql) ? sql : `${sql.trim()}\nLIMIT ${n}`;

function normalizeQuestion(q = "") {
  let out = q;
  out = out.replace(/\butlizaiton\b|\butliza?tion\b|\butl?ization\b/gi, "utilization");
  out = out.replace(/\butilisation\b/gi, "utilization");
  out = out.replace(/\bbtc-?e?t?h?\b/gi, "WETH");
  return out.trim();
}
function normalizeSymbol(sym = "") {
  const s = String(sym).trim().toUpperCase();
  if (s === "ETH") return "WETH";
  return s;
}
function extractSymbolFromText(q = "") {
  const m = q.match(/\b(USDC|USDBC|WETH|ETH|CBETH|CBBTC|GHO|EURC|WEETH)\b/i);
  return m ? normalizeSymbol(m[0]) : null;
}
function looksLikeLatest(q = "") {
  return /\b(latest|most\s+recent|current)\b/i.test(q);
}

// --- heuristic fixer to patch common LLM SQL slips ---
function tweakSqlHeuristics(sql, question = "") {
  let s = String(sql || "");

  // utilization is 0..1; convert percentage thresholds (e.g., >= 85 -> >= 0.85)
  s = s.replace(/(utilization\s*[<>]=?\s*)(\d+(\.\d+)?)/ig, (m, left, num) => {
    const n = parseFloat(num);
    if (isFinite(n) && n >= 1) return `${left}${(n / 100).toFixed(4)}`;
    return m;
  });

  // If question says "at least 24" hours, force >= 24
  if (/\bat\s+least\s+24\b/i.test(question)) {
    s = s.replace(/\bstreak_count\s*=\s*24\b/gi, "streak_count >= 24");
    s = s.replace(/\bhours\s*=\s*24\b/gi, "hours >= 24");
  }

  // Rewrite illegal percentile_cont ... OVER (...) (ordered-set agg) into a correlated subquery.
  const aliasMatch = s.match(/from\s*\(\s*select\s*date_trunc\(\s*'hour'\s*,\s*ts\)\s+as\s+hour[\s\S]+?\)\s+([a-z_][a-z0-9_]*)/i);
  const baseAlias = aliasMatch?.[1] || "h";
  const hasSymbolRef = new RegExp(`\\b${baseAlias}\\s*\\.\\s*symbol\\b`, "i").test(s) || /\bsymbol\b/i.test(s);

  s = s.replace(
    /percentile_cont\s*\(\s*0\s*\.\s*?75\s*\)\s*within\s+group\s*\(\s*order\s+by\s+([a-z_][a-z0-9_\.]*)\s*\)\s*over\s*\([^)]*\)/ig,
    (_m, orderCol) => {
      const colOnly = orderCol.includes(".") ? orderCol.split(".").pop() : orderCol;
      const symbolPredicate = hasSymbolRef ? `AND h2.symbol = ${baseAlias}.symbol` : "";
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

// ---- SAFE SQL GUARD (strings-aware + SRF allowlist + alias aware) ----
function guardSql(sql, whitelistTables, whitelistColsByTable) {
  let s = String(sql || "").trim();
  if (s.endsWith(";")) s = s.slice(0, -1).trim();

  const stripStrings = (text) => text.replace(/'(?:''|[^'])*'/g, (m) => " ".repeat(m.length));
  const sNoStrings = stripStrings(s);

  if (!/^\s*select\b/i.test(sNoStrings)) throw new Error("Only SELECT queries are allowed.");
  if (sNoStrings.includes(";")) throw new Error("Multiple statements are not allowed.");
  const forbidden = /\b(update|insert|delete|drop|alter|truncate|create|grant|revoke|copy|vacuum|analyze)\b/i;
  if (forbidden.test(sNoStrings)) throw new Error("Write/DDL statements are not allowed.");
  if (/(--|\/\*)/.test(sNoStrings)) throw new Error("SQL comments are not allowed.");

  // collect derived-table aliases like ") alias"
  const aliasSet = new Set();
  const aliasRegex = /\)\s+([a-z_][a-z0-9_]*)/gi;
  let m;
  while ((m = aliasRegex.exec(s))) aliasSet.add(m[1].toLowerCase());

  const normalizeTable = (t) => t.replace(/^public\./i, "").toLowerCase();
  const allowedSrfs = new Set(["generate_series", "unnest"]);
  const tableCandidates = new Set();

  const fromJoinRegex = /\b(?:from|join)\s+([a-z_][a-z0-9_]*(?:\s*\.\s*[a-z_][a-z0-9_]*)?)(\s*\(|\s|$)/gi;
  while ((m = fromJoinRegex.exec(s))) {
    const raw = m[1].replace(/\s+/g, "");
    const looksLikeFunc = /\(/.test(m[2] || "");
    const base = raw.split(".")[0].toLowerCase();
    if (looksLikeFunc && allowedSrfs.has(base)) continue; // allowed function in FROM/JOIN
    const candidate = normalizeTable(raw);
    if (!aliasSet.has(candidate)) tableCandidates.add(candidate);
  }

  for (const t of tableCandidates) {
    if (t && !whitelistTables.has(t)) {
      throw new Error(`Table not allowed: ${t}`);
    }
  }

  const fqCols = s.match(/\b([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)\s*\.\s*([a-z_][a-z0-9_]*)/gi) || [];
  for (const ref of fqCols) {
    const parts = ref.split(".");
    let tbl, col;
    if (parts.length >= 3) {
      tbl = parts[parts.length - 2].toLowerCase();
      col = parts[parts.length - 1].toLowerCase();
    } else {
      tbl = parts[0].toLowerCase();
      col = parts[1].toLowerCase();
    }
    tbl = tbl.replace(/^public\./i, "");

    if (allowedSrfs.has(tbl)) continue; // SRFs don't have table columns

    const allowedCols = whitelistColsByTable.get(tbl);
    if (allowedCols && !allowedCols.has(col)) {
      throw new Error(`Column not allowed: ${tbl}.${col}`);
    }
  }

  return ensureLimit(s);
}

// ---------- DB helpers ----------
async function fetchSchema() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT table_schema, table_name, column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name IN ('aave_v1')  -- use your Supabase table
      ORDER BY table_name, ordinal_position;
      `
    );
    const byTable = new Map();
    for (const r of rows) {
      const t = r.table_name.toLowerCase();
      if (!byTable.has(t)) byTable.set(t, new Set());
      byTable.get(t).add(r.column_name.toLowerCase());
    }
    const tables = new Set(Array.from(byTable.keys()));

    const doc = Array.from(byTable.entries())
      .map(
        ([t]) =>
          `${t}(protocol text, symbol text, supply numeric, borrows numeric, available numeric, ` +
          `supply_apy numeric, borrow_apy numeric, utilization numeric, ts timestamptz)`
      )
      .join("\n");

    return { tables, colsByTable: byTable, doc };
  } finally {
    return client.release();
  }
}

async function runQuery(sql, params = []) {
  const client = await pool.connect();
  try {
    await client.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
    log("[sql]", sql, params || []);
    const { rows } = await client.query(sql, params);
    return rows;
  } finally {
    client.release();
  }
}

// ---------- main answerer ----------
async function answer(questionRaw, debugFlag = false) {
  const question = normalizeQuestion(questionRaw);
  const { tables, colsByTable, doc: schemaDoc } = await fetchSchema();

  // fast path for "latest"
  if (looksLikeLatest(question)) {
    const sym = extractSymbolFromText(question) || "USDC";
    const symbol = normalizeSymbol(sym);
    const latestSql = `
      SELECT ts,
             utilization,
             ROUND(utilization*100,2) AS utilization_pct
      FROM public.aave_v1
      WHERE protocol='aave' AND symbol='${symbol}'
      ORDER BY ts DESC
      LIMIT 1
    `;
    const rows = await runQuery(latestSql);
    if (!rows.length) return { answer: "No results for that query.", sql: latestSql, rows: 0 };
    const r = rows[0];
    const pct = Number(r.utilization_pct).toFixed(2);
    return {
      answer: `Latest ${symbol} utilization is ${pct}% at ${r.ts?.toISOString?.() || r.ts}.`,
      sql: latestSql,
      rows: rows.length
    };
  }

  // Ask the model for SQL
  const gen = await openai.chat.completions.create({
    model: "gpt-5",
    messages: [
      {
        role: "system",
        content:
          `You write safe SQL for Postgres using ONLY the whitelisted schema below.\n` +
          `Return STRICT JSON: {"sql":"..."} and nothing else.\n` +
          `Rules:\n` +
          `• Use table public.aave_v1 (or aave_v1) and exact columns.\n` +
          `• Always include protocol='aave'.\n` +
          `• Asset symbols must be UPPERCASE; if the user says ETH, use symbol='WETH'.\n` +
          `• If the user asks for "latest"/"most recent", use ORDER BY ts DESC LIMIT 1 and DO NOT add a time filter.\n` +
          `• For windows (e.g., '7 days'), add ts >= NOW() - INTERVAL '<window>'.\n` +
          `• IMPORTANT: aave_v1.utilization is a 0..1 fraction. If the user says '85%' compare to 0.85, not 85.\n` +
          `• Do NOT use percentile_cont/percentile_disc with OVER(); for rolling percentiles use correlated subqueries.\n` +
          `• Single SELECT only; no comments; no CTEs; no multiple statements.\n\n` +
          `Whitelisted schema:\n${schemaDoc}\n`
      },
      { role: "user", content: `Question: ${question}\nRespond ONLY with JSON containing a single key "sql".` }
    ]
  });

  const raw = gen.choices?.[0]?.message?.content || "{}";
  let sql;
  try {
    const obj = JSON.parse(raw);
    sql = String(obj.sql || "").trim();
  } catch (e) {
    throw new Error(`Model did not return valid JSON SQL: ${raw}`);
  }
  if (!sql) throw new Error("Empty SQL from model");

  sql = tweakSqlHeuristics(sql, question);

  let safeSql;
  try {
    safeSql = guardSql(sql, tables, colsByTable);
  } catch (e) {
    // expose the blocked SQL in debug to help you iterate
    throw new Error(`[guard] ${e.message}. SQL: ${sql}`);
  }

  let rows = await runQuery(safeSql);

  // fallback: if empty & has a time filter, retry without it
  if (rows.length === 0 && /ts\s*>=/i.test(safeSql)) {
    const fallback = safeSql
      .replace(/AND\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, "")
      .replace(/WHERE\s+ts\s*>=\s*.+?(?=(\)|ORDER\s+BY|LIMIT|$))/is, (m) =>
        /\bAND\b/i.test(m) ? m : "WHERE 1=1 "
      );
    log("[sql-fallback]", fallback);
    rows = await runQuery(fallback);
  }

  // Summarize rows (short, 1–2 sentences)
  const sum = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "You are a precise analyst. Answer in plain English (no tables) in at most 1–2 sentences. " +
          "Use only the numbers present in the provided rows. If a value is a rate like 0.0167, render it as 1.67%. " +
          'If rows are empty, say "No results for that query."'
      },
      { role: "user", content: `Question: ${question}` },
      { role: "assistant", content: `Rows (JSON): ${JSON.stringify(rows).slice(0, 100000)}` }
    ]
  });

  const answer = sum.choices?.[0]?.message?.content?.trim() || "No results for that query.";
  return { answer, sql: debugFlag ? safeSql : undefined, rows: rows.length };
}

// ---------- HTTP handler ----------
export default async function handler(req, res) {
  // CORS (simple)
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-api-key");
  res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  // Optional shared secret for your API
  if (process.env.SERVICE_API_KEY) {
    const key = req.headers["x-api-key"];
    if (key !== process.env.SERVICE_API_KEY) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  try {
    const q = req.method === "GET" ? req.query.q : req.body?.q;
    const debug = (req.method === "GET" ? req.query.debug : req.body?.debug) ? true : false;
    if (!q || typeof q !== "string") {
      return res.status(400).json({ error: "Pass a question as 'q'." });
    }

    const { answer, sql, rows } = await answer(q, debug);
    return res.status(200).json({ ok: true, answer, sql: sql || undefined, rows });
  } catch (err) {
    console.error(err);
    const msg = err?.message || "Internal error";
    return res.status(500).json({ ok: false, error: msg });
  }
}