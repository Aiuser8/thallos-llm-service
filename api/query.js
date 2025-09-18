// api/query.js — Hosted LLM→SQL (Aave + Aerodrome + Prices + clean.*)
// runtime hint (Vercel/edge will ignore pg; keep node)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

import { guardSql } from '../lib/guard.js';
import { fetchSchema } from '../lib/schema.js';
import {
  planQuery,
  retryPlan,
  buildPrimaryPrompt,   // kept exported
  buildRetryPrompt,     // kept exported
  generateAnswer
} from '../lib/instructions.js';

// ---------------- Local helpers ----------------
function normalizeQuestion(s = '') { return String(s).replace(/\s+/g, ' ').trim(); }
function extractSymbolFromText(s = '') {
  const re = /\b(BTC|ETH|XRP|USDT|BNB|SOL|USDC|DOGE|TRX|ADA|LINK|HYPE|USDE|SUI|BCH|XLM|AVAX|HBAR|CRO|LEO|LTC|WETH|DAI|WBTC)\b/i;
  const m = String(s).match(re); return m ? m[1].toUpperCase() : null;
}
const normalizeSymbol = (sym='') => String(sym).trim().toUpperCase();
const looksLikeLatest = (s='') => /\b(latest|current|now|most\s*recent)\b/i.test(s);
const looksLikeAero = (s='') => /\b(aero|aerodrome)\b/i.test(s) || /\bdex\b/i.test(s);
const looksLikePrices = (s='') => /\b(price|market\s*cap|volume)\b/i.test(s) ||
  /\b(BTC|ETH|XRP|USDT|BNB|SOL|USDC|DOGE|TRX|ADA|LINK|HYPE|USDE|SUI|BCH|XLM|AVAX|HBAR|CRO|LEO|LTC)\b/i.test(s);
function postProcessAnswer(s=''){ return String(s).replace(/\s+%/g,'%').replace(/\s+,/g,',').trim(); }

// ---------- config ----------
const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 600000);
const MAX_LIMIT = 500;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

function send(res, status, obj) {
  if (typeof res.status === 'function') {
    res.status(status).setHeader('content-type', 'application/json');
  } else {
    res.statusCode = status;
    res.setHeader('content-type', 'application/json');
  }
  res.end(JSON.stringify(obj));
}

const log = (...args) => { if (process.env.DEBUG_SQL) console.log('[debug]', ...args); };

// --- percentify / currency / counts / dates ---
function percentifyRows(rows = []) {
  return rows.map((row) => {
    const out = { ...row };
    for (const key of Object.keys(out)) {
      const v = out[key];
      if (typeof v === 'string' && /\d/.test(v) && v.trim().endsWith('%')) continue;
      const num = (typeof v === 'number') ? v : (v != null && v !== '' ? Number(v) : NaN);
      const looksPctKey = /(_pct|percent|pct|utilization|util|rate|ratio|share)\b/i.test(key);
      if (!looksPctKey || !isFinite(num)) continue;
      let percentVal;
      if (num >= 0 && num <= 1) percentVal = num * 100;
      else if (num > 1 && num <= 100) percentVal = num;
      else continue;
      out[key] = `${percentVal.toFixed(2)}%`;
    }
    return out;
  });
}
const trimTrailingZeros = s => s.replace(/(\.\d*[1-9])0+$|\.0+$/,'$1');
const fmtAbbr = (num, d, suf) => trimTrailingZeros((num / d).toFixed(2)) + suf;
function formatUSD(n) {
  const num = typeof n === 'number' ? n : Number(n);
  if (!isFinite(num)) return n;
  const abs = Math.abs(num);
  if (abs >= 1e12) return `$${fmtAbbr(num, 1e12, 'T')}`;
  if (abs >= 1e9)  return `$${fmtAbbr(num, 1e9,  'B')}`;
  if (abs >= 1e6)  return `$${fmtAbbr(num, 1e6,  'M')}`;
  if (abs >= 1e3)  return `$${fmtAbbr(num, 1e3,  'K')}`;
  return `$${trimTrailingZeros(num.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 }))}`;
}
function scaleCurrencyFields(rows = []) {
  const usdLike = /(usd|tvl_usd|volume_usd|fees_usd|amount_usd|value_usd)$/i;
  return rows.map(r => {
    const out = { ...r };
    for (const k of Object.keys(out)) if (usdLike.test(k)) out[k] = formatUSD(out[k]);
    return out;
  });
}
function formatCount(n) {
  const num = typeof n === 'number' ? n : Number(n);
  if (!isFinite(num)) return n;
  const abs = Math.abs(num);
  if (abs >= 1e9) return `${trimTrailingZeros((num/1e9).toFixed(2))}B`;
  if (abs >= 1e6) return `${trimTrailingZeros((num/1e6).toFixed(2))}M`;
  return num.toLocaleString('en-US');
}
function scaleCountFields(rows = []) {
  const countLike = /(tx(_)?count|trades?|txs?)$/i;
  return rows.map(r => {
    const out = { ...r };
    for (const k of Object.keys(out)) if (countLike.test(k)) out[k] = formatCount(out[k]);
    return out;
  });
}
function englishDate(d) {
  try {
    const dt = new Date(d);
    if (isNaN(dt.getTime())) return d;
    return dt.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC' });
  } catch { return d; }
}

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

  // minimal response switch (answer-only)
  const minimal =
    body?.minimal === true ||
    String(req.headers['x-minimal'] || '').trim().toLowerCase() === '1';

  // DB probe
  try {
    const c = await pool.connect();
    try {
      await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
      await c.query('SELECT 1');
    } finally { c.release(); }
  } catch (e) {
    return send(res, 500, { ok:false, error:`DB error: ${e.message}` });
  }

  // --------- Fast paths (public.*) ---------
  const aeroQ = looksLikeAero(question);
  const pricesQ = looksLikePrices(question);
  const maybeSymbol = extractSymbolFromText(question);

  if (looksLikeLatest(question) && pricesQ && maybeSymbol) {
    const symbol = normalizeSymbol(maybeSymbol);
    const latestPriceSql = `
      SELECT asset_id, ts, price, market_cap, total_volume
      FROM public.token_prices
      WHERE asset_id='${symbol}'
      ORDER BY ts DESC
      LIMIT 1
    `;
    try {
      const c = await pool.connect();
      let rows;
      try { await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c.query(latestPriceSql)).rows; }
      finally { c.release(); }
      const r = rows[0];
      const answer = r
        ? `Latest ${symbol} (${englishDate(r.ts)}): price ${formatUSD(r.price)}, market cap ${formatUSD(r.market_cap)}, volume ${formatUSD(r.total_volume)}.`
        : `No ${symbol} rows found.`;
      if (minimal) return send(res, 200, { ok: true, answer });
      return send(res, 200, { ok: true, answer, sql: latestPriceSql.trim(), rows });
    } catch (e) {
      return send(res, 500, { ok:false, error:`SQL error: ${e.message}` });
    }
  }

  if (looksLikeLatest(question) && aeroQ) {
    const latestAeroSql = `
      SELECT date_utc, volume_usd, fees_usd, tvl_usd, tx_count
      FROM public.aero_protocol_daily
      ORDER BY date_utc DESC
      LIMIT 1
    `;
    try {
      const c = await pool.connect();
      let rows;
      try { await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`); rows = (await c.query(latestAeroSql)).rows; }
      finally { c.release(); }
      const r = rows[0];
      const answer = r
        ? `Latest Aerodrome day (${englishDate(r.date_utc)}): volume ${formatUSD(r.volume_usd)}, fees ${formatUSD(r.fees_usd)}, TVL ${formatUSD(r.tvl_usd)}, tx ${formatCount(r.tx_count)}.`
        : 'No Aerodrome rows found.';
      if (minimal) return send(res, 200, { ok: true, answer });
      return send(res, 200, { ok: true, answer, sql: latestAeroSql.trim(), rows });
    } catch (e) {
      return send(res, 500, { ok:false, error:`SQL error: ${e.message}` });
    }
  }

  // Latest Aave utilization (public.market_data)
  if (looksLikeLatest(question) && !aeroQ && !pricesQ) {
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
      const answer = r
        ? `Latest ${symbol} utilization is ${Number(r.utilization_pct).toFixed(2)}%.`
        : 'No results for that query.';
      if (minimal) return send(res, 200, { ok: true, answer });
      return send(res, 200, { ok: true, answer, sql: latestSql.trim(), rows });
    } catch (e) {
      return send(res, 500, { ok:false, error:`SQL error: ${e.message}`, sql: latestSql.trim() });
    }
  }

  // ---- Planner stage (LLM → ONE SELECT over whitelisted schema) ----
  if (!process.env.OPENAI_API_KEY) return send(res, 500, { ok:false, error:'OPENAI_API_KEY not set' });
  const { tables, colsByTable, doc } = await fetchSchema(pool); // tables is a Set of "schema.table"

  let plan;
  try {
    plan = await planQuery(openai, question, doc);
  } catch (e) {
    return send(res, 500, { ok:false, error:`LLM planning error: ${e.message}` });
  }

  // Guard & run
  let plannedSql = plan.sql;
  let safeSql;
  try {
    safeSql = guardSql(plannedSql, tables, colsByTable, MAX_LIMIT);
  } catch (e) {
    return send(res, 400, { ok:false, error: e.message, sql: plannedSql, plan });
  }

  let rows;
  try {
    const c2 = await pool.connect();
    try {
      await c2.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
      rows = (await c2.query(safeSql)).rows;
    } finally { c2.release(); }
  } catch (err) {
    const msg = String(err?.message || '');
    const retryable =
      /syntax error/i.test(msg) ||
      /OVER is not supported for ordered-set aggregate/i.test(msg) ||
      /percentile_(cont|disc).*OVER/i.test(msg);

    if (!retryable) return send(res, 500, { ok:false, error:`SQL error: ${msg}`, sql: safeSql, plan });

    // retry with a new plan
    try {
      const plan2 = await retryPlan(openai, question, safeSql, msg, doc);
      plannedSql = plan2.sql;
      safeSql = guardSql(plannedSql, tables, colsByTable, MAX_LIMIT);

      const c3 = await pool.connect();
      try {
        await c3.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
        rows = (await c3.query(safeSql)).rows;
      } finally { c3.release(); }

      plan = plan2; // prefer newer presentation
    } catch (e2) {
      return send(res, 500, { ok:false, error:`Retry plan error: ${e2.message}`, sql: plannedSql, plan });
    }
  }

  // ---- Answer stage (LLM formats result) ----
  try {
    const prettyRows = percentifyRows(scaleCountFields(scaleCurrencyFields(rows)));
    const answer = await generateAnswer(openai, rawQuestion, prettyRows, plan.presentation);
    if (minimal) return send(res, 200, { ok: true, answer });
    return send(res, 200, { ok: true, answer, sql: safeSql, rows });
  } catch {
    const fallbackAnswer = postProcessAnswer(`Returned ${rows.length} row(s).`);
    if (minimal) return send(res, 200, { ok: true, answer: fallbackAnswer });
    return send(res, 200, { ok: true, answer: fallbackAnswer, sql: safeSql, rows });
  }
}

BASE="https://YOUR_URL/api/query"
KEY="YOUR_SERVICE_KEY"