// api/query.js — Hosted LLM→SQL (registry-backed, clean.* tables only)
// Node.js Serverless runtime (Vercel)
export const config = { runtime: 'nodejs' };

import { Pool } from 'pg';
import OpenAI from 'openai';

import { guardSql } from '../lib/guard.js';
import { fetchSchema } from '../lib/schema.js';
import { planQuery, retryPlan, generateAnswer } from '../lib/instructions.js';

// ---------------- Config ----------------
const DB_TIMEOUT_MS = Number(process.env.DB_QUERY_TIMEOUT_MS || 600_000);
const MAX_LIMIT = 500;

// ---- CORS ----
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN || '*';
const CORS_HEADERS = {
  'access-control-allow-origin': ALLOW_ORIGIN,
  'access-control-allow-headers': 'content-type, x-service-key',
  'access-control-allow-methods': 'POST, OPTIONS',
};

// ---------------- Utilities ----------------
function send(res, status, obj) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.setHeader('cache-control', 'no-store');
  for (const [k, v] of Object.entries(CORS_HEADERS)) res.setHeader(k, v);
  res.end(JSON.stringify(obj));
}
const log = (...args) => { if (process.env.DEBUG_SQL) console.log('[debug]', ...args); };

function normalizeQuestion(s = '') {
  return String(s).replace(/\s+/g, ' ').trim();
}
function postProcessAnswer(s = '') {
  return String(s).replace(/\s+%/g, '%').replace(/\s+,/g, ',').trim();
}

// --- Format helpers ---
const trimTrailingZeros = s => s.replace(/(\.\d*[1-9])0+$|\.0+$/, '$1');
const fmtAbbr = (num, d, suf) => trimTrailingZeros((num / d).toFixed(2)) + suf;
function formatUSD(n) {
  const num = typeof n === 'number' ? n : Number(n);
  if (!isFinite(num)) return n;
  const abs = Math.abs(num);
  if (abs >= 1e12) return `$${fmtAbbr(num, 1e12, 'T')}`;
  if (abs >= 1e9) return `$${fmtAbbr(num, 1e9, 'B')}`;
  if (abs >= 1e6) return `$${fmtAbbr(num, 1e6, 'M')}`;
  if (abs >= 1e3) return `$${fmtAbbr(num, 1e3, 'K')}`;
  return `$${trimTrailingZeros(num.toLocaleString('en-US', {
    minimumFractionDigits: 0, maximumFractionDigits: 2
  }))}`;
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
  if (abs >= 1e9) return `${trimTrailingZeros((num / 1e9).toFixed(2))}B`;
  if (abs >= 1e6) return `${trimTrailingZeros((num / 1e6).toFixed(2))}M`;
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
function percentifyRows(rows = []) {
  return rows.map((row) => {
    const out = { ...row };
    for (const key of Object.keys(out)) {
      const v = out[key];
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
function englishDate(d) {
  try {
    const dt = new Date(d);
    if (isNaN(dt.getTime())) return d;
    return dt.toLocaleDateString('en-US', {
      year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC'
    });
  } catch { return d; }
}

// ---------------- Lazy singletons ----------------
let _pool = null;
function getPool() {
  if (_pool) return _pool;
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error('DATABASE_URL not set');
  _pool = new Pool({ connectionString: url, ssl: { rejectUnauthorized: false } });
  return _pool;
}
let _openai = null;
function getOpenAI() {
  if (_openai) return _openai;
  const key = process.env.OPENAI_API_KEY;
  if (!key) throw new Error('OPENAI_API_KEY not set');
  _openai = new OpenAI({ apiKey: key });
  return _openai;
}

// ---------------- Handler ----------------
export default async function handler(req, res) {
  try {
    // Preflight
    if (req.method === 'OPTIONS') {
      for (const [k, v] of Object.entries(CORS_HEADERS)) res.setHeader(k, v);
      res.statusCode = 204; res.end(); return;
    }
    for (const [k, v] of Object.entries(CORS_HEADERS)) res.setHeader(k, v);
    if (req.method !== 'POST') return send(res, 405, { ok: false, error: 'Use POST' });

    // auth (optional, via SERVICE_API_KEY)
    const host = req.headers.host || '';
    const referer = req.headers.referer || req.headers.origin || '';
    let sameOrigin = false;
    try { sameOrigin = !!referer && new URL(referer).host === host; } catch { sameOrigin = false; }
    const svcKey = req.headers['x-service-key'] || '';
    const requiredKey = process.env.SERVICE_API_KEY || '';
    if (!sameOrigin && requiredKey && (!svcKey || svcKey !== requiredKey)) {
      return send(res, 401, { ok: false, error: 'unauthorized' });
    }

    // parse body
    let body = req.body;
    if (typeof body === 'string') {
      try { body = JSON.parse(body); }
      catch { return send(res, 400, { ok: false, error: 'Invalid JSON body' }); }
    }
    const rawQuestion = (body?.question || '').trim();
    if (!rawQuestion) return send(res, 400, { ok: false, error: 'Missing "question"' });
    const question = normalizeQuestion(rawQuestion);
    const minimal =
      body?.minimal === true ||
      String(req.headers['x-minimal'] || '').trim().toLowerCase() === '1';

    // ✅ ping fast-path (now safely inside handler)
    if (question.toLowerCase() === 'ping') {
      return send(res, 200, { ok: true, answer: 'pong' });
    }

    // init clients
    let pool, openai;
    try {
      pool = getPool();
      openai = getOpenAI();
    } catch (e) {
      return send(res, 500, { ok: false, error: `Config error: ${e.message}` });
    }

    // DB probe
    try {
      const c = await pool.connect();
      try {
        await c.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
        await c.query('SELECT 1');
      } finally { c.release(); }
    } catch (e) {
      return send(res, 500, { ok: false, error: `DB error: ${e.message}` });
    }

    // -------- Registry-backed schema --------
    const { tables, colsByTable, doc } = await fetchSchema();

    // -------- Planner stage --------
    let plan;
    try {
      plan = await planQuery(getOpenAI(), question, doc);
    } catch (e) {
      return send(res, 500, { ok: false, error: `LLM planning error: ${e.message}` });
    }

    // Guard & run
    let plannedSql = plan.sql;
    let safeSql;
    try {
      safeSql = guardSql(plannedSql, tables, colsByTable, MAX_LIMIT);
    } catch (e) {
      return send(res, 400, { ok: false, error: e.message, sql: plannedSql, plan });
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
      if (!retryable) return send(res, 500, { ok: false, error: `SQL error: ${msg}`, sql: safeSql, plan });

      // retry plan
      try {
        const plan2 = await retryPlan(getOpenAI(), question, safeSql, msg, doc);
        plannedSql = plan2.sql;
        safeSql = guardSql(plannedSql, tables, colsByTable, MAX_LIMIT);

        const c3 = await pool.connect();
        try {
          await c3.query(`SET statement_timeout = ${DB_TIMEOUT_MS}`);
          rows = (await c3.query(safeSql)).rows;
        } finally { c3.release(); }
        plan = plan2;
      } catch (e2) {
        return send(res, 500, { ok: false, error: `Retry plan error: ${e2.message}`, sql: plannedSql, plan });
      }
    }

    // -------- Answer stage --------
    try {
      const prettyRows = percentifyRows(scaleCountFields(scaleCurrencyFields(rows)));
      const answer = await generateAnswer(getOpenAI(), rawQuestion, prettyRows, plan.presentation);
      if (minimal) return send(res, 200, { ok: true, answer });
      return send(res, 200, { ok: true, answer, sql: safeSql, rows });
    } catch {
      const fallbackAnswer = postProcessAnswer(`Returned ${rows.length} row(s).`);
      if (minimal) return send(res, 200, { ok: true, answer: fallbackAnswer });
      return send(res, 200, { ok: true, answer: fallbackAnswer, sql: safeSql, rows });
    }

  } catch (err) {
    return send(res, 500, { ok: false, error: `Unhandled server error: ${String(err?.message || err)}` });
  }
}