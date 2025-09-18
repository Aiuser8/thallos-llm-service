// lib/instructions.js — Unified planner + prompts + answer formatting utilities.
import pkg from '../package.json' assert { type: 'json' };

/* -------------------------- TABLE DOC FROM PACKAGE -------------------------- */

const TABLES = pkg.tables || {};

/** Build a human-readable whitelist doc from package.json's "tables" section. */
function buildSchemaDoc() {
  const lines = [];
  for (const [fqtn, meta] of Object.entries(TABLES)) {
    lines.push(`${fqtn} — ${meta.description || ''}`.trim());
    lines.push('  columns:');
    const cols = meta.columns || {};
    for (const [col, desc] of Object.entries(cols)) {
      lines.push(`    - ${col}: ${desc}`);
    }
    const pk = meta.primary_key || [];
    if (pk.length) lines.push(`  primary_key: [${pk.join(', ')}]`);
    lines.push(''); // blank line between tables
  }
  return lines.join('\n');
}

/* ----------------------------- DATE HELPERS ----------------------------- */

// remove soft hyphens & zero-width chars that sometimes leak from LLM output
const stripInvisibles = (s = '') => s.replace(/[\u00ad\u200b\u200c\u200d\u2060]/g, '');

function ordinal(n) {
  const v = n % 100;
  if (v >= 11 && v <= 13) return `${n}th`;
  switch (n % 10) {
    case 1: return `${n}st`;
    case 2: return `${n}nd`;
    case 3: return `${n}rd`;
    default: return `${n}th`;
  }
}

function isoToEnglish(yyyy, mm, dd) {
  const y = Number(yyyy);
  const m = Number(mm);
  const d = Number(dd);
  const date = new Date(Date.UTC(y, m - 1, d));
  const month = date.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
  return `${month} ${ordinal(d)} ${y}`;
}

function monthTokenToEnglish(yyyy, mm) {
  const y = Number(yyyy);
  const m = Number(mm);
  const date = new Date(Date.UTC(y, m - 1, 1));
  const month = date.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
  return `${month} ${y}`;
}

/** Extract ISO date range from free text (YYYY-MM-DD … YYYY-MM-DD). */
function extractIsoRange(text = '') {
  const re = /(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\s*(?:to|–|—|-)\s*(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])/i;
  const m = text.match(re);
  if (!m) return null;
  const [ , y1, m1, d1, y2, m2, d2 ] = m;
  return `${isoToEnglish(y1, m1, d1)}–${isoToEnglish(y2, m2, d2)}`;
}

/** Convert ISO-like dates in free text to English dates and ranges. */
function humanizeDatesInText(out) {
  if (!out) return out;

  // 0) Timestamps: "YYYY-MM-DDTHH:MM:SS.sssZ" → "Month Dth YYYY"
  out = out.replace(
    /\b(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z\b/g,
    (_m, y, mo, d) => isoToEnglish(y, mo, d)
  );

  // 1) Range with ISO dates
  out = out.replace(
    /\b(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\s*(?:to|–|—|-)\s*(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b/g,
    (_m, y1, m1, d1, y2, m2, d2) => `${isoToEnglish(y1, m1, d1)}–${isoToEnglish(y2, m2, d2)}`
  );

  // 2) Single ISO dates
  out = out.replace(
    /\b(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b/g,
    (_m, y, m, d) => isoToEnglish(y, m, d)
  );

  // 3) Month tokens
  out = out.replace(
    /\b(\d{4})-(0[1-9]|1[0-2])(?!-\d)\b/g,
    (_m, y, m) => monthTokenToEnglish(y, m)
  );

  // 4) Drop leading zeros in English month-day forms
  out = out.replace(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s0([1-9])(\b|,)/g,
    (_m, mon, d, tail) => `${mon} ${d}${tail}`
  );

  return out;
}

/* --------------------------- DOLLAR SCALING --------------------------- */

function abbrevNumber(n) {
  const abs = Math.abs(n);
  if (abs >= 1e12) return `${(n/1e12).toFixed(2)}T`;
  if (abs >= 1e9)  return `${(n/1e9).toFixed(2)}B`;
  if (abs >= 1e6)  return `${(n/1e6).toFixed(2)}M`;
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

/** Scale big dollar amounts like $1,177,020,488.12 -> $1.18B */
function scaleDollars(out = '') {
  return out.replace(/\$\s?(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?\b/g, (_m, whole, dec = '') => {
    const num = Number((whole || '0').replace(/,/g, '') + (dec || ''));
    const abbr = abbrevNumber(num);
    if (/[MBT]$/.test(abbr)) return `$${abbr}`;
    const fixed = num.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 2 });
    return `$${fixed}`;
  });
}

/* ------------------------- PHRASE TIDY / POLISH ------------------------ */

function humanizeLagPhrases(out = '') {
  let s = out;
  s = s.replace(/\b(?:Day\s?1|lag1)\b/gi, 'one day later');
  s = s.replace(/\b(?:Day\s?2|lag2)\b/gi, 'two days later');
  return s;
}

function tightenNumbers(out) {
  if (!out) return out;
  out = out.replace(/(\d)\s+(\.)/g, '$1$2');
  out = out.replace(/\s+(\.)/g, '$1').replace(/\s+%/g, '%');
  out = out.replace(/\s+,/g, ',');
  out = out.replace(/%\s*%/g, '%');
  out = out.replace(/\s{2,}/g, ' ');
  return out.trim();
}

function prependDateRangeIfAny(question = '', answer = '') {
  const range = extractIsoRange(question);
  if (!range) return answer;
  if (/\b(January|February|March|April|May|June|July|August|September|October|November|December)\b/.test(answer)) {
    return answer;
  }
  return `During ${range}, ${answer.charAt(0).toLowerCase()}${answer.slice(1)}`;
}

function finalizeAnswer(text, questionForRange) {
  let out = String(text || '');
  out = humanizeDatesInText(out);
  out = scaleDollars(out);
  out = humanizeLagPhrases(out);
  out = tightenNumbers(out);
  out = prependDateRangeIfAny(questionForRange, out);
  return out;
}

/* ------------------------------ PROMPTS ------------------------------ */

const DEFAULT_DOC = buildSchemaDoc();

export function buildPrimaryPrompt(doc = DEFAULT_DOC) {
  return [
    {
      role: 'system',
      content:
`Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the whitelisted schema below.

Router rules:
• Choose exactly one table from the whitelist that best answers the question.
  – If the question mentions TVL / protocols / chains → prefer clean.protocol_chain_tvl_daily.
  – If it mentions lending supply/borrow/APYs → clean.lending_market_history.
  – If it mentions stablecoins by peg (USD/EUR/JPY) → clean.stablecoin_mcap_by_peg_daily.
  – If it mentions ETF flows → clean.etf_flows_daily.
• Use only columns that exist in the chosen table (see columns list).
• Time filters:
  – For date columns named ts (DATE) or day (DATE), filter with BETWEEN or equality as needed.
  – Always include an ORDER BY when returning time series.

General rules:
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed.
• Avoid non-portable extensions. Use standard Postgres only.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`
    }
  ];
}

export function buildRetryPrompt(question, previousSql, errMsg, doc = DEFAULT_DOC) {
  return [
    {
      role: 'system',
      content:
`Regenerate a SINGLE Postgres query as STRICT JSON {"sql":"..."} that avoids the failing construct.

Hard rules:
• Pick exactly one table from the whitelist that matches the question domain.
• Use only its listed columns.
• Keep to one statement (SELECT or WITH ... SELECT). No semicolons. No comments.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`
    },
    { role: 'user', content: `Original question:\n${question}` },
    { role: 'assistant', content: `Previous SQL:\n${previousSql}\n\nError:\n${errMsg}` }
  ];
}

/* ------------------------------ PLANNER ------------------------------ */

export function buildPlannerMessages(question, doc = DEFAULT_DOC) {
  return [
    {
      role: 'system',
      content:
`You are a query planner for a Postgres warehouse. Think about what data is needed and which single table to use, then output a STRICT JSON plan with ONE SQL.

Router rules:
• TVL / protocol-by-chain → clean.protocol_chain_tvl_daily (ts is DATE).
• Lending supply/borrow/APYs → clean.lending_market_history (ts is BIGINT epoch or timestamp; cast/date_trunc as needed).
• Stablecoin mcap by peg → clean.stablecoin_mcap_by_peg_daily (day is DATE).
• ETF flows → clean.etf_flows_daily (day is DATE).

General constraints:
• Use only columns that exist in the chosen table.
• For date filters, use BETWEEN on day/ts. Include ORDER BY for time series outputs.
• ONE statement only (SELECT or WITH ... SELECT). No semicolons. No comments.
• Keep SQL portable Postgres.

Return STRICT JSON with keys:
{
  "domain": "tvl" | "lending" | "stablecoins" | "etf",
  "reason": "<very short phrase>",
  "sql": "<ONE Postgres statement>",
  "presentation": {
    "style": "concise" | "bulleted" | "headline",
    "include_fields": ["..."],
    "notes": "<short rendering hints (units, percent formatting, date range text)>"
  }
}

Whitelisted schema:
${doc}`
    },
    { role: 'user', content: `Question: ${question}\nReturn ONLY the JSON plan as specified.` }
  ];
}

/* ------------------------------ LLM CALLS ------------------------------ */

export async function planQuery(openai, question, doc = DEFAULT_DOC) {
  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    response_format: { type: 'json_object' },
    messages: buildPlannerMessages(question, doc),
  });
  const text = resp.choices?.[0]?.message?.content || '{}';
  let plan;
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
  }
  if (!plan?.sql) throw new Error('Planner did not return SQL');
  return plan;
}

export function buildRetryPlanMessages(question, previousSql, errMsg, doc = DEFAULT_DOC) {
  return [
    {
      role: 'system',
      content:
`Regenerate a STRICT JSON plan with a SINGLE valid Postgres query that avoids the failing construct.

Hard rules:
• Pick exactly one table that fits the question (see whitelist).
• Use only columns in that table.
• ONE statement only; no semicolons; no comments.

Return JSON:
{ "domain": "...", "reason": "...", "sql": "...", "presentation": { ... } }

Whitelisted schema:
${doc}`
    },
    { role: 'user', content: `Original question:\n${question}` },
    { role: 'assistant', content: `Previous SQL:\n${previousSql}\n\nError:\n${errMsg}` }
  ];
}

export async function retryPlan(openai, question, previousSql, errMsg, doc = DEFAULT_DOC) {
  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    response_format: { type: 'json_object' },
    messages: buildRetryPlanMessages(question, previousSql, errMsg, doc),
  });
  const text = resp.choices?.[0]?.message?.content || '{}';
  let plan;
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
  }
  if (!plan?.sql) throw new Error('Retry planner did not return SQL');
  return plan;
}

/* ------------------------------ ANSWER ------------------------------ */

export async function generateAnswer(openai, question, rows, presentationHint) {
  if (!rows || rows.length === 0) {
    return 'No rows returned for that query.';
  }

  const style = presentationHint?.style || 'concise';
  const include = presentationHint?.include_fields || [];
  const notes = presentationHint?.notes || '';

  const sys = [
    'Write a concise financial/crypto analytics answer using ONLY fields provided.',
    'Answer in 1–2 sentences. Use only numbers from the rows.',
    'Format values in the 0–1 range as percentages with 2 decimals (e.g., 0.8368 → 83.68%).',
    'Format dates in plain English (e.g., "November 11th 2024" instead of "2024-11-11").',
    'If the user question mentions a time period (e.g., "in July 2025", "between July and August 2025"), restate it up front as plain English ("From July 2025 to August 2025, …").',
    'Prefer natural phrasing for lag effects (e.g., "one day later", "two days later").',
    'No tables.'
  ];
  if (style === 'bulleted') sys.push('If helpful, use at most 3 bullets.');
  if (notes) sys.push(`Notes: ${notes}`);

  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    messages: [
      { role: 'system', content: sys.join(' ') },
      { role: 'user', content: `Question: ${stripInvisibles(question)}` },
      {
        role: 'assistant',
        content:
          `Fields to emphasize: ${include.join(', ') || '(none specified)'}\n` +
          `Rows (JSON): ${JSON.stringify(rows).slice(0, 100000)}`
      }
    ]
  });

  const raw = resp.choices?.[0]?.message?.content || '';
  return finalizeAnswer(raw, question);
}

/* ------------------------------ EXPORT UTILS ------------------------------ */

export {
  stripInvisibles
};


