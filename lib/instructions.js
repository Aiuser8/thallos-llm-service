// lib/instructions.js — Registry-backed planner, prompts, and answer formatting.
// Uses config/llm_table_registry.json (or override via LLM_TABLE_REGISTRY_PATH).

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, '../config/llm_table_registry.json');

/* -------------------------- LOAD REGISTRY -------------------------- */
async function loadRegistry() {
  const registryPath = process.env.LLM_TABLE_REGISTRY_PATH || DEFAULT_REGISTRY_PATH;
  const raw = await fs.readFile(registryPath, 'utf8');
  const json = JSON.parse(raw);
  if (!json || typeof json !== 'object') {
    throw new Error('Invalid table registry JSON: root must be an object');
  }
  return json;
}

/* -------------------------- BUILD DOC FOR PROMPT -------------------------- */
export async function buildSchemaDoc() {
  const registry = await loadRegistry();
  const lines = [];
  for (const [fqtn, meta] of Object.entries(registry)) {
    lines.push(`TABLE ${fqtn}`);
    if (meta.description) lines.push(meta.description);

    // Columns
    lines.push('  columns:');
    for (const [col, desc] of Object.entries(meta.columns || {})) {
      lines.push(`    - ${col}: ${desc}`);
    }

    // PK
    if (Array.isArray(meta.primary_key) && meta.primary_key.length) {
      lines.push(`  primary_key: [${meta.primary_key.join(', ')}]`);
    }

    // Aliases
    if (meta.aliases && typeof meta.aliases === 'object') {
      for (const [col, mapping] of Object.entries(meta.aliases)) {
        lines.push(`  aliases for ${col}: ${JSON.stringify(mapping)}`);
      }
    }

    lines.push('');
  }
  return lines.join('\n');
}

/* ----------------------------- DATE + FORMAT HELPERS ----------------------------- */
const stripInvisibles = (s = '') => s.replace(/[\u00ad\u200b\u200c\u200d\u2060]/g, '');

function monthTokenToEnglish(yyyy, mm) {
  const y = Number(yyyy);
  const m = Number(mm);
  const date = new Date(Date.UTC(y, m - 1, 1));
  return date.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' }) + ` ${y}`;
}

function humanizeDatesInText(out) {
  if (!out) return out;
  return out.replace(
    /\b(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b/g,
    (_m, y, m, d) => `${monthTokenToEnglish(y, m)} ${Number(d)}, ${y}`
  );
}

function abbrevNumber(n) {
  const abs = Math.abs(n);
  if (abs >= 1e12) return `${(n/1e12).toFixed(2)}T`;
  if (abs >= 1e9)  return `${(n/1e9).toFixed(2)}B`;
  if (abs >= 1e6)  return `${(n/1e6).toFixed(2)}M`;
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function scaleDollars(out = '') {
  return out.replace(/\$\s?(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?\b/g, (_m, whole, dec = '') => {
    const num = Number((whole || '0').replace(/,/g, '') + (dec || ''));
    const abbr = abbrevNumber(num);
    return /[MBT]$/.test(abbr) ? `$${abbr}` :
      `$${num.toLocaleString('en-US', { maximumFractionDigits: 2 })}`;
  });
}

function tightenNumbers(out) {
  if (!out) return out;
  return out.replace(/\s+%/g, '%').replace(/\s+,/g, ',').trim();
}

function finalizeAnswer(text) {
  let out = String(text || '');
  out = humanizeDatesInText(out);
  out = scaleDollars(out);
  out = tightenNumbers(out);
  return out;
}

/* ------------------------------ PROMPTS ------------------------------ */
export async function buildPrimaryPrompt(doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  return [{
    role: 'system',
    content:
`Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the whitelisted schema below.

Rules:
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed.
• Use only tables/columns listed below.
• Always add ORDER BY when returning time series.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`
  }];
}

export async function buildRetryPrompt(question, previousSql, errMsg, doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  return [
    {
      role: 'system',
      content:
`Regenerate a SINGLE Postgres query as STRICT JSON {"sql":"..."} that avoids the failing construct.
Use only the whitelisted schema below.

Whitelisted schema:
${doc}`
    },
    { role: 'user', content: `Original question:\n${question}` },
    { role: 'assistant', content: `Previous SQL:\n${previousSql}\n\nError:\n${errMsg}` }
  ];
}

export async function buildPlannerMessages(question, doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  return [
    {
      role: 'system',
      content:
`You are a query planner for a Postgres warehouse. Output a STRICT JSON plan with ONE SQL.
Use only the whitelisted schema below.

Whitelisted schema:
${doc}`
    },
    { role: 'user', content: `Question: ${question}\nReturn ONLY the JSON plan as specified.` }
  ];
}

/* ------------------------------ PLANNER ------------------------------ */
export async function planQuery(openai, question, doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    response_format: { type: 'json_object' },
    messages: await buildPlannerMessages(question, doc),
  });
  const text = resp.choices?.[0]?.message?.content || '{}';
  let plan;
  try { plan = JSON.parse(text); }
  catch { const m = text.match(/\{[\s\S]*\}/); if (m) plan = JSON.parse(m[0]); }
  if (!plan?.sql) throw new Error('Planner did not return SQL');
  return plan;
}

export async function retryPlan(openai, question, previousSql, errMsg, doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    response_format: { type: 'json_object' },
    messages: await buildRetryPrompt(question, previousSql, errMsg, doc),
  });
  const text = resp.choices?.[0]?.message?.content || '{}';
  let plan;
  try { plan = JSON.parse(text); }
  catch { const m = text.match(/\{[\s\S]*\}/); if (m) plan = JSON.parse(m[0]); }
  if (!plan?.sql) throw new Error('Retry planner did not return SQL');
  return plan;
}

/* ------------------------------ ANSWER ------------------------------ */
export async function generateAnswer(openai, question, rows, presentationHint) {
  if (!rows || rows.length === 0) return 'No rows returned for that query.';

  const style = presentationHint?.style || 'concise';
  const include = presentationHint?.include_fields || [];

  const sys = [
    'Write a concise financial/crypto analytics answer using ONLY fields provided.',
    'Answer in 1–2 sentences. Use only numbers from the rows.',
    'Format values in 0–1 as percentages with 2 decimals.',
    'Format dates in plain English.',
    'Prefer natural phrasing.',
    'No tables.'
  ];
  if (style === 'bulleted') sys.push('If helpful, use at most 3 bullets.');

  const resp = await openai.chat.completions.create({
    model: 'gpt-5',
    messages: [
      { role: 'system', content: sys.join(' ') },
      { role: 'user', content: `Question: ${stripInvisibles(question)}` },
      {
        role: 'assistant',
        content:
          `Fields to emphasize: ${include.join(', ') || '(none)'}\n` +
          `Rows (JSON): ${JSON.stringify(rows).slice(0, 100000)}`
      }
    ]
  });

  const raw = resp.choices?.[0]?.message?.content || '';
  return finalizeAnswer(raw);
}

/* ------------------------------ EXPORT UTILS ------------------------------ */
export { stripInvisibles };