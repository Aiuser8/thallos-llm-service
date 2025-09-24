// lib/instructions.js — Registry-backed planner, prompts, and answer formatting.
// Uses config/llm_table_registry.json (or override via LLM_TABLE_REGISTRY_PATH).

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, "../config/llm_table_registry.json");

/* --------------------------- Registry / Doc loader --------------------------- */
// Cache the registry and schema docs in memory
let registryCache = null;
let fullSchemaDocCache = null;

async function loadRegistry() {
  if (registryCache) return registryCache;
  
  const registryPath = process.env.LLM_TABLE_REGISTRY_PATH || DEFAULT_REGISTRY_PATH;
  const raw = await fs.readFile(registryPath, "utf8");
  const json = JSON.parse(raw);
  if (!json || typeof json !== "object") {
    throw new Error("Invalid table registry JSON: root must be an object");
  }
  
  registryCache = json;
  return json;
}

export async function buildSchemaDoc() {
  if (fullSchemaDocCache) return fullSchemaDocCache;
  
  const registry = await loadRegistry();
  const lines = [];
  for (const [fqtn, meta] of Object.entries(registry)) {
    lines.push(`TABLE ${fqtn}`);
    if (meta.description) lines.push(meta.description);
    lines.push("  columns:");
    const cols = meta.columns || {};
    for (const [col, desc] of Object.entries(cols)) {
      lines.push(`    - ${col}: ${desc}`);
    }
    const pk = meta.primary_key || [];
    if (pk.length) lines.push(`  primary_key: [${pk.join(", ")}]`);
    lines.push("");
  }
  
  fullSchemaDocCache = lines.join("\n");
  return fullSchemaDocCache;
}

/* --------------------------- Question Scope Detection --------------------------- */
export async function isQuestionInDataScope(question) {
  const q = question.toLowerCase();
  
  // Keywords that indicate general knowledge questions (outside data scope)
  const generalKnowledgeKeywords = [
    'what is', 'what are', 'define', 'definition', 'explain', 'how does',
    'blockchain', 'bitcoin', 'ethereum', 'cryptocurrency', 'crypto',
    'defi', 'nft', 'web3', 'consensus', 'mining', 'staking',
    'smart contract', 'wallet', 'exchange', 'trading', 'market',
    'investment', 'portfolio', 'risk', 'volatility', 'regulation'
  ];
  
  // Keywords that indicate data-specific questions
  const dataSpecificKeywords = [
    'tvl', 'total value locked', 'liquidity', 'protocol',
    'bridge', 'volume', 'deposit', 'withdraw',
    'lending', 'borrow', 'supply', 'apy',
    'price', 'token price', 'usd', 'cost',
    'etf', 'flow', 'inflow', 'outflow',
    'stablecoin', 'peg', 'mcap',
    'pool', 'yield', 'farming',
    'holding', 'treasury', 'reserve'
  ];
  
  // Check if question contains general knowledge indicators
  const hasGeneralKnowledge = generalKnowledgeKeywords.some(keyword => 
    q.includes(keyword)
  );
  
  // Check if question contains data-specific indicators
  const hasDataSpecific = dataSpecificKeywords.some(keyword => 
    q.includes(keyword)
  );
  
  // If it's clearly general knowledge and not data-specific, route to general knowledge
  if (hasGeneralKnowledge && !hasDataSpecific) {
    return false;
  }
  
  // If it contains data-specific keywords, it's in scope
  if (hasDataSpecific) {
    return true;
  }
  
  // Default to in-scope for ambiguous cases
  return true;
}

export async function handleGeneralKnowledgeQuestion(openai, question) {
  const systemPrompt = `You are a knowledgeable assistant specializing in cryptocurrency, blockchain technology, and DeFi. 
Provide clear, accurate, and helpful answers to questions about:

- Blockchain technology and concepts
- Cryptocurrencies (Bitcoin, Ethereum, etc.)
- DeFi protocols and mechanisms
- Trading and investment concepts
- Technical explanations

Guidelines:
- Keep answers concise but informative (2-4 sentences)
- Use simple language when possible
- Provide context and examples when helpful
- If asked about specific data or metrics, mention that detailed data queries would need to be run against the database
- Focus on educational content rather than financial advice`;

  const resp = await openai.chat.completions.create({
    model: "gpt-4.1",
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: question },
    ],
  });

  return resp.choices?.[0]?.message?.content || "I'm sorry, I couldn't generate a response to that question.";
}

/* --------------------------- Smart Schema Filtering --------------------------- */
function extractKeywords(question) {
  const q = question.toLowerCase();
  const keywords = [];
  
  // TVL related
  if (q.includes('tvl') || q.includes('total value locked') || q.includes('liquidity')) {
    keywords.push('tvl', 'liquidity', 'protocol');
  }
  
  // Bridge related
  if (q.includes('bridge') || q.includes('cross-chain') || q.includes('deposit') || q.includes('withdraw')) {
    keywords.push('bridge', 'volume', 'deposit', 'withdraw');
  }
  
  // Price related
  if (q.includes('price') || q.includes('token price') || q.includes('usd') || q.includes('cost')) {
    keywords.push('price', 'token', 'usd');
  }
  
  // Lending related
  if (q.includes('lending') || q.includes('borrow') || q.includes('supply') || q.includes('apy')) {
    keywords.push('lending', 'borrow', 'supply', 'apy', 'market');
  }
  
  // ETF related
  if (q.includes('etf') || q.includes('flow') || q.includes('inflow') || q.includes('outflow')) {
    keywords.push('etf', 'flow');
  }
  
  // Stablecoin related
  if (q.includes('stablecoin') || q.includes('peg') || q.includes('mcap')) {
    keywords.push('stablecoin', 'peg', 'mcap');
  }
  
  // Pool related
  if (q.includes('pool') || q.includes('yield') || q.includes('farming')) {
    keywords.push('pool', 'yield', 'tvl');
  }
  
  // Holdings related
  if (q.includes('holding') || q.includes('treasury') || q.includes('reserve')) {
    keywords.push('holding', 'treasury', 'reserve', 'token');
  }
  
  return keywords;
}

function getRelevantTables(registry, keywords) {
  const relevantTables = new Set();
  
  for (const [fqtn, meta] of Object.entries(registry)) {
    const tableName = fqtn.toLowerCase();
    const description = (meta.description || '').toLowerCase();
    const columns = Object.keys(meta.columns || {}).map(c => c.toLowerCase());
    
    // Check if any keyword matches table name, description, or columns
    const isRelevant = keywords.some(keyword => 
      tableName.includes(keyword) || 
      description.includes(keyword) ||
      columns.some(col => col.includes(keyword))
    );
    
    if (isRelevant) {
      relevantTables.add(fqtn);
    }
  }
  
  // If no specific tables found, include common tables that are likely needed
  if (relevantTables.size === 0) {
    // Include the most commonly used tables
    relevantTables.add('clean.protocol_chain_tvl_daily');
    relevantTables.add('clean.token_price_daily_enriched');
  }
  
  return Array.from(relevantTables);
}

export async function buildFilteredSchemaDoc(question) {
  const registry = await loadRegistry();
  const keywords = extractKeywords(question);
  const relevantTables = getRelevantTables(registry, keywords);
  
  const lines = [];
  for (const fqtn of relevantTables) {
    const meta = registry[fqtn];
    if (!meta) continue;
    
    lines.push(`TABLE ${fqtn}`);
    if (meta.description) lines.push(meta.description);
    lines.push("  columns:");
    const cols = meta.columns || {};
    for (const [col, desc] of Object.entries(cols)) {
      lines.push(`    - ${col}: ${desc}`);
    }
    const pk = meta.primary_key || [];
    if (pk.length) lines.push(`  primary_key: [${pk.join(", ")}]`);
    lines.push("");
  }
  
  return lines.join("\n");
}

/* ----------------------------- Text helpers ----------------------------- */
export const stripInvisibles = (s = "") => s.replace(/[\u00ad\u200b\u200c\u200d\u2060]/g, "");

function monthTokenToEnglish(yyyy, mm) {
  const y = Number(yyyy);
  const m = Number(mm);
  const date = new Date(Date.UTC(y, m - 1, 1));
  const month = date.toLocaleString("en-US", { month: "long", timeZone: "UTC" });
  return `${month} ${y}`;
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
  if (abs >= 1e12) return `${(n / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function scaleDollars(out = "") {
  return out.replace(/\$\s?(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?\b/g, (_m, whole, dec = "") => {
    const num = Number((whole || "0").replace(/,/g, "") + (dec || ""));
    const abbr = abbrevNumber(num);
    if (/[MBT]$/.test(abbr)) return `$${abbr}`;
    return `$${num.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
  });
}

function tightenNumbers(out) {
  if (!out) return out;
  return out.replace(/\s+%/g, "%").replace(/\s+,/g, ",").trim();
}

function finalizeAnswer(text) {
  let out = String(text || "");
  out = humanizeDatesInText(out);
  out = scaleDollars(out);
  out = tightenNumbers(out);
  return out;
}

/* ------------------------------ PROMPTS ------------------------------ */
export async function buildPrimaryPrompt(doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  return [
    {
      role: "system",
      content: `Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the whitelisted schema below.

Global rules:
• String filters on free-text identifiers (e.g., protocol, project, chain, symbol) MUST be case-insensitive.
  – Prefer ILIKE with a suitable pattern (e.g., chain ILIKE 'ethereum').
  – If equality is used, normalize both sides: LOWER(column) = LOWER('value').
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed. No comments.
• Always add ORDER BY when returning time series results.
• Use only tables/columns listed below.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`,
    },
  ];
}

export async function buildRetryPrompt(question, previousSql, errMsg, doc = null) {
  if (!doc) doc = await buildSchemaDoc();
  return [
    {
      role: "system",
      content: `Regenerate a SINGLE Postgres query as STRICT JSON {"sql":"..."} that avoids the failing construct.
Use only the whitelisted schema below.

Global rules:
• String filters on free-text identifiers (e.g., protocol, project, chain, symbol) MUST be case-insensitive.
  – Prefer ILIKE with a suitable pattern (e.g., chain ILIKE 'ethereum').
  – If equality is used, normalize both sides: LOWER(column) = LOWER('value').
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed. No comments.
• Always add ORDER BY when returning time series results.
• Use only tables/columns listed below.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`,
    },
    { role: "user", content: `Original question:\n${question}` },
    { role: "assistant", content: `Previous SQL:\n${previousSql}\n\nError:\n${errMsg}` },
  ];
}

export async function buildPlannerMessages(question, doc = null) {
  if (!doc) doc = await buildFilteredSchemaDoc(question);
  return [
    {
      role: "system",
      content: `You are a query planner for a Postgres warehouse. Output a STRICT JSON plan with ONE SQL.
Use only the whitelisted schema below.

Global rules:
• String filters on free-text identifiers (e.g., protocol, project, chain, symbol) MUST be case-insensitive.
  – Prefer ILIKE with a suitable pattern (e.g., chain ILIKE 'ethereum').
  – If equality is used, normalize both sides: LOWER(column) = LOWER('value').
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed. No comments.
• Always add ORDER BY when returning time series results.
• Use only tables/columns listed below.
• Return STRICT JSON only.

Whitelisted schema:
${doc}`,
    },
    { role: "user", content: `Question: ${question}\nReturn ONLY the JSON plan as specified.` },
  ];
}

/* ------------------------------ LLM calls ------------------------------ */
export async function planQuery(openai, question, doc = null) {
  if (!doc) doc = await buildFilteredSchemaDoc(question);
  const resp = await openai.chat.completions.create({
    model: "gpt-4.1",
    response_format: { type: "json_object" },
    messages: await buildPlannerMessages(question, doc),
  });
  const text = resp.choices?.[0]?.message?.content || "{}";
  let plan;
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
  }
  
  // Handle both "sql" and "query" field names
  if (plan?.sql) {
    // Already has sql field, keep as is
  } else if (plan?.query) {
    // Convert query field to sql field
    plan.sql = plan.query;
  } else {
    throw new Error("Planner did not return SQL");
  }
  return plan;
}

export async function retryPlan(openai, question, previousSql, errMsg, doc = null) {
  if (!doc) doc = await buildSchemaDoc(); // Use full schema for retries to be safe
  const resp = await openai.chat.completions.create({
    model: "gpt-4.1",
    response_format: { type: "json_object" },
    messages: await buildRetryPrompt(question, previousSql, errMsg, doc),
  });
  const text = resp.choices?.[0]?.message?.content || "{}";
  let plan;
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
  }
  
  // Handle both "sql" and "query" field names
  if (plan?.sql) {
    // Already has sql field, keep as is
  } else if (plan?.query) {
    // Convert query field to sql field
    plan.sql = plan.query;
  } else {
    throw new Error("Retry planner did not return SQL");
  }
  return plan;
}

/* ------------------------------ Combined LLM call ------------------------------ */
export async function generateAnswerFromResults(openai, question, rows, presentationHint) {
  if (!rows || rows.length === 0) return "No rows returned for that query.";

  const style = presentationHint?.style || "concise";
  const include = presentationHint?.include_fields || [];

  const systemPrompt = `You are a financial/crypto analytics expert. Write a concise answer using ONLY the provided query results.

Requirements:
• Answer in 1–2 sentences. Use only numbers from the rows.
• Format values in the 0–1 range as percentages with 2 decimals.
• Format dates in plain English.
• Prefer natural phrasing.
• No tables.${style === "bulleted" ? " If helpful, use at most 3 bullets." : ""}`;

  const resp = await openai.chat.completions.create({
    model: "gpt-4.1",
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: `Question: ${stripInvisibles(question)}\n\nQuery Results (JSON): ${JSON.stringify(rows).slice(0, 100000)}` },
    ],
  });

  const raw = resp.choices?.[0]?.message?.content || "";
  return finalizeAnswer(raw);
}

/* ------------------------------ Answer stage ------------------------------ */
export async function generateAnswer(openai, question, rows, presentationHint) {
  if (!rows || rows.length === 0) return "No rows returned for that query.";

  const style = presentationHint?.style || "concise";
  const include = presentationHint?.include_fields || [];

  const sys = [
    "Write a concise financial/crypto analytics answer using ONLY fields provided.",
    "Answer in 1–2 sentences. Use only numbers from the rows.",
    "Format values in the 0–1 range as percentages with 2 decimals.",
    "Format dates in plain English.",
    "Prefer natural phrasing.",
    "No tables.",
  ];
  if (style === "bulleted") sys.push("If helpful, use at most 3 bullets.");

  const resp = await openai.chat.completions.create({
    model: "gpt-4.1",
    messages: [
      { role: "system", content: sys.join(" ") },
      { role: "user", content: `Question: ${stripInvisibles(question)}` },
      {
        role: "assistant",
        content:
          `Fields to emphasize: ${include.join(", ") || "(none specified)"}\n` +
          `Rows (JSON): ${JSON.stringify(rows).slice(0, 100000)}`,
      },
    ],
  });

  const raw = resp.choices?.[0]?.message?.content || "";
  return finalizeAnswer(raw);
}