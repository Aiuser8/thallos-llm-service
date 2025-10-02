// lib/instructions.js — Registry-backed planner, prompts, and answer formatting.
// Uses config/llm_table_registry.json (or override via LLM_TABLE_REGISTRY_PATH).

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, "../config/llm_table_registry.json");

/* --------------------------- Registry / Doc loader --------------------------- */
// Cache the registry and schema docs in memory - ENABLED FOR PRODUCTION
let registryCache = null;
let fullSchemaDocCache = null;
let filteredSchemaCache = {}; // Separate cache for filtered schemas

async function loadRegistry() {
  if (registryCache) return registryCache; // ENABLED FOR PRODUCTION
  
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
  // if (fullSchemaDocCache) return fullSchemaDocCache; // DISABLED FOR TESTING
  
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
  
  // META QUESTIONS ABOUT SERVICE CAPABILITIES (handle specially)
  const metaQuestionPatterns = [
    /what\s+(type|kind|sort).*questions.*ask/,
    /what\s+can\s+(you|i)\s+(ask|query)/,
    /what.*can.*you.*answer/,
    /what.*do.*you.*know/,
    /what.*questions.*answer/,
    /help.*what.*ask/,
  ];
  
  if (metaQuestionPatterns.some(pattern => pattern.test(q))) {
    return 'meta'; // Special handling for meta questions
  }
  
  // EXPLICIT GENERAL KNOWLEDGE PATTERNS (highest priority - route to general knowledge)
  const explicitGeneralPatterns = [
    /should\s+i\s+(buy|sell|invest|trade)/,
    /what\s+(is|are)\s+(blockchain|cryptocurrency|bitcoin|ethereum|defi|smart\s+contract)/,
    /how\s+does\s+(blockchain|cryptocurrency|bitcoin|ethereum|defi|smart\s+contract)/,
    /investment\s+advice/,
    /financial\s+advice/,
    /risk\s+tolerance.*strateg/,
    /recommend.*strateg/,
    /optimal\s+allocation.*given/,
    /based\s+on\s+my\s+risk/,
    /what.*should.*do/,
    /advice.*invest/,
    /construct.*optimal.*allocation/,
    /targeting.*sharpe/,
    /portfolio.*optim/
  ];
  
  // Check explicit general knowledge patterns first
  if (explicitGeneralPatterns.some(pattern => pattern.test(q))) {
    return false; // Route to general knowledge
  }
  
  // Keywords that indicate general knowledge questions (outside data scope)
  const generalKnowledgeKeywords = [
    'define', 'definition', 'explain concept', 'how does consensus',
    'web3', 'consensus mechanism', 'mining algorithm', 'validator',
    'smart contract security', 'wallet security', 'regulation',
    'investment strategy', 'portfolio theory', 'risk management'
  ];
  
  // Keywords that indicate data-specific questions
  const dataSpecificKeywords = [
    // LENDING MARKETS
    'lending', 'lent', 'borrow', 'supply', 'apy', 'apr', 'yield', 'rate', 'interest',
    'aave', 'compound', 'utilization', 'lending rate', 'borrow rate', 'supply rate',
    // LIQUIDITY POOLS  
    'pool', 'liquidity', 'farming', 'yield farming', 'lp token', 'pool apy',
    'uniswap', 'curve', 'balancer', 'aerodrome', 'velodrome', 'sushiswap',
    // TOKEN PRICES
    'price', 'token price', 'usd', 'cost', 'worth', 'value',
    'bitcoin', 'btc', 'ethereum', 'eth', 'usdc', 'usdt', 'dai', 'weth', 'wbtc', 'steth',
    // OPPORTUNITIES & COMPARISONS
    'opportunity', 'opportunities', 'arbitrage', 'best rate', 'highest', 'optimal',
    'compare', 'comparison', 'where can i get', 'maximize yield', 'rate difference'
  ];
  
  // Check if question contains general knowledge indicators
  const hasGeneralKnowledge = generalKnowledgeKeywords.some(keyword => 
    q.includes(keyword)
  );
  
  // Check if question contains data-specific indicators
  const hasDataSpecific = dataSpecificKeywords.some(keyword => 
    q.includes(keyword)
  );
  
  // If it contains data-specific keywords, it's always in scope (overrides general knowledge)
  if (hasDataSpecific) {
    return true;
  }
  
  // If it's clearly general knowledge and not data-specific, route to general knowledge
  if (hasGeneralKnowledge && !hasDataSpecific) {
    return false;
  }
  
  // Default to in-scope for ambiguous cases
  return true;
}


export function detectQueryIntent(question) {
  const lowerQuestion = question.toLowerCase();
  
  // LENDING OPPORTUNITIES
  if (lowerQuestion.includes('lending') && (lowerQuestion.includes('opportunity') || 
      lowerQuestion.includes('opportunities') || lowerQuestion.includes('where') ||
      lowerQuestion.includes('best rate') || lowerQuestion.includes('highest') ||
      lowerQuestion.includes('arbitrage'))) {
    return 'lending_opportunities';
  }
  
  // LIQUIDITY POOL QUERIES
  if (lowerQuestion.includes('pool') || lowerQuestion.includes('liquidity') ||
      lowerQuestion.includes('apy') && (lowerQuestion.includes('weth') || 
      lowerQuestion.includes('usdc') || lowerQuestion.includes('eth'))) {
    return 'pool_analysis';
  }
  
  // TOKEN PRICE QUERIES
  if (lowerQuestion.includes('price') || lowerQuestion.includes('cost') ||
      lowerQuestion.includes('worth') || lowerQuestion.includes('value')) {
    return 'price_query';
  }
  
  // GENERAL PREDICTIONS/ADVICE (route to general knowledge)
  if (lowerQuestion.includes('should i') || lowerQuestion.includes('recommend') ||
      lowerQuestion.includes('advice') || lowerQuestion.includes('predict') ||
      lowerQuestion.includes('forecast') || lowerQuestion.includes('expect')) {
    return 'general_prediction';
  }
  
  return 'standard_query';
}


export async function handleGeneralKnowledgeQuestion(openai, question) {
  const systemPrompt = `You are a cryptocurrency and DeFi expert. Provide clear, educational answers about:
• Blockchain technology and concepts
• Cryptocurrencies (Bitcoin, Ethereum, etc.)
• DeFi protocols and mechanisms
• Trading and investment concepts

Guidelines:
• Keep answers concise (2-4 sentences)
• Use simple language
• Provide examples when helpful
• For specific data/metrics: suggest asking data-specific questions
• Focus on education, not financial advice`;

  try {
    const resp = await openai.chat.completions.create({
      model: "gpt-4.1",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: question },
      ],
    });

    return resp.choices?.[0]?.message?.content || "I'm sorry, I couldn't generate a response to that question.";
  } catch (error) {
    console.error('OpenAI API Error in handleGeneralKnowledgeQuestion:', {
      status: error.status,
      code: error.code,
      type: error.type,
      message: error.message,
      headers: error.headers,
      request_id: error.request_id
    });
    throw error;
  }
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
  
  
  // NEW: Sector/narrative related
  if (q.includes('sector') || q.includes('narrative') || q.includes('ai') || q.includes('gaming') || q.includes('meme') || q.includes('performance')) {
    keywords.push('narrative', 'sector', 'performance');
  }
  
  return keywords;
}

function getRelevantTables(registry, keywords, question = '') {
  const relevantTables = new Set();
  
  // 🚨 SMART SCHEMA SELECTION - Allow both schemas but guide usage
  const isHistoricalQuery = keywords.some(k => 
    ['historical', 'history', 'trend', 'past', 'over time', 'since', 'months', 'years'].includes(k.toLowerCase())
  );
  
  const needsBothSchemas = question.toLowerCase().includes('compare') || 
                          question.toLowerCase().includes('vs') ||
                          question.toLowerCase().includes('historical vs current') ||
                          question.toLowerCase().includes('trend') ||
                          keywords.some(k => ['correlation', 'comparison', 'vs', 'versus'].includes(k.toLowerCase()));
  
  // Determine preferred schema (used for fallback if no tables found)
  const preferredSchema = isHistoricalQuery ? 'clean' : 'update';
  
  // DEBUG: Log schema selection
  console.log(`🔍 Schema Selection Debug:`, {
    question: question?.slice(0, 50),
    keywords: keywords.slice(0, 5),
    isHistoricalQuery,
    needsBothSchemas,
    strategy: needsBothSchemas ? 'BOTH_SCHEMAS' : (isHistoricalQuery ? 'CLEAN_ONLY' : 'UPDATE_ONLY')
  });
  
  for (const [fqtn, meta] of Object.entries(registry)) {
    const [schema] = fqtn.split('.');
    
    // Smart schema filtering
    if (!needsBothSchemas) {
      // Single schema mode - prevent mixing
      if (schema !== preferredSchema) continue;
    }
    // If needsBothSchemas is true, include all relevant tables from both schemas
    
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
  
  // If no specific tables found, include common tables from preferred schema only
  if (relevantTables.size === 0) {
    if (preferredSchema === 'update') {
      relevantTables.add('update.cl_pool_hist');
      relevantTables.add('update.lending_market_history');
      relevantTables.add('update.token_price_daily');
    } else {
      relevantTables.add('clean.cl_pool_hist');
      relevantTables.add('clean.lending_market_history');
      relevantTables.add('clean.token_price_daily_enriched');
    }
  }
  
  const finalTables = Array.from(relevantTables);
  
  // DEBUG: Log final table selection
  const updateTables = finalTables.filter(t => t.startsWith('update.')).length;
  const cleanTables = finalTables.filter(t => t.startsWith('clean.')).length;
  
  console.log(`📊 Final Tables Selected:`, {
    strategy: needsBothSchemas ? 'BOTH_SCHEMAS' : (isHistoricalQuery ? 'CLEAN_ONLY' : 'UPDATE_ONLY'),
    updateTables,
    cleanTables,
    totalTables: finalTables.length,
    tables: finalTables.slice(0, 5) // Show first 5 for debugging
  });
  
  return finalTables;
}

export async function buildFilteredSchemaDoc(question) {
  // Check cache first
  const cacheKey = `schema_${question.toLowerCase().slice(0, 50)}`;
  if (filteredSchemaCache[cacheKey]) {
    return filteredSchemaCache[cacheKey];
  }
  
  const registry = await loadRegistry();
  const keywords = extractKeywords(question);
  const relevantTables = getRelevantTables(registry, keywords, question);
  
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
  
  const result = lines.join("\n");
  
  // Cache the result
  filteredSchemaCache[cacheKey] = result;
  
  return result;
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
      content: `You are a DeFi data analyst. Generate a SINGLE Postgres query as STRICT JSON {"sql":"..."}.

=== CORE RULES ===
1. Return STRICT JSON only: {"sql":"..."}
2. ⚠️ TIMESTAMP FILTER MANDATORY: EVERY query MUST include timestamp filter - ts >= (SELECT MAX(ts) - 21600 FROM table_name)
3. ⚠️ LIMIT 100 MANDATORY: EVERY query MUST use LIMIT 100 (NOT 5, NOT 10, NOT 50 - exactly 100)
4. ONE statement total (SELECT or WITH ... SELECT). No semicolons, no comments.
5. Use only tables/columns from the schema below
6. Always include ORDER BY for time series results
7. String filters MUST be case-insensitive (use ILIKE or LOWER())
8. SINGLE ASSET PRICES: When user asks for ONE token price, query ONLY that token - use symbol = 'BTC' NOT symbol IN ('BTC', 'WBTC')
9. NO json_build_object - return columns directly in SELECT

BEFORE RETURNING YOUR QUERY, VALIDATE IT HAS:
✅ WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.table_name) as FIRST condition
✅ LIMIT 100 at the end
✅ 'chain' and 'project' in SELECT for pool/lending queries
✅ No semicolons, no json_build_object
If ANY checklist item is missing, FIX IT before returning.

=== SCHEMA SELECTION (CRITICAL) ===
🎯 DEFAULT: Use update.* tables for ALL queries unless explicitly asked for historical analysis
🎯 VAGUE QUERIES: Always use update.* (recent data) - only use clean.* if user says "historical", "past", "trend over time"
🎯 NEVER MIX SCHEMAS: Use ONLY update.* OR ONLY clean.* tables in a single query
🎯 NEVER USE UNION/UNION ALL: Timestamp type incompatibility between schemas will cause errors

update.* tables: BIGINT timestamps (use integer arithmetic: ts >= MAX(ts) - 21600 for 6 hours)
clean.* tables: BIGINT timestamps (use integer arithmetic: ts >= MAX(ts) - 21600)

Examples:
• "What are good lending opportunities?" → update.* (vague = recent)
• "Show me ETH pools" → update.* (vague = recent)
• "Historical ETH lending rates" → clean.* (explicitly historical)

=== MANDATORY QUERY TEMPLATE (COPY THIS PATTERN) ===

For "best pools" queries, use EXACTLY this structure:

WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.cl_pool_hist
),
latest_pools AS (
  SELECT DISTINCT ON (pool_id)
    pool_id, symbol, project, chain, apy, tvl_usd, ts
  FROM update.cl_pool_hist, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy IS NOT NULL AND tvl_usd IS NOT NULL AND tvl_usd > 1000000
  ORDER BY pool_id, ts DESC
)
SELECT * FROM latest_pools ORDER BY apy DESC, tvl_usd DESC LIMIT 100

For "best lending" queries, use EXACTLY this structure:

WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.lending_market_history
),
latest_data AS (
  SELECT DISTINCT ON (symbol, project, chain)
    symbol, project, chain, 
    (apy_base_supply + COALESCE(apy_reward_supply, 0)) as total_apy,
    total_supply_usd, ts
  FROM update.lending_market_history, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy_base_supply > 0 AND total_supply_usd > 1000000
  ORDER BY symbol, project, chain, ts DESC
)
SELECT * FROM latest_data ORDER BY total_apy DESC LIMIT 100

🚨 CRITICAL: ALWAYS use DISTINCT ON to avoid duplicates at different timestamps!
Without it, your 100 rows might be the same 5 pools/protocols repeated 20 times each!

🚨 CRITICAL CHECKLIST - EVERY QUERY MUST HAVE:
✅ DISTINCT ON to get one row per unique pool/market combination
✅ Timestamp filter as FIRST condition in WHERE clause  
✅ LIMIT 100 (never 5, 10, or 50)
✅ Both 'chain' and 'project' in SELECT
✅ ORDER BY with primary sort column first

=== DATA SOURCES ===

LIVE DATA (update.* - use for current queries):
• update.token_price_daily: Real-time prices (5-min updates, 585K records)
  - Filter: price_timestamp >= (SELECT MAX(price_timestamp) - INTERVAL '10 minutes' FROM update.token_price_daily)
  - DO NOT filter by confidence (many valid prices have NULL confidence values)
• update.lending_market_history: Live lending rates (30-min updates, 63K records)
  - Filter: ts >= (SELECT MAX(ts) - 21600 FROM update.lending_market_history)
  - Default: project = 'aave-v3' unless user specifies otherwise
• update.cl_pool_hist: Live pool APYs (hourly updates, 1M+ records)
  - Filter: ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist)
• update.protocol_chain_tvl_daily: Protocol TVL rankings (daily updates)
• update.protocol_fees_daily: Protocol revenue (daily updates, 2673 records)
• update.dex_info: DEX volumes (12-hour updates, 885 DEXs)
• update.perp_funding_rates: Funding rates (hourly updates, 5398 records)
• update.narratives: Sector performance (daily updates, 23+ sectors)

HISTORICAL DATA (clean.* - use only when explicitly requested):
• clean.token_price_daily_enriched: Price history (2.3M records)
• clean.lending_market_history: Lending history (685K records)
• clean.cl_pool_hist: Pool history (7.8M records)
• clean.protocol_chain_tvl_daily: TVL history (12M+ records)

=== QUERY PATTERNS ===

PROTOCOL TVL RANKINGS (MANDATORY PATTERN):
SELECT protocol_name, total_liquidity_usd AS tvl, ts, category
FROM update.protocol_chain_tvl_daily 
WHERE ts = (SELECT MAX(ts) FROM update.protocol_chain_tvl_daily) 
  AND total_liquidity_usd IS NOT NULL
  AND category != 'CEX'
ORDER BY total_liquidity_usd DESC 
LIMIT 10

❌ WRONG: Never use SUM() or GROUP BY without date filter (causes inflated values)
❌ WRONG: Never aggregate across all historical dates

LENDING OPPORTUNITIES (COMPREHENSIVE CROSS-CHAIN):
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.lending_market_history
),
latest_data AS (
  SELECT DISTINCT ON (symbol, project, chain)
    symbol, project, chain, 
  (apy_base_supply + COALESCE(apy_reward_supply, 0)) as total_apy,
  total_supply_usd, ts,
  (total_supply_usd / 1000000) * (apy_base_supply + COALESCE(apy_reward_supply, 0)) as quality_score
  FROM update.lending_market_history, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
  AND apy_base_supply > 0
  AND total_supply_usd > 1000000
  ORDER BY symbol, project, chain, ts DESC
)
SELECT * FROM latest_data
ORDER BY quality_score DESC
LIMIT 100

🎯 CRITICAL: Use DISTINCT ON to avoid duplicate protocols at different timestamps!
This pattern: (1) Gets most recent data for each unique (symbol, project, chain), (2) Then orders by quality_score
Without DISTINCT ON, you'll see "fluid-lending" 50 times in 100 rows = looks like only 2 protocols exist!

🚨🚨🚨 ABSOLUTE MANDATORY RULES - NO EXCEPTIONS EVER 🚨🚨🚨
1. TIMESTAMP FILTER: WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.lending_market_history) - FIRST LINE!
   ❌ WRONG: WHERE apy_base_supply > 0 (MISSING TIMESTAMP = RETURNS STALE DATA!)
   ✅ RIGHT: WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.lending_market_history) AND apy_base_supply > 0
2. LIMIT 100: Always use LIMIT 100 (NOT 5, NOT 10, NOT 50 - exactly 100)
3. ALWAYS include 'chain' and 'project' in SELECT to show cross-platform opportunities
4. DO NOT filter by protocol UNLESS user specifically asks - search ALL protocols
5. DO NOT filter by chain UNLESS user specifies - search ALL chains

LIQUIDITY POOL YIELDS (COMPREHENSIVE CROSS-PLATFORM):
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.cl_pool_hist
),
latest_pools AS (
  SELECT DISTINCT ON (pool_id)
    pool_id, symbol, project, chain, apy, tvl_usd, ts
  FROM update.cl_pool_hist, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
  AND apy IS NOT NULL 
  AND tvl_usd IS NOT NULL
  AND tvl_usd > 1000000
  ORDER BY pool_id, ts DESC
)
SELECT * FROM latest_pools
ORDER BY apy DESC, tvl_usd DESC
LIMIT 100

🎯 CRITICAL: Use DISTINCT ON (pool_id) to avoid seeing same pool 20 times at different timestamps!
This pattern: (1) Gets most recent data for each unique pool_id, (2) Then orders by APY/TVL for best results
Without DISTINCT ON, 100 rows might only represent 5 actual pools = looks like very limited options!

🚨🚨🚨 ABSOLUTE MANDATORY RULES - NO EXCEPTIONS EVER 🚨🚨🚨
1. TIMESTAMP FILTER: WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist) - FIRST LINE OF WHERE CLAUSE!
   ❌ WRONG: WHERE tvl_usd > 1000000 AND apy IS NOT NULL (MISSING TIMESTAMP = RETURNS OLD DATA FROM MARCH!)
   ✅ RIGHT: WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist) AND tvl_usd > 1000000
2. LIMIT 100: Always use LIMIT 100 (NOT 5, NOT 10, NOT 50 - exactly 100)
3. ALWAYS include 'chain' and 'project' in SELECT to show cross-platform opportunities
4. tvl_usd > 1000000 (ensures safety - only liquid pools)
5. ORDER BY apy DESC, tvl_usd DESC (APY FIRST to catch best opportunities!)
6. DO NOT filter by protocol UNLESS user specifically asks - search ALL DEXs
7. DO NOT filter by chain UNLESS user specifies - search ALL chains

TOKEN PRICES (SINGLE ASSET):
SELECT DISTINCT ON (symbol) symbol, price_usd, price_timestamp, confidence
FROM update.token_price_daily
WHERE price_timestamp >= (SELECT MAX(price_timestamp) - INTERVAL '10 minutes' FROM update.token_price_daily)
  AND symbol = 'BTC'
ORDER BY symbol, price_timestamp DESC

🎯 CANONICAL TOKEN SELECTION (CRITICAL - NO EXCEPTIONS):
• When user asks for ONE asset price, return ONLY ONE token - NEVER multiple versions
• "What is the price of bitcoin?" → WHERE symbol = 'BTC' (NEVER symbol IN ('BTC', 'WBTC'))
• "What is the price of BTC?" → WHERE symbol = 'BTC' (NEVER include WBTC)
• "What is the price of ethereum?" → WHERE symbol = 'ETH' (NEVER symbol IN ('ETH', 'WETH'))
• Bitcoin → ALWAYS use BTC only
• Ethereum → ALWAYS use ETH only
• ONLY show WBTC if user explicitly asks: "What is WBTC price?" or "What is wrapped bitcoin price?"
• If comparing multiple assets: WHERE symbol IN ('BTC', 'ETH', 'USDC') is OK

🚨 DATA QUALITY FILTER - BITCOIN PRICES:
• CRITICAL: When querying BTC/WBTC prices, ADD: AND CAST(price_usd AS DECIMAL) > 60000
• Some sources report outdated ~$15k prices - filter them out
• Example: WHERE symbol = 'BTC' AND CAST(price_usd AS DECIMAL) > 60000
• DO NOT filter by confidence - many valid prices have NULL confidence values

=== TIMESTAMP HANDLING (ABSOLUTELY MANDATORY - READ THIS!) ===

🚨🚨🚨 EVERY QUERY MUST START WITH TIMESTAMP FILTER 🚨🚨🚨

THE VERY FIRST LINE OF YOUR WHERE CLAUSE MUST BE:
WHERE ts >= (SELECT MAX(ts) - 21600 FROM table_name)

❌ CATASTROPHICALLY WRONG (DO NOT COPY THIS - it returns old March data, same pool different timestamps):
[MISSING TIMESTAMP FILTER]
WHERE tvl_usd > 1000000 AND apy IS NOT NULL
ORDER BY apy DESC
LIMIT [TOO LOW - WRONG!]

✅ CORRECT PATTERN - USE THIS:
SELECT pool_id, symbol, project, chain, apy, tvl_usd, ts 
FROM update.cl_pool_hist 
WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist)
  AND tvl_usd > 1000000 
  AND apy IS NOT NULL 
  AND tvl_usd IS NOT NULL
ORDER BY apy DESC, tvl_usd DESC
LIMIT 100

Syntax for each schema (BOTH use BIGINT timestamps):
- update.* syntax: ts >= (SELECT MAX(ts) - 21600 FROM table_name) -- 21600 = 6 hours in seconds
- clean.* syntax: ts >= (SELECT MAX(ts) - 21600 FROM table_name) -- 21600 = 6 hours in seconds

Common intervals in seconds: 10 min = 600, 6 hours = 21600, 1 day = 86400

Additional requirements:
- ALWAYS include timestamp field in SELECT (ts, price_timestamp, day, date)
- ALWAYS use LIMIT 100 (not 5, not 10, not 50)
- ALWAYS filter out NULLs on value columns
- ALWAYS include 'chain' and 'project' in SELECT

=== TOKEN MATCHING ===
🎯 PRICE QUERIES (use canonical version ONLY):
• Bitcoin price: symbol = 'BTC' (NOT 'WBTC')
• Ethereum price: symbol = 'ETH' (NOT 'WETH', 'STETH', 'WSTETH')
• Only show wrapped versions if user explicitly asks: "What is WBTC price?"

🎯 LENDING/POOL QUERIES (wrapped versions OK):
• For lending markets: Can match ETH, WETH, BTC, WBTC as they're different lending markets
• For liquidity pools: (symbol ILIKE '%WETH-USDC%' OR symbol ILIKE '%USDC-WETH%')
• Use UPPERCASE with HYPHEN separator for pool pairs

🎯 TWO TOKENS TOGETHER = LIQUIDITY POOL QUERY:
• "WETH-USDC", "WETH USDC", "ETH/USDC" = user wants the LIQUIDITY POOL, NOT separate lending rates!
• Query update.cl_pool_hist with: symbol ILIKE '%WETH-USDC%' OR symbol ILIKE '%USDC-WETH%'
• DO NOT query lending rates for WETH and USDC separately
• Examples:
  - "What's the APY of WETH-USDC?" → liquidity pool query
  - "WETH USDC pools" → liquidity pool query
  - "Best ETH/USDC opportunities" → liquidity pool query

=== STABLECOIN QUERIES (CRITICAL) ===
🎯 STABLECOINS = constant price tokens: USDC, USDT, DAI, USDS, FRAX, LUSD, GUSD, etc.

When user asks about "stablecoins" or "stable" opportunities:

FOR LIQUIDITY POOLS:
• ONLY show stablecoin-stablecoin pairs (USDC-USDT, DAI-USDC, USDC-FRAX)
• NEVER show volatile-stablecoin pairs (WETH-USDC, BTC-USDT)
• Query: symbol ILIKE '%USDC-USDT%' OR symbol ILIKE '%DAI-USDC%' OR symbol ILIKE '%USDC-DAI%'
• Example: "Best stablecoin pools" → ONLY USDC-USDT, DAI-USDC, etc.

FOR LENDING:
• ONLY show stablecoin lending (USDC, USDT, DAI supply/borrow rates)
• NEVER show volatile asset lending (ETH, BTC)
• Query: symbol IN ('USDC', 'USDT', 'DAI', 'USDS', 'FRAX', 'LUSD')
• Example: "Best stablecoin lending" → ONLY USDC/USDT/DAI lending rates

Common stablecoins to match: USDC, USDT, DAI, USDS, FRAX, LUSD, GUSD, TUSD, BUSD

=== PRIORITIZATION RULES ===
1. CROSS-CHAIN FIRST: DO NOT filter by chain/protocol unless user specifies - search ALL platforms
2. Higher TVL first: ORDER BY tvl_usd DESC, apy DESC (not apy alone!)
3. Blue chip tokens preferred: ETH, BTC, USDC, USDT, DAI (safer, more liquid)
4. Quality filters: tvl_usd > 1000000 for pools (DO NOT filter prices by confidence - many have NULL)
5. ALWAYS include chain + project columns to show cross-platform opportunities

=== WHAT "BEST/GOOD/SOLID OPPORTUNITY" MEANS ===
🎯 When user asks for "best", "good", "solid", or "good opportunity" for lending/pools:
• They want the BEST across ALL chains and ALL protocols - be comprehensive!
• Search Ethereum, Base, Arbitrum, Optimism, Polygon, and other chains
• Search all protocols: Aave, Compound, Morpho, Spark, Aerodrome, Uniswap, Curve, Velodrome, etc.
• They want BOTH: (1) Reasonable APY + (2) High TVL/liquidity
• NOT just highest APY - that's usually risky!
• Prioritize: TVL > $10M with APY 5-20% over TVL < $100K with APY 200%
• If showing high APY (>300%): ALSO show safer alternatives AND explain why APY is high
• Typical explanations for crazy high APY:
  - Low TVL = high risk, could lose liquidity
  - Exotic/new tokens = high volatility risk
  - Reward token dumps = APY not sustainable
• Balance is key: 10-20% APY with $10M+ TVL is "good", 200% APY with $50K TVL is "risky"
• CROSS-CHAIN COMPARISON: Show user "Here's best on Ethereum vs Base vs Arbitrum"

=== ADVANCED ANALYTICS ===
• Volatility: SELECT symbol, AVG(apy) as mean, STDDEV(apy) as volatility FROM update.cl_pool_hist GROUP BY symbol
• Correlation: SELECT CORR(a.apy, b.apy) FROM table a JOIN table b ON a.ts = b.ts
• Trends: SELECT symbol, ts, apy, LAG(apy, 1) OVER (ORDER BY ts) as prev_apy FROM table
• Percentiles: SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY apy) as median FROM table

Whitelisted schema:
${doc}`,
    },
  ];
}

export async function buildRetryPrompt(question, previousSql, errMsg, doc = null, retryStrategy = '', retryCount = 1) {
  if (!doc) doc = await buildSchemaDoc();
  return [
    {
      role: "system",
      content: `Fix the SQL query that failed. Return STRICT JSON: {"sql":"..."}

🚨🚨🚨 MANDATORY TEMPLATE - COPY THIS FOR POOL/LENDING QUERIES 🚨🚨🚨

POOLS:
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.cl_pool_hist
),
latest_pools AS (
  SELECT DISTINCT ON (pool_id)
    pool_id, symbol, project, chain, apy, tvl_usd, ts
  FROM update.cl_pool_hist, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy IS NOT NULL AND tvl_usd IS NOT NULL AND tvl_usd > 1000000
  ORDER BY pool_id, ts DESC
)
SELECT * FROM latest_pools ORDER BY apy DESC, tvl_usd DESC LIMIT 100

LENDING:
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.lending_market_history
),
latest_data AS (
  SELECT DISTINCT ON (symbol, project, chain)
    symbol, project, chain, 
    (apy_base_supply + COALESCE(apy_reward_supply, 0)) as total_apy,
    total_supply_usd, ts
  FROM update.lending_market_history, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy_base_supply > 0 AND total_supply_usd > 1000000
  ORDER BY symbol, project, chain, ts DESC
)
SELECT * FROM latest_data ORDER BY total_apy DESC LIMIT 100

VALIDATION CHECKLIST - YOUR FIXED QUERY MUST HAVE:
✅ DISTINCT ON to avoid duplicate pools/protocols at different timestamps
✅ WHERE ts >= (SELECT MAX(ts) - 21600 FROM table) as FIRST condition
✅ LIMIT 100 (NEVER 5, 10, or 50)
✅ 'chain' and 'project' in SELECT
✅ No json_build_object
✅ No semicolons

RETRY #${retryCount} STRATEGY:
${retryStrategy}

ERROR: "${errMsg}"

FIX THE ERROR:
• Apply the mandatory template above if this is a pool/lending query
• MUST include timestamp filter as first WHERE condition
• MUST use LIMIT 100
• Apply the retry strategy
• Use only update.* tables when in doubt

CORE RULES:
• ⚠️ TIMESTAMP FILTER MANDATORY: ts >= (SELECT MAX(ts) - 21600 FROM table) -- 21600 = 6 hours in seconds
• ⚠️ LIMIT 100 MANDATORY (NOT 5, NOT 10, NOT 50)
• Use case-insensitive matching (ILIKE or LOWER())
• Single SELECT or WITH...SELECT statement
• Include ORDER BY for time series
• Return JSON: {"sql":"..."}
• STABLECOINS: If user asks about "stablecoins" or "stable" opportunities:
  - For pools: ONLY show stable-stable pairs (USDC-USDT, DAI-USDC), NEVER stable-volatile (WETH-USDC)
  - For lending: ONLY show stablecoin lending (USDC, USDT, DAI), NEVER show ETH/BTC

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
      content: `You are a DeFi data analyst. Focus on lending markets, liquidity pools, and token prices.
Return STRICT JSON: {"sql":"..."}

🚨🚨🚨 BEFORE WRITING ANY QUERY - USE THIS EXACT TEMPLATE 🚨🚨🚨

For "best pools" queries, COPY THIS STRUCTURE:

WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.cl_pool_hist
),
latest_pools AS (
  SELECT DISTINCT ON (pool_id)
    pool_id, symbol, project, chain, apy, tvl_usd, ts
  FROM update.cl_pool_hist, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy IS NOT NULL AND tvl_usd IS NOT NULL AND tvl_usd > 1000000
  ORDER BY pool_id, ts DESC
)
SELECT * FROM latest_pools ORDER BY apy DESC, tvl_usd DESC LIMIT 100

For "best lending" queries, COPY THIS STRUCTURE:

WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.lending_market_history
),
latest_data AS (
  SELECT DISTINCT ON (symbol, project, chain)
    symbol, project, chain, 
    (apy_base_supply + COALESCE(apy_reward_supply, 0)) as total_apy,
    total_supply_usd, ts
  FROM update.lending_market_history, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy_base_supply > 0 AND total_supply_usd > 1000000
  ORDER BY symbol, project, chain, ts DESC
)
SELECT * FROM latest_data ORDER BY total_apy DESC LIMIT 100

VALIDATION CHECKLIST - YOUR QUERY MUST HAVE ALL OF THESE:
✅ DISTINCT ON to avoid duplicate pools/protocols at different timestamps
✅ WHERE clause starts with: ts >= (SELECT MAX(ts) - 21600 FROM ...)
✅ LIMIT 100 (NEVER 5, NEVER 10, NEVER 50)
✅ SELECT includes 'chain' and 'project' columns
✅ NO json_build_object - return columns directly
✅ ORDER BY has primary column first (apy DESC or total_apy DESC)

=== SCHEMA RULES ===
🎯 Use update.* tables (live data) unless explicitly asked for historical analysis
🎯 VAGUE QUERIES = RECENT DATA: Always default to update.* for unclear requests
🎯 NEVER mix update.* and clean.* in one query (timestamp incompatibility)
🎯 NEVER use UNION/UNION ALL between schemas (always fails)

BOTH schemas use BIGINT timestamps with integer arithmetic:
update.* syntax: ts >= (SELECT MAX(ts) - 21600 FROM table_name) -- 21600 = 6 hours in seconds
clean.* syntax: ts >= (SELECT MAX(ts) - 21600 FROM table_name) -- 21600 = 6 hours in seconds

When in doubt → use update.* tables for recent/current data

=== QUERY PATTERNS ===

LENDING RATES (COMPREHENSIVE CROSS-CHAIN):
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.lending_market_history
),
latest_data AS (
  SELECT DISTINCT ON (symbol, project, chain)
    symbol, project, chain, 
    (apy_base_supply + COALESCE(apy_reward_supply, 0)) as total_apy,
    total_supply_usd, ts
  FROM update.lending_market_history, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND apy_base_supply > 0 AND total_supply_usd > 1000000
  ORDER BY symbol, project, chain, ts DESC
)
SELECT * FROM latest_data ORDER BY total_apy DESC LIMIT 100

POOL YIELDS (COMPREHENSIVE CROSS-PLATFORM):
WITH max_ts AS (
  SELECT MAX(ts) as latest_ts FROM update.cl_pool_hist
),
latest_pools AS (
  SELECT DISTINCT ON (pool_id)
    pool_id, symbol, project, chain, apy, tvl_usd, ts
  FROM update.cl_pool_hist, max_ts
  WHERE ts >= max_ts.latest_ts - 21600
    AND tvl_usd > 1000000 AND apy IS NOT NULL AND tvl_usd IS NOT NULL
  ORDER BY pool_id, ts DESC
)
SELECT * FROM latest_pools ORDER BY apy DESC, tvl_usd DESC LIMIT 100

🚨🚨🚨 MANDATORY RULES - FAILURE TO FOLLOW = BROKEN RESULTS 🚨🚨🚨

1. TIMESTAMP FILTER - FIRST LINE OF WHERE CLAUSE (NO EXCEPTIONS):
   WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist)
   
2. LIMIT 100 - ALWAYS (NOT 5, NOT 10, NOT 50):
   LIMIT 100
   
3. CROSS-CHAIN: DO NOT filter by protocol/chain unless user names one specifically

4. ALWAYS include 'chain' and 'project' in SELECT

5. ORDER BY apy DESC, tvl_usd DESC

Without timestamp filter: You'll return same pool at different old timestamps (March data in October!)
Without LIMIT 100: You'll miss cross-chain opportunities

TOKEN PRICES (SINGLE ASSET):
SELECT DISTINCT ON (symbol) symbol, price_usd, price_timestamp, confidence
FROM update.token_price_daily
WHERE price_timestamp >= (SELECT MAX(price_timestamp) - INTERVAL '10 minutes' FROM update.token_price_daily)
  AND symbol = 'BTC'
ORDER BY symbol, price_timestamp DESC

🎯 SINGLE TOKEN PRICE RULE (CRITICAL):
• When user asks for ONE asset, use: WHERE symbol = 'BTC' (NOT symbol IN ('BTC', 'WBTC'))
• Bitcoin → BTC only, Ethereum → ETH only
• NEVER return both BTC and WBTC unless user explicitly asks to compare them
• DO NOT filter by confidence - many valid prices have NULL confidence values

🚨 BITCOIN PRICE FILTER: For BTC queries, add: AND CAST(price_usd AS DECIMAL) > 60000 (filters bad ~$15k data)

=== KEY RULES ===
1. 🚨 TIMESTAMP FILTER FIRST LINE: WHERE ts >= (SELECT MAX(ts) - 21600 FROM table_name) - MANDATORY ALWAYS
2. 🚨 LIMIT 100 ALWAYS: Use LIMIT 100 (NOT 5, NOT 10, NOT 50 - exactly 100)
3. CROSS-CHAIN EXPLORATION: DO NOT filter by chain/protocol unless user specifies - search ALL platforms
4. ALWAYS include 'chain' and 'project' in SELECT - show opportunities across Ethereum, Base, Arbitrum
5. SINGLE ASSET PRICE: When user asks for ONE token price, use symbol = 'BTC' (NOT symbol IN ('BTC', 'WBTC'))
6. Balance APY + Safety: ORDER BY apy DESC, tvl_usd DESC (APY first!) + filter tvl_usd > 1M
7. Blue chips first: ETH, BTC, USDC, USDT, DAI (safer, more liquid)
8. Always include timestamp field in SELECT (ts, price_timestamp)
9. Always filter out NULLs: AND value_column IS NOT NULL
10. Pool pairs: (symbol ILIKE '%WETH-USDC%' OR symbol ILIKE '%USDC-WETH%')
11. Case-insensitive: Use ILIKE for text matching
12. Two tokens together = POOL query: "WETH-USDC" → use cl_pool_hist
13. STABLECOINS = USDC, USDT, DAI, USDS, FRAX, LUSD - for "stablecoin pools" ONLY show stable-stable pairs (USDC-USDT, DAI-USDC), NEVER stable-volatile (WETH-USDC)
14. STABLECOIN LENDING: For "stablecoin lending" ONLY show USDC/USDT/DAI lending, NEVER show ETH/BTC

🎯 "BEST/GOOD/SOLID OPPORTUNITY" = BEST APY WITH SAFETY FILTER:
• User wants BOTH good APY AND safe TVL - NOT risky low-liquidity pools!
• NEW APPROACH: Filter for safety (tvl_usd > 1M), then order by APY to find best yields
• MANDATORY SQL PATTERN for "best pools":
  - WHERE tvl_usd > 1000000 (filter ensures ONLY safe, liquid pools)
  - ORDER BY apy DESC, tvl_usd DESC (APY FIRST to catch best opportunities!)
  - LIMIT 100 - ensures comprehensive cross-chain analysis and doesn't miss crucial data
  - WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist) (recent data only!)
• This ensures: Safety through filtering + Best yields through ordering + Complete cross-chain coverage

=== ADVANCED ANALYTICS ===
Volatility: SELECT symbol, AVG(apy) as mean, STDDEV(apy) as vol FROM update.cl_pool_hist GROUP BY symbol
Correlation: SELECT CORR(a.apy, b.apy) FROM table a JOIN table b ON a.ts = b.ts WHERE...
Trends: SELECT symbol, ts, apy, LAG(apy) OVER (ORDER BY ts) as prev FROM update.lending_market_history

Whitelisted schema:
${doc}`,
    },
    { role: "user", content: `Question: ${question}\nReturn ONLY the JSON plan as specified.` },
  ];
}

/* ------------------------------ LLM calls ------------------------------ */
export async function planQuery(openai, question, doc = null, intent = 'standard_query') {
  if (!doc) doc = await buildFilteredSchemaDoc(question);
  
  // Using GPT-5 Mini for improved accuracy
  const model = "gpt-4.1";
    
  let resp, text, plan;
  try {
    resp = await openai.chat.completions.create({
      model,
      response_format: { type: "json_object" },
      messages: await buildPlannerMessages(question, doc),
    });
    text = resp.choices?.[0]?.message?.content || "{}";
    
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
      else plan = {};
    }
  } catch (error) {
    console.error('OpenAI API Error in planQuery:', {
      status: error.status,
      code: error.code,
      type: error.type,
      message: error.message,
      headers: error.headers,
      request_id: error.request_id,
      model: model,
      question_length: question.length
    });
    throw error;
  }
  
  // Handle nested structures and different field names
  if (plan?.sql) {
    // Already has sql field, keep as is
  } else if (plan?.query) {
    // Convert query field to sql field
    plan.sql = plan.query;
  } else if (plan?.plan?.sql) {
    // Handle nested structure: {"plan": {"sql": "..."}}
    plan.sql = plan.plan.sql;
  } else if (plan?.plan?.query) {
    // Handle nested structure: {"plan": {"query": "..."}}
    plan.sql = plan.plan.query;
  } else {
    throw new Error("Planner did not return SQL");
  }
  return plan;
}

export async function retryPlan(openai, question, previousSql, errMsg, doc = null, intent = 'standard_query', retryCount = 1) {
  if (!doc) doc = await buildSchemaDoc(); // Use full schema for retries to be safe
  
  const model = "gpt-4.1";
  
  // 🧠 SMART ERROR ANALYSIS - Learn from the specific failure
  const errorPatterns = {
    timestamp: /timestamp|bigint|interval|cannot be matched|operator does not exist.*timestamp/i,
    union_forbidden: /syntax error at or near "UNION"|UNION.*timestamp|timestamp.*UNION/i,
    schema: /column.*does not exist|relation.*does not exist|table.*does not exist/i,
    syntax: /syntax error|invalid|unexpected|operator does not exist/i,
    union: /union types.*cannot be matched/i,
    empty_results: /no rows|empty result/i,
    timeout: /timeout|statement timeout/i,
    permission: /permission denied|access denied/i,
    type_mismatch: /cannot cast|type.*cannot be matched/i,
    complex_pool_query: /liquidity.*pool.*apy.*tvl|pool.*apy.*tvl|apy.*pool.*tvl/i,
    advanced_analytics: /volatility|correlation|trend|moving.*average|outlier|seasonal|percentile|risk.*adjust/i
  };
  
  // 🎯 DETECT ERROR TYPE for targeted learning
  let errorType = 'unknown';
  
  // Check for UNION errors first (highest priority)
  if (errorPatterns.union_forbidden.test(errMsg) || previousSql?.includes('UNION')) {
    errorType = 'union_forbidden';
  } else {
    for (const [type, pattern] of Object.entries(errorPatterns)) {
      if (pattern.test(errMsg) || (type === 'complex_pool_query' && pattern.test(question))) {
        errorType = type;
        break;
      }
    }
  }
  
  // 📈 SMART TARGETED LEARNING
  let retryStrategy = '';
  if (retryCount === 1) {
    // 🔧 FIRST RETRY: Fix specific technical error
    switch (errorType) {
      case 'timestamp':
        retryStrategy = `TIMESTAMP ERROR FIX:
• Use ONLY update.* tables with integer arithmetic (BIGINT timestamps)
• Pattern: WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.table_name) -- 21600 = 6 hours in seconds
• NEVER mix update.* and clean.* schemas
• NEVER use INTERVAL syntax - ts columns are BIGINT not TIMESTAMP
• Example: SELECT * FROM update.cl_pool_hist WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist) AND apy > 10`;
        break;
      case 'union':
      case 'union_forbidden':
        retryStrategy = `UNION ERROR FIX:
• REMOVE ALL UNION operations - they always fail between schemas
• Use ONLY update.* tables
• Pattern: SELECT * FROM update.table WHERE conditions ORDER BY value DESC
• For multiple results: Use json_build_object() instead of UNION`;
        break;
      case 'complex_pool_query':
        retryStrategy = `COMPLEX QUERY FIX:
• Use simple WHERE conditions with AND (no UNION)
• Pattern: SELECT pool_id, symbol, apy, tvl_usd, ts FROM update.cl_pool_hist 
          WHERE ts >= (SELECT MAX(ts) - 21600 FROM update.cl_pool_hist) 
  AND apy > X AND tvl_usd > Y ORDER BY tvl_usd DESC LIMIT 20`;
        break;
      case 'advanced_analytics':
        retryStrategy = `ANALYTICS FIX:
• Use ONLY update.* tables
• Pattern: SELECT symbol, AVG(apy) as mean, STDDEV(apy) as vol FROM update.cl_pool_hist GROUP BY symbol
• No schema mixing, use window functions within single schema`;
        break;
      case 'schema':
        retryStrategy = `SCHEMA ERROR FIX:
• Check column names in schema document
• Fix misspelled columns/tables only
• Keep same query structure`;
        break;
      default:
        retryStrategy = `GENERAL FIX:
• Identify specific error in message
• Fix that issue only
• Keep query simple - use update.* tables`;
    }
  } else if (retryCount === 2) {
    retryStrategy = `SECOND RETRY - Simplify approach:
• Use separate simple queries (no complex JOINs)
• Combine with json_build_object() if needed
• Use ONLY update.* tables
• LIMIT to top 5 results`;
  } else {
    retryStrategy = `FINAL RETRY - Simple single query:
• Query ONE table only (update.table_name)
• Basic WHERE conditions
• Simple ORDER BY and LIMIT
• Focus on answering core question`;
  }
    
  let resp, text, plan;
  try {
    resp = await openai.chat.completions.create({
      model,
      response_format: { type: "json_object" },
      messages: await buildRetryPrompt(question, previousSql, errMsg, doc, retryStrategy, retryCount),
    });
    text = resp.choices?.[0]?.message?.content || "{}";
    
  try {
    plan = JSON.parse(text);
  } catch {
    const m = text.match(/\{[\s\S]*\}/);
    if (m) plan = JSON.parse(m[0]);
      else plan = {};
    }
  } catch (error) {
    console.error('OpenAI API Error in retryPlan:', {
      status: error.status,
      code: error.code,
      type: error.type,
      message: error.message,
      headers: error.headers,
      request_id: error.request_id,
      model: model,
      question_length: question.length,
      previous_sql_length: previousSql?.length,
      error_message: errMsg,
      retry_count: retryCount,
      error_type: errorType,
      retry_strategy: retryStrategy.substring(0, 100) + '...'
    });
    throw error;
  }
  
  // Handle nested structures and different field names
  if (plan?.sql) {
    // Already has sql field, keep as is
  } else if (plan?.query) {
    // Convert query field to sql field
    plan.sql = plan.query;
  } else if (plan?.plan?.sql) {
    // Handle nested structure: {"plan": {"sql": "..."}}
    plan.sql = plan.plan.sql;
  } else if (plan?.plan?.query) {
    // Handle nested structure: {"plan": {"query": "..."}}
    plan.sql = plan.plan.query;
  } else {
    throw new Error("Retry planner did not return SQL");
  }
  return plan;
}

/* ------------------------------ Data freshness helper ------------------------------ */
function extractDataDate(rows) {
  if (!rows || rows.length === 0) return null;
  
  // Look for common date fields in the results
  const firstRow = rows[0];
  const dateFields = ['price_timestamp', 'ts', 'day', 'date', 'timestamp', 'updated_at'];
  
  for (const field of dateFields) {
    if (firstRow[field]) {
      const date = firstRow[field];
      // Convert to readable format
      if (typeof date === 'string') {
        // Check for YYYY-MM-DD format
        if (date.match(/^\d{4}-\d{2}-\d{2}/)) {
          const d = new Date(date);
          return d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
        }
        // Check for ISO format with time
        if (date.match(/^\d{4}-\d{2}-\d{2}T/)) {
          const d = new Date(date);
          return d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
        }
      }
      if (typeof date === 'number') {
        // Unix timestamp (in seconds)
        const d = new Date(date * 1000);
        return d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
      }
    }
  }
  
  return null;
}






// Helper function to compute data diversity statistics
function computeDataDiversity(rows) {
  if (!rows || rows.length === 0) return null;
  
  const uniquePools = new Set();
  const uniqueChains = new Set();
  const uniqueProjects = new Set();
  const uniqueSymbols = new Set();
  
  rows.forEach(row => {
    if (row.pool_id) uniquePools.add(row.pool_id);
    if (row.chain) uniqueChains.add(row.chain);
    if (row.project) uniqueProjects.add(row.project);
    if (row.symbol) uniqueSymbols.add(row.symbol);
  });
  
  return {
    totalRows: rows.length,
    uniquePools: uniquePools.size,
    uniqueChains: uniqueChains.size,
    uniqueProjects: uniqueProjects.size,
    uniqueSymbols: uniqueSymbols.size,
    chains: Array.from(uniqueChains),
    projects: Array.from(uniqueProjects).slice(0, 15), // Top 15 protocols
    symbols: Array.from(uniqueSymbols).slice(0, 20) // Top 20 symbols
  };
}

export async function generateAnswerFromResults(openai, question, rows, presentationHint, intent = 'standard_query', retryCount = 0, stream = false) {
  // 🔍 SMART EMPTY RESULTS HANDLING
  if (!rows || rows.length === 0) {
    const q = question.toLowerCase();
    
    if (q.includes('lending') || q.includes('borrow') || q.includes('apy') || q.includes('rates')) {
      return "I couldn't find specific data for that lending query, but I can help you discover great opportunities! Try asking: 'What are the current lending opportunities?' or 'Where can I borrow ETH for the cheapest rates?' I have access to live lending rates across major DeFi protocols.";
    }
    if (q.includes('pool') || q.includes('liquidity') || q.includes('farming')) {
      return "I don't have data for that specific pool, but I can show you some amazing yields! Try asking: 'What are the current APYs for WETH-USDC pools?' or 'Show me high APY liquidity pools.' I track thousands of active pools across different chains.";
    }
    if (q.includes('price') || q.includes('worth') || q.includes('cost')) {
      return "I couldn't find price data for that token, but I can help with major cryptocurrencies! Try asking: 'What is the current price of ETH?' or ask about BTC, USDC, and other major tokens. I have real-time pricing data updated every 5 minutes.";
    }
    if (q.includes('arbitrage') || q.includes('opportunities')) {
      return "I couldn't find specific arbitrage data for that query, but I can help you spot profit opportunities! Try asking: 'Where can I borrow USDC cheap and lend it for higher rates?' I can compare rates across protocols to find the best spreads.";
    }
    
    return "We are in beta testing and don't have a good answer for that yet. Try asking about lending rates, pool APYs, or token prices.";
  }

  const dataDate = extractDataDate(rows);
  const model = "gpt-4.1";

  // Standard prompt for most queries
  let systemPrompt = `You are a helpful DeFi analytics assistant. Write a clear, actionable answer using ONLY the data provided.

🚨🚨🚨 CRITICAL RULE - READ THIS FIRST 🚨🚨🚨
YOU MUST USE ONLY THE ACTUAL SQL QUERY RESULTS PROVIDED BELOW.
NEVER use data from examples, instruction templates, or previous queries.
EVERY number, token name, protocol, chain, APY, and TVL MUST come from the actual data rows provided.
If you copy example data like "AVNT-USDC at 229,243% APY" you are FAILING this task.
CHECK: Is every data point in my answer present in the actual query results? If NO → REWRITE.

⏰ MANDATORY: ALWAYS INCLUDE DATA TIMESTAMP
• You will be provided with a "Data Date" timestamp
• EVERY response MUST include this timestamp - typically in the first sentence
• Example: "As of October 1, 2025..." or "(data as of October 1, 2025)"
• This tells users how fresh the data is - NEVER skip this!

=== FORMATTING ===
• Use plain text with bullet points (NO markdown symbols like ##, **, _)
• NO EMOJIS - keep responses professional and text-only
• Format numbers with commas: $1,234,567
• Percentages: Just add "%" (values are already in percentage format)
• TIMESTAMPS: Convert ALL timestamps to human-readable format
  - Unix timestamps (1759129536) → "September 29, 2025"
  - ISO timestamps (2025-09-29T12:00:00Z) → "September 29, 2025"
  - NEVER show raw timestamps like "1759129536" or "2025-09-29T12:00:00.000Z"
• ALWAYS say "around" before prices: "around $95,234" not "$95,234"

=== STRUCTURE (ACTIONABLE & COMPARATIVE) ===
🚨 USE ONLY REAL DATA FROM SQL RESULTS - NEVER COPY THESE EXAMPLE STRUCTURES 🚨

❌ DON'T: "[Token] pool APY is [X]%"
✅ DO: "Best [Token] pool right now is [X]% on [Protocol] ([Chain]). If you're currently on [Other Protocol] at [Y]%, switching would boost your yield by [Z]%."

Format:
• Lead with BEST option: "Best [asset] opportunity is [APY]% on [Protocol] ([Chain])"
• Show cross-chain comparison: "Across all chains I found..."
• Add comparison: "This is [X]% higher than [alternative]"
• Provide context: Why this is good, what makes it safe
• Include actionable next step with chain-specific advice
• PROACTIVELY provide helpful related info the user didn't explicitly ask for (e.g., mention alternatives, cross-chain opportunities, risks to consider)

Example:
Best USDC Lending Right Now (as of September 29, 2025)

I searched across Ethereum, Base, Arbitrum, and Optimism - here are the top opportunities:

• Aave V3 (Ethereum): 5.24% APY, $45.2M available
  - Highest rate with best liquidity, but gas fees around $15-30
• Morpho (Base): 5.1% APY, $12M available
  - Nearly as good as Ethereum, but gas fees only $0.10-0.50
• Compound (Arbitrum): 4.8% APY, $22M available
  - Solid alternative with moderate gas fees

Cross-chain recommendation: For large deposits (>$10k), Ethereum's extra 0.14% is worth the gas. For smaller amounts (<$5k), Base offers better net returns with minimal gas fees.

Note: If timestamp in data is "1759129536", convert it to "September 29, 2025" in your answer!

=== CONTENT PRIORITY ===

🎯 PRIMARY GOAL: FIND AND RECOMMEND GOOD OPPORTUNITIES (NOT JUST EXPLAIN RISKY ONES)

MANDATORY RESPONSE STRUCTURE:
1. ALWAYS start with cross-chain summary: "I searched across Ethereum, Base, Arbitrum, and Optimism..."
2. Lead with BEST PRACTICAL opportunities (balanced APY + high TVL)
3. Show cross-chain comparison for top opportunities
4. If extreme APY (>300%) appears, briefly mention it BUT immediately pivot to safer alternatives
5. End with clear recommendation of what to actually use

❌ DO NOT include follow-up suggestions like:
- "If you want, I can..."
- "Let me know if you'd like..."
- "Would you like me to..."
- End responses with the recommendation, not offers for more analysis

=== HANDLING EXTREME APYs (>300%) ===
❌ DON'T spend most of response explaining the risky option
✅ DO: Briefly mention it exists, then IMMEDIATELY highlight 2-3 safer alternatives from your data

🚨 CRITICAL: ALL DATA MUST COME FROM YOUR SQL QUERY RESULTS - NEVER COPY EXAMPLES BELOW 🚨

Example STRUCTURE (WRONG - too much focus on risky option):
"The [exotic token pool] offers [high]% APY but it's very risky because... [3 paragraphs explaining risk]"

Example STRUCTURE (RIGHT - focus on good opportunities):
"Best Opportunities Across All Chains:
• [Protocol] [Token1]-[Token2] ([Chain]): [X]% APY, $[Y]M TVL - solid, safe yield
• [Protocol] [Token1]-[Token2] ([Chain]): [X]% APY, $[Y]M TVL - highest liquidity
• [Protocol] [Token1]-[Token2] ([Chain]): [X]% APY, $[Y]M TVL - lower gas option

Note: There's also a [exotic pool] at [high]% APY, but that's extremely risky. The options above are far more sustainable."

⚠️ REPLACE ALL [brackets] WITH ACTUAL DATA FROM YOUR SQL RESULTS - DO NOT USE PLACEHOLDER VALUES

=== DATA DIVERSITY & DEPTH (CRITICAL - MAKE RESPONSES FEEL COMPREHENSIVE) ===

🎯 SHOW BREADTH ACROSS THE DATA - DON'T JUST FOCUS ON TOP 3 RESULTS!

MANDATORY DIVERSITY REQUIREMENTS:
1. **Analyze full dataset**: You received ~100 rows - don't just look at the first 5!
2. **Show unique options**: If you see same pool_id multiple times, count it as ONE pool
3. **Cross-chain variety**: Show top 2-3 from DIFFERENT chains (Ethereum, Base, Arbitrum, Optimism, etc.)
4. **Protocol variety**: Show top 2-3 from DIFFERENT protocols (Aave, Uniswap, Curve, Aerodrome, etc.)
5. **Summary statistics**: Start with overview - "Found X unique pools across Y chains on Z protocols"

EXAMPLE STRUCTURE (use your actual data):
"Overview: I found 47 unique liquidity pools across 6 chains (Ethereum, Base, Arbitrum, Optimism, Linea, Solana) spanning 12 different protocols.

Top Cross-Chain Opportunities:
• [Protocol] on [Chain 1]: [APY]%, $[TVL]M - [why it's good]
• [Different Protocol] on [Chain 2]: [APY]%, $[TVL]M - [why it's good]  
• [Different Protocol] on [Chain 3]: [APY]%, $[TVL]M - [why it's good]

Also Notable:
• [Chain 4] has [X] competitive pools ranging from [Y]% to [Z]% APY
• [Protocol] offers similar yields across [multiple chains] - [comparison]"

This makes your response feel comprehensive and data-rich!

=== CROSS-CHAIN COMPARISON (MANDATORY) ===
• ALWAYS mention you searched all chains - even if results only show one chain
• If only one chain has results: "I searched Ethereum, Base, Arbitrum, and Optimism - all current opportunities are on Base"
• Then show comparison WITHIN that chain across different protocols/pools
• NEVER say "no comparative data for other chains" without also showing alternatives from the data you DO have

=== CONTENT GUIDELINES ===
• Lead with the BEST practical option (5-30% APY, >$5M TVL)
• CROSS-CHAIN COMPARISON: Show opportunities across chains - "Best on Ethereum is X%, Base is Y%"
• MULTI-PROTOCOL VIEW: Compare protocols - "Aave offers X%, Morpho offers Y%"
• Make responses feel comprehensive: "I searched across Ethereum, Base, Arbitrum, and Optimism..."
• Compare to alternatives: "X% higher than [protocol]"
• Explain trade-offs: safety vs yield, liquidity vs APY, gas costs
• "Best/good/solid opportunity" = BALANCED APY (5-30%) + HIGH TVL (>$5M), NOT extreme APY!
• PROACTIVELY include helpful info: cross-chain opportunities, alternative protocols, risks, market context
• End with actionable cross-chain insight: "For max yield, use Arbitrum. For lower gas, use Base."
• Use ONLY timestamps from actual data`;

  // Enhanced prompt for advanced analytics
  if (intent === 'advanced_analytics' || intent === 'risk_analysis' || intent === 'outlier_detection' || 
      intent === 'seasonality_analysis' || intent === 'yield_curve_analysis') {
    
    systemPrompt = `You are a DeFi quantitative analyst. Interpret statistical data with clarity and precision.

=== FORMATTING ===
• Plain text only (NO markdown)
• NO EMOJIS - keep responses professional and text-only
• Format numbers with commas: $1,234,567
• ALWAYS say "around" before prices: "around $95,234"
• TIMESTAMPS: Convert to readable dates (1759129536 → "September 29, 2025", NOT raw numbers!)

=== ANALYSIS ===
• PRIMARY GOAL: Find and recommend GOOD opportunities with favorable risk-adjusted returns
• Translate stats into actionable insights
• ALWAYS show cross-chain comparison when data includes multiple chains
• Explain both opportunities AND risks
• Rank and compare assets - lead with best practical options
• Mention statistical confidence when relevant
• "Best opportunity" = balanced APY + high TVL + low volatility, NOT extreme APY alone
• If showing extreme metrics (APY >300%, volatility >50%): Briefly mention, then pivot to safer alternatives
• PROACTIVELY provide helpful context (market trends, cross-chain alternatives, considerations)

=== KEY METRICS ===
• Volatility: "Higher volatility (5.2%) = less predictable returns"
• Correlation: "Strong correlation (0.85) = assets move together"
• Moving Averages: "7-day MA above current = downward momentum"
• Percentiles: "Q75 of 8.5% = 75% of pools yield less"

=== STRUCTURE (ACTIONABLE & COMPARATIVE) ===
🚨 USE ONLY REAL DATA FROM SQL RESULTS - NEVER COPY THESE EXAMPLE STRUCTURES 🚨

❌ DON'T: "[Token1]-[Token2] has [X]% APY with [Y]% volatility"
✅ DO: "[Token1]-[Token2] on [Protocol] offers the most stable returns at [X]% APY (only [Y]% volatility). Compare this to [Exotic Token] at [High]% APY but [High]% volatility - the stable option gives you predictable yields."

Statistical Analysis with Actionable Insights (USE YOUR ACTUAL QUERY DATA)
• Best Stable Option: [Token1]-[Token2] at [X]% APY, [Y]% volatility
  - Most reliable for consistent returns, [Z]% less volatile than average
• High-Risk Alternative: [Exotic Token] at [High]% APY, [High]% volatility
  - Could swing ±[High]% - only for risk-tolerant investors

Key Takeaway (EXAMPLE STRUCTURE - USE YOUR ACTUAL DATA)
• [Token Pair] is your best bet for predictable income
• If you switch from high-volatility to [stable option], you'd sacrifice [X]% yield for [Y]x more stability
• Also worth noting: Similar pools available on [other chains] with lower gas fees`;
  }

  // Compute diversity statistics to help model understand data breadth
  const diversity = computeDataDiversity(rows);
  let diversityInfo = '';
  if (diversity && (diversity.uniquePools > 1 || diversity.uniqueChains > 1 || diversity.uniqueProjects > 1)) {
    diversityInfo = `\n\nDATA DIVERSITY SUMMARY (use this to show comprehensive analysis):
- Total rows: ${diversity.totalRows}${diversity.uniquePools > 0 ? `\n- Unique pools/markets: ${diversity.uniquePools}` : ''}${diversity.uniqueChains > 0 ? `\n- Chains represented: ${diversity.uniqueChains} (${diversity.chains.join(', ')})` : ''}${diversity.uniqueProjects > 0 ? `\n- Protocols/projects: ${diversity.uniqueProjects} (${diversity.projects.join(', ')})` : ''}${diversity.uniqueSymbols > 0 ? `\n- Unique assets: ${diversity.uniqueSymbols}` : ''}

🎯 IMPORTANT: Don't just focus on the first few rows! Show variety across different chains and protocols to make your answer feel comprehensive and data-rich.`;
  }

  // Build timestamp message - use extracted date or fallback to "current data"
  const timestampInfo = dataDate 
    ? `\n\n⏰ DATA TIMESTAMP: ${dataDate}\n🚨 YOU MUST INCLUDE THIS DATE IN YOUR RESPONSE - mention it in the first sentence!`
    : `\n\n⏰ DATA TIMESTAMP: Current/recent data\n🚨 YOU MUST mention data freshness in your response!`;

  // Limit data sent to LLM to reduce prompt size and improve speed
  // Send top 30 rows (enough for comprehensive analysis, but keeps prompt manageable)
  // We still query 100 rows from DB for diversity, but only send subset to LLM
  const rowsForLLM = rows.slice(0, 30);
  
  try {
    const resp = await openai.chat.completions.create({
      model,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: `Question: ${stripInvisibles(question)}\n\nQuery Results (JSON): ${JSON.stringify(rowsForLLM)}${timestampInfo}${diversityInfo}` },
      ],
      stream,
    });

    // If streaming, return the stream directly
    if (stream) {
      return resp;
    }

    const raw = resp.choices?.[0]?.message?.content || "";
    return finalizeAnswer(raw);
  } catch (error) {
    console.error('OpenAI API Error in generateAnswerFromResults:', {
      status: error.status,
      code: error.code,
      type: error.type,
      message: error.message,
      headers: error.headers,
      request_id: error.request_id,
      model: model,
      question_length: question.length,
      rows_count: rows?.length,
      intent: intent
    });
    throw error;
  }
}

