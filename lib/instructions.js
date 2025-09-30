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
  
  // BASIC ANALYTICS DETECTION  
  if (lowerQuestion.includes('correlation') || lowerQuestion.includes('volatility') || 
      lowerQuestion.includes('trend') || lowerQuestion.includes('moving average')) {
    return 'advanced_analytics';
  }
  
  if (lowerQuestion.includes('risk') && (lowerQuestion.includes('adjusted') || 
      lowerQuestion.includes('score') || lowerQuestion.includes('analysis'))) {
    return 'risk_analysis';
  }
  
  if (lowerQuestion.includes('outlier') || lowerQuestion.includes('unusual') || 
      lowerQuestion.includes('anomaly') || lowerQuestion.includes('spike')) {
    return 'outlier_detection';
  }
  
  if (lowerQuestion.includes('seasonal') || lowerQuestion.includes('pattern') || 
      lowerQuestion.includes('time') && lowerQuestion.includes('pattern')) {
    return 'seasonality_analysis';
  }
  
  if (lowerQuestion.includes('yield curve') || lowerQuestion.includes('curve analysis')) {
    return 'yield_curve_analysis';
  }
  
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
  
  // NEW: Protocol fees/revenue related
  if (q.includes('fee') || q.includes('revenue') || q.includes('earning') || q.includes('profit') || q.includes('generated') || q.includes('made money')) {
    keywords.push('fee', 'revenue', 'protocol');
  }
  
  // NEW: DEX/trading related
  if (q.includes('dex') || q.includes('trading') || q.includes('volume') || q.includes('swap') || q.includes('exchange')) {
    keywords.push('dex', 'trading', 'volume', 'swap');
  }
  
  // NEW: Derivatives/funding related
  if (q.includes('funding') || q.includes('perpetual') || q.includes('futures') || q.includes('derivative') || q.includes('perp')) {
    keywords.push('funding', 'perpetual', 'derivative');
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
      const preferredSchema = isHistoricalQuery ? 'clean' : 'update';
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
      content: `🚨 CRITICAL: You MUST follow this EXACT pattern for ALL queries:

SELECT protocol_name, total_liquidity_usd, ts 
FROM update.protocol_chain_tvl_daily 
WHERE ts = (SELECT MAX(ts) FROM update.protocol_chain_tvl_daily) 
  AND total_liquidity_usd IS NOT NULL 
ORDER BY total_liquidity_usd DESC 
LIMIT N

🚨 FORBIDDEN: NEVER use SUM(), GROUP BY, or json_agg() - this creates wrong inflated numbers!
🚨 CRITICAL SCHEMA RULES:
• **NEVER use UNION or UNION ALL** between update.* and clean.* tables - timestamp types are incompatible!
• **When both schemas available**: Use separate queries or CTEs, never UNION them
• **update.* tables**: Use TIMESTAMP WITH TIME ZONE and INTERVAL syntax
• **clean.* tables**: Use BIGINT timestamps and integer arithmetic
• **For comparisons**: Use separate SELECT statements, not UNION

🚨 SINGLE SCHEMA RULE (MANDATORY):
• **ALWAYS USE ONLY ONE SCHEMA**: Either ALL update.* tables OR ALL clean.* tables - NEVER MIX
• **DEFAULT TO update.* ONLY**: For ALL queries unless explicitly asked for historical analysis
• **ADVANCED ANALYTICS**: Use ONLY update.* tables for volatility, correlation, trend analysis
• **FORBIDDEN PATTERNS**: Any query with both "update." and "clean." table references
• **CORRECT**: SELECT * FROM update.cl_pool_hist WHERE conditions
• **WRONG**: Any CTE, UNION, or JOIN mixing update.cl_pool_hist AND clean.cl_pool_hist

Produce a SINGLE Postgres query as STRICT JSON {"sql":"..."} using ONLY the whitelisted schema below.

Common timestamp field names by table:
• update.protocol_chain_tvl_daily → ts
• update.token_price_daily → price_timestamp  
• update.lending_market_history → ts
• update.perp_funding_rates → timestamp
• update.dex_info → data_timestamp
• clean.tvl_defi_hist → date
• clean.narratives → date
• update.raw_etf → day

Global rules:
• String filters on free-text identifiers (e.g., protocol, project, chain, symbol) MUST be case-insensitive.
  – Prefer ILIKE with a suitable pattern (e.g., chain ILIKE 'ethereum').
  – If equality is used, normalize both sides: LOWER(column) = LOWER('value').
• ONE statement total (SELECT or WITH ... SELECT). No semicolons. CTEs allowed. No comments.
• Always add ORDER BY when returning time series results.
• Use only tables/columns listed below.
• Return STRICT JSON only.

CRITICAL - DATA SOURCE PRIORITY:
• **REAL-TIME DATA**: Use update.* tables for current/live analysis (585K+ price records, 1M+ pool records)
• **HISTORICAL ANALYSIS**: Use clean.* tables for trends and historical patterns (12M+ TVL records, 7M+ pool records)

LIVE UPDATE TABLES (USE FOR CURRENT STATE):
• **Token prices**: update.token_price_daily (5-min updates, 585K records, 2400+ tokens)
• **Lending markets**: update.lending_market_history (30-min updates, 63K records, enhanced risk metrics)
• **Liquidity pools**: update.cl_pool_hist (hourly updates, 1M+ records, real-time yields)
• **Protocol TVL**: update.protocol_chain_tvl_daily (daily updates, 1164 records)
• **Protocol fees**: update.protocol_fees_daily (daily updates, 2673 records, revenue tracking)
• **DEX volumes**: update.dex_info (12-hour updates, 885 DEXs)
• **Funding rates**: update.perp_funding_rates (hourly updates, 5398 records, derivatives)
• **Sector performance**: update.narratives (daily updates, 23+ crypto sectors)
• **ETF flows**: update.raw_etf (real-time, institutional data)
• **Stablecoins**: update.stablecoin_mcap_by_peg_daily (daily updates)

HISTORICAL ARCHIVE (USE FOR TRENDS):
• **Price history**: clean.token_price_daily_enriched (2.3M records)
• **TVL history**: clean.protocol_chain_tvl_daily (12M+ records)
• **Pool history**: clean.cl_pool_hist (7.8M records)
• **Lending history**: clean.lending_market_history (685K records)

ANALYTICS CAPABILITIES:
• Rankings and comparisons between protocols/chains
• Growth rates and market share calculations  
• Category analysis and multi-chain aggregation
• Basic statistical analysis when requested

BASIC STATISTICAL ANALYSIS:
• Use statistical functions when requested: CORR(), STDDEV(), LAG(), LEAD()
• Keep responses simple and focused

NEW ADVANCED CAPABILITIES:
• **PROTOCOL REVENUE**: Use update.protocol_fees_daily for fee/revenue analysis (2673 records)
  - Keywords: "fees", "revenue", "earnings", "profit", "generated", "made money"
  - Example: SELECT name, total_24h FROM update.protocol_fees_daily ORDER BY total_24h DESC LIMIT 10
• **DEX TRADING**: Use update.dex_info for volume analysis across 885 DEXs (12-hour updates)  
  - Keywords: "trading volume", "dex", "swap", "exchange", "traded"
  - Example: SELECT name, total_24h FROM update.dex_info ORDER BY total_24h DESC LIMIT 10
• **DERIVATIVES**: Use update.perp_funding_rates for funding rate analysis (5398 records, hourly)
  - Keywords: "funding rate", "perpetual", "futures", "derivatives", "perp"
  - Example: SELECT marketplace, market, funding_rate FROM update.perp_funding_rates WHERE base_asset = 'BTC'
• **SECTOR PERFORMANCE**: Use update.narratives for real-time sector trends (23+ categories)
  - Keywords: "sector", "narrative", "ai", "defi", "gaming", "meme", "performance"
  - Example: SELECT artificial_intelligence, decentralized_finance, gaming_gamefi FROM update.narratives
• **INSTITUTIONAL FLOWS**: Use update.raw_etf for ETF flow tracking
  - Keywords: "etf", "institutional", "flows", "inflow", "outflow"
  - Example: SELECT gecko_id, total_flow_usd FROM update.raw_etf ORDER BY day DESC
• **ENHANCED LENDING**: Use update.lending_market_history with risk metrics (il_risk, exposure, predictions)
  - Enhanced with: apy_pct_1d, apy_pct_7d, apy_mean_30d, stablecoin, il_risk, exposure
  - **DEFAULT TO AAVE-V3**: For lending queries, filter by project = 'aave-v3' unless user specifies otherwise
• **LIVE POOLS**: Use update.cl_pool_hist for real-time yield opportunities (1M+ records)
  - Real-time TVL and APY data updated hourly

ARBITRAGE & OPPORTUNITY DETECTION:
• **RATE SPREADS**: Compare lending/borrowing rates across protocols for same asset
• **YIELD FARMING**: Find highest APY combinations with (apy_base_supply + apy_reward_supply)
• **CROSS-PROTOCOL**: Use JSON aggregation or separate CTEs to compare rates side-by-side
• **RATE GAPS**: Calculate rate differences and identify arbitrage opportunities
• **OPTIMAL YIELDS**: ORDER BY total_apy DESC to find best opportunities

TREND ANALYSIS PATTERNS:
• **GROWTH MOMENTUM**: Use LAG() to calculate period-over-period changes
• **TVL VELOCITY**: Calculate daily/weekly/monthly growth rates
• **CATEGORY TRENDS**: GROUP BY category and track performance over time
• **PROTOCOL MOMENTUM**: Compare current vs historical performance metrics
• **TIME SERIES**: Use window functions for moving averages and trend detection

RISK METRICS & CONCENTRATION:
• **DOMINANCE**: Calculate protocol market share using SUM() and percentages
• **CONCENTRATION**: Use Herfindahl index or top-N concentration ratios
• **DIVERSITY**: COUNT(DISTINCT protocol_id) and distribution analysis
• **EXPOSURE LIMITS**: Identify protocols above certain TVL thresholds
• **FRAGILITY WARNINGS**: Flag high concentration or single-point-of-failure risks

IMPORTANT - Data Freshness & Time-Series Handling:
• Prioritize update.* schema for all current data queries
• For historical trends: Use clean.* tables (814K+ records available)
• For current prices: update.token_price_daily has 1-5 minute freshness

🚨 CRITICAL - TVL/SNAPSHOT DATA HANDLING (MANDATORY):
• **NEVER GROUP BY protocol_name without date filter** - this sums across ALL historical dates creating impossible values
• **ALWAYS include WHERE ts = (SELECT MAX(ts) FROM table)** before any GROUP BY or SUM operations
• **WRONG**: "SELECT protocol_name, SUM(tvl) FROM table GROUP BY protocol_name" 
• **CORRECT**: "SELECT protocol_name, SUM(tvl) FROM table WHERE ts = (SELECT MAX(ts) FROM table) GROUP BY protocol_name"

🎯 **TVL DATA SOURCE RULES (MANDATORY)**:
• **For TOTAL DeFi TVL**: Use clean.tvl_defi_hist (correct $151.64B total, excludes CEX)
• **For PROTOCOL RANKINGS**: MUST use this exact pattern:

**REQUIRED SQL PATTERN FOR PROTOCOL RANKINGS**:
SELECT protocol_name, total_liquidity_usd AS tvl, ts, category
FROM update.protocol_chain_tvl_daily 
WHERE ts = (SELECT MAX(ts) FROM update.protocol_chain_tvl_daily) 
  AND total_liquidity_usd IS NOT NULL
ORDER BY total_liquidity_usd DESC 
LIMIT N

**CRITICAL**: 
- Always filter to most recent timestamp: WHERE ts = (SELECT MAX(ts) FROM table)
- Always filter out NULL values: AND total_liquidity_usd IS NOT NULL  
- Always sort by highest TVL first: ORDER BY total_liquidity_usd DESC
- Always include ts field for timestamp information

• **CRITICAL**: Use series_type = 'total' to get correct aggregated TVL per protocol
• **NEVER use chain-specific data** - this causes double-counting and inflated numbers
• **ALWAYS exclude CEX** - add AND category != 'CEX' 
• **NO GROUP BY needed** - series_type = 'total' already provides aggregated data

CRITICAL: When user asks for "top DeFi protocols", you MUST use series_type = 'total' and exclude CEX.
WRONG: "SELECT protocol_name, SUM(total_liquidity_usd) FROM table GROUP BY protocol_name" (causes double-counting)
RIGHT: "SELECT protocol_name, total_liquidity_usd FROM table WHERE category != 'CEX' AND series_type = 'total' ORDER BY total_liquidity_usd DESC"

🕒 **TIMESTAMP HANDLING (CRITICAL)**:
• **MANDATORY**: Include timestamp field (ts, date, timestamp, etc.) in ALL SELECT statements - never omit it!
• **MANDATORY**: Always filter to most recent data: WHERE ts = (SELECT MAX(ts) FROM table)
• **MANDATORY**: Filter out NULL values: AND [value_column] IS NOT NULL
• **MANDATORY**: Sort by highest values first: ORDER BY [value_column] DESC
• **MANDATORY**: Use the exact timestamp value from the query results in your answer
• **Format**: Say "Data as of [exact_timestamp_from_results]" using whatever format the data provides
• **Example**: If query returns ts='2025-09-28T04:00:00.000Z', say "Data as of 2025-09-28T04:00:00.000Z"

BACKTESTING RULES (UPDATED):
• **Current/recent prices**: Use update.token_price_daily (coin_id field, confidence > 0.8)
• **Historical prices**: Use clean.token_price_daily_enriched if needed (currently empty)
• **Current lending data**: Use update.lending_market_history (fresh APY every 6 hours)
• **Historical lending**: Use clean.lending_market_history for long-term trends
• Always include quality filters: confidence > 0.8 for prices
• For backtests, prefer update.* tables for recent data, clean.* for historical context

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
      content: `🧠 LEARNING FROM FAILURE - Generate a better query that avoids the specific error.

RETRY #${retryCount} - SMART STRATEGY:
${retryStrategy}

📊 ERROR ANALYSIS:
Previous SQL failed with: "${errMsg}"

🎯 LEARN AND ADAPT:
• Understand WHY the previous approach failed
• Apply the specific strategy above to fix the root cause
• Generate a SINGLE Postgres query as STRICT JSON {"sql":"..."}
• Use only the whitelisted schema below

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
      content: `You are a DeFi data analyst. Generate SQL for lending markets, liquidity pools, and token prices ONLY.

🚨 CRITICAL TIMESTAMP SYNTAX (READ FIRST):
• Token prices: price_timestamp >= (SELECT MAX(price_timestamp) - INTERVAL '10 minutes' FROM update.token_price_daily)
• Lending markets: ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.lending_market_history)
• Liquidity pools: ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist)
• clean.* tables: ts >= (SELECT MAX(ts) - 21600 FROM table_name) [6 hours = 21600 seconds]
• NEVER mix INTERVAL with BIGINT timestamps
• NEVER mix integer arithmetic with TIMESTAMP WITH TIME ZONE
• DEFAULT: Always use update.* tables unless explicitly asked for historical data

🚨 TIMESTAMP TYPE DETECTION (MANDATORY):
• update.* tables: ts column is TIMESTAMP WITH TIME ZONE - use INTERVAL syntax
• clean.* tables: ts column is BIGINT (Unix seconds) - use integer arithmetic
• NEVER guess the type - always use the correct syntax for each schema
• If uncertain about timestamp type, use update.* tables (safer default)

🚨 CRITICAL SCHEMA RULES:
• **NEVER USE UNION**: Completely forbidden between update.* and clean.* - always causes timestamp type errors
• **SINGLE SCHEMA MODE** (Default): Use ONLY update.* OR ONLY clean.* tables per query
• **DUAL SCHEMA MODE** (When both provided): Use separate CTEs with JSON aggregation
• Pattern: SELECT json_build_object('live', (SELECT ... FROM update.table), 'historical', (SELECT ... FROM clean.table))
• **HIGH APY POOLS**: Prefer update.cl_pool_hist for live data, clean.cl_pool_hist for historical analysis

🚨 TVL PRIORITIZATION (MANDATORY):
• ALWAYS prefer higher TVL pools for better liquidity and user safety
• Default sort: ORDER BY tvl_usd DESC, apy DESC (TVL first, then yield)
• Higher TVL = lower slippage, more reliable yields, better user experience

🚨 BLUE CHIP TOKEN PREFERENCE (MANDATORY):
• For vague queries ("good pools", "best opportunities"), prioritize blue chip tokens
• Blue chips: BTC, WBTC, ETH, WETH, STETH, WSTETH, USDC, USDT, DAI
• Only show exotic tokens if APY is exceptionally higher (>50% better than blue chips)
• Pattern: WHERE (symbol ILIKE '%ETH%' OR symbol ILIKE '%BTC%' OR symbol ILIKE '%USDC%') ORDER BY tvl_usd DESC

🚨 CORE FOCUS AREAS:
• LENDING MARKETS: Interest rates, yields, utilization across DeFi protocols
• LIQUIDITY POOLS: Pool APYs, TVL, yield farming opportunities  
• TOKEN PRICES: Current and historical pricing for calculations

🚨 TIMESTAMP RULES (MANDATORY):
• clean.* tables: ts is BIGINT (Unix timestamp) - use arithmetic: ts >= (SELECT MAX(ts) - 3600 FROM table)
• update.* tables: ts is TIMESTAMP WITH TIME ZONE - use INTERVAL: ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM table)
• ❌ NEVER: ts >= MAX(ts) - 3600 (causes errors)
• ❌ NEVER: Mix BIGINT timestamps with INTERVAL operations
• ❌ NEVER: Mix TIMESTAMP WITH TIME ZONE with integer arithmetic
• ✅ ALWAYS: ts >= MAX(ts) - INTERVAL '6 hours' (for update.* tables)
• ✅ ALWAYS: ts >= MAX(ts) - 3600 (for clean.* tables - Unix timestamp arithmetic)

🚨 CROSS-SCHEMA QUERY RULES (MANDATORY):
• **DEFAULT PREFERENCE**: Use update.* tables for ALL queries unless specifically asked for historical analysis
• **NEVER USE UNION OR UNION ALL**: Completely forbidden between ANY schemas (causes timestamp errors)
• **SINGLE SCHEMA ONLY**: Use ONLY update.* OR ONLY clean.* - never mix schemas in one query
• **FOR POOLS WITH CONDITIONS**: Always use ONLY update.cl_pool_hist with simple WHERE conditions
• For ALL current/real-time queries: Use ONLY update.* tables
• For ALL historical analysis (explicitly requested): Use ONLY clean.* tables  
• ✅ CORRECT: SELECT * FROM update.cl_pool_hist WHERE apy > X AND tvl_usd > Y
• ❌ FORBIDDEN: Any UNION operation between schemas
• ❌ FORBIDDEN: Mixing update.lending_market_history WITH clean.cl_pool_hist
• ❌ FORBIDDEN: UNION between update.cl_pool_hist AND clean.cl_pool_hist
• **WHEN IN DOUBT**: Always choose update.* tables (they have live data and TIMESTAMP WITH TIME ZONE)

🚨 MULTI-TABLE STRATEGY (MANDATORY):
• For comparisons (lending vs pools): Use update.lending_market_history AND update.cl_pool_hist
• For arbitrage analysis: Use update.lending_market_history AND update.token_price_daily
• ALWAYS stick to the same schema prefix (update.* or clean.*) for all tables in one query
• Example: SELECT lending.*, pools.* FROM update.lending_market_history lending, update.cl_pool_hist pools

🚨 TIME WINDOWS FOR "CURRENT" QUERIES (MANDATORY):
• Token prices ONLY: INTERVAL '10 minutes' 
• Lending markets: INTERVAL '6 hours' (ALWAYS use this for lending queries)
• Liquidity pools: INTERVAL '6 hours' (ALWAYS use this for pool queries)
• ALL other queries: INTERVAL '6 hours'

🚨 CORE TABLE PATTERNS (MANDATORY SYNTAX):
• update.token_price_daily: WHERE price_timestamp >= (SELECT MAX(price_timestamp) - INTERVAL '10 minutes' FROM update.token_price_daily)
• update.lending_market_history: WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.lending_market_history) AND project = 'aave-v3' ORDER BY total_supply_usd DESC, (apy_base_supply + COALESCE(apy_reward_supply, 0)) DESC
• update.cl_pool_hist: WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist) ORDER BY tvl_usd DESC, apy DESC
• clean.lending_market_history: WHERE ts >= (SELECT MAX(ts) - 21600 FROM clean.lending_market_history) AND project = 'aave-v3' ORDER BY total_supply_usd DESC
• clean.cl_pool_hist: WHERE ts >= (SELECT MAX(ts) - 21600 FROM clean.cl_pool_hist) ORDER BY tvl_usd DESC, apy DESC

🚨 AAVE-V3 DEFAULT RULES (MANDATORY):
• For "lending rates", "borrow rates", "supply APY" queries: Always add AND project = 'aave-v3'
• EXCEPTIONS - Don't use Aave filter when user asks for:
  - "Compare protocols" or "best rates across all protocols"
  - Specific protocol names: "compound rates", "fraxlend APY", etc.
  - Cross-chain analysis: "lending rates on polygon vs ethereum"
• EXAMPLES:
  - "USDC lending rates" → WHERE project = 'aave-v3' AND symbol = 'USDC'
  - "Best ETH supply APY" → WHERE project = 'aave-v3' AND symbol = 'ETH'
  - "Compare lending protocols" → No project filter (show all protocols)

🚨 TIMESTAMP CALCULATION RULES (CRITICAL):
• update.* tables: ALWAYS use INTERVAL arithmetic (e.g., - INTERVAL '6 hours', - INTERVAL '1 day')
• clean.* tables: ALWAYS use integer arithmetic (e.g., - 21600 for 6 hours, - 86400 for 1 day)
• Common intervals: 5 min = 300 sec, 1 hour = 3600 sec, 6 hours = 21600 sec, 1 day = 86400 sec
• NEVER mix these - timestamp type errors will occur
• **TVL PRIORITY**: Always order by TVL/total_supply_usd DESC first, then APY DESC for better liquidity

🚨 LIQUIDITY POOL PAIR MATCHING:
• Pool symbols: "TOKEN1-TOKEN2" format (e.g., "WETH-USDC")
• For pairs: (symbol ILIKE '%WETH-USDC%' OR symbol ILIKE '%USDC-WETH%')
• Use UPPERCASE with HYPHEN separator

🚨 HIGH APY POOL QUERIES (CRITICAL):
• For current high APY pools: Use ONLY update.cl_pool_hist (never use clean.*)
• COMPLEX MULTI-CONDITION QUERIES: Always use simple AND conditions in a single WHERE clause
• EXACT PATTERN for "apy over X and tvl over Y": 
  SELECT pool_id, symbol, project, chain, apy, tvl_usd, ts 
  FROM update.cl_pool_hist 
  WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist) 
    AND apy > X 
    AND tvl_usd > Y 
    AND apy IS NOT NULL 
    AND tvl_usd IS NOT NULL 
  ORDER BY tvl_usd DESC, apy DESC 
  LIMIT 20
• Prioritize blue chip tokens: WHERE (symbol ILIKE '%ETH%' OR symbol ILIKE '%BTC%' OR symbol ILIKE '%USDC%') AND apy > 10 ORDER BY tvl_usd DESC, apy DESC
• For non-blue chip: WHERE apy > 100 AND tvl_usd > 2000000 ORDER BY tvl_usd DESC, apy DESC
• Show blue chip opportunities first, then exceptional non-blue chip if significantly higher APY
• NEVER use UNION between schemas - causes timestamp type errors
• NEVER attempt CTEs that combine update.* and clean.* tables
• NEVER try to be "comprehensive" by querying both schemas
• ❌ WRONG: Any query mixing update.cl_pool_hist and clean.cl_pool_hist
• ❌ WRONG: Complex CTEs when simple AND conditions work
• ✅ CORRECT: Single table query from update.cl_pool_hist with simple WHERE conditions

🚨 TVL PREFERENCE RULES (MANDATORY):
• ALWAYS prioritize higher TVL pools for better liquidity and safety
• Default ordering: ORDER BY tvl_usd DESC, apy DESC (TVL first, then APY)
• For lending: ORDER BY total_supply_usd DESC, (apy_base_supply + COALESCE(apy_reward_supply, 0)) DESC
• Higher TVL = better liquidity, lower slippage, more reliable yields
• Recommend pools with TVL > $1M for better user experience

🚨 TOKEN MATCHING:
• BTC: symbol IN ('BTC', 'WBTC') 
• ETH: symbol IN ('ETH', 'WETH', 'STETH', 'WSTETH')
• USDC: symbol = 'USDC'

🚨 BLUE CHIP TOKEN PRIORITIZATION (MANDATORY):
• For vague queries, ALWAYS prefer blue chip tokens first
• Blue chip tokens: BTC, WBTC, ETH, WETH, STETH, WSTETH, USDC, USDT, DAI
• Blue chip pools: ETH-USDC, WBTC-ETH, USDC-USDT, WETH-USDC, DAI-USDC
• Pattern: WHERE (symbol ILIKE '%ETH%' OR symbol ILIKE '%BTC%' OR symbol ILIKE '%USDC%' OR symbol ILIKE '%USDT%' OR symbol ILIKE '%DAI%') ORDER BY tvl_usd DESC, apy DESC
• Only show non-blue chip opportunities if APY is exceptional (>50% higher than blue chip options)
• For "best" or "good" pools: prioritize established tokens with high TVL

🚨 STABLECOIN POOL IDENTIFICATION:
• Stablecoin pools = BOTH tokens must be stablecoins
• Major stablecoins: USDC, USDT, DAI, FRAX, LUSD
• ✅ CORRECT stablecoin pools: USDC-DAI, USDT-USDC, FRAX-USDC, DAI-LUSD
• ❌ WRONG (not stablecoin pools): USDC-ETH, USDC-BTC, DAI-WETH
• SIMPLE pattern for stablecoin pools: WHERE (symbol ILIKE '%USDC-DAI%' OR symbol ILIKE '%DAI-USDC%' OR symbol ILIKE '%USDT-USDC%' OR symbol ILIKE '%USDC-USDT%' OR symbol ILIKE '%FRAX-USDC%' OR symbol ILIKE '%USDC-FRAX%' OR symbol ILIKE '%DAI-USDT%' OR symbol ILIKE '%USDT-DAI%')
• Keep queries simple - no complex CTEs or regex for stablecoin detection

🚨 LENDING OPPORTUNITIES:
• ✅ PREFER update.lending_market_history - LIVE DATA with amazing rates like 95% GOHM!
• **DEFAULT TO AAVE-V3**: For lending rate queries, prioritize project = 'aave-v3' unless:
  - User specifically mentions another protocol (compound, fraxlend, etc.)
  - User asks for cross-protocol comparison or "best rates across protocols"
  - User asks for cross-chain analysis
• PATTERN: WHERE project = 'aave-v3' AND apy_base_supply > 0 ORDER BY (apy_base_supply + COALESCE(apy_reward_supply, 0)) DESC
• ✅ Also use clean.lending_market_history for historical comparison
• Order by total APY: apy_base_supply + COALESCE(apy_reward_supply, 0) DESC
• Filter: apy_base_supply IS NOT NULL AND apy_base_supply > 0
• Use INTERVAL '6 hours' for recent live data

🚨 ROBUST TIMESTAMP PATTERNS (PREVENT ERRORS):
• NEVER use: ts - INTERVAL (wrong for clean.* tables)
• NEVER use: ts - 3600 (wrong for update.* tables)
• ✅ CORRECT for update.*: ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM table)
• ✅ CORRECT for clean.*: ts >= (SELECT MAX(ts) - 21600 FROM table)
• Always wrap in subquery for safety and consistency
• Default to update.* when unsure about timestamp type

BASIC ANALYTICS:
• Use CORR(), STDDEV(), LAG(), LEAD() functions when requested
• Provide statistical context for volatility and correlation queries
  - Pattern: SELECT EXTRACT(hour FROM ts) as hour, AVG(apy) as avg_hourly_apy FROM table GROUP BY hour

🚨 ARBITRAGE QUERIES (BORROW + LEND/POOL):
• Use ONLY update.* tables for real-time arbitrage analysis
• NEVER use UNION ALL between different table types (causes timestamp type errors)
• Pattern: Use separate CTEs and JSON aggregation instead
• ✅ CORRECT: Use json_build_object() to combine different result sets
• ❌ WRONG: UNION ALL between update.lending_market_history and update.cl_pool_hist
• Example structure:
  WITH borrow_rates AS (...), supply_rates AS (...), pool_rates AS (...)
  SELECT json_build_object('borrow', borrow_rates, 'supply', supply_rates, 'pools', pool_rates)

🚨 MANDATORY RULES:
• Always include timestamp in SELECT
• Use case-insensitive matching (ILIKE)
• APY values are already percentages - don't multiply by 100
• Return JSON: {"sql": "SELECT ..."}

Focus ONLY on lending, pools, and prices. Ignore TVL, protocols, perps, etc.

🧠 ADVANCED ANALYTICS QUERY PATTERNS (MANDATORY):

• **DUAL SCHEMA PATTERN** (When both schemas available):
  SELECT json_build_object(
    'current_data', (SELECT json_agg(row_to_json(t)) FROM (SELECT * FROM update.cl_pool_hist WHERE ...) t),
    'historical_data', (SELECT json_agg(row_to_json(t)) FROM (SELECT * FROM clean.cl_pool_hist WHERE ...) t)
  )

• **TREND ANALYSIS**: "Show ETH lending rate trends"
  SELECT symbol, ts, apy_base_supply, 
    LAG(apy_base_supply, 1) OVER (ORDER BY ts) as prev_apy,
    AVG(apy_base_supply) OVER (ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7d
  FROM update.lending_market_history WHERE symbol = 'ETH' ORDER BY ts DESC LIMIT 30

• **VOLATILITY ANALYSIS**: "Which pools have the most stable yields?"
  SELECT symbol, AVG(apy) as mean_apy, STDDEV(apy) as volatility,
    AVG(apy) / NULLIF(STDDEV(apy), 0) as stability_score
  FROM update.cl_pool_hist GROUP BY symbol ORDER BY stability_score DESC LIMIT 10

• **OUTLIER DETECTION**: "Find unusual APY spikes"
  WITH stats AS (SELECT AVG(apy) as mean_apy, STDDEV(apy) as std_apy FROM update.cl_pool_hist)
  SELECT symbol, apy, ts, 
    CASE WHEN ABS(apy - mean_apy) > 2 * std_apy THEN 'OUTLIER' ELSE 'NORMAL' END as flag
  FROM update.cl_pool_hist, stats WHERE ABS(apy - mean_apy) > 2 * std_apy

• **CORRELATION ANALYSIS**: "How do USDC and ETH rates correlate?"
  SELECT CORR(u.apy_base_supply, e.apy_base_supply) as correlation
  FROM update.lending_market_history u JOIN update.lending_market_history e 
  ON u.ts = e.ts WHERE u.symbol = 'USDC' AND e.symbol = 'ETH'

• **SEASONALITY PATTERNS**: "What are the hourly yield patterns?"
  SELECT EXTRACT(hour FROM ts) as hour, AVG(apy) as avg_hourly_apy, COUNT(*) as samples
  FROM update.cl_pool_hist GROUP BY EXTRACT(hour FROM ts) ORDER BY hour

• **PERCENTILE ANALYSIS**: "Show yield distribution percentiles"
  SELECT symbol, 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY apy) as q25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY apy) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY apy) as q75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY apy) as q95
  FROM update.cl_pool_hist GROUP BY symbol

**CORE COLUMNS TO USE:**
• Lending: symbol, project, chain, ts, apy_base_supply, apy_reward_supply, total_supply_usd
• Pools: symbol, project, chain, ts, apy, apy_base, tvl_usd  
• Prices: symbol, price_usd, price_timestamp

Use only the tables below:

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
export async function planQuery(openai, question, doc = null, intent = 'standard_query') {
  if (!doc) doc = await buildFilteredSchemaDoc(question);
  
  // Use GPT-4.1 for all queries for speed (GPT-5 is too slow)
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
  
  // 📈 SMART TARGETED LEARNING - Fix technical errors while preserving query intent
  let retryStrategy = '';
  if (retryCount === 1) {
    // 🔧 FIRST RETRY: Fix specific technical errors while preserving complexity
    switch (errorType) {
      case 'timestamp':
        retryStrategy = `TIMESTAMP ERROR - IMMEDIATE SINGLE-SCHEMA FIX:
        • This error means wrong timestamp syntax for the schema type
        • SOLUTION: Use ONLY update.* tables with INTERVAL syntax
        • update.* pattern: WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.table_name)
        • clean.* pattern: WHERE ts >= (SELECT MAX(ts) - 21600 FROM clean.table_name) 
        • For high APY + high TVL pools: 
          SELECT pool_id, symbol, project, chain, apy, tvl_usd, ts 
          FROM update.cl_pool_hist 
          WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist) 
            AND apy > 100 AND tvl_usd > 2000000 
          ORDER BY tvl_usd DESC, apy DESC LIMIT 20
        • NEVER mix schemas or timestamp types - pick one schema and stick to it
        • NEVER use UNION between update.* and clean.* tables
        • Default to update.* tables for current data queries`;
        break;
      case 'union':
        retryStrategy = `UNION TYPE ERROR - CRITICAL SCHEMA MIXING FIX:
        • REMOVE ALL UNION operations between update.* and clean.* tables
        • Use ONLY update.* tables for current high APY pool queries
        • Simple pattern: SELECT * FROM update.cl_pool_hist WHERE apy > 100 AND tvl_usd > 2000000 ORDER BY apy DESC
        • NEVER attempt to combine schemas - this always fails due to timestamp type differences
        • Focus on live data from update.* tables only`;
        break;
      case 'union_forbidden':
        retryStrategy = `UNION IS COMPLETELY FORBIDDEN:
        🚨 NEVER USE UNION OR UNION ALL - ALWAYS FAILS WITH TIMESTAMP ERRORS!
        
        • WRONG: SELECT ... FROM update.table UNION SELECT ... FROM clean.table
        • RIGHT: SELECT ... FROM update.table WHERE conditions
        
        • For multiple results, use JSON aggregation:
          SELECT json_build_object('results', json_agg(row_to_json(t))) FROM (SELECT ...) t
        
        • For comparisons, use separate CTEs:
          WITH live AS (SELECT ... FROM update.table), historical AS (SELECT ... FROM clean.table)
          SELECT json_build_object('live', (SELECT json_agg(*) FROM live), 'historical', (SELECT json_agg(*) FROM historical))
        
        • ALWAYS use ONE schema per query - NEVER mix update.* and clean.*`;
        break;
      case 'complex_pool_query':
        retryStrategy = `COMPLEX POOL QUERY - ANTI-UNION APPROACH:
        🚨 UNION IS COMPLETELY FORBIDDEN - IT ALWAYS FAILS WITH TIMESTAMP ERRORS!
        
        • CORRECT PATTERN: SELECT pool_id, symbol, project, chain, apy, tvl_usd, ts 
          FROM update.cl_pool_hist 
          WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist) 
            AND apy > [APY_THRESHOLD] AND tvl_usd > [TVL_THRESHOLD] 
            AND apy IS NOT NULL AND tvl_usd IS NOT NULL 
          ORDER BY tvl_usd DESC, apy DESC LIMIT 20
        
        • FORBIDDEN PATTERNS (NEVER USE):
          ❌ SELECT ... FROM update.table UNION SELECT ... FROM clean.table
          ❌ SELECT ... FROM table1 UNION ALL SELECT ... FROM table2  
          ❌ Any query containing the word "UNION"
        
        • Use simple AND conditions in a single WHERE clause from ONE table only
        • Always include NULL checks for both apy and tvl_usd
        • Prioritize TVL in ORDER BY for better liquidity`;
        break;
      case 'advanced_analytics':
        retryStrategy = `ADVANCED ANALYTICS - SINGLE SCHEMA APPROACH:
        • This is an advanced analytics query (volatility, correlation, trend analysis)
        • **CRITICAL**: Use ONLY update.* tables - NEVER mix with clean.* tables
        • **FORBIDDEN**: Any UNION, UNION ALL, or schema mixing operations
        • EXACT PATTERN for volatility analysis:
          SELECT symbol, AVG(apy) as mean_apy, STDDEV(apy) as volatility,
            AVG(apy) / NULLIF(STDDEV(apy), 0) as stability_score
          FROM update.cl_pool_hist 
          WHERE ts >= (SELECT MAX(ts) - INTERVAL '6 hours' FROM update.cl_pool_hist)
            AND symbol ILIKE '%ETH%' AND apy IS NOT NULL
          GROUP BY symbol ORDER BY stability_score DESC LIMIT 10
        • Use window functions for trend analysis: LAG(), LEAD(), AVG() OVER()
        • For correlations: Use self-joins on update.cl_pool_hist only
        • Focus on statistical analysis within a single schema`;
        break;
      case 'schema':
        retryStrategy = `SCHEMA ERROR - FIX ONLY THE COLUMN/TABLE NAMES:
        • Keep the exact same query structure
        • ONLY fix: Correct any misspelled columns or tables
        • Check schema document for exact column names
        • PRESERVE all complex logic, JOINs, and multi-table queries
        • Do NOT simplify - just fix the column references`;
        break;
      case 'type_mismatch':
        retryStrategy = `TYPE ERROR - FIX ONLY THE DATA TYPE ISSUES:
        • Keep the same complex query structure
        • ONLY fix: Add proper casting (::type or CAST())
        • Ensure matching data types in operations
        • PRESERVE all multi-table logic and complexity
        • Do NOT remove any tables or simplify the intent`;
        break;
      default:
        retryStrategy = `GENERAL ERROR - MINIMAL TARGETED FIX:
        • Identify the specific technical issue in the error message
        • Fix ONLY that specific issue
        • PRESERVE the original query complexity and multi-table intent
        • Do NOT simplify unless absolutely necessary`;
    }
  } else if (retryCount === 2) {
    // 🔄 SECOND RETRY: More conservative but still preserve multi-table intent
    retryStrategy = `CONSERVATIVE APPROACH - Still preserve multi-table analysis:
    1. Keep multi-table queries but simplify JOIN conditions
    2. Use separate CTEs for each data source, then combine with json_build_object()
    3. Ensure all tables use the same schema type (all update.* OR all clean.*)
    4. Reduce complex calculations but keep the comparison logic
    5. LIMIT results to top 5 per category but keep all categories
    6. PRESERVE the intent to compare multiple data sources`;
  } else {
    // 🚨 THIRD RETRY: Fallback but still try to answer the full question
    retryStrategy = `FALLBACK APPROACH - Try to answer as much as possible:
    1. If multi-table fails, query each table separately
    2. Use UNION ALL with compatible columns to combine results
    3. Add table identification: SELECT 'lending' as type, symbol, apy, ...
    4. Still try to provide comparison data, even if simplified
    5. LIMIT 3 per category but show multiple categories if possible
    6. Last resort: single table with most relevant data`;
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
  const dateFields = ['ts', 'day', 'date', 'timestamp'];
  
  for (const field of dateFields) {
    if (firstRow[field]) {
      const date = firstRow[field];
      // Convert to readable format
      if (typeof date === 'string' && date.match(/^\d{4}-\d{2}-\d{2}/)) {
        return date; // Already in YYYY-MM-DD format
      }
      if (typeof date === 'number') {
        // Unix timestamp
        return new Date(date * 1000).toISOString().split('T')[0];
      }
    }
  }
  
  return null;
}






export async function generateAnswerFromResults(openai, question, rows, presentationHint, intent = 'standard_query', retryCount = 0) {
  // 🔍 SMART EMPTY RESULTS HANDLING - Learn what to suggest
  if (!rows || rows.length === 0) {
    const q = question.toLowerCase();
    
    // 💡 INTELLIGENT FALLBACK SUGGESTIONS based on query type
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
    
    // Legacy fallback categories
    if (q.includes('tvl') && (q.includes('grew') || q.includes('fastest') || q.includes('category'))) {
      return "I couldn't find sufficient data for this time period. Try asking about current lending rates or pool APYs instead.";
    }
    if (q.includes('backtest') || q.includes('simulation') || q.includes('portfolio')) {
      return "That analysis is too complex for our current data. Try simpler questions about current rates or prices.";
    }
    if (q.includes('strategy') || q.includes('allocation') || q.includes('optimal') || q.includes('risk tolerance')) {
      return "That requires investment advice beyond our data scope. Try asking about specific lending rates or pool APYs.";
    }
    
    return "We are in beta testing and don't have a good answer for that yet. Try asking about lending rates, pool APYs, or token prices.";
  }

  const style = presentationHint?.style || "concise";
  const include = presentationHint?.include_fields || [];

  // Extract date information from the results for data freshness
  const dataDate = extractDataDate(rows);
  const freshnessNote = dataDate ? ` (Data as of ${dataDate})` : "";

  // Use GPT-4.1 for all queries for speed (GPT-5 is too slow)
  const model = "gpt-4.1";

  // 🧠 ENHANCED ANALYTICS PROMPTS - Specialized prompts for advanced analytics
  let systemPrompt = `You are a helpful DeFi analytics assistant. Write a friendly, actionable answer using ONLY the provided query results.

FORMATTING REQUIREMENTS:
• Write in clear, conversational tone
• Use bullet points instead of tables for better readability
• Format with headers and organized bullet points
• Percentage values are already formatted - just add "%" sign  
• Format large numbers with commas (e.g., $1,234,567)
• Minimal use of bold/italic text
• Convert timestamps to readable dates

BULLET POINT FORMAT:
• Use clear headers followed by organized bullet points
• Example format:
  ## Top USDC Lending Rates
  • Aave V3 (Ethereum): 5.24% APY, $45.2M available
  • Compound V3: 4.87% APY, $23.1M available  
  • Morpho Blue: 6.12% APY, $12.8M available

• For pools, use format:
  ## High APY Liquidity Pools  
  • WETH-USDC: 12.45% APY, $1.2M TVL ✅ Low Risk
  • EXOTIC-TOKEN: 245.67% APY, $12K TVL ⚠️ High Risk

💡 CONTENT REQUIREMENTS:
• Provide actionable insights and suggestions based on the data
• Include context about what the numbers mean for users
• **BLUE CHIP PREFERENCE**: Explain why blue chip tokens (ETH, BTC, USDC, USDT, DAI) are safer choices
• Mention when exotic tokens offer exceptional yields but warn about higher risks
• Use section headers with emojis for better organization
• End with practical next steps or comparisons when relevant
• NEVER use hardcoded dates like "June 2024" - only use timestamps from the actual query results`;

  // 🧠 ADVANCED ANALYTICS ENHANCEMENT - Specialized prompts for sophisticated analysis
  if (intent === 'advanced_analytics' || intent === 'risk_analysis' || intent === 'outlier_detection' || 
      intent === 'seasonality_analysis' || intent === 'yield_curve_analysis') {
    
    systemPrompt = `You are an advanced DeFi quantitative analyst. Interpret sophisticated financial analytics with precision and clarity.

BASIC ANALYTICS (when requested):
• Provide simple statistical context
• Use clean formatting without excessive markdown
• Keep responses focused and practical
• **SECTION HEADERS**: Use emojis and clear organization (📈 📊 💡 ⚠️ ✅)
• **NUMBER FORMATTING**: Add commas for readability ($1,234,567, not $1234567)

📊 ANALYTICAL REQUIREMENTS:
• **PRACTICAL TRANSLATION**: Convert statistical measures into actionable investment insights
• **RISK CONTEXT**: Always explain both opportunities AND risks revealed by the analysis
• **COMPARATIVE ANALYSIS**: When showing multiple assets, rank and compare their metrics
• **CONFIDENCE LEVELS**: Mention statistical significance and confidence when relevant

🎯 KEY METRICS TO INTERPRET:
• **Volatility/STDDEV**: Rate stability → "Higher volatility (5.2%) means less predictable returns"
• **Correlation**: Relationship strength → "Strong correlation (0.85) means these assets move together"  
• **Moving Averages**: Trend direction → "7-day MA above current rate suggests downward momentum"
• **Percentiles**: Distribution position → "Q75 of 8.5% means 75% of pools yield less than this"
• **Risk-Adjusted Scores**: Return efficiency → "Score of 2.1 indicates excellent risk-adjusted returns"
• **Outliers**: Exceptional cases → "2-sigma outlier suggests unusual market conditions"

💡 PRESENTATION STYLE:
• Write in expert but accessible tone with visual appeal
• Use section headers with emojis for better organization
• Provide specific actionable recommendations based on the statistical analysis
• Always include data timestamps and explain what the analysis reveals about market conditions
• Create summary boxes for key insights`;
  }

  try {
    const resp = await openai.chat.completions.create({
      model,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: `Question: ${stripInvisibles(question)}\n\nQuery Results (JSON): ${JSON.stringify(rows).slice(0, 100000)}${dataDate ? `\n\nData Date: ${dataDate}` : ""}` },
      ],
    });

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

/* ------------------------------ Answer stage ------------------------------ */
export async function generateAnswer(openai, question, rows, presentationHint) {
  if (!rows || rows.length === 0) return "No rows returned for that query.";

  const style = presentationHint?.style || "concise";
  const include = presentationHint?.include_fields || [];

  const sys = [
    "Write a helpful, conversational DeFi analytics answer using ONLY fields provided.",
    "Use friendly tone with actionable insights (use 'you can', 'consider', 'this shows').",
    "ALL percentage values are ALREADY in percentage format - just add '%' sign, do NOT multiply by 100.",
    "Convert timestamps to human-friendly dates: Unix timestamps and ISO dates → 'September 29, 2025'.",
    "End with 'as of [friendly_date]' and include practical next steps when relevant.",
    "No tables.",
  ];
  if (style === "bulleted") sys.push("If helpful, use at most 3 bullets.");

  try {
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
  } catch (error) {
    console.error('OpenAI API Error in generateAnswer:', {
      status: error.status,
      code: error.code,
      type: error.type,
      message: error.message,
      headers: error.headers,
      request_id: error.request_id,
      question_length: question.length,
      rows_count: rows?.length,
      presentation_hint: presentationHint
    });
    throw error;
  }
}
