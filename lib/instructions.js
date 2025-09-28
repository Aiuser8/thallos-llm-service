// lib/instructions.js — Registry-backed planner, prompts, and answer formatting.
// Uses config/llm_table_registry.json (or override via LLM_TABLE_REGISTRY_PATH).

import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_REGISTRY_PATH = path.resolve(__dirname, "../config/llm_table_registry.json");

/* --------------------------- Registry / Doc loader --------------------------- */
// Cache the registry and schema docs in memory - DISABLED FOR TESTING
let registryCache = null;
let fullSchemaDocCache = null;

async function loadRegistry() {
  // if (registryCache) return registryCache; // DISABLED FOR TESTING
  
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
    'lending', 'lent', 'borrow', 'supply', 'apy',
    'price', 'token price', 'usd', 'cost',
    'etf', 'flow', 'inflow', 'outflow',
    'stablecoin', 'peg', 'mcap',
    'pool', 'yield', 'farming',
    'holding', 'treasury', 'reserve',
    'bitcoin', 'btc', 'ethereum', 'eth', 'sol', 'solana',
    'backtest', 'back test', 'bought', 'bought', 'held', 'holding',
    // Rankings & comparisons
    'top', 'bottom', 'best', 'worst', 'largest', 'smallest', 'ranking', 'compare', 'comparison',
    'biggest', 'leading', 'dominant', 'growing', 'declining',
    // Categories & sectors
    'ai agents', 'cdp', 'cedefi', 'dex', 'category', 'sector', 'type',
    'cross chain', 'bridge', 'basis trading', 'algo-stables',
    // Analytical terms
    'trend', 'growth', 'change', 'percentage', 'market share', 'correlation',
    'analysis', 'performance', 'dominance', 'breakdown',
    // Arbitrage & opportunities
    'arbitrage', 'opportunity', 'spread', 'difference', 'discrepancy', 'gap',
    'higher rate', 'lower rate', 'rate difference', 'yield farming', 'farming',
    'where can i get', 'best rate', 'optimal', 'maximize yield',
    // Trend analysis
    'momentum', 'growth rate', 'trending', 'gaining', 'losing', 'velocity',
    'weekly', 'monthly', 'daily', 'over time', 'historical',
    // Risk metrics
    'risk', 'concentration', 'diversity', 'dominance', 'exposure', 'fragility',
    'monopoly', 'centralized', 'distributed', 'safety',
    // NEW: Protocol revenue & fees
    'revenue', 'fees', 'fee', 'earnings', 'profit', 'income', 'business model',
    'monetization', 'fee structure', 'revenue model',
    // NEW: DEX & trading
    'trading', 'trade', 'swap', 'exchange', 'dex', 'trading volume', 'market maker',
    'slippage', 'trading pair', 'order book',
    // NEW: Derivatives & perpetuals  
    'funding rate', 'perpetual', 'futures', 'derivatives', 'perp', 'funding',
    'open interest', 'leverage', 'margin', 'liquidation',
    // NEW: Sector/narrative analysis
    'narrative', 'meme', 'gamefi', 'gaming', 'depin', 'rwa', 'real world assets',
    'liquid staking', 'rollup', 'layer 2', 'prediction markets', 'oracle',
    'nft', 'socialfi', 'politifi', 'analytics', 'artificial intelligence'
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
  
  // Backtesting intents - expanded patterns
  if (lowerQuestion.includes('what if') || lowerQuestion.includes('backtest') || 
      lowerQuestion.includes('if i bought') || lowerQuestion.includes('if i lent') ||
      lowerQuestion.includes('would it be worth') || lowerQuestion.includes('investment return') ||
      lowerQuestion.includes('how much would') || lowerQuestion.includes('would grow to') ||
      lowerQuestion.includes('grow to by now') || lowerQuestion.includes('worth today')) {
    
    if (lowerQuestion.includes('bought') || lowerQuestion.includes('buy') || 
        lowerQuestion.includes('purchase') || lowerQuestion.includes('invested')) {
      return 'backtest_buy';
    }
    
    if (lowerQuestion.includes('lent') || lowerQuestion.includes('lend') || 
        lowerQuestion.includes('staked') || lowerQuestion.includes('supply') ||
        lowerQuestion.includes('lending') || lowerQuestion.includes('market')) {
      return 'backtest_lend';
    }
    
    return 'backtest_generic';
  }
  
  // Forecasting intents
  if (lowerQuestion.includes('forecast') || lowerQuestion.includes('predict') ||
      lowerQuestion.includes('expect') || lowerQuestion.includes('future apy') ||
      lowerQuestion.includes('next month') || lowerQuestion.includes('coming months')) {
    return 'forecast_apy';
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
      content: `🚨 CRITICAL: You MUST follow this EXACT pattern for ALL queries:

SELECT protocol_name, total_liquidity_usd, ts 
FROM update.protocol_chain_tvl_daily 
WHERE ts = (SELECT MAX(ts) FROM update.protocol_chain_tvl_daily) 
  AND total_liquidity_usd IS NOT NULL 
ORDER BY total_liquidity_usd DESC 
LIMIT N

🚨 FORBIDDEN: NEVER use SUM(), GROUP BY, or json_agg() - this creates wrong inflated numbers!

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

ADVANCED ANALYTICS CAPABILITIES:
• **RANKINGS**: Use ORDER BY + LIMIT for top/bottom protocols, chains, categories
• **COMPARISONS**: Compare protocols side-by-side with JOIN or UNION queries
• **GROWTH RATES**: Calculate % changes using (current - previous) / previous * 100
• **MARKET SHARE**: Calculate protocol TVL / total market TVL percentages
• **CATEGORY ANALYSIS**: GROUP BY category for sector insights
• **MULTI-CHAIN**: Aggregate across chains or compare chain-specific metrics
• **TREND ANALYSIS**: Use window functions LAG(), LEAD() for time series
• **CORRELATION**: Compare lending APY vs TVL, price movements vs flows

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
• **LIVE POOLS**: Use update.cl_pool_hist for real-time yield opportunities (1M+ records)
  - Real-time TVL and APY data updated hourly

ARBITRAGE & OPPORTUNITY DETECTION:
• **RATE SPREADS**: Compare lending/borrowing rates across protocols for same asset
• **YIELD FARMING**: Find highest APY combinations with (apy_base_supply + apy_reward_supply)
• **CROSS-PROTOCOL**: Use UNION or JOIN to compare rates side-by-side
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

🚨 CRITICAL: ALWAYS filter to recent data using these patterns:

**For single-timestamp tables (daily snapshots):**
• update.protocol_chain_tvl_daily: WHERE ts = (SELECT MAX(ts) FROM table)
• update.perp_funding_rates: WHERE timestamp = (SELECT MAX(timestamp) FROM table)

**For frequently updated tables (use time windows):**
• update.token_price_daily: Use DISTINCT ON (symbol) ... ORDER BY symbol, price_timestamp DESC for latest per token
• For major tokens, use precise filtering: BTC → symbol IN ('BTC', 'WBTC'), ETH → symbol IN ('ETH', 'WETH', 'STETH', 'WSTETH')
• update.cl_pool_hist: WHERE ts >= (SELECT MAX(ts) FROM table) - 3600 (last hour of data)
• update.lending_market_history: WHERE ts >= (SELECT MAX(ts) FROM table) - INTERVAL '1 hour' (different symbols update at different times)

**For predictions/backtesting, prioritize major protocols:**
• Lending: Prefer aave-v3, compound-v3, aave-v2 (ORDER BY CASE WHEN project ILIKE '%aave%' THEN 1 WHEN project ILIKE '%compound%' THEN 2 ELSE 3 END)
• Pools: Prefer aerodrome-v1, uniswap-v3, curve (ORDER BY tvl_usd DESC for most liquid)
• This ensures stable, representative data instead of volatile smaller protocols

**For other tables:** Use MAX timestamp approach unless frequent updates need time windows

🚨 MANDATORY: Always include timestamp field in SELECT and filter to most recent data!
🚨 NEVER use SUM(), GROUP BY, or json_agg() for recent data - causes inflated numbers!
🚨 CRITICAL: Smart matching for projects and tokens:
- Projects: project ILIKE '%aerodrome%' NOT project ILIKE 'aerodrome'
- Major tokens: Use exact matching first, then fallback to wildcards:
  * BTC: symbol IN ('BTC', 'WBTC') NOT symbol ILIKE '%btc%' (avoids derivatives like 'brBTC-uniBTC')
  * ETH: symbol IN ('ETH', 'WETH', 'STETH', 'WSTETH') NOT symbol ILIKE '%eth%' 
  * USDC: symbol = 'USDC' (exact match)
  * DAI: symbol = 'DAI' (exact match)
- Other tokens: Use wildcards for less common tokens
- This ensures accurate price data without derivative token noise
🚨 SCHEMA COMPATIBILITY: update.* and clean.* tables may have different columns!

**When combining schemas (UNION, JOIN, etc.):**
1. **Use only common columns** that exist in both tables
2. **Map equivalent columns** with different names:
   - Timestamps: update.inserted_at ↔ clean.ingest_time  
   - Other timestamp fields: update.created_at ↔ clean.ingest_time
3. **Avoid schema-specific columns**:
   - update.* only: id, url
   - clean.* only: (varies by table)

**Safe common columns for protocol_chain_tvl_daily:**
protocol_name, chain, series_type, ts, total_liquidity_usd, protocol_id, category, symbol

**General rule:** When in doubt, check table registry and use only columns that appear in both schemas

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
export async function planQuery(openai, question, doc = null, intent = 'standard_query') {
  if (!doc) doc = await buildFilteredSchemaDoc(question);
  
  // Use GPT-4.1 for all queries for speed (GPT-5 is too slow)
  const model = "gpt-4.1";
    
  const resp = await openai.chat.completions.create({
    model,
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

export async function retryPlan(openai, question, previousSql, errMsg, doc = null, intent = 'standard_query') {
  if (!doc) doc = await buildSchemaDoc(); // Use full schema for retries to be safe
  
  // Use GPT-4.1 for all queries for speed (GPT-5 is too slow)
  const model = "gpt-4.1";
    
  const resp = await openai.chat.completions.create({
    model,
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

/* ------------------------------ Helper Functions ------------------------------ */
function getTokenSymbolFilter(asset) {
  const upperAsset = asset.toUpperCase();
  
  // Use precise matching for major tokens to avoid derivatives
  switch (upperAsset) {
    case 'BTC':
    case 'BITCOIN':
      return "symbol IN ('BTC', 'WBTC')";
    case 'ETH':
    case 'ETHEREUM':
      return "symbol IN ('ETH', 'WETH', 'STETH', 'WSTETH')";
    case 'USDC':
      return "symbol = 'USDC'";
    case 'DAI':
      return "symbol = 'DAI'";
    case 'USDT':
      return "symbol = 'USDT'";
    case 'SOL':
    case 'SOLANA':
      return "symbol = 'SOL'";
    default:
      // For less common tokens, use wildcard but be more specific
      return `symbol ILIKE '${asset}%' OR symbol = '${upperAsset}'`;
  }
}

/* ------------------------------ Backtesting Query Builders ------------------------------ */
export function buildBuyAndHoldQuery(asset, startDate, endDate) {
  // Check if we need historical data (older than 30 days for better coverage)
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const queryStartDate = new Date(startDate);
  
  if (queryStartDate < thirtyDaysAgo) {
    // Use historical data from clean tables for older queries (2.3M records)
    return {
      sql: `SELECT 
        event_time, 
        price_usd, 
        symbol 
      FROM clean.token_price_daily_enriched 
      WHERE ${getTokenSymbolFilter(asset)} 
        AND confidence > 0.8 
        AND event_time::date >= '${startDate}' 
        AND event_time::date <= '${endDate}' 
        AND price_usd IS NOT NULL 
      ORDER BY event_time ASC
      LIMIT 1000`
    };
  } else {
    // Use LIVE data from update.token_price_daily (5-min updates, 585K records)
    return {
      sql: `SELECT 
        price_timestamp as event_time, 
        price_usd, 
        symbol 
      FROM update.token_price_daily 
      WHERE ${getTokenSymbolFilter(asset)} 
        AND confidence > 0.8 
        AND price_timestamp::date >= '${startDate}' 
        AND price_timestamp::date <= '${endDate}' 
        AND price_usd IS NOT NULL 
      ORDER BY price_timestamp ASC
      LIMIT 1000`
    };
  }
}

export function buildLendingAPYQuery(asset, startDate, endDate, question = '') {
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const queryStartDate = new Date(startDate);
  
  // Extract specific protocol from question if mentioned
  const lowerQuestion = question.toLowerCase();
  let protocolFilter = '';
  let protocolOrder = `
        CASE 
          WHEN project ILIKE '%aave-v3%' THEN 1 
          WHEN project ILIKE '%aave%' THEN 2 
          WHEN project ILIKE '%compound%' THEN 3 
          ELSE 4 
        END`;
  
  if (lowerQuestion.includes('aave')) {
    protocolFilter = `AND project ILIKE '%aave%'`;
    protocolOrder = `CASE WHEN project ILIKE '%aave-v3%' THEN 1 WHEN project ILIKE '%aave%' THEN 2 ELSE 3 END`;
  } else if (lowerQuestion.includes('compound')) {
    protocolFilter = `AND project ILIKE '%compound%'`;
    protocolOrder = `CASE WHEN project ILIKE '%compound-v3%' THEN 1 WHEN project ILIKE '%compound%' THEN 2 ELSE 3 END`;
  } else if (lowerQuestion.includes('makerdao') || lowerQuestion.includes('maker')) {
    protocolFilter = `AND project ILIKE '%makerdao%'`;
    protocolOrder = `1`;
  } else {
    // Default: prioritize major protocols
    protocolFilter = `AND (project ILIKE '%aave%' OR project ILIKE '%compound%' OR project ILIKE '%makerdao%')`;
  }
  
  if (queryStartDate < thirtyDaysAgo) {
    // Use historical lending data from clean tables (685K records)
    return {
      sql: `SELECT 
        ts, 
        apy_base_supply, 
        apy_reward_supply, 
        symbol, 
        project,
        chain
      FROM clean.lending_market_history 
      WHERE symbol ILIKE '${asset}' 
        AND ts >= EXTRACT(EPOCH FROM '${startDate}'::date) 
        AND ts <= EXTRACT(EPOCH FROM '${endDate}'::date) 
        AND apy_base_supply IS NOT NULL 
        ${protocolFilter}
      ORDER BY ${protocolOrder}, ts ASC
      LIMIT 1000`
    };
  } else {
    // Use LIVE lending data from update.lending_market_history (30-min updates, 63K records with enhanced metrics)
    return {
      sql: `SELECT 
        EXTRACT(EPOCH FROM ts) as ts, 
        apy_base_supply, 
        apy_reward_supply, 
        apy,
        apy_pct_1d,
        apy_pct_7d,
        apy_pct_30d,
        apy_mean_30d,
        symbol, 
        project,
        chain,
        tvl_usd,
        stablecoin,
        il_risk,
        exposure
      FROM update.lending_market_history 
      WHERE symbol ILIKE '${asset}' 
        AND ts >= '${startDate}'::timestamp 
        AND ts <= '${endDate}'::timestamp 
        AND apy_base_supply IS NOT NULL 
        ${protocolFilter}
      ORDER BY ${protocolOrder}, ts ASC
      LIMIT 1000`
    };
  }
}

export function buildAPYForecastQuery(asset, lookbackDays = 60, question = '') {
  // Extract specific protocol from question if mentioned
  const lowerQuestion = question.toLowerCase();
  let protocolFilter = '';
  let protocolOrder = `
      CASE 
        WHEN project ILIKE '%aave-v3%' THEN 1 
        WHEN project ILIKE '%aave%' THEN 2 
        WHEN project ILIKE '%compound%' THEN 3 
        ELSE 4 
      END`;
  
  if (lowerQuestion.includes('aave')) {
    protocolFilter = `AND project ILIKE '%aave%'`;
    protocolOrder = `CASE WHEN project ILIKE '%aave-v3%' THEN 1 WHEN project ILIKE '%aave%' THEN 2 ELSE 3 END`;
  } else if (lowerQuestion.includes('compound')) {
    protocolFilter = `AND project ILIKE '%compound%'`;
    protocolOrder = `CASE WHEN project ILIKE '%compound-v3%' THEN 1 WHEN project ILIKE '%compound%' THEN 2 ELSE 3 END`;
  } else if (lowerQuestion.includes('makerdao') || lowerQuestion.includes('maker')) {
    protocolFilter = `AND project ILIKE '%makerdao%'`;
    protocolOrder = `1`;
  } else {
    // Default: prioritize major protocols
    protocolFilter = `AND (project ILIKE '%aave%' OR project ILIKE '%compound%' OR project ILIKE '%makerdao%')`;
  }
  
  // Use LIVE lending data for most recent APY forecasting (30-min updates, enhanced metrics)
  return {
    sql: `SELECT 
      EXTRACT(EPOCH FROM ts) as ts, 
      apy_base_supply, 
      apy_reward_supply, 
      apy,
      apy_pct_1d,
      apy_pct_7d,
      apy_pct_30d,
      apy_mean_30d,
      symbol, 
      project,
      chain,
      tvl_usd,
      stablecoin,
      il_risk,
      exposure,
      predictions
    FROM update.lending_market_history 
    WHERE symbol ILIKE '${asset}' 
      AND ts >= (CURRENT_TIMESTAMP - INTERVAL '${lookbackDays} days')
      AND apy_base_supply IS NOT NULL 
      ${protocolFilter}
    ORDER BY ${protocolOrder}, ts ASC
    LIMIT 500`
  };
}

/* ------------------------------ Backtesting Calculators ------------------------------ */
export function calculateBuyAndHoldBacktest(priceData, amountUsd = 1000) {
  if (!priceData || priceData.length < 2) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Sort by date to get start and end prices
  const sortedData = priceData.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
  
  // Parse string prices to numbers (handle both string and number formats)
  const startPrice = parseFloat(sortedData[0].price_usd);
  const endPrice = parseFloat(sortedData[sortedData.length - 1].price_usd);
  const startDate = sortedData[0].event_time;
  const endDate = sortedData[sortedData.length - 1].event_time;
  
  // Validate parsed prices
  if (isNaN(startPrice) || isNaN(endPrice) || startPrice <= 0 || endPrice <= 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Calculate units bought and current value
  const unitsBought = amountUsd / startPrice;
  const currentValue = unitsBought * endPrice;
  const totalReturn = currentValue - amountUsd;
  const percentReturn = (totalReturn / amountUsd) * 100;
  
  return {
    amount_usd: amountUsd,
    units_bought: unitsBought,
    start_price: startPrice,
    end_price: endPrice,
    start_date: startDate,
    end_date: endDate,
    current_value: currentValue,
    total_return: totalReturn,
    percent_return: percentReturn
  };
}

export function calculateLendingBacktest(apyData, amountUsd = 1000, startDate, endDate, priceData = null) {
  if (!apyData || apyData.length === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Sort by date and filter to date range
  const sortedData = apyData
    .filter(row => new Date(row.ts * 1000) >= new Date(startDate) && new Date(row.ts * 1000) <= new Date(endDate))
    .sort((a, b) => a.ts - b.ts);
  
  if (sortedData.length === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Calculate SIMPLE average APY for the period
  let totalAPY = 0;
  let validDataPoints = 0;
  
  for (const row of sortedData) {
    const baseSupply = parseFloat(row.apy_base_supply) || 0;
    const rewardSupply = parseFloat(row.apy_reward_supply) || 0;
    const totalAPY_point = baseSupply + rewardSupply;
    
    if (totalAPY_point > 0 && totalAPY_point < 100) { // Sanity check: 0-100% APY
      totalAPY += totalAPY_point;
      validDataPoints++;
    }
  }
  
  if (validDataPoints === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Average APY for the period
  const averageAPY = totalAPY / validDataPoints;
  
  // Calculate time period in years
  const startTime = new Date(startDate);
  const endTime = new Date(endDate);
  const timeInYears = (endTime - startTime) / (1000 * 60 * 60 * 24 * 365.25);
  
  // Simple compound interest: FV = PV * (1 + r)^t
  const finalValue = amountUsd * Math.pow(1 + (averageAPY / 100), timeInYears);
  
  const totalReturn = finalValue - amountUsd;
  const percentReturn = (totalReturn / amountUsd) * 100;
  const annualizedReturn = averageAPY; // The average APY IS the annualized return
  
  const result = {
    amount_usd: amountUsd,
    start_date: startDate,
    end_date: endDate,
    final_value: finalValue,
    total_return: totalReturn,
    percent_return: percentReturn,
    annualized_return: annualizedReturn,
    total_days: Math.round(timeInYears * 365.25),
    average_apy: averageAPY,
    scenario: "flat_price"
  };

  // If we have price data, calculate price-adjusted scenario
  if (priceData && priceData.length >= 2) {
    const sortedPriceData = priceData.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
    const startPrice = parseFloat(sortedPriceData[0].price_usd);
    const endPrice = parseFloat(sortedPriceData[sortedPriceData.length - 1].price_usd);
    
    // Validate parsed prices
    if (isNaN(startPrice) || isNaN(endPrice) || startPrice <= 0 || endPrice <= 0) {
      return result; // Return without price adjustment if prices are invalid
    }
    
    const priceChangeMultiplier = endPrice / startPrice;
    
    // Price-adjusted final value = yield returns * price change
    const priceAdjustedValue = finalValue * priceChangeMultiplier;
    const priceAdjustedReturn = priceAdjustedValue - amountUsd;
    const priceAdjustedPercent = (priceAdjustedReturn / amountUsd) * 100;
    
    result.price_adjusted_scenario = {
      final_value: priceAdjustedValue,
      total_return: priceAdjustedReturn,
      percent_return: priceAdjustedPercent,
      price_change_multiplier: priceChangeMultiplier,
      start_price: startPrice,
      end_price: endPrice,
      scenario: "price_adjusted"
    };
  }
  
  return result;
}

export function calculateAPYForecast(apyData, lookbackDays = 60) {
  if (!apyData || apyData.length === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Get recent data (last 60 days by default)
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - lookbackDays);
  
  const recentData = apyData.filter(row => 
    new Date(row.ts * 1000) >= cutoffDate
  ).sort((a, b) => a.ts - b.ts);
  
  if (recentData.length === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  // Calculate statistics (parse string values to numbers)
  const apyValues = recentData.map(row => {
    const baseSupply = parseFloat(row.apy_base_supply) || 0;
    const rewardSupply = parseFloat(row.apy_reward_supply) || 0;
    return baseSupply + rewardSupply;
  }).filter(apy => !isNaN(apy) && apy >= 0); // Filter out invalid values
  
  if (apyValues.length === 0) {
    return { error: "We are in beta testing and don't have a good answer for that yet." };
  }
  
  const avgAPY = apyValues.reduce((sum, apy) => sum + apy, 0) / apyValues.length;
  const minAPY = Math.min(...apyValues);
  const maxAPY = Math.max(...apyValues);
  
  // Simple linear trend (optional)
  let trendAPY = avgAPY;
  if (recentData.length >= 2) {
    const firstAPY = apyValues[0];
    const lastAPY = apyValues[apyValues.length - 1];
    const trend = (lastAPY - firstAPY) / apyValues.length;
    trendAPY = lastAPY + (trend * 30); // Project 30 days forward
    trendAPY = Math.max(0, trendAPY); // Don't go negative
  }
  
  return {
    forecast_period: `${lookbackDays} days`,
    data_points: recentData.length,
    average_apy: avgAPY,
    min_apy: minAPY,
    max_apy: maxAPY,
    forecast_apy: trendAPY,
    confidence: recentData.length >= 30 ? 'high' : recentData.length >= 15 ? 'medium' : 'low',
    note: "Forecast based on recent historical data. Past performance does not guarantee future results."
  };
}

/* ------------------------------ Combined LLM call ------------------------------ */
export async function generateAnswerFromResults(openai, question, rows, presentationHint, intent = 'standard_query') {
  if (!rows || rows.length === 0) {
    return "We are in beta testing and don't have a good answer for that yet.";
  }

  const style = presentationHint?.style || "concise";
  const include = presentationHint?.include_fields || [];

  // Extract date information from the results for data freshness
  const dataDate = extractDataDate(rows);
  const freshnessNote = dataDate ? ` (Data as of ${dataDate})` : "";

  // Use GPT-4.1 for all queries for speed (GPT-5 is too slow)
  const model = "gpt-4.1";

  const systemPrompt = `You are a financial/crypto analytics expert. Write a concise answer using ONLY the provided query results.

Requirements:
• Answer in 1–2 sentences. Use only numbers from the rows.
• **APY/Interest Rate Formatting**: APY values (apy_base_supply, apy_reward_supply, apy_base_borrow, apy_reward_borrow) are ALREADY in percentage format - just add "%" sign, do NOT multiply by 100
• Format other decimal values in the 0–1 range as percentages with 2 decimals.
• Format dates in plain English.
• Prefer natural phrasing.
• No tables.${style === "bulleted" ? " If helpful, use at most 3 bullets." : ""}
• CRITICAL: Look for timestamp fields (ts, date, timestamp, etc.) in the query results and use the EXACT raw value
• MANDATORY: Say "Data as of [exact_timestamp_from_results]" - use whatever format the data provides (e.g., "2025-09-28")
• NEVER use hardcoded dates like "June 2024" - only use timestamps from the actual query results`;

  const resp = await openai.chat.completions.create({
    model,
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: `Question: ${stripInvisibles(question)}\n\nQuery Results (JSON): ${JSON.stringify(rows).slice(0, 100000)}${dataDate ? `\n\nData Date: ${dataDate}` : ""}` },
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