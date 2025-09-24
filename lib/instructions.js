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

export function detectQueryIntent(question) {
  const lowerQuestion = question.toLowerCase();
  
  // Backtesting intents
  if (lowerQuestion.includes('what if') || lowerQuestion.includes('backtest') || 
      lowerQuestion.includes('if i bought') || lowerQuestion.includes('if i lent') ||
      lowerQuestion.includes('would it be worth') || lowerQuestion.includes('investment return')) {
    
    if (lowerQuestion.includes('bought') || lowerQuestion.includes('buy') || 
        lowerQuestion.includes('purchase') || lowerQuestion.includes('invested')) {
      return 'backtest_buy';
    }
    
    if (lowerQuestion.includes('lent') || lowerQuestion.includes('lend') || 
        lowerQuestion.includes('staked') || lowerQuestion.includes('supply')) {
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

IMPORTANT - Data Freshness:
• For time-series data (tables with 'ts', 'day', or date columns), ALWAYS prefer the most recent data unless specifically asked for historical data.
• For TVL calculations: Use MAX(ts) or MAX(day) to get the latest date, then filter to that date.
• For aggregate totals: Filter to the most recent date to avoid summing across all historical data.
• Only use historical date ranges when explicitly requested (e.g., "last week", "in January", "trend over time").

BACKTESTING RULES:
• Buy-and-hold backtests: Get price on start_date and end_date for same symbol/chain from clean.token_price_daily_enriched
• Lending backtests: Get APY history from clean.lending_market_history for date range, use total APY (base + reward)
• Forecasting: Get recent APY data (last 60 days) and calculate average/trend
• Always include confidence filters: confidence > 0.8 for prices, filter by symbol/protocol
• For backtests, extract dates from question or use reasonable defaults (e.g., "2024-01-01" for start)

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

/* ------------------------------ Backtesting Query Builders ------------------------------ */
export function buildBuyAndHoldQuery(asset, startDate, endDate) {
  return {
    sql: `SELECT 
      event_time, 
      price_usd, 
      symbol 
    FROM clean.token_price_daily_enriched 
    WHERE symbol ILIKE '${asset}' 
      AND confidence > 0.8 
      AND event_time::date >= '${startDate}' 
      AND event_time::date <= '${endDate}' 
      AND price_usd IS NOT NULL 
    ORDER BY event_time ASC
    LIMIT 1000`
  };
}

export function buildLendingAPYQuery(asset, startDate, endDate) {
  return {
    sql: `SELECT 
      ts, 
      apy_base_supply, 
      apy_reward_supply, 
      symbol, 
      project 
    FROM clean.lending_market_history 
    WHERE symbol ILIKE '${asset}' 
      AND ts >= EXTRACT(EPOCH FROM '${startDate}'::date) 
      AND ts <= EXTRACT(EPOCH FROM '${endDate}'::date) 
      AND apy_base_supply IS NOT NULL 
    ORDER BY ts ASC
    LIMIT 1000`
  };
}

export function buildAPYForecastQuery(asset, lookbackDays = 60) {
  return {
    sql: `SELECT 
      ts, 
      apy_base_supply, 
      apy_reward_supply, 
      symbol, 
      project 
    FROM clean.lending_market_history 
    WHERE symbol ILIKE '${asset}' 
      AND ts >= EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '${lookbackDays} days'))
      AND apy_base_supply IS NOT NULL 
    ORDER BY ts ASC
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
  const startPrice = sortedData[0].price_usd;
  const endPrice = sortedData[sortedData.length - 1].price_usd;
  const startDate = sortedData[0].event_time;
  const endDate = sortedData[sortedData.length - 1].event_time;
  
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
  
  // Calculate daily compounding (flat price scenario)
  let currentValue = amountUsd;
  let totalDays = 0;
  
  for (let i = 0; i < sortedData.length - 1; i++) {
    const currentRow = sortedData[i];
    const nextRow = sortedData[i + 1];
    
    // Calculate days between snapshots
    const currentDate = new Date(currentRow.ts * 1000);
    const nextDate = new Date(nextRow.ts * 1000);
    const daysDiff = Math.floor((nextDate - currentDate) / (1000 * 60 * 60 * 24));
    
    if (daysDiff > 0) {
      // Use total APY (base + reward) if available, otherwise just base
      const dailyAPY = (currentRow.apy_base_supply + (currentRow.apy_reward_supply || 0)) / 365;
      
      // Compound daily
      for (let day = 0; day < daysDiff; day++) {
        currentValue = currentValue * (1 + dailyAPY);
        totalDays++;
      }
    }
  }
  
  const totalReturn = currentValue - amountUsd;
  const percentReturn = (totalReturn / amountUsd) * 100;
  const annualizedReturn = (Math.pow(currentValue / amountUsd, 365 / totalDays) - 1) * 100;
  
  const result = {
    amount_usd: amountUsd,
    start_date: startDate,
    end_date: endDate,
    final_value: currentValue,
    total_return: totalReturn,
    percent_return: percentReturn,
    annualized_return: annualizedReturn,
    total_days: totalDays,
    average_daily_apy: ((currentValue / amountUsd) ** (365 / totalDays) - 1) * 100,
    scenario: "flat_price"
  };

  // If we have price data, calculate price-adjusted scenario
  if (priceData && priceData.length >= 2) {
    const sortedPriceData = priceData.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
    const startPrice = sortedPriceData[0].price_usd;
    const endPrice = sortedPriceData[sortedPriceData.length - 1].price_usd;
    const priceChangeMultiplier = endPrice / startPrice;
    
    // Price-adjusted final value = yield returns * price change
    const priceAdjustedValue = currentValue * priceChangeMultiplier;
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
  
  // Calculate statistics
  const apyValues = recentData.map(row => row.apy_base_supply + (row.apy_reward_supply || 0));
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
• Format values in the 0–1 range as percentages with 2 decimals.
• Format dates in plain English.
• Prefer natural phrasing.
• No tables.${style === "bulleted" ? " If helpful, use at most 3 bullets." : ""}
• IMPORTANT: Always mention data freshness at the end of your answer using the format "(Data as of [date])" if a date is available.`;

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