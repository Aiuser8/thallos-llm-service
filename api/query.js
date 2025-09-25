// api/query.js — Vercel Node serverless handler (ESM)
import OpenAI from "openai";
import pg from "pg";
import { planQuery, retryPlan, generateAnswerFromResults, isQuestionInDataScope, handleGeneralKnowledgeQuestion, detectQueryIntent, calculateBuyAndHoldBacktest, calculateLendingBacktest, calculateAPYForecast, buildBuyAndHoldQuery, buildLendingAPYQuery, buildAPYForecastQuery } from "../lib/instructions.js";

export const config = { runtime: "nodejs" }; // optionally: { runtime: "nodejs", regions: ["iad1"] }

// Create and cache the database connection pool
let dbPool = null;

function getDbPool() {
  if (!dbPool) {
    const url = new URL(process.env.DATABASE_URL);
    dbPool = new pg.Pool({
      host: url.hostname, // e.g., aws-1-us-east-2.pooler.supabase.com
      port: Number(url.port || 5432),
      user: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
      database: url.pathname.replace(/^\//, ""), // "postgres"
      // Force TLS but relax CA validation to avoid SELF_SIGNED_CERT_IN_CHAIN in Vercel
      ssl: { rejectUnauthorized: false },
      // Connection pool configuration for better performance
      max: 20, // Maximum number of clients in the pool
      idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
      connectionTimeoutMillis: 2000, // Return an error after 2 seconds if connection could not be established
    });
  }
  return dbPool;
}

// Read JSON safely whether req.body exists or not
async function readJson(req) {
  if (req.body && typeof req.body === "object") return req.body;
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try {
    return JSON.parse(raw);
  } catch {
    // Non-JSON caller (e.g., GET or webhook without body)
    return {};
  }
}

export default async function handler(req, res) {
  try {
    // Accept POST (JSON body) and GET (?q=...) to support webhooks/cron
    if (req.method !== "POST" && req.method !== "GET") {
      res.setHeader("Allow", "POST, GET");
      return res.status(405).json({ error: "Method Not Allowed" });
    }

    // Accept from JSON body OR query string (?q= / ?question=)
    const urlObj = new URL(req.url, `https://${req.headers.host}`);
    const qsQuestion = urlObj.searchParams.get("q") || urlObj.searchParams.get("question");

    const body = await readJson(req);
    const question = body?.question || qsQuestion;
    const minimal =
      body?.minimal === true || urlObj.searchParams.get("minimal") === "true";
    const presentationHint = body?.presentationHint;

    if (!question) {
      return res.status(400).json({
        error:
          "Missing 'question'. Provide JSON body {\"question\":\"...\"} or use ?q= in the URL.",
        exampleCurl:
          `curl -H "content-type: application/json" -d '{"question":"What was Ethereum TVL on the most recent date?"}' ` +
          `${urlObj.origin}${urlObj.pathname}`,
      });
    }

    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // Check if question is within data scope or needs general knowledge handling
    const inDataScope = await isQuestionInDataScope(question);
    
    if (!inDataScope) {
      // Handle as general knowledge question
      const answer = await handleGeneralKnowledgeQuestion(openai, question);
      return res.status(200).json({ 
        answer, 
        source: "general_knowledge",
        note: "This question was answered using general knowledge rather than database queries."
      });
    }

    // Detect query intent for backtesting/forecasting
    const intent = detectQueryIntent(question);

    // 1) Plan SQL or use specific backtesting queries
    let { sql } = await planQuery(openai, question, null, intent);
    
    // Override with optimized queries for backtesting
    if (intent === 'backtest_buy') {
      const assetMatch = question.match(/BTC|ETH|USDC|DAI/i);
      const asset = assetMatch ? assetMatch[0] : 'BTC';
      const yearMatch = question.match(/(\d{4})/);
      const startYear = yearMatch ? yearMatch[1] : '2024';
      // Limit to 2 years max to prevent huge queries
      const currentYear = new Date().getFullYear();
      const maxStartYear = Math.max(currentYear - 2, parseInt(startYear));
      const startDate = `${maxStartYear}-01-01`;
      const endDate = new Date().toISOString().split('T')[0];
      
      const backtestQuery = buildBuyAndHoldQuery(asset, startDate, endDate);
      sql = backtestQuery.sql;
    } else if (intent === 'backtest_lend') {
      const assetMatch = question.match(/ETH|BTC|USDC|DAI/i);
      const asset = assetMatch ? assetMatch[0] : 'ETH';
      const yearMatch = question.match(/(\d{4})/);
      const startYear = yearMatch ? yearMatch[1] : '2023';
      // Limit to 2 years max to prevent huge queries
      const currentYear = new Date().getFullYear();
      const maxStartYear = Math.max(currentYear - 2, parseInt(startYear));
      const startDate = `${maxStartYear}-01-01`;
      const endDate = new Date().toISOString().split('T')[0];
      
      const backtestQuery = buildLendingAPYQuery(asset, startDate, endDate);
      sql = backtestQuery.sql;
    } else if (intent === 'forecast_apy') {
      const assetMatch = question.match(/ETH|BTC|USDC|DAI/i);
      const asset = assetMatch ? assetMatch[0] : 'ETH';
      
      const forecastQuery = buildAPYForecastQuery(asset, 60);
      sql = forecastQuery.sql;
    }

    // 2) Execute SQL (with optional statement timeout)
    const pool = getDbPool();
    let rows = [];
    let sqlTried = sql;

    const client = await pool.connect();
    try {
      // Shorter timeout for backtesting to prevent hangs
      const ms = (intent === 'backtest_buy' || intent === 'backtest_lend' || intent === 'forecast_apy') 
        ? 15000  // 15 seconds for backtesting
        : Number(process.env.DB_QUERY_TIMEOUT_MS || 30000);  // 30 seconds for standard queries
      await client.query(`SET statement_timeout TO ${ms}`);

      // First attempt
      try {
        const r = await client.query(sql);
        rows = r.rows || [];
      } catch (e1) {
        // Retry with error-aware planning
        const retry = await retryPlan(openai, question, sql, String(e1), null, intent);
        sql = retry.sql;
        sqlTried = sql;

        // Second attempt
        try {
          const r2 = await client.query(sql);
          rows = r2.rows || [];
        } catch (e2) {
          // Bubble precise DB error + which SQL failed
          const err = new Error(`Query failed after retry: ${e2.message}`);
          err.code = e2.code;
          err.detail = e2.detail;
          err.hint = e2.hint;
          err.position = e2.position;
          err.sql = sqlTried;
          throw err;
        }
      }
    } finally {
      client.release();
      // Note: We no longer call pool.end() - the pool stays alive for reuse
    }

    // 3) Handle backtesting/forecasting calculations
    let backtestResult = null;
    let answer = null;
    
    if (intent === 'backtest_buy' && rows && rows.length > 0) {
      // Extract amount from question or use default (handle k/m suffixes)
      const amountMatch = question.match(/\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*([km]?)/i);
      let amountUsd = 1000; // default
      if (amountMatch) {
        const baseAmount = parseFloat(amountMatch[1].replace(/,/g, ''));
        const suffix = amountMatch[2]?.toLowerCase() || '';
        if (suffix === 'k') {
          amountUsd = baseAmount * 1000;
        } else if (suffix === 'm') {
          amountUsd = baseAmount * 1000000;
        } else {
          amountUsd = baseAmount;
        }
      }
      
      backtestResult = calculateBuyAndHoldBacktest(rows, amountUsd);
      
      if (!backtestResult.error) {
        answer = `If you had invested $${backtestResult.amount_usd.toLocaleString()} on ${backtestResult.start_date}, you would have bought ${backtestResult.units_bought.toFixed(6)} units at $${backtestResult.start_price.toFixed(2)}. Today, your investment would be worth $${backtestResult.current_value.toLocaleString()} (${backtestResult.percent_return > 0 ? '+' : ''}${backtestResult.percent_return.toFixed(1)}% return). (Data as of ${backtestResult.end_date})`;
      }
    } else if (intent === 'backtest_lend' && rows && rows.length > 0) {
      // Extract amount and dates from question (handle k/m suffixes)
      const amountMatch = question.match(/\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*([km]?)/i);
      let amountUsd = 1000; // default
      if (amountMatch) {
        const baseAmount = parseFloat(amountMatch[1].replace(/,/g, ''));
        const suffix = amountMatch[2]?.toLowerCase() || '';
        if (suffix === 'k') {
          amountUsd = baseAmount * 1000;
        } else if (suffix === 'm') {
          amountUsd = baseAmount * 1000000;
        } else {
          amountUsd = baseAmount;
        }
      }
      
      // Extract dates from question or use defaults
      const yearMatch = question.match(/(\d{4})/);
      const startYear = yearMatch ? yearMatch[1] : '2020';
      const startDate = `${startYear}-01-01`;
      const endDate = new Date().toISOString().split('T')[0];
      
      // Extract asset from question
      const assetMatch = question.match(/ETH|BTC|USDC|DAI/i);
      const asset = assetMatch ? assetMatch[0] : 'ETH';
      
      // Get price data for the same asset and date range
      let priceData = null;
      try {
        const priceQuery = buildBuyAndHoldQuery(asset, startDate, endDate);
        const priceResult = await pool.query(priceQuery.sql);
        priceData = priceResult.rows;
      } catch (err) {
        console.log("Could not fetch price data:", err.message);
      }
      
      backtestResult = calculateLendingBacktest(rows, amountUsd, startDate, endDate, priceData);
      
      if (!backtestResult.error) {
        if (backtestResult.price_adjusted_scenario) {
          answer = `If you had lent $${backtestResult.amount_usd.toLocaleString()} of ${asset} starting ${backtestResult.start_date}, here are two scenarios:

1. **Flat Price Scenario**: Your investment would be worth $${backtestResult.final_value.toLocaleString()} (${backtestResult.percent_return > 0 ? '+' : ''}${backtestResult.percent_return.toFixed(1)}% return, ${backtestResult.annualized_return.toFixed(1)}% annualized)

2. **Price-Adjusted Scenario**: Your investment would be worth $${backtestResult.price_adjusted_scenario.final_value.toLocaleString()} (${backtestResult.price_adjusted_scenario.percent_return > 0 ? '+' : ''}${backtestResult.price_adjusted_scenario.percent_return.toFixed(1)}% return) - this includes the ${asset} price change from $${backtestResult.price_adjusted_scenario.start_price.toFixed(2)} to $${backtestResult.price_adjusted_scenario.end_price.toFixed(2)}.

(Data as of ${backtestResult.end_date})`;
        } else {
          answer = `If you had lent $${backtestResult.amount_usd.toLocaleString()} starting ${backtestResult.start_date}, your investment would be worth $${backtestResult.final_value.toLocaleString()} today (${backtestResult.percent_return > 0 ? '+' : ''}${backtestResult.percent_return.toFixed(1)}% return, ${backtestResult.annualized_return.toFixed(1)}% annualized). Note: Price data not available for full analysis. (Data as of ${backtestResult.end_date})`;
        }
      }
    } else if (intent === 'forecast_apy' && rows && rows.length > 0) {
      backtestResult = calculateAPYForecast(rows);
      
      if (!backtestResult.error) {
        answer = `Based on the last ${backtestResult.forecast_period} of data, the expected APY is ${backtestResult.forecast_apy.toFixed(2)}% (range: ${backtestResult.min_apy.toFixed(2)}% - ${backtestResult.max_apy.toFixed(2)}%, confidence: ${backtestResult.confidence}). ${backtestResult.note}`;
      }
    }

    // 4) Respond (JSON only)
    if (minimal) return res.status(200).json({ sql, rows, source: "database_query", intent, backtestResult });

    // Generate standard answer if no backtesting was performed
    if (!answer) {
      answer = await generateAnswerFromResults(openai, question, rows, presentationHint, intent);
    }
    
    return res.status(200).json({ sql, rows, answer, source: "database_query", intent, backtestResult });
  } catch (err) {
    // Clear, JSON-only error surface for Vercel & clients
    const message = err?.message || String(err);
    const payload = {
      error: message,
      db: {
        code: err?.code,
        detail: err?.detail,
        hint: err?.hint,
        position: err?.position,
      },
      sql: err?.sql, // which SQL failed (if any)
      hint:
        "Check DATABASE_URL (host/db/role), ?sslmode=require (optional), schema/table privileges, and request method/body.",
    };
    res.setHeader("content-type", "application/json");
    return res.status(500).end(JSON.stringify(payload));
  }
}
