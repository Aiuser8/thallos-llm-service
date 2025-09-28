// api/query.js — Vercel Node serverless handler (ESM)
import OpenAI from "openai";
import pg from "pg";
import { planQuery, retryPlan, generateAnswerFromResults, isQuestionInDataScope, handleGeneralKnowledgeQuestion, detectQueryIntent } from "../lib/instructions.js";

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
      // Improved connection pool configuration for high-volume usage and stability
      max: 10, // Reduced from 20 to avoid Supabase connection limits
      min: 2, // Keep minimum connections open for faster responses
      idleTimeoutMillis: 20000, // Reduce idle timeout to 20 seconds
      connectionTimeoutMillis: 5000, // Increase connection timeout to 5 seconds
      query_timeout: 30000, // 30 second query timeout
      keepAlive: true, // Keep connections alive to prevent EADDRNOTAVAIL errors
      application_name: 'thallos-llm-service' // Help identify connections in Supabase logs
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
  let intent = 'standard_query'; // Initialize early to prevent undefined errors in catch block
  
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
    intent = detectQueryIntent(question);
    
    // Handle special intents that need general knowledge responses
    if (intent === 'portfolio_optimization') {
      const answer = await handleGeneralKnowledgeQuestion(openai, question);
      return res.status(200).json({ 
        answer, 
        source: "general_knowledge",
        intent: intent
      });
    }
    
    // Handle wallet analysis with improved Alchemy API
    if (intent === 'wallet_analysis') {
      const walletAddress = extractWalletAddress(question);
      
      if (!walletAddress) {
        return res.status(400).json({
          error: "Please provide a valid Ethereum wallet address (0x...)",
          intent: intent
        });
      }
      
      const alchemyApiKey = process.env.ALCHEMY_API_KEY;
      if (!alchemyApiKey) {
        return res.status(500).json({
          error: "Wallet analysis is not configured. Missing Alchemy API key.",
          intent: intent
        });
      }
      
      try {
        const walletAnalysis = await analyzeWalletWithAlchemy(walletAddress, alchemyApiKey);
        
        if (walletAnalysis.error) {
          return res.status(500).json({
            error: walletAnalysis.error,
            wallet_address: walletAddress,
            intent: intent
          });
        }
        
        // Generate a simple plain English summary
        const answer = generateWalletSummary(walletAnalysis);
        
        return res.status(200).json({
          answer: answer,
          wallet_analysis: walletAnalysis,
          source: "alchemy_api",
          intent: intent
        });
        
      } catch (error) {
        return res.status(500).json({
          error: `Wallet analysis failed: ${error.message}`,
          wallet_address: walletAddress,
          intent: intent
        });
      }
    }

    // Let all other intents attempt to generate SQL first - only block if they actually fail

    // 1) Plan SQL or use specific backtesting queries
    let sql;
    
    // Use specific backtesting queries (skip LLM planning for these)
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
      
        const backtestQuery = buildLendingAPYQuery(asset, startDate, endDate, question);
        sql = backtestQuery.sql;
    } else if (intent === 'forecast_apy') {
      const assetMatch = question.match(/ETH|BTC|USDC|DAI/i);
      const asset = assetMatch ? assetMatch[0] : 'ETH';
      
      const forecastQuery = buildAPYForecastQuery(asset, 60, question);
      sql = forecastQuery.sql;
    } else if (intent === 'rotation_strategy') {
      const yearMatch = question.match(/since\s+(\d{4})/);
      const startYear = yearMatch ? yearMatch[1] : '2020';
      
      const rotationQuery = buildRotationStrategyQuery(question, startYear);
      sql = rotationQuery.sql;
    } else {
      // Standard queries: use LLM planning
      const result = await planQuery(openai, question, null, intent);
      sql = result.sql;
    }

    // 2) Execute SQL (with optional statement timeout)
    const pool = getDbPool();
    let rows = [];
    let sqlTried = sql;

    let client;
    try {
      client = await pool.connect();
    } catch (connectionError) {
      console.error('Database connection failed:', connectionError.message);
      return res.status(503).json({ 
        error: "Database temporarily unavailable. Please try again.", 
        details: connectionError.message 
      });
    }
    
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
        // Extract protocol info from the data
        const protocols = [...new Set(rows.map(r => r.project).filter(p => p))];
        const protocolInfo = protocols.length > 0 ? ` using ${protocols.join(', ')}` : '';
        
        if (backtestResult.price_adjusted_scenario) {
          answer = `If you had lent $${backtestResult.amount_usd.toLocaleString()} of ${asset} starting ${backtestResult.start_date}${protocolInfo} (average ${backtestResult.average_apy.toFixed(2)}% APY), here are two scenarios:

1. **Flat Price Scenario**: Your investment would be worth $${backtestResult.final_value.toLocaleString()} (${backtestResult.percent_return > 0 ? '+' : ''}${backtestResult.percent_return.toFixed(1)}% return)

2. **Price-Adjusted Scenario**: Your investment would be worth $${backtestResult.price_adjusted_scenario.final_value.toLocaleString()} (${backtestResult.price_adjusted_scenario.percent_return > 0 ? '+' : ''}${backtestResult.price_adjusted_scenario.percent_return.toFixed(1)}% return) - this includes the ${asset} price change from $${backtestResult.price_adjusted_scenario.start_price.toFixed(2)} to $${backtestResult.price_adjusted_scenario.end_price.toFixed(2)}.

(Data as of ${backtestResult.end_date})`;
        } else {
          answer = `If you had lent $${backtestResult.amount_usd.toLocaleString()} starting ${backtestResult.start_date}${protocolInfo} at an average ${backtestResult.average_apy.toFixed(2)}% APY, your investment would be worth $${backtestResult.final_value.toLocaleString()} today (${backtestResult.percent_return > 0 ? '+' : ''}${backtestResult.percent_return.toFixed(1)}% return). Note: Price data not available for full analysis. (Data as of ${backtestResult.end_date})`;
        }
      }
    } else if (intent === 'forecast_apy' && rows && rows.length > 0) {
      backtestResult = calculateAPYForecast(rows);
      
      if (!backtestResult.error) {
        // Extract protocol info from the data
        const protocols = [...new Set(rows.map(r => r.project).filter(p => p))];
        const protocolInfo = protocols.length > 0 ? ` (based on ${protocols.join(', ')} data)` : '';
        
        answer = `Based on the last ${backtestResult.forecast_period} of data, the expected APY is ${backtestResult.forecast_apy.toFixed(2)}% (range: ${backtestResult.min_apy.toFixed(2)}% - ${backtestResult.max_apy.toFixed(2)}%, confidence: ${backtestResult.confidence})${protocolInfo}. ${backtestResult.note}`;
      }
    } else if (intent === 'rotation_strategy' && rows && rows.length > 0) {
      const amountMatch = question.match(/\$?(\d+(?:,\d{3})*(?:\.\d{2})?)\s*([km]?)/i);
      let initialAmount = 10000; // default
      if (amountMatch) {
        const baseAmount = parseFloat(amountMatch[1].replace(/,/g, ''));
        const suffix = amountMatch[2]?.toLowerCase() || '';
        if (suffix === 'k') {
          initialAmount = baseAmount * 1000;
        } else if (suffix === 'm') {
          initialAmount = baseAmount * 1000000;
        } else {
          initialAmount = baseAmount;
        }
      }
      
      backtestResult = calculateRotationStrategy(rows, initialAmount);
      
      if (!backtestResult.error) {
        answer = `If you had rotated quarterly into the top decile of stablecoin pools since ${backtestResult.start_date?.split('T')[0]}, your CAGR would be ${backtestResult.cagr.toFixed(2)}%. Starting with $${backtestResult.initial_amount.toLocaleString()}, you would have $${backtestResult.final_value.toLocaleString()} today (${backtestResult.total_return_percent.toFixed(1)}% total return over ${backtestResult.years.toFixed(1)} years). Average quarterly APY was ${backtestResult.avg_quarterly_apy.toFixed(2)}% across ${backtestResult.avg_pool_count.toFixed(0)} pools per quarter. (Data as of ${backtestResult.end_date?.split('T')[0]})`;
      }
    }

    // 4) Respond (JSON only)
    if (minimal) return res.status(200).json({ sql, rows, source: "database_query", intent, backtestResult });

    // Generate standard answer if no backtesting was performed
    if (!answer) {
      answer = await generateAnswerFromResults(openai, question, rows, presentationHint, intent);
    }
    
    // Always include debug info for troubleshooting
    const debugInfo = {
      sql: sql,
      raw_data_sample: rows.slice(0, 5), // First 5 rows
      total_rows: rows.length
    };
    
    return res.status(200).json({ 
      sql, 
      rows, 
      answer, 
      source: "database_query", 
      intent, 
      backtestResult,
      debug: debugInfo 
    });
  } catch (err) {
    // Provide context-aware error messages based on intent and error type
    const message = err?.message || String(err);
    let contextualMessage = message;
    
    if (intent === 'complex_backtest') {
      contextualMessage = "This complex backtesting strategy (involving leverage, looping, or multi-protocol interactions) couldn't be processed with our current data. Try simpler backtests with single protocols and basic buy-and-hold or lending strategies.";
    } else if (intent === 'liquidity_pool_analysis') {
      contextualMessage = "This liquidity pool comparison couldn't be completed. Try asking about specific metrics like 'What's the current APY for ETH/USDC pools on Aerodrome?' or 'Compare current yields on Uniswap vs Curve.'";
    } else if (intent === 'rotation_strategy') {
      contextualMessage = "This rotation strategy analysis couldn't be completed with the available data. The query might be too complex or require data we don't have sufficient coverage for.";
    } else if (message.includes('timestamp') || message.includes('bigint') || message.includes('UNION')) {
      contextualMessage = "There was a data compatibility issue with this query. Try asking about a single protocol or shorter time period.";
    } else if (message.includes('does not exist') || message.includes('column')) {
      contextualMessage = "This query requires data fields that aren't available. Try a simpler version of your question.";
    } else if (message.includes('timeout') || message.includes('statement_timeout')) {
      contextualMessage = "This query is too complex and timed out. Try asking about a shorter time period or specific protocols.";
    } else if (message.includes('Planner did not return SQL')) {
      contextualMessage = "This query is too complex for our current capabilities. Try breaking it down into simpler questions or asking about specific protocols and metrics.";
    }
    
    const payload = {
      error: contextualMessage,
      intent: intent,
      technical_details: message,
      db: {
        code: err?.code,
        detail: err?.detail,
        hint: err?.hint,
        position: err?.position,
      },
      sql: err?.sql, // which SQL failed (if any)
    };
    res.setHeader("content-type", "application/json");
    return res.status(500).end(JSON.stringify(payload));
  }
}
