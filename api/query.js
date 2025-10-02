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
    const stream = body?.stream === true || urlObj.searchParams.get("stream") === "true";

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
    
    if (inDataScope === 'meta') {
      // Handle meta questions about service capabilities
      const answer = `I can answer questions about:

📊 **Liquidity Pools**
• Current APYs for pools like WETH-USDC, ETH-BTC
• Best pools by TVL and yield (Aerodrome, Uniswap)
• Pool comparisons across protocols and chains

💰 **Lending Rates**
• Supply and borrow APYs (Aave V3, Compound, Fraxlend)
• Best lending opportunities for ETH, USDC, BTC
• Protocol comparisons for lending rates

💵 **Token Prices**
• Real-time prices for major tokens (BTC, ETH, USDC)
• Recent price data (updated every 5 minutes)

Try asking:
• "What are the best liquidity pools right now?"
• "What's the lending APY for ETH on Aave?"
• "Show me WETH-USDC pool rates"
• "What's the current price of BTC?"`;
      
      return res.status(200).json({ 
        answer, 
        source: "meta_response",
        note: "Service capabilities overview"
      });
    }
    
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
    if (intent === 'portfolio_optimization' || intent === 'general_prediction') {
      const answer = await handleGeneralKnowledgeQuestion(openai, question);
      return res.status(200).json({ 
        answer, 
        source: "general_knowledge",
        intent: intent
      });
    }


    // All queries use general LLM planning - no hardcoded functions
    const result = await planQuery(openai, question, null, intent);
    let sql = result.sql;

    // 2) Execute SQL (with optional statement timeout)
    const pool = getDbPool();
    let rows = [];
    let sqlTried = sql;
    let retryCount = 0; // Track retry attempts for debugging

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
      // Standard timeout for all queries
      const ms = Number(process.env.DB_QUERY_TIMEOUT_MS || 30000);  // 30 seconds for all queries
      await client.query(`SET statement_timeout TO ${ms}`);

      // 🧠 SMART RETRY SYSTEM with Progressive Learning
      const maxRetries = 3;
      let lastError = null;
      
      // First attempt
      try {
        const r = await client.query(sql);
        rows = r.rows || [];
      } catch (e1) {
        lastError = e1;
        
        // Smart retry loop with progressive learning
        for (retryCount = 1; retryCount <= maxRetries; retryCount++) {
          try {
            console.log(`🔄 Retry #${retryCount}: Learning from error - ${String(lastError).substring(0, 100)}...`);
            
            // Learn from the specific error and adapt strategy
            const retry = await retryPlan(openai, question, sql, String(lastError), null, intent, retryCount);
            sql = retry.sql;
            sqlTried = sql;
            
            // Attempt the improved query
            const r = await client.query(sql);
            rows = r.rows || [];
            
            console.log(`✅ Success on retry #${retryCount}! Query learned and adapted.`);
            break; // Success! Break out of retry loop
            
          } catch (retryError) {
            lastError = retryError;
            console.log(`❌ Retry #${retryCount} failed: ${String(retryError).substring(0, 100)}...`);
            
            // If this was the last retry, throw the error
            if (retryCount === maxRetries) {
              const err = new Error(`Query failed after ${maxRetries} learning attempts: ${retryError.message}`);
              err.code = retryError.code;
              err.detail = retryError.detail;
              err.hint = retryError.hint;
              err.position = retryError.position;
              err.sql = sqlTried;
              err.retryCount = retryCount;
              err.originalError = String(e1);
              throw err;
            }
          }
        }
      }
    } finally {
      client.release();
      // Note: We no longer call pool.end() - the pool stays alive for reuse
    }


    // 4) Respond (JSON only)
    if (minimal) return res.status(200).json({ sql, rows, source: "database_query", intent });

    // Handle streaming response
    if (stream) {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      });

      // Send initial data (SQL and rows)
      res.write(`data: ${JSON.stringify({ type: 'sql', sql })}\n\n`);
      res.write(`data: ${JSON.stringify({ type: 'rows', rows: rows.slice(0, 10), totalRows: rows.length })}\n\n`);
      res.write(`data: ${JSON.stringify({ type: 'answer_start' })}\n\n`);

      try {
        const answerStream = await generateAnswerFromResults(openai, question, rows, presentationHint, intent, retryCount, true);
        
        for await (const chunk of answerStream) {
          const content = chunk.choices[0]?.delta?.content || '';
          if (content) {
            res.write(`data: ${JSON.stringify({ type: 'answer_chunk', content })}\n\n`);
          }
        }
        
        res.write(`data: ${JSON.stringify({ type: 'done', retryCount, intent })}\n\n`);
        res.end();
      } catch (error) {
        res.write(`data: ${JSON.stringify({ type: 'error', error: error.message })}\n\n`);
        res.end();
      }
      return;
    }

    // Generate answer from results
    const answer = await generateAnswerFromResults(openai, question, rows, presentationHint, intent, retryCount, false);
    
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
      retryCount: retryCount,
      debug: debugInfo 
    });
  } catch (err) {
    // Provide context-aware error messages based on intent and error type
    const message = err?.message || String(err);
    let contextualMessage = message;
    
    if (message.includes('timestamp') || message.includes('bigint') || message.includes('UNION')) {
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
