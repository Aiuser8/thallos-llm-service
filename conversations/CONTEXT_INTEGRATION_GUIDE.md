# Conversation Context Integration Guide

## Overview

The LLM instructions have been updated to support **conversation history** for context-aware, focused responses in multi-turn conversations.

## What Changed

### 1. Enhanced `generateAnswerFromResults` Function

The function now accepts an optional `conversationHistory` parameter:

```javascript
generateAnswerFromResults(
  openai,
  question,
  rows,
  presentationHint,
  intent = 'standard_query',
  retryCount = 0,
  stream = false,
  conversationHistory = null  // NEW!
)
```

### 2. Context-Aware System Prompt

The system prompt now includes extensive instructions for:
- ✅ Referencing previous conversation
- ✅ Building upon earlier responses
- ✅ Being focused and specific in follow-ups
- ✅ Avoiding generic responses when context exists
- ✅ Acknowledging the conversation thread

## How It Works

### Without Conversation History (Turn 1)

```javascript
import { generateAnswerFromResults } from './lib/instructions.js';

const answer = await generateAnswerFromResults(
  openai,
  "What are the current lending opportunities on Ethereum?",
  queryResults,
  null,
  'standard_query',
  0,
  false,
  null  // No conversation history
);
```

**Response Style**: Comprehensive, broad overview of all lending opportunities

### With Conversation History (Turn 2+)

```javascript
// Conversation history from database
const conversationHistory = [
  {
    role: 'user',
    content: 'What are the current lending opportunities on Ethereum?'
  },
  {
    role: 'assistant',
    content: 'Best Ethereum Lending Opportunities... [previous response with Aave V3, Compound V3, etc.]'
  }
];

const answer = await generateAnswerFromResults(
  openai,
  "Which of those has the highest APY for USDC?",
  queryResults,
  null,
  'standard_query',
  0,
  false,
  conversationHistory  // Include history!
);
```

**Response Style**: 
- ✅ References "those" protocols from Turn 1
- ✅ Focuses specifically on USDC from the previously mentioned options
- ✅ More targeted and less generic
- ✅ Acknowledges the conversation flow

## Integration with Your API

### Step 1: Update Your Query Handler

In `/api/query.js`, modify the handler to accept and use conversation history:

```javascript
export default async function handler(req, res) {
  const body = await readJson(req);
  const { 
    question, 
    conversationId = null,  // Optional conversation ID
    userId = null           // Optional user ID
  } = body;
  
  let conversationHistory = null;
  
  // If conversationId provided, load history from database
  if (conversationId && userId) {
    const { queries } = await import('../conversations/conversation-manager.js');
    const historySQL = queries.getConversationMessages(conversationId, userId, 10);
    
    // Execute SQL to get history
    const pool = getDbPool();
    const result = await pool.query(historySQL);
    
    // Format for LLM
    conversationHistory = result.rows.map(msg => ({
      role: msg.role,
      content: msg.content
    }));
  }
  
  // ... rest of your query logic ...
  
  const answer = await generateAnswerFromResults(
    openai,
    question,
    rows,
    null,
    intent,
    0,
    false,
    conversationHistory  // Pass conversation history!
  );
  
  return res.status(200).json({ answer });
}
```

### Step 2: Client Request Format

Update your client requests to include conversation context:

```javascript
// First question (no context)
const response1 = await fetch('/api/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    question: 'What are the current lending opportunities on Ethereum?',
    userId: 'user-uuid',
    conversationId: null  // New conversation
  })
});

// Create conversation and save both question and response to DB
// Get conversationId from database

// Follow-up question (with context)
const response2 = await fetch('/api/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    question: 'Which of those has the highest APY for USDC?',
    userId: 'user-uuid',
    conversationId: 'conversation-uuid'  // Existing conversation
  })
});
```

## Complete Workflow Example

```javascript
import { safeQueries, queries, formatMessagesForLLM } from './conversations/conversation-manager.js';
import { generateAnswerFromResults } from './lib/instructions.js';

async function handleConversationalQuery(userId, conversationId, question, openai, dbPool) {
  // 1. Get conversation history if this is a follow-up
  let conversationHistory = null;
  if (conversationId) {
    const historySQL = queries.getConversationMessages(conversationId, userId, 10);
    const { rows } = await dbPool.query(historySQL);
    conversationHistory = formatMessagesForLLM(rows);
  }
  
  // 2. Save user question to database
  if (conversationId) {
    const saveUserMsgSQL = safeQueries.addMessage(
      conversationId,
      userId,
      'user',
      question
    );
    await dbPool.query(saveUserMsgSQL);
  }
  
  // 3. Execute data query and generate answer WITH context
  const dataResults = await executeDataQuery(question);  // Your existing logic
  const answer = await generateAnswerFromResults(
    openai,
    question,
    dataResults,
    null,
    'standard_query',
    0,
    false,
    conversationHistory  // Context-aware!
  );
  
  // 4. Save assistant response to database
  if (conversationId) {
    const saveAssistantMsgSQL = safeQueries.addMessage(
      conversationId,
      userId,
      'assistant',
      answer
    );
    await dbPool.query(saveAssistantMsgSQL);
  }
  
  return answer;
}
```

## Expected Behavior Changes

### Before (Without Context)

**Q1**: "What are the current lending opportunities on Ethereum?"
**A1**: Comprehensive list of all lending protocols and tokens

**Q2**: "Which of those has the highest APY for USDC?"
**A2**: ❌ Generic: "Here's USDC lending across all chains..." (ignores "those")

### After (With Context)

**Q1**: "What are the current lending opportunities on Ethereum?"
**A1**: Comprehensive list of all lending protocols and tokens

**Q2**: "Which of those has the highest APY for USDC?"
**A2**: ✅ Focused: "Among the protocols I mentioned (Aave V3, Compound V3, Gearbox), here's the USDC breakdown..."

**Q3**: "What about for ETH instead?"
**A3**: ✅ Context-aware: "Switching to ETH from those same protocols..."

## Benefits

1. **More Natural Conversations**: LLM understands pronoun references ("those", "that", "it")
2. **Focused Responses**: Follow-ups are targeted, not generic
3. **Better UX**: Users don't need to repeat context
4. **Efficiency**: Shorter, more relevant responses in follow-ups
5. **Context Continuity**: Conversation feels coherent and intelligent

## Testing

Use the test script to see context-aware responses:

```bash
node conversations/test-multiturn.mjs
```

This demonstrates:
- Turn 1: Broad question → Comprehensive response
- Turn 2: "Which of those..." → Focused, context-aware response
- Turn 3: "What about instead..." → Continues the thread

## Notes

- Conversation history is limited to last **10 messages** to avoid token limits
- Only `user` and `assistant` roles are included in context
- System prompt emphasizes using context without being generic
- Works with existing conversation database schema
- Backward compatible: works fine without conversation history too!

