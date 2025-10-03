# Conversation Database System

This directory contains the conversation management system for storing and retrieving chat histories with proper user relationships.

## Database Schema

The system uses a `user` schema in PostgreSQL with three main tables:

### `user.user`
Stores user information and metadata.
- `user_id` (UUID, Primary Key)
- `email` (VARCHAR)
- `username` (VARCHAR)
- `created_at` (TIMESTAMPTZ)
- `updated_at` (TIMESTAMPTZ)
- `metadata` (JSONB)

### `user.conversations`
Stores individual conversations linked to users.
- `conversation_id` (UUID, Primary Key)
- `user_id` (UUID, Foreign Key → `user.user`)
- `title` (VARCHAR)
- `created_at` (TIMESTAMPTZ)
- `updated_at` (TIMESTAMPTZ)
- `metadata` (JSONB)

### `user.messages`
Stores individual messages within conversations.
- `message_id` (UUID, Primary Key)
- `conversation_id` (UUID, Foreign Key → `user.conversations`)
- `user_id` (UUID, Foreign Key → `user.user`)
- `role` (VARCHAR) - 'user', 'assistant', or 'system'
- `content` (TEXT)
- `created_at` (TIMESTAMPTZ)
- `metadata` (JSONB)

## Files

### `db-queries.js`
Contains parameterized SQL query templates for all database operations:
- User management (create, get, update)
- Conversation management (create, list, update, delete)
- Message management (add, retrieve, delete)
- Context retrieval for LLM integration

### `conversation-manager.js`
High-level utilities for managing conversations and preparing data for LLMs:
- `safeQueries` - Validated query generators
- `formatMessagesForLLM()` - Format database messages for LLM consumption
- `buildPromptWithHistory()` - Build complete prompts with conversation history
- `prepareContextString()` - Format conversations as readable strings
- Workflow helpers for common operations

### `usage-example.js`
Complete examples demonstrating:
- Creating new conversations
- Adding messages
- Retrieving conversation history
- Formatting data for LLM
- Integration patterns
- `ConversationService` class template

## Quick Start

```javascript
import { safeQueries, formatMessagesForLLM, queries } from './conversation-manager.js';

// 1. Create a conversation
const userId = 'your-user-uuid';
const createSQL = safeQueries.createConversation(userId, 'Chat Title');
const result = await executeSQL(createSQL);
const conversationId = result[0].conversation_id;

// 2. Add a message
const addSQL = safeQueries.addMessage(
  conversationId, 
  userId, 
  'user', 
  'Hello, AI!'
);
await executeSQL(addSQL);

// 3. Get conversation context for LLM
const contextSQL = queries.getConversationContext(conversationId, userId);
const messages = await executeSQL(contextSQL);
const formattedMessages = formatMessagesForLLM(messages);

// 4. Send to LLM (your implementation)
const response = await yourLLM(formattedMessages);

// 5. Save assistant response
const saveSQL = safeQueries.addMessage(
  conversationId,
  userId,
  'assistant',
  response
);
await executeSQL(saveSQL);
```

## Integration with MCP Supabase

To execute queries using the Supabase MCP:

```javascript
// In your application code
async function executeMCPQuery(sql) {
  // Use the mcp_supabase_execute_sql tool
  const result = await mcp_supabase_execute_sql({ query: sql });
  return result;
}

// Then use with conversation manager
const { safeQueries } = require('./conversations/conversation-manager');
const sql = safeQueries.createConversation(userId, 'New Chat');
const result = await executeMCPQuery(sql);
```

## Data Flow

1. **User starts conversation**
   - Create/verify user exists
   - Create new conversation record
   - Return conversation ID

2. **User sends message**
   - Insert user message into `user.messages`
   - Retrieve conversation history
   - Format for LLM

3. **LLM processes**
   - Receive formatted conversation history
   - Generate response

4. **Save response**
   - Insert assistant message into `user.messages`
   - Update conversation timestamp

5. **Retrieve history**
   - Query messages for conversation
   - Format for display or further LLM context

## Security Features

- **Foreign key constraints** ensure referential integrity
- **CASCADE DELETE** automatically cleans up related records
- **UUID validation** in safeQueries prevents injection
- **Role validation** ensures only valid roles are used
- **User ownership checks** in all queries prevent unauthorized access

## Best Practices

1. **Always use `safeQueries`** for user input to prevent SQL injection
2. **Truncate history** for long conversations to manage token limits
3. **Use metadata fields** for flexible data storage without schema changes
4. **Index frequently queried fields** (already implemented)
5. **Implement user authentication** before allowing database access

## Example: Complete Conversation Flow

```javascript
import { ConversationService } from './conversations/usage-example.js';

// Initialize service with your SQL executor
const service = new ConversationService(executeSQLFunction);

// Start conversation
const conversationId = await service.startConversation(userId, 'Tech Support');

// User sends message
const messages = await service.sendMessage(userId, conversationId, 'Help me debug this code');

// Get LLM response (your implementation)
const llmResponse = await yourLLM(messages);

// Save response
await service.saveAssistantResponse(userId, conversationId, llmResponse);

// Get full history
const history = await service.getConversationHistory(userId, conversationId);
```

## Future Enhancements

Consider adding:
- Message editing and deletion
- Conversation search functionality
- Token counting and limits
- Message streaming support
- Conversation sharing/permissions
- Export/import functionality
- Message reactions/feedback
- Conversation branching

## Support

For questions or issues, refer to:
- `usage-example.js` for detailed examples
- `conversation-manager.js` for available utilities
- `db-queries.js` for raw SQL templates

