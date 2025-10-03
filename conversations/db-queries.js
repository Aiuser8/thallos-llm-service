/**
 * Database queries for conversation management
 * These queries interact with the user schema tables:
 * - user.user
 * - user.conversations
 * - user.messages
 */

// ========================================
// USER QUERIES
// ========================================

/**
 * Get or create a user by user_id
 * @param {string} userId - UUID of the user
 */
const getOrCreateUser = (userId) => `
  INSERT INTO "user"."user" (user_id)
  VALUES ('${userId}')
  ON CONFLICT (user_id) DO NOTHING
  RETURNING *;
`;

/**
 * Get user by user_id
 * @param {string} userId - UUID of the user
 */
const getUserById = (userId) => `
  SELECT * FROM "user"."user"
  WHERE user_id = '${userId}';
`;

/**
 * Update user metadata
 * @param {string} userId - UUID of the user
 * @param {object} metadata - Metadata object to merge
 */
const updateUserMetadata = (userId, metadata) => `
  UPDATE "user"."user"
  SET metadata = metadata || '${JSON.stringify(metadata)}'::jsonb
  WHERE user_id = '${userId}'
  RETURNING *;
`;

// ========================================
// CONVERSATION QUERIES
// ========================================

/**
 * Create a new conversation for a user
 * @param {string} userId - UUID of the user
 * @param {string} title - Optional title for the conversation
 */
const createConversation = (userId, title = null) => `
  INSERT INTO "user"."conversations" (user_id, title)
  VALUES ('${userId}', ${title ? `'${title.replace(/'/g, "''")}'` : 'NULL'})
  RETURNING *;
`;

/**
 * Get all conversations for a user
 * @param {string} userId - UUID of the user
 * @param {number} limit - Maximum number of conversations to return
 */
const getUserConversations = (userId, limit = 50) => `
  SELECT 
    conversation_id,
    user_id,
    title,
    created_at,
    updated_at,
    metadata,
    (SELECT COUNT(*) FROM "user"."messages" m WHERE m.conversation_id = c.conversation_id) as message_count
  FROM "user"."conversations" c
  WHERE user_id = '${userId}'
  ORDER BY updated_at DESC
  LIMIT ${limit};
`;

/**
 * Get a specific conversation
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 */
const getConversation = (conversationId, userId) => `
  SELECT * FROM "user"."conversations"
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}';
`;

/**
 * Update conversation title
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 * @param {string} title - New title
 */
const updateConversationTitle = (conversationId, userId, title) => `
  UPDATE "user"."conversations"
  SET title = '${title.replace(/'/g, "''")}'
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}'
  RETURNING *;
`;

/**
 * Delete a conversation (will cascade delete all messages)
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 */
const deleteConversation = (conversationId, userId) => `
  DELETE FROM "user"."conversations"
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}'
  RETURNING conversation_id;
`;

// ========================================
// MESSAGE QUERIES
// ========================================

/**
 * Add a message to a conversation
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user
 * @param {string} role - Role: 'user', 'assistant', or 'system'
 * @param {string} content - Message content
 * @param {object} metadata - Optional metadata
 */
const addMessage = (conversationId, userId, role, content, metadata = {}) => `
  INSERT INTO "user"."messages" (conversation_id, user_id, role, content, metadata)
  VALUES (
    '${conversationId}',
    '${userId}',
    '${role}',
    '${content.replace(/'/g, "''")}',
    '${JSON.stringify(metadata)}'::jsonb
  )
  RETURNING *;
`;

/**
 * Get all messages for a conversation
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 * @param {number} limit - Maximum number of messages to return
 */
const getConversationMessages = (conversationId, userId, limit = 1000) => `
  SELECT 
    message_id,
    conversation_id,
    user_id,
    role,
    content,
    created_at,
    metadata
  FROM "user"."messages"
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}'
  ORDER BY created_at ASC
  LIMIT ${limit};
`;

/**
 * Get recent messages for a conversation (for context window)
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 * @param {number} limit - Number of recent messages to return
 */
const getRecentMessages = (conversationId, userId, limit = 20) => `
  SELECT 
    message_id,
    conversation_id,
    user_id,
    role,
    content,
    created_at,
    metadata
  FROM "user"."messages"
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}'
  ORDER BY created_at DESC
  LIMIT ${limit};
`;

/**
 * Get conversation context formatted for LLM
 * Returns messages in chronological order with role and content
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user (for security)
 * @param {number} messageLimit - Maximum number of messages to include
 */
const getConversationContext = (conversationId, userId, messageLimit = 50) => `
  SELECT 
    role,
    content,
    created_at
  FROM "user"."messages"
  WHERE conversation_id = '${conversationId}'
    AND user_id = '${userId}'
  ORDER BY created_at ASC
  LIMIT ${messageLimit};
`;

/**
 * Delete a specific message
 * @param {string} messageId - UUID of the message
 * @param {string} userId - UUID of the user (for security)
 */
const deleteMessage = (messageId, userId) => `
  DELETE FROM "user"."messages"
  WHERE message_id = '${messageId}'
    AND user_id = '${userId}'
  RETURNING message_id;
`;

/**
 * Update message metadata
 * @param {string} messageId - UUID of the message
 * @param {string} userId - UUID of the user (for security)
 * @param {object} metadata - Metadata to merge
 */
const updateMessageMetadata = (messageId, userId, metadata) => `
  UPDATE "user"."messages"
  SET metadata = metadata || '${JSON.stringify(metadata)}'::jsonb
  WHERE message_id = '${messageId}'
    AND user_id = '${userId}'
  RETURNING *;
`;

// ========================================
// CONVERSATION HISTORY & CONTEXT
// ========================================

/**
 * Get full conversation with all messages for model context
 * This query joins conversation metadata with all messages
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user
 */
const getFullConversationForModel = (conversationId, userId) => `
  WITH conversation_info AS (
    SELECT 
      conversation_id,
      title,
      created_at as conversation_created_at,
      metadata as conversation_metadata
    FROM "user"."conversations"
    WHERE conversation_id = '${conversationId}'
      AND user_id = '${userId}'
  ),
  messages_list AS (
    SELECT 
      role,
      content,
      created_at,
      metadata
    FROM "user"."messages"
    WHERE conversation_id = '${conversationId}'
      AND user_id = '${userId}'
    ORDER BY created_at ASC
  )
  SELECT 
    ci.*,
    json_agg(
      json_build_object(
        'role', ml.role,
        'content', ml.content,
        'timestamp', ml.created_at,
        'metadata', ml.metadata
      ) ORDER BY ml.created_at ASC
    ) as messages
  FROM conversation_info ci
  CROSS JOIN messages_list ml
  GROUP BY ci.conversation_id, ci.title, ci.conversation_created_at, ci.conversation_metadata;
`;

/**
 * Get conversation summary statistics
 * @param {string} conversationId - UUID of the conversation
 * @param {string} userId - UUID of the user
 */
const getConversationStats = (conversationId, userId) => `
  SELECT 
    c.conversation_id,
    c.title,
    c.created_at,
    c.updated_at,
    COUNT(m.message_id) as total_messages,
    COUNT(CASE WHEN m.role = 'user' THEN 1 END) as user_messages,
    COUNT(CASE WHEN m.role = 'assistant' THEN 1 END) as assistant_messages,
    COUNT(CASE WHEN m.role = 'system' THEN 1 END) as system_messages,
    MIN(m.created_at) as first_message_at,
    MAX(m.created_at) as last_message_at
  FROM "user"."conversations" c
  LEFT JOIN "user"."messages" m ON c.conversation_id = m.conversation_id
  WHERE c.conversation_id = '${conversationId}'
    AND c.user_id = '${userId}'
  GROUP BY c.conversation_id, c.title, c.created_at, c.updated_at;
`;

// ========================================
// EXPORTS
// ========================================

export {
  // User queries
  getOrCreateUser,
  getUserById,
  updateUserMetadata,
  
  // Conversation queries
  createConversation,
  getUserConversations,
  getConversation,
  updateConversationTitle,
  deleteConversation,
  
  // Message queries
  addMessage,
  getConversationMessages,
  getRecentMessages,
  deleteMessage,
  updateMessageMetadata,
  
  // Context queries for LLM
  getConversationContext,
  getFullConversationForModel,
  getConversationStats
};

