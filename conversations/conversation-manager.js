/**
 * Conversation Manager
 * High-level interface for managing conversations and preparing context for LLM
 * 
 * Note: This module expects a Supabase client or database connection to be passed in.
 * For now, queries are returned as strings to be executed via MCP or direct connection.
 */

import * as queries from './db-queries.js';

/**
 * Format conversation messages for LLM prompt
 * Converts database message format to standard LLM message format
 * @param {Array} messages - Array of message objects from database
 * @returns {Array} - Formatted messages for LLM
 */
function formatMessagesForLLM(messages) {
  return messages.map(msg => ({
    role: msg.role,
    content: msg.content,
    timestamp: msg.created_at || msg.timestamp
  }));
}

/**
 * Format full conversation context for LLM
 * @param {Object} conversationData - Full conversation data from database
 * @returns {Object} - Formatted context object
 */
function formatConversationContext(conversationData) {
  const messages = conversationData.messages || [];
  
  return {
    conversationId: conversationData.conversation_id,
    title: conversationData.title,
    createdAt: conversationData.conversation_created_at || conversationData.created_at,
    metadata: conversationData.conversation_metadata || conversationData.metadata || {},
    messages: formatMessagesForLLM(messages),
    messageCount: messages.length
  };
}

/**
 * Build a complete prompt with conversation history
 * @param {Array} messages - Formatted messages array
 * @param {string} systemPrompt - Optional system prompt to prepend
 * @returns {Array} - Complete message array for LLM
 */
function buildPromptWithHistory(messages, systemPrompt = null) {
  const promptMessages = [];
  
  if (systemPrompt) {
    promptMessages.push({
      role: 'system',
      content: systemPrompt
    });
  }
  
  // Add all historical messages
  promptMessages.push(...messages);
  
  return promptMessages;
}

/**
 * Truncate conversation history to fit within token limit
 * Keeps most recent messages and system messages
 * @param {Array} messages - Array of messages
 * @param {number} maxMessages - Maximum number of messages to keep
 * @returns {Array} - Truncated messages
 */
function truncateHistory(messages, maxMessages = 50) {
  // Separate system messages from others
  const systemMessages = messages.filter(m => m.role === 'system');
  const otherMessages = messages.filter(m => m.role !== 'system');
  
  // Keep all system messages + most recent other messages
  const recentMessages = otherMessages.slice(-maxMessages);
  
  return [...systemMessages, ...recentMessages];
}

/**
 * Get conversation workflow
 * Returns SQL queries in the order they should be executed
 * @param {string} userId - UUID of the user
 * @param {string} conversationId - UUID of the conversation (optional, creates new if not provided)
 */
function getConversationWorkflow(userId, conversationId = null) {
  const workflow = {
    // Step 1: Ensure user exists
    ensureUser: queries.getOrCreateUser(userId),
    
    // Step 2: Get or create conversation
    getConversation: conversationId 
      ? queries.getConversation(conversationId, userId)
      : queries.createConversation(userId, 'New Conversation'),
    
    // Step 3: Get conversation messages (if conversation exists)
    getMessages: conversationId 
      ? queries.getConversationMessages(conversationId, userId)
      : null,
    
    // Step 4: Get full context for model
    getFullContext: conversationId
      ? queries.getFullConversationForModel(conversationId, userId)
      : null
  };
  
  return workflow;
}

/**
 * Add message workflow
 * Returns SQL queries to add a new message and get updated context
 * @param {string} userId - UUID of the user
 * @param {string} conversationId - UUID of the conversation
 * @param {string} role - Message role ('user', 'assistant', 'system')
 * @param {string} content - Message content
 * @param {Object} metadata - Optional metadata
 */
function addMessageWorkflow(userId, conversationId, role, content, metadata = {}) {
  return {
    // Step 1: Add the message
    addMessage: queries.addMessage(conversationId, userId, role, content, metadata),
    
    // Step 2: Get updated conversation context
    getUpdatedContext: queries.getConversationContext(conversationId, userId)
  };
}

/**
 * New conversation workflow
 * Creates a new conversation and adds initial message
 * @param {string} userId - UUID of the user
 * @param {string} initialMessage - First user message
 * @param {string} title - Optional conversation title
 */
function newConversationWorkflow(userId, initialMessage, title = null) {
  return {
    // Step 1: Ensure user exists
    ensureUser: queries.getOrCreateUser(userId),
    
    // Step 2: Create conversation
    createConversation: queries.createConversation(userId, title),
    
    // Note: After creating conversation, get the conversation_id from the result
    // Then use addMessageWorkflow with that conversation_id
    instructions: 'After executing createConversation, extract conversation_id from result and use addMessageWorkflow'
  };
}

/**
 * Helper to extract conversation messages as simple array
 * @param {Array} dbMessages - Messages from database
 * @returns {Array} - Simple role/content array
 */
function simplifyMessages(dbMessages) {
  return dbMessages.map(msg => ({
    role: msg.role,
    content: msg.content
  }));
}

/**
 * Prepare context string for model
 * Formats conversation history as a readable context string
 * @param {Array} messages - Array of messages
 * @returns {string} - Formatted context string
 */
function prepareContextString(messages) {
  if (!messages || messages.length === 0) {
    return 'No previous conversation history.';
  }
  
  let contextString = '=== Conversation History ===\n\n';
  
  messages.forEach((msg, index) => {
    const roleLabel = msg.role.charAt(0).toUpperCase() + msg.role.slice(1);
    contextString += `[${roleLabel}]: ${msg.content}\n\n`;
  });
  
  return contextString;
}

// ========================================
// QUERY GENERATORS WITH SAFETY
// ========================================

/**
 * Safe query generators that validate input
 */
const safeQueries = {
  /**
   * Safely create or get user
   * @param {string} userId - Must be valid UUID
   */
  getOrCreateUser: (userId) => {
    if (!isValidUUID(userId)) {
      throw new Error('Invalid userId: must be a valid UUID');
    }
    return queries.getOrCreateUser(userId);
  },
  
  /**
   * Safely create conversation
   * @param {string} userId - Must be valid UUID
   * @param {string} title - Optional title (sanitized)
   */
  createConversation: (userId, title = null) => {
    if (!isValidUUID(userId)) {
      throw new Error('Invalid userId: must be a valid UUID');
    }
    if (title && typeof title !== 'string') {
      throw new Error('Title must be a string');
    }
    return queries.createConversation(userId, title);
  },
  
  /**
   * Safely add message
   * @param {string} conversationId - Must be valid UUID
   * @param {string} userId - Must be valid UUID  
   * @param {string} role - Must be 'user', 'assistant', or 'system'
   * @param {string} content - Message content
   * @param {Object} metadata - Optional metadata object
   */
  addMessage: (conversationId, userId, role, content, metadata = {}) => {
    if (!isValidUUID(conversationId)) {
      throw new Error('Invalid conversationId: must be a valid UUID');
    }
    if (!isValidUUID(userId)) {
      throw new Error('Invalid userId: must be a valid UUID');
    }
    if (!['user', 'assistant', 'system'].includes(role)) {
      throw new Error('Invalid role: must be "user", "assistant", or "system"');
    }
    if (typeof content !== 'string') {
      throw new Error('Content must be a string');
    }
    return queries.addMessage(conversationId, userId, role, content, metadata);
  }
};

/**
 * Validate UUID format
 * @param {string} uuid - String to validate
 * @returns {boolean}
 */
function isValidUUID(uuid) {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  return typeof uuid === 'string' && uuidRegex.test(uuid);
}

// ========================================
// EXPORTS
// ========================================

export {
  // Raw queries (re-exported for direct access)
  queries,
  
  // Safe query generators
  safeQueries,
  
  // Formatting utilities
  formatMessagesForLLM,
  formatConversationContext,
  buildPromptWithHistory,
  truncateHistory,
  simplifyMessages,
  prepareContextString,
  
  // Workflows
  getConversationWorkflow,
  addMessageWorkflow,
  newConversationWorkflow,
  
  // Validation
  isValidUUID
};

