/**
 * Usage Examples for Conversation Database
 * 
 * This file demonstrates how to use the conversation manager
 * to interact with the database and prepare context for LLM.
 */

import { queries, safeQueries, formatMessagesForLLM, buildPromptWithHistory, prepareContextString } from './conversation-manager.js';

// ========================================
// EXAMPLE 1: Create New Conversation
// ========================================

async function exampleCreateNewConversation(executeSQL) {
  const userId = '550e8400-e29b-41d4-a716-446655440000'; // Example UUID
  
  console.log('=== Creating New Conversation ===\n');
  
  // Step 1: Ensure user exists
  console.log('Step 1: Ensuring user exists...');
  const userSQL = safeQueries.getOrCreateUser(userId);
  console.log('SQL:', userSQL);
  await executeSQL(userSQL);
  
  // Step 2: Create conversation
  console.log('\nStep 2: Creating conversation...');
  const convSQL = safeQueries.createConversation(userId, 'My First Conversation');
  console.log('SQL:', convSQL);
  const convResult = await executeSQL(convSQL);
  const conversationId = convResult[0].conversation_id;
  console.log('Created conversation:', conversationId);
  
  // Step 3: Add first message
  console.log('\nStep 3: Adding first message...');
  const msgSQL = safeQueries.addMessage(
    conversationId,
    userId,
    'user',
    'Hello! Can you help me with my project?'
  );
  console.log('SQL:', msgSQL);
  await executeSQL(msgSQL);
  
  console.log('\n✓ Conversation created successfully!');
  return conversationId;
}

// ========================================
// EXAMPLE 2: Get Conversation Context for LLM
// ========================================

async function exampleGetContextForLLM(executeSQL, userId, conversationId) {
  console.log('\n=== Getting Conversation Context for LLM ===\n');
  
  // Get conversation messages
  const messagesSQL = queries.getConversationMessages(conversationId, userId);
  console.log('SQL:', messagesSQL);
  const messages = await executeSQL(messagesSQL);
  
  // Format for LLM
  const formattedMessages = formatMessagesForLLM(messages);
  console.log('\nFormatted messages:', JSON.stringify(formattedMessages, null, 2));
  
  // Build complete prompt with system message
  const systemPrompt = 'You are a helpful AI assistant. Be concise and accurate.';
  const completePrompt = buildPromptWithHistory(formattedMessages, systemPrompt);
  
  console.log('\nComplete prompt for LLM:', JSON.stringify(completePrompt, null, 2));
  
  return completePrompt;
}

// ========================================
// EXAMPLE 3: Add Message and Get Response
// ========================================

async function exampleConversationFlow(executeSQL, userId, conversationId) {
  console.log('\n=== Full Conversation Flow ===\n');
  
  // User sends a message
  const userMessage = 'What is the capital of France?';
  console.log('User:', userMessage);
  
  // Add user message to database
  const addUserMsgSQL = safeQueries.addMessage(
    conversationId,
    userId,
    'user',
    userMessage
  );
  await executeSQL(addUserMsgSQL);
  
  // Get conversation context
  const contextSQL = queries.getConversationContext(conversationId, userId);
  const context = await executeSQL(contextSQL);
  const formattedContext = formatMessagesForLLM(context);
  
  // Prepare context string for logging
  const contextString = prepareContextString(formattedContext);
  console.log('\nContext sent to LLM:');
  console.log(contextString);
  
  // Simulate LLM response (in real usage, this would come from your LLM)
  const assistantResponse = 'The capital of France is Paris.';
  console.log('Assistant:', assistantResponse);
  
  // Save assistant response to database
  const addAssistantMsgSQL = safeQueries.addMessage(
    conversationId,
    userId,
    'assistant',
    assistantResponse
  );
  await executeSQL(addAssistantMsgSQL);
  
  console.log('\n✓ Message exchange completed and saved!');
}

// ========================================
// EXAMPLE 4: Get User's Conversations
// ========================================

async function exampleGetUserConversations(executeSQL, userId) {
  console.log('\n=== Getting All User Conversations ===\n');
  
  const conversationsSQL = queries.getUserConversations(userId, 10);
  console.log('SQL:', conversationsSQL);
  const conversations = await executeSQL(conversationsSQL);
  
  console.log('\nUser conversations:');
  conversations.forEach(conv => {
    console.log(`- ${conv.title || 'Untitled'} (${conv.message_count} messages)`);
    console.log(`  ID: ${conv.conversation_id}`);
    console.log(`  Created: ${conv.created_at}`);
  });
  
  return conversations;
}

// ========================================
// EXAMPLE 5: Get Full Conversation Data
// ========================================

async function exampleGetFullConversation(executeSQL, userId, conversationId) {
  console.log('\n=== Getting Full Conversation with Metadata ===\n');
  
  const fullConvSQL = queries.getFullConversationForModel(conversationId, userId);
  console.log('SQL:', fullConvSQL);
  const fullConv = await executeSQL(fullConvSQL);
  
  console.log('\nFull conversation data:');
  console.log(JSON.stringify(fullConv, null, 2));
  
  return fullConv;
}

// ========================================
// EXAMPLE 6: Complete Workflow with MCP
// ========================================

/**
 * Example using MCP Supabase to execute queries
 * This would be called from your main application
 */
async function exampleWithMCP(mcpExecuteSQL) {
  const userId = '550e8400-e29b-41d4-a716-446655440000'; // Your user ID
  
  console.log('=== Complete Workflow Example ===\n');
  
  try {
    // Create new conversation
    const createConvSQL = safeQueries.createConversation(userId, 'AI Chat Session');
    const convResult = await mcpExecuteSQL(createConvSQL);
    const conversationId = convResult[0].conversation_id;
    console.log('Created conversation:', conversationId);
    
    // Add user message
    const userMsg = 'Explain quantum computing in simple terms.';
    const addMsgSQL = safeQueries.addMessage(conversationId, userId, 'user', userMsg);
    await mcpExecuteSQL(addMsgSQL);
    console.log('Added user message');
    
    // Get context for LLM
    const getContextSQL = queries.getConversationContext(conversationId, userId);
    const context = await mcpExecuteSQL(getContextSQL);
    console.log('Retrieved context:', context);
    
    // Format for LLM
    const messages = formatMessagesForLLM(context);
    const prompt = buildPromptWithHistory(messages, 'You are a helpful AI assistant.');
    
    console.log('\nPrompt ready for LLM:');
    console.log(JSON.stringify(prompt, null, 2));
    
    // After getting LLM response, save it
    // const llmResponse = await callYourLLM(prompt);
    // const saveResponseSQL = safeQueries.addMessage(conversationId, userId, 'assistant', llmResponse);
    // await mcpExecuteSQL(saveResponseSQL);
    
    return { conversationId, messages, prompt };
    
  } catch (error) {
    console.error('Error in workflow:', error);
    throw error;
  }
}

// ========================================
// PRACTICAL INTEGRATION TEMPLATE
// ========================================

/**
 * Template for integrating with your LLM service
 */
class ConversationService {
  constructor(executeSQLFunction) {
    this.executeSQL = executeSQLFunction;
  }
  
  async startConversation(userId, title = null) {
    // Ensure user exists
    await this.executeSQL(safeQueries.getOrCreateUser(userId));
    
    // Create conversation
    const result = await this.executeSQL(safeQueries.createConversation(userId, title));
    return result[0].conversation_id;
  }
  
  async sendMessage(userId, conversationId, message) {
    // Add user message
    await this.executeSQL(
      safeQueries.addMessage(conversationId, userId, 'user', message)
    );
    
    // Get conversation context
    const context = await this.executeSQL(
      queries.getConversationContext(conversationId, userId)
    );
    
    // Format for LLM
    const messages = formatMessagesForLLM(context);
    
    return messages;
  }
  
  async saveAssistantResponse(userId, conversationId, response, metadata = {}) {
    await this.executeSQL(
      safeQueries.addMessage(conversationId, userId, 'assistant', response, metadata)
    );
  }
  
  async getConversationHistory(userId, conversationId) {
    const messages = await this.executeSQL(
      queries.getConversationMessages(conversationId, userId)
    );
    return formatMessagesForLLM(messages);
  }
  
  async listUserConversations(userId, limit = 20) {
    return await this.executeSQL(
      queries.getUserConversations(userId, limit)
    );
  }
}

// ========================================
// EXPORTS
// ========================================

export {
  exampleCreateNewConversation,
  exampleGetContextForLLM,
  exampleConversationFlow,
  exampleGetUserConversations,
  exampleGetFullConversation,
  exampleWithMCP,
  ConversationService
};

// ========================================
// QUICK START GUIDE
// ========================================

/*

QUICK START:

1. Import the conversation manager:
   import { safeQueries, formatMessagesForLLM, queries } from './conversation-manager.js';

2. Create a new conversation:
   const convSQL = safeQueries.createConversation(userId, 'My Chat');
   const result = await executeSQL(convSQL);
   const conversationId = result[0].conversation_id;

3. Add a user message:
   const msgSQL = safeQueries.addMessage(conversationId, userId, 'user', 'Hello!');
   await executeSQL(msgSQL);

4. Get context for LLM:
   const contextSQL = queries.getConversationContext(conversationId, userId);
   const messages = await executeSQL(contextSQL);
   const formattedMessages = formatMessagesForLLM(messages);

5. Send to your LLM and save response:
   const response = await yourLLM(formattedMessages);
   const saveSQL = safeQueries.addMessage(conversationId, userId, 'assistant', response);
   await executeSQL(saveSQL);

That's it! You now have a complete conversation flow with database persistence.

*/

