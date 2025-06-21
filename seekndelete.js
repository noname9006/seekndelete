const { Client, GatewayIntentBits, Permissions } = require('discord.js');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');

// Load environment variables
dotenv.config();

// Configure logging
const logDir = path.join(__dirname, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// Log levels and their numeric values (for filtering)
const LOG_LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3
};

// Get the log level from environment variable or default to 'info'
const logLevelStr = (process.env.LOG_LEVEL || 'info').toLowerCase();
const MIN_LOG_LEVEL = LOG_LEVELS[logLevelStr] !== undefined ? LOG_LEVELS[logLevelStr] : LOG_LEVELS.info;

// File logging is disabled by default now
const enableFileLogging = process.env.ENABLE_FILE_LOGGING === 'true'; // Default is false

// Track active operations per channel
const activeOperations = new Map();

// Logger function
function logger(level, message, data = null) {
  // Skip logging if the level is below the minimum
  if (LOG_LEVELS[level] < MIN_LOG_LEVEL) {
    return;
  }

  const timestamp = new Date().toISOString();
  const logEntry = {
    timestamp,
    level,
    message,
    data
  };
  
  // Log to console
  const consoleMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  if (level === 'error') {
    console.error(consoleMessage);
    if (data) console.error(data);
  } else {
    console.log(consoleMessage);
    if (data) console.log(data);
  }
  
  // Log to file - only if explicitly enabled
  if (enableFileLogging) {
    const today = timestamp.split('T')[0];
    const logFile = path.join(logDir, `${today}.log`);
    
    fs.appendFileSync(
      logFile, 
      `${JSON.stringify(logEntry)}\n`, 
      { encoding: 'utf8' }
    );
  }
}

// Set default for case-sensitive search (false by default)
const useCaseSensitiveSearch = process.env.CASE_SENSITIVE_SEARCH === 'true'; // Default is false

// Helper function to check if a message contains the search content
// Optimized for speed - early returns and minimal processing
function messageContainsContent(message, searchContent) {
  // Use lowercase comparison for case-insensitive search
  const needle = useCaseSensitiveSearch ? searchContent : searchContent.toLowerCase();
  
  // Check regular message content first (most common case)
  if (message.content) {
    const haystack = useCaseSensitiveSearch ? message.content : message.content.toLowerCase();
    if (haystack.includes(needle)) return true;
  }
  
  // Check embeds if needed
  if (message.embeds && message.embeds.length > 0) {
    for (const embed of message.embeds) {
      // Check embed title
      if (embed.title) {
        const haystack = useCaseSensitiveSearch ? embed.title : embed.title.toLowerCase();
        if (haystack.includes(needle)) return true;
      }
      
      // Check embed description
      if (embed.description) {
        const haystack = useCaseSensitiveSearch ? embed.description : embed.description.toLowerCase();
        if (haystack.includes(needle)) return true;
      }
      
      // Check embed fields (common in bot messages)
      if (embed.fields && embed.fields.length > 0) {
        for (const field of embed.fields) {
          if (field.name) {
            const haystack = useCaseSensitiveSearch ? field.name : field.name.toLowerCase();
            if (haystack.includes(needle)) return true;
          }
          if (field.value) {
            const haystack = useCaseSensitiveSearch ? field.value : field.value.toLowerCase();
            if (haystack.includes(needle)) return true;
          }
        }
      }
      
      // Less common embed properties - only check if nothing found yet
      if (embed.author && embed.author.name) {
        const haystack = useCaseSensitiveSearch ? embed.author.name : embed.author.name.toLowerCase();
        if (haystack.includes(needle)) return true;
      }
      
      if (embed.footer && embed.footer.text) {
        const haystack = useCaseSensitiveSearch ? embed.footer.text : embed.footer.text.toLowerCase();
        if (haystack.includes(needle)) return true;
      }
    }
  }
  
  // Check webhook name/username fields
  if (message.webhookId && message.author && message.author.username) {
    const haystack = useCaseSensitiveSearch ? message.author.username : message.author.username.toLowerCase();
    if (haystack.includes(needle)) return true;
  }
  
  // Check attachments (file names) - lower priority
  if (message.attachments && message.attachments.size > 0) {
    for (const [_, attachment] of message.attachments) {
      if (attachment.name) {
        const haystack = useCaseSensitiveSearch ? attachment.name : attachment.name.toLowerCase();
        if (haystack.includes(needle)) return true;
      }
    }
  }
  
  return false;
}

// Helper function to determine message source type for logging
function getMessageSourceType(message) {
  if (message.webhookId) {
    return 'webhook';
  } else if (message.author.bot) {
    return 'bot';
  } else if (message.embeds && message.embeds.length > 0) {
    return 'embed';
  } else {
    return 'user';
  }
}

// Helper function to sleep for a given amount of time
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Helper function to format date in a readable format
function formatDate(timestamp) {
  const date = new Date(timestamp);
  return date.toISOString().replace('T', ' ').split('.')[0]; // YYYY-MM-DD HH:MM:SS
}

// Helper function to create a message link
function getMessageLink(message) {
  return `https://discord.com/channels/${message.guild.id}/${message.channel.id}/${message.id}`;
}

// Helper function to parse the max age parameter
function parseMaxAge(maxAgeStr) {
  if (!maxAgeStr) return null;
  
  // Try to parse as a simple number (days)
  const numberDays = parseInt(maxAgeStr, 10);
  if (!isNaN(numberDays)) {
    return numberDays * 24 * 60 * 60 * 1000; // Convert days to milliseconds
  }
  
  // Parse more complex format: 1d2h3m (1 day, 2 hours, 3 minutes)
  const dayMatch = maxAgeStr.match(/(\d+)d/);
  const hourMatch = maxAgeStr.match(/(\d+)h/);
  const minMatch = maxAgeStr.match(/(\d+)m/);
  
  let totalMs = 0;
  
  if (dayMatch) totalMs += parseInt(dayMatch[1], 10) * 24 * 60 * 60 * 1000;
  if (hourMatch) totalMs += parseInt(hourMatch[1], 10) * 60 * 60 * 1000;
  if (minMatch) totalMs += parseInt(minMatch[1], 10) * 60 * 1000;
  
  return totalMs > 0 ? totalMs : null;
}

// Helper function to get a human-readable representation of max age
function formatMaxAge(maxAgeMs) {
  if (!maxAgeMs) return "No limit";
  
  const days = Math.floor(maxAgeMs / (24 * 60 * 60 * 1000));
  const hours = Math.floor((maxAgeMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
  const minutes = Math.floor((maxAgeMs % (60 * 60 * 1000)) / (60 * 1000));
  
  let result = [];
  if (days > 0) result.push(`${days} day${days !== 1 ? 's' : ''}`);
  if (hours > 0) result.push(`${hours} hour${hours !== 1 ? 's' : ''}`);
  if (minutes > 0) result.push(`${minutes} minute${minutes !== 1 ? 's' : ''}`);
  
  return result.join(', ');
}

// Optimized helper function to delete messages in bulk (when possible)
async function bulkDeleteMessages(channel, messages, progressCallback, operationId) {
  // Group messages by age for optimal deletion
  const twoWeeksAgo = Date.now() - 14 * 24 * 60 * 60 * 1000;
  const recentMessages = [];
  const olderMessages = [];
  
  messages.forEach(msg => {
    if (msg.createdTimestamp > twoWeeksAgo) {
      recentMessages.push(msg);
    } else {
      olderMessages.push(msg);
    }
  });
  
  logger('info', `Deletion strategy: ${recentMessages.length} recent messages for bulk deletion, ${olderMessages.length} older messages for individual deletion`);
  
  let deletedCount = 0;
  let skippedCount = 0;
  let lastProgressReport = 0;
  const progressInterval = Math.max(10, Math.floor(messages.length / 10)); // Report progress 10 times
  
  // Process recent messages in bulk (up to 100 at a time)
  if (recentMessages.length > 0) {
    // Split into chunks of max 100
    const bulkChunks = [];
    for (let i = 0; i < recentMessages.length; i += 100) {
      bulkChunks.push(recentMessages.slice(i, i + 100));
    }
    
    logger('info', `Created ${bulkChunks.length} bulk deletion batches`);
    
    // Process each chunk
    for (const chunk of bulkChunks) {
      // Check if operation was aborted
      if (!activeOperations.has(operationId)) {
        logger('info', `Operation ${operationId} was aborted during bulk deletion`);
        return { deletedCount, skippedCount, aborted: true };
      }
      
      try {
        // Extract IDs for bulk deletion
        const messageIds = chunk.map(msg => msg.id);
        await channel.bulkDelete(messageIds);
        
        deletedCount += messageIds.length;
        if (deletedCount - lastProgressReport >= progressInterval) {
          lastProgressReport = deletedCount;
          await progressCallback(deletedCount, skippedCount, messages.length);
        }
        
        // Brief pause between bulk operations
        await sleep(800); // Reduced from 1000ms
      } catch (err) {
        logger('error', `Bulk deletion failed, falling back to individual deletion for this batch`, err);
        
        // If bulk delete fails, try individually but with parallel processing
        const deletionPromises = chunk.map(async msg => {
          // Check again if operation was aborted
          if (!activeOperations.has(operationId)) {
            return { success: false, aborted: true };
          }
          
          try {
            await msg.delete();
            return { success: true };
          } catch (err) {
            logger('error', `Failed to delete message with ID ${msg.id}`, err);
            return { success: false };
          }
        });
        
        // Wait for all deletions to complete
        const results = await Promise.allSettled(deletionPromises);
        
        // Check if any result was aborted
        if (results.some(r => r.status === 'fulfilled' && r.value.aborted)) {
          logger('info', `Operation ${operationId} was aborted during individual deletion fallback`);
          return { deletedCount, skippedCount, aborted: true };
        }
        
        // Count successes and failures
        const successCount = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
        const failCount = results.filter(r => r.status === 'rejected' || (r.status === 'fulfilled' && !r.value.success)).length;
        
        deletedCount += successCount;
        skippedCount += failCount;
        
        // Add smaller delay for individual deletes
        await sleep(1000);
      }
    }
  }
  
  // Process older messages individually (these can't be bulk deleted)
  // Use parallel processing with controlled concurrency
  if (olderMessages.length > 0) {
    // Check if operation was aborted
    if (!activeOperations.has(operationId)) {
      logger('info', `Operation ${operationId} was aborted before processing older messages`);
      return { deletedCount, skippedCount, aborted: true };
    }
    
    logger('info', `Processing ${olderMessages.length} older messages individually with parallel processing`);
    
    // Process in larger parallel batches with controlled concurrency
    const BATCH_SIZE = 25; // Process 25 messages at a time
    const MAX_CONCURRENT = 5; // But with at most 5 concurrent operations
    
    for (let i = 0; i < olderMessages.length; i += BATCH_SIZE) {
      // Check if operation was aborted
      if (!activeOperations.has(operationId)) {
        logger('info', `Operation ${operationId} was aborted during older message batch processing`);
        return { deletedCount, skippedCount, aborted: true };
      }
      
      const batch = olderMessages.slice(i, i + BATCH_SIZE);
      const batchResults = [];
      
      // Process messages in smaller concurrent chunks
      for (let j = 0; j < batch.length; j += MAX_CONCURRENT) {
        // Check if operation was aborted
        if (!activeOperations.has(operationId)) {
          logger('info', `Operation ${operationId} was aborted during concurrent chunk processing`);
          return { deletedCount, skippedCount, aborted: true };
        }
        
        const concurrentBatch = batch.slice(j, j + MAX_CONCURRENT);
        
        const deletionPromises = concurrentBatch.map(async msg => {
          // Check if operation was aborted for each message
          if (!activeOperations.has(operationId)) {
            return { success: false, aborted: true };
          }
          
          try {
            await msg.delete();
            return { success: true };
          } catch (err) {
            logger('error', `Failed to delete message with ID ${msg.id}`, err);
            return { success: false };
          }
        });
        
        const results = await Promise.all(deletionPromises);
        
        // Check if any result was aborted
        if (results.some(r => r.aborted)) {
          logger('info', `Operation ${operationId} was aborted during concurrent message deletion`);
          return { deletedCount, skippedCount, aborted: true };
        }
        
        batchResults.push(...results);
        
        // Small delay between concurrent batches
        if (j + MAX_CONCURRENT < batch.length) {
          await sleep(200);
        }
      }
      
      // Count successes and failures
      const successCount = batchResults.filter(r => r.success).length;
      const failCount = batchResults.filter(r => !r.success).length;
      
      deletedCount += successCount;
      skippedCount += failCount;
      
      if (deletedCount - lastProgressReport >= progressInterval) {
        lastProgressReport = deletedCount;
        await progressCallback(deletedCount, skippedCount, messages.length);
      }
      
      // Rate limit prevention between batches
      await sleep(1500); // Reduced from 2500ms
    }
  }
  
  return { deletedCount, skippedCount, aborted: false };
}

// Create a new client instance with only needed intents to improve performance
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers
  ],
  // Optimize the REST rate limit handling
  rest: {
    retries: 3,
    timeout: 15000
  }
});

// When the client is ready, run this code (only once)
client.once('ready', () => {
  logger('info', `Bot started and logged in as ${client.user.tag}`);
  logger('info', `Current date: ${new Date().toISOString()}`);
  logger('info', `Log level set to: ${logLevelStr.toUpperCase()}`);
  logger('info', `Case-sensitive search: ${useCaseSensitiveSearch}`);
  logger('info', `File logging: ${enableFileLogging}`);
});

// Message fetching with optimized speed
async function fetchMessages(channel, options = {}, operationId) {
  try {
    // Use a shorter timeout for faster response
    return await channel.messages.fetch(options);
  } catch (err) {
    // Check if operation was aborted
    if (operationId && !activeOperations.has(operationId)) {
      logger('info', `Operation ${operationId} was aborted during message fetching`);
      return { size: 0, aborted: true };
    }
    
    logger('error', `Error fetching messages: ${err.message}`);
    // Add a small delay before retrying to avoid rate limits
    await sleep(1000);
    return await channel.messages.fetch(options);
  }
}

// Command handler
client.on('messageCreate', async message => {
  // Ignore messages from bots
  if (message.author.bot) return;
  
  // Check if the message is a command
  if (message.content.startsWith('>seekndelete')) {
    // Check if user has admin permissions
    if (!message.member.permissions.has('ADMINISTRATOR')) {
      logger('warn', `User ${message.author.tag} attempted to use command without admin permissions`);
      return message.reply('You need administrator permissions to use this command.');
    }
    
    // Check for abort command
    if (message.content.trim() === '>seekndelete abort') {
      const channelId = message.channel.id;
      
      // Find operations for this channel
      const operationsToAbort = [];
      for (const [opId, operation] of activeOperations.entries()) {
        if (operation.channelId === channelId) {
          operationsToAbort.push(opId);
        }
      }
      
      if (operationsToAbort.length === 0) {
        logger('info', `No active operations to abort in channel ${channelId}`);
        return message.reply('There are no active deletion operations in this channel to abort.');
      }
      
      // Abort all operations for this channel
      logger('info', `Aborting ${operationsToAbort.length} operations in channel ${channelId}`);
      for (const opId of operationsToAbort) {
        activeOperations.delete(opId);
      }
      
      return message.reply(`Aborted ${operationsToAbort.length} active deletion ${operationsToAbort.length === 1 ? 'operation' : 'operations'} in this channel.`);
    }
    
    // Handle regular seekndelete command
    logger('info', `Command received in channel ${message.channel.name}`, {
      user: message.author.tag,
      command: message.content
    });
    
    // Parse the command
    // Expected format: >seekndelete "messagecontent" [@sender] [maxAge]
    const args = message.content.substring('>seekndelete'.length).trim();
    
    // Extract the message content (between quotes)
    const contentMatch = args.match(/"([^"]*)"/);
    if (!contentMatch) {
      logger('warn', `Invalid command format - missing message content in quotes`);
      return message.reply('Please provide message content in quotes: >seekndelete "message content" [@user] [maxAge]');
    }
    const searchContent = contentMatch[1];
    
    // Check if content is empty
    if (searchContent.trim() === '') {
      logger('warn', `Invalid command format - empty message content`);
      return message.reply('Please provide non-empty message content in quotes: >seekndelete "message content" [@user] [maxAge]');
    }
    
    // Remove the quoted content from args for further processing
    let remainingArgs = args.replace(/"([^"]*)"/, '').trim();
    
    // Extract the mentioned user if any
    let targetUserId = null;
    let searchingAllUsers = false;
    
    if (message.mentions.users.size > 1) {
      logger('warn', `Invalid command format - too many users mentioned`);
      return message.reply('Please mention only one user or none at all: >seekndelete "message content" [@user] [maxAge]');
    } else if (message.mentions.users.size === 1) {
      targetUserId = message.mentions.users.first().id;
      logger('info', `Searching messages from specific user: ${message.mentions.users.first().tag}`);
      
      // Remove the user mention from args
      const mentionStr = `<@${targetUserId}>`;
      remainingArgs = remainingArgs.replace(mentionStr, '').trim();
    } else {
      searchingAllUsers = true;
      logger('info', `Searching messages from all users`);
    }
    
    // Parse the max age parameter (if present)
    const maxAgeStr = remainingArgs.trim();
    const maxAgeMs = parseMaxAge(maxAgeStr);
    
    // Calculate the cutoff time based on max age
    let cutoffTime = null;
    if (maxAgeMs) {
      cutoffTime = Date.now() - maxAgeMs;
      logger('info', `Using max age of ${formatMaxAge(maxAgeMs)}, cutoff time: ${new Date(cutoffTime).toISOString()}`);
    } else {
      logger('info', `No max age specified, searching without time limit`);
    }
    
    // Create a unique operation ID for this task
    const operationId = `${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;
    
    // Register the operation
    activeOperations.set(operationId, {
      channelId: message.channel.id,
      userId: message.author.id,
      startTime: Date.now(),
      type: 'search',
      details: {
        searchContent,
        targetUserId,
        maxAge: maxAgeMs ? formatMaxAge(maxAgeMs) : 'No limit'
      }
    });
    
    try {
      // Inform user that search is in progress
      const searchingMsg = await message.channel.send(
        searchingAllUsers ? 
        `Searching for messages from all users${maxAgeMs ? ' within the last ' + formatMaxAge(maxAgeMs) : ''}, please wait...` : 
        `Searching for messages from the specified user${maxAgeMs ? ' within the last ' + formatMaxAge(maxAgeMs) : ''}, please wait...`
      );
      
      // Find messages that match the criteria - optimized implementation
      const matchingMessages = [];
      let lastId = null;
      let fetched;
      let fetchCount = 0;
      let reachedCutoff = false;
      let searchingAnimation;
      let abortedDuringSearch = false;
      
      // Create a search progress animation
      if (MIN_LOG_LEVEL <= LOG_LEVELS.debug) {
        searchingAnimation = setInterval(async () => {
          // Check if operation was aborted
          if (!activeOperations.has(operationId)) {
            clearInterval(searchingAnimation);
            return;
          }
          
          try {
            await searchingMsg.edit(`Searching... Fetched ${fetchCount} message batches, found ${matchingMessages.length} matches so far.`);
          } catch (error) {
            logger('error', 'Failed to update search animation', error);
            clearInterval(searchingAnimation);
          }
        }, 3000);
      }
      
      // Configure message fetch size (larger batch size for faster search)
      const fetchLimit = 100; // Maximum supported by Discord API
      
      // Prepare optimized search params
      const searchTerm = useCaseSensitiveSearch ? searchContent : searchContent.toLowerCase();
      
      const startTime = Date.now();
      
      // Update operation status
      activeOperations.get(operationId).type = 'searching';
      
      // The main search loop - optimized for speed
      try {
        do {
          // Check if operation was aborted
          if (!activeOperations.has(operationId)) {
            logger('info', `Search operation ${operationId} was aborted`);
            abortedDuringSearch = true;
            break;
          }
          
          const options = { limit: fetchLimit };
          if (lastId) {
            options.before = lastId;
          }
          
          fetchCount++;
          
          // Fetch messages with optimized function
          fetched = await fetchMessages(message.channel, options, operationId);
          
          // Check if operation was aborted during fetch
          if (fetched.aborted) {
            abortedDuringSearch = true;
            break;
          }
          
          lastId = fetched.last()?.id;
          
          // Check if we've reached the cutoff time
          if (cutoffTime && fetched.size > 0 && fetched.last().createdTimestamp < cutoffTime) {
            reachedCutoff = true;
            logger('debug', `Reached cutoff time in batch ${fetchCount}`);
          }
          
          // Filter messages that match criteria - optimized with early returns
          for (const [id, msg] of fetched) {
            // Check if operation was aborted
            if (!activeOperations.has(operationId)) {
              logger('info', `Search operation ${operationId} was aborted during message filtering`);
              abortedDuringSearch = true;
              break;
            }
            
            // Skip if beyond cutoff time
            if (cutoffTime && msg.createdTimestamp < cutoffTime) continue;
            
            // For webhooks, we need special handling
            const isWebhook = !!msg.webhookId;
            
            // Check if the author matches (if we're filtering by user)
            // Note: For webhooks, we'll check based on searchingAllUsers only
            const userMatches = searchingAllUsers || (!isWebhook && msg.author.id === targetUserId);
            if (!userMatches) continue;
            
            // Check if content matches (including embeds)
            const contentMatches = messageContainsContent(msg, searchContent);
            if (contentMatches) {
              matchingMessages.push(msg);
            }
          }
          
          // Check if the operation was aborted during message filtering
          if (abortedDuringSearch) break;
          
          // Introduce small delay only if we're hitting rate limits
          // This is a compromise between speed and reliability
          if (fetched.size > 0 && fetchCount % 5 === 0) {
            await sleep(300);
          }
        } while (fetched.size > 0 && !reachedCutoff);
      } finally {
        // Clear the animation interval if it was created
        if (searchingAnimation) {
          clearInterval(searchingAnimation);
        }
      }
      
      const searchDuration = (Date.now() - startTime) / 1000;
      logger('info', `Search completed in ${searchDuration.toFixed(2)}s. Found ${matchingMessages.length} matching messages in ${fetchCount} batches.`);
      
      // If operation was aborted during search, clean up and exit
      if (abortedDuringSearch || !activeOperations.has(operationId)) {
        // Try to delete the searching message
        try {
          await searchingMsg.delete();
        } catch (error) {
          logger('warn', 'Could not delete search message after abort', error);
        }
        
        // Try to send abort notification
        try {
          await message.channel.send('Search operation was aborted.');
        } catch (error) {
          logger('error', 'Failed to send abort notification', error);
        }
        
        // Remove the operation if it's still there
        activeOperations.delete(operationId);
        return;
      }
      
      // Delete the "searching" message
      try {
        await searchingMsg.delete();
      } catch (error) {
        logger('warn', 'Could not delete search message', error);
      }
      
      if (matchingMessages.length === 0) {
        logger('info', `No matching messages found`);
        // Remove the operation from active operations
        activeOperations.delete(operationId);
        return message.reply(
          searchingAllUsers ?
          `No messages found containing "${searchContent}"${maxAgeMs ? ' within the last ' + formatMaxAge(maxAgeMs) : ''}.` :
          `No messages found from <@${targetUserId}> containing "${searchContent}"${maxAgeMs ? ' within the last ' + formatMaxAge(maxAgeMs) : ''}.`
        );
      }
      
      // Sort messages by timestamp (oldest first)
      matchingMessages.sort((a, b) => a.createdTimestamp - b.createdTimestamp);
      
      // Get first and last message for links
      const oldestMessage = matchingMessages[0];
      const newestMessage = matchingMessages[matchingMessages.length - 1];
      
      const oldestDate = formatDate(oldestMessage.createdTimestamp);
      const newestDate = formatDate(newestMessage.createdTimestamp);
      
      const oldestLink = getMessageLink(oldestMessage);
      const newestLink = getMessageLink(newestMessage);
      
      // Count message sources but only if searching all users (optimization)
      const sourceStats = searchingAllUsers ? {
        user: 0,
        bot: 0,
        webhook: 0,
        embed: 0
      } : null;
      
      // Get unique senders if searching all users
      let fromText = '';
      let uniqueSenders = [];
      
      if (searchingAllUsers) {
        // Optimized unique sender calculation
        const senderMap = new Map();
        
        matchingMessages.forEach(msg => {
          // Count by source type if needed
          if (sourceStats) {
            const sourceType = getMessageSourceType(msg);
            sourceStats[sourceType]++;
          }
          
          // Track unique senders
          if (msg.webhookId) {
            const webhookName = msg.author.username || 'Webhook';
            senderMap.set('wb_' + webhookName, webhookName);
          } else {
            senderMap.set('u_' + msg.author.id, msg.author.id);
          }
        });
        
        // Convert to array of senders
        uniqueSenders = Array.from(senderMap.entries()).map(([key, value]) => {
          return { 
            isWebhook: key.startsWith('wb_'),
            id: value
          };
        });
        
        logger('info', `Found messages from ${uniqueSenders.length} unique sources`);
        
        // Format the list of senders
        if (uniqueSenders.length <= 10) {
          // If 10 or fewer senders, list them all
          fromText = 'From: ' + uniqueSenders.map(sender => {
            if (!sender.isWebhook) {
              return `<@${sender.id}>`;
            } else {
              return `"${sender.id}" (Webhook)`;
            }
          }).join(', ');
        } else {
          // If more than 10 senders, show count and first few
          const firstFew = uniqueSenders.slice(0, 5).map(sender => {
            if (!sender.isWebhook) {
              return `<@${sender.id}>`;
            } else {
              return `"${sender.id}" (Webhook)`;
            }
          });
          
          fromText = `From: ${uniqueSenders.length} sources including ` + 
                     firstFew.join(', ') + 
                     ` and ${uniqueSenders.length - 5} more`;
        }
      } else {
        fromText = `From: <@${targetUserId}>`;
      }
      
      // Include source statistics in the embed if there are mixed sources
      let sourceStatsText = '';
      if (sourceStats && (sourceStats.webhook > 0 || sourceStats.bot > 0)) {
        sourceStatsText = '\n\nSources:';
        if (sourceStats.user > 0) sourceStatsText += `\nUsers: ${sourceStats.user}`;
        if (sourceStats.bot > 0) sourceStatsText += `\nBots: ${sourceStats.bot}`;
        if (sourceStats.webhook > 0) sourceStatsText += `\nWebhooks: ${sourceStats.webhook}`;
      }
      
      // Max age info
      const maxAgeText = maxAgeMs ? `\nMax age: ${formatMaxAge(maxAgeMs)}` : '';
      
      // Time range information
      const timeRangeText = `\n\nTime range: ${oldestDate} to ${newestDate}`;
      
      // Message links
      const linksText = `\n\n[Oldest Message](${oldestLink}) | [Newest Message](${newestLink})`;
      
      // Operation ID info (for debugging)
      const operationText = `\n\nOperation ID: ${operationId}`;
      
      // Create confirmation embed
      const embed = {
        color: 0xFFD700, // Yellow for the embed
        title: 'Message Deletion Confirmation',
        description: `Found ${matchingMessages.length} messages\n${fromText}\nContaining: "${searchContent}"${maxAgeText}${sourceStatsText}${timeRangeText}${linksText}${operationText}\n\nDelete?`,
        footer: { text: 'This action can only be performed by administrators. Use >seekndelete abort to cancel.' }
      };
      
      // Create buttons
      const row = {
        type: 1,
        components: [
          {
            type: 2,
            style: 3, // SUCCESS (green)
            label: 'Yes',
            custom_id: 'confirm_delete'
          },
          {
            type: 2,
            style: 1, // PRIMARY (blue)
            label: 'No',
            custom_id: 'cancel_delete'
          }
        ]
      };
      
      // Update operation status
      activeOperations.get(operationId).type = 'awaiting_confirmation';
      activeOperations.get(operationId).messages = matchingMessages.length;
      
      const confirmationMessage = await message.channel.send({ embeds: [embed], components: [row] });
      logger('info', `Sent confirmation message with ID ${confirmationMessage.id}`);
      
      // Create collector for button interactions
      const filter = i => i.member.permissions.has('ADMINISTRATOR') && ['confirm_delete', 'cancel_delete'].includes(i.customId);
      const collector = confirmationMessage.createMessageComponentCollector({ filter, time: 60000 });
      
      collector.on('collect', async interaction => {
        // Check if operation still exists
        if (!activeOperations.has(operationId)) {
          logger('info', `Operation ${operationId} no longer exists, ignoring button click`);
          await interaction.reply({ content: 'This operation has been aborted.', ephemeral: true });
          collector.stop();
          return;
        }
        
        logger('info', `Button pressed: ${interaction.customId}`, {
          user: interaction.user.tag,
          userId: interaction.user.id
        });
        
        if (interaction.customId === 'cancel_delete') {
          // User clicked "No"
          logger('info', `Deletion cancelled by ${interaction.user.tag}`);
          // Remove the operation from active operations
          activeOperations.delete(operationId);
          
          try {
            await confirmationMessage.delete();
          } catch (error) {
            logger('error', 'Failed to delete confirmation message', error);
          }
          collector.stop();
        } else if (interaction.customId === 'confirm_delete') {
          // User clicked "Yes"
          logger('info', `Deletion confirmed by ${interaction.user.tag}. Starting to delete ${matchingMessages.length} messages`);
          
          // Update operation status
          activeOperations.get(operationId).type = 'deleting';
          
          // Create a separate progress message instead of using the interaction
          // This avoids the "Invalid Webhook Token" error when the operation takes too long
          const progressMessage = await message.channel.send({
            embeds: [{
              color: 0xFFD700,
              title: 'Deletion in progress',
              description: `Starting deletion of ${matchingMessages.length} messages...\n\nTo cancel this operation, use command: >seekndelete abort`
            }]
          });
          
          // Store progress message in operation data
          activeOperations.get(operationId).progressMessageId = progressMessage.id;
          
          // Progress callback to update the status message
          const updateProgress = async (deleted, skipped, total) => {
            // Check if operation still exists
            if (!activeOperations.has(operationId)) {
              return;
            }
            
            const progressEmbed = {
              color: 0xFFD700,
              title: 'Deletion in progress',
              description: `Progress: ${deleted + skipped}/${total} messages processed\nDeleted: ${deleted}\nSkipped: ${skipped}\n\nTo cancel this operation, use command: >seekndelete abort`
            };
            
            try {
              // Try to update the progress message
              await progressMessage.edit({ embeds: [progressEmbed] });
            } catch (error) {
              // Just log the error but continue with the deletion
              logger('error', 'Failed to update progress message', error);
            }
          };
          
          // Initial acknowledgement for the interaction
          try {
            await interaction.update({ 
              content: 'Deletion started! You can track progress in the new message below.', 
              embeds: [], 
              components: [] 
            });
          } catch (error) {
            logger('error', 'Failed to update interaction', error);
            // We can still continue with the process regardless
          }
          
          // Use optimized bulk deletion
          const startTime = Date.now();
          const result = await bulkDeleteMessages(message.channel, matchingMessages, updateProgress, operationId);
          const deleteDuration = (Date.now() - startTime) / 1000;
          
          // Remove operation from active operations
          activeOperations.delete(operationId);
          
          if (result.aborted) {
            logger('info', `Deletion operation ${operationId} was manually aborted after deleting ${result.deletedCount} messages`);
            
            // Send aborted message
            const abortedEmbed = {
              color: 0xFF0000,
              title: 'Deletion Aborted',
              description: `The operation was manually aborted.\n\nProgress before abort:\nDeleted: ${result.deletedCount}\nSkipped: ${result.skippedCount}`
            };
            
            try {
              await progressMessage.edit({ embeds: [abortedEmbed] });
            } catch (error) {
              logger('error', 'Failed to update abort message', error);
              try {
                await message.channel.send({
                  embeds: [abortedEmbed],
                  content: '(Previous message update failed)'
                });
              } catch (secondError) {
                logger('error', 'Failed to send abort message', secondError);
              }
            }
          } else {
            logger('info', `Deletion complete in ${deleteDuration.toFixed(2)}s. Deleted: ${result.deletedCount}, Skipped: ${result.skippedCount}`);
            
            // Send completion message by updating the progress message
            const completionEmbed = {
              color: 0x00ff00,
              title: 'Deletion Complete',
              description: `Successfully deleted ${result.deletedCount} messages in ${deleteDuration.toFixed(1)}s`
            };
            
            if (result.skippedCount > 0) {
              completionEmbed.description += `\nSkipped: ${result.skippedCount} messages`;
            }
            
            try {
              await progressMessage.edit({ embeds: [completionEmbed] });
            } catch (error) {
              logger('error', 'Failed to update completion message', error);
              // Try to send a new message as a last resort
              try {
                await message.channel.send({
                  embeds: [completionEmbed],
                  content: '(Previous message update failed)'
                });
              } catch (secondError) {
                logger('error', 'Failed to send completion message', secondError);
              }
            }
          }
          
          collector.stop();
        }
      });
      
      collector.on('end', (collected, reason) => {
        // Check if operation still exists
        if (!activeOperations.has(operationId)) {
          return;
        }
        
        if (reason === 'time') {
          logger('info', `Confirmation timed out after 60 seconds`);
          // Remove operation from active operations
          activeOperations.delete(operationId);
          
          try {
            confirmationMessage.edit({ content: 'Confirmation timed out.', embeds: [], components: [] });
          } catch (error) {
            logger('error', 'Failed to edit confirmation message after timeout', error);
          }
        }
      });
      
    } catch (error) {
      logger('error', `Error while processing command`, error);
      // Make sure to clean up the operation on error
      activeOperations.delete(operationId);
      
      try {
        await message.reply('An error occurred while processing your command.');
      } catch (replyError) {
        logger('error', 'Failed to send error reply', replyError);
      }
    }
  }
});

// Error handling for the Discord client
client.on('error', error => {
  logger('error', 'Discord client error', error);
});

client.on('warn', info => {
  logger('warn', 'Discord client warning', info);
});

client.on('disconnect', event => {
  logger('warn', `Bot disconnected from Discord`, event);
});

client.on('reconnecting', () => {
  logger('info', 'Bot reconnecting to Discord');
});

// Login to Discord with your app's token
client.login(process.env.DISCORD_TOKEN)
  .then(() => logger('info', 'Bot logged in successfully'))
  .catch(error => logger('error', 'Failed to log in', error));