import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../../utils/retry.js";
import { zendeskLimiter } from "../../utils/rateLimiters/zendesk.js";

const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

function zendeskRequest(fn) {
  const retries = Number(process.env.ZENDESK_RETRIES) || 3;
  const delay = Number(process.env.ZENDESK_RETRY_DELAY) || 1000;
  return withRetry(fn, retries, delay);
}

export function getZendeskClient() {
  return zendeskClient;
}

export function callZendesk(getPromise) {
  return zendeskRequest(getPromise);
}

export function getJobStatus(jobId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/job_statuses/${jobId}.json`));
    return res.data;
  });
}

export function getUserIdentities(userId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/users/${userId}/identities.json`));
    return res.data?.identities || [];
  });
}

export function getUser(userId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/users/${userId}.json`));
    return res.data?.user || null;
  });
}

export function updateUserCustomFields(userId, userFields) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.put(`/users/${userId}.json`, {
      user: { user_fields: userFields },
    }));
    return res.data?.user || null;
  });
}

/**
 * Delete a user identity from Zendesk
 * @param {number} userId - Zendesk user ID
 * @param {number} identityId - Identity ID to delete
 * @returns {Promise<boolean>} True if successful
 */
export function deleteUserIdentity(userId, identityId) {
  return zendeskRequest(async () => {
    await zendeskLimiter.schedule(() => 
      zendeskClient.delete(`/users/${userId}/identities/${identityId}.json`)
    );
    return true;
  });
}

/**
 * Delete the primary email from Zendesk user
 * This is different from deleting an email identity - the primary email must be deleted via /email endpoint
 * @param {number} userId - Zendesk user ID
 * @returns {Promise<boolean>} True if successful
 */
export function deleteUserPrimaryEmail(userId) {
  return zendeskRequest(async () => {
    await zendeskLimiter.schedule(() => 
      zendeskClient.delete(`/users/${userId}/email.json`)
    );
    return true;
  });
}

/**
 * Make an identity primary for a Zendesk user
 * Uses PUT /api/v2/users/{user_id}/identities/{user_identity_id}/make_primary
 * According to Zendesk docs, the 'primary' field is writable only when creating,
 * not when updating. Use this endpoint to make an existing identity primary.
 * @param {number} userId - Zendesk user ID
 * @param {number} identityId - Identity ID to make primary
 * @returns {Promise<boolean>} True if successful
 */
export function makeIdentityPrimary(userId, identityId) {
  return zendeskRequest(async () => {
    await zendeskLimiter.schedule(() => 
      zendeskClient.put(`/users/${userId}/identities/${identityId}/make_primary.json`)
    );
    return true;
  });
}

/**
 * Search for Zendesk user by email address
 * @param {string} email - Email address to search for
 * @returns {Promise<Object|null>} User object or null if not found
 */
export function searchUserByEmail(email) {
  return zendeskRequest(async () => {
    const query = `email:${email}`;
    const res = await zendeskLimiter.schedule(() => 
      zendeskClient.get(`/users/search.json?query=${encodeURIComponent(query)}`)
    );
    const users = res.data?.users || [];
    // Return first active user, or first user if no active users
    const activeUser = users.find(u => u.active) || users[0];
    return activeUser || null;
  });
}

/**
 * Search for Zendesk tickets using the search API
 * @param {string} query - Zendesk search query (e.g., "type:ticket status<solved custom_field_123:value")
 * @returns {Promise<Array>} Array of ticket objects
 */
export function searchTickets(query) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() =>
      zendeskClient.get(`/search.json?query=${encodeURIComponent(query)}`)
    );
    const results = res.data?.results || [];
    logger.debug(`🔍 Zendesk search query: "${query}" returned ${results.length} results`);
    return results;
  });
}

/**
 * Search for Zendesk tickets with pagination support
 * @param {string} query - Zendesk search query
 * @returns {Promise<Array>} Array of all ticket objects across all pages
 */
export async function searchTicketsWithPagination(query) {
  const allTickets = [];
  let nextPage = null;
  let pageCount = 0;

  do {
    const res = await zendeskRequest(async () => {
      return await zendeskLimiter.schedule(() => {
        if (nextPage) {
          // If nextPage is a full URL, extract the path relative to baseURL
          if (nextPage.startsWith('http')) {
            const path = nextPage.replace(zendeskClient.defaults.baseURL, '');
            return zendeskClient.get(path);
          } else {
            // If it's already a relative path, use it directly
            return zendeskClient.get(nextPage);
          }
        } else {
          // First page - use the query
          return zendeskClient.get(`/search.json?query=${encodeURIComponent(query)}`);
        }
      });
    });

    const results = res.data?.results || [];
    allTickets.push(...results);
    pageCount++;
    
    logger.info(`📄 Page ${pageCount}: Retrieved ${results.length} tickets (total: ${allTickets.length})`);

    // Check for next page
    nextPage = res.data?.next_page || null;
  } while (nextPage);

  logger.info(`✅ Search completed. Total tickets found: ${allTickets.length}`);
  return allTickets;
}

/**
 * Delete a Zendesk ticket
 * @param {number} ticketId - Ticket ID to delete
 * @returns {Promise<boolean>} True if successful
 */
export function deleteTicket(ticketId) {
  return zendeskRequest(async () => {
    await zendeskLimiter.schedule(() => 
      zendeskClient.delete(`/tickets/${ticketId}.json`)
    );
    return true;
  });
}


