import { logger } from "../../config/logger.js";
import { callZendesk, getZendeskClient, getUser } from "./zendesk.api.js";
import { zendeskLimiter } from "../../utils/limiter.js";
import { runWithLimit } from "../../utils/rateLimiter.js";

/**
 * Calculate the last day of the current month in ISO 8601 format
 * @returns {string} ISO 8601 date string (e.g., "2025-12-31T23:59:59Z")
 */
export function getLastDayOfMonth() {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth();
  // Get last day of current month
  const lastDay = new Date(year, month + 1, 0);
  // Set to end of day (23:59:59)
  lastDay.setHours(23, 59, 59, 999);
  return lastDay.toISOString();
}

/**
 * Calculate the Friday of the current week in ISO 8601 format
 * @returns {string} ISO 8601 date string (e.g., "2025-12-12T23:59:59Z")
 */
export function getFridayOfCurrentWeek() {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0 = Sunday, 1 = Monday, ..., 5 = Friday, 6 = Saturday
  const daysUntilFriday = (5 - dayOfWeek + 7) % 7; // Calculate days until Friday
  const friday = new Date(now);
  friday.setDate(now.getDate() + daysUntilFriday);
  // Set to end of day (23:59:59)
  friday.setHours(23, 59, 59, 999);
  return friday.toISOString();
}

/**
 * Create a private task ticket in Zendesk
 * 
 * @param {Object} options
 * @param {number} options.requesterId - Zendesk user ID of the client (requester)
 * @param {string} options.subject - Ticket subject
 * @param {string} options.dueAt - Due date in ISO 8601 format
 * @param {string|null} options.contactCategoryValue - Contact Category custom field value (optional, TBD)
 * @param {number|null} options.contactCategoryFieldId - Contact Category custom field ID (optional, from env var)
 * @returns {Promise<Object|null>} Created ticket object or null if failed
 */
export async function createPrivateTaskTicket({
  requesterId,
  subject,
  dueAt,
  contactCategoryValue = null,
  contactCategoryFieldId = null,
}) {
  if (!requesterId || !subject || !dueAt) {
    logger.error("❌ Missing required fields for ticket creation");
    return null;
  }

  return callZendesk(async () => {
    // Build custom fields array if contact category is provided
    const customFields = [];
    if (contactCategoryValue && contactCategoryFieldId) {
      customFields.push({
        id: contactCategoryFieldId,
        value: contactCategoryValue,
      });
    }

    const ticketPayload = {
      ticket: {
        subject,
        type: "task",
        priority: "normal",
        status: "open",
        requester_id: requesterId,
        due_at: dueAt,
        comment: {
          body: "Automated recurring check-in ticket",
          public: false, // Private comment
        },
      },
    };

    // Add custom fields if provided
    if (customFields.length > 0) {
      ticketPayload.ticket.custom_fields = customFields;
    }

    try {
      const res = await zendeskLimiter.schedule(() =>
        getZendeskClient().post("/tickets.json", ticketPayload)
      );

      const ticket = res.data?.ticket;
      if (!ticket) {
        logger.warn(`⚠️ No ticket returned from Zendesk API for requester ${requesterId}`);
        return null;
      }

      logger.info(
        `✅ Created ticket #${ticket.id} for requester ${requesterId}: "${subject}"`
      );
      return ticket;
    } catch (error) {
      const errorMsg = error.response?.data || error.message;
      logger.error(
        `❌ Failed to create ticket for requester ${requesterId}: ${JSON.stringify(errorMsg)}`
      );
      throw error;
    }
  });
}

/**
 * Create multiple tickets in batch with concurrency control
 * 
 * @param {Array} ticketConfigs - Array of ticket configuration objects
 * @param {number} concurrency - Maximum concurrent ticket creations
 * @returns {Promise<Array>} Array of results (ticket objects or errors)
 */
export async function createTicketsBatch(ticketConfigs, concurrency = 5) {
  const tasks = ticketConfigs.map((config) => async () => {
    try {
      const ticket = await createPrivateTaskTicket(config);
      return { success: true, ticket, config };
    } catch (error) {
      return { success: false, error: error.message, config };
    }
  });

  const results = await runWithLimit(tasks, concurrency);
  return results;
}

/**
 * Get Zendesk user data by user ID
 * This fetches the user from Zendesk to get the correct email/name
 * 
 * @param {number} zendeskUserId - Zendesk user ID
 * @returns {Promise<Object|null>} User object or null if not found
 */
export async function getZendeskUserData(zendeskUserId) {
  if (!zendeskUserId) {
    return null;
  }

  try {
    const user = await getUser(zendeskUserId);
    return user;
  } catch (error) {
    logger.error(
      `❌ Failed to fetch Zendesk user ${zendeskUserId}: ${error.message}`
    );
    return null;
  }
}
