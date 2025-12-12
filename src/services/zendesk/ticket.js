import { logger } from "../../config/logger.js";
import { callZendesk, getZendeskClient, getUser } from "./zendesk.api.js";
import { zendeskLimiter } from "../../utils/limiter.js";
import { runWithLimit } from "../../utils/rateLimiter.js";

/**
 * Get current date components in EST timezone
 * @returns {Object} Object with year, month (0-indexed), day
 */
function getCurrentDateInEST() {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  
  const parts = formatter.formatToParts(now);
  return {
    year: parseInt(parts.find((p) => p.type === "year").value),
    month: parseInt(parts.find((p) => p.type === "month").value) - 1, // 0-indexed
    day: parseInt(parts.find((p) => p.type === "day").value),
  };
}

/**
 * Get the UTC offset for a specific date in America/New_York timezone
 * Returns offset in minutes (EST = -300, EDT = -240)
 * @param {Date} date - Date to check
 * @returns {number} Offset in minutes
 */
function getESTOffsetMinutes(date) {
  // Create two formatters: one for EST and one for UTC
  const estFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "short",
  });
  
  const utcFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    timeZoneName: "short",
  });
  
  // Get the time in EST and UTC for the same moment
  const estParts = estFormatter.formatToParts(date);
  const utcParts = utcFormatter.formatToParts(date);
  
  // Calculate the difference
  // This is a bit complex, so let's use a simpler approach:
  // Format the date in EST, then create a date from that string and compare to UTC
  const estDateStr = estFormatter.format(date);
  const utcDateStr = utcFormatter.format(date);
  
  // Actually, simpler: create a date string in EST format and parse it
  // EST is UTC-5 (300 minutes) or EDT is UTC-4 (240 minutes)
  // We can determine which by checking what timezone abbreviation is used
  const tzName = estParts.find((p) => p.type === "timeZoneName")?.value || "";
  return tzName.includes("EDT") ? -240 : -300; // EDT = UTC-4, EST = UTC-5
}

/**
 * Convert EST date/time to UTC ISO string
 * @param {number} year - Year in EST
 * @param {number} month - Month (0-indexed) in EST
 * @param {number} day - Day in EST
 * @param {number} hour - Hour (0-23) in EST
 * @param {number} minute - Minute in EST
 * @param {number} second - Second in EST
 * @param {number} millisecond - Millisecond in EST
 * @returns {string} ISO 8601 UTC string
 */
function convertESTToUTC(year, month, day, hour, minute, second, millisecond = 0) {
  // Create date string in ISO format
  const dateString = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}.${String(millisecond).padStart(3, "0")}`;
  
  // First, try with EST offset (UTC-5)
  let testDate = new Date(`${dateString}-05:00`);
  
  // Check if this date, when formatted back in EST, matches our input
  // This verifies we used the correct offset
  const estFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  
  const estParts = estFormatter.formatToParts(testDate);
  const formattedYear = parseInt(estParts.find((p) => p.type === "year").value);
  const formattedMonth = parseInt(estParts.find((p) => p.type === "month").value) - 1;
  const formattedDay = parseInt(estParts.find((p) => p.type === "day").value);
  const formattedHour = parseInt(estParts.find((p) => p.type === "hour").value);
  
  // If formatted date matches input, we have correct offset
  if (formattedYear === year && formattedMonth === month && formattedDay === day && formattedHour === hour) {
    return testDate.toISOString();
  }
  
  // Otherwise, try EDT (UTC-4)
  testDate = new Date(`${dateString}-04:00`);
  return testDate.toISOString();
}

/**
 * Calculate the last day of the current month in EST, then convert to UTC
 * @returns {string} ISO 8601 date string in UTC (e.g., "2026-01-01T04:59:59.999Z" for Dec 31 11:59:59 PM EST)
 */
export function getLastDayOfMonth() {
  const estNow = getCurrentDateInEST();
  const year = estNow.year;
  const month = estNow.month;
  
  // Get last day of current month
  // Create a date for the first day of next month, then subtract 1 day
  const lastDay = new Date(year, month + 1, 0).getDate();
  
  // Return UTC ISO string for last day of month at 23:59:59.999 EST
  return convertESTToUTC(year, month, lastDay, 23, 59, 59, 999);
}

/**
 * Calculate the Friday of the current week in EST, then convert to UTC
 * @returns {string} ISO 8601 date string in UTC
 */
export function getFridayOfCurrentWeek() {
  const estNow = getCurrentDateInEST();
  
  // Create a date object for current EST time to get day of week
  // We'll use a temporary date to calculate day of week
  const tempDate = new Date(estNow.year, estNow.month, estNow.day);
  const dayOfWeek = tempDate.getDay(); // 0 = Sunday, 5 = Friday
  
  // Calculate days until Friday
  const daysUntilFriday = (5 - dayOfWeek + 7) % 7;
  
  // Calculate Friday date
  const fridayDate = new Date(estNow.year, estNow.month, estNow.day + daysUntilFriday);
  
  // Return UTC ISO string for Friday at 23:59:59.999 EST
  return convertESTToUTC(
    fridayDate.getFullYear(),
    fridayDate.getMonth(),
    fridayDate.getDate(),
    23,
    59,
    59,
    999
  );
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
  commentBody = "Automated recurring check-in ticket",
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
        status: "new",
        requester_id: requesterId,
        due_at: dueAt,
        comment: {
          body: commentBody,
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
 * Check if an error is retryable (network issues, rate limits, server errors)
 * @param {Error} error - Error object
 * @returns {boolean} True if error is retryable
 */
function isRetryableError(error) {
  // Network errors (no response)
  if (!error.response) {
    return true; // Network issues are retryable
  }

  const status = error.response.status;
  
  // Retryable HTTP status codes
  const retryableStatuses = [
    429, // Rate limit
    500, // Internal server error
    502, // Bad gateway
    503, // Service unavailable
    504, // Gateway timeout
    408, // Request timeout
  ];

  return retryableStatuses.includes(status);
}

/**
 * Create multiple tickets in batch with concurrency control and retry logic
 * 
 * @param {Array} ticketConfigs - Array of ticket configuration objects
 * @param {number} concurrency - Maximum concurrent ticket creations
 * @param {number} maxRetries - Maximum retry attempts for failed tickets (default: 1 additional retry)
 * @returns {Promise<Array>} Array of results (ticket objects or errors)
 */
export async function createTicketsBatch(ticketConfigs, concurrency = 5, maxRetries = 1) {
  const tasks = ticketConfigs.map((config) => async () => {
    let lastError = null;
    let attempts = 0;
    const maxAttempts = maxRetries + 1; // Initial attempt + retries

    while (attempts < maxAttempts) {
      try {
        const ticket = await createPrivateTaskTicket(config);
        if (attempts > 0) {
          logger.info(
            `✅ Successfully created ticket for ${config.clientName} (${config.clientAcId}) after ${attempts} retry(ies)`
          );
        }
        return { success: true, ticket, config, attempts: attempts + 1 };
      } catch (error) {
        lastError = error;
        attempts++;

        // Check if error is retryable
        if (!isRetryableError(error)) {
          // Non-retryable error (e.g., validation error, bad request)
          logger.error(
            `❌ Non-retryable error for ${config.clientName} (${config.clientAcId}): ${error.response?.status || error.message}`
          );
          break; // Don't retry non-retryable errors
        }

        // If we've exhausted retries, break
        if (attempts >= maxAttempts) {
          break;
        }

        // Wait before retry (exponential backoff)
        const waitTime = Math.min(1000 * Math.pow(2, attempts - 1), 10000); // Max 10 seconds
        logger.warn(
          `⚠️ Retrying ticket creation for ${config.clientName} (${config.clientAcId}) - attempt ${attempts + 1}/${maxAttempts} after ${waitTime}ms`
        );
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }

    // All retries exhausted or non-retryable error
    const errorMsg = lastError?.response?.data 
      ? JSON.stringify(lastError.response.data)
      : lastError?.message || "Unknown error";
    
    logger.error(
      `❌ Failed to create ticket for ${config.clientName} (${config.clientAcId}) after ${attempts} attempt(s): ${errorMsg}`
    );

    return {
      success: false,
      error: errorMsg,
      config,
      attempts,
      retryable: isRetryableError(lastError),
    };
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
