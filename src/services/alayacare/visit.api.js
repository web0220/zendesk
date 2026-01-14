import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../../utils/retry.js";
import { getCurrentDateInEST } from "../../utils/date.js";

// The AlayaCare visit API endpoint doesn't accept standard HTTP headers (User-Agent, Accept, etc.)
// and returns 502 if they're present. We use native fetch which allows us to send only Authorization.
const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

/**
 * Convert EST date/time to API format string (YYYY-MM-DDTHH:mm:ss)
 * @param {number} year - Year in EST
 * @param {number} month - Month (0-indexed) in EST
 * @param {number} day - Day in EST
 * @param {number} hour - Hour (0-23) in EST
 * @param {number} minute - Minute in EST (default: 0)
 * @returns {string} Formatted date string for API
 */
function formatESTDateTimeForAPI(year, month, day, hour, minute = 0) {
  const monthStr = String(month + 1).padStart(2, "0");
  const dayStr = String(day).padStart(2, "0");
  const hourStr = String(hour).padStart(2, "0");
  const minuteStr = String(minute).padStart(2, "0");
  return `${year}-${monthStr}-${dayStr}T${hourStr}:${minuteStr}:00`;
}

/**
 * Fetch a single page of visits from AlayaCare scheduler API
 * @param {Object} params - Query parameters
 * @param {number} params.alayacare_employee_id - Caregiver's source_ac_id
 * @param {string} params.start_at - Start date in format: YYYY-MM-DDTHH:mm:ss
 * @param {string} params.end_at - End date in format: YYYY-MM-DDTHH:mm:ss
 * @param {boolean} [params.cancelled] - Filter by cancelled status (optional)
 * @param {string} [params.status] - Filter by status (optional, e.g., "scheduled")
 * @param {number} [params.page] - Page number (default: 1)
 * @returns {Promise<Object>} Response object with items, page, total_pages, etc.
 */
async function fetchVisitsPage({
  alayacare_employee_id,
  start_at,
  end_at,
  cancelled = null,
  status = null,
  page = 1,
}) {
  const params = {
    alayacare_employee_id,
    start_at,
    end_at,
    page,
  };

  if (cancelled !== null) {
    params.cancelled = cancelled;
  }

  if (status) {
    params.status = status;
  }

  // Build URL with query parameters
  const url = new URL("/ext/api/v2/scheduler/visit", config.alayacare.baseUrl);
  Object.keys(params).forEach((key) => {
    if (params[key] !== null && params[key] !== undefined) {
      url.searchParams.append(key, params[key]);
    }
  });

  // Use native fetch instead of axios to avoid automatic headers (User-Agent, Accept, etc.)
  // The AlayaCare visit API returns 502 if these headers are present
  const fetchWithRetry = async () => {
    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Basic ${basicAuth}`,
        // Explicitly do NOT set any other headers (no Accept, Content-Type, User-Agent, etc.)
      },
    });

    if (!response.ok) {
      const error = new Error(`Request failed with status code ${response.status}`);
      error.status = response.status;
      error.response = {
        status: response.status,
        statusText: response.statusText,
      };
      throw error;
    }

    const data = await response.json();
    return { data };
  };

  const res = await withRetry(fetchWithRetry, 3, 1000);
  return res.data;
}

/**
 * Fetch visits from AlayaCare scheduler API (handles pagination automatically)
 * @param {Object} params - Query parameters
 * @param {number} params.alayacare_employee_id - Caregiver's source_ac_id
 * @param {string} params.start_at - Start date in format: YYYY-MM-DDTHH:mm:ss
 * @param {string} params.end_at - End date in format: YYYY-MM-DDTHH:mm:ss
 * @param {boolean} [params.cancelled] - Filter by cancelled status (optional)
 * @param {string} [params.status] - Filter by status (optional, e.g., "scheduled")
 * @returns {Promise<Array>} Array of visit objects (all pages combined)
 */
export async function fetchVisits({
  alayacare_employee_id,
  start_at,
  end_at,
  cancelled = null,
  status = null,
}) {
  try {
    // Fetch first page to get pagination info
    const firstPage = await fetchVisitsPage({
      alayacare_employee_id,
      start_at,
      end_at,
      cancelled,
      status,
      page: 1,
    });

    const allItems = [...(firstPage.items || [])];
    const totalPages = firstPage.total_pages || 1;
    const itemsPerPage = firstPage.items_per_page || 100;
    const totalCount = firstPage.count || 0;

    logger.debug(
      `📅 Fetched page 1/${totalPages}: ${firstPage.items?.length || 0} visits (total: ${totalCount}) for caregiver ${alayacare_employee_id}`
    );

    // Fetch remaining pages if any
    if (totalPages > 1) {
      for (let page = 2; page <= totalPages; page++) {
        const pageData = await fetchVisitsPage({
          alayacare_employee_id,
          start_at,
          end_at,
          cancelled,
          status,
          page,
        });

        const pageItems = pageData.items || [];
        allItems.push(...pageItems);

        logger.debug(
          `📅 Fetched page ${page}/${totalPages}: ${pageItems.length} visits for caregiver ${alayacare_employee_id}`
        );
      }
    }

    logger.debug(
      `📅 Fetched ${allItems.length} total visits for caregiver ${alayacare_employee_id} (${start_at} to ${end_at})`
    );

    return allItems;
  } catch (error) {
    logger.error(
      `❌ Failed to fetch visits for caregiver ${alayacare_employee_id}: ${error.message}`
    );
    if (error.status === 404 || error.response?.status === 404) {
      return [];
    }
    throw error;
  }
}

/**
 * Fetch scheduled visits for a caregiver (current day 5 AM EST to current day + 6 days at 5 AM EST)
 * @param {number} alayacare_employee_id - Caregiver's source_ac_id
 * @param {Date} currentDay - Current date (used to get EST day, time is ignored)
 * @returns {Promise<Array>} Array of scheduled visit objects
 */
export async function fetchScheduledVisits(alayacare_employee_id, currentDay = new Date()) {
  // Get current day in EST
  const estDate = getCurrentDateInEST();
  
  // start_at: current day's 5 AM EST
  const startAt = formatESTDateTimeForAPI(estDate.year, estDate.month, estDate.day, 5, 0);
  
  // end_at: current day + 6 days at 5 AM EST
  const endDate = new Date(estDate.year, estDate.month, estDate.day);
  endDate.setDate(endDate.getDate() + 7);
  const endAt = formatESTDateTimeForAPI(
    endDate.getFullYear(),
    endDate.getMonth(),
    endDate.getDate(),
    5,
    0
  );

  logger.debug(
    `📅 Fetching scheduled visits: ${startAt} to ${endAt} (EST) for caregiver ${alayacare_employee_id}`
  );

  return fetchVisits({
    alayacare_employee_id,
    start_at: startAt,
    end_at: endAt,
    status: "scheduled",
    cancelled: false,
  });
}

/**
 * Fetch past visits for a caregiver (from 2022-01-01 to current day's 5 AM EST)
 * Note: Returns all visits including cancelled ones - filtering should be done in the orchestrator
 * @param {number} alayacare_employee_id - Caregiver's source_ac_id
 * @param {Date} currentDay - Current date (used to get EST day, time is ignored)
 * @returns {Promise<Array>} Array of past visit objects (includes cancelled visits)
 */
export async function fetchPastVisits(alayacare_employee_id, currentDay = new Date()) {
  const startAt = "2022-01-01T00:00:00";
  
  // Get current day in EST
  const estDate = getCurrentDateInEST();
  
  // end_at: current day's 5 AM EST
  const endAt = formatESTDateTimeForAPI(estDate.year, estDate.month, estDate.day, 5, 0);

  logger.debug(
    `📅 Fetching past visits: ${startAt} to ${endAt} (EST) for caregiver ${alayacare_employee_id}`
  );

  // Don't filter by cancelled here - return all visits so orchestrator can filter
  const allVisits = await fetchVisits({
    alayacare_employee_id,
    start_at: startAt,
    end_at: endAt,
    cancelled: null, // Don't filter by cancelled - return all visits
  });

  // Return all visits (including cancelled) - filtering will be done in orchestrator
  return allVisits;
}

