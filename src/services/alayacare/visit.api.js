import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../../utils/retry.js";

// The AlayaCare visit API endpoint doesn't accept standard HTTP headers (User-Agent, Accept, etc.)
// and returns 502 if they're present. We use native fetch which allows us to send only Authorization.
const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

/**
 * Format date to ISO string in format: YYYY-MM-DDTHH:mm:ss
 * @param {Date} date - Date object
 * @returns {string} Formatted date string
 */
function formatDateForAPI(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}T00:00:00`;
}

/**
 * Fetch visits from AlayaCare scheduler API
 * @param {Object} params - Query parameters
 * @param {number} params.alayacare_employee_id - Caregiver's source_ac_id
 * @param {string} params.start_at - Start date in format: YYYY-MM-DDTHH:mm:ss
 * @param {string} params.end_at - End date in format: YYYY-MM-DDTHH:mm:ss
 * @param {boolean} [params.cancelled] - Filter by cancelled status (optional)
 * @param {string} [params.status] - Filter by status (optional, e.g., "scheduled")
 * @returns {Promise<Array>} Array of visit objects
 */
export async function fetchVisits({
  alayacare_employee_id,
  start_at,
  end_at,
  cancelled = null,
  status = null,
}) {
  try {
    const params = {
      alayacare_employee_id,
      start_at,
      end_at,
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

    const items = res.data?.items || [];
    logger.debug(
      `📅 Fetched ${items.length} visits for caregiver ${alayacare_employee_id} (${start_at} to ${end_at})`
    );

    return items;
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
 * Fetch scheduled visits for a caregiver (next 5 days)
 * @param {number} alayacare_employee_id - Caregiver's source_ac_id
 * @param {Date} currentTime - Current time (defaults to now)
 * @returns {Promise<Array>} Array of scheduled visit objects
 */
export async function fetchScheduledVisits(alayacare_employee_id, currentTime = new Date()) {
  const startAt = formatDateForAPI(currentTime);
  
  // Add 5 days to current time
  const endDate = new Date(currentTime);
  endDate.setDate(endDate.getDate() + 5);
  const endAt = formatDateForAPI(endDate);

  return fetchVisits({
    alayacare_employee_id,
    start_at: startAt,
    end_at: endAt,
    status: "scheduled",
    cancelled: false,
  });
}

/**
 * Fetch past visits for a caregiver (from 2022-01-10 to current time)
 * @param {number} alayacare_employee_id - Caregiver's source_ac_id
 * @param {Date} currentTime - Current time (defaults to now)
 * @returns {Promise<Array>} Array of past visit objects
 */
export async function fetchPastVisits(alayacare_employee_id, currentTime = new Date()) {
  const startAt = "2022-01-10T00:00:00";
  const endAt = formatDateForAPI(currentTime);

  return fetchVisits({
    alayacare_employee_id,
    start_at: startAt,
    end_at: endAt,
    cancelled: false,
  });
}

