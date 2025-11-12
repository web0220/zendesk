import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";

// build "Basic base64(public:private)" auth
const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

const alayaClient = axios.create({
  baseURL: config.alayacare.baseUrl, // https://alvitacare.alayacare.com/ext/api/v2
  headers: {
    Authorization: `Basic ${basicAuth}`,
  },
});

/**
 * Fetch individual client detail (includes email and phone)
 * @param {number} clientId - Client ID
 * @returns {Promise<Object>} Client detail with demographics
 */
async function fetchClientDetail(clientId) {
  try {
    const res = await alayaClient.get(`/patients/clients/${clientId}`);
    return res.data;
  } catch (err) {
    logger.warn(`Failed to fetch detail for client ${clientId}:`, err.message);
    return null;
  }
}

/**
 * Fetch clients (patients)
 * AlayaCare docs use page & count, not limit
 * Endpoint (from docs):
 *   https://<tenant>.alayacare.com/ext/api/v2/patients/clients/
 * @param {Object} options - Fetch options
 * @param {number} options.page - Page number (default: 1)
 * @param {number} options.count - Items per page (default: 10)
 * @param {string} options.status - Filter by status (e.g., "active")
 * @param {boolean} options.includeDetails - Fetch full details with email/phone (default: true)
 */
export async function fetchClients({ page = 1, count = 10, status, includeDetails = true } = {}) {
  try {
    const params = { page, count };
    // Try API-level filtering if status is provided
    if (status) {
      params.status = status;
    }

    const res = await alayaClient.get("/patients/clients/", { params });
    let clients = res.data?.items || res.data || [];

    // Client-side filtering if API doesn't support status param or returns all statuses
    if (status && clients.length > 0) {
      clients = clients.filter((client) => {
        const clientStatus = client.status?.toLowerCase();
        return clientStatus === status.toLowerCase();
      });
    }

    // Fetch details (email, phone) for each client if requested
    if (includeDetails && clients.length > 0) {
      logger.info(`📞 Fetching details for ${clients.length} clients...`);
      const detailPromises = clients.map((client) => fetchClientDetail(client.id));
      const details = await Promise.all(detailPromises);

      // Merge demographics (email, phone_main) into client objects
      clients = clients.map((client, index) => {
        const detail = details[index];
        if (detail?.demographics) {
          return {
            ...client,
            email: detail.demographics.email || null,
            phone_main: detail.demographics.phone_main || null,
            phone: detail.demographics.phone_main || null, // Alias for compatibility
            // Include other useful demographics fields
            address: detail.demographics.address || null,
            city: detail.demographics.city || null,
            state: detail.demographics.state || null,
            zip: detail.demographics.zip || null,
          };
        }
        return client;
      });
    }

    return clients;
  } catch (err) {
    logger.error("Error fetching clients:", err.response?.data || err.message);
    throw err;
  }
}

/**
 * Fetch individual caregiver detail (includes email and phone if missing)
 * @param {number} caregiverId - Caregiver ID
 * @returns {Promise<Object>} Caregiver detail with demographics
 */
async function fetchCaregiverDetail(caregiverId) {
  try {
    const res = await alayaClient.get(`/employees/employees/${caregiverId}`);
    return res.data;
  } catch (err) {
    logger.warn(`Failed to fetch detail for caregiver ${caregiverId}:`, err.message);
    return null;
  }
}

/**
 * Fetch employees (caregivers)
 * Endpoint:
 *   https://<tenant>.alayacare.com/ext/api/v2/employees/employees/
 * @param {Object} options - Fetch options
 * @param {number} options.page - Page number (default: 1)
 * @param {number} options.count - Items per page (default: 10)
 * @param {string} options.status - Filter by status (e.g., "active")
 * @param {boolean} options.includeDetails - Fetch full details with email/phone if missing (default: false, as list usually has them)
 */
export async function fetchCaregivers({ page = 1, count = 10, status, includeDetails = false } = {}) {
  try {
    const params = { page, count };
    // Try API-level filtering if status is provided
    if (status) {
      params.status = status;
    }

    const res = await alayaClient.get("/employees/employees/", { params });
    let caregivers = res.data?.items || res.data || [];

    // Client-side filtering if API doesn't support status param or returns all statuses
    if (status && caregivers.length > 0) {
      caregivers = caregivers.filter((caregiver) => {
        const caregiverStatus = caregiver.status?.toLowerCase();
        return caregiverStatus === status.toLowerCase();
      });
    }

    // Fetch details for caregivers missing email/phone if requested
    if (includeDetails && caregivers.length > 0) {
      const needsDetail = caregivers.filter((cg) => !cg.email || !cg.phone);
      if (needsDetail.length > 0) {
        logger.info(`📞 Fetching details for ${needsDetail.length} caregivers missing contact info...`);
        const detailPromises = needsDetail.map((cg) => fetchCaregiverDetail(cg.id));
        const details = await Promise.all(detailPromises);

        // Merge demographics into caregiver objects
        const detailMap = new Map();
        needsDetail.forEach((cg, idx) => {
          if (details[idx]) {
            detailMap.set(cg.id, details[idx]);
          }
        });

        caregivers = caregivers.map((caregiver) => {
          const detail = detailMap.get(caregiver.id);
          if (detail?.demographics) {
            return {
              ...caregiver,
              email: caregiver.email || detail.demographics.email || null,
              phone_main: caregiver.phone_main || caregiver.phone || detail.demographics.phone_main || null,
              phone: caregiver.phone || caregiver.phone_main || detail.demographics.phone_main || null,
            };
          }
          return caregiver;
        });
      }
    }

    return caregivers;
  } catch (err) {
    logger.error("Error fetching caregivers:", err.response?.data || err.message);
    throw err;
  }
}

logger.info("📡 AlayaCare external service (Basic Auth) initialized");
