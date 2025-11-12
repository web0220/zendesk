import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";

// build "Basic base64(public:private)" auth
const basicAuth = Buffer.from(
  `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
).toString("base64");

export const alayaClient = axios.create({
  baseURL: config.alayacare.baseUrl, // https://alvitacare.alayacare.com/ext/api/v2
  headers: {
    Authorization: `Basic ${basicAuth}`,
  },
});

/**
 * Fetch full client detail (includes demographics, groups, tags)
 */
export async function fetchClientDetail(id) {
  const { data } = await alayaClient.get(`/patients/clients/${id}`);
  return data;
}

/**
 * Extractors for derived fields from detail payload
 */
function extractMarket(groups = []) {
  // Example: "LOC - NYC (L001)" → "NYC"
  const loc = groups.find(g => typeof g.name === "string" && g.name.startsWith("LOC"));
  if (!loc) return null;
  const match = loc.name.match(/^LOC\s*-\s*([^(]+)/i);
  return match ? match[1].trim() : loc.name;
}

function extractCoordinatorPod(groups = []) {
  // Example: "CSC - Katie" → "Katie"
  const pod = groups.find(g => typeof g.name === "string" && g.name.startsWith("CSC"));
  if (!pod) return null;
  return pod.name.replace(/^CSC\s*-\s*/i, "").trim();
}

function extractSalesRep(tags = []) {
  // Example tag: "BD Michelle Wells" → "Michelle Wells"
  const tag = tags.find(t => typeof t === "string" && t.trim().toUpperCase().startsWith("BD "));
  if (!tag) return null;
  return tag.replace(/^BD\s*/i, "").trim();
}

function firstDigitsPhone(p) {
  if (!p) return null;
  // Some phone_main values contain text like "Name (646) 752-1576 - Son"
  const digits = String(p).replace(/\D/g, "");
  if (digits.length < 10) return null;
  return digits.startsWith("1") ? `+${digits}` : `+1${digits}`;
}

/**
 * Fetch clients (list) and optionally enrich each one with detail (demographics, groups, tags)
 */
export async function fetchClients({ page = 1, count = 10, status, includeDetails = true } = {}) {
  try {
    const params = { page, count };
    if (status) params.status = status;

    const res = await alayaClient.get(`/patients/clients/`, { params });
    let clients = res.data?.items || res.data || [];

    // Client-side filter guard (if API ignores status)
    if (status && clients.length) {
      clients = clients.filter(c => (c.status || "").toLowerCase() === status.toLowerCase());
    }

    if (includeDetails && clients.length) {
      logger.info(`📞 Fetching details for ${clients.length} clients...`);
      const details = await Promise.all(clients.map(c => fetchClientDetail(c.id)));

      // Merge detail + derived fields into each client
      clients = clients.map((c, i) => {
        const d = details[i] || {};
        const demo = d.demographics || {};
        const groups = d.groups || c.groups || [];
        const tags = d.tags || c.tags || [];

        return {
          ...c,
          // flatten commonly-used fields for mapper convenience
          first_name: demo.first_name ?? c.first_name ?? null,
          last_name: demo.last_name ?? c.last_name ?? null,
          email: demo.email ?? c.email ?? null,
          phone_main: demo.phone_main ?? c.phone_main ?? null,
          phone: demo.phone_main ?? c.phone ?? null,
          address: demo.address ?? c.address ?? null,
          city: demo.city ?? c.city ?? null,
          state: demo.state ?? c.state ?? null,
          zip: demo.zip ?? c.zip ?? null,

          // keep raw arrays too
          groups,
          tags,

          // derived fields for Zendesk user_fields
          market: extractMarket(groups),
          coordinator_pod: extractCoordinatorPod(groups),
          case_rating: demo.case_rating ?? null,
          sales_rep: extractSalesRep(tags),

          // convenience: normalized phone
          phone_normalized: firstDigitsPhone(demo.phone_main || c.phone_main || c.phone),
        };
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
export async function fetchCaregiverDetail(caregiverId) {
  try {
    const res = await alayaClient.get(`/employees/employees/${caregiverId}`);
    return res.data;
  } catch (err) {
    logger.warn(`Failed to fetch detail for caregiver ${caregiverId}:`, err.message);
    return null;
  }
}

/**
 * Fetch employees (caregivers) and optionally enrich each one with detail (demographics, groups, tags)
 * Endpoint:
 *   https://<tenant>.alayacare.com/ext/api/v2/employees/employees/
 * @param {Object} options - Fetch options
 * @param {number} options.page - Page number (default: 1)
 * @param {number} options.count - Items per page (default: 10)
 * @param {string} options.status - Filter by status (e.g., "active")
 * @param {boolean} options.includeDetails - Fetch full details with demographics, groups, tags (default: true)
 */
export async function fetchCaregivers({ page = 1, count = 10, status, includeDetails = true } = {}) {
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

    // Fetch full details for all caregivers if requested
    if (includeDetails && caregivers.length > 0) {
      logger.info(`📞 Fetching details for ${caregivers.length} caregivers...`);
      const details = await Promise.all(caregivers.map((cg) => fetchCaregiverDetail(cg.id)));

      // Merge detail + derived fields into each caregiver
      caregivers = caregivers.map((c, i) => {
        const d = details[i] || {};
        const demo = d.demographics || {};
        const groups = d.groups || c.groups || [];
        const tags = d.tags || c.tags || [];
        const departments = d.departments || c.departments || [];

        return {
          ...d, // Start with full detail object (includes all nested structures)
          ...c, // Override with list data where it exists
          // Preserve full demographics object
          demographics: demo,
          // Keep raw arrays
          groups,
          tags,
          departments,
          // Flatten commonly-used fields for mapper convenience
          first_name: demo.first_name ?? c.first_name ?? null,
          last_name: demo.last_name ?? c.last_name ?? null,
          email: demo.email ?? c.email ?? null,
          phone_main: demo.phone_main ?? c.phone_main ?? c.phone ?? null,
          phone: demo.phone_main ?? c.phone_main ?? c.phone ?? null,
          phone_other: demo.phone_other ?? c.phone_other ?? null,
          address: demo.address ?? c.address ?? null,
          city: demo.city ?? c.city ?? null,
          state: demo.state ?? c.state ?? null,
          zip: demo.zip ?? c.zip ?? null,
          // Convenience: normalized phone
          phone_normalized: firstDigitsPhone(demo.phone_main || c.phone_main || c.phone),
        };
      });
    }

    return caregivers;
  } catch (err) {
    logger.error("Error fetching caregivers:", err.response?.data || err.message);
    throw err;
  }
}

logger.info("📡 AlayaCare external service (Basic Auth) initialized");
