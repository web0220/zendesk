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
 * Returns { clients, originalCount } where originalCount is the API response size before filtering
 */
async function fetchClientPage({ page, count, status, includeDetails }) {
  const params = { page, count };
  if (status) params.status = status;

  const res = await alayaClient.get(`/patients/clients/`, { params });
  let clients = res.data?.items || res.data || [];
  const originalCount = clients.length; // Track original API response size

  // Client-side filter guard (if API ignores status)
  if (status && clients.length) {
    clients = clients.filter(
      (c) => (c.status || "").toLowerCase() === status.toLowerCase()
    );
  }

  if (includeDetails && clients.length) {
    logger.info(`📞 Fetching details for ${clients.length} clients...`);
    const details = await Promise.all(clients.map((c) => fetchClientDetail(c.id)));

    // Merge detail + derived fields into each client
    clients = clients.map((c, i) => {
      const d = details[i] || {};
      const demo = d.demographics || {};
      const groups = d.groups || c.groups || [];
      const tags = d.tags || c.tags || [];
      const contacts = d.contacts || c.contacts || [];

      return {
        ...d, // Start with full detail object (includes all nested structures like contacts, demographics)
        ...c, // Override with list data where it exists
        // Preserve full demographics object (needed by mapper for collectAllPhoneDetails)
        demographics: demo,
        // Preserve contacts array (needed by mapper for collectAllPhoneDetails)
        contacts,
        // Keep raw arrays too
        groups,
        tags,

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

        // derived fields for Zendesk user_fields
        market: extractMarket(groups),
        coordinator_pod: extractCoordinatorPod(groups),
        case_rating: demo.case_rating ?? null,
        sales_rep: extractSalesRep(tags),

        // convenience: normalized phone
        phone_normalized: firstDigitsPhone(
          demo.phone_main || c.phone_main || c.phone
        ),
      };
    });
  }

  return { clients, originalCount };
}

export async function fetchClients({
  page,
  count,
  status,
  includeDetails = true,
  fetchAll = true,
  maxPages,
} = {}) {
  try {
    const pageSize = count ?? 100;
    const shouldFetchAll = fetchAll ?? !page;

    if (!shouldFetchAll) {
      const result = await fetchClientPage({
        page: page ?? 1,
        count: pageSize,
        status,
        includeDetails,
      });
      return result.clients;
    }

    const allClients = [];
    const seenIds = new Set(); // Track IDs to prevent duplicates
    let currentPage = page ?? 1;
    let pagesFetched = 0;
    let consecutiveEmptyPages = 0; // Track empty pages to detect API issues
    const MAX_CONSECUTIVE_EMPTY = 3; // Stop after 3 consecutive empty pages
    let totalFilteredOut = 0; // Track how many items were filtered out

    while (true) {
      const result = await fetchClientPage({
        page: currentPage,
        count: pageSize,
        status,
        includeDetails,
      });

      const batch = result.clients;
      const originalCount = result.originalCount;

      // Deduplicate by ID (in case API returns duplicates)
      const uniqueBatch = batch.filter((client) => {
        if (!client?.id) {
          logger.warn(`⚠️ Client missing ID on page ${currentPage}`);
          return false;
        }
        if (seenIds.has(client.id)) {
          logger.debug(`   ⚠️ Duplicate client ID ${client.id} detected on page ${currentPage}, skipping`);
          return false;
        }
        seenIds.add(client.id);
        return true;
      });

      const duplicatesCount = batch.length - uniqueBatch.length;
      if (duplicatesCount > 0) {
        logger.warn(`⚠️ Found ${duplicatesCount} duplicate client(s) on page ${currentPage}`);
      }

      logger.debug(`📄 Clients page ${currentPage}: API returned ${originalCount}, after filtering: ${batch.length}, unique: ${uniqueBatch.length}`);

      // If API returned nothing, we're done
      if (originalCount === 0) {
        consecutiveEmptyPages++;
        if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY) {
          logger.info(`✅ Reached end of clients at page ${currentPage} (${consecutiveEmptyPages} consecutive empty pages)`);
          break;
        }
        logger.debug(`   Empty page ${currentPage}, continuing to check for more...`);
        currentPage += 1;
        continue;
      }

      // Reset empty page counter if we got results
      consecutiveEmptyPages = 0;

      // Track filtering differences
      if (originalCount > batch.length) {
        const filteredThisPage = originalCount - batch.length;
        totalFilteredOut += filteredThisPage;
        logger.debug(`   ℹ️  ${filteredThisPage} client(s) filtered out on page ${currentPage} (status filter)`);
      }

      // Add unique results only
      allClients.push(...uniqueBatch);
      pagesFetched += 1;

      // Use originalCount (API response size) for pagination decision, not filtered batch size
      // This ensures we continue fetching even if client-side filtering reduced the batch
      // When we get a partial page (< pageSize), we're done - don't check next page to avoid duplicates
      if (originalCount < pageSize) {
        logger.info(`✅ Reached end of clients at page ${currentPage} (API returned ${originalCount} < ${pageSize})`);
        break;
      }
      if (maxPages && pagesFetched >= maxPages) {
        logger.info(`✅ Reached max pages limit (${maxPages}) for clients`);
        break;
      }

      currentPage += 1;
    }

    if (totalFilteredOut > 0) {
      logger.info(`ℹ️  Total clients filtered out by status: ${totalFilteredOut}`);
    }

    logger.info(`📊 Total clients fetched: ${allClients.length} across ${pagesFetched} pages`);

    return allClients;
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
 * Returns { caregivers, originalCount } where originalCount is the API response size before filtering
 * @param {Object} options - Fetch options
 * @param {number} options.page - Page number (default: 1)
 * @param {number} options.count - Items per page (default: 10)
 * @param {string} options.status - Filter by status (e.g., "active")
 * @param {boolean} options.includeDetails - Fetch full details with demographics, groups, tags (default: true)
 */
async function fetchCaregiverPage({ page, count, status, includeDetails }) {
  const params = { page, count };
  if (status) {
    params.status = status;
  }

  const res = await alayaClient.get("/employees/employees/", { params });
  let caregivers = res.data?.items || res.data || [];
  const originalCount = caregivers.length; // Track original API response size

  if (status && caregivers.length > 0) {
    caregivers = caregivers.filter((caregiver) => {
      const caregiverStatus = caregiver.status?.toLowerCase();
      return caregiverStatus === status.toLowerCase();
    });
  }

  if (includeDetails && caregivers.length > 0) {
    logger.info(`📞 Fetching details for ${caregivers.length} caregivers...`);
    const details = await Promise.all(
      caregivers.map((cg) => fetchCaregiverDetail(cg.id))
    );

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

  return { caregivers, originalCount };
}

export async function fetchCaregivers({
  page,
  count,
  status,
  includeDetails = true,
  fetchAll = true,
  maxPages,
} = {}) {
  try {
    const pageSize = count ?? 100;
    const shouldFetchAll = fetchAll ?? !page;

    if (!shouldFetchAll) {
      const result = await fetchCaregiverPage({
        page: page ?? 1,
        count: pageSize,
        status,
        includeDetails,
      });
      return result.caregivers;
    }

    const allCaregivers = [];
    const seenIds = new Set(); // Track IDs to prevent duplicates
    let currentPage = page ?? 1;
    let pagesFetched = 0;
    let consecutiveEmptyPages = 0; // Track empty pages to detect API issues
    const MAX_CONSECUTIVE_EMPTY = 3; // Stop after 3 consecutive empty pages
    let totalFilteredOut = 0; // Track how many items were filtered out

    while (true) {
      const result = await fetchCaregiverPage({
        page: currentPage,
        count: pageSize,
        status,
        includeDetails,
      });

      const batch = result.caregivers;
      const originalCount = result.originalCount;

      // Deduplicate by ID (in case API returns duplicates)
      const uniqueBatch = batch.filter((caregiver) => {
        if (!caregiver?.id) {
          logger.warn(`⚠️ Caregiver missing ID on page ${currentPage}`);
          return false;
        }
        if (seenIds.has(caregiver.id)) {
          logger.debug(`   ⚠️ Duplicate caregiver ID ${caregiver.id} detected on page ${currentPage}, skipping`);
          return false;
        }
        seenIds.add(caregiver.id);
        return true;
      });

      const duplicatesCount = batch.length - uniqueBatch.length;
      if (duplicatesCount > 0) {
        logger.warn(`⚠️ Found ${duplicatesCount} duplicate caregiver(s) on page ${currentPage}`);
      }

      logger.debug(`📄 Caregivers page ${currentPage}: API returned ${originalCount}, after filtering: ${batch.length}, unique: ${uniqueBatch.length}`);

      // If API returned nothing, we're done
      if (originalCount === 0) {
        consecutiveEmptyPages++;
        if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY) {
          logger.info(`✅ Reached end of caregivers at page ${currentPage} (${consecutiveEmptyPages} consecutive empty pages)`);
          break;
        }
        logger.debug(`   Empty page ${currentPage}, continuing to check for more...`);
        currentPage += 1;
        continue;
      }

      // Reset empty page counter if we got results
      consecutiveEmptyPages = 0;

      // Track filtering differences
      if (originalCount > batch.length) {
        const filteredThisPage = originalCount - batch.length;
        totalFilteredOut += filteredThisPage;
        logger.debug(`   ℹ️  ${filteredThisPage} caregiver(s) filtered out on page ${currentPage} (status filter)`);
      }

      // Add unique results only
      allCaregivers.push(...uniqueBatch);
      pagesFetched += 1;

      // Use originalCount (API response size) for pagination decision, not filtered batch size
      // This ensures we continue fetching even if client-side filtering reduced the batch
      // When we get a partial page (< pageSize), we're done - don't check next page to avoid duplicates
      if (originalCount < pageSize) {
        logger.info(`✅ Reached end of caregivers at page ${currentPage} (API returned ${originalCount} < ${pageSize})`);
        break;
      }
      if (maxPages && pagesFetched >= maxPages) {
        logger.info(`✅ Reached max pages limit (${maxPages}) for caregivers`);
        break;
      }

      currentPage += 1;
    }

    if (totalFilteredOut > 0) {
      logger.info(`ℹ️  Total caregivers filtered out by status: ${totalFilteredOut}`);
    }

    logger.info(`📊 Total caregivers fetched: ${allCaregivers.length} across ${pagesFetched} pages`);

    return allCaregivers;
  } catch (err) {
    logger.error("Error fetching caregivers:", err.response?.data || err.message);
    throw err;
  }
}

logger.info("📡 AlayaCare external service (Basic Auth) initialized");
