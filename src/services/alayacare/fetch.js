import { logger } from "../../config/logger.js";
import { runWithLimit } from "../../utils/rateLimiter.js";
import {
  alayaClient,
  DETAIL_CONCURRENCY,
  fetchCaregiverDetail,
  fetchClientDetail,
  requestWithRetry,
} from "./alayacare.api.js";

async function fetchClientPage({ page, count, status, includeDetails }) {
  const params = { page, count };
  if (status) params.status = status;

  const res = await requestWithRetry(() => alayaClient.get("/patients/clients/", { params }));
  let clients = res.data?.items || res.data || [];
  const originalCount = clients.length;

  if (status && clients.length) {
    clients = clients.filter(
      (client) => (client.status || "").toLowerCase() === status.toLowerCase()
    );
  }

  if (includeDetails && clients.length) {
    const tasks = clients.map((client) => async () => fetchClientDetail(client.id));
    logger.info(
      `📞 Fetching details for ${clients.length} client users with concurrency ${DETAIL_CONCURRENCY}`
    );
    const details = await runWithLimit(tasks, DETAIL_CONCURRENCY);

    clients = clients.map((client, index) => {
      const detail = details[index] || {};
      const demographics = detail.demographics || {};
      const groups = detail.groups || client.groups || [];
      const tags = detail.tags || client.tags || [];
      const contacts = detail.contacts || client.contacts || [];

      return {
        ...detail,
        ...client,
        demographics,
        contacts,
        groups,
        tags,
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
  const seenIds = new Set();
  let currentPage = page ?? 1;
  let pagesFetched = 0;
  let consecutiveEmptyPages = 0;
  const MAX_CONSECUTIVE_EMPTY = 3;
  let consecutiveNoUniquePages = 0;
  const MAX_CONSECUTIVE_NO_UNIQUE = 3;
  let totalFilteredOut = 0;

  while (true) {
    const result = await fetchClientPage({
      page: currentPage,
      count: pageSize,
      status,
      includeDetails,
    });

    const batch = result.clients;
    const originalCount = result.originalCount;

    // Filter out clients without IDs, but DON'T filter duplicates
    // We need to fetch all users (including duplicates) to get updates
    // The database UPSERT will handle deduplication and updates
    const validBatch = batch.filter((client) => {
      if (!client?.id) {
        logger.warn(`⚠️ Client user missing ID on page ${currentPage}`);
        return false;
      }
      return true;
    });

    // Track seen IDs for loop detection (but still fetch duplicates to allow updates)
    let newUniqueIds = 0;
    for (const client of validBatch) {
      if (!seenIds.has(client.id)) {
        seenIds.add(client.id);
        newUniqueIds++;
      } else {
        logger.debug(`   ℹ️  Duplicate client ID ${client.id} detected on page ${currentPage} (will still fetch to check for updates)`);
      }
    }

    const duplicatesCount = validBatch.length - newUniqueIds;
    if (duplicatesCount > 0) {
      logger.debug(`ℹ️  Found ${duplicatesCount} duplicate client user(s) on page ${currentPage} (fetching anyway to check for updates)`);
    }

    logger.debug(
      `📄 Clients page ${currentPage}: API returned ${originalCount}, after filtering: ${validBatch.length}, new unique IDs: ${newUniqueIds}`
    );

    if (originalCount === 0) {
      consecutiveEmptyPages++;
      if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY) {
        logger.info(
          `✅ Reached end of clients at page ${currentPage} (${consecutiveEmptyPages} consecutive empty pages)`
        );
        break;
      }
      logger.debug(`   Empty page ${currentPage}, continuing to check for more...`);
      currentPage += 1;
      continue;
    }

    consecutiveEmptyPages = 0;

    // Check if all items on this page are duplicates (0 new unique IDs but API returned items)
    // This happens when the API starts returning only duplicates (infinite loop detection)
    // We still fetch them to allow updates, but stop pagination if it's clearly a loop
    if (newUniqueIds === 0 && originalCount > 0) {
      consecutiveNoUniquePages++;
      if (consecutiveNoUniquePages >= MAX_CONSECUTIVE_NO_UNIQUE) {
        logger.info(
          `✅ Reached end of clients at page ${currentPage} (${consecutiveNoUniquePages} consecutive pages with 0 new unique IDs - infinite loop detected, stopping pagination)`
        );
        break;
      }
      logger.debug(
        `   All items on page ${currentPage} are duplicates (0 new unique IDs), fetching anyway for updates... (${consecutiveNoUniquePages}/${MAX_CONSECUTIVE_NO_UNIQUE})`
      );
      // Continue to next page even if all duplicates (to allow updates)
      // But we'll break if this happens too many times in a row
    } else {
      consecutiveNoUniquePages = 0;
    }

    if (originalCount > validBatch.length) {
      const filteredThisPage = originalCount - validBatch.length;
      totalFilteredOut += filteredThisPage;
      logger.debug(
        `   ℹ️  ${filteredThisPage} client user(s) filtered out on page ${currentPage} (status filter)`
      );
    }

    // Fetch ALL valid clients (including duplicates) - database UPSERT will handle updates
    allClients.push(...validBatch);
    pagesFetched += 1;

    if (originalCount < pageSize) {
      logger.info(
        `✅ Reached end of clients at page ${currentPage} (API returned ${originalCount} < ${pageSize})`
      );
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
}

async function fetchCaregiverPage({ page, count, status, includeDetails }) {
  const params = { page, count };
  if (status) params.status = status;

  const res = await requestWithRetry(() => alayaClient.get("/employees/employees/", { params }));
  let caregivers = res.data?.items || res.data || [];
  const originalCount = caregivers.length;

  if (status && caregivers.length > 0) {
    caregivers = caregivers.filter((caregiver) => {
      const caregiverStatus = caregiver.status?.toLowerCase();
      return caregiverStatus === status.toLowerCase();
    });
  }

  if (includeDetails && caregivers.length > 0) {
    const tasks = caregivers.map((caregiver) => async () => fetchCaregiverDetail(caregiver.id));
    logger.info(
      `📞 Fetching details for ${caregivers.length} caregiver users with concurrency ${DETAIL_CONCURRENCY}`
    );
    const details = await runWithLimit(tasks, DETAIL_CONCURRENCY);

    caregivers = caregivers.map((caregiver, index) => {
      const detail = details[index] || {};
      const demographics = detail.demographics || {};
      const groups = detail.groups || caregiver.groups || [];
      const tags = detail.tags || caregiver.tags || [];
      const departments = detail.departments || caregiver.departments || [];

      return {
        ...detail,
        ...caregiver,
        demographics,
        groups,
        tags,
        departments,
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
  const seenIds = new Set();
  let currentPage = page ?? 1;
  let pagesFetched = 0;
  let consecutiveEmptyPages = 0;
  const MAX_CONSECUTIVE_EMPTY = 3;
  let consecutiveNoUniquePages = 0;
  const MAX_CONSECUTIVE_NO_UNIQUE = 3;
  let totalFilteredOut = 0;

  while (true) {
    const result = await fetchCaregiverPage({
      page: currentPage,
      count: pageSize,
      status,
      includeDetails,
    });

    const batch = result.caregivers;
    const originalCount = result.originalCount;

    // Filter out caregivers without IDs, but DON'T filter duplicates
    // We need to fetch all users (including duplicates) to get updates
    // The database UPSERT will handle deduplication and updates
    const validBatch = batch.filter((caregiver) => {
      if (!caregiver?.id) {
        logger.warn(`⚠️ Caregiver user missing ID on page ${currentPage}`);
        return false;
      }
      return true;
    });

    // Track seen IDs for loop detection (but still fetch duplicates to allow updates)
    let newUniqueIds = 0;
    for (const caregiver of validBatch) {
      if (!seenIds.has(caregiver.id)) {
        seenIds.add(caregiver.id);
        newUniqueIds++;
      } else {
        logger.debug(`   ℹ️  Duplicate caregiver ID ${caregiver.id} detected on page ${currentPage} (will still fetch to check for updates)`);
      }
    }

    const duplicatesCount = validBatch.length - newUniqueIds;
    if (duplicatesCount > 0) {
      logger.debug(`ℹ️  Found ${duplicatesCount} duplicate caregiver user(s) on page ${currentPage} (fetching anyway to check for updates)`);
    }

    logger.debug(
      `📄 Caregivers page ${currentPage}: API returned ${originalCount}, after filtering: ${validBatch.length}, new unique IDs: ${newUniqueIds}`
    );

    if (originalCount === 0) {
      consecutiveEmptyPages++;
      if (consecutiveEmptyPages >= MAX_CONSECUTIVE_EMPTY) {
        logger.info(
          `✅ Reached end of caregivers at page ${currentPage} (${consecutiveEmptyPages} consecutive empty pages)`
        );
        break;
      }
      logger.debug(`   Empty page ${currentPage}, continuing to check for more...`);
      currentPage += 1;
      continue;
    }

    consecutiveEmptyPages = 0;

    // Check if all items on this page are duplicates (0 new unique IDs but API returned items)
    // This happens when the API starts returning only duplicates (infinite loop detection)
    // We still fetch them to allow updates, but stop pagination if it's clearly a loop
    if (newUniqueIds === 0 && originalCount > 0) {
      consecutiveNoUniquePages++;
      if (consecutiveNoUniquePages >= MAX_CONSECUTIVE_NO_UNIQUE) {
        logger.info(
          `✅ Reached end of caregivers at page ${currentPage} (${consecutiveNoUniquePages} consecutive pages with 0 new unique IDs - infinite loop detected, stopping pagination)`
        );
        break;
      }
      logger.debug(
        `   All items on page ${currentPage} are duplicates (0 new unique IDs), fetching anyway for updates... (${consecutiveNoUniquePages}/${MAX_CONSECUTIVE_NO_UNIQUE})`
      );
      // Continue to next page even if all duplicates (to allow updates)
      // But we'll break if this happens too many times in a row
    } else {
      consecutiveNoUniquePages = 0;
    }

    if (originalCount > validBatch.length) {
      const filteredThisPage = originalCount - validBatch.length;
      totalFilteredOut += filteredThisPage;
      logger.debug(
        `   ℹ️  ${filteredThisPage} caregiver user(s) filtered out on page ${currentPage} (status filter)`
      );
    }

    // Fetch ALL valid caregivers (including duplicates) - database UPSERT will handle updates
    allCaregivers.push(...validBatch);
    pagesFetched += 1;

    if (originalCount < pageSize) {
      logger.info(
        `✅ Reached end of caregivers at page ${currentPage} (API returned ${originalCount} < ${pageSize})`
      );
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
}

