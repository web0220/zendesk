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

    const uniqueBatch = batch.filter((client) => {
      if (!client?.id) {
        logger.warn(`⚠️ Client user missing ID on page ${currentPage}`);
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
      logger.warn(`⚠️ Found ${duplicatesCount} duplicate client user(s) on page ${currentPage}`);
    }

    logger.debug(
      `📄 Clients page ${currentPage}: API returned ${originalCount}, after filtering: ${batch.length}, unique: ${uniqueBatch.length}`
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

    if (originalCount > batch.length) {
      const filteredThisPage = originalCount - batch.length;
      totalFilteredOut += filteredThisPage;
      logger.debug(
        `   ℹ️  ${filteredThisPage} client user(s) filtered out on page ${currentPage} (status filter)`
      );
    }

    allClients.push(...uniqueBatch);
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

    const uniqueBatch = batch.filter((caregiver) => {
      if (!caregiver?.id) {
        logger.warn(`⚠️ Caregiver user missing ID on page ${currentPage}`);
        return false;
      }
      if (seenIds.has(caregiver.id)) {
        logger.debug(
          `   ⚠️ Duplicate caregiver ID ${caregiver.id} detected on page ${currentPage}, skipping`
        );
        return false;
      }
      seenIds.add(caregiver.id);
      return true;
    });

    const duplicatesCount = batch.length - uniqueBatch.length;
    if (duplicatesCount > 0) {
      logger.warn(`⚠️ Found ${duplicatesCount} duplicate caregiver user(s) on page ${currentPage}`);
    }

    logger.debug(
      `📄 Caregivers page ${currentPage}: API returned ${originalCount}, after filtering: ${batch.length}, unique: ${uniqueBatch.length}`
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

    if (originalCount > batch.length) {
      const filteredThisPage = originalCount - batch.length;
      totalFilteredOut += filteredThisPage;
      logger.debug(
        `   ℹ️  ${filteredThisPage} caregiver user(s) filtered out on page ${currentPage} (status filter)`
      );
    }

    allCaregivers.push(...uniqueBatch);
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

