import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../common/retry.js";
import { pollJobStatus } from "./jobPoller.js";

// ======================================================
// 🔧 Initialize Zendesk API Client
// ======================================================
const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

function buildZendeskUserObject(user = {}) {
  return {
    external_id: user.external_id ?? null,
    name: user.name ?? null,
    email: user.email ?? null,
    phone: user.phone ?? null,
    organization_id: user.organization_id || null,
    user_fields: user.user_fields || {},
  };
}

function normalizeIdentities(identities = []) {
  return identities.map(identity => {
    let identityType;
    if (identity.type === "phone") {
      identityType = "phone_number";
    } else if (identity.type === "email") {
      identityType = "email";
    } else {
      identityType = identity.type;
    }

    return {
      type: identityType,
      value: identity.value,
    };
  });
}

async function addIdentities(userId, identities = []) {
  const formatted = normalizeIdentities(identities);
  if (!userId || formatted.length === 0) return;

  logger.info(`📞 Adding ${formatted.length} identity/identities for user ${userId}...`);

  for (const identity of formatted) {
    try {
      await zendeskClient.post(`/users/${userId}/identities.json`, { identity });
      logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;
      logger.warn(`   ⚠️ Failed to add identity ${identity.value}: ${JSON.stringify(msg)}`);
    }
  }
}

// ======================================================
// ➕ Upsert single user (create or update one)
// ======================================================
export async function upsertSingleUser(user) {
  return withRetry(async () => {
    const { identities, ...userWithoutIdentities } = user || {};

    const payload = {
      user: buildZendeskUserObject(userWithoutIdentities),
    };

    const res = await zendeskClient.post("/users/create_or_update.json", payload);
    const userId = res.data?.user?.id;

    if (!userId) {
      logger.warn(`⚠️ No user ID returned for ${user?.name || user?.email}`);
      return null;
    }

    logger.info(`✅ User ${userId} ${res.data?.user?.created_at ? "created" : "updated"}: ${user?.name || user?.email}`);

    await addIdentities(userId, identities);

    return { userId, user: res.data?.user };
  });
}

// ======================================================
// 🚀 Bulk upsert users (create or update many)
// ======================================================
export async function bulkUpsertUsers(users = []) {
  return withRetry(async () => {
    // Send everything except identities and ac_id (internal field)
    const payload = {
      users: users.map(u => {
        const { identities, ac_id, ...userWithoutIdentities } = u || {};
        return buildZendeskUserObject(userWithoutIdentities);
      }),
    };

    const res = await zendeskClient.post("/users/create_or_update_many.json", payload);
    logger.info(`🧩 Upsert request accepted: ${users.length} users`);
    const jobId = res.data?.job_status?.id;

    if (!jobId) {
      logger.warn("⚠️ No job ID returned from Zendesk");
      return res.data;
    }

    // Wait for job completion
    const job = await pollJobStatus(jobId);
    logger.info(`✅ Job ${jobId} finished with status: ${job.status}`);

    // Return job status with results
    return {
      job_status: job,
      original_users: users, // Include original user data for mapping
    };
  });
}

// ======================================================
// 📊 Get job status
// ======================================================
export async function getJobStatus(jobId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/job_statuses/${jobId}.json`);
    return res.data;
  });
}

// ======================================================
// 📋 Get existing user identities
// ======================================================
export async function getUserIdentities(userId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/users/${userId}/identities.json`);
    return res.data?.identities || [];
  });
}

// ======================================================
// ➕ Sync user identities (secondary emails & phones)
// ======================================================
export async function syncUserIdentities(userId, userData) {
  if (!userId) return;

  // Get existing identities from Zendesk
  const existingIdentities = await getUserIdentities(userId);
  const existingValues = new Set(
    existingIdentities.map(id => id.value?.toLowerCase().trim())
  );

  logger.info(`📋 User ${userId} has ${existingIdentities.length} existing identities`);

  // Collect identities from userData
  const identitiesToAdd = [];
  
  if (Array.isArray(userData.identities)) {
    for (const identity of userData.identities) {
      const normalizedValue = identity.value?.toLowerCase().trim();
      
      // Skip if already exists
      if (existingValues.has(normalizedValue)) {
        logger.debug(`   ⏭️  Skipping duplicate ${identity.type}: ${identity.value}`);
        continue;
      }

      // Normalize type for Zendesk API
      const type = identity.type === "phone" ? "phone_number" : identity.type;
      identitiesToAdd.push({ type, value: identity.value });
    }
  }

  if (identitiesToAdd.length === 0) {
    logger.info(`   ✅ No new identities to add for user ${userId}`);
    return;
  }

  logger.info(`📞 Adding ${identitiesToAdd.length} new identities for user ${userId}`);

  for (const identity of identitiesToAdd) {
    try {
      await zendeskClient.post(`/users/${userId}/identities.json`, { identity });
      logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;
      logger.warn(`   ⚠️ Failed to add ${identity.value}: ${JSON.stringify(msg)}`);
    }
  }
}

logger.info("💬 Zendesk service with retry initialized");
