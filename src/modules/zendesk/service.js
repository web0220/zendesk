import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../common/retry.js";

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

// ======================================================
// 🚀 Bulk upsert users (create or update many)
// ======================================================
export async function bulkUpsertUsers(users = []) {
  return withRetry(async () => {
    const payload = {
      users: users.map(u => ({
        name: u.name,
        email: u.email,
        phone: u.phone,
        user_fields: u.user_fields || {},
      })),
    };

    const res = await zendeskClient.post("/users/create_or_update_many.json", payload);
    logger.info(`🧩 Upsert request accepted: ${users.length} users`);
    return res.data;
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
// ➕ Sync user identities (secondary emails & phones)
// ======================================================
export async function syncUserIdentities(userId, userData) {
  if (!userId) return;

  const extras = [];

  // Collect secondary emails
  if (Array.isArray(userData.emails) && userData.emails.length > 1) {
    for (const email of userData.emails.slice(1)) {
      extras.push({ type: "email", value: email });
    }
  }

  // Collect secondary phones
  if (Array.isArray(userData.phones) && userData.phones.length > 1) {
    for (const phone of userData.phones.slice(1)) {
      extras.push({ type: "phone_number", value: phone });
    }
  }

  if (extras.length === 0) return;

  logger.info(`📞 Adding ${extras.length} extra identities for user ${userId}`);

  for (const identity of extras) {
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
