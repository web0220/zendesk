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

// ======================================================
// 🚀 Bulk upsert users (create or update many)
// ======================================================
export async function bulkUpsertUsers(users = []) {
  return withRetry(async () => {
    // Send everything except identities
    const payload = {
      users: users.map(u => {
        const { identities, ...userWithoutIdentities } = u;
        return {
          name: userWithoutIdentities.name,
          email: userWithoutIdentities.email,
          phone: userWithoutIdentities.phone,
          user_fields: userWithoutIdentities.user_fields || {},
        };
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

    const jobResults = job.results || [];

    // Send identities for each user
    for (let i = 0; i < jobResults.length; i++) {
      const result = jobResults[i];
      const userData = users[i];

      if ((result.status === "Created" || result.status === "Updated") && result.id) {
        const userId = result.id;
        const identities = userData.identities || [];

        // Send each identity separately
        for (const identity of identities) {
          try {
            // Convert identity type to Zendesk API format
            // "phone" -> "phone_number", "email" -> "email"
            let identityType;
            if (identity.type === "phone") {
              identityType = "phone_number";
            } else if (identity.type === "email") {
              identityType = "email";
            } else {
              // Keep other types as-is
              identityType = identity.type;
            }
            
            await zendeskClient.post(`/users/${userId}/identities.json`, {
              identity: {
                type: identityType,
                value: identity.value,
              },
            });
            logger.info(`   ➕ Added ${identityType}: ${identity.value} for user ${userId}`);
          } catch (err) {
            const msg = err.response?.data || err.message;
            logger.warn(`   ⚠️ Failed to add identity ${identity.value} for user ${userId}: ${JSON.stringify(msg)}`);
          }
        }
      }
    }

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
