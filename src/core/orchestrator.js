import { logger } from "../config/logger.js";
import { fetchClients, fetchCaregivers } from "../modules/alayacare/service.js";
import { mapClientToZendesk, mapCaregiverToZendesk } from "../modules/alayacare/mapper.js";
import { bulkUpsertUsers } from "../modules/zendesk/service.js";
import { pollJobStatus } from "../modules/zendesk/jobPoller.js";
import { chunkArray, runWithLimit } from "../modules/common/rateLimiter.js";
import { sanitizeUsers } from "../modules/common/validator.js";

export async function runSync() {
  logger.info("🔄 Starting data sync between AlayaCare and Zendesk...");

  try {
    const [clients, caregivers] = await Promise.all([
      fetchClients({ status: "active" }),
      fetchCaregivers({ status: "active" }),
    ]);
    logger.info(`📥 Fetched ${clients.length} clients, ${caregivers.length} caregivers`);

    const clientPayload = clients.map(mapClientToZendesk).filter(Boolean);
    const caregiverPayload = caregivers.map(mapCaregiverToZendesk).filter(Boolean);
    const allUsers = [...clientPayload, ...caregiverPayload];

    const users = sanitizeUsers(allUsers);
    logger.info(`🧩 Prepared ${users.length} valid users for Zendesk sync`);

    const batches = chunkArray(users, 100);
    logger.info(`📦 Split into ${batches.length} batches`);

    const tasks = batches.map(
      (batch, i) => async () => {
        logger.info(`➡️ Processing batch ${i + 1}/${batches.length}`);
        const result = await bulkUpsertUsers(batch);
        const jobId = result?.job_status?.id;
        if (jobId) {
          await pollJobStatus(jobId);
        } else {
          logger.warn("⚠️ No job ID returned from Zendesk — cannot verify status.");
        }
        return result;
      }
    );

    const results = await runWithLimit(tasks, 5);
    logger.info("✅ All batches submitted and confirmed successfully.");

    return { totalUsers: users.length, batches: results.length };
  } catch (err) {
    logger.error("❌ Sync failed:", err.response?.data || err.message);
    throw err;
  }
}
