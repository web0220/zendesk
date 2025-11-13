import { logger } from "../config/logger.js";
import { fetchClients, fetchCaregivers } from "../modules/alayacare/service.js";
import { mapClientToZendesk, mapCaregiverToZendesk } from "../modules/alayacare/mapper.js";
import { bulkUpsertUsers, syncUserIdentities } from "../modules/zendesk/service.js";
import { pollJobStatus } from "../modules/zendesk/jobPoller.js";
import { chunkArray, runWithLimit } from "../modules/common/rateLimiter.js";
import { sanitizeUsers } from "../modules/common/validator.js";

// ======================================================
// 🧠 Main Orchestrator - Runs Full Sync Flow
// ======================================================
export async function runSync() {
  logger.info("🔄 Starting data sync between AlayaCare and Zendesk...");

  try {
    // 1️⃣ Fetch clients & caregivers
    const [clients, caregivers] = await Promise.all([
      fetchClients({ status: "active" }),
      fetchCaregivers({ status: "active" }),
    ]);
    logger.info(`📥 Fetched ${clients.length} clients, ${caregivers.length} caregivers`);

    // 2️⃣ Map & validate
    const clientPayload = clients.map(mapClientToZendesk).filter(Boolean);
    const caregiverPayload = caregivers.map(mapCaregiverToZendesk).filter(Boolean);
    const allUsers = [...clientPayload, ...caregiverPayload];

    const users = sanitizeUsers(allUsers);
    logger.info(`🧩 Prepared ${users.length} valid users for Zendesk sync`);

    // 3️⃣ Batch users to respect Zendesk limits
    const batches = chunkArray(users, 100);
    logger.info(`📦 Split into ${batches.length} batches`);

    // 4️⃣ Process batches with concurrency limit
    const tasks = batches.map(
      (batch, i) => async () => {
        logger.info(`➡️ Processing batch ${i + 1}/${batches.length}`);
        const result = await bulkUpsertUsers(batch);
        const jobId = result?.job_status?.id;

        if (jobId) {
          const job = await pollJobStatus(jobId);
          logger.info(`✅ Job ${jobId} finished with status: ${job.job_status?.status}`);

          const jobResults = job.job_status?.results || [];

          // 5️⃣ After job completion, add secondary identities
          for (let j = 0; j < jobResults.length; j++) {
            const res = jobResults[j];
            const userData = batch[j];

            if (res.status === "Created" || res.status === "Updated") {
              await syncUserIdentities(res.id, userData);
            }
          }
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
