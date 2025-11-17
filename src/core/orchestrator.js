import { logger } from "../config/logger.js";
import { fetchClients, fetchCaregivers } from "../modules/alayacare/service.js";
import { mapClientToZendesk, mapCaregiverToZendesk } from "../modules/alayacare/mapper.js";
import { bulkUpsertUsers, syncUserIdentities } from "../modules/zendesk/service.js";
import { chunkArray, runWithLimit } from "../modules/common/rateLimiter.js";
import { sanitizeUsers } from "../modules/common/validator.js";
import { upsertUserMapping } from "../infrastructure/database.js";

// ======================================================
// 🧠 Main Orchestrator - Runs Full Sync Flow
// ======================================================
export async function runSync() {
  logger.info("🔄 Starting data sync between AlayaCare and Zendesk...");

  try {
    // 1️⃣ Fetch clients & caregivers (raw JSON from AlayaCare API)
    const [clients, caregivers] = await Promise.all([
      fetchClients({ status: "active" }),
      fetchCaregivers({ status: "active" }),
    ]);
    logger.info(`📥 Fetched ${clients.length} clients, ${caregivers.length} caregivers`);

    // 2️⃣ Map to Zendesk format (includes external_id, ac_id, and identities)
    const clientPayload = clients.map(mapClientToZendesk).filter(Boolean);
    const caregiverPayload = caregivers.map(mapCaregiverToZendesk).filter(Boolean);
    const allUsers = [...clientPayload, ...caregiverPayload];

    const users = sanitizeUsers(allUsers);
    logger.info(`🧩 Prepared ${users.length} valid users for Zendesk sync`);

    // 3️⃣ Batch users to respect Zendesk limits (100 per batch)
    const batches = chunkArray(users, 100);
    logger.info(`📦 Split into ${batches.length} batches`);

    let totalMappingsStored = 0;
    let totalIdentitiesSynced = 0;

    // 4️⃣ Process batches with concurrency limit
    const tasks = batches.map(
      (batch, i) => async () => {
        logger.info(`➡️ Processing batch ${i + 1}/${batches.length}`);
        
        // Send to Zendesk (without identities)
        const result = await bulkUpsertUsers(batch);
        const jobStatus = result?.job_status;
        const jobResults = jobStatus?.results || [];

        if (jobResults.length === 0) {
          logger.warn("⚠️ No results returned from Zendesk job");
          return result;
        }

        logger.info(`✅ Batch ${i + 1} processed: ${jobResults.length} results`);

        // 5️⃣ Store mappings in database
        const syncTimestamp = new Date().toISOString();
        
        for (let j = 0; j < jobResults.length; j++) {
          const jobResult = jobResults[j];
          const userData = batch[j];

          if (jobResult.status === "Created" || jobResult.status === "Updated") {
            // Store mapping: ac_id -> zendesk_user_id
            const mapping = {
              ac_id: String(userData.ac_id),
              zendesk_user_id: jobResult.id,
              external_id: jobResult.external_id || userData.external_id,
              last_synced_at: syncTimestamp,
            };

            upsertUserMapping(mapping);
            totalMappingsStored++;

            logger.debug(
              `💾 Stored: ac_id=${mapping.ac_id} → zendesk_user_id=${mapping.zendesk_user_id}`
            );

            // 6️⃣ Sync identities using stored zendesk_user_id
            await syncUserIdentities(mapping.zendesk_user_id, userData);
            totalIdentitiesSynced++;
          } else {
            logger.warn(
              `⚠️ Skipping user ${userData.name}: status=${jobResult.status}`
            );
          }
        }

        return result;
      }
    );

    const results = await runWithLimit(tasks, 5);
    logger.info("✅ All batches submitted and confirmed successfully.");
    logger.info(`📊 Summary: ${totalMappingsStored} mappings stored, ${totalIdentitiesSynced} identities synced`);

    return {
      totalUsers: users.length,
      batches: results.length,
      mappingsStored: totalMappingsStored,
      identitiesSynced: totalIdentitiesSynced,
    };
  } catch (err) {
    logger.error("❌ Sync failed:", err.response?.data || err.message);
    throw err;
  }
}
