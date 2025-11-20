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

  try {
    // 1️⃣ Fetch clients & caregivers (raw JSON from AlayaCare API)
    logger.info("🔍 Starting fetch from AlayaCare API...");
    const [clients, caregivers] = await Promise.all([
      fetchClients({ status: "active" }),
      fetchCaregivers({ status: "active" }),
    ]);
    logger.info(`📥 Fetched ${clients.length} clients, ${caregivers.length} caregivers`);
    
    // Critical check: log if counts seem inconsistent
    if (clients.length < 100) {
      logger.warn(`⚠️ WARNING: Only ${clients.length} clients fetched. Expected ~500. Check pagination!`);
    }
    if (caregivers.length < 1000) {
      logger.warn(`⚠️ WARNING: Only ${caregivers.length} caregivers fetched. Expected ~2000. Check pagination!`);
    }

    // 2️⃣ Map to Zendesk format (includes external_id, ac_id, and identities)
    logger.info("🔄 Mapping clients and caregivers to Zendesk format...");
    const clientPayload = clients.map(mapClientToZendesk).filter(Boolean);
    const caregiverPayload = caregivers.map(mapCaregiverToZendesk).filter(Boolean);
    const allUsers = [...clientPayload, ...caregiverPayload];
    
    logger.info(`📊 Mapping results: ${clientPayload.length}/${clients.length} clients mapped, ${caregiverPayload.length}/${caregivers.length} caregivers mapped`);
    
    if (clientPayload.length < clients.length * 0.8) {
      logger.warn(`⚠️ WARNING: ${clients.length - clientPayload.length} clients failed to map. Check mapper logic!`);
    }

    const users = sanitizeUsers(allUsers);
    logger.info(`🧩 Prepared ${users.length} valid users for Zendesk sync (${clientPayload.length} clients + ${caregiverPayload.length} caregivers before sanitization)`);

    // 3️⃣ Batch users to respect Zendesk limits (100 per batch)
    // Note: Zendesk allows up to 1000 users per bulk call, but we use 100 for safety
    const batches = chunkArray(users, 100);
    logger.info(`📦 Split into ${batches.length} batches of up to 100 users each (total: ${users.length} users)`);
    
    // Log batch distribution
    const clientBatches = batches.filter(batch => batch.some(u => u.user_fields?.type === "client")).length;
    const caregiverBatches = batches.filter(batch => batch.some(u => u.user_fields?.type === "caregiver")).length;
    logger.info(`   📋 Batch breakdown: ~${clientBatches} batches with clients, ~${caregiverBatches} batches with caregivers`);

    let totalMappingsStored = 0;
    let totalIdentitiesSynced = 0;
    let totalClientsProcessed = 0;
    let totalCaregiversProcessed = 0;

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

        logger.info(`✅ Batch ${i + 1} processed: ${jobResults.length} results (expected ${batch.length} users)`);
        
        // Validate batch results match input
        if (jobResults.length !== batch.length) {
          logger.warn(`⚠️ WARNING: Batch ${i + 1} result count mismatch! Sent ${batch.length} users, got ${jobResults.length} results`);
        }

        // 5️⃣ Store mappings in database
        const syncTimestamp = new Date().toISOString();
        
        // Create a map of external_id -> userData for reliable matching
        // Zendesk results might not be in the same order as input, so we match by external_id
        const batchMap = new Map();
        batch.forEach(user => {
          if (user.external_id) {
            batchMap.set(user.external_id, user);
          }
        });
        
        let batchCreated = 0;
        let batchUpdated = 0;
        let batchFailed = 0;
        const processedExternalIds = new Set();
        
        // Process all results from Zendesk
        for (const jobResult of jobResults) {
          const externalId = jobResult.external_id;
          const userData = externalId ? batchMap.get(externalId) : null;
          
          // If we can't match by external_id, try to match by index as fallback
          // This handles cases where external_id might be missing
          const fallbackIndex = jobResult.index !== undefined ? jobResult.index : 
                                (jobResults.indexOf(jobResult) < batch.length ? jobResults.indexOf(jobResult) : null);
          const matchedUserData = userData || (fallbackIndex !== null ? batch[fallbackIndex] : null);

          if (!matchedUserData) {
            logger.warn(`⚠️ Cannot match Zendesk result: external_id=${externalId || 'N/A'}, index=${fallbackIndex || 'N/A'}`);
            batchFailed++;
            continue;
          }

          // Skip if we've already processed this external_id (duplicate result)
          if (externalId && processedExternalIds.has(externalId)) {
            logger.debug(`   ⏭️  Skipping duplicate result for external_id=${externalId}`);
            continue;
          }
          if (externalId) {
            processedExternalIds.add(externalId);
          }

          const userType = matchedUserData.user_fields?.type || "unknown";
          
          if (jobResult.status === "Created" || jobResult.status === "Updated") {
            if (jobResult.status === "Created") batchCreated++;
            if (jobResult.status === "Updated") batchUpdated++;
            // Store mapping: ac_id -> zendesk_user_id
            const mapping = {
              ac_id: String(matchedUserData.ac_id),
              zendesk_user_id: jobResult.id,
              external_id: jobResult.external_id || matchedUserData.external_id,
              mapped_data: matchedUserData,
              last_synced_at: syncTimestamp,
            };

            upsertUserMapping(mapping);
            totalMappingsStored++;
            
            // Track client vs caregiver counts
            if (userType === "client") {
              totalClientsProcessed++;
            } else if (userType === "caregiver") {
              totalCaregiversProcessed++;
            }

            logger.debug(
              `💾 Stored ${userType}: ac_id=${mapping.ac_id} → zendesk_user_id=${mapping.zendesk_user_id}`
            );

            // 6️⃣ Sync identities using stored zendesk_user_id
            await syncUserIdentities(mapping.zendesk_user_id, matchedUserData);
            totalIdentitiesSynced++;
          } else {
            batchFailed++;
            logger.warn(
              `⚠️ Skipping ${userType} ${matchedUserData.name || matchedUserData.external_id || 'unknown'}: status=${jobResult.status}`
            );
          }
        }
        
        // Check if any users from the batch weren't in the results
        const batchExternalIds = new Set(batch.filter(u => u.external_id).map(u => u.external_id));
        const missingExternalIds = [...batchExternalIds].filter(id => !processedExternalIds.has(id));
        if (missingExternalIds.length > 0) {
          logger.warn(`⚠️ WARNING: Batch ${i + 1} has ${missingExternalIds.length} users not in Zendesk results (may have failed silently)`);
          missingExternalIds.slice(0, 5).forEach(id => {
            const missingUser = batchMap.get(id);
            const userType = missingUser?.user_fields?.type || "unknown";
            logger.debug(`   - Missing ${userType}: ${missingUser?.name || id} (external_id=${id})`);
          });
        }
        
        logger.debug(`📊 Batch ${i + 1} summary: ${batchCreated} created, ${batchUpdated} updated, ${batchFailed} failed`);

        return result;
      }
    );

    const results = await runWithLimit(tasks, 5);
    logger.info("✅ All batches submitted and confirmed successfully.");
    
    // Final validation: ensure we processed all users
    const totalProcessed = totalMappingsStored;
    const totalExpected = users.length;
    const expectedClients = users.filter(u => u.user_fields?.type === "client").length;
    const expectedCaregivers = users.filter(u => u.user_fields?.type === "caregiver").length;
    
    logger.info(`📊 Summary: ${totalMappingsStored} mappings stored, ${totalIdentitiesSynced} identities synced`);
    logger.info(`   👥 Clients: ${totalClientsProcessed}/${expectedClients} processed`);
    logger.info(`   👥 Caregivers: ${totalCaregiversProcessed}/${expectedCaregivers} processed`);
    
    if (totalProcessed < totalExpected * 0.9) {
      logger.error(`❌ CRITICAL: Only processed ${totalProcessed}/${totalExpected} users (${((totalProcessed/totalExpected)*100).toFixed(1)}%). Data loss detected!`);
    } else if (totalProcessed < totalExpected) {
      logger.warn(`⚠️ WARNING: Processed ${totalProcessed}/${totalExpected} users. Some users may have failed.`);
    } else {
      logger.info(`✅ Successfully processed all ${totalProcessed} users.`);
    }
    
    // Warn if client processing is significantly lower than expected
    if (totalClientsProcessed < expectedClients * 0.9) {
      logger.error(`❌ CRITICAL: Only ${totalClientsProcessed}/${expectedClients} clients processed (${((totalClientsProcessed/expectedClients)*100).toFixed(1)}%). Client sync issue detected!`);
    } else if (totalClientsProcessed < expectedClients) {
      logger.warn(`⚠️ WARNING: Only ${totalClientsProcessed}/${expectedClients} clients processed. Some clients may have failed.`);
    }

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
