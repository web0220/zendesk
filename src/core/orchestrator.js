import { logger } from "../config/logger.js";
import { fetchClients, fetchCaregivers } from "../services/alayacare/fetch.js";
import { mapClientUser, mapCaregiverUser } from "../services/alayacare/mapper.js";
import { bulkUpsertUsers, updateUser } from "../services/zendesk/upsert.js";
import { syncUserIdentities } from "../services/zendesk/identitySync.js";
import { UserEntity } from "../domain/UserEntity.js";
import { chunkArray, runWithLimit } from "../utils/rateLimiter.js";
import {
  saveMappedUsersBatch,
  getUsersPendingSync,
  updateZendeskUserId,
  processDuplicateEmailsAndPhones,
  resetCurrentActiveFlag,
  getUsersWithStatusChange,
  fetchAndUpdateUserStatus,
  getAllUsersForSync,
  processNonActiveUserEmailSwaps,
  findEmailGroupsWithoutPrimary,
} from "../infra/database.js";
import { sendEmailNotificationForDuplicateUsers } from "../services/notification/email.js";

const BATCH_LIMIT = 100;
const BATCH_CONCURRENCY = Number(process.env.ZENDESK_BATCH_CONCURRENCY) || 5;
const ALVITA_COMPANY_ORG_ID = "40994316312731";

function isAlvitaCompanyMember(orgId) {
  if (orgId === null || orgId === undefined) return false;
  try {
    return String(orgId) === ALVITA_COMPANY_ORG_ID;
  } catch {
    return false;
  }
}

function logFetchHealth(clients, caregivers) {
  if (clients.length < 100) {
    logger.warn(
      `⚠️ WARNING: Only ${clients.length} clients fetched. Expected ~500. Check pagination!`
    );
  }
  if (caregivers.length < 1000) {
    logger.warn(
      `⚠️ WARNING: Only ${caregivers.length} caregivers fetched. Expected ~2000. Check pagination!`
    );
  }
}

function collectEntities(records, mapper, label) {
  const mapped = records.map(mapper).filter(Boolean);
  const valid = mapped.filter((entity) => entity.validate());

  if (mapped.length < records.length * 0.8) {
    logger.warn(
      `⚠️ WARNING: ${records.length - mapped.length} ${label.toLowerCase()} failed to map. Check mapper logic!`
    );
  }

  return valid;
}

function entitiesToPayloads(entities) {
  return entities.map((entity) => entity.toZendeskPayload()).filter(Boolean);
}

function hydrateEntitiesFromDb(rows) {
  return rows
    .map((row) => UserEntity.fromDbRow(row))
    .filter(Boolean)
    .map((entity) => entity.toZendeskPayload());
}

export async function runSync() {
  try {
    resetCurrentActiveFlag();

    const clients = await fetchClients({ status: "active" });
    const caregivers = await fetchCaregivers({ status: "active" });

    logger.info(`📥 Fetched ${clients.length} clients`);
    logger.info(`📥 Fetched ${caregivers.length} caregivers`);
    logFetchHealth(clients, caregivers);

    logger.info("🔄 Mapping AlayaCare records to UserEntity models...");
    const clientEntities = collectEntities(clients, mapClientUser, "Clients");
    const caregiverEntities = collectEntities(caregivers, mapCaregiverUser, "Caregivers");
    const entityPayloads = entitiesToPayloads([...clientEntities, ...caregiverEntities]);

    logger.info(
      `🧩 Prepared ${entityPayloads.length} valid users (${clientEntities.length} clients + ${caregiverEntities.length} caregivers)`
    );

    logger.info("💾 Saving all mapped data to database...");
    let savedCount = 0;
    try {
      savedCount = saveMappedUsersBatch(entityPayloads);
      logger.info(`✅ Saved ${savedCount}/${entityPayloads.length} users to database`);
    } catch (error) {
      logger.error("❌ Failed to save mapped data batch:", error);
      throw error;
    }
    if (savedCount < entityPayloads.length) {
      logger.warn(
        `⚠️ WARNING: Only saved ${savedCount}/${entityPayloads.length} users to database. Some data may be missing.`
      );
    }

    // Process duplicates for all active users (current_active = 1)
    // The function itself checks if there are active users and returns early if not
    logger.info("🔧 Processing duplicate emails and phone numbers...");
    processDuplicateEmailsAndPhones();
    logger.info("✅ Finished processing duplicates");

    // Step 2: Detect and update status for users who changed from active to inactive
    logger.info("🔍 Checking for users with status changes...");
    const usersWithStatusChange = getUsersWithStatusChange();
    
    // Track status changes with details
    const statusChanges = [];
    
    if (usersWithStatusChange.length > 0) {
      logger.info(
        `📋 Found ${usersWithStatusChange.length} users with potential status change. Fetching current status from AlayaCare...`
      );
      
      // Log removed/inactive users
      logger.info("📋 Users not found in current sync (may be inactive/removed):");
      usersWithStatusChange.slice(0, 20).forEach((user) => {
        const userType = user.user_type || "unknown";
        const userName = user.name || user.external_id || "unknown";
        logger.info(
          `   ➖ ${userType.toUpperCase()}: ${userName} (AC ID: ${user.ac_id}, Zendesk ID: ${user.zendesk_user_id || "N/A"})`
        );
      });
      if (usersWithStatusChange.length > 20) {
        logger.info(`   ... and ${usersWithStatusChange.length - 20} more users`);
      }

      // Fetch status updates with concurrency control
      const statusUpdateTasks = usersWithStatusChange.map((user) => async () => {
        const oldStatus = user.user_type === "client" ? user.client_status : user.caregiver_status;
        const result = await fetchAndUpdateUserStatus(user);
        if (result && result.statusChanged) {
          statusChanges.push({
            name: user.name || user.external_id || "unknown",
            userType: user.user_type || "unknown",
            acId: user.ac_id,
            zendeskUserId: user.zendesk_user_id,
            oldStatus: oldStatus || "null",
            newStatus: result.newStatus || "null",
          });
        }
        return result;
      });

      const DETAIL_CONCURRENCY = Number(process.env.ALAYACARE_DETAIL_CONCURRENCY) || 10;
      const statusUpdateResults = await runWithLimit(statusUpdateTasks, DETAIL_CONCURRENCY);
      
      const successfulUpdates = statusUpdateResults.filter((r) => r && r.success).length;
      const failedUpdates = statusUpdateResults.length - successfulUpdates;
      
      logger.info(
        `✅ Status update complete: ${successfulUpdates} successful, ${failedUpdates} failed`
      );
      
      if (failedUpdates > 0) {
        logger.warn(
          `⚠️ ${failedUpdates} users failed to update status. They will be retried in next sync.`
        );
      }
      
      // Step 3: Process email/phone swaps for non-active users
      // This handles the edge case where a primary user becomes non-active
      // and an active user needs to get the original email back
      const usersToUpdate = processNonActiveUserEmailSwaps(usersWithStatusChange);
      
      // Update users to Zendesk using PUT method (for email/phone changes)
      if (usersToUpdate.length > 0) {
        logger.info(`🔄 Updating ${usersToUpdate.length} user(s) to Zendesk via PUT (email/phone changes)...`);
        
        const updateTasks = usersToUpdate.map((user) => async () => {
          if (!user.zendesk_user_id) {
            logger.warn(`⚠️  Skipping user ${user.ac_id}: no zendesk_user_id`);
            return { success: false, userId: null };
          }
          
          try {
            // Convert database row to Zendesk payload format
            const zendeskPayload = UserEntity.fromDbRow(user)?.toZendeskPayload();
            if (!zendeskPayload) {
              logger.warn(`⚠️  Failed to convert user ${user.ac_id} to Zendesk payload`);
              return { success: false, userId: user.zendesk_user_id };
            }
            
            const result = await updateUser(user.zendesk_user_id, zendeskPayload);
            return { success: !!result, userId: user.zendesk_user_id };
          } catch (error) {
            logger.error(
              `❌ Failed to update user ${user.ac_id} (Zendesk ID: ${user.zendesk_user_id}): ${error.message}`
            );
            return { success: false, userId: user.zendesk_user_id };
          }
        });
        
        const UPDATE_CONCURRENCY = Number(process.env.ZENDESK_UPDATE_CONCURRENCY) || 5;
        const updateResults = await runWithLimit(updateTasks, UPDATE_CONCURRENCY);
        
        const successfulUpdates = updateResults.filter((r) => r && r.success).length;
        const failedZendeskUpdates = updateResults.length - successfulUpdates;
        
        logger.info(
          `✅ Zendesk PUT updates complete: ${successfulUpdates} successful, ${failedZendeskUpdates} failed`
        );
        
        if (failedZendeskUpdates > 0) {
          logger.warn(
            `⚠️ ${failedZendeskUpdates} users failed to update in Zendesk. They will be retried in next sync.`
          );
        }
      }
    } else {
      logger.info("✅ No users with status changes detected.");
    }

    // Send ALL users from database to Zendesk to ensure it's always in sync
    // This includes newly added users, updated users, and users with status changes
    const allUsersForSync = getAllUsersForSync();
    
    // Check for email groups with 2+ users and no zendesk_primary tag
    // These users should NOT be sent to Zendesk
    logger.info("🔍 Checking for email groups without zendesk_primary tag...");
    const problematicGroups = findEmailGroupsWithoutPrimary();
    
    let usersToExclude = new Set();
    if (problematicGroups.length > 0) {
      logger.warn(
        `⚠️  Found ${problematicGroups.length} email group(s) with 2+ users and no zendesk_primary tag`
      );
      
      // Collect all user IDs to exclude
      for (const group of problematicGroups) {
        for (const user of group.users) {
          usersToExclude.add(user.ac_id);
          logger.warn(
            `   ❌ Excluding user ${user.ac_id} (${user.name}): email "${group.email}" - no zendesk_primary tag in group`
          );
        }
      }
      
      // Send email notification to Paula
      await sendEmailNotificationForDuplicateUsers(problematicGroups);
    } else {
      logger.info("✅ No problematic email groups found (all groups have zendesk_primary tag or < 2 users)");
    }
    
    // Filter out users in problematic groups
    const usersToSync = allUsersForSync.filter((u) => !usersToExclude.has(u.ac_id));
    const excludedCount = allUsersForSync.length - usersToSync.length;
    
    if (excludedCount > 0) {
      logger.warn(
        `⚠️  Excluding ${excludedCount} user(s) from Zendesk sync due to missing zendesk_primary tag in email groups`
      );
    }
    
    logger.info(
      `📋 Sending ${usersToSync.length} total users to Zendesk (includes new users, updated users, and users with status changes)`
    );
    
    // Log breakdown of users being sent
    const newUsers = usersToSync.filter((u) => !u.zendesk_user_id).length;
    const alreadySyncedUsers = usersToSync.filter((u) => u.zendesk_user_id).length;
    logger.info(
      `   📊 Breakdown: ${newUsers} new users, ${alreadySyncedUsers} already synced users (will be updated in Zendesk)`
    );
    
    if (usersToSync.length === 0) {
      logger.info("✅ No users pending sync. All users are already synced.");
      if (usersWithStatusChange.length > 0) {
        logger.info(`   ℹ️  Processed ${usersWithStatusChange.length} status updates (users already synced, status updated in DB)`);
      }
      
      // Calculate total counts by type from entities
      const totalClients = clientEntities.length;
      const totalCaregivers = caregiverEntities.length;
      const totalCompanyMembers = [...clientEntities, ...caregiverEntities].filter((e) => 
        isAlvitaCompanyMember(e.organizationId)
      ).length;
      
      return {
        totalUsers: entityPayloads.length,
        savedToDatabase: savedCount,
        syncedToZendesk: 0,
        batches: 0,
        mappingsStored: 0,
        identitiesSynced: 0,
        statusUpdatesProcessed: usersWithStatusChange.length,
        newlyCreated: {
          count: 0,
          users: [],
          byType: {
            clients: 0,
            caregivers: 0,
            companyMembers: 0,
          },
        },
        // updated: {
        //   count: 0,
        //   users: [],
        //   byType: {
        //     clients: 0,
        //     caregivers: 0,
        //     companyMembers: 0,
        //   },
        // },
        statusChanges: {
          count: statusChanges.length,
          changes: statusChanges,
        },
        countsByType: {
          clients: {
            total: totalClients,
            created: 0,
            updated: 0,
          },
          caregivers: {
            total: totalCaregivers,
            created: 0,
            updated: 0,
          },
          companyMembers: {
            total: totalCompanyMembers,
            created: 0,
            updated: 0,
          },
        },
      };
    }

    const zendeskUsers = hydrateEntitiesFromDb(usersToSync);
    logger.info(`📦 Converted ${zendeskUsers.length} database records into Zendesk payloads`);
    
    // Debug: Log status values being sent to Zendesk
    const usersWithStatus = zendeskUsers.filter((u) => {
      const status = u.user_fields?.userstatus;
      return status;
    });
    if (usersWithStatus.length > 0) {
      logger.debug(`📋 ${usersWithStatus.length} users have status to send to Zendesk`);
      usersWithStatus.slice(0, 5).forEach((u) => {
        const status = u.user_fields?.userstatus;
        const userType = u.user_fields?.type || "unknown";
        logger.debug(`   ${userType}: ${u.name} - status: ${status}`);
      });
    }
    
    // Debug: Check for users that should have status but don't (likely company members)
    const usersMissingStatus = zendeskUsers.filter((u) => {
      const userType = u.user_fields?.type;
      const hasStatusField = u.user_fields?.userstatus;
      return userType && !hasStatusField;
    });
    if (usersMissingStatus.length > 0) {
      logger.info(`ℹ️  ${usersMissingStatus.length} users synced without status field (likely company members - status tracked in DB but not sent to Zendesk)`);
      usersMissingStatus.slice(0, 5).forEach((u) => {
        logger.debug(`   ${u.user_fields?.type}: ${u.name} - company member (status tracked in DB)`);
      });
    }

    const batches = chunkArray(zendeskUsers, BATCH_LIMIT);
    logger.info(
      `📦 Split into ${batches.length} batches of up to ${BATCH_LIMIT} users each (total: ${zendeskUsers.length} users)`
    );

    const clientBatches = batches.filter((batch) =>
      batch.some((user) => user.user_fields?.type === "client")
    ).length;
    const caregiverBatches = batches.filter((batch) =>
      batch.some((user) => user.user_fields?.type === "caregiver")
    ).length;
    logger.info(
      `   📋 Batch breakdown: ~${clientBatches} batches with clients, ~${caregiverBatches} batches with caregivers`
    );

    let totalMappingsUpdated = 0;
    let totalIdentitiesSynced = 0;
    let totalClientsProcessed = 0;
    let totalCaregiversProcessed = 0;
    let totalCreated = 0;
    let totalUpdated = 0;
    let totalFailed = 0;
    
    // Track newly created and updated users with details
    const newlyCreatedUsers = [];
    const updatedUsers = [];
    
    // Track counts by type
    let clientsCreated = 0;
    let clientsUpdated = 0;
    let caregiversCreated = 0;
    let caregiversUpdated = 0;
    let companyMembersCreated = 0;
    let companyMembersUpdated = 0;

    const tasks = batches.map((batch, index) => async () => {
      const result = await bulkUpsertUsers(batch);
      const jobStatus = result?.job_status;
      const jobResults = jobStatus?.results || [];

      if (jobResults.length === 0) {
        logger.warn("⚠️ No results returned from Zendesk job");
        return result;
      }

      logger.info(
        `✅ Batch ${index + 1} processed: ${jobResults.length} results (expected ${batch.length} users)`
      );

      if (jobResults.length !== batch.length) {
        logger.warn(
          `⚠️ WARNING: Batch ${index + 1} result count mismatch! Sent ${batch.length} users, got ${jobResults.length} results`
        );
      }

      const syncTimestamp = new Date().toISOString();
      const batchMap = new Map();
      batch.forEach((user) => {
        if (user.external_id) {
          batchMap.set(user.external_id, user);
        }
      });

      let batchCreated = 0;
      let batchUpdated = 0;
      let batchFailed = 0;
      const processedExternalIds = new Set();

      for (const jobResult of jobResults) {
        const externalId = jobResult.external_id;
        const userData = externalId ? batchMap.get(externalId) : null;
        const fallbackIndex =
          jobResult.index !== undefined
            ? jobResult.index
            : jobResults.indexOf(jobResult) < batch.length
            ? jobResults.indexOf(jobResult)
            : null;
        const matchedUserData = userData || (fallbackIndex !== null ? batch[fallbackIndex] : null);

        if (!matchedUserData) {
          logger.warn(
            `⚠️ Cannot match Zendesk result: external_id=${externalId || "N/A"}, index=${
              fallbackIndex || "N/A"
            }`
          );
          batchFailed++;
          continue;
        }

        if (externalId && processedExternalIds.has(externalId)) {
          continue;
        }
        if (externalId) {
          processedExternalIds.add(externalId);
        }

        const userType = matchedUserData.user_fields?.type || "unknown";
        const acId = String(matchedUserData.ac_id);

        if (jobResult.status === "Created" || jobResult.status === "Updated") {
          const userName = matchedUserData.name || matchedUserData.external_id || "unknown";
          const isCompanyMember = isAlvitaCompanyMember(matchedUserData.organization_id);
          const currentStatus = matchedUserData.user_fields?.userstatus || null;
          
          if (jobResult.status === "Created") {
            batchCreated++;
            totalCreated++;
            newlyCreatedUsers.push({
              name: userName,
              userType: userType,
              acId: acId,
              zendeskUserId: jobResult.id,
              isCompanyMember: isCompanyMember,
            });
            
            if (userType === "client") {
              clientsCreated++;
            } else if (userType === "caregiver") {
              caregiversCreated++;
            }
            if (isCompanyMember) {
              companyMembersCreated++;
            }
          }
          if (jobResult.status === "Updated") {
            batchUpdated++;
            totalUpdated++;
            updatedUsers.push({
              name: userName,
              userType: userType,
              acId: acId,
              zendeskUserId: jobResult.id,
              isCompanyMember: isCompanyMember,
              status: currentStatus || null,
            });
            
            if (userType === "client") {
              clientsUpdated++;
            } else if (userType === "caregiver") {
              caregiversUpdated++;
            }
            if (isCompanyMember) {
              companyMembersUpdated++;
            }
          }

          if (matchedUserData.identities && matchedUserData.identities.length > 0) {
            const identitySummary = matchedUserData.identities
              .map((id) => `${id.type}:${id.value}`)
              .slice(0, 3)
              .join(", ");
            const moreCount = matchedUserData.identities.length > 3 
              ? ` (+${matchedUserData.identities.length - 3} more)` 
              : "";
          }

          await syncUserIdentities(jobResult.id, matchedUserData);
          totalIdentitiesSynced++;

          // Update database with zendesk_user_id and identities after syncing
          updateZendeskUserId(acId, jobResult.id, syncTimestamp, userType, matchedUserData.identities);
          totalMappingsUpdated++;

          if (userType === "client") {
            totalClientsProcessed++;
          } else if (userType === "caregiver") {
            totalCaregiversProcessed++;
          }
        } else {
          batchFailed++;
          totalFailed++;
          const userName = matchedUserData.name || matchedUserData.external_id || "unknown";
          logger.warn(`⚠️ Skipping ${userType} ${userName}: status=${jobResult.status}`);
          logger.warn(`   📋 Failed ${userType} details:`);
          logger.warn(`      Name: ${matchedUserData.name || "N/A"}`);
          logger.warn(`      External ID: ${matchedUserData.external_id || "N/A"}`);
          logger.warn(`      AC ID: ${matchedUserData.ac_id || "N/A"}`);
          logger.warn(`      Email: ${matchedUserData.email || "N/A"}`);
          logger.warn(`      Phone: ${matchedUserData.phone || "N/A"}`);
          logger.warn(`      Organization ID: ${matchedUserData.organization_id || "N/A"}`);
          logger.warn(
            `      Identities count: ${matchedUserData.identities?.length || 0}`
          );
          if (matchedUserData.identities && matchedUserData.identities.length > 0) {
            logger.warn(
              `      Identities: ${JSON.stringify(
                matchedUserData.identities.map((identity) => `${identity.type}:${identity.value}`)
              )}`
            );
          }
          logger.warn(`      User Fields: ${JSON.stringify(matchedUserData.user_fields || {})}`);
          if (jobResult.errors) {
            logger.warn(`      Zendesk Errors: ${JSON.stringify(jobResult.errors)}`);
          }
          if (jobResult.index !== undefined) {
            logger.warn(`      Batch Index: ${jobResult.index}`);
          }
          logger.warn(`      Full Job Result: ${JSON.stringify(jobResult, null, 2)}`);
        }
      }

      const batchExternalIds = new Set(batch.filter((user) => user.external_id).map((u) => u.external_id));
      const missingExternalIds = [...batchExternalIds].filter(
        (id) => !processedExternalIds.has(id)
      );
      if (missingExternalIds.length > 0) {
        logger.warn(
          `⚠️ WARNING: Batch ${index + 1} has ${
            missingExternalIds.length
          } users not in Zendesk results (may have failed silently)`
        );
        missingExternalIds.slice(0, 5).forEach((id) => {
          const missingUser = batchMap.get(id);
          const userType = missingUser?.user_fields?.type || "unknown";
          logger.debug(`   - Missing ${userType}: ${missingUser?.name || id} (external_id=${id})`);
        });
      }

      logger.debug(
        `📊 Batch ${index + 1} summary: ${batchCreated} created, ${batchUpdated} updated, ${batchFailed} failed`
      );
      return result;
    });

    const results = await runWithLimit(tasks, BATCH_CONCURRENCY);
    logger.info("✅ All batches submitted and confirmed successfully.");

    const totalProcessed = totalMappingsUpdated;
    const totalExpected = zendeskUsers.length;
    const expectedClients = zendeskUsers.filter(
      (user) => user.user_fields?.type === "client"
    ).length;
    const expectedCaregivers = zendeskUsers.filter(
      (user) => user.user_fields?.type === "caregiver"
    ).length;

    // Final summary with detailed counts
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info("📊 SYNC SUMMARY");
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info(`   💾 Saved to database: ${savedCount} users`);
    logger.info(`   ➕ Created in Zendesk: ${totalCreated} users`);
    logger.info(`   🔄 Updated in Zendesk: ${totalUpdated} users`);
    logger.info(`   ❌ Failed to sync: ${totalFailed} users`);
    logger.info(`   ➖ Removed/Inactive: ${usersWithStatusChange.length} users`);
    logger.info(`   🔗 Identities synced: ${totalIdentitiesSynced}`);
    logger.info(`   👥 Clients: ${totalClientsProcessed}/${expectedClients} processed`);
    logger.info(`   👥 Caregivers: ${totalCaregiversProcessed}/${expectedCaregivers} processed`);
    logger.info("═══════════════════════════════════════════════════════════");

    if (totalProcessed < totalExpected * 0.9) {
      logger.error(
        `❌ CRITICAL: Only processed ${totalProcessed}/${totalExpected} users (${(
          (totalProcessed / totalExpected) *
          100
        ).toFixed(1)}%). Data loss detected!`
      );
    } else if (totalProcessed < totalExpected) {
      logger.warn(
        `⚠️ WARNING: Processed ${totalProcessed}/${totalExpected} users. Some users may have failed.`
      );
    } else {
      logger.info(`✅ Successfully processed all ${totalProcessed} users.`);
    }

    if (totalClientsProcessed < expectedClients * 0.9) {
      logger.error(
        `❌ CRITICAL: Only ${totalClientsProcessed}/${expectedClients} clients processed (${(
          (totalClientsProcessed / expectedClients) *
          100
        ).toFixed(1)}%). Client sync issue detected!`
      );
    } else if (totalClientsProcessed < expectedClients) {
      logger.warn(
        `⚠️ WARNING: Only ${totalClientsProcessed}/${expectedClients} clients processed. Some clients may have failed.`
      );
    }

    // Calculate total counts by type
    const totalClients = zendeskUsers.filter((u) => u.user_fields?.type === "client").length;
    const totalCaregivers = zendeskUsers.filter((u) => u.user_fields?.type === "caregiver").length;
    const totalCompanyMembers = zendeskUsers.filter((u) => 
      isAlvitaCompanyMember(u.organization_id)
    ).length;

    return {
      totalUsers: entityPayloads.length,
      savedToDatabase: savedCount,
      syncedToZendesk: zendeskUsers.length,
      batches: results.length,
      mappingsStored: totalMappingsUpdated,
      identitiesSynced: totalIdentitiesSynced,
      statusUpdatesProcessed: usersWithStatusChange.length,
      // New detailed information
      newlyCreated: {
        count: totalCreated,
        users: newlyCreatedUsers,
        byType: {
          clients: clientsCreated,
          caregivers: caregiversCreated,
          companyMembers: companyMembersCreated,
        },
      },
      updated: {
        count: totalUpdated,
        users: updatedUsers,
        byType: {
          clients: clientsUpdated,
          caregivers: caregiversUpdated,
          companyMembers: companyMembersUpdated,
        },
      },
      statusChanges: {
        count: statusChanges.length,
        changes: statusChanges,
      },
      countsByType: {
        clients: {
          total: totalClients,
          created: clientsCreated,
          updated: clientsUpdated,
        },
        caregivers: {
          total: totalCaregivers,
          created: caregiversCreated,
          updated: caregiversUpdated,
        },
        companyMembers: {
          total: totalCompanyMembers,
          created: companyMembersCreated,
          updated: companyMembersUpdated,
        },
      },
    };
  } catch (err) {
    logger.error("❌ Sync failed:", err.response?.data || err.message);
    throw err;
  }
}
