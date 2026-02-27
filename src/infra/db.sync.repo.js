import { logger } from "../config/logger.js";
import { extractMappedFields, buildStorageKeys, hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";
import { fetchClientDetail, fetchCaregiverDetail, fetchClientStatusOnly, fetchCaregiverStatusOnly } from "../services/alayacare/alayacare.api.js";
import { mapClientUser, mapCaregiverUser } from "../services/alayacare/mapper.js";
import { getUserIdentities, deleteUserIdentity, getUser, updateUserCustomFields, makeIdentityPrimary } from "../services/zendesk/zendesk.api.js";
import { extractAllPhoneNumbers, isAliasedEmail } from "./db.duplicate.repo.js";
import { addIdentities } from "../services/zendesk/identitySync.js";
import { isAlvitaCompanyMember } from "../utils/constants.js";

let insertMappedDataStmt;
let selectZendeskIdStmt;
let saveBatchTransaction;

function initializePreparedStatements() {
  const db = getDb();
  insertMappedDataStmt = db.prepare(`
    INSERT INTO user_mappings (
      external_id, zendesk_user_id, ac_id, name, email, phone, organization_id,
      user_type, source_ac_id, coordinator_pod, case_rating, client_status, clinical_rn_manager,
      sales_rep, scheduling_preferences, caregiver_status, department, market, identities, zendesk_primary,
      shared_phone_number, client_relationship, source_field, current_active, last_synced_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(external_id) DO UPDATE SET
      -- external_id is the PRIMARY KEY - NEVER update it (used to identify the row)
      -- Always update these fields from fresh API data (even for already-synced users)
      -- This ensures we capture any changes in AlayaCare (name, email, phone, etc.)
      ac_id = excluded.ac_id,
      name = excluded.name,
      -- email: Always update from fresh API data
      -- Database is source of truth - always use fresh data from AlayaCare
      email = excluded.email,
      phone = excluded.phone,
      organization_id = excluded.organization_id,
      user_type = excluded.user_type,
      -- source_ac_id is the original AlayaCare ID used to fetch user details
      -- Preserve it for existing users (needed for fetchAndUpdateUserStatus)
      -- Only update if not already set (for new users)
      source_ac_id = CASE WHEN source_ac_id IS NULL OR source_ac_id = '' THEN excluded.source_ac_id ELSE source_ac_id END,
      -- Always update group/tag-based fields from fresh API data
      coordinator_pod = excluded.coordinator_pod,
      case_rating = excluded.case_rating,
      client_status = excluded.client_status,
      clinical_rn_manager = excluded.clinical_rn_manager,
      sales_rep = excluded.sales_rep,
      scheduling_preferences = excluded.scheduling_preferences,
      caregiver_status = excluded.caregiver_status,
      department = excluded.department,
      market = excluded.market,
      -- identities: always update from fresh API data (like phone, name, etc.)
      -- This ensures the database has the latest identities when reading to send to Zendesk
      identities = excluded.identities,
      zendesk_primary = excluded.zendesk_primary,
      -- shared_phone_number: from payload (non-main client profiles set it in normalizer; duplicate processing sets it for active users in Phase 2)
      shared_phone_number = excluded.shared_phone_number,
      client_relationship = excluded.client_relationship,
      source_field = excluded.source_field,
      -- Mark as active (found in current sync)
      current_active = 1,
      -- Reset non_active_status_fetched flag when user becomes active again
      -- This ensures we can detect if they become non-active again in the future
      non_active_status_fetched = NULL,
      updated_at = CURRENT_TIMESTAMP
      -- Note: zendesk_user_id is NOT updated here - it's preserved for already-synced users
      -- Note: external_id is NOT updated here - it's the primary key used to identify the row
  `);

  selectZendeskIdStmt = db.prepare("SELECT zendesk_user_id FROM user_mappings WHERE external_id = ?");

  saveBatchTransaction = db.transaction((batch) => {
    let changed = 0;
    for (const mappedData of batch) {
      if (saveMappedDataInternal(mappedData)) {
        changed += 1;
      }
    }
    return changed;
  });
}

function ensurePreparedStatements() {
  if (!insertMappedDataStmt || !selectZendeskIdStmt || !saveBatchTransaction) {
    initializePreparedStatements();
  }
}

function saveMappedDataInternal(mappedData) {
  if (!mappedData || !mappedData.ac_id || !mappedData.external_id) {
    logger.warn("⚠️ Skipping invalid mapped data (missing ac_id or external_id)");
    return false;
  }

  const fields = extractMappedFields(mappedData);
  const { acKey, sourceAcId } = buildStorageKeys(mappedData, fields);
  const external_id = mappedData.external_id;

  const existing = selectZendeskIdStmt.get(acKey);

  // if (existing && existing.zendesk_user_id !== null) {
  //   logger.debug(
  //     `🔄 Updating mapped data for already-synced user ac_id=${acKey} (updating group/tag-based fields from fresh API data: coordinator_pod, case_rating, market, etc.)`
  //   );
  //   // Continue to update - the SQL will preserve zendesk_user_id but update fields extracted from groups/tags
  // }

  insertMappedDataStmt.run(
    external_id,              // 1. external_id (PRIMARY KEY)
    null,                     // 2. zendesk_user_id
    acKey,                    // 3. ac_id
    fields.name,              // 4. name
    fields.email,             // 5. email
    fields.phone,             // 6. phone
    fields.organization_id,    // 7. organization_id
    fields.user_type,         // 8. user_type
    sourceAcId,               // 9. source_ac_id
    fields.coordinator_pod,   // 10. coordinator_pod
    fields.case_rating,       // 11. case_rating
    fields.client_status,     // 12. client_status
    fields.clinical_rn_manager, // 13. clinical_rn_manager
    fields.sales_rep,         // 14. sales_rep
    fields.scheduling_preferences, // 15. scheduling_preferences
    fields.caregiver_status,  // 16. caregiver_status
    fields.department,        // 17. department
    fields.market,            // 18. market
    fields.identities,        // 19. identities
    fields.zendesk_primary,   // 20. zendesk_primary
    fields.shared_phone_number, // 21. shared_phone_number
    fields.client_relationship, // 22. client_relationship
    fields.source_field,      // 23. source_field
    1,                        // 24. current_active (set to 1 for active users)
    null,                     // 25. last_synced_at (NULL for new inserts)
    // created_at and updated_at are CURRENT_TIMESTAMP in SQL
  );
  // logger.debug(
  //   `💾 Saved mapped data: ac_id=${acKey}, source_ac_id=${sourceAcId}, type=${fields.user_type}`
  // );
  return true;
}

export function saveMappedDataToDatabase(mappedData) {
  ensurePreparedStatements();
  return saveMappedDataInternal(mappedData);
}

export function saveMappedUsersBatch(mappedUsers = []) {
  if (!Array.isArray(mappedUsers) || mappedUsers.length === 0) {
    return 0;
  }
  ensurePreparedStatements();
  return saveBatchTransaction(mappedUsers);
}

export function hasUsersPendingSync() {
  const db = getDb();
  const row = db
    .prepare("SELECT COUNT(1) AS total FROM user_mappings WHERE zendesk_user_id IS NULL")
    .get();
  return (row?.total || 0) > 0;
}

export function getUsersPendingSync() {
  const db = getDb();
  const stmt = db.prepare(
    "SELECT * FROM user_mappings WHERE zendesk_user_id IS NULL ORDER BY created_at ASC"
  );
  const users = stmt.all().map(hydrateMapping);
  // logger.debug(
  //   `📋 Found ${users.length} users pending sync: ${users
  //     .map(
  //       (u) =>
  //         `ac_id=${u.ac_id}, source=${u.source_ac_id || "n/a"}, type=${u.user_type || "unknown"}`
  //     )
  //     .join(" | ")}`
  // );
  return users;
}

/**
 * Gets ALL users from database that should be synced to Zendesk.
 * This includes both new users (pending sync) and already synced users.
 * We send all users to ensure Zendesk is always in sync with our database.
 * 
 * @returns {Array} Array of all user records from database
 */
export function getAllUsersForSync() {
  const db = getDb();
  const stmt = db.prepare(
    "SELECT * FROM user_mappings ORDER BY created_at ASC"
  );
  const users = stmt.all().map(hydrateMapping);
  logger.info(`📋 Found ${users.length} total users in database to sync to Zendesk`);
  return users;
}

/**
 * Resets current_active flag to 0 for all users at the start of sync.
 * This prepares the database to track which users are active in the current sync run.
 */
export function resetCurrentActiveFlag() {
  const db = getDb();
  const stmt = db.prepare("UPDATE user_mappings SET current_active = 0 WHERE current_active = 1");
  const result = stmt.run();
  const changed = result.changes || 0;
  logger.info(`🔄 Reset current_active flag to 0 for ${changed} users (preparing for new sync)`);
  return changed;
}

/**
 * Finds users who were active in previous sync but are not in current active fetch.
 * These users may have changed status (active -> terminated/on hold/etc.)
 * Only returns users who haven't had their status fetched yet (non_active_status_fetched IS NULL or 0).
 * This optimization prevents re-fetching status for all non-active users on every sync.
 * 
 * @returns {Array} Array of user records with current_active = 0, zendesk_user_id IS NOT NULL, and non_active_status_fetched IS NULL
 */
export function getUsersWithStatusChange() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE current_active = 0 
      AND zendesk_user_id IS NOT NULL
      AND (non_active_status_fetched IS NULL OR non_active_status_fetched = 0)
    ORDER BY updated_at DESC
  `);
  const users = stmt.all().map(hydrateMapping);
  logger.info(
    `🔍 Found ${users.length} users with potential status change (current_active = 0, previously synced, status not yet fetched)`
  );
  return users;
}

/**
 * Finds all primary users who are currently non-active.
 * 
 * BUSINESS RULE: There should be NO zendesk_primary users in non-active users.
 * If a primary user is non-active, it's an alert that must be reported until fixed.
 * 
 * This function finds ALL primary users who are currently non-active, regardless of:
 * - User type (clients AND caregivers are both included)
 * - When they were processed (non_active_status_fetched flag)
 * - When they changed status
 * - How long they've been non-active
 * 
 * The daily alert script will keep reporting these users every day until:
 * - The user becomes active again (current_active = 1), OR
 * - The user loses the zendesk_primary tag (zendesk_primary = 0)
 * 
 * @returns {Array} Array of primary user records with current_active = 0, zendesk_primary = 1, and zendesk_user_id IS NOT NULL
 *                  Includes both clients and caregivers
 */
export function getPrimaryUsersDeactivated() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE current_active = 0 
      AND zendesk_primary = 1
      AND zendesk_user_id IS NOT NULL
    ORDER BY updated_at DESC
  `);
  const users = stmt.all().map(hydrateMapping);
  
  // Log breakdown by user type for visibility
  const clients = users.filter(u => u.user_type === 'client');
  const caregivers = users.filter(u => u.user_type === 'caregiver');
  const other = users.filter(u => u.user_type !== 'client' && u.user_type !== 'caregiver');
  
  logger.info(
    `🔍 Found ${users.length} primary user(s) who are currently non-active (violates business rule - must be reported until fixed)`
  );
  if (users.length > 0) {
    logger.info(
      `   📊 Breakdown: ${clients.length} client(s), ${caregivers.length} caregiver(s)${other.length > 0 ? `, ${other.length} other type(s)` : ''}`
    );
  }
  
  return users;
}

/**
 * Updates zendesk_primary to 0 for specified users after alert ticket is created.
 * This prevents these users from being included in future alert tickets.
 * 
 * @param {Array<string>} acIds - Array of ac_id values to update
 * @returns {number} Number of users updated
 */
export function clearZendeskPrimaryForUsers(acIds) {
  if (!acIds || acIds.length === 0) {
    return 0;
  }

  const db = getDb();
  const placeholders = acIds.map(() => '?').join(',');
  const stmt = db.prepare(`
    UPDATE user_mappings
    SET zendesk_primary = 0,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id IN (${placeholders})
      AND zendesk_primary = 1
  `);
  
  const result = stmt.run(...acIds);
  logger.info(
    `🔄 Updated zendesk_primary to 0 for ${result.changes} user(s) after alert ticket creation`
  );
  return result.changes;
}

/**
 * Maps client status from AlayaCare to Zendesk tag format
 * @param {string|null} status - Raw status from AlayaCare
 * @returns {string|null} Formatted status tag (e.g., 'cl_active', 'cl_on_hold')
 */
function mapClientStatus(status) {
  if (!status) return "cl_not_set";
  
  const normalizedStatus = status.trim().toLowerCase();
  
  // Map specific status values to Zendesk tags
  const statusMap = {
    "active": "cl_active",
    "onhold": "cl_on_hold",
    "on hold": "cl_on_hold",
    "discharged": "cl_discharged",
    "waiting list": "cl_waiting_list",
    "waitinglist": "cl_waiting_list",
    "not set": "cl_not_set",
    "notset": "cl_not_set",
  };
  
  // Check exact match first
  if (statusMap[normalizedStatus]) {
    return statusMap[normalizedStatus];
  }
  
  // Fallback: normalize spaces and convert to tag format
  return `cl_${normalizedStatus.replace(/\s+/g, "_")}`;
}

/**
 * Maps caregiver status from AlayaCare to Zendesk tag format
 * @param {string|null} status - Raw status from AlayaCare
 * @returns {string|null} Formatted status tag (e.g., 'cg_active', 'cg_suspended')
 */
function mapCaregiverStatus(status) {
  if (!status) return null;
  
  const normalizedStatus = status.trim().toLowerCase();
  
  // Map specific status values to Zendesk tags
  const statusMap = {
    "active": "cg_active",
    "suspended": "cg_suspended",
    "hold": "cg_hold",
    "terminated": "cg_terminated",
    "pending": "cg_pending",
    "applicant": "cg_applicant",
    "rejected": "cg_rejected",
  };
  
  // Check exact match first
  if (statusMap[normalizedStatus]) {
    return statusMap[normalizedStatus];
  }
  
  // Fallback: normalize spaces and convert to tag format
  return `cg_${normalizedStatus.replace(/\s+/g, "_")}`;
}

/**
 * Formats status value for storage in database
 * @param {string} status - Raw status from AlayaCare API
 * @param {string} userType - 'client' or 'caregiver'
 * @returns {string|null} Formatted status (e.g., 'cl_terminated' or 'cg_on_hold')
 */
function formatStatusForStorage(status, userType) {
  if (userType === "client") {
    return mapClientStatus(status);
  } else if (userType === "caregiver") {
    return mapCaregiverStatus(status);
  }
  return null;
}

/**
 * Fetches only the status field for non-active users from AlayaCare API.
 * This is optimized for users who are not in the current active sync (may be inactive/terminated).
 * Only updates the status field in the database, not all user information.
 * 
 * @param {Object} user - User record from database
 * @returns {Promise<boolean>} True if update was successful, false otherwise
 */
export async function fetchAndUpdateUserStatus(user) {
  if (!user || !user.source_ac_id || !user.user_type) {
    logger.warn(`⚠️ Cannot update user: missing source_ac_id or user_type for ac_id=${user?.ac_id}`);
    return { success: false, statusChanged: false, newStatus: null };
  }

  const sourceAcId = user.source_ac_id;
  const userType = user.user_type;
  const acId = user.ac_id;

  try {
    logger.debug(`📡 Fetching status only for ${userType} ${sourceAcId} (ac_id: ${acId})`);

    let rawStatus = null;
    if (userType === "client") {
      rawStatus = await fetchClientStatusOnly(Number(sourceAcId));
    } else if (userType === "caregiver") {
      rawStatus = await fetchCaregiverStatusOnly(Number(sourceAcId));
    } else {
      logger.warn(`⚠️ Unknown user_type "${userType}" for ac_id=${acId}`);
      return { success: false, statusChanged: false, newStatus: null };
    }

    // If status is null, user might be deleted (404) or status field is missing
    // We treat null as "deleted" for consistency
    if (rawStatus === null) {
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found or has no status in AlayaCare (likely deleted). Marking as deleted.`);
      const deletedStatus = formatStatusForStorage("deleted", userType);
      const currentStatus = userType === "client" ? user.client_status : user.caregiver_status;
      const statusChanged = currentStatus !== deletedStatus;
      updateUserStatusInDatabase(acId, deletedStatus, userType);
      return { success: true, statusChanged, newStatus: deletedStatus };
    }

    // Format status for storage
    const formattedStatus = formatStatusForStorage(rawStatus, userType);
    const currentStatus = userType === "client" ? user.client_status : user.caregiver_status;
    const statusChanged = currentStatus !== formattedStatus;
    
    // Update only the status field in database (optimized for non-active users)
    updateUserStatusInDatabase(acId, formattedStatus, userType);
    
    if (statusChanged) {
      logger.info(
        `🔄 Updated ${userType} ${sourceAcId} (ac_id: ${acId}): status changed ${currentStatus || "null"} → ${formattedStatus || "null"}`
      );
    } else {
      logger.debug(
        `✅ Updated ${userType} ${sourceAcId} (ac_id: ${acId}): status unchanged (${formattedStatus || "null"})`
      );
    }
    return { success: true, statusChanged, newStatus: formattedStatus };
  } catch (error) {
    // Handle API errors (network, timeout, etc.)
    if (error.response?.status === 404) {
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found (404). Marking as deleted.`);
      const deletedStatus = formatStatusForStorage("deleted", userType);
      const currentStatus = userType === "client" ? user.client_status : user.caregiver_status;
      const statusChanged = currentStatus !== deletedStatus;
      updateUserStatusInDatabase(acId, deletedStatus, userType);
      return { success: true, statusChanged, newStatus: deletedStatus };
    }

    logger.error(
      `❌ Failed to fetch status for ${userType} ${sourceAcId} (ac_id: ${acId}): ${error.message}`
    );
    if (error.response) {
      logger.error(`   API Response: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    }
    return { success: false, statusChanged: false, newStatus: null };
  }
}

/**
 * Updates user status in database and marks that status has been fetched for non-active users.
 * @param {string} acId - Primary key (ac_id)
 * @param {string|null} status - Formatted status value (e.g., 'cl_terminated')
 * @param {string} userType - 'client' or 'caregiver'
 */
function updateUserStatusInDatabase(acId, status, userType) {
  const db = getDb();

  if (userType === "client") {
    const stmt = db.prepare(
      `UPDATE user_mappings 
       SET client_status = ?, 
           non_active_status_fetched = 1,
           updated_at = CURRENT_TIMESTAMP 
       WHERE ac_id = ?`
    );
    stmt.run(status, acId);
    logger.debug(`   ✅ Updated client_status for ac_id=${acId}: ${status || "null"} (marked as fetched)`);
  } else if (userType === "caregiver") {
    const stmt = db.prepare(
      `UPDATE user_mappings 
       SET caregiver_status = ?, 
           non_active_status_fetched = 1,
           updated_at = CURRENT_TIMESTAMP 
       WHERE ac_id = ?`
    );
    stmt.run(status, acId);
    logger.debug(`   ✅ Updated caregiver_status for ac_id=${acId}: ${status || "null"} (marked as fetched)`);
  }
}

/**
 * Phase 3: Process non-active users - alias all emails, move all phones to shared_phone_number
 * 
 * Simplified logic:
 * 1. Fetch and map user data from AlayaCare
 * 2. Alias ALL emails (email field + identities) with email+external_id@domain
 * 3. Move ALL phones (phone field + identities) to shared_phone_number
 * 4. Delete ALL email and phone_number identities from Zendesk
 * 5. Add aliased emails as identities to Zendesk
 * 6. Update Zendesk user with aliased email and shared_phone_number field
 * 
 * @param {Object} user - Non-active user from database
 * @returns {Promise<Object>} Result with success status and updated user data
 */
export async function processNonActiveUser(user) {
  if (!user || !user.source_ac_id || !user.user_type || !user.zendesk_user_id) {
    logger.warn(`⚠️ Cannot process non-active user: missing required fields for ac_id=${user?.ac_id}`);
    return { success: false, user: null };
  }

  const sourceAcId = user.source_ac_id;
  const userType = user.user_type;
  const acId = user.ac_id;
  const zendeskUserId = user.zendesk_user_id;

  try {
    logger.info(`📡 Phase 3: Processing non-active ${userType} ${sourceAcId} (ac_id: ${acId})`);

    // Fetch full user data from AlayaCare
    let rawUserData = null;
    if (userType === "client") {
      rawUserData = await fetchClientDetail(Number(sourceAcId));
    } else if (userType === "caregiver") {
      rawUserData = await fetchCaregiverDetail(Number(sourceAcId));
    } else {
      logger.warn(`⚠️ Unknown user_type "${userType}" for ac_id=${acId}`);
      return { success: false, user: null };
    }

    if (!rawUserData) {
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found in AlayaCare (likely deleted)`);
      return { success: false, user: null };
    }

    // Map like Phase 1
    let entity = null;
    if (userType === "client") {
      entity = mapClientUser(rawUserData);
    } else if (userType === "caregiver") {
      entity = mapCaregiverUser(rawUserData);
    }

    if (!entity) {
      logger.warn(`⚠️ Failed to map ${userType} ${sourceAcId} to UserEntity`);
      return { success: false, user: null };
    }

    const mappedPayload = entity.toZendeskPayload();
    const fields = extractMappedFields(mappedPayload);
    const externalId = mappedPayload.external_id;

    // Step 1: Alias ALL emails (email field + identities)
    // Track original emails before aliasing (needed to check against Zendesk primary email)
    const originalEmails = new Set();
    const aliasedEmails = [];
    
    // Alias email field
    if (fields.email && !isAliasedEmail(fields.email)) {
      originalEmails.add(fields.email.toLowerCase());
      const emailParts = fields.email.split("@");
      if (emailParts.length === 2) {
        fields.email = `${emailParts[0]}+${externalId}@${emailParts[1]}`;
        aliasedEmails.push(fields.email);
        logger.info(`   🔄 Aliased email field: ${emailParts[0]}@${emailParts[1]} → ${fields.email}`);
      }
    } else if (fields.email) {
      aliasedEmails.push(fields.email); // Already aliased
    }

    // Alias email identities
    let identities = fields.identities;
    if (typeof identities === "string") {
      try {
        identities = JSON.parse(identities);
      } catch {
        identities = [];
      }
    }
    if (!Array.isArray(identities)) {
      identities = [];
    }

    const aliasedEmailIdentities = [];
    const nonEmailIdentities = [];

    for (const identity of identities) {
      if (identity.type === "email" && identity.value) {
        if (!isAliasedEmail(identity.value)) {
          originalEmails.add(identity.value.toLowerCase());
          const emailParts = identity.value.split("@");
          if (emailParts.length === 2) {
            const aliasedEmail = `${emailParts[0]}+${externalId}@${emailParts[1]}`;
            aliasedEmailIdentities.push({ type: "email", value: aliasedEmail });
            aliasedEmails.push(aliasedEmail);
            logger.info(`   🔄 Aliased email identity: ${identity.value} → ${aliasedEmail}`);
          }
        } else {
          aliasedEmailIdentities.push(identity); // Already aliased
          aliasedEmails.push(identity.value);
        }
      } else if (identity.type !== "phone" && identity.type !== "phone_number") {
        // Keep non-email, non-phone identities
        nonEmailIdentities.push(identity);
      }
      // Skip phone identities - they will be moved to shared_phone_number
    }

    // Step 2: Move ALL phones to shared_phone_number
    const allPhones = extractAllPhoneNumbers({ phone: fields.phone, identities: identities });
    const sharedPhoneNumberStr = allPhones.length > 0 ? allPhones.join("\n") : null;
    
    // Update fields: phone = NULL, identities = only non-email, non-phone identities + aliased emails
    fields.phone = null;
    fields.identities = [...nonEmailIdentities, ...aliasedEmailIdentities];
    fields.shared_phone_number = sharedPhoneNumberStr;

    if (allPhones.length > 0) {
      logger.info(`   📞 Moved ${allPhones.length} phone(s) to shared_phone_number: ${allPhones.join(", ")}`);
    }

    // Step 3: Delete all email and phone identities from Zendesk
    // First, get current identities to delete
    logger.info(`   🔄 Getting current identities from Zendesk for user ${zendeskUserId}...`);
    const currentZendeskIdentities = await getUserIdentities(zendeskUserId);
    
    const identitiesToDelete = currentZendeskIdentities.filter(
      (identity) => identity.type === "email" || identity.type === "phone_number"
    );

    if (identitiesToDelete.length > 0) {
      // logger.info(`   🗑️  Deleting ${identitiesToDelete.length} email/phone identity(ies) from Zendesk`);
      for (const identity of identitiesToDelete) {
        try {
          await deleteUserIdentity(zendeskUserId, identity.id);
          // logger.info(`   ✅ Deleted ${identity.type} identity ${identity.id} (${identity.value})`);
        } catch (error) {
          logger.error(`   ❌ Failed to delete identity ${identity.id}: ${error.message}`);
        }
      }
    }

    // Step 4: Add aliased emails as identities to Zendesk
    if (aliasedEmails.length > 0) {
      logger.info(`   ➕ Adding ${aliasedEmails.length} aliased email(s) as identities to Zendesk`);
      const emailIdentitiesToAdd = aliasedEmails.map(email => ({ type: "email", value: email }));
      await addIdentities(zendeskUserId, emailIdentitiesToAdd);
    }

    // Step 5: Get current Zendesk user and identities, then update primary email if needed
    logger.info(`   🔄 Getting current user and identities from Zendesk for user ${zendeskUserId}...`);
    const zendeskUser = await getUser(zendeskUserId);
    const zendeskIdentities = await getUserIdentities(zendeskUserId);

    if (zendeskUser && zendeskUser.email) {
      const zendeskPrimaryEmail = zendeskUser.email.toLowerCase();
      
      // Check if primary email is already aliased
      const isPrimaryEmailAliased = isAliasedEmail(zendeskUser.email);
      
      if (isPrimaryEmailAliased) {
        logger.info(`   ✅ Primary email ${zendeskUser.email} is already aliased, no update needed`);
      } else {
        // Primary email is not aliased (e.g., abc@gmail.com)
        // We need to make an aliased email primary (e.g., abc+client_0000@gmail.com)
        logger.info(`   🔍 Primary email ${zendeskUser.email} is not aliased, finding aliased email identity to make primary...`);
        
        // Helper function to extract unaliased email from +externalId pattern
        const extractUnaliasedFromExternalId = (aliasedEmail, externalId) => {
          if (!aliasedEmail || !aliasedEmail.includes('+') || !aliasedEmail.includes('@')) {
            return aliasedEmail;
          }
          const [localPart, domain] = aliasedEmail.split('@');
          // Remove +externalId from local part
          const unaliasedLocal = localPart.replace(`+${externalId}`, '');
          return `${unaliasedLocal}@${domain}`;
        };

        // Find aliased email identity whose unaliased form matches the primary email
        // This finds abc+client_0000@gmail.com when primary is abc@gmail.com
        const matchingAliasedIdentity = zendeskIdentities.find((identity) => {
          if (identity.type !== "email" || !identity.value) {
            return false;
          }
          // Check if this identity is aliased
          if (!isAliasedEmail(identity.value)) {
            return false;
          }
          // Check if unaliased form matches primary email
          const unaliased = extractUnaliasedFromExternalId(identity.value.toLowerCase(), externalId);
          return unaliased === zendeskPrimaryEmail;
        });

        if (matchingAliasedIdentity) {
          logger.info(`   🔄 Found aliased identity ${matchingAliasedIdentity.id} (${matchingAliasedIdentity.value}), making it primary...`);
          try {
            // Store the old primary email before making new one primary
            const oldPrimaryEmail = zendeskUser.email.toLowerCase();
            
            await makeIdentityPrimary(zendeskUserId, matchingAliasedIdentity.id);
            logger.info(`   ✅ Made aliased identity ${matchingAliasedIdentity.id} (${matchingAliasedIdentity.value}) primary`);
            
            // After making a new identity primary, the old primary email (abc@gmail.com) becomes a secondary identity
            // We need to delete it since it's a non-aliased email
            logger.info(`   🔄 Old primary email ${oldPrimaryEmail} became a secondary identity, fetching updated identities to delete it...`);
            
            // Fetch updated identities to find the old primary email as a secondary identity
            const updatedIdentities = await getUserIdentities(zendeskUserId);
            const oldPrimaryIdentity = updatedIdentities.find(
              (identity) => identity.type === "email" && identity.value?.toLowerCase() === oldPrimaryEmail
            );
            
            if (oldPrimaryIdentity) {
              logger.info(`   🗑️  Deleting old primary email identity ${oldPrimaryIdentity.id} (${oldPrimaryIdentity.value}) - it's a non-aliased email`);
              try {
                await deleteUserIdentity(zendeskUserId, oldPrimaryIdentity.id);
                logger.info(`   ✅ Deleted old primary email identity ${oldPrimaryIdentity.id}`);
              } catch (error) {
                logger.error(`   ❌ Failed to delete old primary email identity ${oldPrimaryIdentity.id}: ${error.message}`);
              }
            } else {
              logger.warn(`   ⚠️  Old primary email ${oldPrimaryEmail} not found in identities after making aliased email primary`);
            }
          } catch (error) {
            logger.error(`   ❌ Failed to make identity ${matchingAliasedIdentity.id} primary: ${error.message}`);
          }
        } else {
          logger.warn(`   ⚠️  Could not find an aliased email identity whose unaliased form matches primary email ${zendeskUser.email}`);
          logger.warn(`   ⚠️  Available email identities: ${zendeskIdentities.filter(i => i.type === 'email').map(i => i.value).join(', ')}`);
        }
      }
    }

    // Step 6: Update database
    const db = getDb();
    const { acKey } = buildStorageKeys(mappedPayload, fields);
    
    const updateStmt = db.prepare(`
      UPDATE user_mappings
      SET email = ?,
          phone = ?,
          coordinator_pod = ?,
          case_rating = ?,
          client_status = ?,
          clinical_rn_manager = ?,
          sales_rep = ?,
          caregiver_status = ?,
          department = ?,
          market = ?,
          identities = ?,
          zendesk_primary = ?,
          shared_phone_number = ?,
          non_active_status_fetched = 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE ac_id = ?
    `);

    updateStmt.run(
      fields.email,
      fields.phone,
      fields.coordinator_pod,
      fields.case_rating,
      fields.client_status,
      fields.clinical_rn_manager,
      fields.sales_rep,
      fields.caregiver_status,
      fields.department,
      fields.market,
      JSON.stringify(fields.identities),
      fields.zendesk_primary ? 1 : 0,
      fields.shared_phone_number,
      acKey
    );

    logger.info(`   ✅ Updated non-active user ${acId} in database`);

    // Step 7: Update Zendesk user with shared_phone_number field
    // Note: Email field in user object is read-only, so we update via custom field if needed
    // But we already added aliased emails as identities above
    // Update shared_phone_number custom field
    // Reuse zendeskUser from Step 5 (we already fetched it earlier)
    if (zendeskUser) {
      await updateUserCustomFields(zendeskUserId, {
        ...zendeskUser.user_fields,
        shared_phone_number: fields.shared_phone_number,
      });
      logger.info(`   ✅ Updated shared_phone_number field in Zendesk for user ${zendeskUserId}`);
    }

    // Reload user from database
    const updatedUser = db
      .prepare("SELECT * FROM user_mappings WHERE ac_id = ?")
      .get(acKey);
    const hydratedUser = hydrateMapping(updatedUser);

    return { success: true, user: hydratedUser };
  } catch (error) {
    logger.error(
      `❌ Failed to process non-active user ${userType} ${sourceAcId} (ac_id: ${acId}): ${error.message}`
    );
    if (error.response) {
      logger.error(`   API Response: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    }
    return { success: false, user: null };
  }
}

