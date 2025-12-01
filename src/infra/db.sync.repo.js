import { logger } from "../config/logger.js";
import { extractMappedFields, buildStorageKeys, hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";
import { fetchClientDetail, fetchCaregiverDetail, fetchClientStatusOnly, fetchCaregiverStatusOnly } from "../services/alayacare/alayacare.api.js";
import { mapClientUser, mapCaregiverUser } from "../services/alayacare/mapper.js";

const ALVITA_COMPANY_ORG_ID = "40994316312731";

function isAlvitaCompanyMember(orgId) {
  if (orgId === null || orgId === undefined) return false;
  try {
    return String(orgId) === ALVITA_COMPANY_ORG_ID;
  } catch {
    return false;
  }
}

let insertMappedDataStmt;
let selectZendeskIdStmt;
let saveBatchTransaction;

function initializePreparedStatements() {
  const db = getDb();
  insertMappedDataStmt = db.prepare(`
    INSERT INTO user_mappings (
      ac_id, zendesk_user_id, external_id, name, email, phone, organization_id,
      user_type, source_ac_id, coordinator_pod, case_rating, client_status, clinical_rn_manager,
      sales_rep, caregiver_status, department, market, identities, zendesk_primary,
      shared_phone_number, current_active, last_synced_at, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT(ac_id) DO UPDATE SET
      -- ac_id is the PRIMARY KEY - NEVER update it (used to identify the row)
      -- Always update these fields from fresh API data (even for already-synced users)
      -- This ensures we capture any changes in AlayaCare (name, email, phone, etc.)
      external_id = excluded.external_id,
      name = excluded.name,
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
      caregiver_status = excluded.caregiver_status,
      department = excluded.department,
      market = excluded.market,
      identities = excluded.identities,
      zendesk_primary = excluded.zendesk_primary,
      -- shared_phone_number should only be set for new users (duplicate handling)
      -- For existing users, preserve it (it's set by duplicate processing)
      shared_phone_number = CASE WHEN zendesk_user_id IS NULL THEN excluded.shared_phone_number ELSE shared_phone_number END,
      -- Mark as active (found in current sync)
      current_active = 1,
      updated_at = CURRENT_TIMESTAMP
      -- Note: zendesk_user_id is NOT updated here - it's preserved for already-synced users
      -- Note: ac_id is NOT updated here - it's the primary key used to identify the row
  `);

  selectZendeskIdStmt = db.prepare("SELECT zendesk_user_id FROM user_mappings WHERE ac_id = ?");

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

  if (existing && existing.zendesk_user_id !== null) {
    logger.debug(
      `🔄 Updating mapped data for already-synced user ac_id=${acKey} (updating group/tag-based fields from fresh API data: coordinator_pod, case_rating, market, etc.)`
    );
    // Continue to update - the SQL will preserve zendesk_user_id but update fields extracted from groups/tags
  }

  insertMappedDataStmt.run(
    acKey,
    null,
    external_id,
    fields.name,
    fields.email,
    fields.phone,
    fields.organization_id,
    fields.user_type,
    sourceAcId,
    fields.coordinator_pod,
    fields.case_rating,
    fields.client_status,
    fields.clinical_rn_manager,
    fields.sales_rep,
    fields.caregiver_status,
    fields.department,
    fields.market,
    fields.identities,
    fields.zendesk_primary,
    null,
    null
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
 * Gets users from database by their ac_id list.
 * This is used to re-read users after status updates to get fresh data.
 * 
 * @param {Array<string>} acIds - Array of ac_id values to fetch
 * @returns {Array} Array of user records from database
 */
export function getUsersByAcIds(acIds) {
  if (!acIds || acIds.length === 0) {
    return [];
  }
  const db = getDb();
  const placeholders = acIds.map(() => "?").join(",");
  const stmt = db.prepare(
    `SELECT * FROM user_mappings WHERE ac_id IN (${placeholders})`
  );
  const users = stmt.all(...acIds).map(hydrateMapping);
  logger.debug(`📋 Fetched ${users.length} users by ac_id list`);
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
 * 
 * @returns {Array} Array of user records with current_active = 0 and zendesk_user_id IS NOT NULL
 */
export function getUsersWithStatusChange() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE current_active = 0 
      AND zendesk_user_id IS NOT NULL
    ORDER BY updated_at DESC
  `);
  const users = stmt.all().map(hydrateMapping);
  logger.info(
    `🔍 Found ${users.length} users with potential status change (current_active = 0, previously synced)`
  );
  return users;
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
    return false;
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
      return false;
    }

    // If status is null, user might be deleted (404) or status field is missing
    // We treat null as "deleted" for consistency
    if (rawStatus === null) {
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found or has no status in AlayaCare (likely deleted). Marking as deleted.`);
      const deletedStatus = formatStatusForStorage("deleted", userType);
      updateUserStatusInDatabase(acId, deletedStatus, userType);
      return true;
    }

    // Format status for storage
    const formattedStatus = formatStatusForStorage(rawStatus, userType);
    const currentStatus = userType === "client" ? user.client_status : user.caregiver_status;
    
    // Update only the status field in database (optimized for non-active users)
    updateUserStatusInDatabase(acId, formattedStatus, userType);
    
    if (currentStatus !== formattedStatus) {
      logger.info(
        `🔄 Updated ${userType} ${sourceAcId} (ac_id: ${acId}): status changed ${currentStatus || "null"} → ${formattedStatus || "null"}`
      );
    } else {
      logger.debug(
        `✅ Updated ${userType} ${sourceAcId} (ac_id: ${acId}): status unchanged (${formattedStatus || "null"})`
      );
    }
    return true;
  } catch (error) {
    // Handle API errors (network, timeout, etc.)
    if (error.response?.status === 404) {
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found (404). Marking as deleted.`);
      const deletedStatus = formatStatusForStorage("deleted", userType);
      updateUserStatusInDatabase(acId, deletedStatus, userType);
      return true;
    }

    logger.error(
      `❌ Failed to fetch status for ${userType} ${sourceAcId} (ac_id: ${acId}): ${error.message}`
    );
    if (error.response) {
      logger.error(`   API Response: ${error.response.status} - ${JSON.stringify(error.response.data)}`);
    }
    return false;
  }
}

/**
 * Updates user status in database
 * @param {string} acId - Primary key (ac_id)
 * @param {string|null} status - Formatted status value (e.g., 'cl_terminated')
 * @param {string} userType - 'client' or 'caregiver'
 */
function updateUserStatusInDatabase(acId, status, userType) {
  const db = getDb();

  if (userType === "client") {
    const stmt = db.prepare(
      "UPDATE user_mappings SET client_status = ?, updated_at = CURRENT_TIMESTAMP WHERE ac_id = ?"
    );
    stmt.run(status, acId);
    logger.debug(`   ✅ Updated client_status for ac_id=${acId}: ${status || "null"}`);
  } else if (userType === "caregiver") {
    const stmt = db.prepare(
      "UPDATE user_mappings SET caregiver_status = ?, updated_at = CURRENT_TIMESTAMP WHERE ac_id = ?"
    );
    stmt.run(status, acId);
    logger.debug(`   ✅ Updated caregiver_status for ac_id=${acId}: ${status || "null"}`);
  }
}

