import { logger } from "../config/logger.js";
import { extractMappedFields, buildStorageKeys, hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";
import { fetchClientDetail, fetchCaregiverDetail } from "../services/alayacare/alayacare.api.js";

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
      external_id = CASE WHEN zendesk_user_id IS NULL THEN excluded.external_id ELSE external_id END,
      name = CASE WHEN zendesk_user_id IS NULL THEN excluded.name ELSE name END,
      email = CASE WHEN zendesk_user_id IS NULL THEN excluded.email ELSE email END,
      phone = CASE WHEN zendesk_user_id IS NULL THEN excluded.phone ELSE phone END,
      organization_id = CASE WHEN zendesk_user_id IS NULL THEN excluded.organization_id ELSE organization_id END,
      user_type = CASE WHEN zendesk_user_id IS NULL THEN excluded.user_type ELSE user_type END,
      source_ac_id = CASE WHEN zendesk_user_id IS NULL THEN excluded.source_ac_id ELSE source_ac_id END,
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
      shared_phone_number = CASE WHEN zendesk_user_id IS NULL THEN excluded.shared_phone_number ELSE shared_phone_number END,
      current_active = 1,
      updated_at = CURRENT_TIMESTAMP
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
  logger.debug(
    `💾 Saved mapped data: ac_id=${acKey}, source_ac_id=${sourceAcId}, type=${fields.user_type}`
  );
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
  logger.debug(
    `📋 Found ${users.length} users pending sync: ${users
      .map(
        (u) =>
          `ac_id=${u.ac_id}, source=${u.source_ac_id || "n/a"}, type=${u.user_type || "unknown"}`
      )
      .join(" | ")}`
  );
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
 * Fetches current status from AlayaCare API and updates the user record in database.
 * Handles errors gracefully (404 for deleted users, network errors with retry).
 * 
 * @param {Object} user - User record from database
 * @returns {Promise<boolean>} True if update was successful, false otherwise
 */
export async function fetchAndUpdateUserStatus(user) {
  if (!user || !user.source_ac_id || !user.user_type) {
    logger.warn(`⚠️ Cannot update status: missing source_ac_id or user_type for ac_id=${user?.ac_id}`);
    return false;
  }

  const sourceAcId = user.source_ac_id;
  const userType = user.user_type;
  const acId = user.ac_id;

  try {
    logger.debug(`📡 Fetching current status for ${userType} ${sourceAcId} (ac_id: ${acId})`);

    let fetchedData = null;
    if (userType === "client") {
      fetchedData = await fetchClientDetail(Number(sourceAcId));
    } else if (userType === "caregiver") {
      fetchedData = await fetchCaregiverDetail(Number(sourceAcId));
    } else {
      logger.warn(`⚠️ Unknown user_type "${userType}" for ac_id=${acId}`);
      return false;
    }

    if (!fetchedData) {
      // User not found (404) - mark as deleted
      logger.warn(`⚠️ User ${userType} ${sourceAcId} not found in AlayaCare (likely deleted). Marking as deleted.`);
      const deletedStatus = formatStatusForStorage("deleted", userType);
      updateUserStatusInDatabase(acId, deletedStatus, userType);
      return true;
    }

    // Extract status from fetched data
    const rawStatus = fetchedData.status || null;
    const formattedStatus = rawStatus ? formatStatusForStorage(rawStatus, userType) : null;

    // Check if status actually changed
    const currentStatus = userType === "client" ? user.client_status : user.caregiver_status;
    if (currentStatus === formattedStatus) {
      logger.debug(`   ℹ️  Status unchanged for ${userType} ${sourceAcId}: ${formattedStatus || "null"}`);
      // Still update the record to mark it as checked, but no status change
      updateUserStatusInDatabase(acId, formattedStatus, userType);
      return true;
    }

    logger.info(
      `🔄 Status changed for ${userType} ${sourceAcId}: ${currentStatus || "null"} → ${formattedStatus || "null"}`
    );

    // Update database with new status
    updateUserStatusInDatabase(acId, formattedStatus, userType);

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

