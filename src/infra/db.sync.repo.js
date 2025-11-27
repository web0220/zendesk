import { logger } from "../config/logger.js";
import { extractMappedFields, buildStorageKeys, hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";

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
      shared_phone_number, last_synced_at, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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

