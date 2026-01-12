import { logger } from "../config/logger.js";
import {
  extractMappedFields,
  hydrateMapping,
  normalizeAcLookupKey,
} from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";

export function upsertUserMapping(mapping) {
  const db = getDb();
  const { ac_id, zendesk_user_id, external_id, last_synced_at, mapped_data } = mapping;
  const fields = extractMappedFields(mapped_data);

  const stmt = db.prepare(`
    INSERT INTO user_mappings (
      ac_id, zendesk_user_id, external_id, name, email, phone, organization_id,
      user_type, coordinator_pod, case_rating, client_status, clinical_rn_manager,
      sales_rep, scheduling_preferences, caregiver_status, department, market, identities, zendesk_primary,
      shared_phone_number, last_synced_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(ac_id) DO UPDATE SET
      zendesk_user_id = excluded.zendesk_user_id,
      external_id = excluded.external_id,
      name = excluded.name,
      email = excluded.email,
      phone = excluded.phone,
      organization_id = excluded.organization_id,
      user_type = excluded.user_type,
      coordinator_pod = excluded.coordinator_pod,
      case_rating = excluded.case_rating,
      client_status = excluded.client_status,
      clinical_rn_manager = excluded.clinical_rn_manager,
      sales_rep = excluded.sales_rep,
      scheduling_preferences = excluded.scheduling_preferences,
      caregiver_status = excluded.caregiver_status,
      department = excluded.department,
      market = excluded.market,
      identities = excluded.identities,
      zendesk_primary = excluded.zendesk_primary,
      shared_phone_number = excluded.shared_phone_number,
      last_synced_at = excluded.last_synced_at,
      updated_at = CURRENT_TIMESTAMP
  `);

  stmt.run(
    ac_id,
    zendesk_user_id,
    external_id,
    fields.name,
    fields.email,
    fields.phone,
    fields.organization_id,
    fields.user_type,
    fields.coordinator_pod,
    fields.case_rating,
    fields.client_status,
    fields.clinical_rn_manager,
    fields.sales_rep,
    fields.scheduling_preferences,
    fields.caregiver_status,
    fields.department,
    fields.market,
    fields.identities,
    fields.zendesk_primary,
    null,
    last_synced_at
  );
  logger.debug(
    `💾 Stored mapping: ac_id=${ac_id}, zendesk_user_id=${zendesk_user_id}, type=${fields.user_type}`
  );
}

export function getUserMappingByAcId(ac_id, userType) {
  const db = getDb();
  const lookupKey = normalizeAcLookupKey(ac_id, userType);
  const stmt = db.prepare("SELECT * FROM user_mappings WHERE ac_id = ? OR source_ac_id = ?");
  const row = stmt.get(lookupKey || "__ac_lookup__", String(ac_id));
  return hydrateMapping(row) || null;
}

export function getUserMappingByZendeskId(zendesk_user_id) {
  const db = getDb();
  const stmt = db.prepare("SELECT * FROM user_mappings WHERE zendesk_user_id = ?");
  const row = stmt.get(zendesk_user_id);
  return hydrateMapping(row) || null;
}

export function getAllUserMappings() {
  const db = getDb();
  const stmt = db.prepare("SELECT * FROM user_mappings ORDER BY updated_at DESC");
  return stmt.all().map(hydrateMapping);
}

/**
 * Updates zendesk_user_id and last_synced_at in the database after syncing to Zendesk.
 * 
 * Note: zendesk_user_id is set on first sync (when NULL), then preserved for subsequent syncs.
 * Once set, it never changes - we use it to update the user profile in Zendesk.
 * 
 * @param {string} ac_id - AlayaCare ID
 * @param {number} zendesk_user_id - Zendesk user ID (from Zendesk API response)
 * @param {string} last_synced_at - ISO timestamp of when sync completed
 * @param {string} userType - 'client' or 'caregiver'
 */
export function updateZendeskUserId(ac_id, zendesk_user_id, last_synced_at, userType) {
  const db = getDb();
  const lookupKey = normalizeAcLookupKey(ac_id, userType);
  
  const sql = `
    UPDATE user_mappings
    SET zendesk_user_id = ?,
        last_synced_at = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
       OR (source_ac_id = ? AND (user_type = ? OR (user_type IS NULL AND ? IS NULL)))
  `;

  const stmt = db.prepare(sql);
  const params = [
    zendesk_user_id,
    last_synced_at,
    lookupKey || "__ac_lookup__",
    String(ac_id),
    userType || null,
    userType || null
  ];

  const result = stmt.run(...params);
  if (result.changes === 0) {
    logger.warn(
      `⚠️  Could not update zendesk_user_id for ac_id=${ac_id} (lookupKey=${lookupKey}). Record not found.`
    );
  } else {
    // logger.debug(
    //   `🔄 Updated zendesk_user_id: ac_id=${ac_id} (lookup=${lookupKey}) → zendesk_user_id=${zendesk_user_id}`
    // );
  }
}

