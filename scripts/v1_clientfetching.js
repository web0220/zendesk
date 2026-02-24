import { fetchClientDetail } from "../src/services/alayacare/alayacare.api.js";
import { mapClientUser } from "../src/services/alayacare/mapper.js";
import { extractMappedFields, buildStorageKeys } from "../src/domain/user.db.mapper.js";
import { logger } from "../src/config/logger.js";

/**
 * Same workflow as npm start (sync): fetch client detail → mapClientUser → toZendeskPayload.
 * Output includes what gets saved to user_mappings so you can compare with the DB table after sync.
 * Note: After npm start, Phase 2 (duplicate email/phone processing) may change identities
 * and shared_phone_number for some profiles in the DB.
 */
async function showZendeskPayload() {
  const clientId = Number(process.argv[2]) || 5877;

  try {
    logger.info(`🧪 Preparing Zendesk payloads for client ${clientId} (same workflow as sync: fetch → mapClientUser → payload)`);
    logger.info("");

    const client = await fetchClientDetail(clientId);
    
    logger.info("📥 Raw fetched data from AlayaCare:");
    logger.info(JSON.stringify(client, null, 2));
    logger.info("");
    
    const entities = mapClientUser(client);

    if (!entities || entities.length === 0) {
      logger.error("❌ mapClientUser returned no profiles");
      process.exit(1);
    }

    logger.info(`📊 Found ${entities.length} profile(s) created from this client:\n`);

    const summaryRows = [];

    entities.forEach((entity, index) => {
      const payload = entity.toZendeskPayload();
      // Same payload the sync passes to saveMappedUsersBatch (entity.toZendeskPayload())
      const fields = extractMappedFields(payload);
      const { acKey } = buildStorageKeys(payload, fields);
      
      logger.info(`═══════════════════════════════════════════════════════════`);
      logger.info(`Profile ${index + 1} of ${entities.length}`);
      logger.info(`═══════════════════════════════════════════════════════════`);
      logger.info(`External ID: ${payload.external_id || "N/A"}`);
      logger.info(`AC ID (storage key): ${acKey}`);
      logger.info(`Name: ${payload.name || "N/A"}`);
      logger.info(`Email: ${payload.email || "N/A"}`);
      logger.info(`Phone: ${payload.phone || "N/A"}`);
      logger.info(`Relationship: ${entity.relationship || "N/A"}`);
      logger.info(`Source Field: ${entity.sourceField || "N/A"} (not in payload → source_field in DB is null)`);
      logger.info(`Zendesk Primary: ${entity.zendeskPrimary ? "Yes" : "No"}`);
      logger.info(`Identities Count: ${payload.identities?.length || 0}`);
      
      if (payload.identities && payload.identities.length > 0) {
        logger.info(`Identities:`);
        payload.identities.forEach((identity, idx) => {
          logger.info(`  ${idx + 1}. ${identity.type}: ${identity.value}`);
        });
      }
      
      logger.info(`\n📦 Full Zendesk Payload (what sync sends to Zendesk):`);
      logger.info(JSON.stringify(payload, null, 2));

      logger.info(`\n📋 As stored in user_mappings (same columns as DB table after save):`);
      logger.info(JSON.stringify({
        external_id: payload.external_id,
        ac_id: acKey,
        name: fields.name,
        email: fields.email,
        phone: fields.phone,
        user_type: fields.user_type,
        identities: fields.identities,
        zendesk_primary: fields.zendesk_primary,
        shared_phone_number: payload.user_fields?.shared_phone_number ?? null,
        client_relationship: fields.client_relationship,
        source_field: fields.source_field,
        market: fields.market,
        coordinator_pod: fields.coordinator_pod,
        case_rating: fields.case_rating,
        client_status: fields.client_status,
        clinical_rn_manager: fields.clinical_rn_manager,
        sales_rep: fields.sales_rep,
      }, null, 2));
      logger.info("");

      summaryRows.push({
        external_id: payload.external_id,
        ac_id: acKey,
        email: payload.email || "(none)",
        relationship: (entity.relationship || "").slice(0, 40),
        zendesk_primary: entity.zendeskPrimary ? 1 : 0,
        identities_count: payload.identities?.length ?? 0,
        shared_phone_number: payload.user_fields?.shared_phone_number ?? null,
      });
    });

    logger.info(`📋 Summary (compare with user_mappings table for ac_id like 'client_${clientId}' or 'client_${clientId}_%'):`);
    logger.info("   external_id | ac_id | email | relationship | zendesk_primary | identities | shared_phone_number");
    summaryRows.forEach((r) => {
      logger.info(`   ${r.external_id} | ${r.ac_id} | ${r.email} | ${r.relationship} | ${r.zendesk_primary} | ${r.identities_count} | ${r.shared_phone_number ?? "null"}`);
    });
    logger.info("");
    logger.info(`✅ Ready to send ${entities.length} profile(s) to Zendesk`);
    logger.info("   (After npm start, Phase 2 may update identities/shared_phone_number for some rows.)");
  } catch (error) {
    logger.error("❌ Failed to build payload:", error.message);
    if (error.stack) {
      logger.error("Stack trace:", error.stack);
    }
    if (error.response) {
      logger.error("API Response Status:", error.response.status);
      logger.error(
        "API Response Data:",
        JSON.stringify(error.response.data, null, 2)
      );
    }
    process.exit(1);
  }
}

showZendeskPayload();