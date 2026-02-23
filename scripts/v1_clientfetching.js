import { fetchClientDetail } from "../src/services/alayacare/alayacare.api.js";
import { mapClientUser } from "../src/services/alayacare/mapper.js";
import { logger } from "../src/config/logger.js";

async function showZendeskPayload() {
  const clientId = Number(process.argv[2]) || 5877;

  try {
    logger.info(`🧪 Preparing Zendesk payloads for client ${clientId}`);

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

    entities.forEach((entity, index) => {
      const payload = entity.toZendeskPayload();
      
      logger.info(`═══════════════════════════════════════════════════════════`);
      logger.info(`Profile ${index + 1} of ${entities.length}`);
      logger.info(`═══════════════════════════════════════════════════════════`);
      logger.info(`External ID: ${payload.external_id || "N/A"}`);
      logger.info(`Name: ${payload.name || "N/A"}`);
      logger.info(`Email: ${payload.email || "N/A"}`);
      logger.info(`Phone: ${payload.phone || "N/A"}`);
      logger.info(`Relationship: ${entity.relationship || "N/A"}`);
      logger.info(`Source Field: ${entity.sourceField || "N/A"}`);
      logger.info(`AC ID: ${entity.acId || "N/A"}`);
      logger.info(`Zendesk Primary: ${entity.zendeskPrimary ? "Yes" : "No"}`);
      logger.info(`Identities Count: ${payload.identities?.length || 0}`);
      
      if (payload.identities && payload.identities.length > 0) {
        logger.info(`Identities:`);
        payload.identities.forEach((identity, idx) => {
          logger.info(`  ${idx + 1}. ${identity.type}: ${identity.value}`);
        });
      }
      
      logger.info(`\n📦 Full Zendesk Payload:`);
      logger.info(JSON.stringify(payload, null, 2));
      logger.info("");
    });

    logger.info(`✅ Ready to send ${entities.length} profile(s) to Zendesk`);
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