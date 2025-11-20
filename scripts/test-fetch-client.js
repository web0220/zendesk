import { fetchClientDetail } from "../src/modules/alayacare/service.js";
import { mapClientToZendesk } from "../src/modules/alayacare/mapper.js";
import { logger } from "../src/config/logger.js";

async function showZendeskPayload() {
  const clientId = Number(process.argv[2]) || 5877;

  try {
    logger.info(`🧪 Preparing Zendesk payload for client ${clientId}`);

    const client = await fetchClientDetail(clientId);
    
    logger.info("📥 Raw fetched data from AlayaCare:");
    logger.info(JSON.stringify(client, null, 2));
    logger.info("");
    
    const mapped = mapClientToZendesk(client);

    if (!mapped) {
      logger.error("❌ mapClientToZendesk returned null");
      process.exit(1);
    }

    logger.info("📦 Zendesk request body:");
    logger.info(JSON.stringify(mapped, null, 2));
    logger.info("\n✅ Ready to send");
  } catch (error) {
    logger.error("❌ Failed to build payload:", error.message);
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