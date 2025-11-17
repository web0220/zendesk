import { logger } from "../src/config/logger.js";
import { 
  initDatabase, 
  closeDatabase, 
  getAllUserMappings,
  getUserMappingByAcId,
  getUserMappingByZendeskId 
} from "../src/infrastructure/database.js";

/**
 * Inspect the SQLite database
 * - Shows all mappings
 * - Allows querying by ac_id or zendesk_user_id
 */
async function inspectDatabase() {
  logger.info("🔍 Database Inspector\n");
  logger.info("=".repeat(70));

  try {
    // Initialize database
    initDatabase();

    // Get all mappings
    const allMappings = getAllUserMappings();
    
    logger.info(`\n📊 Total mappings: ${allMappings.length}\n`);

    if (allMappings.length === 0) {
      logger.info("⚠️  Database is empty. Run a sync first.\n");
      return;
    }

    // Display all mappings
    logger.info("📋 All Mappings:");
    logger.info("-".repeat(70));
    
    allMappings.forEach((mapping, index) => {
      logger.info(`\n${index + 1}. Mapping:`);
      logger.info(`   ac_id:           ${mapping.ac_id}`);
      logger.info(`   zendesk_user_id: ${mapping.zendesk_user_id}`);
      logger.info(`   external_id:     ${mapping.external_id}`);
      logger.info(`   last_synced_at:  ${mapping.last_synced_at}`);
      logger.info(`   created_at:      ${mapping.created_at}`);
      logger.info(`   updated_at:      ${mapping.updated_at}`);
    });

    // Example queries
    if (allMappings.length > 0) {
      const firstMapping = allMappings[0];
      
      logger.info("\n" + "=".repeat(70));
      logger.info("🔎 Example Queries:");
      logger.info("-".repeat(70));

      // Query by ac_id
      logger.info(`\n1. Query by ac_id: "${firstMapping.ac_id}"`);
      const byAcId = getUserMappingByAcId(firstMapping.ac_id);
      if (byAcId) {
        logger.info(`   ✅ Found: zendesk_user_id = ${byAcId.zendesk_user_id}`);
      }

      // Query by zendesk_user_id
      logger.info(`\n2. Query by zendesk_user_id: ${firstMapping.zendesk_user_id}`);
      const byZendeskId = getUserMappingByZendeskId(firstMapping.zendesk_user_id);
      if (byZendeskId) {
        logger.info(`   ✅ Found: ac_id = ${byZendeskId.ac_id}`);
      }
    }

    // Statistics
    logger.info("\n" + "=".repeat(70));
    logger.info("📈 Statistics:");
    logger.info("-".repeat(70));
    
    const now = new Date();
    const last24h = allMappings.filter(m => {
      const syncDate = new Date(m.last_synced_at);
      return (now - syncDate) < 24 * 60 * 60 * 1000;
    });

    const lastHour = allMappings.filter(m => {
      const syncDate = new Date(m.last_synced_at);
      return (now - syncDate) < 60 * 60 * 1000;
    });

    logger.info(`   Total mappings:        ${allMappings.length}`);
    logger.info(`   Synced in last hour:   ${lastHour.length}`);
    logger.info(`   Synced in last 24h:    ${last24h.length}`);

    if (allMappings.length > 0) {
      const oldestSync = allMappings[allMappings.length - 1].last_synced_at;
      const newestSync = allMappings[0].last_synced_at;
      logger.info(`   Oldest sync:           ${oldestSync}`);
      logger.info(`   Newest sync:           ${newestSync}`);
    }

    logger.info("\n" + "=".repeat(70));
    logger.info("✅ Database inspection complete!");
    logger.info("=".repeat(70) + "\n");

  } catch (error) {
    logger.error("❌ Inspection failed:", error.message);
    logger.error("Stack trace:", error.stack);
    process.exit(1);
  } finally {
    closeDatabase();
  }
}

inspectDatabase();

