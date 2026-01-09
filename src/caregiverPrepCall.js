import { config } from "./config/index.js";
import { logger } from "./config/logger.js";
import { runCaregiverPrepCallTickets } from "./core/caregiverPrepCallOrchestrator.js";
import { initDatabase, closeDatabase } from "./infra/database.js";

async function bootstrap() {
  // Initialize database
  initDatabase();

  try {
    logger.info("🎯 Running caregiver prep call ticket job");

    const result = await runCaregiverPrepCallTickets();
    logger.info("✅ Caregiver prep call ticket job completed successfully");
    logger.info("Summary:", JSON.stringify(result, null, 2));
  } catch (err) {
    logger.error("❌ Caregiver prep call ticket job failed:", err);
    process.exit(1);
  } finally {
    // Close database connection
    closeDatabase();
    // Close log file stream
    logger.close();
  }
}

bootstrap().catch((err) => {
  logger.error("Startup error:", err);
  closeDatabase();
  logger.close();
  process.exit(1);
});


