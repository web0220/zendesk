import { config } from "../config/index.js";
import { logger } from "../config/logger.js";
import { runCaregiverPrepCallTickets } from "../core/caregiverPrepCallOrchestrator.js";
import { initDatabase, closeDatabase } from "../infra/database.js";
import { bootstrap } from "./bootstrap.js";

async function main() {
  await bootstrap(async () => {
    logger.info("🎯 Running caregiver prep call ticket job");

    const result = await runCaregiverPrepCallTickets();
    logger.info("✅ Caregiver prep call ticket job completed successfully");
    logger.info("Summary:", JSON.stringify(result, null, 2));
    return result;
  });
}

main().catch((err) => {
  logger.error("Startup error:", err);
  closeDatabase();
  logger.close();
  process.exit(1);
});

