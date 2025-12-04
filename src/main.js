import { config } from "./config/index.js";
import { logger } from "./config/logger.js";
import { runJob } from "./core/jobRunner.js";
import { initDatabase, closeDatabase } from "./infra/database.js";

async function bootstrap() {
  logger.info("Starting Zendesk ↔ AlayaCare Integration Service");
  logger.info("Environment:", config.env);
  logger.info("AlayaCare Base URL:", config.alayacare.baseUrl);
  logger.info("Zendesk Subdomain:", config.zendesk.subdomain);
  logger.info("Configuration loaded successfully.");

  // Initialize database
  initDatabase();

  try {
    const result = await runJob();
    logger.info("Job completed successfully:", result);
  } catch (err) {
    logger.error("Job failed:", err);
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
