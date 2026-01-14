import { logger } from "../config/logger.js";
import { initDatabase, closeDatabase } from "../infra/database.js";

/**
 * Common bootstrap function for application entry points.
 * Handles database initialization, error handling, and cleanup.
 * 
 * @param {Function} jobFunction - Async function that executes the main job logic
 * @returns {Promise<void>}
 */
export async function bootstrap(jobFunction) {
  // Initialize database
  initDatabase();

  try {
    await jobFunction();
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

