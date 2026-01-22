import { logger } from "../config/logger.js";
import { runJob } from "../core/jobRunner.js";
import { initDatabase, closeDatabase } from "../infra/database.js";
import { bootstrap } from "./bootstrap.js";
import { logRunTime } from "../utils/runTimeLogger.js";

async function main() {
  const startTime = new Date();
  
  await bootstrap(async () => {
    try {
      const result = await runJob();
      const endTime = new Date();
      logRunTime(startTime, endTime, "success");
      return result;
    } catch (err) {
      const endTime = new Date();
      logRunTime(startTime, endTime, "error", err);
      throw err;
    }
  });
}

main().catch((err) => {
  logger.error("Startup error:", err);
  closeDatabase();
  logger.close();
  process.exit(1);
});

