import { logger } from "../config/logger.js";
import { runJob } from "../core/jobRunner.js";
import { initDatabase, closeDatabase } from "../infra/database.js";
import { bootstrap } from "./bootstrap.js";

async function main() {
  await bootstrap(async () => {
    const result = await runJob();
    logger.info("Job completed successfully:", result);
    return result;
  });
}

main().catch((err) => {
  logger.error("Startup error:", err);
  closeDatabase();
  logger.close();
  process.exit(1);
});

