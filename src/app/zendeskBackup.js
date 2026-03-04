import { logger } from "../config/logger.js";
import { runZendeskBackup } from "../core/zendeskBackupOrchestrator.js";
import { bootstrap } from "./bootstrap.js";

async function main() {
  await bootstrap(async () => {
    logger.info("🎯 Zendesk Backup job started");
    const result = await runZendeskBackup();
    logger.info("✅ Zendesk Backup job completed successfully");
    return result;
  });
}

main().catch((err) => {
  logger.error("Zendesk Backup startup error:", err);
  logger.close();
  process.exit(1);
});
