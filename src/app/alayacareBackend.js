import { logger } from "../config/logger.js";
import { runAlayaCareBackend } from "../core/alayacareBackendOrchestrator.js";
import { bootstrap } from "./bootstrap.js";

async function main() {
  await bootstrap(async () => {
    logger.info("🎯 AlayaCare Backend job started");
    const result = await runAlayaCareBackend();
    logger.info("✅ AlayaCare Backend job completed successfully");
    return result;
  });
}

main().catch((err) => {
  logger.error("AlayaCare Backend startup error:", err);
  logger.close();
  process.exit(1);
});
