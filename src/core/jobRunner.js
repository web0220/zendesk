import { logger } from "../config/logger.js";
import { runSync } from "./orchestrator.js";

export async function runJob() {
  const startedAt = new Date().toISOString();
  logger.info(`🕒 Job started at ${startedAt}`);

  try {
    const result = await runSync();
    const finishedAt = new Date().toISOString();
    logger.info(`✅ Job finished at ${finishedAt}`);
    return result;
  } catch (err) {
    const finishedAt = new Date().toISOString();
    logger.error(`❌ Job failed at ${finishedAt}:`, err);
    throw err;
  }
}
