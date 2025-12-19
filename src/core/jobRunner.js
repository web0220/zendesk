import { logger } from "../config/logger.js";
import { runSync } from "./orchestrator.js";

export async function runJob() {
  const startedAt = new Date().toISOString();
  logger.info(`🕒 Job started at ${startedAt}`);

  let result = null;
  let finishedAt = null;

  try {
    result = await runSync();
    finishedAt = new Date().toISOString();
    logger.info(`✅ Job finished at ${finishedAt}`);
    
    return result;
  } catch (err) {
    finishedAt = new Date().toISOString();
    logger.error(`❌ Job failed at ${finishedAt}:`, err);
    
    throw err;
  }
}
