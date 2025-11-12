import { logger } from "../config/logger.js";
import { runSync } from "./orchestrator.js";
import { saveRunResult } from "../infrastructure/storage.js";

export async function runJob() {
  const startedAt = new Date().toISOString();
  logger.info(`🕒 Job started at ${startedAt}`);

  try {
    const result = await runSync();
    const finishedAt = new Date().toISOString();
    logger.info(`✅ Job finished at ${finishedAt}`);
    await saveRunResult({ startedAt, finishedAt, ...result }, "success");
    return result;
  } catch (err) {
    const finishedAt = new Date().toISOString();
    logger.error(`❌ Job failed at ${finishedAt}:`, err);
    await saveRunResult({ startedAt, finishedAt, error: err.message }, "failed");
    throw err;
  }
}
