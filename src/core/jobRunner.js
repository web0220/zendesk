import { logger } from "../config/logger.js";
import { runSync } from "./orchestrator.js";
import { sendJobCompletionAlert } from "../services/notification/email.js";

export async function runJob() {
  const startedAt = new Date().toISOString();
  logger.info(`🕒 Job started at ${startedAt}`);

  let result = null;
  let finishedAt = null;

  try {
    result = await runSync();
    finishedAt = new Date().toISOString();
    logger.info(`✅ Job finished at ${finishedAt}`);
    
    // Send success alert email
    await sendJobCompletionAlert(result, 'success', startedAt, finishedAt);
    
    return result;
  } catch (err) {
    finishedAt = new Date().toISOString();
    logger.error(`❌ Job failed at ${finishedAt}:`, err);
    
    // Send failure alert email
    await sendJobCompletionAlert(result, 'error', startedAt, finishedAt, err);
    
    throw err;
  }
}
