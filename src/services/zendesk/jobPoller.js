import { logger } from "../../config/logger.js";
import { getJobStatus } from "./zendesk.api.js";

/**
 * Polls Zendesk job status until completed or failed
 * @param {string} jobId - Zendesk job ID
 * @param {number} intervalMs - Delay between polls
 * @param {number} timeoutMs - Max total wait time
 */
export async function pollJobStatus(jobId, intervalMs = 3000, timeoutMs = 60000) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const statusData = await getJobStatus(jobId);
    const status = statusData?.job_status?.status;
    const total = statusData?.job_status?.total || 0;
    const completed = statusData?.job_status?.completed || 0;
    const failed = statusData?.job_status?.failed || 0;

    if (status === "completed") {
      logger.info(`✅ Job ${jobId} completed: ${completed}/${total} processed, ${failed} failed`);
      return statusData.job_status;
    }

    if (status === "failed") {
      logger.error(`❌ Job ${jobId} failed: ${failed}/${total} failed`);
      return statusData.job_status;
    }

    // logger.debug(`⌛ Job ${jobId} still ${status} (${completed}/${total})... waiting ${intervalMs}ms`);
    await new Promise((res) => setTimeout(res, intervalMs));
  }

  logger.warn(`⚠️ Job ${jobId} polling timed out after ${timeoutMs / 1000}s`);
  return { id: jobId, status: "timeout" };
}
