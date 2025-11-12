import pLimit from "p-limit";
import { logger } from "../../config/logger.js";

/**
 * Splits an array into smaller chunks.
 * @param {Array} arr - Array to chunk
 * @param {number} size - Chunk size
 * @returns {Array<Array>}
 */
export function chunkArray(arr, size = 100) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/**
 * Execute async tasks with concurrency control.
 * Useful for APIs with rate limits.
 * @param {Array<Function>} tasks - Array of async functions
 * @param {number} concurrency - Max concurrent executions
 */
export async function runWithLimit(tasks, concurrency = 5) {
  const limit = pLimit(concurrency);
  const results = [];

  logger.info(`🚦 Running ${tasks.length} tasks with concurrency ${concurrency}`);

  for (const task of tasks) {
    results.push(limit(() => task()));
  }

  return Promise.all(results);
}
