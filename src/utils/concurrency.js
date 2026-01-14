import pLimit from "p-limit";

/**
 * Splits an array into smaller chunks.
 * @param {Array} arr - Array to chunk
 * @param {number} size - Chunk size
 * @returns {Array<Array>} Array of chunks
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
 * Useful for APIs with rate limits or when processing large batches.
 * 
 * @param {Array<Function>} tasks - Array of async functions to execute
 * @param {number} concurrency - Maximum number of concurrent executions
 * @returns {Promise<Array>} Array of results from all tasks
 */
export async function runWithLimit(tasks, concurrency = 5) {
  const limit = pLimit(concurrency);
  const results = [];

  for (const task of tasks) {
    results.push(limit(() => task()));
  }

  return Promise.all(results);
}

