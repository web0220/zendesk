import { logger } from "../config/logger.js";

/**
 * Retry an async function with exponential backoff
 * @param {Function} fn - Async function to execute
 * @param {number} retries - Max retry attempts
 * @param {number} delay - Initial delay (ms)
 */
export async function withRetry(fn, retries = 3, delay = 1000) {
  let attempt = 0;

  while (attempt <= retries) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      const wait = delay * Math.pow(2, attempt - 1); // exponential backoff
      const shouldRetry =
        attempt <= retries &&
        (!err.response || [429, 500, 502, 503, 504].includes(err.response.status));

      if (!shouldRetry) throw err;

      logger.warn(
        `⚠️ Retry ${attempt}/${retries} after ${wait}ms: ${err.response?.status || err.message}`
      );

      await new Promise((res) => setTimeout(res, wait));
    }
  }
  throw new Error("Max retry attempts reached");
}
