import { logger } from "../config/logger.js";

/**
 * Retry an async function with exponential backoff
 * Respects Retry-After header for 429 rate limit errors
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
      const shouldRetry =
        attempt <= retries &&
        (!err.response || [429, 500, 502, 503, 504].includes(err.response.status));

      if (!shouldRetry) throw err;

      // For 429 errors, check for Retry-After header
      let wait = delay * Math.pow(2, attempt - 1); // default exponential backoff
      
      if (err.response?.status === 429) {
        const retryAfter = err.response?.headers?.["retry-after"];
        if (retryAfter) {
          // Retry-After can be in seconds (number or string)
          const retryAfterSeconds = typeof retryAfter === "string" 
            ? parseInt(retryAfter, 10) 
            : retryAfter;
          
          if (!isNaN(retryAfterSeconds) && retryAfterSeconds > 0) {
            wait = retryAfterSeconds * 1000; // convert to milliseconds
            logger.warn(
              `⏳ Rate limited (429). Waiting ${retryAfterSeconds}s as specified by Retry-After header`
            );
          } else {
            logger.warn(
              `⚠️ Invalid Retry-After header value: ${retryAfter}. Using exponential backoff.`
            );
          }
        } else {
          logger.warn(
            `⚠️ Rate limited (429) but no Retry-After header. Using exponential backoff.`
          );
        }
      }

      logger.warn(
        `⚠️ Retry ${attempt}/${retries} after ${wait}ms: ${err.response?.status || err.message}`
      );

      await new Promise((res) => setTimeout(res, wait));
    }
  }
  throw new Error("Max retry attempts reached");
}
