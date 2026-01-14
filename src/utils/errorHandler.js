import { logger } from "../config/logger.js";

/**
 * Common error handling utilities
 * Follows the principle of fail fast and handle errors gracefully
 */

/**
 * Check if an HTTP error is retryable
 * Network errors (no response) are considered retryable.
 * 
 * @param {Error} error - Error object (may have response property)
 * @param {Array<number>} additionalRetryableStatuses - Additional status codes to consider retryable
 * @returns {boolean} True if the error is retryable
 */
export function isRetryableHttpError(error, additionalRetryableStatuses = []) {
  // Network errors (no response) are retryable
  if (!error?.response) {
    return true;
  }

  const status = error.response.status;
  
  // Standard retryable HTTP status codes
  const retryableStatuses = [
    429, // Rate limit
    500, // Internal server error
    502, // Bad gateway
    503, // Service unavailable
    504, // Gateway timeout
    408, // Request timeout
    ...additionalRetryableStatuses,
  ];

  return retryableStatuses.includes(status);
}

/**
 * Extract error message from various error types
 * @param {Error|Object} error - Error object
 * @returns {string} Human-readable error message
 * @private - Used internally by logError
 */
function extractErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }
  if (error?.response?.data?.error?.message) {
    return error.response.data.error.message;
  }
  if (error?.response?.statusText) {
    return `${error.response.status} ${error.response.statusText}`;
  }
  if (typeof error === "string") {
    return error;
  }
  return "Unknown error occurred";
}

/**
 * Log error with context
 * @param {string} context - Context where error occurred
 * @param {Error|Object} error - Error object
 * @param {Object} metadata - Additional metadata to log
 * @internal - Currently unused but kept for future error handling improvements
 */
function logError(context, error, metadata = {}) {
  const message = extractErrorMessage(error);
  const status = error?.response?.status;
  
  logger.error(`❌ ${context}: ${message}`, {
    status,
    ...metadata,
    stack: error?.stack,
  });
}

