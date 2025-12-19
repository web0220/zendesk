import { logger } from "../../config/logger.js";

/**
 * DEPRECATED: Email notifications have been replaced with ticket-based notifications.
 * This function is kept for backward compatibility but does nothing.
 * @deprecated Use ticket-based notifications instead
 */
export async function sendEmailNotificationForDuplicateUsers(problematicGroups) {
  if (!problematicGroups || problematicGroups.length === 0) {
    return;
  }
  logger.debug("📧 Email notification disabled - using ticket-based notifications instead");
}

/**
 * DEPRECATED: Email notifications have been replaced with ticket-based notifications.
 * This function is kept for backward compatibility but does nothing.
 * @deprecated Use ticket-based notifications instead
 */
export async function sendEmailNotificationForDuplicatePhoneUsers(problematicGroups) {
  if (!problematicGroups || problematicGroups.length === 0) {
    return;
  }
  logger.debug("📧 Email notification disabled - using ticket-based notifications instead");
}

/**
 * DEPRECATED: Email notifications have been replaced with ticket-based notifications.
 * This function is kept for backward compatibility but does nothing.
 * @deprecated Use ticket-based notifications instead
 */
export async function sendEmailNotificationForPrimaryStatusChange(primaryUsersWithStatusChange) {
  if (!primaryUsersWithStatusChange || primaryUsersWithStatusChange.length === 0) {
    return;
  }
  logger.debug("📧 Email notification disabled - using ticket-based notifications instead");
}

/**
 * DEPRECATED: Email notifications have been replaced with ticket-based notifications.
 * This function is kept for backward compatibility but does nothing.
 * @deprecated Use ticket-based notifications instead
 * @param {Object} jobResult - Result object from runSync()
 * @param {string} status - 'success' or 'error'
 * @param {string} startedAt - ISO timestamp when job started
 * @param {string} finishedAt - ISO timestamp when job finished
 * @param {Error|null} error - Error object if job failed, null otherwise
 */
export async function sendJobCompletionAlert(jobResult, status, startedAt, finishedAt, error = null) {
  logger.debug("📧 Email notification disabled - using ticket-based notifications instead");
}

