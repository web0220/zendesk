import { logger } from "../../config/logger.js";

/**
 * Validate email format
 * @param {string} email
 * @returns {boolean}
 */
export function isValidEmail(email) {
  if (!email) return false;
  const pattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return pattern.test(email.trim());
}

/**
 * Normalize and validate phone number (E.164, +1)
 * @param {string} phone
 * @returns {string|null}
 */
export function normalizePhone(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");
  if (digits.length < 10) return null;
  return digits.startsWith("1") ? `+${digits}` : `+1${digits}`;
}

/**
 * Validate required fields for Zendesk user
 * @param {Object} user
 * @returns {boolean}
 */
export function validateZendeskUser(user) {
  if (!user.name || user.name.trim() === "") return false;
  if (!isValidEmail(user.email)) {
    logger.warn(`⚠️ Invalid email skipped: ${user.email}`);
    return false;
  }
  return true;
}

/**
 * Clean user data before sending to Zendesk
 * @param {Array} users
 * @returns {Array} Valid users only
 */
export function sanitizeUsers(users = []) {
  const valid = [];
  for (const user of users) {
    if (validateZendeskUser(user)) {
      const cleaned = { ...user, phone: normalizePhone(user.phone) };
      valid.push(cleaned);
    }
  }
  logger.info(`🧹 Sanitized ${valid.length}/${users.length} users`);
  return valid;
}
