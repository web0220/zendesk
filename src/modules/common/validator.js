import { logger } from "../../config/logger.js";

/**
 * Clean email: remove angle brackets, trailing slashes, etc.
 * @param {string} email
 * @returns {string}
 */
export function cleanEmail(email) {
  if (!email || typeof email !== "string") return email;
  let cleaned = email.trim();
  // Remove angle brackets: <email@example.com> -> email@example.com
  cleaned = cleaned.replace(/^<|>$/g, "");
  // Remove trailing slashes: email@example.com/ -> email@example.com
  cleaned = cleaned.replace(/\/+$/, "");
  return cleaned;
}

/**
 * Validate email format
 * @param {string} email
 * @returns {boolean}
 */
export function isValidEmail(email) {
  if (!email) return false;
  const cleaned = cleanEmail(email);
  const pattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return pattern.test(cleaned);
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
    // Clean email before validation
    const cleanedEmail = user.email ? cleanEmail(user.email) : user.email;
    const cleanedUser = { ...user, email: cleanedEmail };
    
    if (validateZendeskUser(cleanedUser)) {
      const finalUser = { ...cleanedUser, phone: normalizePhone(cleanedUser.phone) };
      valid.push(finalUser);
    }
  }
  logger.info(`🧹 Sanitized ${valid.length}/${users.length} users`);
  return valid;
}
