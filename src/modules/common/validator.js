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
 * Email is now optional - if provided, it must be valid, but users without emails are allowed
 * @param {Object} user
 * @returns {boolean}
 */
export function validateZendeskUser(user) {
  if (!user.name || user.name.trim() === "") return false;
  // Email is optional - if it exists, it must be valid, but absence is OK
  if (user.email !== undefined && user.email !== null && !isValidEmail(user.email)) {
    // Don't log here - logging is handled in sanitizeUsers for better aggregation
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
  const skippedUsers = {
    missingEmail: [],
    invalidEmail: [],
    missingName: []
  };

  for (const user of users) {
    // Clean email before validation
    const cleanedEmail = user.email ? cleanEmail(user.email) : user.email;
    const cleanedUser = { ...user, email: cleanedEmail };
    
    // Check name first
    if (!cleanedUser.name || cleanedUser.name.trim() === "") {
      skippedUsers.missingName.push({
        name: cleanedUser.name || "(unnamed)",
        ac_id: cleanedUser.ac_id,
        external_id: cleanedUser.external_id
      });
      continue;
    }

    // Check email - email is now optional, but if provided, it must be valid
    if (cleanedUser.email !== undefined && cleanedUser.email !== null) {
      // Email field exists - validate it
      if (!isValidEmail(cleanedUser.email)) {
        skippedUsers.invalidEmail.push({
          name: cleanedUser.name,
          email: cleanedUser.email,
          ac_id: cleanedUser.ac_id,
          external_id: cleanedUser.external_id
        });
        continue;
      }
    } else {
      // No email field - this is OK now, just log for info
      skippedUsers.missingEmail.push({
        name: cleanedUser.name,
        ac_id: cleanedUser.ac_id,
        external_id: cleanedUser.external_id
      });
      // Don't skip - continue processing (email is optional)
    }

    const finalUser = { ...cleanedUser, phone: normalizePhone(cleanedUser.phone) };
    valid.push(finalUser);
  }

  // Log summary
  if (skippedUsers.missingEmail.length > 0) {
    logger.info(`ℹ️  ${skippedUsers.missingEmail.length} user(s) without email addresses (will be synced without email field)`);
    // Log details for missing emails (info level, not warning)
    if (skippedUsers.missingEmail.length <= 10) {
      skippedUsers.missingEmail.forEach(u => {
        logger.debug(`   - ${u.name} (AC ID: ${u.ac_id || 'N/A'}, External ID: ${u.external_id || 'N/A'}) - NO EMAIL FOUND`);
      });
    }
  }
  
  const totalSkipped = skippedUsers.invalidEmail.length + skippedUsers.missingName.length;
  if (totalSkipped > 0) {
    if (skippedUsers.invalidEmail.length > 0) {
      logger.warn(`⚠️ Skipped ${skippedUsers.invalidEmail.length} user(s) with invalid email format`);
      if (skippedUsers.invalidEmail.length <= 5) {
        skippedUsers.invalidEmail.forEach(u => {
          logger.debug(`   - ${u.name} (email: "${u.email}", AC ID: ${u.ac_id || 'N/A'})`);
        });
      }
    }
    if (skippedUsers.missingName.length > 0) {
      logger.warn(`⚠️ Skipped ${skippedUsers.missingName.length} user(s) without names`);
      if (skippedUsers.missingName.length <= 5) {
        skippedUsers.missingName.forEach(u => {
          logger.debug(`   - AC ID: ${u.ac_id || 'N/A'}, External ID: ${u.external_id || 'N/A'}`);
        });
      }
    }
  }

  logger.info(`🧹 Sanitized ${valid.length}/${users.length} users`);
  return valid;
}
