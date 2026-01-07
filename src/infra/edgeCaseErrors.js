/**
 * Storage for edge case errors detected during duplicate processing.
 * These errors occur when non-primary users share emails/phones that don't match the primary user.
 * 
 * Edge Case Scenario:
 * - User 1: agency@agency.com (zendesk_primary = 1)
 * - User 2: agency@agency.com, user@gmail.com (zendesk_primary = 0)
 * - User 3: agency@agency.com, user@gmail.com (zendesk_primary = 0)
 * 
 * Problem: User 2 and User 3 share user@gmail.com, but neither is primary for that email.
 * The system aliases agency@agency.com (matches primary), but user@gmail.com remains unaliased,
 * causing a conflict that prevents sync.
 */

// In-memory storage for edge case errors
let edgeCaseEmailErrors = [];
let edgeCasePhoneErrors = [];

// Track unique error keys to prevent duplicates
// Key format: "email:user1_ac_id,user2_ac_id" or "phone:user1_ac_id,user2_ac_id"
let edgeCaseEmailErrorKeys = new Set();
let edgeCasePhoneErrorKeys = new Set();

/**
 * Generate a unique key for an email error
 * @param {string} email - The email address
 * @param {Array} users - Array of user objects with ac_id
 * @returns {string} Unique key for the error
 */
function generateEmailErrorKey(email, users) {
  // Sort user IDs to ensure consistent key regardless of order
  const sortedUserIds = users.map(u => u.ac_id).sort().join(',');
  return `email:${email.toLowerCase()}:${sortedUserIds}`;
}

/**
 * Generate a unique key for a phone error
 * @param {string} phone - The phone number
 * @param {Array} users - Array of user objects with ac_id
 * @returns {string} Unique key for the error
 */
function generatePhoneErrorKey(phone, users) {
  // Sort user IDs to ensure consistent key regardless of order
  const sortedUserIds = users.map(u => u.ac_id).sort().join(',');
  return `phone:${phone}:${sortedUserIds}`;
}

/**
 * Clear all stored edge case errors
 * Should be called:
 * - At the start of each day (before first sync)
 * - After daily alert ticket is created
 * NOT at the start of each sync (to allow errors to persist across syncs)
 */
export function clearEdgeCaseErrors() {
  edgeCaseEmailErrors = [];
  edgeCasePhoneErrors = [];
  edgeCaseEmailErrorKeys = new Set();
  edgeCasePhoneErrorKeys = new Set();
}

/**
 * Store an edge case email error (only if not already stored)
 * @param {Object} error - Error object with email, users, and primaryUser
 * @returns {boolean} True if error was stored, false if it was a duplicate
 */
export function storeEdgeCaseEmailError(error) {
  const key = generateEmailErrorKey(error.email, error.users);
  
  // Check if this error already exists
  if (edgeCaseEmailErrorKeys.has(key)) {
    return false; // Duplicate error, don't store
  }
  
  // Store the error and its key
  edgeCaseEmailErrors.push(error);
  edgeCaseEmailErrorKeys.add(key);
  return true; // New error stored
}

/**
 * Store an edge case phone error (only if not already stored)
 * @param {Object} error - Error object with phone, users, and primaryUser
 * @returns {boolean} True if error was stored, false if it was a duplicate
 */
export function storeEdgeCasePhoneError(error) {
  const key = generatePhoneErrorKey(error.phone, error.users);
  
  // Check if this error already exists
  if (edgeCasePhoneErrorKeys.has(key)) {
    return false; // Duplicate error, don't store
  }
  
  // Store the error and its key
  edgeCasePhoneErrors.push(error);
  edgeCasePhoneErrorKeys.add(key);
  return true; // New error stored
}

/**
 * Get all stored edge case email errors
 * @returns {Array} Array of edge case email errors
 */
export function getEdgeCaseEmailErrors() {
  return [...edgeCaseEmailErrors]; // Return a copy to prevent mutation
}

/**
 * Get all stored edge case phone errors
 * @returns {Array} Array of edge case phone errors
 */
export function getEdgeCasePhoneErrors() {
  return [...edgeCasePhoneErrors]; // Return a copy to prevent mutation
}

/**
 * Get all edge case errors (both email and phone)
 * @returns {Object} Object with edgeCaseEmailErrors and edgeCasePhoneErrors arrays
 */
export function getAllEdgeCaseErrors() {
  return {
    edgeCaseEmailErrors: getEdgeCaseEmailErrors(),
    edgeCasePhoneErrors: getEdgeCasePhoneErrors(),
  };
}

