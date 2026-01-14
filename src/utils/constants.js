/**
 * Application-wide constants
 */

/**
 * Alvita company organization ID used to identify company members
 */
export const ALVITA_COMPANY_ORG_ID = "40994316312731";

/**
 * Check if an organization ID belongs to Alvita company
 * @param {string|number|null|undefined} orgId - Organization ID to check
 * @returns {boolean} True if the org ID matches Alvita company
 */
export function isAlvitaCompanyMember(orgId) {
  if (orgId === null || orgId === undefined) return false;
  try {
    return String(orgId) === ALVITA_COMPANY_ORG_ID;
  } catch {
    return false;
  }
}

