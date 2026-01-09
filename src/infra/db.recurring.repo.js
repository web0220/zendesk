import { logger } from "../config/logger.js";
import { getDb } from "./db.api.js";
import { hydrateMapping } from "../domain/user.db.mapper.js";

/**
 * Get all active clients for coordination monthly check-ins (Task 1)
 * Returns all active clients regardless of tier
 * 
 * @returns {Array} Array of active client records with zendesk_user_id
 */
export function getActiveClientsForCoordinationCheckIn() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE user_type = 'client'
      AND current_active = 1
      AND zendesk_user_id IS NOT NULL
    ORDER BY name ASC
  `);
  const clients = stmt.all().map(hydrateMapping);
  logger.info(`📋 Found ${clients.length} active clients for coordination monthly check-in`);
  return clients;
}

/**
 * Get active concierge clients for clinical weekly check-ins (Task 2)
 * Filters by case_rating containing "Concierge"
 * 
 * @returns {Array} Array of active concierge client records with zendesk_user_id
 */
export function getActiveConciergeClients() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE user_type = 'client'
      AND current_active = 1
      AND zendesk_user_id IS NOT NULL
      AND case_rating IS NOT NULL
      AND LOWER(case_rating) LIKE '%concierge%'
    ORDER BY name ASC
  `);
  const clients = stmt.all().map(hydrateMapping);
  logger.info(`📋 Found ${clients.length} active concierge clients for clinical weekly check-in`);
  return clients;
}

/**
 * Get active premium clients for clinical monthly check-ins (Task 3)
 * Filters by case_rating containing "Premium"
 * 
 * @returns {Array} Array of active premium client records with zendesk_user_id
 */
export function getActivePremiumClients() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE user_type = 'client'
      AND current_active = 1
      AND zendesk_user_id IS NOT NULL
      AND case_rating IS NOT NULL
      AND LOWER(case_rating) LIKE '%premium%'
    ORDER BY name ASC
  `);
  const clients = stmt.all().map(hydrateMapping);
  logger.info(`📋 Found ${clients.length} active premium clients for clinical monthly check-in`);
  return clients;
}

/**
 * Get all active caregivers with source_ac_id for caregiver prep call tickets
 * Returns all active caregivers with their source_ac_id (AlayaCare employee ID)
 * 
 * @returns {Array} Array of active caregiver records with source_ac_id and zendesk_user_id
 */
export function getActiveCaregiversForPrepCalls() {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE user_type = 'caregiver'
      AND caregiver_status = 'cg_active'
      AND source_ac_id IS NOT NULL
      AND source_ac_id != ''
      AND zendesk_user_id IS NOT NULL
    ORDER BY name ASC
  `);
  const caregivers = stmt.all().map(hydrateMapping);
  logger.info(`📋 Found ${caregivers.length} active caregivers for prep call tickets`);
  return caregivers;
}

/**
 * Get client information by AlayaCare client ID (source_ac_id)
 * @param {number|string} alayacareClientId - AlayaCare client ID
 * @returns {Object|null} Client record or null if not found
 */
export function getClientByAlayacareId(alayacareClientId) {
  const db = getDb();
  const stmt = db.prepare(`
    SELECT * FROM user_mappings 
    WHERE user_type = 'client'
      AND source_ac_id = ?
    LIMIT 1
  `);
  const client = stmt.get(String(alayacareClientId));
  return client ? hydrateMapping(client) : null;
}