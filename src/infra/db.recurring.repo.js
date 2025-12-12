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
