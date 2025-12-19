#!/usr/bin/env node

/**
 * Daily Alert Ticket Script
 * 
 * This script should be run at 8:50am EST daily via cronjob.
 * It collects current alerts from the database and creates a Zendesk ticket
 * if any alerts are detected.
 * 
 * Usage:
 *   npm run recurring:premium
 * 
 * Or directly:
 *   node src/dailyAlert.js
 */

import { config } from "./config/index.js";
import { initDatabase } from "./infra/db.api.js";
import { logger } from "./config/logger.js";
import { createDailyAlertTicket } from "./core/dailyAlertTicket.js";

async function main() {
  try {
    logger.info("🚀 Starting daily alert ticket creation...");
    
    // Initialize database
    initDatabase();
    
    // Create daily alert ticket
    const result = await createDailyAlertTicket();
    
    if (result.success) {
      if (result.ticketCreated) {
        logger.info(`✅ Daily alert ticket creation completed successfully`);
        logger.info(`   Ticket ID: ${result.ticketId}`);
        if (result.ticketUrl) {
          logger.info(`   Ticket URL: ${result.ticketUrl}`);
        }
      } else {
        logger.info(`✅ Daily alert check completed - no alerts detected`);
      }
      if (result.logFilePath) {
        logger.info(`   Log file: ${result.logFilePath}`);
      }
      process.exit(0);
    } else {
      logger.error(`❌ Daily alert ticket creation failed`);
      if (result.error) {
        logger.error(`   Error: ${result.error}`);
      }
      process.exit(1);
    }
  } catch (error) {
    logger.error(`❌ Fatal error in daily alert script: ${error.message}`);
    if (error.stack) {
      logger.error(error.stack);
    }
    process.exit(1);
  }
}

main();

