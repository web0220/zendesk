import { logger } from "../config/logger.js";
import { createAlertTicket } from "../services/notification/ticket.js";
import { writeAlertLog } from "../services/notification/alertLogger.js";
import { findEmailGroupsWithoutPrimary, findPhoneGroupsWithoutPrimary, getPrimaryUsersDeactivated } from "../infra/database.js";
import { getAllEdgeCaseErrors, clearEdgeCaseErrors } from "../infra/edgeCaseErrors.js";

/**
 * Collect all current alerts from the database
 * This checks for:
 * 1. Duplicate email groups without primary tag
 * 2. Duplicate phone groups without primary tag
 * 3. Primary users who changed from active to non-active
 * 
 * @returns {Object} Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 */
function collectCurrentAlerts() {
  const alerts = {
    duplicateEmailGroups: [],
    duplicatePhoneGroups: [],
    primaryUsersDeactivated: [],
    edgeCaseEmailErrors: [],
    edgeCasePhoneErrors: [],
  };

  try {
    // Check for email groups without primary tag
    const emailGroups = findEmailGroupsWithoutPrimary();
    if (emailGroups.length > 0) {
      alerts.duplicateEmailGroups = emailGroups;
      logger.warn(`⚠️  Found ${emailGroups.length} email group(s) without primary tag`);
    } else {
      logger.info("✅ No duplicate email groups without primary tag found");
    }

    // Check for phone groups without primary tag
    const phoneGroups = findPhoneGroupsWithoutPrimary();
    if (phoneGroups.length > 0) {
      alerts.duplicatePhoneGroups = phoneGroups;
      logger.warn(`⚠️  Found ${phoneGroups.length} phone group(s) without primary tag`);
    } else {
      logger.info("✅ No duplicate phone groups without primary tag found");
    }

    // Check for primary users who are non-active
    // BUSINESS RULE: There should be NO zendesk_primary users in non-active users.
    // This finds ALL primary users who are currently non-active, regardless of when they were processed.
    // The alert will persist daily until the issue is fixed (user becomes active OR loses primary tag).
    const primaryUsersDeactivated = getPrimaryUsersDeactivated();
    
    if (primaryUsersDeactivated.length > 0) {
      alerts.primaryUsersDeactivated = primaryUsersDeactivated;
      logger.warn(`⚠️  Found ${primaryUsersDeactivated.length} primary user(s) who are currently non-active (violates business rule - will be reported until fixed)`);
    } else {
      logger.info("✅ No primary users with status change found (business rule satisfied)");
    }

    // Check for edge case errors (non-primary users sharing emails/phones that don't match primary user)
    const edgeCaseErrors = getAllEdgeCaseErrors();
    if (edgeCaseErrors.edgeCaseEmailErrors.length > 0) {
      alerts.edgeCaseEmailErrors = edgeCaseErrors.edgeCaseEmailErrors;
      logger.warn(`⚠️  Found ${edgeCaseErrors.edgeCaseEmailErrors.length} edge case email error(s) (non-primary users sharing emails that don't match primary user)`);
    } else {
      logger.info("✅ No edge case email errors found");
    }

    if (edgeCaseErrors.edgeCasePhoneErrors.length > 0) {
      alerts.edgeCasePhoneErrors = edgeCaseErrors.edgeCasePhoneErrors;
      logger.warn(`⚠️  Found ${edgeCaseErrors.edgeCasePhoneErrors.length} edge case phone error(s) (non-primary users sharing phones that don't match primary user)`);
    } else {
      logger.info("✅ No edge case phone errors found");
    }

    return alerts;
  } catch (error) {
    logger.error(`❌ Failed to collect alerts: ${error.message}`);
    return alerts;
  }
}

/**
 * Main function to create daily alert ticket
 * This should be run at 8:50am EST daily via cronjob
 */
export async function createDailyAlertTicket() {

  try {
    // Collect current alerts from database
    const alerts = collectCurrentAlerts();

    // Write alerts to log file (overwrite mode)
    const logFilePath = writeAlertLog(alerts);
    if (logFilePath) {
      logger.info(`📝 Alert log saved to: ${logFilePath}`);
    }

    // Create ticket if there are alerts
    const hasAlerts = 
      (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) ||
      (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) ||
      (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0) ||
      (alerts.edgeCaseEmailErrors && alerts.edgeCaseEmailErrors.length > 0) ||
      (alerts.edgeCasePhoneErrors && alerts.edgeCasePhoneErrors.length > 0);

    if (!hasAlerts) {
      logger.info("✅ No alerts detected - no ticket will be created");
      return {
        success: true,
        ticketCreated: false,
        alerts: alerts,
        logFilePath: logFilePath,
      };
    }

    // Create alert ticket
    logger.info("🎫 Creating alert ticket in Zendesk...");
    const ticket = await createAlertTicket(alerts);

    if (ticket) {
      logger.info(`✅ Successfully created alert ticket #${ticket.id}`);
      
      // Clear edge case errors after ticket is created
      // This ensures errors from previous day don't persist into the next day
      // New errors will be detected in subsequent syncs (7am-10pm)
      clearEdgeCaseErrors();
      logger.info("🧹 Cleared edge case errors after creating alert ticket");
      
      return {
        success: true,
        ticketCreated: true,
        ticketId: ticket.id,
        ticketUrl: ticket.url,
        alerts: alerts,
        logFilePath: logFilePath,
      };
    } else {
      logger.error("❌ Failed to create alert ticket");
      return {
        success: false,
        ticketCreated: false,
        alerts: alerts,
        logFilePath: logFilePath,
      };
    }
  } catch (error) {
    logger.error(`❌ Daily alert ticket creation failed: ${error.message}`);
    if (error.stack) {
      logger.error(error.stack);
    }
    return {
      success: false,
      ticketCreated: false,
      error: error.message,
    };
  }
}

