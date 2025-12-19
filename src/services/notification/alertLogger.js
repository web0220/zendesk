import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { logger } from "../../config/logger.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Get the alert log file path with current date
 * Format: alerts_YYYY-MM-DD.log
 * @returns {string} Full path to alert log file
 */
function getAlertLogFilePath() {
  const logsDir = path.join(__dirname, "../../../logs");
  
  // Create logs directory if it doesn't exist
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }

  // Get current date in YYYY-MM-DD format (in EST timezone)
  const now = new Date();
  const estDate = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);

  const dateStr = estDate.replace(/\//g, "-");
  const logFileName = `alerts_${dateStr}.log`;
  return path.join(logsDir, logFileName);
}

/**
 * Build alert message content for log file
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {string} Formatted alert message
 */
function buildAlertLogMessage(alerts) {
  let message = `Zendesk-AlayaCare Integration Alert Report\n`;
  message += `Generated: ${new Date().toISOString()}\n`;
  message += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  const hasAlerts = 
    (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) ||
    (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) ||
    (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0);

  if (!hasAlerts) {
    message += `✅ No alerts detected during sync.\n`;
    return message;
  }

  // Duplicate email groups without primary tag
  if (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) {
    message += `📧 DUPLICATE EMAIL GROUPS WITHOUT PRIMARY TAG: ${alerts.duplicateEmailGroups.length} group(s)\n`;
    message += `   Total affected users: ${alerts.duplicateEmailGroups.reduce((sum, g) => sum + g.users.length, 0)}\n\n`;
    
    for (let i = 0; i < alerts.duplicateEmailGroups.length; i++) {
      const group = alerts.duplicateEmailGroups[i];
      message += `   Group ${i + 1}: Email "${group.email}"\n`;
      message += `   Users (${group.users.length}):\n`;
      
      for (const user of group.users) {
        message += `     - Name: ${user.name || "N/A"}\n`;
        message += `       External ID: ${user.external_id || "N/A"}\n`;
        message += `       User Type: ${user.user_type || "N/A"}\n`;
        message += `       Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
        message += `       Email: ${user.email || "N/A"}\n`;
        message += `       Reason: No zendesk_primary tag assigned\n\n`;
      }
    }
    message += `\n`;
  }

  // Duplicate phone groups without primary tag
  if (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) {
    message += `📞 DUPLICATE PHONE GROUPS WITHOUT PRIMARY TAG: ${alerts.duplicatePhoneGroups.length} group(s)\n`;
    message += `   Total affected users: ${alerts.duplicatePhoneGroups.reduce((sum, g) => sum + g.users.length, 0)}\n\n`;
    
    for (let i = 0; i < alerts.duplicatePhoneGroups.length; i++) {
      const group = alerts.duplicatePhoneGroups[i];
      message += `   Group ${i + 1}: Phone "${group.phone}"\n`;
      message += `   Users (${group.users.length}):\n`;
      
      for (const user of group.users) {
        message += `     - Name: ${user.name || "N/A"}\n`;
        message += `       External ID: ${user.external_id || "N/A"}\n`;
        message += `       User Type: ${user.user_type || "N/A"}\n`;
        message += `       Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
        message += `       Email: ${user.email || "N/A"}\n`;
        message += `       Phone: ${user.phone || "N/A"}\n`;
        message += `       Reason: No zendesk_primary tag assigned\n\n`;
      }
    }
    message += `\n`;
  }

  // Primary users deactivated
  if (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0) {
    message += `🔴 PRIMARY USERS CHANGED FROM ACTIVE TO NON-ACTIVE: ${alerts.primaryUsersDeactivated.length} user(s)\n\n`;
    
    for (let i = 0; i < alerts.primaryUsersDeactivated.length; i++) {
      const user = alerts.primaryUsersDeactivated[i];
      message += `   User ${i + 1}:\n`;
      message += `     - Name: ${user.name || "N/A"}\n`;
      message += `       External ID: ${user.external_id || "N/A"}\n`;
      message += `       User Type: ${user.user_type || "N/A"}\n`;
      message += `       Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
      message += `       Email: ${user.email || "N/A"}\n`;
      message += `       Phone: ${user.phone || "N/A"}\n`;
      message += `       Previous Status: Active (changed to non-active)\n\n`;
    }
    message += `\n`;
  }

  message += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`;

  return message;
}

/**
 * Write alerts to log file (overwrite mode)
 * The log file is named with current date: alerts_YYYY-MM-DD.log
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {string|null} Path to log file or null if failed
 */
export function writeAlertLog(alerts) {
  try {
    const logFilePath = getAlertLogFilePath();
    const alertMessage = buildAlertLogMessage(alerts);

    // Write to file in overwrite mode (not append)
    fs.writeFileSync(logFilePath, alertMessage, { flag: "w" });

    logger.info(`✅ Alert log written to: ${logFilePath}`);
    return logFilePath;
  } catch (error) {
    logger.error(`❌ Failed to write alert log: ${error.message}`);
    return null;
  }
}

