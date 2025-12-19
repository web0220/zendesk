import { logger } from "../../config/logger.js";
import { createPrivateTaskTicket } from "../zendesk/ticket.js";
import { getLastDayOfMonth } from "../zendesk/ticket.js";

/**
 * Get requester Zendesk user ID from environment variable
 * @returns {number|null} Zendesk user ID or null if not found
 */
function getRequesterUserId() {
  const requesterUserId = process.env.TICKET_REQUESTOR_USER_ZENDESK_ID;
  if (!requesterUserId) {
    logger.error("❌ TICKET_REQUESTOR_USER_ZENDESK_ID environment variable is not set");
    return null;
  }
  return parseInt(requesterUserId, 10);
}

/**
 * Get assignee Zendesk user ID from environment variable
 * @returns {number|null} Zendesk user ID or null if not found
 */
function getAssigneeUserId() {
  const assigneeUserId = process.env.TICKET_ASSIGNEE_USER_ZENDESK_ID;
  if (!assigneeUserId) {
    logger.error("❌ TICKET_ASSIGNEE_USER_ZENDESK_ID environment variable is not set");
    return null;
  }
  return parseInt(assigneeUserId, 10);
}

/**
 * Build alert message content for ticket internal note
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {string} Formatted alert message
 */
function buildAlertMessage(alerts) {
  let message = `Zendesk-AlayaCare Integration Alert Report\n\n`;
  message += `Generated: ${new Date().toISOString()}\n\n`;
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
        message += `       AC ID: ${user.ac_id}\n`;
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
        message += `       AC ID: ${user.ac_id}\n`;
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
      message += `       AC ID: ${user.ac_id}\n`;
      message += `       External ID: ${user.external_id || "N/A"}\n`;
      message += `       User Type: ${user.user_type || "N/A"}\n`;
      message += `       Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
      message += `       Email: ${user.email || "N/A"}\n`;
      message += `       Phone: ${user.phone || "N/A"}\n`;
      message += `       Previous Status: Active (changed to non-active)\n\n`;
    }
    message += `\n`;
  }

  message += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  message += `Please review and take appropriate action.\n`;

  return message;
}

/**
 * Create a Zendesk ticket for sync alerts
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {Promise<Object|null>} Created ticket object or null if failed
 */
export async function createAlertTicket(alerts) {
  const hasAlerts = 
    (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) ||
    (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) ||
    (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0);

  if (!hasAlerts) {
    logger.info("✅ No alerts to create ticket for");
    return null;
  }

  try {
    // Get requester and assignee user IDs from environment variables
    const requesterUserId = getRequesterUserId();
    const assigneeUserId = getAssigneeUserId();

    if (!requesterUserId) {
      logger.error("❌ Cannot create alert ticket: TICKET_REQUESTOR_USER_ZENDESK_ID not set");
      return null;
    }

    if (!assigneeUserId) {
      logger.error("❌ Cannot create alert ticket: TICKET_ASSIGNEE_USER_ZENDESK_ID not set");
      return null;
    }

    // Build alert message
    const alertMessage = buildAlertMessage(alerts);

    // Calculate alert count for subject
    const alertCount = 
      (alerts.duplicateEmailGroups?.length || 0) +
      (alerts.duplicatePhoneGroups?.length || 0) +
      (alerts.primaryUsersDeactivated?.length || 0);

    const subject = `[Zendesk Sync Alert] ${alertCount} Alert(s) Detected`;

    // Set due date to end of current month
    const dueAt = getLastDayOfMonth();

    // Create ticket with internal note and assignee
    const ticket = await createPrivateTaskTicket({
      requesterId: requesterUserId,
      subject: subject,
      dueAt: dueAt,
      commentBody: alertMessage,
      assigneeId: assigneeUserId,
    });

    if (!ticket) {
      logger.error("❌ Failed to create alert ticket");
      return null;
    }

    logger.info(`✅ Created and assigned alert ticket #${ticket.id} to user ID ${assigneeUserId} (requester: ${requesterUserId})`);
    logger.info(`✅ Created alert ticket #${ticket.id} for ${alertCount} alert(s)`);
    return ticket;
  } catch (error) {
    logger.error(`❌ Failed to create alert ticket: ${error.message}`);
    return null;
  }
}

