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
 * Uses HTML formatting for proper display in Zendesk
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {string} Formatted alert message in HTML
 */
/**
 * Format date in American standard format with day of week in EST timezone
 * @returns {string} Formatted date string (e.g., "Monday, December 19, 2025 10:10:03 PM EST")
 */
function formatDateEST() {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
    timeZoneName: "short",
  });
  return formatter.format(now);
}

/**
 * Build alert message content for ticket internal note
 * Uses HTML formatting for proper display in Zendesk
 * @param {Object} alerts - Alert object with duplicateEmailGroups, duplicatePhoneGroups, primaryUsersDeactivated
 * @returns {string} Formatted alert message in HTML
 */
function buildAlertMessage(alerts) {
  let message = `<h2>Zendesk-AlayaCare Integration Alert Report</h2>`;
  message += `<p><strong>Generated:</strong> ${formatDateEST()}</p>`;
  message += `<hr>`;

  const hasAlerts = 
    (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) ||
    (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) ||
    (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0);

  if (!hasAlerts) {
    message += `<p>✅ No alerts detected during sync.</p>`;
    return message;
  }

  // Duplicate email groups without primary tag
  if (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) {
    message += `<h3>DUPLICATE EMAIL GROUPS WITHOUT PRIMARY TAG: ${alerts.duplicateEmailGroups.length} group(s)</h3>`;
    message += `<p><strong>Total affected users:</strong> ${alerts.duplicateEmailGroups.reduce((sum, g) => sum + g.users.length, 0)}</p>`;
    
    for (let i = 0; i < alerts.duplicateEmailGroups.length; i++) {
      const group = alerts.duplicateEmailGroups[i];
      message += `<div style="margin: 15px 0; padding: 10px; background-color: #f5f5f5; border-left: 4px solid #e74c3c;">`;
      message += `<h4>Group ${i + 1}: Email "${group.email}"</h4>`;
      message += `<p><strong>Users (${group.users.length}):</strong></p>`;
      
      for (const user of group.users) {
        message += `<div style="margin: 10px 0; padding: 10px; background-color: #ffffff; border: 1px solid #ddd;">`;
        message += `<ul style="margin: 0; padding-left: 20px;">`;
        message += `<li><strong>Name:</strong> ${user.name || "N/A"}</li>`;
        message += `<li><strong>External ID:</strong> ${user.external_id || "N/A"}</li>`;
        message += `<li><strong>User Type:</strong> ${user.user_type || "N/A"}</li>`;
        message += `<li><strong>Zendesk ID:</strong> ${user.zendesk_user_id || "Not synced"}</li>`;
        message += `</ul>`;
        message += `</div>`;
      }
      message += `</div>`;
    }
  }

  // Duplicate phone groups without primary tag
  if (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) {
    message += `<h3>📞 DUPLICATE PHONE GROUPS WITHOUT PRIMARY TAG: ${alerts.duplicatePhoneGroups.length} group(s)</h3>`;
    message += `<p><strong>Total affected users:</strong> ${alerts.duplicatePhoneGroups.reduce((sum, g) => sum + g.users.length, 0)}</p>`;
    
    for (let i = 0; i < alerts.duplicatePhoneGroups.length; i++) {
      const group = alerts.duplicatePhoneGroups[i];
      message += `<div style="margin: 15px 0; padding: 10px; background-color: #f5f5f5; border-left: 4px solid #e74c3c;">`;
      message += `<h4>Group ${i + 1}: Phone "${group.phone}"</h4>`;
      message += `<p><strong>Users (${group.users.length}):</strong></p>`;
      
      for (const user of group.users) {
        message += `<div style="margin: 10px 0; padding: 10px; background-color: #ffffff; border: 1px solid #ddd;">`;
        message += `<ul style="margin: 0; padding-left: 20px;">`;
        message += `<li><strong>Name:</strong> ${user.name || "N/A"}</li>`;
        message += `<li><strong>External ID:</strong> ${user.external_id || "N/A"}</li>`;
        message += `<li><strong>User Type:</strong> ${user.user_type || "N/A"}</li>`;
        message += `<li><strong>Zendesk ID:</strong> ${user.zendesk_user_id || "Not synced"}</li>`;
        message += `</ul>`;
        message += `</div>`;
      }
      message += `</div>`;
    }
  }

  // Primary users deactivated
  if (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0) {
    message += `<h3>🔴 PRIMARY USERS CHANGED FROM ACTIVE TO NON-ACTIVE: ${alerts.primaryUsersDeactivated.length} user(s)</h3>`;
    
    for (let i = 0; i < alerts.primaryUsersDeactivated.length; i++) {
      const user = alerts.primaryUsersDeactivated[i];
      message += `<div style="margin: 15px 0; padding: 10px; background-color: #fff3cd; border-left: 4px solid #ffc107;">`;
      message += `<h4>User ${i + 1}</h4>`;
      message += `<ul style="margin: 0; padding-left: 20px;">`;
      message += `<li><strong>Name:</strong> ${user.name || "N/A"}</li>`;
      message += `<li><strong>External ID:</strong> ${user.external_id || "N/A"}</li>`;
      message += `<li><strong>User Type:</strong> ${user.user_type || "N/A"}</li>`;
      message += `<li><strong>Zendesk ID:</strong> ${user.zendesk_user_id || "Not synced"}</li>`;
      message += `<li><strong>Email:</strong> ${user.email || "N/A"}</li>`;
      message += `<li><strong>Phone:</strong> ${user.phone || "N/A"}</li>`;
      message += `<li><strong>Previous Status:</strong> Active (changed to non-active)</li>`;
      message += `</ul>`;
      message += `</div>`;
    }
  }

  message += `<hr>`;
  message += `<p><strong>Please review and take appropriate action.</strong></p>`;

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

