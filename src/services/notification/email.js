import { logger } from "../../config/logger.js";
import AWS from "aws-sdk";

const PAULA_EMAIL = "paula.cheng@alvitacare.com";

/**
 * Send email notification to Paula about users with duplicate emails and no primary tag
 */
export async function sendEmailNotificationForDuplicateUsers(problematicGroups) {
  if (!problematicGroups || problematicGroups.length === 0) {
    return;
  }

  try {
    // Build email content
    const subject = `[Zendesk Sync] ${problematicGroups.length} Email Group(s) Without Primary Tag`;
    
    let emailBody = `Dear Paula,\n\n`;
    emailBody += `The following user groups share the same email address but none of them has the zendesk_primary tag.\n`;
    emailBody += `These users will NOT be sent to Zendesk to avoid conflicts.\n\n`;
    emailBody += `Please review and assign a zendesk_primary tag to one user in each group.\n\n`;
    emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    for (let i = 0; i < problematicGroups.length; i++) {
      const group = problematicGroups[i];
      emailBody += `Group ${i + 1}: Email "${group.email}"\n`;
      emailBody += `Users (${group.users.length}):\n`;
      
      for (const user of group.users) {
        emailBody += `  - Name: ${user.name}\n`;
        emailBody += `    AC ID: ${user.ac_id}\n`;
        emailBody += `    External ID: ${user.external_id || "N/A"}\n`;
        emailBody += `    User Type: ${user.user_type || "N/A"}\n`;
        emailBody += `    Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
        emailBody += `    Email: ${user.email || "N/A"}\n`;
        emailBody += `    Reason: No zendesk_primary tag assigned\n`;
        emailBody += `\n`;
      }
      emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    }

    emailBody += `\nTotal affected users: ${problematicGroups.reduce((sum, g) => sum + g.users.length, 0)}\n`;
    emailBody += `\nBest regards,\nZendesk-AlayaCare Integration Service`;

    // Try to send via AWS SES if configured
    const awsRegion = process.env.AWS_REGION || "us-east-2";
    const ses = new AWS.SES({ region: awsRegion });

    try {
      const params = {
        Source: process.env.NOTIFICATION_FROM_EMAIL,
        Destination: {
          ToAddresses: [process.env.NOTIFICATION_TO_EMAIL],
        },
        Message: {
          Subject: {
            Data: subject,
            Charset: "UTF-8",
          },
          Body: {
            Text: {
              Data: emailBody,
              Charset: "UTF-8",
            },
          },
        },
      };

      await ses.sendEmail(params).promise();
      logger.info(`✅ Email notification sent to ${process.env.NOTIFICATION_TO_EMAIL} about ${problematicGroups.length} problematic email group(s)`);
    } catch (sesError) {
      logger.error("❌ SES SEND FAILED", {
        message: sesError.message,
        code: sesError.code,
        statusCode: sesError.statusCode,
        requestId: sesError.requestId,
      });
      // If SES fails, log the email content so it can be sent manually
      logger.error(`❌ Failed to send email via AWS SES: ${sesError.message}`);
      logger.warn(`📧 Email notification content (please send manually to ${PAULA_EMAIL}):`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      logger.warn(`Subject: ${subject}`);
      logger.warn(`To: ${PAULA_EMAIL}`);
      logger.warn(`\n${emailBody}`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    }
  } catch (error) {
    logger.error(`❌ Failed to prepare email notification: ${error.message}`);
  }
}

/**
 * Send email notification to Paula about users with duplicate phone numbers and no primary tag
 */
export async function sendEmailNotificationForDuplicatePhoneUsers(problematicGroups) {
  if (!problematicGroups || problematicGroups.length === 0) {
    return;
  }

  try {
    // Build email content
    const subject = `[Zendesk Sync] ${problematicGroups.length} Phone Group(s) Without Primary Tag`;
    
    let emailBody = `Dear Paula,\n\n`;
    emailBody += `The following user groups share the same phone number but none of them has the zendesk_primary tag.\n`;
    emailBody += `These users will NOT be sent to Zendesk to avoid conflicts.\n\n`;
    emailBody += `Please review and assign a zendesk_primary tag to one user in each group.\n\n`;
    emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    for (let i = 0; i < problematicGroups.length; i++) {
      const group = problematicGroups[i];
      emailBody += `Group ${i + 1}: Phone "${group.phone}"\n`;
      emailBody += `Users (${group.users.length}):\n`;
      
      for (const user of group.users) {
        emailBody += `  - Name: ${user.name}\n`;
        emailBody += `    AC ID: ${user.ac_id}\n`;
        emailBody += `    External ID: ${user.external_id || "N/A"}\n`;
        emailBody += `    User Type: ${user.user_type || "N/A"}\n`;
        emailBody += `    Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
        emailBody += `    Email: ${user.email || "N/A"}\n`;
        emailBody += `    Phone: ${user.phone || "N/A"}\n`;
        emailBody += `    Reason: No zendesk_primary tag assigned\n`;
        emailBody += `\n`;
      }
      emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    }

    emailBody += `\nTotal affected users: ${problematicGroups.reduce((sum, g) => sum + g.users.length, 0)}\n`;
    emailBody += `\nBest regards,\nZendesk-AlayaCare Integration Service`;

    // Try to send via AWS SES if configured
    const awsRegion = process.env.AWS_REGION || "us-east-2";
    const ses = new AWS.SES({ region: awsRegion });

    try {
      const params = {
        Source: process.env.NOTIFICATION_FROM_EMAIL,
        Destination: {
          ToAddresses: [process.env.NOTIFICATION_TO_EMAIL],
        },
        Message: {
          Subject: {
            Data: subject,
            Charset: "UTF-8",
          },
          Body: {
            Text: {
              Data: emailBody,
              Charset: "UTF-8",
            },
          },
        },
      };

      await ses.sendEmail(params).promise();
      logger.info(`✅ Email notification sent to ${PAULA_EMAIL} about ${problematicGroups.length} problematic phone group(s)`);
    } catch (sesError) {
      logger.error("❌ SES SEND FAILED", {
        message: sesError.message,
        code: sesError.code,
        statusCode: sesError.statusCode,
        requestId: sesError.requestId,
      });
      // If SES fails, log the email content so it can be sent manually
      logger.error(`❌ Failed to send email via AWS SES: ${sesError.message}`);
      logger.warn(`📧 Email notification content (please send manually to ${PAULA_EMAIL}):`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      logger.warn(`Subject: ${subject}`);
      logger.warn(`To: ${PAULA_EMAIL}`);
      logger.warn(`\n${emailBody}`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    }
  } catch (error) {
    logger.error(`❌ Failed to prepare email notification: ${error.message}`);
  }
}

/**
 * Send email notification to Paula about primary users who changed from active to non-active
 */
export async function sendEmailNotificationForPrimaryStatusChange(primaryUsersWithStatusChange) {
  if (!primaryUsersWithStatusChange || primaryUsersWithStatusChange.length === 0) {
    return;
  }

  try {
    // Build email content
    const subject = `[Zendesk Sync] ${primaryUsersWithStatusChange.length} Primary User(s) Changed from Active to Non-Active`;
    
    let emailBody = `Dear Paula,\n\n`;
    emailBody += `The following users have the zendesk_primary tag and have changed from active to non-active status.\n\n`;
    emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    for (let i = 0; i < primaryUsersWithStatusChange.length; i++) {
      const user = primaryUsersWithStatusChange[i];
      emailBody += `User ${i + 1}:\n`;
      emailBody += `  - Name: ${user.name || "N/A"}\n`;
      emailBody += `    AC ID: ${user.ac_id}\n`;
      emailBody += `    External ID: ${user.external_id || "N/A"}\n`;
      emailBody += `    User Type: ${user.user_type || "N/A"}\n`;
      emailBody += `    Zendesk ID: ${user.zendesk_user_id || "Not synced"}\n`;
      emailBody += `    Email: ${user.email || "N/A"}\n`;
      emailBody += `    Phone: ${user.phone || "N/A"}\n`;
      emailBody += `    Previous Status: Active (changed to non-active)\n`;
      emailBody += `\n`;
    }
    emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

    emailBody += `\nTotal affected primary users: ${primaryUsersWithStatusChange.length}\n`;
    emailBody += `\nBest regards,\nZendesk-AlayaCare Integration Service`;

    // Try to send via AWS SES if configured
    const awsRegion = process.env.AWS_REGION || "us-east-2";
    const ses = new AWS.SES({ region: awsRegion });

    try {
      const params = {
        Source: process.env.NOTIFICATION_FROM_EMAIL,
        Destination: {
          ToAddresses: [process.env.NOTIFICATION_TO_EMAIL],
        },
        Message: {
          Subject: {
            Data: subject,
            Charset: "UTF-8",
          },
          Body: {
            Text: {
              Data: emailBody,
              Charset: "UTF-8",
            },
          },
        },
      };

      await ses.sendEmail(params).promise();
      logger.info(`✅ Email notification sent to ${PAULA_EMAIL} about ${primaryUsersWithStatusChange.length} primary user(s) with status change`);
    } catch (sesError) {
      logger.error("❌ SES SEND FAILED", {
        message: sesError.message,
        code: sesError.code,
        statusCode: sesError.statusCode,
        requestId: sesError.requestId,
      });
      // If SES fails, log the email content so it can be sent manually
      logger.error(`❌ Failed to send email via AWS SES: ${sesError.message}`);
      logger.warn(`📧 Email notification content (please send manually to ${PAULA_EMAIL}):`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      logger.warn(`Subject: ${subject}`);
      logger.warn(`To: ${PAULA_EMAIL}`);
      logger.warn(`\n${emailBody}`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    }
  } catch (error) {
    logger.error(`❌ Failed to prepare email notification: ${error.message}`);
  }
}

/**
 * Send job completion alert email to Kennedy
 * @param {Object} jobResult - Result object from runSync()
 * @param {string} status - 'success' or 'error'
 * @param {string} startedAt - ISO timestamp when job started
 * @param {string} finishedAt - ISO timestamp when job finished
 * @param {Error|null} error - Error object if job failed, null otherwise
 */
export async function sendJobCompletionAlert(jobResult, status, startedAt, finishedAt, error = null) {
  const KENNEDY_EMAIL = "kennedy.antonio@alvitacare.com";
  const FROM_EMAIL = "integration@alvitacare.com";

  try {
    // Calculate duration
    const startTime = new Date(startedAt);
    const endTime = new Date(finishedAt);
    const durationMs = endTime - startTime;
    const durationMinutes = Math.floor(durationMs / 60000);
    const durationSeconds = Math.floor((durationMs % 60000) / 1000);
    const duration = `${durationMinutes}m ${durationSeconds}s`;

    // Check for alerts
    const alerts = jobResult?.alerts || {};
    const hasAlerts = 
      (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) ||
      (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) ||
      (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0);

    // Build email content
    let subject = status === 'success' 
      ? `[Zendesk Sync] Job Completed Successfully`
      : `[Zendesk Sync] Job Failed`;
    
    if (hasAlerts) {
      const alertCount = 
        (alerts.duplicateEmailGroups?.length || 0) +
        (alerts.duplicatePhoneGroups?.length || 0) +
        (alerts.primaryUsersDeactivated?.length || 0);
      subject += ` - ${alertCount} Alert(s)`;
    }

    let emailBody = `Dear Kennedy,\n\n`;
    
    if (status === 'success') {
      emailBody += `The Zendesk-AlayaCare sync job has completed successfully.\n\n`;
    } else {
      emailBody += `The Zendesk-AlayaCare sync job has failed.\n\n`;
      if (error) {
        emailBody += `Error: ${error.message || JSON.stringify(error)}\n\n`;
      }
    }
    
    emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    emailBody += `Job Details:\n`;
    emailBody += `  - Status: ${status === 'success' ? '✅ Success' : '❌ Failed'}\n`;
    emailBody += `  - Started: ${startedAt}\n`;
    emailBody += `  - Finished: ${finishedAt}\n`;
    emailBody += `  - Duration: ${duration}\n\n`;

    // Include alerts that occurred during sync (hasAlerts already calculated above)
    if (hasAlerts) {
      emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
      emailBody += `⚠️  ALERTS DURING SYNC:\n\n`;

      // Duplicate email groups without primary tag
      if (alerts.duplicateEmailGroups && alerts.duplicateEmailGroups.length > 0) {
        emailBody += `  📧 Duplicate Email Groups Without Primary Tag: ${alerts.duplicateEmailGroups.length} group(s)\n`;
        emailBody += `     Total affected users: ${alerts.duplicateEmailGroups.reduce((sum, g) => sum + g.users.length, 0)}\n\n`;
        
        for (let i = 0; i < Math.min(alerts.duplicateEmailGroups.length, 5); i++) {
          const group = alerts.duplicateEmailGroups[i];
          emailBody += `     Group ${i + 1}: Email "${group.email}" (${group.users.length} users)\n`;
        }
        if (alerts.duplicateEmailGroups.length > 5) {
          emailBody += `     ... and ${alerts.duplicateEmailGroups.length - 5} more group(s)\n`;
        }
        emailBody += `\n`;
      }

      // Duplicate phone groups without primary tag
      if (alerts.duplicatePhoneGroups && alerts.duplicatePhoneGroups.length > 0) {
        emailBody += `  📞 Duplicate Phone Groups Without Primary Tag: ${alerts.duplicatePhoneGroups.length} group(s)\n`;
        emailBody += `     Total affected users: ${alerts.duplicatePhoneGroups.reduce((sum, g) => sum + g.users.length, 0)}\n\n`;
        
        for (let i = 0; i < Math.min(alerts.duplicatePhoneGroups.length, 5); i++) {
          const group = alerts.duplicatePhoneGroups[i];
          emailBody += `     Group ${i + 1}: Phone "${group.phone}" (${group.users.length} users)\n`;
        }
        if (alerts.duplicatePhoneGroups.length > 5) {
          emailBody += `     ... and ${alerts.duplicatePhoneGroups.length - 5} more group(s)\n`;
        }
        emailBody += `\n`;
      }

      // Primary users deactivated
      if (alerts.primaryUsersDeactivated && alerts.primaryUsersDeactivated.length > 0) {
        emailBody += `  🔴 Primary Users Deactivated: ${alerts.primaryUsersDeactivated.length} user(s)\n\n`;
        
        for (let i = 0; i < Math.min(alerts.primaryUsersDeactivated.length, 5); i++) {
          const user = alerts.primaryUsersDeactivated[i];
          emailBody += `     User ${i + 1}: ${user.name || user.external_id || "N/A"} (AC ID: ${user.ac_id}, Zendesk ID: ${user.zendesk_user_id || "N/A"})\n`;
        }
        if (alerts.primaryUsersDeactivated.length > 5) {
          emailBody += `     ... and ${alerts.primaryUsersDeactivated.length - 5} more user(s)\n`;
        }
        emailBody += `\n`;
      }
    }

    if (jobResult && status === 'success') {
      emailBody += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
      emailBody += `Sync Summary:\n\n`;
      emailBody += `  📊 Total Users:\n`;
      emailBody += `     - Fetched from AlayaCare: ${jobResult.totalUsers || 0}\n`;
      emailBody += `     - Saved to Database: ${jobResult.savedToDatabase || 0}\n`;
      emailBody += `     - Synced to Zendesk: ${jobResult.syncedToZendesk || 0}\n\n`;
      
      emailBody += `  ➕ Created in Zendesk: ${jobResult.newlyCreated?.count || 0}\n`;
      if (jobResult.newlyCreated?.byType) {
        emailBody += `     - Clients: ${jobResult.newlyCreated.byType.clients || 0}\n`;
        emailBody += `     - Caregivers: ${jobResult.newlyCreated.byType.caregivers || 0}\n`;
        emailBody += `     - Company Members: ${jobResult.newlyCreated.byType.companyMembers || 0}\n`;
      }
      emailBody += `\n`;
      
      emailBody += `  🔄 Updated in Zendesk: ${(jobResult.countsByType?.clients?.updated || 0) + (jobResult.countsByType?.caregivers?.updated || 0)}\n`;
      if (jobResult.countsByType) {
        emailBody += `     - Clients: ${jobResult.countsByType.clients?.updated || 0}\n`;
        emailBody += `     - Caregivers: ${jobResult.countsByType.caregivers?.updated || 0}\n`;
      }
      emailBody += `\n`;
      
      emailBody += `  🔗 Identities Synced: ${jobResult.identitiesSynced || 0}\n`;
      emailBody += `  📦 Batches Processed: ${jobResult.batches || 0}\n`;
      emailBody += `  ➖ Status Updates: ${jobResult.statusUpdatesProcessed || 0}\n\n`;
      
      if (jobResult.countsByType) {
        emailBody += `  👥 Breakdown by Type:\n`;
        emailBody += `     - Clients: ${jobResult.countsByType.clients?.total || 0} total (${jobResult.countsByType.clients?.created || 0} created, ${jobResult.countsByType.clients?.updated || 0} updated)\n`;
        emailBody += `     - Caregivers: ${jobResult.countsByType.caregivers?.total || 0} total (${jobResult.countsByType.caregivers?.created || 0} created, ${jobResult.countsByType.caregivers?.updated || 0} updated)\n`;
        if (jobResult.countsByType.companyMembers) {
          emailBody += `     - Company Members: ${jobResult.countsByType.companyMembers.total || 0} total (${jobResult.countsByType.companyMembers.created || 0} created, ${jobResult.countsByType.companyMembers.updated || 0} updated)\n`;
        }
      }
    }
    
    emailBody += `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    emailBody += `Best regards,\nZendesk-AlayaCare Integration Service`;

    // Try to send via AWS SES if configured
    const awsRegion = process.env.AWS_REGION || "us-east-2";
    const ses = new AWS.SES({ region: awsRegion });

    try {
      const params = {
        Source: process.env.NOTIFICATION_FROM_EMAIL,
        Destination: {
          ToAddresses: [process.env.NOTIFICATION_TO_EMAIL],
        },
        Message: {
          Subject: {
            Data: subject,
            Charset: "UTF-8",
          },
          Body: {
            Text: {
              Data: emailBody,
              Charset: "UTF-8",
            },
          },
        },
      };

      await ses.sendEmail(params).promise();
      logger.info(`✅ Job completion alert sent to ${KENNEDY_EMAIL}`);
    } catch (sesError) {
      logger.error("❌ SES SEND FAILED", {
        message: sesError.message,
        code: sesError.code,
        statusCode: sesError.statusCode,
        requestId: sesError.requestId,
      });
      // If SES fails, log the email content so it can be sent manually
      logger.error(`❌ Failed to send job completion alert via AWS SES: ${sesError.message}`);
      logger.warn(`📧 Email notification content (please send manually to ${KENNEDY_EMAIL}):`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      logger.warn(`Subject: ${subject}`);
      logger.warn(`To: ${KENNEDY_EMAIL}`);
      logger.warn(`From: ${FROM_EMAIL}`);
      logger.warn(`\n${emailBody}`);
      logger.warn(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    }
  } catch (error) {
    logger.error(`❌ Failed to prepare job completion alert: ${error.message}`);
  }
}

