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
    const awsRegion = process.env.AWS_REGION || "us-east-1";
    const ses = new AWS.SES({ region: awsRegion });

    try {
      const params = {
        Source: process.env.NOTIFICATION_FROM_EMAIL || "noreply@alvitacare.com",
        Destination: {
          ToAddresses: [PAULA_EMAIL],
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
      logger.info(`✅ Email notification sent to ${PAULA_EMAIL} about ${problematicGroups.length} problematic email group(s)`);
    } catch (sesError) {
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

