import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";
import { initDatabase, closeDatabase } from "../src/infra/database.js";
import {
  sendEmailNotificationForDuplicateUsers,
  sendEmailNotificationForDuplicatePhoneUsers,
  sendEmailNotificationForPrimaryStatusChange,
  sendJobCompletionAlert,
} from "../src/services/notification/email.js";

/**
 * Test script to verify email notifications are working
 * Usage: node scripts/test_email_notifications.js [test-type]
 * 
 * Test types:
 * - duplicate-email: Test duplicate email groups notification
 * - duplicate-phone: Test duplicate phone groups notification
 * - primary-deactivated: Test primary user deactivated notification
 * - job-completion: Test job completion alert
 * - all: Test all email types (default)
 */
async function testEmailNotifications(testType = "all") {
  logger.info("🧪 Starting email notification tests...");
  logger.info(`📧 Test type: ${testType}`);

  // Initialize database (required for some functions)
  initDatabase();

  try {
    if (testType === "all" || testType === "duplicate-email") {
      logger.info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      logger.info("📧 Testing: Duplicate Email Groups Notification");
      logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      
      const mockEmailGroups = [
        {
          email: "test@example.com",
          users: [
            {
              ac_id: "test-1",
              name: "Test User 1",
              external_id: "EXT-001",
              user_type: "client",
              zendesk_user_id: "12345",
              email: "test@example.com",
            },
            {
              ac_id: "test-2",
              name: "Test User 2",
              external_id: "EXT-002",
              user_type: "caregiver",
              zendesk_user_id: null,
              email: "test@example.com",
            },
          ],
        },
      ];

      await sendEmailNotificationForDuplicateUsers(mockEmailGroups);
      logger.info("✅ Duplicate email notification test completed");
    }

    if (testType === "all" || testType === "duplicate-phone") {
      logger.info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      logger.info("📞 Testing: Duplicate Phone Groups Notification");
      logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      
      const mockPhoneGroups = [
        {
          phone: "+1234567890",
          users: [
            {
              ac_id: "test-3",
              name: "Test User 3",
              external_id: "EXT-003",
              user_type: "client",
              zendesk_user_id: "67890",
              email: "user3@example.com",
              phone: "+1234567890",
            },
            {
              ac_id: "test-4",
              name: "Test User 4",
              external_id: "EXT-004",
              user_type: "caregiver",
              zendesk_user_id: null,
              email: "user4@example.com",
              phone: "+1234567890",
            },
          ],
        },
      ];

      await sendEmailNotificationForDuplicatePhoneUsers(mockPhoneGroups);
      logger.info("✅ Duplicate phone notification test completed");
    }

    if (testType === "all" || testType === "primary-deactivated") {
      logger.info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      logger.info("🔴 Testing: Primary User Deactivated Notification");
      logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      
      const mockPrimaryUsers = [
        {
          ac_id: "test-5",
          name: "Primary Test User",
          external_id: "EXT-005",
          user_type: "client",
          zendesk_user_id: "11111",
          email: "primary@example.com",
          phone: "+1987654321",
        },
      ];

      await sendEmailNotificationForPrimaryStatusChange(mockPrimaryUsers);
      logger.info("✅ Primary user deactivated notification test completed");
    }

    if (testType === "all" || testType === "job-completion") {
      logger.info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      logger.info("📊 Testing: Job Completion Alert");
      logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      
      const mockJobResult = {
        totalUsers: 100,
        savedToDatabase: 100,
        syncedToZendesk: 95,
        batches: 1,
        mappingsStored: 95,
        identitiesSynced: 95,
        statusUpdatesProcessed: 5,
        alerts: {
          duplicateEmailGroups: [
            {
              email: "test@example.com",
              users: [{ ac_id: "test-1", name: "Test User" }],
            },
          ],
          duplicatePhoneGroups: [],
          primaryUsersDeactivated: [
            {
              ac_id: "test-5",
              name: "Primary User",
              zendesk_user_id: "11111",
            },
          ],
        },
        newlyCreated: {
          count: 10,
          byType: {
            clients: 5,
            caregivers: 5,
            companyMembers: 2,
          },
        },
        countsByType: {
          clients: {
            total: 50,
            created: 5,
            updated: 45,
          },
          caregivers: {
            total: 50,
            created: 5,
            updated: 45,
          },
          companyMembers: {
            total: 10,
            created: 2,
            updated: 8,
          },
        },
      };

      const startedAt = new Date().toISOString();
      const finishedAt = new Date(Date.now() + 120000).toISOString(); // 2 minutes later

      await sendJobCompletionAlert(mockJobResult, "success", startedAt, finishedAt);
      logger.info("✅ Job completion alert test completed");
    }

    logger.info("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("✅ All email notification tests completed!");
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    logger.info("\n📬 Check your email inbox (and spam folder) for the test emails.");
    logger.info("📋 Also check AWS SES console for email sending status.");
  } catch (error) {
    logger.error("❌ Test failed:", error);
    process.exit(1);
  } finally {
    closeDatabase();
    logger.close();
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const testType = args[0] || "all";

const validTestTypes = ["all", "duplicate-email", "duplicate-phone", "primary-deactivated", "job-completion"];
if (!validTestTypes.includes(testType)) {
  console.error(`❌ Invalid test type: ${testType}`);
  console.error(`Valid types: ${validTestTypes.join(", ")}`);
  process.exit(1);
}

testEmailNotifications(testType).catch((err) => {
  console.error("❌ Test script failed:", err);
  process.exit(1);
});
