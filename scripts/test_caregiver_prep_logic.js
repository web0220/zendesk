import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";
import { initDatabase, closeDatabase } from "../src/infra/database.js";
import { getActiveCaregiversForPrepCalls, getClientByAlayacareId } from "../src/infra/db.recurring.repo.js";
import { fetchScheduledVisits, fetchPastVisits } from "../src/services/alayacare/visit.api.js";

/**
 * Test the caregiver prep call logic with only 20 caregivers
 */
async function testCaregiverPrepLogic() {
  // Initialize database
  initDatabase();

  try {
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info("🧪 Testing Caregiver Prep Call Logic (20 caregivers)");
    logger.info("═══════════════════════════════════════════════════════════");

    // Get first 20 caregivers
    const allCaregivers = getActiveCaregiversForPrepCalls();
    const testCaregivers = allCaregivers.slice(0, 20);

    logger.info(`📋 Testing with ${testCaregivers.length} caregivers (out of ${allCaregivers.length} total)`);
    logger.info("");

    const currentTime = new Date();

    for (let i = 0; i < testCaregivers.length; i++) {
      const caregiver = testCaregivers[i];
      const sourceAcId = Number(caregiver.source_ac_id);

      logger.info(`\n[${i + 1}/${testCaregivers.length}] Processing: ${caregiver.name} (ID: ${sourceAcId})`);
      logger.info("─────────────────────────────────────────────────────────");

      try {
        // Fetch scheduled visits (next 5 days)
        logger.info(`📅 Fetching scheduled visits...`);
        const scheduledVisitsRaw = await fetchScheduledVisits(sourceAcId, currentTime);
        logger.info(`   Found ${scheduledVisitsRaw.length} scheduled visits`);

        if (scheduledVisitsRaw.length === 0) {
          logger.info(`   ⏭️  Skipping - no scheduled visits`);
          continue;
        }

        // Extract and log scheduled visit data
        const scheduledVisits = scheduledVisitsRaw.map((visit) => {
          const visitData = {
            alayacare_employee_id: visit.alayacare_employee_id,
            alayacare_client_id: visit.alayacare_client_id,
            start_at: visit.start_at,
            end_at: visit.end_at,
            cancelled: visit.cancelled,
            status: visit.status,
          };
          logger.info(`   Scheduled: Client ${visit.alayacare_client_id}, Start: ${visit.start_at}, Cancelled: ${visit.cancelled}, Status: ${visit.status}`);
          return visitData;
        });

        // Get unique client IDs from scheduled visits
        const uniqueScheduledClients = [...new Set(scheduledVisits.map((v) => v.alayacare_client_id))];
        logger.info(`   Unique clients in scheduled visits: ${uniqueScheduledClients.length} (${uniqueScheduledClients.join(", ")})`);

        // Fetch past visits (from 2022-01-01 to current time)
        logger.info(`📅 Fetching past visits (2022-01-01 to now)...`);
        const pastVisitsRaw = await fetchPastVisits(sourceAcId, currentTime);
        logger.info(`   Found ${pastVisitsRaw.length} past visits`);

        // Extract and log past visit data
        const pastVisits = pastVisitsRaw.map((visit) => {
          const visitData = {
            alayacare_employee_id: visit.alayacare_employee_id,
            alayacare_client_id: visit.alayacare_client_id,
            start_at: visit.start_at,
            end_at: visit.end_at,
            cancelled: visit.cancelled,
            status: visit.status,
          };
          return visitData;
        });

        // Filter out cancelled visits from past visits
        const nonCancelledPastVisits = pastVisits.filter((visit) => !visit.cancelled);
        logger.info(`   Non-cancelled past visits: ${nonCancelledPastVisits.length}`);

        if (nonCancelledPastVisits.length > 0) {
          // Log sample past visits
          const samplePastVisits = nonCancelledPastVisits.slice(0, 5);
          logger.info(`   Sample past visits (first 5):`);
          samplePastVisits.forEach((visit) => {
            logger.info(`     - Client ${visit.alayacare_client_id}, Start: ${visit.start_at}, Cancelled: ${visit.cancelled}, Status: ${visit.status}`);
          });

          // Get unique client IDs from past visits
          const uniquePastClients = [...new Set(nonCancelledPastVisits.map((v) => v.alayacare_client_id))];
          logger.info(`   Unique clients in past visits: ${uniquePastClients.length} (${uniquePastClients.join(", ")})`);

          // Check for new caregiver-client matches
          logger.info(`🔍 Checking for new caregiver-client matches...`);
          const newClientMatches = [];

          for (const scheduledClientId of uniqueScheduledClients) {
            // Convert both to numbers for comparison
            const scheduledClientIdNum = Number(scheduledClientId);
            
            // Check if caregiver has worked with this client before (non-cancelled visits only)
            const hasWorkedWithClient = nonCancelledPastVisits.some((visit) => {
              const pastClientIdNum = Number(visit.alayacare_client_id);
              const match = pastClientIdNum === scheduledClientIdNum;
              
              if (match) {
                logger.info(`     ✓ Found past visit with client ${scheduledClientIdNum} (matched)`);
              }
              
              return match;
            });

            if (!hasWorkedWithClient) {
              logger.info(`     🆕 NEW MATCH: Client ${scheduledClientIdNum} - no past visits found`);
              newClientMatches.push(scheduledClientIdNum);
            } else {
              logger.info(`     ✓ EXISTING: Client ${scheduledClientIdNum} - has past visits`);
            }
          }

          logger.info(`📊 Summary:`);
          logger.info(`   Scheduled clients: ${uniqueScheduledClients.length}`);
          logger.info(`   Past clients: ${uniquePastClients.length}`);
          logger.info(`   New client matches: ${newClientMatches.length}`);
          
          if (newClientMatches.length > 0) {
            logger.info(`   ⚠️  Would create tickets for: ${newClientMatches.join(", ")}`);
          } else {
            logger.info(`   ✅ No new client matches - all scheduled clients have past visits`);
          }
        } else {
          logger.info(`🆕 NEW CAREGIVER: No past visits found (would create new caregiver ticket)`);
        }

      } catch (error) {
        logger.error(`   ❌ Error processing ${caregiver.name}: ${error.message}`);
        logger.error(`   Stack: ${error.stack}`);
      }
    }

    logger.info("\n═══════════════════════════════════════════════════════════");
    logger.info("✅ Test completed");
    logger.info("═══════════════════════════════════════════════════════════");

  } catch (error) {
    logger.error(`❌ Test failed: ${error.message}`);
    logger.error(`Stack: ${error.stack}`);
  } finally {
    closeDatabase();
    logger.close();
  }
}

// Run the test
testCaregiverPrepLogic()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });

