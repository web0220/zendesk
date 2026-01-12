import { logger } from "../src/config/logger.js";
import { initDatabase, closeDatabase } from "../src/infra/database.js";
import { getActiveCaregiversForPrepCalls, getClientByAlayacareId } from "../src/infra/db.recurring.repo.js";
import { fetchScheduledVisits, fetchPastVisits } from "../src/services/alayacare/visit.api.js";
import { searchTickets } from "../src/services/zendesk/zendesk.api.js";

// Zendesk custom field IDs from environment variables
const CG_PREP_DEDUP_KEY_FIELD_ID = process.env.ZENDESK_CG_PREP_DEDUP_KEY_FIELD_ID || null;

/**
 * Format date to MM/DD/YYYY
 * @param {Date|string} date - Date object or ISO string
 * @returns {string} Formatted date string
 */
function formatDateForDisplay(date) {
  const d = typeof date === "string" ? new Date(date) : date;
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const year = d.getFullYear();
  return `${month}/${day}/${year}`;
}

/**
 * Calculate due date (day before the first shift)
 * @param {Date|string} firstShiftDate - Date of first shift
 * @returns {string} ISO 8601 date string for due date
 */
function calculateDueDate(firstShiftDate) {
  const date = typeof firstShiftDate === "string" ? new Date(firstShiftDate) : firstShiftDate;
  const dueDate = new Date(date);
  dueDate.setDate(dueDate.getDate() - 1);
  // Set to end of day (23:59:59) in EST, then convert to UTC
  dueDate.setHours(23, 59, 59, 999);
  return dueDate.toISOString();
}

/**
 * Get assignee group ID from coordinator pod
 * Maps coordinator_pod values to Zendesk group IDs
 * @param {string} coordinatorPod - Coordinator pod name
 * @returns {number|null} Zendesk group ID or null
 */
function getAssigneeGroupId(coordinatorPod) {
  if (!coordinatorPod) {
    return null;
  }

  const podToGroupMapping = process.env.ZENDESK_POD_GROUP_MAPPING 
    ? JSON.parse(process.env.ZENDESK_POD_GROUP_MAPPING)
    : {};
  
  const groupId = podToGroupMapping[coordinatorPod];
  if (groupId) {
    logger.debug(`   📋 Mapped coordinator pod "${coordinatorPod}" to Zendesk group ${groupId}`);
  } else {
    logger.warn(`   ⚠️  No Zendesk group mapping found for coordinator pod "${coordinatorPod}"`);
  }
  
  return groupId || null;
}

/**
 * Check if a ticket exists for new caregiver prep call
 * @param {number} alayacareEmployeeId - Caregiver's AlayaCare employee ID
 * @returns {Promise<boolean>} True if ticket exists
 */
async function checkNewCaregiverTicketExists(alayacareEmployeeId) {
  if (!CG_PREP_DEDUP_KEY_FIELD_ID) {
    logger.warn("⚠️ ZENDESK_CG_PREP_DEDUP_KEY_FIELD_ID not configured, skipping duplicate check");
    return false;
  }

  const dedupValue = `new_cg_caregiver_${alayacareEmployeeId}`;
  const query = `type:ticket status<solved custom_field_${CG_PREP_DEDUP_KEY_FIELD_ID}:${dedupValue}`;
  
  try {
    const results = await searchTickets(query);
    return results.length > 0;
  } catch (error) {
    logger.error(`❌ Failed to search for existing new caregiver ticket: ${error.message}`);
    return false; // Assume no ticket exists on error to avoid blocking
  }
}

/**
 * Check if a ticket exists for new caregiver-client match prep call
 * @param {number} alayacareEmployeeId - Caregiver's AlayaCare employee ID
 * @param {number} alayacareClientId - Client's AlayaCare client ID
 * @returns {Promise<boolean>} True if ticket exists
 */
async function checkNewCaregiverClientMatchTicketExists(alayacareEmployeeId, alayacareClientId) {
  if (!CG_PREP_DEDUP_KEY_FIELD_ID) {
    logger.warn("⚠️ ZENDESK_CG_PREP_DEDUP_KEY_FIELD_ID not configured, skipping duplicate check");
    return false;
  }

  const dedupValue = `new_cg_client_match_caregiver_${alayacareEmployeeId}_client_${alayacareClientId}`;
  const query = `type:ticket status<solved custom_field_${CG_PREP_DEDUP_KEY_FIELD_ID}:${dedupValue}`;
  
  try {
    const results = await searchTickets(query);
    return results.length > 0;
  } catch (error) {
    logger.error(`❌ Failed to search for existing caregiver-client match ticket: ${error.message}`);
    return false; // Assume no ticket exists on error to avoid blocking
  }
}

/**
 * Test the caregiver prep call logic for a specific caregiver ID
 * @param {string|number} caregiverId - Caregiver ID (ac_id or source_ac_id)
 */
async function testCaregiverPrepLogic(caregiverId) {
  // Initialize database
  initDatabase();

  try {
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info("🧪 Testing Caregiver Prep Call Logic");
    logger.info("═══════════════════════════════════════════════════════════");

    // Get all caregivers
    const allCaregivers = getActiveCaregiversForPrepCalls();
    
    // Find caregiver by ID (try both ac_id and source_ac_id)
    let testCaregiver = null;
    if (caregiverId) {
      const idStr = String(caregiverId);
      testCaregiver = allCaregivers.find(
        (cg) => String(cg.ac_id) === idStr || String(cg.source_ac_id) === idStr
      );
      
      if (!testCaregiver) {
        logger.error(`❌ Caregiver with ID ${caregiverId} not found`);
        logger.info(`   Available IDs: ${allCaregivers.slice(0, 5).map(cg => `ac_id:${cg.ac_id} or source_ac_id:${cg.source_ac_id}`).join(", ")}...`);
        return;
      }
      
      logger.info(`📋 Testing with caregiver: ${testCaregiver.name} (ac_id: ${testCaregiver.ac_id}, source_ac_id: ${testCaregiver.source_ac_id})`);
    } else {
      logger.error("❌ Please provide a caregiver ID (ac_id or source_ac_id)");
      logger.info(`   Usage: node scripts/test_caregiver_prep_logic.js <caregiver_id>`);
      logger.info(`   Example: node scripts/test_caregiver_prep_logic.js 12345`);
      return;
    }

    logger.info("");

    // Phase 1: Get current day in EST (not time)
    const currentDay = new Date();
    const estDate = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(currentDay);
    const estYear = parseInt(estDate.find((p) => p.type === "year").value);
    const estMonth = parseInt(estDate.find((p) => p.type === "month").value) - 1;
    const estDay = parseInt(estDate.find((p) => p.type === "day").value);
    logger.info(`📅 Current day in EST: ${estYear}-${String(estMonth + 1).padStart(2, "0")}-${String(estDay).padStart(2, "0")}`);
    logger.info("");

    const caregiver = testCaregiver;
    const sourceAcId = Number(caregiver.source_ac_id);

    logger.info(`\nProcessing: ${caregiver.name} (ac_id: ${caregiver.ac_id}, source_ac_id: ${sourceAcId})`);
    logger.info("─────────────────────────────────────────────────────────");

    try {
      // Phase 4-5: Fetch scheduled visits (current day 5 AM EST to current day + 6 days at 5 AM EST)
      logger.info(`📅 Fetching scheduled visits...`);
      const scheduledVisitsRaw = await fetchScheduledVisits(sourceAcId, currentDay);
      logger.info(`   Found ${scheduledVisitsRaw.length} scheduled visits`);

      if (!scheduledVisitsRaw || scheduledVisitsRaw.length === 0) {
        logger.info(`   ⏭️  No scheduled visits found - skipping`);
        return;
      }

      // Extract scheduled visit data
      const scheduledVisits = scheduledVisitsRaw.map((visit) => ({
        alayacare_employee_id: visit.alayacare_employee_id,
        alayacare_client_id: visit.alayacare_client_id,
        start_at: visit.start_at,
        end_at: visit.end_at,
      }));

      // Log scheduled visits
      logger.info(`   Scheduled visits:`);
      scheduledVisits.forEach((visit) => {
        logger.info(`     - Client ${visit.alayacare_client_id}, Start: ${visit.start_at}`);
      });

      // Get unique client IDs from scheduled visits
      const uniqueScheduledClients = [...new Set(scheduledVisits.map((v) => v.alayacare_client_id))];
      logger.info(`   Unique clients in scheduled visits: ${uniqueScheduledClients.length} (${uniqueScheduledClients.join(", ")})`);

      // Phase 6-7: Fetch past visits (from 2022-01-01 to current day's 5 AM EST)
      logger.info(`📅 Fetching past visits (2022-01-01 to current day 5 AM EST)...`);
      const pastVisitsRaw = await fetchPastVisits(sourceAcId, currentDay);
      logger.info(`   Found ${pastVisitsRaw.length} total past visits (fetched from all pages)`);

      // Extract past visit data including cancelled field
      const pastVisitsWithCancelled = pastVisitsRaw.map((visit) => ({
        alayacare_employee_id: visit.alayacare_employee_id,
        alayacare_client_id: visit.alayacare_client_id,
        start_at: visit.start_at,
        end_at: visit.end_at,
        cancelled: visit.cancelled,
      }));

      // Filter out cancelled visits (same logic as orchestrator)
      const pastVisits = pastVisitsWithCancelled.filter((visit) => {
        const cancelled = visit.cancelled;
        // Handle boolean false
        if (cancelled === false) return true;
        // Handle string "false" (case-insensitive)
        if (typeof cancelled === "string" && cancelled.toLowerCase() === "false") return true;
        // Handle null/undefined as not cancelled
        if (cancelled === null || cancelled === undefined) return true;
        // Everything else (true, "true", etc.) is considered cancelled
        return false;
      });

      logger.info(`   Non-cancelled past visits: ${pastVisits.length}`);

      // Phase 8-10: Check for new caregiver (first shift with Alvita Care)
      if (pastVisits.length === 0) {
        logger.info(`🆕 NEW CAREGIVER DETECTED: No past visits found`);
        
        // Find earliest scheduled visit
        if (scheduledVisits.length > 0) {
          const earliestVisit = scheduledVisits.reduce((earliest, visit) => {
            const visitDate = new Date(visit.start_at);
            const earliestDate = new Date(earliest.start_at);
            return visitDate < earliestDate ? visit : earliest;
          });

          logger.info(`   Earliest scheduled visit: Client ${earliestVisit.alayacare_client_id}, Start: ${earliestVisit.start_at}`);

          // Phase 9: Check if ticket already exists
          logger.info(`🔍 Checking if new caregiver ticket already exists...`);
          const ticketExists = await checkNewCaregiverTicketExists(sourceAcId);
          
          if (ticketExists) {
            logger.info(`   ℹ️  Ticket already exists for new caregiver (dedup key: new_cg_caregiver_${sourceAcId})`);
          } else {
            // Phase 10: Would create new caregiver prep call ticket
            const client = getClientByAlayacareId(earliestVisit.alayacare_client_id);
            const clientName = client ? client.name : `Client ${earliestVisit.alayacare_client_id}`;
            const firstShiftDate = new Date(earliestVisit.start_at);
            const dueDate = calculateDueDate(firstShiftDate);
            
            // Get assignee group from client's coordinator pod
            const assigneeGroupId = client?.coordinator_pod 
              ? getAssigneeGroupId(client.coordinator_pod)
              : null;

            logger.info(`   📝 WOULD CREATE NEW CAREGIVER TICKET:`);
            logger.info(`      Subject: New caregiver prep call - ${caregiver.name}`);
            logger.info(`      Client: ${clientName}`);
            logger.info(`      Date of first shift: ${formatDateForDisplay(firstShiftDate)}`);
            logger.info(`      Due date: ${dueDate}`);
            logger.info(`      Assignee group: ${assigneeGroupId || "None"}`);
            logger.info(`      Requester ID: ${caregiver.zendesk_user_id}`);
            logger.info(`      Dedup key: new_cg_caregiver_${sourceAcId}`);
          }
        }
      } else {
        // Log sample past visits
        const samplePastVisits = pastVisits.slice(0, 5);
        logger.info(`   Sample past visits (first 5):`);
        samplePastVisits.forEach((visit) => {
          logger.info(`     - Client ${visit.alayacare_client_id}, Start: ${visit.start_at}`);
        });

        // Get unique client IDs from past visits
        const uniquePastClients = [...new Set(pastVisits.map((v) => v.alayacare_client_id))];
        logger.info(`   Unique clients in past visits: ${uniquePastClients.length} (${uniquePastClients.join(", ")})`);

        // Phase 11-13: Check for new caregiver-client matches
        logger.info(`🔍 Checking for new caregiver-client matches...`);
        logger.info(`   Comparing scheduled clients with past visit history...`);
        const newClientMatches = [];

        for (const clientId of uniqueScheduledClients) {
          // Convert both to numbers for reliable comparison
          const clientIdNum = Number(clientId);
          logger.info(`\n   Checking scheduled client: ${clientIdNum}`);
          logger.info(`   Searching through ${pastVisits.length} past visits for client ${clientIdNum}...`);
          
          // Find all matching past visits for detailed logging
          const matchingPastVisits = pastVisits.filter((visit) => {
            const visitClientIdNum = Number(visit.alayacare_client_id);
            const matches = visitClientIdNum === clientIdNum;
            
            if (matches) {
              logger.info(`     ✓ Found matching past visit: Client ${visitClientIdNum}, Start: ${visit.start_at}`);
            }
            
            return matches;
          });

          const hasWorkedWithClient = matchingPastVisits.length > 0;

          if (!hasWorkedWithClient) {
            logger.info(`     🆕 NEW MATCH: Client ${clientIdNum} - no past visits found (checked ${pastVisits.length} past visits)`);
            newClientMatches.push(clientIdNum);
          } else {
            logger.info(`     ✓ EXISTING: Client ${clientIdNum} - found ${matchingPastVisits.length} past visit(s) with this client`);
            // Log summary of matching visits
            if (matchingPastVisits.length > 0) {
              const latestMatch = matchingPastVisits.reduce((latest, visit) => {
                const visitDate = new Date(visit.start_at);
                const latestDate = new Date(latest.start_at);
                return visitDate > latestDate ? visit : latest;
              });
              logger.info(`        Most recent past visit: ${latestMatch.start_at}`);
            }
          }
        }

        logger.info(`📊 Summary:`);
        logger.info(`   Scheduled clients: ${uniqueScheduledClients.length}`);
        logger.info(`   Past clients: ${uniquePastClients.length}`);
        logger.info(`   New client matches: ${newClientMatches.length}`);

        // Process each new client match
        for (const clientId of newClientMatches) {
          logger.info(`\n   Processing new match: Client ${clientId}`);
          
          // Check if ticket already exists
          logger.info(`   🔍 Checking if ticket already exists...`);
          const ticketExists = await checkNewCaregiverClientMatchTicketExists(sourceAcId, clientId);
          
          if (ticketExists) {
            logger.info(`      ℹ️  Ticket already exists (dedup key: new_cg_client_match_caregiver_${sourceAcId}_client_${clientId})`);
          } else {
            // Find the earliest scheduled visit with this client
            const clientVisits = scheduledVisits.filter(
              (visit) => visit.alayacare_client_id === clientId
            );
            const earliestClientVisit = clientVisits.reduce((earliest, visit) => {
              const visitDate = new Date(visit.start_at);
              const earliestDate = new Date(earliest.start_at);
              return visitDate < earliestDate ? visit : earliest;
            });

            const client = getClientByAlayacareId(clientId);
            const clientName = client ? client.name : `Client ${clientId}`;
            const firstShiftDate = new Date(earliestClientVisit.start_at);
            const dueDate = calculateDueDate(firstShiftDate);

            // Get assignee group from client's coordinator pod
            const assigneeGroupId = client?.coordinator_pod
              ? getAssigneeGroupId(client.coordinator_pod)
              : null;

            logger.info(`      📝 WOULD CREATE NEW CLIENT MATCH TICKET:`);
            logger.info(`         Subject: New caregiver-client match prep call - ${caregiver.name}`);
            logger.info(`         Client: ${clientName}`);
            logger.info(`         Date of first shift: ${formatDateForDisplay(firstShiftDate)}`);
            logger.info(`         Due date: ${dueDate}`);
            logger.info(`         Assignee group: ${assigneeGroupId || "None"}`);
            logger.info(`         Requester ID: ${caregiver.zendesk_user_id}`);
            logger.info(`         Dedup key: new_cg_client_match_caregiver_${sourceAcId}_client_${clientId}`);
          }
        }

        if (newClientMatches.length === 0) {
          logger.info(`   ✅ No new client matches - all scheduled clients have past visits`);
        }
      }

    } catch (error) {
      logger.error(`   ❌ Error processing ${caregiver.name}: ${error.message}`);
      logger.error(`   Stack: ${error.stack}`);
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

// Get caregiver ID from command line arguments
const caregiverId = process.argv[2];

// Run the test
testCaregiverPrepLogic(caregiverId)
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });

