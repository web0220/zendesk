import { logger } from "../config/logger.js";
import { getActiveCaregiversForPrepCalls, getClientByAlayacareId } from "../infra/db.recurring.repo.js";
import { fetchScheduledVisits, fetchPastVisits } from "../services/alayacare/visit.api.js";
import { searchTickets } from "../services/zendesk/zendesk.api.js";
import { createPrivateTaskTicket, createTicketsBatch } from "../services/zendesk/ticket.js";
import { runWithLimit } from "../utils/concurrency.js";

// Zendesk custom field IDs from environment variables
// These should be set in .env file
const CG_PREP_FIELD_ID = process.env.ZENDESK_CG_PREP_FIELD_ID || null;
const CG_PREP_DEDUP_KEY_FIELD_ID = process.env.ZENDESK_CG_PREP_DEDUP_KEY_FIELD_ID || null;

// Excluded caregiver IDs (fake accounts)
const EXCLUDED_CAREGIVER_IDS = [618, 619, 3088, 4844, 5325, 1744, 1550, 5218, 5206, 5583, 626, 628, 203];

// Excluded client IDs (fake accounts)
const EXCLUDED_CLIENT_IDS = [1096, 1320, 5265];

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

  // Map coordinator_pod to Zendesk group IDs
  // This mapping should be configured in environment variables
  // Example: ZENDESK_POD_GROUP_MAPPING='{"Pod A": 123456, "Pod B": 123457}'
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
    return false; // Assume no ticket exists on error to avoid blocking creation
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
    return false; // Assume no ticket exists on error to avoid blocking creation
  }
}

/**
 * Create a new caregiver-client match ticket configuration
 * @param {Object} caregiver - Caregiver record
 * @param {number} sourceAcId - Caregiver's source AlayaCare ID
 * @param {number} clientId - Client's AlayaCare ID
 * @param {Date} firstShiftDate - Date of first shift with this client
 * @returns {Object} Ticket configuration object
 */
function createNewClientMatchTicketConfig(caregiver, sourceAcId, clientId, firstShiftDate) {
  const client = getClientByAlayacareId(clientId);
  const clientName = client ? client.name : `Client ${clientId}`;
  const dueDate = calculateDueDate(firstShiftDate);

  // Get assignee group from client's coordinator pod
  const assigneeGroupId = client?.coordinator_pod
    ? getAssigneeGroupId(client.coordinator_pod)
    : null;

  const customFields = [];
  if (CG_PREP_FIELD_ID) {
    customFields.push({
      id: Number(CG_PREP_FIELD_ID),
      value: "cg_prep_-_new_cg-client_match_coordination",
    });
  }
  if (CG_PREP_DEDUP_KEY_FIELD_ID) {
    customFields.push({
      id: Number(CG_PREP_DEDUP_KEY_FIELD_ID),
      value: `new_cg_client_match_caregiver_${sourceAcId}_client_${clientId}`,
    });
  }

  const subject = `New caregiver-client match prep call - ${caregiver.name}`;
  const commentBody = `<h3 style="margin-top: 0;">New caregiver-client match prep call</h3><br>

<strong>CG name:</strong> ${caregiver.name}<br><br>

<strong>Client name:</strong> ${clientName}<br><br>

<strong>Date of first shift:</strong> ${formatDateForDisplay(firstShiftDate)}`;

  return {
    requesterId: caregiver.zendesk_user_id,
    subject,
    dueAt: dueDate,
    commentBody,
    tags: ["new_cg-client_match", "cg_prep_-_new_cg-client_match_coordination"],
    groupId: assigneeGroupId,
    customFields,
    caregiverName: caregiver.name,
    clientName,
    firstShiftDate: formatDateForDisplay(firstShiftDate),
  };
}

/**
 * Create a check-in ticket configuration from an original prep call ticket
 * Check-in ticket is due 2 days after the original ticket's due date
 * @param {Object} originalTicketConfig - Original ticket configuration
 * @returns {Object} Check-in ticket configuration object
 */
function createCheckInTicketConfig(originalTicketConfig) {
  // Calculate check-in due date (2 days after original due date)
  const originalDueDate = new Date(originalTicketConfig.dueAt);
  const checkInDueDate = new Date(originalDueDate);
  checkInDueDate.setDate(checkInDueDate.getDate() + 2);
  // Keep the same time (end of day)
  checkInDueDate.setHours(23, 59, 59, 999);

  // Create check-in subject by replacing "prep call" with "check-in call"
  let checkInSubject = originalTicketConfig.subject;
  checkInSubject = checkInSubject.replace("New caregiver prep call", "New caregiver check-in call");
  checkInSubject = checkInSubject.replace("New caregiver-client match prep call", "New caregiver-client match check-in call");

  // Create check-in comment body (similar but indicating it's a check-in)
  let checkInCommentBody = originalTicketConfig.commentBody;
  checkInCommentBody = checkInCommentBody.replace(
    /<h3[^>]*>New caregiver prep call<\/h3>/,
    '<h3 style="margin-top: 0;">New caregiver check-in call</h3>'
  );
  checkInCommentBody = checkInCommentBody.replace(
    /<h3[^>]*>New caregiver-client match prep call<\/h3>/,
    '<h3 style="margin-top: 0;">New caregiver-client match check-in call</h3>'
  );

  // Create check-in tags (add check-in tag)
  const checkInTags = [...originalTicketConfig.tags, "check-in"];

  return {
    requesterId: originalTicketConfig.requesterId,
    subject: checkInSubject,
    dueAt: checkInDueDate.toISOString(),
    commentBody: checkInCommentBody,
    tags: checkInTags,
    groupId: originalTicketConfig.groupId,
    customFields: originalTicketConfig.customFields,
    caregiverName: originalTicketConfig.caregiverName,
    clientName: originalTicketConfig.clientName,
    firstShiftDate: originalTicketConfig.firstShiftDate,
  };
}

/**
 * Process a single caregiver for prep call tickets
 * @param {Object} caregiver - Caregiver record from database
 * @param {Date} currentDay - Current date (used to get EST day, time is ignored)
 * @returns {Promise<Object>} Result object with created tickets
 */
async function processCaregiver(caregiver, currentDay) {
  const result = {
    caregiverId: caregiver.ac_id,
    caregiverName: caregiver.name,
    sourceAcId: caregiver.source_ac_id,
    newCaregiverTickets: [],
    newClientMatchTickets: [],
    errors: [],
  };

  try {
    const sourceAcId = Number(caregiver.source_ac_id);
    if (!sourceAcId) {
      logger.warn(`⚠️ Skipping caregiver ${caregiver.ac_id}: invalid source_ac_id`);
      result.errors.push("Invalid source_ac_id");
      return result;
    }

    // Phase 4-5: Fetch scheduled visits (current day 5 AM EST to current day + 6 days at 5 AM EST)
    logger.debug(`📅 Fetching scheduled visits for caregiver ${caregiver.name} (${sourceAcId})`);
    const scheduledVisitsRaw = await fetchScheduledVisits(sourceAcId, currentDay);
    
    if (!scheduledVisitsRaw || scheduledVisitsRaw.length === 0) {
      logger.debug(`   ℹ️  No scheduled visits found for caregiver ${caregiver.name}`);
      return result;
    }

    // Extract scheduled visit data
    const scheduledVisits = scheduledVisitsRaw.map((visit) => ({
      alayacare_employee_id: visit.alayacare_employee_id,
      alayacare_client_id: visit.alayacare_client_id,
      start_at: visit.start_at,
      end_at: visit.end_at,
    }));

    // Phase 6-7: Fetch past visits (from 2022-01-01 to current day's 5 AM EST)
    logger.debug(`📅 Fetching past visits for caregiver ${caregiver.name} (${sourceAcId})`);
    const pastVisitsRaw = await fetchPastVisits(sourceAcId, currentDay);
    
    // Phase 7: Extract past visit data including cancelled field
    const pastVisitsWithCancelled = pastVisitsRaw.map((visit) => ({
      alayacare_employee_id: visit.alayacare_employee_id,
      alayacare_client_id: visit.alayacare_client_id,
      start_at: visit.start_at,
      end_at: visit.end_at,
      cancelled: visit.cancelled,
    }));

    // Filter out cancelled visits (remove all rows where cancelled is true)
    // Handle both boolean and string values: true, "true", "True", etc.
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

    logger.debug(`   Found ${pastVisitsRaw.length} total past visits, ${pastVisits.length} non-cancelled past visits`);

    // Phase 8-10: Check for new caregiver (first shift with Alvita Care)
    if (pastVisits.length === 0) {
      logger.info(`🆕 New caregiver detected: ${caregiver.name} (${sourceAcId})`);
      
      if (scheduledVisits.length > 0) {
        // Find earliest scheduled visit for type 1 ticket
        const earliestVisit = scheduledVisits.reduce((earliest, visit) => {
          const visitDate = new Date(visit.start_at);
          const earliestDate = new Date(earliest.start_at);
          return visitDate < earliestDate ? visit : earliest;
        });

        const firstClientId = earliestVisit.alayacare_client_id;

        // Phase 9: Check if type 1 ticket already exists
        const ticketExists = await checkNewCaregiverTicketExists(sourceAcId);
        
        if (!ticketExists) {
          // Check if client is excluded (fake account)
          const firstClientIdNum = Number(firstClientId);
          if (EXCLUDED_CLIENT_IDS.includes(firstClientIdNum)) {
            logger.debug(`   ⏭️  Skipping ticket creation for client ${firstClientIdNum} - fake account`);
          } else {
            // Phase 10: Create new caregiver prep call ticket (type 1) for first client
            const client = getClientByAlayacareId(firstClientId);
          const clientName = client ? client.name : `Client ${firstClientId}`;
          const firstShiftDate = new Date(earliestVisit.start_at);
          const dueDate = calculateDueDate(firstShiftDate);
          
          // Get assignee group from client's coordinator pod
          const assigneeGroupId = client?.coordinator_pod 
            ? getAssigneeGroupId(client.coordinator_pod)
            : null;

          const customFields = [];
          if (CG_PREP_FIELD_ID) {
            customFields.push({
              id: Number(CG_PREP_FIELD_ID),
              value: "cg_prep_-_new_cg_coordination",
            });
          }
          if (CG_PREP_DEDUP_KEY_FIELD_ID) {
            customFields.push({
              id: Number(CG_PREP_DEDUP_KEY_FIELD_ID),
              value: `new_cg_caregiver_${sourceAcId}`,
            });
          }
          const subject = `New caregiver prep call - ${caregiver.name}`;
          const commentBody = `<h3 style="margin-top: 0;">New caregiver prep call</h3><br>

<strong>CG name:</strong> ${caregiver.name}<br><br>

<strong>Client:</strong> ${clientName}<br><br>

<strong>Date of first shift:</strong> ${formatDateForDisplay(firstShiftDate)}`;

          const newCaregiverTicket = {
            requesterId: caregiver.zendesk_user_id,
            subject,
            dueAt: dueDate,
            commentBody,
            tags: ["new_cg", "cg_prep_-_new_cg_coordination"],
            groupId: assigneeGroupId,
            customFields,
            caregiverName: caregiver.name,
            clientName,
            firstShiftDate: formatDateForDisplay(firstShiftDate),
          };

          // Add original ticket
          result.newCaregiverTickets.push(newCaregiverTicket);

          // Add check-in ticket (due 2 days after original)
          const checkInTicket = createCheckInTicketConfig(newCaregiverTicket);
          result.newCaregiverTickets.push(checkInTicket);
          }
        } else {
          logger.debug(`   ℹ️  Ticket already exists for new caregiver ${caregiver.name}`);
        }

        // For new caregivers, also check for other clients in scheduled visits
        // Create type 2 tickets (new caregiver-client match) for all other unique clients
        const uniqueClientIds = [...new Set(scheduledVisits.map((v) => v.alayacare_client_id))];
        const otherClientIds = uniqueClientIds.filter((clientId) => {
          const clientIdNum = Number(clientId);
          const firstClientIdNum = Number(firstClientId);
          return clientIdNum !== firstClientIdNum;
        });

        logger.debug(`   Found ${otherClientIds.length} other unique client(s) in scheduled visits for new caregiver`);

        // Create type 2 tickets for other clients
        for (const clientId of otherClientIds) {
          // Check if client is excluded (fake account)
          const clientIdNum = Number(clientId);
          if (EXCLUDED_CLIENT_IDS.includes(clientIdNum)) {
            logger.debug(`   ⏭️  Skipping ticket creation for client ${clientIdNum} - fake account`);
            continue;
          }

          // Check if ticket already exists
          const ticketExists = await checkNewCaregiverClientMatchTicketExists(sourceAcId, clientId);

          if (!ticketExists) {
            // Find the earliest scheduled visit with this client
            const clientVisits = scheduledVisits.filter(
              (visit) => visit.alayacare_client_id === clientId
            );
            const earliestClientVisit = clientVisits.reduce((earliest, visit) => {
              const visitDate = new Date(visit.start_at);
              const earliestDate = new Date(earliest.start_at);
              return visitDate < earliestDate ? visit : earliest;
            });

            const firstShiftDate = new Date(earliestClientVisit.start_at);

            logger.info(
              `🆕 New caregiver-client match (for new caregiver): ${caregiver.name} (${sourceAcId}) with client ${clientId}`
            );

            const ticketConfig = createNewClientMatchTicketConfig(
              caregiver,
              sourceAcId,
              clientId,
              firstShiftDate
            );
            // Add original ticket
            result.newClientMatchTickets.push(ticketConfig);

            // Add check-in ticket (due 2 days after original)
            const checkInTicket = createCheckInTicketConfig(ticketConfig);
            result.newClientMatchTickets.push(checkInTicket);
          } else {
            logger.debug(
              `   ℹ️  Ticket already exists for caregiver-client match: ${caregiver.name} with client ${clientId}`
            );
          }
        }
      }
    }

    // Phase 11-13: Check for new caregiver-client matches
    if (pastVisits.length > 0) {
      // Get unique client IDs from scheduled visits
      const uniqueClientIds = [...new Set(scheduledVisits.map((v) => v.alayacare_client_id))];

      for (const clientId of uniqueClientIds) {
        // Check if client is excluded (fake account)
        const clientIdNum = Number(clientId);
        if (EXCLUDED_CLIENT_IDS.includes(clientIdNum)) {
          logger.debug(`   ⏭️  Skipping ticket creation for client ${clientIdNum} - fake account`);
          continue;
        }

        // Check if caregiver has worked with this client before
        // Convert both to numbers for reliable comparison (handles string vs number mismatch)
        const hasWorkedWithClient = pastVisits.some((visit) => {
          const visitClientIdNum = Number(visit.alayacare_client_id);
          return visitClientIdNum === clientIdNum;
        });

        if (!hasWorkedWithClient) {
          // This is a new caregiver-client match
          logger.info(
            `🆕 New caregiver-client match: ${caregiver.name} (${sourceAcId}) with client ${clientId}`
          );

          // Check if ticket already exists
          const ticketExists = await checkNewCaregiverClientMatchTicketExists(sourceAcId, clientId);

          if (!ticketExists) {
            // Find the earliest scheduled visit with this client
            const clientVisits = scheduledVisits.filter(
              (visit) => visit.alayacare_client_id === clientId
            );
            const earliestClientVisit = clientVisits.reduce((earliest, visit) => {
              const visitDate = new Date(visit.start_at);
              const earliestDate = new Date(earliest.start_at);
              return visitDate < earliestDate ? visit : earliest;
            });

            const firstShiftDate = new Date(earliestClientVisit.start_at);

            const ticketConfig = createNewClientMatchTicketConfig(
              caregiver,
              sourceAcId,
              clientId,
              firstShiftDate
            );
            // Add original ticket
            result.newClientMatchTickets.push(ticketConfig);

            // Add check-in ticket (due 2 days after original)
            const checkInTicket = createCheckInTicketConfig(ticketConfig);
            result.newClientMatchTickets.push(checkInTicket);
          } else {
            logger.debug(
              `   ℹ️  Ticket already exists for caregiver-client match: ${caregiver.name} with client ${clientId}`
            );
          }
        }
      }
    }
  } catch (error) {
    logger.error(`❌ Error processing caregiver ${caregiver.name}: ${error.message}`);
    result.errors.push(error.message);
  }

  return result;
}

/**
 * Main orchestrator function for caregiver prep call tickets
 * @returns {Promise<Object>} Summary of ticket creation results
 */
export async function runCaregiverPrepCallTickets() {
  const startedAt = new Date().toISOString();
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📋 Caregiver Prep Call Ticket Job");
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info(`🕒 Job started at ${startedAt}`);

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

  // Check required configuration
  if (!CG_PREP_FIELD_ID) {
    logger.warn("⚠️ ZENDESK_CG_PREP_FIELD_ID not configured");
  }
  if (!CG_PREP_DEDUP_KEY_FIELD_ID) {
    logger.warn("⚠️ ZENDESK_CG_PREP_DEDUP_KEY_FIELD_ID not configured");
  }

  const results = {
    startedAt,
    finishedAt: null,
    totalCaregivers: 0,
    newCaregiverTickets: {
      total: 0,
      created: 0,
      failed: 0,
    },
    newClientMatchTickets: {
      total: 0,
      created: 0,
      failed: 0,
    },
    errors: [],
  };

  try {
    // Phase 2: Fetch all active caregivers
    logger.info("📋 Phase 2: Fetching active caregivers from database...");
    const allCaregivers = getActiveCaregiversForPrepCalls();
    
    // Filter out excluded caregiver IDs (fake accounts)
    const caregivers = allCaregivers.filter((caregiver) => {
      const sourceAcId = Number(caregiver.source_ac_id);
      const isExcluded = EXCLUDED_CAREGIVER_IDS.includes(sourceAcId);
      if (isExcluded) {
        logger.debug(`   ⏭️  Excluding caregiver ${caregiver.name} (source_ac_id: ${sourceAcId}) - fake account`);
      }
      return !isExcluded;
    });
    
    const excludedCount = allCaregivers.length - caregivers.length;
    if (excludedCount > 0) {
      logger.info(`   ⏭️  Excluded ${excludedCount} caregiver(s) (fake accounts: ${EXCLUDED_CAREGIVER_IDS.join(", ")})`);
    }
    
    results.totalCaregivers = caregivers.length;

    if (caregivers.length === 0) {
      logger.info("✅ No active caregivers found. Skipping prep call tickets.");
      results.finishedAt = new Date().toISOString();
      return results;
    }

    logger.info(`📊 Found ${caregivers.length} active caregivers to process`);

    // Phase 3: Process each caregiver
    logger.info("📋 Phase 3: Processing caregivers...");
    const CAREGIVER_CONCURRENCY = Number(process.env.CAREGIVER_PREP_CALL_CONCURRENCY) || 5;
    
    const processTasks = caregivers.map((caregiver) => async () => 
      processCaregiver(caregiver, currentDay)
    );

    const caregiverResults = await runWithLimit(processTasks, CAREGIVER_CONCURRENCY);

    // Collect all tickets to create
    const newCaregiverTicketConfigs = [];
    const newClientMatchTicketConfigs = [];

    for (const caregiverResult of caregiverResults) {
      if (caregiverResult.errors.length > 0) {
        results.errors.push({
          caregiver: caregiverResult.caregiverName,
          errors: caregiverResult.errors,
        });
      }

      newCaregiverTicketConfigs.push(...caregiverResult.newCaregiverTickets);
      newClientMatchTicketConfigs.push(...caregiverResult.newClientMatchTickets);
    }

    results.newCaregiverTickets.total = newCaregiverTicketConfigs.length;
    results.newClientMatchTickets.total = newClientMatchTicketConfigs.length;

    logger.info(
      `📦 Preparing to create ${newCaregiverTicketConfigs.length} new caregiver tickets and ${newClientMatchTicketConfigs.length} new client match tickets`
    );

    // Create tickets in batches
    const TICKET_CONCURRENCY = Number(process.env.ZENDESK_TICKET_CONCURRENCY) || 5;

    if (newCaregiverTicketConfigs.length > 0) {
      logger.info("📋 Creating new caregiver prep call tickets...");
      const newCaregiverResults = await createTicketsBatch(
        newCaregiverTicketConfigs,
        TICKET_CONCURRENCY
      );
      results.newCaregiverTickets.created = newCaregiverResults.filter(
        (r) => r.success && r.ticket
      ).length;
      results.newCaregiverTickets.failed = newCaregiverResults.filter((r) => !r.success).length;
    }

    if (newClientMatchTicketConfigs.length > 0) {
      logger.info("📋 Creating new caregiver-client match prep call tickets...");
      const newClientMatchResults = await createTicketsBatch(
        newClientMatchTicketConfigs,
        TICKET_CONCURRENCY
      );
      results.newClientMatchTickets.created = newClientMatchResults.filter(
        (r) => r.success && r.ticket
      ).length;
      results.newClientMatchTickets.failed = newClientMatchResults.filter(
        (r) => !r.success
      ).length;
    }

    results.finishedAt = new Date().toISOString();

    // Final summary
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info("📊 CAREGIVER PREP CALL TICKET JOB SUMMARY");
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info(`   📋 Total caregivers processed: ${results.totalCaregivers}`);
    logger.info(`   🆕 New caregiver tickets: ${results.newCaregiverTickets.created} created, ${results.newCaregiverTickets.failed} failed`);
    logger.info(`   🔗 New client match tickets: ${results.newClientMatchTickets.created} created, ${results.newClientMatchTickets.failed} failed`);
    logger.info(`   ⏱️  Duration: ${new Date(results.finishedAt) - new Date(results.startedAt)}ms`);
    if (results.errors.length > 0) {
      logger.warn(`   ⚠️  Errors: ${results.errors.length} caregivers had errors`);
    }
    logger.info("═══════════════════════════════════════════════════════════");

    return results;
  } catch (error) {
    results.finishedAt = new Date().toISOString();
    logger.error(`❌ Caregiver prep call ticket job failed: ${error.message}`, error);
    throw error;
  }
}

