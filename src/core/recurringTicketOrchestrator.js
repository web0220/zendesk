import { logger } from "../config/logger.js";
import {
  getActiveClientsForCoordinationCheckIn,
  getActiveConciergeClients,
  getActivePremiumClients,
} from "../infra/db.recurring.repo.js";
import {
  createPrivateTaskTicket,
  createTicketsBatch,
  getLastDayOfMonth,
  getFridayOfCurrentWeek,
  getZendeskUserData,
} from "../services/zendesk/ticket.js";

// Contact Category field ID from environment
// Default: 44036422587803 (Coordination Contact Category field ID found via list:fields script)
const CONTACT_CATEGORY_FIELD_ID =
  process.env.ZENDESK_CONTACT_CATEGORY_FIELD_ID || "44036422587803";

/**
 * Task 1: Create coordination monthly check-in tickets
 * All active clients, due on last day of month
 */
async function createCoordinationMonthlyTickets() {
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📋 Task 1: Coordination Monthly Check-In Tickets");
  logger.info("═══════════════════════════════════════════════════════════");

  const clients = getActiveClientsForCoordinationCheckIn();

  if (clients.length === 0) {
    logger.info("✅ No active clients found. Skipping coordination check-in tickets.");
    return {
      task: "coordination_monthly",
      total: 0,
      created: 0,
      failed: 0,
      tickets: [],
      errors: [],
    };
  }

  logger.info(`📊 Found ${clients.length} active clients for coordination check-in`);

  const dueAt = getLastDayOfMonth();
  logger.info(`📅 Due date: ${dueAt} (last day of month)`);

  const ticketConfigs = [];
  const errors = [];

  // Prepare ticket configurations
  for (const client of clients) {
    if (!client.zendesk_user_id) {
      logger.warn(
        `⚠️  Skipping client ${client.ac_id} (${client.name}): no zendesk_user_id`
      );
      errors.push({
        client: client.name,
        ac_id: client.ac_id,
        error: "Missing zendesk_user_id",
      });
      continue;
    }

    const subject = `Coordination client monthly check-in - ${client.name || "Unknown"}`;
    const contactCategoryValue = "check_in_-_monthly_coordination";

    ticketConfigs.push({
      requesterId: client.zendesk_user_id,
      subject,
      dueAt,
      contactCategoryValue,
      contactCategoryFieldId: CONTACT_CATEGORY_FIELD_ID,
      clientName: client.name,
      clientAcId: client.ac_id,
      commentBody: "monthly coordination client check-in",
    });
  }

  logger.info(`📦 Preparing to create ${ticketConfigs.length} coordination check-in tickets`);

  // Create tickets with concurrency control
  const TICKET_CONCURRENCY = Number(process.env.ZENDESK_TICKET_CONCURRENCY) || 5;
  const results = await createTicketsBatch(ticketConfigs, TICKET_CONCURRENCY);

  const created = results.filter((r) => r.success && r.ticket).length;
  const failed = results.filter((r) => !r.success).length;

  // Log failures
  results
    .filter((r) => !r.success)
    .forEach((r) => {
      logger.error(
        `❌ Failed to create ticket for ${r.config.clientName} (${r.config.clientAcId}): ${r.error}`
      );
      errors.push({
        client: r.config.clientName,
        ac_id: r.config.clientAcId,
        error: r.error,
      });
    });

  logger.info(
    `✅ Coordination monthly check-in tickets: ${created} created, ${failed} failed`
  );

  return {
    task: "coordination_monthly",
    total: ticketConfigs.length,
    created,
    failed,
    tickets: results.filter((r) => r.success && r.ticket).map((r) => r.ticket),
    errors,
  };
}

/**
 * Task 2: Create clinical weekly check-in tickets for concierge clients
 * Active concierge clients only, due on Friday of current week
 */
async function createClinicalWeeklyConciergeTickets() {
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📋 Task 2: Clinical Weekly Check-In Tickets (Concierge)");
  logger.info("═══════════════════════════════════════════════════════════");

  const clients = getActiveConciergeClients();

  if (clients.length === 0) {
    logger.info(
      "✅ No active concierge clients found. Skipping clinical weekly check-in tickets."
    );
    return {
      task: "clinical_weekly_concierge",
      total: 0,
      created: 0,
      failed: 0,
      tickets: [],
      errors: [],
    };
  }

  logger.info(
    `📊 Found ${clients.length} active concierge clients for clinical weekly check-in`
  );

  const dueAt = getFridayOfCurrentWeek();
  logger.info(`📅 Due date: ${dueAt} (Friday of current week)`);

  const ticketConfigs = [];
  const errors = [];

  // Prepare ticket configurations
  for (const client of clients) {
    if (!client.zendesk_user_id) {
      logger.warn(
        `⚠️  Skipping concierge client ${client.ac_id} (${client.name}): no zendesk_user_id`
      );
      errors.push({
        client: client.name,
        ac_id: client.ac_id,
        error: "Missing zendesk_user_id",
      });
      continue;
    }

    const subject = `Clinical concierge client weekly check-in - ${client.name || "Unknown"}`;
    const contactCategoryValue = "clinical_check_in_-_weekly_concierge_coordination";

    ticketConfigs.push({
      requesterId: client.zendesk_user_id,
      subject,
      dueAt,
      contactCategoryValue,
      contactCategoryFieldId: CONTACT_CATEGORY_FIELD_ID,
      clientName: client.name,
      clientAcId: client.ac_id,
      commentBody: "weekly clinical concierge client check-in",
    });
  }

  logger.info(
    `📦 Preparing to create ${ticketConfigs.length} clinical weekly concierge check-in tickets`
  );

  // Create tickets with concurrency control
  const TICKET_CONCURRENCY = Number(process.env.ZENDESK_TICKET_CONCURRENCY) || 5;
  const results = await createTicketsBatch(ticketConfigs, TICKET_CONCURRENCY);

  const created = results.filter((r) => r.success && r.ticket).length;
  const failed = results.filter((r) => !r.success).length;

  // Log failures
  results
    .filter((r) => !r.success)
    .forEach((r) => {
      logger.error(
        `❌ Failed to create ticket for ${r.config.clientName} (${r.config.clientAcId}): ${r.error}`
      );
      errors.push({
        client: r.config.clientName,
        ac_id: r.config.clientAcId,
        error: r.error,
      });
    });

  logger.info(
    `✅ Clinical weekly concierge check-in tickets: ${created} created, ${failed} failed`
  );

  return {
    task: "clinical_weekly_concierge",
    total: ticketConfigs.length,
    created,
    failed,
    tickets: results.filter((r) => r.success && r.ticket).map((r) => r.ticket),
    errors,
  };
}

/**
 * Task 3: Create clinical monthly check-in tickets for premium clients
 * Active premium clients only, due on last day of month
 */
async function createClinicalMonthlyPremiumTickets() {
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📋 Task 3: Clinical Monthly Check-In Tickets (Premium)");
  logger.info("═══════════════════════════════════════════════════════════");

  const clients = getActivePremiumClients();

  if (clients.length === 0) {
    logger.info(
      "✅ No active premium clients found. Skipping clinical monthly check-in tickets."
    );
    return {
      task: "clinical_monthly_premium",
      total: 0,
      created: 0,
      failed: 0,
      tickets: [],
      errors: [],
    };
  }

  logger.info(
    `📊 Found ${clients.length} active premium clients for clinical monthly check-in`
  );

  const dueAt = getLastDayOfMonth();
  logger.info(`📅 Due date: ${dueAt} (last day of month)`);

  const ticketConfigs = [];
  const errors = [];

  // Prepare ticket configurations
  for (const client of clients) {
    if (!client.zendesk_user_id) {
      logger.warn(
        `⚠️  Skipping premium client ${client.ac_id} (${client.name}): no zendesk_user_id`
      );
      errors.push({
        client: client.name,
        ac_id: client.ac_id,
        error: "Missing zendesk_user_id",
      });
      continue;
    }

    const subject = `Clinical premium client monthly check-in - ${client.name || "Unknown"}`;
    const contactCategoryValue = "clinical_check_in_-_monthly_premium_coordination";

    ticketConfigs.push({
      requesterId: client.zendesk_user_id,
      subject,
      dueAt,
      contactCategoryValue,
      contactCategoryFieldId: CONTACT_CATEGORY_FIELD_ID,
      clientName: client.name,
      clientAcId: client.ac_id,
      commentBody: "monthly clinical premium client check-in",
    });
  }

  logger.info(
    `📦 Preparing to create ${ticketConfigs.length} clinical monthly premium check-in tickets`
  );

  // Create tickets with concurrency control
  const TICKET_CONCURRENCY = Number(process.env.ZENDESK_TICKET_CONCURRENCY) || 5;
  const results = await createTicketsBatch(ticketConfigs, TICKET_CONCURRENCY);

  const created = results.filter((r) => r.success && r.ticket).length;
  const failed = results.filter((r) => !r.success).length;

  // Log failures
  results
    .filter((r) => !r.success)
    .forEach((r) => {
      logger.error(
        `❌ Failed to create ticket for ${r.config.clientName} (${r.config.clientAcId}): ${r.error}`
      );
      errors.push({
        client: r.config.clientName,
        ac_id: r.config.clientAcId,
        error: r.error,
      });
    });

  logger.info(
    `✅ Clinical monthly premium check-in tickets: ${created} created, ${failed} failed`
  );

  return {
    task: "clinical_monthly_premium",
    total: ticketConfigs.length,
    created,
    failed,
    tickets: results.filter((r) => r.success && r.ticket).map((r) => r.ticket),
    errors,
  };
}

/**
 * Main orchestrator function
 * Determines which tasks to run based on command line arguments or runs all
 * 
 * @param {Object} options
 * @param {string|null} options.task - Specific task to run: 'coordination', 'concierge', 'premium', or null for all
 * @returns {Promise<Object>} Summary of all ticket creation results
 */
export async function runRecurringTickets({ task = null } = {}) {
  const startedAt = new Date().toISOString();
  logger.info(`🕒 Recurring ticket job started at ${startedAt}`);

  const results = {
    startedAt,
    finishedAt: null,
    tasks: [],
    summary: {
      totalClients: 0,
      totalTicketsCreated: 0,
      totalTicketsFailed: 0,
    },
  };

  try {
    // Run specific task or all tasks
    if (task === "coordination") {
      const result = await createCoordinationMonthlyTickets();
      results.tasks.push(result);
    } else if (task === "concierge") {
      const result = await createClinicalWeeklyConciergeTickets();
      results.tasks.push(result);
    } else if (task === "premium") {
      const result = await createClinicalMonthlyPremiumTickets();
      results.tasks.push(result);
    } else {
      // Run all tasks
      const coordinationResult = await createCoordinationMonthlyTickets();
      results.tasks.push(coordinationResult);

      const conciergeResult = await createClinicalWeeklyConciergeTickets();
      results.tasks.push(conciergeResult);

      const premiumResult = await createClinicalMonthlyPremiumTickets();
      results.tasks.push(premiumResult);
    }

    // Calculate summary
    results.summary.totalClients = results.tasks.reduce(
      (sum, t) => sum + t.total,
      0
    );
    results.summary.totalTicketsCreated = results.tasks.reduce(
      (sum, t) => sum + t.created,
      0
    );
    results.summary.totalTicketsFailed = results.tasks.reduce(
      (sum, t) => sum + t.failed,
      0
    );

    results.finishedAt = new Date().toISOString();

    // Final summary log
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info("📊 RECURRING TICKET JOB SUMMARY");
    logger.info("═══════════════════════════════════════════════════════════");
    logger.info(`   📋 Total clients processed: ${results.summary.totalClients}`);
    logger.info(`   ✅ Total tickets created: ${results.summary.totalTicketsCreated}`);
    logger.info(`   ❌ Total tickets failed: ${results.summary.totalTicketsFailed}`);
    logger.info(`   ⏱️  Duration: ${new Date(results.finishedAt) - new Date(results.startedAt)}ms`);
    logger.info("═══════════════════════════════════════════════════════════");

    return results;
  } catch (error) {
    results.finishedAt = new Date().toISOString();
    logger.error(`❌ Recurring ticket job failed: ${error.message}`, error);
    throw error;
  }
}

