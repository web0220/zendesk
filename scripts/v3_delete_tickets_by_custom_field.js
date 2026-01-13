import { logger } from "../src/config/logger.js";
import { searchTicketsWithPagination, deleteTicket } from "../src/services/zendesk/zendesk.api.js";

// Custom field ID to search for
const CUSTOM_FIELD_ID = "45253032576027";

// Search query to find tickets with the custom field
const SEARCH_QUERY = `type:ticket custom_field_${CUSTOM_FIELD_ID}:*`;

/**
 * Delete all tickets matching the search query
 */
async function deleteTicketsByCustomField() {
  try {
    logger.info(`🔍 Searching for tickets with custom field ${CUSTOM_FIELD_ID}...`);
    logger.info(`📋 Search query: ${SEARCH_QUERY}`);

    // Search for all tickets with pagination
    const tickets = await searchTicketsWithPagination(SEARCH_QUERY);

    if (tickets.length === 0) {
      logger.info("✅ No tickets found to delete.");
      return;
    }

    logger.info(`📊 Found ${tickets.length} ticket(s) to delete.`);
    
    // Confirm before deletion (in production, you might want to add a confirmation prompt)
    logger.warn(`⚠️  WARNING: About to delete ${tickets.length} ticket(s). This action cannot be undone.`);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    // Delete tickets one by one
    for (let i = 0; i < tickets.length; i++) {
      const ticket = tickets[i];
      const ticketId = ticket.id;
      const ticketSubject = ticket.subject || "No subject";

      try {
        await deleteTicket(ticketId);
        successCount++;
        logger.info(`✅ [${i + 1}/${tickets.length}] Deleted ticket #${ticketId}: "${ticketSubject}"`);
      } catch (error) {
        errorCount++;
        const errorMsg = error.response?.data 
          ? JSON.stringify(error.response.data)
          : error.message || "Unknown error";
        errors.push({ ticketId, error: errorMsg });
        logger.error(`❌ [${i + 1}/${tickets.length}] Failed to delete ticket #${ticketId}: ${errorMsg}`);
      }
    }

    // Summary
    logger.info("\n" + "=".repeat(60));
    logger.info("📊 DELETION SUMMARY");
    logger.info("=".repeat(60));
    logger.info(`✅ Successfully deleted: ${successCount} ticket(s)`);
    logger.info(`❌ Failed to delete: ${errorCount} ticket(s)`);
    logger.info(`📋 Total processed: ${tickets.length} ticket(s)`);

    if (errors.length > 0) {
      logger.warn("\n⚠️  Errors encountered:");
      errors.forEach(({ ticketId, error }) => {
        logger.warn(`   Ticket #${ticketId}: ${error}`);
      });
    }

    logger.info("=".repeat(60));
  } catch (error) {
    logger.error("❌ Fatal error during ticket deletion:", error.response?.data || error.message || error);
    process.exit(1);
  }
}

// Run the script
deleteTicketsByCustomField()
  .then(() => {
    logger.info("✅ Script completed successfully.");
    process.exit(0);
  })
  .catch((error) => {
    logger.error("❌ Script failed:", error);
    process.exit(1);
  });

