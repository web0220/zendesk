import { config } from "../config/index.js";
import { logger } from "../config/logger.js";
import { runRecurringTickets } from "../core/recurringTicketOrchestrator.js";
import { initDatabase, closeDatabase } from "../infra/database.js";
import { bootstrap } from "./bootstrap.js";

async function main() {
  await bootstrap(async () => {
    // Parse command line arguments for specific task
    const args = process.argv.slice(2);
    const taskArg = args.find((arg) => arg.startsWith("--task="));
    const task = taskArg ? taskArg.split("=")[1] : null;

    // Validate task argument
    const validTasks = ["coordination", "concierge", "premium", null];
    if (task && !validTasks.includes(task)) {
      logger.error(
        `❌ Invalid task: ${task}. Valid options: coordination, concierge, premium, or omit for all tasks`
      );
      process.exit(1);
    }

    if (task) {
      logger.info(`🎯 Running specific task: ${task}`);
    } else {
      logger.info("🎯 Running all recurring ticket tasks");
    }

    const result = await runRecurringTickets({ task });
    logger.info("✅ Recurring ticket job completed successfully");
    logger.info("Summary:", JSON.stringify(result.summary, null, 2));
    return result;
  });
}

main().catch((err) => {
  logger.error("Startup error:", err);
  closeDatabase();
  logger.close();
  process.exit(1);
});

