import { logger } from "../src/config/logger.js";
import {
  fetchClientDetail,
  fetchCaregiverDetail,
} from "../src/modules/alayacare/service.js";
import {
  mapClientToZendesk,
  mapCaregiverToZendesk,
} from "../src/modules/alayacare/mapper.js";
import { sanitizeUsers } from "../src/modules/common/validator.js";
import { upsertSingleUser } from "../src/modules/zendesk/service.js";
import {
  initDatabase,
  closeDatabase,
  upsertUserMapping,
} from "../src/infrastructure/database.js";

function printUsage() {
  logger.info(
    "\nUsage: node scripts/sync-single-user.js <client|caregiver> <alayacare_id>\n"
  );
  logger.info("Example: node scripts/sync-single-user.js client 5001");
}

async function loadUser(type, id) {
  if (type === "client") {
    return fetchClientDetail(id);
  }
  if (type === "caregiver") {
    return fetchCaregiverDetail(id);
  }
  throw new Error(`Unsupported type: ${type}`);
}

function mapUser(type, payload) {
  if (type === "client") {
    return mapClientToZendesk(payload);
  }
  if (type === "caregiver") {
    return mapCaregiverToZendesk(payload);
  }
  throw new Error(`Unsupported type: ${type}`);
}

async function main() {
  const [typeArg, idArg] = process.argv.slice(2);

  if (!typeArg || !idArg) {
    logger.error("❌ Missing required arguments.");
    printUsage();
    process.exit(1);
  }

  const targetType = typeArg.toLowerCase();
  if (!["client", "caregiver"].includes(targetType)) {
    logger.error(`❌ Invalid user type "${typeArg}". Must be client or caregiver.`);
    printUsage();
    process.exit(1);
  }

  const targetId = Number(idArg);
  if (!Number.isFinite(targetId) || targetId <= 0) {
    logger.error(`❌ Invalid ID "${idArg}". Must be a positive number.`);
    printUsage();
    process.exit(1);
  }

  logger.info(
    `🎯 Starting single-user sync for ${targetType} with AC ID ${targetId}`
  );

  initDatabase();

  try {
    logger.info("🔍 Fetching AlayaCare record...");
    const rawUser = await loadUser(targetType, targetId);

    if (!rawUser) {
      throw new Error(
        `No ${targetType} found in AlayaCare with ID ${targetId}`
      );
    }

    logger.info("📦 Raw AlayaCare API Response:");
    logger.info(JSON.stringify(rawUser, null, 2));

    logger.info("🧠 Mapping record to Zendesk format...");
    const mapped = mapUser(targetType, rawUser);
    if (!mapped) {
      throw new Error("Mapping returned null. Check mapper logs for details.");
    }

    const sanitized = sanitizeUsers([mapped]);
    if (sanitized.length === 0) {
      throw new Error("User failed validation and cannot be synced.");
    }

    const [user] = sanitized;
    logger.info(
      `📨 Sending ${targetType} ${user.name || user.external_id} to Zendesk...`
    );
    logger.info("📤 Mapped Data (ready to send to Zendesk):");
    logger.info(JSON.stringify(user, null, 2));

    const upsertResult = await upsertSingleUser(user);
    if (!upsertResult?.userId) {
      throw new Error("Zendesk upsert did not return a user ID.");
    }

    const mapping = {
      ac_id: String(user.ac_id),
      zendesk_user_id: upsertResult.userId,
      external_id: user.external_id,
      mapped_data: user,
      last_synced_at: new Date().toISOString(),
    };
    upsertUserMapping(mapping);

    logger.info(
      `✅ Sync complete! Zendesk user ID: ${upsertResult.userId}, AC ID: ${user.ac_id}`
    );
  } catch (err) {
    logger.error("❌ Single-user sync failed:", err.response?.data || err);
    process.exitCode = 1;
  } finally {
    closeDatabase();
    logger.close();
  }
}

main();

