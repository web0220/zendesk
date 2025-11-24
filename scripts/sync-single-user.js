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
import { upsertSingleUser, syncUserIdentities } from "../src/modules/zendesk/service.js";
import {
  initDatabase,
  closeDatabase,
  saveMappedDataToDatabase,
  processDuplicateEmailsAndPhones,
  getUsersPendingSync,
  getUserMappingByAcId,
  convertDatabaseRowToZendeskUser,
  updateZendeskUserId,
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
    const userType = user.user_fields?.type || targetType;
    
    // 1️⃣ Check if user already exists in database
    const acId = String(user.ac_id);
    const existingUser = getUserMappingByAcId(acId, userType);
    const isResync = existingUser && existingUser.zendesk_user_id !== null;
    
    if (isResync) {
      logger.info(`🔄 User ${acId} already synced (zendesk_user_id: ${existingUser.zendesk_user_id}). Re-syncing...`);
    }

    // 2️⃣ Save mapped data to database first (will skip if already synced, but that's OK)
    logger.info("💾 Saving mapped data to database...");
    saveMappedDataToDatabase(user);
    logger.info("✅ Saved mapped data to database");

    // 3️⃣ Process duplicate emails and phone numbers
    logger.info("🔧 Processing duplicate emails and phone numbers...");
    try {
      processDuplicateEmailsAndPhones();
      logger.info("✅ Finished processing duplicates");
    } catch (err) {
      logger.warn(`⚠️ Failed to process duplicates: ${err.message}`);
      // Continue even if duplicate processing fails
    }

    // 4️⃣ Read user from database (after duplicate processing)
    logger.info("📖 Reading user from database...");
    let userFromDb;
    
    if (isResync) {
      // If re-syncing, get the user directly (even if already synced)
      userFromDb = getUserMappingByAcId(acId, userType);
    } else {
      // If new sync, get from pending sync list
      const usersFromDb = getUsersPendingSync();
      userFromDb = usersFromDb.find(
        (u) => String(u.source_ac_id || u.ac_id) === acId
      );
    }
    
    if (!userFromDb) {
      logger.error(`❌ User not found in database. Searched for ac_id: ${acId}`);
      if (!isResync) {
        const usersFromDb = getUsersPendingSync();
        logger.error(`   Available users pending sync: ${usersFromDb.map(u => u.ac_id).join(", ")}`);
      }
      throw new Error("User not found in database after saving. This should not happen.");
    }

    // 4️⃣ Convert database row to Zendesk user format
    logger.info("🔄 Converting database row to Zendesk user format...");
    const zendeskUser = convertDatabaseRowToZendeskUser(userFromDb);
    
    if (!zendeskUser) {
      throw new Error("Failed to convert database row to Zendesk format.");
    }

    logger.info(
      `📨 Sending ${targetType} ${zendeskUser.name || zendeskUser.external_id} to Zendesk...`
    );
    logger.info("📤 Mapped Data (ready to send to Zendesk):");
    logger.info(JSON.stringify(zendeskUser, null, 2));

    // 5️⃣ Send to Zendesk
    const upsertResult = await upsertSingleUser(zendeskUser);
    if (!upsertResult?.userId) {
      throw new Error("Zendesk upsert did not return a user ID.");
    }

    // 6️⃣ Sync identities
    await syncUserIdentities(upsertResult.userId, zendeskUser);

    // 7️⃣ Update database with zendesk_user_id
    const syncTimestamp = new Date().toISOString();
    updateZendeskUserId(
      String(user.ac_id),
      upsertResult.userId,
      syncTimestamp,
      userType
    );

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

