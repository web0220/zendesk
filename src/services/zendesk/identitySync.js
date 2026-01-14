import { logger } from "../../config/logger.js";
import {
  callZendesk,
  getUser,
  getUserIdentities,
  getZendeskClient,
  updateUserCustomFields,
  deleteUserIdentity,
} from "./zendesk.api.js";
import { zendeskLimiter } from "../../utils/rateLimiters/zendesk.js";

function normalizeIdentities(identities = []) {
  return identities
    .filter((identity) => identity?.value)
    .map((identity) => {
      if (identity.type === "phone") {
        return { type: "phone_number", value: identity.value };
      }
      return { type: identity.type, value: identity.value };
    });
}

function isDuplicateError(err) {
  const errorData = err.response?.data;
  if (!errorData) return false;

  if (errorData.error === "RecordInvalid" && errorData.details?.value) {
    const valueErrors = Array.isArray(errorData.details.value)
      ? errorData.details.value
      : [errorData.details.value];

    return valueErrors.some(
      (error) =>
        error?.error === "DuplicateValue" ||
        error?.description?.includes("already being used by another user")
    );
  }

  return false;
}

async function addToContactAddress(userId, contactValue) {
  try {
    const user = await getUser(userId);
    if (!user) {
      logger.warn(`   ⚠️ Could not fetch user ${userId} to update contact_address`);
      return;
    }

    const currentContactAddress = user.user_fields?.contact_address || "";
    const contacts = currentContactAddress ? currentContactAddress.split(/\s*,\s*/).filter(Boolean) : [];

    const normalizedContact = contactValue.trim();
    const normalizedContacts = contacts.map((contact) => contact.trim().toLowerCase());

    if (normalizedContacts.includes(normalizedContact.toLowerCase())) {
      logger.debug(`   ℹ️  Contact ${normalizedContact} already in contact_address`);
      return;
    }

    contacts.push(normalizedContact);
    const updatedContactAddress = contacts.join(", ");

    await updateUserCustomFields(userId, {
      ...user.user_fields,
      contact_address: updatedContactAddress,
    });

    logger.info(`   📝 Added ${normalizedContact} to contact_address for user ${userId}`);
  } catch (error) {
    logger.warn(`   ⚠️ Failed to update contact_address for user ${userId}: ${error.message}`);
  }
}

export async function addIdentities(userId, identities = []) {
  const formatted = normalizeIdentities(identities);
  if (!userId || formatted.length === 0) return;

  // logger.info(`📞 Adding ${formatted.length} identity/identities for user ${userId}...`);

  for (const identity of formatted) {
    try {
      await callZendesk(() =>
        zendeskLimiter.schedule(() => getZendeskClient().post(`/users/${userId}/identities.json`, { identity }))
      );
      // logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;

      if (isDuplicateError(err)) {
        logger.info(
          `   🔄 Duplicate ${identity.type} ${identity.value} detected, adding to contact_address`
        );
        await addToContactAddress(userId, identity.value);
      } else {
        logger.warn(`   ⚠️ Failed to add identity ${identity.value}: ${JSON.stringify(msg)}`);
      }
    }
  }
}

export async function syncUserIdentities(userId, userData) {
  if (!userId) return;

  const existingIdentities = await getUserIdentities(userId);
  const existingValues = new Set(
    existingIdentities.map((identity) => identity.value?.toLowerCase().trim())
  );


  const identitiesToAdd = [];

  if (Array.isArray(userData.identities)) {
    for (const identity of userData.identities) {
      const normalizedValue = identity.value?.toLowerCase().trim();

      if (existingValues.has(normalizedValue)) {
        continue;
      }

      const type = identity.type === "phone" ? "phone_number" : identity.type;
      identitiesToAdd.push({ type, value: identity.value });
    }
  }

  if (identitiesToAdd.length === 0) {
    // logger.info(`   ✅ No new identities to add for user ${userId}`);
    return;
  }

  // logger.info(`📞 Adding ${identitiesToAdd.length} new identities for user ${userId}`);

  for (const identity of identitiesToAdd) {
    try {
      await callZendesk(() =>
        zendeskLimiter.schedule(() => getZendeskClient().post(`/users/${userId}/identities.json`, { identity }))
      );
      // logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;

      if (isDuplicateError(err)) {
        logger.info(
          `   🔄 Duplicate ${identity.type} ${identity.value} detected, adding to contact_address`
        );
        await addToContactAddress(userId, identity.value);
      } else {
        logger.warn(`   ⚠️ Failed to add ${identity.value}: ${JSON.stringify(msg)}`);
      }
    }
  }
}

/**
 * Sync user identities to exactly match database (source of truth).
 * 
 * This function ensures Zendesk identities match the database by:
 * 1. Getting existing identities from Zendesk
 * 2. Comparing with identities from database
 * 3. Deleting identities in Zendesk that are not in database
 * 4. Adding identities in database that are not in Zendesk
 * 
 * Database is the source of truth - Zendesk should match it exactly.
 * 
 * @param {number} userId - Zendesk user ID
 * @param {Array} databaseIdentities - Identities from database (source of truth)
 * @returns {Promise<void>}
 */
export async function syncUserIdentitiesToMatchDatabase(userId, databaseIdentities = []) {
  if (!userId) return;

  // Normalize database identities
  const normalizedDbIdentities = normalizeIdentities(databaseIdentities);
  const dbIdentityMap = new Map();
  
  for (const identity of normalizedDbIdentities) {
    const key = `${identity.type}:${identity.value?.toLowerCase().trim()}`;
    dbIdentityMap.set(key, identity);
  }

  // Get existing identities from Zendesk
  const zendeskIdentities = await getUserIdentities(userId);
  
  // Build map of Zendesk identities
  const zendeskIdentityMap = new Map();
  for (const identity of zendeskIdentities) {
    const key = `${identity.type}:${identity.value?.toLowerCase().trim()}`;
    zendeskIdentityMap.set(key, identity);
  }

  // Step 1: Delete identities in Zendesk that are not in database
  const identitiesToDelete = [];
  for (const [key, zendeskIdentity] of zendeskIdentityMap.entries()) {
    if (!dbIdentityMap.has(key)) {
      identitiesToDelete.push(zendeskIdentity);
    }
  }

  if (identitiesToDelete.length > 0) {
    logger.info(`   🗑️  Deleting ${identitiesToDelete.length} identity(ies) from Zendesk that are not in database`);
    for (const identity of identitiesToDelete) {
      try {
        await deleteUserIdentity(userId, identity.id);
        logger.info(`   ✅ Deleted ${identity.type} identity ${identity.id} (${identity.value})`);
      } catch (error) {
        logger.error(`   ❌ Failed to delete identity ${identity.id}: ${error.message}`);
      }
    }
  }

  // Step 2: Add identities in database that are not in Zendesk
  const identitiesToAdd = [];
  for (const [key, dbIdentity] of dbIdentityMap.entries()) {
    if (!zendeskIdentityMap.has(key)) {
      identitiesToAdd.push(dbIdentity);
    }
  }

  if (identitiesToAdd.length > 0) {
    logger.info(`   ➕ Adding ${identitiesToAdd.length} identity(ies) to Zendesk from database`);
    for (const identity of identitiesToAdd) {
      try {
        await callZendesk(() =>
          zendeskLimiter.schedule(() => getZendeskClient().post(`/users/${userId}/identities.json`, { identity }))
        );
        logger.info(`   ✅ Added ${identity.type}: ${identity.value}`);
      } catch (err) {
        const msg = err.response?.data || err.message;

        if (isDuplicateError(err)) {
          logger.info(
            `   🔄 Duplicate ${identity.type} ${identity.value} detected, adding to contact_address`
          );
          await addToContactAddress(userId, identity.value);
        } else {
          logger.warn(`   ⚠️ Failed to add ${identity.value}: ${JSON.stringify(msg)}`);
        }
      }
    }
  }

  if (identitiesToDelete.length === 0 && identitiesToAdd.length === 0) {
    logger.debug(`   ✅ Zendesk identities already match database for user ${userId}`);
  }
}

