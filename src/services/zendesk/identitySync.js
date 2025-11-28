import { logger } from "../../config/logger.js";
import {
  callZendesk,
  getUser,
  getUserIdentities,
  getZendeskClient,
  updateUserCustomFields,
} from "./zendesk.api.js";

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
        getZendeskClient().post(`/users/${userId}/identities.json`, { identity })
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

  // logger.info(`📋 User ${userId} has ${existingIdentities.length} existing identities`);

  const identitiesToAdd = [];

  if (Array.isArray(userData.identities)) {
    for (const identity of userData.identities) {
      const normalizedValue = identity.value?.toLowerCase().trim();

      if (existingValues.has(normalizedValue)) {
        // logger.debug(`   ⏭️  Skipping duplicate ${identity.type}: ${identity.value}`);
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
        getZendeskClient().post(`/users/${userId}/identities.json`, { identity })
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

