import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../common/retry.js";
import { pollJobStatus } from "./jobPoller.js";

// ======================================================
// 🔧 Initialize Zendesk API Client
// ======================================================
const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

function buildZendeskUserObject(user = {}) {
  return {
    external_id: user.external_id ?? null,
    name: user.name ?? null,
    email: user.email ?? null,
    phone: user.phone ?? null,
    organization_id: user.organization_id || null,
    user_fields: user.user_fields || {},
  };
}

function normalizeIdentities(identities = []) {
  return identities.map(identity => {
    let identityType;
    if (identity.type === "phone") {
      identityType = "phone_number";
    } else if (identity.type === "email") {
      identityType = "email";
    } else {
      identityType = identity.type;
    }

    return {
      type: identityType,
      value: identity.value,
    };
  });
}

async function addIdentities(userId, identities = []) {
  const formatted = normalizeIdentities(identities);
  if (!userId || formatted.length === 0) return;

  logger.info(`📞 Adding ${formatted.length} identity/identities for user ${userId}...`);

  for (const identity of formatted) {
    try {
      await zendeskClient.post(`/users/${userId}/identities.json`, { identity });
      logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;
      
      // Check if this is a duplicate error
      if (isDuplicateError(err)) {
        logger.info(`   🔄 Duplicate ${identity.type} ${identity.value} detected, adding to contact_address`);
        await addToContactAddress(userId, identity.value);
      } else {
        logger.warn(`   ⚠️ Failed to add identity ${identity.value}: ${JSON.stringify(msg)}`);
      }
    }
  }
}

// ======================================================
// ➕ Upsert single user (create or update one)
// ======================================================
export async function upsertSingleUser(user) {
  return withRetry(async () => {
    const { identities, ...userWithoutIdentities } = user || {};

    const payload = {
      user: buildZendeskUserObject(userWithoutIdentities),
    };

    const res = await zendeskClient.post("/users/create_or_update.json", payload);
    const userId = res.data?.user?.id;

    if (!userId) {
      logger.warn(`⚠️ No user ID returned for ${user?.name || user?.email}`);
      return null;
    }

    logger.info(`✅ User ${userId} ${res.data?.user?.created_at ? "created" : "updated"}: ${user?.name || user?.email}`);

    await addIdentities(userId, identities);

    return { userId, user: res.data?.user };
  });
}

// ======================================================
// 🚀 Bulk upsert users (create or update many)
// ======================================================
export async function bulkUpsertUsers(users = []) {
  return withRetry(async () => {
    // Send everything except identities and ac_id (internal field)
    const payload = {
      users: users.map(u => {
        const { identities, ac_id, ...userWithoutIdentities } = u || {};
        return buildZendeskUserObject(userWithoutIdentities);
      }),
    };

    const res = await zendeskClient.post("/users/create_or_update_many.json", payload);
    logger.info(`🧩 Upsert request accepted: ${users.length} users`);
    const jobId = res.data?.job_status?.id;

    if (!jobId) {
      logger.warn("⚠️ No job ID returned from Zendesk");
      return res.data;
    }

    // Wait for job completion
    const job = await pollJobStatus(jobId);
    logger.info(`✅ Job ${jobId} finished with status: ${job.status}`);

    // Return job status with results
    return {
      job_status: job,
      original_users: users, // Include original user data for mapping
    };
  });
}

// ======================================================
// 📊 Get job status
// ======================================================
export async function getJobStatus(jobId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/job_statuses/${jobId}.json`);
    return res.data;
  });
}

// ======================================================
// 📋 Get existing user identities
// ======================================================
export async function getUserIdentities(userId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/users/${userId}/identities.json`);
    return res.data?.identities || [];
  });
}

// ======================================================
// 👤 Get user details
// ======================================================
export async function getUser(userId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/users/${userId}.json`);
    return res.data?.user || null;
  });
}

// ======================================================
// 🔄 Update user custom fields
// ======================================================
export async function updateUserCustomFields(userId, userFields) {
  return withRetry(async () => {
    const res = await zendeskClient.put(`/users/${userId}.json`, {
      user: { user_fields: userFields }
    });
    return res.data?.user || null;
  });
}

// ======================================================
// 🔍 Check if error is a duplicate value error
// ======================================================
function isDuplicateError(err) {
  const errorData = err.response?.data;
  if (!errorData) return false;
  
  // Check for DuplicateValue error
  if (errorData.error === "RecordInvalid" && errorData.details?.value) {
    const valueErrors = Array.isArray(errorData.details.value) 
      ? errorData.details.value 
      : [errorData.details.value];
    
    return valueErrors.some(error => 
      error?.error === "DuplicateValue" || 
      error?.description?.includes("already being used by another user")
    );
  }
  
  return false;
}

// ======================================================
// 📝 Add duplicate contact to contact_address custom field
// ======================================================
async function addToContactAddress(userId, contactValue) {
  try {
    // Get current user data
    const user = await getUser(userId);
    if (!user) {
      logger.warn(`   ⚠️ Could not fetch user ${userId} to update contact_address`);
      return;
    }

    // Get current contact_address value
    const currentContactAddress = user.user_fields?.contact_address || "";
    const contacts = currentContactAddress 
      ? currentContactAddress.split(/\s*,\s*/).filter(Boolean)
      : [];

    // Check if contact already exists (case-insensitive)
    const normalizedContact = contactValue.trim();
    const normalizedContacts = contacts.map(c => c.trim().toLowerCase());
    
    if (normalizedContacts.includes(normalizedContact.toLowerCase())) {
      logger.debug(`   ℹ️  Contact ${normalizedContact} already in contact_address`);
      return;
    }

    // Add new contact
    contacts.push(normalizedContact);
    const updatedContactAddress = contacts.join(", ");

    // Update user custom fields
    await updateUserCustomFields(userId, {
      ...user.user_fields,
      contact_address: updatedContactAddress
    });

    logger.info(`   📝 Added ${normalizedContact} to contact_address for user ${userId}`);
  } catch (err) {
    logger.warn(`   ⚠️ Failed to update contact_address for user ${userId}: ${err.message}`);
  }
}

// ======================================================
// ➕ Sync user identities (secondary emails & phones)
// ======================================================
export async function syncUserIdentities(userId, userData) {
  if (!userId) return;

  // Get existing identities from Zendesk
  const existingIdentities = await getUserIdentities(userId);
  const existingValues = new Set(
    existingIdentities.map(id => id.value?.toLowerCase().trim())
  );

  logger.info(`📋 User ${userId} has ${existingIdentities.length} existing identities`);

  // Collect identities from userData
  const identitiesToAdd = [];
  
  if (Array.isArray(userData.identities)) {
    for (const identity of userData.identities) {
      const normalizedValue = identity.value?.toLowerCase().trim();
      
      // Skip if already exists
      if (existingValues.has(normalizedValue)) {
        logger.debug(`   ⏭️  Skipping duplicate ${identity.type}: ${identity.value}`);
        continue;
      }

      // Normalize type for Zendesk API
      const type = identity.type === "phone" ? "phone_number" : identity.type;
      identitiesToAdd.push({ type, value: identity.value });
    }
  }

  if (identitiesToAdd.length === 0) {
    logger.info(`   ✅ No new identities to add for user ${userId}`);
    return;
  }

  logger.info(`📞 Adding ${identitiesToAdd.length} new identities for user ${userId}`);

  for (const identity of identitiesToAdd) {
    try {
      await zendeskClient.post(`/users/${userId}/identities.json`, { identity });
      logger.info(`   ➕ Added ${identity.type}: ${identity.value}`);
    } catch (err) {
      const msg = err.response?.data || err.message;
      
      // Check if this is a duplicate error
      if (isDuplicateError(err)) {
        logger.info(`   🔄 Duplicate ${identity.type} ${identity.value} detected, adding to contact_address`);
        await addToContactAddress(userId, identity.value);
      } else {
        logger.warn(`   ⚠️ Failed to add ${identity.value}: ${JSON.stringify(msg)}`);
      }
    }
  }
}

logger.info("💬 Zendesk service with retry initialized");
