import { logger } from "../../config/logger.js";
import { callZendesk, getZendeskClient } from "./zendesk.api.js";
import { addIdentities, syncUserIdentities } from "./identitySync.js";
import { pollJobStatus } from "./jobPoller.js";
import { zendeskLimiter } from "../../utils/rateLimiters/zendesk.js";

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

export async function upsertSingleUser(user) {
  return callZendesk(async () => {
    const { identities, ...userWithoutIdentities } = user || {};
    const payload = { user: buildZendeskUserObject(userWithoutIdentities) };

    const res = await zendeskLimiter.schedule(() => getZendeskClient().post("/users/create_or_update.json", payload));
    const userId = res.data?.user?.id;

    if (!userId) {
      logger.warn(`⚠️ No user ID returned for ${user?.name || user?.email}`);
      return null;
    }

    logger.info(
      `✅ User ${userId} ${
        res.data?.user?.created_at ? "created" : "updated"
      }: ${user?.name || user?.email}`
    );

    await addIdentities(userId, identities);

    return { userId, user: res.data?.user };
  });
}

/**
 * Update an existing user in Zendesk using PUT method.
 * This is used for updating users whose email/phone was changed (e.g., after non-active user processing).
 * 
 * @param {number} userId - Zendesk user ID
 * @param {Object} userData - User data to update
 * @returns {Promise<Object|null>} Updated user object or null if failed
 */
export async function updateUser(userId, userData) {
  return callZendesk(async () => {
    const { identities, ...userWithoutIdentities } = userData || {};
    const payload = { user: buildZendeskUserObject(userWithoutIdentities) };
    
    const res = await zendeskLimiter.schedule(() => 
      getZendeskClient().put(`/users/${userId}.json`, payload)
    );
    
    const updatedUser = res.data?.user;
    
    if (!updatedUser) {
      logger.warn(`⚠️ No user returned from PUT update for userId=${userId}`);
      return null;
    }
    
    logger.info(
      `✅ Updated user ${userId} via PUT: ${updatedUser.name || updatedUser.email}`
    );
    
    // Update identities separately if provided
    if (identities && Array.isArray(identities) && identities.length > 0) {
      await syncUserIdentities(userId, { identities });
    }
    
    return { userId, user: updatedUser };
  });
}

export async function bulkUpsertUsers(users = []) {
  return callZendesk(async () => {
    const payload = {
      users: users.map((u) => {
        const { identities, ac_id, ...userWithoutIdentities } = u || {};
        return buildZendeskUserObject(userWithoutIdentities);
      }),
    };

    const res = await zendeskLimiter.schedule(() => getZendeskClient().post("/users/create_or_update_many.json", payload));
    const jobId = res.data?.job_status?.id;

    if (!jobId) {
      logger.warn("⚠️ No job ID returned from Zendesk");
      return res.data;
    }

    const job = await pollJobStatus(jobId);

    return {
      job_status: job,
      original_users: users,
    };
  });
}

