import { logger } from "../../config/logger.js";
import { callZendesk, getZendeskClient } from "./zendesk.api.js";
import { addIdentities } from "./identitySync.js";
import { pollJobStatus } from "./jobPoller.js";
import { zendeskLimiter } from "../../utils/limiter.js";

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

export async function bulkUpsertUsers(users = []) {
  return callZendesk(async () => {
    const payload = {
      users: users.map((u) => {
        const { identities, ac_id, ...userWithoutIdentities } = u || {};
        return buildZendeskUserObject(userWithoutIdentities);
      }),
    };

    const res = await zendeskLimiter.schedule(() => getZendeskClient().post("/users/create_or_update_many.json", payload));
    logger.info(`🧩 Upsert request accepted: ${users.length} users`);
    const jobId = res.data?.job_status?.id;

    if (!jobId) {
      logger.warn("⚠️ No job ID returned from Zendesk");
      return res.data;
    }

    const job = await pollJobStatus(jobId);
    logger.info(`✅ Job ${jobId} finished with status: ${job.status}`);

    return {
      job_status: job,
      original_users: users,
    };
  });
}

