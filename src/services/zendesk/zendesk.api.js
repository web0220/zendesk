import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../../utils/retry.js";
import { zendeskLimiter } from "../../utils/limiter.js";

const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

function zendeskRequest(fn) {
  const retries = Number(process.env.ZENDESK_RETRIES) || 3;
  const delay = Number(process.env.ZENDESK_RETRY_DELAY) || 1000;
  return withRetry(fn, retries, delay);
}

export function getZendeskClient() {
  return zendeskClient;
}

export function callZendesk(getPromise) {
  return zendeskRequest(getPromise);
}

export function getJobStatus(jobId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/job_statuses/${jobId}.json`));
    return res.data;
  });
}

export function getUserIdentities(userId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/users/${userId}/identities.json`));
    return res.data?.identities || [];
  });
}

export function getUser(userId) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.get(`/users/${userId}.json`));
    return res.data?.user || null;
  });
}

export function updateUserCustomFields(userId, userFields) {
  return zendeskRequest(async () => {
    const res = await zendeskLimiter.schedule(() => zendeskClient.put(`/users/${userId}.json`, {
      user: { user_fields: userFields },
    }));
    return res.data?.user || null;
  });
}

/**
 * Delete a user identity from Zendesk
 * @param {number} userId - Zendesk user ID
 * @param {number} identityId - Identity ID to delete
 * @returns {Promise<boolean>} True if successful
 */
export function deleteUserIdentity(userId, identityId) {
  return zendeskRequest(async () => {
    await zendeskLimiter.schedule(() => 
      zendeskClient.delete(`/users/${userId}/identities/${identityId}.json`)
    );
    return true;
  });
}

logger.info("💬 Zendesk API client initialized");

