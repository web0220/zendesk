import axios from "axios";
import { config } from "../../config/index.js";
import { logger } from "../../config/logger.js";
import { withRetry } from "../common/retry.js";

const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

export async function bulkUpsertUsers(users = []) {
  return withRetry(async () => {
    const payload = { users };
    const res = await zendeskClient.post("/users/create_or_update_many.json", payload);
    logger.info(`🧩 Upsert request accepted: ${users.length} users`);
    return res.data;
  });
}

export async function getJobStatus(jobId) {
  return withRetry(async () => {
    const res = await zendeskClient.get(`/job_statuses/${jobId}.json`);
    return res.data;
  });
}

logger.info("💬 Zendesk service with retry initialized");
