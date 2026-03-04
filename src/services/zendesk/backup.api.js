/**
 * Zendesk Backup API – fetch users, tickets, comments, attachments, organizations, and field definitions for backup.
 * Uses existing rate limiter and callZendesk for retries.
 */

import fs from "fs";
import path from "path";
import { getZendeskClient, callZendesk } from "./zendesk.api.js";
import { zendeskLimiter } from "../../utils/rateLimiters/zendesk.js";
import { logger } from "../../config/logger.js";

const zendeskClient = getZendeskClient();

/**
 * Fetch all users with pagination (next_page).
 * @returns {Promise<Array>} All users
 */
export async function fetchAllUsers() {
  const users = [];
  let nextPath = "/users.json";

  logger.info("📥 Backup: Fetching users from Zendesk...");

  while (nextPath) {
    const res = await callZendesk(() =>
      zendeskLimiter.schedule(() => {
        if (nextPath.startsWith("http")) {
          const base = zendeskClient.defaults.baseURL;
          const pathOnly = nextPath.startsWith(base) ? nextPath.slice(base.length) : nextPath;
          return zendeskClient.get(pathOnly);
        }
        return zendeskClient.get(nextPath);
      })
    );

    const batch = res.data?.users || [];
    users.push(...batch);
    logger.info(`   ➕ Users: ${batch.length} (total: ${users.length})`);

    nextPath = res.data?.next_page || null;
  }

  logger.info(`✅ Backup: Users complete. Total: ${users.length}`);
  return users;
}

/**
 * Earliest start_time for incremental ticket export (2010-01-01 UTC).
 * Zendesk Search API hits "Search Response Limits" past ~1000 results; incremental export does not.
 */
const TICKETS_FULL_EXPORT_START = 1262304000;

/**
 * Fetch all tickets via Incremental Export API (paginated by next_page until end_of_stream).
 * Use this for full backup instead of Search API, which returns 422 past ~1000 tickets.
 * @returns {Promise<Array>} All tickets
 */
export async function fetchAllTickets() {
  logger.info("📥 Backup: Fetching tickets from Zendesk (incremental export from start of time)...");
  const { tickets } = await fetchIncrementalTickets(TICKETS_FULL_EXPORT_START);
  logger.info(`✅ Backup: Tickets complete. Total: ${tickets.length}`);
  return tickets;
}

/**
 * Fetch all comments for a single ticket (supports next_page and page-based pagination).
 * @param {number} ticketId - Zendesk ticket ID
 * @returns {Promise<Array>} All comments for the ticket
 */
export async function fetchTicketComments(ticketId) {
  const comments = [];
  let nextPath = `/tickets/${ticketId}/comments.json`;

  while (nextPath) {
    let res;
    try {
      res = await callZendesk(() =>
        zendeskLimiter.schedule(() => {
          if (nextPath.startsWith("http")) {
            const base = zendeskClient.defaults.baseURL;
            const pathOnly = nextPath.startsWith(base) ? nextPath.slice(base.length) : nextPath;
            return zendeskClient.get(pathOnly);
          }
          return zendeskClient.get(nextPath);
        })
      );
    } catch (err) {
      if (err.response?.status === 404) {
        logger.debug(`   Ticket ${ticketId}: 404 (deleted/archived), skipping comments`);
        return [];
      }
      throw err;
    }

    const batch = res.data?.comments || [];
    comments.push(...batch);
    nextPath = res.data?.next_page || null;
  }

  return comments;
}

/**
 * Download an attachment from a URL (Zendesk attachment content_url) and save to disk.
 * Uses same Zendesk client auth as API calls.
 * @param {string} contentUrl - Full URL of the attachment (e.g. from comment.attachments[].content_url)
 * @param {string} destPath - Full path where to save the file
 * @returns {Promise<string>} destPath on success
 */
export async function downloadAttachment(contentUrl, destPath) {
  const res = await zendeskLimiter.schedule(() =>
    zendeskClient.get(contentUrl, {
      responseType: "arraybuffer",
      validateStatus: (status) => status === 200,
    })
  );

  const dir = path.dirname(destPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  fs.writeFileSync(destPath, res.data);
  return destPath;
}

/**
 * Fetch ticket field definitions.
 * @returns {Promise<Array>} Ticket field definitions
 */
export async function fetchTicketFields() {
  const res = await callZendesk(() =>
    zendeskLimiter.schedule(() => zendeskClient.get("/ticket_fields.json"))
  );
  const fields = res.data?.ticket_fields || [];
  logger.info(`✅ Backup: Ticket fields: ${fields.length}`);
  return fields;
}

/**
 * Fetch user field definitions.
 * @returns {Promise<Array>} User field definitions
 */
export async function fetchUserFields() {
  const res = await callZendesk(() =>
    zendeskLimiter.schedule(() => zendeskClient.get("/user_fields.json"))
  );
  const fields = res.data?.user_fields || [];
  logger.info(`✅ Backup: User fields: ${fields.length}`);
  return fields;
}

/**
 * List organization IDs with pagination, then fetch organization details in chunks via show_many.
 * @returns {Promise<Array>} All organizations (with details)
 */
export async function fetchAllOrganizations() {
  const orgList = [];
  let nextPath = "/organizations.json";

  logger.info("📥 Backup: Fetching organization list...");

  while (nextPath) {
    const res = await callZendesk(() =>
      zendeskLimiter.schedule(() => {
        if (nextPath.startsWith("http")) {
          const base = zendeskClient.defaults.baseURL;
          const pathOnly = nextPath.startsWith(base) ? nextPath.slice(base.length) : nextPath;
          return zendeskClient.get(pathOnly);
        }
        return zendeskClient.get(nextPath);
      })
    );

    const batch = res.data?.organizations || [];
    orgList.push(...batch);
    nextPath = res.data?.next_page || null;
  }

  if (orgList.length === 0) {
    logger.info("✅ Backup: Organizations: 0");
    return [];
  }

  const ids = [...new Set(orgList.map((o) => o.id).filter(Boolean))];
  const chunkSize = 100;
  const allOrgs = [];

  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const detailRes = await callZendesk(() =>
      zendeskLimiter.schedule(() =>
        zendeskClient.get("/organizations/show_many.json", { params: { ids: chunk.join(",") } })
      )
    );
    const orgs = detailRes.data?.organizations || [];
    allOrgs.push(...orgs);
  }

  logger.info(`✅ Backup: Organizations: ${allOrgs.length}`);
  return allOrgs;
}

/**
 * Collect unique tags from tickets and users.
 * @param {Array} tickets - Array of ticket objects
 * @param {Array} users - Array of user objects
 * @returns {Array<string>} Sorted unique tags
 */
export function collectTagsFromTicketsAndUsers(tickets = [], users = []) {
  const tagSet = new Set();
  for (const t of tickets) {
    for (const tag of t.tags || []) {
      tagSet.add(String(tag));
    }
  }
  for (const u of users) {
    for (const tag of u.tags || []) {
      tagSet.add(String(tag));
    }
  }
  return [...tagSet].sort();
}

// -----------------------------------------------------------------------------
// Incremental export (fetch only data updated since last run)
// -----------------------------------------------------------------------------

/**
 * Fetch users updated since startTime (Unix seconds). Time-based pagination.
 * @param {number} startTime - Unix epoch seconds (must be > 1 minute in the past)
 * @returns {Promise<{ users: Array, endTime: number|null, endOfStream: boolean }>}
 */
export async function fetchIncrementalUsers(startTime) {
  const allUsers = [];
  let nextPath = `/incremental/users.json?start_time=${startTime}&per_page=1000`;
  let endTime = null;

  logger.info(`📥 Backup: Fetching incremental users since ${startTime}...`);

  while (nextPath) {
    const res = await callZendesk(() =>
      zendeskLimiter.schedule(() => {
        if (nextPath.startsWith("http")) {
          return zendeskClient.get(nextPath);
        }
        const [pathPart, queryPart] = nextPath.split("?");
        const params = queryPart ? Object.fromEntries(new URLSearchParams(queryPart)) : {};
        return zendeskClient.get(pathPart, { params });
      })
    );

    const batch = res.data?.users || [];
    allUsers.push(...batch);
    endTime = res.data?.end_time ?? endTime;
    const endOfStream = res.data?.end_of_stream ?? true;
    nextPath = !endOfStream && res.data?.next_page ? res.data.next_page : null;
  }

  logger.info(`✅ Backup: Incremental users: ${allUsers.length} (end_time: ${endTime})`);
  return { users: allUsers, endTime, endOfStream: true };
}

/**
 * Fetch tickets updated since startTime (Unix seconds). Time-based pagination.
 * @param {number} startTime - Unix epoch seconds
 * @returns {Promise<{ tickets: Array, endTime: number|null, endOfStream: boolean }>}
 */
export async function fetchIncrementalTickets(startTime) {
  const allTickets = [];
  let nextPath = `/incremental/tickets.json?start_time=${startTime}&per_page=1000`;
  let endTime = null;

  logger.info(`📥 Backup: Fetching incremental tickets since ${startTime}...`);

  while (nextPath) {
    const res = await callZendesk(() =>
      zendeskLimiter.schedule(() => {
        if (nextPath.startsWith("http")) {
          return zendeskClient.get(nextPath);
        }
        const [pathPart, queryPart] = nextPath.split("?");
        const params = queryPart ? Object.fromEntries(new URLSearchParams(queryPart)) : {};
        return zendeskClient.get(pathPart, { params });
      })
    );

    const batch = res.data?.tickets || [];
    allTickets.push(...batch);
    endTime = res.data?.end_time ?? endTime;
    const endOfStream = res.data?.end_of_stream ?? true;
    nextPath = !endOfStream && res.data?.next_page ? res.data.next_page : null;
  }

  logger.info(`✅ Backup: Incremental tickets: ${allTickets.length} (end_time: ${endTime})`);
  return { tickets: allTickets, endTime, endOfStream: true };
}
