/**
 * Zendesk Backup Orchestrator – coordinates full or incremental backup of users, tickets, comments, attachments, orgs, and field definitions to local storage.
 * Set ZENDESK_BACKUP_INCREMENTAL=1 to only fetch data updated since last run (uses data/zendesk_backup/current/ and last_run.json).
 */

import fs from "fs";
import path from "path";
import { logger } from "../config/logger.js";
import { runWithLimit } from "../utils/concurrency.js";
import {
  fetchAllUsers,
  fetchAllTickets,
  fetchTicketComments,
  downloadAttachment,
  fetchTicketFields,
  fetchUserFields,
  fetchAllOrganizations,
  collectTagsFromTicketsAndUsers,
  fetchIncrementalUsers,
  fetchIncrementalTickets,
} from "../services/zendesk/backup.api.js";

const BACKUP_BASE = path.resolve("data", "zendesk_backup");
const BACKUP_CURRENT = path.join(BACKUP_BASE, "current");
const LAST_RUN_FILE = path.join(BACKUP_BASE, "last_run.json");

const INCREMENTAL = process.env.ZENDESK_BACKUP_INCREMENTAL === "1" || process.env.ZENDESK_BACKUP_INCREMENTAL === "true";

/**
 * Create a timestamped backup directory. Format: YYYY-MM-DDTHH-mm-ss
 * @returns {{ backupDir: string, attachmentsDir: string, commentsDir: string }}
 */
function createBackupDirs() {
  const now = new Date();
  const ts =
    now.getFullYear() +
    "-" +
    String(now.getMonth() + 1).padStart(2, "0") +
    "-" +
    String(now.getDate()).padStart(2, "0") +
    "T" +
    String(now.getHours()).padStart(2, "0") +
    "-" +
    String(now.getMinutes()).padStart(2, "0") +
    "-" +
    String(now.getSeconds()).padStart(2, "0");
  const backupDir = path.join(BACKUP_BASE, ts);
  const attachmentsDir = path.join(backupDir, "attachments");
  const commentsDir = path.join(backupDir, "ticket_comments");

  if (!fs.existsSync(backupDir)) {
    fs.mkdirSync(backupDir, { recursive: true });
  }
  if (!fs.existsSync(attachmentsDir)) {
    fs.mkdirSync(attachmentsDir, { recursive: true });
  }
  if (!fs.existsSync(commentsDir)) {
    fs.mkdirSync(commentsDir, { recursive: true });
  }

  return { backupDir, attachmentsDir, commentsDir };
}

function writeJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf-8");
}

/**
 * Download all attachments from a comment and save to attachmentsDir.
 * @param {Array} comments - Array of comment objects
 * @param {string} attachmentsDir - Directory to save attachment files
 * @param {number} ticketId - Ticket ID (for file naming)
 * @returns {Promise<{ downloaded: number, failed: number }>}
 */
async function downloadCommentAttachments(comments, attachmentsDir, ticketId) {
  const tasks = [];
  const attachmentList = [];
  for (const comment of comments) {
    for (const att of comment.attachments || []) {
      const contentUrl = att.content_url;
      if (!contentUrl) continue;
      const fileName = `${ticketId}_${comment.id}_${att.id}_${att.file_name || "file"}`.replace(
        /[^a-zA-Z0-9._-]/g,
        "_"
      );
      const destPath = path.join(attachmentsDir, fileName);
      attachmentList.push({ contentUrl, destPath });
    }
  }

  let downloaded = 0;
  let failed = 0;

  const concurrency = Number(process.env.ZENDESK_BACKUP_ATTACHMENT_CONCURRENCY) || 3;
  const taskFns = attachmentList.map(
    ({ contentUrl, destPath }) =>
      async () => {
        try {
          await downloadAttachment(contentUrl, destPath);
          downloaded++;
          if (downloaded % 10 === 0) {
            logger.info(`   📎 Attachments: ${downloaded} downloaded`);
          }
          return { ok: true };
        } catch (err) {
          failed++;
          logger.warn(`   ⚠️ Failed to download ${contentUrl}: ${err.message}`);
          return { ok: false };
        }
      }
  );

  await runWithLimit(taskFns, concurrency);
  return { downloaded, failed };
}

function readLastRun() {
  try {
    if (fs.existsSync(LAST_RUN_FILE)) {
      const data = JSON.parse(fs.readFileSync(LAST_RUN_FILE, "utf-8"));
      return typeof data?.endTime === "number" ? data : null;
    }
  } catch (e) {
    logger.warn(`Could not read last_run: ${e.message}`);
  }
  return null;
}

function writeLastRun(endTime) {
  const dir = path.dirname(LAST_RUN_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(LAST_RUN_FILE, JSON.stringify({ endTime, updatedAt: new Date().toISOString() }, null, 2), "utf-8");
}

function ensureCurrentDirs() {
  const attachmentsDir = path.join(BACKUP_CURRENT, "attachments");
  const commentsDir = path.join(BACKUP_CURRENT, "ticket_comments");
  for (const d of [BACKUP_CURRENT, attachmentsDir, commentsDir]) {
    if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
  }
  return { backupDir: BACKUP_CURRENT, attachmentsDir, commentsDir };
}

/**
 * Merge incremental results into existing JSON by id; write back to backupDir.
 */
function mergeAndWrite(backupDir, filename, incrementalItems, key = "id") {
  const filePath = path.join(backupDir, filename);
  let existing = [];
  if (fs.existsSync(filePath)) {
    try {
      existing = JSON.parse(fs.readFileSync(filePath, "utf-8"));
      if (!Array.isArray(existing)) existing = [];
    } catch (e) {
      existing = [];
    }
  }
  const byId = new Map(existing.map((o) => [o[key], o]));
  for (const item of incrementalItems) {
    const id = item[key];
    if (id != null) byId.set(id, item);
  }
  const merged = [...byId.values()];
  writeJson(filePath, merged);
  return merged.length;
}

/**
 * Run incremental backup: fetch only users/tickets updated since last run, merge into current/, fetch comments+attachments only for updated tickets.
 */
async function runIncrementalBackup() {
  const lastRun = readLastRun();
  const hasCurrent = fs.existsSync(path.join(BACKUP_CURRENT, "users.json")) || fs.existsSync(path.join(BACKUP_CURRENT, "tickets.json"));

  if (!lastRun || !hasCurrent) {
    logger.info("📦 No previous run or current backup found; performing full backup and populating current/ ...");
    const result = await runFullBackupToDir(ensureCurrentDirs());
    const endTime = Math.floor(Date.now() / 1000) - 60;
    writeLastRun(endTime);
    logger.info(`   Next incremental will use start_time=${endTime}`);
    return result;
  }

  const startTime = lastRun.endTime;
  if (typeof startTime !== "number" || startTime <= 0) {
    logger.info("📦 Invalid last_run.endTime; performing full backup to current/ ...");
    return runFullBackupToDir(ensureCurrentDirs());
  }

  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📦 Zendesk Backup – incremental (since last run)");
  logger.info("═══════════════════════════════════════════════════════════");

  const { backupDir, attachmentsDir, commentsDir } = ensureCurrentDirs();
  const summary = { users: 0, organizations: 0, tickets: 0, ticketFields: 0, userFields: 0, tags: 0, commentsWritten: 0, attachmentsDownloaded: 0, attachmentsFailed: 0 };

  const [userRes, ticketRes] = await Promise.all([
    fetchIncrementalUsers(startTime),
    fetchIncrementalTickets(startTime),
  ]);

  const incrementalUsers = userRes.users;
  const incrementalTickets = ticketRes.tickets;
  const newEndTime = Math.max(
    userRes.endTime ?? startTime,
    ticketRes.endTime ?? startTime,
    Math.floor(Date.now() / 1000) - 60
  );

  if (incrementalUsers.length === 0 && incrementalTickets.length === 0) {
    logger.info("   No changes since last run.");
    writeLastRun(newEndTime);
    return { backupDir, summary };
  }

  const mergedUsers = mergeAndWrite(backupDir, "users.json", incrementalUsers);
  const mergedTickets = mergeAndWrite(backupDir, "tickets.json", incrementalTickets);
  summary.users = mergedUsers;
  summary.tickets = mergedTickets;

  const allUsers = JSON.parse(fs.readFileSync(path.join(backupDir, "users.json"), "utf-8"));
  const allTickets = JSON.parse(fs.readFileSync(path.join(backupDir, "tickets.json"), "utf-8"));
  const tags = collectTagsFromTicketsAndUsers(allTickets, allUsers);
  writeJson(path.join(backupDir, "tags.json"), tags);
  summary.tags = tags.length;

  const ticketIdsToUpdate = incrementalTickets.map((t) => t.id).filter(Boolean);
  let totalAttachmentsDownloaded = 0;
  let totalAttachmentsFailed = 0;
  const concurrencyComments = Number(process.env.ZENDESK_BACKUP_COMMENTS_CONCURRENCY) || 5;

  const commentTasks = ticketIdsToUpdate.map((ticketId) => async () => {
    const comments = await fetchTicketComments(ticketId);
    writeJson(path.join(commentsDir, `${ticketId}.json`), comments);
    summary.commentsWritten += 1;
    const { downloaded, failed } = await downloadCommentAttachments(comments, attachmentsDir, ticketId);
    totalAttachmentsDownloaded += downloaded;
    totalAttachmentsFailed += failed;
    return { ticketId, commentsCount: comments.length, downloaded, failed };
  });

  await runWithLimit(commentTasks, concurrencyComments);
  summary.attachmentsDownloaded = totalAttachmentsDownloaded;
  summary.attachmentsFailed = totalAttachmentsFailed;

  writeLastRun(newEndTime);

  const manifest = { backupStarted: new Date().toISOString(), backupDir, summary, incremental: true };
  writeJson(path.join(backupDir, "manifest.json"), manifest);

  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("✅ Zendesk Backup (incremental) completed");
  logger.info(`   Users: ${summary.users}, Tickets: ${summary.tickets}, Tags: ${summary.tags}`);
  logger.info(`   Comments/attachments updated for ${ticketIdsToUpdate.length} tickets`);
  logger.info(`   Backup location: ${backupDir}`);
  logger.info("═══════════════════════════════════════════════════════════");
  return { backupDir, summary };
}

/**
 * Run full Zendesk backup into the given dir structure (used for timestamped run or for populating current/).
 * @returns {Promise<{ backupDir: string, summary: Object }>}
 */
async function runFullBackupToDir({ backupDir, attachmentsDir, commentsDir }) {
  const summary = {
    users: 0,
    organizations: 0,
    tickets: 0,
    ticketFields: 0,
    userFields: 0,
    tags: 0,
    commentsWritten: 0,
    attachmentsDownloaded: 0,
    attachmentsFailed: 0,
  };

  const [ticketFields, userFields] = await Promise.all([fetchTicketFields(), fetchUserFields()]);
  writeJson(path.join(backupDir, "ticket_fields.json"), ticketFields);
  writeJson(path.join(backupDir, "user_fields.json"), userFields);
  summary.ticketFields = ticketFields.length;
  summary.userFields = userFields.length;

  const [users, organizations] = await Promise.all([fetchAllUsers(), fetchAllOrganizations()]);
  writeJson(path.join(backupDir, "users.json"), users);
  writeJson(path.join(backupDir, "organizations.json"), organizations);
  summary.users = users.length;
  summary.organizations = organizations.length;

  const tickets = await fetchAllTickets();
  writeJson(path.join(backupDir, "tickets.json"), tickets);
  summary.tickets = tickets.length;

  const tags = collectTagsFromTicketsAndUsers(tickets, users);
  writeJson(path.join(backupDir, "tags.json"), tags);
  summary.tags = tags.length;

  logger.info("📥 Backup: Fetching comments and attachments per ticket...");
  let totalAttachmentsDownloaded = 0;
  let totalAttachmentsFailed = 0;
  const concurrencyComments = Number(process.env.ZENDESK_BACKUP_COMMENTS_CONCURRENCY) || 5;
  const ticketIds = tickets.map((t) => t.id).filter(Boolean);

  const commentTasks = ticketIds.map((ticketId) => async () => {
    const comments = await fetchTicketComments(ticketId);
    writeJson(path.join(commentsDir, `${ticketId}.json`), comments);
    summary.commentsWritten += 1;
    const { downloaded, failed } = await downloadCommentAttachments(comments, attachmentsDir, ticketId);
    totalAttachmentsDownloaded += downloaded;
    totalAttachmentsFailed += failed;
    return { ticketId, commentsCount: comments.length, downloaded, failed };
  });

  await runWithLimit(commentTasks, concurrencyComments);
  summary.attachmentsDownloaded = totalAttachmentsDownloaded;
  summary.attachmentsFailed = totalAttachmentsFailed;

  const manifest = { backupStarted: new Date().toISOString(), backupDir, summary };
  writeJson(path.join(backupDir, "manifest.json"), manifest);

  logger.info(`✅ Zendesk Backup completed. Users: ${summary.users}, Tickets: ${summary.tickets}, Attachments: ${summary.attachmentsDownloaded}`);
  return { backupDir, summary };
}

/**
 * Run full Zendesk backup: users, orgs, tickets, comments, attachments, fields, tags.
 * When ZENDESK_BACKUP_INCREMENTAL=1, runs incremental instead (update current/ and last_run).
 * @returns {Promise<{ backupDir: string, summary: Object }>}
 */
export async function runZendeskBackup() {
  if (INCREMENTAL) {
    return runIncrementalBackup();
  }

  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📦 Zendesk Backup – full backup");
  logger.info("═══════════════════════════════════════════════════════════");

  const dirs = createBackupDirs();
  logger.info(`📁 Backup directory: ${dirs.backupDir}`);
  const result = await runFullBackupToDir(dirs);
  logger.info(`   Backup location: ${result.backupDir}`);
  return result;
}
