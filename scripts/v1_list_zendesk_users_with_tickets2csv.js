import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { logger } from "../src/config/logger.js";
import { getZendeskClient } from "../src/services/zendesk/zendesk.api.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUTPUT_DIR = path.resolve(__dirname, "data");
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");

const zendeskClient = getZendeskClient();

async function fetchAllTicketsIncremental() {
  const tickets = [];
  let nextPath = "/incremental/tickets.json?start_time=0";
  let pageCount = 0;

  logger.info("📥 Fetching tickets from Zendesk via incremental export...");

  while (nextPath) {
    const res = await zendeskClient.get(nextPath);
    const { tickets: batch = [], next_page: nextPage, end_of_stream: endOfStream } = res.data || {};

    tickets.push(...batch);
    pageCount++;
    logger.info(`   📄 Incremental page ${pageCount}: Retrieved ${batch.length} tickets (total: ${tickets.length})`);

    if (endOfStream || !nextPage) {
      nextPath = null;
    } else {
      const base = zendeskClient.defaults.baseURL;
      nextPath = nextPage.startsWith(base) ? nextPage.slice(base.length) : nextPage;
    }
  }

  logger.info(`✅ Finished fetching tickets via incremental export. Total tickets: ${tickets.length}`);
  return tickets;
}

async function fetchRequesterUserIds() {
  logger.info("🔍 Fetching tickets from Zendesk to find users with at least one ticket...");

  // Use incremental ticket export to avoid search API response size limits
  const tickets = await fetchAllTicketsIncremental();

  const requesterIds = new Set();
  for (const ticket of tickets) {
    if (ticket.requester_id) {
      requesterIds.add(ticket.requester_id);
    }
  }

  logger.info(`📊 Found ${tickets.length} tickets and ${requesterIds.size} unique requester(s).`);
  return Array.from(requesterIds);
}

async function fetchUsersByIds(userIds = []) {
  const users = [];
  if (userIds.length === 0) {
    return users;
  }

  const chunkSize = 100;
  logger.info(`📥 Fetching ${userIds.length} users from Zendesk (requesters only)...`);

  for (let i = 0; i < userIds.length; i += chunkSize) {
    const chunk = userIds.slice(i, i + chunkSize);
    const params = { ids: chunk.join(",") };

    const res = await zendeskClient.get("/users/show_many.json", { params });
    const batch = res.data?.users || [];
    users.push(...batch);

    logger.info(`   ➕ Retrieved ${batch.length} users (total: ${users.length})`);
  }

  logger.info(`✅ Finished fetching users. Total count: ${users.length}`);
  return users;
}

async function fetchOrganizations(orgIds = []) {
  if (orgIds.length === 0) {
    return new Map();
  }

  const orgNames = new Map();
  const chunkSize = 100;
  logger.info(`🏢 Fetching ${orgIds.length} organizations...`);

  for (let i = 0; i < orgIds.length; i += chunkSize) {
    const chunk = orgIds.slice(i, i + chunkSize);
    const params = { ids: chunk.join(",") };
    const res = await zendeskClient.get("/organizations/show_many.json", { params });
    const organizations = res.data?.organizations || [];

    for (const org of organizations) {
      orgNames.set(org.id, org.name || "");
    }

    logger.info(`   🧩 Retrieved ${organizations.length} organizations (chunk ${i / chunkSize + 1})`);
  }

  return orgNames;
}

function ensureOutputDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) {
    return "";
  }

  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function convertToCsv(users = [], orgNames = new Map()) {
  const headers = ["id", "email", "name", "organization"];
  const rows = [headers.join(",")];

  for (const user of users) {
    const organization = orgNames.get(user.organization_id) ?? "";
    const row = [
      user.id ?? "",
      user.email ?? "",
      user.name ?? "",
      organization,
    ].map(escapeCsvValue);

    rows.push(row.join(","));
  }

  return rows.join("\n");
}

function slugify(name) {
  if (!name) return "no-organization";
  return String(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    || "no-organization";
}

async function main() {
  try {
    const outputBaseDir = process.argv[2]
      ? path.resolve(process.cwd(), process.argv[2])
      : OUTPUT_DIR;

    const requesterIds = await fetchRequesterUserIds();
    if (requesterIds.length === 0) {
      logger.info("✅ No users with tickets were found.");
      return;
    }

    const users = await fetchUsersByIds(requesterIds);
    const orgIds = [...new Set(users.map((u) => u.organization_id).filter(Boolean))];
    const organizations = await fetchOrganizations(orgIds);

    // Group users by organization name (or "No Organization")
    const usersByOrgName = new Map();
    for (const user of users) {
      const orgName = organizations.get(user.organization_id) || "No Organization";
      if (!usersByOrgName.has(orgName)) {
        usersByOrgName.set(orgName, []);
      }
      usersByOrgName.get(orgName).push(user);
    }

    ensureOutputDir(outputBaseDir);

    // Write one CSV per organization
    for (const [orgName, orgUsers] of usersByOrgName.entries()) {
      const safeOrg = slugify(orgName);
      const filename = `zendesk-users-with-tickets_${safeOrg}_${timestamp}.csv`;
      const outputPath = path.join(outputBaseDir, filename);

      const csvData = convertToCsv(orgUsers, organizations);
      fs.writeFileSync(outputPath, csvData, "utf-8");

      logger.info(`💾 CSV exported for organization "${orgName}" -> ${outputPath}`);
    }

    logger.info("🎉 Done! Generated CSV files per organization.");
  } catch (error) {
    logger.error("❌ Failed to export Zendesk users with tickets:", error.message);

    if (error.response) {
      logger.error("Zendesk Response Status:", error.response.status);
      logger.error("Zendesk Response Body:", JSON.stringify(error.response.data, null, 2));
    }

    process.exitCode = 1;
  } finally {
    logger.close();
  }
}

main();

