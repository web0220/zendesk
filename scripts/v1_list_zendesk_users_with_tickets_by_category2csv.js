import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { logger } from "../src/config/logger.js";
import { getZendeskClient } from "../src/services/zendesk/zendesk.api.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Write CSVs into the top-level logs folder
const OUTPUT_DIR = path.resolve(__dirname, "../logs");
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");

const zendeskClient = getZendeskClient();

// Organization IDs copied from AlayaCare normalizer logic
//  - Client org: 42824772337179
//  - Caregiver org: 43279021546651
//  - Alvita member org: 40994316312731
const ORG_CLIENT = 42824772337179;
const ORG_CAREGIVER = 43279021546651;
const ORG_ALVITA_MEMBER = 40994316312731;

// We only exclude deleted tickets (via API param exclude_deleted=true).
// All other statuses (including custom ones) are counted so the total matches the Zendesk UI.
function appendExcludeDeleted(urlOrPath) {
  const hasParam = /[?&]exclude_deleted=/.test(urlOrPath);
  if (hasParam) return urlOrPath;
  return urlOrPath.includes("?")
    ? `${urlOrPath}&exclude_deleted=true`
    : `${urlOrPath}?exclude_deleted=true`;
}

async function fetchAllTicketsIncremental() {
  const tickets = [];
  let nextPath = appendExcludeDeleted(
    "/incremental/tickets.json?start_time=0"
  );
  let pageCount = 0;

  logger.info(
    "📥 Fetching tickets from Zendesk (exclude_deleted=true)..."
  );

  while (nextPath) {
    const res = await zendeskClient.get(nextPath);
    const {
      tickets: batch = [],
      next_page: nextPage,
      end_of_stream: endOfStream,
    } = res.data || {};

    tickets.push(...batch);
    pageCount++;
    logger.info(
      `   📄 Incremental page ${pageCount}: Retrieved ${batch.length} tickets (total: ${tickets.length})`
    );

    if (endOfStream || !nextPage) {
      nextPath = null;
    } else {
      const base = zendeskClient.defaults.baseURL;
      nextPath = nextPage.startsWith(base)
        ? nextPage.slice(base.length)
        : nextPage;
      nextPath = appendExcludeDeleted(nextPath);
    }
  }

  logger.info(
    `✅ Finished fetching tickets via incremental export. Total raw tickets: ${tickets.length}`
  );
  return tickets;
}

async function fetchRequesterUserIds() {
  logger.info(
    "🔍 Fetching tickets from Zendesk to find users with at least one (non-deleted) ticket..."
  );

  const tickets = await fetchAllTicketsIncremental();

  const statusCounts = new Map();
  for (const t of tickets) {
    const s = (t && t.status) != null ? String(t.status) : "(none)";
    statusCounts.set(s, (statusCounts.get(s) || 0) + 1);
  }
  const statusList = [...statusCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([s, n]) => `${s}: ${n}`)
    .join(", ");
  logger.info(`   📋 Ticket statuses in response: ${statusList}`);

  const requesterIds = new Set();
  for (const ticket of tickets) {
    if (ticket.requester_id) {
      requesterIds.add(ticket.requester_id);
    }
  }

  logger.info(
    `📊 Tickets: ${tickets.length} → ${requesterIds.size} unique requester(s).`
  );
  return Array.from(requesterIds);
}

async function fetchUsersByIds(userIds = []) {
  const users = [];
  if (userIds.length === 0) {
    return users;
  }

  const chunkSize = 100;
  logger.info(
    `📥 Fetching ${userIds.length} users from Zendesk (requesters only)...`
  );

  for (let i = 0; i < userIds.length; i += chunkSize) {
    const chunk = userIds.slice(i, i + chunkSize);
    const params = { ids: chunk.join(",") };

    const res = await zendeskClient.get("/users/show_many.json", { params });
    const batch = res.data?.users || [];
    users.push(...batch);

    logger.info(
      `   ➕ Retrieved ${batch.length} users (total: ${users.length})`
    );
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
    const res = await zendeskClient.get("/organizations/show_many.json", {
      params,
    });
    const organizations = res.data?.organizations || [];

    for (const org of organizations) {
      orgNames.set(org.id, org.name || "");
    }

    logger.info(
      `   🧩 Retrieved ${organizations.length} organizations (chunk ${
        i / chunkSize + 1
      })`
    );
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

function getUserCategory(user, orgNames) {
  const userType = user?.user_fields?.type || null;
  const orgId = user.organization_id || null;
  const orgName = (orgId && orgNames.get(orgId)) || "";
  const email = (user.email || "").toLowerCase();

  // Highest priority: explicit user_fields.type
  if (userType === "client") return "client";
  if (userType === "caregiver") return "caregiver";

  // Alvitacare detection by org id/name/email domain
  const isAlvitaOrgId = orgId === ORG_ALVITA_MEMBER;
  const isAlvitaOrgName =
    typeof orgName === "string" &&
    /alvita\s*care|alvitacare|alayacare/i.test(orgName);
  const isAlvitaEmail =
    typeof email === "string" &&
    (email.endsWith("@alvitacare.com") || email.endsWith("@alayacare.com"));

  if (isAlvitaOrgId || isAlvitaOrgName || isAlvitaEmail) {
    return "alvitacare";
  }

  // No organization set at all
  if (!orgId) {
    return "no_organization";
  }

  // Fallback: organization_id mapping
  if (orgId === ORG_CLIENT) return "client";
  if (orgId === ORG_CAREGIVER) return "caregiver";

  // Fallback: organization name patterns
  if (/client/i.test(orgName)) return "client";
  if (/caregiver|employee|staff/i.test(orgName)) return "caregiver";

  return "no_organization";
}

function convertToCsv(users = [], orgNames = new Map(), category) {
  const headers = [
    "id",
    "name",
    "email",
    "phone",
    "organization",
    "user_type",
    "category",
  ];
  const rows = [headers.join(",")];

  for (const user of users) {
    const organization = orgNames.get(user.organization_id) ?? "";
    const userType = user?.user_fields?.type ?? "";
    const row = [
      user.id ?? "",
      user.name ?? "",
      user.email ?? "",
      user.phone ?? user.mobile ?? "",
      organization,
      userType,
      category,
    ].map(escapeCsvValue);

    rows.push(row.join(","));
  }

  return rows.join("\n");
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
    const orgIds = [
      ...new Set(users.map((u) => u.organization_id).filter(Boolean)),
    ];
    const organizations = await fetchOrganizations(orgIds);

    // Group users by high-level category (ensure all 4 exist)
    const allCategories = ["client", "caregiver", "alvitacare", "no_organization"];
    const usersByCategory = new Map(
      allCategories.map((cat) => [cat, []])
    );

    for (const user of users) {
      const category = getUserCategory(user, organizations);
      if (!usersByCategory.has(category)) {
        usersByCategory.set(category, []);
      }
      usersByCategory.get(category).push(user);
    }

    ensureOutputDir(outputBaseDir);

    // Fixed filenames in logs folder, always write 4 CSVs (even if empty)
    const filenameByCategory = {
      client: "zendesk_users_with_tickets_client.csv",
      caregiver: "zendesk_users_with_tickets_caregiver.csv",
      alvitacare: "zendesk_users_with_tickets_alvitacare.csv",
      no_organization: "zendesk_users_with_tickets_no_organization.csv",
    };

    for (const category of allCategories) {
      const categoryUsers = usersByCategory.get(category) || [];
      const filename = filenameByCategory[category];
      const outputPath = path.join(outputBaseDir, filename);

      const csvData = convertToCsv(categoryUsers, organizations, category);
      fs.writeFileSync(outputPath, csvData, "utf-8");

      logger.info(
        `💾 CSV exported for category "${category}" -> ${outputPath} (${categoryUsers.length} users)`
      );
    }

    logger.info(
      "🎉 Done! Generated 4 CSV files in logs folder: client, caregiver, alvitacare, no_organization."
    );
  } catch (error) {
    logger.error(
      "❌ Failed to export categorized Zendesk users with tickets:",
      error.message
    );

    if (error.response) {
      logger.error("Zendesk Response Status:", error.response.status);
      logger.error(
        "Zendesk Response Body:",
        JSON.stringify(error.response.data, null, 2)
      );
    }

    process.exitCode = 1;
  } finally {
    logger.close();
  }
}

main();

