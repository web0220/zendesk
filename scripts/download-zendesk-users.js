import fs from "fs";
import path from "path";
import axios from "axios";
import { fileURLToPath } from "url";

import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUTPUT_DIR = path.resolve(__dirname, "data");
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const DEFAULT_OUTPUT = path.join(OUTPUT_DIR, `zendesk-users_${timestamp}.csv`);

const zendeskClient = axios.create({
  baseURL: `https://${config.zendesk.subdomain}.zendesk.com/api/v2`,
  auth: {
    username: `${config.zendesk.email}/token`,
    password: config.zendesk.token,
  },
  headers: { "Content-Type": "application/json" },
});

async function fetchAllUsers() {
  const users = [];
  let nextPath = "/users.json";

  logger.info("📥 Fetching users from Zendesk...");

  while (nextPath) {
    const response = await zendeskClient.get(nextPath);
    const { users: batch = [], next_page: nextPage } = response.data || {};

    users.push(...batch);
    logger.info(`   ➕ Retrieved ${batch.length} users (total: ${users.length})`);

    if (nextPage) {
      const base = zendeskClient.defaults.baseURL;
      nextPath = nextPage.startsWith(base) ? nextPage.slice(base.length) : nextPage;
    } else {
      nextPath = null;
    }
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
  const headers = ["id", "name", "email", "phone", "organization", "status"];
  const rows = [headers.join(",")];

  for (const user of users) {
    const organization = orgNames.get(user.organization_id) ?? "";
    const status = user.active === false ? "inactive" : "active";
    const row = [
      user.id ?? "",
      user.name ?? "",
      user.email ?? "",
      user.phone ?? user.mobile ?? "",
      organization,
      status,
    ].map(escapeCsvValue);

    rows.push(row.join(","));
  }

  return rows.join("\n");
}

async function main() {
  try {
    if (!config.zendesk.subdomain || !config.zendesk.email || !config.zendesk.token) {
      throw new Error("Zendesk credentials are missing. Please check your .env file.");
    }

    const outputPath = process.argv[2]
      ? path.resolve(process.cwd(), process.argv[2])
      : DEFAULT_OUTPUT;

    const users = await fetchAllUsers();
    const orgIds = [...new Set(users.map((u) => u.organization_id).filter(Boolean))];
    const organizations = await fetchOrganizations(orgIds);

    const csvData = convertToCsv(users, organizations);
    ensureOutputDir(path.dirname(outputPath));
    fs.writeFileSync(outputPath, csvData, "utf-8");

    logger.info(`💾 CSV exported to: ${outputPath}`);
    logger.info("🎉 Done!");
  } catch (error) {
    logger.error("❌ Failed to export Zendesk users:", error.message);

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

