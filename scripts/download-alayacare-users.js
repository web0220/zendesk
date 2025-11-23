import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { config } from "../src/config/index.js";
import { logger } from "../src/config/logger.js";
import {
  fetchClients,
  fetchCaregivers,
} from "../src/modules/alayacare/service.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUTPUT_DIR = path.resolve(__dirname, "data");
const TIMESTAMP = new Date().toISOString().replace(/[:.]/g, "-");

const DEFAULT_OUTPUT = (type) =>
  path.join(OUTPUT_DIR, `alayacare-${type}-${TIMESTAMP}.csv`);

const VALID_TYPES = new Set(["clients", "caregivers", "all"]);
const DEFAULT_STATUS = "active";

function parseArgs(argv = []) {
  const options = {
    type: "all",
    status: DEFAULT_STATUS,
    count: 200,
    maxPages: null,
    fetchAll: true,
    output: null,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;

    const [flag, inlineValue] = arg.slice(2).split("=");
    let value = inlineValue;

    if (value === undefined) {
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        value = next;
        i += 1;
      } else {
        value = "true";
      }
    }

    switch (flag) {
      case "type":
        if (VALID_TYPES.has(value)) {
          options.type = value;
        } else {
          throw new Error(
            `Invalid --type value "${value}". Use one of: ${[
              ...VALID_TYPES,
            ].join(", ")}`
          );
        }
        break;
      case "status":
        options.status = value || DEFAULT_STATUS;
        break;
      case "count":
        options.count = Number(value) || 200;
        break;
      case "maxPages":
        options.maxPages = value ? Number(value) : null;
        break;
      case "fetchAll":
        options.fetchAll = value !== "false";
        break;
      case "output":
        options.output = value ? path.resolve(process.cwd(), value) : null;
        break;
      default:
        logger.warn(`⚠️ Unknown argument ignored: --${flag}`);
    }
  }

  return options;
}

function ensureOutputDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (/[",\n]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function normalizePhone(phone) {
  if (!phone) return "";
  const digits = String(phone).replace(/\D/g, "");
  if (digits.length < 10) return phone;
  return digits.startsWith("1") ? `+${digits}` : `+1${digits}`;
}

function pickEmail(record) {
  if (record?.email) return record.email;
  if (record?.demographics?.email) return record.demographics.email;
  if (Array.isArray(record?.contacts)) {
    const email = record.contacts
      .map((contact) => contact?.email || contact?.demographics?.email)
      .find(Boolean);
    if (email) return email;
  }
  return "";
}

function pickPhone(record) {
  const candidates = [
    record?.phone,
    record?.phone_main,
    record?.phone_other,
    record?.phone_personal,
    record?.demographics?.phone_main,
    record?.demographics?.phone_other,
  ].filter(Boolean);
  return candidates.length ? normalizePhone(candidates[0]) : "";
}

function pickOrganization(record, type) {
  if (record?.organization) return record.organization;
  if (record?.branch?.name) return record.branch.name;
  if (record?.branch_name) return record.branch_name;
  if (record?.market && typeof record.market === "string") return record.market;

  if (type === "caregivers" && Array.isArray(record?.departments)) {
    const names = record.departments
      .map((dept) => dept?.name || dept)
      .filter(Boolean);
    if (names.length) {
      return names.join(" | ");
    }
  }

  return "";
}

function toCsvRows(records = [], type) {
  const includeTypeColumn = type === "all";
  const baseHeaders = ["id", "name", "email", "phone", "organization", "status"];
  const headers = includeTypeColumn ? ["type", ...baseHeaders] : baseHeaders;
  const rows = [headers.join(",")];

  for (const item of records) {
    const firstName =
      item?.first_name ||
      item?.firstName ||
      item?.demographics?.first_name ||
      "";
    const lastName =
      item?.last_name ||
      item?.lastName ||
      item?.demographics?.last_name ||
      "";

    const rowValues = [
      item?.id ?? "",
      `${firstName} ${lastName}`.trim(),
      pickEmail(item),
      pickPhone(item),
      pickOrganization(item, type),
      item?.status ?? "",
    ];

    if (includeTypeColumn) {
      rowValues.unshift(item?.__acType || "");
    }

    const row = rowValues.map(escapeCsvValue);

    rows.push(row.join(","));
  }

  return rows.join("\n");
}

function filterByStatus(records = [], status) {
  if (!status) return records;
  const normalizedStatus = String(status).toLowerCase();
  return records.filter(
    (item) => String(item?.status || "").toLowerCase() === normalizedStatus
  );
}

async function fetchRecords(options) {
  const { type, status, count, fetchAll, maxPages } = options;

  if (type === "caregivers") {
    logger.info("📥 Fetching caregivers from AlayaCare...");
    const caregivers = await fetchCaregivers({
      status,
      count,
      fetchAll,
      maxPages,
    });
    return filterByStatus(
      caregivers.map((cg) => ({ ...cg, __acType: "caregiver" })),
      status
    );
  }

  if (type === "clients") {
    logger.info("📥 Fetching clients from AlayaCare...");
    const clients = await fetchClients({
      status,
      count,
      fetchAll,
      maxPages,
    });
    return filterByStatus(
      clients.map((client) => ({ ...client, __acType: "client" })),
      status
    );
  }

  logger.info("📥 Fetching clients and caregivers from AlayaCare...");
  const [clients, caregivers] = await Promise.all([
    fetchClients({ status, count, fetchAll, maxPages }),
    fetchCaregivers({ status, count, fetchAll, maxPages }),
  ]);

  return filterByStatus(
    [
      ...clients.map((client) => ({ ...client, __acType: "client" })),
      ...caregivers.map((cg) => ({ ...cg, __acType: "caregiver" })),
    ],
    status
  );
}

function ensureAlayaConfig() {
  if (
    !config.alayacare.baseUrl ||
    !config.alayacare.publicKey ||
    !config.alayacare.privateKey
  ) {
    throw new Error(
      "AlayaCare credentials are missing. Please set ALAYACARE_BASE_URL, ALAYACARE_PUBLIC_KEY, and ALAYACARE_PRIVATE_KEY in your .env file."
    );
  }
}

async function main() {
  try {
    ensureAlayaConfig();

    const options = parseArgs(process.argv.slice(2));
    const outputPath = options.output || DEFAULT_OUTPUT(options.type);

    const records = await fetchRecords(options);
    logger.info(`✅ Retrieved ${records.length} ${options.type}`);

    const csvData = toCsvRows(records, options.type);
    ensureOutputDir(path.dirname(outputPath));
    fs.writeFileSync(outputPath, csvData, "utf-8");

    logger.info(`💾 CSV exported to: ${outputPath}`);
    logger.info("🎉 Done!");
  } catch (error) {
    logger.error("❌ Failed to export AlayaCare users:", error.message);
    if (error.response) {
      logger.error("AlayaCare Response Status:", error.response.status);
      logger.error(
        "AlayaCare Response Body:",
        JSON.stringify(error.response.data, null, 2)
      );
    }
    process.exitCode = 1;
  } finally {
    logger.close();
  }
}

main();


