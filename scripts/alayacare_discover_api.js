#!/usr/bin/env node
/**
 * AlayaCare API Discovery Script
 * Probes known and common API paths, logs response status and JSON shape.
 * Run from project root: node scripts/alayacare_discover_api.js
 * Optionally write to docs: node scripts/alayacare_discover_api.js --write-docs
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { config } from "../src/config/index.js";
import { alayaClient } from "../src/services/alayacare/alayacare.api.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");

function shapeOf(obj, maxDepth = 2) {
  if (obj === null) return "null";
  if (typeof obj !== "object") return typeof obj;
  if (Array.isArray(obj)) {
    const len = obj.length;
    const sample = obj[0];
    if (len === 0) return "[]";
    return `[${len}] ${shapeOf(sample, maxDepth - 1)}`;
  }
  if (maxDepth <= 0) return "{}";
  const keys = Object.keys(obj).slice(0, 15);
  const sub = {};
  for (const k of keys) {
    sub[k] = shapeOf(obj[k], maxDepth - 1);
  }
  return JSON.stringify(sub, null, 0).slice(0, 200);
}

async function probe(method, pathName, params = null) {
  const label = params ? `${method} ${pathName}?${new URLSearchParams(params)}` : `${method} ${pathName}`;
  try {
    const res = params
      ? await alayaClient.request({ method, url: pathName, params })
      : await alayaClient.request({ method, url: pathName });
    const status = res.status;
    const data = res.data;
    const shape = data !== undefined ? shapeOf(data) : "(no body)";
    return { path: pathName, status, shape, ok: true, error: null };
  } catch (err) {
    const status = err.response?.status ?? "error";
    const message = err.response?.data ? JSON.stringify(err.response.data).slice(0, 150) : err.message;
    return { path: pathName, status, shape: null, ok: false, error: message };
  }
}

async function probeFetch(pathName, params = {}) {
  const basicAuth = Buffer.from(
    `${config.alayacare.publicKey}:${config.alayacare.privateKey}`
  ).toString("base64");
  const url = new URL(pathName, config.alayacare.baseUrl);
  Object.entries(params).forEach(([k, v]) => url.searchParams.append(k, v));
  try {
    const res = await fetch(url.toString(), {
      method: "GET",
      headers: { Authorization: `Basic ${basicAuth}` },
    });
    const text = await res.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = text.slice(0, 200);
    }
    const shape = typeof data === "object" ? shapeOf(data) : String(data).slice(0, 100);
    return { path: pathName, status: res.status, shape, ok: res.ok, error: null };
  } catch (err) {
    return { path: pathName, status: "error", shape: null, ok: false, error: err.message };
  }
}

const knownPaths = [
  { method: "GET", path: "/patients/clients/", params: { page: 1, count: 1 } },
  { method: "GET", path: "/employees/employees/", params: { page: 1, count: 1 } },
];

const commonPaths = [
  "/reports/",
  "/reports",
  "/financial/",
  "/financial",
  "/documents/",
  "/documents",
  "/attachments/",
  "/attachments",
  "/schedules/",
  "/schedules",
  "/confirmations/",
  "/confirmations",
  "/invoices/",
  "/invoices",
  "/visits/",
  "/visits",
  "/ext/api/v2/scheduler/visit",
  "/ext/api/v2/",
  "/api/v2/",
  "/patients/",
  "/employees/",
];

function formatResult(r) {
  const statusStr = r.ok ? `✅ ${r.status}` : `❌ ${r.status}`;
  const shapeStr = r.shape ? ` | ${r.shape}` : "";
  const errStr = r.error ? ` | ${r.error}` : "";
  return `${statusStr} ${r.path}${shapeStr}${errStr}`;
}

async function main() {
  const writeDocs = process.argv.includes("--write-docs");
  const docsPath = path.join(projectRoot, "docs", "alayacare-api-notes.md");

  if (!config.alayacare?.baseUrl || !config.alayacare?.publicKey || !config.alayacare?.privateKey) {
    console.error("Missing AlayaCare config (ALAYACARE_BASE_URL, PUBLIC_KEY, PRIVATE_KEY). Check .env");
    process.exit(1);
  }

  console.log("AlayaCare API Discovery");
  console.log("Base URL:", config.alayacare.baseUrl);
  console.log("");

  const results = [];

  console.log("--- Known paths (from codebase) ---");
  for (const { method, path: p, params } of knownPaths) {
    const r = await probe(method, p, params);
    results.push({ ...r, category: "known" });
    console.log(formatResult(r));
  }

  console.log("\n--- Common REST paths (GET) ---");
  for (const p of commonPaths) {
    const r = await probe("GET", p);
    results.push({ ...r, category: "common" });
    console.log(formatResult(r));
  }

  console.log("\n--- Scheduler visit (native fetch, minimal headers) ---");
  const visitParams = {
    alayacare_employee_id: 1,
    start_at: "2024-01-01T00:00:00",
    end_at: "2024-01-31T23:59:59",
    page: 1,
  };
  const visitResult = await probeFetch("/ext/api/v2/scheduler/visit", visitParams);
  results.push({ ...visitResult, category: "visit" });
  console.log(formatResult(visitResult));

  if (writeDocs) {
    const dir = path.dirname(docsPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const lines = [
      "# AlayaCare API discovery notes",
      "",
      "Generated by `node scripts/alayacare_discover_api.js --write-docs`",
      "",
      "## Summary",
      "",
      "| Path | Status | Shape/Notes |",
      "|------|--------|-------------|",
      ...results.map((r) => `| ${r.path} | ${r.status} | ${r.shape || r.error || "-"} |`),
      "",
      "## Known working endpoints (from codebase)",
      "",
      "- `GET /patients/clients/` – list clients (params: page, count)",
      "- `GET /patients/clients/:id` – client detail",
      "- `GET /employees/employees/` – list employees/caregivers (params: page, count)",
      "- `GET /employees/employees/:id` – employee detail",
      "- `GET /ext/api/v2/scheduler/visit` – visits (params: alayacare_employee_id, start_at, end_at, page, status, cancelled). Use native fetch with only Authorization header.",
      "",
    ];
    fs.writeFileSync(docsPath, lines.join("\n"), "utf-8");
    console.log("\nWrote:", docsPath);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
