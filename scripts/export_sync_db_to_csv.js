#!/usr/bin/env node
/**
 * Export sync.db tables to CSV files.
 * Usage: node scripts/export_sync_db_to_csv.js [output_dir] [table_name]
 * Examples:
 *   node scripts/export_sync_db_to_csv.js
 *   node scripts/export_sync_db_to_csv.js ./exports
 *   node scripts/export_sync_db_to_csv.js ./exports user_mappings
 */

import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const DB_PATH = path.resolve(projectRoot, "data", "sync.db");

function escapeCsvField(value) {
  if (value === null || value === undefined) {
    return "";
  }
  const str = String(value);
  const needsQuotes = /[",\r\n]/.test(str);
  if (!needsQuotes) return str;
  return `"${str.replace(/"/g, '""')}"`;
}

function tableToCsv(db, tableName) {
  const columns = db
    .prepare(`PRAGMA table_info(${tableName})`)
    .all()
    .map((col) => col.name);
  if (columns.length === 0) return null;

  const header = columns.map(escapeCsvField).join(",");
  const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
  const lines = [header];
  for (const row of rows) {
    const line = columns.map((col) => escapeCsvField(row[col])).join(",");
    lines.push(line);
  }
  return lines.join("\n");
}

function getTableNames(db) {
  const rows = db
    .prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    .all();
  return rows.map((r) => r.name);
}

function main() {
  const outputDir = path.resolve(projectRoot, process.argv[2] || "data");
  const tableArg = process.argv[3]; // optional: single table

  if (!fs.existsSync(DB_PATH)) {
    console.error(`Database not found: ${DB_PATH}`);
    process.exit(1);
  }

  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const db = new Database(DB_PATH, { readonly: true });
  const tables = tableArg ? [tableArg] : getTableNames(db);

  console.log(`Exporting ${tables.length} table(s) from ${DB_PATH} to ${outputDir}\n`);

  for (const tableName of tables) {
    try {
      const csv = tableToCsv(db, tableName);
      if (csv === null) {
        console.warn(`Skipped ${tableName}: no columns or table missing`);
        continue;
      }
      const outPath = path.join(outputDir, `${tableName}.csv`);
      fs.writeFileSync(outPath, csv, "utf8");
      const rows = csv.split("\n").length - 1;
      console.log(`Wrote ${outPath} (${rows} rows)`);
    } catch (err) {
      console.error(`Error exporting ${tableName}:`, err.message);
    }
  }

  db.close();
  console.log("\nDone.");
}

main();
