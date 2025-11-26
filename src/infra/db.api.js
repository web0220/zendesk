import Database from "better-sqlite3";
import path from "path";
import fs from "fs";
import { logger } from "../config/logger.js";
import { ensureSchema } from "./db.schema.js";

const DB_PATH = path.resolve("data", "sync.db");
let db;

export function initDatabase() {
  if (db) {
    return db;
  }

  const dataDir = path.dirname(DB_PATH);
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
    logger.info(`📁 Created data directory: ${dataDir}`);
  }

  db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  logger.info(`📂 Database initialized at ${DB_PATH}`);

  ensureSchema(db);

  return db;
}

export function getDb() {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }
  return db;
}

export function closeDatabase() {
  if (db) {
    db.close();
    db = null;
    logger.info("🔒 Database connection closed");
  }
}

logger.info("🗄️  Database API module loaded");

