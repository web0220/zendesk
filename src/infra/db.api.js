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
  
  // Checkpoint any leftover WAL files from previous runs (safe recovery)
  try {
    db.pragma("wal_checkpoint(TRUNCATE)");
    logger.info("✅ Checkpointed WAL file (merged any leftover changes)");
  } catch (error) {
    logger.warn(`⚠️ WAL checkpoint warning: ${error.message}`);
    // Continue anyway - SQLite will handle recovery automatically
  }
  
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
    try {
      // Checkpoint WAL before closing to merge changes and clean up WAL file
      db.pragma("wal_checkpoint(TRUNCATE)");
      logger.info("✅ Checkpointed WAL before closing");
    } catch (error) {
      logger.warn(`⚠️ WAL checkpoint warning during close: ${error.message}`);
      // Continue to close anyway
    }
    db.close();
    db = null;
    logger.info("🔒 Database connection closed");
  }
}

logger.info("🗄️  Database API module loaded");

