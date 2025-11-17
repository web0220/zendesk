import Database from "better-sqlite3";
import path from "path";
import fs from "fs";
import { logger } from "../config/logger.js";

const DB_PATH = path.resolve("data", "sync.db");

let db;

/**
 * Initialize the database connection and create tables
 */
export function initDatabase() {
  try {
    // Ensure data directory exists
    const dataDir = path.dirname(DB_PATH);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
      logger.info(`📁 Created data directory: ${dataDir}`);
    }

    db = new Database(DB_PATH);
    logger.info(`📂 Database initialized at ${DB_PATH}`);

    // Create user_mappings table
    db.exec(`
      CREATE TABLE IF NOT EXISTS user_mappings (
        ac_id TEXT PRIMARY KEY,
        zendesk_user_id INTEGER NOT NULL,
        external_id TEXT NOT NULL,
        last_synced_at TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_zendesk_user_id ON user_mappings(zendesk_user_id);
      CREATE INDEX IF NOT EXISTS idx_external_id ON user_mappings(external_id);
    `);

    logger.info("✅ Database tables initialized");
  } catch (err) {
    logger.error("❌ Database initialization failed:", err);
    throw err;
  }
}

/**
 * Store or update user mapping
 * @param {Object} mapping - User mapping data
 * @param {string} mapping.ac_id - AlayaCare user ID
 * @param {number} mapping.zendesk_user_id - Zendesk user ID
 * @param {string} mapping.external_id - External ID (formatted AC ID)
 * @param {string} mapping.last_synced_at - Last sync timestamp
 */
export function upsertUserMapping(mapping) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const { ac_id, zendesk_user_id, external_id, last_synced_at } = mapping;

  const stmt = db.prepare(`
    INSERT INTO user_mappings (ac_id, zendesk_user_id, external_id, last_synced_at, updated_at)
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(ac_id) DO UPDATE SET
      zendesk_user_id = excluded.zendesk_user_id,
      external_id = excluded.external_id,
      last_synced_at = excluded.last_synced_at,
      updated_at = CURRENT_TIMESTAMP
  `);

  stmt.run(ac_id, zendesk_user_id, external_id, last_synced_at);
  logger.debug(`💾 Stored mapping: ac_id=${ac_id}, zendesk_user_id=${zendesk_user_id}`);
}

/**
 * Get user mapping by AlayaCare ID
 * @param {string} ac_id - AlayaCare user ID
 * @returns {Object|null} User mapping or null if not found
 */
export function getUserMappingByAcId(ac_id) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const stmt = db.prepare("SELECT * FROM user_mappings WHERE ac_id = ?");
  return stmt.get(ac_id) || null;
}

/**
 * Get user mapping by Zendesk user ID
 * @param {number} zendesk_user_id - Zendesk user ID
 * @returns {Object|null} User mapping or null if not found
 */
export function getUserMappingByZendeskId(zendesk_user_id) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const stmt = db.prepare("SELECT * FROM user_mappings WHERE zendesk_user_id = ?");
  return stmt.get(zendesk_user_id) || null;
}

/**
 * Get all user mappings
 * @returns {Array<Object>} All user mappings
 */
export function getAllUserMappings() {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const stmt = db.prepare("SELECT * FROM user_mappings ORDER BY updated_at DESC");
  return stmt.all();
}

/**
 * Close the database connection
 */
export function closeDatabase() {
  if (db) {
    db.close();
    logger.info("🔒 Database connection closed");
  }
}

logger.info("🗄️  Database module loaded");

