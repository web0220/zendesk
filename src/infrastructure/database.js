import Database from "better-sqlite3";
import path from "path";
import fs from "fs";
import { logger } from "../config/logger.js";

const DB_PATH = path.resolve("data", "sync.db");

let db;

function ensureColumnExists(tableName, columnName, columnDefinition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  const hasColumn = columns.some((column) => column.name === columnName);
  if (!hasColumn) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnDefinition}`);
    logger.info(`🆕 Added missing column "${columnName}" to ${tableName}`);
  }
}

function ensureIndexExists(indexName, indexDefinition) {
  try {
    const indexes = db.prepare(`SELECT name FROM sqlite_master WHERE type='index' AND name=?`).get(indexName);
    if (!indexes) {
      db.exec(indexDefinition);
      logger.info(`🆕 Created index "${indexName}"`);
    }
  } catch (err) {
    // If index creation fails (e.g., column doesn't exist), log warning but don't fail
    logger.warn(`⚠️ Failed to create index "${indexName}": ${err.message}`);
  }
}

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

    // Create user_mappings table with all mapped data fields
    db.exec(`
      CREATE TABLE IF NOT EXISTS user_mappings (
        ac_id TEXT PRIMARY KEY,
        zendesk_user_id INTEGER,
        external_id TEXT NOT NULL,
        name TEXT,
        email TEXT,
        phone TEXT,
        organization_id INTEGER,
        user_type TEXT,
        -- Client-specific fields
        coordinator_pod TEXT,
        case_rating TEXT,
        client_status TEXT,
        clinical_rn_manager TEXT,
        sales_rep TEXT,
        -- Caregiver-specific fields
        caregiver_status TEXT,
        department TEXT,
        -- Common fields
        market TEXT,
        identities TEXT,
        last_synced_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Ensure newer columns exist for previously created databases (migration support)
    // MUST be done BEFORE creating indexes
    const columnsToAdd = [
      { name: "name", def: "name TEXT" },
      { name: "email", def: "email TEXT" },
      { name: "phone", def: "phone TEXT" },
      { name: "organization_id", def: "organization_id INTEGER" },
      { name: "user_type", def: "user_type TEXT" },
      { name: "coordinator_pod", def: "coordinator_pod TEXT" },
      { name: "case_rating", def: "case_rating TEXT" },
      { name: "client_status", def: "client_status TEXT" },
      { name: "clinical_rn_manager", def: "clinical_rn_manager TEXT" },
      { name: "sales_rep", def: "sales_rep TEXT" },
      { name: "caregiver_status", def: "caregiver_status TEXT" },
      { name: "department", def: "department TEXT" },
      { name: "market", def: "market TEXT" },
      { name: "identities", def: "identities TEXT" },
    ];

    columnsToAdd.forEach(({ name, def }) => {
      ensureColumnExists("user_mappings", name, def);
    });

    // Create indexes for commonly queried fields
    // MUST be done AFTER ensuring columns exist
    ensureIndexExists("idx_zendesk_user_id", "CREATE INDEX IF NOT EXISTS idx_zendesk_user_id ON user_mappings(zendesk_user_id)");
    ensureIndexExists("idx_external_id", "CREATE INDEX IF NOT EXISTS idx_external_id ON user_mappings(external_id)");
    ensureIndexExists("idx_email", "CREATE INDEX IF NOT EXISTS idx_email ON user_mappings(email)");
    ensureIndexExists("idx_user_type", "CREATE INDEX IF NOT EXISTS idx_user_type ON user_mappings(user_type)");
    ensureIndexExists("idx_organization_id", "CREATE INDEX IF NOT EXISTS idx_organization_id ON user_mappings(organization_id)");
    ensureIndexExists("idx_name", "CREATE INDEX IF NOT EXISTS idx_name ON user_mappings(name)");
    ensureIndexExists("idx_phone", "CREATE INDEX IF NOT EXISTS idx_phone ON user_mappings(phone)");
    ensureIndexExists("idx_client_status", "CREATE INDEX IF NOT EXISTS idx_client_status ON user_mappings(client_status)");
    ensureIndexExists("idx_caregiver_status", "CREATE INDEX IF NOT EXISTS idx_caregiver_status ON user_mappings(caregiver_status)");

    logger.info("✅ Database tables initialized");
  } catch (err) {
    logger.error("❌ Database initialization failed:", err);
    throw err;
  }
}

/**
 * Extract and normalize fields from mapped user data
 * @param {Object} mappedData - Mapped user data from mapper
 * @returns {Object} Extracted fields for database storage
 */
function extractMappedFields(mappedData) {
  if (!mappedData) return {};

  const userFields = mappedData.user_fields || {};
  const userType = userFields.type || null;

  // Helper to convert arrays/objects to JSON strings
  const toJsonString = (value) => {
    if (value === null || value === undefined) return null;
    if (Array.isArray(value) || typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value);
  };

  const extracted = {
    name: mappedData.name || null,
    email: mappedData.email || null,
    phone: mappedData.phone || null,
    organization_id: mappedData.organization_id || null,
    user_type: userType,
    identities: toJsonString(mappedData.identities),
    market: toJsonString(userFields.market),
  };

  // Client-specific fields
  if (userType === "client") {
    extracted.coordinator_pod = userFields.coordinator_pod || null;
    extracted.case_rating = userFields.case_rating || null;
    extracted.client_status = userFields.client_status || null;
    extracted.clinical_rn_manager = toJsonString(userFields.clinical_rn_manager);
    extracted.sales_rep = toJsonString(userFields.sales_rep);
  }

  // Caregiver-specific fields
  if (userType === "caregiver") {
    extracted.caregiver_status = userFields.caregiver_status || null;
    extracted.department = toJsonString(userFields.department);
  }

  return extracted;
}

/**
 * Store mapped data to database BEFORE sending to Zendesk
 * This preserves all mapped data and doesn't require zendesk_user_id
 * If record already exists with zendesk_user_id, mapped data is NOT updated (preserved)
 * @param {Object} mappedData - Mapped user data from mapper
 * @returns {void}
 */
export function saveMappedDataToDatabase(mappedData) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  if (!mappedData || !mappedData.ac_id || !mappedData.external_id) {
    logger.warn("⚠️ Skipping invalid mapped data (missing ac_id or external_id)");
    return;
  }

  const { ac_id, external_id } = mappedData;
  const fields = extractMappedFields(mappedData);

  // Check if record already exists with zendesk_user_id
  const existing = db.prepare("SELECT zendesk_user_id FROM user_mappings WHERE ac_id = ?").get(ac_id);
  
  if (existing && existing.zendesk_user_id !== null) {
    // Record already synced - preserve mapped data, don't update
    logger.debug(`⏭️  Skipping mapped data update for ac_id=${ac_id} (already synced, preserving mapped data)`);
    return;
  }

  const stmt = db.prepare(`
    INSERT INTO user_mappings (
      ac_id, zendesk_user_id, external_id, name, email, phone, organization_id,
      user_type, coordinator_pod, case_rating, client_status, clinical_rn_manager,
      sales_rep, caregiver_status, department, market, identities,
      last_synced_at, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT(ac_id) DO UPDATE SET
      -- Only update mapped data fields if zendesk_user_id is NULL (not yet synced)
      -- If zendesk_user_id exists, preserve all mapped data
      external_id = CASE WHEN zendesk_user_id IS NULL THEN excluded.external_id ELSE external_id END,
      name = CASE WHEN zendesk_user_id IS NULL THEN excluded.name ELSE name END,
      email = CASE WHEN zendesk_user_id IS NULL THEN excluded.email ELSE email END,
      phone = CASE WHEN zendesk_user_id IS NULL THEN excluded.phone ELSE phone END,
      organization_id = CASE WHEN zendesk_user_id IS NULL THEN excluded.organization_id ELSE organization_id END,
      user_type = CASE WHEN zendesk_user_id IS NULL THEN excluded.user_type ELSE user_type END,
      coordinator_pod = CASE WHEN zendesk_user_id IS NULL THEN excluded.coordinator_pod ELSE coordinator_pod END,
      case_rating = CASE WHEN zendesk_user_id IS NULL THEN excluded.case_rating ELSE case_rating END,
      client_status = CASE WHEN zendesk_user_id IS NULL THEN excluded.client_status ELSE client_status END,
      clinical_rn_manager = CASE WHEN zendesk_user_id IS NULL THEN excluded.clinical_rn_manager ELSE clinical_rn_manager END,
      sales_rep = CASE WHEN zendesk_user_id IS NULL THEN excluded.sales_rep ELSE sales_rep END,
      caregiver_status = CASE WHEN zendesk_user_id IS NULL THEN excluded.caregiver_status ELSE caregiver_status END,
      department = CASE WHEN zendesk_user_id IS NULL THEN excluded.department ELSE department END,
      market = CASE WHEN zendesk_user_id IS NULL THEN excluded.market ELSE market END,
      identities = CASE WHEN zendesk_user_id IS NULL THEN excluded.identities ELSE identities END,
      updated_at = CURRENT_TIMESTAMP
  `);

  stmt.run(
    ac_id,
    null, // zendesk_user_id - will be set after Zendesk sync
    external_id,
    fields.name,
    fields.email,
    fields.phone,
    fields.organization_id,
    fields.user_type,
    fields.coordinator_pod,
    fields.case_rating,
    fields.client_status,
    fields.clinical_rn_manager,
    fields.sales_rep,
    fields.caregiver_status,
    fields.department,
    fields.market,
    fields.identities,
    null // last_synced_at - will be set after Zendesk sync
  );
  logger.debug(`💾 Saved mapped data: ac_id=${ac_id}, type=${fields.user_type}`);
}

/**
 * Store or update user mapping (legacy function - now only updates zendesk_user_id)
 * @param {Object} mapping - User mapping data
 * @param {string} mapping.ac_id - AlayaCare user ID
 * @param {number} mapping.zendesk_user_id - Zendesk user ID
 * @param {string} mapping.external_id - External ID (formatted AC ID)
 * @param {string} mapping.last_synced_at - Last sync timestamp
 * @param {Object} mapping.mapped_data - Full mapped user data object
 */
export function upsertUserMapping(mapping) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const { ac_id, zendesk_user_id, external_id, last_synced_at, mapped_data } =
    mapping;

  // Extract individual fields from mapped_data
  const fields = extractMappedFields(mapped_data);

  const stmt = db.prepare(`
    INSERT INTO user_mappings (
      ac_id, zendesk_user_id, external_id, name, email, phone, organization_id,
      user_type, coordinator_pod, case_rating, client_status, clinical_rn_manager,
      sales_rep, caregiver_status, department, market, identities,
      last_synced_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(ac_id) DO UPDATE SET
      zendesk_user_id = excluded.zendesk_user_id,
      external_id = excluded.external_id,
      name = excluded.name,
      email = excluded.email,
      phone = excluded.phone,
      organization_id = excluded.organization_id,
      user_type = excluded.user_type,
      coordinator_pod = excluded.coordinator_pod,
      case_rating = excluded.case_rating,
      client_status = excluded.client_status,
      clinical_rn_manager = excluded.clinical_rn_manager,
      sales_rep = excluded.sales_rep,
      caregiver_status = excluded.caregiver_status,
      department = excluded.department,
      market = excluded.market,
      identities = excluded.identities,
      last_synced_at = excluded.last_synced_at,
      updated_at = CURRENT_TIMESTAMP
  `);

  stmt.run(
    ac_id,
    zendesk_user_id,
    external_id,
    fields.name,
    fields.email,
    fields.phone,
    fields.organization_id,
    fields.user_type,
    fields.coordinator_pod,
    fields.case_rating,
    fields.client_status,
    fields.clinical_rn_manager,
    fields.sales_rep,
    fields.caregiver_status,
    fields.department,
    fields.market,
    fields.identities,
    last_synced_at
  );
  logger.debug(`💾 Stored mapping: ac_id=${ac_id}, zendesk_user_id=${zendesk_user_id}, type=${fields.user_type}`);
}

/**
 * Parse JSON string fields back to arrays/objects
 * @param {Object} row - Database row
 * @returns {Object} Row with parsed JSON fields
 */
function hydrateMapping(row) {
  if (!row) return row;

  // Fields that are stored as JSON strings and need parsing
  const jsonFields = [
    "identities",
    "market",
    "clinical_rn_manager",
    "sales_rep",
    "department",
  ];

  jsonFields.forEach((field) => {
    if (row[field] && typeof row[field] === "string") {
      try {
        row[field] = JSON.parse(row[field]);
      } catch (err) {
        // If parsing fails, keep as string
        logger.debug(`⚠️ Failed to parse ${field} JSON, keeping as string`);
      }
    }
  });

  return row;
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
  const row = stmt.get(ac_id);
  return hydrateMapping(row) || null;
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
  const row = stmt.get(zendesk_user_id);
  return hydrateMapping(row) || null;
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
  return stmt.all().map(hydrateMapping);
}

/**
 * Get users that need to be synced to Zendesk (where zendesk_user_id is NULL)
 * @returns {Array<Object>} Users that need syncing
 */
export function getUsersPendingSync() {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const stmt = db.prepare("SELECT * FROM user_mappings WHERE zendesk_user_id IS NULL ORDER BY created_at ASC");
  return stmt.all().map(hydrateMapping);
}

/**
 * Convert database row to Zendesk user format
 * @param {Object} row - Database row with hydrated JSON fields
 * @returns {Object} Zendesk user object
 */
export function convertDatabaseRowToZendeskUser(row) {
  if (!row) return null;

  const userFields = {};

  // Add user_type
  if (row.user_type) {
    userFields.type = row.user_type;
  }

  // Client-specific fields
  if (row.user_type === "client") {
    if (row.coordinator_pod) userFields.coordinator_pod = row.coordinator_pod;
    if (row.case_rating) userFields.case_rating = row.case_rating;
    if (row.client_status) userFields.client_status = row.client_status;
    if (row.clinical_rn_manager) userFields.clinical_rn_manager = row.clinical_rn_manager;
    if (row.sales_rep) userFields.sales_rep = row.sales_rep;
  }

  // Caregiver-specific fields
  if (row.user_type === "caregiver") {
    if (row.caregiver_status) userFields.caregiver_status = row.caregiver_status;
    if (row.department) userFields.department = row.department;
  }

  // Common fields
  if (row.market) userFields.market = row.market;

  // Handle identities - ensure it's an array
  let identities = [];
  if (row.identities) {
    if (Array.isArray(row.identities)) {
      identities = row.identities;
    } else if (typeof row.identities === "string") {
      try {
        identities = JSON.parse(row.identities);
      } catch (err) {
        logger.debug(`⚠️ Failed to parse identities JSON for ac_id=${row.ac_id}`);
      }
    }
  }

  const zendeskUser = {
    external_id: row.external_id,
    ac_id: row.ac_id,
    name: row.name,
    email: row.email || undefined, // Omit if null
    phone: row.phone || undefined, // Omit if null
    organization_id: row.organization_id || undefined, // Omit if null
    identities: identities,
    user_fields: userFields,
  };

  // Remove undefined fields
  Object.keys(zendeskUser).forEach(key => {
    if (zendeskUser[key] === undefined) {
      delete zendeskUser[key];
    }
  });

  return zendeskUser;
}

/**
 * Update only zendesk_user_id and last_synced_at without changing mapped data
 * This preserves all mapped data fields
 * @param {string} ac_id - AlayaCare user ID
 * @param {number} zendesk_user_id - Zendesk user ID
 * @param {string} last_synced_at - Last sync timestamp
 */
export function updateZendeskUserId(ac_id, zendesk_user_id, last_synced_at) {
  if (!db) {
    throw new Error("Database not initialized. Call initDatabase() first.");
  }

  const stmt = db.prepare(`
    UPDATE user_mappings
    SET zendesk_user_id = ?,
        last_synced_at = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
  `);

  stmt.run(zendesk_user_id, last_synced_at, ac_id);
  logger.debug(`🔄 Updated zendesk_user_id: ac_id=${ac_id} → zendesk_user_id=${zendesk_user_id}`);
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

