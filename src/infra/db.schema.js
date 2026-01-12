import { logger } from "../config/logger.js";

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
  { name: "scheduling_preferences", def: "scheduling_preferences TEXT" },
  { name: "caregiver_status", def: "caregiver_status TEXT" },
  { name: "department", def: "department TEXT" },
  { name: "market", def: "market TEXT" },
  { name: "identities", def: "identities TEXT" },
  { name: "zendesk_primary", def: "zendesk_primary INTEGER DEFAULT 0" },
  { name: "shared_phone_number", def: "shared_phone_number TEXT" },
  { name: "source_ac_id", def: "source_ac_id TEXT" },
  { name: "current_active", def: "current_active INTEGER DEFAULT 0" },
  { name: "non_active_status_fetched", def: "non_active_status_fetched INTEGER DEFAULT 0" },
];

export function ensureSchema(db) {
  if (!db) {
    throw new Error("Database connection not available");
  }

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
      source_ac_id TEXT,
      coordinator_pod TEXT,
      case_rating TEXT,
      client_status TEXT,
      clinical_rn_manager TEXT,
      sales_rep TEXT,
      caregiver_status TEXT,
      department TEXT,
      market TEXT,
      identities TEXT,
      zendesk_primary INTEGER DEFAULT 0,
      shared_phone_number TEXT,
      last_synced_at TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
  `);

  columnsToAdd.forEach(({ name, def }) => ensureColumnExists(db, "user_mappings", name, def));

  ensureIndexExists(
    db,
    "idx_zendesk_user_id",
    "CREATE INDEX IF NOT EXISTS idx_zendesk_user_id ON user_mappings(zendesk_user_id)"
  );
  ensureIndexExists(
    db,
    "idx_external_id",
    "CREATE INDEX IF NOT EXISTS idx_external_id ON user_mappings(external_id)"
  );
  ensureIndexExists(
    db, 
    "idx_email", 
    "CREATE INDEX IF NOT EXISTS idx_email ON user_mappings(email)");
  ensureIndexExists(
    db,
    "idx_user_type",
    "CREATE INDEX IF NOT EXISTS idx_user_type ON user_mappings(user_type)"
  );
  ensureIndexExists(
    db,
    "idx_organization_id",
    "CREATE INDEX IF NOT EXISTS idx_organization_id ON user_mappings(organization_id)"
  );
  ensureIndexExists(
    db,
    "idx_name",
    "CREATE INDEX IF NOT EXISTS idx_name ON user_mappings(name)"
  );
  ensureIndexExists(
    db,
    "idx_phone",
    "CREATE INDEX IF NOT EXISTS idx_phone ON user_mappings(phone)"
  );
  ensureIndexExists(
    db,
    "idx_client_status",
    "CREATE INDEX IF NOT EXISTS idx_client_status ON user_mappings(client_status)"
  );
  ensureIndexExists(
    db,
    "idx_caregiver_status",
    "CREATE INDEX IF NOT EXISTS idx_caregiver_status ON user_mappings(caregiver_status)"
  );
  ensureIndexExists(
    db,
    "idx_current_active",
    "CREATE INDEX IF NOT EXISTS idx_current_active ON user_mappings(current_active)"
  );
  ensureIndexExists(
    db,
    "idx_non_active_status_fetched",
    "CREATE INDEX IF NOT EXISTS idx_non_active_status_fetched ON user_mappings(non_active_status_fetched)"
  );
}

function ensureColumnExists(db, tableName, columnName, columnDefinition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  const hasColumn = columns.some((column) => column.name === columnName);
  if (!hasColumn) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnDefinition}`);
    logger.info(`🆕 Added missing column "${columnName}" to ${tableName}`);
  }
}

function ensureIndexExists(db, indexName, indexDefinition) {
  try {
    const indexes = db
      .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name=?")
      .get(indexName);
    if (!indexes) {
      db.exec(indexDefinition);
      logger.info(`🆕 Created index "${indexName}"`);
    }
  } catch (err) {
    logger.warn(`⚠️ Failed to create index "${indexName}": ${err.message}`);
  }
}

