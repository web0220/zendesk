/**
 * Infrastructure layer exports
 * Provides a clean interface for database operations and data access
 */

// Database connection management
export { initDatabase, closeDatabase, getDb } from "./db.api.js";

// Database schema
export { ensureSchema } from "./db.schema.js";

// Repository exports - organized by domain
export * from "./db.sync.repo.js";
export * from "./db.user.repo.js";
export * from "./db.duplicate.repo.js";
export * from "./db.recurring.repo.js";
export * from "./db.acBackend.repo.js";

// Domain mappers
export { convertDatabaseRowToZendeskUser } from "../domain/user.db.mapper.js";

