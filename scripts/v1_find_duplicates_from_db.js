import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Get the project root (go up from scripts/ to project root)
const projectRoot = path.resolve(__dirname, "..");
// Database is at top-level data/sync.db
const DB_PATH = path.resolve(projectRoot, "data", "sync.db");

/**
 * Find duplicate values in a database column
 * Usage: node scripts/find-duplicates.js [column_name] [table_name]
 * Example: node scripts/find-duplicates.js zendesk_user_id
 * Example: node scripts/find-duplicates.js email user_mappings
 */
async function findDuplicates() {
  const column = process.argv[2] || "zendesk_user_id";
  const table = process.argv[3] || "user_mappings";

  console.log(`🔍 Finding duplicates for column: ${column} in table: ${table}`);
  console.log(`📂 Database: ${DB_PATH}`);
  console.log("━".repeat(60));
  console.log("");

  let db;
  try {
    // Connect directly to the database at top-level data/sync.db
    db = new Database(DB_PATH);

    // Query all non-null values for the column
    const query = `SELECT ${column} FROM ${table} WHERE ${column} IS NOT NULL`;
    const rows = db.prepare(query).all();

    // Count occurrences
    const counts = new Map();
    for (const row of rows) {
      const value = row[column];
      counts.set(value, (counts.get(value) || 0) + 1);
    }

    // Find duplicates
    const duplicates = [];
    for (const [value, count] of counts.entries()) {
      if (count > 1) {
        duplicates.push({ value, count });
      }
    }

    // Sort by count (descending)
    duplicates.sort((a, b) => b.count - a.count);

    // Display results
    if (duplicates.length === 0) {
      console.log("  ✓ No duplicates found!");
    } else {
      for (const { value, count } of duplicates) {
        console.log(`  ✗ ${value} appears ${count} times`);
      }
      console.log("");
      console.log("━".repeat(60));
      console.log(`📊 Summary: Found ${duplicates.length} duplicate value(s)`);
    }

  } catch (error) {
    console.error("❌ Error:", error.message);
    if (error.message.includes("no such column")) {
      console.error(`   Column "${column}" does not exist in table "${table}"`);
    } else if (error.message.includes("no such table")) {
      console.error(`   Table "${table}" does not exist`);
    } else if (error.message.includes("no such file")) {
      console.error(`   Database file not found at: ${DB_PATH}`);
    }
    process.exit(1);
  } finally {
    if (db) {
      db.close();
    }
  }
}

findDuplicates();
