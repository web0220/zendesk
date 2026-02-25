import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const DB_PATH = path.resolve(projectRoot, "data", "sync.db");

/**
 * Count unique ac_id values in user_mappings.
 * Usage: node scripts/count_unique_ac_id.js
 */
function countUniqueAcIds() {
  console.log("📂 Database:", DB_PATH);
  console.log("━".repeat(50));

  let db;
  try {
    db = new Database(DB_PATH, { readonly: true });

    const uniqueCount = db
      .prepare(
        "SELECT COUNT(DISTINCT ac_id) AS count FROM user_mappings WHERE ac_id IS NOT NULL"
      )
      .get();

    const totalWithAcId = db
      .prepare(
        "SELECT COUNT(*) AS count FROM user_mappings WHERE ac_id IS NOT NULL"
      )
      .get();

    const totalRows = db.prepare("SELECT COUNT(*) AS count FROM user_mappings").get();

    console.log("  Unique ac_id (distinct):", uniqueCount.count);
    console.log("  Rows with non-null ac_id:", totalWithAcId.count);
    console.log("  Total rows in user_mappings:", totalRows.count);
    console.log("━".repeat(50));
  } catch (error) {
    console.error("❌ Error:", error.message);
    if (error.message.includes("no such table")) {
      console.error('   Table "user_mappings" does not exist');
    } else if (error.message.includes("no such file")) {
      console.error("   Database file not found at:", DB_PATH);
    }
    process.exit(1);
  } finally {
    if (db) {
      db.close();
    }
  }
}

countUniqueAcIds();
