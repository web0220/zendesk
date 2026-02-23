/**
 * Verify duplicate phone processing flow: DB → hydrateMapping → processPhoneDuplicates → shared_phone_number → Zendesk payload.
 *
 * Run from project root: node scripts/verify_duplicate_phone_flow.js
 * Or from scripts/: node verify_duplicate_phone_flow.js
 * (Script switches to project root so DB is always project_root/data/sync.db)
 *
 * Checks:
 * 1. Active users in DB have phone/identities in expected shape
 * 2. processPhoneDuplicates finds groups and updates shared_phone_number
 * 3. convertDatabaseRowToZendeskUser includes shared_phone_number in user_fields
 */

import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, "..");

// Switch to project root BEFORE loading db.api.js so it resolves data/sync.db to project_root/data/sync.db
process.chdir(projectRoot);

async function main() {
  const { initDatabase, getDb } = await import("../src/infra/db.api.js");
  const { hydrateMapping, convertDatabaseRowToZendeskUser } = await import("../src/domain/user.db.mapper.js");
  const {
    extractAllPhoneNumbers,
    processDuplicateEmailsAndPhones,
  } = await import("../src/infra/db.duplicate.repo.js");
  const log = (msg) => process.stdout.write(msg + "\n");
  log("═".repeat(60));
  log("Duplicate phone flow verification");
  log("═".repeat(60));

  initDatabase();
  const db = getDb();

  // 1) Check active users and phone data shape
  const activeRows = db
    .prepare("SELECT external_id, ac_id, phone, identities, shared_phone_number, zendesk_primary FROM user_mappings WHERE current_active = 1")
    .all();

  log("\n1) Active users in DB: " + activeRows.length);

  let withPhone = 0;
  let withPhoneInIdentities = 0;
  const hydrated = activeRows.map((row) => hydrateMapping({ ...row }));
  for (const user of hydrated) {
    if (user.phone) withPhone++;
    const phones = extractAllPhoneNumbers(user);
    if (phones.length > 0) withPhoneInIdentities++;
  }
  log("   - With phone field set: " + withPhone);
  log("   - With at least one phone (phone or identities): " + withPhoneInIdentities);

  if (activeRows.length > 0) {
    const sample = hydrated[0];
    log("   - Sample user keys after hydrateMapping: " + Object.keys(sample).sort().join(", "));
    log("   - Sample phone: " + (sample.phone ?? "(null)"));
    log("   - Sample identities type: " + (Array.isArray(sample.identities) ? "array" : typeof sample.identities));
    if (Array.isArray(sample.identities)) {
      const phoneIdentities = sample.identities.filter(
        (i) => i && (i.type === "phone" || i.type === "phone_number")
      );
      log("   - Sample phone identities count: " + phoneIdentities.length);
    }
  }

  // 2) Run duplicate processing (same as Phase 2)
  log("\n2) Running processDuplicateEmailsAndPhones() (includes processPhoneDuplicates)...");
  const usersNeedingPrimary = processDuplicateEmailsAndPhones();
  log("   - Users needing zendesk_primary tag: " + usersNeedingPrimary.length);

  // 3) Check DB after processing
  const afterRows = db
    .prepare("SELECT external_id, ac_id, phone, identities, shared_phone_number, zendesk_primary FROM user_mappings WHERE current_active = 1")
    .all();
  const withShared = afterRows.filter(
    (r) => r.shared_phone_number != null && String(r.shared_phone_number).trim() !== ""
  );
  log("\n3) After duplicate processing:");
  log("   - Active users with shared_phone_number set: " + withShared.length);
  if (withShared.length > 0) {
    const ex = withShared[0];
    log("   - Sample shared_phone_number: " + (ex.shared_phone_number?.substring(0, 80) + (ex.shared_phone_number?.length > 80 ? "..." : "")));
  }

  // 4) Verify mapper includes shared_phone_number in Zendesk payload
  const withSharedRow = afterRows.find(
    (r) => r.shared_phone_number != null && String(r.shared_phone_number).trim() !== ""
  );
  if (withSharedRow) {
    const fullRow = db.prepare("SELECT * FROM user_mappings WHERE external_id = ?").get(withSharedRow.external_id);
    const hydratedFull = hydrateMapping(fullRow);
    const zendeskUser = convertDatabaseRowToZendeskUser(hydratedFull);
    const hasInUserFields =
      zendeskUser?.user_fields && "shared_phone_number" in zendeskUser.user_fields;
    const value = zendeskUser?.user_fields?.shared_phone_number;
    log("\n4) Zendesk payload (convertDatabaseRowToZendeskUser):");
    log("   - user_fields.shared_phone_number present: " + hasInUserFields);
    log("   - user_fields.shared_phone_number value: " + (value == null ? "(null)" : String(value).substring(0, 60) + "..."));
    if (!hasInUserFields || (value == null && withSharedRow.shared_phone_number)) {
      log("   ❌ DISCONNECT: DB has shared_phone_number but payload does not.");
    } else {
      log("   ✅ Connection OK: DB shared_phone_number → user_fields.shared_phone_number");
    }
  } else {
    log("\n4) Skipped (no users with shared_phone_number to verify payload).");
  }

  log("\nNote: If 'Processed 0 non-primary users' and many 'No zendesk_primary user found',");
  log("phone duplicate logic is working but no user in those groups has zendesk_primary=1.");
  log("Once at least one user per group is tagged primary, movePhoneToShared will run for the others.");
  log("\n" + "═".repeat(60));
}

main().catch((err) => {
  process.stderr.write(String(err) + "\n");
  process.exit(1);
});
