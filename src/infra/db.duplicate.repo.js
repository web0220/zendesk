import { logger } from "../config/logger.js";
import { hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";

function extractAllEmails(user) {
  const emails = new Set();

  if (user.email) {
    emails.add(user.email.toLowerCase());
  }

  if (user.identities) {
    let identities = user.identities;
    if (typeof identities === "string") {
      try {
        identities = JSON.parse(identities);
      } catch {
        return Array.from(emails);
      }
    }

    if (Array.isArray(identities)) {
      identities.forEach((identity) => {
        if (identity.type === "email" && identity.value) {
          emails.add(identity.value.toLowerCase());
        }
      });
    }
  }

  return Array.from(emails);
}

function extractAllPhoneNumbers(user) {
  const phones = new Set();

  if (user.phone) {
    phones.add(user.phone);
  }

  if (user.identities) {
    let identities = user.identities;
    if (typeof identities === "string") {
      try {
        identities = JSON.parse(identities);
      } catch {
        return Array.from(phones);
      }
    }

    if (Array.isArray(identities)) {
      identities.forEach((identity) => {
        if ((identity.type === "phone" || identity.type === "phone_number") && identity.value) {
          phones.add(identity.value);
        }
      });
    }
  }

  return Array.from(phones);
}

export function processDuplicateEmailsAndPhones() {
  const db = getDb();
  logger.info("🔍 Processing duplicate emails and phone numbers...");

  // Step 1: Find all users with zendesk_primary = 1
  const primaryUsers = db
    .prepare("SELECT * FROM user_mappings WHERE zendesk_primary = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📋 Found ${primaryUsers.length} users with zendesk_primary = 1`);

  if (primaryUsers.length === 0) {
    logger.info("✅ No primary users found, skipping duplicate processing");
    return;
  }

  // Step 2: Get pending users (not yet synced) - these are the ones that might still have conflicts
  // We don't need to check already-synced users because once processed, their emails/phones are changed
  const pendingUsers = db
    .prepare("SELECT * FROM user_mappings WHERE zendesk_user_id IS NULL")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${pendingUsers.length} pending users to check for duplicates`);

  if (pendingUsers.length === 0) {
    logger.info("✅ No pending users found, skipping duplicate processing");
    return;
  }

  // Step 3: Build indexes for emails and phones (including from identities)
  // We need to index all users (including primary and already-synced) to find matches,
  // but we only process duplicates from pending users
  const allUsersForIndex = db
    .prepare("SELECT * FROM user_mappings")
    .all()
    .map(hydrateMapping);

  const emailIndex = new Map();
  const phoneIndex = new Map();

  for (const user of allUsersForIndex) {
    // Index emails (from email field and identities)
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      if (!emailIndex.has(email)) {
        emailIndex.set(email, []);
      }
      emailIndex.get(email).push(user);
    }

    // Index phones (from phone field and identities)
    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user);
    }
  }

  let processedCount = 0;

  // Step 4: For each primary user, find duplicates based on email, phone, or identities
  for (const primaryUser of primaryUsers) {
    const primaryEmails = extractAllEmails(primaryUser);
    const primaryPhones = extractAllPhoneNumbers(primaryUser);

    logger.debug(
      `   Checking primary user ${primaryUser.ac_id}: emails=${primaryEmails.join(", ") || "none"}, phones=${primaryPhones.join(", ") || "none"}`
    );

    if (primaryEmails.length === 0 && primaryPhones.length === 0) {
      logger.debug(`⏭️  Skipping primary user ${primaryUser.ac_id} (no email or phone)`);
      continue;
    }

    // Step 5: Find all pending users who share the same email, phone, or identity values
    const duplicateUsersSet = new Set();

    // Check emails (from email field and identities)
    for (const email of primaryEmails) {
      const usersWithSameEmail = emailIndex.get(email) || [];
      for (const user of usersWithSameEmail) {
        // Only process duplicates that are pending (not yet synced)
        if (user.ac_id !== primaryUser.ac_id && (user.zendesk_user_id === null || user.zendesk_user_id === undefined)) {
          duplicateUsersSet.add(user);
        }
      }
    }

    // Check phones (from phone field and identities)
    for (const phone of primaryPhones) {
      const usersWithSamePhone = phoneIndex.get(phone) || [];
      for (const user of usersWithSamePhone) {
        // Only process duplicates that are pending (not yet synced)
        if (user.ac_id !== primaryUser.ac_id && (user.zendesk_user_id === null || user.zendesk_user_id === undefined)) {
          duplicateUsersSet.add(user);
        }
      }
    }

    const duplicateUsers = Array.from(duplicateUsersSet);

    if (duplicateUsers.length === 0) {
      logger.debug(`   No duplicates found for primary user ${primaryUser.ac_id}`);
      continue;
    }

    logger.info(
      `   Processing ${duplicateUsers.length} duplicate(s) for primary user ${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id})`
    );

    // Step 6: Collect all shared phone numbers (from primary and duplicates)
    const allSharedPhones = new Set(primaryPhones);
    for (const duplicateUser of duplicateUsers) {
      const dupPhones = extractAllPhoneNumbers(duplicateUser);
      dupPhones.forEach((phone) => allSharedPhones.add(phone));
    }
    const sharedPhones = Array.from(allSharedPhones);
    const sharedPhoneNumberStr = sharedPhones.join("\n");

    // Step 7: For each duplicate user, change email to alias and move phone to shared_phone_number
    const normalizedPrimaryEmails = new Set(primaryEmails.map((e) => e.toLowerCase()));

    for (const duplicateUser of duplicateUsers) {
      let newEmail = duplicateUser.email;
      
      // If duplicate user's email matches any primary email, create alias
      if (newEmail && normalizedPrimaryEmails.has(newEmail.toLowerCase())) {
        const emailParts = newEmail.split("@");
        if (emailParts.length === 2) {
          newEmail = `${emailParts[0]}+${duplicateUser.external_id}@${emailParts[1]}`;
        }
      }

      // Filter identities: remove phones and emails that match primary user
      let identities = duplicateUser.identities;
      if (typeof identities === "string") {
        try {
          identities = JSON.parse(identities);
        } catch {
          identities = [];
        }
      }
      if (!Array.isArray(identities)) {
        identities = [];
      }

      const duplicateUserEmails = new Set(extractAllEmails(duplicateUser).map((e) => e.toLowerCase()));
      const filteredIdentities = identities.filter((identity) => {
        // Remove all phone identities (they'll be in shared_phone_number)
        if (identity.type === "phone" || identity.type === "phone_number") {
          return false;
        }
        // Remove email identities that match primary user's emails or duplicate user's own email
        if (identity.type === "email") {
          const identityEmail = identity.value?.toLowerCase();
          if (
            normalizedPrimaryEmails.has(identityEmail) ||
            (duplicateUser.email && identityEmail === duplicateUser.email.toLowerCase())
          ) {
            return false;
          }
        }
        return true;
      });

      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET email = ?,
            phone = NULL,
            identities = ?,
            shared_phone_number = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      updateStmt.run(newEmail, JSON.stringify(filteredIdentities), sharedPhoneNumberStr, duplicateUser.ac_id);
      logger.debug(
        `   Updated duplicate user ${duplicateUser.ac_id}: email=${newEmail || "null"}, moved ${extractAllPhoneNumbers(duplicateUser).length} phone(s) to shared_phone_number`
      );
      processedCount++;
    }

    // Step 8: Ensure primary user has shared_phone_number set to NULL
    const updatePrimaryStmt = db.prepare(`
      UPDATE user_mappings
      SET shared_phone_number = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE ac_id = ?
    `);

    updatePrimaryStmt.run(primaryUser.ac_id);
    logger.debug(`   Updated primary user ${primaryUser.ac_id}: set shared_phone_number to NULL`);
  }

  logger.info(`✅ Processed ${processedCount} duplicate users`);
}

