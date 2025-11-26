import { logger } from "../config/logger.js";
import { hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";

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

  const primaryUsers = db
    .prepare("SELECT * FROM user_mappings WHERE zendesk_primary = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📋 Found ${primaryUsers.length} users with zendesk_primary = 1`);

  if (primaryUsers.length === 0) {
    logger.info("✅ No primary users found, skipping duplicate processing");
    return;
  }

  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE zendesk_user_id IS NULL OR zendesk_primary = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Processing ${allUsers.length} pending/primary users`);

  if (allUsers.length === 0) {
    logger.info("✅ No pending users found, skipping duplicate processing");
    return;
  }

  const emailIndex = new Map();
  const phoneIndex = new Map();

  for (const user of allUsers) {
    if (user.email) {
      const emailKey = user.email.toLowerCase();
      if (!emailIndex.has(emailKey)) {
        emailIndex.set(emailKey, []);
      }
      emailIndex.get(emailKey).push(user);
    }

    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user);
    }
  }

  let processedCount = 0;

  for (const primaryUser of primaryUsers) {
    const primaryEmail = primaryUser.email;
    const primaryPhones = extractAllPhoneNumbers(primaryUser);

    logger.debug(
      `   Checking primary user ${primaryUser.ac_id}: email=${primaryEmail}, phones=${primaryPhones.join(", ")}`
    );

    if (!primaryEmail && primaryPhones.length === 0) {
      logger.debug(`⏭️  Skipping primary user ${primaryUser.ac_id} (no email or phone)`);
      continue;
    }

    const duplicateUsersSet = new Set();

    if (primaryEmail) {
      const emailKey = primaryEmail.toLowerCase();
      const usersWithSameEmail = emailIndex.get(emailKey) || [];
      for (const user of usersWithSameEmail) {
        if (user.ac_id !== primaryUser.ac_id) {
          duplicateUsersSet.add(user);
        }
      }
    }

    for (const phone of primaryPhones) {
      const usersWithSamePhone = phoneIndex.get(phone) || [];
      for (const user of usersWithSamePhone) {
        if (user.ac_id !== primaryUser.ac_id) {
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

    const allSharedPhones = new Set(primaryPhones);
    for (const duplicateUser of duplicateUsers) {
      const dupPhones = extractAllPhoneNumbers(duplicateUser);
      dupPhones.forEach((phone) => allSharedPhones.add(phone));
    }
    const sharedPhones = Array.from(allSharedPhones);
    const sharedPhoneNumberStr = sharedPhones.join("\n");

    for (const duplicateUser of duplicateUsers) {
      let newEmail = duplicateUser.email;
      if (newEmail && primaryEmail && newEmail.toLowerCase() === primaryEmail.toLowerCase()) {
        const emailParts = newEmail.split("@");
        if (emailParts.length === 2) {
          newEmail = `${emailParts[0]}+${duplicateUser.external_id}@${emailParts[1]}`;
        }
      }

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

      const originalDuplicateEmail = duplicateUser.email?.toLowerCase();
      const filteredIdentities = identities.filter((identity) => {
        if (identity.type === "phone" || identity.type === "phone_number") {
          return false;
        }
        if (identity.type === "email" && originalDuplicateEmail) {
          const identityEmail = identity.value?.toLowerCase();
          if (identityEmail === originalDuplicateEmail) {
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
        `   Updated duplicate user ${duplicateUser.ac_id}: email=${newEmail}, moved ${extractAllPhoneNumbers(duplicateUser).length} phone(s) to shared_phone_number`
      );
      processedCount++;
    }

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

