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

/**
 * Find groups of users who share the same email addresses.
 * Groups are based ONLY on email matches (from email field and identities).
 */
function findEmailGroups(allUsers) {
  // Build email index: email -> [users who have this email]
  const emailIndex = new Map();

  for (const user of allUsers) {
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      if (!emailIndex.has(email)) {
        emailIndex.set(email, []);
      }
      emailIndex.get(email).push(user);
    }
  }

  // Find groups: users connected by shared emails
  const userGroups = new Map(); // user.ac_id -> Set of user.ac_ids in same group

  // Use union-find to connect users who share any email
  const parent = new Map();
  const rank = new Map();

  function find(x) {
    if (!parent.has(x)) {
      parent.set(x, x);
      rank.set(x, 0);
    }
    if (parent.get(x) !== x) {
      parent.set(x, find(parent.get(x)));
    }
    return parent.get(x);
  }

  function union(x, y) {
    const rootX = find(x);
    const rootY = find(y);
    if (rootX === rootY) return;

    if (rank.get(rootX) < rank.get(rootY)) {
      parent.set(rootX, rootY);
    } else if (rank.get(rootX) > rank.get(rootY)) {
      parent.set(rootY, rootX);
    } else {
      parent.set(rootY, rootX);
      rank.set(rootX, rank.get(rootX) + 1);
    }
  }

  // Connect users who share the same email
  for (const [email, users] of emailIndex.entries()) {
    if (users.length > 1) {
      for (let i = 0; i < users.length; i++) {
        for (let j = i + 1; j < users.length; j++) {
          union(users[i].ac_id, users[j].ac_id);
        }
      }
    }
  }

  // Group users by their root parent
  const groups = new Map();
  for (const user of allUsers) {
    const root = find(user.ac_id);
    if (!groups.has(root)) {
      groups.set(root, []);
    }
    groups.get(root).push(user);
  }

  return Array.from(groups.values()).filter((group) => group.length > 1);
}

/**
 * Select the primary user from an email group.
 * Priority: 1) zendesk_primary = 1, 2) already synced, 3) first user
 * Logs a warning if no zendesk_primary user is found.
 */
function selectPrimaryUserForEmailGroup(group) {
  // First, try to find a user with zendesk_primary = 1
  const primaryTagged = group.find((u) => u.zendesk_primary === 1 || u.zendesk_primary === true);
  if (primaryTagged) {
    return primaryTagged;
  }

  // Log warning if no zendesk_primary user found
  const userList = group.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ");
  logger.warn(
    `⚠️  No zendesk_primary user found in email group. Users: ${userList}. Selecting fallback primary.`
  );

  // Second, prefer already synced users (they're more stable)
  const synced = group.filter((u) => u.zendesk_user_id != null);
  if (synced.length > 0) {
    logger.info(`   Selected already-synced user as primary: ${synced[0].ac_id}`);
    return synced[0];
  }

  // Otherwise, just pick the first one (random selection)
  logger.info(`   Selected first user as primary: ${group[0].ac_id}`);
  return group[0];
}

/**
 * Process email duplicates: group users by shared emails, alias duplicate emails.
 * This is the FIRST step and only handles emails, not phones.
 */
function processEmailDuplicates() {
  const db = getDb();
  logger.info("📧 Step 1: Processing duplicate emails...");

  // Get all users
  const allUsers = db
    .prepare("SELECT * FROM user_mappings")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${allUsers.length} total users in database`);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping email duplicate processing");
    return;
  }

  // Find email groups (users connected by shared emails)
  const emailGroups = findEmailGroups(allUsers);
  logger.info(`🔗 Found ${emailGroups.length} email group(s)`);

  if (emailGroups.length === 0) {
    logger.info("✅ No email duplicate groups found");
    return;
  }

  let processedCount = 0;
  const processedUserIds = new Set();

  // Process each email group
  for (const group of emailGroups) {
    // Skip groups where all users are already synced
    const pendingInGroup = group.filter((u) => u.zendesk_user_id == null);
    if (pendingInGroup.length === 0) {
      logger.debug(`⏭️  Skipping email group (all users already synced): ${group.map((u) => u.ac_id).join(", ")}`);
      continue;
    }

    // Select primary user for this email group
    const primaryUser = selectPrimaryUserForEmailGroup(group);
    const duplicateUsers = group.filter((u) => u.ac_id !== primaryUser.ac_id);

    // Skip if already processed
    if (processedUserIds.has(primaryUser.ac_id)) {
      logger.debug(`⏭️  Skipping email group (already processed): primary=${primaryUser.ac_id}`);
      continue;
    }

    logger.info(
      `   Processing email group: primary=${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id}), ${duplicateUsers.length} duplicate(s)`
    );

    const primaryEmails = extractAllEmails(primaryUser);
    const normalizedPrimaryEmails = new Set(primaryEmails.map((e) => e.toLowerCase()));

    // Process each duplicate user in the email group
    for (const duplicateUser of duplicateUsers) {
      // Skip if already synced
      if (duplicateUser.zendesk_user_id != null) {
        logger.debug(`⏭️  Skipping duplicate user ${duplicateUser.ac_id} (already synced)`);
        continue;
      }

      let newEmail = duplicateUser.email;

      // If duplicate user's email matches any primary email, create alias
      if (newEmail && normalizedPrimaryEmails.has(newEmail.toLowerCase())) {
        const emailParts = newEmail.split("@");
        if (emailParts.length === 2) {
          newEmail = `${emailParts[0]}+${duplicateUser.external_id}@${emailParts[1]}`;
          logger.debug(`   Creating alias email for duplicate user ${duplicateUser.ac_id}: ${duplicateUser.email} → ${newEmail}`);
        }
      }

      // Filter identities: remove email identities that match primary user's emails
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

      const filteredIdentities = identities.filter((identity) => {
        // Keep phone identities (they'll be processed separately)
        if (identity.type === "phone" || identity.type === "phone_number") {
          return true;
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

      // Update duplicate user: only change email and identities, DON'T touch phone or shared_phone_number
      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET email = ?,
            identities = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      updateStmt.run(newEmail, JSON.stringify(filteredIdentities), duplicateUser.ac_id);
      logger.debug(`   Updated duplicate user ${duplicateUser.ac_id}: email=${newEmail || "null"}`);
      processedCount++;
      processedUserIds.add(duplicateUser.ac_id);
    }

    processedUserIds.add(primaryUser.ac_id);
  }

  logger.info(`✅ Processed ${processedCount} duplicate users in ${emailGroups.length} email group(s)`);
}

/**
 * Process phone duplicates: find users with same phone numbers and move to shared_phone_number.
 * This is the SECOND step and only handles phones, not emails.
 */
function processPhoneDuplicates() {
  const db = getDb();
  logger.info("📞 Step 2: Processing duplicate phone numbers...");

  // Get all users (refresh from database after email processing)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings")
    .all()
    .map(hydrateMapping);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping phone duplicate processing");
    return;
  }

  // Build phone index: phone -> [users who have this phone]
  const phoneIndex = new Map();

  for (const user of allUsers) {
    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user);
    }
  }

  // Find phone groups (users who share the same phone)
  const phoneGroups = Array.from(phoneIndex.entries()).filter(([phone, users]) => users.length > 1);
  logger.info(`🔗 Found ${phoneGroups.length} phone group(s)`);

  if (phoneGroups.length === 0) {
    logger.info("✅ No phone duplicate groups found");
    return;
  }

  let processedCount = 0;
  const processedUserIds = new Set();

  // Process each phone group
  for (const [phone, phoneGroup] of phoneGroups) {
    // Skip if all users in group are already synced
    const pendingInGroup = phoneGroup.filter((u) => u.zendesk_user_id == null);
    if (pendingInGroup.length === 0) {
      logger.debug(`⏭️  Skipping phone group (all users already synced): phone=${phone}`);
      continue;
    }

    // Select primary user for this phone group (prefer zendesk_primary)
    const primaryTagged = phoneGroup.find((u) => u.zendesk_primary === 1 || u.zendesk_primary === true);
    let primaryUser;
    
    if (primaryTagged) {
      primaryUser = primaryTagged;
    } else {
      // Log warning if no zendesk_primary user found
      const userList = phoneGroup.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ");
      logger.warn(
        `⚠️  No zendesk_primary user found in phone group (phone=${phone}). Users: ${userList}. Selecting fallback primary.`
      );
      
      // Fallback: prefer already synced users, then first user
      const synced = phoneGroup.filter((u) => u.zendesk_user_id != null);
      if (synced.length > 0) {
        primaryUser = synced[0];
        logger.info(`   Selected already-synced user as primary: ${primaryUser.ac_id}`);
      } else {
        primaryUser = phoneGroup[0];
        logger.info(`   Selected first user as primary: ${primaryUser.ac_id}`);
      }
    }
    
    const duplicateUsers = phoneGroup.filter((u) => u.ac_id !== primaryUser.ac_id);

    // Skip if already processed
    if (processedUserIds.has(primaryUser.ac_id)) {
      logger.debug(`⏭️  Skipping phone group (already processed): primary=${primaryUser.ac_id}`);
      continue;
    }

    logger.info(
      `   Processing phone group: phone=${phone}, primary=${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id}), ${duplicateUsers.length} duplicate(s)`
    );

    // Collect all phone numbers from the entire group
    const allGroupPhones = new Set();
    for (const user of phoneGroup) {
      extractAllPhoneNumbers(user).forEach((p) => allGroupPhones.add(p));
    }
    const sharedPhones = Array.from(allGroupPhones);
    const sharedPhoneNumberStr = sharedPhones.length > 0 ? sharedPhones.join("\n") : null;

    // Process each duplicate user in the phone group
    for (const duplicateUser of duplicateUsers) {
      // Skip if already synced
      if (duplicateUser.zendesk_user_id != null) {
        logger.debug(`⏭️  Skipping duplicate user ${duplicateUser.ac_id} (already synced)`);
        continue;
      }

      // Filter identities: remove phone identities (they'll be in shared_phone_number)
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

      const filteredIdentities = identities.filter((identity) => {
        // Remove all phone identities (they'll be in shared_phone_number)
        if (identity.type === "phone" || identity.type === "phone_number") {
          return false;
        }
        return true;
      });

      // Update duplicate user: move phone to shared_phone_number, remove phone from identities
      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET phone = NULL,
            identities = ?,
            shared_phone_number = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      const phoneCount = extractAllPhoneNumbers(duplicateUser).length;
      updateStmt.run(JSON.stringify(filteredIdentities), sharedPhoneNumberStr, duplicateUser.ac_id);
      logger.debug(
        `   Updated duplicate user ${duplicateUser.ac_id}: moved ${phoneCount} phone(s) to shared_phone_number`
      );
      processedCount++;
      processedUserIds.add(duplicateUser.ac_id);
    }

    // Ensure primary user has shared_phone_number set to NULL
    // (Primary keeps their phone in the phone field, not shared_phone_number)
    const updatePrimaryStmt = db.prepare(`
      UPDATE user_mappings
      SET shared_phone_number = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE ac_id = ?
    `);

    updatePrimaryStmt.run(primaryUser.ac_id);
    logger.debug(`   Updated primary user ${primaryUser.ac_id}: set shared_phone_number to NULL`);
    processedUserIds.add(primaryUser.ac_id);
  }

  logger.info(`✅ Processed ${processedCount} duplicate users in ${phoneGroups.length} phone group(s)`);
}

/**
 * Main function: Process email duplicates first, then phone duplicates separately.
 */
export function processDuplicateEmailsAndPhones() {
  logger.info("🔍 Processing duplicate emails and phone numbers...");

  // Step 1: Process email duplicates (most important - avoid email conflicts)
  processEmailDuplicates();

  // Step 2: Process phone duplicates (independent - move to shared_phone_number)
  processPhoneDuplicates();

  logger.info("✅ Finished processing all duplicates");
}
