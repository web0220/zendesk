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
 * Find connected components of users who share emails or phones.
 * Uses union-find (disjoint set) algorithm to group connected users.
 */
function findConnectedGroups(allUsers) {
  // Map user ac_id to user object for quick lookup
  const userMap = new Map();
  allUsers.forEach((user) => {
    userMap.set(user.ac_id, user);
  });

  // Union-Find data structure
  const parent = new Map();
  const rank = new Map();

  function find(x) {
    if (!parent.has(x)) {
      parent.set(x, x);
      rank.set(x, 0);
    }
    if (parent.get(x) !== x) {
      parent.set(x, find(parent.get(x))); // Path compression
    }
    return parent.get(x);
  }

  function union(x, y) {
    const rootX = find(x);
    const rootY = find(y);
    if (rootX === rootY) return;

    // Union by rank
    if (rank.get(rootX) < rank.get(rootY)) {
      parent.set(rootX, rootY);
    } else if (rank.get(rootX) > rank.get(rootY)) {
      parent.set(rootY, rootX);
    } else {
      parent.set(rootY, rootX);
      rank.set(rootX, rank.get(rootX) + 1);
    }
  }

  // Build email and phone indexes
  const emailIndex = new Map();
  const phoneIndex = new Map();

  for (const user of allUsers) {
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      if (!emailIndex.has(email)) {
        emailIndex.set(email, []);
      }
      emailIndex.get(email).push(user.ac_id);
    }

    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user.ac_id);
    }
  }

  // Connect users who share the same email
  for (const [email, userIds] of emailIndex.entries()) {
    if (userIds.length > 1) {
      for (let i = 0; i < userIds.length; i++) {
        for (let j = i + 1; j < userIds.length; j++) {
          union(userIds[i], userIds[j]);
        }
      }
    }
  }

  // Connect users who share the same phone
  for (const [phone, userIds] of phoneIndex.entries()) {
    if (userIds.length > 1) {
      for (let i = 0; i < userIds.length; i++) {
        for (let j = i + 1; j < userIds.length; j++) {
          union(userIds[i], userIds[j]);
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
 * Select the primary user from a group of connected users.
 * Priority: 1) zendesk_primary = 1, 2) already synced, 3) first user
 */
function selectPrimaryUser(group) {
  // First, try to find a user with zendesk_primary = 1
  const primaryTagged = group.find((u) => u.zendesk_primary === 1 || u.zendesk_primary === true);
  if (primaryTagged) {
    return primaryTagged;
  }

  // Second, prefer already synced users (they're more stable)
  const synced = group.filter((u) => u.zendesk_user_id != null);
  if (synced.length > 0) {
    return synced[0];
  }

  // Otherwise, just pick the first one
  return group[0];
}

export function processDuplicateEmailsAndPhones() {
  const db = getDb();
  logger.info("🔍 Processing duplicate emails and phone numbers...");

  // Step 1: Get all users (we need to see all connections)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${allUsers.length} total users in database`);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping duplicate processing");
    return;
  }

  // Step 2: Find connected groups (users who share emails or phones)
  const connectedGroups = findConnectedGroups(allUsers);
  logger.info(`🔗 Found ${connectedGroups.length} connected group(s) of users`);

  if (connectedGroups.length === 0) {
    logger.info("✅ No duplicate groups found, skipping duplicate processing");
    return;
  }

  let processedCount = 0;
  const processedUserIds = new Set(); // Track which users we've already processed

  // Step 3: Process each connected group
  for (const group of connectedGroups) {
    // Skip groups where all users are already synced (they've been processed before)
    const pendingInGroup = group.filter((u) => u.zendesk_user_id == null);
    if (pendingInGroup.length === 0) {
      logger.debug(`⏭️  Skipping group (all users already synced): ${group.map((u) => u.ac_id).join(", ")}`);
      continue;
    }

    // Select primary user for this group
    const primaryUser = selectPrimaryUser(group);
    const duplicateUsers = group.filter((u) => u.ac_id !== primaryUser.ac_id);

    // Skip if this group was already processed (check if primary is already processed)
    if (processedUserIds.has(primaryUser.ac_id)) {
      logger.debug(`⏭️  Skipping group (already processed): primary=${primaryUser.ac_id}`);
      continue;
    }

    logger.info(
      `   Processing group: primary=${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id}), ${duplicateUsers.length} duplicate(s)`
    );

    // Step 4: Collect all emails and phones from the entire group
    const allGroupEmails = new Set();
    const allGroupPhones = new Set();

    for (const user of group) {
      extractAllEmails(user).forEach((email) => allGroupEmails.add(email));
      extractAllPhoneNumbers(user).forEach((phone) => allGroupPhones.add(phone));
    }

    const primaryEmails = extractAllEmails(primaryUser);
    const primaryPhones = extractAllPhoneNumbers(primaryUser);
    const normalizedPrimaryEmails = new Set(primaryEmails.map((e) => e.toLowerCase()));

    // Step 5: Collect all shared phone numbers from the entire group
    const sharedPhones = Array.from(allGroupPhones);
    const sharedPhoneNumberStr = sharedPhones.length > 0 ? sharedPhones.join("\n") : null;

    // Step 6: Process each duplicate user in the group
    for (const duplicateUser of duplicateUsers) {
      // Skip if already synced (don't modify synced users)
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

      // Filter identities: preserve all non-conflicting identities
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

      const phoneCount = extractAllPhoneNumbers(duplicateUser).length;
      updateStmt.run(newEmail, JSON.stringify(filteredIdentities), sharedPhoneNumberStr, duplicateUser.ac_id);
      logger.debug(
        `   Updated duplicate user ${duplicateUser.ac_id}: email=${newEmail || "null"}, moved ${phoneCount} phone(s) to shared_phone_number`
      );
      processedCount++;
      processedUserIds.add(duplicateUser.ac_id);
    }

    // Step 7: Ensure primary user has shared_phone_number set to NULL
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

  logger.info(`✅ Processed ${processedCount} duplicate users across ${connectedGroups.length} group(s)`);
}
