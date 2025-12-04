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
 * Extract original email from an aliased email (removes +external_id pattern)
 * @param {string} aliasedEmail - Email that may be aliased (e.g., "user+123@example.com")
 * @param {string} externalId - The external_id that was used for aliasing
 * @returns {string|null} - Original email or null if not aliased
 */
function extractOriginalEmail(aliasedEmail, externalId) {
  if (!aliasedEmail || !externalId) return null;
  
  // Check if email contains the aliasing pattern: localpart+external_id@domain
  const pattern = new RegExp(`^(.+)\\+${externalId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}@(.+)$`, 'i');
  const match = aliasedEmail.match(pattern);
  
  if (match) {
    return `${match[1]}@${match[2]}`;
  }
  
  return null;
}

/**
 * Process email duplicates: group users by shared emails, alias duplicate emails.
 * This is the FIRST step and only handles emails, not phones.
 * After aliasing all duplicates, restores original emails for zendesk_primary users.
 */
function processEmailDuplicates() {
  const db = getDb();
  logger.info("📧 Step 1: Processing duplicate emails...");

  // Get all ACTIVE users (current_active = 1)
  // Non-active users don't need processing because their data won't be overwritten (we only fetch active users)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${allUsers.length} active users in database (processing all, regardless of sync status)`);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping email duplicate processing");
    return;
  }

  // Store original emails and identities before processing (for primary user restoration)
  const originalEmailData = new Map();
  for (const user of allUsers) {
    originalEmailData.set(user.ac_id, {
      originalEmail: user.email,
      originalIdentities: user.identities
    });
  }

  // Build global email index: email -> [users who have this email]
  // This includes ALL users (synced and pending) to catch all duplicates
  const globalEmailIndex = new Map();
  for (const user of allUsers) {
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      const normalizedEmail = email.toLowerCase();
      if (!globalEmailIndex.has(normalizedEmail)) {
        globalEmailIndex.set(normalizedEmail, []);
      }
      globalEmailIndex.get(normalizedEmail).push(user);
    }
  }

  // Find email groups (users connected by shared emails)
  const emailGroups = findEmailGroups(allUsers);

  if (emailGroups.length === 0) {
    logger.info("✅ No email duplicate groups found");
    return;
  }

  let processedCount = 0;
  const processedUserIds = new Set();

  // Process each email group
  for (const group of emailGroups) {
    // Skip if already processed
    if (group.some((u) => processedUserIds.has(u.ac_id))) {
      logger.debug(`⏭️  Skipping email group (already processed): ${group.map((u) => u.ac_id).join(", ")}`);
      continue;
    }

    logger.info(
      `   Processing email group with ${group.length} user(s): ${group.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ")}`
    );

    // Process ALL users in the group (including primary) - alias all emails to avoid conflicts
    // This ensures all emails are unique and there's no confusion if primary user is deleted
    for (const user of group) {

      let newEmail = user.email;
      const originalEmail = user.email ? user.email.toLowerCase() : null;
      let emailWasAliased = false;

      // Alias email if it matches ANY other user's email (including other users in the same group)
      // This ensures ALL emails are unique, even for primary users
      if (newEmail) {
        const normalizedEmail = newEmail.toLowerCase();
        const usersWithThisEmail = globalEmailIndex.get(normalizedEmail) || [];
        const otherUsersWithEmail = usersWithThisEmail.filter(
          (u) => u.ac_id !== user.ac_id
        );
        
        // If another user has this email, ALWAYS alias it (even for primary user)
        if (otherUsersWithEmail.length > 0) {
          const emailParts = newEmail.split("@");
          if (emailParts.length === 2) {
            newEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
            emailWasAliased = true;
            logger.debug(
              `   Aliasing email for user ${user.ac_id}: ${user.email} → ${newEmail} (found in ${otherUsersWithEmail.length} other user(s))`
            );
          }
        }
      }

      // Process identities: alias all email identities that match other users' emails
      let identities = user.identities;
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

      // Process email identities: alias ALL email identities that match other users' emails
      const processedIdentities = identities.map((identity) => {
        // Keep phone identities (they'll be processed separately)
        if (identity.type === "phone" || identity.type === "phone_number") {
          return identity;
        }
        
        // Handle email identities
        if (identity.type === "email") {
          const identityEmail = identity.value?.toLowerCase();
          
          // If we aliased the email field and this is the same email, remove it from identities
          // (to avoid having both the aliased email in email field and original in identities)
          if (emailWasAliased && originalEmail && identityEmail === originalEmail) {
            return null; // Remove this identity
          }
          
          // Check if this email already exists for another user
          // If ANY other user has this email, we must alias it to avoid Zendesk duplicates
          const usersWithThisEmail = globalEmailIndex.get(identityEmail) || [];
          const otherUsersWithEmail = usersWithThisEmail.filter(
            (u) => u.ac_id !== user.ac_id
          );
          
          // If another user has this email, ALWAYS alias it (regardless of sync status)
          if (otherUsersWithEmail.length > 0) {
            // Alias this email identity
            const emailParts = identity.value.split("@");
            if (emailParts.length === 2) {
              const aliasedEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
              logger.debug(
                `   Aliasing email identity ${identity.value} → ${aliasedEmail} for user ${user.ac_id} (found in ${otherUsersWithEmail.length} other user(s))`
              );
              return { ...identity, value: aliasedEmail };
            }
          }
        }
        
        return identity;
      }).filter((identity) => identity !== null); // Remove null entries

      // Update user: alias email and identities if needed
      // Only update if something changed
      if (emailWasAliased || JSON.stringify(processedIdentities) !== JSON.stringify(identities)) {
        const updateStmt = db.prepare(`
          UPDATE user_mappings
          SET email = ?,
              identities = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE ac_id = ?
        `);

        updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.ac_id);
        const emailChanges = emailWasAliased ? `email aliased: ${user.email} → ${newEmail}` : "";
        const identityChanges = processedIdentities.length !== identities.length 
          ? `, ${identities.length - processedIdentities.length} email identity(ies) aliased/removed`
          : "";
        logger.debug(`   Updated user ${user.ac_id}: ${emailChanges}${identityChanges}`);
        processedCount++;
      }
      processedUserIds.add(user.ac_id);
    }
  }

  logger.info(`✅ Processed ${processedCount} duplicate users in ${emailGroups.length} email group(s)`);

  // Second pass: Process ALL active users (even those not in groups) to catch any remaining duplicates
  // This ensures we catch edge cases where emails might be missed
  logger.info("🔍 Second pass: Checking all active users for duplicate emails...");
  // allUsers is already filtered by current_active = 1, so use it directly
  let secondPassCount = 0;

  for (const user of allUsers) {
    // Skip if already processed in first pass
    if (processedUserIds.has(user.ac_id)) {
      continue;
    }

    let newEmail = user.email;
    const originalEmail = user.email ? user.email.toLowerCase() : null;
    let emailWasAliased = false;

    // Check email field against global index
    if (newEmail) {
      const normalizedEmail = newEmail.toLowerCase();
      const usersWithThisEmail = globalEmailIndex.get(normalizedEmail) || [];
      const otherUsersWithEmail = usersWithThisEmail.filter((u) => u.ac_id !== user.ac_id);
      
      if (otherUsersWithEmail.length > 0) {
        const emailParts = newEmail.split("@");
        if (emailParts.length === 2) {
          newEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
          emailWasAliased = true;
          logger.debug(
            `   [Second pass] Aliasing email field ${user.email} → ${newEmail} for user ${user.ac_id}`
          );
        }
      }
    }

    // Check all email identities against global index
    let identities = user.identities;
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

    const processedIdentities = identities.map((identity) => {
      if (identity.type === "phone" || identity.type === "phone_number") {
        return identity;
      }
      
      if (identity.type === "email") {
        const identityEmail = identity.value?.toLowerCase();
        
        // Remove if we aliased the email field and this is the same email
        if (emailWasAliased && originalEmail && identityEmail === originalEmail) {
          return null;
        }
        
        // Check if this email exists for another user
        const usersWithThisEmail = globalEmailIndex.get(identityEmail) || [];
        const otherUsersWithEmail = usersWithThisEmail.filter((u) => u.ac_id !== user.ac_id);
        
        if (otherUsersWithEmail.length > 0) {
          const emailParts = identity.value.split("@");
          if (emailParts.length === 2) {
            const aliasedEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
            logger.debug(
              `   [Second pass] Aliasing email identity ${identity.value} → ${aliasedEmail} for user ${user.ac_id}`
            );
            return { ...identity, value: aliasedEmail };
          }
        }
      }
      
      return identity;
    }).filter((identity) => identity !== null);

    // Only update if something changed
    if (emailWasAliased || JSON.stringify(processedIdentities) !== JSON.stringify(identities)) {
      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET email = ?,
            identities = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.ac_id);
      secondPassCount++;
    }
  }

  if (secondPassCount > 0) {
    logger.info(`✅ Second pass: Processed ${secondPassCount} additional users`);
  }

  // Step 3: Restore original emails for zendesk_primary users
  // After aliasing all duplicates, restore primary users' original emails to ensure they keep their real emails
  logger.info("🔑 Step 3: Restoring original emails for zendesk_primary users...");
  const primaryUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1 AND zendesk_primary = 1")
    .all()
    .map(hydrateMapping);
  
  let restoredCount = 0;
  
  for (const primaryUser of primaryUsers) {
    const originalData = originalEmailData.get(primaryUser.ac_id);
    if (!originalData) {
      continue; // Skip if we don't have original data
    }

    let needsUpdate = false;
    let restoredEmail = primaryUser.email;
    let restoredIdentities = primaryUser.identities;

    // Restore email field if it was aliased
    if (primaryUser.email && originalData.originalEmail) {
      const extractedOriginal = extractOriginalEmail(primaryUser.email, primaryUser.external_id);
      if (extractedOriginal) {
        // Email was aliased, restore to original
        restoredEmail = originalData.originalEmail;
        needsUpdate = true;
        logger.debug(
          `   Restoring email for primary user ${primaryUser.ac_id}: ${primaryUser.email} → ${restoredEmail}`
        );
      }
    }

    // Restore email identities if they were aliased
    if (primaryUser.identities) {
      let identities = primaryUser.identities;
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

      // Get original identities
      let originalIdentities = originalData.originalIdentities;
      if (typeof originalIdentities === "string") {
        try {
          originalIdentities = JSON.parse(originalIdentities);
        } catch {
          originalIdentities = [];
        }
      }
      if (!Array.isArray(originalIdentities)) {
        originalIdentities = [];
      }

      // Build a map of original email identities for quick lookup
      const originalEmailIdentitiesMap = new Map();
      originalIdentities.forEach((identity) => {
        if (identity.type === "email" && identity.value) {
          originalEmailIdentitiesMap.set(identity.value.toLowerCase(), identity.value);
        }
      });

      // Restore aliased email identities to their original values
      restoredIdentities = identities.map((identity) => {
        if (identity.type === "email" && identity.value) {
          const extractedOriginal = extractOriginalEmail(identity.value, primaryUser.external_id);
          if (extractedOriginal) {
            // This identity was aliased, restore to original
            const originalValue = originalEmailIdentitiesMap.get(extractedOriginal.toLowerCase());
            if (originalValue) {
              logger.debug(
                `   Restoring email identity for primary user ${primaryUser.ac_id}: ${identity.value} → ${originalValue}`
              );
              return { ...identity, value: originalValue };
            }
          }
        }
        return identity;
      });

      // Check if identities changed
      if (JSON.stringify(restoredIdentities) !== JSON.stringify(identities)) {
        needsUpdate = true;
      }
    }

    // Update primary user if needed
    if (needsUpdate) {
      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET email = ?,
            identities = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      updateStmt.run(
        restoredEmail,
        typeof restoredIdentities === "string" ? restoredIdentities : JSON.stringify(restoredIdentities),
        primaryUser.ac_id
      );
      restoredCount++;
      logger.debug(`   ✅ Restored original emails for primary user ${primaryUser.ac_id}`);
    }
  }

  if (restoredCount > 0) {
    logger.info(`✅ Restored original emails for ${restoredCount} zendesk_primary user(s)`);
  } else {
    logger.info("✅ No primary users needed email restoration");
  }
}

/**
 * Process phone duplicates: find users with same phone numbers and move to shared_phone_number.
 * This is the SECOND step and only handles phones, not emails.
 */
function processPhoneDuplicates() {
  const db = getDb();
  logger.info("📞 Step 2: Processing duplicate phone numbers...");

  // Get all ACTIVE users (current_active = 1)
  // Non-active users don't need processing because their data won't be overwritten (we only fetch active users)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${allUsers.length} active users in database (processing all, regardless of sync status)`);

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

  if (phoneGroups.length === 0) {
    logger.info("✅ No phone duplicate groups found");
    return;
  }

  let processedCount = 0;
  const processedUserIds = new Set();

  // Process each phone group
  for (const [phone, phoneGroup] of phoneGroups) {
    // Process all active users in the group (regardless of sync status)

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

    // Process each duplicate user in the phone group (including primary)
    for (const duplicateUser of duplicateUsers) {

      // Get THIS USER'S phone numbers (not all phones from the group)
      const duplicateUserPhones = extractAllPhoneNumbers(duplicateUser);
      const sharedPhoneNumberStr = duplicateUserPhones.length > 0 ? duplicateUserPhones.join("\n") : null;

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

      // Update duplicate user: move THIS USER'S phones to shared_phone_number, remove phone from identities
      const updateStmt = db.prepare(`
        UPDATE user_mappings
        SET phone = NULL,
            identities = ?,
            shared_phone_number = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE ac_id = ?
      `);

      const phoneCount = duplicateUserPhones.length;
      updateStmt.run(JSON.stringify(filteredIdentities), sharedPhoneNumberStr, duplicateUser.ac_id);
      logger.debug(
        `   Updated duplicate user ${duplicateUser.ac_id}: moved ${phoneCount} phone(s) to shared_phone_number: ${duplicateUserPhones.join(", ")}`
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
