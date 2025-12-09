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
 * After aliasing all duplicates, removes alias pattern from zendesk_primary users.
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
  // After aliasing all duplicates, remove the alias pattern (+tag) from primary users' emails
  logger.info("🔑 Step 3: Removing alias pattern from zendesk_primary users...");
  const primaryUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1 AND zendesk_primary = 1")
    .all()
    .map(hydrateMapping);
  
  let restoredCount = 0;
  
  for (const primaryUser of primaryUsers) {
    let needsUpdate = false;
    let restoredEmail = primaryUser.email;
    let restoredIdentities = primaryUser.identities;

    // Remove alias pattern from email field (remove everything after +)
    if (primaryUser.email && primaryUser.email.includes('+') && primaryUser.email.includes('@')) {
      const emailParts = primaryUser.email.split('@');
      if (emailParts.length === 2) {
        const localPart = emailParts[0].split('+')[0]; // Remove everything after +
        restoredEmail = `${localPart}@${emailParts[1]}`;
        needsUpdate = true;
        logger.info(
          `   🔑 Removing alias from email for primary user ${primaryUser.ac_id}: ${primaryUser.email} → ${restoredEmail}`
        );
      }
    }

    // Remove alias pattern from email identities (remove everything after +)
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

      restoredIdentities = identities.map((identity) => {
        if (identity.type === "email" && identity.value && identity.value.includes('+') && identity.value.includes('@')) {
          // Remove alias pattern: remove everything after +
          const emailParts = identity.value.split('@');
          if (emailParts.length === 2) {
            const localPart = emailParts[0].split('+')[0]; // Remove everything after +
            const restoredValue = `${localPart}@${emailParts[1]}`;
            logger.debug(
              `   Removing alias from email identity for primary user ${primaryUser.ac_id}: ${identity.value} → ${restoredValue}`
            );
            return { ...identity, value: restoredValue };
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

      const identitiesJson = typeof restoredIdentities === "string" ? restoredIdentities : JSON.stringify(restoredIdentities);
      const result = updateStmt.run(restoredEmail, identitiesJson, primaryUser.ac_id);
      
      if (result.changes > 0) {
        restoredCount++;
        logger.info(`   ✅ Removed alias pattern from primary user ${primaryUser.ac_id}`);
      } else {
        logger.warn(`   ⚠️  Failed to update primary user ${primaryUser.ac_id} - no rows changed`);
      }
    }
  }

  if (restoredCount > 0) {
    logger.info(`✅ Removed alias pattern from ${restoredCount} zendesk_primary user(s)`);
  } else {
    logger.info("✅ No primary users needed alias removal");
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
 * Extract unaliased email from aliased email format (e.g., "user+tag@domain.com" -> "user@domain.com")
 */
function extractUnaliasedEmail(aliasedEmail) {
  if (!aliasedEmail || !aliasedEmail.includes('+') || !aliasedEmail.includes('@')) {
    return aliasedEmail; // Not aliased
  }
  const [localPart, domain] = aliasedEmail.split('@');
  const originalLocal = localPart.split('+')[0];
  return `${originalLocal}@${domain}`;
}

/**
 * Check if an email is aliased (contains + before @)
 */
function isEmailAliased(email) {
  if (!email) return false;
  return email.includes('+') && email.includes('@');
}

/**
 * Find all active users who share the same email (checking both email field and identities)
 * This checks if any active user has the target email (either directly or as an aliased email)
 */
function findUsersSharingEmail(targetEmail, excludeAcId) {
  const db = getDb();
  const normalizedTargetEmail = targetEmail.toLowerCase();
  
  // Get all active users
  const allActiveUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  
  return allActiveUsers.filter(user => {
    if (user.ac_id === excludeAcId) return false;
    
    // Get all emails for this user (from email field and identities)
    const userEmails = extractAllEmails(user);
    
    // Check if any of the user's emails match the target (either directly or unaliased)
    return userEmails.some(userEmail => {
      const emailLower = userEmail.toLowerCase();
      
      // Direct match
      if (emailLower === normalizedTargetEmail) {
        return true;
      }
      
      // Check if this user's email is aliased and matches when unaliased
      if (isEmailAliased(emailLower)) {
        const unaliased = extractUnaliasedEmail(emailLower).toLowerCase();
        if (unaliased === normalizedTargetEmail) {
          return true;
        }
      }
      
      // Check if target email is aliased and matches when unaliased
      // (This handles the case where target is aliased but user has original)
      if (isEmailAliased(normalizedTargetEmail)) {
        const unaliasedTarget = extractUnaliasedEmail(normalizedTargetEmail).toLowerCase();
        if (emailLower === unaliasedTarget) {
          return true;
        }
      }
      
      return false;
    });
  });
}

/**
 * Alias a user's email (both email field and email identities)
 */
function aliasUserEmail(user) {
  const db = getDb();
  let newEmail = user.email;
  let emailWasAliased = false;
  
  if (newEmail && !isEmailAliased(newEmail)) {
    const emailParts = newEmail.split("@");
    if (emailParts.length === 2) {
      newEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
      emailWasAliased = true;
    }
  }
  
  // Process identities
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
    if (identity.type === "email" && identity.value && !isEmailAliased(identity.value)) {
      const emailParts = identity.value.split("@");
      if (emailParts.length === 2) {
        return { ...identity, value: `${emailParts[0]}+${user.external_id}@${emailParts[1]}` };
      }
    }
    return identity;
  });
  
  // Update database
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET email = ?,
        identities = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
  `);
  
  updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.ac_id);
  
  return { newEmail, processedIdentities, emailWasAliased };
}

/**
 * Un-alias a user's email (both email field and email identities)
 */
function unaliasUserEmail(user) {
  const db = getDb();
  let newEmail = user.email;
  let emailWasUnaliased = false;
  
  if (newEmail && isEmailAliased(newEmail)) {
    newEmail = extractUnaliasedEmail(newEmail);
    emailWasUnaliased = true;
  }
  
  // Process identities
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
    if (identity.type === "email" && identity.value && isEmailAliased(identity.value)) {
      return { ...identity, value: extractUnaliasedEmail(identity.value) };
    }
    return identity;
  });
  
  // Update database
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET email = ?,
        identities = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
  `);
  
  updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.ac_id);
  
  return { newEmail, processedIdentities, emailWasUnaliased };
}

/**
 * Move user's phone numbers to shared_phone_number field
 */
function movePhoneToShared(user) {
  const db = getDb();
  const userPhones = extractAllPhoneNumbers(user);
  const sharedPhoneNumberStr = userPhones.length > 0 ? userPhones.join("\n") : null;
  
  // Filter identities: remove phone identities
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
  
  const filteredIdentities = identities.filter((identity) => {
    return !(identity.type === "phone" || identity.type === "phone_number");
  });
  
  // Update database
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET phone = NULL,
        identities = ?,
        shared_phone_number = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
  `);
  
  updateStmt.run(JSON.stringify(filteredIdentities), sharedPhoneNumberStr, user.ac_id);
  
  return { sharedPhoneNumberStr, filteredIdentities };
}

/**
 * Move phone numbers from shared_phone_number back to phone field and identities
 */
function movePhoneFromShared(user) {
  const db = getDb();
  
  if (!user.shared_phone_number) {
    return; // No phone to move
  }
  
  const phones = user.shared_phone_number.split("\n").filter(p => p.trim());
  const primaryPhone = phones[0] || null;
  
  // Add remaining phones to identities
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
  
  // Add phones (except first one) as phone identities
  const phoneIdentities = phones.slice(1).map(phone => ({
    type: "phone_number",
    value: phone.trim()
  }));
  
  const updatedIdentities = [...identities, ...phoneIdentities];
  
  // Update database
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET phone = ?,
        identities = ?,
        shared_phone_number = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE ac_id = ?
  `);
  
  updateStmt.run(primaryPhone, JSON.stringify(updatedIdentities), user.ac_id);
  
  return { primaryPhone, updatedIdentities };
}

/**
 * Save original user data before duplicate processing (for reference)
 */
export function saveOriginalUserData() {
  const db = getDb();
  const allActiveUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  
  const originalData = new Map();
  for (const user of allActiveUsers) {
    originalData.set(user.ac_id, {
      email: user.email,
      phone: user.phone,
      identities: user.identities,
      shared_phone_number: user.shared_phone_number,
    });
  }
  
  logger.debug(`💾 Saved original data for ${originalData.size} active users`);
  return originalData;
}

/**
 * Find email groups with 2+ users and no zendesk_primary tag
 * These users should not be sent to Zendesk and need notification
 */
export function findEmailGroupsWithoutPrimary() {
  const db = getDb();
  
  // Get ALL users (active + non-active) to check for conflicts
  const allUsers = db
    .prepare("SELECT * FROM user_mappings")
    .all()
    .map(hydrateMapping);
  
  // Build email index: email -> [users who have this email]
  const emailIndex = new Map();
  
  for (const user of allUsers) {
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      const normalizedEmail = email.toLowerCase();
      // Extract unaliased email for grouping
      const unaliasedEmail = isEmailAliased(normalizedEmail) 
        ? extractUnaliasedEmail(normalizedEmail).toLowerCase()
        : normalizedEmail;
      
      if (!emailIndex.has(unaliasedEmail)) {
        emailIndex.set(unaliasedEmail, []);
      }
      emailIndex.get(unaliasedEmail).push(user);
    }
  }
  
  // Find groups with 2+ users and no zendesk_primary
  const problematicGroups = [];
  
  for (const [email, users] of emailIndex.entries()) {
    // Only consider groups with 2+ users
    if (users.length < 2) continue;
    
    // Check if any user has zendesk_primary tag
    const hasPrimary = users.some(
      (u) => u.zendesk_primary === 1 || u.zendesk_primary === true
    );
    
    // If no primary tag, this is a problematic group
    if (!hasPrimary) {
      problematicGroups.push({
        email: email,
        users: users.map(u => ({
          ac_id: u.ac_id,
          name: u.name || u.external_id || "unknown",
          email: u.email,
          external_id: u.external_id,
          user_type: u.user_type,
          zendesk_user_id: u.zendesk_user_id,
        })),
      });
    }
  }
  
  return problematicGroups;
}

/**
 * Process non-active users: swap emails/phones with active users who share the same email
 */
export function processNonActiveUserEmailSwaps(usersWithStatusChange) {
  const db = getDb();
  const usersToUpdate = [];
  
  logger.info("🔄 Processing email/phone swaps for non-active users...");
  
  for (const nonActiveUser of usersWithStatusChange) {
    // Skip if user doesn't have zendesk_user_id (not synced yet)
    if (!nonActiveUser.zendesk_user_id) {
      continue;
    }
    
    // Skip if email is already aliased
    if (isEmailAliased(nonActiveUser.email)) {
      logger.debug(`⏭️  Skipping non-active user ${nonActiveUser.ac_id}: email already aliased`);
      continue;
    }
    
    // Find active users who share the same email
    const activeUsersSharingEmail = findUsersSharingEmail(
      nonActiveUser.email,
      nonActiveUser.ac_id
    );
    
    if (activeUsersSharingEmail.length === 0) {
      continue; // No conflict, skip
    }
    
    logger.info(
      `🔄 Processing non-active user ${nonActiveUser.ac_id} (${nonActiveUser.name || nonActiveUser.external_id}): ` +
      `found ${activeUsersSharingEmail.length} active user(s) sharing email ${nonActiveUser.email}`
    );
    
    // Alias non-active user's email and move phone to shared_phone_number
    aliasUserEmail(nonActiveUser);
    movePhoneToShared(nonActiveUser);
    
    // Reload non-active user from database to get updated values
    const updatedNonActiveUser = db
      .prepare("SELECT * FROM user_mappings WHERE ac_id = ?")
      .get(nonActiveUser.ac_id);
    const hydratedNonActiveUser = hydrateMapping(updatedNonActiveUser);
    usersToUpdate.push(hydratedNonActiveUser);
    
    logger.info(
      `   ✅ Aliased non-active user ${nonActiveUser.ac_id}: email → ${hydratedNonActiveUser.email}, phone moved to shared_phone_number`
    );
    
    // Handle active user(s)
    if (activeUsersSharingEmail.length === 1) {
      // Single active user: un-alias email and move phone back
      const activeUser = activeUsersSharingEmail[0];
      unaliasUserEmail(activeUser);
      movePhoneFromShared(activeUser);
      
      // Reload active user from database
      const updatedActiveUser = db
        .prepare("SELECT * FROM user_mappings WHERE ac_id = ?")
        .get(activeUser.ac_id);
      const hydratedActiveUser = hydrateMapping(updatedActiveUser);
      usersToUpdate.push(hydratedActiveUser);
      
      logger.info(
        `   ✅ Un-aliased active user ${activeUser.ac_id}: email → ${hydratedActiveUser.email}, phone restored`
      );
    } else {
      // Multiple active users: select one as primary, un-alias that one
      const primaryTagged = activeUsersSharingEmail.find(
        (u) => u.zendesk_primary === 1 || u.zendesk_primary === true
      );
      let primaryUser;
      
      if (primaryTagged) {
        primaryUser = primaryTagged;
      } else {
        // Prefer already synced users
        const synced = activeUsersSharingEmail.filter((u) => u.zendesk_user_id != null);
        if (synced.length > 0) {
          primaryUser = synced[0];
        } else {
          primaryUser = activeUsersSharingEmail[0];
        }
        logger.warn(
          `   ⚠️  No zendesk_primary user found in group. Selected ${primaryUser.ac_id} as primary.`
        );
      }
      
      // Un-alias primary user and move phone back
      unaliasUserEmail(primaryUser);
      movePhoneFromShared(primaryUser);
      
      // Reload primary user from database
      const updatedPrimaryUser = db
        .prepare("SELECT * FROM user_mappings WHERE ac_id = ?")
        .get(primaryUser.ac_id);
      const hydratedPrimaryUser = hydrateMapping(updatedPrimaryUser);
      usersToUpdate.push(hydratedPrimaryUser);
      
      logger.info(
        `   ✅ Un-aliased primary active user ${primaryUser.ac_id}: email → ${hydratedPrimaryUser.email}, phone restored`
      );
      
      // Note: Other active users remain aliased (will be handled in next duplicate processing cycle)
    }
  }
  
  if (usersToUpdate.length > 0) {
    logger.info(`📋 Found ${usersToUpdate.length} user(s) that need Zendesk updates`);
  } else {
    logger.info("✅ No email/phone swaps needed for non-active users");
  }
  
  return usersToUpdate;
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
