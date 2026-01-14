import { logger } from "../config/logger.js";
import { hydrateMapping } from "../domain/user.db.mapper.js";
import { getDb } from "./db.api.js";
import { storeEdgeCaseEmailError, storeEdgeCasePhoneError } from "./edgeCaseErrors.js";

export function extractAllEmails(user) {
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

export function extractAllPhoneNumbers(user) {
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
 * Check if an email is aliased using our pattern (+client_ or +caregiver_)
 * This distinguishes our aliases from natural + signs in emails
 */
export function isAliasedEmail(email) {
  if (!email || !email.includes('+') || !email.includes('@')) {
    return false;
  }
  
  const [localPart] = email.split('@');
  // Check if it matches our pattern: +client_ or +caregiver_ followed by external_id
  const aliasPattern = /\+client_|\+caregiver_/;
  return aliasPattern.test(localPart);
}

/**
 * Extract unaliased email from aliased format
 * Only works for our aliasing pattern (+client_ or +caregiver_)
 */
export function extractUnaliasedEmail(aliasedEmail) {
  if (!isAliasedEmail(aliasedEmail)) {
    return aliasedEmail; // Not aliased or not our pattern
  }
  
  const [localPart, domain] = aliasedEmail.split('@');
  // Remove everything after +client_ or +caregiver_
  const unaliasedLocal = localPart.replace(/\+client_.*$|\+caregiver_.*$/, '');
  return `${unaliasedLocal}@${domain}`;
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
 * Simplified version: Only alias non-primary users, track users needing primary tag.
 * 
 * @returns {Array} Array of users who need zendesk_primary tag (groups with no primary)
 */
function processEmailDuplicates() {
  const db = getDb();
  logger.info("📧 Phase 2: Processing duplicate emails for active users...");

  // Get all ACTIVE users (current_active = 1)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  logger.info(`📊 Found ${allUsers.length} active users in database`);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping email duplicate processing");
    return [];
  }

  // Build email index: email -> [users who have this email]
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
    return [];
  }

  const usersNeedingPrimary = [];
  let processedCount = 0;

  // Process each email group
  for (const group of emailGroups) {
    logger.info(
      `   Processing email group with ${group.length} user(s): ${group.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ")}`
    );

    // Find zendesk_primary user in this group
    const primaryUser = group.find((u) => u.zendesk_primary === 1 || u.zendesk_primary === true);

    if (!primaryUser) {
      // No primary user found - log error and save to constant
      logger.error(
        `❌ ERROR: No zendesk_primary user found in email group. Users: ${group.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ")}`
      );
      // Add all users in this group to the list
      usersNeedingPrimary.push(...group);
      continue;
    }

    logger.info(`   ✅ Found zendesk_primary user: ${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id})`);

    // Alias all non-primary users' emails
    for (const user of group) {
      if (user.ac_id === primaryUser.ac_id) {
        continue; // Skip primary user
      }

      let newEmail = user.email;
      let emailWasAliased = false;

      // Alias email field if it matches primary user's email
      if (newEmail) {
        const normalizedEmail = newEmail.toLowerCase();
        const primaryEmails = extractAllEmails(primaryUser).map(e => e.toLowerCase());
        
        if (primaryEmails.includes(normalizedEmail)) {
          const emailParts = newEmail.split("@");
          if (emailParts.length === 2) {
            // external_id already contains the prefix (e.g., "client_4767" or "caregiver_123")
            newEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
            emailWasAliased = true;
            logger.info(
              `   🔄 Aliasing email for user ${user.ac_id}: ${user.email} → ${newEmail}`
            );
          }
        }
      }

      // Process identities: alias email identities that match primary user's emails
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

      const primaryEmails = extractAllEmails(primaryUser).map(e => e.toLowerCase());
      const processedIdentities = identities.map((identity) => {
        if (identity.type === "phone" || identity.type === "phone_number") {
          return identity; // Keep phone identities
        }
        
        if (identity.type === "email" && identity.value) {
          const identityEmail = identity.value.toLowerCase();
          
          // If we aliased the email field and this is the same email, remove it from identities
          if (emailWasAliased && user.email && identityEmail === user.email.toLowerCase()) {
            return null; // Remove this identity
          }
          
          // Check if this email matches primary user's email
          if (primaryEmails.includes(identityEmail)) {
            const emailParts = identity.value.split("@");
            if (emailParts.length === 2) {
              // external_id already contains the prefix (e.g., "client_4767" or "caregiver_123")
              const aliasedEmail = `${emailParts[0]}+${user.external_id}@${emailParts[1]}`;
              logger.info(
                `   🔄 Aliasing email identity ${identity.value} → ${aliasedEmail} for user ${user.ac_id}`
              );
              return { ...identity, value: aliasedEmail };
            }
          }
        }
        
        return identity;
      }).filter((identity) => identity !== null);

      // Update user if email or identities changed
      if (emailWasAliased || JSON.stringify(processedIdentities) !== JSON.stringify(identities)) {
        const updateStmt = db.prepare(`
          UPDATE user_mappings
          SET email = ?,
              identities = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE ac_id = ?
        `);

        updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.ac_id);
        processedCount++;
      }
    }

    // EDGE CASE DETECTION: Check if non-primary users share emails that don't match primary user
    // After aliasing emails that match primary user, check if non-primary users still share other emails
    const nonPrimaryUsers = group.filter((u) => u.ac_id !== primaryUser.ac_id);
    if (nonPrimaryUsers.length > 1) {
      const primaryEmails = extractAllEmails(primaryUser).map(e => e.toLowerCase());
      
      // Build email index for non-primary users only (excluding primary user's emails)
      const nonPrimaryEmailIndex = new Map();
      for (const user of nonPrimaryUsers) {
        const userEmails = extractAllEmails(user);
        for (const email of userEmails) {
          const normalizedEmail = email.toLowerCase();
          // Only consider emails that don't match primary user's emails
          if (!primaryEmails.includes(normalizedEmail)) {
            if (!nonPrimaryEmailIndex.has(normalizedEmail)) {
              nonPrimaryEmailIndex.set(normalizedEmail, []);
            }
            nonPrimaryEmailIndex.get(normalizedEmail).push(user);
          }
        }
      }

      // Find emails shared by 2+ non-primary users (edge case)
      for (const [email, sharingUsers] of nonPrimaryEmailIndex.entries()) {
        if (sharingUsers.length > 1) {
          logger.error(
            `❌ EDGE CASE DETECTED: Non-primary users share email "${email}" that doesn't match primary user. ` +
            `Users: ${sharingUsers.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ")}. ` +
            `Primary user: ${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id})`
          );
          
          // Store edge case error (deduplication handled internally)
          const errorStored = storeEdgeCaseEmailError({
            email: email,
            users: sharingUsers.map(u => ({
              ac_id: u.ac_id,
              name: u.name || u.external_id || "unknown",
              email: u.email,
              external_id: u.external_id,
              user_type: u.user_type,
              zendesk_user_id: u.zendesk_user_id,
            })),
            primaryUser: {
              ac_id: primaryUser.ac_id,
              name: primaryUser.name || primaryUser.external_id || "unknown",
              email: primaryUser.email,
              external_id: primaryUser.external_id,
              user_type: primaryUser.user_type,
              zendesk_user_id: primaryUser.zendesk_user_id,
            },
          });
          
          if (!errorStored) {
            logger.debug(`   ⏭️  Edge case email error already stored (duplicate detected): ${email}`);
          }
        }
      }
    }
  }

  logger.info(`✅ Processed ${processedCount} duplicate users in ${emailGroups.length} email group(s)`);
  
  if (usersNeedingPrimary.length > 0) {
    logger.error(`❌ Found ${usersNeedingPrimary.length} user(s) in groups without zendesk_primary tag`);
  }

  return usersNeedingPrimary;
}

/**
 * Process phone duplicates for ACTIVE users.
 *
 * Rules:
 * - Primary user (zendesk_primary = 1) keeps phone numbers in `phone` + phone identities,
 *   and must have `shared_phone_number = NULL`.
 * - Non-primary users move all their phone numbers into `shared_phone_number`
 *   and remove them from `phone` + phone identities (using movePhoneToShared).
 *
 * If a phone group has no zendesk_primary user, we log an error and skip that group
 * (so we don't make an arbitrary choice). These groups can be surfaced via logs / email.
 */
function processPhoneDuplicates() {
  const db = getDb();
  logger.info("📞 Processing duplicate phone numbers for active users...");

  // Get all ACTIVE users (current_active = 1)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);

  logger.info(`📊 Found ${allUsers.length} active users in database (for phone duplicate processing)`);

  if (allUsers.length === 0) {
    logger.info("✅ No users found, skipping phone duplicate processing");
    return [];
  }

  // Build phone index: phone -> [users who have this phone]
  const phoneIndex = new Map();

  for (const user of allUsers) {
    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phone) continue;
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user);
    }
  }

  // Groups that share the same phone (size >= 2)
  const phoneGroups = Array.from(phoneIndex.entries()).filter(([, users]) => users.length > 1);

  if (phoneGroups.length === 0) {
    logger.info("✅ No phone duplicate groups found");
    return [];
  }

  let processedCount = 0;
  const processedUserIds = new Set();
  const problematicPhoneGroups = [];

  for (const [phone, phoneGroup] of phoneGroups) {
    // Select primary user for this phone group (prefer zendesk_primary)
    const primaryUser = phoneGroup.find(
      (u) => u.zendesk_primary === 1 || u.zendesk_primary === true
    );

    if (!primaryUser) {
      const userList = phoneGroup
        .map((u) => `${u.ac_id} (${u.name || u.external_id})`)
        .join(", ");
      logger.error(
        `❌ ERROR: No zendesk_primary user found in phone group (phone=${phone}). Users: ${userList}`
      );
      problematicPhoneGroups.push({ phone, users: phoneGroup });
      continue;
    }

    const duplicateUsers = phoneGroup.filter((u) => u.ac_id !== primaryUser.ac_id);

    // Skip if already processed this primary (to avoid double work)
    if (processedUserIds.has(primaryUser.ac_id)) {
      logger.debug(
        `⏭️  Skipping phone group (already processed primary): primary=${primaryUser.ac_id}`
      );
      continue;
    }

    logger.info(
      `   Processing phone group: phone=${phone}, primary=${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id}), ${duplicateUsers.length} duplicate(s)`
    );

    // Ensure primary user has phones in phone/identities and no shared_phone_number
    try {
      movePhoneFromShared(primaryUser);
    } catch (error) {
      logger.warn(
        `⚠️  Failed to normalize phones for primary user ${primaryUser.ac_id}: ${error.message}`
      );
    }

    // For each non-primary user, move phones to shared_phone_number
    for (const duplicateUser of duplicateUsers) {
      try {
        const { sharedPhoneNumberStr } = movePhoneToShared(duplicateUser);
        const phoneCount = sharedPhoneNumberStr
          ? sharedPhoneNumberStr.split("\n").filter((p) => p.trim()).length
          : 0;
        logger.debug(
          `   Updated non-primary user ${duplicateUser.ac_id}: moved ${phoneCount} phone(s) to shared_phone_number`
        );
        processedCount++;
        processedUserIds.add(duplicateUser.ac_id);
      } catch (error) {
        logger.error(
          `❌ Failed to move phones to shared_phone_number for user ${duplicateUser.ac_id}: ${error.message}`
        );
      }
    }

    processedUserIds.add(primaryUser.ac_id);

    // EDGE CASE DETECTION: Check if non-primary users share phones that don't match primary user
    // After moving phones to shared_phone_number for primary user's phones, check if non-primary users still share other phones
    const nonPrimaryUsersForEdgeCase = phoneGroup.filter((u) => u.ac_id !== primaryUser.ac_id);
    if (nonPrimaryUsersForEdgeCase.length > 1) {
      const primaryPhones = extractAllPhoneNumbers(primaryUser);
      
      // Build phone index for non-primary users only (excluding primary user's phones)
      const nonPrimaryPhoneIndex = new Map();
      for (const user of nonPrimaryUsersForEdgeCase) {
        const userPhones = extractAllPhoneNumbers(user);
        for (const phone of userPhones) {
          if (!phone) continue;
          // Only consider phones that don't match primary user's phones
          if (!primaryPhones.includes(phone)) {
            if (!nonPrimaryPhoneIndex.has(phone)) {
              nonPrimaryPhoneIndex.set(phone, []);
            }
            nonPrimaryPhoneIndex.get(phone).push(user);
          }
        }
      }

      // Find phones shared by 2+ non-primary users (edge case)
      for (const [phone, sharingUsers] of nonPrimaryPhoneIndex.entries()) {
        if (sharingUsers.length > 1) {
          logger.error(
            `❌ EDGE CASE DETECTED: Non-primary users share phone "${phone}" that doesn't match primary user. ` +
            `Users: ${sharingUsers.map((u) => `${u.ac_id} (${u.name || u.external_id})`).join(", ")}. ` +
            `Primary user: ${primaryUser.ac_id} (${primaryUser.name || primaryUser.external_id})`
          );
          
          // Store edge case error (deduplication handled internally)
          const errorStored = storeEdgeCasePhoneError({
            phone: phone,
            users: sharingUsers.map(u => ({
              ac_id: u.ac_id,
              name: u.name || u.external_id || "unknown",
              email: u.email,
              phone: u.phone,
              external_id: u.external_id,
              user_type: u.user_type,
              zendesk_user_id: u.zendesk_user_id,
            })),
            primaryUser: {
              ac_id: primaryUser.ac_id,
              name: primaryUser.name || primaryUser.external_id || "unknown",
              email: primaryUser.email,
              phone: primaryUser.phone,
              external_id: primaryUser.external_id,
              user_type: primaryUser.user_type,
              zendesk_user_id: primaryUser.zendesk_user_id,
            },
          });
          
          if (!errorStored) {
            logger.debug(`   ⏭️  Edge case phone error already stored (duplicate detected): ${phone}`);
          }
        }
      }
    }
  }

  logger.info(
    `✅ Processed ${processedCount} non-primary users in ${phoneGroups.length} phone group(s)`
  );

  if (problematicPhoneGroups.length > 0) {
    logger.error(
      `❌ Found ${problematicPhoneGroups.length} phone group(s) with 2+ users and no zendesk_primary tag`
    );
  }

  // Return array of users (not groups) that need to be excluded from sync
  const usersNeedingPrimary = [];
  for (const group of problematicPhoneGroups) {
    usersNeedingPrimary.push(...group.users);
  }

  return usersNeedingPrimary;
}


/**
 * Find all active users who share the same email (checking both email field and identities)
 * This checks if any active user has the target email (either directly or as an aliased email)
 */
export function findUsersSharingEmail(targetEmail, excludeAcId) {
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
      if (isAliasedEmail(emailLower)) {
        const unaliased = extractUnaliasedEmail(emailLower).toLowerCase();
        if (unaliased === normalizedTargetEmail) {
          return true;
        }
      }
      
      // Check if target email is aliased and matches when unaliased
      // (This handles the case where target is aliased but user has original)
      if (isAliasedEmail(normalizedTargetEmail)) {
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
  
  if (newEmail && !isAliasedEmail(newEmail)) {
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
    if (identity.type === "email" && identity.value && !isAliasedEmail(identity.value)) {
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
  
  if (newEmail && isAliasedEmail(newEmail)) {
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
    if (identity.type === "email" && identity.value && isAliasedEmail(identity.value)) {
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
 * @deprecated - This function is not used and may be removed in a future version
 * @internal
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
/**
 * Find phone groups with 2+ users and no zendesk_primary tag
 * These users should not be sent to Zendesk and need notification
 */
export function findPhoneGroupsWithoutPrimary() {
  const db = getDb();
  
  // Get only active users to check for conflicts (ignore non-active users like discharged, onhold, etc.)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  
  // Build phone index: phone -> [users who have this phone]
  const phoneIndex = new Map();
  
  for (const user of allUsers) {
    const userPhones = extractAllPhoneNumbers(user);
    for (const phone of userPhones) {
      if (!phone) continue;
      if (!phoneIndex.has(phone)) {
        phoneIndex.set(phone, []);
      }
      phoneIndex.get(phone).push(user);
    }
  }
  
  // Find groups with 2+ users and no zendesk_primary
  const problematicGroups = [];
  
  for (const [phone, users] of phoneIndex.entries()) {
    // Only consider groups with 2+ users
    if (users.length < 2) continue;
    
    // Check if any user has zendesk_primary tag
    const hasPrimary = users.some(
      (u) => u.zendesk_primary === 1 || u.zendesk_primary === true
    );
    
    // If no primary tag, this is a problematic group
    if (!hasPrimary) {
      problematicGroups.push({
        phone: phone,
        users: users.map(u => ({
          ac_id: u.ac_id,
          name: u.name || u.external_id || "unknown",
          email: u.email,
          phone: u.phone,
          external_id: u.external_id,
          user_type: u.user_type,
          zendesk_user_id: u.zendesk_user_id,
        })),
      });
    }
  }
  
  return problematicGroups;
}

export function findEmailGroupsWithoutPrimary() {
  const db = getDb();
  
  // Get only active users to check for conflicts (ignore non-active users like discharged, onhold, etc.)
  const allUsers = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);
  
  // Build email index: email -> [users who have this email]
  const emailIndex = new Map();
  
  for (const user of allUsers) {
    const userEmails = extractAllEmails(user);
    for (const email of userEmails) {
      const normalizedEmail = email.toLowerCase();
      // Extract unaliased email for grouping
      const unaliasedEmail = isAliasedEmail(normalizedEmail) 
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
    if (isAliasedEmail(nonActiveUser.email)) {
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
 * Main function: Process email duplicates only.
 * Phone duplicate processing is commented out per business requirements.
 * 
 * @returns {Array} Array of users who need zendesk_primary tag (groups with no primary)
 */
export function processDuplicateEmailsAndPhones() {
  logger.info("🔍 Processing duplicate emails...");

  // Step 1: Process email duplicates (most important - avoid email conflicts)
  const emailUsersNeedingPrimary = processEmailDuplicates();

  // Step 2: Process phone duplicates
  const phoneUsersNeedingPrimary = processPhoneDuplicates();
  
  if (phoneUsersNeedingPrimary.length > 0) {
    logger.error(`❌ Found ${phoneUsersNeedingPrimary.length} user(s) in phone groups without zendesk_primary tag`);
  }

  logger.info("✅ Finished processing email and phone duplicates");
  
  // Combine users from both email and phone groups that need primary tag
  const allUsersNeedingPrimary = [...(emailUsersNeedingPrimary || []), ...(phoneUsersNeedingPrimary || [])];
  
  // Remove duplicates (in case a user is in both an email group and phone group without primary)
  const uniqueUsersNeedingPrimary = Array.from(
    new Map(allUsersNeedingPrimary.map(u => [u.ac_id, u])).values()
  );
  
  return uniqueUsersNeedingPrimary;
}

