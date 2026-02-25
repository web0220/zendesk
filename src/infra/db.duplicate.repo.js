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
 * Find all field paths in a raw object where the value equals targetValue (case-insensitive for strings).
 * Path format: "demographics.email", "contacts[0].demographics.email"
 * @param {object} obj - Raw client or caregiver object
 * @param {string} targetValue - Email or value to find
 * @param {string} prefix - Current path prefix (internal)
 * @returns {string[]} Array of path strings
 */
function getFieldPathsWithValue(obj, targetValue, prefix = "") {
  if (obj === null || obj === undefined) return [];
  const norm = (v) => (typeof v === "string" ? v.toLowerCase().trim() : String(v));
  const targetNorm = norm(targetValue);
  if (typeof obj !== "object" || Array.isArray(obj)) {
    if (Array.isArray(obj)) {
      const paths = [];
      for (let i = 0; i < obj.length; i++) {
        const seg = prefix ? `${prefix}[${i}]` : `[${i}]`;
        paths.push(...getFieldPathsWithValue(obj[i], targetValue, seg));
      }
      return paths;
    }
    return norm(obj) === targetNorm ? [prefix || "."] : [];
  }
  const paths = [];
  for (const [key, val] of Object.entries(obj)) {
    const seg = prefix ? `${prefix}.${key}` : key;
    if (typeof val === "object" && val !== null && !Array.isArray(val)) {
      paths.push(...getFieldPathsWithValue(val, targetValue, seg));
    } else if (Array.isArray(val)) {
      for (let i = 0; i < val.length; i++) {
        paths.push(...getFieldPathsWithValue(val[i], targetValue, `${seg}[${i}]`));
      }
    } else {
      if (norm(val) === targetNorm) paths.push(seg);
    }
  }
  return paths;
}

/**
 * Build a map of ac_id -> raw client or caregiver for lookup during duplicate processing.
 * @param {Array} clients - Raw client list from API (items with .id)
 * @param {Array} caregivers - Raw caregiver list from API (items with .id)
 * @returns {Map<string, object>} Map of "client_6150" -> raw client, "caregiver_123" -> raw caregiver
 */
function buildRawByAcId(clients = [], caregivers = []) {
  const map = new Map();
  for (const c of clients) {
    const id = c.id ?? c.ac_id;
    if (id != null) map.set(`client_${id}`, c);
  }
  for (const g of caregivers) {
    const id = g.id ?? g.ac_id;
    if (id != null) map.set(`caregiver_${id}`, g);
  }
  return map;
}

/**
 * Convert a field path to a human-readable label for non-technical users.
 * e.g. "demographics.email" -> "Email", "contacts[0].demographics.email" -> "Daughter's email"
 * @param {string} path - Path like "demographics.email", "contacts[0].demographics.email", "demographics.invoice_email_recipients"
 * @param {object} raw - Raw client or caregiver object (to resolve contact relationship)
 * @returns {string} Human-readable label
 */
function pathToHumanReadable(path, raw) {
  if (!path || typeof path !== "string") return path || "";
  const trimmed = path.trim();
  if (trimmed === "demographics.email") return "Email";
  if (trimmed === "demographics.invoice_email_recipients") return "Invoice email";
  const contactMatch = trimmed.match(/^contacts\[(\d+)\]\.demographics\.(email|invoice_email_recipients)$/);
  if (contactMatch) {
    const index = parseInt(contactMatch[1], 10);
    const field = contactMatch[2];
    const contact = raw?.contacts?.[index];
    const relationship = (contact?.relationship ?? contact?.demographics?.relationship ?? "").toString().trim();
    const label = field === "invoice_email_recipients" ? "invoice email" : "email";
    if (relationship) return `${relationship}'s ${label}`;
    return label === "email" ? "Contact email" : "Contact invoice email";
  }
  if (trimmed === "demographics.phone_main" || trimmed === "demographics.phone") return "Phone";
  return trimmed;
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

  // Connect users who share the same email (use external_id so multiple profiles per client stay separate unless they share an email)
  for (const users of emailIndex.values()) {
    if (users.length > 1) {
      for (let i = 0; i < users.length; i++) {
        for (let j = i + 1; j < users.length; j++) {
          union(users[i].external_id, users[j].external_id);
        }
      }
    }
  }

  // Group users by their root parent (by external_id so only profiles that share an email are in the same group)
  const groups = new Map();
  for (const user of allUsers) {
    const root = find(user.external_id);
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
 * After finding primary, fills association1..relation3 on primary's main profile with non-primary users whose raw response contains primary's email (up to 3).
 *
 * @param {Map<string, object>} rawByAcId - Optional map of ac_id -> raw client/caregiver for association lookup
 * @returns {Array} Array of users who need zendesk_primary tag (groups with no primary)
 */
function processEmailDuplicates(rawByAcId = new Map()) {
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

    // Association/relation: find non-primary users whose raw response has primary's email; store up to 3 on primary's main profile
    const primaryEmail = primaryUser.email ? primaryUser.email.trim().toLowerCase() : null;
    if (primaryEmail && rawByAcId.size > 0) {
      const nonPrimaryUsers = group.filter((u) => u.external_id !== primaryUser.external_id);
      const found = [];
      for (const nonPrimary of nonPrimaryUsers) {
        if (found.length >= 3) break;
        const raw = rawByAcId.get(nonPrimary.ac_id);
        if (!raw) continue;
        const paths = getFieldPathsWithValue(raw, primaryEmail);
        if (paths.length > 0) found.push({ ac_id: nonPrimary.ac_id, paths, raw });
      }
      if (found.length > 0) {
        const updateAssocStmt = db.prepare(`
          UPDATE user_mappings
          SET association1 = ?, relation1 = ?,
              association2 = ?, relation2 = ?,
              association3 = ?, relation3 = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE external_id = ?
        `);
        const toRelation = (f) =>
          (f?.paths ?? [])
            .map((p) => pathToHumanReadable(p, f?.raw))
            .filter(Boolean)
            .join(", ") || null;
        const a1 = found[0]?.ac_id ?? null;
        const r1 = toRelation(found[0]);
        const a2 = found[1]?.ac_id ?? null;
        const r2 = toRelation(found[1]);
        const a3 = found[2]?.ac_id ?? null;
        const r3 = toRelation(found[2]);
        updateAssocStmt.run(a1, r1, a2, r2, a3, r3, primaryUser.ac_id);
        logger.info(`   📎 Stored ${found.length} association(s) on primary ${primaryUser.ac_id}: ${found.map((f) => f.ac_id).join(", ")}`);
      }
    }

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

      // Update user if email or identities changed (use external_id so we update only this profile row, not all profiles with same ac_id)
      if (emailWasAliased || JSON.stringify(processedIdentities) !== JSON.stringify(identities)) {
        const updateStmt = db.prepare(`
          UPDATE user_mappings
          SET email = ?,
              identities = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE external_id = ?
        `);

        updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.external_id);
        processedCount++;
      }
    }

    // EDGE CASE DETECTION: Check if non-primary users share emails that don't match primary user
    // After aliasing emails that match primary user, check if non-primary users still share other emails
    const nonPrimaryUsers = group.filter((u) => u.external_id !== primaryUser.external_id);
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
  const processedPrimaryExternalIds = new Set();
  const problematicPhoneGroups = [];

  for (const [phone, phoneGroup] of phoneGroups) {
    // Select primary user for this phone group (prefer zendesk_primary)
    const primaryUser = phoneGroup.find(
      (u) => u.zendesk_primary === 1 || u.zendesk_primary === true
    );

    if (!primaryUser) {
      const userList = phoneGroup
        .map((u) => `${u.external_id} (${u.name || u.external_id})`)
        .join(", ");
      logger.error(
        `❌ ERROR: No zendesk_primary user found in phone group (phone=${phone}). Users: ${userList}`
      );
      problematicPhoneGroups.push({ phone, users: phoneGroup });
      continue;
    }

    const duplicateUsers = phoneGroup.filter((u) => u.external_id !== primaryUser.external_id);

    // Skip if already processed this primary (to avoid double work)
    if (processedPrimaryExternalIds.has(primaryUser.external_id)) {
      logger.debug(
        `⏭️  Skipping phone group (already processed primary): primary=${primaryUser.external_id}`
      );
      continue;
    }

    logger.info(
      `   Processing phone group: phone=${phone}, primary=${primaryUser.external_id} (${primaryUser.name || primaryUser.external_id}), ${duplicateUsers.length} duplicate(s)`
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
          `   Updated non-primary user ${duplicateUser.external_id}: moved ${phoneCount} phone(s) to shared_phone_number`
        );
        processedCount++;
      } catch (error) {
        logger.error(
          `❌ Failed to move phones to shared_phone_number for user ${duplicateUser.external_id}: ${error.message}`
        );
      }
    }

    processedPrimaryExternalIds.add(primaryUser.external_id);

    // EDGE CASE DETECTION: Check if non-primary users share phones that don't match primary user
    // After moving phones to shared_phone_number for primary user's phones, check if non-primary users still share other phones
    const nonPrimaryUsersForEdgeCase = phoneGroup.filter((u) => u.external_id !== primaryUser.external_id);
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
  
  // Update database (use external_id so we update only this profile row)
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET email = ?,
        identities = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE external_id = ?
  `);
  
  updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.external_id);
  
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
  
  // Update database (use external_id so we update only this profile row)
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET email = ?,
        identities = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE external_id = ?
  `);
  
  updateStmt.run(newEmail, JSON.stringify(processedIdentities), user.external_id);
  
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
  
  // Update database (use external_id so we update only this profile row)
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET phone = NULL,
        identities = ?,
        shared_phone_number = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE external_id = ?
  `);
  
  updateStmt.run(JSON.stringify(filteredIdentities), sharedPhoneNumberStr, user.external_id);
  
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
  
  // Update database (use external_id so we update only this profile row)
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET phone = ?,
        identities = ?,
        shared_phone_number = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE external_id = ?
  `);
  
  updateStmt.run(primaryPhone, JSON.stringify(updatedIdentities), user.external_id);
  
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
 * When a non-main profile has the same name as the main profile, set its name to "Original Name (suffix)"
 * where suffix is the part of external_id after ac_id (e.g. client_6437_email_2 -> "email_2").
 * Run after saving mapped data so duplicate profiles are distinguishable.
 */
export function normalizeDuplicateProfileNames() {
  const db = getDb();
  const allActive = db
    .prepare("SELECT external_id, ac_id, name FROM user_mappings WHERE current_active = 1")
    .all();

  const byAcId = new Map();
  for (const row of allActive) {
    const acId = row.ac_id;
    if (!acId) continue;
    if (!byAcId.has(acId)) byAcId.set(acId, []);
    byAcId.get(acId).push(row);
  }

  const updateNameStmt = db.prepare(`
    UPDATE user_mappings SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE external_id = ?
  `);
  let updatedCount = 0;
  for (const [acId, profiles] of byAcId) {
    const main = profiles.find((p) => p.external_id === acId);
    if (!main || !main.name) continue;
    const mainName = main.name.trim();
    for (const p of profiles) {
      if (p.external_id === acId) continue;
      if ((p.name || "").trim() !== mainName) continue;
      const suffix = p.external_id.startsWith(acId + "_")
        ? p.external_id.slice(acId.length + 1)
        : p.external_id;
      const newName = `${mainName} (${suffix})`;
      updateNameStmt.run(newName, p.external_id);
      updatedCount += 1;
    }
  }
  if (updatedCount > 0) {
    logger.info(`   Renamed ${updatedCount} duplicate profile(s) to include suffix (e.g. "Name (email_2)")`);
  }
}

/**
 * Build placeholder email when all profiles for an ac_id have only aliased emails.
 * Format: name (lowercase, spaces removed)@noemail.com
 * @param {string} name - Display name (e.g. "Bernice Stein")
 * @returns {string} e.g. "bernicestein@noemail.com"
 */
function toNoEmail(name) {
  const base = (name || "").toLowerCase().replace(/\s+/g, "");
  return (base || "unknown") + "@noemail.com";
}

/**
 * For each unique ac_id that has at least one profile with an aliased email:
 * - If ALL profiles (with email) have only aliased emails: collapse to single main profile (delete non-main, set main email to name@noemail.com).
 * - If at least one profile has non-aliased email: delete only the profiles that have aliased email (keep the rest; do not change any email).
 * Runs after saving mapped data and after processDuplicateEmailsAndPhones.
 * Main profile = the row where external_id === ac_id (e.g. client_6150).
 */
export function collapseAllAliasedProfilesPerAcId() {
  const db = getDb();
  const allActive = db
    .prepare("SELECT * FROM user_mappings WHERE current_active = 1")
    .all()
    .map(hydrateMapping);

  const byAcId = new Map();
  for (const user of allActive) {
    const acId = user.ac_id;
    if (!acId) continue;
    if (!byAcId.has(acId)) byAcId.set(acId, []);
    byAcId.get(acId).push(user);
  }

  const deleteStmt = db.prepare("DELETE FROM user_mappings WHERE external_id = ?");
  const updateStmt = db.prepare(`
    UPDATE user_mappings
    SET email = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE external_id = ?
  `);

  let collapsedCount = 0;
  let deletedOnlyCount = 0;
  for (const [acId, profiles] of byAcId) {
    const withAliased = profiles.filter((p) => p.email && isAliasedEmail(p.email));
    if (withAliased.length === 0) continue;

    const withEmail = profiles.filter((p) => p.email);
    const allAliased = withEmail.length > 0 && withEmail.every((p) => isAliasedEmail(p.email));
    const mainProfile = profiles.find((p) => p.external_id === acId);

    if (allAliased && mainProfile) {
      // All profiles have only aliased email: keep only main, set main email to name@noemail.com
      const nonMain = profiles.filter((p) => p.external_id !== acId);
      const noEmail = toNoEmail(mainProfile.name);
      const run = db.transaction(() => {
        for (const p of nonMain) {
          deleteStmt.run(p.external_id);
        }
        updateStmt.run(noEmail, acId);
      });
      run();
      collapsedCount += 1;
      logger.info(
        `   Collapsed all-aliased ac_id=${acId} (${mainProfile.name}): kept main profile, deleted ${nonMain.length} other(s), email → ${noEmail}`
      );
    } else {
      // At least one profile has non-aliased email: delete only profiles that have aliased email
      const toDelete = profiles.filter((p) => p.email && isAliasedEmail(p.email));
      if (toDelete.length > 0) {
        const run = db.transaction(() => {
          for (const p of toDelete) {
            deleteStmt.run(p.external_id);
          }
        });
        run();
        deletedOnlyCount += 1;
        logger.info(
          `   Removed ${toDelete.length} aliased-only profile(s) for ac_id=${acId}: ${toDelete.map((p) => p.external_id).join(", ")}`
        );
      }
    }
  }

  if (collapsedCount > 0 || deletedOnlyCount > 0) {
    logger.info(
      `✅ Collapsed ${collapsedCount} ac_id(s) to single @noemail.com; removed aliased-only profiles for ${deletedOnlyCount} other ac_id(s)`
    );
  }
}

/**
 * Main function: Process email duplicates only.
 * Phone duplicate processing is commented out per business requirements.
 * Pass raw clients and caregivers so we can store association/relation (non-primary users whose raw response has primary's email) on primary's main profile.
 *
 * @param {Array} [rawClients=[]] - Raw client list from API (for association lookup)
 * @param {Array} [rawCaregivers=[]] - Raw caregiver list from API (for association lookup)
 * @returns {Array} Array of users who need zendesk_primary tag (groups with no primary)
 */
export function processDuplicateEmailsAndPhones(rawClients = [], rawCaregivers = []) {
  logger.info("🔍 Processing duplicate emails...");

  const rawByAcId = buildRawByAcId(rawClients, rawCaregivers);

  // Step 1: Process email duplicates (most important - avoid email conflicts)
  const emailUsersNeedingPrimary = processEmailDuplicates(rawByAcId);

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

