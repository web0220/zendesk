import { logger } from "../../config/logger.js";
import { isValidEmail, cleanEmail, normalizePhone } from "../../utils/validator.js";

/**
 * Strip known non-email prefixes from the start of a string so the real email is found.
 * E.g. "N/A VA Referral-Davidbluepadilla@gmail.com" -> "Davidbluepadilla@gmail.com"
 * @param {string} value
 * @returns {string}
 */
export function stripKnownEmailPrefixes(value) {
  if (!value || typeof value !== "string") return value;
  let s = value.trim();
  const prefixes = [
    /^N\/A\s+/i,
    /^VA\s+/i,
    /^Referral-\s*/i,
  ];
  let changed = true;
  while (changed) {
    changed = false;
    for (const p of prefixes) {
      const match = s.match(p);
      if (match) {
        s = s.slice(match[0].length);
        changed = true;
        break;
      }
    }
  }
  return s;
}

/**
 * Extract emails from a string value (handles comma-separated, space-separated, etc.)
 * Strips known prefixes (e.g. "N/A VA Referral-") so "N/A VA Referral-Davidbluepadilla@gmail.com" yields "davidbluepadilla@gmail.com".
 * @param {string} value - String that may contain emails
 * @returns {Array<string>} Array of valid email addresses
 */
function extractEmailsFromString(value) {
  if (!value || typeof value !== "string") return [];
  const normalized = stripKnownEmailPrefixes(value);
  const emailPattern = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
  const matches = normalized.match(emailPattern) || [];
  return matches.map(email => cleanEmail(email)).filter(email => isValidEmail(email));
}

/**
 * Extract emails from AlayaCare client response with priority ranking
 * Returns array of email objects with priority rank and source field
 *
 * Priority (profile type): main first, then contact, then field-only.
 * If an email appears in both a contact and in billing/scheduling/etc. fields,
 * one profile is created as the contact profile with field names appended to relationship and external_id.
 *
 * Rank 1: demographics.email (main profile)
 * Rank 2: contacts[i].demographics.email (contact profile; may have rank2FieldNames if also in fields)
 * Rank 3: billing_contact, scheduling_contact, emergency_contact, invoice_email_recipients (field-only profile)
 *
 * @param {Object} client - Raw AlayaCare client response
 * @returns {Array<{email: string, rank: number, sourceField: string, contactIndex?: number, contactRelationship?: string, rank2FieldNames?: string[]}>}
 */
export function extractEmailsWithPriority(client) {
  const emails = new Map();
  const demographics = client.demographics || {};

  // Rank 1: demographics.email
  if (demographics.email) {
    const emailList = extractEmailsFromString(demographics.email);
    for (const email of emailList) {
      const emailKey = email.toLowerCase();
      if (!emails.has(emailKey)) {
        emails.set(emailKey, {
          email,
          rank: 1,
          sourceField: "email",
        });
      }
    }
  }

  // Rank 2: contacts[i].demographics.email (contact profile wins when email is in both contact and fields)
  if (Array.isArray(client.contacts)) {
    client.contacts.forEach((contact, index) => {
      const contactDemo = contact?.demographics || {};
      if (contactDemo.email) {
        const emailList = extractEmailsFromString(contactDemo.email);
        for (const email of emailList) {
          const contactRelationship = contactDemo.relationship || contact.relationship || null;
          const emailKey = email.toLowerCase();

          if (!emails.has(emailKey)) {
            emails.set(emailKey, {
              email,
              rank: 2,
              sourceField: "contact",
              contactIndex: index,
              contactRelationship: contactRelationship ?? null,
              rank2FieldNames: [], // fields (billing_contact, etc.) where this email also appears
            });
          } else {
            const existing = emails.get(emailKey);
            if (existing.rank === 1) {
              // Main email also listed as contact: keep as main, optionally track contact relationship
              if (contactRelationship) {
                existing.contactRelationship = contactRelationship;
              }
            } else if (existing.rank === 2 && existing.contactIndex !== index) {
              // Same email in two contacts: combine contact relationships
              if (contactRelationship) {
                const parts = (existing.contactRelationship || "").split(", ").filter(Boolean);
                if (!parts.includes(contactRelationship)) {
                  existing.contactRelationship = [...parts, contactRelationship].join(", ");
                }
              }
            }
          }
        }
      }
    });
  }

  // Rank 3: billing_contact, scheduling_contact, emergency_contact, invoice_email_recipients
  // If email already exists (e.g. from contact), attach field name to that profile; otherwise create field-only profile.
  const rank3Fields = [
    "billing_contact",
    "scheduling_contact",
    "emergency_contact",
    "invoice_email_recipients",
  ];

  for (const fieldName of rank3Fields) {
    if (demographics[fieldName]) {
      const emailList = extractEmailsFromString(demographics[fieldName]);
      for (const email of emailList) {
        const emailKey = email.toLowerCase();
        if (!emails.has(emailKey)) {
          emails.set(emailKey, {
            email,
            rank: 3,
            sourceField: fieldName,
            relationship: fieldName,
          });
        } else {
          const existing = emails.get(emailKey);
          if (existing.rank === 2 && existing.sourceField === "contact" && Array.isArray(existing.rank2FieldNames)) {
            if (!existing.rank2FieldNames.includes(fieldName)) {
              existing.rank2FieldNames.push(fieldName);
            }
          } else if (existing.rank === 3) {
            if (existing.relationship !== fieldName) {
              const parts = (existing.relationship || existing.sourceField || "").split(", ").filter(Boolean);
              if (!parts.includes(fieldName)) {
                existing.relationship = [...parts, fieldName].join(", ");
              }
            }
          }
        }
      }
    }
  }

  return Array.from(emails.values()).sort((a, b) => a.rank - b.rank);
}

/**
 * Extract contacts that have unique phone numbers but no email
 * These contacts should still get profiles created
 * 
 * @param {Object} client - Raw AlayaCare client response
 * @param {Array} emailProfiles - Already extracted email profiles
 * @returns {Array<{contactIndex: number, phone: string, relationship: string}>}
 */
export function extractContactsWithUniquePhones(client, emailProfiles) {
  const contactsWithPhones = [];
  const demographics = client.demographics || {};
  
  // Collect all phone numbers already used in email profiles or demographics fields
  const usedPhones = new Set();
  const phonePattern = /(?:(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4})/g;
  
  // Add phones from demographics fields
  const demographicsPhoneFields = [
    demographics.phone_main,
    demographics.billing_contact,
    demographics.scheduling_contact,
    demographics.emergency_contact,
  ];
  
  for (const fieldValue of demographicsPhoneFields) {
    if (typeof fieldValue === "string") {
      const matches = fieldValue.match(phonePattern);
      if (matches) {
        matches.forEach(phone => {
          const normalized = normalizePhone(phone);
          if (normalized) {
            const normalizedForComparison = normalized.replace(/\D/g, "");
            if (normalizedForComparison.length >= 10) {
              usedPhones.add(normalizedForComparison);
            }
          }
        });
      }
    }
  }
  
  // Add phones from email profiles
  emailProfiles.forEach(emailProfile => {
    if (emailProfile.phone) {
      const normalized = normalizePhone(emailProfile.phone);
      if (normalized) {
        const normalizedForComparison = normalized.replace(/\D/g, "");
        usedPhones.add(normalizedForComparison);
      }
    }
  });
  
  // Check contacts for unique phones without emails
  if (Array.isArray(client.contacts)) {
    client.contacts.forEach((contact, index) => {
      const contactDemo = contact?.demographics || {};
      const contactEmail = contactDemo.email;
      
      // Skip if contact has an email (already handled in emailProfiles)
      if (contactEmail) {
        const emailList = extractEmailsFromString(contactEmail);
        if (emailList.length > 0) {
          return; // Skip this contact - has email
        }
      }
      
      // Check if contact has a phone number
      if (contactDemo.phone_main) {
        const matches = contactDemo.phone_main.match(phonePattern);
        if (matches && matches.length > 0) {
          const rawPhone = matches[0].trim();
          const normalized = normalizePhone(rawPhone);
          if (normalized) {
            const normalizedForComparison = normalized.replace(/\D/g, "");
            
            // Only create profile if phone is unique (not already used)
            if (!usedPhones.has(normalizedForComparison)) {
              contactsWithPhones.push({
                contactIndex: index,
                phone: normalized,
                relationship: contactDemo.relationship || contact.relationship || "contact",
              });
              // Mark this phone as used
              usedPhones.add(normalizedForComparison);
            }
          }
        }
      }
    });
  }
  
  return contactsWithPhones;
}

/**
 * Extract phone number for a specific email profile
 * 
 * Rules:
 * - Main profile (rank 1, sourceField="email"): demographics.phone_main
 * - Contact profiles (rank 3): demographics.contacts[i].demographics.phone_main
 *   (unless that number was already used in demographics fields)
 * 
 * @param {Object} client - Raw AlayaCare client response
 * @param {Object} emailProfile - Email profile object from extractEmailsWithPriority
 * @returns {string|null} Phone number or null
 */
export function extractPhoneForEmailProfile(client, emailProfile) {
  const demographics = client.demographics || {};
  
  // Collect all phone numbers from demographics fields (billing_contact, scheduling_contact, etc.)
  const demographicsPhoneFields = [
    demographics.phone_main,
    demographics.phone_other,
    demographics.billing_contact,
    demographics.scheduling_contact,
    demographics.emergency_contact,
  ];
  
  const usedPhones = new Set();
  const phonePattern = /(?:(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4})/g;
  
  for (const fieldValue of demographicsPhoneFields) {
    if (typeof fieldValue === "string") {
      const matches = fieldValue.match(phonePattern);
      if (matches) {
        matches.forEach(phone => {
          // Normalize phone (remove spaces, dashes, etc. for comparison)
          const normalized = normalizePhone(phone);
          if (normalized) {
            const normalizedForComparison = normalized.replace(/\D/g, "");
            if (normalizedForComparison.length >= 10) {
              usedPhones.add(normalizedForComparison);
            }
          }
        });
      }
    }
  }
  
  // Main profile: use demographics.phone_main
  if (emailProfile.rank === 1 && emailProfile.sourceField === "email") {
    if (demographics.phone_main) {
      const matches = demographics.phone_main.match(phonePattern);
      if (matches && matches.length > 0) {
        const rawPhone = matches[0].trim();
        return normalizePhone(rawPhone);
      }
    }
    return null;
  }
  
  // Contact profiles (rank 2): use contact's phone_main. Return it even if it appears in demographics
  // (e.g. emergency_contact); the normalizer will null it only when a main profile exists to hold it.
  if (emailProfile.rank === 2 && emailProfile.sourceField === "contact" && emailProfile.contactIndex !== undefined) {
    const contact = client.contacts?.[emailProfile.contactIndex];
    const contactDemo = contact?.demographics || {};

    if (contactDemo.phone_main) {
      const matches = contactDemo.phone_main.match(phonePattern);
      if (matches && matches.length > 0) {
        const rawPhone = matches[0].trim();
        const normalized = normalizePhone(rawPhone);
        if (normalized) return normalized;
      }
    }
  }

  // Rank 3 profiles (field-only): try to extract phone from the field itself
  if (emailProfile.rank === 3) {
    const fieldValue = demographics[emailProfile.sourceField];
    if (fieldValue && typeof fieldValue === "string") {
      const matches = fieldValue.match(phonePattern);
      if (matches && matches.length > 0) {
        const rawPhone = matches[0].trim();
        return normalizePhone(rawPhone);
      }
    }
  }
  
  return null;
}

/** Humanize field name for display (e.g. scheduling_contact -> "scheduling contact", invoice_email_recipients -> "invoice recipient") */
function humanizeFieldName(fieldName) {
  if (!fieldName || typeof fieldName !== "string") return fieldName;
  const s = fieldName.replace(/_/g, " ").trim();
  return s.replace(/\brecipients\b/i, "recipient");
}

/**
 * Get relationship string for an email profile
 *
 * For contact profiles (rank 2) that also appear in fields: "daughter, scheduling contact, billing contact, invoice recipient"
 *
 * @param {Object} emailProfile - Email profile object from extractEmailsWithPriority
 * @returns {string} Relationship string
 */
export function getRelationshipForEmailProfile(emailProfile) {
  if (emailProfile.rank === 1 && emailProfile.sourceField === "email") {
    if (emailProfile.contactRelationship) {
      const relationships = ["self", emailProfile.contactRelationship].filter(Boolean);
      return Array.from(new Set(relationships)).join(", ");
    }
    return "self";
  }

  if (emailProfile.rank === 2 && emailProfile.sourceField === "contact") {
    // Contact profile: contact relationship first, then humanized rank2FieldNames
    const parts = [];
    if (emailProfile.contactRelationship) {
      const rels = emailProfile.contactRelationship.split(", ").filter(Boolean);
      parts.push(...Array.from(new Set(rels)));
    }
    const fieldNames = emailProfile.rank2FieldNames || [];
    for (const f of fieldNames) {
      const human = humanizeFieldName(f);
      if (human && !parts.includes(human)) parts.push(human);
    }
    return parts.length ? parts.join(", ") : "contact";
  }

  if (emailProfile.rank === 3) {
    // Field-only: relationship may be combined field names (e.g. "billing_contact, scheduling_contact")
    const relationship = emailProfile.relationship || emailProfile.sourceField;
    const parts = (relationship || "").split(", ").map(humanizeFieldName).filter(Boolean);
    return parts.length ? parts.join(", ") : "unknown";
  }

  return emailProfile.sourceField || "unknown";
}
