import { logger } from "../../config/logger.js";
import {
  collectAllEmailDetails,
  collectAllPhoneDetails,
  collectCaregiverEmailDetails,
  collectCaregiverPhoneDetails,
  findEmailsDeep,
  removeInternalEmails,
  sanitizeMarketValues,
} from "./identity.extractor.js";
import {
  extractEmailsWithPriority,
  extractPhoneForEmailProfile,
  getRelationshipForEmailProfile,
  extractContactsWithUniquePhones,
} from "./email.priority.extractor.js";
import { normalizePhone } from "../../utils/validator.js";

function extractMarket(groups = []) {
  const locGroups = groups.filter(
    (group) => typeof group?.name === "string" && group.name.trim().toUpperCase().startsWith("LOC")
  );
  if (locGroups.length === 0) return null;

  const markets = locGroups
    .map((loc) => {
      const match = loc.name.match(/^LOC\s*-\s*([^(]+)/i);
      return match ? match[1].trim() : loc.name;
    })
    .filter(Boolean);

  return markets.length > 0 ? markets.join(", ") : null;
}

function extractCoordinatorPod(groups = []) {
  const pods = groups
    .filter(
      (group) =>
        typeof group?.name === "string" && group.name.trim().toUpperCase().startsWith("POD")
    )
    .map((group) => group.name.replace(/^POD\s*-\s*/i, "").trim())
    .filter(Boolean);
  return pods.length > 0 ? pods.join(", ") : null;
}

function extractClinicalRNManager(groups = []) {
  const managers = groups
    .filter(
      (group) =>
        typeof group?.name === "string" && group.name.trim().toUpperCase().startsWith("CM")
    )
    .map((group) => group.name.replace(/^CM\s*-\s*/i, "").trim())
    .filter(Boolean);
  return managers.length > 0 ? managers.join(", ") : null;
}

function extractSalesRep(tags = []) {
  const tag = tags
    .filter((t) => typeof t === "string")
    .map((t) => t.trim())
    .find((t) => t.toUpperCase().startsWith("BD "));
  if (!tag) return null;
  return tag.replace(/^BD\s*/i, "").trim();
}

function extractTierCaseRating(groups = []) {
  const tierGroup = groups
    .filter((group) => typeof group?.name === "string")
    .find((group) => group.name.trim().toUpperCase().startsWith("TIER"));

  if (!tierGroup) return null;

  const match = tierGroup.name.match(/^TIER\s*-\s*(.+)$/i);
  const value = match ? match[1].trim() : tierGroup.name.replace(/^TIER\s*/i, "").trim();

  return value ? value.toLowerCase() : null;
}

function extractZendeskPrimary(tags = []) {
  return tags
    .filter((t) => typeof t === "string")
    .map((t) => t.trim().toLowerCase())
    .some((tag) => tag === "zendesk primary");
}

/**
 * Maps client status from AlayaCare to Zendesk tag format
 * @param {string|null} status - Raw status from AlayaCare
 * @returns {string|null} Formatted status tag (e.g., 'cl_active', 'cl_on_hold')
 */
function mapClientStatus(status) {
  if (!status) return "cl_not_set";
  
  const normalizedStatus = status.trim().toLowerCase();
  
  // Map specific status values to Zendesk tags
  const statusMap = {
    "active": "cl_active",
    "onhold": "cl_on_hold",
    "on hold": "cl_on_hold",
    "discharged": "cl_discharged",
    "waiting list": "cl_waiting_list",
    "waitinglist": "cl_waiting_list",
    "not set": "cl_not_set",
    "notset": "cl_not_set",
  };
  
  // Check exact match first
  if (statusMap[normalizedStatus]) {
    return statusMap[normalizedStatus];
  }
  
  // Fallback: normalize spaces and convert to tag format
  return `cl_${normalizedStatus.replace(/\s+/g, "_")}`;
}

/**
 * Maps caregiver status from AlayaCare to Zendesk tag format
 * @param {string|null} status - Raw status from AlayaCare
 * @returns {string|null} Formatted status tag (e.g., 'cg_active', 'cg_suspended')
 */
function mapCaregiverStatus(status) {
  if (!status) return null;
  
  const normalizedStatus = status.trim().toLowerCase();
  
  // Map specific status values to Zendesk tags
  const statusMap = {
    "active": "cg_active",
    "suspended": "cg_suspended",
    "hold": "cg_hold",
    "terminated": "cg_terminated",
    "pending": "cg_pending",
    "applicant": "cg_applicant",
    "rejected": "cg_rejected",
  };
  
  // Check exact match first
  if (statusMap[normalizedStatus]) {
    return statusMap[normalizedStatus];
  }
  
  // Fallback: normalize spaces and convert to tag format
  return `cg_${normalizedStatus.replace(/\s+/g, "_")}`;
}

function determineOrganizationId(allEmails, type) {
  const isMember = allEmails.some(
    (email) => typeof email === "string" && email.toLowerCase().includes("@alvitacare.com")
  );

  if (type === "client") {
    return isMember ? 40994316312731 : 42824772337179;
  }

  return isMember ? 40994316312731 : 43279021546651;
}

function buildClientUserFields({
  coordinatorPodValue,
  caseRatingValue,
  clientStatusValue,
  clinicalRNManagerArray,
  salesRepArray,
  marketArray,
  schedulingPreferences,
}) {
  return {
    market: marketArray,
    coordinator_pod: coordinatorPodValue,
    clinical_rn_manager: clinicalRNManagerArray,
    case_rating: caseRatingValue,
    userstatus: clientStatusValue,
    sales_rep: salesRepArray,
    scheduling_preferences: schedulingPreferences,
    type: "client",
  };
}

function buildCaregiverUserFields({ marketArray, caregiverStatusValue, departmentNames }) {
  return {
    market: marketArray,
    userstatus: caregiverStatusValue,
    department: departmentNames,
    type: "caregiver",
  };
}

/**
 * Normalize client record and create multiple profiles (one per unique email)
 * Returns array of profile objects
 */
export function normalizeClientRecord(client) {
  try {
    const demographics = client.demographics || {};
    const firstName =
      client.first_name ||
      client.firstName ||
      demographics.first_name ||
      demographics.firstName ||
      "";
    const lastName =
      client.last_name ||
      client.lastName ||
      demographics.last_name ||
      demographics.lastName ||
      "";

    const status = client.status || null;
    const groups = client.groups || [];
    const tags = client.tags || [];

    // Extract emails with priority ranking
    const emailProfiles = extractEmailsWithPriority(client);
    
    // Also check for contacts with unique phone numbers but no email
    const contactsWithUniquePhones = extractContactsWithUniquePhones(client, emailProfiles);
    
    if (emailProfiles.length === 0 && contactsWithUniquePhones.length === 0) {
      logger.warn(`⚠️ No emails or unique phone contacts found for client ${client.id || client.ac_id || "unknown"}`);
      return [];
    }

    // Extract common fields (same for all profiles)
    const market = extractMarket(groups) || null;
    const coordinator = extractCoordinatorPod(groups) || null;
    const clinicalRNManager =
      client.clinical_rn_manager || client.clinicalRNManager || extractClinicalRNManager(groups);
    const caseRating = extractTierCaseRating(groups);
    const salesRep = extractSalesRep(tags) || null;
    const zendeskPrimary = extractZendeskPrimary(tags);
    const schedulingPreferences = demographics.scheduling_preferences || null;

    const marketValues = Array.isArray(market)
      ? market
      : typeof market === "string"
      ? market.split(",")
      : [];
    const marketArray = sanitizeMarketValues(marketValues);

    const coordinatorPodValue = coordinator
      ? coordinator.split(",")[0].trim().replace(/\s+/g, "_").toLowerCase()
      : null;
    const caseRatingValue = caseRating ? caseRating.replace(/\s+/g, "_").toLowerCase() : null;
    const clientStatusValue = mapClientStatus(status);

    const clinicalRNManagerArray = clinicalRNManager
      ? clinicalRNManager
          .split(",")
          .map((value) => value.trim().replace(/\s+/g, "_").toLowerCase())
          .filter(Boolean)
      : null;
    const salesRepArray = salesRep
      ? salesRep
          .split(",")
          .map((value) => value.trim().replace(/\s+/g, "_").toLowerCase())
          .filter(Boolean)
      : null;

    const name = `${firstName} ${lastName}`.trim() || null;
    const acId = client.id || client.ac_id || null;
    
    // Build common user fields
    const commonUserFields = buildClientUserFields({
      coordinatorPodValue,
      caseRatingValue,
      clientStatusValue,
      clinicalRNManagerArray,
      salesRepArray,
      marketArray,
      schedulingPreferences,
    });

    // Collect phone numbers ONLY from demographics fields (excluding contact fields)
    // These phones go to main profile identities, even if contacts also have them
    const clientDemographics = client.demographics || {};
    const demographicsPhoneFields = [
      clientDemographics.phone_main,
      clientDemographics.phone_other,
      clientDemographics.billing_contact,
      clientDemographics.scheduling_contact,
      clientDemographics.emergency_contact,
    ];
    
    const phonePattern = /(?:(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4})/g;
    const mainProfilePhonesSet = new Set();
    
    for (const fieldValue of demographicsPhoneFields) {
      if (typeof fieldValue === "string") {
        const matches = fieldValue.match(phonePattern);
        if (matches) {
          matches.forEach(phone => {
            const normalized = normalizePhone(phone);
            if (normalized) {
              const normalizedForComparison = normalized.replace(/\D/g, "");
              if (normalizedForComparison.length >= 10) {
                mainProfilePhonesSet.add(normalizedForComparison);
              }
            }
          });
        }
      }
    }
    
    // Get unique contact phones that will have their own profiles (phones that ONLY exist in contact fields)
    const uniqueContactPhones = new Set();
    contactsWithUniquePhones.forEach(contactPhone => {
      const normalized = normalizePhone(contactPhone.phone);
      if (normalized) {
        const normalizedForComparison = normalized.replace(/\D/g, "");
        uniqueContactPhones.add(normalizedForComparison);
      }
    });
    
    // Main profile phones: all phones from demographics fields (excluding unique contact phones)
    const mainProfilePhones = Array.from(mainProfilePhonesSet)
      .filter(phoneNormalized => !uniqueContactPhones.has(phoneNormalized))
      .map(phoneNormalized => {
        // Convert back to normalized format (+1XXXXXXXXXX)
        return phoneNormalized.startsWith("1") ? `+${phoneNormalized}` : `+1${phoneNormalized}`;
      });
    
    // Create one profile per unique email
    const profiles = [];
    // Track index among rank-1 "demographics.email" profiles so multiple main emails get unique external_ids
    let mainEmailProfileIndex = 0;

    for (const emailProfile of emailProfiles) {
      const email = emailProfile.email;
      let phone = extractPhoneForEmailProfile(client, emailProfile);
      const relationship = getRelationshipForEmailProfile(emailProfile);

      // For contact profiles (rank 2): if their phone exists in demographics fields,
      // don't include it (set to null) - it goes to main profile instead
      if (emailProfile.rank === 2 && emailProfile.sourceField === "contact" && phone) {
        const phoneNormalized = normalizePhone(phone);
        if (phoneNormalized) {
          const phoneForComparison = phoneNormalized.replace(/\D/g, "");
          // If phone exists in demographics fields, exclude it from contact profile
          if (mainProfilePhonesSet.has(phoneForComparison)) {
            phone = null;
          }
        }
      }

      // Build external_id: primary first (unique per main email when multiple), then contact, then other fields
      let externalId;
      if (emailProfile.rank === 1 && emailProfile.sourceField === "email") {
        mainEmailProfileIndex += 1;
        if (mainEmailProfileIndex === 1) {
          externalId = acId ? `client_${acId}` : null;
        } else {
          externalId = acId ? `client_${acId}_email_${mainEmailProfileIndex}` : null;
        }
      } else if (emailProfile.rank === 2 && emailProfile.sourceField === "contact") {
        // Contact profile: client_xxxx_{contactRelationship}_{field1}_{field2}...
        const contactRelationship = emailProfile.contactRelationship || "contact";
        const relParts = contactRelationship.split(", ").map((s) => s.trim()).filter(Boolean);
        const rank2FieldNames = emailProfile.rank2FieldNames || [];
        const allParts = [...relParts, ...rank2FieldNames];
        const sanitized = allParts.map((p) => p.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase()).filter(Boolean);
        const suffix = Array.from(new Set(sanitized)).join("_");
        externalId = acId ? `client_${acId}_${suffix || "contact"}` : null;
      } else {
        // Rank 3 (field-only): client_xxxx_{sourceField} or combined fields
        const relationship = emailProfile.relationship || emailProfile.sourceField;
        const parts = (relationship || "").split(", ").map((s) => s.trim()).filter(Boolean);
        const sanitized = parts.map((p) => p.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase()).filter(Boolean);
        const suffix = Array.from(new Set(sanitized)).join("_");
        externalId = acId && suffix ? `client_${acId}_${suffix}` : acId ? `client_${acId}_${(emailProfile.sourceField || "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase()}` : null;
      }
      
      // Organization ID will be set to null initially (will be added after sync)
      const organizationId = null;
      
      // Identities:
      // - Main profile (rank 1): additional phone numbers (different from phone field value)
      // - Rank 2 & 3: empty (phone goes to main profile if exists in demographics, email is in email field)
      let identities = [];
      if (emailProfile.rank === 1 && emailProfile.sourceField === "email") {
        // Main profile: add phones to identities, but exclude the phone field value
        const phoneForComparison = phone ? normalizePhone(phone)?.replace(/\D/g, "") : null;
        identities = mainProfilePhones
          .filter(profilePhone => {
            // Exclude phone if it matches the phone field value
            if (phoneForComparison) {
              const profilePhoneForComparison = normalizePhone(profilePhone)?.replace(/\D/g, "");
              return profilePhoneForComparison !== phoneForComparison;
            }
            return true;
          })
          .map(phone => ({ type: "phone_number", value: phone }));
      }
      // Rank 2 & 3: empty identities array
      
      // Build source_field for tracking
      let sourceField = emailProfile.sourceField;
      if (emailProfile.rank === 1) {
        sourceField = `demographics.email`;
      } else if (emailProfile.rank === 2 && emailProfile.contactIndex !== undefined) {
        sourceField = `contacts[${emailProfile.contactIndex}].demographics.email`;
      } else if (emailProfile.rank === 3) {
        sourceField = `demographics.${emailProfile.sourceField}`;
      }

      // Profile name: contact profiles (rank 2 from contact) use contact's name; main (rank 1) and field-only (rank 3) use client name
      let profileName = name;
      if (emailProfile.rank === 2 && emailProfile.sourceField === "contact" && emailProfile.contactIndex != null) {
        const contact = client.contacts?.[emailProfile.contactIndex];
        const contactDemo = contact?.demographics || {};
        const contactFirstName = contactDemo.first_name || contact?.first_name || "";
        const contactLastName = contactDemo.last_name || contact?.last_name || "";
        const contactName = `${contactFirstName} ${contactLastName}`.trim();
        if (contactName) profileName = contactName;
      }
      
      profiles.push({
        acId: acId,
        externalId,
        name: profileName,
        email,
        phone,
        organizationId,
        identities,
        userType: "client",
        userFields: commonUserFields,
        zendeskPrimary,
        relationship,
        sourceField,
      });
    }
    
    // Also create profiles for contacts with unique phones but no email
    for (const contactPhone of contactsWithUniquePhones) {
      const contact = client.contacts?.[contactPhone.contactIndex];
      const contactDemo = contact?.demographics || {};
      const contactFirstName = contactDemo.first_name || "";
      const contactLastName = contactDemo.last_name || "";
      const contactName = `${contactFirstName} ${contactLastName}`.trim() || name; // Fallback to client name if contact name missing

      // Build external_id: client_xxxx_contact_relationship_contactIndex (contactIndex ensures uniqueness when multiple contacts share same relationship, e.g. multiple "MD")
      const sanitizedRelationship = contactPhone.relationship.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
      const externalId = acId ? `client_${acId}_contact_${sanitizedRelationship}_${contactPhone.contactIndex}` : null;
      
      // Organization ID will be set to null initially
      const organizationId = null;
      
      // Identities: empty (phone is already in phone field)
      const identities = [];
      
      profiles.push({
        acId: acId,
        externalId,
        name: contactName,
        email: null, // No email
        phone: contactPhone.phone,
        organizationId,
        identities,
        userType: "client",
        userFields: commonUserFields,
        zendeskPrimary,
        relationship: contactPhone.relationship,
        sourceField: `contacts[${contactPhone.contactIndex}].demographics.phone_main`,
      });
    }
    
    return profiles;
  } catch (error) {
    logger.error("Mapping error (client):", error);
    return [];
  }
}

export function normalizeCaregiverRecord(caregiver) {
  try {
    const demographics = caregiver.demographics || {};
    const firstName =
      caregiver.first_name ||
      caregiver.firstName ||
      demographics.first_name ||
      demographics.firstName ||
      "";
    const lastName =
      caregiver.last_name ||
      caregiver.lastName ||
      demographics.last_name ||
      demographics.lastName ||
      "";

    const status = caregiver.status || null;
    const groups = caregiver.groups || [];
    const departments = caregiver.departments || [];
    const tags = caregiver.tags || [];
    
    // For caregivers, tags can also be in demographics.tags as a string
    // Combine tags from both sources
    let allTags = Array.isArray(tags) ? [...tags] : [];
    const demographicsTags = demographics.tags;
    if (demographicsTags) {
      if (typeof demographicsTags === "string") {
        // If it's a string, split by comma and add to tags array
        const splitTags = demographicsTags.split(",").map((t) => t.trim()).filter(Boolean);
        allTags = [...allTags, ...splitTags];
      } else if (Array.isArray(demographicsTags)) {
        allTags = [...allTags, ...demographicsTags];
      }
    }

    const phoneDetails = collectCaregiverPhoneDetails(caregiver);
    const phones = phoneDetails.map((entry) => entry.normalized);
    const primaryPhone = phones.length > 0 ? phones[0] : null;

    const emailDetails = collectCaregiverEmailDetails(caregiver, demographics);
    const emails = emailDetails.map((entry) => entry.email);

    let primaryEmail = emails.length > 0 ? emails[0] : null;
    if (!primaryEmail) {
      const allFoundEmails = findEmailsDeep(caregiver);
      const uniqueValidEmails = [...new Set(allFoundEmails)];
      if (uniqueValidEmails.length > 0) {
        primaryEmail = uniqueValidEmails[0];
        uniqueValidEmails.forEach((email) => {
          if (!emails.includes(email)) {
            emails.push(email);
          }
        });
      }
    }

    if (!primaryEmail && emails.length > 0) {
      primaryEmail = emails[0];
    }

    const market = extractMarket(groups) || caregiver.market || caregiver.branch?.name || null;
    const marketValues = Array.isArray(market)
      ? market
      : typeof market === "string"
      ? market.split(",")
      : [];
    const marketArray = sanitizeMarketValues(marketValues);

    const departmentArray = departments
      .map((dept) => dept?.name || dept)
      .filter(Boolean)
      .map((name) => name.replace(/\s+/g, "_").toLowerCase());
    const departmentNames = departmentArray.length > 0 ? departmentArray : null;

    const zendeskPrimary = extractZendeskPrimary(allTags);
    const caregiverStatusValue = mapCaregiverStatus(status);

    const phoneIdentities = phones
      .slice(1)
      .map((phone) => ({ type: "phone", value: phone }))
      .filter((identity) => identity.value);
    const emailIdentities = emails
      .slice(1)
      .map((email) => ({ type: "email", value: email }))
      .filter((identity) => identity.value);

    const allEmails = [primaryEmail, ...emails.slice(1)].filter(Boolean);
    const organizationId = determineOrganizationId(allEmails, "caregiver");
    const externalId = caregiver.id ? `caregiver_${caregiver.id}` : null;

    return {
      acId: caregiver.id || null,
      externalId,
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail || null,
      phone: primaryPhone,
      organizationId,
      identities: [...phoneIdentities, ...emailIdentities],
      userType: "caregiver",
      userFields: buildCaregiverUserFields({
        marketArray,
        caregiverStatusValue,
        departmentNames,
      }),
      zendeskPrimary,
    };
  } catch (error) {
    logger.error("Mapping error (caregiver):", error);
    return null;
  }
}

