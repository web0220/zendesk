import { logger } from "../config/logger.js";
import { isAlvitaCompanyMember } from "../utils/constants.js";

export function determineUserTypeForStorage(mappedData, fields) {
  return (
    fields.user_type ||
    mappedData.user_fields?.type ||
    mappedData.user_type ||
    null
  );
}

export function buildAcKeyFromParts(sourceAcId, userType) {
  const typeSlug = (userType || "unknown").toLowerCase();
  return `${typeSlug}_${sourceAcId}`;
}

export function buildStorageKeys(mappedData, fields) {
  const sourceAcId = String(mappedData.ac_id);
  const userType = determineUserTypeForStorage(mappedData, fields);
  const acKey = buildAcKeyFromParts(sourceAcId, userType);
  return { acKey, sourceAcId, userType };
}

export function normalizeAcLookupKey(ac_id, userType) {
  if (ac_id === undefined || ac_id === null) return null;
  const raw = String(ac_id);
  if (raw.includes("_")) {
    const parts = raw.split("_");
    const typePart = parts.shift();
    const sourcePart = parts.join("_");
    return buildAcKeyFromParts(sourcePart, typePart);
  }
  if (userType) {
    return buildAcKeyFromParts(raw, userType);
  }
  return null;
}

export function extractMappedFields(mappedData = {}) {
  if (!mappedData) return {};

  const userFields = mappedData.user_fields || {};
  const userType = userFields.type || null;
  const organizationId = mappedData.organization_id || null;
  const isCompanyMember = isAlvitaCompanyMember(organizationId);

  const toJsonString = (value) => {
    if (value === null || value === undefined) return null;
    if (Array.isArray(value) || typeof value === "object") {
      return JSON.stringify(value);
    }
    return String(value);
  };

  const extracted = {
    name: mappedData.name || null,
    email: mappedData.email || null,
    phone: mappedData.phone || null,
    organization_id: organizationId,
    user_type: userType,
    identities: toJsonString(mappedData.identities),
    market: toJsonString(userFields.market),
    zendesk_primary: mappedData.zendesk_primary === true ? 1 : 0,
  };

  if (userType === "client") {
    extracted.coordinator_pod = userFields.coordinator_pod || null;
    extracted.case_rating = userFields.case_rating || null;
    // Store actual status in database for tracking (even for company members)
    // We'll filter it out when sending to Zendesk, but we need to track changes
    extracted.client_status = userFields.userstatus || null;
    extracted.clinical_rn_manager = toJsonString(userFields.clinical_rn_manager);
    extracted.sales_rep = toJsonString(userFields.sales_rep);
    extracted.scheduling_preferences = userFields.scheduling_preferences || null;
  }

  if (userType === "caregiver") {
    // Store actual status in database for tracking (even for company members)
    // We'll filter it out when sending to Zendesk, but we need to track changes
    extracted.caregiver_status = userFields.userstatus || null;
    extracted.department = toJsonString(userFields.department);
  }

  return extracted;
}

export function hydrateMapping(row) {
  if (!row) return row;

  const jsonFields = [
    "identities",
    "market",
    "clinical_rn_manager",
    "sales_rep",
    "department",
  ];

  jsonFields.forEach((field) => {
    if (row[field] && typeof row[field] === "string") {
      try {
        row[field] = JSON.parse(row[field]);
      } catch (err) {
        logger.debug(`⚠️ Failed to parse ${field} JSON, keeping as string`);
      }
    }
  });

  return row;
}

export function convertDatabaseRowToZendeskUser(row) {
  if (!row) return null;

  const userFields = {};
  const isCompanyMember = isAlvitaCompanyMember(row.organization_id);

  if (row.user_type) {
    userFields.type = row.user_type;
  }

  if (row.user_type === "client") {
    if (row.coordinator_pod) userFields.coordinator_pod = row.coordinator_pod;
    if (row.case_rating) userFields.case_rating = row.case_rating;
    if (row.client_status && !isCompanyMember) {
      userFields.userstatus = row.client_status;
    }
    if (row.clinical_rn_manager) userFields.clinical_rn_manager = row.clinical_rn_manager;
    if (row.sales_rep) userFields.sales_rep = row.sales_rep;
    if (row.scheduling_preferences) userFields.scheduling_preferences = row.scheduling_preferences;
  }

  if (row.user_type === "caregiver") {
    if (row.caregiver_status && !isCompanyMember) {
      userFields.userstatus = row.caregiver_status;
    }
    if (row.department) userFields.department = row.department;
  }

  if (row.market) userFields.market = row.market;

  if (row.zendesk_primary === 1 || row.zendesk_primary === true) {
    userFields.shared_phone_number = null;
  } else if (row.shared_phone_number !== null && row.shared_phone_number !== undefined) {
    userFields.shared_phone_number = row.shared_phone_number;
  } else {
    userFields.shared_phone_number = null;
  }

  let identities = [];
  if (row.identities) {
    if (Array.isArray(row.identities)) {
      identities = row.identities;
    } else if (typeof row.identities === "string") {
      try {
        identities = JSON.parse(row.identities);
      } catch (err) {
        logger.debug(`⚠️ Failed to parse identities JSON for ac_id=${row.ac_id}`);
      }
    }
  }
  
  // For non-primary users with shared_phone_number, filter out phone identities
  // Phone numbers should only be in shared_phone_number field, not in identities
  const isPrimary = row.zendesk_primary === 1 || row.zendesk_primary === true;
  if (!isPrimary && row.shared_phone_number) {
    identities = identities.filter(
      (identity) => identity.type !== "phone" && identity.type !== "phone_number"
    );
  }

  const zendeskUser = {
    external_id: row.external_id,
    ac_id: row.source_ac_id || row.ac_id,
    name: row.name,
    email: row.email || undefined,
    phone: row.phone || undefined,
    organization_id: row.organization_id || undefined,
    identities,
    zendesk_primary: row.zendesk_primary === 1 || row.zendesk_primary === true,
    user_fields: userFields,
  };

  Object.keys(zendeskUser).forEach((key) => {
    if (zendeskUser[key] === undefined) {
      delete zendeskUser[key];
    }
  });

  return zendeskUser;
}

