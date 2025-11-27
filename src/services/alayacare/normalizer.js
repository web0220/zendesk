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
}) {
  return {
    market: marketArray,
    coordinator_pod: coordinatorPodValue,
    clinical_rn_manager: clinicalRNManagerArray,
    case_rating: caseRatingValue,
    client_status: clientStatusValue,
    sales_rep: salesRepArray,
    type: "client",
  };
}

function buildCaregiverUserFields({ marketArray, caregiverStatusValue, departmentNames }) {
  return {
    market: marketArray,
    caregiver_status: caregiverStatusValue,
    department: departmentNames,
    type: "caregiver",
  };
}

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

    const phoneDetails = collectAllPhoneDetails(client);
    const phones = phoneDetails.map((entry) => entry.normalized);
    const primaryPhone = phones.length > 0 ? phones[0] : null;

    let emailDetails = collectAllEmailDetails(client, demographics);
    let emails = emailDetails.map((entry) => entry.email);
    emails = removeInternalEmails(emails);

    let primaryEmail = emails.length > 0 ? emails[0] : null;

    if (!primaryEmail) {
      const allFoundEmails = findEmailsDeep(client);
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

    const market = client.branch?.name || extractMarket(groups) || null;
    const coordinator = extractCoordinatorPod(groups) || null;
    const clinicalRNManager =
      client.clinical_rn_manager || client.clinicalRNManager || extractClinicalRNManager(groups);
    const caseRating = extractTierCaseRating(groups);
    const salesRep = extractSalesRep(tags) || null;
    const zendeskPrimary = extractZendeskPrimary(tags);

    const marketValues = Array.isArray(market)
      ? market
      : typeof market === "string"
      ? market.split(",")
      : [];
    const marketArray = sanitizeMarketValues(marketValues);

    const phoneIdentities = phones
      .slice(1)
      .map((phone) => ({ type: "phone", value: phone }))
      .filter((identity) => identity.value);
    const emailIdentities = emails
      .slice(1)
      .map((email) => ({ type: "email", value: email }))
      .filter((identity) => identity.value);

    const coordinatorPodValue = coordinator
      ? coordinator.split(",")[0].trim().replace(/\s+/g, "_").toLowerCase()
      : null;
    const caseRatingValue = caseRating ? caseRating.replace(/\s+/g, "_").toLowerCase() : null;
    const clientStatusValue = status ? `cl_${status.replace(/\s+/g, "_").toLowerCase()}` : null;

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

    const allEmails = [primaryEmail, ...emails.slice(1)].filter(Boolean);
    const organizationId = determineOrganizationId(allEmails, "client");
    const externalId = client.id ? `client_${client.id}` : null;

    return {
      acId: client.id || null,
      externalId,
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail || null,
      phone: primaryPhone,
      organizationId,
      identities: [...phoneIdentities, ...emailIdentities],
      userType: "client",
      userFields: buildClientUserFields({
        coordinatorPodValue,
        caseRatingValue,
        clientStatusValue,
        clinicalRNManagerArray,
        salesRepArray,
        marketArray,
      }),
      zendeskPrimary,
    };
  } catch (error) {
    logger.error("Mapping error (client):", error);
    return null;
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

    const zendeskPrimary = extractZendeskPrimary(tags);
    const caregiverStatusValue = status ? `cg_${status.replace(/\s+/g, "_").toLowerCase()}` : null;

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

