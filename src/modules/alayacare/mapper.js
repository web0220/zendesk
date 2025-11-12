import { logger } from "../../config/logger.js";

const PHONE_CAPTURE_REGEX =
  /(?:(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4})/g;

function normalizePhone(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");
  if (digits.length < 10) return null;
  return digits.startsWith("1") ? `+${digits}` : `+1${digits}`;
}

function uniqueBy(arr, keyFn) {
  const seen = new Set();
  return arr.filter((item) => {
    const key = keyFn(item);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function collectAllPhoneDetails(client = {}) {
  const entries = new Map();

  const pushMatch = (raw, normalized, source) => {
    if (!normalized) return;
    if (!entries.has(normalized)) {
      entries.set(normalized, { normalized, raw: [], sources: [] });
    }
    const entry = entries.get(normalized);
    if (!entry.raw.includes(raw)) {
      entry.raw.push(raw);
    }
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const stringValue = value.trim();
    if (!stringValue) return;
    const matches = stringValue.match(PHONE_CAPTURE_REGEX);
    if (!matches) return;
    matches.forEach((rawMatch) => {
      const raw = rawMatch.trim();
      const normalized = normalizePhone(rawMatch);
      if (!normalized) return;
      pushMatch(raw, normalized, source);
    });
  };

  // Top-level client fields
  [
    ["client.phone", client.phone],
    ["client.phone_main", client.phone_main],
    ["client.phone_other", client.phone_other],
    ["client.phone_personal", client.phone_personal],
    ["client.phone_business", client.phone_business],
    ["client.phone_mobile", client.phone_mobile],
  ].forEach(([source, value]) => addValue(value, source));

  const demographics = client.demographics || {};

  // Demographic fields
  [
    "phone_main",
    "phone_other",
    "billing_contact",
    "scheduling_contact",
    "emergency_contact",
    "phone_personal",
  ].forEach((field) => {
    addValue(demographics[field], `demographics.${field}`);
  });

  // Contacts
  if (Array.isArray(client.contacts)) {
    client.contacts.forEach((contact, idx) => {
      [
        ["contacts[].phone", contact?.phone],
        ["contacts[].phone_main", contact?.phone_main],
        ["contacts[].phone_other", contact?.phone_other],
        ["contacts[].phone_personal", contact?.phone_personal],
        ["contacts[].phone_business", contact?.phone_business],
        ["contacts[].phone_mobile", contact?.phone_mobile],
      ].forEach(([label, value]) =>
        addValue(value, `contacts[${idx}].${label.replace("contacts[].", "")}`)
      );

      const contactDemo = contact?.demographics || {};
      [
        "phone_main",
        "phone_other",
        "phone_personal",
        "phone_business",
        "phone_mobile",
      ].forEach((field) => {
        addValue(
          contactDemo[field],
          `contacts[${idx}].demographics.${field}`
        );
      });
    });
  }

  return Array.from(entries.values());
}

function collectAllEmailDetails(client, demographics = {}) {
  const entries = new Map();

  const pushEmail = (email, source) => {
    if (typeof email !== "string") return;
    const candidate = email.trim().toLowerCase();
    if (!candidate) return;
    if (!entries.has(candidate)) {
      entries.set(candidate, { email: candidate, sources: [] });
    }
    const entry = entries.get(candidate);
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const parts = value.split(/[;,]/);
    parts.forEach((part) => pushEmail(part, source));
  };

  addValue(client.email, "client.email");
  addValue(client.invoice_email_recipients, "client.invoice_email_recipients");
  addValue(demographics.email, "demographics.email");
  addValue(
    demographics.invoice_email_recipients,
    "demographics.invoice_email_recipients"
  );

  if (Array.isArray(client.contacts)) {
    client.contacts.forEach((contact, index) => {
      addValue(contact?.email, `contacts[${index}].email`);
      addValue(
        contact?.invoice_email_recipients,
        `contacts[${index}].invoice_email_recipients`
      );
      addValue(
        contact?.demographics?.email,
        `contacts[${index}].demographics.email`
      );
      addValue(
        contact?.demographics?.invoice_email_recipients,
        `contacts[${index}].demographics.invoice_email_recipients`
      );
    });
  }

  return Array.from(entries.values());
}

function extractMarket(groups = []) {
  const locGroups = groups.filter(
    (g) => typeof g?.name === "string" && g.name.trim().toUpperCase().startsWith("LOC")
  );
  if (locGroups.length === 0) return null;
  
  const markets = locGroups
    .map((loc) => {
      const match = loc.name.match(/^LOC\s*-\s*([^(]+)/i);
      return match ? match[1].trim() : loc.name;
    })
    .filter((m) => m);
  
  return markets.length > 0 ? markets.join(", ") : null;
}

function extractCoordinatorPod(groups = []) {
  const cscGroups = groups.filter(
    (g) => typeof g?.name === "string" && g.name.trim().toUpperCase().startsWith("CSC")
  );
  if (cscGroups.length === 0) return null;
  
  const pods = cscGroups
    .map((csc) => {
      const cleaned = csc.name.replace(/^CSC\s*-\s*/i, "").trim();
      return cleaned || csc.name;
    })
    .filter((p) => p);
  
  return pods.length > 0 ? pods.join(", ") : null;
}

function extractClinicalRNManager(groups = []) {
  const cmGroups = groups.filter(
    (g) => typeof g?.name === "string" && g.name.trim().toUpperCase().startsWith("CM")
  );
  if (cmGroups.length === 0) return null;
  
  const managers = cmGroups
    .map((cm) => {
      const cleaned = cm.name.replace(/^CM\s*-\s*/i, "").trim();
      return cleaned || cm.name;
    })
    .filter((m) => m);
  
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

export function mapClientToZendesk(client) {
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

    const emailDetails = collectAllEmailDetails(client, demographics);
    const emails = emailDetails.map((entry) => entry.email);
    const primaryEmail = emails.length > 0 ? emails[0] : null;

    const market =
      client.market ||
      client.branch?.name ||
      extractMarket(groups) ||
      null;
    const coordinator =
      client.coordinator ||
      extractCoordinatorPod(groups) ||
      null;
    const clinicalRNManager =
      client.clinical_rn_manager ||
      client.clinicalRNManager ||
      extractClinicalRNManager(groups) ||
      null;
    const caseRating =
      client.case_rating ||
      client.caseRating ||
      demographics.case_rating ||
      null;
    const salesRep =
      client.sales_rep ||
      client.salesRep ||
      extractSalesRep(tags) ||
      null;

    return {
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail,
      phone: primaryPhone,
      emails,
      phones,
      user_fields: {
        market: market,
        coordinator_pod: coordinator,
        clinical_rn_manager: clinicalRNManager,
        case_rating: caseRating,
        client_status: status,
        sales_rep: salesRep,
      },
    };
  } catch (err) {
    logger.error("Mapping error (client):", err);
    return null;
  }
}

function collectCaregiverPhoneDetails(caregiver = {}) {
  const entries = new Map();

  const pushMatch = (raw, normalized, source) => {
    if (!normalized) return;
    if (!entries.has(normalized)) {
      entries.set(normalized, { normalized, raw: [], sources: [] });
    }
    const entry = entries.get(normalized);
    if (!entry.raw.includes(raw)) {
      entry.raw.push(raw);
    }
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const stringValue = value.trim();
    if (!stringValue) return;
    const matches = stringValue.match(PHONE_CAPTURE_REGEX);
    if (!matches) return;
    matches.forEach((rawMatch) => {
      const raw = rawMatch.trim();
      const normalized = normalizePhone(rawMatch);
      if (!normalized) return;
      pushMatch(raw, normalized, source);
    });
  };

  // Top-level caregiver fields
  [
    ["caregiver.phone", caregiver.phone],
    ["caregiver.phone_main", caregiver.phone_main],
    ["caregiver.phone_other", caregiver.phone_other],
    ["caregiver.phone_personal", caregiver.phone_personal],
  ].forEach(([source, value]) => addValue(value, source));

  const demographics = caregiver.demographics || {};

  // Demographic fields
  [
    "phone_main",
    "phone_other",
    "phone_personal",
  ].forEach((field) => {
    addValue(demographics[field], `demographics.${field}`);
  });

  return Array.from(entries.values());
}

function collectCaregiverEmailDetails(caregiver, demographics = {}) {
  const entries = new Map();

  const pushEmail = (email, source) => {
    if (typeof email !== "string") return;
    const candidate = email.trim().toLowerCase();
    if (!candidate) return;
    if (!entries.has(candidate)) {
      entries.set(candidate, { email: candidate, sources: [] });
    }
    const entry = entries.get(candidate);
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const parts = value.split(/[;,]/);
    parts.forEach((part) => pushEmail(part, source));
  };

  addValue(caregiver.email, "caregiver.email");
  addValue(demographics.email, "demographics.email");

  return Array.from(entries.values());
}

export function mapCaregiverToZendesk(cg) {
  try {
    const demographics = cg.demographics || {};
    const firstName =
      cg.first_name ||
      cg.firstName ||
      demographics.first_name ||
      demographics.firstName ||
      "";
    const lastName =
      cg.last_name ||
      cg.lastName ||
      demographics.last_name ||
      demographics.lastName ||
      "";

    const status = cg.status || null;
    const groups = cg.groups || [];
    const departments = cg.departments || [];

    const phoneDetails = collectCaregiverPhoneDetails(cg);
    const phones = phoneDetails.map((entry) => entry.normalized);
    const primaryPhone = phones.length > 0 ? phones[0] : null;

    const emailDetails = collectCaregiverEmailDetails(cg, demographics);
    const emails = emailDetails.map((entry) => entry.email);
    const primaryEmail = emails.length > 0 ? emails[0] : null;

    const market =
      cg.market ||
      cg.branch?.name ||
      extractMarket(groups) ||
      null;

    // Get all department names
    const departmentNames = departments
      .map((dept) => dept?.name || dept)
      .filter((name) => name)
      .join(", ") || null;

    return {
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail,
      phone: primaryPhone,
      emails,
      phones,
      user_fields: {
        market: market,
        caregiver_status: status,
        department: departmentNames,
      },
    };
  } catch (err) {
    logger.error("Mapping error (caregiver):", err);
    return null;
  }
}

logger.info("🧠 AlayaCare data mapper ready");
