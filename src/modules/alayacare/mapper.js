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
  const loc = groups.find(
    (g) => typeof g?.name === "string" && g.name.trim().toUpperCase().startsWith("LOC")
  );
  if (!loc?.name) return null;
  const match = loc.name.match(/^LOC\s*-\s*([^(]+)/i);
  return match ? match[1].trim() : loc.name;
}

function extractCoordinatorPod(groups = []) {
  const pod = groups.find(
    (g) => typeof g?.name === "string" && g.name.trim().toUpperCase().startsWith("CSC")
  );
  if (!pod?.name) return null;
  return pod.name.replace(/^CSC\s*-\s*/i, "").trim();
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
      email_details: emailDetails,
      phones,
      phone_details: phoneDetails,
      user_fields: {
        market: market,
        coordinator_pod: coordinator,
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

export function mapCaregiverToZendesk(cg) {
  try {
    // Handle both snake_case (API) and camelCase (if transformed)
    const firstName = cg.first_name || cg.firstName || "";
    const lastName = cg.last_name || cg.lastName || "";
    const email = cg.email || null;
    const phone = cg.phone_main || cg.phoneMain || cg.phone || null;
    const status = cg.status || null;
    const market = cg.branch?.name || cg.market || null;
    const department = cg.departments?.[0]?.name || cg.department || null;

    return {
      name: `${firstName} ${lastName}`.trim() || null,
      email: email,
      phone: normalizePhone(phone),
      user_fields: {
        market: market,
        caregiver_status: status,
        department: department,
      },
    };
  } catch (err) {
    logger.error("Mapping error (caregiver):", err);
    return null;
  }
}

logger.info("🧠 AlayaCare data mapper ready");
