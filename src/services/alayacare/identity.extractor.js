import { logger } from "../../config/logger.js";
import { isValidEmail, normalizePhone } from "../../utils/validator.js";

const DISALLOWED_MARKETS = new Set(["alvitacare"]);
const PHONE_CAPTURE_REGEX =
  /(?:(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4})/g;

export function collectAllPhoneDetails(user = {}) {
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

  [
    ["user.phone", user.phone],
    ["user.phone_main", user.phone_main],
    ["user.phone_other", user.phone_other],
    ["user.phone_personal", user.phone_personal],
    ["user.phone_business", user.phone_business],
    ["user.phone_mobile", user.phone_mobile],
  ].forEach(([source, value]) => addValue(value, source));

  const demographics = user.demographics || {};
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

  if (Array.isArray(user.contacts)) {
    user.contacts.forEach((contact, idx) => {
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
        addValue(contactDemo[field], `contacts[${idx}].demographics.${field}`);
      });
    });
  }

  return Array.from(entries.values());
}

export function collectCaregiverPhoneDetails(user = {}) {
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

  [
    ["caregiver.phone", user.phone],
    ["caregiver.phone_main", user.phone_main],
    ["caregiver.phone_other", user.phone_other],
    ["caregiver.phone_personal", user.phone_personal],
  ].forEach(([source, value]) => addValue(value, source));

  const demographics = user.demographics || {};
  ["phone_main", "phone_other", "phone_personal"].forEach((field) => {
    addValue(demographics[field], `demographics.${field}`);
  });

  return Array.from(entries.values());
}

export function removeInternalEmails(emails = []) {
  const INTERNAL_DOMAINS = ["@alvitacare.com", "@alayacare.com"];
  return emails.filter(
    (email) => !INTERNAL_DOMAINS.some((domain) => email.toLowerCase().endsWith(domain))
  );
}

export function findEmailsDeep(value, depth = 0, maxDepth = 5) {
  if (depth > maxDepth || !value) return [];

  const found = [];
  const emailPattern = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;

  if (typeof value === "string") {
    const matches = value.match(emailPattern);
    if (matches) {
      matches.forEach((match) => {
        const candidate = match.trim().toLowerCase();
        if (isValidEmail(candidate)) {
          found.push(candidate);
        }
      });
    }
  } else if (Array.isArray(value)) {
    value.forEach((item) => found.push(...findEmailsDeep(item, depth + 1, maxDepth)));
  } else if (typeof value === "object") {
    Object.values(value).forEach((item) => found.push(...findEmailsDeep(item, depth + 1, maxDepth)));
  }

  return found;
}

export function collectAllEmailDetails(user, demographics = {}) {
  const entries = new Map();

  const pushEmail = (email, source) => {
    if (typeof email !== "string") return;

    let candidate = email.trim().toLowerCase();
    candidate = candidate.replace(/^<|>$/g, "");
    candidate = candidate.replace(/\/+$/, "");
    if (!candidate) return;

    if (!isValidEmail(candidate)) {
      logger.debug(`   ⏭️  Skipping invalid email from ${source}: ${candidate}`);
      return;
    }

    if (!entries.has(candidate)) {
      entries.set(candidate, { email: candidate, sources: [] });
    }
    const entry = entries.get(candidate);
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const emailPattern = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
    const matches = value.match(emailPattern);
    if (matches) {
      matches.forEach((email) => pushEmail(email, source));
    }
  };

  addValue(user.email, "user.email");
  addValue(user.invoice_email_recipients, "user.invoice_email_recipients");
  addValue(demographics.email, "demographics.email");
  addValue(demographics.invoice_email_recipients, "demographics.invoice_email_recipients");

  if (Array.isArray(user.contacts)) {
    user.contacts.forEach((contact, index) => {
      addValue(contact?.email, `contacts[${index}].email`);
      addValue(contact?.invoice_email_recipients, `contacts[${index}].invoice_email_recipients`);
      addValue(contact?.demographics?.email, `contacts[${index}].demographics.email`);
      addValue(
        contact?.demographics?.invoice_email_recipients,
        `contacts[${index}].demographics.invoice_email_recipients`
      );
    });
  }

  return Array.from(entries.values());
}

export function collectCaregiverEmailDetails(user, demographics = {}) {
  const entries = new Map();

  const pushEmail = (email, source) => {
    if (typeof email !== "string") return;

    let candidate = email.trim().toLowerCase();
    candidate = candidate.replace(/^<|>$/g, "");
    candidate = candidate.replace(/\/+$/, "");
    if (!candidate) return;

    if (!isValidEmail(candidate)) {
      logger.debug(`   ⏭️  Skipping invalid email from ${source}: ${candidate}`);
      return;
    }

    if (!entries.has(candidate)) {
      entries.set(candidate, { email: candidate, sources: [] });
    }
    const entry = entries.get(candidate);
    entry.sources.push(source);
  };

  const addValue = (value, source) => {
    if (typeof value !== "string") return;
    const emailPattern = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
    const matches = value.match(emailPattern);
    if (matches) {
      matches.forEach((email) => pushEmail(email, source));
    }
  };

  addValue(user.email, "caregiver.email");
  addValue(demographics.email, "demographics.email");

  return Array.from(entries.values());
}

export function sanitizeMarketValues(values = []) {
  if (!Array.isArray(values) || values.length === 0) {
    return null;
  }

  const normalized = values
    .map((value) =>
      typeof value === "string" ? value.trim().replace(/\s+/g, "_").toLowerCase() : null
    )
    .filter((value) => value && !DISALLOWED_MARKETS.has(value));

  const unique = Array.from(new Set(normalized));
  return unique.length > 0 ? unique : null;
}

