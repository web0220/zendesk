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
    // Split on semicolons, commas, and whitespace (spaces, tabs, newlines)
    // This handles cases like "email1@example.com email2@example.com"
    const parts = value.split(/[;,\s]+/).filter(part => part.trim());
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

    // Convert market to array: split by comma, trim, lowercase
    const marketArray = market
      ? market
          .split(",")
          .map((m) => m.trim().toLowerCase())
          .filter((m) => m)
      : null;

    // Convert secondary phones (all phones except primary) to identities array
    const phoneIdentities = phones.length > 1 
      ? phones.slice(1).map((phone) => ({
          type: "phone",
          value: phone,
        }))
      : [];

    // Add secondary emails (all emails except primary) to identities array
    const emailIdentities = emails.length > 1 
      ? emails.slice(1).map((email) => ({
          type: "email",
          value: email,
        }))
      : [];

    // Combine phone and email identities
    const identities = [...phoneIdentities, ...emailIdentities];

    // DROP-DOWN fields (single value - string)
    // coordinator_pod, case_rating, client_status
    const coordinatorPodValue = coordinator
      ? coordinator.split(",")[0].trim().replace(/\s+/g, "_").toLowerCase()
      : null;
    
    const caseRatingValue = caseRating 
      ? caseRating.replace(/\s+/g, "_").toLowerCase() 
      : null;
    
    const clientStatusValue = status 
      ? `cl_${status.replace(/\s+/g, "_").toLowerCase()}` 
      : null;

    // MULTISELECT fields (arrays)
    // clinical_rn_manager, market, sales_rep
    const clinicalRNManagerArray = clinicalRNManager
      ? clinicalRNManager.split(",").map((m) => m.trim().replace(/\s+/g, "_").toLowerCase()).filter((m) => m)
      : null;
    
    const salesRepArray = salesRep
      ? salesRep.split(",").map((s) => s.trim().replace(/\s+/g, "_").toLowerCase()).filter((s) => s)
      : null;

    // Organization ID for clients
    const organizationId = 42824772337179;

    // Format external_id as AC + zero-padded ID (e.g., AC000000417)
    const externalId = client.id ? `AC${String(client.id).padStart(9, '0')}` : null;

    return {
      external_id: externalId,
      ac_id: client.id || null,
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail,
      phone: primaryPhone,
      organization_id: organizationId,
      identities,
      user_fields: {
        market: marketArray,
        coordinator_pod: coordinatorPodValue,
        clinical_rn_manager: clinicalRNManagerArray,
        case_rating: caseRatingValue,
        client_status: clientStatusValue,
        sales_rep: salesRepArray,
        type: "client",
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
    // Split on semicolons, commas, and whitespace (spaces, tabs, newlines)
    // This handles cases like "email1@example.com email2@example.com"
    const parts = value.split(/[;,\s]+/).filter(part => part.trim());
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

    // MULTISELECT fields (arrays)
    // market, department
    const marketArray = market
      ? market
          .split(",")
          .map((m) => m.trim().toLowerCase())
          .filter((m) => m)
      : null;

    const departmentArray = departments
      .map((dept) => dept?.name || dept)
      .filter((name) => name)
      .map((name) => name.replace(/\s+/g, "_").toLowerCase());
    const departmentNames = departmentArray.length > 0 ? departmentArray : null;

    // DROP-DOWN fields (single value - string)
    // caregiver_status
    const caregiverStatusValue = status 
      ? `cg_${status.replace(/\s+/g, "_").toLowerCase()}` 
      : null;

    // Convert secondary phones (all phones except primary) to identities array
    const phoneIdentities = phones.length > 1 
      ? phones.slice(1).map((phone) => ({
          type: "phone",
          value: phone,
        }))
      : [];

    // Add secondary emails (all emails except primary) to identities array
    const emailIdentities = emails.length > 1 
      ? emails.slice(1).map((email) => ({
          type: "email",
          value: email,
        }))
      : [];

    // Combine phone and email identities
    const identities = [...phoneIdentities, ...emailIdentities];

    // Determine organization ID for caregivers
    // Check if any email (primary or secondary) contains @alvitacare.com domain
    const allEmails = [primaryEmail, ...emails.slice(1)].filter(Boolean);
    const isAlvitacareMember = allEmails.some(email => 
      typeof email === "string" && email.toLowerCase().includes("@alvitacare.com")
    );
    
    const organizationId = isAlvitacareMember 
      ? 40994316312731  // AlvitaCare members
      : 43279021546651; // Regular caregivers

    // Format external_id as AC + zero-padded ID (e.g., AC000000417)
    const externalId = cg.id ? `AC${String(cg.id).padStart(9, '0')}` : null;

    return {
      external_id: externalId,
      ac_id: cg.id || null,
      name: `${firstName} ${lastName}`.trim() || null,
      email: primaryEmail,
      phone: primaryPhone,
      organization_id: organizationId,
      identities,
      user_fields: {
        market: marketArray,
        caregiver_status: caregiverStatusValue,
        department: departmentNames,
        type: "caregiver",
      },
    };
  } catch (err) {
    logger.error("Mapping error (caregiver):", err);
    return null;
  }
}

logger.info("🧠 AlayaCare data mapper ready");
