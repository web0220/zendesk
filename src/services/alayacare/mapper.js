import { logger } from "../../config/logger.js";
import { UserEntity } from "../../domain/UserEntity.js";
import { normalizeCaregiverRecord, normalizeClientRecord } from "./normalizer.js";
import { findEmailsDeep } from "./identity.extractor.js";

/**
 * Check for alvitacare.com emails in client data before mapping.
 * Alvitacare emails in invoice_email_recipients are expected and will be removed.
 * Alvitacare emails in other fields should be reported.
 */
function checkAlvitacareEmailsInClient(client) {
  const INTERNAL_DOMAINS = ["@alvitacare.com", "@alayacare.com"];
  const clientId = client.id || client.ac_id || "unknown";
  const clientName = client.demographics?.first_name && client.demographics?.last_name
    ? `${client.demographics.first_name} ${client.demographics.last_name}`
    : client.name || "unknown";
  
  // Helper to extract all emails from a string (handles comma-separated, space-separated, etc.)
  const extractEmailsFromString = (str) => {
    if (!str || typeof str !== "string") return [];
    const emailPattern = /\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b/g;
    return str.match(emailPattern) || [];
  };
  
  // Collect all invoice_email_recipients emails (expected locations)
  const invoiceEmailsSet = new Set();
  
  // Check main client invoice_email_recipients
  const mainInvoiceEmails = client.demographics?.invoice_email_recipients || client.invoice_email_recipients;
  if (mainInvoiceEmails) {
    extractEmailsFromString(String(mainInvoiceEmails)).forEach(email => {
      invoiceEmailsSet.add(email.toLowerCase());
    });
  }
  
  // Check contacts' invoice_email_recipients
  if (Array.isArray(client.contacts)) {
    for (const contact of client.contacts) {
      const contactInvoiceEmails = contact.demographics?.invoice_email_recipients || contact.invoice_email_recipients;
      if (contactInvoiceEmails) {
        extractEmailsFromString(String(contactInvoiceEmails)).forEach(email => {
          invoiceEmailsSet.add(email.toLowerCase());
        });
      }
    }
  }
  
  // Find all emails in the client object
  const allEmails = findEmailsDeep(client);
  
  // Check each email
  for (const email of allEmails) {
    const emailLower = email.toLowerCase();
    const isInternalEmail = INTERNAL_DOMAINS.some(domain => emailLower.endsWith(domain));
    
    if (isInternalEmail) {
      // If not found in invoice_email_recipients, this is unexpected and should be reported
      if (!invoiceEmailsSet.has(emailLower)) {
        logger.warn(
          `⚠️  Found alvitacare.com email in unexpected field for client ${clientId} (${clientName}): ${email}. ` +
          `This email will be removed during mapping, but please verify the source data.`
        );
      }
    }
  }
}

/**
 * Map client to UserEntity objects (one per unique email)
 * Returns array of UserEntity objects
 */
export function mapClientUser(rawClient) {
  // Check for alvitacare emails in unexpected fields before mapping
  checkAlvitacareEmailsInClient(rawClient);
  
  const normalizedProfiles = normalizeClientRecord(rawClient);
  if (!normalizedProfiles || normalizedProfiles.length === 0) return [];
  
  const entities = [];
  for (const normalized of normalizedProfiles) {
    const entity = UserEntity.fromAlayaCare(normalized);
    if (entity?.validate()) {
      entities.push(entity);
    }
  }
  
  return entities;
}

export function mapCaregiverUser(rawCaregiver) {
  const normalized = normalizeCaregiverRecord(rawCaregiver);
  if (!normalized) return null;
  const entity = UserEntity.fromAlayaCare(normalized);
  return entity?.validate() ? entity : null;
}

// Backwards compatibility helpers (return plain Zendesk payloads)
export function mapClientToZendesk(client) {
  const entities = mapClientUser(client);
  // Return first entity for backwards compatibility, or null if none
  return entities.length > 0 ? entities[0].toZendeskPayload() : null;
}

export function mapCaregiverToZendesk(caregiver) {
  const entity = mapCaregiverUser(caregiver);
  return entity ? entity.toZendeskPayload() : null;
}

