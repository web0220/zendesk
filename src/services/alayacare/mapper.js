import { UserEntity } from "../../domain/UserEntity.js";
import { normalizeCaregiverRecord, normalizeClientRecord } from "./normalizer.js";

export function mapClientUser(rawClient) {
  const normalized = normalizeClientRecord(rawClient);
  if (!normalized) return null;
  const entity = UserEntity.fromAlayaCare(normalized);
  return entity?.validate() ? entity : null;
}

export function mapCaregiverUser(rawCaregiver) {
  const normalized = normalizeCaregiverRecord(rawCaregiver);
  if (!normalized) return null;
  const entity = UserEntity.fromAlayaCare(normalized);
  return entity?.validate() ? entity : null;
}

// Backwards compatibility helpers (return plain Zendesk payloads)
export function mapClientToZendesk(client) {
  const entity = mapClientUser(client);
  return entity ? entity.toZendeskPayload() : null;
}

export function mapCaregiverToZendesk(caregiver) {
  const entity = mapCaregiverUser(caregiver);
  return entity ? entity.toZendeskPayload() : null;
}

