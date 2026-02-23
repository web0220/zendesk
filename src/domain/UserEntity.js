import { cleanEmail, isValidEmail, normalizePhone } from "../utils/validator.js";
import { convertDatabaseRowToZendeskUser } from "./user.db.mapper.js";

export class UserEntity {
  constructor({
    acId,
    externalId,
    name,
    email,
    phone,
    organizationId,
    userType,
    identities = [],
    userFields = {},
    zendeskPrimary = false,
    sharedPhoneNumber = null,
    relationship = null,
    sourceField = null,
  }) {
    this.acId = acId ? String(acId) : null;
    this.externalId = externalId || null;
    this.name = name ? name.trim() : null;
    this.email = email ? cleanEmail(email) : null;
    this.phone = normalizePhone(phone);
    this.organizationId = organizationId || null;
    this.userType = userType || userFields.type || null;
    this.userFields = { ...userFields, type: userType || userFields.type || null };
    this.zendeskPrimary = Boolean(zendeskPrimary);
    this.sharedPhoneNumber = sharedPhoneNumber || null;
    this.relationship = relationship || null;
    this.sourceField = sourceField || null;
    this.identities = [];
    this._identityIndex = new Set();

    identities.forEach((identity) => {
      if (identity) {
        this.addIdentity(identity.type, identity.value);
      }
    });
  }

  validate() {
    if (!this.name) return false;
    if (this.email && !isValidEmail(this.email)) {
      return false;
    }
    return true;
  }

  primaryEmail() {
    if (this.email) {
      return this.email;
    }
    const emailIdentity = this.identities.find((identity) => identity.type === "email");
    return emailIdentity?.value || null;
  }

  addIdentity(type, value) {
    if (!type || !value) return;

    const normalizedValue = value.trim();
    if (!normalizedValue) return;

    const normalizedType =
      type === "phone" ? "phone_number" : type === "phone_number" ? "phone_number" : type;

    const key = `${normalizedType}:${normalizedValue.toLowerCase()}`;
    if (this._identityIndex.has(key)) {
      return;
    }

    this._identityIndex.add(key);
    this.identities.push({ type: normalizedType, value: normalizedValue });
  }

  toZendeskPayload() {
    // For non-primary users with shared_phone_number, filter out phone identities
    // Phone numbers should only be in shared_phone_number field, not in identities
    let identities = this.identities.slice();
    if (!this.zendeskPrimary && this.sharedPhoneNumber) {
      identities = identities.filter(
        (identity) => identity.type !== "phone" && identity.type !== "phone_number"
      );
    }
    
    const payload = {
      external_id: this.externalId,
      ac_id: this.acId,
      name: this.name,
      organization_id: this.organizationId || undefined,
      identities,
      zendesk_primary: this.zendeskPrimary,
      user_fields: {
        ...this.userFields,
        shared_phone_number: this.zendeskPrimary ? null : this.sharedPhoneNumber,
        relationship: this.relationship || undefined,
      },
    };

    if (this.email) {
      payload.email = this.email;
    }
    if (this.phone) {
      payload.phone = this.phone;
    }

    Object.keys(payload).forEach((key) => {
      if (payload[key] === undefined) {
        delete payload[key];
      }
    });

    return payload;
  }

  static fromDbRow(row) {
    const zendeskUser = convertDatabaseRowToZendeskUser(row);
    if (!zendeskUser) return null;

    return new UserEntity({
      acId: zendeskUser.ac_id,
      externalId: zendeskUser.external_id,
      name: zendeskUser.name,
      email: zendeskUser.email,
      phone: zendeskUser.phone,
      organizationId: zendeskUser.organization_id,
      userType: zendeskUser.user_fields?.type,
      identities: zendeskUser.identities,
      userFields: zendeskUser.user_fields,
      zendeskPrimary: zendeskUser.zendesk_primary,
      sharedPhoneNumber: zendeskUser.user_fields?.shared_phone_number ?? null,
      relationship: row.client_relationship ?? row.relationship ?? null,
      sourceField: row.source_field || null,
    });
  }

  static fromAlayaCare(data) {
    if (!data) return null;
    return new UserEntity({
      acId: data.acId || data.id,
      externalId: data.externalId,
      name: data.name,
      email: data.email,
      phone: data.phone,
      organizationId: data.organizationId,
      userType: data.userType,
      identities: data.identities || [],
      userFields: data.userFields || {},
      zendeskPrimary: Boolean(data.zendeskPrimary),
      sharedPhoneNumber: data.sharedPhoneNumber || null,
      relationship: data.relationship || null,
      sourceField: data.sourceField || null,
    });
  }
}

