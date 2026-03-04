/**
 * AlayaCare Backend repository – persist structured client/caregiver/visit data and document metadata (files on disk).
 */

import fs from "fs";
import path from "path";
import { getDb } from "./db.api.js";
import { logger } from "../config/logger.js";

const AC_BACKUP_BASE = path.resolve("data", "alayacare_backup");
const DOCUMENTS_DIR = path.join(AC_BACKUP_BASE, "documents");

function now() {
  return new Date().toISOString();
}

function safe(str) {
  if (str == null) return "";
  return String(str).replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 200);
}

function pickName(c) {
  const d = c.demographics || {};
  const first = d.first_name ?? d.firstName ?? c.first_name ?? "";
  const last = d.last_name ?? d.lastName ?? c.last_name ?? "";
  if (first || last) return [first, last].filter(Boolean).join(" ").trim();
  return c.name ?? "";
}

function pickEmail(c) {
  const d = c.demographics || {};
  const e = d.email ?? c.email ?? (Array.isArray(c.emails) && c.emails[0]) ?? "";
  if (e) return e;
  const contacts = c.contacts || [];
  for (const contact of contacts) {
    const ce = contact?.demographics?.email ?? contact?.email;
    if (ce) return ce;
  }
  return "";
}

function pickPhone(c) {
  const d = c.demographics || {};
  const p = d.phone_main ?? d.phone ?? c.phone ?? c.phone_main ?? "";
  if (p) return p;
  const contacts = c.contacts || [];
  for (const contact of contacts) {
    const cp = contact?.demographics?.phone_main ?? contact?.phone;
    if (cp) return cp;
  }
  return "";
}

/**
 * Batch upsert client snapshots with structured columns + full payload.
 * @param {Array<{ id, status, demographics, contacts, groups, tags, ... }>} clients
 */
export function upsertClientsBatch(clients) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO ac_client_info (
      ac_id, name, status, email, phone,
      demographics_json, contacts_json, groups_json, tags_json, payload, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ac_id) DO UPDATE SET
      name = excluded.name,
      status = excluded.status,
      email = excluded.email,
      phone = excluded.phone,
      demographics_json = excluded.demographics_json,
      contacts_json = excluded.contacts_json,
      groups_json = excluded.groups_json,
      tags_json = excluded.tags_json,
      payload = excluded.payload,
      updated_at = excluded.updated_at
  `);
  const run = db.transaction((list) => {
    for (const c of list) {
      const acId = String(c.id ?? c.ac_id ?? "");
      if (!acId) continue;
      const name = pickName(c);
      const status = c.status ?? "";
      const email = pickEmail(c);
      const phone = pickPhone(c);
      const demographics_json = JSON.stringify(c.demographics ?? {});
      const contacts_json = JSON.stringify(c.contacts ?? []);
      const groups_json = JSON.stringify(c.groups ?? []);
      const tags_json = JSON.stringify(c.tags ?? []);
      const payload = JSON.stringify(c);
      stmt.run(acId, name, status, email, phone, demographics_json, contacts_json, groups_json, tags_json, payload, now());
    }
  });
  run(clients);
  logger.debug(`ac_client_info: upserted ${clients.length} clients (structured + payload)`);
}

/**
 * Batch upsert caregiver snapshots with structured columns + full payload.
 * @param {Array<{ id, status, demographics, departments, groups, tags, ... }>} caregivers
 */
export function upsertCaregiversBatch(caregivers) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO ac_caregiver_list (
      ac_id, name, status, email, phone, department,
      demographics_json, groups_json, tags_json, payload, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ac_id) DO UPDATE SET
      name = excluded.name,
      status = excluded.status,
      email = excluded.email,
      phone = excluded.phone,
      department = excluded.department,
      demographics_json = excluded.demographics_json,
      groups_json = excluded.groups_json,
      tags_json = excluded.tags_json,
      payload = excluded.payload,
      updated_at = excluded.updated_at
  `);
  const run = db.transaction((list) => {
    for (const g of list) {
      const acId = String(g.id ?? g.ac_id ?? "");
      if (!acId) continue;
      const name = pickName(g);
      const status = g.status ?? "";
      const email = pickEmail(g);
      const phone = pickPhone(g);
      const department = (Array.isArray(g.departments) ? g.departments.map((d) => d?.name ?? d).filter(Boolean).join(", ") : g.department) ?? "";
      const demographics_json = JSON.stringify(g.demographics ?? {});
      const groups_json = JSON.stringify(g.groups ?? []);
      const tags_json = JSON.stringify(g.tags ?? []);
      const payload = JSON.stringify(g);
      stmt.run(acId, name, status, email, phone, department, demographics_json, groups_json, tags_json, payload, now());
    }
  });
  run(caregivers);
  logger.debug(`ac_caregiver_list: upserted ${caregivers.length} caregivers (structured + payload)`);
}

export function upsertClientInfo(acId, payload) {
  if (!acId) return;
  const c = typeof payload === "object" ? payload : {};
  if (typeof payload === "string") try { Object.assign(c, JSON.parse(payload)); } catch (_) {}
  upsertClientsBatch([{ ...c, id: acId, ac_id: acId }]);
}

export function upsertCaregiverList(acId, payload) {
  if (!acId) return;
  const g = typeof payload === "object" ? payload : {};
  if (typeof payload === "string") try { Object.assign(g, JSON.parse(payload)); } catch (_) {}
  upsertCaregiversBatch([{ ...g, id: acId, ac_id: acId }]);
}

/**
 * Upsert a financial report row by report_key.
 */
export function upsertFinancialReport(reportKey, payload) {
  if (!reportKey) return;
  const db = getDb();
  const json = typeof payload === "string" ? payload : JSON.stringify(payload);
  const stmt = db.prepare(`
    INSERT INTO ac_financial_reports (report_key, payload, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(report_key) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
  `);
  stmt.run(reportKey, json, now());
}

/**
 * Batch upsert visits with structured columns (employee_id, client_id, start_at, end_at, status, cancelled) + payload.
 */
export function upsertVisitsBatch(visits) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO ac_visits (id, employee_id, client_id, start_at, end_at, status, cancelled, payload, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      employee_id = excluded.employee_id,
      client_id = excluded.client_id,
      start_at = excluded.start_at,
      end_at = excluded.end_at,
      status = excluded.status,
      cancelled = excluded.cancelled,
      payload = excluded.payload,
      updated_at = excluded.updated_at
  `);
  const run = db.transaction((list) => {
    for (const v of list) {
      const id =
        v.id ??
        (v.alayacare_employee_id != null && (v.alayacare_patient_id != null || v.client_id != null) && v.start_at
          ? `${v.alayacare_employee_id}_${v.alayacare_patient_id ?? v.client_id}_${v.start_at}`
          : null);
      if (!id) continue;
      const employee_id = v.alayacare_employee_id ?? v.employee_id ?? null;
      const client_id = v.alayacare_patient_id ?? v.client_id ?? v.patient_id ?? null;
      const start_at = v.start_at ?? null;
      const end_at = v.end_at ?? null;
      const status = v.status ?? null;
      const cancelled = v.cancelled === true || v.cancelled === 1 ? 1 : 0;
      const payload = JSON.stringify(v);
      stmt.run(String(id), employee_id, client_id, start_at, end_at, status, cancelled, payload, now());
    }
  });
  run(visits);
  logger.debug(`ac_visits: upserted ${visits.length} visits (structured + payload)`);
}

export function upsertVisit(visitId, payload) {
  if (!visitId) return;
  const v = typeof payload === "object" ? payload : {};
  if (typeof payload === "string") try { Object.assign(v, JSON.parse(payload)); } catch (_) {}
  upsertVisitsBatch([{ ...v, id: visitId }]);
}

// -----------------------------------------------------------------------------
// Documents: store files on disk, metadata in ac_documents
// -----------------------------------------------------------------------------

/**
 * Ensure documents base dir and entity subdir exist.
 * @param {string} entityType - e.g. "client", "caregiver", "visit"
 * @param {string} entityId - AlayaCare entity id
 * @returns {string} Dir path
 */
export function getDocumentDir(entityType, entityId) {
  const dir = path.join(DOCUMENTS_DIR, safe(entityType), safe(entityId));
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

/**
 * Save a document: write content to a file under data/alayacare_backup/documents/{entity_type}/{entity_id}/ and record in ac_documents.
 * @param {string} entityType - e.g. "client", "caregiver"
 * @param {string} entityId - Entity id
 * @param {Buffer|Uint8Array} content - File content
 * @param {string} originalName - Original filename (used for extension and storage)
 * @param {string} [fileType] - MIME or extension, e.g. "application/pdf", "pdf"
 * @returns {{ filePath: string, id: number }}
 */
export function saveDocument(entityType, entityId, content, originalName, fileType = null) {
  const dir = getDocumentDir(entityType, entityId);
  const ext = path.extname(originalName || "") || (fileType && fileType.startsWith("application/") ? ".bin" : "") || ".bin";
  const base = safe(path.basename(originalName || "document", ext)) || "document";
  const fileName = `${base}_${Date.now()}${ext}`;
  const filePath = path.join(dir, fileName);
  fs.writeFileSync(filePath, content);
  const fileSize = content.length;
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO ac_documents (entity_type, entity_id, file_path, file_type, original_name, file_size, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const relPath = path.relative(path.resolve("."), filePath);
  stmt.run(entityType, entityId, relPath, fileType || ext, originalName || fileName, fileSize, now());
  const row = db.prepare("SELECT last_insert_rowid() as id").get();
  logger.debug(`ac_documents: saved ${relPath} (id=${row.id})`);
  return { filePath: relPath, id: row.id };
}

/**
 * Save a text snippet as a file (e.g. confirmation email body, notes) and record in ac_documents.
 * @param {string} entityType - e.g. "client", "visit"
 * @param {string} entityId - Entity id
 * @param {string} text - Plain text or HTML content
 * @param {string} originalName - e.g. "confirmation_email.txt", "notes.html"
 * @param {string} [mimeType] - e.g. "text/plain", "text/html"
 */
export function saveDocumentText(entityType, entityId, text, originalName, mimeType = "text/plain") {
  const buf = Buffer.from(String(text), "utf-8");
  return saveDocument(entityType, entityId, buf, originalName, mimeType);
}
