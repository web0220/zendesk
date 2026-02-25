#!/usr/bin/env node
/**
 * Diagnose why AlayaCare user count (2943) can exceed unique ac_id in DB (2939).
 * Fetches clients + caregivers, runs same map/validate as sync, reports which
 * (type, id) produce zero valid entities (mapping or validation drop).
 *
 * Usage: node scripts/diagnose_missing_ac_ids.js
 * Requires: .env with AlayaCare credentials (same as sync).
 */

import { config } from "../src/config/index.js";
import { fetchClients, fetchCaregivers } from "../src/services/alayacare/fetch.js";
import { mapClientUser, mapCaregiverUser } from "../src/services/alayacare/mapper.js";

async function main() {
  await config();

  console.log("Fetching clients and caregivers from AlayaCare (active)...\n");

  const clients = await fetchClients({ status: "active" });
  const caregivers = await fetchCaregivers({ status: "active" });

  const clientIds = new Set(clients.map((c) => c.id).filter(Boolean));
  const caregiverIds = new Set(caregivers.map((c) => c.id).filter(Boolean));

  console.log(`Fetched: ${clients.length} clients, ${caregivers.length} caregivers`);
  console.log(`Unique client IDs: ${clientIds.size}, unique caregiver IDs: ${caregiverIds.size}`);

  const failedClients = [];
  const failedCaregivers = [];
  let clientEntities = 0;
  let caregiverEntities = 0;

  for (const client of clients) {
    const id = client.id ?? client.ac_id;
    const result = mapClientUser(client);
    const entities = Array.isArray(result) ? result : result ? [result] : [];
    const valid = entities.filter((e) => e && e.validate());
    if (valid.length === 0) {
      failedClients.push({ id, mapped: entities.length, name: client.demographics ? `${client.demographics.first_name || ""} ${client.demographics.last_name || ""}`.trim() : client.name || "—" });
    }
    clientEntities += valid.length;
  }

  for (const cg of caregivers) {
    const id = cg.id;
    const result = mapCaregiverUser(cg);
    const entities = result ? [result] : [];
    const valid = entities.filter((e) => e && e.validate());
    if (valid.length === 0) {
      failedCaregivers.push({ id, mapped: entities.length, name: cg.first_name && cg.last_name ? `${cg.first_name} ${cg.last_name}` : cg.name || "—" });
    }
    caregiverEntities += valid.length;
  }

  const totalValidEntities = clientEntities + caregiverEntities;
  const distinctSourceRecords = clients.length + caregivers.length;

  console.log(`\nAfter map + validate: ${clientEntities} client entities, ${caregiverEntities} caregiver entities (total ${totalValidEntities})`);
  console.log(`Source records (clients + caregivers): ${distinctSourceRecords}`);

  const failed = failedClients.length + failedCaregivers.length;
  console.log(`\nRecords that produced zero valid entities: ${failed}`);
  if (failed > 0) {
    if (failedClients.length > 0) {
      console.log("\nClients that produced 0 valid entities:");
      failedClients.forEach(({ id, mapped, name }) =>
        console.log(`  client id=${id} (mapped ${mapped}) ${name ? `"${name}"` : ""}`)
      );
    }
    if (failedCaregivers.length > 0) {
      console.log("\nCaregivers that produced 0 valid entities:");
      failedCaregivers.forEach(({ id, mapped, name }) =>
        console.log(`  caregiver id=${id} (mapped ${mapped}) ${name ? `"${name}"` : ""}`)
      );
    }
  }

  console.log("\nPossible reasons for 0 entities: missing/invalid name, invalid email, no emails/phones (clients), or normalizer/mapper error.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
