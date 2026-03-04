/**
 * AlayaCare Backend Orchestrator – fetch clients, caregivers, and optionally visits from AlayaCare and persist to local DB.
 */

import { logger } from "../config/logger.js";
import { fetchClients, fetchCaregivers } from "../services/alayacare/fetch.js";
import { fetchScheduledVisits } from "../services/alayacare/visit.api.js";
import { runWithLimit } from "../utils/concurrency.js";
import {
  upsertClientsBatch,
  upsertCaregiversBatch,
  upsertVisitsBatch,
} from "../infra/db.acBackend.repo.js";

const VISITS_MAX_CAREGIVERS = Number(process.env.ALAYACARE_BACKEND_VISITS_MAX_CAREGIVERS) || 0;

/**
 * Run AlayaCare Backend sync: persist clients, caregivers, and optionally visits to DB.
 * @returns {Promise<{ clients: number, caregivers: number, visits: number }>}
 */
export async function runAlayaCareBackend() {
  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("📦 AlayaCare Backend – syncing clients, caregivers, visits to DB");
  logger.info("═══════════════════════════════════════════════════════════");

  const summary = { clients: 0, caregivers: 0, visits: 0 };

  // 1. Fetch all clients with details and persist
  logger.info("📥 Fetching clients from AlayaCare...");
  const clients = await fetchClients({
    includeDetails: true,
    fetchAll: true,
  });
  if (clients.length > 0) {
    upsertClientsBatch(clients);
    summary.clients = clients.length;
    logger.info(`✅ Persisted ${summary.clients} clients to ac_client_info`);
  } else {
    logger.info("   No clients returned.");
  }

  // 2. Fetch all caregivers with details and persist
  logger.info("📥 Fetching caregivers from AlayaCare...");
  const caregivers = await fetchCaregivers({
    includeDetails: true,
    fetchAll: true,
  });
  if (caregivers.length > 0) {
    upsertCaregiversBatch(caregivers);
    summary.caregivers = caregivers.length;
    logger.info(`✅ Persisted ${summary.caregivers} caregivers to ac_caregiver_list`);
  } else {
    logger.info("   No caregivers returned.");
  }

  // 3. Optionally fetch scheduled visits for a bounded set of caregivers
  if (VISITS_MAX_CAREGIVERS > 0 && caregivers.length > 0) {
    const caregiversToFetch = caregivers.slice(0, VISITS_MAX_CAREGIVERS);
    logger.info(
      `📥 Fetching scheduled visits for ${caregiversToFetch.length} caregivers (next 7 days)...`
    );
    const visitTasks = caregiversToFetch.map((cg) => {
      const employeeId = cg.id ?? cg.source_ac_id;
      return async () => {
        try {
          return await fetchScheduledVisits(employeeId);
        } catch (err) {
          logger.warn(`   ⚠️ Visits for caregiver ${employeeId}: ${err.message}`);
          return [];
        }
      };
    });
    const visitArrays = await runWithLimit(visitTasks, 5);
    const allVisits = visitArrays.flat();
    const seen = new Set();
    const uniqueVisits = allVisits.filter((v) => {
      const key = v.id ?? `${v.alayacare_employee_id}_${v.alayacare_patient_id}_${v.start_at}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    if (uniqueVisits.length > 0) {
      upsertVisitsBatch(uniqueVisits);
      summary.visits = uniqueVisits.length;
      logger.info(`✅ Persisted ${summary.visits} visits to ac_visits`);
    }
  } else if (VISITS_MAX_CAREGIVERS === 0) {
    logger.info("   Visits skip (set ALAYACARE_BACKEND_VISITS_MAX_CAREGIVERS > 0 to enable).");
  }

  // Financial reports: no endpoint wired yet; discovery script can find one and we add later
  logger.info("   Financial reports: use discovery script to find endpoint, then add to orchestrator.");

  logger.info("═══════════════════════════════════════════════════════════");
  logger.info("✅ AlayaCare Backend sync completed");
  logger.info(`   Clients: ${summary.clients}, Caregivers: ${summary.caregivers}, Visits: ${summary.visits}`);
  logger.info("═══════════════════════════════════════════════════════════");

  return summary;
}
