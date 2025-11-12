import { logger } from "../../config/logger.js";

function normalizePhone(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, "");
  return digits.startsWith("1") ? `+${digits}` : `+1${digits}`;
}

export function mapClientToZendesk(client) {
  try {
    // Handle both snake_case (API) and camelCase (if transformed)
    const firstName = client.first_name || client.firstName || "";
    const lastName = client.last_name || client.lastName || "";
    const email = client.email || null;
    const phone = client.phone_main || client.phoneMain || client.phone || null;
    const status = client.status || null;
    const market = client.branch?.name || client.market || null;
    const coordinator = client.coordinator || null;
    const caseRating = client.case_rating || client.caseRating || null;
    const salesRep = client.sales_rep || client.salesRep || null;

    return {
      name: `${firstName} ${lastName}`.trim() || null,
      email: email,
      phone: normalizePhone(phone),
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
