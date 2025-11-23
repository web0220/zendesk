import { fetchClients, fetchCaregivers } from "../src/modules/alayacare/service.js";
import { logger } from "../src/config/logger.js";

const DEFAULT_EMAIL = "joenlinv@aol.com";
const DEFAULT_ENTITY = "client";
const PAGE_SIZE = Number(process.env.ALAYACARE_PAGE_SIZE) || 100;

function normalizeEmail(email) {
  return (email || "").trim().toLowerCase();
}

function extractEmail(user) {
  return (
    user?.email ||
    user?.demographics?.email ||
    user?.contact?.email ||
    null
  );
}

async function searchCaregivers(targetEmail) {
  let page = 1;
  const matches = [];

  while (true) {
    const caregivers = await fetchCaregivers({
      page,
      count: PAGE_SIZE,
      includeDetails: true,
      fetchAll: false,
      status: "active",
    });

    if (!caregivers.length) {
      break;
    }

    caregivers.forEach((caregiver) => {
      const email = normalizeEmail(extractEmail(caregiver));
      if (email === targetEmail) {
        matches.push({ ...caregiver, entityType: "caregiver" });
      }
    });

    if (matches.length > 0 || caregivers.length < PAGE_SIZE) {
      break;
    }

    page += 1;
  }

  return matches;
}

async function searchClients(targetEmail) {
  let page = 1;
  const matches = [];

  while (true) {
    const clients = await fetchClients({
      page,
      count: PAGE_SIZE,
      includeDetails: true,
      fetchAll: false,
      status: "active",
    });

    if (!clients.length) {
      break;
    }

    clients.forEach((client) => {
      const email = normalizeEmail(extractEmail(client));
      if (email === targetEmail) {
        matches.push({ ...client, entityType: "client" });
      }
    });

    if (matches.length > 0 || clients.length < PAGE_SIZE) {
      break;
    }

    page += 1;
  }

  return matches;
}

async function findUserByEmail(targetEmailRaw, entityRaw) {
  const targetEmail = normalizeEmail(targetEmailRaw || DEFAULT_EMAIL);
  const entityType = (entityRaw || DEFAULT_ENTITY).toLowerCase();

  if (!targetEmail) {
    throw new Error("Please provide an email to search for.");
  }

  logger.info(`🔎 Searching for ${entityType} by email: ${targetEmail}`);

  const searchOrder =
    entityType === "both"
      ? ["caregiver", "client"]
      : [entityType];

  for (const type of searchOrder) {
    logger.info(`📂 Scanning ${type} records...`);
    const matches =
      type === "caregiver"
        ? await searchCaregivers(targetEmail)
        : await searchClients(targetEmail);

    if (matches.length > 0) {
      logger.info(`✅ Found ${matches.length} matching ${type}(s). Raw data:`);
      matches.forEach((match, index) => {
        logger.info(`\n#${index + 1} (${type})`);
        logger.info(JSON.stringify(match, null, 2));
      });
      return;
    }
  }

  logger.warn("❌ No user found with the provided email.");
}

const [, , emailArg, entityArg] = process.argv;

findUserByEmail(emailArg, entityArg).catch((error) => {
  logger.error("❌ Failed to search for user:", error.message);
  if (error.response) {
    logger.error("API Response Status:", error.response.status);
    logger.error("API Response Data:", JSON.stringify(error.response.data, null, 2));
  }
  process.exit(1);
});


