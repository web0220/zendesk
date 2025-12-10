// deleteUsersByOrganization.js
import axios from "axios";
import { zendeskLimiter } from "../src/utils/limiter.js";
import { logger } from "../src/config/logger.js";

const ZENDESK_SUBDOMAIN = "alvitacare";
const ZENDESK_EMAIL = "paula.cheng@alvitacare.com";
const ZENDESK_API_TOKEN = "9oFYAOqNqcc3QsXP2J9gIetZGHU6dcEzmeENUwpS";

// Organization IDs to delete users from
const ORGANIZATION_IDS_TO_DELETE = ["42824772337179", "43279021546651"];
// Organization ID to keep (alvita member)
const ALVITA_MEMBER_ORG_ID = "40994316312731";
// Alvita member external IDs to delete (excluding paula: caregiver_1536 and kennedy: caregiver_6213)
const ALVITA_MEMBER_EXTERNAL_IDS_TO_DELETE = [
  "caregiver_102", "caregiver_596", "caregiver_602", "caregiver_603", "caregiver_605",
  "caregiver_610", "caregiver_611", "caregiver_613", "caregiver_618", "caregiver_619",
  "caregiver_626", "caregiver_628", "caregiver_698", "caregiver_791", "caregiver_810",
  "caregiver_837", "caregiver_843", "caregiver_875", "caregiver_882", "caregiver_911",
  "caregiver_928", "caregiver_965", "caregiver_966", "caregiver_1008", "caregiver_1058",
  "caregiver_1074", "caregiver_1151", "caregiver_1180", "caregiver_1293", "caregiver_1533",
  "caregiver_1545", "caregiver_1548", "caregiver_1707", "caregiver_1708", "caregiver_1729",
  "caregiver_1744", "caregiver_1920", "caregiver_2081", "caregiver_2082", "caregiver_2125",
  "caregiver_2140", "caregiver_2144", "caregiver_2170", "caregiver_2184", "caregiver_2191",
  "caregiver_2233", "caregiver_2236", "caregiver_2238", "caregiver_2388", "caregiver_2389",
  "caregiver_2423", "caregiver_2424", "caregiver_2501", "caregiver_2513", "caregiver_2552",
  "caregiver_2600", "caregiver_2663", "caregiver_2683", "caregiver_2705", "caregiver_2736",
  "caregiver_2738", "caregiver_2739", "caregiver_2743", "caregiver_2745", "caregiver_2746",
  "caregiver_2747", "caregiver_2748", "caregiver_2777", "caregiver_2828", "caregiver_2848",
  "caregiver_2892", "caregiver_2926", "caregiver_2928", "caregiver_2929", "caregiver_2930",
  "caregiver_2931", "caregiver_2949", "caregiver_3229", "caregiver_3231", "caregiver_3431",
  "caregiver_3455", "caregiver_3497", "caregiver_3499", "caregiver_3816", "caregiver_3817",
  "caregiver_3929", "caregiver_3962", "caregiver_4052", "caregiver_4086", "caregiver_4229",
  "caregiver_4321", "caregiver_4407", "caregiver_4450", "caregiver_4467", "caregiver_4493",
  "caregiver_4495", "caregiver_4523", "caregiver_4624", "caregiver_4666", "caregiver_4769",
  "caregiver_4788", "caregiver_4839", "caregiver_4840", "caregiver_4844", "caregiver_5015",
  "caregiver_5016", "caregiver_5052", "caregiver_5099", "caregiver_5101", "caregiver_5116",
  "caregiver_5117", "caregiver_5118", "caregiver_5119", "caregiver_5126", "caregiver_5205",
  "caregiver_5206", "caregiver_5213", "caregiver_5219", "caregiver_5262", "caregiver_5264",
  "caregiver_5360", "caregiver_5386", "caregiver_5491", "caregiver_5507", "caregiver_5582",
  "caregiver_5583", "caregiver_5694", "caregiver_5703", "caregiver_5721", "caregiver_5722",
  "caregiver_5723", "caregiver_5735", "caregiver_5761", "caregiver_5764", "caregiver_5765",
  "caregiver_5814", "caregiver_5830", "caregiver_5831", "caregiver_5832", "caregiver_5833",
  "caregiver_5834", "caregiver_5877", "caregiver_5937", "caregiver_5976", "caregiver_5977",
  "caregiver_6028", "caregiver_6029", "caregiver_6155", "caregiver_6161", "caregiver_6162",
  "caregiver_6264", "caregiver_6265", "caregiver_6312", "caregiver_6363", "caregiver_6366",
  "caregiver_6370", "caregiver_6371", "caregiver_6372"
];

// -------------------------------
// Axios Client
// -------------------------------
const zendesk = axios.create({
  baseURL: `https://${ZENDESK_SUBDOMAIN}.zendesk.com/api/v2`,
  auth: {
    username: `${ZENDESK_EMAIL}/token`,
    password: ZENDESK_API_TOKEN,
  },
});

// -------------------------------
// 1. Fetch users by organization_id
// -------------------------------
async function fetchUsersByOrganization(organizationId) {
  let url = `/users/search.json?query=organization_id:${organizationId}`;
  let allUsers = [];

  while (url) {
    const res = await zendeskLimiter.schedule(() => zendesk.get(url));
    allUsers = allUsers.concat(res.data.users);
    url = res.data.next_page ? res.data.next_page.replace(/.*\.zendesk\.com\/api\/v2/, "") : null;
  }

  return allUsers;
}

async function fetchUsersToDelete() {
  let allUsers = [];

  // Fetch users from each organization ID
  for (const orgId of ORGANIZATION_IDS_TO_DELETE) {
    const users = await fetchUsersByOrganization(orgId);
    allUsers = allUsers.concat(users);
  }

  // Filter out users with alvita member organization_id
  const usersToDelete = allUsers.filter(
    (user) => user.organization_id?.toString() !== ALVITA_MEMBER_ORG_ID
  );

  const keptCount = allUsers.length - usersToDelete.length;
  if (keptCount > 0) {
  }

  return usersToDelete.map((u) => u.id);
}

// -------------------------------
// 2. Delete users in batches
// -------------------------------
async function deleteUsersInBatches(userIds) {
  const batchSize = 50;
  let jobIds = [];

  for (let i = 0; i < userIds.length; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize).join(",");

    const res = await zendeskLimiter.schedule(() => zendesk.delete(`/users/destroy_many.json?ids=${batch}`));
    const jobId = res.data.job_status.id;

    jobIds.push(jobId);
  }

  return jobIds;
}

// -------------------------------
// 3. Monitor job status
// -------------------------------
async function waitForJob(jobId) {
  while (true) {
    const res = await zendeskLimiter.schedule(() => zendesk.get(`/job_statuses/${jobId}.json`));
    const { status, message, total, progress } = res.data.job_status;

    if (status === "completed") return true;
    if (status === "failed") {
      return false;
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }
}

// -------------------------------
// 4. Get zendesk_user_ids from external_ids via Zendesk API
// -------------------------------
async function getZendeskUserIdsFromExternalIds(externalIds) {
  const userIds = [];
  
  for (const externalId of externalIds) {
    try {
      const query = `external_id:${externalId}`;
      const res = await zendeskLimiter.schedule(() => zendesk.get(`/users/search.json?query=${encodeURIComponent(query)}`));
      
      if (res.data.users && res.data.users.length > 0) {
        const user = res.data.users[0];
        if (user.id) {
          userIds.push(user.id);
        }
      }
    } catch (err) {
      logger.warn(`Failed to find user with external_id ${externalId}:`, err.response?.data || err.message);
    }
  }
  
  logger.info(`Found ${userIds.length} zendesk_user_ids for ${externalIds.length} external_ids`);
  return userIds;
}

// -------------------------------
// 5. Main function
// -------------------------------
async function main() {
  try {
    // Step 1: Delete normal users (non-alvita members)
    const userIds = await fetchUsersToDelete();

    if (userIds.length > 0) {
      logger.info(`Deleting ${userIds.length} normal users...`);
      const jobIds = await deleteUsersInBatches(userIds);

      for (const jobId of jobIds) {
        await waitForJob(jobId);
      }
      logger.info("✅ Normal users deletion completed");
    } else {
      logger.info("No normal users found to delete.");
    }

    // Step 2: Delete alvita members (except paula and kennedy)
    if (ALVITA_MEMBER_EXTERNAL_IDS_TO_DELETE.length > 0) {
      logger.info(`Looking up zendesk_user_ids for ${ALVITA_MEMBER_EXTERNAL_IDS_TO_DELETE.length} alvita member external_ids...`);
      const alvitaMemberUserIds = await getZendeskUserIdsFromExternalIds(ALVITA_MEMBER_EXTERNAL_IDS_TO_DELETE);
      
      if (alvitaMemberUserIds.length > 0) {
        logger.info(`Deleting ${alvitaMemberUserIds.length} alvita member users (excluding paula and kennedy)...`);
        const jobIds = await deleteUsersInBatches(alvitaMemberUserIds);

        for (const jobId of jobIds) {
          await waitForJob(jobId);
        }
        logger.info("✅ Alvita member users deletion completed");
      } else {
        logger.info("No zendesk_user_ids found for alvita member external_ids.");
      }
    } else {
      logger.info("No alvita member external_ids to delete.");
    }

  } catch (err) {
    logger.error("Fatal error:", err.response?.data || err);
  }
}

main();
