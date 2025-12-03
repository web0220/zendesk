// deleteUsersByOrganization.js
import axios from "axios";
import { zendeskLimiter } from "../src/utils/limiter.js";

const ZENDESK_SUBDOMAIN = "alvitacare";
const ZENDESK_EMAIL = "paula.cheng@alvitacare.com";
const ZENDESK_API_TOKEN = "9oFYAOqNqcc3QsXP2J9gIetZGHU6dcEzmeENUwpS";

// Organization IDs to delete users from
const ORGANIZATION_IDS_TO_DELETE = ["42824772337179", "43279021546651"];
// Organization ID to keep (alvita member)
const ALVITA_MEMBER_ORG_ID = "40994316312731";

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
// 4. Main function
// -------------------------------
async function main() {
  try {
    const userIds = await fetchUsersToDelete();

    if (userIds.length === 0) {
      console.log("No users found to delete (all users are alvita members or organizations are empty).");
      return;
    }

    const jobIds = await deleteUsersInBatches(userIds);

    for (const jobId of jobIds) {
      await waitForJob(jobId);
    }

  } catch (err) {
    logger.error("Fatal error:", err.response?.data || err);
  }
}

main();
