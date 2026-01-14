/**
 * @deprecated Use imports from './index.js' instead
 * This file is kept for backward compatibility but will be removed in a future version
 */
export { initDatabase, closeDatabase } from "./db.api.js";
export {
  saveMappedDataToDatabase,
  saveMappedUsersBatch,
  hasUsersPendingSync,
  getUsersPendingSync,
  resetCurrentActiveFlag,
  getUsersWithStatusChange,
  getPrimaryUsersDeactivated,
  fetchAndUpdateUserStatus,
  getAllUsersForSync,
  processNonActiveUser,
  clearZendeskPrimaryForUsers,
} from "./db.sync.repo.js";
export {
  upsertUserMapping,
  getUserMappingByAcId,
  getUserMappingByZendeskId,
  getAllUserMappings,
  updateZendeskUserId,
} from "./db.user.repo.js";
export { 
  processDuplicateEmailsAndPhones,
  processNonActiveUserEmailSwaps,
  findEmailGroupsWithoutPrimary,
  findPhoneGroupsWithoutPrimary,
} from "./db.duplicate.repo.js";
export { convertDatabaseRowToZendeskUser } from "../domain/user.db.mapper.js";
