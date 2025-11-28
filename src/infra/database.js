export { initDatabase, closeDatabase } from "./db.api.js";
export {
  saveMappedDataToDatabase,
  saveMappedUsersBatch,
  hasUsersPendingSync,
  getUsersPendingSync,
  resetCurrentActiveFlag,
  getUsersWithStatusChange,
  fetchAndUpdateUserStatus,
} from "./db.sync.repo.js";
export {
  upsertUserMapping,
  getUserMappingByAcId,
  getUserMappingByZendeskId,
  getAllUserMappings,
  updateZendeskUserId,
} from "./db.user.repo.js";
export { processDuplicateEmailsAndPhones } from "./db.duplicate.repo.js";
export { convertDatabaseRowToZendeskUser } from "../domain/user.db.mapper.js";
