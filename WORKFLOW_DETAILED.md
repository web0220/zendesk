# Zendesk-AlayaCare Integration: Complete Workflow Analysis

## Overview
This document provides a detailed breakdown of the entire sync workflow from start to finish, identifying potential issues with duplicate handling, primary user management, and status change processing.

---

## Phase 1: Data Fetching & Initial Database Save

### 1.1 Reset Active Flag
- **Location**: `resetCurrentActiveFlag()` in `db.sync.repo.js`
- **Action**: Sets `current_active = 0` for ALL users in database
- **Purpose**: Prepares database to track which users are active in current sync

### 1.2 Fetch Active Users from AlayaCare
- **Location**: `orchestrator.js` lines 87-88
- **Actions**:
  - Fetches active clients: `fetchClients({ status: "active" })`
  - Fetches active caregivers: `fetchCaregivers({ status: "active" })`
- **Note**: Only fetches ACTIVE users, not all users

### 1.3 Map to UserEntity Models
- **Location**: `orchestrator.js` lines 94-97
- **Actions**:
  - Maps clients using `mapClientUser()`
  - Maps caregivers using `mapCaregiverUser()`
  - Validates entities (filters invalid ones)
  - Converts to Zendesk payload format

### 1.4 Save to Database
- **Location**: `orchestrator.js` lines 103-116, `saveMappedUsersBatch()` in `db.sync.repo.js`
- **Actions**:
  - Saves ALL mapped users to `user_mappings` table
  - Uses `ON CONFLICT(ac_id) DO UPDATE` to update existing users
  - **CRITICAL**: Sets `current_active = 1` for all saved users
  - **CRITICAL**: Sets `shared_phone_number = NULL` (resets phone duplicates)
  - **CRITICAL**: Preserves `zendesk_user_id` if user already synced
  - **CRITICAL**: Updates `identities` field from fresh API data

### Issues Identified:
1. **Email Preservation Logic Conflict**: Lines 43 and 68 in `db.sync.repo.js` both update email field - line 43 preserves aliased emails for synced users, but line 68 always updates from fresh data. This creates ambiguity.
2. **Identities Always Updated**: Line 62 always updates identities from fresh API data, which may overwrite aliased email identities that were set in Phase 2.

---

## Phase 2: Duplicate Processing (Email & Phone)

### 2.1 Process Email Duplicates
- **Location**: `processEmailDuplicates()` in `db.duplicate.repo.js` lines 204-359
- **Steps**:
  1. Gets ALL active users (`current_active = 1`)
  2. Extracts emails from `email` field AND `identities` array using `extractAllEmails()`
  3. Builds email index: `email -> [users]`
  4. Uses union-find algorithm to find connected email groups
  5. For each group:
     - **Checks for `zendesk_primary` tag**
     - If NO primary tag → adds all users to `usersNeedingPrimary` (excluded from sync)
     - If primary exists:
       - Primary user keeps original emails
       - Non-primary users get emails aliased: `email+external_id@domain`
       - Aliased emails in both `email` field AND `identities` array
       - Updates database with aliased values

### 2.2 Process Phone Duplicates
- **Location**: `processPhoneDuplicates()` in `db.duplicate.repo.js` lines 373-495
- **Steps**:
  1. Gets ALL active users (`current_active = 1`)
  2. Extracts phones from `phone` field AND `identities` array using `extractAllPhoneNumbers()`
  3. Builds phone index: `phone -> [users]`
  4. Finds phone groups (2+ users sharing same phone)
  5. For each group:
     - **Checks for `zendesk_primary` tag**
     - If NO primary tag → adds all users to `usersNeedingPrimary` (excluded from sync)
     - If primary exists:
       - Primary user: moves phones from `shared_phone_number` back to `phone` + identities
       - Non-primary users: moves ALL phones to `shared_phone_number`, removes from `phone` and identities
       - Updates database

### 2.3 Return Users Needing Primary Tag
- **Location**: `processDuplicateEmailsAndPhones()` in `db.duplicate.repo.js` lines 1008-1032
- **Returns**: Combined list of users from email AND phone groups without primary tag
- **Action**: These users are excluded from Zendesk sync

### Issues Identified:
1. **Duplicate Processing Only Runs on Active Users**: Only processes users with `current_active = 1`. Non-active users are not included in duplicate detection, which may miss conflicts.
2. **Phone Duplicate Logic Doesn't Check Zendesk**: Phone duplicates are detected only from database. If a phone exists in Zendesk (in another user's identities) but not in database, it won't be caught until identity sync fails.
3. **No Cross-User Identity Check**: When processing duplicates, only checks database. Doesn't check if phones/emails already exist in Zendesk for other users.
4. **Timing Issue**: Duplicate processing happens BEFORE users are synced to Zendesk. If a phone/email exists in Zendesk from a previous sync but not in current database, it won't be detected.

---

## Phase 3: Non-Active User Processing

### 3.1 Detect Status Changes
- **Location**: `getUsersWithStatusChange()` in `db.sync.repo.js` lines 224-238
- **Query**: Users where `current_active = 0`, `zendesk_user_id IS NOT NULL`, `non_active_status_fetched IS NULL`
- **Purpose**: Finds users who were active before but not in current active fetch

### 3.2 Process Each Non-Active User
- **Location**: `processNonActiveUser()` in `db.sync.repo.js` lines 468-680
- **Steps**:
  1. Fetches FULL user data from AlayaCare API (not just status)
  2. Maps to UserEntity (same as Phase 1)
  3. **Email Conflict Detection**:
     - Extracts all emails from mapped data (email field + identities)
     - For each email, checks if active users share it using `findUsersSharingEmail()`
     - If conflict found:
       - Aliases non-active user's email: `email+external_id@domain`
       - Deletes conflicting email identities from Zendesk
       - Updates database with aliased emails
  4. Saves updated data to database
  5. Marks `non_active_status_fetched = 1`

### 3.3 Update Non-Active Users in Zendesk
- **Location**: `orchestrator.js` lines 176-205
- **Steps**:
  1. Converts database row to Zendesk payload
  2. Updates user via PUT (status change)
  3. Syncs identities via POST (like normal users)

### 3.4 Primary User Deactivation Alert
- **Location**: `orchestrator.js` lines 139-150
- **Action**: If a `zendesk_primary` user becomes non-active, adds to alerts
- **Business Rule**: Primary users should NOT be non-active

### Issues Identified:
1. **Non-Active Users Not in Duplicate Processing**: Non-active users are processed separately and don't go through Phase 2 duplicate processing. This means:
   - They may have duplicate phones/emails that aren't detected
   - They don't get grouped with active users for duplicate checking
   - Phone duplicates for non-active users aren't handled
2. **Email Conflict Only Checks Active Users**: When non-active user has email conflict, only checks against active users. Doesn't check against other non-active users.
3. **No Phone Duplicate Handling for Non-Active**: Non-active users don't go through phone duplicate processing. If a non-active user shares a phone with another user, it's not handled.

---

## Phase 4: Pre-Sync Validation

### 4.1 Get All Users for Sync
- **Location**: `getAllUsersForSync()` in `db.sync.repo.js` lines 193-201
- **Query**: Gets ALL users from database (active + non-active)
- **Purpose**: Prepares list of users to sync to Zendesk

### 4.2 Check Email Groups Without Primary
- **Location**: `findEmailGroupsWithoutPrimary()` in `db.duplicate.repo.js` lines 825-882
- **Steps**:
  1. Gets ALL users (active + non-active)
  2. Extracts emails from `email` field AND `identities` array
  3. Groups by unaliased email (handles aliased emails)
  4. Finds groups with 2+ users and NO `zendesk_primary` tag
  5. Adds all users in these groups to exclusion list

### 4.3 Check Phone Groups Without Primary
- **Location**: `findPhoneGroupsWithoutPrimary()` in `db.duplicate.repo.js` lines 770-823
- **Steps**:
  1. Gets ALL users (active + non-active)
  2. Extracts phones from `phone` field AND `identities` array
  3. Groups by phone number
  4. Finds groups with 2+ users and NO `zendesk_primary` tag
  5. Adds all users in these groups to exclusion list

### 4.4 Exclude Problematic Users
- **Location**: `orchestrator.js` lines 236-303
- **Actions**:
  - Excludes users from email groups without primary
  - Excludes users from phone groups without primary
  - Excludes users from Phase 2 `usersNeedingPrimary` list
  - Filters final list: `usersToSync`

### Issues Identified:
1. **Redundant Checking**: `findEmailGroupsWithoutPrimary()` and `findPhoneGroupsWithoutPrimary()` check ALL users again, even though Phase 2 already checked active users. This is inefficient but serves as a safety check.
2. **Non-Active Users Included in Groups**: Non-active users are included in group checking, but they weren't processed in Phase 2. This may create groups that weren't properly handled.
3. **Timing Gap**: These checks happen AFTER Phase 3 (non-active processing), so non-active users may have been updated but groups weren't re-checked.

---

## Phase 5: Zendesk Sync

### 5.1 Batch Users
- **Location**: `orchestrator.js` lines 429-442
- **Action**: Splits users into batches of 100 for Zendesk bulk API

### 5.2 Bulk Upsert to Zendesk
- **Location**: `bulkUpsertUsers()` in `services/zendesk/upsert.js`
- **Action**: Sends batch to Zendesk bulk API
- **Result**: Returns job status with results for each user (Created/Updated/Failed)

### 5.3 Process Batch Results
- **Location**: `orchestrator.js` lines 497-630
- **For each user**:
  1. Matches Zendesk result to user data
  2. If Created/Updated:
     - Calls `syncUserIdentities()` to sync identities
     - Updates database with `zendesk_user_id` and `last_synced_at`
  3. If Failed: Logs error details

### 5.4 Identity Sync
- **Location**: `syncUserIdentities()` in `services/zendesk/identitySync.js` lines 101-151
- **Steps**:
  1. Gets existing identities from Zendesk for current user
  2. Compares with identities from database
  3. For each new identity:
     - Tries to add to Zendesk via POST
     - **If duplicate error** (phone/email already exists for another user):
       - Catches error
       - Adds to `contact_address` field
       - **Does NOT check for primary tag**
       - **Does NOT trigger duplicate grouping logic**
       - **Does NOT exclude user from sync**

### Issues Identified:
1. **Identity Sync Doesn't Check Cross-User Duplicates**: When adding identities, only checks if identity exists for CURRENT user. Doesn't check if it exists for OTHER users before attempting to add.
2. **Duplicate Detection is Reactive**: Only detects duplicates AFTER Zendesk API rejects them. Should be proactive.
3. **No Grouping Logic**: When duplicate phone/email detected during identity sync, doesn't trigger the same grouping/primary tag checking logic from Phase 2.
4. **Contact Address Fallback**: Duplicate phones/emails are added to `contact_address` as fallback, but this doesn't solve the underlying duplicate issue.
5. **No Re-check After Identity Sync**: After identity sync completes, doesn't re-check if new duplicates were created that need grouping.

---

## Phase 6: Primary User Alias Cleanup

### 6.1 Clean Aliases for Primary Users
- **Location**: `orchestrator.js` lines 658-742
- **Steps**:
  1. Gets all primary users that were synced
  2. For each primary user:
     - Gets identities from Zendesk
     - Finds aliased email identities (`+client_` or `+caregiver_` pattern)
     - Deletes aliased email identities from Zendesk
  3. **Purpose**: Primary users should have unaliased emails. If they were previously non-primary, they may have aliased emails that need cleanup.

### Issues Identified:
1. **Only Cleans Email Aliases**: Doesn't clean phone duplicates. If a primary user has phones in `shared_phone_number`, they should be moved back.
2. **Timing**: Runs AFTER all users are synced. If a primary user was synced with aliased emails, they're already in Zendesk before cleanup.

---

## Critical Issues Summary

### 1. Duplicate Detection Timing
- **Problem**: Duplicate processing (Phase 2) happens BEFORE users are synced to Zendesk. If a phone/email exists in Zendesk from a previous sync but not in current database, it won't be detected until identity sync fails.
- **Impact**: Duplicates may slip through and only be caught reactively.

### 2. Identity Sync Doesn't Check Cross-User Duplicates
- **Problem**: `syncUserIdentities()` only checks if identity exists for current user. Doesn't check if it exists for OTHER users before attempting to add.
- **Impact**: May try to add duplicate phones/emails that already exist for other users, causing API errors.

### 3. Non-Active Users Not in Duplicate Processing
- **Problem**: Non-active users are processed separately (Phase 3) and don't go through Phase 2 duplicate processing.
- **Impact**: 
  - Non-active users may have duplicate phones/emails that aren't detected
  - They don't get grouped with active users for duplicate checking
  - Phone duplicates for non-active users aren't handled

### 4. Duplicate Detection Doesn't Check Zendesk
- **Problem**: Phase 2 duplicate processing only checks database. Doesn't check if phones/emails already exist in Zendesk for other users.
- **Impact**: May miss duplicates that exist in Zendesk but not in current database snapshot.

### 5. Identity Sync Duplicates Don't Trigger Grouping Logic
- **Problem**: When `syncUserIdentities()` detects a duplicate (via API error), it just adds to `contact_address`. Doesn't trigger the same grouping/primary tag checking logic from Phase 2.
- **Impact**: Duplicates are handled individually rather than as groups, which may violate business rules.

### 6. Email Preservation Logic Conflict
- **Problem**: In `db.sync.repo.js`, lines 43 and 68 both update email field with conflicting logic.
- **Impact**: Ambiguity about when emails are preserved vs updated.

### 7. Phone Duplicate Logic Only Processes Active Users
- **Problem**: `processPhoneDuplicates()` only processes users with `current_active = 1`. Non-active users aren't included.
- **Impact**: Phone duplicates involving non-active users aren't handled.

### 8. No Re-check After Identity Sync
- **Problem**: After identity sync completes, doesn't re-check if new duplicates were created that need grouping.
- **Impact**: New duplicates created during sync aren't properly grouped.

---

## Recommendations

1. **Proactive Duplicate Detection**: Before syncing identities, check if phone/email already exists in Zendesk for other users.
2. **Unified Duplicate Processing**: Include non-active users in duplicate processing, or create separate logic for them.
3. **Cross-User Identity Check**: Before adding identities, query Zendesk to check if they exist for other users.
4. **Reactive Duplicate Handling**: When identity sync detects duplicates, trigger the same grouping/primary tag checking logic.
5. **Fix Email Preservation Logic**: Resolve conflict between lines 43 and 68 in `db.sync.repo.js`.
6. **Re-check After Sync**: After identity sync completes, re-check for new duplicates and group them properly.
7. **Zendesk State Check**: Before duplicate processing, optionally check Zendesk state to catch duplicates that exist there but not in database.

