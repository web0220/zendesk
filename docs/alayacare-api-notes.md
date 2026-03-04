# AlayaCare API discovery notes

AlayaCare API docs are legacy and incomplete. This file records endpoints discovered from the codebase and from running `scripts/alayacare_discover_api.js`.

## Known working endpoints (from codebase)

- **GET /patients/clients/** – List clients. Params: `page`, `count`. Response: `items` array (or array directly), pagination may vary.
- **GET /patients/clients/:id** – Client detail (demographics, contacts, groups, tags).
- **GET /employees/employees/** – List employees/caregivers. Params: `page`, `count`.
- **GET /employees/employees/:id** – Employee/caregiver detail (demographics, departments, groups, tags).
- **GET /ext/api/v2/scheduler/visit** – Visits. Params: `alayacare_employee_id`, `start_at`, `end_at`, `page`, optional `status`, `cancelled`. Use **native fetch** with only `Authorization: Basic <base64>` header (no User-Agent/Accept or the API may return 502). Response: `items`, `total_pages`, `items_per_page`, `count`.

## Authentication

All requests use Basic auth: `Authorization: Basic base64(publicKey:privateKey)`. Env: `ALAYACARE_BASE_URL`, `ALAYACARE_PUBLIC_KEY`, `ALAYACARE_PRIVATE_KEY`.

## Discovery runs

Run from project root:

```bash
node scripts/alayacare_discover_api.js
```

To write this file with a summary table of probed paths:

```bash
node scripts/alayacare_discover_api.js --write-docs
```

Add any newly discovered endpoints (financial reports, documents, schedules, confirmations, etc.) below as they are verified.

## Additional endpoints (to discover / document)

- Financial reports
- CG (caregiver) list exports
- Client confirmation emails / schedule
- Attached files / documents
- Other report or list endpoints
