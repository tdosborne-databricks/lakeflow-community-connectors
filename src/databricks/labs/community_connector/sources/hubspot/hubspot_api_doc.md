# HubSpot API — Connector Reference

This document maps each connector table to its underlying HubSpot CRM v3 API
endpoint(s), pagination strategy, and rate-limit behavior. The implementation
lives in `hubspot.py`; tables and authentication are documented in `README.md`.

## Authentication

All endpoints use HubSpot Private App tokens, sent as a Bearer token:

```
Authorization: Bearer <access_token>
Content-Type: application/json
```

Private apps must be granted the appropriate CRM scopes for each object type
(`crm.objects.contacts.read`, `crm.objects.companies.read`, etc.).

## Base URL

```
https://api.hubapi.com
```

## Rate Limits

- 100 requests per 10 seconds per token (default).
- 250,000 requests per day per token (free / starter; varies by plan).
- 429 responses include a `Retry-After` header — the connector respects this
  via exponential backoff.

## Pagination

HubSpot CRM v3 paginates with an opaque `paging.next.after` cursor. The
connector follows it until either:
1. The endpoint returns no `paging.next.after`, or
2. The configured `max_records_per_batch` (admission control) is reached, in
   which case the partial cursor is returned and the next microbatch resumes.

## Tables

| Table        | Endpoint (full refresh)                  | Endpoint (incremental search)                  | Primary Key | Cursor Field |
|--------------|------------------------------------------|------------------------------------------------|-------------|--------------|
| `contacts`   | `GET /crm/objects/2026-09/contacts`           | `POST /crm/objects/2026-09/contacts/search`         | `id`        | `updatedAt`  |
| `companies`  | `GET /crm/objects/2026-09/companies`          | `POST /crm/objects/2026-09/companies/search`        | `id`        | `updatedAt`  |
| `deals`      | `GET /crm/objects/2026-09/deals`              | `POST /crm/objects/2026-09/deals/search`            | `id`        | `updatedAt`  |
| `tickets`    | `GET /crm/objects/2026-09/tickets`            | `POST /crm/objects/2026-09/tickets/search`          | `id`        | `updatedAt`  |
| `calls`      | `GET /crm/objects/2026-09/calls`              | `POST /crm/objects/2026-09/calls/search`            | `id`        | `updatedAt`  |
| `emails`     | `GET /crm/objects/2026-09/emails`             | `POST /crm/objects/2026-09/emails/search`           | `id`        | `updatedAt`  |
| `meetings`   | `GET /crm/objects/2026-09/meetings`           | `POST /crm/objects/2026-09/meetings/search`         | `id`        | `updatedAt`  |
| `tasks`      | `GET /crm/objects/2026-09/tasks`              | `POST /crm/objects/2026-09/tasks/search`            | `id`        | `updatedAt`  |
| `notes`      | `GET /crm/objects/2026-09/notes`              | `POST /crm/objects/2026-09/notes/search`            | `id`        | `updatedAt`  |

Custom objects discovered via `GET /crm-object-schemas/2026-09/schemas` are exposed as additional
tables using the same patterns as the standard objects above.

### Cursor property mapping

The search API filters on the property-level cursor field, which differs from
the response-level `updatedAt`:

| Table       | Property cursor field   |
|-------------|-------------------------|
| `contacts`  | `lastmodifieddate`      |
| All others  | `hs_lastmodifieddate`   |

### Deletes (CDC)

HubSpot supports `archived=true` queries on the following tables. The
connector emits these via `read_table_deletes`:

- `contacts`, `companies`, `deals`, `tickets`, `emails`, `tasks`, `notes`

`calls` and `meetings` do not support archived queries and are append-only
incremental.

## Schema discovery

Each table's column set is discovered at runtime via:

```
GET /crm/properties/2026-09/{type}
```

The connector caches the schema per `HubspotLakeflowConnect` instance.

Property responses wrap fields in `results`. Custom schema discovery uses
`GET /crm-object-schemas/2026-09/schemas`; returned directional association
definitions resolve columns by canonical objectTypeId after collecting all schemas.

## Complete association reads

`POST /crm/associations/2026-09/{fromType}/{toType}/batch/read` receives
`inputs: [{id}]`. Each result has `from.id`, `to[].toObjectId` and optional
per-parent `paging.next.after`. Continue with `{id, after}` for that parent.
The connector drains all pages and reconciles every requested parent before
publishing rows. HTTP207 is not sufficient evidence of a complete result.

Association-only changes are not guaranteed to advance a searched parent cursor.
The optional `association_sync_mode=full_refresh` returns snapshot metadata and
performs a complete parent/association scan each update; the default incremental
mode covers only parents selected by Search.

## Search API request shape

Incremental reads use the search API with a filter on the property-level
cursor field. Example request body for `contacts`:

```json
{
  "filterGroups": [{
    "filters": [{
      "propertyName": "lastmodifieddate",
      "operator": "BETWEEN",
      "highValue": "<init_ts>",
      "value": "<cursor>"
    }]
  }],
  "sorts": [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}],
  "limit": 100,
  "after": "<paging cursor>"
}
```

The high-value bound (`<init_ts>`) is captured once per connector instance to
guarantee `Trigger.AvailableNow` termination — without it, the read window
chases incoming updates forever.

## Property history (`{object}_property_history`)

HubSpot returns a property's previous values when it is listed in
`propertiesWithHistory`. Which endpoints accept it (verified against
HubSpot's OpenAPI specs for the CRM objects API):

| Endpoint | `propertiesWithHistory` | Notes |
|---|---|---|
| `GET /crm/objects/2026-09/{type}/{id}` | yes (query) | one object |
| `GET /crm/objects/2026-09/{type}` (list) | yes (query) | "will reduce the maximum number of [objects] that can be read by a single request"; long property lists hit URL-length limits |
| `POST /crm/objects/2026-09/{type}/batch/read` | yes (body) | up to 100 IDs; may return `207` with per-ID errors |
| `POST /crm/objects/2026-09/{type}/search` | **no** | request schema is `after`, `filterGroups`, `limit`, `properties`, `query`, `sorts` |

Each history entry (`ValueWithTimestamp`) has `value`, `timestamp`,
`sourceType` (required) and `sourceId`, `sourceLabel`, `updatedByUserId`
(optional).

### Read pattern

1. `POST /crm/objects/2026-09/{type}/search` with `GTE <cursor - lookback>` and
   `LTE <init_ts>` on the cursor property, ascending, `limit: 200`, returning
   only IDs and `updatedAt`.
2. `POST /crm/objects/2026-09/{type}/batch/read` with up to 50 IDs and
   `propertiesWithHistory` (all properties from the Properties API, or the
   `history_properties` table option).
3. One output row per history entry; entries older than the query's lower
   bound (or `history_since`) are dropped.

Offset: `{"updatedAt": <max parent updatedAt processed>}`. The lookback is
applied once per connector instance (one pipeline update), not stored in the
offset.

### Limits that shape the implementation

- Search: **5 requests/second per account**, max 200 results per page, max
  **10,000 results per query** (paging past it returns 400). The reader spaces
  search calls 250 ms apart and ends a read before the cap, re-anchoring the
  next search at the latest `updatedAt`.
- General burst limits for privately distributed apps: 100 requests / 10 s
  (Free/Starter), 190 / 10 s (Professional/Enterprise), 250 / 10 s with the API
  limit increase. Daily: 250k / 625k / 1M per account.
- 429 responses: retried up to 5 times, honouring `Retry-After` when present,
  else exponential backoff (1, 2, 4, 8, 16 s; capped at 30 s). Retry applies to
  the property history tables only.

### Known gaps (to confirm in live testing)

- HubSpot does not document the per-request cap for batch read *with* history;
  the reader uses 50 IDs per call.
- Calculated / rollup properties may change without updating the object's
  last-modified date (Airbyte documents this as cursor drift); such changes are
  not picked up until the object is next modified.

## Property filtering (`include_properties` / `exclude_properties`)

By default every table discovers and requests **all** properties. Two table
options bound that set (both are applied to object tables and to
`*_property_history` tables):

- `include_properties` — comma-separated allowlist; unknown names are ignored
  (HubSpot itself ignores unrecognised property names).
- `exclude_properties` — removed after `include_properties` is applied.

The object's cursor property (`lastmodifieddate` / `hs_lastmodifieddate`) is
always retained so incremental reads keep working. Changing the options
changes the schema and future ingestion; previously ingested rows remain
until the table is re-ingested. To force a re-ingest after changing options,
change any connector option value on the table (even trivially) — this
recreates the table's ingestion flow and re-reads the source from the
beginning. On serverless managed ingestion, a full refresh alone resumes
from the pipeline's stored cursor and may not re-read the source for tables
whose options did not change.

**Why this matters — verified against a live HubSpot portal (2026-10-07):**

- **Full refresh (list endpoint)** takes `properties=` in the URL. Requests fail
  with **HTTP 414 Request-URI Too Large** once the URL passes roughly **22,000
  characters** (~630 property parameters). Objects with 1,000+ properties
  produce ~35,000-character URLs — full refresh fails on them, always.
- **Incremental (Search API)** accepts large `properties` bodies (verified
  HTTP 200 at ~29,700 characters / 1,099 entries); HubSpot's documented
  3,000-character "query" limit did not reject the properties array in
  testing. Still, requesting every property inflates every page of every
  response.

Use `include_properties` / `exclude_properties` to bound wide objects to the
properties you need — for 1,000+-property objects it is currently the only
way to ingest them at all. Chunking the all-properties path across multiple
requests (Airbyte-style) is a possible follow-up.

## References

- HubSpot CRM v3 API: https://developers.hubspot.com/docs/api/crm
- Search API: https://developers.hubspot.com/docs/api/crm/search
- Properties API: https://developers.hubspot.com/docs/api/crm/properties
- Rate limits: https://developers.hubspot.com/docs/api/usage-details
- Search limits: https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits
- Batch read (contacts example, same shape for all CRM objects): https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/contacts/batch/get-contacts
- Usage guidelines (current limits table): https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines
