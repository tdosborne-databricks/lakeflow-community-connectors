# Lakeflow HubSpot Connector

The Lakeflow HubSpot Connector allows you to extract data from your HubSpot instance and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for HubSpot objects.

This is an open-source community connector with no Databricks SLA or certification.
CRM schema, property, object, history and association reads use the pinned HubSpot
`2026-09` API release. Public API documentation is the contract; live acceptance
must also be verified against the target account's permissions and subscription.

## Set up

### Prerequisites
- Access to a HubSpot instance with API permissions
- HubSpot Private App with appropriate read permissions
- Developer or admin role in HubSpot (depending on the data you want to access)

### Required Parameters

To configure the HubSpot connector, you'll need to provide the following parameters in your connector options:

1. access_token

### Getting Your Private App Access Token

1. Log in to your HubSpot account as an admin
2. Navigate to Settings → Integrations → Private Apps
3. Click "Create a private app"
4. Configure the app name and description
5. Set the required scopes (contacts read permissions)
6. Create the app and copy the access token
7. Store the token securely (you won't be able to see it again)

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.

## Objects Supported

The HubSpot connector supports the following CRM objects with dynamic schema discovery and incremental synchronization:

| Table Name | Primary Key | Cursor Field | Associations |
|------------|-------------|--------------|--------------|
| `contacts` | `id` | `updatedAt` | companies |
| `companies` | `id` | `updatedAt` | contacts |
| `deals` | `id` | `updatedAt` | contacts, companies, tickets |
| `tickets` | `id` | `updatedAt` | contacts, companies, deals |
| `calls` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `emails` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `meetings` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `tasks` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `notes` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `contacts_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `companies_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `deals_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `tickets_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `<custom_object>_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |

> **Note**: Table names are case-sensitive. Use the exact names shown above (lowercase with underscores). Custom objects are also supported and will be discovered automatically, each with a matching `<custom_object>_property_history` table.

### Resource snapshots

The following tables use complete snapshot ingestion. Each read drains every
configured partition and page before returning rows. An HTTP error, malformed
page, incomplete partition discovery, or conflicting duplicate fails the read.
The downstream snapshot consumer reconciles removals after a successful read.
These tables retain all source fields in a JSON `payload` column alongside typed
common fields. They do not use `max_records_per_batch` or property filters.

| Table | Primary key | Coverage |
|---|---|---|
| `owners` | `id` | Active and archived owners, including teams |
| `pipelines` | `objectTypeId`, `id` | Pipelines and nested stages for configured object types |
| `forms` | `id` | Active and archived forms |
| `contact_lists` | `listId` | Contact lists with MANUAL, DYNAMIC, or SNAPSHOT processing type |
| `contact_list_memberships` | `listId`, `recordId` | Current memberships of each discovered contact list |
| `form_submissions` | `formId`, `conversionId` | Submissions for each discovered form |

Private apps need `crm.objects.owners.read` for owners, `forms` for forms and
submissions, and an appropriate list read scope (`crm.segments.read` or
`crm.lists.read`) for contact lists and memberships. Pipeline access also depends
on the object permissions and subscription. Missing permission fails a selected
snapshot; it is not interpreted as an empty table.

By default, pipeline snapshots enumerate deals (`0-3`), tickets (`0-5`), tasks
(`0-27`), and discovered custom objects. Schema discovery must succeed for that
inventory to be complete. Use `pipeline_object_types` to supply a comma-separated
explicit set, such as `0-3,0-5`; that set replaces discovery. Add this option to an
existing Unity Catalog connection's external options allowlist before using it.
Every selected partition must be readable; inaccessible partitions are not skipped.

### Custom objects and associations

Custom table names remain the lowercase schema names; API calls use their
canonical `objectTypeId`. Directional schema association definitions add one
target ID-array column per related object, including custom-to-custom and
self-associations. The connector rejects ambiguous names and unsupported target
types instead of silently dropping relationships.

Object snapshot and incremental reads both hydrate relationships through the
2026-09 association batch API. Each parent has its own pagination cursor. IDs are
deduplicated and sorted across pages and labels. Empty arrays mean a successful
empty read. Missing parents are retried; incomplete responses or invalid cursors
fail the read before it returns a new checkpoint.

The default incremental mode refreshes associations only for parents selected by
their modification timestamp. HubSpot does not guarantee that association-only
changes update that timestamp. Set `association_sync_mode=full_refresh` on an
object table to advertise snapshot ingestion and enumerate all current parents
and relationships on each pipeline update. This reconciles association removals
without requiring a parent property change, at the cost of a complete scan.
Property-history tables retain their own ingestion strategy.

### Object Details

### Standard CRM Objects

#### `contacts`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: companies, deals, tickets
- **Schema**: All contact properties (discovered dynamically) including standard fields like email, firstname, lastname, and all custom properties

#### `companies`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, deals, tickets
- **Schema**: All company properties (discovered dynamically) including standard fields like name, domain, and all custom properties

#### `deals`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, tickets
- **Schema**: All deal properties (discovered dynamically) including standard fields like dealname, amount, stage, and all custom properties

#### `tickets`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals
- **Schema**: All ticket properties (discovered dynamically) including standard fields like subject, content, priority, and all custom properties

### Engagement Objects

#### `calls`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All call properties (discovered dynamically) including standard fields like duration, outcome, recording details, and all custom properties

#### `emails`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All email properties (discovered dynamically) including standard fields like subject, body, sender, recipient, and all custom properties

#### `meetings`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All meeting properties (discovered dynamically) including standard fields like title, start time, end time, location, and all custom properties

#### `tasks`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All task properties (discovered dynamically) including standard fields like subject, due date, status, priority, and all custom properties

#### `notes`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All note properties (discovered dynamically) including standard fields like body, timestamp, and all custom properties

### Custom Objects
- **Dynamic Discovery**: The connector automatically discovers and supports any custom objects in your HubSpot instance
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Schema**: All custom object properties discovered dynamically via HubSpot Properties API

### Property History Tables

`contacts_property_history`, `companies_property_history`, `deals_property_history`, `tickets_property_history`, and `<custom_object>_property_history` contain one row per historical value of each property, from HubSpot's `propertiesWithHistory`.

- **Ingestion type**: `cdc` (upsert on the primary key, so rows that are read again by the lookback window are de-duplicated)
- **Primary Key**: `object_id`, `property`, `timestamp`
- **Cursor Field**: `timestamp`
- **Schema**: `object_id`, `property`, `value`, `timestamp`, `source_type`, `source_id`, `source_label`, `updated_by_user_id`
- **How it reads**: the Search API finds objects whose last-modified date changed since the last run (Search does not return history), then the Batch Read API (`POST /crm/objects/2026-09/{object}/batch/read`) fetches `propertiesWithHistory` for those objects, up to 50 IDs per call.
- **Rate limiting**: Search calls are spaced to stay under HubSpot's 5 requests/second Search limit, and HTTP 429 responses are retried (honouring `Retry-After`, otherwise exponential backoff, up to 5 retries). Before paging past the Search API's 10,000-result cap, the connector returns only timestamp groups proven complete by observing a later timestamp. The final group is reread next time. If no complete new group fits in the search window, the read fails without advancing its checkpoint.
- **Partial history responses**: The connector reconciles returned parent IDs against every requested batch, including HTTP 200 and 207 responses. It retries only missing IDs, up to 5 times. If any remain unresolved, the read fails before returning a new checkpoint. This also applies to records deleted between search and batch read; the connector does not silently discard their missing history.

Known completeness limits remain: the current timestamp-only checkpoint cannot
recover late-indexed parents at or below its watermark, despite the lookback
search window. A timestamp group that fills the Search API's result window
blocks ingestion until a different pagination strategy is available; it is not
silently skipped. Neither case is resolved by the partial-response retry.
- **Scopes**: the same `crm.objects.<object>.read` scope as the parent object.
- **Known limitation**: changes that do not update the object's last-modified date (some calculated, rollup, or formula properties) are not detected until the object is next modified.

### Common Schema Structure
All objects follow the same base structure:
- **Base Fields**: `id`, `createdAt`, `updatedAt`, `archived`
- **Associations**: Arrays of associated object IDs (for standard objects)
- **Properties**: All object properties flattened with `properties_` prefix
- **Dynamic Discovery**: Schema adapts automatically to your HubSpot configuration

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_records_per_batch` | all incremental tables | no cap | Soft limit on records returned per microbatch. Object and property-history reads drain parents sharing the last processed `updatedAt`, including across search pages, before stopping. A timestamp group can exceed this value by more than one batch. |
| `association_sync_mode` | object tables | `incremental` | `incremental` refreshes selected changed parents. `full_refresh` uses snapshot ingestion and reconciles all current relationships on every update. Add this option to an existing UC connection's external options allowlist before using it. |
| `pipeline_object_types` | `pipelines` | deals, tickets, tasks, discovered custom types | Comma-separated canonical object type IDs. Replaces the default partition inventory; each configured partition must succeed. |
| `history_properties` | `*_property_history` | all properties | Comma-separated property names to fetch history for (e.g. `lifecyclestage,hs_lead_status,dealstage`). Recommended for portals with many properties, because requesting history for every property makes each Batch Read response larger. |
| `include_properties` | CRM object and history tables | all properties | Comma-separated allowlist of properties to ingest. If set, only these discovered properties are read and exposed in the schema (unknown names are ignored, matching HubSpot's behavior). Required for objects with very many properties — full refresh fails with HTTP 414 once the request URL passes ~22,000 characters (objects with 1,000+ properties always exceed this). The object's cursor property is always retained. |
| `exclude_properties` | CRM object and history tables | none | Comma-separated list of properties to drop from ingestion. Applied after `include_properties`. |
| `history_since` | `*_property_history` | none | ISO-8601 timestamp (e.g. `2025-01-01T00:00:00Z`). Only objects modified on or after this time are read, and older history entries are dropped. Caps the initial backfill. |
| `history_lookback_minutes` | `*_property_history` | `10` | Minutes subtracted from the saved cursor for the first search in a pipeline update. The current strict watermark filter still drops parents at or below the cursor, so this does not yet recover late-indexed parents. Set to `0` to disable. |

`include_properties` / `exclude_properties` also apply to `*_property_history` tables (they bound which properties are requested in `propertiesWithHistory`; `history_properties` still narrows further for history reads). Two operational notes:

- Rows ingested before a filter change are not retroactively removed. To rebuild a table so its contents match the new filter, force a re-ingest (below).
- **Forcing a re-ingest:** changing any connector option value on a table — even trivially, e.g. bumping `history_lookback_minutes` by 1 — recreates that table's ingestion flow and re-reads the source from the beginning with the new settings. This is the reliable way to rebuild a table after changing filters: on serverless managed ingestion, a full refresh alone re-processes from the pipeline's stored cursor and may not re-read the source for tables whose options did not change.

These options must be included in the connection's `externalOptionsAllowList` (see `connector_spec.yaml`).

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing contacts to test your pipeline
- **Monitor API Limits**: HubSpot has rate limits (100 requests per 10 seconds for most endpoints)
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness needs with API usage
- **Test Thoroughly**: Validate data accuracy and completeness after initial setup

#### Troubleshooting

**Common Issues:**
- **Authentication Errors**: Verify access token is correct and has proper scopes
- **Rate Limiting**: Reduce sync frequency or implement exponential backoff
- **Missing Data**: Check Private App permissions and scopes
- **Schema Changes**: Update connector code if HubSpot adds new fields

**Error Handling:**
The connector includes built-in error handling for common scenarios like rate limiting, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.
