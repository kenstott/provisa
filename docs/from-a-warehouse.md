# Coming from a data warehouse

In a traditional data warehouse, a data engineering team operates a pipeline. The jobs look like this:

- Extract jobs pull data from each source.
- Load jobs push copies into the warehouse.
- Refresh schedules decide how often those copies are updated.
- Workload management and resource queues protect the warehouse from busy periods; off-peak batch windows push heavy loads to quieter times.
- Transformation jobs turn raw tables into the views and aggregates analysts consume.

Provisa handles the same work as declarations. You write down what you want; the engine decides when to move data, where to put it, and how much load to place on each source.

The decisions live in the [org admin's layer](access-model.md#three-layers-of-administration). Capabilities enforce the boundary between that layer and the platform: the platform admin holds no data capability, and beyond deciding the orgs and their org admins it never touches an org's data or members. Within the org, the layers are defaults carried by the standard roles. A small org's admin may hold all of them. A larger org may define a dedicated data engineer role for source and table registration, or split load management further.

## The same jobs, as declarations

| What you do in a warehouse | What you declare in Provisa |
| --- | --- |
| Write an extract job to pull a source | Connect the source. See [`source_registration`](configuration.md#capabilities). |
| Write a load job to copy rows | Set [`replicate`](configuration.md#the-replicate-setting) on the source or table. `0` means always copy (Always); `-1` means never copy and always read live (Never); `N` above 0 means copy once the table is busy (Hot-N). |
| Schedule refreshes | Set [`cache_ttl`](configuration.md#cache) on the source or table. Without a TTL or change feed, the engine builds the replica once and refreshes it only when a change signal arrives. |
| Restrict a source to off-peak hours | Set `load_protected: true` and declare an `off_peak_window` on the source or table. The source is never read live; the scheduler refreshes its snapshot only while the window is open. |
| Cap concurrent warehouse connections | Set `max_live_concurrency` on the source. Live reads beyond the cap wait for a permit; replica reads are exempt. |
| Apply resource queues to a user group | Set `rate_limit` on a role. The `rate_limit.requests_per_second` field throttles request volume from that role. |
| Limit result size per user tier | Set `max_rows` on a role. A LIMIT is injected on every result, even when the query names none. |
| Decide which warehouse a copy lands in | Set [`region`](regions.md#region-homing-a-table-names-its-home) on the table. Only the named region builds and refreshes the replica; every other region reads from it in place. |
| Write a transformation job | Define a [materialized view](configuration.md#materialized-views). A role with `create_view` defines it; one with `approve_view` approves it. The engine builds and refreshes it through the same governed pipeline. |

The engine does the movement. No pipeline code runs outside the platform.

## Managing load

Each freshness setting is a trade-off between how current the data is and how much load lands on the source.

By default, a source is read live. Every query hits it directly, so data is as fresh as the source allows — and every request adds load. For a small table queried occasionally this is fine.

**Replicate** keeps a copy in the engine's store. After the initial build, only scheduled refreshes hit the source; queries read the copy. Staleness is the refresh interval, governed by `cache_ttl` and the source's change signal. `replicate: 0` (Always) copies every table in the source; Hot-N promotes a table automatically once traffic passes the threshold. When a source needs insulation from query load, replicate it.

**Load-protected** goes further. The source is never read live at all. Queries always serve the last materialized snapshot, and the scheduler refreshes it only on its gates — `off_peak_window`, a `cache_ttl` cadence, or a change signal. At least one gate is required. For fragile or rate-limited upstream systems, `load_protected` keeps the query path entirely away from the source.

**Response cache.** When `cache_ttl` is set and a query opts in with `@cached`, repeat queries are answered from cache without touching any source. The result is at most `cache_ttl` seconds old.

**Concurrency cap.** `max_live_concurrency` on a source limits live reads to N at once, across all instances sharing one Redis. Reads beyond the cap wait for a permit. Replica reads are exempt.

**Per-role caps** let you apply different limits to different populations of users:

| Setting | What it limits |
| --- | --- |
| `rate_limit.requests_per_second` | Requests per second from one role, across all instances |
| `max_rows` | Rows in any single result — a LIMIT injected by the engine |

See [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides) for the full precedence rules.

## What stays with the org admin

The org admin controls the settings above, plus who holds which role.

- Which sources are connected
- `replicate`, `cache_ttl`, `load_protected`, `off_peak_window`, `max_live_concurrency`, and `region` on each source and table
- Role definitions (capabilities, parents) and role assignments
- Outbound syncs, package registries, and AI model configuration

Residency grants — `data_residency` capability and `residency_values` on roles — belong to the data steward, who decides what each role may see. The org admin controls placement (a table's or source's home region); the steward controls which regions a role may be assigned.

## Delegating registration work

In a larger organization the org admin does not need to register every source and table personally. Two capabilities open the registration surfaces:

| Capability | What it allows |
| --- | --- |
| `source_registration` | Register, change, and remove data sources |
| `table_registration` | Register, change, and remove tables from a source |

Assign both to a data engineer role and the team can connect sources and configure `replicate`, `cache_ttl`, `load_protected`, and `region` on their own. The org admin retains role assignment, so a data engineer cannot grant themselves new access or raise another member's role.

## Three layers at a glance

The [Access Model — Three layers of administration](access-model.md#three-layers-of-administration) page describes all three layers. For a data engineer coming from a warehouse, the most relevant split is between the org admin, who sets up sources and governs load, and the data steward, who registers tables and controls data access.

- The **platform admin** sets up infrastructure. Capabilities enforce its boundary with the org: no data access, no org members, no model.
- The **org admin** connects sources, manages load, sets placement, defines roles, and assigns members.
- The **data steward** registers tables, defines what they mean, and controls who may see which columns and rows.
