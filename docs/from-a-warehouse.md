# Coming from a data warehouse

In a traditional data warehouse, a data engineering team operates a pipeline. The jobs look like this:

- Extract jobs pull data from each source.
- Load jobs push copies into the warehouse.
- Refresh schedules decide how often those copies are updated.
- Environment copies stand up development and staging databases.
- Transformation jobs turn raw tables into the views and aggregates analysts consume.

Provisa handles the same work as declarations. You write down what you want; the engine decides when to move data and where to put it.

The decisions live in the [org admin's layer](access-model.md#three-layers-of-administration). In a larger organization the org admin can delegate registration work to a data engineer role, while keeping role assignment and residency grants.

## The same jobs, as declarations

| What you do in a warehouse | What you declare in Provisa |
| --- | --- |
| Write an extract job to pull a source | Connect the source. See [`source_registration`](configuration.md#capabilities). |
| Write a load job to copy rows | Set [`replicate`](configuration.md#the-replicate-setting) on the source or table. `0` means always copy (Always); `-1` means never copy and always read live (Never); `N` above 0 means copy once the table is busy (Hot-N). |
| Schedule refreshes | Set [`cache_ttl`](configuration.md#cache) on the source or table. The engine refreshes the replica when the copy is older than the TTL. A table with no TTL and no change feed is built once and refreshed only when a change signal arrives. |
| Decide which warehouse a copy lands in | Set [`region`](regions.md#region-homing-a-table-names-its-home) on the table. Only the named region builds and refreshes the replica; every other region reads from it in place. |
| Write a transformation job | Define a [materialized view](configuration.md#materialized-views). A role with `create_view` defines it; one with `approve_view` approves it. The engine builds and refreshes it through the same governed pipeline. |

The engine does the movement. No pipeline code runs outside the platform.

## What stays with the org admin

The org admin controls the settings above, plus who holds which role. Specifically, the org admin keeps:

- which sources are connected
- `replicate`, `cache_ttl`, and `region` on each source and table
- residency grants (`data_residency` capability and `residency_values` on roles)
- role assignment: which members hold `source_admin`, data engineer, or any other role

## Delegating registration work

In a larger organization the org admin does not need to register every source and table personally. Two capabilities open the registration surfaces:

| Capability | What it allows |
| --- | --- |
| `source_registration` | Register, change, and remove data sources |
| `table_registration` | Register, change, and remove tables from a source |

Assign both to a data engineer role and the team can connect sources and configure `replicate`, `cache_ttl`, and `region` on their own. The org admin retains role assignment and residency grants, so a data engineer cannot grant themselves new regions or raise another member's access.

## Three layers at a glance

Provisa divides administration into three layers. The [Access Model — Three layers of administration](access-model.md#three-layers-of-administration) page describes them in full. The short version for a data engineer:

- The **platform admin** sets up the infrastructure your org's data runs on.
- The **org admin** connects sources, sets data placement, and assigns roles. Pipeline decisions live here.
- **Specialized roles** (including a data engineer role the org admin defines) build on top: views, quality checks, relationships, synthetic data.
