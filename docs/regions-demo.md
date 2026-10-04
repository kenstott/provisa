# Two-region demo (eu + us)

A Docker-free demo that runs two Provisa nodes — one in region `eu`, one in region `us` — on the
embedded local platform, so the region-specific admin UX is visible end to end. Both nodes share one
embedded Postgres (the model store and both regions' stores), each runs its own DuckDB engine, and a
single fakeredis TCP server is the cache.

> Status: scaffolded, not yet provable. Two regions share one Postgres, so it depends on
> region-qualified schema and cache names (so the regions do not collide). Do not run the launch
> until that lands; `scripts/launch-demo-regions.sh --check` validates the layout without starting
> anything.

## Start it

```
scripts/launch-demo-regions.sh            # start (Ctrl-C or --stop to end)
scripts/launch-demo-regions.sh --reset    # wipe its data dir first
scripts/launch-demo-regions.sh --stop     # stop everything it started
```

It uses its own instance directory `~/.provisa/demo-regions` and its own ports. It never touches
local-dev (API 8001, UI/vite 5173, MCP 8009, pgwire 5439, `~/.provisa/demo`) or demo-snowflake-live
(8200/3200).

| Node | UI | API |
|------|------|------|
| eu | http://127.0.0.1:8310 | http://127.0.0.1:8110 |
| us | http://127.0.0.1:8320 | http://127.0.0.1:8120 |

Open the **eu** UI at http://127.0.0.1:8310 and the **us** UI at http://127.0.0.1:8320 side by side.

## What to click

- **Region field and its default.** On either node, open **Sources** → add a source, or **Tables** →
  register a table. The region field defaults to the node's own region (`eu` on the eu node, `us` on
  the us node) for a table, and to the connected region for a view.
- **The region selector and its hidden count.** On **Sources**, **Tables**, **Materialized Views**,
  or **Cache → Hot** (hot tables / replica status), the region selector sits in the header. It starts
  at the node's region, showing objects homed there plus objects with no region. Switch it to another
  region or to "all regions"; the line next to it says how many rows the current selection hides. Each
  row shows its region in the region column. Searching by name crosses regions and labels each result.
- **A cross-region read.** On the **us** node, query the `intake_eu` table (homed in eu). The us node
  reads it from eu's replica store — the data processed and kept in eu, read from us under one model.
- **The 503 refusal when a home region is stopped.** Stop just the eu node (leave us up), then on the
  us node read `intake_eu` again: the read is refused by name, because eu's store is not reachable.
- **data_residency refusals and draft.** (Pending replica-layout-2's data_residency and draft work;
  this section and the demo's draft table are added when those land.)
