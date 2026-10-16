# Regions

Provisa gives you two primitives for placing data: a **region**, and **region homing**, which means creating a region and assigning a table to it. Neither one is a policy by itself. What you use them for falls into three groups:

- **Performance.** Latency to the people reading, how recent their copy is, and the fit of the engine: where its data center is, what kind of storage it reads, how much CPU and memory it has.
- **Cost.** Where work runs and what it runs on: owned hardware or cloud, a large engine or a small one, and what moving data between places costs (egress).
- **Sovereignty.** Who controls data and where it may go: the law's residency rules (where personal data may be processed and where it may be stored), and an organization's own lines of control, such as a business unit or subsidiary that keeps its data and infrastructure under its own control.

The mechanics are the same for all three. Only the rules you write on top differ.

## A region is an engine and its stores in a place

A region is one place where Provisa does work and keeps what the work leaves behind. Each region has its own engine and its own stores. (REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| Store | What it holds |
| --- | --- |
| `engine` | The federation engine that runs statements in this region |
| `replicas` | Replicas of tables homed here, and of unhomed tables this region copies |
| `views` | This region's copies of materialized views |
| `cache` | Cached responses |
| `state` | Build and refresh state |
| `record` | The record of each request served here |

One model governs every region. Sources, tables, relationships, roles, row and column rules, views and freshness settings are declared once, in one model store, and every region serves that same model, so definitions cannot drift apart between regions. A change reaches every region by the same stamp, and each region then converges on it. (REQ-1914, REQ-1922)

The platform declares the physical regions its nodes run in, with the address each region answers at. Each organization's model selects which of those regions it uses and names its own store for each role in each region. A node starts in one region with `provisa run --region <id>` and serves that region for every organization that selects it. With regions declared, a node started without `--region` refuses to start and lists the regions it could serve. (REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```

[inferred: the URLs and secret names are placeholders; the shape follows `PlatformRegion`, `StoreConfig` and `OrgRegion` in `provisa/core/regions.py`]

When the configuration loads, it is refused, with the reason named, if the regions do not hold together. A region must name a declared store for each role. Its `replicas` and `views` must be the same store, and its engine store must name an engine `kind`. With more than one platform region, an embedded engine file is refused, because the other regions cannot reach it. [tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

Several regions may share one database instance, one Redis or one object-store bucket. Everything a region keeps carries the region in its name, so their stores do not collide. (REQ-1922)

A deployment that declares no regions has one implicit region and shows region nowhere. There is no region field, no `--region` flag, and a configuration that names a region is refused. (REQ-1922)

## Region homing: a table names its home

A table may name one of its organization's regions as its home. Its home is the only region that builds and refreshes its replica, and the only one that may keep it hot. Every other region reads the table from the home region's replica, in place, at the address the model gives. The reader's row and column rules go into that statement, so the filtering happens in the home region and only what the reader may see crosses. (REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```

Some consequences:

- No other region reads the table's source; they read only the home's replica. A source you replicate to protect it from load stays protected.
- If the replica has not been built, or the home region cannot be reached, the request is refused, naming the home region. The source is not read live instead.
- A homed table that is not replicated, with `replicate` Never or a setting that has not yet called for a copy, is read live from its source by every region, as any unreplicated table is.
- A table's hot count is its home region's traffic only. (REQ-239)
- A materialized view may name a region too. It is then built only there, and every other region reads that copy, exactly as for a homed table.

A region that reads another region's replica needs an engine that can read it: `duckdb`, `motherduck`, `pg`, `trino` or `trino-byo`. The home region's `replicas` store must be a PostgreSQL server. The configuration names both when they do not fit. [tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

A source may name a region as well, but that region only fills in the form when you register a new table of the source. It decides nothing about where data lives, and changing it later moves no table. (REQ-1921)

## Unhomed tables are copied where they are read

A table that names no region may be copied into every region. When its `replicate` setting calls for a copy, the region a reader connected to builds one in its own store. A materialized view that names no region is built in every region, each copy in that region's view storage. (REQ-1921)

Adding a region therefore means more copies and more refresh work. It never means more governance. One model governs every copy, and the same row and column rules apply in every region. A view's build in a region is governed as the organization's administrator acting in that region, so each region's copy holds what may be kept there. (REQ-1921, REQ-1922)

## Three reasons to use regions

### Performance

Regions used only for performance declare no cross-region rules. The tables, roles and rules are the ones you would write for a single region, and region homing decides where work happens:

- **Latency.** Readers connect to the region near them, and its engine, cache and copies are local to them.
- **Recency.** A table homed where it changes is refreshed there, close to its source.
- **Engine fit.** Each region runs its own engine, so you can match an engine to a place: the data center it sits in, the storage it reads, and the CPU and memory it has.

**Example: two identical regions.** Declare `east` and `west` in two data centers, each with the same engine kind and the same kinds of store, and home no tables. Each region serves the readers nearest it: it builds its own copies of what they read and caches their responses. The two can sit behind one load balancer and share the load. There is one refresh schedule, the model's, and both regions start refreshing their copies on it, but their engines may not finish in the same time. So at a given moment the two are not guaranteed to be in the same state, and a reader moved from one region to the other may see data of a different freshness. Provisa does not move a client to the other region when one stops; where a client is sent is the load balancer's decision. [inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### Cost

Cost regions declare no cross-region rules either. They put work where it is cheapest to run:

- **Owned hardware and cloud.** Keep a region on premises, on hardware you already own, for the tables and readers that live there, and run another region in the cloud for the rest.
- **Engine size.** Run a large engine in one region for occasional heavy work, and a small one in another for everyday reads. Readers choose the region by its address.
- **Egress.** Readers in other regions get only the filtered rows of a homed table's replica, and a large table homed in one region is not copied into every region.

### Sovereignty

Regions used for sovereignty do declare cross-region rules, whether the reason is the law or the organization: a subsidiary that homes its tables in its own region keeps them processed and stored on its own infrastructure, and decides through these rules what the rest of the group reads. They are ordinary row and column rules, written on attributes that say where the reader, or the node doing the work, is:

- `current_setting('provisa.region')` is the region of the node answering the request, when the platform declares regions. A row filter written on it keeps a region's rows in that region. (REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- A member's `region` attribute makes them hold the pseudo-roles `region` and `region__<value>`, so a mask or grant entry can name `analyst+region__eu`. A node's own background work carries its region the same way. (REQ-2057) See [Roles: granted, pseudo and meta](security.md#roles-granted-pseudo-and-meta).

Together with homing, these rules decide what crosses a border. A homed table's personal data is stored and processed in its home region, and a reader elsewhere receives only what their rules allow, filtered at home. An unhomed view built in each region holds what an administrator in that region may see, so the copy in one region can include rows that the copy in another region leaves out. (REQ-1921)

## Who may home a table

Setting, changing or removing the region of a table, a source or a materialized view moves data across a border. It needs the right `data_residency` in addition to owning the object through its domain. The grant names the values it covers: the organization's regions, and "no region". Moving an object between two regions needs a grant that covers both. A save outside the grant is refused, naming the value it does not cover. (REQ-1921)

The right exists only when the platform declares regions. Domain ownership still decides who may edit everything else about the object. A region confers no right of its own, so an organization that wants a region's tables run by that region's people gives them domains of their own. (REQ-1921)

A table or view can also be set to **draft**: out of service, with no copy kept in any region. A table registered through the admin starts as draft. A move between regions can pass through draft, so no data is held or read anywhere in between: the first region's side sets draft, and the second region's holder claims the object by setting their region. (REQ-1921)

See also the [two-region demo](regions-demo.md).
