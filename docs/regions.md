# Regions

A region is a logical definition and a cluster bound to it. The cluster has its own engine and its own stores; the definition says what the region is for.

Sources, tables, relationships, roles, row and column rules, views, and freshness settings are declared once and are the same in every region. Engine and stores are per-region.

Region homing assigns a table to a specific cluster. Its replica is built and refreshed there, and reads from other regions go through that replica. The row and column rules you write decide what readers in other regions may see.

## A region is an engine and its stores in a place

Each region keeps its data in six store roles.

| Store | What it holds |
| --- | --- |
| `engine` | The federation engine that runs statements in this region |
| `replicas` | Replicas of tables homed here, and of unhomed tables this region copies |
| `views` | This region's copies of materialized views |
| `cache` | Cached responses |
| `state` | Build and refresh state |
| `record` | The record of each request served here |

A change to the model reaches every region by the same stamp. Each region then converges on it on its own schedule.

## Declaring and assigning regions

**Platform regions** are declared on the Platform page by the platform admin. Each region has an `id`, a `description`, one or more `purposes`, and an optional list of `data_addresses` for the UI's data-node picker (covered below). The id is lowercase and up to 40 characters. Description and purposes appear wherever the region is listed, in the UI and in error messages.

Five purposes describe why a region exists:

| Purpose | Meaning |
| --- | --- |
| `residency` | Data must stay in a particular jurisdiction |
| `workload` | Separates a category of workload from others |
| `latency` | Placed near users or sources for lower response time |
| `resilience` | A failover or recovery location |
| `dedicated` | Infrastructure reserved for one organization, for its own sovereignty |

Use these values to describe what each region is for. The row and column rules you write are where the distinction lives; `residency` and `workload` regions route and store identically.

**Org regions** are granted by the platform admin, also on the Platform page. The platform admin grants one or more platform regions to an organization and sets the store for each role in each granted region. Every new organization receives a default grant, so a self-serve organization starts with a working region without any manual setup.

The org admin sees its regions and their stores read-only on **Admin > Regions**. Within those granted regions the org admin can set where tables and sources are homed, and can configure `data_residency` grants on roles and the default region for newly registered objects.

A node starts in one region with `provisa run --region <id>` and serves that region for every organization that uses it. With regions declared, a node started without `--region` refuses to start and prints the available region ids.

```yaml
platform:
  regions:
    - id: eu
      description: "EU data residency cluster, Frankfurt"
      purposes: [residency, latency]
    - id: us
      description: "US operations cluster, Virginia"
      purposes: [workload, latency]

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

When the configuration loads, it is refused, naming the problem, if the regions do not hold together. A region must name a declared store for each role. Its `replicas` and `views` must be the same store, and its engine store must name an engine `kind`. With more than one platform region, an embedded engine file is refused because the other regions cannot reach it.

Several regions may share the same database or object store. Every schema and key a region keeps carries the region, so stores do not collide.

A deployment that declares no regions has one implicit region and shows regions nowhere. No region field appears in the UI and no `--region` flag is needed. A configuration that names a region is refused.

**In a demo sandbox** the Platform page is visible and read-only, with a note explaining that regions cannot be changed there. Open it to see how regions are defined before the residency queries.

## Admin > Federation

When an organization uses regions, **Admin > Federation** shows one tab per region. Each tab is the engine configuration form for that region's engine store. With no regions the page has no tabs and edits the single engine as before.

## Where a coordinator's admin UI sends data requests

A node started with `provisa run --mode coordinator` serves the admin UI but no data requests. A UI it serves therefore sends every data request (everything under `/data`) to a data node you choose, with your sign-in carried along. Choose it under **Data node** in the navigation bar. The menu lists the addresses the coordinator offers for the organization's region, and you can type another `http` or `https` address instead. Your choice is kept in this browser for the organization. A UI served by an `every` node, the default mode, sends data requests to its own address and shows no **Data node** menu.

List the addresses to offer on each platform region as `data_addresses`. Each is an absolute `http` or `https` URL with no query string, such as a load balancer or a single-node address.

```yaml
platform:
  regions:
    - id: eu
      description: "EU data residency cluster, Frankfurt"
      purposes: [residency, latency]
      data_addresses:
        - "https://data.eu.provisa.example"
        - "https://eu-node-2.provisa.example:8001"
```

A region with no declared addresses offers its registered live `query` and `every` nodes instead. Each node is listed at its `PROVISA_NODE_URL`; nodes without one are not offered. In a deployment with no declared regions, the coordinator offers its own nodes the same way.

Data requests then cross from one address to another. A data node answers the browser's cross-origin preflight before it checks sign-in, and its refusals stay readable across origins.

## Region homing: a table names its home

A table may name one of its organization's regions as its home. Only the home region builds and refreshes the table's replica, and only it may keep the table hot. From every other region, reads go to the home region's replica at the store address the model names. The reader's row and column rules go into that statement; filtering happens in the home region, and only what the reader may see crosses.

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```

A few consequences follow:

- No other region reads the table's source; they read only the home's replica. Sources replicated to protect them from load stay protected.
- When the replica has not been built, or the home region cannot be reached, the request is refused, naming the home region. Only the home region's replica can answer the read.
- A homed table with `replicate` Never is read live from its source by every region, as any unreplicated table is.
- A table's hot count is its home region's traffic only.
- A materialized view may name a region too. It is then built only there, and every other region reads that copy in the same way.

To read another region's replica, the region's engine must be `duckdb`, `motherduck`, `pg`, `trino` or `trino-byo`. The home region's `replicas` store must be a PostgreSQL server. The configuration names both when they do not fit.

A source may carry a region name. That region pre-fills the form when you register a new table, and that is all it does. Changing it later moves no existing table.

## Unhomed tables are copied where they are read

A table that names no region may be copied into every region. When its `replicate` setting calls for a copy, the region the reader connected to builds one in its own store. For materialized views naming no region, each region builds its own copy in its view storage.

Each region builds materialized views as the organization's administrator acting in that region. So each region's copy holds only what an administrator there may see.

## Why use regions

### Performance

For performance, declare no cross-region rules. The model is the same as for a single region; region homing decides where work happens.

- **Latency.** Readers connect to the region near them; its engine and local copies answer quickly.
- **Recency.** A table homed where it changes is refreshed there, close to its source.
- **Engine fit.** Each region runs its own engine. Match it to its data center and hardware profile.
- **Scale.** Two identical regions in different data centers, homing no tables, each serve the users nearest them and share the load.

**Example: two identical regions.** Declare `east` in US-East-1 and `west` in US-West-2, both running Trino over object storage. Each region serves the readers nearest it. It builds its own copies and caches their responses. There is one refresh schedule; both regions start refreshing on it, but their engines may not finish at the same time. At a given moment the two are not guaranteed to be in the same state; a reader moved from one region to the other may see data of a different freshness. Failover between regions is the load balancer's job.

### Cost

Cost regions also carry no cross-region rules. Keep a region on premises for the tables and readers that live there and run another in the cloud for the rest. Or run a large engine in one region for occasional heavy work and a small one in another for everyday reads; readers choose by which data address they connect to. Homing a large table to one region avoids copying it everywhere. Readers in other regions get only their filtered rows of the home replica.

### Sovereignty

Sovereignty regions do carry cross-region rules. A subsidiary homes its tables in its own region, keeping them processed and stored on its own infrastructure. What the rest of the group reads is controlled by ordinary row and column rules written on attributes that say where the reader, or the node doing the work, is.

`current_setting('provisa.region')` is the region of the node answering the request, when the platform declares regions. A row filter written on it keeps a region's rows in that region.

A member's `region` attribute makes them hold the derived roles `region` and `region__<value>`. A mask or grant entry can require `analyst+region__eu`. A node's own background work carries its region the same way. See [Roles: granted, derived and meta](security.md#roles-granted-derived-and-meta).

For a homed table, personal data is stored and processed in the home region, and a reader elsewhere receives only what their rules allow, filtered at home. For an unhomed view, each region's copy holds what an administrator in that region may see. The copies can differ. Data permitted in one jurisdiction may not be held in another; the EU region's copy of an aggregate view can legitimately return different figures than the US region's.

## Who may home a table

Changing any registered object's home region requires the right `data_residency` in addition to owning the object through its domain. The grant lists the org's regions it applies to, and whether it also covers "no region". A move between two specific regions needs a grant that covers both, and a save outside the grant is refused naming the value it does not cover.

The right exists only when the platform declares regions. Domain ownership still decides who may edit everything else about the object. A region confers no right of its own, so an organization that wants a region's tables run by that region's people gives them domains of their own.

You can also set a table or view to **draft**. A draft is out of service and no reader is offered it. A table registered through the admin starts as a draft. Preview runs on the table's homed region. Tables with no region are previewed wherever the steward is. The copy that preview lands stays in that region, ready for the table's release. When a table's region changes, every copy another region built before the change is retired and dropped. Draft gives a move between regions a clean gap. The first region's side sets it to draft. The second region's holder claims it by setting their region, and the table is served nowhere in between.

See also the [two-region demo](regions-demo.md).
