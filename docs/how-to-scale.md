# How to Scale Provisa

The Provisa config is a single declaration of the enterprise data estate, and all Provisa instances coordinate from it. Every process reads the same model from the same control plane.

---

## Process modes

Every Provisa process starts in one of three modes, set at launch and fixed for the process's lifetime.

### `every` (the default)

A process in `every` mode serves data requests and runs background work — replica builds, view refreshes and scheduled jobs. It serves the admin UI and all non-data HTTP routes too.

Start in `every` mode by launching with no `--mode` flag. A single-node deployment runs one `every` process.

```bash
provisa run
# same as:
provisa run --mode every
```

### `query`

A `query` process serves data requests. It serves the admin UI and all non-data HTTP routes. It does not run background work.

```bash
provisa run --mode query
```

### `coordinator`

A `coordinator` process runs background work. It serves the admin UI and all non-data HTTP routes. Data requests arriving at `/data` or over Arrow Flight are refused with HTTP 503 before reaching a route; the refusal names the transport and tells the caller to use a `query` or `every` node.

```bash
provisa run --mode coordinator
```

---

## Scaled topology

Start with one `every` process. When query load grows, split the roles into a coordinator node and a pool of query nodes.

```
  admin clients ────────────────────────┐
  data clients  ───► load balancer ─────┼──► query node
                                        ├──► query node
                                        └──► query node
  heavy clients ───────────────────────────► query node (direct)

  background work ─────────────────────────► coordinator node
```

**Coordinator node.** One coordinator handles background work for most deployments; a second gives redundancy. Scale it vertically first. It runs the scheduler and holds background job slots; more CPU and memory let it take on more concurrent work before a second node adds value.

Background jobs are claimed per-job. Replica builds hold session advisory locks on the control plane, one build per replica per region, with engine and host slot limits. View refreshes run a per-view atomic conditional UPDATE with a lease. Neither mechanism needs a global leader, so two coordinators claim different jobs in parallel with no setup beyond running them. When background load outgrows one coordinator, add a second.

**Query nodes.** Query nodes are CPU-bound. Each node governs and routes a request, compiles it, then encodes result rows in batches as the engine streams them. Results stream in batches, so memory per request stays bounded. Scale query nodes by core count and by the number of nodes. Size the federation engine for memory.

---

## Client addressing

Query nodes are identical. The simplest setup is a pool behind a load balancer at one address; all clients use it.

For clients that need a dedicated path — long-running analytical queries that hold connections for minutes, for example — run a query node with its own DNS name and point those clients straight at it. They avoid competing with other traffic for load-balancer connections.

Multiple pools each with a distinct address are also an option, each sized for the traffic it carries. Routing clients across pools is a capacity and addressing decision; workload isolation at the engine and data level is a region concern, described next.

The coordinator node answers HTTP at its own address. Point admin-UI traffic there if you want it off the query pool.

---

## Workload separation: regions

A region is an instance of Provisa with a dedicated engine and its own address, sharing the same model and control plane as every other region. Declaring a region for a workload gives that workload its own engine, sized for the work, at its own hostname.

Regions are declared in `platform.regions` in the config:

```yaml
platform:
  regions:
    - id: transactional
      address: transactional.provisa.example.com
    - id: bi
      address: bi.provisa.example.com
    - id: provisioning
      address: provisioning.provisa.example.com
```

Each region's nodes launch with `--region <id>`:

```bash
# transactional region: many small, low-latency queries and mutations
provisa run --mode query --region transactional

# BI region: large analytical scans; size the engine for memory
provisa run --mode query --region bi

# provisioning region: replica builds and view refreshes as coordinator work
provisa run --mode coordinator --region provisioning
```

The org's config selects the regions it uses and gives each a store configuration covering its engine, replica storage, cache, state store and request record. A table may be pinned to a region; a table pinned to none may be replicated in every region.

From a client's perspective, `transactional.provisa.example.com` and `bi.provisa.example.com` are addresses like any other. The deployment names them to match the team's language.

For intra-engine isolation when Trino is the engine, Trino's per-org resource groups apply. See [Deployment](deployment.md#kubernetes--helm) for the configuration.

---

## Sizing summary

| Node type | Scale by | Size by |
|---|---|---|
| Query node | Core count and number of nodes | CPU |
| Federation engine | — | Memory |
| Coordinator node | Number of nodes (jobs are claimed per-job) | Background job volume |
