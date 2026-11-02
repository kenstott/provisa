# How to Scale Provisa

The Provisa config is a single declaration of the enterprise data estate, and all Provisa instances coordinate from it. Every process reads the same model from the same control plane.

---

## Process modes

Every Provisa process starts in one of three modes, set at launch and fixed for the process's lifetime.

### `every` (the default)

A process in `every` mode serves data requests, runs background work and serves the admin UI. A single-node deployment runs one `every` process.

```bash
provisa run
# same as:
provisa run --mode every
```

### `query`

A `query` process serves data requests and the admin UI. It does not run background work.

```bash
provisa run --mode query
```

### `coordinator`

A `coordinator` process runs background work and serves the admin UI. Data requests arriving at `/data` or over Arrow Flight are refused with HTTP 503 before reaching a route; the refusal names the transport and tells the caller to use a `query` or `every` node.

```bash
provisa run --mode coordinator
```

---

## Scaled topology

Start with one `every` process. When query load grows, split the roles.

```
  clients ───► load balancer ─────► query node
                               ├──► query node
                               └──► query node (or direct)

  background work ────────────────► coordinator node
```

**Coordinator node.** The coordinator runs the admin UI, the scheduler and all background work. Scale it vertically first. More CPU and memory let it carry more concurrent background jobs before adding a second node makes sense.

Background work is claimed per-job. Replica builds use session advisory locks on the control plane, scoped to one build per replica per region, with engine and host slot limits. View refreshes take a per-view lease via an atomic conditional UPDATE. Because neither mechanism needs a global leader, more coordinator nodes claim different jobs in parallel with no extra configuration. When background load grows past one coordinator, add more.

**Query nodes.** Query nodes are CPU-bound. Each node governs and routes a request, compiles it and encodes result rows in batches as the engine streams them. For large result sets, encoding shifts the cost to the network. Memory per request stays bounded because results always stream. Scale query nodes by count.

---

## Client addressing

The simplest setup puts query nodes behind a load balancer at one address. All clients use it.

A node or pool with its own address works alongside the shared pool. Give it a DNS name and point specific clients straight at it. Several pools, each with its own address, are also an option. Adding pools gives more capacity and more addressing flexibility. Workload separation belongs to regions, described next.

The coordinator answers HTTP at its own address. Point admin-UI traffic there to keep it off the query pool.

---

## Workload separation: regions

A region is an instance of Provisa with a dedicated engine and its own address, sharing the same model and control plane as every other region.

Declare a `provisioning` region to isolate replica builds and view refreshes. Give it a large engine and its own address. Provisioning work runs as coordinator background work in that region; other nodes in that region claim builds and refreshes independently.

```yaml
platform:
  regions:
    - id: main
      address: provisa.example.com
    - id: provisioning
      address: provisioning.provisa.example.com
```

```bash
# main region: query nodes for user-facing requests
provisa run --mode query --region main

# provisioning region: coordinator nodes for replica builds and view refreshes
provisa run --mode coordinator --region provisioning
```

The org's config selects its regions and assigns each a store configuration: engine, replica storage, cache, state store and request record. A table may be pinned to a region; a table pinned to none may be replicated in every region.

From the user side, `provisioning.provisa.example.com` is just an address. The deployment names it whatever fits the team's language — it could be `refresh.internal` or `etl.provisa.example.com`.

For intra-engine isolation when Trino is the engine, Trino's per-org resource groups apply. See [Deployment](deployment.md#kubernetes--helm) for the configuration.

---

## Sizing summary

| Node type | Scale by | Size by |
|---|---|---|
| Query node | Count | CPU (network for large results) |
| Federation engine | — | Memory |
| Coordinator node | Count (jobs claimed per-job) | Background job volume |
