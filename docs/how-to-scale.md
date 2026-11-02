# How to Scale Provisa

A single Provisa process handles both user-facing queries and background work such as replica builds. That single-process topology is the right starting point: it needs no coordination overhead and you can run it anywhere with one command.

When query load grows past what one process handles, you split those two responsibilities across dedicated nodes. This page explains the topology, how to launch each process mode, and how regions fit in.

---

## Process modes

Every Provisa process starts in one of three modes. The mode is set at launch and does not change while the process runs.

### `every` (the default)

A process in `every` mode does both jobs: it serves data requests over all transports (HTTP, Arrow Flight, gRPC) and runs the background work a coordinator is responsible for, including replica builds and scheduled tasks.

Start a process in `every` mode by launching it with no `--mode` flag, or by setting `PROVISA_MODE=every`. A single-node deployment runs exactly one `every` process.

```bash
provisa run
# equivalent:
PROVISA_MODE=every provisa run
```

### `query`

A process in `query` mode serves data requests only. It never runs replica builds or scheduled work.

```bash
provisa run --mode query
```

Use `query` nodes behind a load balancer when you need more query throughput than a single process provides. You can add as many as you need; they are stateless with respect to query routing and scale horizontally without coordination.

### `coordinator`

A process in `coordinator` mode runs background work only. It refuses all data requests at the transport boundary, before any data is read, and returns an error that names the transport and tells the caller to direct its request to a `query` or `every` node instead.

```bash
provisa run --mode coordinator
```

You need at least one `coordinator` node in a scaled deployment so that replica builds and scheduled tasks continue while `query` nodes handle traffic.

---

## Scaled topology

Start with one `every` process. When you need more capacity, split into a dedicated coordinator tier and a pool of query nodes:

```
                 ┌─────────────────┐
  clients ──────▶│  load balancer  │
                 └────────┬────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
      query node     query node     query node
      (data only)   (data only)    (data only)

      coordinator   coordinator
      (background)  (background)
```

Query nodes receive all user-facing traffic through the load balancer. Coordinators run in the background; clients never dial them directly. Nothing in Provisa requires coordinators to coordinate with each other, so you can run more than one for redundancy without any extra configuration.

### Launching query nodes

```bash
provisa run --mode query
```

Add as many as you need. Put them behind any HTTP load balancer that health-checks `GET /health`. All query nodes read configuration from the shared PostgreSQL control plane, so no node-specific config is required.

### Launching coordinator nodes

```bash
provisa run --mode coordinator
```

One coordinator is enough for most deployments. Run a second one for redundancy; background work is claimed by exactly one coordinator at a time, so two coordinators do not duplicate work.

### Kubernetes (Helm)

Set `PROVISA_MODE` in the pod's environment. The chart's `provisa.env` map is the right place:

```yaml
provisa:
  env:
    PROVISA_MODE: query
```

Deploy a separate Deployment for coordinators with `PROVISA_MODE: coordinator` and `replicaCount: 1` (or 2 for redundancy).

---

## Regions

When a platform declares regions, each node is assigned to one at launch. A node refuses to start if the platform declares regions but no `--region` is given, or if a `--region` is given but the platform declares none.

```bash
provisa run --mode query    --region us-east-1
provisa run --mode coordinator --region us-east-1
```

Set the region via environment variable for deployments that bypass the CLI:

```bash
PROVISA_MODE=query PROVISA_REGION=us-east-1 <launch command>
```

A platform with no declared regions runs with a single implicit region. In that case, `--region` is an error; omit it.

For a full explanation of how regions map to data and organizations, see [Regions](regions.md).

---

## Summary

| Goal | Mode | Notes |
|---|---|---|
| Single-process development or small production | `every` | The default; no flag needed |
| Horizontal query throughput | `query` | Add nodes behind a load balancer |
| Background work in a scaled cluster | `coordinator` | One is enough; two gives redundancy |
