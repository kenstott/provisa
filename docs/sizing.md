# Sizing and Tuning

The sizing tool measures your own Provisa deployment under your own workload. From those measurements it writes a sizing and tuning guide: how many physical cores and worker processes the workload needs, and what each setting buys. Every figure in the guide comes from a measurement, so after a change to the deployment or the workload you rerun the tool and regenerate the guide.

The tool lives in the Provisa repository under `demo/named/perf/bench/`. Run every command below from that directory with the repository's Python environment (`../../../../.venv/bin/python3`).

## What it reports

A single sizing run is several benchmark runs against your deployment. Each varies one thing:

| Run | What changes | What it gives |
|---|---|---|
| Baseline | Nothing: one column, one row, no filter, no join, uncached | The base cost of a request |
| Knob runs | One request knob at a time: fields, filters, rows, same-source joins, cross-source joins | The cost each knob adds per unit |
| Cross-source join runs | The cross-source join values, once with both ends read live, once with both from replicas, once with one of each | The cost of a cross-source join by how its ends are read, and the break-even between live and replica ends |
| Cache runs | Every request opted into the response cache, then none | The cost of a cache hit and of a miss |
| Replication runs | A source read live, then from a replica | The rate above which a replica costs less |
| Worker runs | Worker processes and in-flight requests per worker | Workers per physical core, in-flight per worker, what a core delivers |

Knob runs, cache runs and replication runs are made per transport and per source, and once per federation engine you name. Worker runs are made per transport.

Cost here means Provisa's CPU time per request, in milliseconds. The knob runs are uncached, so a knob's cost never includes the cache's effect.

From the runs, `bench_report.py` fits a cost model per engine, transport and source: a base cost plus a cost per field, filter, row and join, with a cross-source join costed by how its two ends are read. When the engine runs as a server of its own and its container's CPU is measured, the engine gets a cost model too. `sizing_guide.py` turns the report into the guide. Given a contract that describes your workload, it also sizes that workload.

## Write a setup contract

The setup contract is one YAML file. It names your deployment's endpoints, the tables the benchmark may read, the shape of the requests, and the load. The benchmark checks the contract against the deployment before it runs. It refuses a table, column or join the deployment does not expose.

Tables and columns are named as your deployment registers them. What each transport calls them (SQL names, GraphQL fields, Cypher labels, REST paths, gRPC types) is looked up from the deployment when the run starts.

```yaml
version: 1

deployment:
  name: acme-prod
  endpoints:
    http: {base_url: "https://provisa.acme.internal"}
    pgwire: {host: provisa.acme.internal, port: 5439}
    bolt: {host: provisa.acme.internal, port: 7687}
    flight: {host: provisa.acme.internal, port: 8815}
    grpc: {host: provisa.acme.internal, port: 50051}
  # The credential is read from an environment variable; the secret never appears here.
  # kind: password (the variable holds the user's password) or token (a ready bearer token).
  credentials: {mode: env, kind: password, user: bench-user, env: ACME_BENCH_PASSWORD}
  # The roles requests are sent as, with the share of requests each gets.
  roles:
    - {role: analyst, weight: 0.8}
    - {role: regional_manager, weight: 0.2}

sources:
  sales-pg:
    # What this source's reads are measured under: live, or a replica refreshed every ttl_seconds.
    replication: {setting: live, ttl_seconds: 0}
    # Optional: a Docker container whose CPU is sampled as the source's CPU.
    monitor: {container: sales-postgres}
    tables:
      - schema: public
        table: orders
        weight: 1.0              # share of this source's requests that start at this table
        base_column: order_id    # the one column a baseline request selects
        columns:
          # A column with a domain can be filtered on; the filter's value is drawn from it.
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
          - {name: region, domain: {kind: set, values: [NA, EMEA, APAC]}}
          - {name: amount}
      - schema: public
        table: order_lines
        weight: 0.0              # reached only through the join below
        base_column: line_id
        columns:
          - {name: line_id}
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
    # Joins within the source, by registered table and column. The relationship must be registered.
    joins:
      - {left: "orders.order_id", right: "order_lines.order_id"}

# Joins across sources: "source:table.column".
cross_source_joins: []

# The rows a request returns when the rows knob does not apply.
base: {rows: 1}

# Each knob is the probability it applies to a request and the distribution its value is drawn
# from when it does. Distributions: constant {value}, uniform {min, max}, zipf {exponent, max},
# geometric {p, max}.
knobs:
  fields: {probability: 1, distribution: {kind: uniform, min: 1, max: 3}}
  filters: {probability: 0.5, distribution: {kind: constant, value: 1}}
  rows: {probability: 1, distribution: {kind: zipf, exponent: 1.2, max: 100}}
  joins_same_source: {probability: 0.2, distribution: {kind: constant, value: 1}}
  joins_cross_source: {probability: 0, distribution: {kind: constant, value: 0}}
  # How often a request repeats one sent shortly before it: the value is how many requests back.
  cache_repetition: {probability: 0.3, distribution: {kind: uniform, min: 1, max: 50}}
  # Share of requests that opt into the response cache. REST and JSON:API have no opt-in.
  cache: {probability: 0.6}
  # How a request's cross-source joins are chosen by how their two ends are read: both_live,
  # both_replica, or mixed (one of each), as the sources' replication settings above make them.
  # Probability 0 chooses joins whatever their class.
  join_replication: {probability: 0, weights: {both_live: 1.0}}
  # Share of requests whose table is in each source.
  source_weights: {sales-pg: 1.0}
  # auto lets the deployment choose the route; leave every source on auto to size a workload.
  route:
    sales-pg: {mode: auto, federated_probability: 0}

# The transports the benchmark runs, one after another. A transport's entry overrides any of the
# knobs above for that transport only; {} means no override.
transports:
  graphql: {}
  pgwire:
    knobs:
      rows: {probability: 1, distribution: {kind: uniform, min: 100, max: 1000}}

load:
  mode: closed_loop
  steps: [1, 16, 64, 256]   # concurrent connections, one step after another
  window_s: 30              # seconds per step
  processes: half_cores     # load-generator processes: an integer, or half the client's cores
  profile_requests: 0
  idle_window_s: 0          # seconds of no load after the ramp; see the replication runs below

seed: 0

# The workload to size: its request rate and each transport's share of it. The knobs above
# describe its requests. A benchmark run does not read this section.
workload:
  rate_per_s: 1500
  transport_weights: {graphql: 0.7, pgwire: 0.3}
```

The contract format is defined by `setup_contract.py` and `contract_parse.py`; an invalid contract is refused with a message that names the field. The `workload` weights must sum to 1, and each transport they name must be one the contract runs.

Describe the requests as they really are. The guide sizes exactly the requests this contract generates: the sizing draws them from the same seeded generator a run uses.

## Run it

### 1. Resolve the names once

Run the baseline contract once. The run asks the deployment what each transport calls your tables and writes `resolved-names.json` next to its results.

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` names the federation engine your deployment runs (`duckdb`, `pg` or `trino`). `--server-pid` is the server's process id; the run reads the CPU time of that process and its child processes (the workers). It reads the process directly, so it works only when the load generator runs on the server's host.

### Measuring the server's CPU from another host

Run the load generator on its own machine when you can: then it takes no cores from the server. To measure the server's CPU from there, start `server_cpu_agent.py` on the server's host and give the run its URL instead of `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

The agent answers one figure, the same one `--server-pid` reads: `GET /` returns `{"pid": ..., "cpu_seconds": ...}`, the cumulative CPU seconds of the server process and its workers. Every other path answers 404, and every other method is refused. Give a run one of `--server-pid` or `--server-cpu-url`, never both.

`--listen` is required: the agent has no default address. Bind it to a private address that only the load generator can reach, never a public interface, and stop it (Ctrl+C) when the runs are done.

### 2. Plan the runs

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: the server's physical cores, not its hardware threads. On Linux, `lscpu` shows cores per socket and sockets; multiply them. The tool never guesses this.
- `--shared-host`: `yes` when your sources or the load generator run on the server's host. They then compete with Provisa for its cores, and the guide says so.
- `--workers` and `--request-threads`: the worker processes and in-flight requests per worker to try. Give both or neither.
- `--replication` and `--ttl`: add the live and replica runs for each source, the replica refreshed every `--ttl` seconds. The cross-source join runs read from replicas need `--ttl` too.
- `--engines`: the federation engines to measure, `pg` by default. Every knob run is planned once per engine; the engine is set when the server starts, so each engine's runs need the server restarted with it.
- `--engine-container`: `ENGINE=CONTAINER` for an engine that runs as a server in its own container, whose CPU each run then reports. Leave out an engine that runs inside Provisa (DuckDB).

The plan writes one contract per run and a `manifest.json`. A run the deployment cannot serve, such as joins over gRPC or a table a transport does not expose, is listed in the manifest with the reason. It is never dropped silently.

The replica runs measure the replica's refresh cost during a window with no load. Set `load.idle_window_s` in your contract (for example `120`) before planning them. Without it the report refuses the break-even with `no idle.json`.

### 3. Run every planned contract

Each manifest entry names its contract file. Run each one into `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

From another host, use `--server-cpu-url` in place of `--server-pid`. A worker run restarts the server, so its process id changes: restart the agent with the new `--pid` too.

Add `--apply-replication` to every run whose manifest entry has `"apply_replication": true` (the replication runs and the cross-source join runs). It sets the replication the run's contract declares and restores it afterwards.

Every run carries a `server_env` in the manifest: the server must be running with it. A knob run's is its engine, `{"PROVISA_ENGINE": "duckdb"}`; run all of one engine's runs, then restart the server with the next. A worker run's adds its workers and in-flight bound, for example `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` is the number of worker processes and `PROVISA_REQUEST_THREADS` sets `concurrency.request_threads`, the in-flight requests each worker runs at once. Wait until every worker is serving (`/health` reports `workers.ready` of `workers.expected`) before you start the run.

### 4. Build the report

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (default `0.05`) sets how close to the best measured throughput a worker setting must come to count as enough.

### 5. Generate the guide

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Without `--setup` the guide has the tuning sections only. With it, the guide sizes the contract's `workload` with the engine `--engine` names (`pg` by default); an engine the runs did not measure is refused.

## Read the guide

### Sizing this workload

The guide ends this section with one line, for example: **12 physical cores, 24 worker processes, 2 in-flight requests per worker.** Above it, a table shows each transport and source: the share of requests, the mean fields, filters, rows and joins, the share that are cache hits, and the CPU one request costs.

A request costs the cost model at its knob means on a miss and the measured hit cost on a hit. A transport's Provisa CPU per second, divided by what one physical core delivers, gives its physical cores. The guide adds these across transports and rounds up. Workers are the cores times the largest workers-per-core any of the transports needs. In-flight per worker is likewise the largest any of them needs. One setting serves them all.

The tool sizes nothing it did not measure. A part of the workload the runs have no model for stops the sizing with a message that names it:

- an engine the runs did not measure
- a transport with no worker runs
- a source with no cost model on that transport
- a knob the runs did not vary (sweep it, then rerun)
- a route hint (`direct` or `federated`) on a source
- a replication setting or TTL other than the one measured
- cache hits when the runs have no cache figures

### Worker processes and in-flight requests

For each transport, the guide shows the fewest workers whose peak throughput comes within the tolerance of the best. At that worker count it then picks the fewest in-flight requests per worker that do the same. It shows that setting in two forms: workers per physical core and in-flight per worker. More of either buys no throughput. Fewer leaves cores idle. The table also gives the peak requests per second and its p99 latency. Every measured point is listed below it.

The capacity factor is the requests per second one physical core delivers, times the CPU one request costs on its own, over 1000. Above 1 the core does more than one thread's worth of requests, as it does when its hardware threads both serve.

### What a request costs

For each engine, transport and source, side by side: the base cost of an uncached request, the cost each field, filter, row and join adds, and how well the model fits the measurements (R², 1 is a perfect fit). A cross-source join has three costs, by how its two ends are read: both live, both from replicas, or one of each. A dash means the runs did not vary that knob. Where the engine runs as a server of its own, a second table gives the engine's CPU the same way: a request the engine serves, such as a cross-source join, shows its engine time there. Use these figures to see what a change in your requests would cost before you make it: two more fields on every request add twice the per-field cost.

### The response-cache opt-in

For each transport and source: the CPU of a miss, of a hit, and what a hit saves. A request is answered from the cache only when it opts in and repeats an earlier request, so the saving applies to repeated requests. Opt the repeated ones in.

### Live or replica reads, and the TTL

For each engine, transport and source with replication runs: the cost of a live read, of a replica read, the CPU of one refresh, the TTL, and the break-even rate. Above the break-even rate a replica costs less CPU, Provisa's, the source's and the engine's together, than reading live. Below it, read live. A second table gives the same for cross-source joins, by the number of joins in the request: both ends read live against both ends from replicas, the refresh counting every source the switch moved to a replica.

The break-even is the refresh's CPU per second over what one read saves:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Doubling the TTL halves the break-even rate, and the data can be up to twice as old. Choose the TTL from how stale your users can accept the data to be, then compare your request rate on that source with the break-even at that TTL.

A replica read's cost depends on the store and on its indexes, which are built shortly after each landing. Measure after the indexes are in, and rerun after a change of store. [Replicas](replicas.md) covers the store choice and the freshness strategies.
