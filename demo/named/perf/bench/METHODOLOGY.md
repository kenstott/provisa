# Publishable benchmark: methodology

One run produces every published number. It answers a narrow question: how fast is Provisa itself on each language and transport people use, when nothing else shares its machine, and what does each governance feature cost on top of that.

`../METHODOLOGY.md` holds the mechanics of the three machines: provisioning, seeding, checkpoints and recovery. This page holds the design of the run those mechanics carry out. Where the two differ, this page wins.

## Version under test

A run names one commit before it starts, and every machine checks out that commit. `bench_lifecycle.sh --commit <sha>` sets it, and Provisa's agent states it in every run's device facts as `commit`. Each run records the commit it served, and the report shows it per run, in `commits_by_run`. `published.json` writes it at the top as `version`. A published table opens with that version, then the date of the run.

A fix made after a stop can change the commit partway through a benchmark. A report that spans commits then lists them, and it carries this caveat: the differences between these commits were not judged consequential to the results. If a mid-benchmark change does invalidate the earlier runs, the benchmark restarts from the beginning (see [Stops](#stops)).

The first commit has to be on `main`, with the mini benchmark clean at that sha.

## Setup

Three machines in one zone, under one compact placement policy, on on-demand pricing.

| Device | Runs | Machine type | Why this size |
|---|---|---|---|
| 1, server | Provisa, all workers and listeners, and the federation engine in its own container | `c4d-standard-32` | 16 physical cores, the reference app server |
| 2, client | The load generator | `c4d-standard-32` | A client that saturates first measures itself, and such a step is never published |
| 3, backends | The sources, the control-plane Postgres, Redis, the telemetry pipeline | `c4d-standard-32` | Its CPU is reported per container, so a backend limit shows |

The engine is Postgres. It runs beside Provisa so that the machine sizes what a deployment buys for its query tier. A cached or direct read does not reach it. Its container CPU is reported apart from Provisa's on every step, as `engine_cpu_cores`.

Each machine's facts are read off the machine and written beside every run: machine type, zone, instance id, CPU platform, physical cores, NIC driver and the measured round trips between machines. A fact that cannot be read is reported as an error.

## What runs

### Baseline

Every knob at zero: one row, one column, no filter, the Postgres `orders` table at 20 million orders, telemetry off. This is the published table.

The contract's ramp is 1, 16, 64 and 256 connections, 30 s each, after a 60 s warm-up per transport that is thrown away. Five repetitions, with Provisa restarted for each. Telemetry-on runs alternate with them so that drift falls on both sides. They measure what telemetry costs, for us. They are not published.

pgwire runs again per repetition with the response cache off, so the table shows both.

One server start serves one repetition at one telemetry setting. Within it, each transport is its own unit (see [Units](#units)).

### Published transports

| Row | Language and wire |
|---|---|
| pgwire | SQL over the PostgreSQL wire protocol, cached and uncached |
| Flight SQL | SQL text in an Arrow Flight ticket |
| GraphQL | GraphQL over HTTP |
| Bolt | Cypher over Bolt |
| gRPC | A typed per-table RPC with no query language |
| REST | REST over HTTP, JSON |
| JSON:API | JSON:API over HTTP |

SQL over HTTP, Cypher over HTTP and SPARQL run in the same pass and stay in `internal.json`. MCP is not in the contract.

### Features on

The same request, reading ten columns of `orders`, one knob at a time against the zero run: row rules, masked columns, central redaction, role depth, derived roles, then all of them together, then zero again to show drift. Each variant runs cached and uncached, so the response cache has a cost and a saving of its own. The role mix comes from the role depth and derived-role variants. A different mix of weighted roles is not part of this pass.

The difference from zero, in server CPU per request, is that feature's cost. Three repetitions, ramp 1 and 16, 15 s each, after a 15 s warm-up, telemetry off.

### Correctness first

The mini benchmark runs at the same commit before any of this: the query matrix on DuckDB, PostgreSQL and Trino, with every cell answering and row counts agreeing across transports. It runs on one cheap VM, and its timings are never published.

The timing sanity report (`bench_sanity.py`) is part of that gate. It flags a cached cell slower than uncached, a large query faster than its small counterpart, a cold query faster than warm, and a cell that moved 2x or more against the last run. The last check runs only when the two runs came from one VM. A flag fails nothing by itself; each one needs an explanation in the run's notes before the run goes ahead.

### Kill a node

Two Provisa nodes behind HAProxy, all listeners in TCP mode, under a short load on every published transport. One node takes SIGKILL 20 s in. `chaos_check.py` compares every answer with the first one from that transport.

Long-lived sessions on the dead node drop, and clients reconnect. That is the only failure allowed, and only in the 15 s after the kill. A wrong answer or a server error anywhere fails the check, and so does a transport that goes quiet afterwards. The check needs a fourth machine for the second node. It measures nothing, and that machine can be small.

The supported control plane for this setup is a Postgres behind a failover address. The run keeps one control-plane Postgres, because killing it would test Postgres failover and not Provisa.

### Profile

Once the repetitions are done, `three_device_run.sh --profile` runs each published transport alone at 64 connections with py-spy on device 1. A profile slows what it watches, so no published figure comes from these runs. The profiles stay internal, and they exist to explain the figures.

### Competitors

We state a competitor's number only when that product ran on device 1's machine type, against device 3's Postgres, under the same client and concurrency. A figure taken from someone else's page does not go beside ours, because its hardware and conditions are not ours to state.

### Internal only

The mixed workload is never published and never runs in this session. It remains a tool for scenario analysis. The sizing runs stay internal for the same reason.

## Units

The unit of work is one transport, one repetition, written `<rep>-<transport>-<telemetry>`, or `<rep>-<variant>-<cache>-<transport>-<telemetry>` for the features pass. `bench_units.py plan` gives every unit in order.

### The check after each unit

When a unit's results come back, `bench_units.py check` rechecks it before anything else runs. Each unit is valid only if:

- its throughput peak has zero errors and is not client-limited. A step past the peak that errs or hits the client limit is the server's ceiling showing, and is not a reason to stop;
- no device paged at its published step, and a device whose paging could not be read fails this check;
- its three machines and their CPU platforms match the run's first unit;
- its server states the run's telemetry setting and a commit;
- the round trips between machines stay under 1 ms at p99;
- once a second repetition exists, the highest and lowest requests per second for that transport and setting are within 1.2x;
- `zero_end` agrees with `zero` within 1.1x, once `zero_end` exists.

A unit that fails is invalid. It is moved to `results-discarded/` with its reason recorded in the run's ledger, and the run stops.

Sanity flags are checked at the same point. A cached read slower than the same read uncached raises a flag, once both units of a repetition exist. A flag with a note in `sanity_notes.yaml` is recorded with that explanation. A flag with no note keeps the unit and stops the run, because the flag needs an explanation before the run goes ahead.

The run stops on an invalid result and never on a disappointing number. None of these checks compares a figure with what anyone hoped for.

### Stages

Stages run from least to most risky, and a failure stops the run so the issue can be filed and resolved.

1. Baseline: telemetry off, pgwire uncached, then telemetry on, in each repetition.
2. Features: `zero`, `redaction`, `derived_roles_2`, `masked_quarter`, `row_rules_2`, `role_depth_2`, `all`, `zero_end`, in that order whatever order they are named. The last job's smoke finished the first three of these and ran out of time on the ones that change the most per transport.
3. Profiles, on a repetition's machines, after everything they explain.

Within a stage the transports run in the same order: the stateless HTTP surfaces, then the ones that hold a long-lived session.

The baseline has to pass before features start. A features pass takes `--baseline-ledger <that run's ledger>` and refuses to start otherwise. `--smoke` runs a pass without one, for numbers that will not be published.

### Stops

A stopped run stops its machines and keeps them, with their disks and terraform state. They have to be the same machines for the run to go on, and only their disks bill while they wait. Nothing is destroyed until the run finishes or is abandoned.

`bench_lifecycle.sh --resume <run id> --commit <sha> --fix "<what was done>"` starts the same machines at the commit given and runs what is left. Finished units are kept, and the run continues from the point of stopping. A commit whose seed hash differs cannot resume, because the data changed.

`bench_lifecycle.sh --abandon <run id> --fix "<what was done>"` records a restart and destroys the machines. If a mid-benchmark change truly invalidates the earlier benchmark, the benchmark restarts from the beginning. That decision is the maintainer's.

The ledger (`results-ledger.json`) records every unit's verdict and commit, every discarded unit with its reason, and every stop with its fix and whether it resumed or restarted. The report carries the stops and discarded units.

A machine that is gone is a new run. On Spot, a reclaimed machine is deleted along with its disk, so that run also ends and the restart is the maintainer's call.

Every machine has a self-destruct: `--max-run-hours` tells GCE to delete it, disk and all, that long after it starts. Set it long enough for the stage.

## What counts as a valid result

A run is valid when all of these hold. If any fails, the affected figures are withheld and the point reruns. The unit check above enforces these as the run goes, and the report enforces the ones that cross units.

- Every machine records its CPU platform and instance id, and all repetitions of a figure come from the same three instances. The report refuses a mix. Two VMs with the same type and CPU platform differed by 1.29x to 1.61x in the last mini benchmark, more than any code change in it. A replaced machine therefore means a new run, and Spot cannot produce a valid result.
- Each run states its commit, on a clean checkout. A report across commits lists them with the caveat above.
- The published step has zero errors and is not client-limited.
- Telemetry is off, as Provisa's agent states it.
- The server served each repetition from a fresh start with every worker ready.
- Over the five repetitions, the highest and lowest requests per second are within 1.2x of each other. Repeats on one VM have spread by 0.88 to 1.08, so a wider spread means something moved during the run.
- The `zero` and `zero_end` variants of the features pass agree within 1.1x.
- No device paged during a published step: `pswpin` and `pswpout` are zero.
- The round trip between machines stays under 1 ms at p99.
- The sanity report's flags are explained and the kill-a-node check passed, both at this commit.

## How numbers are reported

Per transport, at the highest-throughput step that was clean:

- requests per second, as the median of the repetitions with the minimum and maximum beside it;
- p50 and p99 latency, the same way, and errors, which are 0;
- Provisa CPU per request, with client CPU, engine CPU and each backend container's CPU;
- the machines, CPU platforms and round trips.

Above the table, the version, the date and the run's caption, "single source, direct read; Telemetry OFF". The caption names no engine, because this request does not reach one.

Each row carries its transport caption from `published.json`. Transport names conflate the wire and the language, so the table says which is which. HTTP appears under several rows because those rows share a wire and differ in encoding. Flight carries SQL in its ticket, and no Cypher runs over Flight.

Features-on results report the difference from zero in milliseconds of server CPU per request, for each feature, cached and uncached. They go in `variants.json`, marked internal. Publishing them is the maintainer's call. Until then they serve customer conversations.

We keep the contract, the commit and the raw results of every repetition with the published table, so anyone can check a figure against what produced it.

## Cost of a run

List prices for `us-east1` from the Google Cloud Billing Catalog, read on 2026-10-31. These are estimates, not billed figures.

| Item | Price |
|---|---|
| C4D core, on demand | $0.0327035 per vCPU hour |
| C4D memory, on demand | $0.003495578 per GB hour |
| `c4d-standard-32` (32 vCPU, 124 GB), on demand | $1.480 per hour, derived |
| `c4d-standard-32`, Spot | $0.657 per hour, derived |
| Hyperdisk Balanced capacity | $0.09 per GiB month, about $0.025 per hour for 200 GB |
| Stored image | $0.05 per GiB month |

Three machines with their disks cost $4.51 an hour. The estimate assumes traffic between machines in one zone is free. The catalog lists no charge for it.

| Stage | Machines | Hours | Cost |
|---|---|---|---|
| Seed job, once per seed hash | 1 | 2.0, estimated | $3.00 |
| Boot, engine image build, warm start, for each of two sessions | 3 | 1.5 | $6.80 |
| Baseline: 10 runs of 10 transports at 33 min each | 3 | 5.5 | $24.80 |
| pgwire uncached: 5 runs at 6 min each | 3 | 0.5 | $2.30 |
| Features on: 16 runs per repetition, 3 repetitions, 10.5 min each | 3 | 8.4 | $37.90 |
| Profiles, 7 transports | 3 | 0.5 | $2.30 |
| Kill a node, with a second node | 4 | 0.5 | $3.00 |
| Seed image storage for one month | | | $3.00 |

That comes to **about $85** and 19 session hours. Allow **$120**, which covers one rerun of the baseline if a validity rule fails. The seed time is the softest number here, because 20 million orders has not been timed on this machine type. Dropping the three internal transports from the baseline saves about $7. Features at two repetitions saves about $13.

Sources: [Cloud Billing Catalog API](https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list), Compute Engine service `6F81-5844-456A`, SKUs "C4D Instance Core running in Americas", "C4D Instance Ram running in Americas", "Spot Preemptible C4D Instance Core running in Americas", "Spot Preemptible C4D Instance Ram running in Americas", "Hyperdisk Balanced Capacity in South Carolina" and "Storage Image"; [VM instance pricing](https://cloud.google.com/compute/vm-instance-pricing); [Network pricing](https://cloud.google.com/vpc/network-pricing); machine size from `gcloud compute machine-types describe c4d-standard-32 --zone us-east1-b`.

## Order of work

1. The mini benchmark and its sanity report at the commit, on one small Spot VM.
2. A short functional run of the split on Spot VMs, with one deliberate abort and resume. It proves the mechanics and measures nothing.
3. The maintainer approves the cost.
4. The seed job, if the seed hash has no stored image.
5. `bench_lifecycle.sh --provisioning-model ON_DEMAND --reps 5` for the baseline, then the features pass with `--variants zero,row_rules_2,masked_quarter,redaction,role_depth_2,derived_roles_2,all,zero_end --caches cached,uncached --telemetry off --steps 1,16 --window-s 15 --warmup-s 15 --reps 3 --baseline-ledger <the baseline's results-ledger.json>`.
6. The kill-a-node check.
7. Validity checks, then the published table. Nothing keeps running afterwards.
