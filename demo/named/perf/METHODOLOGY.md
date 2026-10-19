# Optimistic benchmark: three-device methodology

The optimistic benchmark measures Provisa's own ceiling per transport: one row, one column, no filter, the Postgres `orders` table. It runs on three machines, so no figure carries another program's CPU. The published figures are measured with telemetry off; the same points are measured again with telemetry on, to measure what telemetry costs.

## What runs where

| Device | Runs | Nothing else |
|---|---|---|
| 1, server | Provisa: its worker processes, every listener (HTTP, pgwire, Flight, Bolt, gRPC); DuckDB when it is the engine (in-process); `host_agent.py` | No database, no collector, no load generator |
| 2, client | `run_benchmark.py --optimistic` (the load generator) | No Provisa, no source |
| 3, backends | The Postgres source (`orders`); the federation engine when it is a server (Postgres, or Trino), in its own container; the control-plane Postgres; Redis (the shared response cache); the OTel collector, otlp2sql and its ops Postgres; `host_agent.py` | No Provisa, no load generator |

Each device has 16 physical cores. On GCP that is a 32-vCPU type; `c4d-standard-32` is the default.

Redis is on device 3: a server Provisa reaches over the network, as in production.

The standard engine is Postgres (`PROVISA_ENGINE=pg`). It runs on device 3 in its own container, apart from the source's Postgres, so its CPU is reported on its own. Trino, when a run names it, is also its own container on device 3. DuckDB runs inside Provisa on device 1, so its time is part of Provisa's CPU. A request that reaches the engine (a knob run's federated read) shows its engine time there; the optimistic request does not reach it.

All three are in one zone, under one compact placement policy, so the round trip is short and stays the same from run to run.

## Provisioning

`terraform/perf` creates the three instances from variables (machine type per device, zone, image), the compact placement policy, a firewall rule that lets only the three devices reach each other, and one for the operator's SSH from the addresses given. The maintainer applies it. The repository is checked out at the commit under test on all three, at the same path, with its virtual environment.

One command runs a session from the operator's machine, and nothing outlives it, so nothing bills between sessions:

```bash
demo/named/perf/bench/bench_lifecycle.sh --project P --zone us-east1-b --commit <sha on GitHub> \
  --ssh-cidr <operator address>/32 [--provisioning-model ON_DEMAND] [--reps 5] [--keep] [--dry-run]
```

It applies `terraform/perf` (its state in a run directory of its own under `~/.provisa-bench/runs`), waits until every device's ready marker names the commit (its startup script has finished; on device 3 that includes the first seed), refusing by name a device not ready within `--ready-timeout` (7200 s), runs the repetitions, copies the results back into the run directory, and destroys every device. Destroy runs on failure and on interrupt too, unless `--keep` is given. `--dry-run` prints the terraform plan and every command of the run, and creates nothing.

Within a session, `demo/named/perf/bench/three_device_run.sh` does the run over `gcloud compute ssh`:

- **Device 3, once:** `docker compose --profile three-device up -d` in `demo/named/perf` (adding `--profile three-device-trino` for a Trino run), with `PROVISA_BENCH_BIND` set to device 3's private address. That brings up the source, the engine Postgres, the control-plane Postgres, Redis, the collector, otlp2sql and its ops Postgres. It waits for `seeder` to finish, then starts `host_agent.py`.
- **Device 1, each run:** `orchestrate.sh serve --telemetry off|on --engine pg` (`serve_three_device.sh`). It starts Provisa only, every backend address pointing at device 3, and waits until every worker serves. Then it starts `host_agent.py` with Provisa's pid, the run's facts (telemetry, engine, workers) and a round-trip target on device 3.
- **Device 2, each run:** `run_benchmark.py --optimistic` with a contract whose endpoints name device 1's private address. It reads Provisa's CPU from device 1's agent (`--server-cpu-url`) and the containers' CPU from device 3's (`--source-cpu-url`, `--backend-container`). It writes every device's facts and the round trips to `devices.json` (`--device`).
- **Device 1, after each run:** Provisa is stopped, so the next run starts it fresh.

The results are copied back and reported by `three_device.py report`.

Each device sets itself up at boot (`terraform/perf/startup.sh.tftpl`): Docker and the compose plugin, the repository at the `commit` variable in `repo_dir`, `uv sync`, and the idle shutdown (power off after 30 idle minutes, `vm_idle_shutdown.sh`). Device 3 also brings up its data stack and runs the first seed (20 million orders, which takes a while; a device kept with `--keep` finds it seeded at its next boot). When every step has passed, the script writes the commit to its ready marker. `three_device_run.sh` holds every device up for the length of a run; the idle shutdown matters only for a device kept with `--keep`.

The devices are Spot by default (`provisioning_model = "SPOT"`) while the harness is made to work; the final measured run sets `provisioning_model = "ON_DEMAND"`. A reclaimed Spot device stops, keeping its disk.

Before the first apply: the project's quota must allow three 32-vCPU devices (96 vCPUs, all regions); `--ssh-cidr` names the operator's own address; and `--commit` names a commit on GitHub, which every device clones.

`three_device_run.sh --dry-run` prints every command it would run on each device, and runs none.

## Reported per device

Read off each machine by its host agent, never typed in:

- the machine type (GCE metadata `machine-type`) and the zone (metadata `zone`);
- physical cores, sockets and threads per core (`lscpu`);
- the link: each network interface's driver (`gve` is gVNIC);
- the measured round trip, device 2 → device 1 and device 1 → device 3: the median of 100 TCP connects, with its p99.

A fact that cannot be read is reported with the reason under `errors`.

CPU per step, each from its own device:

- device 1: Provisa's CPU per request and in cores;
- device 2: the load generator's CPU in cores and `client_limited`. A client-limited step is a measurement of the client and is never published;
- device 3: the CPU of each container (the source's Postgres, the engine, the control-plane Postgres, Redis, the collector, otlp2sql, the ops Postgres), so a backend bottleneck shows.

## Telemetry off and on

Each point is measured with telemetry off and with telemetry on, on the same three devices. One setting per run: `orchestrate.sh serve --telemetry off|on`. Device 1's agent states it, and the report refuses a run whose server says otherwise.

- **Off: the published figure.** No exporter endpoint, and `observability.sample_rate: 0`, so no span is recorded or exported.
- **On: the impact, not published.** Provisa exports to the collector on device 3, at the sample rate the perf config states. Provisa pays for creating and exporting spans; receiving and storing them is on device 3.

`three_device.py report` writes two files. `published.json` has the telemetry-off figures only. `internal.json` has both settings and the difference per transport: requests per second, p99, and Provisa CPU per request.

## Warm-up, repetitions and spread

- **Warm-up:** once every worker serves, one step at the highest concurrency, per transport, discarded (`load.warmup_s: 60` in the contract). It warms the plan cache, the response cache, connection pools and the source's buffers.
- **Ramp:** the contract's steps (1, 16, 64, 256 connections), 30 s each.
- **Repetitions:** 5 per point, with off and on alternating (off, on, off, on, ...) so drift over the session falls on both alike. Provisa is restarted for every run, because the sample rate is fixed at start.
- **Spread:** each figure is the median of the 5, with the minimum and maximum beside it.

## The published point

Per transport, with telemetry off: the step with the highest throughput that had no errors and was not client-limited. At that step the report gives:

- requests per second, median and p99 latency, and errors (0);
- Provisa CPU per request;
- client CPU, source CPU and the other backend containers' CPU;
- the three devices and the network as above;
- the caption: "single source, direct read; Telemetry OFF". It names no federation engine: the optimistic request reads one source, from the response cache or directly from Postgres on a miss, so no engine is in its path. The engine is stated per run only for the sizing knob runs, where requests do reach it;
- the commit, the contract, and the raw results.

The language and transport pairs are those already published: SQL over pgwire (cached and uncached, side by side), SQL over Arrow Flight, GraphQL over HTTP, Cypher over Bolt, gRPC, REST and JSON:API.
