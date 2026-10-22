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
  --ssh-cidr <operator address>/32 [--provisioning-model ON_DEMAND] \
  [--backends-provisioning-model ON_DEMAND] [--reps 5] [--keep] [--dry-run]
```

It applies `terraform/perf` (its state in a run directory of its own under `~/.provisa-bench/runs`), waits until every device's ready marker names the commit (its startup script has finished), refusing by name a device not ready within `--ready-timeout` (7200 s), runs the repetitions, copies the results back into the run directory, and destroys every device. Destroy runs on failure and on interrupt too, unless `--keep` is given. `--dry-run` prints the terraform plan and every command of the run, and creates nothing.

### The seed job and the seed image

A run never seeds. Seeding is its own job, run once for each seed hash, and every run boots device 3 from the disk image that job stores.

The seed hash (`bench/seed_inputs.py`) is the sha256 of the inputs that decide the seeded data, read at the commit: the seeder's files (`generate_*.py`, `seed.py`, `Dockerfile.seeder`) and `Dockerfile.postgres`, each as its path and content, then, out of `docker-compose.yml`, the engines' pinned image tags (postgresql, mongodb, clickhouse, neo4j) and the seeder's scale settings (`PROVISA_BENCH_ORDERS`, `PROVISA_BENCH_NEO4J_ORDERS`, `PROVISA_BENCH_ITEMS_PER_ORDER`). An engine upgrade or a new scale changes the hash; an unrelated compose edit does not.

```bash
demo/named/perf/bench/seed_lifecycle.sh --project P --zone us-east1-b --commit <sha on GitHub> \
  --ssh-cidr <operator address>/32 [--backends-provisioning-model ON_DEMAND] [--machine-type T] [--disk-gb GB]
```

It applies `terraform/perf` with device 3 alone (`devices = ["backends"]`, `seed_mode = "seed"`), waits for its ready marker (the stack is up and seeded), stops the device, and stores its boot disk as the image `provisa-perf-seed-<first 12 characters of the hash>`, labelled `seed_hash=<first 63 characters of the hash>` (a GCE label value holds at most 63). Then it destroys the device; the image is not a terraform resource, so it is kept. When an image for the hash already exists, the job says so and creates nothing. A run's `--disk-gb` must be at least the seed job's.

`bench_lifecycle.sh` computes the hash at the commit and looks the image up before it applies anything. When there is none, it stops, naming the hash: the seed inputs changed, and `seed_lifecycle.sh` stores the image first. The image is passed as `backends_image`, which only device 3 boots from (`seed_mode = "from_image"`); devices 1 and 2 boot from `image`. Device 3's startup script then brings the stack up with the seeder scaled to nothing. A device without the seeder's done-marker (`data/.seeded`) is refused: its startup script exits without writing the ready marker.

### Checkpoints and recovery

Each run (one repetition at one telemetry setting, or one variant of a governance pass) is copied back as soon as it finishes and checkpointed to the run's bucket, `gs://<bucket>/<run id>/<run>/`, with a `DONE` object written last. The bucket is a `terraform/perf` resource (`results_bucket`, named after the run id, deleted with what it holds on destroy); before destroy, `bench_lifecycle.sh` copies it into `checkpoints/` in the run directory. `three_device_run.sh` lists the bucket first and skips every run already done there, saying which.

When the run fails and a device is stopped or terminated (a reclaimed Spot device), `bench_lifecycle.sh` starts the stopped devices again. If they do not start, it replaces the devices (a targeted destroy of the instances, then apply, device 3 again from the seed image), keeping the bucket. It then waits for every device to be ready and reruns `three_device_run.sh` with the same run id, which repeats only the runs not done. It recovers at most 3 times, then stops and destroys. A failure with every device running is a real failure and is not retried.

Within a session, `demo/named/perf/bench/three_device_run.sh` does the run over `gcloud compute ssh`:

- **Device 3, once:** `docker compose --profile three-device up -d --scale seeder=0` in `demo/named/perf` (adding `--profile three-device-trino` for a Trino run), with `PROVISA_BENCH_BIND` set to device 3's private address, refusing a device without the seed. That brings up the source, the engine Postgres, the control-plane Postgres, Redis, the collector, otlp2sql and its ops Postgres. Then it starts `host_agent.py`.
- **Device 1, each run:** `orchestrate.sh serve --telemetry off|on --engine pg` (`serve_three_device.sh`). It starts Provisa only, every backend address pointing at device 3, and waits until every worker serves. Then it starts `host_agent.py` with Provisa's pid, the run's facts (telemetry, engine, workers) and a round-trip target on device 3.
- **Device 2, each run:** `run_benchmark.py --optimistic` with a contract whose endpoints name device 1's private address. It reads Provisa's CPU from device 1's agent (`--server-cpu-url`) and the containers' CPU from device 3's (`--source-cpu-url`, `--backend-container`). It writes every device's facts and the round trips to `devices.json` (`--device`).
- **Device 1, after each run:** Provisa is stopped, so the next run starts it fresh. The run's results are copied back and checkpointed.

The results are reported by `three_device.py report`.

Each device sets itself up at boot (`terraform/perf/startup.sh.tftpl`): Docker and the compose plugin, the repository at the `commit` variable in `repo_dir`, `uv sync`, and the idle shutdown (power off after 30 idle minutes, `vm_idle_shutdown.sh`). Device 3 also brings up its data stack: the seed job's device seeds it (20 million orders, which takes a while), a run's device finds it seeded on the seed image. When every step has passed, the script writes the commit to its ready marker. `three_device_run.sh` holds every device up for the length of a run; the idle shutdown matters only for a device kept with `--keep`.

The devices are Spot by default (`provisioning_model = "SPOT"`) while the harness is made to work; the final measured run sets `provisioning_model = "ON_DEMAND"`. A reclaimed Spot device stops, keeping its disk. `--backends-provisioning-model ON_DEMAND` (`backends_provisioning_model`, unset meaning the same as the others) keeps device 3 on-demand while devices 1 and 2 stay on Spot; `seed_lifecycle.sh` takes the same option for its one device.

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
