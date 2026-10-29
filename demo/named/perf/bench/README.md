# Perf benchmark

`run_benchmark.py` sends the query matrix in `queries.py` to a Provisa server that is already running. Each query goes over every transport that can express it. The script writes one JSON report per engine and prints a summary table. It never starts or stops Provisa.

## Cells

A cell is one query over one transport in one dialect. The table labels each cell `transport(dialect)`:

| Label | Wire | Request |
|---|---|---|
| `sql(sql)` | pgwire | SQL |
| `flight(sql)` | Arrow Flight (gRPC) | SQL in the ticket |
| `cypher(cypher)` | Bolt | Cypher |
| `http(cypher)` | HTTP, `POST /data/cypher` | Cypher |
| `graphql(graphql)` | HTTP, `POST /data/graphql` | GraphQL |
| `grpc(grpc)` | native gRPC service | typed per-table RPC |

`flight(sql)` carries SQL. The matrix sends no Cypher over Flight.

The matrix runs twice. The `nocache` pass asks for no response cache and measures every iteration. The `cache` pass opts each request into the cache and drops its first iteration before computing percentiles.

## Trial run on your machine

A trial checks that the benchmark code works. Every cell should answer, and row counts should agree across transports. The report should also parse. A trial's latencies come from a laptop and small data, so don't publish them.

```bash
~/.provisa-agent/heavy.sh .venv/bin/python demo/named/perf/bench/harness_trial.py \
    --engine duckdb --output-dir ~/bench_trial
```

Run it from the repository root. `harness_trial.py` brings up a compose project of its own (`provisa-benchtrial-<pid>`) with the four bench sources on leased loopback ports. It seeds them with 40,000 orders, the smallest data at which every query in the matrix returns rows. It then starts an isolated Provisa server on `demo/named/perf/config.yaml` and runs `run_benchmark.py --skip-saturation` against it. On exit it removes the server and runs `docker compose down -v`.

Repeat `--engine` to run several engines on one stack. `--engine trino` adds a Trino coordinator to the project. `--engine pg` federates through the bench Postgres. In the trial, that database is a stock `postgres:16`. It lacks the extensions the pg engine loads for the other sources.

Results land in `<output-dir>/<engine>/<timestamp>.json`, with the server's log beside them as `<engine>-server.stderr`.

## Full run on the perf VM

On the VM, the data stack in `demo/named/perf/docker-compose.yml` is brought up and seeded at full scale first. Then run:

```bash
demo/named/perf/bench/orchestrate.sh duckdb pg trino
```

For each engine, `orchestrate.sh` starts Provisa with `start-ui-install.sh --demo perf`, runs `run_benchmark.py` and the Artillery ramps, then stops Provisa. Results land in `demo/named/perf/bench/results/<engine>/`. `METHODOLOGY.md` one directory up describes the three-device layout and what each number means.

## Reading the report

Each entry in the report's `results` list is one cell in one pass. It has `query_id`, `transport`, `dialect` and `cache_pass`. A cell that answered carries `n`, `total_rows`, `qps`, `bytes_per_sec` and the `latency_ms_p50`, `latency_ms_p95` and `latency_ms_p99` percentiles. Percentiles use the nearest rank. A cell whose every iteration failed has `"status": "all_failed"`. Every cell carries `errors` and `first_error`, the text of its first failure.

Compare latency only for the same query across engines or transports. `bytes_per_sec` is comparable across queries.
