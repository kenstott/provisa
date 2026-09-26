# How to create a named demo

A named demo is a self-contained, reusable demo environment you drop into `demo/named/<name>/`.
Once the directory exists, `start-ui-install.sh --demo <name>` picks it up with no change to
the launcher script and no developer involvement required (REQ-1858). This is the mechanism for
client-specific and vertical demo scenarios — a future `--demo banking` or `--demo healthcare`
is just a new directory in this shape.

Named demos are distinct from the `--source=<name>` mechanism. The `--source` flag adds a single
toy source to the standard demo (backed by `demo/sources/<name>/`). A named demo is a
self-contained multi-source package: its own docker-compose.yml, its own fragment.yaml, and
whatever seeding tooling the scenario needs.

## The fixed directory layout

Every named demo lives at `demo/named/<name>/` and must contain exactly these files:

```
demo/named/<name>/
  docker-compose.yml    # required — the one start point
  fragment.yaml         # required — sources/domains/tables/relationships to splice in
  Dockerfile.seeder     # if the demo self-seeds (recommended)
  seed.py               # entrypoint for the seeder service
  generate_*.py         # one per data store, optional but conventional
```

`start-ui-install.sh` looks up `demo/named/$DEMO_NAME/fragment.yaml` generically [tool-verified]:

```bash
# start-ui-install.sh:272-276
if [ -n "$DEMO_NAME" ]; then
  _NAMED_DIR="$SCRIPT_DIR/demo/named/$DEMO_NAME"
  _NAMED_FRAGMENT="$_NAMED_DIR/fragment.yaml"
  if [ ! -f "$_NAMED_FRAGMENT" ]; then
    echo "--demo $DEMO_NAME has no $_NAMED_FRAGMENT to register it with"; exit 1
  fi
```

If `fragment.yaml` is absent the launcher exits immediately. No other validation happens — the
name itself is not in any registry or allowlist.

## The one-start-point invariant

`docker compose up -d` against `docker-compose.yml` is the complete setup-and-seed story. No
manual commands. No host-side scripts to run before it works.

This does **not** mean the demo must be a single process or single container. The `perf` demo
runs five containers (four engines plus a seeder) [tool-verified]. A demo may also reference
external managed resources it does not containerize — a live Databricks or Snowflake source
reached by connection string, registered in `fragment.yaml` with no corresponding compose
service. Those are implementation details behind the single entry point.

What the invariant prohibits: telling the solutions engineer to run scripts by hand, set up
databases manually, or issue commands in a specific order. One command, fully automated.

## The seeder pattern

Heavy data generation must not re-run on every container restart. The `perf` demo uses a
`seeder` service that writes a marker file on a bind-mounted data volume [tool-verified]:

```python
# demo/named/perf/seed.py:29,42-43
MARKER = Path("/data-marker/.seeded")

def main() -> int:
    if MARKER.exists():
        print(f"perf demo already seeded ({MARKER.read_text().strip()}); skipping")
        return 0
```

When the seeder container starts, it checks for the marker. If present, it exits immediately. If
absent, it runs all generation scripts in sequence, then writes the marker. Because the marker
lives on the bind-mounted `./data/` volume, it survives container restarts but not `./data/`
being deleted. [tool-verified]

The seeder service in `docker-compose.yml` gates on every other service's healthcheck
[tool-verified]:

```yaml
# demo/named/perf/docker-compose.yml:129-137
depends_on:
  postgresql:
    condition: service_healthy
  mongodb:
    condition: service_healthy
  clickhouse:
    condition: service_healthy
  neo4j:
    condition: service_healthy
```

For lighter demos — a few hundred rows, fast to regenerate — skipping the marker and letting
the seeder re-run every start is fine. The marker pattern is only necessary when generation
takes minutes or writes gigabytes.

## Bind mounts, not named volumes

Use bind mounts to `./data/` for all data volumes. Docker Desktop stores named volumes inside
its own VM disk image on the macOS boot volume, which fills fast. Bind mounts to `./data/` keep
every byte on the external volume (e.g. `/Volumes/main`) [tool-verified].

This requires `/Volumes/main` (or whatever host path contains the repo) to be added under
Docker Desktop → Settings → Resources → File Sharing.

## Container labels

Every service in `docker-compose.yml` carries two labels [tool-verified]:

```yaml
# demo/named/perf/docker-compose.yml:36-37, 43-44
x-demo-labels: &demo-labels
  com.provisa.demo: perf   # replace with your demo name
services:
  postgresql:
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb   # role: rdb / dw / doc-store / graph / seeder / etc.
```

`com.provisa.demo` groups every container the demo starts so
`docker ps --filter label=com.provisa.demo=<name>` lists exactly those containers without
needing the compose file open. `com.provisa.demo.role` identifies each service's part.

## fragment.yaml — registering sources and tables

`fragment.yaml` is a static, checked-in config file. It is spliced into the Provisa config by
the launcher at startup, not loaded at runtime. Use Provisa's standard YAML config format: top-
level `domains:`, `sources:`, `tables:`, and `relationships:` keys.

### Sources with native connectors (no explicit `tables:`)

PostgreSQL, ClickHouse, and MongoDB have native federation connectors that auto-discover
their tables and collections at query time. Register the source and stop there [tool-verified]:

```yaml
# demo/named/perf/fragment.yaml:32-52
- id: bench-postgresql
  type: postgresql
  host: ${env:PROVISA_BENCH_POSTGRESQL_HOST:-localhost}
  port: ${env:PROVISA_BENCH_POSTGRESQL_PORT:-25632}
  database: provisa_bench
  username: provisa
  password: provisa
```

No `tables:` block. The engine discovers the schema at query time.

Use env-var interpolation with a `:-` default for every host and port. The seeder uses the
in-container service name as the host; the host-side generate scripts use `localhost` plus the
published port. The fragment uses the same env-var names so both work without editing the file
[tool-verified from `generate_postgres.py:29-30`]:

```python
HOST = os.environ.get("PROVISA_BENCH_POSTGRESQL_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_POSTGRESQL_PORT", "25632"))
```

### Sources backed by api_source (explicit `tables:` required)

Neo4j has no native federation connector. It is an `api_source`-backed type where each table
is one fixed Cypher projection. You must hand-author every table in `fragment.yaml`
[tool-verified, with comment from `demo/named/perf/fragment.yaml:19-27`]:

```yaml
# fragment.yaml:63-94 (excerpt)
tables:
- source_id: bench-neo4j
  domain_id: perf-bench
  schema: neo4j
  table: bench_order_node
  description: One row per Order node
  query_template: >-
    MATCH (o:Order)
    RETURN o.order_id AS order_id, o.customer_id AS customer_id, ...
    ORDER BY o.order_id
  columns:
  - {name: order_id, data_type: integer, is_primary_key: true, visible_to: [org_admin, analyst]}
  - {name: customer_id, data_type: integer, visible_to: [org_admin, analyst]}
  ...
```

`query_template` is a Cypher query whose result set becomes the table. Write it from the actual
schema your generation scripts produce, not from an assumed schema. Each column entry matches
Provisa's `Column` model shape: `name`, `data_type`, optional `is_primary_key`, and
`visible_to` (list of role names).

The same explicit `tables:` requirement applies to any other `api_source`-backed connector
(Elasticsearch is another example — see `demo/sources/elasticsearch/fragment.yaml`).

### Relationships

Cross-table relationships go in a top-level `relationships:` block [tool-verified]:

```yaml
# fragment.yaml:167-178
relationships:
- id: bench-placed-to-customer
  source_table_id: bench_placed_edge
  source_column: customer_id
  target_table_id: bench_customer_node
  target_column: customer_id
  cardinality: many-to-one
```

Relationship IDs must be unique within the file. Table IDs referenced here come from the
`table:` field in the `tables:` block above, or from auto-discovered tables for native
connectors (use the actual table name as the ID for those).

## Step-by-step: build a new named demo

This walkthrough assumes a fictional `--demo banking` scenario with two local databases and one
external Snowflake source. The Snowflake source carries no local container.

**1. Create the directory.**

```bash
mkdir -p demo/named/banking/data
```

**2. Write `docker-compose.yml`.**

Include one service per local data store plus a `seeder` service. Add the standard labels.
Use bind mounts to `./data/`. Expose ports on non-conflicting host ports.

```yaml
x-demo-labels: &demo-labels
  com.provisa.demo: banking

services:
  postgresql:
    image: postgres:16
    labels:
      <<: *demo-labels
      com.provisa.demo.role: rdb
    ports:
      - "${PROVISA_BANKING_PG_PORT:-25700}:5432"
    volumes:
      - ./data/postgresql:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U provisa -d banking"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 30s
    environment:
      POSTGRES_USER: provisa
      POSTGRES_PASSWORD: provisa
      POSTGRES_DB: banking

  seeder:
    build:
      context: .
      dockerfile: Dockerfile.seeder
    restart: "no"
    labels:
      <<: *demo-labels
      com.provisa.demo.role: seeder
    depends_on:
      postgresql:
        condition: service_healthy
    environment:
      PROVISA_BANKING_PG_HOST: postgresql
      PROVISA_BANKING_PG_PORT: "5432"
    volumes:
      - ./data:/data-marker
```

The Snowflake source requires no service entry — it is external. Its credentials belong in
`fragment.yaml` via env-var interpolation.

**3. Write `Dockerfile.seeder`.**

Install only the drivers your generation scripts need.

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary
WORKDIR /app
COPY generate_postgres.py seed.py ./
ENTRYPOINT ["python", "seed.py"]
```

**4. Write `seed.py`.**

Check for a marker file. If present, skip. If absent, run generation scripts in order and
write the marker.

```python
import os, subprocess, sys, time
from pathlib import Path

MARKER = Path("/data-marker/.seeded")

def main():
    if MARKER.exists():
        print("already seeded; skipping")
        return 0
    t0 = time.monotonic()
    subprocess.run([sys.executable, "generate_postgres.py"], check=True)
    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(f"seeded in {time.monotonic() - t0:.0f}s\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
```

**5. Write `generate_postgres.py` (and any other generate scripts).**

Read host and port from env vars, with localhost + published port as the defaults. This lets
the script run both from inside the seeder container and directly from the host for debugging.

```python
import os
HOST = os.environ.get("PROVISA_BANKING_PG_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BANKING_PG_PORT", "25700"))
```

**6. Write `fragment.yaml`.**

Register a domain. For each native-connector source (postgresql, clickhouse, mongodb): one
`sources:` entry, no `tables:`. For each api_source-backed source (neo4j, elasticsearch): one
`sources:` entry and a `tables:` block with every projected table spelled out. For external
sources like Snowflake: one `sources:` entry with credentials as env-var interpolations.

```yaml
domains:
- id: banking-demo
  description: "Banking vertical demo"

sources:
- id: banking-pg
  type: postgresql
  host: ${env:PROVISA_BANKING_PG_HOST:-localhost}
  port: ${env:PROVISA_BANKING_PG_PORT:-25700}
  database: banking
  username: provisa
  password: provisa

- id: banking-snowflake
  type: snowflake
  account: ${env:PROVISA_BANKING_SNOWFLAKE_ACCOUNT}
  username: ${env:PROVISA_BANKING_SNOWFLAKE_USER}
  password: ${env:PROVISA_BANKING_SNOWFLAKE_PASSWORD}
  database: ${env:PROVISA_BANKING_SNOWFLAKE_DATABASE}
```

**7. Bring the stack up and test.**

```bash
docker compose -f demo/named/banking/docker-compose.yml up -d
```

Watch `docker compose logs seeder` to confirm generation finishes and the marker is written.

**8. Start Provisa with the named demo.**

```bash
./start-ui-install.sh --demo banking
```

The launcher splices `fragment.yaml` in, prints `Config with named demo 'banking' sources:`,
and reminds you the compose stack must already be up.

## The `--source` mechanism: what it is not

`--source=<name>` adds a single source from `demo/sources/<name>/` to the standard demo. Each
source in that directory carries its own `fragment.yaml` and a `prime.py` that seeds a small
toy dataset. The launcher starts and stops those containers. [tool-verified, start-ui-install.sh:37]

Named demos deliberately do not follow that pattern. Their data is large, meant to persist
across restarts, and they manage their own compose lifecycle. `start-ui-install.sh --demo <name>`
never starts, stops, or resets the named demo's compose stack — that is the solutions engineer's
responsibility, not the launcher's [tool-verified from `start-ui-install.sh:264-288`].

You can combine both: `./start-ui-install.sh --demo perf --source=cassandra` adds a toy
Cassandra source on top of the perf demo. The two mechanisms are additive.
