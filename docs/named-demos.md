# How to create a named demo

A named demo is a self-contained, reusable demo environment you drop into `demo/named/<name>/`.
Once the directory exists, `start-ui-install.sh --demo <name>` picks it up with no change to
the launcher script and no developer involvement required (REQ-1858). This is the mechanism for
client-specific and vertical demo scenarios — a future `--demo banking` or `--demo healthcare`
is just a new directory in this shape.

Named demos are distinct from the `--source=<name>` mechanism. The `--source` flag adds a single
toy source to the standard demo (backed by `demo/sources/<name>/`). A named demo is a
self-contained multi-source package: its own config.yaml (a whole config), its own
docker-compose.yml when it has data stores, and whatever seeding tooling the scenario needs.

## The fixed directory layout

Every named demo lives at `demo/named/<name>/` and contains these files:

```
demo/named/<name>/
  config.yaml           # required — the demo's whole config, used in place of the standard demo's
  docker-compose.yml    # the one start point for the demo's own data stores (none: omit it)
  Dockerfile.seeder     # if the demo self-seeds (recommended)
  seed.py               # entrypoint for the seeder service
  generate_*.py         # one per data store, optional but conventional
```

`start-ui-install.sh` picks the demo's config generically, from `demo/named/$DEMO_NAME/`
[tool-verified]:

```bash
# start-ui-install.sh, after the directory check
_NAMED_WHOLE=false
if [ -n "$DEMO_NAME" ]; then
  _NC="$SCRIPT_DIR/demo/named/$DEMO_NAME/config.yaml"
  _NF="$SCRIPT_DIR/demo/named/$DEMO_NAME/fragment.yaml"
  if [ -f "$_NC" ] && [ -f "$_NF" ]; then
    echo "--demo $DEMO_NAME has both config.yaml and fragment.yaml in demo/named/$DEMO_NAME/. Keep one: ..."; exit 1
  fi
  if [ ! -f "$_NC" ] && [ ! -f "$_NF" ]; then
    echo "--demo $DEMO_NAME has no config.yaml (and no fragment.yaml) in demo/named/$DEMO_NAME/ to register it with."; exit 1
  fi
  [ -f "$_NC" ] && _NAMED_WHOLE=true
fi
```

`config.yaml` is a **whole config**: the launcher sets `PROVISA_CONFIG` to it in place of the
standard demo config (`config/provisa-install.yaml`), so the demo is exactly what the file lists.
A `--source` toy source is still overlaid on top. A demo may instead ship a `fragment.yaml`,
overlaid on the standard config; a demo with neither, or with both, is refused by name. The name
itself is not in any registry or allowlist.

## The one-start-point invariant

`docker compose up -d` against `docker-compose.yml` is the complete setup-and-seed story. No
manual commands. No host-side scripts to run before it works.

This does **not** mean the demo must be a single process or single container. The `perf` demo
runs five containers (four engines plus a seeder) [tool-verified]. A demo may also reference
external managed resources it does not containerize — a live Databricks or Snowflake source
reached by connection string, registered in `config.yaml` with no corresponding compose
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
being deleted. A start without `--keep-data` deletes it, so the marker only matters to a demo started
with `--keep-data`. [tool-verified]

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

## config.yaml — the demo's whole config

`config.yaml` is a static, checked-in config file in Provisa's standard YAML config format. Every
start rebuilds the control plane from it. The usual way to produce it is to run the standard demo,
build the scenario in the UI, and export the model (Admin → Maintenance → Configuration File →
View / Diff, the **Current** pane, or `GET /admin/config/live`), then save it as `config.yaml`.
The export omits sources added in the UI, so add them to `sources:` by hand. The excerpts below
come from `demo/named/perf/config.yaml`, which is the standard demo config with the perf sections
appended.

### Every source needs explicit `tables:`

Register each source and then list every table it exposes. A bare `sources:` entry never
produces tables in the SQL catalog, for any connector type, because the catalog is built from the
registered tables [tool-verified from the notes in `demo/named/perf/config.yaml`]:

```yaml
# demo/named/perf/config.yaml, the bench-postgresql source
- id: bench-postgresql
  type: postgresql
  host: ${env:PROVISA_BENCH_POSTGRESQL_HOST:-localhost}
  port: ${env:PROVISA_BENCH_POSTGRESQL_PORT:-25632}
  database: provisa_bench
  username: provisa
  password: provisa
```

Use env-var interpolation with a `:-` default for every host and port. The seeder uses the
in-container service name as the host; the host-side generate scripts use `localhost` plus the
published port. The config uses the same env-var names so both work without editing the file
[tool-verified from `generate_postgres.py:29-30`]:

```python
HOST = os.environ.get("PROVISA_BENCH_POSTGRESQL_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_POSTGRESQL_PORT", "25632"))
```

### Sources backed by api_source (each table is a query)

Neo4j is an `api_source`-backed type where each table is one fixed Cypher projection. You
hand-author every table in `config.yaml`:

```yaml
# demo/named/perf/config.yaml (excerpt)
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



### Relationships

Cross-table relationships go in a top-level `relationships:` block [tool-verified]:

```yaml
# demo/named/perf/config.yaml (excerpt)
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
`config.yaml` via env-var interpolation.

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

**6. Create `config.yaml`.**

Start the data stack and the standard demo, build the scenario in the UI (the two local databases
and the Snowflake source, their tables and relationships), export the model, and save it as
`demo/named/banking/config.yaml`. Add the sources you created in the UI to its `sources:` list by
hand, since the export omits them. Give each connection value as an env-var interpolation, and the
external Snowflake source's credentials too:

```yaml
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

Every source still needs explicit `tables:`, which the export supplies. For a small addition to the
pet store you can ship a `fragment.yaml` of list sections instead of a whole config; see
[Running and building demos on your laptop](sales-engineer-demos.md#pro-tip-a-fragment-instead-of-a-whole-config).

**7. Bring the stack up and test.**

```bash
docker compose -f demo/named/banking/docker-compose.yml up -d
```

Watch `docker compose logs seeder` to confirm generation finishes and the marker is written.

**8. Start Provisa with the named demo.**

```bash
./start-ui-install.sh --demo banking
```

The launcher brings the compose stack up and waits for the seeder, uses `config.yaml` in place of the
standard demo config, and prints `Config for named demo 'banking': ...`. The stack in step 7 can be started first for
testing, but unless you pass `--keep-data` the launcher removes it and seeds it again.

## The `--source` mechanism: what it is not

`--source=<name>` adds a single source from `demo/sources/<name>/` to the standard demo. Each
source in that directory carries its own `fragment.yaml` and a `prime.py` that seeds a small
toy dataset. The launcher starts and stops those containers. [tool-verified, start-ui-install.sh:37]

Named demos deliberately do not follow that pattern. Their data is large, meant to persist
across restarts, and they manage their own compose lifecycle. `start-ui-install.sh --demo <name>`
starts the named demo's stack on every launch. By default it first removes the stack's containers and
volumes and deletes its `data/` directory (`docker compose down -v`), so the seeder reseeds from
empty; `--keep-data` skips that and reuses the data from the last seeding. The launcher never stops
the stack: stopping it is yours to do. See [Running and building demos on your
laptop](sales-engineer-demos.md#stop-and-reset) [tool-verified from `start-ui-install.sh:285-310`].

You can combine both: `./start-ui-install.sh --demo perf --source=cassandra` adds a toy
Cassandra source on top of the perf demo. The two mechanisms are additive.
