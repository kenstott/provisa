# Developer Quick Start

For evaluating Provisa without building from source, see the [Quick Start](index.md) — download the macOS, Windows, or Linux installer and run `provisa start`.

This guide is for running Provisa **from the repository** — active development, debugging, or contributing.

---

## Prerequisites

- **Docker Desktop** (running)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Clone and set up

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` creates `.venv/`, installs all Python dependencies via `pip install -e ".[dev]"`, and configures git hooks to `.githooks/`.

---

## 2. Start everything

```bash
./start-ui.sh
```

When it finishes starting up you'll see:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**What it starts:**

- Docker Compose core services (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis
- Docker Compose dev overlay (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry
- Backend API on port 8001 (hot-reload on changes to `provisa/` and `config/`)
- Vite UI dev server on port 3000 (HMR)
- OpenTelemetry tracing and Grafana at `http://localhost:3100`. The observability stack is an opt-in docker-compose `observability` profile (OTel Collector, Prometheus, Tempo, Grafana), not on by default at the platform level; `start-ui.sh` enables it as a dev-script convenience unless you pass `--no-observability`.

**Ctrl+C** stops everything — backend, UI, and all Docker services — and reverts any config patches.

**Ctrl+R** restarts only the backend (useful after a config change that hot-reload misses).

### Options

`--no-observability` — Disables distributed tracing. By default, `start-ui.sh` downloads the OpenTelemetry Java agent if not already present, patches Trino's `jvm.config` to load it, and starts the OTel collector, Prometheus, Tempo, and Grafana. Pass `--no-observability` to skip all of that. The `jvm.config` patch is reverted on Ctrl+C.

`--seed-data` — Seeds Kafka with demo data after Docker services are healthy. Not run by default.

`--keep-docker` — Leaves Docker Compose services running after Ctrl+C instead of calling `docker compose down`.

`--reset-volumes` — Wipes all Docker volumes and restarts with a clean state. Useful for Docker crash recovery.

`--demo` — Starts additional demo data sources (PostgreSQL pet-store schema, OpenAPI petstore mock, SQLite, and a GraphQL remote). Seeds petstore users and orders automatically.

`--source=<name>` (`start-ui-install.sh` only, repeatable) — Provisions an optional data source alongside `--demo`. Each name maps to `demo/sources/<name>/`. The start calls `demo/sources/provision.py up`, which starts the source's `compose.yml` as its own Docker Compose project (`provisa-demo-<name>`), waits for its health check, and runs `prime.py` when the source has one to seed data. The start then writes a wrapper config at `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` that includes the base config plus each source's `fragment.yaml`, and boots from it.

The same `provision.py` is what the UI end-to-end suite calls to stand up these sources (under the `provisa-e2e-<name>` project prefix on its own ports), so the seed data a demo shows and the rows the suite asserts on are defined once.

Under a Docker start (no `--demo`/`--native`) the coordinator is a container, so each source is joined to the core stack's network and registered at `<name>:<container port>`; under a native start it is registered at `localhost:<published port>`. A source whose `demo/sources/<name>/engine` file names an engine the start does not run is refused.

Shipped sources:

| Name | Port(s) | Notes |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | Two Cypher tables (`adopter`, `adopter_referral`); graph seeded by `seed.cypher`; tables registered from the fragment |
| `mongodb` | 27117 | Source registered; `product_reviews` collection seeded by `db/mongo-init.js`; register tables by hand through Register Table |
| `redis` | 26379 | Source registered; `support_agent:*` and `agent_status:*` hashes seeded by `prime.py`; each prefix registers as a table through Register Table |
| `cassandra` | 29042 | Source registered; `shelter_ops.intake_events` seeded by `prime.py` (needs the `cassandra` extra); the keyspace registers as a schema through Register Table |
| `sparql` | 23030 | Apache Jena Fuseki; source and one query-backed table (`volunteer`) registered from the fragment, graph seeded by `prime.py`; more tables through Register Table (query + Preview) |
| `prometheus` | 29090 | Source registered; the server scrapes itself, so `up` and the `prometheus_*` metrics register as tables through Register Table |
| `elasticsearch` | 29200 | Source and index mapping registered; `support_tickets` index seeded by `prime.py`; read over HTTP by the native engine, through the connector on Trino |
| `splunk` | mgmt 8089, HEC 8088 | Source registered with token auth and `disable_ssl_validation` (the container's certificate is self-signed); an index, seven shelter-alert events and the `shelter_alerts` Data Model seeded by `prime.py`, which also mints the API token the fragment reads as `PROVISA_DEMO_SPLUNK_TOKEN`. Data Models register as tables through Register Table: on Trino through the `splunk` catalog, and on every other engine through the Splunk driver's pgwire endpoint, which the engine attaches |
| `chinook` | 25433 | Postgres holding the snake_case Chinook subset Hasura's metadata sample tracks, seeded by `prime.py` from `tests/fixtures/hasura_v2_t1_seed.sql`; source registered from the fragment, and the source a Hasura v2 import of `tests/fixtures/hasura_v2_t1_metadata.json` lands on |

`--idp=basic|firebase` — Enables an identity provider for auth. Without this flag, the backend runs with no auth provider and all requests are treated as `admin`.

---

## 3. Connect a data source

Provisa reads configuration from `config/`. Add a source file — for example `config/sources/my-db.yaml`:

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

Set the env var and the backend will pick it up on next reload:

```bash
export MY_DB_PASSWORD=secret
```

See [docs/configuration.md](configuration.md) for the full YAML reference and all supported source types.

---

## 4. Run your first query

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

No auth is required when no `auth` section is present in `config/provisa.yaml` (the default in dev). The role defaults to `admin`.

---

## 5. Open the UI

Open `http://localhost:3000` in a browser.

The nav bar has four top-level menus:

- **Explore** — Schema Explorer (`/schema`), GraphQL editor (`/query`), Cypher editor (`/graph`), SQL editor (`/sql`)
- **Model** — Views and Commands
- **Security** — Row-level security and column masking policies
- **Admin** — Overview, domains, cache, scheduled tasks, system health, observability, users, orgs, roles

The admin GraphQL API is at `http://localhost:8001/admin/graphql`.

---

## Troubleshooting

**Backend won't start** — check `.logs/server.log`. Most common cause is a missing env var or a port conflict on 8001.

**Docker services not healthy** — run `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` to see which service is stuck. The federation engine takes ~30 seconds on first start.

**Port conflict on 3000 or 8001** — `start-ui.sh` kills stale processes on those ports before starting. If something else owns the port, stop it manually first.

**Fresh start** — stop the script, then run `./start-ui.sh --reset-volumes` to wipe all volumes and restart.

---

## Next steps

| Goal | Doc |
| ------ | ----- |
| Full YAML configuration reference | [configuration.md](configuration.md) |
| Row-level security, column masking, auth | [security.md](security.md) |
| All supported source types | [sources.md](sources.md) |
| Real-time subscriptions | [subscriptions.md](subscriptions.md) |
| JDBC, BI tools, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Python client | [python-client.md](python-client.md) |
| Production deployment | [deployment.md](deployment.md) |
