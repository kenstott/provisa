# Developer Quick Start

For evaluating Provisa without building from source, see the [README Quick Start](../README.md#quick-start) — download the macOS, Windows, or Linux installer and run `provisa start`. (REQ-223, REQ-224, REQ-227)

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

`setup.sh` creates `.venv/`, installs all Python dependencies via `pip install -e ".[dev]"`, and configures git hooks to `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Start everything

```bash
./start-ui.sh
```

When it finishes starting up you'll see:

```
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**What it starts:** [tool-verified: start-ui.sh]

- Docker Compose core services (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Docker Compose dev overlay (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- Backend API on port 8001 (hot-reload on changes to `provisa/` and `config/`) (REQ-618)
- Vite UI dev server on port 3000 (HMR)
- OpenTelemetry tracing and Grafana at `http://localhost:3100` (observability is on by default) (REQ-302, REQ-303, REQ-330)

**Ctrl+C** stops everything — backend, UI, and all Docker services — and reverts any config patches. (REQ-619)

**Ctrl+R** restarts only the backend (useful after a config change that hot-reload misses). (REQ-619)

### Options

`--no-observability` — Disables distributed tracing. By default, `start-ui.sh` downloads the OpenTelemetry Java agent if not already present, patches Trino's `jvm.config` to load it, and starts the OTel collector, Prometheus, Tempo, and Grafana. Pass `--no-observability` to skip all of that. The `jvm.config` patch is reverted on Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Seeds Kafka with demo data after Docker services are healthy. Not run by default. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Leaves Docker Compose services running after Ctrl+C instead of calling `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Wipes all Docker volumes and restarts with a clean state. Useful for Docker crash recovery. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Starts additional demo data sources (PostgreSQL pet-store schema, OpenAPI petstore mock, SQLite, and a GraphQL remote). Seeds petstore users and orders automatically. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Enables an identity provider for auth. Without this flag, the backend runs with no auth provider and all requests are treated as `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

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

No auth is required when no `auth` section is present in `config/provisa.yaml` (the default in dev). The role defaults to `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Open the UI

Open `http://localhost:3000` in a browser.

The nav bar has four top-level menus: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Schema Explorer (`/schema`), GraphQL editor (`/query`), Cypher editor (`/graph`), SQL editor (`/sql`)
- **Model** — Views and Commands
- **Security** — Row-level security and column masking policies (REQ-038, REQ-041)
- **Admin** — Overview, domains, cache, scheduled tasks, system health, observability, users, orgs, roles

The admin GraphQL API is at `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Troubleshooting

**Backend won't start** — check `.logs/server.log`. Most common cause is a missing env var or a port conflict on 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Docker services not healthy** — run `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` to see which service is stuck. The federation engine takes ~30 seconds on first start. (REQ-055)

**Port conflict on 3000 or 8001** — `start-ui.sh` kills stale processes on those ports before starting. If something else owns the port, stop it manually first. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Fresh start** — stop the script, then run `./start-ui.sh --reset-volumes` to wipe all volumes and restart. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Next steps

| Goal | Doc |
|------|-----|
| Full YAML configuration reference | [configuration.md](configuration.md) |
| Row-level security, column masking, auth | [security.md](security.md) |
| All supported source types | [sources.md](sources.md) |
| Real-time subscriptions | [subscriptions.md](subscriptions.md) |
| JDBC, BI tools, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Python client | [python-client.md](python-client.md) |
| Production deployment | [deployment.md](deployment.md) |
