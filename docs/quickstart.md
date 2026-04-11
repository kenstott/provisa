# Developer Quick Start

For evaluating Provisa without building from source, see the [README Quick Start](../README.md#quick-start) — download the macOS, Windows, or Linux installer and run `provisa start`.

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

`setup.sh` creates `.venv/`, installs all Python dependencies (including dev extras), and configures git hooks.

---

## 2. Start everything

```bash
./start-ui.sh
```

That's it. When it finishes starting up you'll see:

```
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**What it starts:**
- Docker Compose core services (PostgreSQL, PgBouncer, Federation engine, Redis, MinIO)
- Docker Compose dev services (Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry)
- Seeds Kafka with demo data
- Backend API on port 8001 (hot-reload on changes to `provisa/` and `config/`)
- Vite UI dev server on port 3000 (HMR)

**Ctrl+C** stops everything — backend, UI, and all Docker services — and reverts any config patches.

### Options

`--reset-volumes` — Destroys all Docker volumes before starting (PostgreSQL data, MinIO objects, Redis state, etc.). Use after a schema change or when Docker has left volumes in a bad state. **All data will be lost.**

`--observability` — Adds distributed tracing and metrics. Downloads the OpenTelemetry Java agent, patches Trino's `jvm.config` to load it, and starts the OTel collector, Prometheus, Tempo, and Grafana at `http://localhost:3100`. The `jvm.config` patch is reverted on Ctrl+C.

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

Set the env var and the backend will pick it up on next reload (hot-reload is active):

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
  -d '{"query": "{ orders { id amount region } "}' | jq

# SQL — same endpoint, detected automatically
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

No auth is required when `PROVISA_MODE=test` (the default in dev). The role defaults to `admin`.

---

## 5. Open the UI

Navigate to `http://localhost:3000`.

- **Explorer** — browse registered sources, tables, and columns
- **Query** — GraphiQL editor with "View SQL" and "Submit for Approval"
- **Admin** — config editor, relationship editor, query approval queue

The admin GraphQL API is at `http://localhost:8001/admin/graphql`.

---

## Troubleshooting

**Backend won't start** — check `.logs/server.log`. Most common cause is a missing env var or a port conflict on 8001.

**Docker services not healthy** — run `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` to see which service is stuck. The federation engine takes ~30 seconds on first start.

**Port conflict on 3000 or 8001** — `start-ui.sh` kills stale processes on those ports before starting. If something else owns the port, stop it manually first.

**Fresh start** — `./start-ui.sh --reset-volumes` wipes all state and starts clean.

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
