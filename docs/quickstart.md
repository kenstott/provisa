# Quick Start

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Node.js 20+ (UI only)

## 1. Start Infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL, Trino, Redis, MinIO, PgBouncer, Kafka, and the Zaychik Arrow Flight proxy. Trino takes ~30 seconds to become ready.

Check readiness:
```bash
docker compose ps          # all services should be healthy
curl http://localhost:8080/v1/info   # Trino: {"starting":false,...}
```

## 2. Install Provisa

```bash
pip install -e ".[dev]"
export PG_PASSWORD=provisa
```

## 3. Write a Config

Create `config.yaml`:

```yaml
sources:
  - id: sales-pg
    type: postgresql
    host: localhost
    port: 5432
    database: provisa
    username: provisa
    password: ${PG_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id

roles:
  - id: admin
    full_results: true
  - id: analyst
    row_limit: 1000
```

See [docs/configuration.md](configuration.md) for the full YAML reference.

## 4. Start the Server

```bash
uvicorn main:app --reload --port 8001
```

On startup Provisa:
1. Loads `config.yaml`
2. Registers Trino dynamic catalogs for each source
3. Generates per-role GraphQL schemas from `INFORMATION_SCHEMA`
4. Starts background services (MV refresh, cache warm-up)

## 5. Run Your First Query

```bash
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }", "role": "admin"}'
```

Response:
```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 142.50, "region": "west"},
      ...
    ]
  }
}
```

## 6. Open the UI

```bash
cd provisa-ui && npm install && npm run dev
```

Navigate to `http://localhost:5173`. The UI provides:
- **Explorer** — browse registered tables and columns
- **Query** — GraphiQL with View SQL and Submit for Approval
- **Admin** — config editor, relationship editor, query approval queue

## 7. Add Row-Level Security

```yaml
sources:
  - id: sales-pg
    tables:
      - id: orders
        rls:
          - role_id: analyst
            filter: "region = '{{ role.region }}'"
```

The `{{ role.region }}` token is replaced at query time with the value from the authenticated user's role claims.

## Next Steps

| Goal | Doc |
|------|-----|
| Lock down data access | [security.md](security.md) |
| Connect additional data sources | [sources.md](sources.md) |
| Set up production deployment | [deployment.md](deployment.md) |
| Expose data to BI tools | [integrations.md](integrations.md) |
| Configure real-time subscriptions | [subscriptions.md](subscriptions.md) |
| Import from Hasura | [import.md](import.md) |
