# Deployment

## Development (Docker Compose)

The default `docker-compose.yml` starts all dependencies locally. Provisa itself runs on the host for fast iteration.

```bash
docker compose up -d
uvicorn main:app --reload --port 8001
```

Services started:

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL | 5432 | Config metadata + Iceberg catalog |
| PgBouncer | 6432 | Connection pooling |
| Trino | 8080 | Query federation engine |
| Zaychik | 8480 | Arrow Flight SQL proxy |
| Redis | 6379 | Query result cache |
| MinIO | 9000/9001 | S3-compatible object storage |
| Kafka | 9092 | Streaming sources |
| Schema Registry | 8081 | Avro/Protobuf schema management |
| MongoDB | 27017 | Demo NoSQL source |

### Full Stack (with Provisa container)

```bash
docker compose -f docker-compose.prod.yml up -d
```

Adds the Provisa API container alongside the dependency stack. Useful for integration testing without a local Python environment.

---

## Production (Kubernetes / Helm)

Provisa ships a Helm chart in `helm/provisa/`.

### Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- External PostgreSQL, Redis, and S3-compatible storage (or deploy via the chart's sub-charts)

### Install

```bash
helm repo add provisa https://charts.provisa.io
helm install provisa provisa/provisa \
  --set config.pgPassword=<password> \
  --set config.adminToken=<token> \
  --set s3.endpoint=https://s3.amazonaws.com \
  --set s3.bucket=my-provisa-results \
  --namespace provisa --create-namespace
```

### Key Values

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `2` | Provisa API replicas (stateless) |
| `config.pgHost` | `postgres` | PostgreSQL host |
| `config.redisHost` | `redis` | Redis host |
| `trino.enabled` | `true` | Deploy Trino sub-chart |
| `zaychik.enabled` | `true` | Deploy Zaychik sub-chart |
| `s3.endpoint` | | S3-compatible endpoint URL |
| `s3.bucket` | `provisa-results` | Bucket for large result redirect |
| `ingress.enabled` | `false` | Enable ingress for HTTP port |

### Config Map

The Provisa `config.yaml` is mounted as a Kubernetes ConfigMap. Update it:
```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

### Scaling

Provisa is stateless — all query state is in PostgreSQL and Redis. Scale horizontally:
```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Trino scales independently via its own Helm chart. Add worker nodes to increase federation throughput.

---

## macOS (Desktop Installer)

The macOS DMG installer packages Provisa as a self-contained desktop app with no external dependencies. It uses Lima VM + containerd to run all services in an airgapped environment — Docker is not required on the user machine.

### Install

1. Download `Provisa-<version>.dmg` from the releases page
2. Open the DMG and drag **Provisa.app** to `/Applications`
3. Double-click Provisa.app to trigger first-launch setup (one-time, ~2 minutes):
   - Stages bundled container image tarballs to `~/.provisa/images/`
   - Creates and starts a Lima VM named `provisa` (uses Virtualization.framework on Apple Silicon)
   - Imports all images into containerd inside the VM
   - Writes a default `~/.provisa/config.yaml`
   - Installs the `provisa` CLI at `/usr/local/bin/provisa` (prompts for password once)
4. Open Terminal and start services:

```bash
provisa start
provisa open    # opens the UI in your browser
```

All service images are bundled in the DMG — no internet connection required at install or runtime.

### CLI Commands

```bash
provisa start       # Start all services
provisa stop        # Stop all services
provisa restart     # Restart
provisa status      # Show service health
provisa open        # Open the UI in the browser
provisa logs        # Tail service logs
provisa upgrade     # Pull latest Provisa image
provisa uninstall   # Remove Lima VM and all data
```

### Data Persistence

All data is stored in `~/.provisa/`. The Lima VM disk image persists between restarts. Use `provisa uninstall` to remove everything.

### Airgap Notes

The installer bundles all required container images. After the initial download, Provisa runs fully offline. The `provisa upgrade` command requires internet access.

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PG_PASSWORD` | | PostgreSQL password |
| `PROVISA_ADMIN_TOKEN` | | Admin API bearer token |
| `PROVISA_CONFIG_PATH` | `config.yaml` | Path to config file |
| `PROVISA_REDIRECT_ENABLED` | `false` | Enable large result redirect |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Row count threshold for redirect |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 bucket |
| `PROVISA_REDIRECT_ENDPOINT` | | S3-compatible endpoint URL |
| `PROVISA_REDIRECT_TTL` | `3600` | Presigned URL TTL (seconds) |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `TRINO_HOST` | `localhost` | Trino host |
| `TRINO_PORT` | `8080` | Trino HTTP port |
