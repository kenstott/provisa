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
| Federation Engine | 8080 | Query federation engine |
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
| `trino.enabled` | `true` | Deploy the federation engine sub-chart |
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

The federation engine scales independently via its own Helm chart. Add worker nodes to increase federation throughput.

---

## Desktop Installers

All three desktop installers are fully airgapped — no internet connection required after download, and no prerequisites to install.

---

## macOS

1. Download `Provisa-<version>-macOS.dmg` from the releases page
2. Open the DMG and drag **Provisa.app** to `/Applications`
3. Double-click Provisa.app — first-launch setup runs once (~2 minutes)
4. Open Terminal:

```bash
provisa start
provisa open    # opens the UI in your browser
```

### Data Persistence

All data is stored in `~/.provisa/`. Use `provisa uninstall` to remove everything.

---

## Linux

1. Download `Provisa-<version>-linux-x86_64.AppImage` from the releases page
2. Make it executable and run it — first-launch setup runs once (no internet required):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
```

3. Open a terminal:

```bash
provisa start && provisa open
```

### Data Persistence

All data is stored in `~/.provisa/`.

---

## Windows

1. Download `Provisa-<version>-windows-x64.exe` from the releases page
2. Run the installer — no admin rights required; installs to `%LOCALAPPDATA%\Programs\Provisa\`
3. Open **Provisa First Launch** from the Start Menu — first-launch setup runs once (~5 minutes)
4. Open a new terminal:

```
provisa start
```

### Data Persistence

All data is stored in `%USERPROFILE%\.provisa\`.

---

## CLI Commands (all platforms)

```bash
provisa start       # Start all services
provisa stop        # Stop all services
provisa restart     # Restart
provisa status      # Show service health
provisa open        # Open the UI in the browser
provisa logs        # Tail service logs
```

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
| `TRINO_HOST` | `localhost` | Federation engine host |
| `TRINO_PORT` | `8080` | Federation engine HTTP port |
