# Deployment

## Choosing a Deployment Path

Provisa supports six deployment paths. Choose based on your audience and operational context:

| Path | Artifact / Script | Best for |
|------|-------------------|----------|
| **Development** | `start-ui.sh` | From-source development, evaluation with full demo data |
| **macOS installer** | `Provisa-<version>-macOS.dmg` | Developer workstations, evaluation |
| **Windows installer** | `Provisa-<version>-windows-x64.exe` | Developer workstations, evaluation |
| **Linux AppImage** | `Provisa.AppImage` | On-prem servers, cloud VMs, air-gapped environments |
| **Cloud VMs (AWS)** | `terraform/deploy.sh` | Multi-node cloud deployment with load balancers |
| **Kubernetes** | `helm/provisa/` | Teams already operating K8s |

### VM vs Kubernetes

Both are enterprise-grade. The VM/AppImage path is simpler: no cluster to provision, no CNI or RBAC policies to configure, and the AppImage is entirely self-contained. It fits naturally into existing server management tooling (Ansible, Puppet, Datadog agents, Splunk forwarders, etc.).

Choose Kubernetes only if your team already operates a K8s cluster and wants Provisa to participate in that operational model (rolling deploys, HPA, unified observability). The capabilities are equivalent — Kubernetes adds operational overhead, not capability.

### Image acquisition and security scanning

All production paths require obtaining the Provisa artifacts before any deployment can run. "Air-gapped" refers to what happens at install time on the target machine — the artifacts must be acquired first.

**macOS and Windows installers:** Download from the [GitHub releases page](https://github.com/provisa/provisa/releases). Fully bundled; no internet required after download. Intended for dev/eval, not production — no image scanning gate expected.

**AppImage path:** Download from the [GitHub releases page](https://github.com/provisa/provisa/releases) and transfer to the target machine. The AppImage bundles all component images as tarballs inside a squashfs filesystem — most registry scanners cannot inspect these in-place. Contact your Provisa account team for component image digests to verify against your scanner independently.

**Terraform path:** The AppImage must be uploaded to S3 before running `terraform/deploy.sh`. EC2 nodes download it at boot via IAM role — they require outbound S3 access (direct or via VPC gateway endpoint). Apply the same scanning policy as the AppImage path.

**Helm / Kubernetes path:** Individual images must be pushed to a registry the cluster can reach. This path is most compatible with registry-based scanning (Prisma Cloud, Aqua, Trivy, AWS Inspector) — images are first-class objects scanners understand natively. For air-gapped clusters, mirror images to an internal registry and override references in `values.yaml`.

---

## Development (from source)

### Recommended: `start-ui.sh`

The easiest way to run Provisa from source. Starts all infrastructure, the backend API, and the UI dev server in one command. Ctrl+C shuts everything down cleanly.

**Prerequisites:** Docker Desktop, Node.js, Python virtualenv at `.venv/`

```bash
./start-ui.sh
```

What it does:
- Starts `docker-compose.core.yml` + `docker-compose.dev.yml` (all core + demo services) and waits for healthy
- Seeds Kafka with demo data
- Syncs Python dependencies from `.venv/`
- Starts the backend API on port 8001 (logs to `.logs/server.log`)
- Starts the Vite UI dev server on port 3000
- Prints URLs and waits; Ctrl+C stops everything and tears down compose

```
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Options:**

`--reset-volumes` — Runs `docker compose down -v` before starting, destroying all Docker volumes (PostgreSQL data, MinIO objects, Redis state, etc.). Use when you want a completely clean slate — after a schema change during development, or when Docker has crashed and left volumes corrupt. **All data will be lost.**

`--observability` — Adds full tracing and metrics instrumentation. Downloads the OpenTelemetry Java agent and patches Trino's `jvm.config` to load it, instruments the Provisa backend with OTLP export, and starts the OTel collector, Prometheus, Tempo, and Grafana (`http://localhost:3100`). The `jvm.config` patch is automatically reverted on Ctrl+C.

### Manual steps (backend only, no UI)

If you only need the API:

1. Install [Docker Desktop](https://docs.docker.com/get-docker/)
2. Start core services:
   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```
3. Start the API:
   ```bash
   uvicorn main:app --reload --port 8001
   ```
4. Verify: `curl http://localhost:8001/health`

### Full stack (Provisa in container)

To run the API as a container instead of on the host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Services

**Core (`docker-compose.core.yml`) — always required:**

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL | 5432 | Config metadata + Iceberg catalog |
| PgBouncer | 6432 | Connection pooling |
| Federation engine | 8080 | Query federation |
| Redis | 6379 | Query result cache |
| MinIO | 9000/9001 | S3-compatible object storage |

**Demo (`docker-compose.dev.yml`) — optional, included by `start-ui.sh`:**

| Service | Port | Purpose |
|---------|------|---------|
| MongoDB | 27017 | Demo NoSQL source |
| Kafka | 9092 | Demo streaming source |
| Schema Registry | 8081 | Demo Avro/Protobuf schema management |
| Debezium | — | Demo CDC connector |
| Elasticsearch | 9200 | Demo search source |
| Neo4j | 7474/7687 | Demo graph source |
| Fuseki | 3030 | Demo SPARQL triplestore |
| OpenTelemetry Collector | — | Trace collection (with `--observability`) |
| Prometheus | 9090 | Metrics (with `--observability`) |
| Tempo | — | Trace storage (with `--observability`) |
| Grafana | 3100 | Dashboards (with `--observability`) |

---

## macOS Installer

For developer workstations and evaluation. Fully air-gapped — no internet required after download.

### Steps

1. Download `Provisa-<version>-macOS.dmg` from the [GitHub releases page](https://github.com/provisa/provisa/releases)
2. Open the DMG and drag **Provisa.app** to `/Applications`
3. Double-click **Provisa.app** — first-launch setup runs once (~2 minutes, loads bundled images)
4. Open Terminal:
   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in your browser
   ```

### Data persistence

All data is stored in `~/.provisa/`. To remove everything: `provisa uninstall`.

---

## Windows Installer

For developer workstations and evaluation. Fully air-gapped — no internet required after download.

### Steps

1. Download `Provisa-<version>-windows-x64.exe` from the [GitHub releases page](https://github.com/provisa/provisa/releases)
2. Run the installer — no admin rights required; installs to `%LOCALAPPDATA%\Programs\Provisa\`
3. Open **Provisa First Launch** from the Start Menu — setup runs once (~5 minutes)
4. Open a new terminal:
   ```
   provisa status
   provisa open
   ```

### Data persistence

All data is stored in `%USERPROFILE%\.provisa\`.

---

## Linux AppImage — Single or Multi-Node VM

### What it is

`Provisa.AppImage` is a single self-contained executable bundling:

- A rootless Docker daemon (`dockerd-rootless.sh` + `rootlesskit`) — no system Docker or root required
- All container image tarballs (PostgreSQL, PgBouncer, MinIO, Redis, Federation engine, Provisa API)
- The Provisa CLI wrapper and first-launch setup script

The Provisa image is pre-built at packaging time — Python source is never included.

### When to use

- On-premises bare metal or VM (single node or multi-node)
- Cloud VMs without a K8s cluster
- Air-gapped environments
- When you want simpler operations than Kubernetes

---

### Steps — Single Node

1. Download `Provisa.AppImage` from the [GitHub releases page](https://github.com/provisa/provisa/releases) and transfer to the target machine
2. Make it executable:
   ```bash
   chmod +x Provisa.AppImage
   ```
3. Run first-launch setup:
   ```bash
   ./Provisa.AppImage
   ```
4. The setup wizard asks:
   - **Role** → select `primary`
   - **RAM budget** → amount of RAM to allocate (0 = all available); determines Trino worker count
   - **Hostname** → this node's advertised address
   - **API port** → default `8000`
5. Setup loads all container images (~2–5 minutes), writes config, and starts services
6. Verify:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Steps — Multi-Node (Primary)

Run these steps on the primary node first. Secondaries must be set up after the primary is running.

1. Download and transfer `Provisa.AppImage` to the primary machine
2. Open required firewall ports (secondaries will connect inbound on these):

   | Port | Service |
   |------|---------|
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Federation engine coordinator |
   | 8000 | Provisa API |

3. Make executable and run:
   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```
4. The setup wizard asks:
   - **Role** → select `primary`
   - **RAM budget**, **hostname**, **API port** → answer as for single node
5. After setup completes, note the **private IP** of this machine — secondaries need it
6. The wizard prints an nginx upstream block — save it for your load balancer configuration
7. Verify:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Steps — Multi-Node (Each Secondary)

Repeat these steps on each additional node after the primary is running and reachable.

1. Download and transfer `Provisa.AppImage` to the secondary machine
2. Confirm the secondary can reach the primary:
   ```bash
   curl http://<primary-ip>:8000/health
   ```
3. Make executable and run:
   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```
4. The setup wizard asks:
   - **Role** → select `secondary`
   - **Primary IP** → enter the primary node's IP (connectivity is verified live)
   - **RAM budget**, **hostname**, **API port** → answer as above
5. Setup loads a reduced image set (no PostgreSQL, PgBouncer, MinIO, Redis — those run only on primary), starts the Provisa API and a federation engine worker
6. Verify:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```
7. Add this node to your load balancer upstream

---

### Primary / secondary topology

**Primary node** runs all singleton services:

| Service | Why singleton |
|---------|---------------|
| PostgreSQL | Shared schema, app config, governed queries, semantic model |
| Redis | Shared query result cache and subscription state |
| MinIO | Shared object store for redirect results and MV snapshots |
| Federation engine coordinator | All workers (primary + secondaries) register here |

**Secondary nodes** run only:
- Provisa API — stateless; reads all config from PostgreSQL on the primary at startup
- Federation engine worker — self-registers with the coordinator on the primary

All application state flows through the primary's PostgreSQL. No manual sync required.

---

### Non-interactive (automated) first-launch

For Terraform, cloud-init, or Ansible — pass flags instead of answering prompts:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Non-interactive mode installs a systemd unit (`/etc/systemd/system/provisa.service`) for start-on-boot.

| Flag | Description |
|------|-------------|
| `--non-interactive` | Skip all prompts; install systemd unit |
| `--role primary\|secondary` | Node role |
| `--primary-ip <ip>` | Primary node IP (required for secondary) |
| `--ram-gb <n>` | RAM to allocate (0 = all available) |

---

## Cloud VM Deployment — Terraform (AWS)

Provisions a full multi-node Provisa cluster on AWS — VPC, security groups, EC2 instances, ALB, NLB — in one interactive command.

### Files

| File | Purpose |
|------|---------|
| `terraform/deploy.sh` | Interactive wrapper — collects parameters, validates credentials, writes `terraform.tfvars`, runs apply |
| `terraform/aws/variables.tf` | All variable definitions with defaults |
| `terraform/aws/main.tf` | VPC, subnets, security groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | Endpoint URLs and node IPs |

### Steps

1. Download `Provisa.AppImage` from the [GitHub releases page](https://github.com/provisa/provisa/releases)

2. Upload it to an S3 bucket in your AWS account:
   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Ensure AWS credentials are available in your shell (any of):
   - Environment variables: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Named profile: `export AWS_PROFILE=my-profile`
   - Active SSO session: `aws sso login`

4. (Optional) If you want SSH access to nodes, create an EC2 key pair in your target region and note the key pair name

5. Run the deploy wrapper:
   ```bash
   bash terraform/deploy.sh
   ```

6. Answer the wizard questions (see reference table below). The script verifies the AppImage exists in S3 before proceeding and aborts if it does not

7. Review the deployment summary and confirm

8. Terraform provisions all infrastructure (~5–10 minutes). After apply, the script prints:
   ```
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

9. (Optional) Point DNS records at the ALB and NLB DNS names

10. Verify:
    ```bash
    curl http://<api_endpoint>/health
    ```

### Wizard questions

| Question | Default | Notes |
|----------|---------|-------|
| Cloud provider | — | AWS only today |
| AWS credentials | — | Checks for active session first |
| Region | `us-east-1` | |
| Node count | `2` | 1 = primary only, no LB; 2+ = primary + secondaries + ALB/NLB |
| Instance type | `m7i.2xlarge` | See sizing guide below |
| Root volume size | `100 GB` | Per node |
| RAM budget | `0` (all RAM) | Determines Trino worker count per node |
| S3 bucket | — | Verified live before proceeding |
| S3 key | `releases/Provisa.AppImage` | |
| SSH access | No | Requires existing key pair name + admin CIDR |
| VPC CIDR | `10.0.0.0/16` | |

### Instance sizing guide

| Type | vCPU | RAM | Trino workers/node | Use case |
|------|------|-----|--------------------|----------|
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / small datasets |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Small production |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Medium production |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Large production |

All nodes contribute workers to one coordinator on the primary. A 3-node `m7i.4xlarge` cluster yields 6 Trino workers total.

### What gets provisioned

- VPC with two public subnets across two availability zones
- Security groups: LB group (public ingress on 8000/8815), nodes group (LB → nodes, intra-cluster, optional SSH)
- IAM role + instance profile with S3 GetObject on the AppImage bucket
- Primary EC2 instance — runs first-launch in `--non-interactive --role primary` mode
- Secondary EC2 instances (node_count − 1) — run first-launch in `--non-interactive --role secondary --primary-ip <primary private IP>` mode; depend on primary completing first
- ALB on port 8000 — HTTP API, health-checks `/health`
- NLB on port 8815 — Arrow Flight / gRPC
- Both LBs attach to all nodes

### Prerequisites checklist

- [ ] IAM permissions: EC2 full, ELB full, VPC full, IAM role create, S3 GetObject on AppImage bucket
- [ ] `Provisa.AppImage` uploaded to S3
- [ ] EC2 nodes have outbound S3 access (direct internet or S3 VPC gateway endpoint)
- [ ] EC2 key pair exists in target region (if SSH is needed)
- [ ] Terraform ≥ 1.5 installed locally
- [ ] DNS records planned for ALB / NLB (optional but recommended)
- [ ] ACM certificate ready if HTTPS is required (not included in base Terraform)

### Secrets

No secrets are embedded in Terraform. The AppImage generates credentials during first-launch and writes them to `~/.provisa/config.yaml` on each node. For production, retrieve the admin token from the primary node after deployment:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### When to use

Your team already operates a Kubernetes cluster and wants Provisa to participate in that operational model. If you are evaluating Provisa or deploying on-premises without an existing cluster, the AppImage path is simpler.

Note: the Provisa AppImage cannot run inside a Kubernetes pod — it requires FUSE and a rootless Docker daemon, which are not available in standard pod security profiles.

### Steps

1. Confirm cluster access:
   ```bash
   kubectl cluster-info
   ```

2. Pull and mirror images to your internal registry (required for air-gapped or scanned environments; skip if pulling from public registries directly):

   | Image | Used for |
   |-------|----------|
   | `provisa/provisa:<version>` | Provisa API |
   | `trinodb/trino:480` | Federation engine coordinator + workers |
   | `postgres:16` | In-cluster PostgreSQL (if `postgresql.enabled`) |
   | `edoburu/pgbouncer:latest` | In-cluster PgBouncer (if `pgbouncer.enabled`) |
   | `redis:7.2` | In-cluster Redis (if `redis.enabled` and no `redis.host`) |
   | `minio/minio:latest` | In-cluster MinIO (if `minio.enabled`) |

   For registry-scanned environments:
   - Push each image to your staging registry
   - Run your scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) and obtain approval
   - Promote to your production internal registry

3. Decide before installing:
   - **PostgreSQL** — in-cluster (`postgresql.enabled: true`) or external managed (`postgresql.host`)? External recommended for production
   - **Redis** — in-cluster or external (`redis.host`)? Change the default password (`redis.password`)
   - **MinIO / S3** — in-cluster MinIO or native S3? For AWS, use S3 with an IAM role
   - **Secrets** — pass via `--set` for evaluation; use External Secrets or Vault Agent for production

4. Install the chart:
   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   If using an internal registry, add image overrides:
   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Verify pods are running:
   ```bash
   kubectl get pods -n provisa
   ```

6. Check the API:
   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Optional) Enable ingress for external access — set `ingress.enabled: true` and configure your ingress controller

### Prerequisites checklist

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Storage class supporting `ReadWriteOnce` PVCs (for in-cluster stateful services)
- [ ] Images available to the cluster (public or internal registry)
- [ ] PostgreSQL endpoint + credentials (if external)
- [ ] Redis endpoint + credentials (if external)
- [ ] S3 bucket + credentials or IAM role
- [ ] Admin token chosen
- [ ] Ingress controller configured (if external access needed)

### Key values

| Value | Default | Description |
|-------|---------|-------------|
| `replicaCount` | `2` | Provisa API replicas (stateless) |
| `config.pgHost` | `postgres` | PostgreSQL host |
| `config.pgPassword` | | PostgreSQL password |
| `config.adminToken` | | Admin API bearer token |
| `redis.enabled` | `true` | Deploy in-cluster Redis StatefulSet |
| `redis.host` | `""` | Set to use external Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Change this |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Deploy federation engine |
| `trino.workers` | `2` | Federation engine worker replicas |
| `postgresql.enabled` | `true` | Deploy in-cluster PostgreSQL |
| `postgresql.host` | `""` | Set to use external PostgreSQL |
| `minio.enabled` | `true` | Deploy in-cluster MinIO |
| `s3.endpoint` | | S3-compatible endpoint URL |
| `s3.bucket` | `provisa-results` | Bucket for large result redirect |
| `ingress.enabled` | `false` | Enable ingress |

### Scaling

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Federation engine workers scale independently — more workers increase throughput and concurrent query capacity.

### Updating config

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `PG_PASSWORD` | | PostgreSQL password |
| `PROVISA_ADMIN_TOKEN` | | Admin API bearer token |
| `PROVISA_CONFIG_PATH` | `config.yaml` | Path to config file |
| `PROVISA_REDIRECT_ENABLED` | `false` | Enable large result redirect to S3 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Row count threshold for redirect |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3 bucket |
| `PROVISA_REDIRECT_ENDPOINT` | | S3-compatible endpoint URL |
| `PROVISA_REDIRECT_TTL` | `3600` | Presigned URL TTL (seconds) |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | | Redis password |
| `REDIS_TLS` | `false` | Enable TLS for Redis |
| `TRINO_HOST` | `localhost` | Federation engine host |
| `TRINO_PORT` | `8080` | Federation engine HTTP port |

---

## CLI Commands

```bash
provisa start       # Start all services
provisa stop        # Stop all services
provisa restart     # Restart
provisa status      # Show service health
provisa open        # Open the UI in the browser
provisa logs        # Tail service logs
```
