# Audit — Group 11: Platform

Date: 2026-06-18
Scope: **Group 11 — Platform, Infrastructure & Delivery** (REQ-055, 056, 057, 064,
065, 069, 070, 071, 072, 073, 074, 169, 170, 171, 223–228, 254, 255, 294, 302, 303,
330). Covers Docker Compose, Helm, installer/packaging (`packaging/`), OTel
instrumentation, error/architecture policy, and commercial tiers.
Method: read implementation against requirement text with file:line evidence.
Companion to the Group-2 audit ([group-2.md](group-2.md)).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 055 | Infrastructure | To spec | Single-command stack, Trino coordinator + replicas=0 workers, catalogs pre-loaded: `docker-compose.core.yml:52`, `start-ui.sh:54` |
| 056 | Infrastructure | To spec | HPA for Trino workers + resource groups in Helm chart: `helm/provisa/templates/trino-worker.yaml:94`, `trino/etc/resource-groups.json:1` |
| 057 | Infrastructure | To spec | Provisa is a stateless `Deployment` (no PVC); Trino topology is config: `helm/provisa/templates/provisa-deployment.yaml:8` |
| 064 | Error Handling | To spec | Fail-fast policy encoded; webhook executor raises on 4xx/5xx/timeout: `CLAUDE.md:1`, `tests/unit/test_error_handling.py:74` |
| 065 | Error Handling | To spec | No-migrations policy encoded; no migration tooling present: `CLAUDE.md:2` |
| 069 | Architecture | To spec | Planning docs present and maintained under `docs/arch/`: `docs/arch/requirements.md:1` |
| 070 | Architecture | To spec | Brevity policy encoded: `CLAUDE.md:3` |
| 071 | Architecture | To spec | requirements-tracker agent + workflow defined: `.claude/agents/requirements-tracker.md:1`, `CLAUDE.md:15` |
| 072 | Commercial | To spec | Core stack/compiler/transpiler open under BSL 1.1: `LICENSE:1`, `docker-compose.core.yml:1` |
| 073 | Commercial | Not added | No hosted control plane / customer data plane split; only `state.multitenancy` flag: `provisa/api/app.py:153` |
| 074 | Commercial | Incomplete | Query audit log exists; no SLA monitoring or compliance reporting: `provisa/audit/query_log.py:1` |
| 169 | Infrastructure | To spec | Trino 480, Iceberg results catalog (JDBC-on-PG + native S3): `docker-compose.core.yml:53`, `trino/catalog/results.properties:1` |
| 170 | Infrastructure | Not added | No `--reset-volumes` flag in any launcher; `down -v` only under `--demo`: `start-ui.sh:57` |
| 171 | Infrastructure | To spec | `minio-init` creates `provisa-results` bucket at startup: `docker-compose.observability.yml:23` |
| 223 | Installer | To spec | DMG/AppImage bundle full stack; tech hidden behind `provisa` CLI; datasets not bundled: `packaging/macos/build-dmg.sh:1` |
| 224 | Installer | To spec | State in `~/.provisa/`; `start/stop/status/open` dispatch: `scripts/provisa:10`, `scripts/provisa:512` |
| 225 | Installer | Not to spec | Embedded admin DB + bundled Trino present, but uses `postgres:16`, not `pgserver`: `docker-compose.core.yml:5` |
| 226 | Installer | To spec | External admin PG via `CONFIG_DB_HOST`; external Trino/auth pluggable: `docker-compose.app.yml:12` |
| 227 | Installer | Incomplete | AF2a DMG + AF2b AppImage complete; AF2c Windows uses VirtualBox, not bundled native runtime: `packaging/windows/first-launch.ps1:77` |
| 228 | Installer | To spec | AF1 shell installer + AF2 airgap (Lima/containerd, .tar images): `install.sh:1`, `packaging/macos/build-dmg.sh:1` |
| 254 | Testing | Incomplete | Integration tests target Docker services; no in-test container spin-up/teardown helper found: `tests/e2e/test_health_checks.py:64` |
| 255 | Testing | To spec | Unit error tests mock HTTP/capabilities, no live services: `tests/unit/test_error_handling.py:74` |
| 294 | Installer | Incomplete | `ctr images import` + airgap path exist, but `docker-compose.airgap.yml` uses local tags, not digests: `docker-compose.airgap.yml:12` |
| 302 | OTel | Incomplete | Manual spans on compile/execute/cache/rls/mask/mv; Flight, admin GraphQL, API sources, discovery, adapters uninstrumented: `provisa/api/otel_setup.py:425` |
| 303 | OTel | Incomplete | SDK + auto-instrumentation + most manual spans present; `handle_api_query` span missing: `provisa/api/otel_setup.py:262`, `provisa/api_source/router_integration.py:65` |
| 330 | Observability | To spec | Opt-in observability overlay: OTLP 4317/4318, Prometheus+Tempo, Grafana:3100 pre-provisioned: `docker-compose.observability.yml:1` |

15 To spec, 5 Incomplete, 2 Not added, 1 Not to spec.

## Detail

### Infrastructure (REQ-055, 056, 057, 169, 170, 171)

- **055 (To spec)** — `start-ui.sh:54` runs the core + dev overlays in one command;
  `docker-compose.core.yml:52` defines a Trino coordinator with workers at
  `deploy.replicas: 0` (configurable), catalogs mounted from `trino/catalog/`.
- **056 (To spec)** — `helm/provisa/templates/trino-worker.yaml:94` declares an HPA;
  `helm/provisa/values.yaml:62` defaults workers to min 1 / max 10 / 70% CPU;
  resource groups at `trino/etc/resource-groups.json:1`.
- **057 (To spec)** — Provisa runs as a `Deployment` with no persistent volume;
  `helm/provisa/templates/provisa-deployment.yaml:8`. Trino endpoint is set via
  config, matching "topology is a configuration concern."
- **169 (To spec)** — `docker-compose.core.yml:53` pins `trinodb/trino:480`;
  `trino/catalog/results.properties:1` is an Iceberg catalog with JDBC catalog on
  PostgreSQL and `fs.native-s3.enabled=true`.
- **170 (Not added)** — No `--reset-volumes` flag exists in `start-ui.sh`,
  `scripts/provisa`, or `install.sh`. The only `down -v` runs under `--demo`
  (`start-ui.sh:57`), so there is no dedicated crash-recovery path.
- **171 (To spec)** — `docker-compose.observability.yml:23` `minio-init` runs
  `mc mb --ignore-existing local/provisa-results` at startup.

### Error Handling & Reliability (REQ-064, 065)

- **064 (To spec)** — Policy at `CLAUDE.md:1`; enforced behavior verified by
  `tests/unit/test_error_handling.py:74` (webhook executor raises on 4xx/5xx/timeout).
- **065 (To spec)** — Policy at `CLAUDE.md:2`; no migration framework or `migrations/`
  directory present in the tree.

### Architecture & Design Patterns (REQ-069, 070, 071)

- **069 (To spec)** — `docs/arch/requirements.md:1` and sibling planning docs are
  present and tracked.
- **070 (To spec)** — Brevity rules at `CLAUDE.md:3` with explicit good/bad examples.
- **071 (To spec)** — `.claude/agents/requirements-tracker.md:1` defines the format;
  `CLAUDE.md:15` wires the spawn-on-new-requirement workflow.

### Commercial Positioning (REQ-072, 073, 074)

- **072 (To spec)** — `LICENSE:1` is BSL 1.1 (small-org use grant, converts to
  Apache 2.0); core stack, compiler, and transpiler ship in-repo
  (`docker-compose.core.yml:1`).
- **073 (Not added)** — No hosted control plane or customer-hosted data plane
  separation. A `state.multitenancy` flag exists (`provisa/api/app.py:153`) but does
  not constitute the SaaS topology described.
- **074 (Incomplete)** — `provisa/audit/query_log.py:1` provides query audit logging;
  no SLA monitoring, dedicated-support hooks, or compliance reporting modules found.

### Installer & Packaging (REQ-223–228, 294)

- **223 (To spec)** — `packaging/macos/build-dmg.sh:1` and
  `packaging/linux/build-appimage.sh:1` bundle the full stack into a single artifact;
  source datasets connect over the wire (demo sources are mock APIs in
  `config/provisa-install.yaml`).
- **224 (To spec)** — `scripts/provisa:10` sets `PROVISA_HOME=~/.provisa`;
  `scripts/provisa:512` dispatches `start/stop/status/open`.
- **225 (Not to spec)** — Admin DB is embedded and Trino bundled, but the image is
  `postgres:16` (`docker-compose.core.yml:5`), not the `pgserver` embedded binary the
  requirement names. Vertical-scale default holds (`trino-worker` replicas=0).
- **226 (To spec)** — `docker-compose.app.yml:12` reads `CONFIG_DB_HOST`/`PORT`/`NAME`
  for an external admin PG; auth providers and external Trino are configurable.
- **227 (Incomplete)** — AF2a macOS DMG (signed + notarized,
  `packaging/macos/build-dmg.sh:330`) and AF2b Linux AppImage
  (`packaging/linux/build-appimage.sh:1`) are complete. AF2c Windows
  (`packaging/windows/first-launch.ps1:77`) requires/installs VirtualBox rather than
  bundling a container runtime, so the "no hypervisor prerequisite visible" goal is
  not met.
- **228 (To spec)** — AF1 shell installer (`install.sh:1`, detects
  Docker/OrbStack/Colima) and AF2 airgap bundle (Lima + containerd, images saved as
  `.tar.gz`, `packaging/macos/build-dmg.sh:1`) both exist; AF3 superseded.
- **294 (Incomplete)** — Airgap path bundles images and loads them via
  `ctr images import` (`packaging/macos/first-launch.sh:287`), but
  `docker-compose.airgap.yml:12` references images by local tag
  (`provisa/provisa:local`), not `sha256:` digests as the requirement and its named
  test demand.

### Testing & Quality (REQ-254, 255)

- **254 (Incomplete)** — Integration/e2e tests assume running Docker services
  (`tests/e2e/test_health_checks.py:64`); no in-test container spin-up/teardown
  fixture was found, so the "tests must not assume a pre-existing stack" clause is
  not satisfied.
- **255 (To spec)** — Unit tests mock HTTP and capability checks with no live
  dependency (`tests/unit/test_error_handling.py:74`).

### OpenTelemetry (REQ-302, 303, 330)

- **302 (Incomplete)** — Manual spans exist on `compile_query`
  (`provisa/compiler/sql_gen.py:51`), `execute_trino`, `execute_direct`, cache
  get/set, `rls.inject`, `masking.inject`, and `mv.rewrite`; auto-instrumentation
  covers FastAPI/httpx/asyncpg/redis/grpc (`provisa/api/otel_setup.py:425`). The
  Arrow Flight server (`provisa/api/flight/server.py`), admin GraphQL resolvers, API
  sources, discovery pipeline, and source adapters carry no manual spans.
- **303 (Incomplete)** — SDK, OTLP exporters, and auto-instrumentation are wired
  (`provisa/api/otel_setup.py:262`), endpoint via `OTEL_EXPORTER_OTLP_ENDPOINT`. Of
  the named manual spans, `handle_api_query` is uninstrumented
  (`provisa/api_source/router_integration.py:65`).
- **330 (To spec)** — `docker-compose.observability.yml:1` is an opt-in overlay; OTel
  Collector receives OTLP on 4317/4318
  (`observability/otel-collector-config.yaml:1`), exports to Prometheus and Tempo;
  Grafana on 3100 with datasources and a Provisa dashboard pre-provisioned
  (`observability/grafana/provisioning/`).

## Named tests

- `tests/e2e/test_health_checks.py` — **exists** (REQ-055/056/057/169/170/171/330
  health/liveness/readiness coverage). Does not assert the `--reset-volumes` flag.
- `tests/e2e/test_installer.py` — **exists**. CLI dispatch, config parsing,
  start/stop/status/open, and `TestAirgappedBundle` are present, but three airgap
  tests are currently red against the tree:
  - `test_image_references_use_digests_not_tags` asserts `sha256:` in
    `docker-compose.airgap.yml`, which uses local tags — **fails**.
  - `test_macos_dmg_bundle_exists` / `test_linux_appimage_bundle_exists` glob `dist/`,
    which does not exist locally — **fail** outside the release workflow.
- `tests/unit/test_error_handling.py` — **exists** (REQ-064/065): capability errors,
  webhook fail-fast, scheduled-trigger exception swallowing.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 170 | Not added | S | Add a `--reset-volumes` flag to `start-ui.sh` (and `scripts/provisa`) that runs `down -v` independently of `--demo` |
| 2 | 294 | Incomplete | S | Rewrite `docker-compose.airgap.yml` image refs to `image@sha256:` digests so `test_image_references_use_digests_not_tags` passes |
| 3 | 225 | Not to spec | M | Either swap the embedded admin DB to `pgserver` or amend REQ-225 to record `postgres:16` as the chosen embedded engine |
| 4 | 302 | Incomplete | M | Add manual spans to Arrow Flight server, admin GraphQL resolvers, API sources, discovery pipeline, and source adapters |
| 5 | 303 | Incomplete | S | Add a `handle_api_query` span in `provisa/api_source/router_integration.py` |
| 6 | 227 | Incomplete | L | Bundle a native container runtime for AF2c Windows instead of requiring VirtualBox |
| 7 | 254 | Incomplete | M | Add a Docker spin-up/teardown fixture so integration tests do not assume a running stack |
| 8 | 074 | Incomplete | L | Build SLA monitoring and compliance-reporting features for the enterprise tier |
| 9 | 073 | Not added | L | Design and implement the SaaS hosted-control-plane / customer-data-plane split |
