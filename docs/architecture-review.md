# Architecture Review

Provisa runs entirely on your infrastructure. It makes no outbound calls to the vendor at runtime; the product has no telemetry endpoint, no update-check call, and no cloud control plane. An air-gapped deployment works from first startup.

## Installation

The embedded profile ships the entire runnable system, precompiled UI included, as a single Python wheel. Regulated and air-gapped organisations already trust Artifactory-as-PyPI, so there is no Docker registry to mirror, no JVM, and no root required.

```bash
pip install "provisa[embedded]"
provisa run
```

Python 3.12. The embedded profile brings its own PostgreSQL model store (pgserver) and DuckDB query engine. No Docker, no daemon. The wheel is published to PyPI with Sigstore attestation; the dependency set is pinned with hashes in `uv.lock`.

To upgrade, install the newer version from PyPI.

## What data leaves the network

Nothing reaches the vendor. OpenTelemetry data goes wherever you configure the collector to send it. Provisa makes no calls to provisa.dev, to any analytics service, or to any update endpoint.

The only outbound network calls Provisa makes are the ones you configure. Sources are queried at the addresses you register. Authentication requests go to the provider you name. OpenTelemetry data goes to the endpoint you set.

## Deployment options

- **Single wheel** — `pip install "provisa[embedded]"`, runs on a VM or a laptop, no container runtime required.
- **Docker Compose** — a compose stack for a multi-service deployment with an external query engine.
- **Kubernetes** — a Helm chart deploys the control plane, query workers and engine; the query tier scales horizontally.
- **Terraform** — modules for AWS, Azure and GCP stand up a VM or cluster deployment end to end.

See [Deployment](deployment.md) for configuration details.

## Licence

Licensed per production core. The price does not change with connection count, query volume or data volume.

Provisa ships under Business Source License 1.1. The key terms:

- **Evaluation is free forever**, for any size organisation. Non-production use — development, staging, demos, internal tooling — requires no licence.
- **Small-organisation production grant** — production use is free for organisations with fewer than 100 employees and contractors and under $1,000,000 in annual revenue, budget, or expenditures, counting all affiliates together.
- **Commercial licence** — organisations above those thresholds need a commercial licence for production use.
- **Converts to GPL** — each released version converts to GPL v2-or-later on the fourth anniversary of its public release.

The licence text is in [LICENSE](https://github.com/kenstott/provisa/blob/main/LICENSE).
