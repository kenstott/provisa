# Provisa Helm chart

## Sizing

The chart ships production defaults in `values.yaml`. It also ships a lean test sizing in
`ci-values.yaml` for the minikube cluster lane (`tests/e2e/test_helm_minikube.py`). Install
without `ci-values.yaml` in production.

### Production (`values.yaml`)

| Component | CPU request | CPU limit | Memory request | Memory limit | Replicas |
|---|---|---|---|---|---|
| API (`provisa.resources`) | 1 | 2 | 1Gi | 4Gi | 2, HPA 2 to 10 at 70% CPU |
| UI (`provisa.ui.resources`) | 100m | 1 | 128Mi | 512Mi | 2 |
| Trino coordinator | 1 | 2 | 2Gi | 4Gi | 1 |
| Trino worker | 1 | 4 | 2Gi | 8Gi | 2, HPA 1 to 10 |
| Zaychik | 100m | 1 | 256Mi | 512Mi | 1 |

The API requests the memory the process holds at rest. Its limit is twice the smallest limit it
boots in, which leaves room for result sets, the response cache and per-request threads. Each
request runs on its own thread and Arrow and DuckDB work runs outside the GIL, so a pod uses more
than one core under load; the 1-core request is what the HPA's 70% target measures against.

Trino query memory (`trino.memory`) is sized to the worker limit. Each node gets 2GB, each query
4GB, and the cluster 8GB in total. PostgreSQL, PgBouncer, Redis, MinIO and MongoDB carry fixed requests and limits in
their templates.

### Test (`ci-values.yaml`)

The cluster lane runs on a minikube with 6 GiB of memory and 4 CPUs, and passes the file on the
install and on every upgrade:

```bash
helm upgrade --install provisa helm/provisa --values helm/provisa/ci-values.yaml ...
```

| Component | CPU request | CPU limit | Memory request | Memory limit | Replicas |
|---|---|---|---|---|---|
| API | 250m | 1 | 512Mi | 2Gi | 1, no HPA |
| UI | 50m | 500m | 128Mi | 256Mi | 1 |
| Trino coordinator | 250m | 2 | 512Mi | 1Gi | 1 |
| Trino worker | 250m | 4 | 512Mi | 1Gi | 1, no HPA |

It also lowers Trino query memory to fit the 1Gi pods, lengthens the Trino probes for slow JVM
startup on minikube, turns MongoDB off, keeps MinIO on as the exchange store, and serves Flight
over ClusterIP.

The API's 2Gi limit is the lean size the lane boots in. `test_no_container_was_oom_killed` fails
on any container killed at its limit. `test_provisa_boot_memory_leaves_headroom` fails when the
API's measured boot peak passes 75% of the API's limit. `tests/unit/test_helm_sizing.py` checks that `values.yaml` renders the production numbers and
that the lane deploys with `ci-values.yaml`.
