# Copyright (c) 2026 Kenneth Stott
# Canary: 43085cc5-c385-40cf-b8ea-1fab183a4086
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Helm chart points the OTel store at its own MinIO (REQ-1332, REQ-1913).

Boot provisions Trino's `otel` Iceberg catalog on the store named by the setting otel.s3_endpoint.
The chart set nothing, so the endpoint was the setting's default, http://minio:9000 (the compose
service name); in the cluster that name does not resolve, and the cluster lane's API pod stopped
logging after its seed phase until the liveness probe killed it.
"""

# Requirements: REQ-1332, REQ-1913

from __future__ import annotations

from tests.unit.test_helm_auth import _LOCAL, _documents, _render


def test_the_api_reads_the_otel_store_at_this_releases_minio():
    rendered = _render(*_LOCAL)
    assert rendered.returncode == 0, rendered.stderr
    docs = list(_documents(rendered.stdout))
    deployment = next(
        d
        for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-provisa")
    )
    minio = next(
        d for d in docs if d.get("kind") == "Service" and d["metadata"]["name"].endswith("-minio")
    )
    (api,) = [
        c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "provisa"
    ]
    env = {e["name"]: e for e in api.get("env", [])}
    assert env["PROVISA_OTEL_S3_ENDPOINT"]["value"] == f"http://{minio['metadata']['name']}:9000"
    for name, key in (
        ("PROVISA_OTEL_S3_ACCESS_KEY", "minio-access-key"),
        ("PROVISA_OTEL_S3_SECRET_KEY", "minio-secret-key"),
    ):
        assert env[name]["valueFrom"]["secretKeyRef"]["key"] == key


def test_trino_may_reach_this_releases_minio():
    """Trino reads the otel catalog's store (and spools its exchange) on MinIO; the chart's MinIO
    network policy admitted only the API, so Trino's connect timed out and boot stalled."""
    rendered = _render(*_LOCAL)
    assert rendered.returncode == 0, rendered.stderr
    policy = next(
        d
        for d in _documents(rendered.stdout)
        if d.get("kind") == "NetworkPolicy" and d["metadata"]["name"].endswith("-minio")
    )
    admitted = [
        peer["podSelector"]["matchLabels"].get("app")
        for rule in policy["spec"]["ingress"]
        for peer in rule["from"]
    ]
    assert "trino" in admitted
