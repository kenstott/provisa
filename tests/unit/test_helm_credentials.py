# Copyright (c) 2026 Kenneth Stott
# Canary: d42d2bf4-0121-4b3a-aa9d-2c33a608aec5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The chart sets no store password of its own: PostgreSQL, Redis, MinIO and Grafana read theirs
from a Secret the operator creates (credentials.existingSecret), and TLS is on by default."""

from __future__ import annotations

import shutil
import subprocess

from tests.unit.test_helm_auth import _KEY, _LOCAL, CHART, _documents, _render

_SECRET = "provisa-credentials"


def _refs(docs: list[dict]) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            ref = node.get("secretKeyRef")
            if isinstance(ref, dict):
                found.add((ref["name"], ref["key"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(docs)
    return found


def test_without_a_credentials_secret_the_chart_does_not_render():
    helm = shutil.which("helm")
    assert helm is not None, "the helm CLI is required to verify the chart"
    rendered = subprocess.run(
        [
            helm,
            "template",
            "rel",
            str(CHART),
            "--set",
            _KEY,
            *(a for s in _LOCAL for a in ("--set", s)),
        ],
        capture_output=True,
        text=True,
    )
    assert rendered.returncode != 0
    assert "credentials.existingSecret" in rendered.stderr


def test_every_store_reads_its_password_from_the_operators_secret():
    rendered = _render(*_LOCAL, "observability.enabled=true")
    assert rendered.returncode == 0, rendered.stderr
    refs = _refs(_documents(rendered.stdout))
    for key in (
        "pg-password",
        "redis-password",
        "minio-access-key",
        "minio-secret-key",
        "grafana-admin-password",
    ):
        assert (_SECRET, key) in refs
    assert not any(name.endswith("-secrets") for name, _ in refs)


def test_no_default_password_is_rendered():
    rendered = _render(*_LOCAL, "observability.enabled=true")
    assert rendered.returncode == 0, rendered.stderr
    assert "minioadmin" not in rendered.stdout
    secrets = [d for d in _documents(rendered.stdout) if d.get("kind") == "Secret"]
    for secret in secrets:
        data = {**(secret.get("stringData") or {}), **(secret.get("data") or {})}
        assert not {"pg-password", "redis-password", "grafana-admin-password"} & set(data)


def test_trinos_exchange_reads_the_in_cluster_minio_credentials_from_its_env():
    docs = _documents(_render(*_LOCAL).stdout)
    exchange = next(
        d
        for d in docs
        if d.get("kind") == "Secret" and d["metadata"]["name"].endswith("-trino-exchange")
    )
    props = exchange["stringData"]["exchange-manager.properties"]
    assert "exchange.s3.aws-access-key=${ENV:PROVISA_EXCHANGE_S3_ACCESS_KEY}" in props
    assert "exchange.s3.aws-secret-key=${ENV:PROVISA_EXCHANGE_S3_SECRET_KEY}" in props
    refs = _refs([d for d in docs if d["metadata"]["name"].startswith("rel-trino-")])
    assert (_SECRET, "minio-access-key") in refs and (_SECRET, "minio-secret-key") in refs


def test_tls_is_on_by_default():
    import pathlib

    import yaml

    values = yaml.safe_load(pathlib.Path(CHART, "values.yaml").read_text())
    assert values["tls"]["enabled"] is True
