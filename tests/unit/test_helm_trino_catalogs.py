# Copyright (c) 2026 Kenneth Stott
# Canary: 61376143-8cc8-4980-be5f-75794c4142ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The chart's Trino registers catalogs at runtime, as the API's boot needs (REQ-1332).

The API's boot creates its own catalogs (provisa_admin, otel, results) with CREATE CATALOG, and
refreshes one by dropping it first. The chart's Trino used the static catalog store, which refuses
both ("DROP CATALOG is not supported by the static catalog store"), so in the cluster lane boot
stopped at 'register system catalogs'. The compose stack (trino/etc/config.properties) already ran
the dynamic store; the chart now does too, with a catalog directory Trino can write.
"""

# Requirements: REQ-1332

from __future__ import annotations

from tests.unit.test_helm_auth import _LOCAL, _documents, _render


def test_trino_runs_the_dynamic_catalog_store_with_a_writable_directory():
    rendered = _render(*_LOCAL)
    assert rendered.returncode == 0, rendered.stderr
    docs = list(_documents(rendered.stdout))
    config = next(
        d
        for d in docs
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"].endswith("-trino-config")
    )
    for role in ("coordinator-config.properties", "worker-config.properties"):
        assert "catalog.management=dynamic" in config["data"][role], role
    for d in docs:
        if (
            d.get("kind") not in ("Deployment", "StatefulSet")
            or "trino" not in d["metadata"]["name"]
        ):
            continue
        spec = d["spec"]["template"]["spec"]
        (trino,) = [c for c in spec["containers"] if c["name"] == "trino"]
        store = [m for m in trino["volumeMounts"] if m["mountPath"] == "/etc/trino/catalog"]
        assert store and "subPath" not in store[0], d["metadata"]["name"]
        volume = next(v for v in spec["volumes"] if v["name"] == store[0]["name"])
        assert "emptyDir" in volume, d["metadata"]["name"]
