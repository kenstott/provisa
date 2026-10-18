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
the dynamic store; the chart now does too.

The catalogs carry connection passwords, so Trino keeps them in memory (catalog.store=memory) and
nothing mounts a catalog directory: no catalog is written to the node's disk. The API reissues them
when a coordinator restarts (provisa/federation/engine_wake.py).
"""

# Requirements: REQ-1332, REQ-2073

from __future__ import annotations

from tests.unit.test_helm_auth import _LOCAL, _documents, _render


def test_trino_runs_the_dynamic_catalog_store_in_memory_with_no_catalog_directory():
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
    assert "catalog.store=memory" in config["data"]["coordinator-config.properties"]
    # The coordinator alone stores catalogs: a worker given catalog.store refuses to start ("was not
    # used"), and with no worker the coordinator runs no query.
    assert "catalog.store" not in config["data"]["worker-config.properties"]
    trino_pods = 0
    for d in docs:
        if (
            d.get("kind") not in ("Deployment", "StatefulSet")
            or "trino" not in d["metadata"]["name"]
        ):
            continue
        trino_pods += 1
        spec = d["spec"]["template"]["spec"]
        (trino,) = [c for c in spec["containers"] if c["name"] == "trino"]
        catalog_mounts = [
            m for m in trino["volumeMounts"] if m["mountPath"].startswith("/etc/trino/catalog")
        ]
        assert catalog_mounts == [], (d["metadata"]["name"], catalog_mounts)
    assert trino_pods == 2
