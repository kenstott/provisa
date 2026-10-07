# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a6d15-9c42-4e7b-b0d3-6a1e5c9f2b74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The chart's Trino coordinator keeps a Salesforce catalog's describe results (REQ-1946).

The Salesforce catalog names a describe cache directory under /data/trino/cache
(``trino_connectors.SALESFORCE_DESCRIBE_CACHE_ROOT``). The compose stack mounts a volume there;
the chart mounted nothing, so a restarted coordinator described the org again, out of its daily
API allowance. Only the coordinator enumerates the org's sObjects, so only it has the volume.
"""

# Requirements: REQ-1946

from __future__ import annotations

from provisa.federation.trino_connectors import SALESFORCE_DESCRIBE_CACHE_ROOT
from tests.unit.test_helm_auth import _LOCAL, _documents, _render

_MOUNT = "/data/trino/cache"


def _coordinator(*sets: str) -> dict:
    rendered = _render(*_LOCAL, *sets)
    assert rendered.returncode == 0, rendered.stderr
    return next(
        d
        for d in _documents(rendered.stdout)
        if d.get("kind") == "StatefulSet" and d["metadata"]["name"] == "rel-trino-coordinator"
    )


def _cache_mounts(coordinator: dict) -> list[dict]:
    (trino,) = [
        c for c in coordinator["spec"]["template"]["spec"]["containers"] if c["name"] == "trino"
    ]
    return [m for m in trino["volumeMounts"] if m["mountPath"] == _MOUNT]


def test_the_describe_cache_root_is_on_the_mounted_volume():
    assert SALESFORCE_DESCRIBE_CACHE_ROOT.startswith(_MOUNT + "/")


def test_the_coordinator_keeps_its_cache_on_a_claimed_volume_its_user_can_write():
    coordinator = _coordinator()
    (mount,) = _cache_mounts(coordinator)
    (claim,) = coordinator["spec"]["volumeClaimTemplates"]
    assert claim["metadata"]["name"] == mount["name"]
    assert claim["spec"]["accessModes"] == ["ReadWriteOnce"]
    assert claim["spec"]["resources"]["requests"]["storage"] == "1Gi"
    assert "storageClassName" not in claim["spec"]  # the cluster default
    pod = coordinator["spec"]["template"]["spec"]["securityContext"]
    assert pod["fsGroup"] == pod["runAsUser"]  # what makes the volume's root writable by Trino


def test_the_volume_takes_its_size_and_storage_class_from_values():
    coordinator = _coordinator(
        "trino.coordinator.cache.persistence.size=5Gi",
        "trino.coordinator.cache.persistence.storageClass=fast-ssd",
    )
    (claim,) = coordinator["spec"]["volumeClaimTemplates"]
    assert claim["spec"]["resources"]["requests"]["storage"] == "5Gi"
    assert claim["spec"]["storageClassName"] == "fast-ssd"


def test_the_volume_falls_back_to_the_global_storage_class():
    # REQ-1946: the chart's own rule for every claim -- global.storageClass when the claim names none.
    (claim,) = _coordinator("global.storageClass=standard")["spec"]["volumeClaimTemplates"]
    assert claim["spec"]["storageClassName"] == "standard"


def test_turned_off_the_coordinator_claims_and_mounts_nothing():
    coordinator = _coordinator("trino.coordinator.cache.persistence.enabled=false")
    assert _cache_mounts(coordinator) == []
    assert "volumeClaimTemplates" not in coordinator["spec"]
