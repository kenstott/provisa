# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a91c2-77bd-4e58-9a10-c4d2e8b31f07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1518: the Arrow Flight transport is the ACTIVE org's shard's, not the boot shard's.

The Zaychik proxy is a sidecar in the shard's pod. A single connection built at boot against
``boot_shard()`` sent an isolated org's Arrow/stream query through the SHARED shard's proxy, and
held a released pod address after any shard restart.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from provisa.federation.backend import TrinoBackend


def _backend() -> TrinoBackend:
    import types
    from typing import cast

    from provisa.federation.engine import FederationEngine

    return TrinoBackend(cast(FederationEngine, types.SimpleNamespace(name="trino")))


def _state(registry: dict) -> SimpleNamespace:
    return SimpleNamespace(
        flight_client=None,
        flight_clients={},
        org_registry=SimpleNamespace(get=registry.get),
    )


def _runtime(*, isolated: bool, shard: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        engine_endpoint=None,
        engine_url=None,
        isolated_engine=isolated,
        shard=shard,
    )


@pytest.fixture
def cluster(monkeypatch):
    """A provisioning deployment whose shards answer at distinct pod addresses."""
    monkeypatch.setattr("provisa.federation.k8s_provisioner.provisioning_available", lambda: True)
    monkeypatch.setattr(
        "provisa.federation.k8s_provisioner.isolated_shard", lambda org: f"trino-org-{org}"
    )
    monkeypatch.setattr(
        "provisa.federation.k8s_provisioner.shard_flight_endpoint",
        lambda shard: (f"10.0.0.{len(shard)}", 8480),
    )
    monkeypatch.setattr("provisa.federation.engine_wake.boot_shard", lambda: "trino-shared-1")
    epochs = {"trino-shared-1": 1, "trino-org-ks": 1}
    monkeypatch.setattr(
        "provisa.federation.k8s_provisioner.coordinator_epoch", lambda shard: epochs[shard]
    )
    return epochs


@pytest.fixture
def connections():
    """Records every (host, port) a Flight connection was opened against."""
    opened: list[tuple[str, int]] = []

    def _connect(host: str, port: int, **_kw):
        opened.append((host, port))
        return MagicMock(name=f"flight:{host}:{port}")

    with patch("provisa.executor.trino_flight.create_flight_connection", new=_connect):
        yield opened


@pytest.fixture
def set_org(request):
    """Bind the org the query is running under, and unbind it when the test ends."""
    from provisa.api.org_runtime import current_org

    def _set(org_id: str | None) -> None:
        token = current_org.set(org_id)
        request.addfinalizer(lambda: current_org.reset(token))

    return _set


def test_isolated_org_dials_its_own_shard_not_the_boot_shard(cluster, connections, set_org):
    """The defect: an isolated org's Arrow query drained the shared shard's proxy."""
    backend = _backend()
    state = _state({"ks": _runtime(isolated=True, shard="trino-org-ks")})
    set_org("ks")

    backend._flight_transport(state)

    boot_host, _ = ("10.0.0.14", 8480)  # len("trino-shared-1")
    assert connections == [("10.0.0.12", 8480)]  # len("trino-org-ks")
    assert boot_host not in [h for h, _ in connections]
    assert "trino-org-ks" in state.flight_clients


def test_pooled_org_dials_the_shard_its_runtime_records(cluster, connections, set_org):
    backend = _backend()
    state = _state({"acme": _runtime(isolated=False, shard="trino-shared-1")})
    set_org("acme")

    backend._flight_transport(state)

    assert connections == [("10.0.0.14", 8480)]


def test_unselected_org_dials_the_boot_shard(cluster, connections, set_org):
    backend = _backend()
    state = _state({})
    set_org(None)

    backend._flight_transport(state)

    assert connections == [("10.0.0.14", 8480)]


def test_warm_shard_reuses_the_connection(cluster, connections, set_org):
    backend = _backend()
    state = _state({"ks": _runtime(isolated=True, shard="trino-org-ks")})
    set_org("ks")

    first = backend._flight_transport(state)
    second = backend._flight_transport(state)

    assert first is second
    assert len(connections) == 1


def test_restarted_shard_closes_the_stale_connection_and_redials(cluster, connections, set_org):
    """A new coordinator generation means the old pod — and its sidecar — is gone."""
    backend = _backend()
    state = _state({"ks": _runtime(isolated=True, shard="trino-org-ks")})
    set_org("ks")

    stale = backend._flight_transport(state)
    cluster["trino-org-ks"] = 2
    fresh = backend._flight_transport(state)

    assert stale.close.call_count == 1
    assert fresh is not stale
    assert len(connections) == 2
    assert state.flight_clients["trino-org-ks"][0] == 2


def test_external_engine_org_has_no_sidecar_to_dial(cluster, connections, set_org):
    """An org running its own coordinator is not on a shard this control plane provisions."""
    backend = _backend()
    rt = _runtime(isolated=False, shard=None)
    rt.engine_url = "https://trino.acme.example:443"
    state = _state({"acme": rt})
    set_org("acme")

    with pytest.raises(RuntimeError, match="REQ-1518"):
        backend._flight_transport(state)
    assert connections == []


def test_non_provisioning_deployment_uses_the_single_boot_client(monkeypatch):
    """Desktop/self-hosted/tests run one proxy beside the control plane at a stable name."""
    monkeypatch.setattr("provisa.federation.k8s_provisioner.provisioning_available", lambda: False)
    backend = _backend()
    state = _state({})
    state.flight_client = MagicMock(name="compose-zaychik")

    assert backend._flight_transport(state) is state.flight_client
