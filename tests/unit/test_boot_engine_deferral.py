# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1619: the control plane starts whether or not a shard can be allocated.

The shared engine scales to zero, so every cold start asks the cluster for a brand-new node. When it
cannot supply one — quota, capacity — the wake raises, and boot used to carry that failure all the
way out of ``lifespan``: ``Application startup failed. Exiting.`` and a 503 on every page, including
sign-in, org administration and the metadata surfaces, none of which touch the engine.

The recovery already exists on the query path (REQ-1448): ``ensure_engine_awake`` wakes the shard and
``restore_shared_terminal`` rebuilds the terminal on whatever coordinator it lands on. So boot skips
its engine phase and leaves the default runtime's generation unstamped, which that comparison reads
as "restarted" — the first query does the wake and the restore.
"""

import inspect

from provisa.api import app as app_module
from provisa.api import app_loaders, startup_seed


def test_the_wake_failure_does_not_leave_the_lifespan():
    """The one failure that is caught, and only where engines are provisioned."""
    src = inspect.getsource(app_module._load_and_build)
    guard = src.index("converge_boot_shard()")
    assert "except K8sProvisioningError:" in src[guard : guard + 400]
    assert src.index("if provisioning_available():") < guard


def test_every_engine_step_of_the_boot_is_behind_the_flag():
    """A guard on the wake alone moves the crash rather than removing it: the seed reads the
    coordinator's address, the terminal dials it, provision_infra sets up over it, and load_config
    issues catalogs on it. Each is skipped by the same flag the wake failure sets."""
    src = inspect.getsource(app_module._load_and_build)
    assert "_seed_built_in_sources(\n        pg_host" in src
    assert "engine_addressable=not engine_deferred" in src
    assert "_apply_server_and_engine_config(raw_config, connect_engine=not engine_deferred)" in src
    assert "if not engine_deferred:\n        await state.federation_engine.provision_infra()" in src
    assert "None if engine_deferred else state.federation_engine" in src


def test_the_generation_is_left_unstamped_so_the_first_query_restores():
    """The stamp is in the else branch. Stamping it after a failed wake would tell the query path
    the terminal is current on a coordinator that was never connected, and the restore would never
    run."""
    src = inspect.getsource(app_module._load_and_build)
    assert src.index("except K8sProvisioningError:") < src.index("default_rt.engine_generation")


def test_the_settings_are_applied_without_connecting_the_terminal(monkeypatch):
    """The server config is not engine state: hostname, limits and security mode are read by every
    non-engine surface, so they are applied on the deferred path too."""
    calls: list[str] = []

    class _Engine:
        def provision(self, _views):
            calls.append("provision")

        def configure_session(self, _cfg):
            calls.append("configure_session")

    monkeypatch.setattr(app_module.state, "federation_engine", _Engine())
    monkeypatch.delenv("PROVISA_HOSTNAME", raising=False)

    app_loaders._apply_server_and_engine_config(
        {"server": {"hostname": "example.test", "limits": {"default_row_limit": 7}}},
        connect_engine=False,
    )

    assert calls == []
    assert app_module.state.hostname == "example.test"
    assert app_module.state.server_limits["default_row_limit"] == 7


def test_the_seed_asks_for_an_address_only_when_there_is_one():
    """``configured_engine_endpoint`` RAISES on a shard that has not been woken — a guessed address
    is an engine nothing can reach (REQ-1448) — so the deferred boot must not call it at all."""
    src = inspect.getsource(startup_seed._seed_built_in_sources)
    assert "configured_engine_endpoint() if engine_addressable else None" in src
    # The otel row's host/port are the only thing that reads it, and a column the boot cannot
    # supply is not a column it may name in the update list.
    assert "_otel_update = [c for c in _DERIVED_FROM_DEPLOYMENT if c in _otel_row]" in src
