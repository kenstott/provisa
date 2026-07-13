# Copyright (c) 2026 Kenneth Stott
# Canary: 6e0a3d18-9c47-4b52-a1f8-2d7b5c93e461
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-888: the federation-engine contract is wired into the live query path via
EngineRuntime, bound mandatorily at AppState construction as
``state.federation_engine`` — there is no unbound state and no per-call-site
fallback. Integration tier: constructs a real AppState and drives the real
engine factory (selected by $PROVISA_ENGINE).
"""

from __future__ import annotations

import pytest

from provisa.federation.runtime import (
    EngineCapability,
    EngineRuntime,
    UnsupportedCapabilityError,
)

pytestmark = pytest.mark.integration


def test_appstate_is_born_with_a_bound_engine_runtime():
    from provisa.api.app import AppState

    state = AppState()
    assert state.federation_engine is not None
    assert isinstance(state.federation_engine, EngineRuntime)


def test_bound_runtime_back_references_its_owning_state():
    from provisa.api.app import AppState

    state = AppState()
    # The runtime reads self._state.engine_conn lazily at execute time; the binding
    # must point back at the AppState that owns it.
    assert state.federation_engine._state is state


def test_engine_selected_by_env(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE", "duckdb")
    from provisa.api.app import AppState

    state = AppState()
    assert state.federation_engine.name == "duckdb"


def test_default_engine_is_duckdb(monkeypatch):
    # REQ-989: the zero-config default is the fully-embedded in-process DuckDB engine (not trino).
    monkeypatch.delenv("PROVISA_ENGINE", raising=False)
    from provisa.api.app import AppState

    state = AppState()
    assert state.federation_engine.name == "duckdb"


def test_bound_runtime_gates_capabilities_fail_closed(monkeypatch):
    # The pg engine advertises ROWS only (no Arrow transport), so requiring ARROW_STREAM must
    # fail closed. (DuckDB/Trino now advertise all three transports per REQ-986.)
    monkeypatch.setenv("PROVISA_ENGINE", "pg")
    from provisa.api.app import AppState

    rt = AppState().federation_engine
    assert rt.supports(EngineCapability.ROWS) is True
    assert rt.supports(EngineCapability.ARROW_STREAM) is False
    with pytest.raises(UnsupportedCapabilityError):
        rt.require(EngineCapability.ARROW_STREAM)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
