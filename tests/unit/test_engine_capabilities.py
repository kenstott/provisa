# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9b4e60-7d31-4a89-8f14-6b0c2e9a3d57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-888: EngineCapability gating — engine-specific consumer transports
(ROWS/ARROW/ARROW_STREAM) are advertised per engine and missing capabilities
fail closed via UnsupportedCapabilityError, with no silent fallback.

Pure logic — no live connection. Complements test_engine_runtime.py (terminal
dispatch) by pinning the capability matrix and the fail-closed contract.
"""

from __future__ import annotations

import types

import pytest

from provisa.federation.engine import (
    build_clickhouse_engine,
    build_databricks_engine,
    build_duckdb_engine,
    build_snowflake_engine,
    build_sqlalchemy_engine,
    build_trino_engine,
)
from provisa.federation.runtime import (
    EngineCapability,
    EngineRuntime,
    UnsupportedCapabilityError,
)


def _build_sqlalchemy():
    return build_sqlalchemy_engine("postgresql://h/db")


def _state():
    return types.SimpleNamespace(trino_conn=object(), flight_client=None, source_pools=None)


def _rt(build):
    return EngineRuntime(build(), _state())


# ---- capability matrix (REQ-888) --------------------------------------------


def test_every_engine_advertises_rows():
    for build in (build_trino_engine, build_duckdb_engine, _build_sqlalchemy):
        assert _rt(build).supports(EngineCapability.ROWS) is True


def test_trino_advertises_all_three_transports():
    caps = _rt(build_trino_engine).capabilities
    assert caps == frozenset(
        {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
    )


def test_duckdb_supports_arrow_and_stream():
    # REQ-986: DuckDB (the zero-config default engine) surfaces Arrow AND lazy record-batch streaming
    # through the Flight server (fetch_arrow_table + to_batches).
    rt = _rt(build_duckdb_engine)
    assert rt.supports(EngineCapability.ARROW) is True
    assert rt.supports(EngineCapability.ARROW_STREAM) is True


def test_clickhouse_advertises_all_three_transports():
    # REQ-986: ClickHouse honors ARROW and ARROW_STREAM via the Provisa Arrow Flight server.
    caps = _rt(build_clickhouse_engine).capabilities
    assert caps == frozenset(
        {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
    )


def test_snowflake_and_databricks_advertise_arrow_transports():
    # REQ-987/988: first-class warehouse engines with Arrow-native read transport.
    for build in (build_snowflake_engine, build_databricks_engine):
        caps = _rt(build).capabilities
        assert caps == frozenset(
            {EngineCapability.ROWS, EngineCapability.ARROW, EngineCapability.ARROW_STREAM}
        )


def test_sqlalchemy_supports_rows_only():
    # A generic SQLAlchemy-backed engine is row-oriented — no Arrow transport.
    rt = _rt(_build_sqlalchemy)
    assert rt.supports(EngineCapability.ROWS) is True
    assert rt.supports(EngineCapability.ARROW) is False
    assert rt.supports(EngineCapability.ARROW_STREAM) is False


# ---- fail-closed contract (REQ-888) -----------------------------------------


def test_require_supported_capability_does_not_raise():
    _rt(build_trino_engine).require(EngineCapability.ARROW_STREAM)


def test_require_unsupported_capability_fails_closed():
    # A generic SQLAlchemy engine advertises ROWS only, so ARROW_STREAM fails closed.
    rt = _rt(_build_sqlalchemy)
    with pytest.raises(UnsupportedCapabilityError):
        rt.require(EngineCapability.ARROW_STREAM)


def test_unsupported_capability_error_names_engine_and_capability():
    rt = _rt(_build_sqlalchemy)
    with pytest.raises(UnsupportedCapabilityError) as exc:
        rt.require(EngineCapability.ARROW_STREAM)
    msg = str(exc.value)
    assert "sqlalchemy" in msg
    assert "arrow_stream" in msg


def test_capability_enum_values_are_stable_transport_names():
    assert EngineCapability.ROWS.value == "rows"
    assert EngineCapability.ARROW.value == "arrow"
    assert EngineCapability.ARROW_STREAM.value == "arrow_stream"


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
