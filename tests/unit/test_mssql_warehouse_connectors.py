# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Microsoft Fabric / Azure Synapse federation engines — object/lake ATTACH connectors (zero-copy
external links via OPENROWSET) + the derived warehouse set + the T-SQL type map. Driver-free; the live
governed round-trip + R2-via-OneLake-shortcut external link is in the integration e2e."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import build_fabric_engine, build_synapse_engine
from provisa.federation.mssql_warehouse_connectors import openrowset_link_connectors
from provisa.federation.strategy import Strategy, federate


def _src(stype, path="s3://b/dir/orders.parquet", hints=None):
    return SimpleNamespace(
        id="x", type=SimpleNamespace(value=stype), path=path, federation_hints=hints or {}
    )


@pytest.mark.parametrize(
    "engine,expected",
    [
        # Fabric additionally attaches Iceberg via OneLake Delta virtualization (FORMAT='DELTA');
        # Synapse serverless has no OneLake virtualization, so Iceberg lands there.
        ("fabric", {"parquet", "csv", "delta_lake", "iceberg"}),
        ("synapse", {"parquet", "csv", "delta_lake"}),
    ],
)
def test_openrowset_link_connectors_are_attach_r(engine, expected):
    conns = {c.source_type: c for c in openrowset_link_connectors(engine)}
    assert set(conns) == expected
    assert all(c.mechanism is Mechanism.ATTACH_R for c in conns.values())
    assert all(c.engine == engine for c in conns.values())
    d = conns["parquet"].details(_src("parquet"))
    assert d == {"format": "PARQUET", "location": "s3://b/dir/orders.parquet"}


def test_fabric_iceberg_reads_as_delta_synapse_lands():
    fabric = {c.source_type: c for c in openrowset_link_connectors("fabric")}
    assert fabric["iceberg"].details(_src("iceberg"))["format"] == "DELTA"
    assert "iceberg" not in {c.source_type for c in openrowset_link_connectors("synapse")}


@pytest.mark.parametrize("build", [build_fabric_engine, build_synapse_engine])
def test_engine_is_partial_attaches_lake_lands_rest(build):
    e = build()
    assert e.driver_class().value == "partial"
    assert e.dialect == "tsql"
    for t in ("parquet", "csv", "delta_lake"):
        assert federate(_src(t), e) is Strategy.SCAN, t
    for t in ("postgresql", "mongodb", "kafka"):
        assert federate(_src(t), e) is Strategy.MATERIALIZED, t


def test_tsql_type_mapping():
    from provisa.federation.mssql_warehouse_runtime import _tsql_type

    assert _tsql_type("bigint") == "BIGINT"
    assert _tsql_type("text") == "VARCHAR(8000)"
    assert _tsql_type("double") == "FLOAT"
    assert _tsql_type("boolean") == "BIT"
    assert _tsql_type("timestamptz") == "DATETIME2"  # native spelling normalizes via to_ir
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        _tsql_type("geography")


def test_fabric_and_synapse_in_engine_registry():
    from provisa.federation.engine import engine_registry

    keys = {r["key"] for r in engine_registry()}
    assert {"fabric", "synapse"} <= keys


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
