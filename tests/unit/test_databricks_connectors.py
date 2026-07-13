# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-987: Databricks object/lake ATTACH connectors (zero-copy external links) + the derived
warehouse connector set. Driver-free — the live UC provisioning + external-table round-trip over R2
is exercised in tests/integration/test_databricks_external_link_e2e.py."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.connector_base import Mechanism
from provisa.federation.databricks_connectors import databricks_object_link_connectors
from provisa.federation.databricks_uc import ExternalLinkError, parse_location
from provisa.federation.engine import build_databricks_engine, build_snowflake_engine
from provisa.federation.strategy import Strategy, federate

_R2 = "r2://pubs@acct123.r2.cloudflarestorage.com/dir/orders.parquet"


def _src(stype, path=_R2, hints=None):
    return SimpleNamespace(
        id="x", type=SimpleNamespace(value=stype), path=path, federation_hints=hints or {}
    )


def test_object_link_connectors_are_attach_r_with_formats():
    conns = {c.source_type: c for c in databricks_object_link_connectors()}
    assert set(conns) == {"parquet", "csv", "iceberg", "delta_lake"}
    assert all(
        c.mechanism is Mechanism.SCAN for c in conns.values()
    )  # object-link = SCAN (REQ-951)
    assert conns["parquet"]._format == "PARQUET"  # noqa: SLF001
    assert conns["iceberg"]._format == "ICEBERG"  # noqa: SLF001
    assert conns["delta_lake"]._format == "DELTA"  # noqa: SLF001
    # read-only external link — never writes upstream
    assert conns["parquet"].capability().write is False


def test_connector_details_extract_location_and_credential():
    conns = {c.source_type: c for c in databricks_object_link_connectors()}
    d = conns["parquet"].details(
        _src(
            "parquet",
            hints={"access_key_id": "AK", "secret_access_key": "SK", "account_id": "acct123"},
        )
    )
    assert d["format"] == "PARQUET"
    assert d["location"] == _R2
    assert d["credential"] == {
        "access_key_id": "AK",
        "secret_access_key": "SK",
        "account_id": "acct123",
    }


def test_parse_location_splits_scheme_bucket_root():
    scheme, bucket, root = parse_location(_R2)
    assert scheme == "r2"
    assert bucket == "pubs"
    assert root == "r2://pubs@acct123.r2.cloudflarestorage.com/"


def test_parse_location_rejects_non_url():
    with pytest.raises(ExternalLinkError):
        parse_location("/local/file.parquet")


def test_databricks_engine_attaches_lake_scans_and_lands_the_rest():
    e = build_databricks_engine()
    assert e.driver_class().value == "partial"  # attaches cloud lake sources + lands the rest
    # object/lake → zero-copy SCAN (external link)
    for t in ("parquet", "iceberg", "delta_lake", "csv"):
        assert federate(_src(t), e) is Strategy.SCAN, t
    # everything else readable → MATERIALIZED (land), not the demo 6-tuple
    for t in ("postgresql", "mongodb", "kafka", "oracle", "mysql"):
        assert federate(_src(t), e) is Strategy.MATERIALIZED, t


def test_warehouse_land_set_is_derived_not_a_demo_tuple():
    # The Databricks land set covers many readable types (>> the old 6), each a DIRECT/FETCH connector.
    e = build_databricks_engine()
    land = {
        t for t, c in e.connectors.items() if c.mechanism in (Mechanism.DIRECT, Mechanism.FETCH)
    }
    assert {"postgresql", "mysql", "mongodb", "kafka", "oracle", "redis"} <= land
    assert len(land) > 20  # derived readable universe, not a curated subset
    assert "databricks" not in land  # the engine's own native store is excluded


def test_snowflake_lands_readable_universe():
    e = build_snowflake_engine()
    land = {
        t for t, c in e.connectors.items() if c.mechanism in (Mechanism.DIRECT, Mechanism.FETCH)
    }
    assert {"postgresql", "mongodb", "kafka"} <= land  # parquet/iceberg now ATTACH (external link)
    assert "snowflake" not in land


def test_iceberg_unreachable_without_attach_connector_elsewhere():
    # iceberg has no Provisa reader (no direct driver), so on an engine that cannot ATTACH it, it is
    # unreachable — it can only surface via a native external link (which the warehouse engines now
    # provide). The generic self-only SQLAlchemy engine has no object/lake attach connector, so it
    # cannot reach iceberg at all.
    from provisa.federation.engine import UnreachableSource, build_sqlalchemy_engine

    e = build_sqlalchemy_engine("postgresql://h/db")
    with pytest.raises(UnreachableSource):
        federate(_src("iceberg"), e)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
