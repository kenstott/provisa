# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BigQuery federation engine — object/lake ATTACH connectors (zero-copy external links) + the derived
warehouse connector set + external-table DDL. Driver-free; the live land/read/RLS + GCS external-link
round-trip is exercised in tests/integration/test_bigquery_federation_engine_e2e.py."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.bigquery_connectors import (
    bigquery_object_link_connectors,
    external_table_ddl,
)
from provisa.federation.connector_base import Mechanism
from provisa.federation.engine import build_bigquery_engine
from provisa.federation.strategy import Strategy, federate


def _src(stype, path="gs://b/dir/orders.parquet", hints=None):
    return SimpleNamespace(
        id="x", type=SimpleNamespace(value=stype), path=path, federation_hints=hints or {}
    )


def test_object_link_connectors_are_attach_r():
    conns = {c.source_type: c for c in bigquery_object_link_connectors()}
    assert set(conns) == {"parquet", "csv", "json", "iceberg", "delta_lake"}
    assert all(
        c.mechanism is Mechanism.SCAN for c in conns.values()
    )  # object-link = SCAN (REQ-951)
    assert conns["parquet"].capability().write is False


def test_connector_details_carry_format_location_connection():
    conns = {c.source_type: c for c in bigquery_object_link_connectors()}
    d = conns["iceberg"].details(_src("iceberg", hints={"connection": "proj.us.conn"}))
    assert d == {
        "format": "ICEBERG",
        "location": "gs://b/dir/orders.parquet",
        "connection": "proj.us.conn",
    }


def test_external_table_ddl_single_file_and_directory_and_connection():
    # single-file URI scans as-is
    ddl = external_table_ddl(
        "p", "ds", "orders", {"format": "PARQUET", "location": "gs://b/dir/f.parquet"}
    )
    assert "CREATE OR REPLACE EXTERNAL TABLE `p`.`ds`.`orders`" in ddl
    assert "format = 'PARQUET'" in ddl and "uris = ['gs://b/dir/f.parquet']" in ddl
    # a directory URI gets a trailing glob
    ddl2 = external_table_ddl("p", "ds", "t", {"format": "CSV", "location": "gs://b/dir"})
    assert "uris = ['gs://b/dir/*']" in ddl2
    # a connection (BigLake / cross-cloud) is referenced
    ddl3 = external_table_ddl(
        "p", "ds", "t", {"format": "ICEBERG", "location": "gs://b/x", "connection": "p.us.c"}
    )
    assert "WITH CONNECTION `p.us.c`" in ddl3


def test_external_table_ddl_requires_location():
    with pytest.raises(ValueError, match="no 'path'"):
        external_table_ddl("p", "ds", "t", {"format": "PARQUET", "location": None})


def test_bigquery_engine_attaches_lake_scans_lands_rest():
    e = build_bigquery_engine()
    assert e.driver_class().value == "partial"
    # object/lake file formats (json included) read in place as a zero-copy SCAN (REQ-951): the
    # connector's declared SCAN reach mode drives the strategy, not a source-type name list.
    for t in ("parquet", "csv", "json", "iceberg", "delta_lake"):
        assert federate(_src(t), e) is Strategy.SCAN, t
    # everything else readable lands, not the demo 6-tuple
    for t in ("postgresql", "mongodb", "kafka", "oracle"):
        assert federate(_src(t), e) is Strategy.MATERIALIZED, t


def test_bigquery_ir_type_mapping():
    from provisa.federation.bigquery_runtime import _bq_type

    assert _bq_type("bigint") == "INT64"
    assert _bq_type("text") == "STRING"
    assert _bq_type("double") == "FLOAT64"
    assert _bq_type("timestamptz") == "TIMESTAMP"  # native spelling normalizes via to_ir
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        _bq_type("geography")


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
