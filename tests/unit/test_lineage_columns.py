# Copyright (c) 2026 Kenneth Stott
# Canary: 6f0a2c4e-8b1d-4739-9e5a-0c2b4d6f8a1c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for column-level lineage resolution (REQ-862).

Pure SQL analysis — no I/O. Resolves per-output-column derivations from a view
SELECT and flattens them into OTel span attributes.
"""

from __future__ import annotations

import pytest
from sqlglot.errors import SqlglotError

from provisa.lineage import (
    ColumnDerivation,
    lineage_span_attributes,
    resolve_column_lineage,
)

_JOIN_SQL = (
    "SELECT o.id AS order_id, c.name AS customer, o.amount * 1.1 AS gross "
    "FROM orders o JOIN customers c ON o.cust_id = c.id"
)


def _by_output(sql: str) -> dict[str, ColumnDerivation]:
    return {d.output: d for d in resolve_column_lineage(sql)}


def test_resolves_one_derivation_per_output_column():
    ds = resolve_column_lineage(_JOIN_SQL)
    assert [d.output for d in ds] == ["order_id", "customer", "gross"]


def test_simple_column_maps_to_source():
    d = _by_output(_JOIN_SQL)["order_id"]
    assert d.sources == ("o.id",)


def test_column_from_second_table():
    d = _by_output(_JOIN_SQL)["customer"]
    assert d.sources == ("c.name",)


def test_transform_expression_captured():
    d = _by_output(_JOIN_SQL)["gross"]
    assert "1.1" in d.transform
    assert d.sources == ("o.amount",)  # transform's leaf source column


def test_multi_source_column():
    ds = _by_output("SELECT a.x + b.y AS total FROM a JOIN b ON a.id = b.id")
    assert ds["total"].sources == ("a.x", "b.y")


def test_star_projection_recorded_with_no_sources():
    ds = resolve_column_lineage("SELECT * FROM t")
    assert len(ds) == 1 and ds[0].output == "*"
    assert ds[0].sources == ()


def test_literal_projection_has_no_sources():
    ds = _by_output("SELECT 1 AS one, name FROM t")
    assert ds["one"].sources == ()


def test_unparseable_sql_raises():
    with pytest.raises(SqlglotError):
        resolve_column_lineage(">>> not sql <<<")


def test_span_attributes_flatten_derivations():
    attrs = lineage_span_attributes(resolve_column_lineage(_JOIN_SQL))
    assert attrs["lineage.columns"] == "order_id,customer,gross"
    assert attrs["lineage.column.order_id.sources"] == "o.id"
    assert "1.1" in attrs["lineage.column.gross.transform"]


def test_span_attributes_all_strings():
    # OTel attribute values must be scalars — verify every value is a str.
    attrs = lineage_span_attributes(resolve_column_lineage(_JOIN_SQL))
    assert all(isinstance(v, str) for v in attrs.values())
