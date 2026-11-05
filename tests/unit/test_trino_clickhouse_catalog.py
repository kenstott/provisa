# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c6d52-7a1e-4b8e-9d4a-6c1b2e5f8a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A ClickHouse source gets a Trino catalog, and a JDBC source Provisa cannot build a URL for is
refused by name instead of skipped (J285 P1, P3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from provisa.core.catalog import TrinoCatalogNotCreated, _build_catalog_properties, create_catalog
from provisa.core.models import Source, SourceType


def _clickhouse(port: int = 8123) -> Source:
    return Source(
        id="bench-clickhouse",
        type=SourceType.clickhouse,
        host="clickhouse",
        port=port,
        database="analytics",
        username="default",
        password="pw",
    )


def test_clickhouse_jdbc_url_uses_the_http_port():
    assert _clickhouse().jdbc_url() == "jdbc:clickhouse://clickhouse:8123/analytics"


def test_clickhouse_jdbc_url_defaults_to_8123():
    assert _clickhouse(port=0).jdbc_url() == "jdbc:clickhouse://clickhouse:8123/analytics"


def test_clickhouse_catalog_properties():
    assert _build_catalog_properties(_clickhouse(), "") == {
        "connection-url": "jdbc:clickhouse://clickhouse:8123/analytics",
        "connection-user": "default",
        "connection-password": "pw",
        "clickhouse.map-string-as-varchar": "true",
    }


def test_create_catalog_issues_create_catalog_for_clickhouse():
    conn = MagicMock()
    create_catalog(conn, _clickhouse(), "")
    statements = [c.args[0] for c in conn.cursor.return_value.execute.call_args_list]
    create = [s for s in statements if s.startswith("CREATE CATALOG")]
    assert len(create) == 1
    assert create[0].startswith("CREATE CATALOG bench_clickhouse USING clickhouse WITH (")
    assert "\"clickhouse.map-string-as-varchar\" = 'true'" in create[0]


@pytest.mark.parametrize("stype", ["singlestore", "databricks"])
def test_jdbc_source_without_a_url_is_refused_by_name(stype):
    src = Source(id="no-url", type=SourceType(stype), host="h", port=1, database="d")
    conn = MagicMock()
    with pytest.raises(TrinoCatalogNotCreated) as exc:
        create_catalog(conn, src, "")
    assert "'no-url'" in str(exc.value)
    assert repr(stype) in str(exc.value)
    conn.cursor.return_value.execute.assert_not_called()


def test_type_with_no_trino_connector_is_still_skipped():
    src = Source(id="duck", type=SourceType.duckdb, host="h", port=1, database="d")
    conn = MagicMock()
    create_catalog(conn, src, "")
    conn.cursor.assert_not_called()
