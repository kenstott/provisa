# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-3b5d-4f08-9a6e-2d8f1b0c5e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""postgresql_fdw_validator on Trino, ClickHouse and PostgreSQL raises 42601 for an option outside
the catalog's list, as PostgreSQL does, instead of answering t."""

# Requirements: REQ-2236

from __future__ import annotations

import chdb
import pytest
import sqlglot

from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.transpile import transpile

pytestmark = pytest.mark.unit


def _written(options: str, catalog: int, dialect: str) -> str:
    sql = (
        f"SELECT pg_catalog.\"postgresql_fdw_validator\"('{options}'::text[], {catalog}::oid) AS v"
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    return transpile(expand(tree, CatalogFacts("analyst")).sql("postgres"), dialect)


@pytest.mark.parametrize("dialect", ["trino", "clickhouse", "postgres"])
def test_the_form_is_written_for_each_engine_and_raises_42601(dialect):
    written = _written('{"",a}', 12345, dialect)
    assert "42601" in written
    assert "invalid option" in written


@pytest.mark.parametrize(
    ("options", "catalog"), [("{}", 12345), ("{host,port=1}", 1417), ("{user}", 1418)]
)
def test_clickhouse_answers_true_for_options_the_catalog_lists(options, catalog):
    assert str(chdb.query(_written(options, catalog, "clickhouse"), "CSV")).strip() == "true"


@pytest.mark.parametrize(
    ("options", "catalog", "name"),
    [('{"",日本語😀,é}', 12345, ""), ("{host,user}", 1417, "user"), ("{a=1}", 1418, "a")],
)
def test_clickhouse_raises_42601_for_an_option_the_catalog_does_not_list(options, catalog, name):
    with pytest.raises(Exception, match=rf'postgresql-error 42601: invalid option "{name}"'):
        chdb.query(_written(options, catalog, "clickhouse"), "CSV")
