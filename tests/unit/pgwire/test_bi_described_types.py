# Copyright (c) 2026 Kenneth Stott
# Canary: d7831cf6-277b-4cc9-a187-cce9bccec100
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The BI tools' catalog statements are described with the types PostgreSQL 17 describes
(REQ-2204, REQ-2167): each statement is the one the tool sent, as replayed from
scripts/conformance/bi_tools, and each column's described OID is PostgreSQL's."""

# Requirements: REQ-2204, REQ-2167

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.pgwire.catalog import _declared_types
from provisa.pgwire.catalog_types import result_types
from provisa.pgwire.server import _described_oid

pytestmark = pytest.mark.unit

_BI_TOOLS = Path(__file__).resolve().parents[3] / "scripts" / "conformance" / "bi_tools"


def _statement(tool: str, statement_id: int) -> str:
    stored = yaml.safe_load((_BI_TOOLS / f"{tool}.yaml").read_text())
    return next(s["sql"] for s in stored["statements"] if s["id"] == statement_id)


def _described(sql: str, served: dict[str, str] | None = None) -> dict[str, int]:
    """Each column's described OID, the catalog answer serving every column as text unless
    ``served`` names its type."""
    names = [name for name, _ in result_types(sql)]
    served_types = [(served or {}).get(name, "text") for name in names]
    declared = _declared_types(sql, names, served_types)
    return {name: _described_oid(t) for name, t in zip(names, declared)}


# G4: a UNION of an information_schema domain with another type resolves as PostgreSQL's
# select_common_type does: the domain is its base type.


@pytest.mark.parametrize("statement_id", [14, 15, 17, 19, 22])
def test_metabases_column_union_describes_domains_as_their_base_types(statement_id):
    described = _described(_statement("metabase", statement_id))
    assert described["name"] == 19
    assert described["table-schema"] == 19
    assert described["table-name"] == 19
    assert described["database-default"] == 1043


def test_a_union_of_one_domain_keeps_the_domain():
    sql = (
        "SELECT t.table_name FROM information_schema.tables t "
        "UNION ALL SELECT v.table_name FROM information_schema.views v"
    )
    assert result_types(sql) == (("table_name", "information_schema.sql_identifier"),)


def test_a_union_of_a_domain_and_null_is_the_base_type():
    sql = (
        "SELECT c.column_default AS d FROM information_schema.columns c UNION ALL SELECT NULL AS d"
    )
    assert result_types(sql) == (("d", "character varying"),)


def test_a_plain_domain_column_is_described_as_its_base_type():
    """PostgreSQL describes a domain column with its base type (printtup.c getBaseTypeAndTypmod):
    sql_identifier as name."""
    sql = "SELECT c.column_name FROM information_schema.columns c"
    assert _described(sql) == {"column_name": 19}


# G5: a regclass cast in a CASE's subquery, current_catalog, and an integer division.


@pytest.mark.parametrize("statement_id", [26, 38])
def test_psqls_trigger_parent_is_regclass(statement_id):
    assert _described(_statement("psql", statement_id))["parent"] == 2205


def test_current_catalog_is_name():
    assert [t for _, t in result_types(_statement("metabase", 4))] == ["name"]
    assert list(_described(_statement("metabase", 4)).values()) == [19]


def test_grafanas_version_division_is_int4():
    sql = _statement("grafana", 2)
    assert _described(sql, {"version": "bigint"}) == {"version": 23}


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT 7::int2 / 2::int2 AS v", "smallint"),
        ("SELECT 7::int2 / 2 AS v", "int"),
        ("SELECT 7::int / 2 AS v", "int"),
        ("SELECT 7::int8 / 2 AS v", "bigint"),
        ("SELECT 7 / 2::int8 AS v", "bigint"),
    ],
)
def test_integer_division_is_the_wider_operands_type(sql, expected):
    assert result_types(sql) == (("v", expected),)
