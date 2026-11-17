# Copyright (c) 2026 Kenneth Stott
# Canary: 7fa8a432-637e-4317-a1a0-c203c51ae995
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An imported Hasura project reads only its own files, and its permission values stay values.

- Every ``!include``, and every file the parser opens, resolves on its real path inside the
  uploaded project; one outside (an absolute path, ``..``, or a symbolic link out) is refused by
  name.
- A permission filter's values become SQL literals rendered by sqlglot, its column and table names
  identifiers: a value carrying a quote and SQL is one string literal in the rule, and the rule
  grants no more rows than an equality with that string does. The DDN importer renders the same.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pytest
import sqlglot
import yaml
from sqlglot import exp

from provisa.ddn.mapper import _ddn_filter_to_sql
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.filters import Hop, bool_expr_to_sql
from provisa.import_shared.refused import (
    FilterOperatorRefused,
    FilterValueRefused,
    IncludeOutsideUpload,
)

INJECTION = "x' OR '1'='1"


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def _project(tmp_path: Path, index: list[str]) -> Path:
    """A databases/ layout project whose tables index lists ``index`` entries."""
    md = tmp_path / "upload" / "metadata"
    _write(
        md / "databases" / "databases.yaml",
        yaml.dump(
            [
                {
                    "name": "default",
                    "kind": "postgres",
                    "tables": "!include default/tables/tables.yaml",
                }
            ]
        ),
    )
    _write(md / "databases" / "default" / "tables" / "tables.yaml", "\n".join(index) + "\n")
    return md


def _outside(tmp_path: Path) -> Path:
    secret = tmp_path / "elsewhere" / "secret.yaml"
    _write(secret, yaml.dump({"table": {"schema": "public", "name": "leaked"}}))
    return secret


# --- T5: includes confined to the upload --------------------------------------------------------


def test_include_of_etc_hosts_is_refused(tmp_path: Path) -> None:
    md = _project(tmp_path, ["- !include /etc/hosts"])
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_metadata_dir(md)
    assert caught.value.code == "import.include_outside_upload"
    assert caught.value.params == {"path": "/etc/hosts"}


def test_include_climbing_out_with_dotdot_is_refused(tmp_path: Path) -> None:
    _outside(tmp_path)
    md = _project(tmp_path, ['- "!include ../../../../../elsewhere/secret.yaml"'])
    with pytest.raises(IncludeOutsideUpload):
        parse_metadata_dir(md)


def test_include_through_a_link_out_is_refused(tmp_path: Path) -> None:
    secret = _outside(tmp_path)
    md = _project(tmp_path, ["- !include public_leaked.yaml"])
    os.symlink(secret, md / "databases" / "default" / "tables" / "public_leaked.yaml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_metadata_dir(md)
    assert caught.value.params == {"path": "public_leaked.yaml"}


def test_project_file_that_is_a_link_out_is_refused(tmp_path: Path) -> None:
    """Not only includes: a file the parser opens by its fixed name is confined too."""
    md = tmp_path / "upload" / "metadata"
    md.mkdir(parents=True)
    leaked = tmp_path / "elsewhere" / "tables.yaml"
    _write(leaked, yaml.dump([{"table": {"schema": "public", "name": "leaked"}}]))
    os.symlink(leaked, md / "tables.yaml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_metadata_dir(md)
    assert caught.value.params == {"path": "tables.yaml"}


def test_missing_file_outside_is_refused_not_ignored(tmp_path: Path) -> None:
    """Whether a file outside the project exists is not told: refused either way."""
    md = _project(tmp_path, ["- !include /nonexistent/nowhere.yaml"])
    with pytest.raises(IncludeOutsideUpload):
        parse_metadata_dir(md)


def test_include_inside_the_project_still_loads(tmp_path: Path) -> None:
    md = _project(tmp_path, ["- !include public_orders.yaml"])
    _write(
        md / "databases" / "default" / "tables" / "public_orders.yaml",
        yaml.dump({"table": {"schema": "public", "name": "orders"}}),
    )
    metadata = parse_metadata_dir(md)
    assert [t.name for t in metadata.sources[0].tables] == ["orders"]


# --- T6: permission values are literals ---------------------------------------------------------


def _parsed(rule: str) -> exp.Expression:
    tree = sqlglot.parse_one(rule, read="postgres")
    assert tree is not None
    return tree


def _rows(rule: str) -> list[tuple]:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    con.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, ?)", [INJECTION])
    return con.execute(f"SELECT id FROM t WHERE {rule} ORDER BY id").fetchall()  # noqa: S608  # nosec B608


def test_quote_and_sql_in_a_value_is_one_string_literal() -> None:
    rule = bool_expr_to_sql({"name": {"_eq": INJECTION}})
    tree = _parsed(rule)
    assert isinstance(tree, exp.EQ)
    assert tree.this == exp.column("name")
    assert isinstance(tree.expression, exp.Literal) and tree.expression.is_string
    assert tree.expression.this == INJECTION
    assert not list(tree.find_all(exp.Or))


def test_injected_value_grants_nothing_more() -> None:
    """Only the row whose name is the whole crafted string matches, not every row."""
    assert _rows(bool_expr_to_sql({"name": {"_eq": INJECTION}})) == [(3,)]


def test_values_in_a_list_are_literals() -> None:
    tree = _parsed(bool_expr_to_sql({"name": {"_in": ["bob", INJECTION]}}))
    assert isinstance(tree, exp.In)
    assert [e.this for e in tree.expressions] == ["bob", INJECTION]
    assert all(isinstance(e, exp.Literal) and e.is_string for e in tree.expressions)


def test_backslash_in_a_value_stays_in_the_literal() -> None:
    value = "a\\' OR TRUE --"
    tree = _parsed(bool_expr_to_sql({"name": {"_eq": value}}))
    assert isinstance(tree, exp.EQ) and tree.expression.this == value


def test_column_name_with_sql_is_one_identifier() -> None:
    key = "name = name OR TRUE --"
    tree = _parsed(bool_expr_to_sql({key: {"_eq": "bob"}}))
    assert isinstance(tree, exp.EQ)
    assert isinstance(tree.this, exp.Column) and tree.this.name == key


def test_session_variable_name_with_sql_is_one_literal() -> None:
    header = "X-Hasura-User') OR TRUE --"
    tree = _parsed(bool_expr_to_sql({"id": {"_eq": header}}))
    assert isinstance(tree, exp.EQ)
    call = tree.expression
    assert isinstance(call, exp.Anonymous) and call.name.lower() == "current_setting"
    [arg] = call.expressions
    assert isinstance(arg, exp.Literal) and arg.is_string
    assert not list(tree.find_all(exp.Or))


def test_exists_table_names_are_identifiers() -> None:
    expr = {
        "_exists": {"_table": {"schema": "auth", "name": "m WHERE TRUE) OR (TRUE"}, "_where": {}}
    }
    tree = _parsed(bool_expr_to_sql(expr))
    table = tree.find(exp.Table)
    assert table is not None and table.name == "m WHERE TRUE) OR (TRUE"
    assert not list(tree.find_all(exp.Or))


def test_relationship_names_are_identifiers() -> None:
    def relations(table: str, name: str) -> Hop | None:
        return Hop("s.orders", [("id", "cust id")]) if name == "orders" else None

    rule = bool_expr_to_sql({"orders": {"total": {"_gt": 5}}}, table="s.c", relations=relations)
    tree = _parsed(rule)
    assert any(c.name == "cust id" for c in tree.find_all(exp.Column))


def test_numbers_booleans_and_null_are_typed_literals() -> None:
    assert bool_expr_to_sql({"n": {"_eq": 42}}) == "n = 42"
    assert bool_expr_to_sql({"n": {"_eq": -1.5}}) == "n = -1.5"
    assert bool_expr_to_sql({"b": {"_eq": True}}) == "b = TRUE"
    assert bool_expr_to_sql({"b": {"_eq": None}}) == "b = NULL"


@pytest.mark.parametrize("value", [{"nested": "x"}, float("nan"), float("inf"), object()])
def test_a_value_that_is_no_literal_is_refused(value: object) -> None:
    with pytest.raises(FilterValueRefused) as caught:
        bool_expr_to_sql({"name": {"_eq": value}})
    assert caught.value.code == "import.filter_value_refused"


def test_an_unknown_operator_is_refused_by_name() -> None:
    with pytest.raises(FilterOperatorRefused) as caught:
        bool_expr_to_sql({"name": {"_x */ OR TRUE /*": 1}})
    assert caught.value.params == {"operator": "_x */ OR TRUE /*"}


# --- the DDN importer renders the same ---------------------------------------------------------


def test_ddn_value_with_quote_and_sql_is_one_literal() -> None:
    rule = _ddn_filter_to_sql({"name": {"_eq": INJECTION}}, {})
    tree = _parsed(rule)
    assert isinstance(tree, exp.EQ) and tree.expression.this == INJECTION
    assert _rows(rule) == [(3,)]


def test_ddn_list_values_and_column_are_rendered() -> None:
    rule = _ddn_filter_to_sql({"nm": {"_in": [INJECTION, "bob"]}}, {"nm": "full name"})
    tree = _parsed(rule)
    assert isinstance(tree, exp.In) and tree.this.name == "full name"
    assert [e.this for e in tree.expressions] == [INJECTION, "bob"]


def test_ddn_unknown_operator_is_refused() -> None:
    with pytest.raises(FilterOperatorRefused):
        _ddn_filter_to_sql({"name": {"_x */ OR TRUE /*": 1}}, {})


# --- every refusal is catalogued in every locale ------------------------------------------------

_PARAMS = {
    "include_outside_upload": "{{path}}",
    "filter_value_refused": "{{value}}",
    "filter_operator_refused": "{{operator}}",
}


def test_refusals_are_catalogued_in_every_locale() -> None:
    locales = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) >= 12
    for catalog in catalogs:
        entries = json.loads(catalog.read_text())["serverErrors"]["import"]
        for key, param in _PARAMS.items():
            assert param in entries[key], (catalog, key)
