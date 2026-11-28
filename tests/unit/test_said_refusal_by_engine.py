# Copyright (c) 2026 Kenneth Stott
# Canary: 82410acd-8974-48c3-9fae-9f3482134802
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A folded refusal of interval_out, interval_send and intervaltypmodout raises PostgreSQL's error on
every engine (REQ-2236): provisa_raise where the engine's dialect has it, or where a cast of a text
that is no number answers instead of raising (refused by name there); elsewhere the cast that raises.

27e9b7ad3b wrote provisa_raise on every engine: on the pass-through PostgreSQL engines (cockroachdb,
greenplum, opengauss, yugabytedb, sqlalchemy) it is no function, and on Db2, Exasol, SQL Server and
SAP HANA it has no form, so those edges stopped raising PostgreSQL's error."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest
from sqlglot.dialects.dialect import Dialects

from provisa.core.pg_error import MARK
from provisa.transpiler.function_macros import MACROS, FunctionRefused
from provisa.transpiler.metadata_server_forms import RAISE_DIALECTS, SERVER_FORM_RAISE
from provisa.transpiler.pg_forms_type_io import readable
from provisa.transpiler.pg_forms_types import CASTS_UNCHECKED
from provisa.transpiler.rows.base import Step
from provisa.transpiler.transpile import transpile

#: Each edge PostgreSQL refuses, by the function it calls.
_EDGES = {
    "interval_out": "SELECT pg_catalog.\"interval_out\"('178956971 years'::interval) AS v",
    "interval_send": "SELECT pg_catalog.\"interval_send\"('178956971 years'::interval) AS v",
    "intervaltypmodout": "SELECT pg_catalog.\"intervaltypmodout\"('0'::integer) AS v",
}

#: Engines whose dialect writes provisa_raise as its own error function.
_RAISING = ("duckdb", "trino", "presto", "athena", "clickhouse")
#: Engines whose cast of a text that is no number answers 0 or NULL.
_UNCHECKED = ("mysql", "singlestore", "sqlite", "spark", "databricks", "hive")
#: Engines with no provisa_raise whose cast of a text that is no number raises: PostgreSQL's
#: pass-through engines, Db2, Exasol, SQL Server, SAP HANA and the other warehouses.
_CASTING = ("postgres", "tsql", "db2", "exasol", "saphana", "oracle", "snowflake", "bigquery")

_CASES = [(name, sql) for name, sql in _EDGES.items()]


def _is_cast_refusal(folded: str) -> bool:
    return MARK in folded and "AS INT" in folded.upper() and SERVER_FORM_RAISE not in folded.lower()


class TestFoldedRefusal:
    @pytest.mark.parametrize("dialect", _RAISING)
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_provisa_raise_where_the_engine_has_it(self, dialect: str, name: str, sql: str) -> None:
        folded = readable(sql, dialect)
        assert f"{SERVER_FORM_RAISE}(" in folded.lower(), (name, dialect, folded)

    @pytest.mark.parametrize("dialect", _UNCHECKED)
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_provisa_raise_where_a_cast_answers(self, dialect: str, name: str, sql: str) -> None:
        folded = readable(sql, dialect)
        assert f"{SERVER_FORM_RAISE}(" in folded.lower(), (name, dialect, folded)

    @pytest.mark.parametrize("dialect", _CASTING)
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_cast_where_the_engine_has_no_provisa_raise(
        self, dialect: str, name: str, sql: str
    ) -> None:
        folded = readable(sql, dialect)
        assert _is_cast_refusal(folded), (name, dialect, folded)

    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_cast_where_the_engine_is_not_named(self, name: str, sql: str) -> None:
        assert _is_cast_refusal(readable(sql)), name


class TestTranspiled:
    @pytest.mark.parametrize("dialect", ("duckdb", "trino", "clickhouse"))
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_raises_through_the_engine_s_error(self, dialect: str, name: str, sql: str) -> None:
        written = transpile(sql, dialect, generated=False)
        assert MARK in written and SERVER_FORM_RAISE not in written.lower(), (
            name,
            dialect,
            written,
        )

    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_postgres_pass_through_carries_no_provisa_raise(self, name: str, sql: str) -> None:
        written = transpile(sql, "postgres", generated=False)
        assert SERVER_FORM_RAISE not in written.lower() and MARK in written, (name, written)

    @pytest.mark.parametrize("dialect", ("tsql", "exasol"))
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_cast_engines_are_not_refused_by_name(self, dialect: str, name: str, sql: str) -> None:
        written = transpile(sql, dialect, generated=False)
        assert MARK in written and SERVER_FORM_RAISE not in written.lower(), (
            name,
            dialect,
            written,
        )

    @pytest.mark.parametrize("dialect", ("mysql", "sqlite"))
    @pytest.mark.parametrize(("name", "sql"), _CASES)
    def test_unchecked_casts_are_refused_by_name(self, dialect: str, name: str, sql: str) -> None:
        with pytest.raises(FunctionRefused):
            transpile(sql, dialect, generated=False)


class TestCapability:
    def test_raise_dialects_are_those_the_macro_has_a_form_on(self) -> None:
        """RAISE_DIALECTS is what rows.system's provisa_raise resolves on, and nothing else."""
        written = {d.value for d in Dialects if d.value} | set(RAISE_DIALECTS)
        for arity in (2, 3):
            macro = MACROS[(SERVER_FORM_RAISE, arity)]
            formed = {
                d for d in written if d != "postgres" and macro.resolved(d)[0] is not Step.REFUSED
            }
            assert formed == set(RAISE_DIALECTS), arity

    def test_no_dialect_both_raises_and_reads_an_unchecked_cast(self) -> None:
        assert not RAISE_DIALECTS & CASTS_UNCHECKED
