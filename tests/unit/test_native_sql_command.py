# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0d3b7a-8f21-4c69-b0a4-2c7e9d1f6a38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source_query command holding native SQL for a relational source (REQ-1988, amended
2026-11-13): its text is checked against its source when it is saved, split into statements by
the source's own dialect, and a call binds each argument as a driver parameter of its type."""

# Requirements: REQ-1988

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, LiteralString, cast

import pydantic
import pytest

from provisa.core.models import Function, FunctionArgument, ProvisaConfig, Source, SourceType
from provisa.core.native_sql import DIALECTS, NativeSqlRefused, check_on_source, split
from provisa.executor.result import QueryResult

_SQL = (
    "INSERT INTO app.notes (id, body) VALUES ({{id}}, {{body}});\n"
    "SELECT id, body FROM app.notes WHERE id = {{id}}"
)


def _command(**kw: Any) -> Function:
    fields: dict[str, Any] = {
        "name": "add_note",
        "source_id": "pg",
        "function_name": "add_note",
        "returns": "",
        "kind": "mutation",
        "impl_kind": "source_query",
        "binding": {"sql": _SQL},
        "domain_id": "notes",
        "arguments": [
            FunctionArgument(name="id", type="integer"),
            FunctionArgument(name="body", type="text"),
        ],
    }
    fields.update(kw)
    return Function(**fields)


def _config(command: Function, source_type: SourceType = SourceType.postgresql) -> ProvisaConfig:
    source = Source(
        id="pg", type=source_type, host="db", port=5432, database="app", username="u", password="p"
    )
    return ProvisaConfig(sources=[source], domains=[], tables=[], roles=[], functions=[command])


# --- saved -----------------------------------------------------------------------------------


def test_a_native_sql_command_on_postgresql_is_saved_and_may_write():
    held = _config(_command())
    assert held.functions[0].kind == "mutation"


def test_a_reading_native_sql_command_is_saved():
    held = _config(
        _command(
            kind="query",
            binding={"sql": "SELECT body FROM app.notes WHERE id = {{id}}"},
            arguments=[FunctionArgument(name="id", type="integer")],
        )
    )
    assert held.functions[0].kind == "query"


@pytest.mark.parametrize(
    ("binding", "reason"),
    [
        ({"sql": "  "}, "holds its SQL as 'sql'"),
        ({"sql": "SELECT 1", "dialect": "postgres"}, r"holds 'sql' only, not \['dialect'\]"),
    ],
)
def test_a_native_sql_binding_without_its_text_is_refused(binding, reason):
    with pytest.raises(pydantic.ValidationError, match=reason):
        _command(binding=binding)


def test_a_name_that_is_no_declared_argument_is_refused():
    with pytest.raises(pydantic.ValidationError, match=r"names \['body'\], which the command"):
        _config(_command(arguments=[FunctionArgument(name="id", type="integer")]))


def test_a_declared_argument_never_used_is_refused():
    extra = FunctionArgument(name="tag", type="text")
    with pytest.raises(pydantic.ValidationError, match=r"arguments \['tag'\] appear nowhere"):
        _config(_command(arguments=[*_command().arguments, extra]))


def test_a_non_relational_source_is_refused_by_name():
    with pytest.raises(pydantic.ValidationError, match="'pg' is a mongodb source"):
        _config(_command(), SourceType.mongodb)


def test_a_relational_source_without_a_runner_is_refused_by_name():
    with pytest.raises(
        pydantic.ValidationError, match="native SQL commands are not available on mysql yet"
    ):
        _config(_command(), SourceType.mysql)


def test_a_command_whose_source_is_not_declared_is_refused():
    with pytest.raises(pydantic.ValidationError, match="its source 'pg' is not declared"):
        ProvisaConfig(sources=[], domains=[], tables=[], roles=[], functions=[_command()])


def test_a_mongodb_pipeline_command_is_unchanged():
    pipeline = {"inputCollection": "movies", "pipeline": [{"$match": {"t": "{{q}}"}}]}
    args = [FunctionArgument(name="q", type="text")]
    with pytest.raises(pydantic.ValidationError, match="a source_query command is a query"):
        _command(binding=pipeline, arguments=args)
    assert _command(binding=pipeline, arguments=args, kind="query").binding == pipeline


# --- split -----------------------------------------------------------------------------------


def test_a_semicolon_inside_quotes_and_comments_does_not_end_a_statement():
    sql = (
        "SELECT 'a;b', E'c\\';d' /* e; /* f; */ g; */;\n"
        "CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $body$ LANGUAGE sql;"
        "SELECT {{x}}"
    )
    statements = split("c", sql, "postgres")
    assert len(statements) == 3
    assert statements[0].texts == ("SELECT 'a;b', E'c\\';d'",)
    assert statements[1].texts[0].endswith("LANGUAGE sql")
    assert (statements[2].texts, statements[2].names) == (("SELECT ", ""), ("x",))


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("SELECT 'unterminated", "cannot be split into postgres statements"),
        ("SELECT '{{x}}'", "inside a quoted string"),
        ("SELECT $1", r"'\$' is a driver parameter"),
        ("BEGIN; INSERT INTO t VALUES (1); COMMIT", "begins with BEGIN"),
        ("INSERT INTO t VALUES (1); COMMIT", "begins with COMMIT"),
        ("  ;; -- nothing\n", "holds no statement"),
    ],
)
def test_a_text_that_cannot_be_run_as_one_transaction_is_refused(sql, reason):
    with pytest.raises(NativeSqlRefused, match=reason):
        split("c", sql, "postgres")


def test_every_type_native_sql_is_checked_for_has_a_run():
    from provisa.executor.native_sql_command import _RUNNERS

    assert set(DIALECTS) == set(_RUNNERS) == {"postgresql"}


def test_check_on_source_names_the_command():
    with pytest.raises(NativeSqlRefused, match="command 'c': native SQL commands are not"):
        check_on_source("c", "w", "snowflake", {"sql": "SELECT 1"}, [])


# --- a call ----------------------------------------------------------------------------------


class _Driver:
    def __init__(self) -> None:
        self.ran: list[tuple[str, dict]] = []
        self.read_only: bool | None = None

    async def execute_in_one_transaction(self, statements, *, read_only=False):
        self.ran = statements
        self.read_only = read_only
        return QueryResult(
            rows=[(7, "it's; DROP TABLE app.notes; --")],
            column_names=["id", "body"],
            column_types=["int4", "text"],
        )


def _state(driver: _Driver) -> SimpleNamespace:
    pools = SimpleNamespace(has=lambda sid: sid == "pg", get=lambda _sid: driver)
    return SimpleNamespace(
        udf_trace_sink=None,
        minted_sessions=None,
        source_types={"pg": "postgresql"},
        source_pools=pools,
    )


async def test_arguments_are_bound_as_parameters_never_as_text():
    from psycopg._queries import PostgresQuery
    from psycopg.adapt import Transformer

    from provisa.executor.function_dispatch import dispatch_function

    driver = _Driver()
    value = "it's; DROP TABLE app.notes; --"
    # The SQL surfaces pass a call's arguments by position.
    rows = await dispatch_function(
        _command().model_dump(), {"a0": "7", "a1": value}, _state(driver), "editor"
    )
    assert rows == [{"id": 7, "body": value}]
    (insert, insert_params), (select, select_params) = driver.ran
    assert value not in insert and value not in select
    assert insert == "INSERT INTO app.notes (id, body) VALUES (%(a_id)s, %(a_body)s)"
    assert insert_params == {"a_id": 7, "a_body": value}  # converted to the declared types
    assert select_params == {"a_id": 7}
    # psycopg sends each as a bound $n parameter; a repeated name is one parameter.
    query = PostgresQuery(Transformer())
    query.convert(cast(LiteralString, insert), insert_params)
    assert query.query == b"INSERT INTO app.notes (id, body) VALUES ($1, $2)"


async def test_a_percent_of_the_text_stays_a_percent():
    from psycopg._queries import PostgresQuery
    from psycopg.adapt import Transformer

    from provisa.executor.function_dispatch import dispatch_function

    driver = _Driver()
    fn = _command(
        kind="query",
        binding={"sql": "SELECT id, body FROM app.notes WHERE body LIKE '50%' AND id = {{id}}"},
        arguments=[FunctionArgument(name="id", type="integer")],
    ).model_dump()
    await dispatch_function(fn, {"id": 1}, _state(driver), "editor")
    query = PostgresQuery(Transformer())
    sql, params = driver.ran[0]
    query.convert(cast(LiteralString, sql), params)
    assert query.query == b"SELECT id, body FROM app.notes WHERE body LIKE '50%' AND id = $1"


async def test_an_argument_not_of_its_type_runs_nothing():
    from provisa.core.parameter_values import ParameterValueRefused
    from provisa.executor.function_dispatch import dispatch_function

    driver = _Driver()
    with pytest.raises(ParameterValueRefused, match="'id' takes a value of type integer"):
        await dispatch_function(
            _command().model_dump(), {"id": "seven", "body": "x"}, _state(driver), "editor"
        )
    assert driver.ran == []


async def test_a_declared_output_the_rows_do_not_match_fails_the_call():
    from fastapi import HTTPException

    from provisa.core.models import DatasetColumn
    from provisa.executor.function_dispatch import dispatch_function

    fn = _command(output_columns=[DatasetColumn(name="title", type="text")]).model_dump()
    with pytest.raises(HTTPException) as refused:
        await dispatch_function(fn, {"id": 7, "body": "x"}, _state(_Driver()), "editor")
    assert refused.value.status_code == 422


async def test_a_query_runs_in_a_read_only_transaction_and_a_mutation_read_write():
    from provisa.executor.function_dispatch import dispatch_function

    reader = _Driver()
    fn = _command(
        kind="query",
        binding={"sql": "SELECT id, body FROM app.notes WHERE id = {{id}}"},
        arguments=[FunctionArgument(name="id", type="integer")],
    ).model_dump()
    await dispatch_function(fn, {"id": 1}, _state(reader), "editor")
    assert reader.read_only is True

    writer = _Driver()
    await dispatch_function(
        _command().model_dump(), {"id": 7, "body": "x"}, _state(writer), "editor"
    )
    assert writer.read_only is False


# --- a writing native SQL command is called on its own -----------------------------------------


def _commands(kind: str) -> dict:
    return {"add_note": _command(kind=kind).model_dump()}


def test_a_writing_native_sql_command_is_refused_inside_a_larger_statement():
    import sqlglot

    from provisa.pgwire._pipeline import _refuse_composed_mutators

    tree = sqlglot.parse_one(
        "SELECT n.id FROM add_note(1, 'x') AS n JOIN other o ON o.id = n.id", read="postgres"
    )
    with pytest.raises(PermissionError, match="add_note.*writes to its source"):
        _refuse_composed_mutators(tree, _commands("mutation"))
    _refuse_composed_mutators(tree, _commands("query"))


def test_a_writing_native_sql_command_is_refused_in_a_view_definition():
    from provisa.executor.source_operation import refuse_writes_in_definition

    sql = "SELECT * FROM add_note(1, 'x') AS n"
    with pytest.raises(ValueError, match="add_note.*writes to its source"):
        refuse_writes_in_definition(sql, _commands("mutation"), "view 'v'")
    refuse_writes_in_definition(sql, _commands("query"), "view 'v'")


async def test_a_direct_call_of_a_writing_native_sql_command_still_runs():
    from provisa.executor.function_dispatch import dispatch_function

    driver = _Driver()
    rows = await dispatch_function(
        _command().model_dump(), {"id": 7, "body": "x"}, _state(driver), "editor"
    )
    assert rows and len(driver.ran) == 2
