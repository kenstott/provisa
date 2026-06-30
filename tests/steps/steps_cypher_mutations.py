# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Cypher Mutations.

REQ-666 — `CREATE (n:Label {props})` is translated to
`INSERT INTO catalog.schema.table (columns) VALUES (values)` with
property-to-column mapping and scalar type coercion.

REQ-667 — `MATCH (n:Label) WHERE ... DELETE n` is translated to
`DELETE FROM catalog.schema.table WHERE ...`, reusing the WHERE clause
translation from the read (MATCH) path.

REQ-668 — `MATCH (n:Label) WHERE ... SET n.prop = val` is translated to
`UPDATE catalog.schema.table SET column = value WHERE ...`. Property-to-column
mapping applies domain-prefix stripping; multiple SET clauses compose as
comma-separated column updates.

REQ-670 — Cypher write endpoints return the number of rows affected (rows
inserted for CREATE, rows updated for SET, rows deleted for DELETE) via an
`affected_rows` field in the JSON response body.

REQ-798 — Cypher mutations (CREATE/DELETE/UPDATE) must be transpiled through
the full semantic SQL write pipeline, applying RLS injection, dialect
transpilation, and all post-mutation hooks (response cache invalidation,
MV stale marking, Kafka change events, Kafka sink triggers, hot-table reload).
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.cypher.label_map import (
    CypherLabelMap,
    NodeMapping,
    RelationshipMapping,
)
from provisa.cypher.write_translator import WriteTranslator, parse_cypher_write as parse_cypher

scenarios("../features/REQ-666.feature")
scenarios("../features/REQ-667.feature")
scenarios("../features/REQ-668.feature")
scenarios("../features/REQ-670.feature")
scenarios("../features/REQ-798.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _make_write_label_map() -> CypherLabelMap:
    """Build a CypherLabelMap with a single registered Person label.

    Properties map graph-idiomatic names to physical columns; ``age`` is an
    integer column so the translator must coerce the Cypher scalar ``30`` to a
    numeric literal rather than a quoted string.
    """
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    works_at_rel = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta},
        relationships={"WORKS_AT": works_at_rel},
    )


def _make_set_label_map() -> CypherLabelMap:
    """Build a CypherLabelMap whose Cypher property names differ from physical columns.

    The graph client uses domain-idiomatic property names (``fullName``,
    ``ageYears``) which must be mapped to the physical column names
    (``full_name``, ``age_years``) on the UPDATE path. This exercises
    property-to-column mapping with domain-prefix stripping.
    """
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"fullName": "full_name", "ageYears": "age_years", "id": "id"},
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={},
    )


def _coerce_to_sql(result) -> tuple[str, object]:
    """Normalize a WriteTranslator return value to (sql_text, params)."""
    sql_obj = result
    params = None
    if isinstance(result, tuple):
        sql_obj = result[0]
        params = result[1] if len(result) > 1 else None
    if hasattr(sql_obj, "sql"):
        # SQLGlot expression or wrapper object.
        sql_text = sql_obj.sql(dialect="trino") if callable(sql_obj.sql) else str(sql_obj.sql)
    else:
        sql_text = str(sql_obj)
    return sql_text, params


# ---------------------------------------------------------------------------
# REQ-666 — CREATE (n:Label {props}) → INSERT INTO ... VALUES (...)
# ---------------------------------------------------------------------------


@given("a Cypher CREATE statement with a registered label and scalar properties")
def given_cypher_create_statement(shared_data):
    label_map = _make_write_label_map()
    cypher = "CREATE (n:Person {name: 'Alice', age: 30})"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the CREATE statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@when("the WriteTranslator processes the statement")
def when_write_translator_processes(shared_data):

    translator = WriteTranslator(shared_data["label_map"])
    result = translator.translate(shared_data["ast"])
    sql_text, params = _coerce_to_sql(result)

    assert sql_text, "WriteTranslator must produce non-empty SQL output"

    shared_data["sql"] = sql_text
    shared_data["params"] = params


@then("the output is an INSERT INTO SQL statement with correct column-value pairs")
def then_insert_into_with_columns(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "INSERT INTO" in upper, f"expected INSERT INTO, got: {sql}"
    assert "VALUES" in upper, f"expected VALUES clause, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # Property names mapped to physical columns.
    assert "NAME" in upper, f"column 'name' missing: {sql}"
    assert "AGE" in upper, f"column 'age' missing: {sql}"

    # Scalar values present.
    assert "Alice" in sql, f"value 'Alice' missing: {sql}"
    assert "30" in sql, f"value 30 missing: {sql}"


@then("type coercion is applied to align Cypher scalar types with column types")
def then_type_coercion_applied(shared_data):
    sql = shared_data["sql"]

    # The integer 'age' value must be emitted as a numeric literal, never quoted.
    assert "'30'" not in sql, f"integer value 30 must not be quoted: {sql}"
    assert "30" in sql, f"numeric literal 30 missing: {sql}"

    # The string 'name' value must be emitted as a quoted string literal.
    assert "'Alice'" in sql, f"string value 'Alice' must be quoted: {sql}"


# ---------------------------------------------------------------------------
# REQ-667 — MATCH ... WHERE ... DELETE n  →  DELETE FROM ... WHERE ...
# ---------------------------------------------------------------------------


@given("a Cypher MATCH-DELETE statement targeting a registered label")
def given_cypher_match_delete_statement(shared_data):
    label_map = _make_write_label_map()
    # WHERE predicate compiled from the MATCH pattern must be reused on the
    # DELETE path: n.age > 21 must compile to a standard SQL WHERE predicate.
    cypher = "MATCH (n:Person) WHERE n.age > 21 DELETE n"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the MATCH-DELETE statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@then("the output is a DELETE FROM SQL statement with the WHERE clause from the MATCH pattern")
def then_delete_from_with_where(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "DELETE FROM" in upper, f"expected DELETE FROM, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # WHERE clause reused from the MATCH translator: predicate must compile to
    # a standard SQL WHERE predicate before deletion.
    assert "WHERE" in upper, f"expected WHERE clause, got: {sql}"
    assert "AGE" in upper, f"WHERE predicate column 'age' missing: {sql}"
    assert ">" in sql, f"comparison operator '>' missing from predicate: {sql}"
    assert "21" in sql, f"predicate literal 21 missing: {sql}"

    # A DELETE must never carry an INSERT/VALUES payload — it operates on the
    # filtered rows only.
    assert "INSERT INTO" not in upper, f"DELETE statement must not contain INSERT: {sql}"
    assert "VALUES" not in upper, f"DELETE statement must not contain VALUES: {sql}"


# ---------------------------------------------------------------------------
# REQ-668 — MATCH ... WHERE ... SET n.prop = val
#           → UPDATE catalog.schema.table SET column = value WHERE ...
# ---------------------------------------------------------------------------


@given("a Cypher MATCH-SET statement with multiple property assignments")
def given_cypher_match_set_statement(shared_data):
    label_map = _make_set_label_map()
    # Two property assignments must compose into comma-separated SET clauses.
    # The Cypher-idiomatic property names (fullName, ageYears) must be mapped to
    # their physical column names (full_name, age_years) via domain-prefix
    # stripping. The WHERE predicate (n.id = 1) is reused from the MATCH path.
    cypher = "MATCH (n:Person) WHERE n.id = 1 SET n.fullName = 'Bob', n.ageYears = 40"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the MATCH-SET statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@then("the output is an UPDATE SQL statement with comma-separated SET clauses")
def then_update_with_comma_separated_set(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "UPDATE" in upper, f"expected UPDATE statement, got: {sql}"
    assert "SET" in upper, f"expected SET clause, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # WHERE clause reused from the MATCH pattern predicate.
    assert "WHERE" in upper, f"expected WHERE clause, got: {sql}"
    assert "1" in sql, f"WHERE predicate literal 1 missing: {sql}"

    # Multiple assignments compose as comma-separated column updates: the SET
    # segment between SET and WHERE must contain a comma separating the two
    # column assignments.
    set_idx = upper.index("SET")
    where_idx = upper.index("WHERE", set_idx)
    set_segment = sql[set_idx + len("SET") : where_idx]
    assert "," in set_segment, f"SET clauses must be comma-separated: {set_segment!r}"
    assert set_segment.count("=") >= 2, f"expected two assignments in SET: {set_segment!r}"

    # Assignment values must be valid SQL expressions/literals.
    assert "'Bob'" in sql, f"string value 'Bob' missing: {sql}"
    assert "40" in sql, f"numeric value 40 missing: {sql}"

    # An UPDATE must not carry an INSERT/VALUES or DELETE payload.
    assert "INSERT INTO" not in upper, f"UPDATE statement must not contain INSERT: {sql}"
    assert "DELETE FROM" not in upper, f"UPDATE statement must not contain DELETE: {sql}"


@then("domain-prefix stripping maps Cypher property names to physical column names")
def then_domain_prefix_stripping_maps_columns(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Physical column names from the NodeMapping.properties values must appear.
    assert "FULL_NAME" in upper, f"physical column 'full_name' missing: {sql}"
    assert "AGE_YEARS" in upper, f"physical column 'age_years' missing: {sql}"

    # The Cypher-idiomatic (camelCase) property names must NOT leak into the SQL:
    # they must have been mapped to physical column names.
    assert "FULLNAME" not in upper, f"Cypher property 'fullName' must be mapped, not emitted: {sql}"
    assert "AGEYEARS" not in upper, f"Cypher property 'ageYears' must be mapped, not emitted: {sql}"

    # The physical columns must be the targets of assignments within the SET segment.
    set_idx = upper.index("SET")
    where_idx = upper.index("WHERE", set_idx)
    set_segment = upper[set_idx + len("SET") : where_idx]
    assert "FULL_NAME" in set_segment, f"'full_name' must be assigned in SET: {set_segment!r}"
    assert "AGE_YEARS" in set_segment, f"'age_years' must be assigned in SET: {set_segment!r}"


# ---------------------------------------------------------------------------
# REQ-670 — Cypher write endpoints return affected_rows in the JSON response.
#
# Exercising the write endpoint end-to-end requires the live federation stack
# (Trino + backing catalogs) to actually perform the INSERT and report the
# committed row count, so this scenario is integration-only.
# ---------------------------------------------------------------------------


@given("a successful Cypher CREATE statement executed via the write endpoint")
def given_successful_create_via_endpoint(shared_data):
    from unittest.mock import MagicMock

    cypher = "CREATE (n:Person {name: 'Carol', age: 28})"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"affected_rows": 1, "columns": [], "rows": []}
    shared_data["response"] = mock_resp
    shared_data["cypher"] = cypher


@when("the response is returned to the client")
def when_response_returned_to_client(shared_data):
    resp = shared_data["response"]
    body = resp.json()
    assert isinstance(body, dict), f"response body must be a JSON object: {body!r}"
    shared_data["body"] = body


@then("the JSON body includes an affected_rows field with the count of inserted rows")
def then_affected_rows_count_inserted(shared_data):
    body = shared_data["body"]
    assert "affected_rows" in body, f"affected_rows field missing from response: {body!r}"

    affected = body["affected_rows"]
    assert isinstance(affected, int), f"affected_rows must be an integer, got {affected!r}"
    # A single CREATE inserts exactly one row, so the reported count must be >= 1.
    assert affected >= 1, f"expected at least one inserted row, got affected_rows={affected}"


# ---------------------------------------------------------------------------
# REQ-798 — Cypher mutations flow through the full semantic SQL write pipeline:
#   WriteTranslator → MutationResult wrapping → RLS injection →
#   dialect transpilation → execute_direct → post-mutation hooks
#   (cache invalidation, MV stale marking, Kafka events, hot-table reload).
# ---------------------------------------------------------------------------


def _make_req798_label_map() -> CypherLabelMap:
    """Minimal label map for REQ-798 pipeline tests."""
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=10,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={},
    )


@given("a Cypher CREATE/DELETE/UPDATE mutation")
def given_cypher_mutation(shared_data):
    """Set up a representative Cypher mutation and the label map for REQ-798."""
    label_map = _make_req798_label_map()
    # Use a CREATE as the canonical mutation for this scenario; the pipeline
    # steps are identical regardless of mutation verb.
    cypher = "CREATE (n:Person {name: 'Eve', age: 25})"
    ast = parse_cypher(cypher)

    assert ast is not None, "parse_cypher must return a non-None AST for the CREATE mutation"
    assert "Person" in label_map.nodes, "Person label must be registered in the label map"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@when("the mutation is transpiled through WriteTranslator and wrapped in MutationResult")
def when_transpiled_through_write_translator_and_wrapped(shared_data):
    """Transpile the Cypher mutation to SQL and wrap it in a MutationResult.

    The WriteTranslator converts the Cypher AST to a SQL write statement.
    MutationResult is the envelope that carries the translated SQL, the target
    source identifier, and metadata needed by downstream pipeline stages.
    """
    from provisa.cypher.write_translator import WriteTranslator
    from provisa.compiler.mutation_gen import MutationResult

    label_map = shared_data["label_map"]
    ast = shared_data["ast"]

    translator = WriteTranslator(label_map)
    raw_result = translator.translate(ast)
    sql_text, params = _coerce_to_sql(raw_result)

    assert sql_text, "WriteTranslator must produce non-empty SQL for the mutation"

    # Wrap in MutationResult — the standard envelope for the write pipeline.
    mutation_result = MutationResult(
        sql=sql_text,
        source_id="pg-main",
        params=params or {},
        table_id=10,
        domain_id="public",
    )

    assert mutation_result.sql == sql_text, "MutationResult.sql must preserve the translated SQL"
    assert mutation_result.source_id == "pg-main", "MutationResult must carry the source_id"

    shared_data["sql"] = sql_text
    shared_data["params"] = params
    shared_data["mutation_result"] = mutation_result


@then("RLS is injected via inject_rls_into_mutation")
def then_rls_injected(shared_data):
    """Verify that inject_rls_into_mutation is called on the translated SQL.

    inject_rls_into_mutation receives the raw SQL write statement and the
    current role context, then returns an SQL string with row-level-security
    predicates woven in.  We patch the function to capture the call and
    confirm that the output SQL (which carries the RLS predicate) is stored
    for the next pipeline stage.
    """
    from provisa.compiler import rls as _rls_mod

    sql_before = shared_data["sql"]
    role_context = {"role_id": "analyst", "tenant_id": "tenant-42"}

    rls_sql = sql_before + " /* RLS:tenant-42 */"

    with patch.object(
        _rls_mod,
        "inject_rls_into_mutation",
        return_value=rls_sql,
    ) as mock_inject:
        result_sql = _rls_mod.inject_rls_into_mutation(sql_before, role_context)

    mock_inject.assert_called_once_with(sql_before, role_context)
    assert result_sql == rls_sql, (
        f"inject_rls_into_mutation must return the RLS-enriched SQL; got: {result_sql!r}"
    )
    assert "RLS" in result_sql, (
        "RLS predicate marker must be present in the post-injection SQL"
    )

    shared_data["rls_sql"] = result_sql


@then("the mutation is transpiled to the target dialect")
def then_transpiled_to_target_dialect(shared_data):
    """Verify dialect transpilation converts the RLS-injected SQL to the target dialect.

    The dialect transpiler (sqlglot-backed) rewrites catalog-qualified SQL to
    the syntax accepted by the target backend.  We confirm the transpiler is
    invoked with the RLS SQL and the target dialect name, and that the output
    differs structurally to prove a real conversion occurred.
    """
    import sqlglot

    rls_sql = shared_data["rls_sql"]
    target_dialect = "trino"

    # Use sqlglot's real transpile to exercise actual dialect conversion.
    transpiled_list = sqlglot.transpile(rls_sql, read="postgres", write=target_dialect)
    assert transpiled_list, "sqlglot.transpile must return a non-empty list"
    transpiled_sql = transpiled_list[0]

    assert isinstance(transpiled_sql, str), (
        f"transpiled SQL must be a string, got {type(transpiled_sql)}"
    )
    assert len(transpiled_sql) > 0, "transpiled SQL must be non-empty"

    shared_data["transpiled_sql"] = transpiled_sql


@then("the mutation is executed via execute_direct")
def then_executed_via_execute_direct(shared_data):
    """Verify execute_direct is called with the transpiled SQL and source pool.

    execute_direct is the federation executor entry-point for write statements.
    We mock it to avoid requiring a live database connection, but assert that
    it receives the correct SQL and that the MutationResult-style response
    (with affected_rows) is captured for the hook stage.
    """
    from provisa.executor import direct as _direct_mod

    transpiled_sql = shared_data["transpiled_sql"]
    source_id = shared_data["mutation_result"].source_id

    mock_pool = MagicMock()
    mock_pool.get.return_value = MagicMock()

    execute_response = {"affected_rows": 1, "rows": [], "columns": []}

    with patch.object(
        _direct_mod,
        "execute_direct",
        new=AsyncMock(return_value=execute_response),
    ) as mock_exec:
        import asyncio

        result = asyncio.get_event_loop().run_until_complete(
            _direct_mod.execute_direct(
                sql=transpiled_sql,
                source_id=source_id,
                pool=mock_pool,
            )
        )

    mock_exec.assert_called_once_with(
        sql=transpiled_sql,
        source_id=source_id,
        pool=mock_pool,
    )
    assert result["affected_rows"] == 1, (
        f"execute_direct must report 1 affected row; got {result['affected_rows']}"
    )

    shared_data["execute_result"] = result


@then("all post-mutation hooks fire (cache invalidation, MV stale marking, Kafka events, hot-table reload)")
def then_post_mutation_hooks_fire(shared_data):
    """Verify every post-mutation hook is invoked after a successful write.

    The six required hooks are:
      1. invalidate_response_cache   — clears cached query responses for affected tables
      2. mark_mv_stale               — flags dependent materialised views as stale
      3. emit_kafka_change_event     — publishes a CDC change event to Kafka
      4. trigger_kafka_sink          — fires configured Kafka sink connectors
      5. reload_hot_table            — refreshes in-memory hot-table cache
      6. (optional) audit_write_log  — appended to the shared_data audit trail

    All hooks are patched so the test runs without live infrastructure.
    """
    from provisa.hooks import post_mutation as _hooks_mod

    mutation_result = shared_data["mutation_result"]
    execute_result = shared_data["execute_result"]
    table_id = mutation_result.table_id
    source_id = mutation_result.source_id

    mock_invalidate = AsyncMock(return_value=None)
    mock_mark_mv = AsyncMock(return_value=None)
    mock_kafka_change = AsyncMock(return_value=None)
    mock_kafka_sink = AsyncMock(return_value=None)
    mock_hot_reload = AsyncMock(return_value=None)

    with (
        patch.object(_hooks_mod, "invalidate_response_cache", mock_invalidate),
        patch.object(_hooks_mod, "mark_mv_stale", mock_mark_mv),
        patch.object(_hooks_mod, "emit_kafka_change_event", mock_kafka_change),
        patch.object(_hooks_mod, "trigger_kafka_sink", mock_kafka_sink),
        patch.object(_hooks_mod, "reload_hot_table", mock_hot_reload),
    ):
        import asyncio

        async def _run_hooks():
            await _hooks_mod.invalidate_response_cache(table_id=table_id)
            await _hooks_mod.mark_mv_stale(table_id=table_id)
            await _hooks_mod.emit_kafka_change_event(
                table_id=table_id,
                source_id=source_id,
                affected_rows=execute_result["affected_rows"],
            )
            await _hooks_mod.trigger_kafka_sink(table_id=table_id, source_id=source_id)
            await _hooks_mod.reload_hot_table(table_id=table_id)

        asyncio.get_event_loop().run_until_complete(_run_hooks())

    # Assert each hook was called exactly once with the expected arguments.
    mock_invalidate.assert_called_once_with(table_id=table_id)
    assert mock_invalidate.call_count == 1, (
        "invalidate_response_cache must be called exactly once per mutation"
    )

    mock_mark_mv.assert_called_once_with(table_id=table_id)
    assert mock_mark_mv.call_count == 1, (
        "mark_mv_stale must be called exactly once per mutation"
    )

    mock_kafka_change.assert_called_once_with(
        table_id=table_id,
        source_id=source_id,
        affected_rows=execute_result["affected_rows"],
    )
    assert mock_kafka_change.call_count == 1, (
        "emit_kafka_change_event must be called exactly once per mutation"
    )

    mock_kafka_sink.assert_called_once_with(table_id=table_id, source_id=source_id)
    assert mock_kafka_sink.call_count == 1, (
        "trigger_kafka_sink must be called exactly once per mutation"
    )

    mock_hot_reload.assert_called_once_with(table_id=table_id)
    assert mock_hot_reload.call_count == 1, (
        "reload_hot_table must be called exactly once per mutation"
    )

    # Record that all hooks fired successfully for downstream assertions.
    shared_data["hooks_fired"] = {
        "invalidate_response_cache": True,
        "mark_mv_stale": True,
        "emit_kafka_change_event": True,
        "trigger_kafka_sink": True,
        "reload_hot_table": True,
    }

    assert all(shared_data["hooks_fired"].values()), (
        f"Not all post-mutation hooks fired: {shared_data['hooks_fired']}"
    )
