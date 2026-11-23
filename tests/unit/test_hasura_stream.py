# Copyright (c) 2026 Kenneth Stott
# Canary: 6a0d3f58-c2e7-4b19-8f4a-e95b17c03d26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Hasura-preset table serves Hasura's streaming subscription (REQ-194, amended 2026-11-22):
``<table>_stream(batch_size, cursor, where)`` with ``<table>_stream_cursor_input`` and
``cursor_ordering``, spelled per preset. Each batch holds the rows past the cursor in cursor
order, at most ``batch_size``, then the cursor moves on; ``where`` and governance (row rules,
masks) apply as on queries, since each batch is answered as the table's own query."""

# Requirements: REQ-194

from __future__ import annotations

import duckdb
import pytest
from graphql import (
    GraphQLEnumType,
    GraphQLInputObjectType,
    GraphQLList,
    GraphQLNonNull,
    OperationDefinitionNode,
    get_named_type,
    parse,
    validate,
)

from provisa.compiler import naming
from provisa.compiler.context import build_context
from provisa.compiler.hasura_stream import (
    StreamRefused,
    masked_fields,
    stream_batches,
    stream_request,
)
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.names_table import root_field
from provisa.compiler.rls import RLSContext, as_rule
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.security.masking import MaskingRule, MaskType
from tests.helpers import governed_for_role

#: Each Hasura preset with the Hasura convention it serves.
_PRESETS = [("hasura_graphql", "hasura-default"), ("hasura_graphql_default", "graphql-default")]

_ROWS = [(i, f"c{i}", 10 * i) for i in range(1, 8)]


def _schema_input(convention: str) -> SchemaInput:
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "store",
            "schema_name": "public",
            "table_name": "order_lines",
            "alias": None,
            "gql_naming_convention": convention,
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "customer_name", "visible_to": ["admin"]},
                {"column_name": "total", "visible_to": ["admin"]},
            ],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
            "write_grants": [],
        }
    ]
    types = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="customer_name", data_type="varchar", is_nullable=True),
        ColumnMetadata(column_name="total", data_type="integer", is_nullable=True),
    ]
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types={1: types},
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "store", "description": "Store"}],
        source_types={"pg": "postgresql"},
    )


def _built(convention: str):
    naming.configure(gql="apollo_graphql", sql="snake")
    si = _schema_input(convention)
    return generate_schema(si), build_context(si)


def _names(conv: str) -> naming.HasuraTableNames:
    return naming.hasura_table_names(conv, "order_lines")


# ── the schema ────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_the_stream_field_takes_hasuras_arguments(preset, conv):
    schema, _ = _built(preset)
    names = _names(conv)
    assert schema.subscription_type is not None
    stream = schema.subscription_type.fields[names.select_stream]

    def arg(name: str) -> str:
        return naming.hasura_argument_name(conv, name)

    assert set(stream.args) == {arg("batch_size"), arg("cursor"), arg("where")}
    assert str(stream.type) == f"[{names.type}!]!"
    assert str(stream.args[arg("batch_size")].type) == "Int!"
    assert str(stream.args[arg("cursor")].type) == f"[{names.stream_cursor_input}]!"
    assert get_named_type(stream.args[arg("where")].type).name == names.bool_exp

    cursor_input = schema.type_map[names.stream_cursor_input]
    assert isinstance(cursor_input, GraphQLInputObjectType)
    assert set(cursor_input.fields) == {arg("initial_value"), arg("ordering")}
    initial = cursor_input.fields[arg("initial_value")].type
    assert isinstance(initial, GraphQLNonNull)
    value_input = get_named_type(initial)
    assert value_input.name == naming.hasura_type_name(
        conv, names.type, "stream_cursor_value_input"
    )
    assert isinstance(value_input, GraphQLInputObjectType)
    assert set(value_input.fields) == {
        naming.hasura_field_name(conv, c) for c in ("id", "customer_name", "total")
    }
    assert all(not isinstance(f.type, GraphQLNonNull) for f in value_input.fields.values())

    ordering = cursor_input.fields[arg("ordering")].type
    assert isinstance(ordering, GraphQLEnumType)
    assert ordering.name == ("cursor_ordering" if conv == "hasura-default" else "CursorOrdering")
    assert list(ordering.values) == ["ASC", "DESC"]


def test_graphql_default_spells_the_stream_in_camel_and_pascal_case():
    schema, _ = _built("hasura_graphql_default")
    assert schema.subscription_type is not None
    stream = schema.subscription_type.fields["orderLinesStream"]
    assert set(stream.args) == {"batchSize", "cursor", "where"}
    cursor_input = schema.type_map["OrderLinesStreamCursorInput"]
    assert isinstance(cursor_input, GraphQLInputObjectType)
    assert set(cursor_input.fields) == {"initialValue", "ordering"}
    assert "OrderLinesStreamCursorValueInput" in schema.type_map
    assert "customerName" in schema.type_map["OrderLinesStreamCursorValueInput"].fields  # type: ignore[union-attr]


def test_hasura_default_spells_the_stream_in_snake_case():
    schema, _ = _built("hasura_graphql")
    assert schema.subscription_type is not None
    stream = schema.subscription_type.fields["order_lines_stream"]
    assert set(stream.args) == {"batch_size", "cursor", "where"}
    assert "order_lines_stream_cursor_value_input" in schema.type_map


def test_a_provisa_convention_serves_no_stream():
    schema, _ = _built("apollo_graphql")
    sub = schema.subscription_type
    assert sub is None or not any(f.endswith(("_stream", "Stream")) for f in sub.fields)


@pytest.mark.parametrize(("preset", "conv"), _PRESETS)
def test_the_stream_resolves_through_the_names_table(preset, conv):
    _, ctx = _built(preset)
    found = root_field(ctx, _names(conv).select_stream)
    assert found is not None
    surface, table = found
    assert surface == "graphql_stream" and table.table_name == "order_lines"


# ── the batches ───────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def db():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    con.execute(
        "CREATE TABLE public.order_lines (id INTEGER, customer_name VARCHAR, total INTEGER)"
    )
    con.executemany("INSERT INTO public.order_lines VALUES (?, ?, ?)", _ROWS)
    # Inserted out of cursor order: the batches order them.
    con.execute("INSERT INTO public.order_lines VALUES (0, 'c0', 0)")
    yield con
    con.close()


def _runner(db, built, *, rls=None, masking_rules=None):
    """A batch's query answered as the query pipeline answers it for the role: compiled,
    governed (row rules, masks) and run. Each query text it was given is kept."""
    schema, ctx = built
    asked: list[str] = []

    async def run(text: str) -> dict:
        asked.append(text)
        doc = parse(text)
        errors = validate(schema, doc)
        assert not errors, errors
        (compiled,) = compile_query(doc, ctx)
        sql = governed_for_role(compiled.sql, ctx, "admin", rls=rls, masking_rules=masking_rules)
        rows = db.execute(sql, compiled.params).fetchall()
        records = [{c.field_name: v for c, v in zip(compiled.columns, row)} for row in rows]
        return {"data": {compiled.root_field: records}}

    return run, asked


async def _frames(
    built, text: str, run, variables=None, polls: int = 0, masking_rules=None
) -> list[dict]:
    """The frames the stream sends, reading again ``polls`` times after a batch that was not
    full; ``masking_rules`` are the masks the role ``admin`` reads the table through."""
    schema, ctx = built
    doc = parse(text)
    errors = validate(schema, doc)
    assert not errors, errors
    (op,) = [d for d in doc.definitions if isinstance(d, OperationDefinitionNode)]
    (field,) = op.selection_set.selections
    found = root_field(ctx, field.name.value)  # type: ignore[union-attr]
    assert found is not None
    masked = masked_fields(masking_rules or {}, ctx, found[1], "admin")
    req = stream_request(schema, op, field, found[1], variables, masked)  # type: ignore[arg-type]
    left = [polls]

    async def idle() -> bool:
        left[0] -= 1
        return left[0] >= 0

    return [f async for f in stream_batches(req, run, idle)]


def _ids(frames: list[dict], key: str) -> list[list[int]]:
    return [[r["id"] for r in f["data"][key]] for f in frames]


async def test_batches_hold_the_rows_past_the_cursor_in_order_at_most_batch_size(db):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built)
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 2,"
        " cursor: {initial_value: {id: 2}, ordering: ASC}) { id customer_name } }",
        run,
    )
    assert _ids(frames, "order_lines_stream") == [[3, 4], [5, 6], [7]]
    assert frames[0]["data"]["order_lines_stream"][0] == {"id": 3, "customer_name": "c3"}


async def test_the_cursor_moves_on_so_a_new_row_arrives_in_the_next_batch(db):
    built = _built("hasura_graphql")
    run, asked = _runner(db, built)

    async def run_then_insert(text: str) -> dict:
        answer = await run(text)
        if len(asked) == 2:
            db.execute("INSERT INTO public.order_lines VALUES (9, 'c9', 90)")
        return answer

    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 10,"
        " cursor: {initial_value: {id: 5}}) { id } }",
        run_then_insert,
        polls=2,
    )
    assert _ids(frames, "order_lines_stream") == [[6, 7], [9]]


async def test_a_descending_cursor_reads_down(db):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built)
    frames = await _frames(
        built,
        "subscription { s: order_lines_stream(batch_size: 3,"
        " cursor: [{initial_value: {id: 5}, ordering: DESC}]) { id } }",
        run,
    )
    assert _ids(frames, "s") == [[4, 3, 2], [1, 0]]


async def test_where_narrows_every_batch(db):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built)
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 2, cursor: {initial_value: {id: 0}},"
        " where: {total: {_gte: 40}}) { id total } }",
        run,
    )
    assert _ids(frames, "order_lines_stream") == [[4, 5], [6, 7]]


async def test_variables_feed_the_cursor_and_where(db):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built)
    frames = await _frames(
        built,
        "subscription S($from: Int, $w: order_lines_bool_exp) { order_lines_stream("
        "batch_size: 5, cursor: {initial_value: {id: $from}}, where: $w) { id } }",
        run,
        variables={"from": 4, "w": {"total": {"_lt": 70}}},
    )
    assert _ids(frames, "order_lines_stream") == [[5, 6]]


async def test_a_row_rule_governs_every_batch(db):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built, rls=RLSContext(rules={1: as_rule("total <= 50")}))
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 2,"
        " cursor: {initial_value: {id: 1}}) { id } }",
        run,
    )
    assert _ids(frames, "order_lines_stream") == [[2, 3], [4, 5]]


async def test_a_mask_governs_every_batch(db):
    built = _built("hasura_graphql")
    rule = MaskingRule(mask_type=MaskType.constant, value=None)
    run, _ = _runner(db, built, masking_rules={(1, "admin"): {"customer_name": (rule, "varchar")}})
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 10,"
        " cursor: {initial_value: {id: 5}}) { id customer_name } }",
        run,
    )
    assert frames == [
        {
            "data": {
                "order_lines_stream": [
                    {"id": 6, "customer_name": None},
                    {"id": 7, "customer_name": None},
                ]
            }
        }
    ]


_CONSTANT = MaskingRule(mask_type=MaskType.constant, value=None)


@pytest.mark.parametrize(
    ("preset", "stream", "cursor", "masked"),
    [
        ("hasura_graphql", "order_lines_stream", "initial_value", "customer_name"),
        ("hasura_graphql_default", "orderLinesStream", "initialValue", "customerName"),
    ],
)
async def test_a_stream_on_a_masked_cursor_column_is_refused_naming_it(
    db, preset, stream, cursor, masked
):
    """REQ-194 (amended 2026-11-22): each batch's WHERE compares the real value while the cursor
    would move on the masked one, so a cursor the role reads masked is refused by name."""
    built = _built(preset)
    rules = {(1, "admin"): {"customer_name": (_CONSTANT, "varchar")}}
    run, asked = _runner(db, built, masking_rules=rules)
    batch = "batch_size" if preset == "hasura_graphql" else "batchSize"
    with pytest.raises(StreamRefused, match=f"cursor column {masked!r} .*masked"):
        await _frames(
            built,
            f'subscription {{ {stream}({batch}: 2, cursor: {{{cursor}: {{{masked}: "c0"}}}})'
            " { id } }",
            run,
            masking_rules=rules,
        )
    assert asked == []


async def test_a_stream_on_an_unmasked_cursor_column_streams_beside_a_masked_one(db):
    built = _built("hasura_graphql")
    rules = {(1, "admin"): {"customer_name": (_CONSTANT, "varchar")}}
    run, _ = _runner(db, built, masking_rules=rules)
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 2,"
        " cursor: {initial_value: {id: 4}}) { id customer_name } }",
        run,
        masking_rules=rules,
    )
    assert _ids(frames, "order_lines_stream") == [[5, 6], [7]]
    assert all(r["customer_name"] is None for f in frames for r in f["data"]["order_lines_stream"])


async def test_a_fake_on_the_cursor_column_is_not_a_mask(db):
    """A fake is the column's own declaration, never a mask (REQ-1942): a condition on it reads
    the fake values the cursor moves on, so the stream is not refused."""
    schema, ctx = _built("hasura_graphql")
    fake = MaskingRule(mask_type=MaskType.fake, value="name")
    found = root_field(ctx, "order_lines_stream")
    assert found is not None
    rules = {(1, "admin"): {"customer_name": (fake, "varchar")}}
    assert masked_fields(rules, ctx, found[1], "admin") == frozenset()


def test_another_roles_mask_does_not_refuse_the_stream():
    _, ctx = _built("hasura_graphql")
    found = root_field(ctx, "order_lines_stream")
    assert found is not None
    rules = {(1, "analyst"): {"customer_name": (_CONSTANT, "varchar")}}
    assert masked_fields(rules, ctx, found[1], "admin") == frozenset()
    assert masked_fields(rules, ctx, found[1], "analyst") == frozenset({"customer_name"})


async def test_graphql_default_streams_with_its_spellings(db):
    built = _built("hasura_graphql_default")
    run, asked = _runner(db, built)
    frames = await _frames(
        built,
        "subscription { orderLinesStream(batchSize: 2,"
        " cursor: {initialValue: {id: 4}, ordering: ASC}, where: {total: {_lte: 60}})"
        " { id customerName } }",
        run,
    )
    assert _ids(frames, "orderLinesStream") == [[5, 6]]
    assert frames[0]["data"]["orderLinesStream"][0] == {"id": 5, "customerName": "c5"}
    assert "orderBy" in asked[0] and "ASC" in asked[0]


async def test_a_batch_with_no_rows_sends_nothing(db):
    built = _built("hasura_graphql")
    run, asked = _runner(db, built)
    frames = await _frames(
        built,
        "subscription { order_lines_stream(batch_size: 2,"
        " cursor: {initial_value: {id: 7}}) { id } }",
        run,
        polls=1,
    )
    assert frames == [] and len(asked) == 2


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ("batch_size: 0, cursor: {initial_value: {id: 1}}", "batch_size must be at least 1"),
        (
            "batch_size: 1, cursor: [{initial_value: {id: 1}}, {initial_value: {id: 2}}]",
            "exactly one cursor",
        ),
        ("batch_size: 1, cursor: {initial_value: {id: 1, total: 2}}", "exactly one column"),
        ("batch_size: 1, cursor: {initial_value: {}}", "exactly one column"),
    ],
)
async def test_a_stream_hasura_does_not_take_is_refused_by_name(db, arguments, message):
    built = _built("hasura_graphql")
    run, _ = _runner(db, built)
    with pytest.raises(StreamRefused, match=message):
        await _frames(built, f"subscription {{ order_lines_stream({arguments}) {{ id }} }}", run)


def test_the_cursor_list_holds_nullable_inputs():
    schema, _ = _built("hasura_graphql")
    assert schema.subscription_type is not None
    cursor = schema.subscription_type.fields["order_lines_stream"].args["cursor"].type
    assert isinstance(cursor, GraphQLNonNull) and isinstance(cursor.of_type, GraphQLList)
    assert not isinstance(cursor.of_type.of_type, GraphQLNonNull)


# ── the transport ─────────────────────────────────────────────────────────────────────────────


async def test_the_subscription_transport_sends_each_batch_as_an_sse_frame(db):
    """POST /data/graphql with a stream subscription: each batch is one SSE frame, answered
    through the query pipeline (``_answer``) for the request's role."""
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, patch

    from provisa.api.data import subscription_sse

    built = _built("hasura_graphql")
    schema, ctx = built
    run, asked = _runner(db, built)
    roles: list[str] = []

    async def answer(text, schema_, ctx_, state_, variables, role, role_id):
        assert schema_ is schema and ctx_ is ctx
        roles.append(role_id)
        return await run(text)

    doc = parse(
        "subscription { order_lines_stream(batch_size: 4,"
        " cursor: {initial_value: {id: 1}}) { id } }"
    )
    raw_request = MagicMock()
    raw_request.headers = {}
    raw_request.is_disconnected = AsyncMock(return_value=True)
    state = MagicMock()
    state.masking_rules = {}
    with (
        patch(
            "provisa.api.role_surface.built_surface",
            return_value=SimpleNamespace(schema=schema, context=ctx),
        ),
        patch.object(subscription_sse, "_answer", answer),
        patch.object(subscription_sse, "STREAM_REFETCH_SECONDS", 0),
        patch("provisa.subscriptions.owner.owner_of", return_value=object()),
        patch("provisa.subscriptions.owner.ensure_held", AsyncMock(return_value=None)),
        patch("provisa.api.request_timeout.outlive_deadline"),
    ):
        response = await subscription_sse.handle_subscription_sse(
            doc, ctx, None, state, None, {"id": "admin"}, "admin", raw_request
        )
        chunks = [c async for c in response.body_iterator]  # type: ignore[union-attr]
    frames = [json.loads(str(c).removeprefix("data: ")) for c in chunks]
    assert _ids(frames, "order_lines_stream") == [[2, 3, 4, 5], [6, 7]]
    assert roles == ["admin", "admin"] and len(asked) == 2


async def test_the_subscription_transport_refuses_a_masked_cursor_by_name(db):
    """The transport reads the role's masks from the server state: a stream whose cursor column
    the role reads masked is answered with one frame of errors naming the column, and no batch is
    read."""
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, patch

    from provisa.api.data import subscription_sse

    schema, ctx = _built("hasura_graphql")
    asked: list[str] = []

    async def answer(text, *_args):
        asked.append(text)
        raise AssertionError("a refused stream reads no batch")

    doc = parse(
        "subscription { order_lines_stream(batch_size: 4,"
        ' cursor: {initial_value: {customer_name: "c1"}}) { id } }'
    )
    raw_request = MagicMock()
    raw_request.headers = {}
    state = MagicMock()
    state.masking_rules = {(1, "admin"): {"customer_name": (_CONSTANT, "varchar")}}
    with (
        patch(
            "provisa.api.role_surface.built_surface",
            return_value=SimpleNamespace(schema=schema, context=ctx),
        ),
        patch.object(subscription_sse, "_answer", answer),
        patch("provisa.subscriptions.owner.owner_of", return_value=object()),
        patch("provisa.subscriptions.owner.ensure_held", AsyncMock(return_value=None)),
        patch("provisa.api.request_timeout.outlive_deadline"),
    ):
        response = await subscription_sse.handle_subscription_sse(
            doc, ctx, None, state, None, {"id": "admin"}, "admin", raw_request
        )
        chunks = [c async for c in response.body_iterator]  # type: ignore[union-attr]
    (frame,) = [json.loads(str(c).removeprefix("data: ")) for c in chunks]
    (error,) = frame["errors"]
    assert "'customer_name'" in error["message"] and "masked" in error["message"]
    assert asked == []
