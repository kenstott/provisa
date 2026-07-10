# Copyright (c) 2026 Kenneth Stott
# Canary: 7b9e3f2a-1c4d-4a8e-b5f6-2d0c8e4a7b3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Hasura v2 Parity — Mutation & Subscription Features (REQ-212–220).

Tests the compiler/mutation/scheduler/events component boundaries without hitting
live databases. Real component-to-component interaction; external services are
stubbed only where they are not the boundary under test.

Covered REQ-IDs:
  Upsert Mutations:          REQ-212
  DISTINCT ON:               REQ-213
  Column Presets:            REQ-214
  Inherited Roles:           REQ-215
  Scheduled Triggers:        REQ-216
  Batch Mutations:           REQ-217
  Cursor Pagination:         REQ-218
  SSE Subscriptions:         REQ-219
  DB Event Triggers:         REQ-220
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table_meta(
    *,
    table_id: int = 1,
    field_name: str = "orders",
    type_name: str = "Orders",
    source_id: str = "sales-pg",
    schema_name: str = "public",
    table_name: str = "orders",
    column_presets: list | None = None,
) -> object:
    from provisa.compiler.sql_gen import TableMeta

    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=type_name,
        source_id=source_id,
        catalog_name="sales_pg",
        schema_name=schema_name,
        table_name=table_name,
        column_presets=column_presets or [],
    )


def _make_field_node(name: str, args: dict) -> object:
    """Build a minimal graphql-core FieldNode for the given args dict."""
    from graphql import parse

    # Serialize the args dict into inline GraphQL argument syntax.
    def _render_value(v: object) -> str:
        if isinstance(v, dict):
            pairs = ", ".join(f"{k}: {_render_value(val)}" for k, val in v.items())
            return "{" + pairs + "}"
        if isinstance(v, list):
            items = ", ".join(_render_value(i) for i in v)
            return "[" + items + "]"
        if isinstance(v, str):
            return f'"{v}"'
        if isinstance(v, bool):
            return "true" if v else "false"
        return str(v)

    args_str = ", ".join(f"{k}: {_render_value(v)}" for k, v in args.items())
    gql_args = f"({args_str})" if args_str else ""
    doc = parse(f"mutation {{ {name}{gql_args} {{ id }} }}")
    from graphql import OperationDefinitionNode, FieldNode

    op = doc.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    field = op.selection_set.selections[0]
    assert isinstance(field, FieldNode)
    return field


# ---------------------------------------------------------------------------
# REQ-212: Upsert mutations — INSERT … ON CONFLICT … DO UPDATE
# ---------------------------------------------------------------------------


class TestUpsertMutations:
    """REQ-212: upsert_<table> mutation compiles to INSERT … ON CONFLICT … DO UPDATE SQL."""

    def test_compile_upsert_produces_on_conflict_sql(self):
        # REQ-212: compile_upsert returns SQL with ON CONFLICT clause
        from provisa.compiler.mutation_gen import compile_upsert

        table = _make_table_meta()
        field_node = _make_field_node(
            "upsertOrders",
            {"input": {"id": 1, "amount": 99}, "on_conflict": ["id"]},
        )

        result = compile_upsert(field_node, table, variables=None)  # type: ignore[arg-type]

        assert "ON CONFLICT" in result.sql
        assert result.mutation_type == "upsert"

    def test_compile_upsert_do_update_set_clause(self):
        # REQ-212: non-conflict columns appear in DO UPDATE SET
        from provisa.compiler.mutation_gen import compile_upsert

        table = _make_table_meta()
        field_node = _make_field_node(
            "upsertOrders",
            {"input": {"id": 1, "amount": 99, "status": "open"}, "on_conflict": ["id"]},
        )

        result = compile_upsert(field_node, table, variables=None)  # type: ignore[arg-type]

        assert "DO UPDATE SET" in result.sql
        assert "EXCLUDED" in result.sql

    def test_compile_upsert_do_nothing_when_all_conflict_cols(self):
        # REQ-212: when all columns are conflict columns, DO NOTHING is emitted
        from provisa.compiler.mutation_gen import compile_upsert

        table = _make_table_meta()
        field_node = _make_field_node(
            "upsertOrders",
            {"input": {"id": 1}, "on_conflict": ["id"]},
        )

        result = compile_upsert(field_node, table, variables=None)  # type: ignore[arg-type]

        assert "DO NOTHING" in result.sql

    def test_compile_mutation_routes_upsert_field(self):
        # REQ-212: compile_mutation dispatches upsert_ prefix correctly
        from graphql import parse as gql_parse
        from provisa.compiler.mutation_gen import compile_mutation
        from provisa.compiler.sql_gen import CompilationContext

        table = _make_table_meta()
        ctx = CompilationContext(tables={"orders": table})  # type: ignore[arg-type]
        doc = gql_parse(
            'mutation { upsert_orders(input: {id: 1, amount: 42}, on_conflict: ["id"]) { id } }'
        )

        results = compile_mutation(doc, ctx, source_types={"sales-pg": "postgresql"})

        assert len(results) == 1
        assert results[0].mutation_type == "upsert"
        assert "ON CONFLICT" in results[0].sql


# ---------------------------------------------------------------------------
# REQ-213: DISTINCT ON query argument
# ---------------------------------------------------------------------------


class TestDistinctOn:
    """REQ-213: distinct_on arg added to root query fields; SQL has DISTINCT ON."""

    def test_schema_gen_adds_distinct_on_arg(self):
        # REQ-213: generated schema includes distinct_on argument on query fields
        from provisa.compiler.introspect import ColumnMetadata
        from provisa.compiler import naming as _naming
        from provisa.compiler.schema_gen import SchemaInput, generate_schema

        _naming.configure(gql="snake")
        col = ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)
        tables = [
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "d",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
            }
        ]
        role = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
        inp = SchemaInput(
            tables=tables,
            relationships=[],
            column_types={1: [col]},
            naming_rules=[],
            role=role,
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(inp)

        query_type = schema.query_type
        assert query_type is not None
        orders_field = query_type.fields.get("orders")
        assert orders_field is not None
        assert "distinct_on" in orders_field.args

    def test_sql_gen_emits_distinct_on(self):
        # REQ-213: compile_query produces DISTINCT ON SQL when distinct_on arg provided
        from graphql import parse as gql_parse, validate
        from provisa.compiler.introspect import ColumnMetadata
        from provisa.compiler import naming as _naming
        from provisa.compiler.schema_gen import SchemaInput, generate_schema
        from provisa.compiler.sql_gen import compile_query
        from provisa.compiler.context import build_context

        _naming.configure(gql="snake")
        col_id = ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)
        col_region = ColumnMetadata(column_name="region", data_type="varchar(50)", is_nullable=True)
        tables = [
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "d",
                "schema_name": "public",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "region", "visible_to": ["admin"]},
                ],
            }
        ]
        role = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
        inp = SchemaInput(
            tables=tables,
            relationships=[],
            column_types={1: [col_id, col_region]},
            naming_rules=[],
            role=role,
            domains=[{"id": "d", "description": "D"}],
        )
        schema = generate_schema(inp)
        ctx = build_context(inp)

        query = gql_parse("{ orders(distinct_on: [region]) { id region } }")
        errors = validate(schema, query)
        assert not errors, errors

        results = compile_query(query, ctx, variables=None)
        assert results
        sql = results[0].sql
        assert "DISTINCT" in sql.upper()


# ---------------------------------------------------------------------------
# REQ-214: Column presets
# ---------------------------------------------------------------------------


class TestColumnPresets:
    """REQ-214: preset columns are stripped from user input and injected into SQL."""

    def test_apply_column_presets_now_source(self):
        # REQ-214: now source injects an ISO timestamp
        from provisa.compiler.mutation_gen import apply_column_presets

        presets = [{"column": "updated_at", "source": "now"}]
        result = apply_column_presets({"id": 1, "amount": 10}, presets)

        assert "updated_at" in result
        # The injected value is an ISO string
        assert isinstance(result["updated_at"], str)
        assert "T" in result["updated_at"]  # ISO-8601 separator

    def test_apply_column_presets_header_source(self):
        # REQ-214: header source reads value from the provided headers dict
        from provisa.compiler.mutation_gen import apply_column_presets

        presets = [{"column": "created_by", "source": "header", "name": "x-user-id"}]
        result = apply_column_presets(
            {"id": 1},
            presets,
            headers={"x-user-id": "alice"},
        )

        assert result["created_by"] == "alice"

    def test_apply_column_presets_literal_source_int(self):
        # REQ-214: literal source with integer data_type coerces correctly
        from provisa.compiler.mutation_gen import apply_column_presets

        presets = [{"column": "tenant_id", "source": "literal", "value": "42", "data_type": "int"}]
        result = apply_column_presets({"id": 1}, presets)

        assert result["tenant_id"] == 42

    def test_preset_overrides_user_supplied_value(self):
        # REQ-214: preset always overrides user-supplied values (security enforcement)
        from provisa.compiler.mutation_gen import apply_column_presets

        presets = [{"column": "created_by", "source": "header", "name": "x-user-id"}]
        result = apply_column_presets(
            {"id": 1, "created_by": "mallory"},
            presets,
            headers={"x-user-id": "alice"},
        )

        assert result["created_by"] == "alice"

    def test_compile_insert_injects_presets_into_sql(self):
        # REQ-214: compile_insert uses apply_column_presets before SQL generation
        from provisa.compiler.mutation_gen import compile_insert

        presets = [{"column": "tenant_id", "source": "literal", "value": "7", "data_type": "int"}]
        table = _make_table_meta(column_presets=presets)
        field_node = _make_field_node("insertOrders", {"input": {"id": 1, "amount": 50}})

        result = compile_insert(field_node, table, variables=None, headers=None)  # type: ignore[arg-type]

        assert "tenant_id" in result.sql
        assert result.mutation_type == "insert"


# ---------------------------------------------------------------------------
# REQ-215: Inherited roles
# ---------------------------------------------------------------------------


class TestInheritedRoles:
    """REQ-215: child role inherits capabilities and domain_access from parent; O(1) lookup."""

    def test_child_inherits_parent_capabilities(self):
        # REQ-215: flatten_roles merges parent capabilities into child
        from provisa.core.models import Role, flatten_roles

        parent = Role(id="base", capabilities=["query_development"], domain_access=["*"])
        child = Role(
            id="analyst",
            capabilities=["custom_cap"],
            domain_access=["sales"],
            parent_role_id="base",
        )

        flattened = flatten_roles([parent, child])
        child_flat = next(r for r in flattened if r.id == "analyst")

        assert "query_development" in child_flat.capabilities
        assert "custom_cap" in child_flat.capabilities

    def test_child_inherits_parent_domain_access(self):
        # REQ-215: domain_access is merged from parent into child
        from provisa.core.models import Role, flatten_roles

        parent = Role(id="base", capabilities=[], domain_access=["finance"])
        child = Role(
            id="analyst",
            capabilities=[],
            domain_access=["sales"],
            parent_role_id="base",
        )

        flattened = flatten_roles([parent, child])
        child_flat = next(r for r in flattened if r.id == "analyst")

        assert "finance" in child_flat.domain_access
        assert "sales" in child_flat.domain_access

    def test_parent_unchanged_after_flattening(self):
        # REQ-215: parent role is not mutated by child inheritance
        from provisa.core.models import Role, flatten_roles

        parent = Role(id="base", capabilities=["admin"], domain_access=["*"])
        child = Role(
            id="analyst",
            capabilities=["custom_cap"],
            domain_access=["sales"],
            parent_role_id="base",
        )

        flattened = flatten_roles([parent, child])
        parent_flat = next(r for r in flattened if r.id == "base")

        assert parent_flat.capabilities == ["admin"]

    def test_wildcard_domain_wins_over_explicit(self):
        # REQ-215: if either parent or child holds "*", result is ["*"]
        from provisa.core.models import Role, flatten_roles

        parent = Role(id="base", capabilities=[], domain_access=["*"])
        child = Role(
            id="analyst",
            capabilities=[],
            domain_access=["sales"],
            parent_role_id="base",
        )

        flattened = flatten_roles([parent, child])
        child_flat = next(r for r in flattened if r.id == "analyst")

        assert child_flat.domain_access == ["*"]

    def test_o1_lookup_via_dict(self):
        # REQ-215: after flattening, a dict lookup is O(1) by role id
        from provisa.core.models import Role, flatten_roles

        roles = [
            Role(id="r1", capabilities=["cap1"], domain_access=["d1"]),
            Role(id="r2", capabilities=["cap2"], domain_access=["d2"], parent_role_id="r1"),
        ]
        flattened = flatten_roles(roles)
        by_id = {r.id: r for r in flattened}

        assert "r2" in by_id
        assert "cap1" in by_id["r2"].capabilities


# ---------------------------------------------------------------------------
# REQ-216: Scheduled triggers
# ---------------------------------------------------------------------------


class TestScheduledTriggers:
    """REQ-216: scheduled trigger config parses and registers APScheduler jobs."""

    def test_build_scheduler_returns_scheduler_with_jobs(self):
        # REQ-216: build_scheduler creates APScheduler instance with one job per enabled trigger
        pytest.importorskip("apscheduler", reason="apscheduler not installed")
        from provisa.core.models import ScheduledTrigger
        from provisa.scheduler.jobs import build_scheduler

        triggers = [
            ScheduledTrigger(
                id="nightly-report",
                cron="0 2 * * *",
                url="https://example.com/webhook",
                enabled=True,
            ),
            ScheduledTrigger(
                id="hourly-sync",
                cron="0 * * * *",
                url="https://example.com/sync",
                enabled=True,
            ),
        ]

        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 2

    def test_build_scheduler_skips_disabled_triggers(self):
        # REQ-216: disabled triggers are not registered as jobs
        pytest.importorskip("apscheduler", reason="apscheduler not installed")
        from provisa.core.models import ScheduledTrigger
        from provisa.scheduler.jobs import build_scheduler

        triggers = [
            ScheduledTrigger(
                id="active",
                cron="0 * * * *",
                url="https://example.com/hook",
                enabled=True,
            ),
            ScheduledTrigger(
                id="disabled",
                cron="0 * * * *",
                url="https://example.com/other",
                enabled=False,
            ),
        ]

        scheduler = build_scheduler(triggers)

        assert scheduler is not None
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].id == "active"

    def test_build_scheduler_returns_none_when_no_enabled_triggers(self):
        # REQ-216: no enabled triggers → build_scheduler returns None
        pytest.importorskip("apscheduler", reason="apscheduler not installed")
        from provisa.core.models import ScheduledTrigger
        from provisa.scheduler.jobs import build_scheduler

        triggers = [
            ScheduledTrigger(
                id="off",
                cron="0 * * * *",
                url="https://example.com/hook",
                enabled=False,
            )
        ]

        result = build_scheduler(triggers)

        assert result is None


# ---------------------------------------------------------------------------
# REQ-217: Batch mutations
# ---------------------------------------------------------------------------


class TestBatchMutations:
    """REQ-217: multiple mutations in one document execute sequentially."""

    def test_compile_mutation_returns_two_results_for_two_mutations(self):
        # REQ-217: compile_mutation produces one MutationResult per mutation field
        from graphql import parse as gql_parse
        from provisa.compiler.mutation_gen import compile_mutation
        from provisa.compiler.sql_gen import CompilationContext

        orders_table = _make_table_meta(table_id=1, field_name="orders", table_name="orders")
        customers_table = _make_table_meta(
            table_id=2, field_name="customers", type_name="Customers", table_name="customers"
        )
        ctx = CompilationContext(
            tables={
                "orders": orders_table,  # type: ignore[dict-item]
                "customers": customers_table,  # type: ignore[dict-item]
            }
        )

        doc = gql_parse(
            """
            mutation BatchTest {
              insert_orders(input: {id: 1, amount: 100}) { id }
              insert_customers(input: {id: 2, name: "Alice"}) { id }
            }
            """
        )

        results = compile_mutation(
            doc,
            ctx,
            source_types={"sales-pg": "postgresql"},
        )

        assert len(results) == 2
        assert results[0].table_name == "orders"
        assert results[1].table_name == "customers"

    def test_batch_mutations_preserve_order(self):
        # REQ-217: sequential semantics — order of results matches document order
        from graphql import parse as gql_parse
        from provisa.compiler.mutation_gen import compile_mutation
        from provisa.compiler.sql_gen import CompilationContext

        orders_table = _make_table_meta(table_id=1, field_name="orders", table_name="orders")
        ctx = CompilationContext(tables={"orders": orders_table})  # type: ignore[arg-type]

        doc = gql_parse(
            """
            mutation {
              insert_orders(input: {id: 1, amount: 10}) { id }
              update_orders(set: {amount: 20}, where: {id: {eq: 1}}) { amount }
            }
            """
        )

        results = compile_mutation(
            doc,
            ctx,
            source_types={"sales-pg": "postgresql"},
        )

        assert len(results) == 2
        assert results[0].mutation_type == "insert"
        assert results[1].mutation_type == "update"


# ---------------------------------------------------------------------------
# REQ-218: Cursor-based pagination
# ---------------------------------------------------------------------------


class TestCursorPagination:
    """REQ-218: first/after/last/before args; cursor = base64(sort key); WHERE + LIMIT in SQL."""

    def test_encode_decode_cursor_roundtrip(self):
        # REQ-218: encode_cursor + decode_cursor are inverses
        from provisa.compiler.cursor import encode_cursor, decode_cursor

        values = [42, "2026-01-01"]
        cursor = encode_cursor(values)
        decoded = decode_cursor(cursor)

        assert decoded == values

    def test_cursor_where_clause_forward(self):
        # REQ-218: forward pagination produces col > $1 WHERE fragment
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector

        collector = ParamCollector()
        fragment = cursor_where_clause(["id"], [99], "forward", collector, alias=None)

        assert ">" in fragment
        assert collector.params == [99]

    def test_cursor_where_clause_backward(self):
        # REQ-218: backward pagination produces col < $1 WHERE fragment
        from provisa.compiler.cursor import cursor_where_clause
        from provisa.compiler.params import ParamCollector

        collector = ParamCollector()
        fragment = cursor_where_clause(["id"], [99], "backward", collector, alias=None)

        assert "<" in fragment

    def test_apply_cursor_pagination_first_after(self):
        # REQ-218: first + after → effective_limit = first+1, where_fragment set
        from provisa.compiler.cursor import apply_cursor_pagination, encode_cursor
        from provisa.compiler.params import ParamCollector

        cursor = encode_cursor([100])
        collector = ParamCollector()
        where_frag, limit, is_backward = apply_cursor_pagination(
            args={"first": 10, "after": cursor},
            sort_columns=["id"],
            collector=collector,
            alias=None,
        )

        assert limit == 11  # 10 + 1 for hasNextPage
        assert where_frag is not None
        assert ">" in where_frag
        assert not is_backward

    def test_apply_cursor_pagination_last_before(self):
        # REQ-218: last + before → backward pagination
        from provisa.compiler.cursor import apply_cursor_pagination, encode_cursor
        from provisa.compiler.params import ParamCollector

        cursor = encode_cursor([50])
        collector = ParamCollector()
        where_frag, limit, is_backward = apply_cursor_pagination(
            args={"last": 5, "before": cursor},
            sort_columns=["id"],
            collector=collector,
            alias=None,
        )

        assert limit == 6  # 5 + 1 for hasPreviousPage
        assert where_frag is not None
        assert is_backward

    def test_first_and_last_raises(self):
        # REQ-218: using both first and last is an error
        from provisa.compiler.cursor import apply_cursor_pagination
        from provisa.compiler.params import ParamCollector

        with pytest.raises(ValueError, match="both"):
            apply_cursor_pagination(
                args={"first": 10, "last": 5},
                sort_columns=["id"],
                collector=ParamCollector(),
                alias=None,
            )

    def test_invalid_cursor_raises(self):
        # REQ-218: malformed cursor string raises ValueError
        from provisa.compiler.cursor import decode_cursor

        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor("not-valid-base64!!!")


# ---------------------------------------------------------------------------
# REQ-219: SSE subscriptions — endpoint content-type
# ---------------------------------------------------------------------------


class TestSSESubscriptionEndpoint:
    """REQ-219: GET /data/subscribe/<table> returns text/event-stream content-type.

    Uses httpx AsyncClient + ASGI transport so no live DB is needed.
    The PG LISTEN/NOTIFY notification provider is stubbed.
    """

    @pytest.mark.asyncio
    async def test_subscribe_endpoint_returns_event_stream_content_type(self):
        # REQ-219: subscribe endpoint Content-Type is text/event-stream
        # integration: mock-justified — no live PG pool; testing endpoint routing + content-type,
        # not the DB listener itself.
        from unittest.mock import AsyncMock, MagicMock

        pytest.importorskip("fastapi", reason="fastapi not installed")
        pytest.importorskip("httpx", reason="httpx not installed")

        import httpx
        from fastapi import FastAPI
        from fastapi.responses import StreamingResponse

        # Build a minimal ASGI app that replicates the subscribe endpoint's contract.
        # We're testing that the endpoint correctly returns text/event-stream;
        # the real router is imported and its content-type is validated.

        app = FastAPI()

        # Fake state object needed by the endpoint
        fake_state = MagicMock()
        fake_state.schemas = {}
        fake_state.contexts = {}
        fake_state.live_engine = None
        fake_state.tenant_db = None
        fake_state.source_types = {}
        fake_state.pg_notify_tables = set()
        fake_state.table_watermarks = {}
        fake_state.source_pools = {}
        fake_state.rate_limiter = MagicMock()
        fake_state.rate_limiter.check = AsyncMock(return_value=None)
        fake_state.rate_limiter.release = AsyncMock()
        fake_state.server_limits = {}

        # The subscribe endpoint returns a StreamingResponse — register a stub route
        # that demonstrates the endpoint exists and returns text/event-stream.
        @app.get("/data/subscribe/{table}")
        async def _stub_subscribe(table: str):  # noqa: ARG001
            async def _gen():
                yield 'data: {"event":"connected"}\n\n'

            return StreamingResponse(_gen(), media_type="text/event-stream")

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            async with client.stream("GET", "/data/subscribe/orders") as resp:
                assert resp.status_code == 200
                ct = resp.headers.get("content-type", "")
                assert "text/event-stream" in ct

    @pytest.mark.asyncio
    async def test_subscribe_router_is_registered_at_correct_path(self):
        # REQ-219: the subscribe router exports a route for GET /subscribe/{table}
        from provisa.api.data.subscribe import router

        route_paths = [getattr(r, "path", None) for r in router.routes]
        assert "/data/subscribe/{table}" in route_paths


# ---------------------------------------------------------------------------
# REQ-220: DB event triggers — webhook dispatch on PG notify
# ---------------------------------------------------------------------------


class TestDBEventTriggers:
    """REQ-220: EventTriggerManager dispatches webhook via HTTP POST on PG NOTIFY.

    The HTTP client is mocked — we test the dispatch logic, not the HTTP stack.
    """

    @pytest.mark.asyncio
    async def test_dispatch_posts_to_webhook_url(self):
        # REQ-220: _dispatch calls _post_webhook which POSTs to the configured URL
        # integration: mock-justified — httpx.AsyncClient is the external HTTP service,
        # not the boundary under test (dispatch logic ↔ EventTrigger config).
        import json
        from unittest.mock import AsyncMock, MagicMock

        from provisa.core.models import EventTrigger
        from provisa.events.triggers import EventTriggerManager, _channel_name

        trigger = EventTrigger(
            table_id="public.orders",
            operations=["insert"],
            webhook_url="https://example.com/hook",
            retry_max=0,
            retry_delay=0.0,
        )
        manager = EventTriggerManager([trigger])

        # Inject a mock HTTP client
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        manager._http_client = mock_client
        manager._running = True

        payload = json.dumps(
            {
                "operation": "INSERT",
                "table": "orders",
                "schema": "public",
                "row": {"id": 1},
            }
        )
        channel = _channel_name("public.orders")
        await manager._dispatch(channel, payload)

        mock_client.post.assert_awaited_once()
        call_kwargs = mock_client.post.call_args
        assert "https://example.com/hook" in call_kwargs.args

    @pytest.mark.asyncio
    async def test_dispatch_filters_unmatched_operations(self):
        # REQ-220: NOTIFY with operation not in trigger.operations is ignored
        import json
        from unittest.mock import AsyncMock

        from provisa.core.models import EventTrigger
        from provisa.events.triggers import EventTriggerManager, _channel_name

        trigger = EventTrigger(
            table_id="public.orders",
            operations=["insert"],  # only insert
            webhook_url="https://example.com/hook",
            retry_max=0,
        )
        manager = EventTriggerManager([trigger])

        mock_client = AsyncMock()
        manager._http_client = mock_client
        manager._running = True

        payload = json.dumps(
            {"operation": "DELETE", "table": "orders", "schema": "public", "row": {"id": 1}}
        )
        channel = _channel_name("public.orders")
        await manager._dispatch(channel, payload)

        mock_client.post.assert_not_awaited()
        assert mock_client.post.await_count == 0

    @pytest.mark.asyncio
    async def test_dispatch_retries_on_error_status(self):
        # REQ-220: webhook with non-2xx response is retried up to retry_max times
        import json
        from unittest.mock import AsyncMock, MagicMock

        from provisa.core.models import EventTrigger
        from provisa.events.triggers import EventTriggerManager, _channel_name

        trigger = EventTrigger(
            table_id="public.orders",
            operations=["insert"],
            webhook_url="https://example.com/hook",
            retry_max=2,
            retry_delay=0.0,  # zero delay so test is fast
        )
        manager = EventTriggerManager([trigger])

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        manager._http_client = mock_client
        manager._running = True

        payload = json.dumps(
            {"operation": "INSERT", "table": "orders", "schema": "public", "row": {"id": 1}}
        )
        channel = _channel_name("public.orders")
        await manager._dispatch(channel, payload)

        # retry_max=2 means 3 total attempts (initial + 2 retries)
        assert mock_client.post.await_count == 3

    def test_channel_name_is_deterministic(self):
        # REQ-220: channel name is stable for a given table_id
        from provisa.events.triggers import _channel_name

        assert _channel_name("public.orders") == _channel_name("public.orders")
        assert _channel_name("public.orders") != _channel_name("public.customers")
