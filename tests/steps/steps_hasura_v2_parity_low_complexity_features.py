# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for Hasura v2 parity low-complexity features.

REQ-212 — upsert mutations compile to ``INSERT ... ON CONFLICT ... DO UPDATE``.
REQ-213 — ``distinct_on`` query argument deduplicates results via ``DISTINCT ON``
          (PostgreSQL) or a window-function fallback (non-PostgreSQL dialects).
REQ-214 — column presets auto-set audit columns on insert/update from session
          variables (headers) or built-in functions (``now``), removing those
          columns from user input before SQL generation.
REQ-215 — inherited roles: a child role declares ``parent_role_id`` and inherits
          (merges up the chain) the parent's capabilities and domain_access. The
          hierarchy is flattened at startup into per-role dicts so authorization
          lookups remain O(1).
REQ-216 — scheduled triggers: time-based execution of registered webhooks or
          internal functions via APScheduler using cron expression syntax,
          configured per trigger in ``provisa.yaml``.
REQ-217 — batch mutations: multiple mutations in a single GraphQL request execute
          sequentially per the GraphQL specification (mutation fields are resolved
          serially in selection-set order).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from graphql import (
    FieldNode,
    OperationDefinitionNode,
    parse,
)
from pytest_bdd import given, scenario, then, when

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.mutation_gen import (
    MutationResult,
    apply_column_presets,
    compile_upsert,
)
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import CompilationContext, TableMeta, build_context, compile_query
from provisa.core.models import Role, ScheduledTrigger, flatten_roles


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict for passing state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table_meta(source_type: str = "postgresql") -> TableMeta:
    """Build a TableMeta for a simple orders table with an `id` primary key."""
    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
    )


def _make_field_node(name: str, args: dict) -> FieldNode:
    """Build a minimal graphql-core FieldNode for the given args dict."""

    def _render_value(v: object) -> str:
        if isinstance(v, dict):
            pairs = ", ".join(f"{k}: {_render_value(val)}" for k, val in v.items())
            return "{" + pairs + "}"
        if isinstance(v, list):
            items = ", ".join(_render_value(i) for i in v)
            return "[" + items + "]"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, str):
            return f'"{v}"'
        return str(v)

    args_str = ", ".join(f"{k}: {_render_value(v)}" for k, v in args.items())
    gql_args = f"({args_str})" if args_str else ""
    doc = parse(f"mutation {{ {name}{gql_args} {{ id }} }}")
    op = doc.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    field = op.selection_set.selections[0]
    assert isinstance(field, FieldNode)
    return field


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_input(source_type: str = "postgresql") -> SchemaInput:
    """Build a SchemaInput for an orders table backed by the given source type."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
            ],
        },
    ]
    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
        ],
    }
    return SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": source_type},
    )


def _build_batch_schema_and_ctx() -> tuple:
    """Build a SchemaInput/schema/ctx covering orders and customers tables."""
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "region", "visible_to": ["admin"]},
                {"column_name": "status", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]
    col_types = {
        1: [
            _col("id", "integer"),
            _col("amount", "decimal(10,2)"),
            _col("region", "varchar(50)"),
            _col("status", "varchar(20)"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
            _col("email", "varchar(255)"),
        ],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=col_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


def _query_field_node(query: str) -> tuple:
    """Parse a GraphQL query string and return (DocumentNode, FieldNode)."""
    doc = parse(query)
    op = doc.definitions[0]
    assert isinstance(op, OperationDefinitionNode)
    field = op.selection_set.selections[0]
    assert isinstance(field, FieldNode)
    return doc, field


def _run_compile_query(doc_and_field, ctx: CompilationContext, table: TableMeta) -> object:
    """Invoke compile_query(document, ctx) and return the first CompiledQuery."""
    if isinstance(doc_and_field, tuple):
        document, _field = doc_and_field
    else:
        raise TypeError(f"_run_compile_query expects (doc, field) tuple, got {type(doc_and_field)}")
    results = compile_query(document, ctx)
    assert results, "compile_query produced no results"
    return results[0]


# ---------------------------------------------------------------------------
# REQ-215 helpers — role hierarchy flattening
# ---------------------------------------------------------------------------


def _flatten_roles_from_dicts(role_definitions: list[dict]) -> dict[str, dict]:
    """Flatten a role hierarchy into per-role dicts with merged capabilities/domain_access."""
    by_id: dict[str, dict] = {r["id"]: r for r in role_definitions}
    resolved: dict[str, dict] = {}

    def _resolve(role_id: str, visiting: set[str]) -> dict:
        if role_id in resolved:
            return resolved[role_id]
        if role_id in visiting:
            raise ValueError(f"Cycle detected in role hierarchy at role '{role_id}'")
        visiting = visiting | {role_id}
        role = by_id[role_id]
        caps: set[str] = set(role.get("capabilities") or [])
        domains: set[str] = set(role.get("domain_access") or [])
        parent_id = role.get("parent_role_id")
        if parent_id:
            parent_resolved = _resolve(parent_id, visiting)
            caps |= set(parent_resolved["capabilities"])
            domains |= set(parent_resolved["domain_access"])
        merged = {
            "id": role_id,
            "capabilities": sorted(caps),
            "domain_access": sorted(domains),
        }
        resolved[role_id] = merged
        return merged

    for role in role_definitions:
        _resolve(role["id"], set())

    return resolved


def _role(
    role_id: str,
    caps: list[str],
    domains: list[str],
    parent: str | None = None,
) -> Role:
    """Construct a Role model instance."""
    return Role(
        id=role_id,
        capabilities=caps,
        domain_access=domains,
        parent_role_id=parent,
    )


def _make_scheduled_trigger(**kwargs: object) -> ScheduledTrigger:
    """Build a ScheduledTrigger with sensible defaults."""
    defaults: dict[str, object] = dict(
        id="trigger-1",
        cron="* * * * *",
        url="https://example.com/hook",
        enabled=True,
    )
    defaults.update(kwargs)
    return ScheduledTrigger(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# REQ-212 scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-212.feature",
    "REQ-212 default behaviour",
)
def test_req_212_default_behaviour() -> None:
    """Bind the REQ-212 upsert mutations scenario."""


@given("a GraphQL upsert_<table> mutation request")
def _given_upsert_mutation_request(shared_data: dict) -> None:
    """Construct a GraphQL upsert_orders mutation field node and table metadata."""
    table = _make_table_meta()
    field_node = _make_field_node(
        "upsertOrders",
        {"input": {"id": 1, "amount": 99}, "on_conflict": ["id"]},
    )
    shared_data["table"] = table
    shared_data["field_node"] = field_node


@when("the compiler processes it")
def _when_compiler_processes_upsert(shared_data: dict) -> None:
    """Pass the field node and table metadata through ``compile_upsert``."""
    if "preset_field_node" in shared_data:
        _when_insert_or_update_mutation_executed(shared_data)
        return

    if "pg_field_node" in shared_data:
        pg_result = _run_compile_query(
            shared_data["pg_field_node"],
            shared_data["pg_ctx"],
            shared_data["pg_table"],
        )
        trino_result = _run_compile_query(
            shared_data["trino_field_node"],
            shared_data["trino_ctx"],
            shared_data["trino_table"],
        )
        shared_data["pg_result"] = pg_result
        shared_data["trino_result"] = trino_result
        return

    field_node: FieldNode = shared_data["field_node"]
    table: TableMeta = shared_data["table"]
    result: MutationResult = compile_upsert(field_node, table, variables=None)  # type: ignore[arg-type]
    shared_data["result"] = result


@then(
    "INSERT ... ON CONFLICT ... DO UPDATE SQL is generated with conflict columns from primary key metadata"
)
def _then_on_conflict_sql_generated(shared_data: dict) -> None:
    """Assert that the compiled SQL contains the expected upsert clauses."""
    result: MutationResult = shared_data["result"]

    assert result.mutation_type == "upsert", (
        f"Expected mutation_type='upsert', got {result.mutation_type!r}"
    )

    sql_upper = result.sql.upper()
    assert "ON CONFLICT" in sql_upper, (
        f"Expected 'ON CONFLICT' in generated SQL, got:\n{result.sql}"
    )
    assert "DO UPDATE" in sql_upper, f"Expected 'DO UPDATE' in generated SQL, got:\n{result.sql}"

    assert "id" in result.sql.lower(), (
        f"Expected conflict column 'id' to appear in generated SQL, got:\n{result.sql}"
    )


# ---------------------------------------------------------------------------
# REQ-213 scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-213.feature",
    "REQ-213 default behaviour",
)
def test_req_213_default_behaviour() -> None:
    """Bind the REQ-213 DISTINCT ON scenario."""


@given("a GraphQL query with a distinct_on argument specifying columns")
def _given_distinct_on_query(shared_data: dict) -> None:
    """Build two GraphQL query field nodes containing a distinct_on argument."""
    pg_si = _build_schema_input(source_type="postgresql")
    pg_ctx = build_context(pg_si)

    trino_si = _build_schema_input(source_type="trino")
    trino_ctx = build_context(trino_si)

    pg_field_node = _query_field_node(
        "query { orders(distinct_on: [region]) { id amount region } }"
    )
    trino_field_node = _query_field_node(
        "query { orders(distinct_on: [region]) { id amount region } }"
    )

    pg_table = _make_table_meta(source_type="postgresql")
    trino_table = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
    )

    shared_data["pg_field_node"] = pg_field_node
    shared_data["pg_ctx"] = pg_ctx
    shared_data["pg_table"] = pg_table

    shared_data["trino_field_node"] = trino_field_node
    shared_data["trino_ctx"] = trino_ctx
    shared_data["trino_table"] = trino_table

    shared_data["distinct_columns"] = ["region"]


@then(
    "deduplicated results are returned using DISTINCT ON or a window function fallback for non-PostgreSQL dialects"
)
def _then_distinct_on_or_window_fallback(shared_data: dict) -> None:
    """Assert correct deduplication SQL for PostgreSQL and non-PostgreSQL dialects."""
    if "pg_result" not in shared_data and "pg_field_node" in shared_data:
        pg_result = _run_compile_query(
            shared_data["pg_field_node"],
            shared_data["pg_ctx"],
            shared_data["pg_table"],
        )
        trino_result = _run_compile_query(
            shared_data["trino_field_node"],
            shared_data["trino_ctx"],
            shared_data["trino_table"],
        )
        shared_data["pg_result"] = pg_result
        shared_data["trino_result"] = trino_result

    pg_result = shared_data.get("pg_result")
    trino_result = shared_data.get("trino_result")

    def _sql(result: object) -> str:
        if isinstance(result, str):
            return result
        if hasattr(result, "sql"):
            return str(getattr(result, "sql"))
        if hasattr(result, "query"):
            return str(getattr(result, "query"))
        return str(result)

    assert pg_result is not None, "PostgreSQL compile_query result must not be None"
    assert trino_result is not None, "Trino compile_query result must not be None"

    pg_sql = _sql(pg_result)
    pg_sql_upper = pg_sql.upper()

    pg_dedup_present = (
        "DISTINCT ON" in pg_sql_upper
        or "DISTINCT" in pg_sql_upper
        or "ROW_NUMBER" in pg_sql_upper
        or "QUALIFY" in pg_sql_upper
    )
    assert pg_dedup_present, (
        f"Expected PostgreSQL SQL to contain DISTINCT ON, DISTINCT, ROW_NUMBER, or QUALIFY "
        f"for deduplication. Got:\n{pg_sql}"
    )

    assert "region" in pg_sql.lower(), (
        f"Expected 'region' column to appear in PostgreSQL SQL for distinct_on=[region]. "
        f"Got:\n{pg_sql}"
    )

    trino_sql = _sql(trino_result)
    trino_sql_upper = trino_sql.upper()

    trino_dedup_present = (
        "ROW_NUMBER" in trino_sql_upper
        or "DISTINCT" in trino_sql_upper
        or "QUALIFY" in trino_sql_upper
    )
    assert trino_dedup_present, (
        f"Expected Trino SQL to contain ROW_NUMBER, DISTINCT, or QUALIFY "
        f"as a DISTINCT ON fallback. Got:\n{trino_sql}"
    )

    assert "region" in trino_sql.lower(), (
        f"Expected 'region' column to appear in Trino SQL for distinct_on=[region]. "
        f"Got:\n{trino_sql}"
    )

    if "DISTINCT ON" in pg_sql_upper:
        assert "DISTINCT ON" not in trino_sql_upper, (
            "Trino SQL must not use DISTINCT ON syntax — it must use a window "
            f"function or plain DISTINCT fallback. Got:\n{trino_sql}"
        )


# ---------------------------------------------------------------------------
# REQ-214 scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-214.feature",
    "REQ-214 default behaviour",
)
def test_req_214_default_behaviour() -> None:
    """Bind the REQ-214 column presets scenario."""


@given("a table config with column_presets for created_by and updated_at")
def _given_table_config_with_column_presets(shared_data: dict) -> None:
    """Set up a TableMeta with column_presets for created_by (header) and updated_at (now)."""
    column_presets = [
        {
            "column": "created_by",
            "source": "header",
            "name": "x_user_id",
            "value": None,
        },
        {
            "column": "updated_at",
            "source": "now",
            "name": None,
            "value": None,
        },
    ]

    user_input = {
        "created_by": "attacker-supplied-value",
        "updated_at": "1970-01-01T00:00:00",
        "title": "My Record",
        "amount": 42,
    }

    request_headers = {
        "x_user_id": "user-abc-123",
        "Authorization": "Bearer sometoken",
    }

    table = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name="orders",
        column_presets=column_presets,
    )

    preset_field_node = _make_field_node(
        "insertOrders",
        {
            "input": {
                "created_by": "attacker-supplied-value",
                "updated_at": "1970-01-01T00:00:00",
                "title": "My Record",
                "amount": 42,
            }
        },
    )

    shared_data["column_presets"] = column_presets
    shared_data["user_input"] = user_input
    shared_data["request_headers"] = request_headers
    shared_data["table"] = table
    shared_data["preset_field_node"] = preset_field_node


@when("an insert or update mutation is executed")
def _when_insert_or_update_mutation_executed(shared_data: dict) -> None:
    """Invoke apply_column_presets to process the user input through the column presets."""
    user_input: dict = dict(shared_data["user_input"])
    column_presets: list[dict] = shared_data["column_presets"]
    request_headers: dict = shared_data["request_headers"]

    shared_data["before_apply"] = datetime.now(timezone.utc)

    result = apply_column_presets(user_input, column_presets, headers=request_headers)

    shared_data["after_apply"] = datetime.now(timezone.utc)
    shared_data["preset_result"] = result
    shared_data["original_user_input"] = shared_data["user_input"]


@then(
    "preset columns are removed from user input and injected with session variable or built-in function values before SQL generation"
)
def _then_preset_columns_injected(shared_data: dict) -> None:
    """Assert that apply_column_presets correctly processes all configured presets."""
    result: dict = shared_data["preset_result"]
    original_input: dict = shared_data["original_user_input"]
    before_apply: datetime = shared_data["before_apply"]
    after_apply: datetime = shared_data["after_apply"]

    assert isinstance(result, dict), f"apply_column_presets must return a dict; got {type(result)}"

    assert "created_by" in result, (
        "created_by column must be present in the result after preset injection"
    )
    assert result["created_by"] != "attacker-supplied-value", (
        "created_by must NOT contain the client-supplied attacker value; "
        f"got {result['created_by']!r}"
    )
    assert result["created_by"] == "user-abc-123", (
        f"created_by must be set to the x_user_id header value 'user-abc-123'; "
        f"got {result['created_by']!r}"
    )

    assert "updated_at" in result, (
        "updated_at column must be present in the result after preset injection"
    )
    assert result["updated_at"] != "1970-01-01T00:00:00", (
        f"updated_at must NOT contain the client-supplied epoch value; got {result['updated_at']!r}"
    )

    try:
        parsed_updated_at = datetime.fromisoformat(result["updated_at"])
    except (ValueError, TypeError) as exc:
        raise AssertionError(
            f"updated_at must be a valid ISO-format datetime string; "
            f"got {result['updated_at']!r}: {exc}"
        ) from exc

    if parsed_updated_at.tzinfo is None:
        parsed_updated_at = parsed_updated_at.replace(tzinfo=timezone.utc)

    before_utc = (
        before_apply.replace(tzinfo=timezone.utc) if before_apply.tzinfo is None else before_apply
    )
    after_utc = (
        after_apply.replace(tzinfo=timezone.utc) if after_apply.tzinfo is None else after_apply
    )

    assert before_utc <= parsed_updated_at <= after_utc, (
        f"updated_at must be within the window [{before_utc.isoformat()}, "
        f"{after_utc.isoformat()}]; got {parsed_updated_at.isoformat()}"
    )

    assert result.get("title") == original_input["title"], (
        f"Non-preset column 'title' must pass through unchanged; "
        f"expected {original_input['title']!r}, got {result.get('title')!r}"
    )
    assert result.get("amount") == original_input["amount"], (
        f"Non-preset column 'amount' must pass through unchanged; "
        f"expected {original_input['amount']!r}, got {result.get('amount')!r}"
    )


# ---------------------------------------------------------------------------
# REQ-215 scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-215.feature",
    "REQ-215 default behaviour",
)
def test_req_215_default_behaviour() -> None:
    """Bind the REQ-215 inherited roles scenario."""


@given("roles configured with parent_role_id forming a hierarchy")
def _given_roles_with_parent_role_id(shared_data: dict) -> None:
    """Configure a three-level role hierarchy using parent_role_id."""
    role_dicts = [
        {
            "id": "base_viewer",
            "capabilities": ["read"],
            "domain_access": ["public"],
            "parent_role_id": None,
        },
        {
            "id": "analyst",
            "capabilities": ["aggregate"],
            "domain_access": ["analytics"],
            "parent_role_id": "base_viewer",
        },
        {
            "id": "senior_analyst",
            "capabilities": ["export"],
            "domain_access": ["finance"],
            "parent_role_id": "analyst",
        },
    ]

    role_models = [
        _role("base_viewer", caps=["read"], domains=["public"], parent=None),
        _role("analyst", caps=["aggregate"], domains=["analytics"], parent="base_viewer"),
        _role("senior_analyst", caps=["export"], domains=["finance"], parent="analyst"),
    ]

    shared_data["role_dicts"] = role_dicts
    shared_data["role_models"] = role_models


@when("the system starts up")
def _when_system_starts_up(shared_data: dict) -> None:
    """Simulate the startup flattening step by calling flatten_roles."""
    role_models: list[Role] = shared_data["role_models"]
    role_dicts: list[dict] = shared_data["role_dicts"]

    flattened_models: dict[str, Role] = {r.id: r for r in flatten_roles(role_models)}
    shared_data["flattened_models"] = flattened_models

    flattened_dicts: dict[str, dict] = _flatten_roles_from_dicts(role_dicts)
    shared_data["flattened_dicts"] = flattened_dicts


@then("capabilities and domain_access are flattened up the chain so lookups remain O(1)")
def _then_capabilities_and_domain_access_flattened(shared_data: dict) -> None:
    """Assert that flattening produced correct merged capabilities and domain_access."""
    flattened_models: dict[str, Role] = shared_data["flattened_models"]
    flattened_dicts: dict[str, dict] = shared_data["flattened_dicts"]

    expected_role_ids = {"base_viewer", "analyst", "senior_analyst"}

    assert set(flattened_models.keys()) >= expected_role_ids, (
        f"flatten_roles (model path) is missing role ids. "
        f"Expected at least {expected_role_ids}, got {set(flattened_models.keys())}"
    )
    assert set(flattened_dicts.keys()) >= expected_role_ids, (
        f"flatten_roles (dict path) is missing role ids. "
        f"Expected at least {expected_role_ids}, got {set(flattened_dicts.keys())}"
    )

    def _caps(entry: object) -> set[str]:
        if isinstance(entry, dict):
            return set(entry.get("capabilities") or [])
        return set(getattr(entry, "capabilities", None) or [])

    def _domains(entry: object) -> set[str]:
        if isinstance(entry, dict):
            return set(entry.get("domain_access") or [])
        return set(getattr(entry, "domain_access", None) or [])

    bv_model = flattened_models["base_viewer"]
    bv_dict = flattened_dicts["base_viewer"]

    assert _caps(bv_model) == {"read"}, (
        f"base_viewer (model) capabilities must be {{'read'}}; got {_caps(bv_model)}"
    )
    assert _domains(bv_model) == {"public"}, (
        f"base_viewer (model) domain_access must be {{'public'}}; got {_domains(bv_model)}"
    )
    assert _caps(bv_dict) == {"read"}, (
        f"base_viewer (dict) capabilities must be {{'read'}}; got {_caps(bv_dict)}"
    )
    assert _domains(bv_dict) == {"public"}, (
        f"base_viewer (dict) domain_access must be {{'public'}}; got {_domains(bv_dict)}"
    )

    an_model = flattened_models["analyst"]
    an_dict = flattened_dicts["analyst"]

    assert _caps(an_model) >= {"aggregate", "read"}, (
        f"analyst (model) capabilities must include {{'aggregate','read'}}; got {_caps(an_model)}"
    )
    assert _domains(an_model) >= {"analytics", "public"}, (
        f"analyst (model) domain_access must include {{'analytics','public'}}; "
        f"got {_domains(an_model)}"
    )
    assert _caps(an_dict) >= {"aggregate", "read"}, (
        f"analyst (dict) capabilities must include {{'aggregate','read'}}; got {_caps(an_dict)}"
    )
    assert _domains(an_dict) >= {"analytics", "public"}, (
        f"analyst (dict) domain_access must include {{'analytics','public'}}; "
        f"got {_domains(an_dict)}"
    )

    sa_model = flattened_models["senior_analyst"]
    sa_dict = flattened_dicts["senior_analyst"]

    assert _caps(sa_model) >= {"export", "aggregate", "read"}, (
        f"senior_analyst (model) capabilities must include the full inherited chain "
        f"{{'export','aggregate','read'}}; got {_caps(sa_model)}"
    )
    assert _domains(sa_model) >= {"finance", "analytics", "public"}, (
        f"senior_analyst (model) domain_access must include the full inherited chain "
        f"{{'finance','analytics','public'}}; got {_domains(sa_model)}"
    )
    assert _caps(sa_dict) >= {"export", "aggregate", "read"}, (
        f"senior_analyst (dict) capabilities must include the full inherited chain "
        f"{{'export','aggregate','read'}}; got {_caps(sa_dict)}"
    )
    assert _domains(sa_dict) >= {"finance", "analytics", "public"}, (
        f"senior_analyst (dict) domain_access must include the full inherited chain "
        f"{{'finance','analytics','public'}}; got {_domains(sa_dict)}"
    )
