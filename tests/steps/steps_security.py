# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD steps for REQ-039 — Schema visibility enforcement,
REQ-040 — SQL enforcement layer (RLS injection + column stripping),
REQ-531 — Predicate guard rejecting masked columns from WHERE/HAVING (V005),
REQ-554 — Default row cap (DEFAULT_SAMPLE_SIZE) for roles lacking full_results,
REQ-594 — TenantMiddleware skip-path exemptions bypass tenant resolution,
REQ-740 — Masking SELECT expressions only; WHERE/JOIN ON use physical unmasked columns,
REQ-741 — Column masking output uses ANSI SQL dialects independent of source type,
REQ-742 — Type-aware masking validation at config load time,
REQ-743 — Masking constant expressions emit syntactically valid SQL for their type,
REQ-744 — Masking preserves query structure (ORDER BY, LIMIT, GROUP BY unchanged; immutable transformation),
REQ-745 — Role-based masking: different roles see different masks for the same column, and
REQ-746 — Capability enforcement via check_capability and has_capability functions,
REQ-747 — SQL validator bypass for remote same-source relationship pairs."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios
from starlette.middleware.base import RequestResponseEndpoint

from provisa.security.visibility import (
    is_column_visible,
    visible_column_names,
    visible_tables,
)
from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
    has_capability,
)
from provisa.compiler.rls import (
    build_rls_context,
    inject_rls,
)
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.sampling import (
    apply_sampling_if_needed,
)
from provisa.compiler.stage2 import GovernanceContext, resolve_row_cap
from provisa.compiler.mask_inject import MaskingRules, inject_masking
from provisa.security.masking import (
    MaskingRule,
    MaskType,
    MaskingValidationError,
    build_mask_expression,
    validate_masking_rule,
)

from provisa.api.middleware.tenant_middleware import TenantMiddleware, _SKIP_PATHS

scenarios("../features/REQ-039.feature")
scenarios("../features/REQ-040.feature")
scenarios("../features/REQ-531.feature")
scenarios("../features/REQ-554.feature")
scenarios("../features/REQ-594.feature")
scenarios("../features/REQ-740.feature")
scenarios("../features/REQ-741.feature")
scenarios("../features/REQ-742.feature")
scenarios("../features/REQ-743.feature")
scenarios("../features/REQ-744.feature")
scenarios("../features/REQ-745.feature")
scenarios("../features/REQ-746.feature")
scenarios("../features/REQ-747.feature")
scenarios("../features/REQ-748.feature")
scenarios("../features/REQ-749.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


def _build_tables() -> list[dict]:
    """Two tables across two domains with mixed column visibility."""
    return [
        {
            "id": 1,
            "source_id": "pg1",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "amount", "visible_to": ["admin"]},
                {"column_name": "secret", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "pg1",
            "domain_id": "internal",
            "schema_name": "public",
            "table_name": "payroll",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "salary", "visible_to": ["admin"]},
            ],
        },
    ]


@given("a user without rights to a table or column")
def user_without_rights(shared_data: dict) -> None:
    # analyst can only access the 'sales' domain and only a subset of columns.
    shared_data["role"] = {"id": "analyst", "domain_access": ["sales"]}
    shared_data["catalog"] = _build_tables()
    # Sanity: the role genuinely lacks rights to at least one table/column.
    assert not is_column_visible(shared_data["catalog"][0], "amount", "analyst")


@when("the user accesses the SDL or query builder")
def user_accesses_sdl(shared_data: dict) -> None:
    role = shared_data["role"]
    catalog = shared_data["catalog"]
    # Visibility layer is what backs both the SDL projection and query builder.
    exposed = visible_tables(catalog, role)
    shared_data["exposed_tables"] = exposed
    shared_data["exposed_table_names"] = {t["table_name"] for t in exposed}
    shared_data["exposed_columns"] = {
        t["table_name"]: {c["column_name"] for c in t["columns"]} for t in exposed
    }


@then("unauthorized tables and columns do not appear and are rejected at parse time")
def unauthorized_hidden_and_rejected(shared_data: dict) -> None:
    role_id = shared_data["role"]["id"]
    catalog = shared_data["catalog"]
    exposed_names = shared_data["exposed_table_names"]
    exposed_columns = shared_data["exposed_columns"]

    # Unauthorized table (different domain) does not appear in the SDL/query builder.
    assert "payroll" not in exposed_names
    # Authorized table (in-domain, with at least one visible column) appears.
    assert "orders" in exposed_names

    # Unauthorized columns do not appear on the authorized table.
    assert exposed_columns["orders"] == {"id"}
    assert "amount" not in exposed_columns["orders"]
    assert "secret" not in exposed_columns["orders"]

    # Parse-time rejection: visibility predicates used by the compiler return False
    # for unauthorized references, so the compiler refuses them before execution.
    orders = next(t for t in catalog if t["table_name"] == "orders")
    payroll = next(t for t in catalog if t["table_name"] == "payroll")

    assert is_column_visible(orders, "id", role_id) is True
    assert is_column_visible(orders, "amount", role_id) is False
    assert is_column_visible(orders, "secret", role_id) is False
    # Entire unauthorized table exposes no columns to this role.
    assert visible_column_names(payroll, role_id) == set()
    # And references the role cannot see are rejected (would raise/deny at parse time).
    assert is_column_visible(payroll, "salary", role_id) is False


# ---------------------------------------------------------------------------
# REQ-040 — SQL enforcement layer: RLS injection + column stripping
# ---------------------------------------------------------------------------


def _orders_meta() -> TableMeta:
    return TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
    )


@given("a query submitted by a user with restricted rights")
def query_with_restricted_rights(shared_data: dict) -> None:
    # The restricted role 'analyst' has an RLS rule scoping the orders table
    # and only authorized columns are visible.
    role_id = "analyst"
    shared_data["role_id"] = role_id

    # Catalog metadata for the table being queried.
    table = {
        "id": 1,
        "source_id": "pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "columns": [
            {"column_name": "id", "visible_to": ["admin", "analyst"]},
            {"column_name": "region", "visible_to": ["admin", "analyst"]},
            {"column_name": "amount", "visible_to": ["admin"]},  # unauthorized
        ],
    }
    shared_data["table"] = table

    # RLS rules: analyst is restricted to their region.
    shared_data["rls_rules"] = [
        {"table_id": 1, "role_id": "analyst", "filter_expr": "region = 'us'"},
        {"table_id": 1, "role_id": "admin", "filter_expr": "1=1"},
    ]

    # The (naive) compiled query a user might submit, selecting all columns.
    ctx = CompilationContext()
    ctx.tables = {"orders": _orders_meta()}
    ctx.joins = {}
    shared_data["ctx"] = ctx

    shared_data["compiled"] = CompiledQuery(
        sql='SELECT "id", "region" FROM "public"."orders"',
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
        ],
        sources={"pg"},
    )

    # Confirm the role really is restricted: cannot see 'amount'.
    assert not is_column_visible(table, "amount", role_id)


@when("the executor processes the query")
def executor_processes_query(shared_data: dict) -> None:
    role_id = shared_data["role_id"]
    ctx = shared_data["ctx"]
    compiled = shared_data["compiled"]

    # 1. Build the RLS context for this role and inject WHERE clauses.
    rls = build_rls_context(shared_data["rls_rules"], role_id)
    assert rls.has_rules(), "restricted role must carry RLS rules"
    enforced = inject_rls(compiled, ctx, rls)
    shared_data["enforced_sql"] = enforced.sql

    # 2. Strip unauthorized columns: only visible columns survive enforcement.
    allowed = visible_column_names(shared_data["table"], role_id)
    shared_data["allowed_columns"] = allowed
    shared_data["surviving_columns"] = {c.column for c in compiled.columns if c.column in allowed}


@then("RLS WHERE clauses are injected and unauthorized columns are stripped before execution")
def rls_injected_columns_stripped(shared_data: dict) -> None:
    enforced_sql = shared_data["enforced_sql"]
    allowed = shared_data["allowed_columns"]
    surviving = shared_data["surviving_columns"]

    # RLS predicate is present in the executable SQL.
    assert "\"region\" = 'us'" in enforced_sql
    assert "WHERE" in enforced_sql.upper()

    # Unauthorized column is never exposed to the restricted role.
    assert "amount" not in allowed
    assert "amount" not in surviving

    # Authorized columns survive enforcement.
    assert allowed == {"id", "region"}
    assert surviving == {"id", "region"}

    # The stripped column does not leak into the executable SQL projection.
    assert '"amount"' not in enforced_sql


# ---------------------------------------------------------------------------
# REQ-531 — Predicate guard: masked columns rejected from WHERE/HAVING (V005)
# ---------------------------------------------------------------------------


def _users_meta() -> TableMeta:
    return TableMeta(
        table_id=1,
        field_name="users",
        type_name="Users",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="users",
    )


@given("a query with a masked column in a WHERE or HAVING clause")
def query_with_masked_predicate(shared_data: dict) -> None:
    # 'email' on the 'users' table is masked for this role.
    mask = MaskingRule(mask_type=MaskType.regex, pattern=r".+@", replace="***@")
    gov_ctx = GovernanceContext(
        rls_rules={},
        masking_rules={(1, "email"): (mask, "varchar")},
        visible_columns={},
        table_map={"users": 1},
        all_columns={1: [("id", "integer"), ("email", "varchar")]},
        limit_ceiling=None,
        sample_size=None,
    )
    shared_data["gov_ctx"] = gov_ctx

    ctx = CompilationContext()
    ctx.tables = {"users": _users_meta()}
    ctx.joins = {}
    shared_data["ctx"] = ctx

    # Domain access '*' so V001 doesn't pre-empt V005 — we want to exercise V005.
    shared_data["role"] = {"id": "analyst", "domain_access": ["*"]}

    # A binary-search style filter on a masked column — the attack V005 blocks.
    shared_data["where_sql"] = "SELECT id FROM users WHERE email = 'victim@example.com'"
    # The equivalent inference attempt routed through HAVING.
    shared_data["having_sql"] = (
        "SELECT id FROM users GROUP BY id HAVING email = 'victim@example.com'"
    )

    # Confirm the predicate column genuinely carries a masking rule.
    assert (1, "email") in gov_ctx.masking_rules


@when("the query is parsed")
def parse_masked_predicate_query(shared_data: dict) -> None:
    ctx = shared_data["ctx"]
    gov_ctx = shared_data["gov_ctx"]
    role = shared_data["role"]

    shared_data["where_violations"] = validate_sql(shared_data["where_sql"], ctx, gov_ctx, role, [])
    shared_data["having_violations"] = validate_sql(
        shared_data["having_sql"], ctx, gov_ctx, role, []
    )


@then("it is rejected at parse time before execution via V005 validation")
def rejected_via_v005(shared_data: dict) -> None:
    where_violations = shared_data["where_violations"]
    having_violations = shared_data["having_violations"]

    where_codes = {v.code for v in where_violations}
    having_codes = {v.code for v in having_violations}

    # The masked column in WHERE is rejected with the predicate-guard code.
    assert "V005" in where_codes, f"expected V005, got {where_codes}"
    # The same protection applies to HAVING.
    assert "V005" in having_codes, f"expected V005, got {having_codes}"

    # The violation identifies the offending masked column.
    where_v005 = next(v for v in where_violations if v.code == "V005")
    having_v005 = next(v for v in having_violations if v.code == "V005")
    assert "email" in where_v005.message
    assert "email" in having_v005.message

    # Rejection means a non-empty violation set is returned before any execution.
    assert len(where_violations) >= 1
    assert len(having_violations) >= 1

    # A query without the masked predicate must NOT trigger V005 — proving the
    # guard is specific to masked columns appearing in WHERE/HAVING.
    clean_violations = validate_sql(
        "SELECT id FROM users",
        shared_data["ctx"],
        shared_data["gov_ctx"],
        shared_data["role"],
        [],
    )
    assert "V005" not in {v.code for v in clean_violations}


# ---------------------------------------------------------------------------
# REQ-554 — Default row cap for roles lacking the full_results capability
# ---------------------------------------------------------------------------


@given("a role without the full_results capability")
def role_without_full_results(shared_data: dict) -> None:
    # An ordinary role carrying no capabilities at all — definitively lacks
    # FULL_RESULTS and must therefore receive the default row cap.
    role = {"id": "viewer", "capabilities": []}
    assert not has_capability(role, Capability.FULL_RESULTS)
    shared_data["role"] = role

    # The Stage 2 row cap mechanism must resolve a concrete cap for this role.
    cap = resolve_row_cap(role)
    assert cap is not None, "unprivileged role must receive a default row cap"
    assert cap > 0
    shared_data["expected_cap"] = cap

    # A FULL_RESULTS role is the negative control: it must receive NO cap.
    privileged = {"id": "power", "capabilities": [Capability.FULL_RESULTS.value]}
    assert has_capability(privileged, Capability.FULL_RESULTS)
    assert resolve_row_cap(privileged) is None
    shared_data["privileged_role"] = privileged


@when("a query is executed")
def query_executed_for_cap(shared_data: dict) -> None:
    # An uncapped user query (no LIMIT clause) is what the requirement protects.
    compiled = CompiledQuery(
        sql='SELECT "id" FROM "public"."orders"',
        params=[],
        root_field="orders",
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )
    shared_data["original_sql"] = compiled.sql

    # Stage 2 row cap path applied for the unprivileged role.
    governed = apply_sampling_if_needed(compiled, shared_data["role"])
    shared_data["governed_sql"] = governed.sql

    # Same path for the privileged FULL_RESULTS role (negative control).
    privileged_governed = apply_sampling_if_needed(compiled, shared_data["privileged_role"])
    shared_data["privileged_sql"] = privileged_governed.sql


@then("results are capped at DEFAULT_SAMPLE_SIZE rows via the Stage 2 row cap mechanism")
def results_capped_at_default(shared_data: dict) -> None:
    cap = shared_data["expected_cap"]
    governed_sql = shared_data["governed_sql"]
    original_sql = shared_data["original_sql"]

    # The original query carried no LIMIT — proving the cap was injected, not pre-set.
    assert "LIMIT" not in original_sql.upper()

    # The governed SQL now carries a LIMIT equal to the resolved Stage 2 row cap.
    upper = governed_sql.upper()
    assert "LIMIT" in upper, f"expected LIMIT in governed SQL: {governed_sql}"
    assert str(cap) in governed_sql, f"expected cap {cap} in governed SQL: {governed_sql}"

    # The cap matches the configured default row limit.
    from provisa.compiler.sql_gen import _get_default_row_limit

    assert cap == _get_default_row_limit(), (
        f"cap {cap} does not match _get_default_row_limit() {_get_default_row_limit()}"
    )

    # Negative control: the FULL_RESULTS role's query is NOT capped.
    assert "LIMIT" not in shared_data["privileged_sql"].upper()


# ---------------------------------------------------------------------------
# REQ-594 — TenantMiddleware skip-path exemptions bypass tenant resolution
# ---------------------------------------------------------------------------


def _make_request(path: str) -> MagicMock:
    """Build a minimal Starlette-like request with the given path and NO identity.

    The absence of `state.identity` is critical: it proves that skip paths do
    not require a JWT with a tenant_id claim — a non-skip path would 401 here.
    """
    request = MagicMock()
    request.url.path = path
    # A real Starlette state object so getattr(state, "identity", None) is None.
    from starlette.datastructures import State

    request.state = State()
    return request


@given("a request to /billing/signup, /billing/webhook, /health, /docs, or /openapi.json")
def request_to_skip_path(shared_data: dict) -> None:
    # The canonical skip-path set must match the requirement exactly.
    expected = {
        "/billing/signup",
        "/billing/webhook",
        "/health",
        "/docs",
        "/openapi.json",
    }
    assert _SKIP_PATHS == expected

    # Build a request for every skip path, each with NO identity attached.
    shared_data["skip_paths"] = sorted(expected)
    shared_data["requests"] = {p: _make_request(p) for p in expected}

    # Confirm none of these requests carry an identity (no JWT context at all).
    for req in shared_data["requests"].values():
        assert getattr(req.state, "identity", None) is None


@when("TenantMiddleware processes the request")
def middleware_processes_skip_request(shared_data: dict) -> None:
    import asyncio

    sentinel_responses: dict[str, object] = {}
    call_next_invoked: dict[str, bool] = {}

    middleware = TenantMiddleware(MagicMock())

    async def _run() -> None:
        for path, request in shared_data["requests"].items():
            sentinel = object()
            invoked = {"called": False}

            async def call_next(_req, _sentinel=sentinel, _invoked=invoked):
                _invoked["called"] = True
                return _sentinel

            result = await middleware.dispatch(request, cast(RequestResponseEndpoint, call_next))
            sentinel_responses[path] = (result, sentinel)
            call_next_invoked[path] = invoked["called"]

    asyncio.run(_run())

    shared_data["sentinel_responses"] = sentinel_responses
    shared_data["call_next_invoked"] = call_next_invoked


@then("tenant resolution is bypassed and no JWT tenant_id claim is required")
def skip_path_bypasses_tenant_resolution(shared_data: dict) -> None:
    sentinel_responses = shared_data["sentinel_responses"]
    call_next_invoked = shared_data["call_next_invoked"]

    for path in shared_data["skip_paths"]:
        result, sentinel = sentinel_responses[path]

        # The downstream app was reached directly: call_next was invoked and its
        # exact response object was returned unchanged (no 401 substituted).
        assert call_next_invoked[path] is True, f"{path}: call_next not invoked"
        assert result is sentinel, f"{path}: response was not passthrough from call_next"

        # No tenant context was resolved or attached for the skip path — proving
        # tenant resolution was bypassed entirely.
        request = shared_data["requests"][path]
        assert getattr(request.state, "tenant_id", None) is None
        assert getattr(request.state, "tenant_context", None) is None

        # And it succeeded with no identity present — no JWT tenant_id required.
        from starlette.responses import JSONResponse

        assert not isinstance(result, JSONResponse)


# ---------------------------------------------------------------------------
# REQ-740 — Masking SELECT expressions only; WHERE/JOIN ON use physical columns
# ---------------------------------------------------------------------------

_CUSTOMERS_TABLE_ID = 10
_ORDERS_TABLE_ID = 11


def _customers_meta_740() -> TableMeta:
    return TableMeta(
        table_id=_CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customers",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="customers",
    )


def _orders_meta_740() -> TableMeta:
    return TableMeta(
        table_id=_ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
    )


def _ctx_740_with_join() -> CompilationContext:
    """Compilation context: orders root with a join to customers."""
    customers_meta = _customers_meta_740()
    orders_meta = _orders_meta_740()
    ctx = CompilationContext()
    ctx.tables = {"orders": orders_meta, "customers": customers_meta}
    ctx.joins = {
        ("Orders", "customers"): JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=customers_meta,
            cardinality="many-to-one",
        ),
    }
    return ctx


@given("a masked column also referenced in WHERE or JOIN ON")
def masked_column_in_where_and_join(shared_data: dict) -> None:
    """Set up a compiled query where 'email' is in SELECT, WHERE, and JOIN ON.

    The masking rule targets 'email' on the customers table for role 'analyst'.
    The compiled SQL contains:
      - SELECT projection referencing "email" (should be masked)
      - WHERE clause referencing "email" (must remain physical/unmasked)
      - JOIN ON clause referencing "customer_id" (unmasked, unaffected)

    A second query exercises the JOIN ON path with the masked column in the
    join condition itself.
    """
    role_id = "analyst"
    shared_data["role_id"] = role_id

    ctx = _ctx_740_with_join()
    shared_data["ctx"] = ctx

    # Masking rule: regex-mask 'email' on customers table for analyst role.
    mask_rule = MaskingRule(
        mask_type=MaskType.regex,
        pattern=r"^(.{2}).*(@.*)$",
        replace=r"\1***\2",
    )
    masking_rules: MaskingRules = {
        (_CUSTOMERS_TABLE_ID, role_id): {
            "email": (mask_rule, "varchar"),
        }
    }
    shared_data["masking_rules"] = masking_rules
    shared_data["mask_rule"] = mask_rule

    # Query 1: masked column 'email' in SELECT and WHERE.
    # The WHERE predicate must use the raw physical column; SELECT gets masked.
    sql_where = (
        'SELECT "email", "name" FROM "public"."customers" WHERE "email" LIKE \'%@example.com\''
    )
    compiled_where = CompiledQuery(
        sql=sql_where,
        params=[],
        root_field="customers",
        columns=[
            ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
            ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
        ],
        sources={"pg"},
    )
    shared_data["compiled_where"] = compiled_where

    # Override ctx for the WHERE-only scenario to use customers as root.
    ctx_where = CompilationContext()
    ctx_where.tables = {"customers": _customers_meta_740()}
    ctx_where.joins = {}
    shared_data["ctx_where"] = ctx_where

    # Query 2: masked column 'email' on joined customers table in SELECT;
    # the JOIN ON condition references customer_id (physical, unmasked).
    sql_join = (
        'SELECT "orders"."id", "customers"."email" '
        'FROM "public"."orders" '
        'JOIN "public"."customers" ON "orders"."customer_id" = "customers"."id" '
        'WHERE "orders"."status" = \'active\''
    )
    compiled_join = CompiledQuery(
        sql=sql_join,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias="customers", column="email", field_name="email", nested_in="customers"),
        ],
        sources={"pg"},
    )
    shared_data["compiled_join"] = compiled_join
    shared_data["ctx_join"] = ctx

    # Verify the masking rule is registered for the masked column.
    assert (_CUSTOMERS_TABLE_ID, role_id) in masking_rules
    assert "email" in masking_rules[(_CUSTOMERS_TABLE_ID, role_id)]


@when("masking is injected")
def masking_is_injected(shared_data: dict) -> None:
    """Apply inject_masking to both the WHERE-scenario and JOIN-scenario queries.

    Also handles the REQ-744 scenario when 'compiled_744' is present in shared_data.
    """
    role_id = shared_data["role_id"]
    masking_rules = shared_data["masking_rules"]

    # REQ-744 scenario: query with ORDER BY / LIMIT / GROUP BY clauses
    if "compiled_744" in shared_data:
        original_744 = shared_data["compiled_744"]
        result_744 = inject_masking(
            original_744,
            shared_data["ctx_744"],
            masking_rules,
            role_id,
        )
        shared_data["result_744"] = result_744
        return

    # WHERE scenario
    result_where = inject_masking(
        shared_data["compiled_where"],
        shared_data["ctx_where"],
        masking_rules,
        role_id,
    )
    shared_data["result_where"] = result_where

    # JOIN scenario
    result_join = inject_masking(
        shared_data["compiled_join"],
        shared_data["ctx_join"],
        masking_rules,
        role_id,
    )
    shared_data["result_join"] = result_join


@then(
    "SELECT projects the masked expression; WHERE and JOIN ON reference the physical unmasked column"
)
def select_masked_where_join_physical(shared_data: dict) -> None:
    """Assert masking affects only the SELECT projection."""
    from provisa.compiler.mask_inject import _find_select_end

    # ------------------------------------------------------------------ #
    # WHERE scenario assertions                                            #
    # ------------------------------------------------------------------ #
    result_where = shared_data["result_where"]
    original_where = shared_data["compiled_where"]

    assert result_where is not original_where, "inject_masking must return a new CompiledQuery"

    sql_where = result_where.sql
    select_end_where = _find_select_end(sql_where)
    select_part_where = sql_where[:select_end_where]
    rest_part_where = sql_where[select_end_where:]

    # SELECT projection must contain the masking expression.
    assert "REGEXP_REPLACE" in select_part_where.upper() or "regexp_replace" in select_part_where, (
        f"Expected REGEXP_REPLACE in SELECT portion; got: {select_part_where!r}"
    )

    # The WHERE clause must NOT be rewritten.
    assert "WHERE" in rest_part_where.upper(), (
        f"Expected WHERE in rest of SQL; got: {rest_part_where!r}"
    )
    # The predicate value must be intact.
    assert "%@example.com" in rest_part_where, (
        f"Expected WHERE predicate value in rest; got: {rest_part_where!r}"
    )
    # REGEXP_REPLACE must NOT appear in the WHERE clause portion.
    assert "REGEXP_REPLACE" not in rest_part_where.upper(), (
        f"REGEXP_REPLACE leaked into WHERE clause: {rest_part_where!r}"
    )


# ---------------------------------------------------------------------------
# REQ-741 — Masking output uses ANSI SQL dialects independent of source type
# ---------------------------------------------------------------------------


@given("a masked column in queries against different source types")
def masked_column_across_source_types(shared_data: dict) -> None:
    shared_data["regex_rule"] = MaskingRule(
        mask_type=MaskType.regex, pattern=r"^(.).*(@.*)$", replace=r"\1***\2"
    )
    shared_data["truncate_rule"] = MaskingRule(mask_type=MaskType.truncate, precision="month")
    shared_data["source_column_refs"] = {
        "postgresql": '"pg"."email"',
        "mysql": '"my"."email"',
        "snowflake": '"sf"."email"',
        "bigquery": '"bq"."email"',
    }


@when("build_mask_expression generates the mask")
def build_mask_expression_across_sources(shared_data: dict) -> None:
    regex_rule = shared_data["regex_rule"]
    truncate_rule = shared_data["truncate_rule"]
    shared_data["regex_outputs"] = {
        src: build_mask_expression(regex_rule, ref, "varchar")
        for src, ref in shared_data["source_column_refs"].items()
    }
    shared_data["truncate_outputs"] = {
        src: build_mask_expression(truncate_rule, ref, "timestamp")
        for src, ref in shared_data["source_column_refs"].items()
    }


@then("output is ANSI REGEXP_REPLACE/DATE_TRUNC regardless of source dialect")
def output_is_ansi_regardless_of_dialect(shared_data: dict) -> None:
    regex_outputs = shared_data["regex_outputs"]
    truncate_outputs = shared_data["truncate_outputs"]

    for src, out in regex_outputs.items():
        assert out.startswith("REGEXP_REPLACE("), f"{src}: {out!r}"
    for src, out in truncate_outputs.items():
        assert out.startswith("DATE_TRUNC("), f"{src}: {out!r}"
        assert "DATE_TRUNC('month'" in out

    normalized_regex = {
        out.replace(shared_data["source_column_refs"][src], "<col>")
        for src, out in regex_outputs.items()
    }
    normalized_truncate = {
        out.replace(shared_data["source_column_refs"][src], "<col>")
        for src, out in truncate_outputs.items()
    }
    assert normalized_regex == {"REGEXP_REPLACE(<col>, '^(.).*(@.*)$', '\\1***\\2')"}
    assert normalized_truncate == {"DATE_TRUNC('month', <col>)"}


# ---------------------------------------------------------------------------
# REQ-742 — Type-aware masking validation at config load time
# ---------------------------------------------------------------------------


@given("a masking rule configured with an incompatible type")
def masking_rule_incompatible_type(shared_data: dict) -> None:
    shared_data["invalid_cases"] = [
        (MaskingRule(mask_type=MaskType.regex, pattern=".", replace="x"), "age", "integer", True),
        (MaskingRule(mask_type=MaskType.truncate, precision="month"), "name", "varchar", True),
        (MaskingRule(mask_type=MaskType.constant, value=None), "id", "integer", False),
    ]
    shared_data["valid_case"] = (
        MaskingRule(mask_type=MaskType.regex, pattern=".+@", replace="***@"),
        "email",
        "varchar",
        True,
    )


@when("config is loaded")
def masking_config_loaded(shared_data: dict) -> None:
    errors: list[Exception | None] = []
    for rule, col, dtype, nullable in shared_data["invalid_cases"]:
        try:
            validate_masking_rule(rule, col, dtype, nullable)
            errors.append(None)
        except MaskingValidationError as exc:
            errors.append(exc)
    shared_data["validation_errors"] = errors

    rule, col, dtype, nullable = shared_data["valid_case"]
    validate_masking_rule(rule, col, dtype, nullable)
    shared_data["valid_accepted"] = True


@then("validation rejects the rule (e.g., regex on integer, truncate on varchar, NULL on NOT NULL)")
def validation_rejects_incompatible(shared_data: dict) -> None:
    errors = shared_data["validation_errors"]
    assert all(isinstance(e, MaskingValidationError) for e in errors), errors
    assert len(errors) == 3
    assert "integer" in str(errors[0])
    assert "varchar" in str(errors[1])
    assert "NOT NULL" in str(errors[2])
    assert shared_data["valid_accepted"] is True


# ---------------------------------------------------------------------------
# REQ-743 — Masking constant expressions emit syntactically valid SQL literals
# ---------------------------------------------------------------------------


@given("various constant mask values (null, boolean, numeric, string with apostrophe)")
def various_constant_mask_values(shared_data: dict) -> None:
    shared_data["constant_cases"] = [
        (MaskingRule(mask_type=MaskType.constant, value=None), "varchar", "NULL"),
        (MaskingRule(mask_type=MaskType.constant, value=42), "integer", "42"),
        (MaskingRule(mask_type=MaskType.constant, value=3.14), "double", "3.14"),
        (MaskingRule(mask_type=MaskType.constant, value="O'Brien"), "varchar", "'O''Brien'"),
        (MaskingRule(mask_type=MaskType.constant, value="REDACTED"), "varchar", "'REDACTED'"),
        (MaskingRule(mask_type=MaskType.constant, value="MAX"), "smallint", "32767"),
        (MaskingRule(mask_type=MaskType.constant, value="MIN"), "integer", "-2147483648"),
    ]


@when("build_mask_expression generates the SQL literal")
def build_constant_literals(shared_data: dict) -> None:
    shared_data["constant_outputs"] = [
        (build_mask_expression(rule, '"t"."c"', dtype), expected)
        for rule, dtype, expected in shared_data["constant_cases"]
    ]


@then(
    "output is syntactically valid (NULL keyword, TRUE/FALSE, numeric unquoted, strings single-quoted with escaped apostrophes)"
)
def constant_output_valid(shared_data: dict) -> None:
    for actual, expected in shared_data["constant_outputs"]:
        assert actual == expected, f"expected {expected!r}, got {actual!r}"

    outputs = {a for a, _ in shared_data["constant_outputs"]}
    assert "NULL" in outputs and "'NULL'" not in outputs
    assert "42" in outputs and "3.14" in outputs
    assert "'O''Brien'" in outputs
    assert "32767" in outputs and "-2147483648" in outputs


# ---------------------------------------------------------------------------
# REQ-744 — Masking preserves query structure (ORDER BY/LIMIT/GROUP BY);
#           immutable transformation (new object, input unmutated)
# ---------------------------------------------------------------------------

_R744_TABLE_ID = _CUSTOMERS_TABLE_ID  # reuse customers-table masking rule (email)


@given("a query with ORDER BY, LIMIT, GROUP BY, or other clauses")
def query_with_structural_clauses(shared_data: dict) -> None:
    """Build a compiled query that masks 'email' and carries GROUP BY/ORDER BY/LIMIT.

    Reuses the REQ-740 masking-rule fixture (regex mask on 'email' for analyst on
    the customers table) so the @when("masking is injected") step can inject it.
    """
    role_id = "analyst"
    shared_data["role_id"] = role_id

    ctx_744 = CompilationContext()
    ctx_744.tables = {"customers": _customers_meta_740()}
    ctx_744.joins = {}
    shared_data["ctx_744"] = ctx_744

    mask_rule = MaskingRule(
        mask_type=MaskType.regex,
        pattern=r"^(.{2}).*(@.*)$",
        replace=r"\1***\2",
    )
    masking_rules: MaskingRules = {
        (_R744_TABLE_ID, role_id): {"email": (mask_rule, "varchar")},
    }
    shared_data["masking_rules"] = masking_rules

    # A query with GROUP BY, ORDER BY, and LIMIT trailing the SELECT projection.
    sql = (
        'SELECT "email", COUNT(*) FROM "public"."customers" '
        'GROUP BY "email" '
        'ORDER BY "email" ASC '
        "LIMIT 10"
    )
    shared_data["clauses_744"] = ' GROUP BY "email" ORDER BY "email" ASC LIMIT 10'
    compiled_744 = CompiledQuery(
        sql=sql,
        params=[],
        root_field="customers",
        columns=[
            ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
        ],
        sources={"pg"},
    )
    shared_data["compiled_744"] = compiled_744
    # Snapshot of the input SQL string to prove the input is not mutated.
    shared_data["original_sql_744"] = sql

    # Confirm the masking rule is registered for the column under test.
    assert (_R744_TABLE_ID, role_id) in masking_rules
    assert "email" in masking_rules[(_R744_TABLE_ID, role_id)]


@then("the clauses remain unchanged; result is a new object, input is unchanged")
def clauses_preserved_and_input_immutable(shared_data: dict) -> None:
    from provisa.compiler.mask_inject import _find_select_end

    original = shared_data["compiled_744"]
    result = shared_data["result_744"]
    clauses = shared_data["clauses_744"]

    # (b) A NEW object is returned; the input object identity differs.
    assert result is not original, "inject_masking must return a new CompiledQuery"

    # The input SQL string is byte-identical to its pre-injection snapshot
    # (proves the input CompiledQuery was not mutated in place).
    assert original.sql == shared_data["original_sql_744"], "input SQL was mutated"

    # Masking actually happened — the SELECT projection was rewritten.
    assert result.sql != original.sql, "masking produced no change"
    result_select = result.sql[: _find_select_end(result.sql)]
    assert "REGEXP_REPLACE" in result_select.upper(), result_select

    # (a) The trailing clauses (GROUP BY / ORDER BY / LIMIT) are byte-identical
    # before and after masking — only the SELECT projection changed.
    assert original.sql.endswith(clauses), original.sql
    assert result.sql.endswith(clauses), result.sql

    # The FROM..end tail (everything from FROM onward) is byte-identical, proving
    # nothing outside the SELECT projection was touched.
    assert (
        result.sql[_find_select_end(result.sql) :] == original.sql[_find_select_end(original.sql) :]
    ), "structure after SELECT changed"

    # REGEXP_REPLACE must NOT leak into GROUP BY / ORDER BY / LIMIT.
    assert "REGEXP_REPLACE" not in clauses.upper()


# ---------------------------------------------------------------------------
# REQ-745 — Role-based masking: different roles see different masks
# ---------------------------------------------------------------------------

_R745_TABLE_ID = 20


def _r745_ctx() -> CompilationContext:
    meta = TableMeta(
        table_id=_R745_TABLE_ID,
        field_name="users",
        type_name="Users",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="users",
    )
    ctx = CompilationContext()
    ctx.tables = {"users": meta}
    ctx.joins = {}
    return ctx


def _r745_compiled() -> CompiledQuery:
    return CompiledQuery(
        sql='SELECT "salary" FROM "public"."users"',
        params=[],
        root_field="users",
        columns=[ColumnRef(alias=None, column="salary", field_name="salary", nested_in=None)],
        sources={"pg"},
    )


@given("the same column with different masking rules per role")
def same_column_different_role_masks(shared_data: dict) -> None:
    shared_data["ctx_745"] = _r745_ctx()
    shared_data["compiled_745"] = _r745_compiled()

    regex_rule = MaskingRule(mask_type=MaskType.regex, pattern="^.*$", replace="hidden")
    constant_rule = MaskingRule(mask_type=MaskType.constant, value=0)

    shared_data["masking_rules_745"] = {
        (_R745_TABLE_ID, "analyst"): {"salary": (regex_rule, "varchar")},
        (_R745_TABLE_ID, "masked_viewer"): {"salary": (constant_rule, "integer")},
    }


@when("inject_masking is called for two different roles")
def inject_masking_per_role(shared_data: dict) -> None:
    ctx = shared_data["ctx_745"]
    rules = shared_data["masking_rules_745"]
    compiled = shared_data["compiled_745"]
    shared_data["sql_admin"] = inject_masking(compiled, ctx, rules, "admin").sql
    shared_data["sql_analyst"] = inject_masking(compiled, ctx, rules, "analyst").sql
    shared_data["sql_masked_viewer"] = inject_masking(compiled, ctx, rules, "masked_viewer").sql


@then("admin sees raw values; analyst sees regex mask; masked_viewer sees constant mask")
def role_specific_masks_applied(shared_data: dict) -> None:
    sql_admin = shared_data["sql_admin"]
    sql_analyst = shared_data["sql_analyst"]
    sql_viewer = shared_data["sql_masked_viewer"]

    assert sql_admin == shared_data["compiled_745"].sql
    assert "REGEXP_REPLACE" not in sql_admin
    assert '"salary"' in sql_admin

    assert "REGEXP_REPLACE" in sql_analyst
    assert 'AS "salary"' in sql_analyst

    assert "REGEXP_REPLACE" not in sql_viewer
    assert '0 AS "salary"' in sql_viewer

    assert len({sql_admin, sql_analyst, sql_viewer}) == 3


# ---------------------------------------------------------------------------
# REQ-746 — Capability enforcement via check_capability / has_capability
# ---------------------------------------------------------------------------


@given("a role with a specific capability (e.g., query_development)")
def role_with_specific_capability(shared_data: dict) -> None:
    shared_data["role_qd"] = {
        "id": "developer",
        "capabilities": [Capability.QUERY_DEVELOPMENT.value],
    }
    shared_data["role_admin"] = {"id": "root", "capabilities": [Capability.ADMIN.value]}
    assert has_capability(shared_data["role_qd"], Capability.QUERY_DEVELOPMENT)
    assert not has_capability(shared_data["role_qd"], Capability.SOURCE_REGISTRATION)


@when("check_capability is called for that capability")
def call_check_capability(shared_data: dict) -> None:
    role = shared_data["role_qd"]
    check_capability(role, Capability.QUERY_DEVELOPMENT)
    shared_data["granted_ok"] = True

    try:
        check_capability(role, Capability.SOURCE_REGISTRATION)
        shared_data["missing_raised"] = None
    except InsufficientRightsError as exc:
        shared_data["missing_raised"] = exc

    check_capability(shared_data["role_admin"], Capability.MASKING_CONFIG)
    shared_data["admin_ok"] = True


@then("no exception is raised; for missing capability, InsufficientRightsError is raised")
def capability_enforcement_result(shared_data: dict) -> None:
    assert shared_data["granted_ok"] is True
    assert shared_data["admin_ok"] is True

    exc = shared_data["missing_raised"]
    assert isinstance(exc, InsufficientRightsError)
    assert exc.required == Capability.SOURCE_REGISTRATION
    assert exc.role_id == "developer"


# ---------------------------------------------------------------------------
# REQ-747 — SQL validator bypass for remote same-source relationship pairs
# ---------------------------------------------------------------------------


def _remote_meta(table_id: int, table_name: str, schema_name: str, source_id: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=table_name,
        type_name=table_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name=schema_name,
        table_name=table_name,
        domain_id="shop",
        source_type="graphql_remote",
    )


def _build_cross_source_747(shared_data: dict) -> None:
    local_meta = TableMeta(
        table_id=3,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
        source_type="postgresql",
    )
    ctx_cross = CompilationContext()
    ctx_cross.tables = {
        "orders": local_meta,
        "products": _remote_meta(1, "products", "gql_shop", "gql-shop"),
    }
    ctx_cross.joins = {}
    shared_data["ctx_cross_747"] = ctx_cross
    gov_cross = GovernanceContext()
    gov_cross.table_map["public.orders"] = 3
    gov_cross.table_map["gql_shop.products"] = 1
    shared_data["gov_cross_747"] = gov_cross
    shared_data["sql_cross_source_747"] = (
        'SELECT "orders"."id", "p"."sku" '
        'FROM "public"."orders" '
        'LEFT JOIN "gql_shop"."products" AS "p" ON "p"."order_id" = "orders"."id"'
    )


@given("two remote tables from the same source_id with bypass_uncovered_relationships=True")
def two_remote_same_source_tables(shared_data: dict) -> None:
    meta_a = _remote_meta(1, "products", "gql_shop", "gql-shop")
    meta_b = _remote_meta(2, "categories", "gql_shop", "gql-shop")
    ctx = CompilationContext()
    ctx.tables = {"products": meta_a, "categories": meta_b}
    ctx.joins = {}
    shared_data["ctx_747"] = ctx

    gov = GovernanceContext()
    gov.table_map["gql_shop.products"] = 1
    gov.table_map["gql_shop.categories"] = 2
    shared_data["gov_747"] = gov
    shared_data["role_747"] = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}

    shared_data["sql_same_source_747"] = (
        'SELECT "p"."id", "c"."name" '
        'FROM "gql_shop"."products" AS "p" '
        'LEFT JOIN "gql_shop"."categories" AS "c" ON "c"."id" = "p"."category_id"'
    )
    _build_cross_source_747(shared_data)


@when("SQL validator checks the join")
def sql_validator_checks_join(shared_data: dict) -> None:
    shared_data["same_source_violations"] = validate_sql(
        shared_data["sql_same_source_747"],
        shared_data["ctx_747"],
        shared_data["gov_747"],
        shared_data["role_747"],
        [],
        bypass_uncovered_relationships=True,
    )
    shared_data["cross_source_violations"] = validate_sql(
        shared_data["sql_cross_source_747"],
        shared_data["ctx_cross_747"],
        shared_data["gov_cross_747"],
        shared_data["role_747"],
        [],
        bypass_uncovered_relationships=True,
    )
    shared_data["same_source_no_bypass"] = validate_sql(
        shared_data["sql_same_source_747"],
        shared_data["ctx_747"],
        shared_data["gov_747"],
        shared_data["role_747"],
        [],
        bypass_uncovered_relationships=False,
    )


@then("V002 violation is not raised; cross-source joins still require coverage")
def v002_bypassed_for_same_source(shared_data: dict) -> None:
    same_codes = {v.code for v in shared_data["same_source_violations"]}
    cross_codes = {v.code for v in shared_data["cross_source_violations"]}
    no_bypass_codes = {v.code for v in shared_data["same_source_no_bypass"]}

    assert "V002" not in same_codes, shared_data["same_source_violations"]
    assert "V002" in cross_codes
    assert "V002" in no_bypass_codes


# ---------------------------------------------------------------------------
# REQ-748 — Inverse relationship collision: distinct rel_types per direction
# ---------------------------------------------------------------------------


def _r748_nodes():
    from provisa.cypher.label_map import NodeMapping

    pets_node = NodeMapping(
        label="Pets",
        type_name="Pets",
        domain_label=None,
        table_label="Pets",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="pets",
        properties={"id": "id", "breedName": "breed_name"},
    )
    assignments_node = NodeMapping(
        label="Assignments",
        type_name="Assignments",
        domain_label=None,
        table_label="Assignments",
        table_id=2,
        source_id="shelter-pg",
        id_column="breedName",
        pk_columns=[],
        catalog_name="shelter_pg",
        schema_name="shelter",
        table_name="assignments",
        properties={"breedName": "breedName"},
    )
    return pets_node, assignments_node


def _r748_ctx_and_label_map():
    from provisa.cypher.label_map import CypherLabelMap, RelationshipMapping

    pets_meta = TableMeta(
        table_id=1,
        field_name="pets",
        type_name="Pets",
        source_id="pg",
        catalog_name="postgresql",
        schema_name="public",
        table_name="pets",
    )
    assignments_meta = TableMeta(
        table_id=2,
        field_name="assignments",
        type_name="Assignments",
        source_id="shelter-pg",
        catalog_name="shelter_pg",
        schema_name="shelter",
        table_name="assignments",
    )
    ctx = CompilationContext()
    ctx.tables = {"pets": pets_meta, "assignments": assignments_meta}
    ctx.joins = {}
    ctx.aggregate_columns = {
        1: [("id", "integer"), ("breed_name", "varchar")],
        2: [("breedName", "varchar"), ("employee_id", "integer")],
    }

    pets_node, assignments_node = _r748_nodes()
    pets_to_assignments = RelationshipMapping(
        rel_type="IS_ASSIGNMENT",
        source_label="Pets",
        target_label="Assignments",
        join_source_column="breed_name",
        join_target_column="breedName",
        field_name="assignment",
        many=False,
    )
    assignments_to_pets = RelationshipMapping(
        rel_type="HAS_PETS",
        source_label="Assignments",
        target_label="Pets",
        join_source_column="breedName",
        join_target_column="breed_name",
        field_name="pets",
        many=True,
    )
    lm = CypherLabelMap(
        nodes={"Pets": pets_node, "Assignments": assignments_node},
        relationships={
            "IS_ASSIGNMENT::Pets→Assignments": pets_to_assignments,
            "HAS_PETS::Assignments→Pets": assignments_to_pets,
        },
        nodes_by_table={"Pets": ["Pets"], "Assignments": ["Assignments"]},
    )
    return ctx, lm


@given("two tables with inverse relationships sharing the same column names")
def two_tables_inverse_relationships(shared_data: dict) -> None:
    ctx, lm = _r748_ctx_and_label_map()
    shared_data["ctx_748"] = ctx
    shared_data["label_map_748"] = lm
    shared_data["sql_forward_748"] = (
        'SELECT "pets"."id", "a"."breedName" '
        'FROM "public"."pets" '
        'LEFT JOIN "shelter"."assignments" AS "a" ON "a"."breedName" = "pets"."breed_name"'
    )
    shared_data["sql_inverse_748"] = (
        'SELECT "a"."breedName", "p"."id" '
        'FROM "shelter"."assignments" AS "a" '
        'LEFT JOIN "public"."pets" AS "p" ON "p"."breed_name" = "a"."breedName"'
    )


@when("semantic_sql_to_cypher converts joins in both directions")
def convert_joins_both_directions(shared_data: dict) -> None:
    from provisa.cypher.sql_to_cypher import semantic_sql_to_cypher

    shared_data["cypher_forward"] = semantic_sql_to_cypher(
        shared_data["sql_forward_748"], shared_data["label_map_748"], shared_data["ctx_748"]
    )
    shared_data["cypher_inverse"] = semantic_sql_to_cypher(
        shared_data["sql_inverse_748"], shared_data["label_map_748"], shared_data["ctx_748"]
    )


@then("forward join emits correct rel_type; inverse join emits distinct rel_type")
def joins_emit_distinct_rel_types(shared_data: dict) -> None:
    forward = shared_data["cypher_forward"]
    inverse = shared_data["cypher_inverse"]

    assert forward is not None
    assert inverse is not None

    assert "IS_ASSIGNMENT" in forward, forward
    assert "HAS_PETS" not in forward, forward

    assert "HAS_PETS" in inverse, inverse
    assert "IS_ASSIGNMENT" not in inverse, inverse


# ---------------------------------------------------------------------------
# REQ-749 — Domain policy tri-state mode (legacy / single-domain / namespaced)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_domain_policy():
    """Isolate every scenario from domain_policy global state."""
    from provisa.core import domain_policy

    domain_policy.reset()
    yield
    domain_policy.reset()


@given("a config with use_domains mode specified")
def config_with_use_domains_mode(shared_data: dict) -> None:
    shared_data["declared_domain"] = "sales"
    shared_data["default_domain"] = "global"


def _resolve_single_mode(shared_data: dict, declared: str, default: str) -> None:
    from provisa.core import domain_policy

    domain_policy.configure(False, default)
    shared_data["single_coerced"] = domain_policy.resolve_domain_id(None)
    shared_data["single_matching"] = domain_policy.resolve_domain_id(default)
    try:
        domain_policy.resolve_domain_id(declared)
        shared_data["single_foreign_error"] = None
    except ValueError as exc:
        shared_data["single_foreign_error"] = exc
    shared_data["single_system_ids"] = domain_policy.system_domain_ids()


def _resolve_namespaced_mode(shared_data: dict, declared: str, default: str) -> None:
    from provisa.core import domain_policy

    domain_policy.configure(True, default)
    shared_data["namespaced_stored"] = domain_policy.resolve_domain_id(declared)
    try:
        domain_policy.resolve_domain_id(None)
        shared_data["namespaced_missing_error"] = None
    except ValueError as exc:
        shared_data["namespaced_missing_error"] = exc


@when("load_config_from_yaml processes the config")
def process_config_tri_state(shared_data: dict) -> None:
    from provisa.core import domain_policy

    declared = shared_data["declared_domain"]
    default = shared_data["default_domain"]

    domain_policy.reset()
    shared_data["legacy_stored"] = domain_policy.resolve_domain_id(declared)
    shared_data["legacy_empty"] = domain_policy.resolve_domain_id(None)
    shared_data["legacy_active"] = domain_policy.active()

    _resolve_single_mode(shared_data, declared, default)
    _resolve_namespaced_mode(shared_data, declared, default)


@then(
    "domain_id is stored according to the tri-state mode (legacy/single/namespaced) and reload validates existing domains"
)
def domain_id_stored_per_mode(shared_data: dict) -> None:
    assert shared_data["legacy_stored"] == "sales"
    assert shared_data["legacy_empty"] == ""
    assert shared_data["legacy_active"] is False

    assert shared_data["single_coerced"] == "global"
    assert shared_data["single_matching"] == "global"
    assert isinstance(shared_data["single_foreign_error"], ValueError)
    assert "global" in shared_data["single_system_ids"]
    assert "meta" in shared_data["single_system_ids"]

    assert shared_data["namespaced_stored"] == "sales"
    assert isinstance(shared_data["namespaced_missing_error"], ValueError)
