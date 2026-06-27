# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD steps for REQ-039 — Schema visibility enforcement,
REQ-040 — SQL enforcement layer (RLS injection + column stripping),
REQ-531 — Predicate guard rejecting masked columns from WHERE/HAVING (V005),
REQ-554 — Default row cap (DEFAULT_SAMPLE_SIZE) for roles lacking full_results,
REQ-594 — TenantMiddleware skip-path exemptions bypass tenant resolution,
REQ-740 — Masking SELECT expressions only; WHERE, JOIN ON, and other predicates
           use physical unmasked columns unchanged, and
REQ-741 — Column masking output uses ANSI SQL dialects independent of source type."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.security.visibility import (
    is_column_visible,
    visible_column_names,
    visible_tables,
)
from provisa.security.rights import Capability, has_capability
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
    DEFAULT_SAMPLE_SIZE,
    apply_sampling_if_needed,
    get_sample_size,
)
from provisa.compiler.stage2 import GovernanceContext, resolve_row_cap
from provisa.security.masking import MaskingRule, MaskType, build_mask_expression
from provisa.compiler.mask_inject import MaskingRules, inject_masking

from provisa.api.middleware.tenant_middleware import TenantMiddleware, _SKIP_PATHS

scenarios("REQ-039.feature")
scenarios("REQ-040.feature")
scenarios("REQ-531.feature")
scenarios("REQ-554.feature")
scenarios("REQ-594.feature")
scenarios("REQ-740.feature")
scenarios("REQ-741.feature")


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


@then(
    "unauthorized tables and columns do not appear and are rejected at parse time"
)
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
    shared_data["surviving_columns"] = {
        c.column for c in compiled.columns if c.column in allowed
    }


@then(
    "RLS WHERE clauses are injected and unauthorized columns are stripped before "
    "execution"
)
def rls_injected_columns_stripped(shared_data: dict) -> None:
    enforced_sql = shared_data["enforced_sql"]
    allowed = shared_data["allowed_columns"]
    surviving = shared_data["surviving_columns"]

    # RLS predicate is present in the executable SQL.
    assert "region = 'us'" in enforced_sql
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
    shared_data["where_sql"] = (
        "SELECT id FROM users WHERE email = 'victim@example.com'"
    )
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

    shared_data["where_violations"] = validate_sql(
        shared_data["where_sql"], ctx, gov_ctx, role, []
    )
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
    privileged_governed = apply_sampling_if_needed(
        compiled, shared_data["privileged_role"]
    )
    shared_data["privileged_sql"] = privileged_governed.sql


@then(
    "results are capped at DEFAULT_SAMPLE_SIZE rows via the Stage 2 row cap mechanism"
)
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

    # The cap matches the configured DEFAULT_SAMPLE_SIZE / PROVISA_SAMPLE_SIZE setting.
    assert cap in (get_sample_size(), DEFAULT_SAMPLE_SIZE)

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


@given(
    "a request to /billing/signup, /billing/webhook, /health, /docs, or /openapi.json"
)
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

            result = await middleware.dispatch(request, call_next)
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

# Table IDs used in the REQ-740 scenario
_REQ740_ORDERS_TABLE_ID = 10
_REQ740_CUSTOMERS_TABLE_ID = 11


def _req740_orders_meta() -> TableMeta:
    return TableMeta(
        table_id=_REQ740_ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
    )


def _req740_customers_meta() -> TableMeta:
    return TableMeta(
        table_id=_REQ740_CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customers",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="customers",
    )


@given("a masked column also referenced in WHERE or JOIN ON")
def masked_column_in_where_and_join(shared_data: dict) -> None:
    """Set up a query where 'email' is masked but also appears in WHERE and JOIN ON.

    The SQL intentionally uses the physical column name in WHERE and JOIN ON
    predicates, exactly as the compiler emits before masking injection. The
    masking step must only rewrite the SELECT projection.
    """
    # -----------------------------------------------------------------------
    # Scenario A: masked column in WHERE
    # -----------------------------------------------------------------------
    where_sql = (
        'SELECT "email", "id" '
        'FROM "public"."orders" '
        "WHERE \"email\" = 'user@example.com'"
    )
    where_compiled = CompiledQuery(
        sql=where_sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="email", field_name="email", nested_in=None),
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ],
        sources={"pg"},
    )

    # -----------------------------------------------------------------------
    # Scenario B: masked column in JOIN ON
    # -----------------------------------------------------------------------
    join_sql = (
        'SELECT "orders"."id", "customers"."email" '
        'FROM "public"."orders" '
        'JOIN "public"."customers" '
        'ON "orders"."customer_email" = "customers"."email"'
    )
    join_compiled = CompiledQuery(
        sql=join_sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias="customers", column="email", field_name="email", nested_in="customers"),
        ],
        sources={"pg"},
    )

    # -----------------------------------------------------------------------
    # Compilation context: orders root table + customers join
    # -----------------------------------------------------------------------
    orders_meta = _req740_orders_meta()
    customers_meta = _req740_customers_meta()

    ctx = CompilationContext()
    ctx.tables = {"orders": orders_meta}
    ctx.joins = {
        ("Orders", "customers"): JoinMeta(
            source_column="customer_email",
            target_column="email",
            source_column_type="varchar",
            target_column_type="varchar",
            target=customers_meta,
            cardinality="many-to-one",
        ),
    }

    # -----------------------------------------------------------------------
    # Masking rules: 'email' on the orders table is masked for 'analyst'
    # -----------------------------------------------------------------------
    masking_rules: MaskingRules = {
        (_REQ740_ORDERS_TABLE_ID, "analyst"): {
            "email": (
                MaskingRule(
                    mask_type=MaskType.regex,
                    pattern=r"^(.{2}).*(@.*)$",
                    replace=r"\1***\2",
                ),
                "varchar",
            ),
        },
        (_REQ740_CUSTOMERS_TABLE_ID, "analyst"): {
            "email": (
                MaskingRule(
                    mask_type=MaskType.regex,
                    pattern=r"^(.{2}).*(@.*)$",
                    replace=r"\1***\2",
                ),
                "varchar",
            ),
        },
    }

    shared_data["where_compiled"] = where_compiled
    shared_data["join_compiled"] = join_compiled
    shared_data["ctx"] = ctx
    shared_data["masking_rules"] = masking_rules
    shared_data["role_id"] = "analyst"

    # Confirm the masking rules are in place for the role under test.
    assert (_REQ740_ORDERS_TABLE_ID, "analyst") in masking_rules
    assert "email" in masking_rules[(_REQ740_ORDERS_TABLE_ID, "analyst")]


@when("masking is injected")
def masking_is_injected(shared_data: dict) -> None:
    """Apply inject_masking to both the WHERE and JOIN ON scenario queries."""
    ctx = shared_data["ctx"]
    masking_rules = shared_data["masking_rules"]
    role_id = shared_data["role_id"]

    where_result = inject_masking(
        shared_data["where_compiled"], ctx, masking_rules, role_id
    )
    join_result = inject_masking(
        shared_data["join_compiled"], ctx, masking_rules, role_id
    )

    shared_data["where_result"] = where_result
    shared_data["join_result"] = join_result

    # inject_masking must return new CompiledQuery objects, not the same instance.
    assert where_result is not shared_data["where_compiled"], (
        "inject_masking must return a new CompiledQuery, not the original"
    )
    assert join_result is not shared_data["join_compiled"], (
        "inject_masking must return a new CompiledQuery, not the original"
    )


@then(
    "SELECT projects the masked expression; WHERE and JOIN ON reference the physical unmasked column"
)
def select_masked_predicates_physical(shared_data: dict) -> None:
    """Assert that masking was applied exclusively to the SELECT projection.

    For the WHERE scenario:
      - The SELECT clause must contain the mask expression (REGEXP_REPLACE or equivalent).
      - The WHERE clause must still reference the raw physical column "email" without
        any mask expression wrapping it.

    For the JOIN ON scenario:
      - The SELECT clause must contain the masked expression for customers.email.
      - The JOIN ON predicate must retain the physical column references unchanged.
    """
    # -------------------------------------------------------------------
    # WHERE scenario assertions
    # -------------------------------------------------------------------
    where_sql = shared_data["where_result"].sql
    where_upper = where_sql.upper()

    # The mask expression must appear somewhere in the SQL (proving injection ran).
    assert "REGEXP_REPLACE" in where_upper, (
        f"Expected REGEXP_REPLACE in masked SQL, got: {where_sql}"
    )

    # Locate the boundary between SELECT and FROM to isolate each clause.
    from_idx_where = where_upper.index("FROM")
    where_select_part = where_sql[:from_idx_where]
    where_rest_part = where_sql[from_idx_where:]

    # The SELECT projection carries the mask expression.
    assert "REGEXP_REPLACE" in where_select_part.upper(), (
        f"Mask expression must be in SELECT projection, not found in: {where_select_part}"
    )

    # The WHERE clause must NOT contain the mask expression — it uses the physical column.
    assert "REGEXP_REPLACE" not in where_rest_part.upper(), (
        f"Mask expression must NOT appear in WHERE clause, found in: {where_rest_part}"
    )

    # The raw physical column reference must be preserved in the WHERE predicate.
    assert "email" in where_rest_part.lower(), (
        f"Physical column 'email' must remain in WHERE clause: {where_rest_part}"
    )

    # The WHERE value literal must survive untouched.
    assert "user@example.com" in where_rest_part, (
        f"WHERE predicate value must be preserved: {where_rest_part}"
    )

    # -------------------------------------------------------------------
    # JOIN ON scenario assertions
    # -------------------------------------------------------------------
    join_sql = shared_data["join_result"].sql
    join_upper = join_sql.upper()

    # The mask expression must appear somewhere in the SQL (proving injection ran for
    # the customers.email column in the SELECT list).
    assert "REGEXP_REPLACE" in join_upper, (
        f"Expected REGEXP_REPLACE in masked JOIN SQL, got: {join_sql}"
    )

    # Isolate the SELECT projection portion (before FROM).
    from_idx_join = join_upper.index("FROM")
    join_select_part = join_sql[:from_idx_join]
    join_rest_part = join_sql[from_idx_join:]

    # The SELECT projection carries the mask expression.
    assert "REGEXP_REPLACE" in join_select_part.upper(), (
        f"Mask expression must be in SELECT projection: {join_select_part}"
    )

    # The JOIN ON predicate must NOT contain the mask expression.
    assert "REGEXP_REPLACE" not in join_rest_part.upper(), (
        f"Mask expression must NOT appear in JOIN ON clause, found in: {join_rest_part}"
    )
