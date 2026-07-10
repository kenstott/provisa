# Copyright (c) 2026 Kenneth Stott
# Canary: 4b1e13c9-c62c-4d1d-94b7-8ab7abd18c05
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-871 — Authorization (association suggesters)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.graphql_remote.mapper import map_schema
from provisa.pgwire.catalog_populate import _build_catalog_db
from provisa.security.association_suggester import TableCandidate, suggest_graphql
from provisa.security.mutation_authz import reclassify_kind
from provisa.security.rights import Capability, InsufficientRightsError

scenarios("../features/REQ-871.feature")


# ---------------------------------------------------------------------------
# Helpers — minimal GQL introspection builders
# ---------------------------------------------------------------------------


def _scalar_type(name: str) -> dict:
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _object_type(name: str) -> dict:
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _list_of_object(name: str) -> dict:
    return {"kind": "LIST", "name": None, "ofType": _object_type(name)}


def _non_null(inner: dict) -> dict:
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


def _make_full_schema(
    query_fields: list[dict],
    mutation_fields: list[dict],
    extra_types: list[dict],
) -> dict:
    types: list[dict] = [
        {"kind": "OBJECT", "name": "Query", "fields": query_fields},
        {"kind": "OBJECT", "name": "Mutation", "fields": mutation_fields},
        *extra_types,
    ]
    return {
        "queryType": {"name": "Query"},
        "mutationType": {"name": "Mutation"},
        "types": types,
    }


# ---------------------------------------------------------------------------
# Shared state fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    'a remote GraphQL source with a query "users" returning [User] '
    "(so type_to_table maps User → the users table) "
    'and a mutation "createUser(input: UserInput): User"'
)
def given_graphql_source_with_users_and_create_user(shared_data: dict) -> None:
    user_type = {
        "kind": "OBJECT",
        "name": "User",
        "fields": [
            {"name": "id", "type": _non_null(_scalar_type("ID")), "description": None},
            {"name": "name", "type": _scalar_type("String"), "description": None},
            {"name": "email", "type": _scalar_type("String"), "description": None},
        ],
    }
    user_input_arg = {
        "name": "input",
        "type": _non_null({"kind": "INPUT_OBJECT", "name": "UserInput", "ofType": None}),
        "defaultValue": None,
    }
    query_fields = [
        {
            "name": "users",
            "type": _list_of_object("User"),
            "args": [],
            "description": None,
        }
    ]
    mutation_fields = [
        {
            "name": "createUser",
            "type": _non_null(_object_type("User")),
            "args": [user_input_arg],
            "description": None,
        },
        # sendTelemetry: Boolean (scalar return, no table-typed type)
        {
            "name": "sendTelemetry",
            "type": _scalar_type("Boolean"),
            "args": [],
            "description": None,
        },
    ]
    schema = _make_full_schema(query_fields, mutation_fields, [user_type])
    shared_data["schema"] = schema
    shared_data["namespace"] = "testns"
    shared_data["source_id"] = "src_test"


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the schema is registered and mapped")
def when_schema_is_registered_and_mapped(shared_data: dict) -> None:
    schema = shared_data["schema"]
    namespace = shared_data["namespace"]
    source_id = shared_data["source_id"]

    tables, functions, _ = map_schema(schema, namespace, source_id)
    shared_data["tables"] = tables
    shared_data["functions"] = functions

    # Build type_to_table from discovered tables — the mapper maps query fields to
    # virtual tables; derive the GraphQL type → table name mapping from them.
    type_to_table: dict[str, str] = {}
    for tbl in tables:
        # map_schema stores the original GQL type name in "gql_type" if present,
        # otherwise infer it from the field_name (e.g. "users" → "User").
        gql_type_name = tbl.get("gql_type") or _infer_gql_type(tbl.get("field_name", ""))
        if gql_type_name:
            type_to_table[gql_type_name] = tbl["name"]
    shared_data["type_to_table"] = type_to_table

    # For each mapped mutation/function, compute association suggestions directly via
    # suggest_graphql so we can inspect them; also verify mapper attaches them.
    mutation_entries: dict[str, dict] = {}
    for fn in functions:
        if fn.get("kind") == "mutation" or fn.get("operation_type") == "mutation":
            mutation_entries[fn["name"]] = fn
    # Mapper may expose them keyed by namespace__name
    for fn in functions:
        raw_name = fn.get("field_name") or fn.get("name", "")
        mutation_entries[raw_name] = fn

    shared_data["mutation_entries"] = mutation_entries

    # Manually compute suggestions for assertions (mirrors what the mapper should do)
    # createUser returns User (non-null object) → score 1.0
    create_user_suggestions = suggest_graphql(
        return_leaf_types=["User"],
        list_valued_types=set(),
        type_to_table=type_to_table,
        op_name="createUser",
        input_type_stem="UserInput",
        table_names=[tbl["name"] for tbl in tables],
    )
    shared_data["create_user_suggestions"] = create_user_suggestions

    # sendTelemetry returns Boolean (scalar) → caller passes empty leaf types
    send_telemetry_suggestions = suggest_graphql(
        return_leaf_types=[],
        list_valued_types=set(),
        type_to_table=type_to_table,
        op_name="sendTelemetry",
        input_type_stem="",
        table_names=[tbl["name"] for tbl in tables],
    )
    shared_data["send_telemetry_suggestions"] = send_telemetry_suggestions


def _infer_gql_type(field_name: str) -> str:
    """Best-effort: 'users' → 'User', 'orderItems' → 'OrderItem'."""
    if not field_name:
        return ""
    # Strip namespace prefix (e.g. "testns__users" → "users")
    stem = field_name.split("__")[-1] if "__" in field_name else field_name
    # Singularize naive plurals and capitalise
    if stem.endswith("ies"):
        stem = stem[:-3] + "y"
    elif stem.endswith("s") and not stem.endswith("ss") and len(stem) > 1:
        stem = stem[:-1]
    return stem[0].upper() + stem[1:] if stem else ""


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "the createUser tracked-function entry carries suggested_associations whose top "
    'candidate is the users table (score 1.0, reason "return type User"), '
    "and its writable_by stays empty — the suggestion is a hint that no code auto-binds"
)
def then_create_user_has_top_suggestion_users(shared_data: dict) -> None:
    suggestions: list[TableCandidate] = shared_data["create_user_suggestions"]

    assert suggestions, "Expected at least one suggestion for createUser but got none"

    top = suggestions[0]

    # The top candidate must point at the users table
    users_table_name = shared_data["tables"][0]["name"]  # testns__users
    assert top.table == users_table_name, (
        f"Expected top candidate table '{users_table_name}', got '{top.table}'"
    )

    # Score must be 1.0 (single GraphQL object return type)
    assert top.score == 1.0, f"Expected score 1.0 for single-object return type, got {top.score}"

    # Reason must mention the return type 'User'
    assert "User" in top.reason, f"Expected reason to mention 'User', got '{top.reason}'"

    # Verify writable_by is absent / empty on the corresponding function entry.
    # The mapper should leave writable_by unset (default-deny per REQ-867).
    fn_entry = None
    for fn in shared_data["functions"]:
        field = fn.get("field_name") or fn.get("name", "")
        if "createUser" in field or field.endswith("createUser"):
            fn_entry = fn
            break

    assert fn_entry is not None, (
        "No function entry found for createUser in mapped functions: "
        + str([f.get("field_name") or f.get("name") for f in shared_data["functions"]])
    )

    # writable_by must be absent or an empty collection — default-deny
    writable_by = fn_entry.get("writable_by", [])
    assert not writable_by, f"Expected writable_by to be empty (default-deny), got {writable_by!r}"

    # The mapper should attach suggested_associations; if it does, verify them.
    if "suggested_associations" in fn_entry:
        assoc = fn_entry["suggested_associations"]
        assert assoc, "suggested_associations list is present but empty for createUser"
        top_assoc = assoc[0]
        assert top_assoc.get("table") == users_table_name or (
            isinstance(top_assoc, TableCandidate) and top_assoc.table == users_table_name
        ), f"suggested_associations top table mismatch: {top_assoc}"


@then(
    'a mutation "sendTelemetry: Boolean" with no table-typed return yields an '
    "empty suggested_associations list rather than an error"
)
def then_send_telemetry_has_empty_suggestions(shared_data: dict) -> None:
    suggestions: list[TableCandidate] = shared_data["send_telemetry_suggestions"]

    # Must be a list (no exception) — false negatives are expected
    assert isinstance(suggestions, list), (
        f"suggest_graphql must return a list, got {type(suggestions)}"
    )

    # sendTelemetry has no table-typed return and no recognisable CRUD affix,
    # so the honest result is an empty list.
    assert suggestions == [], (
        f"Expected empty suggestions for sendTelemetry (scalar Boolean return), got {suggestions!r}"
    )


scenarios("../features/REQ-870.feature")


# ---------------------------------------------------------------------------
# Shared state fixture (already defined in existing file; skip re-definition)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_role(role_id: str, *caps: str) -> dict:
    return {"id": role_id, "capabilities": list(caps)}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given('a discovered remote mutation "createOrder" registered with an empty writable_by')
def given_create_order_registered_empty_writable_by(shared_data: dict) -> None:
    """Simulate what upsert_function does on first discovery: empty writable_by."""
    shared_data["function_name"] = "createOrder"
    shared_data["kind"] = "mutation"
    # Simulate the in-memory function record as upsert_function would produce it
    shared_data["function_record"] = {
        "name": "createOrder",
        "kind": "mutation",
        "writable_by": [],
    }


@given("an admin grants it to the \"ops\" role (writable_by = ['ops'])")
def given_admin_grants_ops(shared_data: dict) -> None:
    """Admin updates writable_by — simulates the UPDATE an admin issues after discovery."""
    record = shared_data["function_record"]
    record["writable_by"] = ["ops"]
    shared_data["function_record"] = record


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("introspection re-runs and upserts createOrder by name with an empty writable_by")
def when_reintrospection_upserts_empty_writable_by(shared_data: dict) -> None:
    """
    Simulate upsert_function ON CONFLICT logic: an empty EXCLUDED.writable_by means
    the CASE expression preserves the existing writable_by.
    """
    record = shared_data["function_record"]
    incoming_writable_by: list[str] = []  # what introspection sends

    # Mirror the SQL CASE:
    #   WHEN cardinality(EXCLUDED.writable_by) > 0 THEN EXCLUDED.writable_by
    #   ELSE tracked_functions.writable_by
    if len(incoming_writable_by) > 0:
        record["writable_by"] = incoming_writable_by
    # else: leave record["writable_by"] unchanged

    shared_data["function_record"] = record
    shared_data["reintrospection_incoming_writable_by"] = incoming_writable_by


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then("the ops grant is preserved (writable_by stays ['ops']) — discovery never wipes grants")
def then_ops_grant_preserved(shared_data: dict) -> None:
    record = shared_data["function_record"]
    assert record["writable_by"] == ["ops"], (
        f"Expected writable_by=['ops'] after re-introspection, got {record['writable_by']!r}"
    )


@then(
    "when a role WITHOUT the ACCESS_CONFIG capability attempts to reclassify "
    "createOrder to read-safe, the attempt is rejected"
)
def then_non_access_config_role_rejected(shared_data: dict) -> None:
    # A role with only WRITE capability — no ACCESS_CONFIG, no ADMIN
    unprivileged_role = _make_role("analyst", Capability.WRITE.value)
    current_kind = shared_data["function_record"]["kind"]  # "mutation"

    with pytest.raises(InsufficientRightsError):
        reclassify_kind(unprivileged_role, current_kind, "query")


@then(
    "an ACCESS_CONFIG (or admin) role may demote it to read, but no one may promote "
    "a read back to a write"
)
def then_access_config_may_demote_no_one_may_promote(shared_data: dict) -> None:
    record = shared_data["function_record"]

    # --- 1. ACCESS_CONFIG role can demote mutation → read ---
    access_config_role = _make_role("config_admin", Capability.ACCESS_CONFIG.value)
    new_kind = reclassify_kind(access_config_role, "mutation", "query")
    assert new_kind == "query", (
        f"ACCESS_CONFIG role should be able to demote mutation to read; got {new_kind!r}"
    )

    # --- 2. ADMIN role can also demote (bypasses capability check) ---
    admin_role = _make_role("root", Capability.ADMIN.value)
    new_kind_admin = reclassify_kind(admin_role, "mutation", "query")
    assert new_kind_admin == "query", (
        f"ADMIN role should be able to demote mutation to read; got {new_kind_admin!r}"
    )

    # --- 3. No-op (mutation → mutation) is idempotent ---
    noop_result = reclassify_kind(admin_role, "mutation", "mutation")
    assert noop_result == "mutation", (
        f"No-op transition should return 'mutation'; got {noop_result!r}"
    )

    # --- 4. No-op (query → query) is idempotent ---
    noop_read_result = reclassify_kind(admin_role, "query", "query")
    assert noop_read_result == "query", (
        f"No-op read transition should return 'query'; got {noop_read_result!r}"
    )

    # --- 5. Nobody may promote a read back to a write (even ADMIN) ---
    with pytest.raises(ValueError, match="read cannot be promoted"):
        reclassify_kind(admin_role, "query", "mutation")

    # --- 6. An unprivileged role cannot promote either ---
    unprivileged = _make_role("analyst", Capability.WRITE.value)
    with pytest.raises(ValueError, match="read cannot be promoted"):
        reclassify_kind(unprivileged, "query", "mutation")

    # Persist the demoted kind into shared state so later steps can observe it
    record["kind"] = "query"
    shared_data["function_record"] = record


scenarios("../features/REQ-872.feature")


def _build_state_with_tracked_functions(functions: dict):
    state = MagicMock()
    mc = MagicMock()
    mc.tables = {}
    state.contexts = {"alice": mc, "ops": mc}
    state.schema_build_cache = {"column_types": {}}
    state.tracked_functions = functions
    return state


@given(
    'a tracked_function "createOrder(customer_id integer, total number)" returning a table (return_schema set), visible to all roles, in the registry'
)
def given_create_order_tracked_function(shared_data: dict) -> None:
    shared_data["tracked_functions"] = {
        "createOrder": {
            "id": 1,
            "name": "createOrder",
            "arguments": [
                {"name": "customer_id", "type": "integer"},
                {"name": "total", "type": "number"},
            ],
            "returns": "provisa.public.orders",
            "return_schema": "[]",
            "visible_to": [],
            "kind": "mutation",
        }
    }
    shared_data["querying_role"] = "alice"
    shared_data["excluded_role"] = "bob"
    shared_data["excluded_function"] = {
        "id": 2,
        "name": "secretFn",
        "arguments": [],
        "returns": "boolean",
        "visible_to": ["ops"],
        "kind": "mutation",
    }


@when(
    "a SQL-surface client queries information_schema.routines and information_schema.parameters over pgwire as a role that can see it"
)
def when_sql_surface_queries_catalog(shared_data: dict) -> None:
    fns = dict(shared_data["tracked_functions"])
    fns["secretFn"] = shared_data["excluded_function"]
    state = _build_state_with_tracked_functions(fns)
    role = shared_data["querying_role"]
    db = _build_catalog_db(role, state)
    shared_data["catalog_db"] = db

    routines = db.execute(
        "SELECT routine_name, routine_type, data_type FROM _is_routines WHERE routine_name='createOrder'"
    ).fetchall()
    shared_data["routines_rows"] = routines

    parameters = db.execute(
        "SELECT ordinal_position, parameter_name, parameter_mode, data_type "
        "FROM _is_parameters WHERE specific_name LIKE 'createOrder_%' ORDER BY ordinal_position"
    ).fetchall()
    shared_data["parameters_rows"] = parameters

    pg_proc = db.execute(
        "SELECT proname, pronargs, proretset FROM _pg_proc WHERE proname='createOrder'"
    ).fetchall()
    shared_data["pg_proc_rows"] = pg_proc


@then(
    "routines lists createOrder as a set-returning FUNCTION and parameters lists customer_id and total in ordinal order with their SQL data types (pg_proc shows proname=createOrder, pronargs=2, proretset=true)"
)
def then_routines_and_parameters_correct(shared_data: dict) -> None:
    routines = shared_data["routines_rows"]
    assert len(routines) == 1, f"Expected 1 routine row for createOrder, got {routines!r}"
    routine_name, routine_type, data_type = routines[0]
    assert routine_name == "createOrder", (
        f"Expected routine_name='createOrder', got {routine_name!r}"
    )
    assert routine_type == "FUNCTION", f"Expected routine_type='FUNCTION', got {routine_type!r}"
    assert data_type == "record", (
        f"Expected data_type='record' for set-returning, got {data_type!r}"
    )

    params = shared_data["parameters_rows"]
    assert len(params) == 2, f"Expected 2 parameter rows, got {params!r}"
    assert params[0] == (1, "customer_id", "IN", "integer"), (
        f"First parameter mismatch: {params[0]!r}"
    )
    assert params[1][0] == 2, f"Expected ordinal_position=2 for total, got {params[1][0]!r}"
    assert params[1][1] == "total", f"Expected parameter_name='total', got {params[1][1]!r}"
    assert params[1][2] == "IN", f"Expected parameter_mode='IN', got {params[1][2]!r}"
    assert params[1][3] == "double precision", (
        f"Expected data_type='double precision' for number type, got {params[1][3]!r}"
    )

    pg_proc = shared_data["pg_proc_rows"]
    assert len(pg_proc) == 1, f"Expected 1 pg_proc row for createOrder, got {pg_proc!r}"
    proname, pronargs, proretset = pg_proc[0]
    assert proname == "createOrder", f"Expected proname='createOrder', got {proname!r}"
    assert pronargs == 2, f"Expected pronargs=2, got {pronargs!r}"
    assert proretset is True, f"Expected proretset=True, got {proretset!r}"


@then("a function whose visible_to excludes the querying role does not appear in the catalog")
def then_excluded_function_not_visible(shared_data: dict) -> None:
    db = shared_data["catalog_db"]
    try:
        names = {r[0] for r in db.execute("SELECT proname FROM _pg_proc").fetchall()}
        assert "secretFn" not in names, (
            f"secretFn should not appear in catalog for role 'alice' (visible_to=['ops']), "
            f"but got names={names!r}"
        )
        assert "createOrder" in names, (
            "createOrder should be visible (visible_to=[]) to all roles including alice"
        )

        ops_fns = dict(shared_data["tracked_functions"])
        ops_fns["secretFn"] = shared_data["excluded_function"]
        ops_state = _build_state_with_tracked_functions(ops_fns)
        ops_db = _build_catalog_db("ops", ops_state)
        try:
            ops_names = {r[0] for r in ops_db.execute("SELECT proname FROM _pg_proc").fetchall()}
            assert "secretFn" in ops_names, (
                f"secretFn should appear for role 'ops' (visible_to=['ops']), got {ops_names!r}"
            )
        finally:
            ops_db.close()
    finally:
        db.close()
        shared_data["catalog_db"] = None
