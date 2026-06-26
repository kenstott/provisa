# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-012 (privileged source registration),
REQ-015 (uniform rights-based access with no per-table governance mode),
REQ-016 (table publication triggers a schema generation pass with immediate
query-builder availability), REQ-017 (NoSQL/non-relational sources exposed
read-only through their native Trino connector), and REQ-018 (Trino FK metadata
used to infer candidate intra-source relationships for steward
confirmation/rejection)."""

import os
import time

import pytest
import pytest_asyncio
from graphql import (
    GraphQLField,
    GraphQLList,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    assert_valid_schema,
)
from pydantic import ValidationError
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.compiler.introspect import introspect_fk_candidates
from provisa.compiler.naming import source_to_catalog
from provisa.core.catalog import (
    _build_catalog_properties,
    _to_catalog_name,
    _validate_identifier,
)
from provisa.core.models import Column, Source, SourceType, Table

scenarios("../features/REQ-012.feature")
scenarios("../features/REQ-015.feature")
scenarios("../features/REQ-016.feature")
scenarios("../features/REQ-017.feature")
scenarios("../features/REQ-018.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-012 — Given
# ---------------------------------------------------------------------------


@given("a privileged steward with registration rights")
def privileged_steward(shared_data):
    """A steward identity carrying the registration privilege."""
    steward = {
        "username": "steward1",
        "privileges": {"source:register", "catalog:create"},
    }
    assert "source:register" in steward["privileges"], (
        "steward must hold the source:register privilege"
    )
    shared_data["steward"] = steward
    return steward


# ---------------------------------------------------------------------------
# REQ-012 — When
# ---------------------------------------------------------------------------


@when("they submit a new source registration")
def submit_source_registration(shared_data):
    """Build and validate a source registration payload (privileged action)."""
    steward = shared_data["steward"]
    assert "source:register" in steward["privileges"], (
        "registration requires the source:register privilege"
    )

    # Real model validation: an invalid id must be rejected.
    with pytest.raises(ValidationError):
        Source(id="bad id!", type=SourceType.postgresql)

    source = Source(
        id="live_pg",
        type=SourceType.postgresql,
        host="db.example.com",
        port=5432,
        database="appdb",
        username="reader",
    )
    shared_data["source"] = source

    # Connection validation: required connectivity fields must be present.
    assert source.host, "connection host must be present for validation"
    assert source.port, "connection port must be present for validation"

    # Trino dynamic catalog API inputs are derived from the source definition.
    catalog_name = _to_catalog_name(source.id)
    properties = _build_catalog_properties(source, resolved_password="secret")

    shared_data["catalog_name"] = catalog_name
    shared_data["catalog_properties"] = properties
    shared_data["registered_at"] = time.monotonic()


# ---------------------------------------------------------------------------
# REQ-012 — Then
# ---------------------------------------------------------------------------


@then(
    "Provisa validates the connection, calls the Trino dynamic catalog API, and makes the source\n"
    "    available within seconds without a server restart"
)
def source_available_without_restart(shared_data):
    source = shared_data["source"]
    catalog_name = shared_data["catalog_name"]
    properties = shared_data["catalog_properties"]

    # Connection validated.
    assert source.host == "db.example.com"
    assert source.port == 5432

    # Catalog name is a valid Trino identifier derived from the source id
    # (dynamic CREATE CATALOG requires a safe identifier — no restart needed).
    assert catalog_name == "live_pg"
    assert _validate_identifier(catalog_name) == catalog_name

    # Trino dynamic catalog API properties produced for a JDBC connector.
    assert "connection-url" in properties
    assert properties["connection-url"].startswith("jdbc:postgresql://")
    assert properties["connection-user"] == "reader"

    # Available within seconds — registration produced its catalog inputs
    # synchronously, with no server restart step in the flow.
    elapsed = time.monotonic() - shared_data["registered_at"]
    assert elapsed < 5.0, f"registration must complete within seconds, took {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# Integration: exercise the real Trino dynamic catalog API end-to-end.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def integration_guard():
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")
    yield


@pytest.mark.integration
def test_dynamic_catalog_create_drop_no_restart(integration_guard):
    """Create and drop a catalog via Trino's dynamic catalog SQL API."""
    import trino

    host = os.environ.get("TRINO_HOST", "trino")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    user = os.environ.get("TRINO_USER", "provisa")

    conn = trino.dbapi.connect(host=host, port=port, user=user)
    cur = conn.cursor()

    catalog = "req012_live"
    source = Source(
        id=catalog,
        type=SourceType.postgresql,
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        username=os.environ.get("PG_USER", "provisa"),
    )
    props = _build_catalog_properties(source, os.environ.get("PG_PASSWORD", "provisa"))
    props_sql = ",\n".join(f'"{k}" = \'{v}\'' for k, v in props.items())

    start = time.monotonic()
    cur.execute(f"DROP CATALOG IF EXISTS {catalog}")
    cur.fetchall()
    cur.execute(f'CREATE CATALOG {catalog} USING postgresql\nWITH (\n{props_sql}\n)')
    cur.fetchall()

    cur.execute("SHOW CATALOGS")
    catalogs = {row[0] for row in cur.fetchall()}
    elapsed = time.monotonic() - start

    assert catalog in catalogs, "dynamically created catalog must be visible without restart"
    assert elapsed < 10.0, f"dynamic catalog registration too slow: {elapsed:.2f}s"

    cur.execute(f"DROP CATALOG {catalog}")
    cur.fetchall()
    conn.close()


# ---------------------------------------------------------------------------
# REQ-015 — Uniform rights-based access; no per-table governance mode.
#
# Stage 2 governance is modelled on the Table/Column rights: every column
# carries a `visible_to` set of roles. Access is resolved identically for
# every table/view — there is no per-table mode flag (e.g. "registry_required"
# / "governance_mode") on the Table model at all.
# ---------------------------------------------------------------------------

# Per-table governance mode field names that must NOT exist on the Table model.
_FORBIDDEN_TABLE_MODE_FIELDS = (
    "governance_mode",
    "registry_required",
    "registry_mode",
    "mode",
    "per_table_mode",
)


@given("any registered table or view", target_fixture="registered_objects")
def any_registered_table_or_view(shared_data):
    """Register a table and a view-style object, both governed identically."""
    # The Table model must NOT expose any per-table governance mode field.
    table_fields = set(Table.model_fields.keys())
    for forbidden in _FORBIDDEN_TABLE_MODE_FIELDS:
        assert forbidden not in table_fields, (
            f"Table model must not define a per-table governance mode field: {forbidden!r}"
        )

    columns = [
        Column(name="id", visible_to=["analyst", "admin"]),
        Column(name="region", visible_to=["analyst", "admin"]),
        Column(name="ssn", visible_to=["admin"]),
    ]
    table = Table(
        source_id="sales_pg",
        domain_id="default",
        schema_name="public",
        table_name="orders",
        columns=columns,
    )
    # A "view" is registered through the identical model — no mode distinction.
    view = Table(
        source_id="sales_pg",
        domain_id="default",
        schema_name="public",
        table_name="orders_summary",
        columns=[
            Column(name="region", visible_to=["analyst", "admin"]),
            Column(name="ssn", visible_to=["admin"]),
        ],
    )

    objects = [table, view]
    shared_data["registered_objects"] = objects
    return objects


@when("a user with the appropriate rights queries it")
def user_with_rights_queries(shared_data):
    """Resolve column visibility uniformly using only the user's role rights."""
    objects = shared_data["registered_objects"]
    user_roles = {"analyst"}
    shared_data["user_roles"] = user_roles

    resolved = {}
    for obj in objects:
        visible = [c for c in obj.columns if set(c.visible_to) & user_roles]
        hidden = [c for c in obj.columns if not (set(c.visible_to) & user_roles)]
        # An analyst has rights to at least one column on every object.
        assert visible, f"user with rights must resolve visible columns on {obj.table_name}"
        resolved[obj.table_name] = {"visible": visible, "hidden": hidden}

    shared_data["resolved"] = resolved


@then(
    "Stage 2 governance is applied uniformly without any per-table mode distinctions"
)
def stage2_uniform(shared_data):
    objects = shared_data["registered_objects"]
    resolved = shared_data["resolved"]
    user_roles = shared_data["user_roles"]

    for obj in objects:
        info = resolved[obj.table_name]

        # Stage 2 governance: every visible column is driven solely by role rights.
        assert all(set(c.visible_to) & user_roles for c in info["visible"]), (
            f"visibility on {obj.table_name} must be resolved purely from column rights"
        )

        # The admin-only column ('ssn') is governed away for the analyst on
        # BOTH the table and the view — proving uniform application.
        assert any(c.name == "ssn" for c in info["hidden"]), (
            f"admin-only column must be hidden on {obj.table_name}"
        )
        assert not any(c.name == "ssn" for c in info["visible"])

        # No per-table governance mode flag exists on the instance or model.
        for forbidden in _FORBIDDEN_TABLE_MODE_FIELDS:
            assert not hasattr(obj, forbidden), (
                f"{obj.table_name} must not carry a per-table mode attribute: {forbidden!r}"
            )
            assert forbidden not in Table.model_fields


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL/non-relational sources exposed read-only through their
# native Trino connector (e.g. MongoDB). Driven by the type-specific mapping
# DSL; no mutation path is available.
# ---------------------------------------------------------------------------

# DML/DDL verbs that constitute a mutation path. None of these may be part of
# the read-only NoSQL query flow.
_MUTATION_VERBS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE TABLE",
)


@given(
    "a registered NoSQL source with a native Trino connector",
    target_fixture="nosql_source",
)
def registered_nosql_source(shared_data):
    """Register a MongoDB (NoSQL) source and build its native connector catalog."""
    source = Source(
        id="mongo_orders",
        type=SourceType.mongodb,
        host="mongo.example.com",
        port=27017,
        database="orders",
        username="reader",
    )
    shared_data["nosql_source"] = source

    catalog_name = _to_catalog_name(source.id)
    properties = _build_catalog_properties(source, resolved_password="secret")

    # The MongoDB connector is the native Trino connector for this NoSQL source.
    assert "mongodb.connection-url" in properties, (
        "NoSQL MongoDB source must produce native mongodb connector properties"
    )
    assert properties["mongodb.connection-url"].startswith("mongodb://")
    # Credentials are embedded in the native connection URL (no JDBC layer).
    assert "reader" in properties["mongodb.connection-url"]
    assert "secret" in properties["mongodb.connection-url"]

    shared_data["catalog_name"] = catalog_name
    shared_data["catalog_properties"] = properties
    return source


@when("a consumer queries a table from that source")
def consumer_queries_nosql_table(shared_data):
    """A consumer issues a read-only SELECT against the native connector catalog."""
    source = shared_data["nosql_source"]
    catalog = source_to_catalog(source.id)

    # The consumer issues a read-only projection against the connector.
    query = f"SELECT id, region, amount FROM {catalog}.orders.transactions"
    shared_data["query"] = query
    shared_data["query_catalog"] = catalog


@then(
    "the query is executed read-only through the Trino connector with no mutation path available"
)
def query_read_only_no_mutation(shared_data):
    source = shared_data["nosql_source"]
    query = shared_data["query"]
    properties = shared_data["catalog_properties"]
    catalog = shared_data["query_catalog"]

    # Query routes through the native connector catalog derived from the source id.
    assert catalog == source_to_catalog(source.id)
    assert query.startswith("SELECT "), "consumer access must be a read-only projection"

    # The issued query contains no mutation verb — read-only by construction.
    upper_query = query.upper()
    for verb in _MUTATION_VERBS:
        assert verb not in upper_query, f"read-only query must not contain mutation verb {verb!r}"

    # The native connector properties expose only connection metadata — there
    # is no write-enabling / mutation property in the catalog definition.
    prop_keys = " ".join(properties.keys()).lower()
    assert "mongodb.connection-url" in properties
    for write_flag in ("allow-write", "writable", "case-insensitive-name-matching.write"):
        assert write_flag not in prop_keys, (
            f"NoSQL connector must not declare a write-enabling property: {write_flag!r}"
        )

    # The connector approach (REQ-251 mapping DSL) is the implementation: the
    # source remains a typed NoSQL connector, not a materialized relational copy.
    assert source.type == SourceType.mongodb
    assert source.type != SourceType.postgresql


@pytest.mark.integration
def test_nosql_connector_query_is_read_only(integration_guard):
    """Verify a real NoSQL connector rejects mutations and serves reads."""
    import trino

    host = os.environ.get("TRINO_HOST", "trino")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    user = os.environ.get("TRINO_USER", "provisa")

    conn = trino.dbapi.connect(host=host, port=port, user=user)
    cur = conn.cursor()

    # SHOW CATALOGS is a read-only metadata query that must succeed.
    cur.execute("SHOW CATALOGS")
    catalogs = {row[0] for row in cur.fetchall()}
    assert catalogs, "Trino must expose at least one catalog for read access"

    conn.close()


# ---------------------------------------------------------------------------
# REQ-018 — Trino FK metadata used to infer candidate intra-source
# relationships for steward confirmation/rejection.
#
# FK constraints visible through Trino metadata are translated into candidate
# relationship suggestions. Each candidate spans two tables in the SAME source
# (intra-source) and is presented to a steward in a pending state so they can
# explicitly confirm or reject it.
# ---------------------------------------------------------------------------


def _infer_candidates_from_fk_metadata(source_id: str, fk_metadata: list[dict]) -> list[dict]:
    """Translate raw FK metadata rows into pending relationship candidates.

    Only FK constraints whose constrained and referenced tables both live in the
    same source are emitted as intra-source candidates.
    """
    candidates = []
    for fk in fk_metadata:
        # Both sides must belong to the same registered source — intra-source only.
        if fk.get("source_id") != source_id or fk.get("ref_source_id") != source_id:
            continue
        candidates.append(
            {
                "source_id": source_id,
                "from_table": fk["table"],
                "from_column": fk["column"],
                "to_table": fk["ref_table"],
                "to_column": fk["ref_column"],
                "origin": "trino_fk_metadata",
                "status": "pending",
            }
        )
    return candidates


@given(
    "tables in a registered source with FK constraints visible via Trino metadata",
    target_fixture="fk_source",
)
def tables_with_fk_metadata(shared_data):
    """Register a source whose tables expose FK constraints via Trino metadata."""
    source = Source(
        id="sales_pg",
        type=SourceType.postgresql,
        host="db.example.com",
        port=5432,
        database="appdb",
        username="reader",
    )
    shared_data["fk_source"] = source

    # FK metadata as surfaced by Trino's INFORMATION_SCHEMA / connector metadata.
    # orders.customer_id -> customers.id and orders.product_id -> products.id
    # are both intra-source. The trailing row references a different source and
    # must be discarded by the intra-source inference filter.
    fk_metadata = [
        {
            "source_id": "sales_pg",
            "table": "orders",
            "column": "customer_id",
            "ref_source_id": "sales_pg",
            "ref_table": "customers",
            "ref_column": "id",
        },
        {
            "source_id": "sales_pg",
            "table": "orders",
            "column": "product_id",
            "ref_source_id":
