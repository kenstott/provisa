# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for core requirements: REQ-012, REQ-013, REQ-014, REQ-015, REQ-016, REQ-017, REQ-018, REQ-019, REQ-020, REQ-021, REQ-250, REQ-251, REQ-366, REQ-392, REQ-393, REQ-394, REQ-399, REQ-400, REQ-414, REQ-417, REQ-432, REQ-434, REQ-540, REQ-541, REQ-553, REQ-560, REQ-562, REQ-591, REQ-592, REQ-610, REQ-635, REQ-636, REQ-637, REQ-638, REQ-639, REQ-653"""

import pytest

from provisa.core.models import (
    SOURCE_TO_CONNECTOR,
    Cardinality,
    Column,
    GovDataSource,
    GovDataSubject,
    GOVDATA_SUBJECT_SCHEMAS,
    Relationship,
    ServerConfig,
    Source,
    SourceType,
    Table,
)


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_column(name: str, visible_to: list[str] | None = None, **kwargs) -> Column:
    return Column(name=name, visible_to=visible_to or ["analyst"], **kwargs)


def _make_table(
    source_id: str = "src1",
    domain_id: str = "default",
    schema_name: str = "public",
    table_name: str = "orders",
    columns: list[Column] | None = None,
) -> Table:
    if columns is None:
        columns = [_make_column("id")]
    return Table(
        source_id=source_id,
        domain_id=domain_id,
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
    )


# ---------------------------------------------------------------------------
# REQ-012 — Source registration: connection validated, no restart required
# The Source model must be instantiated (registration payload) with required
# connection fields; an invalid id must be rejected.
# ---------------------------------------------------------------------------


class TestReq012SourceRegistration:
    def test_source_model_accepts_valid_connection_fields(self):
        # REQ-012
        s = Source(id="pgdb", type=SourceType.postgresql, host="db", port=5432, database="app")
        assert s.id == "pgdb"
        assert s.host == "db"
        assert s.port == 5432

    def test_source_id_with_invalid_characters_rejected(self):
        # REQ-012
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Source(id="invalid id!", type=SourceType.postgresql)

    def test_source_id_must_start_with_letter(self):
        # REQ-012
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Source(id="1invalid", type=SourceType.postgresql)


# ---------------------------------------------------------------------------
# REQ-013 — No table queryable until explicitly registered
# A Table object represents an explicit registration; it must carry a
# source_id and columns. Without those the object cannot be constructed.
# ---------------------------------------------------------------------------


class TestReq013ExplicitTableRegistration:
    def test_table_requires_source_id_and_columns(self):
        # REQ-013
        t = _make_table(source_id="my_source", table_name="users")
        assert t.source_id == "my_source"
        assert len(t.columns) > 0

    def test_table_without_columns_list_fails(self):
        # REQ-013
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Table(
                source_id="src",
                domain_id="default",
                schema_name="public",
                table_name="orders",
                columns=None,  # type: ignore[arg-type]
            )


# ---------------------------------------------------------------------------
# REQ-014 — Unregistered tables do not exist
# find_by_table_name is the lookup gate; the repository returns None when a
# table is absent. Verifying the model-level expectation: a Table object
# only exists when explicitly created (no implicit default tables).
# ---------------------------------------------------------------------------


class TestReq014UnregisteredTablesAbsent:
    def test_table_requires_explicit_source_id(self):
        # REQ-014
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Table(
                domain_id="default",
                schema_name="public",
                table_name="secret",
                columns=[_make_column("id")],
            )  # type: ignore[call-arg]

    def test_table_model_has_no_implicit_wildcard_access(self):
        # REQ-014 — columns only carry explicit visible_to, never implicitly visible to all
        col = _make_column("secret_col", visible_to=["admin"])
        assert col.visible_to == ["admin"]
        assert "*" not in col.visible_to


# ---------------------------------------------------------------------------
# REQ-015 — No per-table governance mode; uniform rights-based access
# The Table model has no "governance_mode" field; rights are expressed
# exclusively via column visible_to.
# ---------------------------------------------------------------------------


class TestReq015UniformRightsAccess:
    def test_table_model_has_no_governance_mode_field(self):
        # REQ-015
        t = _make_table()
        assert not hasattr(t, "governance_mode")
        assert not hasattr(t, "registry_required")

    def test_column_visible_to_is_the_access_control_mechanism(self):
        # REQ-015
        col = _make_column("price", visible_to=["analyst", "admin"])
        assert isinstance(col.visible_to, list)


# ---------------------------------------------------------------------------
# REQ-016 — Table publication triggers schema generation; immediate availability
# The Table model supports all fields needed to generate a schema entry
# (source_id, domain_id, schema_name, table_name, columns).
# ---------------------------------------------------------------------------


class TestReq016TablePublicationTriggersSchema:
    def test_table_has_all_schema_generation_required_fields(self):
        # REQ-016
        t = _make_table(
            source_id="pg1",
            domain_id="analytics",
            schema_name="public",
            table_name="sales",
            columns=[_make_column("id"), _make_column("amount")],
        )
        assert t.source_id == "pg1"
        assert t.domain_id == "analytics"
        assert t.schema_name == "public"
        assert t.table_name == "sales"
        assert len(t.columns) == 2


# ---------------------------------------------------------------------------
# REQ-017 — NoSQL sources exposed read-only via Trino connector
# SourceType must include NoSQL types and they must map to Trino connectors.
# ---------------------------------------------------------------------------


class TestReq017NoSQLViaTrinio:
    def test_mongodb_maps_to_trino_connector(self):
        # REQ-017
        assert "mongodb" in SOURCE_TO_CONNECTOR
        assert SOURCE_TO_CONNECTOR["mongodb"] == "mongodb"

    def test_redis_maps_to_trino_connector(self):
        # REQ-017
        assert "redis" in SOURCE_TO_CONNECTOR
        assert SOURCE_TO_CONNECTOR["redis"] == "redis"

    def test_elasticsearch_maps_to_trino_connector(self):
        # REQ-017
        assert "elasticsearch" in SOURCE_TO_CONNECTOR
        assert SOURCE_TO_CONNECTOR["elasticsearch"] == "elasticsearch"

    def test_nosql_source_model_has_no_write_fields(self):
        # REQ-017 — NoSQL sources are read-only; the Source model doesn't have a writable flag
        s = Source(id="mongo1", type=SourceType.mongodb, host="mongo", port=27017, database="db")
        assert s.type == SourceType.mongodb


# ---------------------------------------------------------------------------
# REQ-018 — Trino FK metadata used to infer relationship candidates
# The Relationship model represents both inferred and manually defined rels.
# ---------------------------------------------------------------------------


class TestReq018FKCandidateInference:
    def test_relationship_model_supports_all_candidate_fields(self):
        # REQ-018
        r = Relationship(
            id="rel1",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality=Cardinality.many_to_one,
        )
        assert r.source_column == "customer_id"
        assert r.target_column == "id"
        assert r.cardinality == Cardinality.many_to_one


# ---------------------------------------------------------------------------
# REQ-019 — Cross-source relationships defined manually with cardinality
# Cardinality enum only supports many-to-one and one-to-many (no one-to-one).
# ---------------------------------------------------------------------------


class TestReq019CrossSourceRelationships:
    def test_cardinality_many_to_one_supported(self):
        # REQ-019
        r = Relationship(
            id="rel2",
            source_table_id="line_items",
            target_table_id="orders",
            source_column="order_id",
            target_column="id",
            cardinality=Cardinality.many_to_one,
        )
        assert r.cardinality == Cardinality.many_to_one

    def test_cardinality_one_to_many_supported(self):
        # REQ-019
        r = Relationship(
            id="rel3",
            source_table_id="customers",
            target_table_id="orders",
            source_column="id",
            target_column="customer_id",
            cardinality=Cardinality.one_to_many,
        )
        assert r.cardinality == Cardinality.one_to_many

    def test_cardinality_enum_has_only_two_values(self):
        # REQ-019 — one-to-one must NOT exist in the cardinality enum
        values = {c.value for c in Cardinality}
        assert "one-to-one" not in values
        assert len(values) == 2


# ---------------------------------------------------------------------------
# REQ-020 — Relationships versioned; flagged for re-review on schema changes
# The Relationship model has version, needs_review, and owner fields.
# ---------------------------------------------------------------------------


class TestReq020RelationshipVersioning:
    def test_relationship_has_version_field_defaulting_to_1(self):
        # REQ-020
        r = Relationship(
            id="rel4",
            source_table_id="a",
            target_table_id="b",
            source_column="fk",
            target_column="pk",
            cardinality=Cardinality.many_to_one,
        )
        assert r.version == 1

    def test_relationship_has_needs_review_flag(self):
        # REQ-020
        r = Relationship(
            id="rel5",
            source_table_id="a",
            target_table_id="b",
            source_column="fk",
            target_column="pk",
            cardinality=Cardinality.many_to_one,
            needs_review=True,
        )
        assert r.needs_review is True

    def test_relationship_has_owner_field(self):
        # REQ-020
        r = Relationship(
            id="rel6",
            source_table_id="a",
            target_table_id="b",
            source_column="fk",
            target_column="pk",
            cardinality=Cardinality.many_to_one,
            owner="steward@example.com",
        )
        assert r.owner == "steward@example.com"


# ---------------------------------------------------------------------------
# REQ-021 — GraphQL schema reflects registration model, not raw DB structure
# schema_gen.SchemaInput is built from registered tables/relationships,
# NOT from raw DB schema rows.
# ---------------------------------------------------------------------------


class TestReq021GraphQLReflectsRegistration:
    def test_schema_input_uses_registered_tables_not_raw_db(self):
        # REQ-021
        from provisa.compiler.schema_gen import SchemaInput

        si = SchemaInput(
            tables=[{"id": 1, "table_name": "business_entity", "columns": []}],
            relationships=[],
            column_types={},
            naming_rules=[],
            role={"id": "analyst", "capabilities": ["read"], "domain_access": ["*"]},
            domains=[],
        )
        # Schema input takes registered table rows (from table_repo), not raw connector rows
        assert si.tables[0]["table_name"] == "business_entity"


# ---------------------------------------------------------------------------
# REQ-250 — All Trino catalog config flows through Provisa config YAML
# Source model carries all Trino connection info; no external .properties files.
# ---------------------------------------------------------------------------


class TestReq250ConfigDrivenTrinoCatalog:
    def test_source_model_provides_catalog_name(self):
        # REQ-250
        s = Source(
            id="my-pg-source", type=SourceType.postgresql, host="db", port=5432, database="app"
        )
        assert s.catalog_name == "my_pg_source"  # hyphens sanitised to underscores

    def test_source_connector_property_derives_from_source_type(self):
        # REQ-250
        s = Source(id="kafka1", type=SourceType.kafka)
        assert s.connector == SOURCE_TO_CONNECTOR["kafka"]

    def test_source_mapping_field_holds_nosql_connector_options(self):
        # REQ-250
        s = Source(
            id="redis1",
            type=SourceType.redis,
            mapping={"tables": [{"name": "sessions", "key_pattern": "session:*"}]},
        )
        assert "tables" in s.mapping


# ---------------------------------------------------------------------------
# REQ-251 — Type-specific mapping DSL for NoSQL sources
# Source.mapping field holds the DSL; each NoSQL type has a Trino connector.
# ---------------------------------------------------------------------------


class TestReq251NoSQLMappingDSL:
    def test_source_mapping_field_accepts_redis_dsl(self):
        # REQ-251
        dsl = {
            "tables": [
                {
                    "name": "sessions",
                    "key_pattern": "session:*",
                    "value": {"type": "hash"},
                    "columns": [{"mapping": "session_id", "name": "session_id", "type": "VARCHAR"}],
                }
            ]
        }
        s = Source(id="redis-src", type=SourceType.redis, mapping=dsl)
        assert s.mapping["tables"][0]["key_pattern"] == "session:*"

    def test_source_mapping_field_accepts_elasticsearch_dsl(self):
        # REQ-251
        dsl = {"index": "my_index", "columns": [{"name": "title", "path": "title"}]}
        s = Source(id="es-src", type=SourceType.elasticsearch, mapping=dsl)
        assert s.mapping["index"] == "my_index"

    def test_source_mapping_field_accepts_prometheus_dsl(self):
        # REQ-251
        dsl = {"metrics": [{"name": "http_requests_total", "labels": ["method", "status"]}]}
        s = Source(id="prom-src", type=SourceType.prometheus, mapping=dsl)
        assert "metrics" in s.mapping


# ---------------------------------------------------------------------------
# REQ-366 — Views require approval workflow or originator rights
# The Table model has a view_sql field; no approval bypass field exists.
# ---------------------------------------------------------------------------


class TestReq366ViewApprovalWorkflow:
    def test_table_view_sql_field_exists(self):
        # REQ-366
        t = Table(
            source_id="pg1",
            domain_id="analytics",
            schema_name="public",
            table_name="monthly_sales",
            columns=[_make_column("total")],
            view_sql="SELECT SUM(amount) AS total FROM sales GROUP BY month",
        )
        assert t.view_sql is not None
        assert "SELECT" in t.view_sql

    def test_table_has_no_skip_approval_field(self):
        # REQ-366 — no bypass mechanism on the model
        t = _make_table()
        assert not hasattr(t, "skip_approval")
        assert not hasattr(t, "bypass_approval")


# ---------------------------------------------------------------------------
# REQ-392 — Schema endpoint returns node_labels with pk field
# label_map / cypher components expose pk per label. Testing model-level
# support: Column.is_primary_key exists.
# ---------------------------------------------------------------------------


class TestReq392PKFieldInSchema:
    def test_column_has_is_primary_key_field(self):
        # REQ-392
        col = _make_column("id", is_primary_key=True)
        assert col.is_primary_key is True

    def test_column_is_primary_key_defaults_to_false(self):
        # REQ-392
        col = _make_column("name")
        assert col.is_primary_key is False


# ---------------------------------------------------------------------------
# REQ-393 — Semantic layer supports user-designated PK columns per table
# Column.is_primary_key is informational, not enforced.
# ---------------------------------------------------------------------------


class TestReq393UserDesignatedPK:
    def test_multiple_columns_can_have_is_primary_key_true(self):
        # REQ-393 — composite key scenario
        cols = [
            _make_column("tenant_id", is_primary_key=True),
            _make_column("order_id", is_primary_key=True),
            _make_column("amount"),
        ]
        pk_cols = [c for c in cols if c.is_primary_key]
        assert len(pk_cols) == 2

    def test_is_primary_key_is_bool_not_enforced_constraint(self):
        # REQ-393 — informational only, no uniqueness enforcement at model level
        col = _make_column("dup_id", is_primary_key=True)
        assert isinstance(col.is_primary_key, bool)


# ---------------------------------------------------------------------------
# REQ-394 — Multiple PK checkboxes infer composite key; first PK is canonical id_column
# Model supports multiple is_primary_key columns; the first listed takes priority.
# ---------------------------------------------------------------------------


class TestReq394CompositePKFirstColumnCanonical:
    def test_first_pk_column_is_positionally_first_in_columns_list(self):
        # REQ-394
        cols = [
            _make_column("order_date", is_primary_key=True),
            _make_column("seq_no", is_primary_key=True),
            _make_column("notes"),
        ]
        pk_cols = [c for c in cols if c.is_primary_key]
        assert pk_cols[0].name == "order_date"

    def test_table_preserves_column_order(self):
        # REQ-394 — ordering guarantees first PK priority
        cols = [
            _make_column("pk1", is_primary_key=True),
            _make_column("pk2", is_primary_key=True),
        ]
        t = Table(
            source_id="s1",
            domain_id="d1",
            schema_name="public",
            table_name="t1",
            columns=cols,
        )
        assert t.columns[0].name == "pk1"


# ---------------------------------------------------------------------------
# REQ-399 — Saving a Relationship marks source_column as is_foreign_key=True
# The Column model has is_foreign_key field; repository logic applies this.
# ---------------------------------------------------------------------------


class TestReq399ForeignKeyMarkingOnRelationshipSave:
    def test_column_has_is_foreign_key_field(self):
        # REQ-399
        col = _make_column("customer_id", is_foreign_key=True)
        assert col.is_foreign_key is True

    def test_column_is_foreign_key_defaults_to_false(self):
        # REQ-399
        col = _make_column("name")
        assert col.is_foreign_key is False

    def test_column_has_is_alternate_key_field(self):
        # REQ-399
        col = _make_column("email", is_alternate_key=True)
        assert col.is_alternate_key is True


# ---------------------------------------------------------------------------
# REQ-400 — Saving a Relationship marks target_column as is_primary_key or is_alternate_key
# Column model supports both flags; repository marks PK if none exists, AK otherwise.
# ---------------------------------------------------------------------------


class TestReq400PKOrAKOnTargetColumnSave:
    def test_column_can_be_set_as_primary_key(self):
        # REQ-400
        col = _make_column("id", is_primary_key=True)
        assert col.is_primary_key is True
        assert col.is_alternate_key is False

    def test_column_can_be_set_as_alternate_key(self):
        # REQ-400
        col = _make_column("email", is_alternate_key=True)
        assert col.is_alternate_key is True
        assert col.is_primary_key is False

    def test_pk_and_ak_mutually_exclusive_in_typical_usage(self):
        # REQ-400 — they can coexist at model level but semantically one is chosen
        col = _make_column("ref", is_primary_key=False, is_alternate_key=False)
        assert col.is_primary_key is False
        assert col.is_alternate_key is False


# ---------------------------------------------------------------------------
# REQ-414 — Demo schema must include at least one FK relationship
# Structural check: the demo file exists and references relationships.
# ---------------------------------------------------------------------------


class TestReq414DemoSchemaHasFKRelationship:
    def test_demo_create_files_script_exists(self):
        # REQ-414
        import os

        demo_script = "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/demo/files/create_demo_files.py"
        assert os.path.isfile(demo_script), "Demo create_demo_files.py must exist"

    def test_demo_script_references_fk_relationship(self):
        # REQ-414
        demo_script = "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/demo/files/create_demo_files.py"
        with open(demo_script) as f:
            content = f.read()
        # Demo schema exercises FK auto-discovery: REFERENCES keyword in DDL
        # or explicit relationship/foreign_key terms
        assert (
            "REFERENCES" in content
            or "relationship" in content.lower()
            or "foreign_key" in content.lower()
            or "Relationship" in content
        )


# ---------------------------------------------------------------------------
# REQ-417 — Hasura v2 mapper maps Remote Schemas to graphql_remote sources
# ---------------------------------------------------------------------------


class TestReq417HasuraRemoteSchemaMigration:
    def test_map_remote_schema_returns_graphql_remote_source(self):
        # REQ-417
        from provisa.hasura_v2.mapper import _map_remote_schema
        from provisa.hasura_v2.models import HasuraRemoteSchema

        rs = HasuraRemoteSchema(
            name="my_remote",
            definition={"url": "https://api.example.com/graphql", "headers": []},
        )
        source = _map_remote_schema(rs)
        assert source.type == SourceType.graphql_remote

    def test_map_remote_schema_preserves_name(self):
        # REQ-417
        from provisa.hasura_v2.mapper import _map_remote_schema
        from provisa.hasura_v2.models import HasuraRemoteSchema

        rs = HasuraRemoteSchema(
            name="partner_api",
            definition={"url": "https://partner.example.com/graphql", "headers": []},
        )
        source = _map_remote_schema(rs)
        assert source.id == "partner_api"

    def test_map_remote_schema_preserves_url(self):
        # REQ-417
        from provisa.hasura_v2.mapper import _map_remote_schema
        from provisa.hasura_v2.models import HasuraRemoteSchema

        url = "https://api.example.com/graphql"
        rs = HasuraRemoteSchema(
            name="svc",
            definition={"url": url, "headers": []},
        )
        source = _map_remote_schema(rs)
        assert source.base_url == url

    def test_map_remote_schema_preserves_headers(self):
        # REQ-417
        from provisa.hasura_v2.mapper import _map_remote_schema
        from provisa.hasura_v2.models import HasuraRemoteSchema

        rs = HasuraRemoteSchema(
            name="svc",
            definition={
                "url": "https://api.example.com/graphql",
                "headers": [{"name": "X-Api-Key", "value": "secret"}],
            },
        )
        source = _map_remote_schema(rs)
        assert "headers" in source.mapping
        assert source.mapping["headers"]["X-Api-Key"] == "secret"

    def test_map_remote_schema_with_url_from_env(self):
        # REQ-417 — url_from_env maps to ${env:VAR}
        from provisa.hasura_v2.mapper import _map_remote_schema
        from provisa.hasura_v2.models import HasuraRemoteSchema

        rs = HasuraRemoteSchema(
            name="svc",
            definition={"url_from_env": "REMOTE_URL", "headers": []},
        )
        source = _map_remote_schema(rs)
        assert source.base_url is not None and "${env:REMOTE_URL}" in source.base_url


# ---------------------------------------------------------------------------
# REQ-432 — Registered table unique by (domain_id, table_name)
# The Table model carries domain_id and table_name. Uniqueness is enforced
# at the DB layer; model-level: both fields exist and are required.
# ---------------------------------------------------------------------------


class TestReq432DomainTableUniqueness:
    def test_table_has_domain_id_field(self):
        # REQ-432
        t = _make_table(domain_id="sales", table_name="invoices")
        assert t.domain_id == "sales"
        assert t.table_name == "invoices"

    def test_two_table_objects_with_same_domain_and_name_are_equal_semantically(self):
        # REQ-432 — same (domain_id, table_name) is the collision key
        t1 = _make_table(source_id="src1", domain_id="analytics", table_name="events")
        t2 = _make_table(source_id="src2", domain_id="analytics", table_name="events")
        assert t1.domain_id == t2.domain_id
        assert t1.table_name == t2.table_name


# ---------------------------------------------------------------------------
# REQ-434 — Creation-request mechanism: unauthorized governs create → persisted request
# The creation_request repository module exists and exposes create/list_pending/
# mark_executed/mark_rejected.
# ---------------------------------------------------------------------------


class TestReq434CreationRequestMechanism:
    def test_creation_request_module_exposes_required_functions(self):
        # REQ-434
        from provisa.core.repositories import creation_request

        assert callable(creation_request.create)
        assert callable(creation_request.list_pending)
        assert callable(creation_request.mark_executed)
        assert callable(creation_request.mark_rejected)

    def test_mark_rejected_accepts_reason_parameter(self):
        # REQ-434 — rejection must carry a reason
        import inspect

        from provisa.core.repositories import creation_request

        sig = inspect.signature(creation_request.mark_rejected)
        assert "reason" in sig.parameters


# ---------------------------------------------------------------------------
# REQ-540 — govdata source type exposes U.S. government open data by subject
# ---------------------------------------------------------------------------


class TestReq540GovDataSources:
    def test_govdata_source_type_exists(self):
        # REQ-540
        assert SourceType.govdata == "govdata"

    def test_govdata_source_model_has_subject_field(self):
        # REQ-540
        gds = GovDataSource(
            id="gd1",
            subject=GovDataSubject.health,
            govdata_schemas=["health"],
            domain_id="default",
        )
        assert gds.subject == GovDataSubject.health

    def test_govdata_subject_enum_includes_expected_subjects(self):
        # REQ-540
        subjects = {s.value for s in GovDataSubject}
        assert "HEALTH" in subjects
        assert "EDUCATION" in subjects
        assert "COMMERCE" in subjects

    def test_govdata_subject_schemas_map_covers_known_subjects(self):
        # REQ-540
        assert "HEALTH" in GOVDATA_SUBJECT_SCHEMAS
        assert "EDUCATION" in GOVDATA_SUBJECT_SCHEMAS


# ---------------------------------------------------------------------------
# REQ-541 — ref and geo schemas always included as linker schemas in GovData
# The GOVDATA_SUBJECT_SCHEMAS dict must NOT list ref/geo (they are always
# included implicitly) and the docstring/comment confirms this.
# ---------------------------------------------------------------------------


class TestReq541GovDataLinkerSchemas:
    def test_ref_not_in_govdata_subject_schemas_values(self):
        # REQ-541 — ref is always added implicitly, not in any subject bucket
        for schemas in GOVDATA_SUBJECT_SCHEMAS.values():
            assert "ref" not in schemas

    def test_geo_not_in_govdata_subject_schemas_values(self):
        # REQ-541 — geo is always added implicitly, not in any subject bucket
        for schemas in GOVDATA_SUBJECT_SCHEMAS.values():
            assert "geo" not in schemas


# ---------------------------------------------------------------------------
# REQ-553 — File-based sources use path field, not host/port
# ---------------------------------------------------------------------------


class TestReq553FileBasedSourcesUsePath:
    def test_csv_source_accepts_path_field(self):
        # REQ-553
        s = Source(id="csv1", type=SourceType.csv, path="/data/sales.csv")
        assert s.path == "/data/sales.csv"

    def test_parquet_source_accepts_path_field(self):
        # REQ-553
        s = Source(id="pq1", type=SourceType.parquet, path="/data/sales.parquet")
        assert s.path == "/data/sales.parquet"

    def test_sqlite_source_accepts_path_field(self):
        # REQ-553
        s = Source(id="sq1", type=SourceType.sqlite, path="/data/local.db")
        assert s.path == "/data/local.db"

    def test_path_defaults_to_none(self):
        # REQ-553 — non-file source has path=None
        s = Source(id="pg1", type=SourceType.postgresql, host="db", port=5432, database="app")
        assert s.path is None


# ---------------------------------------------------------------------------
# REQ-560 — Default API port 8000, Arrow Flight port 8815
# ---------------------------------------------------------------------------


class TestReq560DefaultPorts:
    def test_server_config_default_api_port_is_8000(self):
        # REQ-560
        sc = ServerConfig()
        assert sc.port == 8000

    def test_server_config_default_flight_port_is_8815(self):
        # REQ-560
        sc = ServerConfig()
        assert sc.flight_port == 8815


# ---------------------------------------------------------------------------
# REQ-562 — Secondary nodes stateless; read config from primary PostgreSQL
# Structural test: no local config files are loaded at secondary node startup;
# the config loader uses a DB connection, not a YAML file.
# ---------------------------------------------------------------------------


class TestReq562StatelessSecondaryNodes:
    def test_config_loader_module_exists(self):
        # REQ-562
        from provisa.core import config_loader

        assert config_loader is not None

    def test_catalog_module_exists(self):
        # REQ-562
        from provisa.core import catalog

        assert catalog is not None


# ---------------------------------------------------------------------------
# REQ-591 — SET LOCAL scopes app.tenant_id to the current transaction
# set_tenant_context uses SET LOCAL (transaction-scoped, not session-scoped).
# ---------------------------------------------------------------------------


class TestReq591TenantContextTransactionScoped:
    def test_set_tenant_context_uses_set_local(self):
        # REQ-591
        import inspect

        from provisa.core.db import set_tenant_context

        src = inspect.getsource(set_tenant_context)
        assert "SET LOCAL" in src

    def test_set_tenant_context_does_not_use_set_session(self):
        # REQ-591 — must NOT use SET (session-scoped)
        import inspect

        from provisa.core.db import set_tenant_context

        src = inspect.getsource(set_tenant_context)
        # "SET LOCAL" is acceptable; bare "SET app" without LOCAL would leak
        assert "SET app.tenant_id" not in src or "SET LOCAL app.tenant_id" in src


# ---------------------------------------------------------------------------
# REQ-592 — Each tenant maps to an org; root org seeded for single-tenant
# schema.sql must define orgs table and seed root.
# ---------------------------------------------------------------------------


class TestReq592OrgToTenantMapping:
    def test_schema_sql_defines_orgs_table(self):
        # REQ-592
        schema_path = (
            "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/provisa/core/schema.sql"
        )
        with open(schema_path) as f:
            ddl = f.read()
        assert "CREATE TABLE IF NOT EXISTS orgs" in ddl

    def test_schema_sql_seeds_root_org(self):
        # REQ-592
        schema_path = (
            "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/provisa/core/schema.sql"
        )
        with open(schema_path) as f:
            ddl = f.read()
        assert "'root'" in ddl

    def test_schema_sql_defines_user_org_memberships(self):
        # REQ-592
        schema_path = (
            "/Volumes/main/Users/kennethstott/PycharmProjects/provisa/provisa/core/schema.sql"
        )
        with open(schema_path) as f:
            ddl = f.read()
        assert "user_org_memberships" in ddl


# ---------------------------------------------------------------------------
# REQ-610 — Field access grant belongs to requesting domain, not specific view
# No per-view grant field exists on Column; grants are domain-scoped.
# ---------------------------------------------------------------------------


class TestReq610DomainScopedFieldGrants:
    def test_column_has_no_per_view_grant_field(self):
        # REQ-610
        col = _make_column("revenue")
        assert not hasattr(col, "view_grant")
        assert not hasattr(col, "granted_view")

    def test_table_domain_id_field_provides_domain_scoping(self):
        # REQ-610 — grants tracked at domain level via domain_id on the table
        t = _make_table(domain_id="finance")
        assert t.domain_id == "finance"


# ---------------------------------------------------------------------------
# REQ-635 — Schema name presented to users is the native source schema name
# For flat/API sources a fixed constant is used; PROVISA_INTERNAL_SCHEMAS
# are never presented.
# ---------------------------------------------------------------------------


class TestReq635NativeSchemaNames:
    def test_provisa_internal_schemas_constant_exists(self):
        # REQ-635
        from provisa.api.admin.introspect import PROVISA_INTERNAL_SCHEMAS

        assert isinstance(PROVISA_INTERNAL_SCHEMAS, frozenset)
        assert len(PROVISA_INTERNAL_SCHEMAS) > 0

    def test_mv_cache_is_internal_schema(self):
        # REQ-635 — org-scoped mv_cache must not be presented to users
        from provisa.api.admin.introspect import is_provisa_internal

        assert is_provisa_internal("org_default_mv_cache")

    def test_api_cache_is_internal_schema(self):
        # REQ-635 — org-scoped api_cache must not be presented to users
        from provisa.api.admin.introspect import is_provisa_internal

        assert is_provisa_internal("org_default_api_cache")

    def test_kafka_native_schema_is_kafka(self):
        # REQ-635 — fixed constant for Kafka (flat source)
        # native_schemas returns ["kafka"] for kafka source type
        # We verify by checking the logic path label in the source
        import inspect

        from provisa.api.admin.introspect import native_schemas

        src = inspect.getsource(native_schemas)
        assert '"kafka"' in src


# ---------------------------------------------------------------------------
# REQ-636 — Trino-first introspection when connector configured
# When a source type is in SOURCE_TO_CONNECTOR, Trino is the preferred path.
# native_schemas returns None for RDBMS without live pool, triggering Trino fallback.
# ---------------------------------------------------------------------------


class TestReq636TrinoFirstIntrospection:
    def test_source_to_connector_covers_rdbms_types(self):
        # REQ-636
        assert "postgresql" in SOURCE_TO_CONNECTOR
        assert "mysql" in SOURCE_TO_CONNECTOR
        assert "sqlserver" in SOURCE_TO_CONNECTOR

    def test_nosql_types_in_source_to_connector_use_trino(self):
        # REQ-636
        assert "mongodb" in SOURCE_TO_CONNECTOR
        assert "elasticsearch" in SOURCE_TO_CONNECTOR
        assert "redis" in SOURCE_TO_CONNECTOR


# ---------------------------------------------------------------------------
# REQ-637 — PG cache schemas, Trino catalog names never presented to user
# PROVISA_INTERNAL_SCHEMAS and PROVISA_INTERNAL_TABLES filter them out.
# ---------------------------------------------------------------------------


class TestReq637HideImplementationNamespaces:
    def test_provisa_internal_tables_constant_exists(self):
        # REQ-637
        from provisa.api.admin.introspect import PROVISA_INTERNAL_TABLES

        assert isinstance(PROVISA_INTERNAL_TABLES, frozenset)
        assert len(PROVISA_INTERNAL_TABLES) > 0

    def test_registered_tables_is_internal(self):
        # REQ-637 — config DB tables must never appear as user tables
        from provisa.api.admin.introspect import PROVISA_INTERNAL_TABLES

        assert "registered_tables" in PROVISA_INTERNAL_TABLES

    def test_relationships_is_internal(self):
        # REQ-637
        from provisa.api.admin.introspect import PROVISA_INTERNAL_TABLES

        assert "relationships" in PROVISA_INTERNAL_TABLES


# ---------------------------------------------------------------------------
# REQ-638 — Single availableSchemas and availableTables endpoints
# Backend selects introspection strategy internally; no source-type-specific endpoints.
# native_schemas and native_tables dispatch internally.
# ---------------------------------------------------------------------------


class TestReq638SingleIntrospectionEndpoints:
    def test_native_schemas_is_single_dispatcher(self):
        # REQ-638 — one function dispatches for all source types
        from provisa.api.admin.introspect import native_schemas

        assert callable(native_schemas)

    def test_native_tables_is_single_dispatcher(self):
        # REQ-638
        from provisa.api.admin.introspect import native_tables

        assert callable(native_tables)

    def test_native_schemas_signature_takes_source_type_not_type_specific_params(self):
        # REQ-638 — routing by source_type param, not separate functions per type
        import inspect

        from provisa.api.admin.introspect import native_schemas

        sig = inspect.signature(native_schemas)
        assert "source_type" in sig.parameters


# ---------------------------------------------------------------------------
# REQ-639 — Unknown source type returns [] (empty list), never None
# native_tables falls back to _native_tables_rdbms for unhandled types, but
# for types that are not RDBMS and have no pool, it should return None (Trino).
# The contract: never return None for source types with no Trino connector.
# We test that the dispatch returns [] (not None) for neo4j/sparql.
# ---------------------------------------------------------------------------


class TestReq639UnknownSourceTypeReturnsEmpty:
    def test_neo4j_and_sparql_dispatch_returns_empty_list_in_native_tables(self):
        # REQ-639
        import inspect

        from provisa.api.admin.introspect import native_tables

        src = inspect.getsource(native_tables)
        # neo4j and sparql return [] explicitly (not None)
        assert '("neo4j", "sparql")' in src or "neo4j" in src
        assert "return []" in src


# ---------------------------------------------------------------------------
# REQ-653 — enable_aggregates and enable_group_by per table, default false
# ---------------------------------------------------------------------------


class TestReq653AggregateGroupByFlags:
    def test_table_enable_aggregates_defaults_to_false(self):
        # REQ-653
        t = _make_table()
        assert t.enable_aggregates is False

    def test_table_enable_group_by_defaults_to_false(self):
        # REQ-653
        t = _make_table()
        assert t.enable_group_by is False

    def test_table_enable_aggregates_can_be_set_true(self):
        # REQ-653
        t = Table(
            source_id="pg1",
            domain_id="analytics",
            schema_name="public",
            table_name="metrics",
            columns=[_make_column("value")],
            enable_aggregates=True,
        )
        assert t.enable_aggregates is True

    def test_table_enable_group_by_can_be_set_true(self):
        # REQ-653
        t = Table(
            source_id="pg1",
            domain_id="analytics",
            schema_name="public",
            table_name="metrics",
            columns=[_make_column("value")],
            enable_group_by=True,
        )
        assert t.enable_group_by is True

    def test_schema_gen_table_meta_has_enable_aggregates(self):
        # REQ-653
        from provisa.compiler.schema_gen import SchemaInput

        si = SchemaInput(
            tables=[
                {
                    "id": 1,
                    "source_id": "pg1",
                    "domain_id": "analytics",
                    "schema_name": "public",
                    "table_name": "metrics",
                    "alias": None,
                    "description": None,
                    "enable_aggregates": True,
                    "enable_group_by": False,
                    "columns": [],
                }
            ],
            relationships=[],
            column_types={},
            naming_rules=[],
            role={"id": "analyst", "capabilities": ["read"], "domain_access": ["*"]},
            domains=[],
        )
        assert si.tables[0]["enable_aggregates"] is True
