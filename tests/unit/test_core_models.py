# Copyright (c) 2026 Kenneth Stott
# Canary: c1a2b3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for core Pydantic models: Source, Table, Role, RLSRule, flatten_roles, etc."""

import pytest
from pydantic import ValidationError

from provisa.core.models import (
    Cardinality,
    Column,
    ColumnPreset,
    Domain,
    EventTrigger,
    Function,
    FunctionArgument,
    GovernanceLevel,
    HotTablesConfig,
    InlineType,
    LiveDeliveryConfig,
    LiveOutputConfig,
    NamingConfig,
    NamingRule,
    ProvisaConfig,
    RLSRule,
    Relationship,
    Role,
    ScheduledTrigger,
    ServerConfig,
    Source,
    SourceType,
    SOURCE_TO_CONNECTOR,
    SOURCE_TO_DIALECT,
    Table,
    Webhook,
    flatten_roles,
)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class TestSource:
    def test_valid_postgresql_defaults(self):
        s = Source(
            id="pg1",
            type="postgresql",
            host="localhost",
            port=5432,
            database="mydb",
            username="user",
            password="s3cr3t",
        )
        assert s.pool_min == 1
        assert s.pool_max == 5
        assert s.use_pgbouncer is False
        assert s.pgbouncer_port == 6432
        assert s.cache_enabled is True
        assert s.cache_ttl is None
        assert s.naming_convention is None
        assert s.federation_hints == {}

    def test_source_id_must_start_with_letter(self):
        with pytest.raises(ValidationError):
            Source(
                id="1bad",
                type="postgresql",
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )

    def test_source_id_special_chars_rejected(self):
        with pytest.raises(ValidationError):
            Source(
                id="bad!id",
                type="postgresql",
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )

    def test_source_id_hyphens_and_underscores_allowed(self):
        s = Source(
            id="my-pg_source1",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
        )
        assert s.id == "my-pg_source1"

    def test_catalog_name_replaces_hyphens(self):
        s = Source(
            id="sales-pg-prod",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
        )
        assert s.catalog_name == "sales_pg_prod"

    def test_connector_mapping_all_types(self):
        for stype, connector in SOURCE_TO_CONNECTOR.items():
            s = Source(
                id="src",
                type=stype,
                host="h",
                port=5432,
                database="d",
                username="u",
                password="p",
            )
            assert s.connector == connector

    def test_dialect_postgres(self):
        s = Source(
            id="pg",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
        )
        assert s.dialect == "postgres"

    def test_dialect_none_for_nosql(self):
        s = Source(
            id="m",
            type="mongodb",
            host="h",
            port=27017,
            database="d",
            username="u",
            password="p",
        )
        assert s.dialect is None

    def test_jdbc_url_postgresql(self):
        s = Source(
            id="pg",
            type="postgresql",
            host="db.example.com",
            port=5432,
            database="sales",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == "jdbc:postgresql://db.example.com:5432/sales"

    def test_jdbc_url_mysql(self):
        s = Source(
            id="my",
            type="mysql",
            host="db.local",
            port=3306,
            database="app",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == "jdbc:mysql://db.local:3306/app"

    def test_jdbc_url_mariadb(self):
        s = Source(
            id="ma",
            type="mariadb",
            host="db.local",
            port=3307,
            database="app",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == "jdbc:mariadb://db.local:3307/app"

    def test_jdbc_url_sqlserver(self):
        s = Source(
            id="ss",
            type="sqlserver",
            host="sql.local",
            port=1433,
            database="MyDB",
            username="u",
            password="p",
        )
        url = s.jdbc_url()
        assert url.startswith("jdbc:sqlserver://sql.local:1433")
        assert "databaseName=MyDB" in url

    def test_jdbc_url_oracle(self):
        s = Source(
            id="ora",
            type="oracle",
            host="ora.local",
            port=1521,
            database="ORCLDB",
            username="u",
            password="p",
        )
        url = s.jdbc_url()
        assert "jdbc:oracle:thin:" in url
        assert "ora.local:1521/ORCLDB" in url

    def test_jdbc_url_empty_for_mongodb(self):
        s = Source(
            id="mg",
            type="mongodb",
            host="mongo.local",
            port=27017,
            database="d",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == ""

    def test_jdbc_url_empty_for_cassandra(self):
        s = Source(
            id="cs",
            type="cassandra",
            host="cs.local",
            port=9042,
            database="d",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == ""

    def test_jdbc_url_empty_for_snowflake(self):
        s = Source(
            id="sf",
            type="snowflake",
            host="acc.snowflakecomputing.com",
            port=443,
            database="d",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == ""

    def test_pool_min_max_custom(self):
        s = Source(
            id="pg",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
            pool_min=3,
            pool_max=20,
        )
        assert s.pool_min == 3
        assert s.pool_max == 20

    def test_federation_hints_stored(self):
        s = Source(
            id="pg",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
            federation_hints={"join_distribution_type": "BROADCAST"},
        )
        assert s.federation_hints["join_distribution_type"] == "BROADCAST"


# ---------------------------------------------------------------------------
# Domain
# ---------------------------------------------------------------------------


class TestDomain:
    def test_domain_minimal(self):
        d = Domain(id="finance")
        assert d.id == "finance"
        assert d.description == ""

    def test_domain_with_description(self):
        d = Domain(id="ops", description="Operations data")
        assert d.description == "Operations data"


# ---------------------------------------------------------------------------
# NamingConfig / NamingRule
# ---------------------------------------------------------------------------


class TestNamingConfig:
    def test_defaults(self):
        nc = NamingConfig()
        assert nc.convention == "snake_case"
        assert nc.rules == []
        assert nc.relay_pagination is False

    def test_custom_convention(self):
        nc = NamingConfig(convention="camelCase")
        assert nc.convention == "camelCase"

    def test_multiple_rules(self):
        nc = NamingConfig(
            rules=[
                NamingRule(pattern="^prod_", replace=""),
                NamingRule(pattern="_v2$", replace=""),
            ]
        )
        assert len(nc.rules) == 2
        assert nc.rules[0].pattern == "^prod_"
        assert nc.rules[1].replace == ""

    def test_relay_pagination_flag(self):
        nc = NamingConfig(relay_pagination=True)
        assert nc.relay_pagination is True


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


class TestColumn:
    def test_column_minimal(self):
        col = Column(name="id", visible_to=["admin"])
        assert col.writable_by == []
        assert col.unmasked_to == []
        assert col.mask_type is None
        assert col.alias is None
        assert col.description is None
        assert col.path is None

    def test_column_full(self):
        col = Column(
            name="email",
            visible_to=["admin", "analyst"],
            writable_by=["admin"],
            unmasked_to=["admin"],
            mask_type="regex",
            mask_pattern=r"(.+)@",
            mask_replace=r"***@",
            alias="emailAddress",
            description="User email",
            path="contact.email",
        )
        assert col.mask_type == "regex"
        assert col.alias == "emailAddress"
        assert col.path == "contact.email"


# ---------------------------------------------------------------------------
# ColumnPreset
# ---------------------------------------------------------------------------


class TestColumnPreset:
    def test_preset_header(self):
        cp = ColumnPreset(column="created_by", source="header", name="X-User-ID")
        assert cp.source == "header"
        assert cp.name == "X-User-ID"

    def test_preset_now(self):
        cp = ColumnPreset(column="updated_at", source="now")
        assert cp.source == "now"
        assert cp.name is None

    def test_preset_literal(self):
        cp = ColumnPreset(column="tenant_id", source="literal", value="acme")
        assert cp.value == "acme"


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------


class TestTable:
    def _make(self, **overrides):
        defaults = dict(
            source_id="pg1",
            domain_id="sales",
            **{"schema": "public", "table": "orders"},
            governance="pre-approved",
            columns=[Column(name="id", visible_to=["admin"])],
        )
        defaults.update(overrides)
        return Table(**defaults)

    def test_schema_alias(self):
        t = self._make()
        assert t.schema_name == "public"

    def test_table_alias(self):
        t = self._make()
        assert t.table_name == "orders"

    def test_governance_pre_approved(self):
        t = self._make(governance="pre-approved")
        assert t.governance == GovernanceLevel.pre_approved

    def test_governance_registry_required(self):
        t = self._make(governance="registry-required")
        assert t.governance == GovernanceLevel.registry_required

    def test_invalid_governance_rejected(self):
        with pytest.raises(ValidationError):
            self._make(governance="open")

    def test_table_defaults(self):
        t = self._make()
        assert t.column_presets == []
        assert t.alias is None
        assert t.description is None
        assert t.cache_ttl is None
        assert t.naming_convention is None
        assert t.hot is None
        assert t.relay_pagination is None
        assert t.live is None

    def test_table_with_column_presets(self):
        t = self._make(
            column_presets=[ColumnPreset(column="created_by", source="header", name="X-User-ID")]
        )
        assert len(t.column_presets) == 1

    def test_table_with_live_config(self):
        live = LiveDeliveryConfig(
            query_id="q-abc123",
            watermark_column="updated_at",
            poll_interval=30,
            outputs=[LiveOutputConfig(type="sse")],
        )
        t = self._make(live=live)
        assert t.live.query_id == "q-abc123"
        assert t.live.outputs[0].type == "sse"


# ---------------------------------------------------------------------------
# Relationship
# ---------------------------------------------------------------------------


class TestRelationship:
    def test_many_to_one(self):
        r = Relationship(
            id="orders-customers",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
        )
        assert r.cardinality == Cardinality.many_to_one
        assert r.materialize is False
        assert r.refresh_interval == 300

    def test_one_to_many(self):
        r = Relationship(
            id="customers-orders",
            source_table_id="customers",
            target_table_id="orders",
            source_column="id",
            target_column="customer_id",
            cardinality="one-to-many",
        )
        assert r.cardinality == Cardinality.one_to_many

    def test_materialize_flag(self):
        r = Relationship(
            id="r1",
            source_table_id="a",
            target_table_id="b",
            source_column="aid",
            target_column="id",
            cardinality="many-to-one",
            materialize=True,
            refresh_interval=60,
        )
        assert r.materialize is True
        assert r.refresh_interval == 60

    def test_invalid_cardinality(self):
        with pytest.raises(ValidationError):
            Relationship(
                id="r1",
                source_table_id="a",
                target_table_id="b",
                source_column="c",
                target_column="d",
                cardinality="one-to-one",
            )


# ---------------------------------------------------------------------------
# Role / flatten_roles
# ---------------------------------------------------------------------------


class TestRole:
    def test_role_minimal(self):
        r = Role(id="viewer", capabilities=[], domain_access=[])
        assert r.parent_role_id is None

    def test_role_with_parent(self):
        r = Role(
            id="senior_analyst",
            capabilities=["query_approval"],
            domain_access=["finance"],
            parent_role_id="analyst",
        )
        assert r.parent_role_id == "analyst"

    def test_role_wildcard_domain_access(self):
        r = Role(id="admin", capabilities=["admin"], domain_access=["*"])
        assert r.domain_access == ["*"]


class TestFlattenRoles:
    def test_no_inheritance(self):
        roles = [
            Role(id="a", capabilities=["query_development"], domain_access=["sales"]),
            Role(id="b", capabilities=["admin"], domain_access=["*"]),
        ]
        flat = flatten_roles(roles)
        by_id = {r.id: r for r in flat}
        assert set(by_id["a"].capabilities) == {"query_development"}
        assert set(by_id["b"].capabilities) == {"admin"}

    def test_child_inherits_parent_capabilities(self):
        roles = [
            Role(id="base", capabilities=["query_development"], domain_access=["sales"]),
            Role(
                id="senior",
                capabilities=["query_approval"],
                domain_access=["finance"],
                parent_role_id="base",
            ),
        ]
        flat = flatten_roles(roles)
        senior = next(r for r in flat if r.id == "senior")
        assert "query_development" in senior.capabilities
        assert "query_approval" in senior.capabilities

    def test_child_inherits_parent_domain_access(self):
        roles = [
            Role(id="base", capabilities=["query_development"], domain_access=["sales"]),
            Role(
                id="senior",
                capabilities=["query_approval"],
                domain_access=["finance"],
                parent_role_id="base",
            ),
        ]
        flat = flatten_roles(roles)
        senior = next(r for r in flat if r.id == "senior")
        assert "sales" in senior.domain_access
        assert "finance" in senior.domain_access

    def test_wildcard_domain_preserved(self):
        roles = [
            Role(id="admin", capabilities=["admin"], domain_access=["*"]),
            Role(
                id="superadmin",
                capabilities=["source_registration"],
                domain_access=["ops"],
                parent_role_id="admin",
            ),
        ]
        flat = flatten_roles(roles)
        superadmin = next(r for r in flat if r.id == "superadmin")
        assert superadmin.domain_access == ["*"]

    def test_parent_unchanged(self):
        roles = [
            Role(id="analyst", capabilities=["query_development"], domain_access=["sales"]),
            Role(
                id="lead",
                capabilities=["query_approval"],
                domain_access=["finance"],
                parent_role_id="analyst",
            ),
        ]
        flat = flatten_roles(roles)
        analyst = next(r for r in flat if r.id == "analyst")
        assert set(analyst.capabilities) == {"query_development"}

    def test_capabilities_sorted(self):
        roles = [
            Role(id="r", capabilities=["query_development", "admin"], domain_access=[]),
        ]
        flat = flatten_roles(roles)
        assert flat[0].capabilities == sorted(["query_development", "admin"])

    def test_parent_role_id_preserved(self):
        roles = [
            Role(id="base", capabilities=["query_development"], domain_access=["sales"]),
            Role(
                id="child",
                capabilities=["query_approval"],
                domain_access=[],
                parent_role_id="base",
            ),
        ]
        flat = flatten_roles(roles)
        child = next(r for r in flat if r.id == "child")
        assert child.parent_role_id == "base"


# ---------------------------------------------------------------------------
# RLSRule
# ---------------------------------------------------------------------------


class TestRLSRule:
    def test_basic_rule(self):
        rule = RLSRule(
            table_id="orders",
            role_id="analyst",
            filter="region = current_setting('provisa.user_region')",
        )
        assert rule.table_id == "orders"
        assert rule.role_id == "analyst"
        assert "current_setting" in rule.filter

    def test_simple_equality_filter(self):
        rule = RLSRule(table_id="products", role_id="viewer", filter="active = true")
        assert rule.filter == "active = true"


# ---------------------------------------------------------------------------
# ServerConfig
# ---------------------------------------------------------------------------


class TestServerConfig:
    def test_defaults(self):
        sc = ServerConfig()
        assert sc.hostname == "localhost"
        assert sc.port == 8000
        assert sc.grpc_port == 50051
        assert sc.flight_port == 8815

    def test_custom_values(self):
        sc = ServerConfig(hostname="prod.example.com", port=9000, grpc_port=51001)
        assert sc.hostname == "prod.example.com"
        assert sc.port == 9000
        assert sc.grpc_port == 51001


# ---------------------------------------------------------------------------
# HotTablesConfig
# ---------------------------------------------------------------------------


class TestHotTablesConfig:
    def test_defaults(self):
        ht = HotTablesConfig()
        assert ht.auto_threshold == 1_000
        assert ht.refresh_interval == 300

    def test_custom(self):
        ht = HotTablesConfig(auto_threshold=500, refresh_interval=60)
        assert ht.auto_threshold == 500
        assert ht.refresh_interval == 60


# ---------------------------------------------------------------------------
# EventTrigger
# ---------------------------------------------------------------------------


class TestEventTrigger:
    def test_defaults(self):
        et = EventTrigger(table_id="orders", webhook_url="https://hook.example.com/notify")
        assert et.operations == ["insert", "update", "delete"]
        assert et.retry_max == 3
        assert et.retry_delay == 1.0
        assert et.enabled is True

    def test_custom_operations(self):
        et = EventTrigger(
            table_id="orders",
            webhook_url="https://hook.example.com/notify",
            operations=["insert"],
        )
        assert et.operations == ["insert"]


# ---------------------------------------------------------------------------
# ScheduledTrigger
# ---------------------------------------------------------------------------


class TestScheduledTrigger:
    def test_minimal(self):
        st = ScheduledTrigger(id="daily-sync", cron="0 0 * * *", url="https://sync.example.com/run")
        assert st.enabled is True
        assert st.function is None

    def test_function_trigger(self):
        st = ScheduledTrigger(id="hourly-agg", cron="0 * * * *", function="aggregate_sales")
        assert st.url is None
        assert st.function == "aggregate_sales"


# ---------------------------------------------------------------------------
# Function / Webhook
# ---------------------------------------------------------------------------


class TestFunction:
    def test_function_minimal(self):
        fn = Function(
            name="get_order",
            source_id="pg1",
            **{"schema": "public"},
            function_name="get_order_fn",
            returns="pg1.public.orders",
        )
        assert fn.arguments == []
        assert fn.visible_to == []
        assert fn.writable_by == []
        assert fn.domain_id == ""

    def test_function_with_arguments(self):
        fn = Function(
            name="get_order",
            source_id="pg1",
            **{"schema": "public"},
            function_name="get_order_fn",
            returns="pg1.public.orders",
            arguments=[FunctionArgument(name="order_id", type="Int")],
        )
        assert len(fn.arguments) == 1
        assert fn.arguments[0].name == "order_id"


class TestWebhook:
    def test_webhook_defaults(self):
        wh = Webhook(name="send_notification", url="https://api.example.com/notify")
        assert wh.method == "POST"
        assert wh.timeout_ms == 5000
        assert wh.returns is None
        assert wh.arguments == []
        assert wh.inline_return_type == []

    def test_webhook_with_inline_type(self):
        wh = Webhook(
            name="create_ticket",
            url="https://ticket.example.com/create",
            inline_return_type=[InlineType(name="ticket_id", type="String")],
        )
        assert wh.inline_return_type[0].name == "ticket_id"


# ---------------------------------------------------------------------------
# ProvisaConfig (top-level validation)
# ---------------------------------------------------------------------------


class TestProvisaConfig:
    def _minimal_config(self):
        return {
            "sources": [
                {
                    "id": "pg1",
                    "type": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "database": "d",
                    "username": "u",
                    "password": "p",
                }
            ],
            "domains": [{"id": "sales"}],
            "tables": [
                {
                    "source_id": "pg1",
                    "domain_id": "sales",
                    "schema": "public",
                    "table": "orders",
                    "governance": "pre-approved",
                    "columns": [{"name": "id", "visible_to": ["admin"]}],
                }
            ],
            "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
        }

    def test_minimal_valid_config(self):
        cfg = ProvisaConfig.model_validate(self._minimal_config())
        assert len(cfg.sources) == 1
        assert len(cfg.domains) == 1
        assert len(cfg.tables) == 1
        assert len(cfg.roles) == 1

    def test_defaults_populated(self):
        cfg = ProvisaConfig.model_validate(self._minimal_config())
        assert cfg.relationships == []
        assert cfg.rls_rules == []
        assert cfg.event_triggers == []
        assert cfg.scheduled_triggers == []
        assert cfg.functions == []
        assert cfg.webhooks == []
        assert isinstance(cfg.server, ServerConfig)
        assert isinstance(cfg.naming, NamingConfig)
        assert isinstance(cfg.hot_tables, HotTablesConfig)

    def test_missing_tables_rejected(self):
        data = self._minimal_config()
        del data["tables"]
        with pytest.raises(ValidationError):
            ProvisaConfig.model_validate(data)

    def test_missing_roles_rejected(self):
        data = self._minimal_config()
        del data["roles"]
        with pytest.raises(ValidationError):
            ProvisaConfig.model_validate(data)

    def test_with_rls_rules(self):
        data = self._minimal_config()
        data["rls_rules"] = [
            {"table_id": "orders", "role_id": "admin", "filter": "1=1"}
        ]
        cfg = ProvisaConfig.model_validate(data)
        assert len(cfg.rls_rules) == 1

    def test_with_relationships(self):
        data = self._minimal_config()
        data["sources"][0:0] = []  # keep existing source
        data["tables"].append(
            {
                "source_id": "pg1",
                "domain_id": "sales",
                "schema": "public",
                "table": "customers",
                "governance": "pre-approved",
                "columns": [{"name": "id", "visible_to": ["admin"]}],
            }
        )
        data["relationships"] = [
            {
                "id": "orders-customers",
                "source_table_id": "orders",
                "target_table_id": "customers",
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ]
        cfg = ProvisaConfig.model_validate(data)
        assert len(cfg.relationships) == 1

    def test_sample_config_fixture(self, sample_config):
        """Validate the shared sample fixture loads cleanly."""
        cfg = ProvisaConfig.model_validate(sample_config)
        assert len(cfg.sources) >= 1
        assert len(cfg.tables) >= 1
        assert len(cfg.roles) >= 1


# ---------------------------------------------------------------------------
# Source-to-connector / source-to-dialect maps
# ---------------------------------------------------------------------------


class TestSourceMaps:
    def test_connector_map_nonempty(self):
        assert len(SOURCE_TO_CONNECTOR) > 0

    def test_dialect_map_nonempty(self):
        assert len(SOURCE_TO_DIALECT) > 0

    def test_postgresql_in_both_maps(self):
        assert "postgresql" in SOURCE_TO_CONNECTOR
        assert "postgresql" in SOURCE_TO_DIALECT

    def test_mongodb_no_dialect(self):
        assert "mongodb" not in SOURCE_TO_DIALECT

    def test_cassandra_no_dialect(self):
        assert "cassandra" not in SOURCE_TO_DIALECT
