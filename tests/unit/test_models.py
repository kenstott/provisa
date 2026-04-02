# Copyright (c) 2025 Kenneth Stott
# Canary: 7617fbc9-fe7c-4d45-a161-9105a83300af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Pydantic models."""

import pytest
from pydantic import ValidationError

from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    GovernanceLevel,
    NamingConfig,
    NamingRule,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    Source,
    SourceType,
    Table,
)


class TestSource:
    def test_valid_postgresql_source(self):
        s = Source(
            id="pg1",
            type="postgresql",
            host="localhost",
            port=5432,
            database="mydb",
            username="user",
            password="${env:PG_PASSWORD}",
        )
        assert s.type == SourceType.postgresql
        assert s.connector == "postgresql"
        assert s.dialect == "postgres"
        assert s.catalog_name == "pg1"
        assert "jdbc:postgresql://localhost:5432/mydb" == s.jdbc_url()

    def test_catalog_name_sanitizes_hyphens(self):
        s = Source(
            id="my-pg-source",
            type="postgresql",
            host="h",
            port=5432,
            database="d",
            username="u",
            password="p",
        )
        assert s.catalog_name == "my_pg_source"

    def test_invalid_source_type_rejected(self):
        with pytest.raises(ValidationError):
            Source(
                id="bad",
                type="not_a_real_database",
                host="h",
                port=1521,
                database="d",
                username="u",
                password="p",
            )

    def test_mysql_jdbc_url(self):
        s = Source(
            id="m1",
            type="mysql",
            host="db.local",
            port=3306,
            database="app",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == "jdbc:mysql://db.local:3306/app"

    def test_sqlserver_jdbc_url(self):
        s = Source(
            id="ss1",
            type="sqlserver",
            host="sql.local",
            port=1433,
            database="app",
            username="u",
            password="p",
        )
        assert "databaseName=app" in s.jdbc_url()

    def test_mongodb_no_jdbc(self):
        s = Source(
            id="m1",
            type="mongodb",
            host="mongo.local",
            port=27017,
            database="app",
            username="u",
            password="p",
        )
        assert s.jdbc_url() == ""


class TestDomain:
    def test_domain_defaults(self):
        d = Domain(id="test")
        assert d.description == ""

    def test_domain_with_description(self):
        d = Domain(id="sales", description="Sales data")
        assert d.description == "Sales data"


class TestTable:
    def test_table_with_alias_fields(self):
        t = Table(
            source_id="pg1",
            domain_id="sales",
            **{"schema": "public", "table": "orders"},
            governance="pre-approved",
            columns=[Column(name="id", visible_to=["admin"])],
        )
        assert t.schema_name == "public"
        assert t.table_name == "orders"
        assert t.governance == GovernanceLevel.pre_approved

    def test_table_registry_required(self):
        t = Table(
            source_id="pg1",
            domain_id="sales",
            **{"schema": "public", "table": "t"},
            governance="registry-required",
            columns=[],
        )
        assert t.governance == GovernanceLevel.registry_required

    def test_invalid_governance_rejected(self):
        with pytest.raises(ValidationError):
            Table(
                source_id="pg1",
                domain_id="sales",
                **{"schema": "public", "table": "t"},
                governance="invalid",
                columns=[],
            )


class TestRelationship:
    def test_valid_relationship(self):
        r = Relationship(
            id="r1",
            source_table_id="orders",
            target_table_id="customers",
            source_column="customer_id",
            target_column="id",
            cardinality="many-to-one",
        )
        assert r.cardinality == Cardinality.many_to_one

    def test_invalid_cardinality_rejected(self):
        with pytest.raises(ValidationError):
            Relationship(
                id="r1",
                source_table_id="a",
                target_table_id="b",
                source_column="c",
                target_column="d",
                cardinality="invalid",
            )


class TestRole:
    def test_role_with_capabilities(self):
        r = Role(
            id="admin",
            capabilities=["source_registration", "admin"],
            domain_access=["*"],
        )
        assert "admin" in r.capabilities
        assert r.domain_access == ["*"]


class TestRLSRule:
    def test_rls_rule(self):
        rule = RLSRule(table_id="orders", role_id="analyst", filter="region = 'us'")
        assert rule.filter == "region = 'us'"


class TestNamingConfig:
    def test_naming_rules(self):
        nc = NamingConfig(rules=[NamingRule(pattern="^prod_", replace="")])
        assert len(nc.rules) == 1

    def test_empty_naming(self):
        nc = NamingConfig()
        assert nc.rules == []


class TestProvisaConfig:
    def test_full_config_from_yaml_dict(self, sample_config):
        config = ProvisaConfig.model_validate(sample_config)
        assert len(config.sources) == 1
        assert len(config.domains) == 2
        assert len(config.tables) == 3
        assert len(config.relationships) == 1
        assert len(config.roles) == 2
        assert len(config.rls_rules) == 1
        assert config.sources[0].id == "sales-pg"
        assert config.tables[0].table_name == "orders"
        assert config.relationships[0].cardinality == Cardinality.many_to_one

    def test_config_missing_required_field_rejected(self):
        with pytest.raises(ValidationError):
            ProvisaConfig.model_validate({"sources": [], "domains": []})
