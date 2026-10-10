# Copyright (c) 2026 Kenneth Stott
# Canary: 5681a366-c3f2-4706-8c35-646b3ec87e83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1967: the governed catalog as a dbt sources file -- what a role is served, with the
standard dbt tests the model can state and no others."""

# Requirements: REQ-1967
from __future__ import annotations

from types import SimpleNamespace

import pytest
import yaml

from provisa.api.errors import ApiError
from provisa.core.models import (
    Column,
    Domain,
    ProvisaConfig,
    Relationship,
    Role,
    Source,
    Table,
    UniqueConstraint,
)
from provisa.dbt.sources import (
    DBT_VERSIONS,
    GOVERNANCE,
    UnknownRole,
    build_dbt_sources,
    dbt_sources_yaml,
)


def _column(name: str, **more) -> Column:
    return Column(name=name, visible_to=more.pop("visible_to", ["*"]), **more)


def _config() -> ProvisaConfig:
    return ProvisaConfig(
        sources=[Source(id="pg1", type="postgresql"), Source(id="wh", type="postgresql")],
        domains=[Domain(id="sales"), Domain(id="hr")],
        roles=[
            Role(id="analyst", capabilities=[], domain_access=["sales"]),
            Role(id="junior", capabilities=[], domain_access=[], parent_role_id="analyst"),
            Role(id="people", capabilities=[], domain_access=["hr"]),
        ],
        tables=[
            Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                description="Order fact table",
                columns=[
                    _column("id", is_primary_key=True),
                    _column("customer_id"),
                    _column("order_no", description="As printed on the invoice"),
                    _column("margin", visible_to=["finance"]),
                    _column("ordered_at"),
                ],
                unique_constraints=[
                    UniqueConstraint(name="orders_no", columns=["order_no"]),
                    UniqueConstraint(name="orders_nk", columns=["customer_id", "ordered_at"]),
                ],
            ),
            Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="public",
                table_name="customers",
                alias="clients",
                columns=[_column("id", is_primary_key=True), _column("name")],
            ),
            Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="archive",
                table_name="order_lines",
                columns=[
                    _column("order_id", is_primary_key=True),
                    _column("line_no", is_primary_key=True),
                ],
            ),
            Table(
                source_id="wh",
                domain_id="hr",
                schema_name="public",
                table_name="salaries",
                columns=[_column("employee_id", is_primary_key=True), _column("amount")],
            ),
        ],
        relationships=[
            Relationship(
                id="orders_customer",
                source_table_id="orders",
                target_table_id="clients",
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
            ),
            Relationship(
                id="orders_salary",
                source_table_id="orders",
                target_table_id="salaries",
                source_column="customer_id",
                target_column="employee_id",
                cardinality="many-to-one",
            ),
        ],
    )


def _tables(document: dict) -> dict[tuple[str, str], dict]:
    return {
        (source["name"], table["name"]): table
        for source in document["sources"]
        for table in source["tables"]
    }


def _tests(table: dict) -> dict[str, list]:
    return {column["name"]: column.get("data_tests", []) for column in table["columns"]}


def test_a_role_gets_the_tables_it_is_served_under_their_source_and_schema():
    document = build_dbt_sources(_config(), "analyst").document
    assert document["version"] == 2
    assert [(s["name"], s["schema"]) for s in document["sources"]] == [
        ("pg1_public", "public"),
        ("pg1_archive", "archive"),
    ]
    assert set(_tables(document)) == {
        ("pg1_public", "orders"),
        ("pg1_public", "customers"),
        ("pg1_archive", "order_lines"),
    }


def test_a_source_with_one_schema_is_named_by_its_id():
    document = build_dbt_sources(_config(), "people").document
    assert [(s["name"], s["schema"]) for s in document["sources"]] == [("wh", "public")]


def test_a_table_and_a_column_carry_their_descriptions():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert orders["description"] == "Order fact table"
    described = {c["name"]: c.get("description") for c in orders["columns"]}
    assert described["order_no"] == "As printed on the invoice"
    assert described["id"] is None


def test_a_column_the_role_is_not_served_is_not_in_the_file():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert "margin" not in _tests(orders)


def test_a_table_outside_the_roles_domains_is_not_in_the_file():
    assert ("wh", "salaries") not in _tables(build_dbt_sources(_config(), "analyst").document)
    assert set(_tables(build_dbt_sources(_config(), "people").document)) == {("wh", "salaries")}


def test_a_role_is_served_what_its_parent_is():
    assert _tables(build_dbt_sources(_config(), "junior").document).keys() == (
        _tables(build_dbt_sources(_config(), "analyst").document).keys()
    )


def test_a_primary_key_of_one_column_is_unique_and_not_null():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert _tests(orders)["id"] == ["unique", "not_null"]


def test_a_unique_constraint_of_one_column_is_unique():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert _tests(orders)["order_no"] == ["unique"]


def test_each_column_of_a_primary_key_of_several_is_not_null_and_none_is_unique():
    lines = _tables(build_dbt_sources(_config(), "analyst").document)[
        ("pg1_archive", "order_lines")
    ]
    assert _tests(lines) == {"order_id": ["not_null"], "line_no": ["not_null"]}


def test_a_key_over_several_columns_is_named_and_not_written_as_a_test():
    sources = build_dbt_sources(_config(), "analyst")
    assert sources.not_stated == [
        ("pg1_public", "orders", "unique constraint", ("customer_id", "ordered_at")),
        ("pg1_archive", "order_lines", "primary key", ("order_id", "line_no")),
    ]
    orders = _tables(sources.document)[("pg1_public", "orders")]
    assert _tests(orders)["ordered_at"] == []


def test_a_relationship_is_a_relationships_test_naming_the_other_source_table():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert _tests(orders)["customer_id"] == [
        {"relationships": {"arguments": {"to": "source('pg1_public', 'customers')", "field": "id"}}}
    ]


def test_a_relationship_to_a_table_the_role_is_not_served_is_not_stated():
    orders = _tables(build_dbt_sources(_config(), "analyst").document)[("pg1_public", "orders")]
    assert "salaries" not in yaml.safe_dump(orders)


def test_a_column_with_nothing_to_test_carries_no_tests():
    customers = _tables(build_dbt_sources(_config(), "analyst").document)[
        ("pg1_public", "customers")
    ]
    assert customers["columns"][1] == {"name": "name"}


def test_the_file_opens_by_saying_what_it_does_not_carry_and_what_it_could_not_state():
    text = dbt_sources_yaml(_config(), "analyst", derived_at="2026-10-10T00:00:00+00:00")
    head = [line for line in text.splitlines() if line.startswith("#")]
    assert head[0] == "# dbt sources from Provisa's governed model, as role 'analyst' is served it."
    assert "2026-10-10T00:00:00+00:00" in head[1]
    assert head[2] == f"# {GOVERNANCE}"
    assert head[3] == f"# {DBT_VERSIONS}" and "1.12.5" in DBT_VERSIONS and "1.9" in DBT_VERSIONS
    assert "#   pg1_public.orders: unique constraint (customer_id, ordered_at)" in head
    assert "#   pg1_archive.order_lines: primary key (order_id, line_no)" in head
    assert yaml.safe_load(text) == build_dbt_sources(_config(), "analyst").document


def test_a_file_with_every_key_stated_says_nothing_of_keys():
    text = dbt_sources_yaml(_config(), "people", derived_at="now")
    assert "no standard dbt test" not in text


def test_it_carries_nothing_of_grants_masks_or_metrics():
    text = dbt_sources_yaml(_config(), "analyst", derived_at="now")
    body = "\n".join(line for line in text.splitlines() if not line.startswith("#"))
    for word in ("visible_to", "mask", "finance", "metric", "domain"):
        assert word not in body, word


def test_a_role_that_does_not_exist_is_refused_by_name():
    with pytest.raises(UnknownRole, match="No role 'nobody' exists"):
        build_dbt_sources(_config(), "nobody")


@pytest.mark.asyncio
class TestTheEndpoint:
    @pytest.fixture
    def wired(self, monkeypatch):
        from provisa.api.admin import capabilities, config_export, ossie_router

        asked: dict = {"inspected": []}

        async def live() -> dict:
            return _config().model_dump(by_alias=True)

        monkeypatch.setattr(config_export, "build_live_config", live)
        monkeypatch.setattr(ossie_router, "require_org_settings", lambda request: None)
        monkeypatch.setattr(
            capabilities,
            "require_inspectable_role_request",
            lambda request, role: asked["inspected"].append(role),
        )
        return asked

    async def test_it_answers_the_file_as_a_yaml_download(self, wired):
        from provisa.api.admin.ossie_router import download_dbt_sources

        answer = await download_dbt_sources(SimpleNamespace(), role="analyst")
        assert answer.media_type == "text/yaml"
        assert answer.headers["content-disposition"] == "attachment; filename=provisa.sources.yml"
        assert yaml.safe_load(answer.body)["sources"][0]["name"] == "pg1_public"
        assert wired["inspected"] == ["analyst"]

    async def test_a_role_the_caller_may_not_inspect_is_refused_before_anything_is_read(
        self, wired, monkeypatch
    ):
        from provisa.api.admin import capabilities, config_export
        from provisa.api.admin.ossie_router import download_dbt_sources

        def refuse(request, role):
            raise ApiError(403, "auth.role_not_assigned", "no", role_id=role)

        async def never() -> dict:
            raise AssertionError("the model was read for a role the caller may not inspect")

        monkeypatch.setattr(capabilities, "require_inspectable_role_request", refuse)
        monkeypatch.setattr(config_export, "build_live_config", never)
        with pytest.raises(ApiError) as raised:
            await download_dbt_sources(SimpleNamespace(), role="people")
        assert raised.value.code == "auth.role_not_assigned"

    async def test_a_role_that_does_not_exist_is_a_refusal_by_name(self, wired):
        from provisa.api.admin.ossie_router import download_dbt_sources

        with pytest.raises(ApiError) as raised:
            await download_dbt_sources(SimpleNamespace(), role="nobody")
        assert (raised.value.status_code, raised.value.code) == (404, "dbt_sources.unknown_role")
