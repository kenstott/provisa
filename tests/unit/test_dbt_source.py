# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7c21-94fd-46a3-8d17-c2a96e4f1b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt project as a source type (REQ-2228), on a real manifest.

``tests/fixtures/dbt/manifest.json`` was written by ``dbt build`` (dbt-core 1.12.6, dbt-duckdb
1.11.1) of a project with a table model, an incremental model, a view, two seeds, a snapshot, data
tests and two macros. Only dbt's own macros were thinned, for size.
"""

# Requirements: REQ-2228

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from provisa.dbt.commands import (
    TargetRefused,
    cloud_binding,
    command_named,
    offered_commands,
    script_binding,
)
from provisa.dbt.lineage import compiled_views
from provisa.dbt.manifest import NotAManifest, from_definitions, parse_manifest, to_definitions
from provisa.dbt.warehouse import warehouse_for, warehouse_offers
from provisa.lineage.merge import build_federation_graph

FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest.json"


@pytest.fixture(name="document")
def _document() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(name="manifest")
def _manifest(document: dict):
    return parse_manifest(document)


@dataclass
class _Source:
    id: str
    type: str
    database: str = ""
    path: str | None = None


def test_the_manifest_names_the_adapter_and_each_models_relation(manifest):
    assert manifest.adapter == "duckdb"
    assert manifest.project == "jaffle"
    by_name = {m.name: m for m in manifest.models}
    assert set(by_name) == {"customer_orders", "order_events", "stg_orders"}
    model = by_name["customer_orders"]
    assert (model.database, model.schema, model.relation) == (
        "jaffle",
        "analytics",
        "customer_orders",
    )
    assert model.materialized == "table"
    assert by_name["order_events"].materialized == "incremental"


def test_a_model_brings_its_descriptions_and_compiled_sql_never_data(manifest):
    model = next(m for m in manifest.models if m.name == "customer_orders")
    assert model.description == "One row per customer with lifetime value."
    columns = {c.name: c.description for c in model.columns}
    assert columns["lifetime_value"] == "Sum of the customer's orders."
    assert model.compiled_sql is not None and "stg_orders" in model.compiled_sql
    assert "{{" not in model.compiled_sql


def test_a_document_that_is_no_manifest_is_refused():
    with pytest.raises(NotAManifest):
        parse_manifest('{"openapi": "3.0.0"}')
    with pytest.raises(NotAManifest):
        parse_manifest("not json")


def test_what_is_held_is_the_definitions(manifest):
    assert from_definitions(to_definitions(manifest)) == manifest


def test_everything_dbt_can_run_is_offered_as_a_command(manifest):
    names = {c.name for c in offered_commands(manifest)}
    assert {"run_customer_orders", "run_order_events", "run_stg_orders"} <= names
    assert "snapshot_orders_snapshot" in names
    assert {"seed_raw_customers", "seed_raw_orders"} <= names
    assert {"build", "source_freshness"} <= names
    tests = {n for n in names if n.startswith("test_")}
    assert len(tests) == 3
    assert {"run_operation_grant_read", "run_operation_cents"} <= names


def test_dbts_own_macros_are_not_offered(manifest):
    assert {m.package for m in manifest.macros} == {"jaffle"}


def test_a_new_manifest_changes_what_is_on_offer(document):
    newer = copy.deepcopy(document)
    newer["nodes"]["model.jaffle.renamed"] = {
        **newer["nodes"].pop("model.jaffle.order_events"),
        "unique_id": "model.jaffle.renamed",
        "name": "renamed",
        "alias": "renamed",
    }
    names = {c.name for c in offered_commands(parse_manifest(newer))}
    assert "run_renamed" in names
    assert "run_order_events" not in names


def test_a_model_command_is_invoked_as_dbt_is(manifest):
    assert command_named(manifest, "run_order_events").invocation == (
        "run",
        "--select",
        "order_events",
    )
    assert command_named(manifest, "source_freshness").invocation == ("source", "freshness")
    macro = command_named(manifest, "run_operation_grant_read")
    assert macro.invocation == ("run-operation", "grant_read")
    assert macro.arguments == ("args",)


def test_a_script_command_runs_where_dbt_is_installed(manifest):
    binding = script_binding(
        command_named(manifest, "run_order_events"), "dbt --project-dir '/srv/my jaffle'"
    )
    assert binding["argv"] == [
        "dbt",
        "--project-dir",
        "/srv/my jaffle",
        "run",
        "--select",
        "order_events",
    ]
    assert binding["stdout"] == "text"
    assert "json_argument" not in binding


def test_a_macro_script_command_passes_its_arguments_as_json(manifest):
    binding = script_binding(command_named(manifest, "run_operation_grant_read"), "dbt")
    assert binding["json_argument"] == {"flag": "--args", "argument": "args"}


def test_a_script_target_names_what_runs_dbt(manifest):
    with pytest.raises(TargetRefused):
        script_binding(command_named(manifest, "build"), "  ")


def test_a_cloud_command_runs_the_job_with_the_sources_token(manifest):
    binding = cloud_binding(
        command_named(manifest, "run_order_events"),
        source_id="dbt-prod",
        host="https://cloud.getdbt.com/",
        account_id="12",
        job_id="34",
    )
    assert binding["url"] == "https://cloud.getdbt.com/api/v2/accounts/12/jobs/34/run/"
    assert binding["method"] == "POST"
    assert binding["body"]["steps_override"] == ["dbt run --select order_events"]
    assert binding["credential_source"] == "dbt-prod"
    assert binding["auth_scheme"] == "Token"


def test_a_cloud_target_names_its_account_and_job(manifest):
    with pytest.raises(TargetRefused):
        cloud_binding(
            command_named(manifest, "build"), source_id="d", host="", account_id="1", job_id="2"
        )


def test_a_registered_warehouse_matching_adapter_and_database_is_used():
    sources = [
        _Source("other", "postgresql", "jaffle"),
        _Source("wh", "duckdb", path="/data/jaffle.duckdb"),
    ]
    assert warehouse_for("duckdb", "jaffle", sources) == "wh"
    assert warehouse_for("postgres", "JAFFLE", sources) == "other"
    assert warehouse_for("snowflake", "jaffle", sources) is None


def test_several_matches_resolve_in_id_order():
    sources = [_Source("b", "postgresql", "d"), _Source("a", "postgresql", "d")]
    assert warehouse_for("postgres", "d", sources) == "a"


def test_an_unmatched_database_is_offered_for_creation_with_its_type(manifest):
    offers = warehouse_offers(manifest.adapter, manifest.databases, [])
    assert [(o.database, o.source_type, o.source_id) for o in offers] == [
        ("jaffle", "duckdb", None)
    ]


def test_lineage_runs_through_compiled_sql_and_models_nobody_registered(manifest):
    views = compiled_views(manifest, {("analytics", "customer_orders"): "sales.customer_orders"})
    relations = {relation for relation, _ in views}
    assert "sales.customer_orders" in relations
    assert "analytics.stg_orders" in relations
    graph = build_federation_graph(views).graph
    # lifetime_value <- stg_orders.amount <- raw_orders.amount: through a model not registered.
    assert any(
        e.source == "analytics.stg_orders.amount"
        and e.target == "sales.customer_orders.lifetime_value"
        for e in graph.edges
    )
    assert any(e.target == "analytics.stg_orders.amount" for e in graph.edges)


def test_a_manifest_without_compiled_sql_has_no_lineage_to_add(document):
    for node in document["nodes"].values():
        node.pop("compiled_code", None)
    assert compiled_views(parse_manifest(document), {}) == []
