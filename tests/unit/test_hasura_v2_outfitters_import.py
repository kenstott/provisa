# Copyright (c) 2026 Kenneth Stott
# Canary: ed13f731-d857-4d3f-a8ba-599b4269494f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A realistic Hasura v2 export converts to a model that basically works (REQ-417, REQ-2074).

The fixture is an outfitter's store as a Hasura v2 export, both as the CLI writes it (a
metadata directory of ``!include`` files) and as the metadata API returns it (one document):
tables with custom names, object and array relationships, select permissions as row rules and
column lists, insert/update/delete permissions with check, filter and set, an inherited role, a
computed field, a tracked function, a remote schema, an action, a cron trigger and an event
trigger. What converts is carried; what a Provisa feature does better is mapped to it, saying
why; only what has no Provisa equivalent is left to redo by hand, saying what to do."""

# Requirements: REQ-417, REQ-2074, REQ-186, REQ-187, REQ-205, REQ-2054

from __future__ import annotations

import json
from pathlib import Path

import pytest

from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir, parse_metadata_document
from provisa.import_shared.warnings import WarningCollector

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "hasura_v2_outfitters"


def _convert(layout: str):
    collector = WarningCollector()
    if layout == "directory":
        metadata = parse_metadata_dir(_FIXTURE / "metadata", collector)
    else:
        metadata = parse_metadata_document(
            json.loads((_FIXTURE / "metadata.json").read_text()), collector
        )
    return convert_metadata(metadata, collector), collector


@pytest.fixture(params=["directory", "document"])
def converted(request):
    return _convert(request.param)


def _table(config, name):
    return next(t for t in config.tables if t.table_name == name)


def _redo(collector, category):
    return [w.message for w in collector.warnings if w.category == category]


# --- the two layouts --------------------------------------------------------------------------


def test_the_directory_export_converts_as_the_document_does():
    """The CLI's layout lists a database's functions behind an !include index too; it crashed."""
    (by_dir, _), (by_doc, _) = _convert("directory"), _convert("document")
    assert sorted(t.table_name for t in by_dir.tables) == sorted(
        t.table_name for t in by_doc.tables
    )
    assert sorted(f.name for f in by_dir.functions) == sorted(f.name for f in by_doc.functions)
    assert by_dir.sources[0].type == by_doc.sources[0].type


def test_a_connection_read_from_an_environment_variable_is_not_guessed(converted):
    """databases.yaml (or the document's configuration) names the URL's variable; no host,
    database, user or password is made up for it, and the report says how to supply it."""
    config, collector = converted
    (source,) = [s for s in config.sources if s.id == "default"]
    assert (source.host, source.database, source.username, source.password) == ("", "", "", "")
    (redo,) = _redo(collector, "sources")
    assert "HASURA_GRAPHQL_DATABASE_URL" in redo


# --- names ------------------------------------------------------------------------------------


def test_custom_column_names_are_read_from_column_config(converted):
    config, _ = converted
    customers = _table(config, "customers")
    aliases = {c.name: c.alias for c in customers.columns}
    assert aliases["first_name"] == "givenName" and aliases["last_name"] == "familyName"


def test_root_field_names_beyond_select_are_reported(converted):
    config, collector = converted
    assert _table(config, "products").alias == "catalog"
    reported = " ".join(m.item + " " + m.detail for m in collector.mapped)
    assert "catalog_item" in reported and "catalog_aggregate" in reported


# --- relationships ----------------------------------------------------------------------------


def test_each_relationship_keeps_its_hasura_name_as_its_field(converted):
    config, collector = converted
    fields = {(r.source_table_id, r.graphql_alias) for r in config.relationships}
    assert ("orders", "items") in fields and ("orders", "customer") in fields
    assert ("order_items", "product") in fields and ("customers", "orders") in fields
    carried = [c.item for c in collector.carried if c.category == "relationships"]
    assert "orders.items" in carried


# --- row rules --------------------------------------------------------------------------------


def test_a_row_rule_through_a_relationship_becomes_an_exists_on_its_table(converted):
    config, collector = converted
    (rule,) = [
        r for r in config.rls_rules if r.table_id == "order_items" and r.role_id == "customer"
    ]
    assert rule.filter.startswith("EXISTS (SELECT 1 FROM orders AS _r1 WHERE _r1.id = order_id")
    assert "current_setting('provisa.hasura_user_id')" in rule.filter
    assert "unsupported" not in rule.filter
    assert not _redo(collector, "rls")


def test_the_converted_rules_validate_against_the_model(converted):
    from provisa.compiler.rls_validate import validate_rls_predicate

    config, _ = converted
    registry = [
        {
            "table_name": t.table_name,
            "alias": t.alias,
            "domain_id": t.domain_id,
            "columns": [{"column_name": c.name, "data_type": "varchar"} for c in t.columns],
        }
        for t in config.tables
    ]
    for rule in config.rls_rules:
        target = [r for r in registry if (r["alias"] or r["table_name"]) == rule.table_id]
        assert validate_rls_predicate(rule.filter, target, registry) is None, rule


# --- roles ------------------------------------------------------------------------------------


def test_an_inherited_role_of_several_roles_holds_the_union_of_their_rights(converted):
    """Hasura's support_lead inherits support and analyst; it reads what either reads, and its
    rows are the OR of theirs -- none here, support reading orders unfiltered."""
    config, collector = converted
    lead = next(r for r in config.roles if r.id == "support_lead")
    assert lead.parents == ["viewer"]  # written out as the union; builds on viewer (REQ-2155)
    customers = _table(config, "customers")
    visible = {c.name for c in customers.columns if "support_lead" in c.visible_to}
    assert {"email", "sf_account_id", "created_at"} <= visible
    assert not [
        r for r in config.rls_rules if r.role_id == "support_lead" and r.table_id == "orders"
    ]
    grants = {(g.role, g.operation) for g in _table(config, "orders").write_grants}
    assert ("support_lead", "update") in grants
    assert any(m.item == "support_lead" for m in collector.mapped)


# --- functions, actions, triggers -------------------------------------------------------------


def test_a_computed_field_is_left_to_redo_naming_its_function(converted):
    config, collector = converted
    assert "full_name" not in {f.name for f in config.functions}
    (redo,) = _redo(collector, "computed_fields")
    assert "full_name" in redo and "customer_full_name" in redo


def test_a_tracked_function_waits_for_its_signature_from_the_database(converted):
    """The export names the function and its roles, not its arguments or what it returns: the
    preview reads them from the database it connects to."""
    config, collector = converted
    assert "search_products" not in {f.name for f in config.functions}
    (pending,) = collector.pending_functions
    assert pending["name"] == "search_products" and pending["kind"] == "query"
    assert pending["visible_to"] == ["customer", "support"]


def test_an_action_is_a_command_calling_its_handler(converted):
    config, collector = converted
    (hook,) = config.webhooks
    assert hook.name == "refund_order" and hook.kind == "mutation"
    assert hook.url == "${env:PAYMENTS_BASE_URL}/refund"
    assert [(a.name, a.type) for a in hook.arguments] == [
        ("order_id", "integer"),
        ("reason", "text"),
    ]
    assert hook.visible_to == ["support"]
    assert {(t.name, t.type) for t in hook.inline_return_type} == {
        ("refund_id", "String"),
        ("status", "String"),
    }
    assert "refund_order" not in {f.name for f in config.functions}


def test_a_cron_trigger_is_a_scheduled_trigger(converted):
    config, _ = converted
    (trigger,) = [t for t in config.scheduled_triggers if t.cron]
    assert trigger.id == "nightly_sales_digest" and trigger.cron == "0 6 * * *"
    assert trigger.url == "${env:REPORTS_BASE_URL}/digest"


def test_an_event_trigger_is_a_change_trigger_on_its_table(converted):
    config, collector = converted
    assert not config.event_triggers
    (trigger,) = [t for t in config.scheduled_triggers if t.on_table]
    assert trigger.id == "order_placed" and trigger.on_event == "change"
    assert trigger.on_table == "default.public.orders"
    assert trigger.url == "${env:FULFILMENT_WEBHOOK_URL}"
    assert any(m.item == "order_placed" for m in collector.mapped)


def test_each_session_variable_is_mapped_naming_the_claim_that_supplies_it(converted):
    """X-Hasura-User-Id is the app's own user key, read from the token's x-hasura-user-id claim
    as Hasura read it: it is provisa.hasura_user_id, and provisa.user_id stays the identity."""
    config, collector = converted
    presets = [
        p for t in config.tables for g in t.write_grants for p in g.presets if p.source == "session"
    ]
    assert presets and {p.name for p in presets} == {"hasura_user_id"}
    (item,) = [m for m in collector.mapped if m.category == "session_variables"]
    assert item.item == "X-Hasura-User-Id"
    assert "hasura_user_id" in item.detail and "x-hasura-user-id" in item.detail


# --- the report -------------------------------------------------------------------------------


def test_the_report_says_what_was_carried_mapped_and_left_to_redo(converted):
    _, collector = converted
    assert {c.category for c in collector.carried} >= {"tables", "relationships", "permissions"}
    for item in collector.mapped:
        assert item.detail, item  # each mapping says why
    redo_categories = {w.category for w in collector.warnings}
    assert redo_categories <= {"sources", "computed_fields", "triggers", "environment"}, (
        collector.warnings
    )


def test_each_environment_variable_a_handler_url_reads_is_named(converted):
    """Hasura read its handler URLs from its own environment; Provisa reads them from its own,
    and a configuration naming one that is not set does not load: each is named, once."""
    _, collector = converted
    named = _redo(collector, "environment")
    for var in ("PAYMENTS_BASE_URL", "REPORTS_BASE_URL", "FULFILMENT_WEBHOOK_URL"):
        assert sum(var in m for m in named) == 1, named


def _with(layout_mutator):
    collector = WarningCollector()
    doc = json.loads((_FIXTURE / "metadata.json").read_text())
    layout_mutator(doc)
    return convert_metadata(parse_metadata_document(doc, collector), collector), collector


def _orders(doc):
    return next(
        t for t in doc["metadata"]["sources"][0]["tables"] if t["table"]["name"] == "orders"
    )


def test_a_relationship_named_as_a_column_takes_the_provisa_name():
    def rename(doc):
        _orders(doc)["object_relationships"][0]["name"] = "status"

    config, collector = _with(rename)
    rel = next(r for r in config.relationships if r.id == "orders.status")
    assert rel.graphql_alias is None
    assert any("'status'" in m for m in _redo(collector, "relationships"))


def test_a_relationship_declaring_a_join_already_declared_merges_into_it():
    def duplicate(doc):
        rels = _orders(doc)["array_relationships"]
        rels.append({**rels[0], "name": "lines"})

    config, collector = _with(duplicate)
    assert "orders.lines" not in {r.id for r in config.relationships}
    assert any(c.item == "orders.lines" and "merged" in c.detail for c in collector.carried)


def test_an_input_object_argument_is_the_json_object_the_handler_receives():
    """Hasura's own sample: action foo takes foo: FooInput!, an input object of custom_types."""
    collector = WarningCollector()
    sample = _FIXTURE.parent / "hasura_v2_t1_metadata.json"
    metadata = parse_metadata_document(json.loads(sample.read_text()), collector)
    (hook,) = convert_metadata(metadata, collector).webhooks
    assert [(a.name, a.type) for a in hook.arguments] == [("foo", "json")]


def test_an_input_object_in_the_actions_sdl_is_the_json_object_the_handler_receives(tmp_path):
    from provisa.hasura_v2.models import HasuraAction, HasuraMetadata
    from provisa.hasura_v2.parser import _read_actions_sdl

    metadata = HasuraMetadata(actions=[HasuraAction(name="refund_order")])
    _read_actions_sdl(
        "input RefundInput { order_id: Int! }\n"
        "type Mutation { refund_order(refund: RefundInput!, note: String): RefundResult }\n"
        "type RefundResult { status: String! }\n",
        metadata,
    )
    (hook,) = convert_metadata(metadata, WarningCollector()).webhooks
    assert [(a.name, a.type) for a in hook.arguments] == [("refund", "json"), ("note", "text")]
