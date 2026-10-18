# Copyright (c) 2026 Kenneth Stott
# Canary: 4c613166-836a-4478-9bab-b2a4699a9333
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Jira as a source type read through its Calcite adapter's pgwire server (REQ-2069): the server's
model, the forms its registered tables are, the per-engine connectors, the bundle, the write route,
the Register Table list and a form's columns."""

# Requirements: REQ-2069

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from provisa.core.models import Column, Source, SourceType, Table
from provisa.federation import jira
from provisa.federation import pgwire_replica as pr
from provisa.federation import served_tables
from provisa.federation.connector_base import Mechanism
from provisa.federation.connector_config import MissingConnectorConfig
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    live_source_types,
    reachable_source_types,
)
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.runtime_deps import pgwire_bundles as rd

SITE = "https://acme.atlassian.net"


def _source(**kw) -> Source:
    return Source(
        **{
            "id": "tickets",
            "type": SourceType.jira,
            "base_url": SITE,
            "username": "ada@acme.example",
            "password": "atl-token",
            **kw,
        }
    )


def _column(name: str, path: str | None = None) -> Column:
    return Column(name=name, data_type="varchar", visible_to=["admin"], path=path)


def _incident(**kw) -> Table:
    return Table(
        **{
            "source_id": "tickets",
            "domain_id": "support",
            "schema": "tickets",
            "table": "incident",
            "arguments": {"project": "KAN", "issue_type": "Incident"},
            "columns": [
                _column("key"),
                _column("summary"),
                _column("status"),
                _column("priority"),
                _column("flagged", "customfield_10021"),
            ],
            **kw,
        }
    )


def _operand(source: Source, state_dir: Path) -> dict:
    return pr.build_model_json(source, state_dir=state_dir)["schemas"][0]["operand"]


@pytest.fixture
def served(monkeypatch):
    monkeypatch.setattr(served_tables, "_SERVED_TABLES", {})
    monkeypatch.setattr(served_tables, "_IMPORT_SETS", {})
    monkeypatch.setattr(served_tables, "_FORMS", {})


# -- the pgwire server's model ---------------------------------------------------------------------


def test_model_names_the_jira_schema_factory(tmp_path):
    model = pr.build_model_json(_source(), state_dir=tmp_path)
    assert model["schemas"][0]["factory"] == "org.apache.calcite.adapter.jira.JiraSchemaFactory"


def test_operand_carries_the_site_and_the_accounts_email_and_token(tmp_path, served):
    assert _operand(_source(), tmp_path) == {
        "baseUrl": SITE,
        "email": "ada@acme.example",
        "apiToken": "atl-token",
    }


def test_the_site_may_ride_in_host(tmp_path, served):
    assert _operand(_source(base_url=None, host=SITE + "/"), tmp_path)["baseUrl"] == SITE


@pytest.mark.parametrize("url", ["acme.atlassian.net", "http://acme.atlassian.net", "https://"])
def test_a_site_that_is_not_the_full_https_address_is_refused(url):
    with pytest.raises(MissingConnectorConfig, match="baseUrl must be the full https://"):
        jira.jira_settings(_source(base_url=url))


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"base_url": None}, "baseUrl"),
        ({"username": ""}, "the account's email"),
        ({"password": ""}, "the API token"),
    ],
)
def test_a_missing_setting_is_refused_by_name(overrides, names):
    with pytest.raises(MissingConnectorConfig, match=names):
        jira.jira_settings(_source(**overrides))


def test_secrets_are_resolved_at_use(monkeypatch, served):
    monkeypatch.setenv("JIRA_TOKEN_UNDER_TEST", "from-env")
    settings = jira.jira_settings(_source(password="${env:JIRA_TOKEN_UNDER_TEST}"))
    assert settings["apiToken"] == "from-env"


def test_bundle_is_pgwire_jira():
    assert rd.BUNDLE_CONNECTOR["jira"] == "jira"


# -- the forms its registered tables are ------------------------------------------------------------


def test_a_table_is_the_form_its_arguments_and_columns_say(served):
    assert jira.form_of(_incident()) == {
        "name": "incident",
        "project": "KAN",
        "issueType": "Incident",
        "fields": [
            {"field": "priority", "column": "priority"},
            {"field": "customfield_10021", "column": "flagged"},
        ],
    }


@pytest.mark.parametrize(
    ("arguments", "says"),
    [
        ({"project": "KAN"}, "it lacks issue_type"),
        ({"project": "KAN", "issue_type": "Incident", "jql": "x"}, "jql is not one"),
    ],
)
def test_a_table_that_is_no_form_is_refused_by_name(arguments, says):
    with pytest.raises(ValueError, match=says):
        jira.form_of(_incident(arguments=arguments))


def test_the_registered_forms_reach_the_server(tmp_path, served):
    form = jira.form_of(_incident())
    assert served_tables.serve_tables("tickets", ["incident"], forms=[form]) is True
    assert _operand(_source(), tmp_path)["forms"] == [form]


def test_a_changed_form_is_a_change_and_the_same_one_is_not(served):
    form = jira.form_of(_incident())
    assert served_tables.serve_tables("tickets", ["incident"], forms=[form]) is True
    assert served_tables.serve_tables("tickets", ["incident"], forms=[form]) is False
    fewer = jira.form_of(_incident(columns=[_column("key"), _column("priority")]))
    assert served_tables.serve_tables("tickets", ["incident"], forms=[fewer]) is True


def test_a_config_jira_table_that_is_no_form_fails_the_load():
    from provisa.core.config_loader import _validate_table_arguments
    from provisa.core.models import ProvisaConfig

    config = ProvisaConfig.model_construct(
        sources=[_source()], tables=[_incident(arguments={"project": "KAN"})]
    )
    with pytest.raises(ValueError, match="it lacks issue_type"):
        _validate_table_arguments(config)


# -- per-engine connectors -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "engine", [build_duckdb_engine(), build_pg_engine(), build_clickhouse_engine()]
)
def test_the_engine_attaches_the_sources_pgwire_server(engine):
    assert engine_attaches(engine, "jira")
    assert engine.connectors["jira"].mechanism == Mechanism.ATTACH_R


def test_live_where_attached_and_landed_through_its_server_on_trino():
    for key in ("duckdb", "pg", "clickhouse"):
        assert "jira" in live_source_types(key), key
    assert "jira" in reachable_source_types("trino")
    assert "jira" not in live_source_types("trino")


def test_strategy_is_virtual_where_attached_and_a_replica_elsewhere():
    from provisa.federation.engine import build_sqlalchemy_engine

    assert federate(_source(), build_duckdb_engine()) is Strategy.VIRTUAL
    assert federate(_source(), build_sqlalchemy_engine("mysql://h/db")) is Strategy.MATERIALIZED


# -- writes ----------------------------------------------------------------------------------------


def test_a_jira_table_takes_all_three_writes_through_its_own_server_and_returns_rows():
    from provisa.executor.write_capability import table_write_ops, table_write_returns_rows
    from provisa.executor.writable import WritePath, resolve_write_path

    table = {"source_id": "tickets", "table_name": "incident"}
    for engine in (build_duckdb_engine(), build_pg_engine(), None):
        assert resolve_write_path("jira", engine) is WritePath.PGWIRE
        assert table_write_ops(table, "jira", engine, source_path=None) == {
            "insert",
            "update",
            "delete",
        }
        assert table_write_returns_rows(table, "jira", engine, source_path=None) is True


# -- the Register Table list and a form's columns ---------------------------------------------------


@pytest.fixture
def adapter_reads(monkeypatch):
    """The statements discovery sends the source's server, answered from ``answers``."""
    sent: list[str] = []
    answers: dict[str, list[tuple]] = {}

    async def read(source, statement):
        sent.append(statement)
        return next(rows for key, rows in answers.items() if key in statement)

    monkeypatch.setattr(pr, "_read_information_schema", read)
    return sent, answers


async def test_the_form_list_is_read_from_the_adapters_list(adapter_reads):
    sent, answers = adapter_reads
    answers["_jira_forms"] = [
        ("KAN", "Incident", "kan_incident"),
        ("KAN", "Problem", "kan_problem"),
    ]
    assert await pr.jira_form_list(_source(), "tickets") == answers["_jira_forms"]
    assert sent == [
        'SELECT project_key, issue_type, name FROM "tickets"."_jira_forms" '
        "ORDER BY project_key, issue_type"
    ]


async def test_a_forms_fields_are_read_for_that_form_alone_its_names_quoted(adapter_reads):
    sent, answers = adapter_reads
    answers["_jira_fields"] = [("priority", "Priority", "priority", "character varying", 0, 0)]
    fields = await pr.jira_fields(_source(), "tickets", "KAN", "Customer's Request")
    assert fields == [("priority", "Priority", "priority", "character varying", False, False)]
    assert sent == [
        "SELECT field_id, field_name, column_name, data_type, required, read_only "
        'FROM "tickets"."_jira_fields" '
        "WHERE project_key = 'KAN' AND issue_type = 'Customer''s Request' "
        "ORDER BY ordinal_position"
    ]


def test_a_forms_columns_are_the_fixed_ones_then_one_per_field_as_the_adapter_names_it():
    columns = jira.form_columns(
        [
            ("summary", "Summary", "summary", "character varying", True, False),
            ("priority", "Priority", "priority", "character varying", False, False),
            ("customfield_10016", "Story Points", "story_points", "double precision", False, False),
            ("assignee", "Assignee", "assignee", "character varying", False, False),
            (
                "assignee.displayName",
                "Assignee (display name)",
                "assignee_name",
                "character varying",
                False,
                True,
            ),
            ("customfield_10099", "Priority", "priority", "character varying", False, False),
        ]
    )
    assert columns == [
        ("key", "character varying", None, True),
        ("id", "character varying", None, True),
        ("summary", "character varying", None, False),
        ("status", "character varying", None, False),
        ("created", "timestamp", None, True),
        ("updated", "timestamp", None, True),
        ("priority", "character varying", "priority", False),
        ("story_points", "double precision", "customfield_10016", False),
        ("assignee", "character varying", "assignee", False),
        ("assignee_name", "character varying", "assignee.displayName", True),
        # A second field named Priority: named by its field id, never renamed to make room.
        ("customfield_10099", "character varying", "customfield_10099", False),
    ]


async def test_the_register_table_list_groups_each_form_under_its_project(
    adapter_reads, monkeypatch
):
    from provisa.api.admin import introspect

    _sent, answers = adapter_reads
    answers["_jira_forms"] = [("KAN", "Incident", "kan_incident")]

    async def registered(source_id):
        return _source()

    monkeypatch.setattr(introspect, "_adapter_source", registered)
    monkeypatch.setattr(introspect, "_adapter_bound", contextlib.nullcontext)
    tables = await introspect.native_tables("tickets", "jira", "tickets", None, None, None)
    assert [(t.name, t.comment, t.group) for t in tables] == [("kan_incident", "Incident", "KAN")]


async def test_a_form_registered_from_the_admin_gets_its_project_issue_type_and_fields(
    adapter_reads, monkeypatch
):
    from provisa.api.admin import introspect

    _sent, answers = adapter_reads
    answers["_jira_forms"] = [("KAN", "Incident", "incident")]
    answers["_jira_fields"] = [
        ("priority", "Priority", "priority", "character varying", 0, 0),
        ("customfield_10021", "Flagged", "flagged", "character varying", 0, 0),
        (
            "assignee.displayName",
            "Assignee (display name)",
            "assignee_name",
            "character varying",
            0,
            1,
        ),
    ]

    async def registered(source_id):
        return _source()

    monkeypatch.setattr(introspect, "_adapter_source", registered)
    monkeypatch.setattr(introspect, "_adapter_bound", contextlib.nullcontext)
    model = _incident(
        arguments={}, columns=[_column("key"), _column("priority"), _column("flagged")]
    )
    await introspect.complete_jira_form(model)
    assert model.arguments == {"project": "KAN", "issue_type": "Incident"}
    assert [(c.name, c.path) for c in model.columns] == [
        ("key", None),
        ("priority", "priority"),
        ("flagged", "customfield_10021"),
    ]
    model.columns.append(_column("not_a_field"))
    with pytest.raises(ValueError, match="no field of its form is column 'not_a_field'"):
        await introspect.complete_jira_form(model)
    model.columns[-1] = Column(name="assignee_name", data_type="varchar", visible_to=["admin"])
    await introspect.complete_jira_form(model)
    flagged = {c.name: c.description for c in model.columns}
    assert flagged["assignee_name"] == introspect.READ_ONLY_NOTE
    assert flagged["key"] == introspect.READ_ONLY_NOTE
    assert flagged["priority"] is None
