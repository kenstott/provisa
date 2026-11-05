# Copyright (c) 2026 Kenneth Stott
# Canary: fd4a9b0f-6a23-4d79-9013-7897ad6c57d1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the ServiceNow source type (REQ-1954): the pgwire server's model operand and its
refusals, the Trino catalog properties, the per-engine connectors, the bundle name and the
federation strategy."""

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from provisa.core.catalog import _build_catalog_properties
from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_base import Mechanism
from provisa.federation import served_tables
from provisa.federation.connector_config import MissingConnectorConfig
from provisa.federation.engine import (
    build_clickhouse_engine,
    build_duckdb_engine,
    build_pg_engine,
    live_source_types,
    reachable_source_types,
)
from provisa.federation.strategy import Strategy, engine_attaches, federate
from provisa.federation.trino_connectors import TRINO_CONNECTORS, trino_connector_name
from provisa.runtime_deps import pgwire_bundles as rd

INSTANCE_URL = "https://acme.service-now.com"


def _source(**kw) -> Source:
    return Source(
        **{
            "id": "snow-ops",
            "type": SourceType.servicenow,
            "base_url": INSTANCE_URL,
            "username": "admin",
            "password": "pw",
            **kw,
        }
    )


def _operand(source: Source, state_dir: Path) -> dict:
    return pr.build_model_json(source, state_dir=state_dir)["schemas"][0]["operand"]


# -- the pgwire server's model ---------------------------------------------------


def test_model_names_the_servicenow_schema_factory(tmp_path):
    model = pr.build_model_json(_source(), state_dir=tmp_path)
    schema = model["schemas"][0]
    assert schema["factory"] == "org.apache.calcite.adapter.servicenow.ServiceNowSchemaFactory"
    assert schema["name"] == model["defaultSchema"] == "snow_ops"


def test_operand_carries_the_instance_and_basic_credential(tmp_path):
    assert _operand(_source(), tmp_path) == {
        "instanceUrl": INSTANCE_URL,
        "authType": "basic",
        "username": "admin",
        "password": "pw",
    }


def test_the_operand_names_no_tables(tmp_path):
    # Every table of the instance is offered; none is chosen for the steward.
    assert "tables" not in _operand(_source(), tmp_path)


def test_instance_url_may_ride_in_host(tmp_path):
    assert _operand(_source(base_url=None, host=INSTANCE_URL), tmp_path)["instanceUrl"] == (
        INSTANCE_URL
    )


@pytest.mark.parametrize("url", ["acme.service-now.com", "http://acme.service-now.com", "https://"])
def test_an_instance_url_that_is_not_the_full_https_url_is_refused_by_both_readers(tmp_path, url):
    with pytest.raises(MissingConnectorConfig, match="instanceUrl must be the full https://"):
        _operand(_source(base_url=url), tmp_path)
    with pytest.raises(MissingConnectorConfig, match="instanceUrl must be the full https://"):
        _build_catalog_properties(_source(base_url=url), "")


@pytest.mark.parametrize(
    ("overrides", "names"),
    [
        ({"base_url": None, "host": ""}, "instanceUrl"),
        ({"username": ""}, "username"),
        ({"password": ""}, "password"),
    ],
)
def test_a_missing_setting_is_refused_by_name(tmp_path, overrides, names):
    with pytest.raises(MissingConnectorConfig, match=names):
        _operand(_source(**overrides), tmp_path)


def test_secrets_are_resolved_at_use(tmp_path, monkeypatch):
    monkeypatch.setenv("SNOW_TEST_SECRET", "from-env")
    assert _operand(_source(password="${env:SNOW_TEST_SECRET}"), tmp_path)["password"] == (
        "from-env"
    )


def test_bundle_is_pgwire_servicenow():
    assert rd.bundle_spec_for("servicenow").artifact_name == "pgwire-servicenow"
    assert "servicenow" in pr.PGWIRE_REPLICA_TYPES


# -- Trino catalog -----------------------------------------------------------------


def test_trino_connector_is_registered():
    assert "servicenow" in TRINO_CONNECTORS
    assert trino_connector_name("servicenow") == "servicenow"


def test_trino_props_carry_the_same_settings_as_the_pgwire_server():
    assert _build_catalog_properties(_source(), "") == {
        "instance-url": INSTANCE_URL,
        "auth-type": "basic",
        "username": "admin",
        "password": "pw",
        "schema": "snow_ops",
        "case-insensitive-name-matching": "true",
    }


def test_trino_reads_only():
    connector = TRINO_CONNECTORS["servicenow"]
    assert connector.mechanism in (Mechanism.ATTACH_R, Mechanism.SCAN)
    assert not connector.capability().write


# -- per-engine connectors -------------------------------------------------------------


@pytest.mark.parametrize(
    "engine", [build_duckdb_engine(), build_pg_engine(), build_clickhouse_engine()]
)
def test_the_engine_attaches_the_sources_pgwire_server(engine):
    assert engine_attaches(engine, "servicenow")
    assert engine.connectors["servicenow"].mechanism == Mechanism.ATTACH_R


def test_reachable_live_on_every_engine():
    for key in ("trino", "duckdb", "pg", "clickhouse"):
        assert "servicenow" in reachable_source_types(key), key
        assert "servicenow" in live_source_types(key), key


def test_strategy_is_virtual_where_attached_and_a_replica_elsewhere():
    from provisa.federation.engine import build_sqlalchemy_engine

    assert federate(_source(), build_duckdb_engine()) is Strategy.VIRTUAL
    assert federate(_source(), build_sqlalchemy_engine("mysql://h/db")) is Strategy.MATERIALIZED


def test_duckdb_attach_points_at_the_sources_endpoint(monkeypatch):
    ports = pr.PortPair(pgwire_port=5999, calcite_child_host="127.0.0.1", calcite_child_port=6099)
    monkeypatch.setattr(pr, "ensure_endpoint", lambda source, **kw: ports)
    details = build_duckdb_engine().connectors["servicenow"].details(_source())
    assert "port=5999" in details["attach"]
    assert details["remote_schema"] == "snow_ops"


# -- the Register Table list and a selected table's columns -----------------------------------


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


async def test_the_table_list_is_read_from_the_adapters_list_with_label_and_theme(adapter_reads):
    sent, answers = adapter_reads
    answers["_servicenow_tables"] = [
        ("change_request", "Change Request", "Change Management"),
        ("incident", "Incident", "Incident Management"),
    ]
    listed = await pr.servicenow_table_list(_source(), "snow_ops")
    assert listed == answers["_servicenow_tables"]
    assert sent == [
        'SELECT name, label, theme FROM "snow_ops"."_servicenow_tables" ORDER BY theme, name'
    ]


async def test_a_selected_tables_columns_are_read_for_that_table_alone(adapter_reads):
    sent, answers = adapter_reads
    answers["_servicenow_columns"] = [("sys_id", "character varying"), ("number", "integer")]
    columns = await pr.servicenow_columns(_source(), "snow_ops", "incident")
    assert columns == [("sys_id", "character varying"), ("number", "integer")]
    assert sent == [
        'SELECT column_name, data_type FROM "snow_ops"."_servicenow_columns" '
        "WHERE table_name = 'incident' ORDER BY ordinal_position"
    ]


async def test_a_table_name_that_is_not_an_identifier_is_refused(adapter_reads):
    with pytest.raises(ValueError, match="is not a table name"):
        await pr.servicenow_columns(_source(), "snow_ops", "incident' OR '1'='1")


async def test_the_register_table_list_groups_each_table_under_its_theme(
    adapter_reads, monkeypatch
):
    from provisa.api.admin import introspect

    _sent, answers = adapter_reads
    answers["_servicenow_tables"] = [("incident", "Incident", "Incident Management")]

    async def registered(source_id):
        return _source()

    monkeypatch.setattr(introspect, "_adapter_source", registered)
    # The org-vault binding: the source above carries no vault reference.
    monkeypatch.setattr(introspect, "_adapter_bound", contextlib.nullcontext)
    tables = await introspect.native_tables("snow-ops", "servicenow", "snow_ops", None, None, None)
    assert [(t.name, t.comment, t.group) for t in tables] == [
        ("incident", "Incident", "Incident Management")
    ]


# -- the server serves the registered tables -----------------------------------------------------


@pytest.fixture
def served(monkeypatch):
    monkeypatch.setattr(served_tables, "_SERVED_TABLES", {})


def test_the_registered_tables_are_the_tables_both_readers_serve(tmp_path, served):
    assert (
        served_tables.serve_tables("snow-ops", ["incident", "change_request", "incident"]) is True
    )
    assert _operand(_source(), tmp_path)["tables"] == ["change_request", "incident"]
    assert _build_catalog_properties(_source(), "")["tables"] == "change_request,incident"


def test_a_changed_registration_is_a_change_and_the_same_one_is_not(served):
    assert (
        served_tables.serve_tables("snow-ops", []) is False
    )  # nothing registered, nothing served before
    assert served_tables.serve_tables("snow-ops", ["incident"]) is True
    assert served_tables.serve_tables("snow-ops", ["incident"]) is False
    assert served_tables.serve_tables("snow-ops", []) is True


async def test_a_changed_registration_restarts_the_server_and_provisions_the_source_again(
    served, monkeypatch
):
    from types import SimpleNamespace

    from provisa.api import app_rebuild
    from provisa.api.admin import schema_common

    restarted: list[str] = []
    provisioned: list[tuple[str, str]] = []

    async def restart(model):
        restarted.append(model.id)

    monkeypatch.setattr(schema_common, "restart_source_server", restart)
    engine = SimpleNamespace(
        register_source=lambda source, password, catalog_name: provisioned.append(
            (source.id, catalog_name)
        )
    )
    table = SimpleNamespace(source_id="snow-ops", table_name="incident", import_set=None)
    state = SimpleNamespace(
        config=SimpleNamespace(sources=[_source()], tables=[table]),
        federation_engine=engine,
        source_catalogs={"snow-ops": "snow_ops"},
    )
    await app_rebuild.serve_registered_tables(state)
    await app_rebuild.serve_registered_tables(state)  # unchanged: left running
    assert restarted == ["snow-ops"]
    assert provisioned == [("snow-ops", "snow_ops")]


# -- writes -------------------------------------------------------------------------------------


def test_a_servicenow_table_takes_all_three_writes_through_its_own_server_and_returns_rows():
    from provisa.executor.writable import WritePath, resolve_write_path
    from provisa.executor.write_capability import table_write_ops, table_write_returns_rows

    table = {"source_id": "snow-ops", "table_name": "incident"}
    for engine in (build_duckdb_engine(), build_pg_engine(), None):
        assert resolve_write_path("servicenow", engine) is WritePath.PGWIRE
        assert table_write_ops(table, "servicenow", engine, source_path=None) == {
            "insert",
            "update",
            "delete",
        }
        assert table_write_returns_rows(table, "servicenow", engine, source_path=None) is True


def test_the_other_pgwire_written_sources_still_return_no_rows():
    from provisa.executor.write_capability import table_write_returns_rows

    table = {"source_id": "sf", "table_name": "Account"}
    assert table_write_returns_rows(table, "salesforce", None, source_path=None) is False


# -- a table bound to an import set ---------------------------------------------------------------


def test_a_bound_tables_import_set_reaches_both_readers(tmp_path, served):
    served_tables.serve_tables(
        "snow-ops", ["incident", "sys_user"], {"sys_user": "imp_user", "incident": "u_imp_inc"}
    )
    operand = _operand(_source(), tmp_path)
    assert operand["tables"] == ["incident", "sys_user"]
    assert operand["importSet"] == {"sys_user": "imp_user", "incident": "u_imp_inc"}
    props = _build_catalog_properties(_source(), "")
    assert props["import-set"] == "incident:u_imp_inc,sys_user:imp_user"


def test_an_unbound_source_names_no_import_set(tmp_path, served):
    served_tables.serve_tables("snow-ops", ["incident"])
    assert "importSet" not in _operand(_source(), tmp_path)
    assert "import-set" not in _build_catalog_properties(_source(), "")


def test_binding_or_unbinding_a_table_is_a_change(served):
    assert served_tables.serve_tables("snow-ops", ["incident"]) is True
    assert served_tables.serve_tables("snow-ops", ["incident"], {"incident": "u_imp_inc"}) is True
    assert served_tables.serve_tables("snow-ops", ["incident"], {"incident": "u_imp_inc"}) is False
    assert served_tables.serve_tables("snow-ops", ["incident"]) is True


async def test_the_transform_maps_into_a_table_are_read_for_it_alone(adapter_reads):
    """Only a map whose import set the integration user can read is offered: a live instance
    keeps maps of plugins it does not have (oidc_adfs_import), and binding to one is refused."""
    sent, answers = adapter_reads
    answers["_servicenow_transform_maps"] = [("User Import", "imp_user")]
    maps = await pr.servicenow_transform_maps(_source(), "snow_ops", "sys_user")
    assert maps == [("User Import", "imp_user")]
    assert sent == [
        'SELECT name, source_table FROM "snow_ops"."_servicenow_transform_maps" '
        "WHERE target_table = 'sys_user' AND active "
        'AND source_table IN (SELECT name FROM "snow_ops"."_servicenow_tables") ORDER BY name'
    ]


@pytest.fixture
def bound_check(monkeypatch):
    """The save-time import set check, against a stored source of ``kind`` whose instance has one
    active transform map into sys_user."""
    from provisa.api.admin import _import_set
    from provisa.core.repositories import source as source_repo

    kind = {"type": "servicenow"}

    async def stored(conn, source_id):
        return {"id": source_id, "type": kind["type"]}

    async def maps(source, schema, target):
        return [("User Import", "imp_user")] if target == "sys_user" else []

    monkeypatch.setattr(source_repo, "get", stored)
    monkeypatch.setattr("provisa.core.repositories.source.source_from_row", lambda row: row)
    monkeypatch.setattr(pr, "servicenow_transform_maps", maps)
    monkeypatch.setattr("provisa.core.secrets_store.bound_to_request_org", contextlib.nullcontext)
    return _import_set.table_import_set_refusal, kind


def _table(import_set, table_name="sys_user"):
    from types import SimpleNamespace

    return SimpleNamespace(
        source_id="snow-ops", schema_name="snow_ops", table_name=table_name, import_set=import_set
    )


async def test_a_table_bound_to_a_transform_maps_import_set_is_saved(bound_check):
    check, _kind = bound_check
    assert await check(None, _table("imp_user")) is None
    assert await check(None, _table(None)) is None


async def test_an_import_set_no_active_map_writes_into_the_table_from_is_refused(bound_check):
    check, _kind = bound_check
    refused = await check(None, _table("imp_user", table_name="incident"))
    assert refused is not None and refused.code == "schema.import_set_unknown"


async def test_an_import_set_on_a_table_of_another_source_type_is_refused(bound_check):
    check, kind = bound_check
    kind["type"] = "postgresql"
    refused = await check(None, _table("imp_user"))
    assert refused is not None and refused.code == "schema.import_set_not_servicenow"


# -- a bundle named explicitly ------------------------------------------------------------------


def test_a_bundle_named_by_its_setting_is_used_and_nothing_is_downloaded(tmp_path, monkeypatch):
    bundle = tmp_path / "pgwire-servicenow"
    (bundle / "bin").mkdir(parents=True)
    (bundle / "bin" / "pgwire-servicenow").write_text("#!/bin/sh\n")
    monkeypatch.setenv("PROVISA_PGWIRE_BUNDLE_SERVICENOW", str(bundle))

    def download(spec, dest):
        raise AssertionError("the pinned release was fetched")

    resolver = rd.BundleResolver(cache_root=tmp_path / "cache", downloader=download)
    assert resolver.resolve(rd.bundle_spec_for("servicenow")) == bundle


def test_a_named_bundle_without_its_launcher_is_refused_not_replaced(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_PGWIRE_BUNDLE_SERVICENOW", str(tmp_path / "missing"))

    def download(spec, dest):
        raise AssertionError("the pinned release was fetched")

    resolver = rd.BundleResolver(cache_root=tmp_path / "cache", downloader=download)
    with pytest.raises(
        rd.BundleUnavailable, match="setting connectors.servicenow.bundle_dir names"
    ):
        resolver.resolve(rd.bundle_spec_for("servicenow"))
