# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8d5f17-c2a4-4e69-b0d3-86f1a7e94c25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The telemetry settings are stored in the control plane (REQ-1913).

Saving the ``otel`` block used to write the config file of the node that served the request and
the environment of the one worker process that served it, inside an ``except Exception: pass``:
the other workers and instances never saw the change, and a value that could not be used was
dropped without a word. The block is now operator settings like any other — stored, validated,
resolved in the one order — and what a process does about a change is one function."""

# Requirements: REQ-1913, REQ-545, REQ-549

from __future__ import annotations

import os
import types

import pytest

import provisa.api.app  # noqa: F401 - imported before any test narrows process state
from provisa.api.errors import ApiError
from provisa.core import deployment_settings, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    from provisa.encryption import runtime

    for s in settings_registry.all_settings():
        if s.env:
            monkeypatch.delenv(s.env, raising=False)
    monkeypatch.delenv(settings_registry.IGNORE_STORED_ENV, raising=False)
    monkeypatch.setattr(settings_registry, "_config", {})
    monkeypatch.setattr(settings_registry, "_frozen", None)
    monkeypatch.setattr(runtime, "_service", None)
    cfg = tmp_path / "provisa.yaml"
    cfg.write_text("sources: []\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table])
    db = Database(engine, name="platform")
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.setattr(deployment_settings, "_db", db)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=db, roles={}), raising=False
    )
    yield types.SimpleNamespace(db=db, config=cfg)
    engine.dispose()


@pytest.fixture
def attached(monkeypatch):
    """Record exporter attachments instead of building exporters."""
    from provisa.api import otel_setup

    calls: list[tuple] = []
    monkeypatch.setattr(otel_setup, "attach_otlp_exporters", lambda *a: calls.append(a))
    monkeypatch.setattr(otel_setup, "_attached", None)
    return calls


# --- saving the block ----------------------------------------------------------------------------


def test_the_block_is_stored_in_the_control_plane_not_the_file_or_the_environment(
    control_plane, attached
):
    from provisa.api.admin.settings_router import _apply_otel

    before = control_plane.config.read_text()
    updated: list[str] = []
    _apply_otel(
        {
            "endpoint": "http://collector.example:4317",
            "protocol": "grpc",
            "service_name": "provisa-east",
            "sample_rate": 0.25,
            "log_level": "DEBUG",
            "compact_cron": "*/5 * * * *",
            "compact_batch_size": 42,
            "compact_file_chunk": 7,
            "compact_max_files_per_run": 70,
            "span_export_delay_millis": 250,
            "otlp2parquet_max_age_secs": 9,
            "collector_batch_timeout_ms": 500,
            "ops_snapshot_retention_hours": 24,
            "s3_endpoint": "http://localhost:9000",
            "support_endpoint": "http://support.example:4317",
            "support_redact_sql_literals": False,
            "support_redact_attributes": ["db.statement"],
            "subsystem_traces": {"catalog_database": True, "outbound_http": False},
        },
        updated,
        updated_by="alice",
    )
    value = settings_registry.resolve
    assert value("otel.endpoint") == ("http://collector.example:4317", "stored")
    assert value("otel.service_name") == ("provisa-east", "stored")
    assert value("otel.sample_rate") == (0.25, "stored")
    assert value("otel.log_level") == ("DEBUG", "stored")
    assert value("otel.compact_cron") == ("*/5 * * * *", "stored")
    assert value("otel.compact_batch_size").value == 42
    assert value("otel.compact_file_chunk").value == 7
    assert value("otel.compact_max_files_per_run").value == 70
    assert value("otel.span_export_delay_millis").value == 250
    assert value("otel.otlp2parquet_max_age_secs").value == 9
    assert value("otel.collector_batch_timeout_ms").value == 500
    assert value("otel.ops_snapshot_retention_hours").value == 24
    assert value("otel.s3_endpoint").value == "http://localhost:9000"
    assert value("otel.support_endpoint").value == "http://support.example:4317"
    assert value("otel.support_redact_sql_literals").value is False
    assert value("otel.support_redact_attributes").value == ["db.statement"]
    assert value("otel.subsystem_traces.catalog_database") == (True, "stored")
    assert value("otel.subsystem_traces.outbound_http") == (False, "stored")
    assert value("otel.subsystem_traces.http_api") == (True, "default")
    assert {"otel.endpoint", "otel.sample_rate", "otel.subsystem_traces"} <= set(updated)
    # Neither the node's config file nor this worker's environment was written.
    assert control_plane.config.read_text() == before
    assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ
    assert "OTEL_SERVICE_NAME" not in os.environ


def test_a_blank_retention_clears_the_stored_value(control_plane, attached):
    from provisa.api.admin.settings_router import _apply_otel

    _apply_otel({"ops_snapshot_retention_hours": 24}, [], updated_by="a")
    _apply_otel({"ops_snapshot_retention_hours": ""}, [], updated_by="a")
    assert settings_registry.resolve("otel.ops_snapshot_retention_hours") == (None, "default")


def test_a_value_that_cannot_be_used_is_refused_naming_the_field(control_plane, attached):
    """It used to be swallowed: the save answered success and nothing changed."""
    from provisa.api.admin.settings_router import _apply_otel

    with pytest.raises(ApiError) as err:
        _apply_otel({"service_name": "ok", "sample_rate": 7}, [], updated_by="a")
    assert (err.value.status_code, err.value.code) == (400, "settings.invalid_value")
    assert err.value.params["field"] == "otel.sample_rate"
    assert err.value.params["reason"] == "above_max"
    # Nothing of the body was stored.
    assert settings_registry.resolve("otel.service_name") == ("provisa", "default")


def test_an_unknown_subsystem_or_protocol_is_refused(control_plane, attached):
    from provisa.api.admin.settings_router import _apply_otel

    with pytest.raises(ApiError) as err:
        _apply_otel({"subsystem_traces": {"mainframe": True}}, [], updated_by="a")
    assert err.value.params == {
        "field": "otel.subsystem_traces.mainframe",
        "reason": "unknown_setting",
    }
    with pytest.raises(ApiError) as err:
        _apply_otel({"protocol": "carrier-pigeon"}, [], updated_by="a")
    assert err.value.params["field"] == "otel.protocol"


def test_the_settings_endpoint_reports_the_stored_block(control_plane, attached):
    from provisa.api.admin.settings_router import _apply_otel, _otel_block

    _apply_otel(
        {"endpoint": "http://collector.example:4317", "subsystem_traces": {"result_cache": False}},
        [],
        updated_by="a",
    )
    block = _otel_block()
    assert block["endpoint"] == "http://collector.example:4317"
    assert block["service_name"] == "provisa"
    assert block["sample_rate"] == 1.0
    assert block["subsystem_traces"]["result_cache"] is False
    assert block["subsystem_traces"]["http_api"] is True
    assert block["support_redact_sql_literals"] is True
    assert block["support_endpoint"] == "" and block["support_redact_attributes"] == []


# --- what a process does about a change: one function --------------------------------------------


def test_the_exporters_are_attached_when_the_endpoint_changes_and_not_otherwise(
    control_plane, attached
):
    from provisa.api import otel_setup

    otel_setup.apply_exporter_settings()
    assert attached == []  # no endpoint: nothing to export to
    settings_registry.store(
        control_plane.db,
        {"otel.endpoint": "http://collector.example:4317", "otel.service_name": "east"},
        updated_by="another-worker",
    )
    otel_setup.apply_exporter_settings()
    assert attached == [("http://collector.example:4317", "east", "grpc")]
    otel_setup.apply_exporter_settings()
    assert len(attached) == 1


def test_saving_the_block_applies_it_in_the_worker_that_saved(control_plane, attached):
    from provisa.api.admin.settings_router import _apply_otel

    _apply_otel(
        {"endpoint": "http://collector.example:4318", "protocol": "http/protobuf"},
        [],
        updated_by="a",
    )
    assert attached == [("http://collector.example:4318", "provisa", "http/protobuf")]


def test_the_transport_belongs_to_the_endpoint_that_won(control_plane, monkeypatch):
    """REQ-549: ``observability.protocol`` in the config file describes the config file's
    endpoint. An endpoint from a higher source does not inherit it."""
    from provisa.api import otel_setup

    settings_registry.bind_config(
        {"observability": {"endpoint": "http://otlp2parquet:4318", "protocol": "http/protobuf"}}
    )
    assert otel_setup.exporter_settings() == (
        "http://otlp2parquet:4318",
        "provisa",
        "http/protobuf",
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.example:4317")
    assert otel_setup.exporter_settings()[2] == "grpc"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert otel_setup.exporter_settings()[2] == "http/protobuf"
    settings_registry.store(
        control_plane.db, {"otel.endpoint": "http://stored.example:4317"}, updated_by="a"
    )
    assert otel_setup.exporter_settings() == ("http://stored.example:4317", "provisa", "grpc")


# --- the other readers ---------------------------------------------------------------------------


def test_the_compaction_settings_a_boot_runs_on_are_the_stored_ones(control_plane):
    from provisa.api import app_loaders

    settings_registry.store(
        control_plane.db,
        {
            "otel.compact_cron": "*/5 * * * *",
            "otel.compact_batch_size": 42,
            "otel.compact_file_chunk": 7,
            "otel.compact_max_files_per_run": 70,
            "otel.ops_snapshot_retention_hours": 24,
            "otel.s3_endpoint": "http://localhost:9000",
        },
        updated_by="a",
    )
    state = types.SimpleNamespace()
    app_loaders.apply_telemetry_settings(state)
    assert vars(state) == {
        "otel_compact_cron": "*/5 * * * *",
        "otel_compact_batch_size": 42,
        "otel_compact_file_chunk": 7,
        "otel_compact_max_files_per_run": 70,
        "otel_snapshot_retention_hours": 24,
        "otel_s3_endpoint": "http://localhost:9000",
    }


def test_the_telemetry_object_store_follows_the_stored_endpoint(control_plane, monkeypatch):
    from provisa.core.trino_system_catalogs import otel_object_store

    assert otel_object_store()["endpoint"] == "http://minio:9000"
    monkeypatch.setenv("PROVISA_OTEL_S3_ENDPOINT", "http://env.example:9000")
    assert otel_object_store()["endpoint"] == "http://env.example:9000"
    settings_registry.store(
        control_plane.db, {"otel.s3_endpoint": "http://stored.example:9000"}, updated_by="a"
    )
    assert otel_object_store()["endpoint"] == "http://stored.example:9000"


def test_the_defaults_are_the_config_models(control_plane):
    from provisa.core.models import OtelConfig, SubsystemTracesConfig

    for field in (
        "service_name", "sample_rate", "log_level", "compact_cron", "compact_batch_size",
        "compact_file_chunk", "compact_max_files_per_run", "span_export_delay_millis",
        "otlp2parquet_max_age_secs", "collector_batch_timeout_ms", "s3_endpoint", "protocol",
    ):  # fmt: skip
        assert settings_registry.resolve(f"otel.{field}") == (
            OtelConfig.model_fields[field].default,
            "default",
        ), field
    for name, field in SubsystemTracesConfig.model_fields.items():
        assert settings_registry.resolve(f"otel.subsystem_traces.{name}").value is field.default
