# Copyright (c) 2026 Kenneth Stott
# Canary: 5b9d2a81-7e34-4c06-a1f9-3d8c0e6b4f27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A setting changed through the admin API is the deployment's, not one worker's (REQ-1900).

`PUT /admin/settings` used to write ``os.environ`` in the worker that served the request: the
other workers and instances never saw the change, and a restart lost it. Deployment settings are
now rows in the platform control plane, and every reader resolves them from a snapshot of those
rows held in its process. REQ-1914: the snapshot is loaded at the plane's ``settings`` config
stamp, which the control plane advances with every write, and the process's config watcher
reloads the snapshot when the stored stamp differs — no reader queries the control plane."""

# Requirements: REQ-1900, REQ-165, REQ-1914

from __future__ import annotations

import asyncio

import pytest

from provisa.core import config_stamp, config_watch, deployment_settings, limits, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import config_stamp as stamp_table
from provisa.core.schema_admin import deployment_settings as settings_table
from provisa.core.schema_admin import metadata


def _watcher_checks(db: Database) -> list[str]:
    """One check of this process's config watcher against the platform plane."""
    target = config_watch.Target(
        name="operator settings",
        db=db,
        kind=config_stamp.SETTINGS,
        loaded=deployment_settings.loaded_stamp,
        reload=settings_registry.reload_and_apply,
    )
    return asyncio.run(config_watch.check([target]))


@pytest.fixture
def control_plane(tmp_path, monkeypatch):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[settings_table, stamp_table])
        config_stamp.install(conn, config_stamp.PLATFORM_TABLES)
    db = Database(engine, name="platform")
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    yield db
    engine.dispose()


def test_an_unbound_process_has_no_stored_settings(monkeypatch):
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    assert deployment_settings.get("limits.default_row_limit") is None


def test_a_written_setting_is_read_back_and_survives_a_new_process(control_plane, monkeypatch):
    deployment_settings.bind(control_plane)
    deployment_settings.write(control_plane, {"limits.default_row_limit": 7}, updated_by="alice")
    assert deployment_settings.get("limits.default_row_limit") == 7
    # A process that starts later (a restart, another worker) binds and reads the same value.
    monkeypatch.setattr(deployment_settings, "_held", None)
    deployment_settings.bind(control_plane)
    assert deployment_settings.get("limits.default_row_limit") == 7


def test_another_workers_write_is_seen_when_the_watcher_finds_the_stamp_changed(control_plane):
    deployment_settings.bind(control_plane)
    assert deployment_settings.get("sampling.default_sample_size") is None
    loaded_at = deployment_settings.loaded_stamp()
    assert _watcher_checks(control_plane) == []  # nothing stored has changed: nothing reloads

    # Another worker writes the control plane directly: this process's snapshot does not know.
    with control_plane.engine.begin() as conn:
        conn.execute(settings_table.insert().values(key="sampling.default_sample_size", value="50"))
    # A reader never goes to the control plane, however long it has been.
    assert deployment_settings.get("sampling.default_sample_size") is None

    assert _watcher_checks(control_plane) == ["operator settings"]
    assert deployment_settings.get("sampling.default_sample_size") == 50
    stamp = deployment_settings.loaded_stamp()
    assert stamp is not None and loaded_at is not None and stamp > loaded_at
    assert _watcher_checks(control_plane) == []  # loaded at the stored stamp: nothing to do


def test_a_reader_queries_the_control_plane_once_however_often_it_reads(control_plane, monkeypatch):
    deployment_settings.bind(control_plane)
    loads: list[int] = []
    real_load = deployment_settings._load

    def _counted(db):
        loads.append(1)
        return real_load(db)

    monkeypatch.setattr(deployment_settings, "_load", _counted)
    for _ in range(50):
        deployment_settings.get("limits.default_row_limit")
    assert len(loads) == 1


def test_the_writing_worker_sees_its_own_change_at_once(control_plane):
    deployment_settings.bind(control_plane)
    deployment_settings.get("relationships.auto_track_fk")
    deployment_settings.write(control_plane, {"relationships.auto_track_fk": False}, updated_by="a")
    assert deployment_settings.get("relationships.auto_track_fk") is False


def test_the_row_limit_is_the_stored_setting_over_env_over_config(control_plane, monkeypatch):
    monkeypatch.setattr(limits, "_server_limits", {"default_row_limit": 100})
    monkeypatch.delenv("PROVISA_DEFAULT_ROW_LIMIT", raising=False)
    deployment_settings.bind(control_plane)
    assert limits.default_row_limit() == 100
    monkeypatch.setenv("PROVISA_DEFAULT_ROW_LIMIT", "500")
    assert limits.default_row_limit() == 500
    deployment_settings.write(control_plane, {"limits.default_row_limit": 9}, updated_by="a")
    assert limits.default_row_limit() == 9


def test_the_sample_size_and_fk_tracking_read_the_stored_setting(control_plane, monkeypatch):
    from provisa.compiler import sampling

    monkeypatch.delenv("PROVISA_SAMPLE_SIZE", raising=False)
    monkeypatch.delenv("PROVISA_AUTO_TRACK_FK", raising=False)
    deployment_settings.bind(control_plane)
    assert sampling.get_sample_size() == sampling.DEFAULT_SAMPLE_SIZE
    assert deployment_settings.auto_track_fk() is True
    deployment_settings.write(
        control_plane,
        {"sampling.default_sample_size": 25, "relationships.auto_track_fk": False},
        updated_by="a",
    )
    assert sampling.get_sample_size() == 25
    assert deployment_settings.auto_track_fk() is False


def test_the_admin_endpoint_no_longer_writes_the_process_environment():
    import inspect

    from provisa.api.admin import settings_router

    src = inspect.getsource(settings_router._apply_scalars)
    assert "os.environ[" not in src
    assert "deployment_settings.write(" in src
