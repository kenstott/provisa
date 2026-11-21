# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4d2f61-3e97-4a1b-b6d5-7f02a9c1e345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The connectors block promotes with the model (REQ-2221): a connector projects to a file of the
environment's tree and reads back from it, so the model's branches decide where a version runs."""

# Requirements: REQ-2221

from __future__ import annotations

import pytest

from provisa.core import env_deploy
from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
from provisa.core.env_project import KIND_DIRS, project
from provisa.core.models import ConnectorConfig, ConnectorRequirement
from provisa.core.repositories import connector as connector_repo

pytestmark = pytest.mark.unit

SCHEMA = "org_default"


def _connector(**overrides) -> ConnectorConfig:
    return ConnectorConfig(
        name="crm",
        git="https://git.example/crm.git",
        ref="main",
        commit="a" * 40,
        path="connectors/crm",
        build_command="make dist",
        language="python",
        image="registry.example/data/crm",
        digest="sha256:" + "b" * 64,
        code_hash="c" * 64,
        arch="amd64",
        requires=[ConnectorRequirement(name="db_uri", secret=True)],
        **overrides,
    )


def test_the_connectors_directory_is_a_kind_of_the_project():
    assert KIND_DIRS["connectors"] == "connectors"
    assert env_deploy.table_of("connectors/crm.yaml") == "connectors"
    assert "connectors" in env_deploy.PROJECTED


async def test_a_connector_projects_to_its_file_and_reads_back(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await connector_repo.upsert(conn, _connector())
        tree = await project(conn, SCHEMA)
        body = tree["connectors/crm.yaml"]
        assert body["digest"] == "sha256:" + "b" * 64
        assert body["commit"] == "a" * 40
        assert body["requires"] == [{"name": "db_uri", "secret": True}]
        rows = _decompose(
            tree, await _existing_ids(conn, SCHEMA), await _roles_after(conn, SCHEMA, tree)
        )
    (row,) = rows["connectors"]
    assert row["name"] == "crm"
    # The block reads back whole, its build settings with it: they are the CLI's, not the server's.
    assert row["digest"] == "sha256:" + "b" * 64 and row["code_hash"] == "c" * 64
    assert row["path"] == "connectors/crm" and row["build_command"] == "make dist"


async def test_a_new_version_replaces_the_row(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await connector_repo.upsert(conn, _connector())
        updated = _connector().model_copy(update={"digest": "sha256:" + "d" * 64})
        await connector_repo.upsert(conn, updated)
        whole = await connector_repo.list_all(conn)
    assert [r["digest"] for r in whole] == ["sha256:" + "d" * 64]


async def test_the_standard_files_values_are_kept_and_offered(control_plane):
    from provisa.core.models import ConnectorSetting

    db = control_plane.org()
    connector = _connector().model_copy(
        update={
            "description": "Our CRM",
            "icon": "data:image/png;base64,AAAA",
            "settings": [ConnectorSetting(name="replicate", default_value=0)],
            "requires": [
                ConnectorRequirement(name="db_uri", secret=True),
                ConnectorRequirement(name="script", multiline=True, fixed_value="print(1)"),
            ],
        }
    )
    async with db.acquire() as conn:
        await connector_repo.upsert(conn, connector)
        (row,) = await connector_repo.offered(conn)
    assert row["description"] == "Our CRM" and row["icon"] == "data:image/png;base64,AAAA"
    assert row["settings"] == [{"name": "replicate", "default_value": 0}]
    assert (
        row["requires"][1]["multiline"] is True and row["requires"][1]["fixed_value"] == "print(1)"
    )
    assert "path" not in row and "build_command" not in row


def test_the_new_columns_reach_a_database_made_before_them():
    from pathlib import Path

    import provisa.core

    schema = (Path(provisa.core.__file__).parent / "schema.sql").read_text()
    for column in ("description", "icon", "settings"):
        assert f"ALTER TABLE connectors ADD COLUMN IF NOT EXISTS {column} " in schema


# -- the platform's own block (REQ-2225) ------------------------------------------------------------


def _platform_block(tmp_path, monkeypatch, *entries: ConnectorConfig):
    """The packaged block file, as a release build writes it, holding ``entries``."""
    import yaml

    block = tmp_path / "platform_connectors.yaml"
    block.write_text(
        yaml.safe_dump({"connectors": [c.model_dump(exclude_defaults=True) for c in entries]})
    )
    monkeypatch.setattr(connector_repo, "_PLATFORM_FILE", block)


def _spark(**overrides) -> ConnectorConfig:
    return _connector().model_copy(
        update={
            "name": "spark",
            "description": "Spark",
            "requires": [ConnectorRequirement(name="script", multiline=True, script=True)],
            **overrides,
        }
    )


def test_the_package_ships_an_empty_block_until_a_release_fills_it():
    import yaml

    shipped = yaml.safe_load(connector_repo._PLATFORM_FILE.read_text())
    assert shipped == {"connectors": []}


async def test_sources_shows_nothing_of_the_platform_until_a_release_fills_it(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        assert await connector_repo.offered(conn) == []
        assert await connector_repo.get(conn, "spark") is None


async def test_a_filled_block_is_offered_beside_the_orgs_own(control_plane, tmp_path, monkeypatch):
    _platform_block(tmp_path, monkeypatch, _spark())
    db = control_plane.org()
    async with db.acquire() as conn:
        await connector_repo.upsert(conn, _connector())
        offered = await connector_repo.offered(conn)
    assert [row["name"] for row in offered] == ["crm", "spark"]
    spark = offered[1]
    assert spark["description"] == "Spark"
    assert spark["requires"] == [{"name": "script", "multiline": True, "script": True}]
    assert "path" not in spark and "build_command" not in spark


async def test_a_platform_connector_is_found_by_name(control_plane, tmp_path, monkeypatch):
    _platform_block(tmp_path, monkeypatch, _spark())
    db = control_plane.org()
    async with db.acquire() as conn:
        found = await connector_repo.get(conn, "spark")
    assert found is not None
    assert found["image"] == "registry.example/data/crm" and found["code_hash"] == "c" * 64
    assert found["path"] == "connectors/crm"  # whole, as the org's own row reads


async def test_an_orgs_own_connector_of_that_name_keeps_it(control_plane, tmp_path, monkeypatch):
    _platform_block(tmp_path, monkeypatch, _spark(digest="sha256:" + "f" * 64))
    db = control_plane.org()
    async with db.acquire() as conn:
        await connector_repo.upsert(conn, _spark(digest="sha256:" + "e" * 64))
        offered = await connector_repo.offered(conn)
        found = await connector_repo.get(conn, "spark")
    assert [row["digest"] for row in offered] == ["sha256:" + "e" * 64]
    assert found is not None and found["digest"] == "sha256:" + "e" * 64


async def test_the_platform_block_is_not_the_orgs_model(control_plane, tmp_path, monkeypatch):
    _platform_block(tmp_path, monkeypatch, _spark())
    db = control_plane.org()
    async with db.acquire() as conn:
        assert await connector_repo.list_all(conn) == []
        assert "connectors/spark.yaml" not in await project(conn, SCHEMA)


async def test_a_block_that_does_not_read_is_refused(control_plane, tmp_path, monkeypatch):
    block = tmp_path / "platform_connectors.yaml"
    block.write_text("connectors:\n- name: spark\n  git: no-commit\n")
    monkeypatch.setattr(connector_repo, "_PLATFORM_FILE", block)
    db = control_plane.org()
    async with db.acquire() as conn:
        with pytest.raises(ValueError, match="platform_connectors.yaml"):
            await connector_repo.offered(conn)
