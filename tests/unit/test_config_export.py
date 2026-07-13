# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Live-config generation (REQ-164): the CURRENT config overlays live DB state (admin-created
views/MVs, relationships, roles, rls, domains) onto the on-disk base so the export reflects reality.
"""

from __future__ import annotations

import contextlib
from unittest.mock import AsyncMock, patch

import pytest
import yaml

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self._conn


async def _run(*, base, tables, rels=None, roles=None, rls=None, domains=None):
    from provisa.api.admin import config_export

    conn = object()
    with (
        patch.object(config_export, "read_config", lambda: base),
        patch(
            "provisa.api.admin.schema_helpers._get_pool",
            AsyncMock(return_value=_FakePool(conn)),
        ),
        patch("provisa.core.repositories.table.list_all", AsyncMock(return_value=tables)),
        patch(
            "provisa.core.repositories.relationship.list_all", AsyncMock(return_value=rels or [])
        ),
        patch("provisa.core.repositories.role.list_all", AsyncMock(return_value=roles or [])),
        patch("provisa.core.repositories.rls.list_all", AsyncMock(return_value=rls or [])),
        patch("provisa.core.repositories.domain.list_all", AsyncMock(return_value=domains or [])),
    ):
        return await config_export.build_live_config()


async def test_created_view_appears_in_tables():
    # The concrete complaint: a materialized view created in the UI is absent from the on-disk file.
    base = {"server": {"port": 8000}, "sources": [{"id": "pg", "password": "secret"}], "tables": []}
    view_row = {
        "id": 42,
        "source_id": "__provisa__",
        "schema_name": "org_x_mv_cache",
        "table_name": "test",
        "view_sql": "SELECT * FROM pet_store.users",
        "materialize": True,
        "domain_id": "pet-store",
        "columns": [{"name": "id"}],
        "created_at": "2026-07-13",  # internal — must be dropped
    }
    cfg = await _run(base=base, tables=[view_row])

    assert len(cfg["tables"]) == 1
    t = cfg["tables"][0]
    assert t["table"] == "test" and t["schema"] == "org_x_mv_cache"  # renamed
    assert t["view_sql"].startswith("SELECT") and t["materialize"] is True
    assert "id" not in t and "created_at" not in t  # internal bookkeeping stripped


async def test_file_only_sections_and_credentials_preserved():
    base = {
        "server": {"port": 8000},
        "sources": [{"id": "pg", "password": "secret"}],
        "auth": {"provider": "oidc"},
    }
    cfg = await _run(base=base, tables=[])
    # Sources (with credentials) and other file-only sections are untouched.
    assert cfg["sources"] == [{"id": "pg", "password": "secret"}]
    assert cfg["auth"] == {"provider": "oidc"}
    assert cfg["server"] == {"port": 8000}


async def test_quoted_name_flattened_to_plain_strings():
    # DB rows carry SQLAlchemy quoted_name (str subclass) in keys AND values; yaml.dump would emit
    # !!python/object/apply:sqlalchemy.sql.elements.quoted_name tags. They must be plain strings.
    from sqlalchemy.sql.elements import quoted_name

    domain_row = {quoted_name("id", None): quoted_name("pet-store", None), "description": None}
    cfg = await _run(base={"tables": []}, tables=[], domains=[domain_row])

    text = yaml.dump(cfg, default_flow_style=False, sort_keys=False)
    assert "!!python/object" not in text
    assert "quoted_name" not in text
    assert cfg["domains"][0] == {"id": "pet-store"}  # None dropped, quoted_name → str


async def test_relationships_project_to_schema_and_resolve_table_ids():
    # The DB stores relationships with integer *_table_id and control-plane-only columns; the config
    # uses table NAMES and a narrow field set. Both must be reconciled or every relationship shows as
    # changed (and the config would not round-trip).
    tables = [
        {
            "id": 35,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "pets",
            "domain_id": "d",
        },
        {
            "id": 37,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "domain_id": "d",
        },
    ]
    rels = [
        {
            "id": "pets-to-orders",
            "cardinality": "one-to-many",
            "source_table_id": 35,  # int in DB → must resolve to "pets"
            "target_table_id": 37,  # → "orders"
            "source_column": "id",
            "target_column": "pet_id",
            # DB-only columns that must NOT leak into the config:
            "disable_cypher": False,
            "materialize": False,
            "version": 1,
            "refresh_interval": 300,
        }
    ]
    cfg = await _run(base={"tables": []}, tables=tables, rels=rels)
    r = cfg["relationships"][0]
    assert r["source_table_id"] == "pets" and r["target_table_id"] == "orders"
    for db_only in ("disable_cypher", "materialize", "version", "refresh_interval"):
        assert db_only not in r


async def test_table_id_resolves_to_alias_when_set():
    # The config references a table by its VIRTUAL name: alias when set, else table_name (matches the
    # loader's find_by_table_name). Resolving to table_name instead made every aliased relationship
    # diff (e.g. file 'pet_by_status' vs current 'find_pets_by_status').
    tables = [
        {
            "id": 1,
            "source_id": "api",
            "schema_name": "public",
            "table_name": "find_pets_by_status",
            "alias": "pet_by_status",
            "domain_id": "pet-store",
        }
    ]
    rels = [{"id": "r", "source_table_id": 1, "target_table_id": 1, "cardinality": "many-to-one"}]
    cfg = await _run(base={"tables": []}, tables=tables, rels=rels)
    assert cfg["relationships"][0]["source_table_id"] == "pet_by_status"  # alias, not table_name


async def test_internal_tables_and_their_relationships_excluded():
    # meta/ops (and provisa-internal-schema) tables are internal-only; neither they nor relationships
    # referencing them may appear in the external config.
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "pets",
            "domain_id": "pet-store",
        },
        {
            "id": 9,
            "source_id": "sys",
            "schema_name": "org_x",
            "table_name": "meta_v",
            "domain_id": "meta",
        },
    ]
    rels = [
        {"id": "ok", "source_table_id": 1, "target_table_id": 1, "cardinality": "many-to-one"},
        {
            "id": "internal",
            "source_table_id": 9,
            "target_table_id": 1,
            "cardinality": "many-to-one",
        },
    ]
    cfg = await _run(base={"tables": []}, tables=tables, rels=rels)
    assert [t["table"] for t in cfg["tables"]] == ["pets"]
    assert [r["id"] for r in cfg["relationships"]] == ["ok"]


async def test_internal_meta_ops_excluded():
    # meta/ops domains + their tables are seeded internally and never in the file — excluding them
    # keeps the current's scope matching the file so they don't show as spurious additions.
    domains = [
        {"id": "pet-store", "description": "Pet store"},
        {"id": "meta", "description": "System metadata"},
        {"id": "ops", "description": "Operational telemetry"},
    ]
    tables = [
        {
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "pets",
            "domain_id": "pet-store",
        },
        {"source_id": "sys", "schema_name": "org_x_meta", "table_name": "m", "domain_id": "meta"},
    ]
    cfg = await _run(base={"tables": []}, tables=tables, domains=domains)
    assert [d["id"] for d in cfg["domains"]] == ["pet-store"]
    assert [t["table"] for t in cfg["tables"]] == ["pets"]


async def test_normalize_config_is_order_independent():
    # The same content in different section/key/entity order normalizes to an identical structure —
    # this is what makes the diff show only real changes, not reordering.
    from provisa.api.admin.config_export import normalize_config

    a = {"domains": [{"id": "b"}, {"id": "a"}], "server": {"port": 8000, "host": "x"}}
    b = {"server": {"host": "x", "port": 8000}, "domains": [{"id": "a"}, {"id": "b"}]}
    assert normalize_config(a) == normalize_config(b)


async def test_config_diff_uses_boot_snapshot_as_baseline():
    # The diff baseline is the boot snapshot (state at startup, incl runtime-derived entities), NOT
    # the file — so runtime-derived relationships don't show; only changes since startup do.
    from provisa.api.admin import config_export

    class _State:
        config_boot_snapshot = "domains: []\nrelationships:\n- id: derived\n"

    with (
        patch.object(config_export, "read_config", lambda: {"should": "not be used"}),
        patch.object(
            config_export,
            "build_live_config",
            AsyncMock(return_value={"domains": [], "relationships": [{"id": "derived"}]}),
        ),
        patch("provisa.api.app.state", _State()),
    ):
        diff = await config_export.config_diff()
    # original is the snapshot verbatim, not the file.
    assert diff["original"] == "domains: []\nrelationships:\n- id: derived\n"
    assert "should" not in diff["original"]


async def test_config_diff_falls_back_to_file_without_snapshot():
    from provisa.api.admin import config_export

    class _State:
        config_boot_snapshot = None

    with (
        patch.object(config_export, "read_config", lambda: {"domains": []}),
        patch.object(config_export, "build_live_config", AsyncMock(return_value={"domains": []})),
        patch("provisa.api.app.state", _State()),
    ):
        diff = await config_export.config_diff()
    assert "domains" in diff["original"]  # from the file


async def test_make_config_patch_is_git_apply_compatible():
    from provisa.api.admin import config_export

    class _State:
        config_boot_snapshot = "a: 1\nb: 2\n"

    with patch("provisa.api.app.state", _State()):
        patch_text = config_export.make_config_patch("a: 1\nb: 3\nc: 4\n")
    # git-style headers + a hunk; applying it to the baseline yields the revised.
    assert patch_text.startswith("--- a/")
    assert "+++ b/" in patch_text
    assert "-b: 2" in patch_text and "+b: 3" in patch_text and "+c: 4" in patch_text
    assert patch_text.endswith("\n")


async def test_make_config_patch_empty_when_unchanged():
    from provisa.api.admin import config_export

    class _State:
        config_boot_snapshot = "a: 1\n"

    with patch("provisa.api.app.state", _State()):
        assert config_export.make_config_patch("a: 1\n") == ""


async def test_config_diff_returns_both_sides_normalized():
    from provisa.api.admin import config_export

    original = {"domains": [{"id": "b"}, {"id": "a"}]}
    with (
        patch.object(config_export, "read_config", lambda: original),
        patch.object(
            config_export,
            "build_live_config",
            AsyncMock(return_value={"domains": [{"id": "a"}, {"id": "b"}]}),
        ),
    ):
        diff = await config_export.config_diff()
    # Same content, different order → identical normalized YAML on both sides (no phantom diff).
    assert diff["original"] == diff["current"]


async def test_yaml_serialization_round_trips():
    from provisa.api.admin import config_export

    with patch.object(
        config_export, "build_live_config", AsyncMock(return_value={"tables": [{"table": "t"}]})
    ):
        text = await config_export.build_live_config_yaml()
    assert yaml.safe_load(text) == {"tables": [{"table": "t"}]}
