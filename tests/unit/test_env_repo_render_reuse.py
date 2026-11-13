# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b9d2e-7a41-4c58-b0e3-91d2c6a8f475
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write-through of a model copied a moment ago starts from its source's rendering (REQ-2006).

A demo sandbox's open wrote its model's history three times -- its prod, cloned from its template,
then each environment created from that prod -- and each write rendered every file of the model
again, though each held the very entities just rendered for the one it was copied from. A
write-through told what it was copied from (``like``) re-renders only what differs from that
rendering; what it commits is the same text a full rendering gives."""

# Requirements: REQ-1882, REQ-2006

from __future__ import annotations

import pytest

from tests.helpers import ready_org_folder

from provisa.core import env_files, env_repo

pytestmark = pytest.mark.unit

TREE = {
    f"meta/tables/t{n}.yaml": {"table_name": f"t{n}", "columns": [{"name": "id"}]} for n in range(5)
}


@pytest.fixture(autouse=True)
def plane(tmp_path, monkeypatch):
    """Everything around the projection answered: the model is ``TREE`` in every schema, and the
    files committed per write are collected."""
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    for org in ("tpl", "sbx"):
        ready_org_folder(org)
    monkeypatch.setattr(env_repo, "_rendered", {})

    async def _project(conn, schema):
        return {path: dict(entity) for path, entity in TREE.items()}

    async def _stamp(conn, schema):
        return 1

    async def _quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(env_repo, "project", _project)
    monkeypatch.setattr("provisa.core.env_project.model_stamp", _stamp)
    monkeypatch.setattr("provisa.core.env_store.set_drifted", _quiet)
    monkeypatch.setattr("provisa.core.env_store.set_position", _quiet)
    monkeypatch.setattr("provisa.core.env_store.set_deployed_stamp", _quiet)
    monkeypatch.setattr("provisa.core.env_store.get_env", _quiet)
    monkeypatch.setattr("provisa.core.env_ci.announce", _quiet)


@pytest.fixture
def emitted(monkeypatch) -> list[dict]:
    """Each entity rendered to text."""
    seen: list[dict] = []
    real = env_files._emit

    def _emit(entity):
        seen.append(entity)
        return real(entity)

    monkeypatch.setattr(env_files, "_emit", _emit)
    return seen


async def _write(org: str, env: str, like: tuple[str, str] | None = None) -> str | None:
    return await env_repo.write_through(
        "conn", "admin", org, env, f"org_{org}", "note", None, like=like
    )


async def test_an_environment_copied_from_another_renders_none_of_its_unchanged_files(emitted):
    await _write("sbx", "prod")
    assert len(emitted) == len(TREE)
    emitted.clear()

    await _write("sbx", "dev", like=("sbx", "prod"))

    assert emitted == []


async def test_a_sandbox_starts_from_its_templates_rendering(emitted):
    await _write("tpl", "prod")
    emitted.clear()

    await _write("sbx", "prod", like=("tpl", "prod"))

    assert emitted == []


async def test_what_is_committed_is_the_text_a_full_rendering_gives(emitted):
    await _write("tpl", "prod")
    sha = await _write("sbx", "prod", like=("tpl", "prod"))

    assert sha is not None
    assert env_repo.files_at("sbx", sha) == env_files.dump(TREE)


async def test_an_entity_that_differs_from_the_source_is_rendered_again(emitted, monkeypatch):
    await _write("sbx", "prod")
    emitted.clear()
    changed = {**TREE, "meta/tables/t0.yaml": {"table_name": "t0", "columns": []}}

    async def _project(conn, schema):
        return changed

    monkeypatch.setattr(env_repo, "project", _project)
    sha = await _write("sbx", "dev", like=("sbx", "prod"))

    assert emitted == [changed["meta/tables/t0.yaml"]]
    assert sha is not None
    assert env_repo.files_at("sbx", sha) == env_files.dump(changed)


async def test_without_a_source_every_file_is_rendered(emitted):
    await _write("sbx", "prod")
    emitted.clear()

    await _write("sbx", "dev")

    assert len(emitted) == len(TREE)


async def test_a_sandboxs_record_renders_from_its_templates_prod(monkeypatch):
    from pathlib import Path

    from provisa.api import demo_sandbox
    from provisa.api.demo_templates import PublishedDemo

    demo = PublishedDemo(name="alpha", title="Alpha", directory=Path("/demos/alpha/v1"))
    calls: list[tuple] = []

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor, *, like=None):
        calls.append((org_id, env, like))

    class _Db:
        def acquire(self):
            class _Conn:
                async def __aenter__(self):
                    return "conn"

                async def __aexit__(self, *exc):
                    return False

            return _Conn()

    monkeypatch.setattr(env_repo, "write_through", _write_through)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: "admin")

    await demo_sandbox._record(_Db(), demo, "sbx", "visitor")

    assert calls == [("sbx", "prod", (demo.template_org, "prod"))]


#: A built-in table names the schema it lives in, which a copy rebases onto its own (env_copy).
_IN_SCHEMA = {
    f"meta/tables/m{n}.yaml": {"table_name": f"m{n}", "columns": [{"name": "id"}]} for n in range(4)
}


def _in(schema: str) -> dict[str, dict]:
    return {path: {**entity, "schema_name": schema} for path, entity in _IN_SCHEMA.items()}


def test_an_entity_rebased_onto_the_copys_schema_reuses_its_sources_text(emitted):
    _, held = env_files.dump_reusing(_in("org_sbx"), {})
    emitted.clear()

    files, _ = env_files.dump_reusing(
        _in("org_sbx_env_dev"), held, rebased=("org_sbx", "org_sbx_env_dev")
    )

    assert emitted == []
    assert files == env_files.dump(_in("org_sbx_env_dev"))


def test_an_entity_that_differs_beyond_its_schema_is_rendered_again(emitted):
    _, held = env_files.dump_reusing(_in("org_sbx"), {})
    emitted.clear()
    changed = _in("org_sbx_env_dev")
    changed["meta/tables/m0.yaml"] = {**changed["meta/tables/m0.yaml"], "columns": []}

    files, _ = env_files.dump_reusing(changed, held, rebased=("org_sbx", "org_sbx_env_dev"))

    assert emitted == [changed["meta/tables/m0.yaml"]]
    assert files == env_files.dump(changed)


def test_a_schema_name_not_rebased_is_rendered_again(emitted):
    """An entity naming another schema than the copy's own was not rebased by the copy."""
    _, held = env_files.dump_reusing(_in("org_sbx"), {})
    emitted.clear()

    files, _ = env_files.dump_reusing(
        _in("org_elsewhere"), held, rebased=("org_sbx", "org_sbx_env_dev")
    )

    assert len(emitted) == len(_IN_SCHEMA)
    assert files == env_files.dump(_in("org_elsewhere"))


async def test_an_environment_holding_built_in_tables_renders_none_of_them(emitted, monkeypatch):
    """The sandbox's environments as they are: their built-in tables name their own schema."""

    async def _project(conn, schema):
        return _in(schema)

    monkeypatch.setattr(env_repo, "project", _project)
    await env_repo.write_through("conn", "admin", "sbx", "prod", "org_sbx", "note", None)
    emitted.clear()

    sha = await env_repo.write_through(
        "conn", "admin", "sbx", "dev", "org_sbx_env_dev", "note", None, like=("sbx", "prod")
    )

    assert emitted == []
    assert sha is not None
    assert env_repo.files_at("sbx", sha) == env_files.dump(_in("org_sbx_env_dev"))


async def test_a_sandbox_renders_none_of_its_templates_built_in_tables(emitted, monkeypatch):
    async def _project(conn, schema):
        return _in(schema)

    monkeypatch.setattr(env_repo, "project", _project)
    await env_repo.write_through("conn", "admin", "tpl", "prod", "org_tpl", "note", None)
    emitted.clear()

    await env_repo.write_through(
        "conn", "admin", "sbx", "prod", "org_sbx", "note", None, like=("tpl", "prod")
    )

    assert emitted == []
