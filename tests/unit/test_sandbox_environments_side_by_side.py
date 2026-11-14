# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0d7a31-9c42-4b8e-a6f1-2d83c7b9e410
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's environments are made side by side at its open, and end as they ended one after
another (REQ-2006).

The open made its environments (a demo's dev and test) one after another: each created from prod,
then the test one's rows, then each runtime. A second open of a demo took 13.4 s on CI against an
11 s bound, 5.8 s of it the two creations and 2.3 s the two runtimes. Each environment is now its
own line (created, given its rows, its runtime built), the lines side by side, each on a thread
of its own: the control plane blocks the thread that calls it, and this test's does too.

What the two lines share is the org's: its registry of environments, its model repository, its
redaction load and its domain policy. Each test here runs the real open path
(demo_sandbox._environments, demo_templates.create_environments, env_create.create_environment,
app.ensure_org_runtime, OrgRegistry, redaction_load.ensure_loaded, a real org repository through
env_repo) over an in-memory registry and schemas whose every step yields, and compares what it
leaves with what the old one-after-another order leaves from the same start: the same registry
rows and recorded positions, the same branches, the same runtimes, the same domain policy read by
each build. A failure surfaces as it did one after another, every line has ended when it does,
and an environment is whole or absent: the open removes the sandbox whole (demo_sandbox.reap)."""

# Requirements: REQ-2006, REQ-1488, REQ-1543, REQ-1266

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api import app, demo_sandbox, demo_synthetic, demo_templates, redaction_load
from provisa.api.demo_templates import PublishedDemo
from provisa.api.org_runtime import OrgRegistry, runtime_key
from provisa.core import domain_policy, env_create, env_repo
from provisa.core.env_classes import INHERIT, REFUSED, TEST_SYNTHETIC
from provisa.core.request_context import reset_current_org, set_current_org
from tests.helpers import ready_org_folder

ORG = "sbx0a1b2c3d4e5f"
PERSON = "sandbox-visitor"
WANTED = [
    {"name": "dev", "data_mode": INHERIT, "mutation_handling": REFUSED},
    {"name": "test", "data_mode": TEST_SYNTHETIC, "mutation_handling": REFUSED},
]
PROD_FILES = {"model/tables/orders.yaml": "name: orders\n"}
#: prod's tip, as each open's own repository names it (its commit's time differs between two).
PROD_TIP = "<prod's tip>"
#: The deployment's naming, which a runtime build sets first, and the org's own, which it sets
#: after (app._build_org_runtime): the org's is what its prod build left.
SEED = (None, "default")
OWN = (True, "shop")
#: Each step's yield, by environment: dev's schema is slower than test's (CI: 1.7 s and 0.7 s).
_SCHEMA_S = {"dev": 0.03, "test": 0.01}


class Boom(Exception):
    pass


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _Db:
    """The admin and tenant planes: the one statement issued through them here is
    create_environments' read of the environments the org holds."""

    def __init__(self, plane: "_Plane") -> None:
        self._plane = plane

    def acquire(self) -> "_Db":
        return self

    async def __aenter__(self) -> "_Db":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def execute_core(self, statement: Any) -> _Result:
        return _Result([(name,) for name in self._plane.rows])


class _Plane:
    """What the open writes, in memory, and when each line was in what."""

    def __init__(self, fail: dict[tuple[str, str], Exception] | None = None) -> None:
        self.fail = fail or {}
        self.rows: dict[str, dict[str, Any]] = {}
        self.schemas: set[str] = set()
        self.given_rows: set[str] = set()
        self.loaded: set[str] = set()
        #: Whether the org's redaction rules were loaded when each environment was reserved.
        self.loaded_at_reservation: dict[str, bool] = {}
        self.events: list[tuple[str, str]] = []
        self.creating = 0
        self.most_creating = 0
        self.building = 0
        self.most_building = 0
        #: The lines run on threads of their own: the counts are taken under this.
        self.counting = threading.Lock()
        self.policy_read: dict[str, tuple[bool | None, str]] = {}
        self.db = _Db(self)
        #: Each branch's tip once the open has ended, prod's named ``PROD_TIP``: the org's repository
        #: is in this open's own data directory.
        self.branches: dict[str, str | None] = {}

    def count(self, what: str, by: int) -> None:
        with self.counting:
            now = getattr(self, what) + by
            setattr(self, what, now)
            most = f"most_{what}"
            setattr(self, most, max(getattr(self, most), now))

    def _maybe_fail(self, step: str, env: str) -> None:
        failure = self.fail.get((step, env))
        if failure is not None:
            raise failure


def _install(monkeypatch, tmp_path: Path, plane: _Plane) -> OrgRegistry:
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder(ORG)
    env_repo.commit_files(env_repo.ensure_repo(ORG), "prod", PROD_FILES, "seed", None)

    async def _template_environments(template_org):
        return WANTED

    async def _reserve_env(state, db, org_id, name, **kwargs):
        plane.events.append(("reserved", name))
        plane.loaded_at_reservation[name] = org_id in plane.loaded
        assert name not in plane.rows
        await asyncio.sleep(0)
        plane.rows[name] = {
            "name": name,
            "parent": kwargs["parent"],
            "data_mode": kwargs["data_mode"],
            "mutation_handling": kwargs["mutation_handling"],
            "created_by": kwargs["created_by"],
            "expires_at": None,
        }

    async def _forget_env(db, org_id, name):
        plane.rows.pop(name, None)

    async def _provision_org(pool, schema_sql, org_id, redis_url, redis_password, env):
        plane.count("creating", +1)
        try:
            # The control plane blocks the thread that calls it (core.database): so does this.
            time.sleep(_SCHEMA_S[env])
            plane._maybe_fail("schema", env)
            plane.schemas.add(env)
        finally:
            plane.count("creating", -1)

    async def _deprovision_org(pool, org_id, env):
        plane.schemas.discard(env)

    async def _copy_model(model_db, org_id, from_env, name, **kwargs):
        plane.count("creating", +1)
        try:
            time.sleep(0.01)
            plane._maybe_fail("copy", name)
        finally:
            plane.count("creating", -1)
        return SimpleNamespace()

    async def _set_position(db, org_id, name, *, deployed_sha, deployed_stamp, redo_sha):
        await asyncio.sleep(0)
        plane.rows[name].update(deployed_sha=deployed_sha, redo_sha=redo_sha)

    async def _set_origin(db, org_id, name, origin_sha):
        await asyncio.sleep(0)
        plane.rows[name]["origin_sha"] = origin_sha

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor, like=None):
        # The environment's model is prod's a moment ago: its projection is prod's tree, which
        # writes no commit (env_repo.commit_files), and its position stays where it was started.
        await asyncio.sleep(0.01)
        assert like == (org_id, "prod")
        return env_repo.commit_files(env_repo.ensure_repo(org_id), env, PROD_FILES, message, actor)

    async def _copy_to_sandbox(template_org, org_id, envs):
        for env in envs:
            assert env in plane.schemas, f"{env}'s rows given before it was created"
            await asyncio.sleep(0.01)
            plane._maybe_fail("rows", env)
            plane.given_rows.add(env)

    async def _refresh_org(org_id, **kwargs):
        await asyncio.sleep(0.02)
        plane.loaded.add(org_id)

    async def _none(*args, **kwargs):
        return None

    async def _flags(org_id):
        return app.OrgLane(False, False, None, None, None, "", None)

    async def _get_env(admin_db, org_id, env):
        return plane.rows.get(env)

    async def _build(org_id, *, env, model_cloned=False, **kwargs):
        # app._build_org_runtime's domain-policy window: the deployment's naming set (app.py,
        # ``domain_policy.configure(seed.naming...)``), the model read under it, then the org's
        # own set (``naming_override``) -- on the org's one policy, which every build shares.
        plane.events.append(("building", env))
        plane.count("building", +1)
        token = set_current_org(org_id)
        try:
            domain_policy.configure(*SEED)
            await asyncio.sleep(0.01)
            plane._maybe_fail("runtime", env)
            plane.policy_read[env] = domain_policy.snapshot()
            domain_policy.configure(*OWN)
            await asyncio.sleep(0)
        finally:
            reset_current_org(token)
            plane.count("building", -1)
        return SimpleNamespace(org_id=org_id, env=env, model_cloned=model_cloned, model_db=plane.db)

    registry = OrgRegistry()
    # The sandbox's prod runtime, built by the open before its environments (demo_sandbox._build).
    registry.set(ORG, cast(Any, SimpleNamespace(org_id=ORG, env="prod", model_db=plane.db)))
    monkeypatch.setattr(app.state, "org_registry", registry)
    monkeypatch.setattr(app.state, "admin_db", plane.db)
    monkeypatch.setattr("provisa.api.admin.orgs_router._admin_pool", lambda: plane.db)
    monkeypatch.setattr("provisa.api.admin.orgs_router._pool", lambda: plane.db)
    monkeypatch.setattr(demo_templates, "template_environments", _template_environments)
    monkeypatch.setattr(env_create, "reserve_env", _reserve_env)
    monkeypatch.setattr(env_create, "forget_env", _forget_env)
    monkeypatch.setattr(env_create, "copy_model", _copy_model)
    monkeypatch.setattr("provisa.core.org_provisioning.provision_org", _provision_org)
    monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _deprovision_org)
    monkeypatch.setattr("provisa.core.env_store.set_position", _set_position)
    monkeypatch.setattr("provisa.core.env_store.set_origin", _set_origin)
    monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
    monkeypatch.setattr(env_repo, "write_through", _write_through)
    monkeypatch.setattr(demo_synthetic, "copy_to_sandbox", _copy_to_sandbox)
    monkeypatch.setattr(redaction_load, "refresh_org", _refresh_org)
    monkeypatch.setattr(
        redaction_load.redaction, "is_loaded", lambda org_id: org_id in plane.loaded
    )
    monkeypatch.setattr(app, "ensure_org_encryption", _none)
    monkeypatch.setattr(app, "_read_org_flags", _flags)
    monkeypatch.setattr(app, "_region_lane_of", _none)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    # The org's policy as its prod build left it: its own naming.
    token = set_current_org(ORG)
    domain_policy.configure(*OWN)
    reset_current_org(token)
    return registry


async def _one_after_another(demo: PublishedDemo, org_id: str, person: str) -> None:
    """The open's environments as they were made before (release/v0.1.0-alpha.509 aeb90a988e):
    each created, then the test one's rows, then each runtime, one after another."""
    from provisa.api.app import ensure_org_runtime
    from provisa.api.demo_templates import create_environments, template_environments

    wanted = await template_environments(demo.template_org)
    for env in wanted:
        await create_environments(org_id, [env], person)
    await demo_synthetic.copy_to_sandbox(
        demo.template_org, org_id, demo_synthetic.synthetic_envs(wanted)
    )
    for env in wanted:
        await ensure_org_runtime(org_id, env["name"], model_cloned=True)


_DEMO = PublishedDemo(name="alpha", title="Alpha", directory=Path("/demos/alpha"))


def _opened(
    monkeypatch, tmp_path: Path, how, fail: dict | None = None
) -> tuple[_Plane, OrgRegistry, BaseException | None]:
    plane = _Plane(fail)
    registry = _install(monkeypatch, tmp_path, plane)

    async def _open() -> BaseException | None:
        try:
            await how(_DEMO, ORG, PERSON)
        except Exception as exc:  # noqa: BLE001 -- the failure is what is compared
            raised = exc
        else:
            raised = None
        settled = len(plane.events)
        # Nothing of the open goes on after it returns or raises: it is about to be removed.
        await asyncio.sleep(0.2)
        assert len(plane.events) == settled, plane.events[settled:]
        assert plane.creating == 0 and plane.building == 0
        return raised

    raised = asyncio.run(_open())
    prod_tip = env_repo.tip(ORG, "prod")
    plane.branches = {
        name: PROD_TIP if tip == prod_tip else tip
        for name in env_repo.branches(ORG)
        if (tip := env_repo.tip(ORG, name)) is not None
    }
    for row in plane.rows.values():
        for column in ("deployed_sha", "origin_sha"):
            if row.get(column) == prod_tip:
                row[column] = PROD_TIP
    return plane, registry, raised


def _left(plane: _Plane, registry: OrgRegistry) -> dict[str, Any]:
    """What an open leaves: the registry rows with their recorded positions, the schemas, the
    rows given, the branches and their tips, the runtimes, the domain policy each build read and
    the org's policy after."""
    token = set_current_org(ORG)
    try:
        policy = domain_policy.snapshot()
    finally:
        reset_current_org(token)
    return {
        "rows": plane.rows,
        "schemas": plane.schemas,
        "given_rows": plane.given_rows,
        "branches": plane.branches,
        "runtimes": {
            env["name"]: (rt.env, getattr(rt, "model_cloned"))
            for env in WANTED
            if (rt := registry.get(runtime_key(ORG, env["name"]))) is not None
        },
        "policy_read": plane.policy_read,
        "policy": policy,
    }


@pytest.fixture(autouse=True)
def _policies():
    yield
    domain_policy.reset_all()


def test_the_environments_are_created_side_by_side(monkeypatch, tmp_path):
    plane, _registry, raised = _opened(monkeypatch, tmp_path, demo_sandbox._environments)

    assert raised is None
    # test's creation ran while dev's did: one after another, never more than one at once.
    assert plane.most_creating == 2, plane.events
    # The runtimes are built one at a time: each sets the org's one domain policy.
    assert plane.most_building == 1, plane.events


def test_side_by_side_leaves_what_one_after_another_left(monkeypatch, tmp_path):
    serial, serial_registry, serial_raised = _opened(
        monkeypatch, tmp_path / "serial", _one_after_another
    )
    serial_left = _left(serial, serial_registry)
    domain_policy.reset_all()
    plane, registry, raised = _opened(monkeypatch, tmp_path / "side", demo_sandbox._environments)

    assert serial_raised is None and raised is None
    left = _left(plane, registry)
    assert left == serial_left
    # And what that is: each environment recorded from prod, standing where prod's line stands,
    # its own runtime built cloned, each build reading the deployment's naming in its window.
    for env in WANTED:
        row = left["rows"][env["name"]]
        assert row["parent"] == "prod" and row["data_mode"] == env["data_mode"], row
        assert row["deployed_sha"] == PROD_TIP and row["origin_sha"] == PROD_TIP, row
        assert left["branches"][env["name"]] == PROD_TIP
        assert left["runtimes"][env["name"]] == (env["name"], True)
        assert left["policy_read"][env["name"]] == SEED
    assert left["schemas"] == {"dev", "test"} and left["given_rows"] == {"test"}
    assert left["policy"] == OWN


def test_the_redaction_load_is_done_before_any_environment_is_created(monkeypatch, tmp_path):
    serial, _serial_registry, _serial_raised = _opened(
        monkeypatch, tmp_path / "serial", _one_after_another
    )
    domain_policy.reset_all()
    plane, _registry, raised = _opened(monkeypatch, tmp_path / "side", demo_sandbox._environments)

    assert raised is None
    # One after another, the first creation's prod lookup loaded them (redaction_load
    # ensure_loaded) before either environment was reserved; side by side, a second lookup
    # finding the load under way goes on without it, so the load comes first.
    assert serial.loaded_at_reservation == {"dev": True, "test": True}
    assert plane.loaded_at_reservation == serial.loaded_at_reservation


def _whole_or_absent(plane: _Plane, name: str) -> bool:
    parts = (name in plane.rows, name in plane.schemas, name in plane.branches)
    return all(parts) or not any(parts)


@pytest.mark.parametrize(
    "fail",
    [
        # dev's creation fails part way: its copy, after its schema.
        {("copy", "dev"): Boom("dev's model could not be copied")},
        # test's creation fails part way.
        {("copy", "test"): Boom("test's model could not be copied")},
        # test's rows cannot be given.
        {("rows", "test"): Boom("test's rows could not be given")},
        # dev's runtime fails.
        {("runtime", "dev"): Boom("dev's runtime could not be built")},
        # dev's runtime fails and test's creation fails: one after another, test's creation came
        # first, and its failure was the one raised.
        {
            ("runtime", "dev"): Boom("dev's runtime could not be built"),
            ("schema", "test"): Boom("test's schema could not be laid out"),
        },
    ],
    ids=["dev-creation", "test-creation", "test-rows", "dev-runtime", "both"],
)
def test_a_failure_surfaces_as_it_did_one_after_another(monkeypatch, tmp_path, fail):
    serial, _serial_registry, serial_raised = _opened(
        monkeypatch, tmp_path / "serial", _one_after_another, fail
    )
    domain_policy.reset_all()
    plane, registry, raised = _opened(
        monkeypatch, tmp_path / "side", demo_sandbox._environments, fail
    )

    # The same failure is raised, and only once every line has ended (_opened's own check).
    assert serial_raised is not None
    assert type(raised) is type(serial_raised) and str(raised) == str(serial_raised)
    # Nothing is left half-made: every environment is whole (its row, schema and branch) or
    # absent, as the creation's own rollback leaves a failed one. The open then removes the
    # sandbox whole (demo_sandbox._open: reap).
    for env in WANTED:
        assert _whole_or_absent(plane, env["name"]), (env["name"], plane.rows, plane.schemas)
        assert _whole_or_absent(serial, env["name"])
    # A failed runtime is not registered.
    for (step, env), _exc in fail.items():
        if step == "runtime":
            assert registry.get(runtime_key(ORG, env)) is None
        if step in {"schema", "copy"}:
            assert env not in plane.rows and env not in plane.schemas
