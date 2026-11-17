# Copyright (c) 2026 Kenneth Stott
# Canary: 588ba5a4-bf46-4b07-97b5-c309ddc90fc5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each schema build is recorded: its cause, its phases in order, the stamps it was loaded at and
its counts, in one log line and in a recorder a test counts (REQ-1914).

The recorder measures and changes nothing. What a build does, in what order, and when a writer
builds are the subjects of other tests; these pin what is recorded of them.
"""

# Requirements: REQ-1914

from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import threading
from pathlib import Path

import pytest

from provisa.api import rebuild_trace
from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import reset_current_org, set_current_org

_PROVISA = Path(__file__).resolve().parents[2] / "provisa"
_HERE = __name__


@pytest.fixture
def state(monkeypatch):
    from provisa.api.app import AppState

    state = AppState()
    state.org_id = "default"
    state.org_registry.set("default", OrgRuntime(org_id="default"))
    monkeypatch.setattr("provisa.api.app.state", state)
    return state


@pytest.fixture
def stored(monkeypatch) -> dict[str, int]:
    """The stamps the control plane holds, and a runtime with a model store to read them from."""
    from provisa.core import config_stamp

    held = {config_stamp.MODEL: 9, config_stamp.BUILD: 4}

    async def _read(db):
        del db
        return dict(held)

    monkeypatch.setattr(config_stamp, "read", _read)
    return held


@pytest.fixture
def runtime(state, stored):
    del stored
    rt = state.org_registry.get("default")
    rt.model_db = object()
    return rt


# --- the cause -------------------------------------------------------------------------------------


@pytest.mark.usefixtures("state")
async def test_a_writer_that_names_no_cause_is_recorded_by_its_own_name(model_builds) -> None:
    from provisa.api.app import _rebuild_schemas

    async def save_something() -> None:
        await _rebuild_schemas()

    await save_something()
    assert model_builds.causes == [
        f"{_HERE}:test_a_writer_that_names_no_cause_is_recorded_by_its_own_name.<locals>."
        "save_something"
    ]


@pytest.mark.usefixtures("state")
async def test_a_writer_calling_through_the_admin_helpers_is_the_cause_not_the_helper(
    model_builds,
) -> None:
    """Most mutations import the build from ``admin.schema_helpers``, which only passes it on."""
    from provisa.api.admin.schema_helpers import _rebuild_schemas

    class Mutation:
        async def update_table(self) -> None:
            await _rebuild_schemas()

    await Mutation().update_table()
    assert model_builds.causes == [
        f"{_HERE}:test_a_writer_calling_through_the_admin_helpers_is_the_cause_not_the_helper"
        ".<locals>.Mutation.update_table"
    ]


@pytest.mark.usefixtures("state")
async def test_the_cause_is_the_caller_however_the_build_is_then_run(model_builds) -> None:
    """Read when the call is made: a build wrapped in a task, or run on a thread of its own with
    its own loop, is still the caller's."""
    from provisa.api.app import _rebuild_schemas

    async def in_a_task() -> None:
        await asyncio.wait_for(_rebuild_schemas(), timeout=5.0)

    def on_a_thread() -> None:
        token = set_current_org("default")
        try:
            asyncio.run(_rebuild_schemas())
        finally:
            reset_current_org(token)

    await in_a_task()
    thread = threading.Thread(target=on_a_thread)
    thread.start()
    thread.join(10.0)
    prefix = f"{_HERE}:test_the_cause_is_the_caller_however_the_build_is_then_run.<locals>."
    assert model_builds.causes == [f"{prefix}in_a_task", f"{prefix}on_a_thread"]


@pytest.mark.usefixtures("state")
async def test_a_cause_given_is_the_cause_recorded(model_builds) -> None:
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas(cause=rebuild_trace.BOOT)
    assert model_builds.causes == ["boot"]


async def test_the_watchers_reload_is_recorded_as_the_watcher(
    runtime, model_builds, monkeypatch
) -> None:
    import provisa.api.redaction_load as redaction_load
    from provisa.api import model_reload

    async def _nothing(*_a) -> None:
        return None

    monkeypatch.setattr(model_reload, "prune_region_state", _nothing)
    monkeypatch.setattr(model_reload, "reschedule_triggers", _nothing)
    monkeypatch.setattr(redaction_load, "refresh_org", _nothing)

    await model_reload.reload_model(runtime)
    assert model_builds.causes == [rebuild_trace.WATCHER]


def _functions(tree: ast.AST):
    """Every call in ``tree`` with the functions it is nested in, outermost first."""

    def walk(node: ast.AST, inside: tuple[ast.AST, ...]):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Call):
                yield child, inside
            scope = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
            yield from walk(child, (*inside, child) if isinstance(child, scope) else inside)

    yield from walk(tree, ())


def _build_references() -> list[tuple[str, ast.AST, tuple[ast.AST, ...], ast.Module]]:
    """Every use of the build's name in the product, other than importing or defining it."""
    found = []
    for path in sorted(_PROVISA.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if "_rebuild_schemas" not in text:
            continue
        tree = ast.parse(text)
        rel = path.relative_to(_PROVISA.parent).as_posix()
        calls = {id(call.func): inside for call, inside in _functions(tree)}
        # The pass-through in admin.schema_helpers binds the build to ``rebuild``.
        names = {"_rebuild_schemas"} | (
            {"rebuild"} if rel == "provisa/api/admin/schema_helpers.py" else set()
        )
        for node in ast.walk(tree):
            named = (isinstance(node, ast.Name) and node.id in names) or (
                isinstance(node, ast.Attribute) and node.attr in names
            )
            if named:
                found.append((rel, node, calls.get(id(node)), tree))
    return found


def test_no_caller_reports_an_unknown_cause() -> None:
    """A writer's cause is the function calling the build, read from its frame at the call. That
    names every caller exactly when every use of the build in the product is a call made inside
    a named function: none at module level, none inside a lambda, and the build never handed on
    as a value for something else to call."""
    references = _build_references()
    # 89 writers were counted when this was written; the number is not pinned, only that the
    # walk finds them.
    assert len(references) > 50

    unnamed = []
    for rel, node, inside, _tree in references:
        where = f"{rel}:{getattr(node, 'lineno', 0)}"
        if inside is None:
            unnamed.append(f"{where}: the build is passed on, not called")
            continue
        functions = [f for f in inside if not isinstance(f, ast.ClassDef)]
        if not functions:
            unnamed.append(f"{where}: called at module level")
        elif isinstance(functions[-1], ast.Lambda):
            unnamed.append(f"{where}: called inside a lambda")
    assert unnamed == []


def test_the_causes_that_are_not_a_writer_are_named_where_the_build_is_called() -> None:
    """Boot, an org's build and the watcher name themselves; every other caller passes nothing."""
    given: dict[str, list[str]] = {}
    for rel, node, inside, _tree in _build_references():
        if inside is None:
            continue
        call = next(
            c
            for c in ast.walk(ast.Module(body=list(inside[-1:]), type_ignores=[]))
            if isinstance(c, ast.Call) and c.func is node
        )
        for keyword in call.keywords:
            if keyword.arg == "cause":
                given.setdefault(rel, []).append(ast.unparse(keyword.value))
    assert given == {
        "provisa/api/app.py": [
            "rebuild_trace.BOOT",
            "rebuild_trace.ORG_BUILD",
            "rebuild_trace.ORG_BUILD",
        ],
        "provisa/api/model_reload.py": ["rebuild_trace.WATCHER", "rebuild_trace.SETTINGS_WATCHER"],
    }


# --- the phases ------------------------------------------------------------------------------------


def test_the_build_enters_every_phase_once_in_the_listed_order() -> None:
    """The build is one straight run: the phases it marks, read from its source in order, are
    the listed phases, each once, none inside a branch or a loop."""
    from provisa.api import app

    tree = ast.parse(inspect.getsource(app._rebuild_schemas_impl))
    function = tree.body[0]
    assert isinstance(function, ast.AsyncFunctionDef)

    marked: list[tuple[int, str]] = []
    conditional: list[str] = []

    def walk(node: ast.AST, branched: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "phase"
                and isinstance(child.func.value, ast.Name)
                and child.func.value.id == "_build"
            ):
                arg = child.args[0]
                assert isinstance(arg, ast.Attribute)
                marked.append((child.lineno, getattr(rebuild_trace, arg.attr)))
                if branched:
                    conditional.append(arg.attr)
            walk(
                child,
                branched
                or isinstance(
                    child, (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.FunctionDef)
                ),
            )

    walk(function, False)
    assert [name for _line, name in sorted(marked)] == list(rebuild_trace.PHASES)
    assert conditional == []


def test_a_record_holds_its_phases_in_the_order_entered_and_refuses_an_unlisted_one() -> None:
    record = rebuild_trace.BuildRecord(org_id="default", env="prod", cause="a test")
    with rebuild_trace.building(record):
        record.phase(rebuild_trace.TABLES)
        record.phase(rebuild_trace.COMPILE)
        with pytest.raises(ValueError, match="not a phase of the build before its answer"):
            record.phase("something else")
    assert record.phase_names == [rebuild_trace.TABLES, rebuild_trace.COMPILE]
    assert record.outcome == rebuild_trace.BUILT
    assert all(seconds >= 0 for _name, seconds in record.phases)


def test_the_model_build_runs_only_inside_a_recorded_build() -> None:
    with pytest.raises(LookupError, match="no schema build is being recorded"):
        rebuild_trace.this_build()
    assert rebuild_trace.build_if_any() is None


# --- what became of the call -----------------------------------------------------------------------


@pytest.mark.usefixtures("state")
async def test_a_build_that_fails_is_recorded_with_the_phase_it_stopped_in(
    rebuild_recorder, monkeypatch
) -> None:
    import provisa.api.app as app

    async def _fails(raw_config=None, *, announce=True, converge=True):
        del raw_config, announce, converge
        build = rebuild_trace.this_build()
        build.phase(rebuild_trace.TABLES)
        build.phase(rebuild_trace.ROLES)
        raise RuntimeError("a role names a parent that is not a role")

    monkeypatch.setattr(app, "_rebuild_schemas_impl", _fails)
    with pytest.raises(RuntimeError, match="names a parent"):
        await app._rebuild_schemas()

    (record,) = rebuild_recorder.builds
    assert record.outcome == rebuild_trace.FAILED
    assert record.failed_in == rebuild_trace.ROLES
    assert record.phase_names == [rebuild_trace.TABLES, rebuild_trace.ROLES]


async def test_a_reload_that_finds_the_runtime_current_is_a_call_and_not_a_build(
    runtime, model_builds
) -> None:
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    assert len(model_builds.builds) == 1
    await _rebuild_schemas(announce=False, if_stale=True, cause=rebuild_trace.WATCHER)

    assert len(model_builds.builds) == 1
    assert [(r.cause, r.outcome) for r in model_builds.calls][1] == (
        rebuild_trace.WATCHER,
        rebuild_trace.CURRENT,
    )
    assert runtime.build_stamp == 4


async def test_a_build_records_the_stamps_it_found_and_the_stamps_it_read(
    runtime, stored, model_builds
) -> None:
    from provisa.api.app import _rebuild_schemas
    from provisa.core import config_stamp

    await _rebuild_schemas()
    stored[config_stamp.MODEL] = 11
    stored[config_stamp.BUILD] = 5
    await _rebuild_schemas()

    first, second = model_builds.builds
    assert first.loaded_stamps == {config_stamp.MODEL: None, config_stamp.BUILD: None}
    assert first.read_stamps == {config_stamp.MODEL: 9, config_stamp.BUILD: 4}
    assert second.loaded_stamps == {config_stamp.MODEL: 9, config_stamp.BUILD: 4}
    assert second.read_stamps == {config_stamp.MODEL: 11, config_stamp.BUILD: 5}
    assert runtime.build_stamp == 5
    assert first.phase_names == list(rebuild_trace.PHASES)
    assert (first.org_id, first.env) == ("default", "prod")


# --- the log line ----------------------------------------------------------------------------------


def _record(**kw) -> rebuild_trace.BuildRecord:
    record = rebuild_trace.BuildRecord(
        org_id="acme", env="prod", cause="provisa.api.admin.schema_mutation:Mutation.update_table"
    )
    with rebuild_trace.building(record):
        record.phase(rebuild_trace.STAMP_READ)
        record.stamps(loaded={"model": 7, "build": 4}, read={"model": 9, "build": 5})
        record.phase(rebuild_trace.TABLES)
        record.count(tables=306, roles=5, sources=12)
        record.phase(rebuild_trace.COMPILE)
    for name, value in kw.items():
        setattr(record, name, value)
    return record


def test_a_build_is_logged_in_one_line_with_its_cause_counts_stamps_and_phases(caplog) -> None:
    record = _record(waited_s=0.25, took_s=1.5)
    with caplog.at_level(logging.INFO, logger="provisa.api.rebuild_trace"):
        rebuild_trace.finished(record, slow_s=2.0)

    (logged,) = caplog.records
    assert logged.levelno == logging.INFO
    line = logged.getMessage()
    assert "\n" not in line
    assert line.startswith("schema rebuild of acme: waited 0.2s for its turn, took 1.5s; ")
    assert "env=prod cause=provisa.api.admin.schema_mutation:Mutation.update_table" in line
    assert "outcome=built" in line
    assert "tables=306 roles=5 sources=12" in line
    assert "stamps model 7->9 build 4->5" in line
    phases = line.split("phases ")[1].split()
    assert [p.split("=")[0] for p in phases] == ["stamp_read", "tables", "compile"]
    assert all(float(p.split("=")[1]) >= 0 for p in phases)


def test_a_slow_build_is_logged_at_warning(caplog) -> None:
    with caplog.at_level(logging.INFO, logger="provisa.api.rebuild_trace"):
        rebuild_trace.finished(_record(waited_s=1.5, took_s=0.5), slow_s=2.0)
    assert [r.levelno for r in caplog.records] == [logging.WARNING]


def test_a_call_that_built_nothing_is_recorded_and_not_logged(caplog, rebuild_recorder) -> None:
    record = rebuild_trace.BuildRecord(org_id="acme", env="prod", cause=rebuild_trace.WATCHER)
    record.outcome = rebuild_trace.CURRENT
    with caplog.at_level(logging.INFO, logger="provisa.api.rebuild_trace"):
        rebuild_trace.finished(record, slow_s=2.0)
    assert caplog.records == []
    assert rebuild_recorder.calls == [record]
    assert rebuild_recorder.builds == []


# --- the work after a build ------------------------------------------------------------------------


class _Lazy:
    """A role's surface not built yet, counting its builds."""

    def __init__(self) -> None:
        self.built = False

    def resolve(self) -> None:
        self.built = True


@pytest.fixture
def warm_up(monkeypatch):
    """A runtime of two unbuilt roles, for the warm-up of a build's pass to build."""
    from types import SimpleNamespace

    from provisa.api import app_loaders, role_surface

    monkeypatch.setattr(app_loaders, "_WARM_PAUSE_S", 0)
    monkeypatch.setattr(role_surface, "LazySurface", _Lazy)
    runtime = SimpleNamespace(
        org_id="acme",
        role_build_inputs={"tables": []},
        surfaces={"analyst": _Lazy(), "steward": _Lazy()},
    )
    return SimpleNamespace(runtime=runtime)


async def test_the_warm_up_after_a_build_is_recorded_on_that_build(warm_up, caplog) -> None:
    """Every role's surface is built after the build ends: roles x tables of work that is in no
    build's time. The build's record takes the roles it built and what that held the loop for."""
    from provisa.api import app_loaders

    record = rebuild_trace.BuildRecord(org_id="acme", env="prod", cause="a test")
    with rebuild_trace.building(record):
        followed = warm_up.runtime.role_build_inputs
    assert record.warmed is None, "it has not run yet: the build's own time ends before it"

    with caplog.at_level(logging.INFO, logger="provisa.api.rebuild_trace"):
        await app_loaders.warm_role_surfaces(warm_up.runtime, followed, build=record)

    assert record.warmed is not None
    assert (record.warmed.roles, record.warmed.superseded) == (2, False)
    assert record.warmed.busy_s >= 0
    (logged,) = caplog.records
    assert logged.getMessage().startswith(
        "role surface warm-up of acme after the build by a test: 2 role(s) in "
    )


async def test_a_warm_up_a_newer_build_superseded_says_so(warm_up) -> None:
    from provisa.api import app_loaders

    record = rebuild_trace.BuildRecord(org_id="acme", env="prod", cause="a test")
    followed = warm_up.runtime.role_build_inputs
    warm_up.runtime.role_build_inputs = {"tables": []}  # a newer build's
    await app_loaders.warm_role_surfaces(warm_up.runtime, followed, build=record)

    assert record.warmed is not None
    assert (record.warmed.roles, record.warmed.superseded) == (0, True)


def test_a_pass_through_must_be_a_plain_function() -> None:
    """Its caller's frame is read under it when the build is called; a coroutine's is gone."""

    async def later() -> None:
        return None

    with pytest.raises(TypeError, match="coroutine function"):
        rebuild_trace.passes_on(later)


async def test_the_admin_helper_calls_the_build_with_nothing(monkeypatch) -> None:
    """Tests of the mutations stand in for the build with a function taking nothing; the helper
    hands it no cause to refuse."""
    import provisa.api.app as app
    from provisa.api.admin import schema_helpers

    ran: list[str] = []

    async def _rebuild() -> None:
        ran.append("built")

    monkeypatch.setattr(app, "_rebuild_schemas", _rebuild)
    await schema_helpers._rebuild_schemas()
    assert ran == ["built"]
