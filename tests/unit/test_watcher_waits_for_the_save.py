# Copyright (c) 2026 Kenneth Stott
# Canary: 80ece534-d231-47a5-9365-7368003747bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config watcher waits for the save it follows (REQ-1914, amended 2026-11-08).

A save's stamp is visible as soon as its transaction commits, and the save's commit to the
environment's history (REQ-1524) runs after that. The watcher seeing that stamp does not start
its rebuild until the commit has finished; a change from outside the process is picked up at once.
The save runs on a thread and event loop of its own, as a request does beside the watcher."""

# Requirements: REQ-1914

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.api import model_reload
from provisa.api.org_runtime import OrgRegistry, OrgRuntime
from provisa.core import config_stamp, config_watch, model_change
from provisa.core.environments import PROD
from provisa.core.model_change import ModelPlane

pytestmark = pytest.mark.unit

ORG = "acme"
OTHER_ORG = "globex"
PLANE = ModelPlane(ORG, PROD)
OTHER_PLANE = ModelPlane(OTHER_ORG, PROD)
_TURNS = 50  # event-loop turns a check that was not held back would finish in


class _Model:
    """The model plane a save writes: only what the change scope reads of it."""

    search_path = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[object]:
        yield object()


class _Commit:
    """The save's commit: reports that it began and finishes when released."""

    def __init__(self, *, raises: bool = False) -> None:
        self.began = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.raises = raises

    async def __call__(self, *_args: object, **_kwargs: object) -> str:
        self.began.set()
        await asyncio.to_thread(self.release.wait)
        self.finished.set()
        if self.raises:
            raise RuntimeError("the repository refused the commit")
        return "sha"


@pytest.fixture
def commit() -> Iterator[_Commit]:
    held = _Commit()
    model_change.attach(object(), commit=held, projected=frozenset({"roles"}))  # type: ignore[arg-type]
    yield held
    held.release.set()
    model_change.detach()


@pytest.fixture(autouse=True)
def stored_stamp(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _read(_db: object) -> dict[str, int]:
        return {config_stamp.MODEL: 2}

    monkeypatch.setattr(config_stamp, "read", _read)


def _save_on_its_own_thread(plane: ModelPlane) -> tuple[threading.Thread, threading.Event]:
    """A save writing ``plane`` from a loop of its own; the event is set once it has written."""
    written = threading.Event()

    async def _save() -> None:
        async with model_change.scope("save"):
            model_change.record(_Model(), plane, "insert", "roles", None)  # type: ignore[arg-type]
            written.set()

    def _run() -> None:
        try:
            asyncio.run(_save())
        except RuntimeError:
            pass  # the save whose commit is refused: what is under test is that the wait ends

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread, written


def _target(
    name: str,
    plane: ModelPlane,
    reloaded: list[str],
    on_reload: Callable[[], None] = lambda: None,
) -> config_watch.Target:
    async def _reload() -> None:
        on_reload()
        reloaded.append(name)

    return config_watch.Target(
        name=name,
        db=SimpleNamespace(name=name),  # type: ignore[arg-type]
        kind=config_stamp.MODEL,
        loaded=lambda: 1,
        reload=_reload,
        saves=lambda: model_change.saves_in_flight(plane),
    )


async def _turns() -> None:
    for _ in range(_TURNS):
        await asyncio.sleep(0)


async def _began(event: threading.Event) -> None:
    await asyncio.to_thread(event.wait)


async def test_the_watcher_does_not_rebuild_until_the_saves_commit_has_finished(commit):
    reloaded: list[str] = []
    finished_at_reload: list[bool] = []
    target = _target(
        ORG, PLANE, reloaded, on_reload=lambda: finished_at_reload.append(commit.finished.is_set())
    )
    thread, written = _save_on_its_own_thread(PLANE)
    await _began(written)

    check = asyncio.create_task(config_watch.check([target]))
    await _began(commit.began)
    await _turns()
    assert reloaded == [], "the rebuild started while the save was still committing"

    commit.release.set()
    assert await check == [ORG]
    assert finished_at_reload == [True]
    thread.join()


async def test_a_change_from_outside_the_process_rebuilds_at_once(commit):
    # No save of this process writes the plane: the stamp was moved by another process. The
    # commit stays unreleased, so a wait of any kind would hang this check.
    reloaded: list[str] = []

    assert await config_watch.check([_target(ORG, PLANE, reloaded)]) == [ORG]
    assert reloaded == [ORG]
    assert not commit.began.is_set()


async def test_another_runtimes_change_does_not_wait_behind_a_save(commit):
    reloaded: list[str] = []
    saving = _target(ORG, PLANE, reloaded)
    external = _target(OTHER_ORG, OTHER_PLANE, reloaded)
    thread, written = _save_on_its_own_thread(PLANE)
    await _began(written)

    check = asyncio.create_task(config_watch.check([saving, external]))
    await _began(commit.began)
    await _turns()
    assert reloaded == [OTHER_ORG]

    commit.release.set()
    assert await check == [OTHER_ORG, ORG]
    thread.join()


async def test_a_commit_that_fails_still_lets_the_watcher_rebuild(commit):
    commit.raises = True
    reloaded: list[str] = []
    thread, written = _save_on_its_own_thread(PLANE)
    await _began(written)

    check = asyncio.create_task(config_watch.check([_target(ORG, PLANE, reloaded)]))
    await _began(commit.began)
    commit.release.set()

    assert await check == [ORG]
    assert model_change.saves_in_flight(PLANE) == []
    thread.join()


async def test_a_save_is_no_longer_in_flight_once_its_commit_has_flushed(commit):
    commit.release.set()
    async with model_change.scope("save") as change:
        model_change.record(_Model(), PLANE, "insert", "roles", None)  # type: ignore[arg-type]
        assert len(model_change.saves_in_flight(PLANE)) == 1
        await model_change.flush(change)
        assert model_change.saves_in_flight(PLANE) == []
    assert commit.finished.is_set()


async def test_a_scope_that_wrote_nothing_is_not_a_save(commit):
    async with model_change.scope("read"):
        assert model_change.saves_in_flight(PLANE) == []


async def test_a_runtimes_model_target_follows_that_runtimes_saves(commit, monkeypatch):
    commit.release.set()
    rt = OrgRuntime(org_id=ORG)
    rt.model_db = SimpleNamespace(name="model plane")  # type: ignore[assignment]
    rt.tenant_db = SimpleNamespace(name="tenant plane")  # type: ignore[assignment]
    registry = OrgRegistry()
    registry.set(ORG, rt)
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(admin_db=None, org_registry=registry),
        raising=False,
    )
    model_target = next(t for t in model_reload.targets() if t.name == f"org {ORG}: model")

    assert list(model_target.saves()) == []
    async with model_change.scope("save") as change:
        model_change.record(_Model(), PLANE, "insert", "roles", None)  # type: ignore[arg-type]
        assert len(model_target.saves()) == 1
        await model_change.flush(change)
    assert list(model_target.saves()) == []
