# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1f9e72-c3a8-4d06-8e47-2a9c0d6f4b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reload on a changed config stamp: the one re-apply mechanism of a process (REQ-1914).

Every process holds copies of what the control plane stores — the compiled model of each org it
serves, the operator settings, each org's own settings. Each copy was loaded at a stamp
(``provisa/core/config_stamp.py``). On a short interval the process reads the stored stamps — one
statement per control plane it holds something from — and reloads each copy whose stamp differs.
A request reads nothing from the control plane for this; it runs on what its process has loaded.

A :class:`Target` is one such copy: where its stamp is stored, the stamp the copy was loaded at,
and how to reload it. The reload records the stamp it loaded at, having read the stamp BEFORE the
rows, so a change that lands during a reload is picked up by the next check and never missed.

The interval is the operator setting ``config.reload_interval``.
"""

# Requirements: REQ-1914

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.core import config_stamp

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

INTERVAL_SETTING = "config.reload_interval"


@dataclass(frozen=True)
class Target:
    """One copy this process holds of something a control plane stores."""

    name: str  # for the log: "settings", "org acme: model"
    db: "Database"  # the control plane whose config_stamp row is compared
    kind: str  # the row: config_stamp.MODEL | config_stamp.SETTINGS
    loaded: Callable[[], int | None]  # the stamp the copy was loaded at; None = no copy held
    reload: Callable[[], Awaitable[None]]  # load again, recording the stamp read before the rows


async def check(targets: list[Target]) -> list[str]:
    """Reload every target whose stored stamp differs from the one it was loaded at; the names of
    those reloaded. One read per distinct control plane.

    A target whose plane cannot be read, or whose reload fails, is logged and left as it was: its
    loaded stamp still differs, so the next check tries it again."""
    stored: dict[int, dict[str, int]] = {}
    reloaded: list[str] = []
    for target in targets:
        try:
            if id(target.db) not in stored:
                stored[id(target.db)] = await config_stamp.read(target.db)
            current = stored[id(target.db)][target.kind]
            loaded = target.loaded()
            # No copy held (still being built, or dropped by this process's own write): whoever
            # loads it records the stamp it loaded at. There is nothing here to reload.
            if loaded is None or loaded == current:
                continue
            await target.reload()
        except Exception:  # allow-ble: a background check's boundary — logged, next check retries
            log.exception("config reload of %s failed; retrying on the next check", target.name)
            continue
        log.info("config reloaded: %s (stamp %s)", target.name, current)
        reloaded.append(target.name)
    return reloaded


def _interval(in_force: float | None) -> float:
    """The reload interval now. A stored value that cannot be used is logged naming the setting
    and the interval in force stays — the rule every live setting follows
    (``settings_registry.apply_changes``). With none in force yet (the first read, at start) the
    error stops the start."""
    from provisa.core import settings_registry

    try:
        return float(settings_registry.value(INTERVAL_SETTING))
    except settings_registry.SettingInvalid as err:
        if in_force is None:
            raise
        log.error("setting %s is not applied, the value in force stays: %s", err.key, err)
        return in_force


async def _loop(targets: Callable[[], list[Target]], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await check(targets())
        except Exception:  # allow-ble: a background loop's boundary — logged, next tick retries
            log.exception("config stamp check failed; retrying on the next tick")
        interval = _interval(interval)


_task: Any = None


def start(targets: Callable[[], list[Target]]) -> None:
    """Start this process's watcher (once; stopped with the other background work).
    ``targets`` is asked on every tick, because the orgs a process serves come and go."""
    global _task
    if _task is not None and not _task.done():
        return
    from provisa.core.connection_loop import spawn_long_lived

    _task = spawn_long_lived(_loop(targets, _interval(None)), name="config-watch")
