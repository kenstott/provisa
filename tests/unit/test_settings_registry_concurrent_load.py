# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9d2b68-7a15-4e0c-b4d7-9c8e6a1f5d02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The settings registry loads its catalog once, whichever threads reach it first (REQ-1913).

Every request runs on its own thread, so the first read of a setting after a process starts can
come from many threads at once. Each of them must find the whole catalog."""

# Requirements: REQ-1913

from __future__ import annotations

import threading

from provisa.core import settings_registry

_THREADS = 8


def test_threads_reaching_an_unloaded_registry_all_find_the_setting(monkeypatch):
    monkeypatch.setattr(settings_registry, "_settings", {})
    monkeypatch.setattr(settings_registry, "_loaded", False)
    barrier = threading.Barrier(_THREADS)
    found: list[str] = []
    errors: list[BaseException] = []

    def _read() -> None:
        barrier.wait()
        try:
            found.append(settings_registry.setting("limits.request_timeouts").key)
        except BaseException as exc:  # noqa: BLE001 — collected and asserted on below
            errors.append(exc)

    threads = [threading.Thread(target=_read) for _ in range(_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert errors == []
    assert found == ["limits.request_timeouts"] * _THREADS
    assert len(settings_registry.all_settings()) == len(set(settings_registry._settings))


def test_a_registry_that_failed_to_load_is_not_marked_loaded(monkeypatch):
    """A load that raises leaves the registry unloaded, so the next read loads it again instead
    of answering from a part-filled registry."""
    from provisa.core import settings_catalog

    declared = list(settings_catalog.DECLARED)
    monkeypatch.setattr(settings_registry, "_settings", {})
    monkeypatch.setattr(settings_registry, "_loaded", False)
    monkeypatch.setattr(settings_catalog, "DECLARED", [declared[0], declared[0]])

    try:
        settings_registry.all_settings()
    except ValueError as err:
        assert "declared twice" in str(err)
    else:
        raise AssertionError("a catalog declaring a setting twice must not load")
    assert settings_registry._loaded is False

    monkeypatch.setattr(settings_registry, "_settings", {})
    monkeypatch.setattr(settings_catalog, "DECLARED", declared)
    assert len(settings_registry.all_settings()) == len(declared)
