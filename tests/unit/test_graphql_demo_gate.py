# Copyright (c) 2026 Kenneth Stott
# Canary: f514d5df-ca96-4b9c-9c85-7acf645522f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An optional integration's ENABLED switch must be able to say no.

The class of defect: a feature is gated on "is any of its config present?" while the deployment
template always supplies that config with a default. The gate is then permanently open and the
explicit off switch is dead. graphql-demo did exactly this — docker-compose.app.yml:33 always
injects ``GRAPHQL_DEMO_URL``, so ``GRAPHQL_DEMO_ENABLED=false`` never disabled anything and every
deploy without a graphql-demo container tried to introspect ``graphql-demo:4000`` at boot.

So these tests vary ONLY the switch while holding the URL set, which is the state every real
deployment is in, and is the state the old gate could not distinguish from "on".
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

import pytest

from provisa.api import app_startup

_URL = "http://graphql-demo:4000/graphql"


@pytest.fixture
def registrations(monkeypatch):
    """Records the registration coroutines the gate would have scheduled, without running one."""
    scheduled: list[str] = []

    def _fake_create_task(coro, *a, **kw):
        scheduled.append(getattr(coro, "__name__", repr(coro)))
        coro.close()  # never let the real introspection run
        return None

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)
    return scheduled


async def _run(monkeypatch, enabled: str | None, url: str | None = _URL):
    for name, value in (("GRAPHQL_DEMO_ENABLED", enabled), ("GRAPHQL_DEMO_URL", url)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    await app_startup._auto_register_graphql_demo(logging.getLogger("test"))


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", ["false", "0", "no", "", None])
async def test_a_set_url_does_not_enable_the_demo(monkeypatch, registrations, enabled):
    # The regression: URL set (as compose always sets it) + switch off must mean OFF. Reaching out
    # to a host that is not in the stack is what produced the boot-time name-resolution error.
    await _run(monkeypatch, enabled)
    assert registrations == [], f"GRAPHQL_DEMO_ENABLED={enabled!r} still scheduled a registration"


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", ["1", "true", "TRUE", "yes"])
async def test_the_switch_still_turns_the_demo_on(monkeypatch, registrations, enabled):
    await _run(monkeypatch, enabled)
    assert len(registrations) == 1


@pytest.mark.asyncio
async def test_the_switch_alone_is_enough_without_a_url(monkeypatch, registrations):
    """URL absent falls back to the compose service name — the URL says WHERE, not WHETHER."""
    await _run(monkeypatch, "true", url=None)
    assert len(registrations) == 1


def test_compose_always_supplies_a_default_url():
    """Pins the deployment fact the gate has to survive: the template gives GRAPHQL_DEMO_URL a
    default, so 'is the URL set?' can never be the switch. If this stops being true the gate is
    still correct — but the reason it must stay ENABLED-only should be re-read."""
    compose = Path(__file__).resolve().parents[2] / "docker-compose.app.yml"
    src = compose.read_text()
    assert re.search(r"GRAPHQL_DEMO_URL:\s*\"\$\{GRAPHQL_DEMO_URL:-", src), (
        "docker-compose.app.yml no longer defaults GRAPHQL_DEMO_URL"
    )
    assert re.search(r"GRAPHQL_DEMO_ENABLED:\s*\"\$\{GRAPHQL_DEMO_ENABLED:-false\}\"", src)
