# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e0c94-5d21-4a8f-b6e3-9c4f1a2d7e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox's environments' runtimes are built inline during its open, each once and cloned
from the sandbox's PROD (REQ-2006, amended 2026-11-04).

A sandbox's environments are created from its prod and given the template's prepared rows through
their schemas, with no runtime built (J656). Left to the first request that asked for one, a tour
step (profiling's ``env: synthetic``, testdata's preprod and dev) waited on an ordinary build of
15-20 s that seeded the whole model again. The open builds each one once, cloned from the sandbox's
PROD (copying its compiled schema rather than rebuilding it), so the open takes ~9 s rather than
~20 s. The template's are never built."""

# Requirements: REQ-2006

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from provisa.api import app, demo_sandbox, demo_synthetic, demo_templates, redaction_load
from provisa.api.demo_templates import PublishedDemo
from provisa.api.org_runtime import OrgRegistry, runtime_key
from provisa.core.env_classes import INHERIT, REFUSED, TEST_SYNTHETIC

_SANDBOX = "sbx0123456789ab"
_WANTED = [
    {"name": "synthetic", "data_mode": TEST_SYNTHETIC, "mutation_handling": REFUSED},
    {"name": "preprod", "data_mode": INHERIT, "mutation_handling": REFUSED},
]


def test_the_open_builds_each_environments_runtime_once_cloned_after_its_rows(monkeypatch):
    demo = PublishedDemo(name="profiling", title="Profiling", directory=Path("/demos/profiling"))
    events: list[tuple] = []

    async def _none(*args, **kwargs):
        return None

    async def _template_environments(template_org):
        assert template_org == demo.template_org
        return _WANTED

    async def _create(org_id, wanted, person):
        events.extend(("created", org_id, env["name"]) for env in wanted)
        return [env["name"] for env in wanted]

    async def _copy(template_org, org_id, envs):
        events.append(("rows", org_id, tuple(envs)))

    async def _flags(org_id):
        return app.OrgLane(False, False, None, None, None, "", None)

    async def _env(admin_db, org_id, env):
        held = {e["name"]: e for e in _WANTED}[env]
        return {"expires_at": None, **held}

    async def _build(org_id, *, env, model_cloned=False, **kwargs):
        events.append(("built", org_id, env, model_cloned))
        return SimpleNamespace(org_id=org_id, env=env)

    registry = OrgRegistry()
    # The sandbox's prod runtime, built by the open before its environments (demo_sandbox._build).
    registry.set(_SANDBOX, SimpleNamespace(org_id=_SANDBOX, env="prod"))
    monkeypatch.setattr(demo_templates, "template_environments", _template_environments)
    monkeypatch.setattr(demo_templates, "create_environments", _create)
    monkeypatch.setattr(demo_synthetic, "copy_to_sandbox", _copy)
    monkeypatch.setattr(app, "ensure_org_encryption", _none)
    monkeypatch.setattr(app, "_read_org_flags", _flags)
    monkeypatch.setattr(app, "_region_lane_of", _none)
    monkeypatch.setattr(app, "build_org_runtime", _build)
    monkeypatch.setattr("provisa.core.env_store.get_env", _env)
    monkeypatch.setattr(redaction_load, "ensure_loaded", _none)
    monkeypatch.setattr(app.state, "org_registry", registry)
    monkeypatch.setattr(app.state, "admin_db", object())

    asyncio.run(demo_sandbox._environments(demo, _SANDBOX, "sandbox-visitor"))

    # Each env runtime is built inline (before _environments returns), cloned, after its rows.
    # Each environment is its own line (created, given its rows, built), the lines side by side.
    assert sorted(events, key=repr) == sorted(
        [
            ("created", _SANDBOX, "synthetic"),
            ("created", _SANDBOX, "preprod"),
            ("rows", _SANDBOX, ("synthetic",)),
            # Each once, cloned, after its rows and bindings are given; none of the template's.
            ("built", _SANDBOX, "synthetic", True),
            ("built", _SANDBOX, "preprod", True),
        ],
        key=repr,
    ), events
    for name in ("synthetic", "preprod"):
        line = [e for e in events if name in e or (name,) in e]
        assert line[0] == ("created", _SANDBOX, name), events
        assert line[-1] == ("built", _SANDBOX, name, True), events
    assert events.index(("rows", _SANDBOX, ("synthetic",))) < events.index(
        ("built", _SANDBOX, "synthetic", True)
    ), events
    # Built when the open returns: the first request is served it, and builds nothing.
    for env in _WANTED:
        assert registry.get(runtime_key(_SANDBOX, env["name"])) is not None, env["name"]
