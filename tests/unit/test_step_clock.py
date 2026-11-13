# Copyright (c) 2026 Kenneth Stott
# Canary: 5d07b3c9-2e8a-4f61-9c4b-a7e1f0d6382b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The steps inside a demo sandbox's open are timed on its stages' own line (REQ-2006): a slow
open names what inside a stage was slow, and outside an open a step logs nothing."""

# Requirements: REQ-2006

from __future__ import annotations

import logging
import re

import pytest

from provisa.core import step_clock

pytestmark = pytest.mark.unit

#: tests/integration/test_demo_open_steady_e2e.py's stage line.
_STAGE = re.compile(r"demo sandbox (sbx[0-9a-z]+) of [a-z0-9]+: (.+) took ([0-9.]+)s")


def test_a_step_within_an_open_is_logged_as_its_stages_are(caplog):
    caplog.set_level(logging.WARNING, logger=step_clock.__name__)
    with step_clock.every_step_of("demo sandbox sbx0123456789ab of alpha"):
        with step_clock.step("environment dev: its schema"):
            pass

    found = [_STAGE.search(r.getMessage()) for r in caplog.records]
    assert [(m.group(1), m.group(2)) for m in found if m] == [
        ("sbx0123456789ab", "environment dev: its schema")
    ]


def test_a_step_outside_an_open_logs_nothing(caplog):
    caplog.set_level(logging.DEBUG, logger=step_clock.__name__)
    with step_clock.step("environment dev: its schema"):
        pass
    step_clock.took("history of prod: its commit", 1.0)

    assert caplog.records == []
    assert step_clock.timing() is False


async def test_a_history_write_names_each_of_its_steps(caplog, tmp_path, monkeypatch):
    from tests.helpers import ready_org_folder

    from provisa.core import env_repo

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder("sbx0123456789ab")

    async def _project(conn, schema):
        return {"meta/tables/t.yaml": {"table_name": "t"}}

    async def _stamp(conn, schema):
        return 1

    async def _quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(env_repo, "project", _project)
    monkeypatch.setattr("provisa.core.env_project.model_stamp", _stamp)
    for name in ("set_drifted", "set_position", "set_deployed_stamp", "get_env"):
        monkeypatch.setattr(f"provisa.core.env_store.{name}", _quiet)
    monkeypatch.setattr("provisa.core.env_ci.announce", _quiet)
    caplog.set_level(logging.WARNING, logger=step_clock.__name__)

    with step_clock.every_step_of("demo sandbox sbx0123456789ab of alpha"):
        await env_repo.write_through(
            "conn", "admin", "sbx0123456789ab", "prod", "org_sbx", "note", None
        )

    steps = [m.group(2) for r in caplog.records if (m := _STAGE.search(r.getMessage()))]
    assert steps == [
        "history of prod: its model read",
        "history of prod: its files rendered",
        "history of prod: its commit",
        "history of prod: its position recorded",
    ]
