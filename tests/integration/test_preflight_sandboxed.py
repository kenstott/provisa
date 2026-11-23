# Copyright (c) 2026 Kenneth Stott
# Canary: b17d4e3a-90c5-4f62-8a1b-5e7c2d9f0364
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A held organization's Python preflight under the real sandbox (REQ-2133, REQ-1165): the check
runs in the organization's preflight worker, which the kernel confines, reaches the same verdict,
and whatever code is in that worker opens no file outside what the profile names and no connection
but the egress proxy's.

The last is shown by a probe served as the worker's code, by the same host, command, read list and
environment: it stands for a check that has escaped the language's restrictions altogether."""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import json
import os
import socket
import textwrap

import pytest

import provisa.api.app  # noqa: F401  (says what the deployment is at import; the fixtures restate it)
from provisa.core import secrets_providers
from provisa.codesource.worker import pool
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.events.processor import NodeContext
from provisa.mv import preflight_services
from provisa.mv.preflight_confined import PreflightRaised, run_confined
from provisa.mv.preflight_eval import make_rows_evaluator
from provisa.security import sandbox_egress

pytestmark = [pytest.mark.integration]

SERVER_SECRET = "the-server's-own-value-9b2e"

_ROWS = [{"qty": 1, "sku": "a"}, {"qty": 2, "sku": "b"}, {"qty": -3, "sku": None}]

_SAYS_ITS_PROCESS = """
def preflight(streams, ctx):
    held = []

    def walk():
        yield "{0.gi_frame.f_back.f_back.f_globals[PID]}".format(held[0])

    held.append(walk())
    for seen in held[0]:
        ctx.warn(seen)
    negative = [r["sku"] for r in streams["orders"] if r["qty"] < 0]
    return ctx.abort("negative: " + str(negative)) if negative else ctx.ok()
"""

_FORMAT_WALK = """
def preflight(streams, ctx):
    held = []

    def walk():
        yield "{0.gi_frame.f_back.f_back.f_globals[SERVER_SECRET]}".format(held[0])

    held.append(walk())
    for seen in held[0]:
        return ctx.abort(seen)
"""


@pytest.fixture
def held_org(monkeypatch):
    """An organization on a deployment of many, with a secret in the server's environment and one
    in a global of the module that calls a check in the server."""
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    monkeypatch.setattr(preflight_services, "PID", os.getpid(), raising=False)
    monkeypatch.setattr(preflight_services, "SERVER_SECRET", SERVER_SECRET, raising=False)
    monkeypatch.setenv("PROVISA_SECRET_KEY", SERVER_SECRET)
    token = set_current_org("acme")
    assert sandbox_egress.held("acme")
    yield
    reset_current_org(token)
    pool.stop_all()


def _ctx() -> NodeContext:
    return NodeContext(node="orders", kind="source", claimed=[], prior_hash=None)


@pytest.mark.asyncio
async def test_a_held_organizations_check_runs_confined_and_reaches_its_verdict(held_org):
    evaluate = make_rows_evaluator(_SAYS_ITS_PROCESS, "orders")
    assert evaluate is not None
    ctx = _ctx()
    verdict = await evaluate(_ROWS, ctx)
    assert verdict is not None and verdict.is_abort and verdict.reason == "negative: [None]"
    (seen,) = ctx.warns
    assert int(seen) != os.getpid(), "the check ran in the server process"
    again = _ctx()
    assert (await evaluate(_ROWS, again)) == verdict
    assert again.warns == [seen], "the second run was not served by the same worker process"


@pytest.mark.asyncio
async def test_the_format_walk_reads_nothing_of_the_servers_under_the_sandbox(held_org):
    evaluate = make_rows_evaluator(_FORMAT_WALK, "orders")
    assert evaluate is not None
    with pytest.raises(PreflightRaised) as raised:
        await evaluate(_ROWS, _ctx())
    assert raised.value.raised == "KeyError" and SERVER_SECRET not in str(raised.value)


#: Served as the worker's code. It answers a preflight call with what each attempt came to.
_PROBE = textwrap.dedent(
    """
    import json, os, socket

    ASKED = json.loads(ASKED_TEXT)

    def tried(what):
        try:
            what()
        except BaseException as exc:
            return type(exc).__name__ + ": " + str(exc)
        return "done"

    def opened(path, mode="rb"):
        def attempt():
            with open(path, mode) as f:
                if "r" in mode:
                    f.read(1)
                else:
                    f.write(b"x")
        return tried(attempt)

    def connected(family, kind, address):
        def attempt():
            with socket.socket(family, kind) as s:
                s.settimeout(5)
                s.connect(address)
        return tried(attempt)

    def _provisa_preflight(source, fields, inputs, whole, pull, pa):
        found = {
            "environment": sorted(os.environ),
            "own file": opened(__file__),
            "outside file": opened(ASKED["outside"]),
            "passwd": opened("/etc/passwd"),
            "server environ": opened("/proc/%d/environ" % os.getppid()),
            "write beside itself": opened(os.path.join(os.path.dirname(__file__), "w"), "wb"),
            "write tmp": opened("/tmp/preflight-probe-w", "wb"),
            "listener": connected(socket.AF_INET, socket.SOCK_STREAM, ("127.0.0.1", ASKED["port"])),
            "internet": connected(socket.AF_INET, socket.SOCK_STREAM, ("1.1.1.1", 443)),
            "udp": connected(socket.AF_INET, socket.SOCK_DGRAM, ("127.0.0.1", 53)),
            "unix": connected(socket.AF_UNIX, socket.SOCK_STREAM, ASKED["unix"]),
        }
        return {"decision": "continue", "reason": json.dumps(found), "warns": [], "pid": os.getpid()}
    """
)


@pytest.mark.asyncio
async def test_code_in_the_confined_process_opens_no_file_and_no_connection(held_org, tmp_path):
    outside = tmp_path / "outside" / "secret.txt"
    outside.parent.mkdir()
    outside.write_text(SERVER_SECRET)
    probe_dir = tmp_path / "probe"
    probe_dir.mkdir()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    unix_path = str(tmp_path / "outside" / "s.sock")
    unix = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix.bind(unix_path)
    unix.listen(1)
    asked = {"outside": str(outside), "port": listener.getsockname()[1], "unix": unix_path}
    # The worker's entry module, in the probe's directory: what it is asked is written into it.
    (probe_dir / "preflight_entry.py").write_text(
        _PROBE.replace("ASKED_TEXT", repr(json.dumps(asked)))
    )
    try:
        verdict = await run_confined(
            "def preflight(streams, ctx):\n    return ctx.ok()",
            {"orders": _ROWS},
            _ctx(),
            subject="table orders",
            code=probe_dir,
        )
    finally:
        listener.close()
        unix.close()
    found = json.loads(verdict.reason)
    assert found.pop("own file") == "done", "the probe did not run as the worker's code does"
    environment = found.pop("environment")
    assert "PROVISA_SECRET_KEY" not in environment
    assert set(environment) <= {
        "PATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONUNBUFFERED",
        "PROVISA_HOST_LIB",
        "PROVISA_HOST_CONNECTIONS",
        "PROVISA_CODE_HASH",
        "PROVISA_DISCOVERED",  # REQ-2267: its own generated items' handlers, from its manifest
        "PROVISA_SERVICES",  # the worker's own grant: its organization's, the builds' role
        "UV_THREADPOOL_SIZE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "NODE_USE_ENV_PROXY",
        "LC_CTYPE",  # CPython's own, set as it starts under the C locale
    }
    for attempt, came in found.items():
        assert came != "done", f"the confined process could {attempt}"
        assert came.split(":")[0] in {"PermissionError", "OSError"}, (attempt, came)
    assert not (probe_dir / "w").exists()
