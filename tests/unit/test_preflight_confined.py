# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9c81-3d5e-4b07-a6c4-1e8d7b0f5a23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where an organization's Python preflight runs (REQ-2133, REQ-1165): in a held organization, in
the organization's preflight worker (a warm host started through the code source's sandbox), at
each of the three places a check runs; in the server everywhere else, and for Provisa's own
built-in check.

The worker here is a real one, started with the command the sandbox was asked to confine; the
confinement itself is the kernel's and is shown on Linux by
``tests/integration/test_preflight_sandboxed.py``."""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import datetime
import decimal
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

import provisa.api.app  # noqa: F401  (says what the deployment is at import; the fixtures restate it)
from provisa.codesource import sandbox as code_sandbox
from provisa.codesource import services as services_mod
from provisa.codesource.worker import HostCommand, pool
from provisa.core import secrets_providers
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.events.handlers import _apply_preflight
from provisa.events.processor import NodeContext, PreflightQuarantine, PreprocessError
from provisa.federation.replica_preflight import row_check
from provisa.federation.runtime import EngineCapability
from provisa.mv import hold_checks, preflight_services
from provisa.mv.preflight import Decision, PreflightContractError, Verdict, run_preflight
from provisa.mv.preflight_eval import evaluate_streams, make_rows_evaluator
from provisa.security import sandbox_egress

pytestmark = pytest.mark.unit

#: A global of the server's that no check is given. A check that walks out of its own frame in the
#: server process can read it.
SERVER_SECRET = "the-server's-own-value-4f1c"

#: The check says which process ran it: it reads ``PID`` from the globals of whatever called it,
#: through a format string (which the registration check does not read). In the server that is a
#: module of the server's; in a confined run it is the worker's entry module.
_SAYS_ITS_PROCESS = """
def preflight(streams, ctx):
    held = []

    def walk():
        yield "{0.gi_frame.f_back.f_back.f_globals[PID]}".format(held[0])

    held.append(walk())
    for seen in held[0]:
        ctx.warn(seen)
    for name in streams:
        ctx.warn(name + "=" + str(len(list(streams[name]))))
    return ctx.ok()
"""

#: SEC-C5's format-string walk: the caller's globals, read as text by ``str.format``.
_FORMAT_WALK = """
def preflight(streams, ctx):
    held = []

    def walk():
        yield "{0.gi_frame.f_back.f_back.f_globals[SERVER_SECRET]}".format(held[0])

    held.append(walk())
    for seen in held[0]:
        return ctx.abort(seen)
"""

#: SEC-C5's built key: no constant in the check names a frame attribute or the global it reads.
_BUILT_KEY = """
def preflight(streams, ctx):
    held = []
    frame = "gi_" + "frame"
    back = "f_" + "back"
    key = "SERVER_" + "SECRET"
    path = "{0." + frame + "." + back + "." + back + ".f_" + "globals[" + key + "]}"

    def walk():
        yield path.format(held[0])

    held.append(walk())
    for seen in held[0]:
        return ctx.abort(seen)
"""


@pytest.fixture
def started(monkeypatch):
    """Every worker asked for: its argv, what it may read, its environment and its egress. The
    host is started as asked, outside a sandbox (the kernel's part is the integration test's)."""
    asked: list[SimpleNamespace] = []

    def sandboxed(argv, *, executable, read, env, cwd, egress):
        env = {**env, **code_sandbox.egress_proxy(egress).environment()}
        asked.append(
            SimpleNamespace(
                argv=list(argv("ADDRESS")),
                executable=executable,
                read=list(read),
                env=env,
                cwd=cwd,
                egress=egress,
            )
        )
        return HostCommand(argv=argv, env=env, cwd=cwd)

    monkeypatch.setattr(code_sandbox, "sandboxed", sandboxed)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=object()), raising=False)
    return asked


@pytest.fixture(autouse=True)
def _no_worker_outlives_its_test():
    """Each test starts with no preflight worker, and leaves none."""
    pool.stop_source("preflight")
    yield
    pool.stop_source("preflight")


def _worker_pid() -> int:
    """The process of the bound organization's preflight worker."""
    from provisa.mv import preflight_confined

    process = preflight_confined.worker("acme", "a test")._process
    assert process is not None
    return process.pid


def _deployment(monkeypatch, *, many: bool) -> None:
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
    monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: "default")
    # What a check that walks out of its frame in the server finds there.
    monkeypatch.setattr(preflight_services, "PID", os.getpid(), raising=False)
    monkeypatch.setattr(preflight_services, "SERVER_SECRET", SERVER_SECRET, raising=False)


@pytest.fixture
def held_org(monkeypatch, started):
    """An organization on a deployment of many: its sources, and its code, are held."""
    _deployment(monkeypatch, many=True)
    token = set_current_org("acme")
    assert sandbox_egress.held("acme")
    yield started
    reset_current_org(token)


@pytest.fixture
def single_org(monkeypatch, started):
    """A deployment of one organization, outside a sandbox."""
    _deployment(monkeypatch, many=False)
    token = set_current_org("acme")
    assert not sandbox_egress.held("acme")
    yield started
    reset_current_org(token)


def _ctx(**fields) -> NodeContext:
    return NodeContext(
        **{"node": "orders", "kind": "source", "claimed": [], "prior_hash": None, **fields}
    )


class _Engine:
    """An engine that streams ``batches`` for every input."""

    name = "test-engine"

    def __init__(self, batches):
        self._batches = batches

    def address_replicas(self, sql):
        return sql

    def require(self, cap) -> None:
        assert cap is EngineCapability.ARROW_STREAM

    def execute_engine_stream(self, sql, *a, **k):
        return object(), iter(self._batches)


def _batches(*groups):
    schema = pa.schema([("qty", pa.int64()), ("sku", pa.string())])
    return [pa.RecordBatch.from_pylist(list(g), schema=schema) for g in groups]


_THREE_BATCHES = _batches(
    [{"qty": 1, "sku": "a"}, {"qty": 2, "sku": "b"}],
    [{"qty": 3, "sku": None}],
    [{"qty": 4, "sku": "d"}, {"qty": 5, "sku": "e"}],
)


async def _at_view_refresh(source, rows_or_batches, ctx):
    return await evaluate_streams(_Engine(rows_or_batches), source, ["orders"], ctx)


async def _at_landed_source(source, rows, ctx):
    evaluate = make_rows_evaluator(source, "orders")
    assert evaluate is not None
    return await evaluate(rows, ctx)


async def _at_replica_build(source, rows, ctx):
    """The build's own steps: start, feed each batch, end. The check's ctx is the build's, so its
    warnings are copied to ``ctx`` for the caller to read."""
    check = row_check(
        SimpleNamespace(mv_preprocess=source), node="orders", columns=[], prior_hash=None
    )
    assert check is not None
    check.start()
    for batch in rows:
        await check.feed(batch)
    try:
        await check.end()
    finally:
        ctx.warn(check._ctx.warns)
    return Verdict(Decision.CONTINUE)


_ROWS = [{"qty": 1, "sku": "a"}, {"qty": 2, "sku": "b"}, {"qty": 3, "sku": None}]

_SITES = {
    "view refresh": (_at_view_refresh, _THREE_BATCHES, 5),
    "landed source": (_at_landed_source, _ROWS, 3),
    "replica build": (_at_replica_build, [_ROWS[:2], _ROWS[2:]], 3),
}


# -- (a) a held organization: the check runs in a child process, at each site ---------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("site", list(_SITES))
async def test_in_a_held_organization_the_check_runs_in_a_process_of_its_own(held_org, site):
    run, given, count = _SITES[site]
    ctx = _ctx()
    await run(_SAYS_ITS_PROCESS, given, ctx)
    seen, counted = ctx.warns
    assert counted == f"orders={count}"
    assert int(seen) != os.getpid(), "the check ran in the server process"
    assert int(seen) == _worker_pid(), "the check ran outside the organization's worker"
    (asked,) = held_org
    assert asked.argv[:2] == [sys.executable, "-I"]
    assert asked.argv[2].endswith("provisa/python/host.py")
    assert asked.argv[3:] == ["ADDRESS", str(asked.cwd), "preflight_entry"]
    assert str(asked.cwd).endswith("provisa/mv/preflight_host")


# -- (h) the worker is warm: one process serves every run -----------------------------------------


@pytest.mark.asyncio
async def test_consecutive_runs_use_the_same_worker_process(held_org):
    seen = []
    for run, given, _count in _SITES.values():
        for _ in range(2):
            ctx = _ctx()
            await run(_SAYS_ITS_PROCESS, given, ctx)
            seen.append(int(ctx.warns[0]))
    assert len(set(seen)) == 1 and seen[0] == _worker_pid() != os.getpid()
    assert len(held_org) == 1, "a second worker was started"


@pytest.mark.asyncio
async def test_each_organization_has_a_worker_of_its_own(held_org):
    ctx = _ctx()
    await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    other = set_current_org("globex")
    try:
        theirs = _ctx()
        await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, theirs)
    finally:
        reset_current_org(other)
    assert ctx.warns[0] != theirs.warns[0] and len(held_org) == 2
    assert [a.env["PATH"] for a in held_org] == [os.environ["PATH"]] * 2


@pytest.mark.asyncio
async def test_a_worker_that_ended_is_started_again_at_the_next_run(held_org):
    ctx = _ctx()
    await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    from provisa.mv import preflight_confined

    preflight_confined.worker("acme", "a test").stop()
    again = _ctx()
    await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, again)
    assert again.warns[0] != ctx.warns[0] and len(held_org) == 2


@pytest.mark.asyncio
async def test_runs_at_once_each_reach_their_own_verdict(held_org):
    import asyncio

    def check(n):
        return (
            "def preflight(streams, ctx):\n"
            f"    total = sum(r['qty'] for r in streams['orders']) + {n}\n"
            "    return ctx.abort(str(total))"
        )

    async def one(n):
        evaluate = make_rows_evaluator(check(n), "orders")
        assert evaluate is not None
        return (await evaluate(_ROWS, _ctx())).reason

    assert await asyncio.gather(*(one(n) for n in range(12))) == [str(6 + n) for n in range(12)]
    assert len(held_org) == 1


# -- (d) a single-organization install: in the server, as before ----------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("site", list(_SITES))
async def test_a_single_organization_install_runs_the_check_in_the_server(single_org, site):
    run, given, count = _SITES[site]
    ctx = _ctx()
    await run(_SAYS_ITS_PROCESS, given, ctx)
    assert ctx.warns == [str(os.getpid()), f"orders={count}"]
    assert single_org == [], "a confined process was started on a single-organization install"


@pytest.mark.asyncio
async def test_with_no_organization_bound_the_check_runs_in_the_server(monkeypatch, started):
    """Wiring and a deployment's own jobs bind none: ``held`` is false of no organization."""
    _deployment(monkeypatch, many=True)
    ctx = _ctx()
    await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    assert ctx.warns[0] == str(os.getpid()) and started == []


# -- (b) the same verdict, warnings and rows as in the server -------------------------------------

_SHOWS_ITS_ROWS = """
def preflight(streams, ctx):
    rows = [r for r in streams["orders"]]
    ctx.warn(str([[name, str(r[name])] for r in rows for name in sorted(r)]))
    ctx.warn(["n=" + str(len(rows)), "kind=" + ctx.kind, "prior=" + str(ctx.prior_hash)])
    return ctx.ok()
"""

_CHECKS = {
    "continue": "def preflight(streams, ctx):\n    return ctx.ok()",
    "none": "def preflight(streams, ctx):\n    return None",
    "false": "def preflight(streams, ctx):\n    return False",
    "string": "def preflight(streams, ctx):\n    return 'quarantine'",
    "abort with a reason": (
        "def preflight(streams, ctx):\n"
        "    ctx.warn('before')\n"
        "    return ctx.abort('negative quantity')"
    ),
    "quarantine": "def preflight(streams, ctx):\n    return ctx.quarantine('held back')",
    "short circuit": (
        "def preflight(streams, ctx):\n"
        "    for r in streams['orders']:\n"
        "        if r['qty'] > 1:\n"
        "            return ctx.abort('over one at ' + str(r['sku']))\n"
        "    return ctx.ok()"
    ),
    "whole table": (
        "def preflight(streams, ctx):\n"
        "    total = sum(r['qty'] for r in streams['orders'])\n"
        "    return ctx.ok() if total < 100 else ctx.abort(str(total))"
    ),
    "async": "async def preflight(streams, ctx):\n    return ctx.quarantine('later')",
    "shows its rows": _SHOWS_ITS_ROWS,
}

_TYPED = [
    {
        "qty": 7,
        "price": decimal.Decimal("12.50"),
        "ratio": 0.25,
        "ok": True,
        "sku": "a",
        "at": datetime.datetime(2026, 11, 16, 9, 30, tzinfo=datetime.UTC),
        "day": datetime.date(2026, 11, 16),
        "tags": ["x", "y"],
        "raw": b"\x00\x01",
        "none": None,
    },
    {
        "qty": -1,
        "price": decimal.Decimal("0.01"),
        "ratio": 1.5,
        "ok": False,
        "sku": None,
        "at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        "day": datetime.date(2026, 1, 1),
        "tags": [],
        "raw": b"",
        "none": None,
    },
]


def test_typed_rows_cross_to_the_process_as_equal_values():
    """Each value a check reads in its process equals the one the server holds (a timestamp's
    time zone is the same zone, as Arrow names it)."""
    from provisa.mv.preflight_confined import _batches, _encoded

    crossed = [
        row
        for batch in _batches(list(_TYPED))
        for read in pa.ipc.open_stream(_encoded(batch))
        for row in read.to_pylist()
    ]
    assert crossed == _TYPED
    assert [type(v) for r in crossed for v in r.values()] == [
        type(v) for r in _TYPED for v in r.values()
    ]


_INPUTS = {
    "empty rows": (_at_landed_source, lambda: []),
    "rows": (_at_landed_source, lambda: list(_ROWS)),
    "typed rows": (_at_landed_source, lambda: list(_TYPED)),
    "no batch": (_at_view_refresh, lambda: []),
    "an empty batch": (_at_view_refresh, lambda: _batches([])),
    "many batches": (_at_view_refresh, lambda: list(_THREE_BATCHES)),
    "build batches": (_at_replica_build, lambda: [_ROWS[:2], [], _ROWS[2:]]),
    "build of nothing": (_at_replica_build, lambda: []),
}


async def _outcome(run, source, given):
    """What a run of the check comes to, as the event loop sees it."""
    ctx = _ctx(prior_hash="abc", forced=True, columns=[("qty", "bigint")])

    async def bound(rows, ctx_):
        del rows
        return await run(source, given, ctx_)

    try:
        await _apply_preflight(bound, [], ctx, [("qty", "bigint")])
        came = "continue"
    except PreprocessError as exc:
        came = f"abort: {exc}"
    except PreflightQuarantine as exc:
        came = f"quarantine: {exc}"
    return came, ctx.warns


@pytest.mark.asyncio
@pytest.mark.parametrize("check", list(_CHECKS))
@pytest.mark.parametrize("given", list(_INPUTS))
async def test_a_confined_run_reaches_what_the_server_run_does(monkeypatch, started, check, given):
    run, make = _INPUTS[given]
    _deployment(monkeypatch, many=False)
    token = set_current_org("acme")
    try:
        in_server = await _outcome(run, _CHECKS[check], make())
        assert started == []
        _deployment(monkeypatch, many=True)
        confined = await _outcome(run, _CHECKS[check], make())
        assert len(started) == 1
        # A second run, on the worker the first left warm, comes to the same.
        assert await _outcome(run, _CHECKS[check], make()) == confined
        assert len(started) == 1
    finally:
        reset_current_org(token)
    assert confined == in_server


@pytest.mark.asyncio
async def test_a_check_that_raises_raises_the_same_words(held_org):
    source = (
        "def preflight(streams, ctx):\n    ctx.warn('first')\n    raise ValueError('bad row 7')"
    )
    from provisa.mv.preflight_confined import PreflightRaised

    ctx = _ctx()
    with pytest.raises(PreflightRaised) as raised:
        await _at_landed_source(source, _ROWS, ctx)
    assert str(raised.value) == "bad row 7" and raised.value.raised == "ValueError"
    assert ctx.warns == ["first"]


@pytest.mark.asyncio
async def test_a_check_that_returns_rows_breaks_the_contract_in_either_place(held_org):
    source = "def preflight(streams, ctx):\n    return [1, 2]"
    with pytest.raises(PreflightContractError, match="does not return rows"):
        await _at_landed_source(source, _ROWS, _ctx())


@pytest.mark.asyncio
async def test_an_input_the_check_never_reads_is_never_read(held_org):
    """Lazy as in the server: the engine's stream is not drawn from for a check that decides
    without it, and a check that stops early leaves the rest unread."""
    drawn: list[int] = []

    def counted():
        for i, batch in enumerate(_THREE_BATCHES):
            drawn.append(i)
            yield batch

    class _Counting(_Engine):
        def execute_engine_stream(self, sql, *a, **k):
            return object(), counted()

    verdict = await evaluate_streams(_Counting(None), _CHECKS["continue"], ["orders"], _ctx())
    assert verdict is not None and verdict.is_continue and drawn == []
    verdict = await evaluate_streams(_Counting(None), _CHECKS["short circuit"], ["orders"], _ctx())
    assert verdict is not None and verdict.is_abort and drawn == [0]


# -- (c) SEC-C5's two escapes read nothing of the server's -----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("escape", [_FORMAT_WALK, _BUILT_KEY], ids=["format walk", "built key"])
async def test_an_escape_reads_the_servers_globals_only_in_the_server(monkeypatch, started, escape):
    """Both forms pass the registration check. In the server they read a global of the server's;
    in a held organization the frame they walk to is the worker's, in another process, where the
    server's global does not exist."""
    _deployment(monkeypatch, many=False)
    token = set_current_org("acme")
    try:
        in_server = await _at_landed_source(escape, _ROWS, _ctx())
        assert in_server is not None and in_server.reason == SERVER_SECRET, (
            "the escape is real where the check is not confined"
        )
        _deployment(monkeypatch, many=True)
        from provisa.mv.preflight_confined import PreflightRaised

        ctx = _ctx()
        with pytest.raises(PreflightRaised) as raised:
            await _at_landed_source(escape, _ROWS, ctx)
    finally:
        reset_current_org(token)
    assert raised.value.raised == "KeyError" and "SERVER_SECRET" in str(raised.value)
    assert SERVER_SECRET not in str(raised.value) and SERVER_SECRET not in str(ctx.warns)
    assert len(started) == 1


def test_the_worker_is_given_nothing_of_the_servers(monkeypatch, held_org):
    """Its environment is PATH, Python's switches, where its pyarrow is and the proxy's variables;
    it reads Python's host, its code, the purity check's directory and Provisa's Python; its
    egress allows no host; its grant is the organization's, answering as the builds' role."""
    import pyarrow

    from provisa.core import region_admin
    from provisa.mv import preflight_confined
    from provisa.python.source import HOST

    monkeypatch.setenv("PROVISA_SECRET_KEY", "not-for-a-check")
    monkeypatch.setenv("PG_PASSWORD", "not-for-a-check")
    command = preflight_confined.command("acme")
    (asked,) = held_org
    packages = str(Path(pyarrow.__file__).resolve().parent.parent)
    assert set(command.env) == {
        "PATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONUNBUFFERED",
        "PROVISA_HOST_LIB",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "NODE_USE_ENV_PROXY",
    }
    assert command.env["PROVISA_HOST_LIB"] == packages
    assert "not-for-a-check" not in json.dumps(command.env)
    assert asked.egress == () and not code_sandbox.egress_proxy(()).admits("example.com", 443)
    assert [str(p) for p in asked.read] == [
        str(HOST.parent),
        str(preflight_confined.CODE),
        str(preflight_confined.CODE.parent),
        sys.base_prefix,
        packages,
    ]
    grant = command.services
    assert grant is not None and grant.org_id == "acme" and grant.role == region_admin.ROLE
    assert grant.secrets == {} and grant.egress == () and grant.product_ids == ()


# -- (e) Provisa's own built-in check runs in the server everywhere --------------------------------

_ASKS_DQ = (
    "import provisa_services as ps\n\n"
    "def preflight(streams, ctx):\n"
    "    found = ps.dq.dataset(7)\n"
    "    return ctx.abort(found['dataset'])\n"
)


@pytest.fixture
def answered(monkeypatch):
    """Provisa's services: each call recorded with the grant it is answered under, answered from
    ``replies``."""
    seen: list[dict] = []
    replies: dict[str, dict] = {}

    def dispatch(grant, service, method, sub, body, span, role):
        del body, span
        seen.append({"method": method, "path": f"{service}/{sub}", "grant": grant, "role": role})
        return json.dumps(replies[f"{service}/{sub}"]).encode()

    monkeypatch.setattr(services_mod, "_dispatch", dispatch)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(admin_db=object()), raising=False)
    return SimpleNamespace(seen=seen, replies=replies)


@pytest.mark.asyncio
async def test_hold_on_failing_checks_runs_in_the_server_in_a_held_organization(
    monkeypatch, held_org, answered
):
    async def inputs(table_id):
        del table_id
        return (1,)

    monkeypatch.setattr(hold_checks, "nearest_inputs", inputs)
    answered.replies["dq/dataset/1"] = {
        "dataset": "sales.orders",
        "checker_tables": [{"checker_table": "dq.orders", "latest_run": {"outcome": "fail"}}],
    }
    verdict = await evaluate_streams(_Engine([]), None, ["orders"], _ctx(), hold=True, table_id=9)
    assert verdict is not None and verdict.is_abort
    assert verdict.reason == "a check on its inputs failed: dq.orders failed on sales.orders"
    assert held_org == [], "Provisa's own check was sent to a confined process"


@pytest.mark.asyncio
async def test_an_organizations_copy_of_the_built_in_check_is_confined(held_org, answered):
    """The option shows code a steward can copy into the view's own preflight: there it is the
    organization's, and reaches dq from the worker, through the proxy, under the worker's grant:
    answered for the view the call checks and its products, as the builds' role."""
    from provisa.core import region_admin

    answered.replies["dq/dataset/7"] = {"dataset": "sales.orders", "checker_tables": []}
    check = preflight_services.compile_check(_ASKS_DQ, subject="view v", product_ids=("p1",))
    verdict = await run_preflight(check, {}, _ctx())
    assert verdict.decision is Decision.ABORT and verdict.reason == "sales.orders"
    other = preflight_services.compile_check(_ASKS_DQ, subject="view w", product_ids=("p2", "p3"))
    await run_preflight(other, {}, _ctx())
    (asked,) = held_org
    assert "PROVISA_SERVICES" not in asked.env, "the grant is issued as the worker starts"
    first, second = answered.seen
    assert first["grant"].org_id == "acme" and first["role"] == region_admin.ROLE
    assert first["grant"].product_ids == ("p1",) and first["grant"].source_id == "view v"
    assert second["grant"].product_ids == ("p2", "p3") and second["grant"].source_id == "view w"
    held = [g for g in services_mod._grants.values() if g.org_id == "acme"]
    assert len(held) == 1 and held[0].source_id == "preflight"
    pool.stop_source("preflight")
    assert not [g for g in services_mod._grants.values() if g.org_id == "acme"], (
        "the grant outlived the worker"
    )


# -- (f) no confined process: refused by name, never run in the server -----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("site", list(_SITES))
async def test_with_no_sandbox_a_held_organizations_check_is_refused_by_name(monkeypatch, site):
    from provisa.mv.preflight_confined import PreflightConfinedUnavailable

    _deployment(monkeypatch, many=True)
    monkeypatch.setattr(code_sandbox.platform, "system", lambda: "Windows")
    run, given, _count = _SITES[site]
    token = set_current_org("acme")
    ctx = _ctx()
    try:
        with pytest.raises(PreflightConfinedUnavailable) as refused:
            await run(_SAYS_ITS_PROCESS, given, ctx)
    finally:
        reset_current_org(token)
    assert refused.value.code == "preflight.confined_unavailable"
    assert refused.value.params == {
        "subject": "view orders" if site == "view refresh" else "table orders",
        "reason": "a code source cannot run on Windows: it has no code-source sandbox",
    }
    assert ctx.warns == [], "the check ran in the server instead"


def _host(monkeypatch, argv: list[str]) -> None:
    """The sandbox answers with ``argv`` (given the socket's address last) as the host."""
    monkeypatch.setattr(
        code_sandbox,
        "sandboxed",
        lambda _argv, *, env, cwd, **k: HostCommand(
            argv=lambda address: [*argv, address], env=env, cwd=cwd
        ),
    )


@pytest.mark.asyncio
async def test_a_host_that_cannot_be_started_is_refused_by_name(monkeypatch, held_org):
    from provisa.mv.preflight_confined import PreflightConfinedUnavailable

    _host(monkeypatch, ["/nonexistent/sandbox-launcher"])
    ctx = _ctx()
    with pytest.raises(PreflightConfinedUnavailable, match="sandbox-launcher"):
        await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    assert ctx.warns == []


@pytest.mark.asyncio
async def test_a_launcher_that_refuses_to_confine_is_refused_with_what_it_said(
    monkeypatch, held_org
):
    """The Linux launcher exits before the host starts when the kernel gives it no Landlock."""
    from provisa.mv.preflight_confined import PreflightConfinedUnavailable

    said = "code-source sandbox unavailable: the kernel has no Landlock"
    _host(monkeypatch, [sys.executable, "-c", f"import sys; sys.exit({said!r})"])
    ctx = _ctx()
    with pytest.raises(PreflightConfinedUnavailable) as refused:
        await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    assert refused.value.params["subject"] == "table orders"
    assert said in refused.value.params["reason"] and ctx.warns == []


#: A host that says hello on each connection, then ends at the first call it is sent.
_DIES_AT_ITS_FIRST_CALL = """
import json, os, socket, struct, sys
socks = []
for _ in range(int(os.environ["PROVISA_HOST_CONNECTIONS"])):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sys.argv[1])
    hello = {"op": "hello", "protocol": 5, "code_hash": os.environ["PROVISA_CODE_HASH"],
             "exports": []}
    body = b"J" + json.dumps(hello).encode()
    sock.sendall(struct.pack(">I", len(body)) + body)
    socks.append(sock)
import select
select.select(socks, [], [])
sys.exit("killed at its memory limit")
"""


@pytest.mark.asyncio
async def test_a_worker_that_ends_under_a_run_is_refused_by_name(monkeypatch, held_org):
    from provisa.mv.preflight_confined import PreflightConfinedFailed

    _host(monkeypatch, [sys.executable, "-c", _DIES_AT_ITS_FIRST_CALL])
    ctx = _ctx()
    with pytest.raises(PreflightConfinedFailed) as refused:
        await _at_landed_source(_SAYS_ITS_PROCESS, _ROWS, ctx)
    assert refused.value.code == "preflight.confined_failed"
    assert refused.value.params["subject"] == "table orders"
    assert "killed at its memory limit" in refused.value.params["reason"]
    assert ctx.warns == []


@pytest.mark.asyncio
async def test_a_code_sources_own_host_serves_no_preflight(tmp_path, monkeypatch, held_org):
    """The call is the protocol's, and only the preflight worker's code answers it: a host on any
    other code refuses it by name."""
    from provisa.mv import preflight_confined
    from provisa.mv.preflight_confined import PreflightConfinedFailed

    (tmp_path / "preflight_entry.py").write_text("def rows():\n    return []\n")
    with pytest.raises(PreflightConfinedFailed, match="this host serves no preflight"):
        await preflight_confined.run_confined(
            _CHECKS["continue"], {"orders": _ROWS}, _ctx(), subject="table orders", code=tmp_path
        )
    pool.stop_source(f"preflight:{tmp_path}")


def test_both_refusals_stop_the_land_as_an_abort_does():
    """They are the loop's fatal preflight outcome: the node's error event, nothing landed."""
    from provisa.mv.preflight_confined import PreflightConfinedFailed, PreflightConfinedUnavailable

    assert issubclass(PreflightConfinedUnavailable, PreprocessError)
    assert issubclass(PreflightConfinedFailed, PreprocessError)


def test_each_refusal_has_its_message_in_every_locale():
    from pathlib import Path

    locales = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) >= 12
    for catalog in catalogs:
        entries = json.loads(catalog.read_text())["serverErrors"]["preflight"]
        for key in ("confined_unavailable", "confined_failed"):
            assert "{{subject}}" in entries[key] and "{{reason}}" in entries[key], catalog
