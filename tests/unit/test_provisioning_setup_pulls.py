# Copyright (c) 2026 Kenneth Stott
# Canary: 0b43f969-a3dd-4d44-88f5-6e91bc51714a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisioning demo's setup step (setup_pulls.py) runs its consumer as the run compose does:
told its topics' suffix, the broker's namespace (PROVISIONING_KAFKA_NS). The bundle build runs the
step directly, where no compose file hands the consumer TOPIC_SUFFIX (Demo bundles run
39467273812: KeyError: 'TOPIC_SUFFIX')."""

import importlib.util
from pathlib import Path

import pytest

_SETUP = Path(__file__).resolve().parents[2] / "demo/named/provisioning/setup_pulls.py"


@pytest.fixture
def setup_pulls(monkeypatch):
    monkeypatch.setenv("PROVISA_URL", "http://127.0.0.1:1")
    spec = importlib.util.spec_from_file_location("provisioning_setup_pulls", _SETUP)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_dataset_built", lambda client: True)
    monkeypatch.setattr(module, "_sql", lambda client, sql: [])
    return module


@pytest.mark.parametrize("namespace", ["", "_provisioning_1a2b3c4d"])
def test_the_consumer_is_told_its_topics_suffix(setup_pulls, monkeypatch, namespace):
    monkeypatch.setenv("PROVISIONING_KAFKA_NS", namespace)
    ran: list[dict] = []
    monkeypatch.setattr(
        setup_pulls.subprocess, "run", lambda cmd, env, check, timeout: ran.append(env)
    )
    setup_pulls.main()
    [env] = ran
    assert env["TOPIC_SUFFIX"] == namespace


def test_a_run_given_no_namespace_stops_naming_it(setup_pulls, monkeypatch):
    monkeypatch.delenv("PROVISIONING_KAFKA_NS", raising=False)
    monkeypatch.setattr(setup_pulls.subprocess, "run", lambda *a, **k: None)
    with pytest.raises(KeyError, match="PROVISIONING_KAFKA_NS"):
        setup_pulls.main()


# -- the read window (GitHub #323): the view is refused outside its window, 503 with the opening --

_WINDOW = "data.view_outside_load_window"
_OPENING = {"view": "view_order_margins", "opens_at": "2026-11-08T00:30+00:00"}


class _FakeServer:
    """Answers ``refusal`` (by default the window's, naming 00:30 UTC) until the fake clock, in
    seconds after 00:10 UTC, reaches ``opens_at``; then rows."""

    def __init__(self, clock, opens_at, refusal=None):
        import httpx

        self.clock, self.opens_at = clock, opens_at
        self.refusal = refusal or httpx.Response(
            503, json={"detail": "closed", "code": _WINDOW, "params": _OPENING}
        )
        self.posts: list[float] = []

    def post(self, path, json):
        import httpx

        self.posts.append(self.clock["now"])
        if self.clock["now"] < self.opens_at:
            return self.refusal
        return httpx.Response(200, json={"data": {"sql": [{"n": 1}]}})


@pytest.fixture
def clock():
    return {"now": 0.0}


@pytest.fixture
def module(monkeypatch, clock):
    """setup_pulls with its own _sql, a fake clock, and a sleep that advances it."""
    from datetime import datetime, timedelta, timezone

    monkeypatch.setenv("PROVISA_URL", "http://127.0.0.1:1")
    spec = importlib.util.spec_from_file_location("provisioning_setup_pulls_real", _SETUP)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    base = datetime(2026, 11, 8, 0, 10, tzinfo=timezone.utc)
    monkeypatch.setattr(mod, "_now", lambda: base + timedelta(seconds=clock["now"]))
    monkeypatch.setattr(mod.time, "sleep", lambda s: clock.__setitem__("now", clock["now"] + s))
    return mod


def test_a_read_refused_for_the_window_waits_until_it_opens_then_reads(module, clock):
    server = _FakeServer(clock, opens_at=20 * 60)
    assert module._sql(server, "SELECT 1") == [{"n": 1}]
    assert server.posts == [0.0, 20 * 60.0]


def test_any_other_refusal_fails_at_once(module, clock):
    import httpx

    other = httpx.Response(
        503, json={"detail": "x", "code": "data.source_unavailable", "params": {"source": "s"}}
    )
    server = _FakeServer(clock, opens_at=10**9, refusal=other)
    with pytest.raises(SystemExit, match="data.source_unavailable"):
        module._sql(server, "SELECT 1")
    assert server.posts == [0.0] and clock["now"] == 0.0


@pytest.mark.parametrize(
    "body",
    [
        {"code": _WINDOW, "params": {}},
        {"code": _WINDOW, "params": {"opens_at": "soon"}},
        {"code": _WINDOW, "params": {"opens_at": "2026-11-08T00:30"}},
        {"code": _WINDOW},
    ],
)
def test_an_unparseable_window_refusal_is_an_error_not_a_default_wait(module, clock, body):
    import httpx

    server = _FakeServer(clock, opens_at=10**9, refusal=httpx.Response(503, json=body))
    with pytest.raises(SystemExit, match=_WINDOW):
        module._sql(server, "SELECT 1")
    assert clock["now"] == 0.0


def test_the_overall_wait_covers_the_windows_thirty_minutes(setup_pulls):
    assert setup_pulls.WAIT_S >= 30 * 60 + 900
    assert setup_pulls.WINDOW_WAIT_S >= 30 * 60
