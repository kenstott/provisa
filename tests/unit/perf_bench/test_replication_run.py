# Copyright (c) 2026 Kenneth Stott
# Canary: 9f425d63-918b-4e54-a7f1-b70da8a75473
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: a run is measured under the declared replication, checks the route that served it, and
the same workload can be run live and from replicas and diffed.

FIXTURE: the deployment, the admin API, pgwire and the audit log are fakes."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench_compare as bc  # noqa: E402
import contract_model  # noqa: E402
import deployment_fixture as fx  # noqa: E402
import lookup  # noqa: E402
import optimistic_load as ol  # noqa: E402
import replication as rp  # noqa: E402
import request_mix  # noqa: E402
import setup_contract as sc  # noqa: E402

PERF_CONTRACT = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}
TRANSPORTS = list(ol.TRANSPORTS)


def _raw() -> dict[str, Any]:
    return json.loads(json.dumps(yaml.safe_load(PERF_CONTRACT.read_text())))


def _contract(tmp_path: Path, mutate: Any = None) -> Path:
    raw = _raw()
    if mutate:
        mutate(raw)
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


def _names(tmp_path: Path, path: Path) -> Path:
    unbound = sc.load_setup(path, environ=ENV, known_transports=TRANSPORTS)
    out = tmp_path / "given.json"
    lookup.write_resolved(lookup.resolve(*sc.identities(unbound), fx.build()), out)
    return out


def _args(path: Path, names: Path, **kw: Any) -> SimpleNamespace:
    base = {
        "setup": str(path),
        "server_pid": None,
        "resolved_names": str(names),
        "apply_replication": False,
        "skip_route_verification": True,
    }
    return SimpleNamespace(**(base | kw))


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROVISA_HTTP_BASE_URL", "http://localhost:8001")


# ------------------------------------------------------------------ the CLI flags


def test_the_flags_exist_and_default_off() -> None:
    import argparse

    p = argparse.ArgumentParser()
    ol.add_arguments(p)
    a = p.parse_args([])
    assert a.apply_replication is False and a.skip_route_verification is False
    assert (
        p.parse_args(["--apply-replication", "--skip-route-verification"]).apply_replication is True
    )


# ------------------------------------------------------------------ declared vs the deployment


def test_a_deployment_that_is_not_as_declared_is_refused_without_the_flag(tmp_path: Path) -> None:
    path = _contract(
        tmp_path,
        lambda r: r["sources"]["bench-postgresql"].update(
            replication={"setting": "replica", "ttl_seconds": 300}
        ),
    )
    with pytest.raises(contract_model.SetupError, match="--apply-replication"):
        ol.run_from_args(_args(path, _names(tmp_path, path)), tmp_path / "out", argv=["x"])


def test_a_matching_deployment_runs_without_touching_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _contract(tmp_path)
    touched: list[Any] = []
    monkeypatch.setattr(rp, "AdminReplication", lambda *a, **k: touched.append(a))
    ran: list[Any] = []
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: ran.append((a, k)) or [])
    ol.run_from_args(_args(path, _names(tmp_path, path)), tmp_path / "out", argv=["x"])
    assert ran and not touched


def test_the_flag_sets_the_declared_setting_for_the_run_and_restores_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _contract(
        tmp_path,
        lambda r: r["sources"]["bench-postgresql"].update(
            replication={"setting": "replica", "ttl_seconds": 300}
        ),
    )
    log: list[str] = []

    class Admin:
        def __init__(self, client: Any, role: str) -> None:
            log.append(f"admin as {role}")

        @contextmanager
        def applied(self, setup: contract_model.Setup, resolved: lookup.Resolved) -> Any:
            log.append("set")
            try:
                yield
            finally:
                log.append("restored")

    monkeypatch.setattr(rp, "AdminReplication", Admin)
    monkeypatch.setattr(ol, "run_all", lambda *a, **k: log.append("run") or [])
    ol.run_from_args(
        _args(path, _names(tmp_path, path), apply_replication=True), tmp_path / "out", argv=["x"]
    )
    assert log == ["admin as org_admin", "set", "run", "restored"]


def test_the_setting_is_restored_when_the_run_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _contract(
        tmp_path,
        lambda r: r["sources"]["bench-postgresql"].update(
            replication={"setting": "replica", "ttl_seconds": 300}
        ),
    )
    log: list[str] = []

    class Admin:
        def __init__(self, client: Any, role: str) -> None:
            pass

        @contextmanager
        def applied(self, setup: contract_model.Setup, resolved: lookup.Resolved) -> Any:
            try:
                yield
            finally:
                log.append("restored")

    def boom(*a: Any, **k: Any) -> Any:
        raise rp.RouteMismatch("orders: declared replica, expected route engine, served direct x1")

    monkeypatch.setattr(rp, "AdminReplication", Admin)
    monkeypatch.setattr(ol, "run_all", boom)
    with pytest.raises(rp.RouteMismatch):
        ol.run_from_args(
            _args(path, _names(tmp_path, path), apply_replication=True),
            tmp_path / "out",
            argv=["x"],
        )
    assert log == ["restored"]


# ------------------------------------------------------------------ route verification in a run


class FakeAudit:
    def __init__(self, route: str) -> None:
        self.route = route
        self.sent = 0

    def baseline(self) -> int:
        return 0

    def since(self, baseline: int) -> list[tuple[str, str, str]]:
        return [("orders", self.route, "sql")] * self.sent


def _setup(tmp_path: Path) -> contract_model.Setup:
    return sc.load_setup(
        PERF_CONTRACT, environ=ENV, known_transports=TRANSPORTS, deployment=fx.build()
    )


def _spec() -> request_mix.RequestSpec:
    return request_mix.RequestSpec("bench-postgresql", "orders", ("order_id",), 1, False)


def _run_transport(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, audit: Any) -> dict[str, Any]:
    setup = _setup(tmp_path)

    def run_step(transport: str, ep: Any, c: int, *rest: Any) -> dict[str, Any]:
        if audit is not None:
            audit.sent = 3
        return {"concurrency": c, "route_classes": rp.classes_of(ep.setup, [_spec()] * 3)}

    monkeypatch.setattr(
        ol, "verify", lambda t, ep: {"rows": 1, "columns": 1, "second_request_hit": None}
    )
    monkeypatch.setattr(ol, "run_step", run_step)
    return ol.run_transport(
        "graphql", ol.endpoints_from_setup(setup), (1, 16), 1.0, 1, None, audit=audit
    )


def test_every_step_is_verified_against_the_audit_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    report = _run_transport(monkeypatch, tmp_path, FakeAudit("direct"))
    assert [s["route_verification"]["verified"] for s in report["steps"]] == [3, 3]
    assert report["route_verification"] == "verified"


def test_a_step_served_by_the_wrong_route_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(
        rp.RouteMismatch, match="orders: declared live, expected route direct, served engine x3"
    ):
        _run_transport(monkeypatch, tmp_path, FakeAudit("engine"))


def test_skipping_verification_is_stated_in_the_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    report = _run_transport(monkeypatch, tmp_path, None)
    assert report["route_verification"] == "skipped (--skip-route-verification)"
    assert all(s["route_verification"] is None for s in report["steps"])


def test_the_audit_reader_is_built_from_the_contract_domains(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _contract(tmp_path)
    got: dict[str, Any] = {}
    import psycopg

    monkeypatch.setattr(
        psycopg, "connect", lambda **kw: got.setdefault("connect", kw) or SimpleNamespace()
    )
    monkeypatch.setattr(
        rp,
        "audit_reader",
        lambda conn, domains: got.setdefault("domains", list(domains)) or FakeAudit("direct"),
    )
    monkeypatch.setattr(
        ol, "run_all", lambda *a, **k: got.setdefault("audit", k.get("audit")) or []
    )
    ol.run_from_args(
        _args(path, _names(tmp_path, path), skip_route_verification=False),
        tmp_path / "out",
        argv=["x"],
    )
    assert (
        got["domains"] == ["perf-bench"]
        and got["connect"]["user"] == "org_admin"
        and got["connect"]["port"] == 5439
    )
    assert got["audit"] is not None


# ------------------------------------------------------------------ run live and from replicas, and diff


def test_the_same_contract_with_every_source_under_one_setting() -> None:
    live = bc.under_setting(_raw(), "live", ttl_seconds=0)
    replica = bc.under_setting(_raw(), "replica", ttl_seconds=120)
    for contract, want in ((live, ("live", 0)), (replica, ("replica", 120))):
        assert {
            (s["replication"]["setting"], s["replication"]["ttl_seconds"])
            for s in contract["sources"].values()
        } == {want}
    assert replica["knobs"] == _raw()["knobs"]  # nothing else changes


def test_a_table_level_override_is_dropped_by_the_comparison() -> None:
    raw = _raw()
    raw["sources"]["bench-postgresql"]["tables"][1]["replication"] = {
        "setting": "replica",
        "ttl_seconds": 60,
    }
    contract = bc.under_setting(raw, "live", ttl_seconds=0)
    assert all("replication" not in t for s in contract["sources"].values() for t in s["tables"])


def _write_run(
    root: Path,
    transport: str,
    *,
    req_per_s: float,
    p50: float,
    server: float,
    source: dict[str, float],
    refreshes: int | None,
) -> None:
    out = root
    out.mkdir(parents=True, exist_ok=True)
    step = {
        "concurrency": 1,
        "req_per_s": req_per_s,
        "p50_ms": p50,
        "p99_ms": p50 * 3,
        "server_cpu_ms_per_request": server,
        "source_cpu_ms_per_request": source,
        "route_verification": {
            "verified": 100,
            "unverified": 0,
            "cache_served": 0,
            "mismatches": [],
        },
    }
    (out / f"{transport}.json").write_text(
        json.dumps(
            {
                "transport": transport,
                "replication": {"bench-clickhouse": {"replication": "replica"}},
                "steps": [step],
                "refreshes": refreshes,
            }
        )
    )


def test_compare_diffs_the_two_runs_step_by_step(tmp_path: Path) -> None:
    _write_run(
        tmp_path / "live",
        "graphql",
        req_per_s=100,
        p50=10.0,
        server=2.0,
        source={"bench-clickhouse": 3.0},
        refreshes=None,
    )
    _write_run(
        tmp_path / "replica",
        "graphql",
        req_per_s=400,
        p50=2.5,
        server=1.0,
        source={"bench-clickhouse": 0.5},
        refreshes=4,
    )
    out = bc.compare_dirs(tmp_path / "live", tmp_path / "replica")
    (row,) = out["transports"]["graphql"]["steps"]
    assert row["concurrency"] == 1
    assert row["req_per_s"] == {"live": 100, "replica": 400, "ratio": 4.0}
    assert row["p50_ms"] == {"live": 10.0, "replica": 2.5, "ratio": 0.25}
    assert row["provisa_cpu_ms_per_request"] == {"live": 2.0, "replica": 1.0, "ratio": 0.5}
    assert row["source_cpu_ms_per_request"]["bench-clickhouse"] == {
        "live": 3.0,
        "replica": 0.5,
        "ratio": pytest.approx(0.5 / 3.0),
    }
    assert out["label"] == "internal: not for publication"


def test_compare_refuses_runs_whose_routes_were_not_verified(tmp_path: Path) -> None:
    _write_run(
        tmp_path / "live", "graphql", req_per_s=100, p50=10.0, server=2.0, source={}, refreshes=None
    )
    _write_run(
        tmp_path / "replica",
        "graphql",
        req_per_s=400,
        p50=2.5,
        server=1.0,
        source={},
        refreshes=None,
    )
    path = tmp_path / "replica" / "graphql.json"
    data = json.loads(path.read_text())
    data["steps"][0]["route_verification"] = None
    path.write_text(json.dumps(data))
    with pytest.raises(
        ValueError, match="graphql step c=1 of the replica run has no verified routes"
    ):
        bc.compare_dirs(tmp_path / "live", tmp_path / "replica")


def test_compare_refuses_runs_that_differ_in_steps(tmp_path: Path) -> None:
    _write_run(
        tmp_path / "live", "graphql", req_per_s=100, p50=10.0, server=2.0, source={}, refreshes=None
    )
    _write_run(
        tmp_path / "replica",
        "graphql",
        req_per_s=400,
        p50=2.5,
        server=1.0,
        source={},
        refreshes=None,
    )
    path = tmp_path / "replica" / "graphql.json"
    data = json.loads(path.read_text())
    data["steps"][0]["concurrency"] = 16
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="graphql: the runs have different steps"):
        bc.compare_dirs(tmp_path / "live", tmp_path / "replica")


def test_the_compare_command_runs_the_contract_twice_and_diffs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runs: list[tuple[str, str, bool]] = []

    def fake_run(args: Any, output_dir: Path, argv: Any = None) -> list[dict]:
        setting = yaml.safe_load(Path(args.setup).read_text())["sources"]["bench-clickhouse"][
            "replication"
        ]["setting"]
        runs.append((setting, str(output_dir), args.apply_replication))
        replica = setting == "replica"
        out = output_dir
        out.mkdir(parents=True, exist_ok=True)
        step = {
            "concurrency": 1,
            "req_per_s": 400 if replica else 100,
            "p50_ms": 1.0,
            "p99_ms": 2.0,
            "server_cpu_ms_per_request": 1.0,
            "source_cpu_ms_per_request": {},
            "route_verification": {"verified": 5},
        }
        (out / "graphql.json").write_text(json.dumps({"transport": "graphql", "steps": [step]}))
        return []

    monkeypatch.setattr(ol, "run_from_args", fake_run)
    path = _contract(tmp_path)
    out = bc.compare(
        SimpleNamespace(
            setup=str(path),
            server_pid=None,
            resolved_names=str(_names(tmp_path, path)),
            skip_route_verification=False,
        ),
        tmp_path / "cmp",
        replica_ttl_s=120,
    )
    assert [(s, a) for s, _d, a in runs] == [
        ("live", True),
        ("replica", True),
    ]  # both applied, then restored
    assert out["transports"]["graphql"]["steps"][0]["req_per_s"]["ratio"] == 4.0
    assert (tmp_path / "cmp" / "compare.json").exists()
