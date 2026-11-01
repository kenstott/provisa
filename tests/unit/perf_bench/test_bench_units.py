# Copyright (c) 2026 Kenneth Stott
# Canary: b7f1e4a2-6c3d-4e59-8a17-2d9c0f3e5b61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911, a run as units: one transport, one repetition, in order, each rechecked; invalid ones
discarded with their reason, the run stopped, and resumed from where it stopped. What a person
hoped for a figure is no reason."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import bench_units as bu  # noqa: E402


def _step(c: int = 16, rps: float = 1000.0, **over: Any) -> dict[str, Any]:
    return {
        "concurrency": c,
        "req_per_s": rps,
        "p50_ms": 2.0,
        "errors": 0,
        "client_limited": False,
        "paging": {"server": {"pswpin": 0, "pswpout": 0}},
        **over,
    }


def _vm(instance: str, platform: str = "AMD Turin") -> dict[str, Any]:
    return {"instance_id": instance, "cpu_platform": platform}


def _devices(telemetry: str = "off", commit: str = "abc", server: str = "1") -> dict[str, Any]:
    return {
        "server": {**_vm(server), "facts": {"telemetry": telemetry, "commit": commit}},
        "client": _vm("2"),
        "backends": _vm("3"),
        "network": {"client->server": {"p99_ms": 0.2}},
    }


def _unit(
    root: Path,
    name: str,
    *steps: dict[str, Any],
    transport: str | None = None,
    devices: dict[str, Any] | None = None,
) -> Path:
    fields = bu.parse_unit(name)
    out = root / name / "optimistic"
    out.mkdir(parents=True)
    t = transport or fields["transport"]
    (out / f"{t}.json").write_text(json.dumps({"transport": t, "steps": list(steps) or [_step()]}))
    (out / "devices.json").write_text(json.dumps(devices or _devices(fields["telemetry"])))
    return root / name


@pytest.fixture
def ledger() -> dict[str, Any]:
    return {"units": {}, "discarded": [], "stops": [], "stages": {}, "machines": {}}


# --- the plan ----------------------------------------------------------------------------------


def test_the_baseline_runs_each_repetition_as_units_least_risky_first() -> None:
    units = bu.plan(2, transports=["bolt", "graphql"])
    assert [u.name for u in units] == [
        "1-graphql-off",
        "1-bolt-off",
        "1-uncached-off",
        "1-graphql-on",
        "1-bolt-on",
        "2-graphql-off",
        "2-bolt-off",
        "2-uncached-off",
        "2-graphql-on",
        "2-bolt-on",
    ]
    assert {u.group for u in units[:2]} == {"1-off"}
    assert units[2].transport == "pgwire" and units[2].cache == "uncached"
    assert {u.stage for u in units} == {"baseline"}


def test_a_pass_runs_its_variants_least_risky_first_whatever_order_they_are_named() -> None:
    units = bu.plan(
        1,
        transports=["graphql"],
        telemetries=["off"],
        variants=["all", "zero_end", "row_rules_2", "zero"],
        caches=["cached"],
    )
    assert [u.variant for u in units] == ["zero", "row_rules_2", "all", "zero_end"]
    assert units[0].name == "1-zero-cached-graphql-off"
    assert units[0].group == "1-zero-cached-off" and units[0].stage == "features"


def test_a_name_the_plan_does_not_know_is_refused() -> None:
    with pytest.raises(ValueError, match="not a transport: ftp"):
        bu.plan(1, transports=["ftp"])
    with pytest.raises(ValueError, match="not a variant: most"):
        bu.plan(1, variants=["most"], caches=["cached"])


def test_a_unit_line_round_trips_and_a_name_parses() -> None:
    assert bu.plan(1, transports=["rest"])[0].line().split("\t")[2:6] == [
        "1-rest-off",
        "1",
        "off",
        "rest",
    ]
    assert bu.parse_unit("2-zero-cached-flight_sql-off") == {
        "rep": "2",
        "variant": "zero",
        "cache": "cached",
        "transport": "flight_sql",
        "telemetry": "off",
    }
    assert bu.parse_unit("3-uncached-off")["cache"] == "uncached"
    with pytest.raises(ValueError, match="a unit is"):
        bu.parse_unit("nonsense")


# --- the check ---------------------------------------------------------------------------------


def test_a_clean_unit_is_valid_and_records_its_commit(tmp_path: Path, ledger: dict) -> None:
    v = bu.check_unit(_unit(tmp_path, "1-graphql-off"), ledger)
    assert v.valid and not v.reasons
    assert ledger["units"]["1-graphql-off"]["commit"] == "abc"
    assert ledger["machines"]["server"] == ["1", "AMD Turin"]


@pytest.mark.parametrize(
    ("steps", "expected"),
    [
        ([_step(errors=2)], "2 errors at its peak"),
        ([_step(client_limited=True)], "client-limited at its peak"),
        (
            [_step(paging={"server": {"pswpin": 1, "pswpout": 0}})],
            "server paged at c=16: pswpin=1",
        ),
        ([_step(paging={"client": {"error": "no /proc"}})], "paging on client was not read"),
        ([{"concurrency": 1, "error": "boom"}], "no step measured"),
    ],
)
def test_an_invalid_unit_is_not_kept(
    tmp_path: Path, ledger: dict, steps: list, expected: str
) -> None:
    v = bu.check_unit(_unit(tmp_path, "1-graphql-off", *steps), ledger)
    assert not v.valid and expected in "; ".join(v.reasons)
    assert "1-graphql-off" not in ledger["units"]


def test_a_step_past_the_peak_that_errs_is_the_servers_limit_not_a_reason(
    tmp_path: Path, ledger: dict
) -> None:
    unit = _unit(
        tmp_path,
        "1-graphql-off",
        _step(16, 1000.0),
        _step(64, 900.0, errors=4),
        _step(256, 800.0, client_limited=True),
    )
    assert bu.check_unit(unit, ledger).valid
    assert ledger["units"]["1-graphql-off"]["concurrency"] == 16


def test_a_low_figure_is_never_a_reason(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-graphql-off", _step(rps=0.5)), ledger).valid


def test_a_replaced_machine_or_a_new_platform_is_invalid(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-graphql-off"), ledger).valid
    v = bu.check_unit(_unit(tmp_path, "1-rest-off", devices=_devices(server="9")), ledger)
    assert not v.valid and "a replaced machine is a new run" in v.reasons[0]
    moved = _devices()
    moved["client"]["cpu_platform"] = "AMD Genoa"
    v = bu.check_unit(_unit(tmp_path, "1-jsonapi-off", devices=moved), ledger)
    assert not v.valid and "client ran on 2 (AMD Genoa)" in v.reasons[0]


def test_a_new_commit_on_the_same_machines_is_valid(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-graphql-off"), ledger).valid
    unit = _unit(tmp_path, "1-rest-off", devices=_devices(commit="def"))
    assert bu.check_unit(unit, ledger).valid
    assert ledger["units"]["1-rest-off"]["commit"] == "def"


def test_telemetry_and_round_trips_come_from_the_devices(tmp_path: Path, ledger: dict) -> None:
    v = bu.check_unit(_unit(tmp_path, "1-rest-on", devices=_devices("off")), ledger)
    assert not v.valid and "the server states telemetry off, not on" in v.reasons[0]
    slow = _devices()
    slow["network"]["client->server"]["p99_ms"] = 1.4
    v = bu.check_unit(_unit(tmp_path, "1-rest-off", devices=slow), ledger)
    assert not v.valid and "round trip client->server p99 1.4 ms" in v.reasons[0]


def test_spread_applies_once_a_second_repetition_exists(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-rest-off", _step(rps=1000.0)), ledger).valid
    assert bu.check_unit(_unit(tmp_path, "2-rest-off", _step(rps=1190.0)), ledger).valid
    v = bu.check_unit(_unit(tmp_path, "3-rest-off", _step(rps=800.0)), ledger)
    assert not v.valid and "spread 1.49x over 3 repetitions" in v.reasons[0]
    # another transport is its own spread
    assert bu.check_unit(_unit(tmp_path, "3-bolt-off", _step(rps=50.0)), ledger).valid
    # so are cached and uncached of one transport
    assert bu.check_unit(_unit(tmp_path, "1-pgwire-off", _step(rps=9000.0)), ledger).valid
    uncached = _unit(tmp_path, "1-uncached-off", _step(rps=500.0), transport="pgwire")
    assert bu.check_unit(uncached, ledger).valid


def test_zero_end_against_zero(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-zero-cached-rest-off", _step(rps=1000.0)), ledger).valid
    v = bu.check_unit(_unit(tmp_path, "1-zero_end-cached-rest-off", _step(rps=1200.0)), ledger)
    assert not v.valid and "zero_end against zero: 1.20x" in v.reasons[0]


def test_a_cached_read_slower_than_uncached_needs_a_note_and_keeps_the_unit(
    tmp_path: Path, ledger: dict
) -> None:
    fast, slow = _step(p50_ms=2.0), _step(p50_ms=3.0)
    assert bu.check_unit(_unit(tmp_path, "1-zero-uncached-rest-off", fast), ledger).valid
    v = bu.check_unit(_unit(tmp_path, "1-zero-cached-rest-off", slow), ledger)
    assert v.valid and v.unexplained == ["cached_slower zero rest: 3.0 ms cached, 2.0 ms uncached"]
    assert "1-zero-cached-rest-off" in ledger["units"]
    notes = [{"flag": "cached_slower zero *", "why": "a round trip to Redis"}]
    assert bu.check_unit(_unit(tmp_path, "2-zero-uncached-rest-off", fast), ledger).valid
    v = bu.check_unit(_unit(tmp_path, "2-zero-cached-rest-off", slow), ledger, notes=notes)
    assert v.valid and not v.unexplained
    assert ledger["units"]["2-zero-cached-rest-off"]["flags"][0]["why"] == "a round trip to Redis"


def test_the_pgwire_pair_is_compared_in_the_baseline(tmp_path: Path, ledger: dict) -> None:
    assert bu.check_unit(_unit(tmp_path, "1-pgwire-off", _step(p50_ms=9.0)), ledger).valid
    unit = _unit(tmp_path, "1-uncached-off", _step(p50_ms=2.0), transport="pgwire")
    assert bu.check_unit(unit, ledger).unexplained == [
        "cached_slower baseline pgwire: 9.0 ms cached, 2.0 ms uncached"
    ]


# --- the ledger and the command line -----------------------------------------------------------


def test_a_discarded_unit_is_kept_with_its_reason(tmp_path: Path, ledger: dict) -> None:
    results = tmp_path / "results"
    unit = _unit(results, "1-rest-off", _step(errors=1))
    v = bu.check_unit(unit, ledger)
    kept = bu.discard(unit, ledger, v.reasons)
    assert not unit.exists() and kept == tmp_path / "results-discarded" / "1-rest-off-1"
    assert (kept / "optimistic" / "rest.json").exists()
    assert ledger["discarded"][0]["unit"] == "1-rest-off"
    assert "1 errors" in ledger["discarded"][0]["reasons"][0]
    unit = _unit(results, "1-rest-off", _step(errors=1))
    assert bu.discard(unit, ledger, ["again"]).name == "1-rest-off-2"


def test_each_stop_records_its_fix_and_what_followed(ledger: dict) -> None:
    bu.record_stop(ledger, unit="1-rest-off", reasons=["rest: 1 errors"])
    assert [s["unit"] for s in bu.open_stops(ledger)] == ["1-rest-off"]
    bu.resolve_stop(ledger, fix="the pool size", commit="def", decision="resumed")
    assert not bu.open_stops(ledger)
    stop = ledger["stops"][0]
    assert (stop["fix"], stop["commit"], stop["decision"]) == ("the pool size", "def", "resumed")
    with pytest.raises(ValueError, match="no stop is open"):
        bu.resolve_stop(ledger, fix="x", commit="y", decision="restarted")
    bu.record_stop(ledger, unit="2-rest-off", reasons=["again"])
    with pytest.raises(ValueError, match="decision must be one of"):
        bu.resolve_stop(ledger, fix="x", commit="y", decision="ignored")


def test_the_report_block_gives_every_commit_only_when_there_are_several() -> None:
    assert bu.commit_block({"a": "1", "b": "1"}) == {"commits_by_run": {"a": "1", "b": "1"}}
    block = bu.commit_block({"a": "1", "b": "2"})
    assert block["commits"] == ["1", "2"] and block["commit_caveat"] == bu.COMMIT_CAVEAT


def _cli(*args: str) -> tuple[int, str]:
    import subprocess

    done = subprocess.run(
        [sys.executable, str(BENCH / "bench_units.py"), *args], capture_output=True, text=True
    )
    return done.returncode, done.stdout


def test_the_check_exit_codes(tmp_path: Path) -> None:
    results = tmp_path / "results"
    ledger = tmp_path / "ledger.json"
    notes = tmp_path / "notes.yaml"
    notes.write_text("notes: []\n")
    common = ("--ledger", str(ledger), "--results", str(results), "--notes", str(notes))
    _unit(results, "1-zero-uncached-rest-off", _step(p50_ms=2.0))
    assert _cli("check", *common, "--unit", "1-zero-uncached-rest-off")[0] == 0
    _unit(results, "1-zero-cached-rest-off", _step(p50_ms=3.0))
    code, out = _cli("check", *common, "--unit", "1-zero-cached-rest-off")
    assert code == bu.FLAGGED and "no explanation" in out
    _unit(results, "1-zero-cached-bolt-off", _step(errors=1))
    code, out = _cli("check", *common, "--unit", "1-zero-cached-bolt-off")
    assert code == bu.INVALID and "invalid, discarded" in out
    written = json.loads(ledger.read_text())
    assert len(written["stops"]) == 2 and written["discarded"][0]["unit"].endswith("bolt-off")
    assert not (results / "1-zero-cached-bolt-off").exists()


def test_the_features_do_not_start_before_the_baseline_passed(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.json"
    code, out = _cli("gate", "--ledger", str(ledger))
    assert code == 1 and "the baseline has not passed" in out
    assert _cli("stage-passed", "--ledger", str(ledger), "--stage", "baseline")[0] == 0
    assert _cli("gate", "--ledger", str(ledger))[0] == 0


def test_the_plan_prints_one_line_per_unit() -> None:
    code, out = _cli("plan", "--reps", "1", "--transports", "rest")
    assert code == 0
    assert [line.split("\t")[2] for line in out.splitlines()] == [
        "1-rest-off",
        "1-uncached-off",
        "1-rest-on",
    ]
