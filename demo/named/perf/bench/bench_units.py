# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f3c1e-7a42-4b6f-9e21-3c8a6f1d2b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The unit of a benchmark run, its order, the check after each one, and the run's ledger
(REQ-1911, METHODOLOGY.md).

A unit is one transport, one repetition. ``plan`` gives the units in order, least risky first.
``check`` runs after each unit on what came back from the devices. A unit that fails a validity
rule is discarded, with its reason, and the run stops; a sanity flag with no explanation stops the
run and keeps the unit. Neither looks at whether a figure is good. The ledger holds every unit's
verdict and commit, every discarded unit and every stop with its fix and whether it resumed or
restarted, so a stopped run resumes from where it stopped and the report can say what happened.

    python bench_units.py plan --reps 5
    python bench_units.py check --ledger run-ledger.json --results results --unit 1-graphql-off
    python bench_units.py stage-passed --ledger run-ledger.json --stage baseline
    python bench_units.py gate --ledger baseline-ledger.json
    python bench_units.py stop --ledger run-ledger.json --unit 1-graphql-off --reason "ssh failed"
    python bench_units.py resolve --ledger run-ledger.json --fix "..." --commit SHA \\
        --decision resumed
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import fnmatch
import json
import os
import shutil
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

import bench_sanity

# The least risky first: stateless HTTP surfaces, then what holds a long-lived session.
TRANSPORTS = (
    "graphql",
    "rest",
    "jsonapi",
    "data_sql",
    "cypher_http",
    "sparql",
    "pgwire",
    "flight_sql",
    "grpc",
    "bolt",
)
# zero first and zero_end last, for the drift between them. Between them the simplest knob first:
# the last job's smoke finished the first three and timed out on the others, which change the
# most per transport.
FEATURES = (
    "zero",
    "redaction",
    "derived_roles_2",
    "masked_quarter",
    "row_rules_2",
    "role_depth_2",
    "all",
    "zero_end",
)
PROFILED = ("pgwire", "flight_sql", "graphql", "bolt", "grpc", "rest", "jsonapi")
BASELINE, FEATURES_STAGE, PROFILE = "baseline", "features", "profile"
UNCACHED = "uncached"
SPREAD_LIMIT = 1.2
ZERO_DRIFT_LIMIT = 1.1
RTT_P99_LIMIT_MS = 1.0
DEVICES = ("server", "client", "backends")
DECISIONS = ("resumed", "restarted")
DEFAULT_NOTES = Path(__file__).parent / "sanity_notes.yaml"
REPORT_FILES = ("devices.json", "idle.json", "resolved-names.json", "lookup-responses.json")
# Exit codes of ``check``, which three_device_run.sh acts on.
INVALID, FLAGGED = 3, 4


@dataclasses.dataclass(frozen=True)
class Unit:
    stage: str
    group: str  # the units one server start serves
    name: str
    rep: int
    telemetry: str
    transport: str
    variant: str = "-"
    cache: str = "-"

    def line(self) -> str:
        return "\t".join(str(v) for v in dataclasses.astuple(self))


def plan(
    reps: int,
    *,
    transports: Sequence[str] = TRANSPORTS,
    telemetries: Sequence[str] = ("off", "on"),
    variants: Sequence[str] = (),
    caches: Sequence[str] = (),
    profile: bool = False,
) -> list[Unit]:
    """Every unit of a pass, in the order it runs. Telemetry settings alternate within a
    repetition, so drift falls on both alike. A governance pass runs its variants least risky
    first, whatever order they were named in."""
    unknown = sorted(set(transports) - set(TRANSPORTS))
    if unknown:
        raise ValueError(f"not a transport: {', '.join(unknown)}")
    wanted = [t for t in TRANSPORTS if t in transports]
    units: list[Unit] = []
    if variants:
        missing = sorted(set(variants) - set(FEATURES))
        if missing:
            raise ValueError(f"not a variant: {', '.join(missing)}")
        ordered = [v for v in FEATURES if v in variants]
        for rep in range(1, reps + 1):
            for variant in ordered:
                for cache in caches:
                    for telemetry in telemetries:
                        group = f"{rep}-{variant}-{cache}-{telemetry}"
                        units += [
                            Unit(
                                FEATURES_STAGE,
                                group,
                                f"{rep}-{variant}-{cache}-{t}-{telemetry}",
                                rep,
                                telemetry,
                                t,
                                variant,
                                cache,
                            )
                            for t in wanted
                        ]
        return units
    for rep in range(1, reps + 1):
        for telemetry in telemetries:
            group = f"{rep}-{telemetry}"
            units += [
                Unit(BASELINE, group, f"{rep}-{t}-{telemetry}", rep, telemetry, t) for t in wanted
            ]
            if telemetry == "off":
                name = f"{rep}-{UNCACHED}-off"
                units.append(Unit(BASELINE, name, name, rep, "off", "pgwire", cache=UNCACHED))
    if profile:
        for t in PROFILED:
            name = f"profile-{t}-off"
            units.append(Unit(PROFILE, name, name, 0, "off", t))
    return units


def parse_unit(name: str) -> dict[str, str]:
    """What a unit's name says: a repetition with its transport and telemetry, a governance pass's
    also with its variant and cache."""
    parts = name.split("-")
    if len(parts) == 5:
        return dict(zip(("rep", "variant", "cache", "transport", "telemetry"), parts))
    if len(parts) == 3:
        rep, transport, telemetry = parts
        if transport == UNCACHED:
            return {"rep": rep, "transport": "pgwire", "telemetry": telemetry, "cache": UNCACHED}
        return {"rep": rep, "transport": transport, "telemetry": telemetry}
    raise ValueError(f"{name}: a unit is <rep>-<transport>-<telemetry>, or a pass's")


# --- the ledger --------------------------------------------------------------------------------


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_ledger(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {"units": {}, "discarded": [], "stops": [], "stages": {}, "machines": {}}


def save_ledger(path: Path, ledger: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(ledger, indent=2))
    os.replace(temporary, path)


def record_stop(ledger: dict[str, Any], *, unit: str, reasons: Sequence[str]) -> None:
    """A stop, open until a person records its fix and whether the run resumed or restarted."""
    ledger["stops"].append(
        {
            "at": _now(),
            "unit": unit,
            "reasons": list(reasons),
            "fix": None,
            "commit": None,
            "decision": None,
        }
    )


def resolve_stop(ledger: dict[str, Any], *, fix: str, commit: str, decision: str) -> None:
    """The open stop's fix, the commit it left the run on, and resumed or restarted."""
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {', '.join(DECISIONS)}")
    open_stops = [s for s in ledger["stops"] if s["decision"] is None]
    if not open_stops:
        raise ValueError("no stop is open in this ledger")
    open_stops[-1].update(fix=fix, commit=commit, decision=decision, resolved_at=_now())


def open_stops(ledger: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [s for s in ledger["stops"] if s["decision"] is None]


# --- the check after a unit --------------------------------------------------------------------


def _reports(unit_dir: Path) -> list[dict[str, Any]]:
    out = []
    for path in sorted((unit_dir / "optimistic").glob("*.json")):
        if path.name in REPORT_FILES:
            continue
        report = json.loads(path.read_text())
        if "transport" in report:
            out.append(report)
    return out


def _clean(step: Mapping[str, Any]) -> bool:
    return "error" not in step and step["errors"] == 0 and not step["client_limited"]


def published_step(report: Mapping[str, Any]) -> dict[str, Any] | None:
    """The step with the highest throughput that had no errors and was not client-limited."""
    if "error" in report:
        return None
    clean = [s for s in report["steps"] if _clean(s)]
    return max(clean, key=lambda s: s["req_per_s"]) if clean else None


def step_reasons(report: Mapping[str, Any]) -> list[str]:
    """Why a transport's report is invalid: it measured nothing clean, or its ceiling is hidden
    behind errors or the client. A step past the ceiling that errs, or that the client limits, is
    the server's limit showing, and is not a reason."""
    name = report.get("transport", "?")
    if "error" in report:
        return [f"{name}: {report['error']}"]
    measured = [s for s in report.get("steps", []) if "error" not in s]
    if not measured:
        return [f"{name}: no step measured"]
    reasons = []
    peak = max(measured, key=lambda s: s["req_per_s"])
    if peak["errors"]:
        reasons.append(f"{name}: {peak['errors']} errors at its peak, c={peak['concurrency']}")
    if peak["client_limited"]:
        reasons.append(f"{name}: client-limited at its peak, c={peak['concurrency']}")
    point = published_step(report)
    if point is None:
        return reasons or [f"{name}: no clean step"]
    for host, paging in sorted(point["paging"].items()):
        if "error" in paging:
            reasons.append(f"{name}: paging on {host} was not read: {paging['error']}")
        elif paging["pswpin"] or paging["pswpout"]:
            reasons.append(
                f"{name}: {host} paged at c={point['concurrency']}: "
                f"pswpin={paging['pswpin']} pswpout={paging['pswpout']}"
            )
    return reasons


def device_reasons(devices: Mapping[str, Any], telemetry: str) -> list[str]:
    """The unit's own facts: what its server stated, and the round trips."""
    reasons = []
    facts = (devices.get("server") or {}).get("facts") or {}
    if facts.get("telemetry") != telemetry:
        reasons.append(f"the server states telemetry {facts.get('telemetry')}, not {telemetry}")
    if not facts.get("commit"):
        reasons.append("the server states no commit")
    for path, rtt in sorted((devices.get("network") or {}).items()):
        if rtt.get("p99_ms", 0) >= RTT_P99_LIMIT_MS:
            reasons.append(f"round trip {path} p99 {rtt['p99_ms']} ms, limit {RTT_P99_LIMIT_MS}")
    return reasons


def machine_reasons(devices: Mapping[str, Any], machines: dict[str, Any]) -> list[str]:
    """The machines against the run's first unit: instance and CPU platform, each device. The first
    unit names them. A replaced machine is a new run."""
    reasons = []
    for name in DEVICES:
        try:
            identity = list(bench_sanity.vm_identity(devices.get(name) or {}, label=name))
        except bench_sanity.CrossVmComparison as exc:
            reasons.append(str(exc))
            continue
        known = machines.setdefault(name, identity)
        if known != identity:
            reasons.append(
                f"{name} ran on {identity[0]} ({identity[1]}), the run on {known[0]} ({known[1]}): "
                "a replaced machine is a new run"
            )
    return reasons


def _peers(ledger: Mapping[str, Any], fields: Mapping[str, str]) -> list[Mapping[str, Any]]:
    """The valid units that measured the same thing in other repetitions."""
    same = ("transport", "telemetry", "variant", "cache")
    return [
        u
        for name, u in ledger["units"].items()
        if all(parse_unit(name).get(k) == fields.get(k) for k in same)
    ]


def spread_reason(figures: Iterable[float], latest: float, label: str) -> list[str]:
    values = [*figures, latest]
    if len(values) < 2 or not min(values):
        return []
    ratio = max(values) / min(values)
    if ratio <= SPREAD_LIMIT:
        return []
    return [f"{label}: spread {ratio:.2f}x over {len(values)} repetitions, limit {SPREAD_LIMIT}x"]


def drift_reason(ledger: Mapping[str, Any], fields: Mapping[str, str], rps: float) -> list[str]:
    """zero_end against zero: the same transport, cache setting and repetition."""
    if fields.get("variant") != "zero_end":
        return []
    name = f"{fields['rep']}-zero-{fields['cache']}-{fields['transport']}-{fields['telemetry']}"
    zero = ledger["units"].get(name)
    if zero is None:
        return []
    ratio = max(rps, zero["req_per_s"]) / min(rps, zero["req_per_s"])
    if ratio <= ZERO_DRIFT_LIMIT:
        return []
    return [f"zero_end against zero: {ratio:.2f}x, limit {ZERO_DRIFT_LIMIT}x"]


def _low_step(report: Mapping[str, Any]) -> Mapping[str, Any]:
    return min((s for s in report["steps"] if "error" not in s), key=lambda s: s["concurrency"])


def sanity_flags(ledger: Mapping[str, Any], fields: Mapping[str, str], p50: float) -> list[str]:
    """A cached read slower than the same read uncached, at the lowest concurrency, once both of a
    repetition exist. Each flag is the cell a note in sanity_notes.yaml explains."""
    cache = fields.get("cache", "-")
    rep = fields["rep"]
    if "variant" in fields:
        partner = "uncached" if cache == "cached" else "cached"
        other = ledger["units"].get(
            f"{rep}-{fields['variant']}-{partner}-{fields['transport']}-{fields['telemetry']}"
        )
        cell = f"{fields['variant']} {fields['transport']}"
        cached_ms, uncached_ms = (p50, None) if cache == "cached" else (None, p50)
    elif fields["transport"] == "pgwire" and fields["telemetry"] == "off":
        partner_name = f"{rep}-uncached-off" if cache != UNCACHED else f"{rep}-pgwire-off"
        other = ledger["units"].get(partner_name)
        cell = "baseline pgwire"
        cached_ms, uncached_ms = (None, p50) if cache == UNCACHED else (p50, None)
    else:
        return []
    if other is None:
        return []
    cached_ms = cached_ms if cached_ms is not None else other["p50_ms_low"]
    uncached_ms = uncached_ms if uncached_ms is not None else other["p50_ms_low"]
    if cached_ms <= uncached_ms * bench_sanity.CACHED_TOLERANCE:
        return []
    return [f"cached_slower {cell}: {cached_ms} ms cached, {uncached_ms} ms uncached"]


def load_notes(path: Path) -> list[Mapping[str, str]]:
    if not path.exists():
        return []
    return yaml.safe_load(path.read_text())["notes"]


def explanation(flag: str, notes: Sequence[Mapping[str, str]]) -> str | None:
    """The note that explains a flag: its ``flag`` pattern, a glob, matches the flag's text."""
    for note in notes:
        if fnmatch.fnmatch(flag, note["flag"]):
            return note["why"]
    return None


@dataclasses.dataclass
class Verdict:
    valid: bool
    reasons: list[str]
    unexplained: list[str]


def check_unit(
    unit_dir: Path, ledger: dict[str, Any], *, notes: Sequence[Mapping[str, str]] = ()
) -> Verdict:
    """The recheck of one finished unit. A valid unit goes in the ledger with its commit; an
    invalid one does not. Nothing here compares a figure with what was hoped for."""
    name = unit_dir.name
    fields = parse_unit(name)
    devices_path = unit_dir / "optimistic" / "devices.json"
    if not devices_path.exists():
        return Verdict(False, [f"{name}: no devices.json came back"], [])
    devices = json.loads(devices_path.read_text())
    reports = _reports(unit_dir)
    if len(reports) != 1:
        return Verdict(False, [f"{name}: {len(reports)} transport reports, one expected"], [])
    report = reports[0]
    reasons = device_reasons(devices, fields["telemetry"])
    reasons += machine_reasons(devices, ledger["machines"])
    reasons += step_reasons(report)
    point = published_step(report)
    flags: list[str] = []
    if point is not None:
        rps = point["req_per_s"]
        peers = _peers(ledger, fields)
        reasons += spread_reason((p["req_per_s"] for p in peers), rps, name)
        reasons += drift_reason(ledger, fields, rps)
    if reasons:
        return Verdict(False, reasons, [])
    assert point is not None  # no clean step is a reason, returned above
    low = _low_step(report)
    flags = sanity_flags(ledger, fields, low["p50_ms"])
    explained = {f: explanation(f, notes) for f in flags}
    ledger["units"][name] = {
        "commit": devices["server"]["facts"]["commit"],
        "checked_at": _now(),
        "transport": report["transport"],
        "req_per_s": point["req_per_s"],
        "concurrency": point["concurrency"],
        "p50_ms_low": low["p50_ms"],
        "flags": [{"flag": f, "why": w} for f, w in explained.items()],
    }
    return Verdict(True, [], [f for f, w in explained.items() if w is None])


def discard(unit_dir: Path, ledger: dict[str, Any], reasons: Sequence[str]) -> Path:
    """Move an invalid unit aside, keeping it, and record why."""
    aside = unit_dir.parent.parent / f"{unit_dir.parent.name}-discarded"
    aside.mkdir(parents=True, exist_ok=True)
    n = sum(1 for d in ledger["discarded"] if d["unit"] == unit_dir.name) + 1
    target = aside / f"{unit_dir.name}-{n}"
    shutil.move(str(unit_dir), str(target))
    ledger["discarded"].append(
        {"unit": unit_dir.name, "at": _now(), "reasons": list(reasons), "kept_at": str(target)}
    )
    return target


# --- a run's commits in the report -------------------------------------------------------------

COMMIT_CAVEAT = (
    "Measured across more than one commit. The differences between these commits were not judged "
    "consequential to the results."
)


def commit_block(commits_by_run: Mapping[str, str | None]) -> dict[str, Any]:
    """The report's statement of what was measured: the commit of each run, and, when they are not
    one commit, the caveat and the commits."""
    commits = sorted({c for c in commits_by_run.values() if c})
    block: dict[str, Any] = {"commits_by_run": dict(sorted(commits_by_run.items()))}
    if len(commits) > 1:
        block["commit_caveat"] = COMMIT_CAVEAT
        block["commits"] = commits
    return block


def stops_block(ledger: Mapping[str, Any] | None) -> dict[str, Any]:
    """What stopped the run and what was done, for the report."""
    if ledger is None:
        return {}
    return {"stops": ledger["stops"], "discarded": ledger["discarded"]}


# --- the command line --------------------------------------------------------------------------


def _cmd_check(args: argparse.Namespace) -> int:
    path = Path(args.ledger)
    ledger = load_ledger(path)
    unit_dir = Path(args.results) / args.unit
    verdict = check_unit(unit_dir, ledger, notes=load_notes(Path(args.notes)))
    if not verdict.valid:
        kept = discard(unit_dir, ledger, verdict.reasons)
        record_stop(ledger, unit=args.unit, reasons=verdict.reasons)
        save_ledger(path, ledger)
        print(f"{args.unit}: invalid, discarded to {kept}")
        for reason in verdict.reasons:
            print(f"  {reason}")
        return INVALID
    if verdict.unexplained:
        record_stop(ledger, unit=args.unit, reasons=verdict.unexplained)
        save_ledger(path, ledger)
        print(f"{args.unit}: valid, but the run stops for a flag with no explanation")
        for flag in verdict.unexplained:
            print(f"  {flag}")
        return FLAGGED
    save_ledger(path, ledger)
    print(f"{args.unit}: valid at {ledger['units'][args.unit]['commit']}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="A benchmark run's units, checks and ledger")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan", help="every unit in order, tab-separated")
    p.add_argument("--reps", type=int, required=True)
    p.add_argument("--transports")
    p.add_argument("--telemetry", default="off,on")
    p.add_argument("--variants")
    p.add_argument("--caches")
    p.add_argument("--profile", action="store_true")
    c = sub.add_parser("check", help="the recheck after one unit")
    c.add_argument("--ledger", required=True)
    c.add_argument("--results", required=True)
    c.add_argument("--unit", required=True)
    c.add_argument("--notes", default=str(DEFAULT_NOTES))
    s = sub.add_parser("stage-passed", help="every unit of a stage is valid")
    s.add_argument("--ledger", required=True)
    s.add_argument("--stage", required=True)
    g = sub.add_parser("gate", help="refuses unless the baseline passed and no stop is open")
    g.add_argument("--ledger", required=True)
    t = sub.add_parser("stop", help="a stop that no check raised: a failed step, an operator's")
    t.add_argument("--ledger", required=True)
    t.add_argument("--unit", required=True)
    t.add_argument("--reason", required=True)
    r = sub.add_parser("resolve", help="the open stop's fix, and resumed or restarted")
    r.add_argument("--ledger", required=True)
    r.add_argument("--fix", required=True)
    r.add_argument("--commit", required=True)
    r.add_argument("--decision", choices=DECISIONS, required=True)
    args = parser.parse_args()
    if args.command == "plan":
        units = plan(
            args.reps,
            transports=args.transports.split(",") if args.transports else TRANSPORTS,
            telemetries=args.telemetry.split(","),
            variants=args.variants.split(",") if args.variants else (),
            caches=args.caches.split(",") if args.caches else (),
            profile=args.profile,
        )
        for unit in units:
            print(unit.line())
        return 0
    if args.command == "check":
        return _cmd_check(args)
    path = Path(args.ledger)
    ledger = load_ledger(path)
    if args.command == "gate":
        if ledger["stages"].get(BASELINE) != "passed":
            print(f"{path}: the baseline has not passed, so the features do not start")
            return 1
        return 0
    if args.command == "stage-passed":
        ledger["stages"][args.stage] = "passed"
    elif args.command == "stop":
        record_stop(ledger, unit=args.unit, reasons=[args.reason])
    else:
        resolve_stop(ledger, fix=args.fix, commit=args.commit, decision=args.decision)
    save_ledger(path, ledger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
