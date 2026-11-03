# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e5a17-6b2f-4d90-a1e4-82f7c0d95b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The legs of Conformance (functions), one per engine kind that runs in a container a hosted runner
can boot (REQ-2167, REQ-2171): read from function_legs.yaml, which also says why each other engine
kind is not run.

``plan`` writes the workflow's matrix for the lanes a dispatch names (free, warehouse or all) and
the engines its merge expects, with no URL in it: the runner masks a URL's credentials and drops a
job output holding a masked value, so a leg's URLs reach its job by ``urls``, from this file.
``wait`` and ``setup`` make an engine's server ready over its engine URL.
``failure`` writes why a leg left no result, which the merge reports beside it: a leg whose engine
did not start is a failure with its reason, never a leg skipped.
"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import yaml

LEGS = Path(__file__).with_name("function_legs.yaml")
LANES = {"free": ("free",), "warehouse": ("warehouse",), "all": ("free", "warehouse")}
#: A leg's URLs, which the matrix never carries, by the variable ``urls`` gives each to its job.
URLS = {
    "engine-url": "PROVISA_ENGINE_URL",
    "setup-url": "LEG_SETUP_URL",
    "control-plane-url": "LEG_CONTROL_PLANE_URL",
}
#: A leg's fields, each defaulting to empty; the matrix carries those that are no URL.
FIELDS = (
    "engine",
    "lane",
    "image",
    "service",
    "demo-source",
    "engine-url",
    "secrets",
    "concurrency",
    "ready-sql",
    "setup-url",
    "setup-sql",
    "extras",
    "pip",
    "apt",
    "odbc",
    "source-host",
    "wait",
    "control-plane-url",
    "boot-attempts",
)
#: The steps of a leg, in the order they run, by their id in the workflow.
STEPS = (
    ("odbc", "installing ODBC Driver 18"),
    ("client", "installing the engine's client"),
    ("service", "starting the engine's server"),
    ("trino", "starting the Trino stack"),
    ("warehouse", "bringing up the warehouse"),
    ("seed", "seeding the column table"),
    ("start", "starting the Provisa instance on the engine"),
    ("measure", "measuring"),
)


def load(path: Path = LEGS) -> dict[str, Any]:
    doc = yaml.safe_load(path.read_text())
    engines = [leg["engine"] for leg in doc["legs"]]
    repeated = sorted({e for e in engines if engines.count(e) > 1})
    if repeated:
        raise ValueError(f"engines with more than one leg: {repeated}")
    both = sorted(set(engines) & set(doc["not_run"]))
    if both:
        raise ValueError(f"engines both run and not run: {both}")
    for leg in doc["legs"]:
        if leg["lane"] not in LANES["all"]:
            raise ValueError(f"{leg['engine']}: unknown lane {leg['lane']!r}")
        unknown = set(leg) - set(FIELDS)
        if unknown:
            raise ValueError(f"{leg['engine']}: unknown fields {sorted(unknown)}")
        if bool(leg.get("secrets")) != (leg["lane"] == "warehouse"):
            raise ValueError(f"{leg['engine']}: a warehouse leg names its secrets, a free leg none")
    return doc


def uncovered(kinds: frozenset[str], doc: dict[str, Any]) -> dict[str, list[str]]:
    """Engine kinds neither run nor said why not, and names that are no engine kind."""
    named = {leg["engine"] for leg in doc["legs"]} | set(doc["not_run"])
    return {"missing": sorted(kinds - named), "unknown": sorted(named - kinds)}


def matrix_entry(leg: dict[str, Any]) -> dict[str, Any]:
    entry = {field: leg.get(field, "") for field in FIELDS if field not in URLS}
    entry["odbc"] = bool(leg.get("odbc"))
    entry["wait"] = str(leg.get("wait", 600))
    entry["boot-attempts"] = str(leg.get("boot-attempts", 90))
    entry["extras"] = leg.get("extras", "dev")
    entry["secrets"] = " ".join(leg.get("secrets", ()))
    return entry


def plan(lanes: str, doc: dict[str, Any]) -> dict[str, Any]:
    """The matrix of the legs in ``lanes``, and the engines the merge expects of them."""
    if lanes not in LANES:
        raise ValueError(f"lanes must be one of {sorted(LANES)}, not {lanes!r}")
    legs = [matrix_entry(leg) for leg in doc["legs"] if leg["lane"] in LANES[lanes]]
    return {"matrix": {"include": legs}, "engines": ",".join(leg["engine"] for leg in legs)}


def urls(engine: str, doc: dict[str, Any]) -> dict[str, str]:
    """The URLs of ``engine``'s leg, by the variable its job reads each from."""
    leg = next((leg for leg in doc["legs"] if leg["engine"] == engine), None)
    if leg is None:
        raise ValueError(f"no leg measures {engine!r}")
    return {variable: leg.get(field, "") for field, variable in URLS.items()}


def wait(url: str, sql: str, timeout: float, *, interval: float = 5.0) -> None:
    """Until ``url`` answers ``sql``; the last error when it never does in ``timeout`` seconds."""
    from sqlalchemy import create_engine, text

    deadline = time.monotonic() + timeout
    last = "never tried"
    while time.monotonic() < deadline:
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                conn.execute(text(sql)).fetchall()
            return
        except Exception as exc:  # noqa: BLE001 - kept as the reason the engine never answered
            last = f"{type(exc).__name__}: {exc}".splitlines()[0]
        finally:
            engine.dispose()
        time.sleep(interval)
    raise SystemExit(f"the engine never answered {sql!r} in {timeout:.0f}s: {last}")


def setup(url: str, sql: str) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
    finally:
        engine.dispose()


def failure(engine: str, steps: dict[str, Any], logs: list[Path]) -> dict[str, Any]:
    """Why a leg left no result: the first of its steps that failed, and the end of each log."""
    failed = next(
        (
            (step_id, what)
            for step_id, what in STEPS
            if steps.get(step_id, {}).get("outcome") == "failure"
        ),
        None,
    )
    step_id, what = failed or ("", "a step of the leg's own")
    tails = {}
    for log in logs:
        if log.is_file():
            lines = log.read_text(errors="replace").splitlines()
            tails[log.name] = lines[-40:]
    return {
        "engine": engine,
        "step": step_id,
        "reason": f"{engine}: the leg failed {what}",
        "logs": tails,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="The legs of Conformance (functions).")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--lanes", required=True, choices=sorted(LANES))
    u = sub.add_parser("urls")
    u.add_argument("--engine", required=True)
    w = sub.add_parser("wait")
    w.add_argument("--url", required=True)
    w.add_argument("--sql", required=True)
    w.add_argument("--timeout", type=float, required=True)
    s = sub.add_parser("setup")
    s.add_argument("--url", required=True)
    s.add_argument("--sql", required=True)
    f = sub.add_parser("failure")
    f.add_argument("--engine", required=True)
    f.add_argument("--steps", required=True, help="the job's steps context, as JSON")
    f.add_argument("--log", action="append", default=[], type=Path)
    f.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv[1:])
    if args.command == "plan":
        planned = plan(args.lanes, load())
        print(f"matrix={json.dumps(planned['matrix'])}")
        print(f"engines={planned['engines']}")
        return 0
    if args.command == "urls":
        for variable, value in urls(args.engine, load()).items():
            print(f"{variable}={value}")
        return 0
    if args.command == "wait":
        wait(args.url, args.sql, args.timeout)
        return 0
    if args.command == "setup":
        setup(args.url, args.sql)
        return 0
    found = failure(args.engine, json.loads(args.steps), args.log)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(found, indent=1) + "\n")
    print(found["reason"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
