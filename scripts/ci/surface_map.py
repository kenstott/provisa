# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1e0c3a-92b4-4d17-8a55-3e7c1b9d2f40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What verifies a surface, read from scripts/ci/surfaces.toml.

The plan job of verify-surface.yml runs this with the standard library alone, like run_lane.py's
own plan: a test's markers are read from its source, never by collecting it.

Usage: surface_map.py plan <surface> [--full] [--base <ref>]   (GITHUB_OUTPUT lines)
       surface_map.py surface-of <path>...                     (the one surface these paths take)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_lane  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
MAP = REPO / "scripts" / "ci" / "surfaces.toml"

DEFAULT = "control-plane"
CONFORMANCE = (
    "conformance-bolt",
    "conformance-cypher",
    "conformance-flight-sql",
    "conformance-graphql",
    "conformance-pgwire",
)
#: The container lanes a verification may run. Live lanes never: they are not in run_lane.SUITE, and
#: no job here passes them credentials. On-demand lanes run only when named by hand.
LANES = tuple(n for n in run_lane.SUITE if n not in run_lane.ON_DEMAND) + run_lane.OWN_JOBS


@dataclass(frozen=True)
class Surface:
    name: str
    wider: str | None = None
    suites: tuple[str, ...] = ()
    lanes: tuple[str, ...] = ()
    paths: tuple[str, ...] = ()
    tests: tuple[str, ...] = ()


@dataclass(frozen=True)
class Plan:
    surface: str
    full: bool
    widened_by: tuple[str, ...]
    suites: tuple[str, ...]
    lanes: tuple[str, ...]
    tests: tuple[str, ...]
    #: Tests of the surface that only a live or on-demand lane selects: they run nowhere here.
    not_run: tuple[str, ...]

    @property
    def integration(self) -> bool:
        return bool(self.lanes or self.tests)


def load(path: Path = MAP) -> dict:
    return tomllib.loads(path.read_text())


def surfaces(data: dict) -> dict[str, Surface]:
    found = {
        name: Surface(
            name,
            wider=body.get("wider"),
            suites=tuple(body.get("suites", ())),
            lanes=tuple(body.get("lanes", ())),
            paths=tuple(body.get("paths", ())),
            tests=tuple(body.get("tests", ())),
        )
        for name, body in data["surfaces"].items()
    }
    for name, body in data["sources"].items():
        found[f"source-{name}"] = Surface(
            f"source-{name}",
            wider="engines",
            paths=tuple(body.get("paths", ())),
            tests=tuple(body.get("tests", ())),
        )
    return found


def contract_paths(data: dict) -> tuple[str, ...]:
    return tuple(path for group in data["contract"].values() for path in group)


def owners(path: str, patterns: dict[str, tuple[str, ...]]) -> list[str]:
    return [name for name, globs in patterns.items() if any(fnmatch(path, g) for g in globs)]


def integration_files() -> list[str]:
    return run_lane.test_files(("tests/integration",))


def files_of(name: str, all_surfaces: dict[str, Surface], files: list[str]) -> list[str]:
    """The tests/integration files ``name`` runs: its own, and for the default surface also every
    file no surface names."""
    tests = {n: s.tests for n, s in all_surfaces.items()}
    return [
        f for f in files if name in owners(f, tests) or (name == DEFAULT and not owners(f, tests))
    ]


def _lanes_selecting(path: str, marks: frozenset[str]) -> list[str]:
    return [
        lane
        for lane in LANES
        if run_lane._in_lane(lane, path) and run_lane.selects(lane, marks)  # noqa: SLF001
    ]


def runnable(path: str) -> tuple[list[str], list[str], set[str]]:
    """The tests of ``path`` a verification can run (as the file when that is all of them, which a
    targeted run prefers), those it cannot, and the lanes the first run in. A parametrized test
    with a variant only a live lane selects is one it cannot: a targeted run refuses it."""
    keep: list[str] = []
    drop: list[str] = []
    lanes: set[str] = set()
    tests = run_lane.test_markers(path)
    for test, marker_sets in tests.items():
        selected = [_lanes_selecting(path, marks) for marks in marker_sets]
        if all(selected):
            keep.append(test)
            lanes.update(*selected)
        else:
            drop.append(test)
    return ([path] if keep and not drop else keep), drop, lanes


def changed_contract(data: dict, changed: list[str]) -> tuple[str, ...]:
    contract = set(contract_paths(data))
    return tuple(sorted(contract.intersection(changed)))


def plan(data: dict, name: str, *, full: bool, changed: list[str]) -> Plan:
    all_surfaces = surfaces(data)
    if name not in all_surfaces:
        raise ValueError(f"no such surface: {name}; surfaces: {', '.join(sorted(all_surfaces))}")
    surface = all_surfaces[name]
    widened_by = changed_contract(data, changed)
    named: list[str] = []
    not_run: list[str] = []
    lanes: set[str] = set()
    for path in files_of(name, all_surfaces, integration_files()):
        keep, drop, selected = runnable(path)
        named += keep
        not_run += drop
        lanes |= selected
    if full or widened_by:
        return Plan(
            name,
            True,
            widened_by,
            tuple(dict.fromkeys((*surface.suites, *CONFORMANCE))),
            ("all",),
            (),
            tuple(not_run),
        )
    if surface.lanes:
        # Its own lanes whole, and with them the lane of every file it took in another lane.
        return Plan(
            name,
            False,
            (),
            surface.suites,
            tuple(n for n in LANES if n in surface.lanes or n in lanes),
            (),
            tuple(not_run),
        )
    return Plan(name, False, (), surface.suites, (), tuple(named), tuple(not_run))


def surface_of(data: dict, paths: list[str]) -> str:
    """The one surface a change to ``paths`` belongs to: tests/ outside tests/integration follow
    the code, an unnamed path is the default surface, and paths on several surfaces take their
    nearest common wider surface."""
    all_surfaces = surfaces(data)
    code = {n: s.paths for n, s in all_surfaces.items()}
    tests = {n: s.tests for n, s in all_surfaces.items()}
    touched: set[str] = set()
    for path in paths:
        if path.startswith("tests/integration/"):
            touched.update(owners(path, tests) or [DEFAULT])
        elif not path.startswith("tests/"):
            touched.update(owners(path, code) or [DEFAULT])
    if not touched:
        return "delivery"

    def chain(name: str) -> list[str]:
        wider = all_surfaces[name].wider
        return [name, *(chain(wider) if wider else [])]

    chains = [chain(name) for name in sorted(touched)]
    return next(n for n in chains[0] if all(n in c for c in chains))


def changed_since(base: str) -> list[str]:
    out = subprocess.run(  # noqa: S603 - fixed argv
        ["git", "diff", "--name-only", f"{base}...HEAD"],  # noqa: S607
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.split()


def outputs(result: Plan) -> str:
    suites = ",".join(result.suites)
    return "\n".join(
        [
            f"full={'true' if result.full else 'false'}",
            f"widened_by={' '.join(result.widened_by)}",
            # Comma-bounded, so that a job's `contains` matches a whole name.
            f"suites=,{suites},",
            f"integration={'true' if result.integration else 'false'}",
            f"lanes={','.join(result.lanes)}",
            f"tests={' '.join(result.tests)}",
            f"not_run={len(result.not_run)}",
        ]
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="What verifies a surface.")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("surface")
    p.add_argument("--full", action="store_true")
    p.add_argument("--base", help="the ref the changed paths are read against")
    s = sub.add_parser("surface-of")
    s.add_argument("paths", nargs="+")
    args = parser.parse_args(argv)
    data = load()
    if args.command == "surface-of":
        print(surface_of(data, args.paths))
        return 0
    changed = changed_since(args.base) if args.base else []
    print(outputs(plan(data, args.surface, full=args.full, changed=changed)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
