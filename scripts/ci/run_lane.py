# Copyright (c) 2026 Kenneth Stott
# Canary: 141cb354-2666-496e-b9cf-511cf6417d5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Run one CI lane, or one shard of it, of the container-backed suite.

The lanes are scripts/test-all's: each holds the tests whose session-provisioned services can live
together in one Docker host's memory. The large lanes are cut into file shards so each job fits a
runner and finishes in reasonable time. A shard is every Nth test file of the lane's paths, in
sorted order, so the shards partition the lane: each file runs in exactly one shard.

Usage: run_lane.py <lane> [--shard K/N] [extra pytest args...]
       run_lane.py --matrix <all | lane,lane,...>   (the CI suite matrix, as JSON)
"""

from __future__ import annotations

import argparse
import ast
import itertools
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# The selectors scripts/test-all uses, by name. The base exclusions are folded in because a CLI -m
# replaces the ini addopts -m.
_BASE = (
    "not packaging and not cluster and not isolated and not requires_warehouse "
    "and not requires_vector and not demo_walk"
)
_KAFKA = "requires_kafka or requires_debezium"
_DATASTORE = "requires_elasticsearch or requires_sparql or requires_mongodb or requires_prometheus"
_HIVE_S3 = "requires_hive_s3"
# Container-backed paths. tests/unit runs in unit-tests.yml and is not repeated here.
_SUITE = ("tests/integration", "tests/steps")

# The longest one test may run in a lane. Generous: a heavy engine's provisioning happens inside its
# first test's setup.
TEST_TIMEOUT_S = 900
# How long before that bound a still-running test's stacks are written to the log.
STACKS_BEFORE_BOUND_S = 60


@dataclass(frozen=True)
class Lane:
    paths: tuple[str, ...]
    marker: str
    clear_addopts: bool = False
    ignore: tuple[str, ...] = ()
    #: The longest one test of the lane may run.
    bound: int = TEST_TIMEOUT_S


# A demo's walk: its template built and its replicas landed, every tour step, then the close's
# grace and the reaper (scripts/ci/run_lane.py demo_walk).
DEMO_WALK_TIMEOUT_S = 3600

LANES: dict[str, Lane] = {
    "core": Lane(
        _SUITE, f"{_BASE} and not ({_KAFKA} or requires_neo4j or {_DATASTORE}) and not e2e"
    ),
    "app": Lane(
        _SUITE,
        f"{_BASE} and not ({_KAFKA} or requires_neo4j or {_DATASTORE} or {_HIVE_S3}) and e2e",
    ),
    "hive_s3": Lane(_SUITE, f"{_BASE} and ({_HIVE_S3}) and e2e"),
    "kafka": Lane(_SUITE, f"{_BASE} and ({_KAFKA})"),
    "neo4j": Lane(_SUITE, f"{_BASE} and requires_neo4j and not ({_KAFKA})"),
    "datastores": Lane(_SUITE, f"{_BASE} and ({_DATASTORE}) and not ({_KAFKA} or requires_neo4j)"),
    # Every directory but tests/e2e, as test-all runs it: the isolated tests under tests/unit are
    # deselected by unit-tests.yml's addopts and run only here.
    "isolated": Lane(("tests",), "isolated", clear_addopts=True, ignore=("tests/e2e",)),
    "e2e": Lane(("tests/e2e",), "e2e and not cluster", clear_addopts=True),
    # REQ-2016: the tests that embed with a local sentence-transformers model (the vector extra,
    # with CPU-only torch): their own job installs it (scripts/ci/lanes/suite-prepare.sh vector), so no
    # other lane pays for torch.
    "vector": Lane(
        _SUITE,
        "requires_vector and not packaging and not cluster and not isolated "
        "and not requires_warehouse",
    ),
    "cluster": Lane(("tests/e2e/test_helm_minikube.py",), "cluster", clear_addopts=True),
    # Live cloud services: nightly and on demand, with the credentials from repo secrets. A live
    # test runs in CI only in a lane whose job declares its secrets and checks they are present
    # (integration-suite-lanes.yml), so what a lane runs is decided here, by marker, never by a
    # test skipping itself. The live Salesforce test is in no nightly lane: a cold run costs
    # about 2,450 API calls of an org allowed 15,000 a day. The live Microsoft 365 test is in no
    # CI lane at all: its tenant is the maintainer's own and no job declares M365 secrets.
    # Nor is the live Google Workspace test: its domain is the maintainer's own too.
    "warehouse": Lane(
        ("tests/integration", "tests/steps"),
        "requires_warehouse and not requires_salesforce and not requires_athena and not requires_servicenow "
        "and not requires_microsoft_365 and not requires_google_workspace and not requires_polly_bakeoff",
        clear_addopts=True,
    ),
    # The live Salesforce org: on demand only (workflow_dispatch, include_salesforce), its own job.
    "salesforce": Lane(
        ("tests/integration", "tests/steps"), "requires_salesforce", clear_addopts=True
    ),
    # The live Amazon Athena account (REQ-1981): on demand only (include_athena), its own job.
    "athena": Lane(("tests/integration", "tests/steps"), "requires_athena", clear_addopts=True),
    # The live ServiceNow instance (REQ-1954): on demand only (include_servicenow), its own job.
    "servicenow": Lane(
        ("tests/integration", "tests/steps"), "requires_servicenow", clear_addopts=True
    ),
    # REQ-2006: a visitor's walk through one published demo, served from the bundle demo-bundles.yml
    # just built (PROVISA_TEST_DEMO_BUNDLES_DIR), before it is published: that job runs it with the
    # demo's node id. One test opens the demo's template, which lands its replicas first.
    "demo_walk": Lane(
        ("tests/integration/test_sandbox_visitor_walk.py",),
        "demo_walk and not requires_warehouse",
        clear_addopts=True,
        bound=DEMO_WALK_TIMEOUT_S,
    ),
    # REQ-2012: Polly's model bake-off asks paid models real questions; on demand only, its own
    # workflow (polly-bakeoff.yml).
    "polly_bakeoff": Lane(
        ("tests/integration/test_polly_bakeoff_e2e.py",),
        "requires_polly_bakeoff",
        clear_addopts=True,
    ),
}


# The suite job's matrix: lane -> (file shards, timeout minutes). cluster, warehouse and salesforce
# are their own jobs, not matrix entries.
SUITE: dict[str, tuple[int, int]] = {
    "core": (6, 150),
    "app": (3, 120),
    "hive_s3": (1, 90),
    "kafka": (1, 90),
    "neo4j": (1, 90),
    "datastores": (1, 90),
    "isolated": (1, 90),
    "e2e": (1, 120),
    "vector": (1, 90),
}


# The lanes a dispatch of integration-suite.yml can run, beside the suite matrix: each its own job.
# A live lane runs only when the dispatch includes it (its include_<lane> input).
OWN_JOBS = ("cluster",)
LIVE = ("warehouse", "salesforce", "athena", "servicenow")


def named_tests() -> list[str]:
    """The tests a targeted run names (PROVISA_LANE_TESTS: space-separated pytest node ids or
    files), or none for an ordinary run. A named file that does not exist is refused, so a typo
    cannot pass by running nothing."""
    named = os.environ.get("PROVISA_LANE_TESTS", "").split()
    missing = [t for t in named if not (REPO / t.split("::", 1)[0]).is_file()]
    if missing:
        raise ValueError(f"no such test file: {', '.join(missing)}")
    return named


# ── Which lanes select a named test ────────────────────────────────────────────────────────────
# Read from the test's source, never by collecting it: the plan job has only the standard library,
# and collecting tests/integration starts the test stack. A test's markers are its module's
# pytestmark, its class's and its own decorators'; a parameter marked with pytest.param(marks=...)
# -- in its own parametrize, or a fixture's params it uses -- is a variant of the test with those
# markers too. Each variant must be selected by a lane the run can run.


def _is_param(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "param"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "pytest"
    )


def _mark_names(node: ast.AST) -> set[str]:
    """The ``pytest.mark.<name>`` names under ``node``, but not those of a pytest.param's marks."""
    found: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if _is_param(current):
            continue
        if (
            isinstance(current, ast.Attribute)
            and isinstance(current.value, ast.Attribute)
            and current.value.attr == "mark"
            and isinstance(current.value.value, ast.Name)
            and current.value.value.id == "pytest"
        ):
            found.add(current.attr)
        stack.extend(ast.iter_child_nodes(current))
    return found


def _alternatives(marks: ast.AST) -> list[set[str]]:
    """The marker sets a pytest.param's ``marks=`` can be: each branch of a conditional."""
    if isinstance(marks, ast.IfExp):
        return _alternatives(marks.body) + _alternatives(marks.orelse)
    return [_mark_names(marks)]


def _values_variants(values: ast.AST) -> list[set[str]]:
    """The extra marker sets of a parametrize's or a fixture's values: one per distinct marking.
    An empty set is a value with no markers of its own."""
    variants: list[set[str]] = []
    elements = values.elts if isinstance(values, (ast.List, ast.Tuple)) else None
    params = [n for n in ast.walk(values) if isinstance(n, ast.Call) and _is_param(n)]
    for param in params:
        marked = [k.value for k in param.keywords if k.arg == "marks"]
        variants += _alternatives(marked[0]) if marked else [set()]
    if elements is not None and any(not _is_param(e) for e in elements):
        variants.append(set())
    if not params:
        variants.append(set())
    unique: list[set[str]] = []
    for v in variants:
        if v not in unique:
            unique.append(v)
    return unique


def _decorator_call(node: ast.AST, attr: str) -> ast.Call | None:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == attr:
            return node
    return None


def _parametrize_variants(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[list[list[set[str]]], set[str]]:
    """Each parametrize decorator's value variants, and the argument names they parametrize: a
    parametrized argument is the parameter's value, not a fixture of that name."""
    axes: list[list[set[str]]] = []
    argnames: set[str] = set()
    for deco in func.decorator_list:
        call = _decorator_call(deco, "parametrize")
        if call is None or len(call.args) < 2:
            continue
        axes.append(_values_variants(call.args[1]))
        names = call.args[0]
        if isinstance(names, ast.Constant) and isinstance(names.value, str):
            argnames |= {n.strip() for n in names.value.split(",")}
        elif isinstance(names, (ast.List, ast.Tuple)):
            argnames |= {
                e.value
                for e in names.elts
                if isinstance(e, ast.Constant) and isinstance(e.value, str)
            }
    return axes, argnames


def _fixture_params(module: ast.Module) -> tuple[dict[str, list[set[str]]], dict[str, list[str]]]:
    """Each module fixture's param variants (if it has params) and the fixtures it takes."""
    variants: dict[str, list[set[str]]] = {}
    takes: dict[str, list[str]] = {}
    for node in module.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for deco in node.decorator_list:
            call = _decorator_call(deco, "fixture")
            named = isinstance(deco, ast.Attribute) and deco.attr == "fixture"
            if call is None and not named:
                continue
            takes[node.name] = [a.arg for a in node.args.args]
            params = [k.value for k in (call.keywords if call else []) if k.arg == "params"]
            if params:
                variants[node.name] = _values_variants(params[0])
    return variants, takes


def _combine(base: set[str], axes: list[list[set[str]]]) -> list[frozenset[str]]:
    combos = []
    for picked in itertools.product(*axes) if axes else [()]:
        combo = frozenset(base.union(*picked))
        if combo not in combos:
            combos.append(combo)
    return combos


def test_markers(path: str) -> dict[str, list[frozenset[str]]]:
    """The tests of the module at ``path`` (``path::name`` or ``path::Class::name``), each with
    the marker sets its variants run with. A module that defines no test function of its own (a
    pytest-bdd steps module: its scenarios are generated) is one test, ``path``."""
    module = ast.parse((REPO / path).read_text(), filename=path)
    module_marks: set[str] = set()
    for node in module.body:
        if isinstance(node, (ast.Assign, ast.AugAssign)):
            targets_ = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in targets_):
                module_marks |= _mark_names(node.value)
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and isinstance(node.value.func.value, ast.Name)
            and node.value.func.value.id == "pytestmark"
        ):
            module_marks |= _mark_names(node.value)
    fixture_variants, takes = _fixture_params(module)

    def used(names: list[str]) -> list[str]:
        seen: list[str] = []
        stack = list(names)
        while stack:
            name = stack.pop()
            if name in seen or name not in takes:
                continue
            seen.append(name)
            stack.extend(takes[name])
        return seen

    def variants(
        func: ast.FunctionDef | ast.AsyncFunctionDef, outer: set[str]
    ) -> list[frozenset[str]]:
        base = module_marks | outer | set().union(*map(_mark_names, func.decorator_list))
        axes, parametrized = _parametrize_variants(func)
        args = [a.arg for a in func.args.args if a.arg not in parametrized]
        axes += [fixture_variants[f] for f in used(args) if f in fixture_variants]
        return _combine(base, axes)

    tests: dict[str, list[frozenset[str]]] = {}
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(
            "test"
        ):
            tests[f"{path}::{node.name}"] = variants(node, set())
        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            class_marks = set().union(*map(_mark_names, node.decorator_list))
            for item in node.body:
                if isinstance(item, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "pytestmark" for t in item.targets
                ):
                    class_marks |= _mark_names(item.value)
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                    item.name.startswith("test")
                ):
                    tests[f"{path}::{node.name}::{item.name}"] = variants(item, class_marks)
    if not tests:
        tests[path] = [frozenset(module_marks)]
    return tests


def selects(lane_name: str, marks: frozenset[str]) -> bool:
    """Whether the lane's ``-m`` expression selects a test carrying ``marks``."""
    expression = LANES[lane_name].marker

    class _Marks(dict):
        def __missing__(self, name: str) -> bool:
            return name in marks

    return bool(eval(expression, {"__builtins__": {}}, _Marks()))  # noqa: S307 - LANES' own


def _in_lane(lane_name: str, path: str) -> bool:
    lane = LANES[lane_name]
    return _under(path, lane.paths) and not _under(path, lane.ignore)


def routes(runnable: tuple[str, ...]) -> dict[str, list[str]]:
    """For a targeted run: each lane of ``runnable`` that selects a named test, with the named
    tests it runs. A named test of which a variant no runnable lane selects is refused, naming
    the test, its markers and the lanes that would select it."""
    planned: dict[str, list[str]] = {}
    unrunnable: list[str] = []
    for name in named_tests():
        path, _, rest = name.partition("::")
        found = test_markers(path)
        if rest:
            wanted = f"{path}::{rest.split('[', 1)[0]}"
            chosen = {k: v for k, v in found.items() if k == wanted or k.startswith(wanted + "::")}
            if not chosen and path in found:  # a steps module: its scenarios are generated
                chosen = found
            if not chosen:
                raise ValueError(f"no such test: {name}")
        else:
            chosen = found
        for test, marker_sets in chosen.items():
            for marks in marker_sets:
                lanes = [lane for lane in LANES if _in_lane(lane, path) and selects(lane, marks)]
                here = [lane for lane in lanes if lane in runnable]
                if not here:
                    need = ", ".join(lanes) if lanes else "no lane at all"
                    unrunnable.append(
                        f"{test} [{', '.join(sorted(marks)) or 'no markers'}] needs {need}"
                    )
                for lane in here:
                    if name not in planned.setdefault(lane, []):
                        planned[lane].append(name)
    if unrunnable:
        raise ValueError(
            "a targeted run must run every test it names; this one cannot run:\n  "
            + "\n  ".join(unrunnable)
            + "\n(a live lane runs when the dispatch includes it: include_<lane>)"
        )
    return planned


def runnable_lanes() -> tuple[str, ...]:
    """The lanes a targeted run may route to: every suite lane and own job, whatever lanes the
    dispatch listed (it routes each named test to the lanes that select it), and each live lane
    the dispatch includes (PROVISA_INCLUDE_LIVE, comma-separated)."""
    included = [n.strip() for n in os.environ.get("PROVISA_INCLUDE_LIVE", "").split(",")]
    return (*SUITE, *OWN_JOBS, *(lane for lane in LIVE if lane in included))


def matrix(selection: str) -> dict:
    """The suite matrix for ``selection``: "all", or comma-separated lane names. A name that is
    no lane is refused, so a typo cannot dispatch an empty run that reports green. A targeted run
    (named_tests) is routed instead: each lane that selects a named test runs it, as one job (its
    few tests are not sharded), whatever lanes were listed; one naming a test no lane it can run
    selects is refused (routes)."""
    names = [n.strip() for n in selection.split(",") if n.strip()]
    if names == ["all"]:
        names = list(SUITE)
    unknown = [n for n in names if n not in LANES]
    if unknown:
        raise ValueError(f"no such lane: {', '.join(unknown)}; lanes: {', '.join(sorted(LANES))}")
    include = []
    targeted = bool(named_tests())
    if targeted:
        routed = routes(runnable_lanes())
        names = [name for name in SUITE if name in routed]
    for name in names:
        if name not in SUITE:
            continue
        shards, timeout = SUITE[name]
        if shards == 1 or targeted:
            include.append({"lane": name, "shard": "", "timeout": timeout})
        else:
            include += [
                {"lane": name, "shard": f"{k}/{shards}", "timeout": timeout}
                for k in range(1, shards + 1)
            ]
    return {"include": include}


def test_files(paths: tuple[str, ...]) -> list[str]:
    """Every test module under ``paths``, sorted (pytest's own file patterns)."""
    found: set[str] = set()
    for path in paths:
        root = REPO / path
        if root.is_file():
            found.add(path)
            continue
        for pattern in ("test_*.py", "steps_*.py"):
            for f in root.rglob(pattern):
                found.add(str(f.relative_to(REPO)))
    return sorted(found)


def shard(files: list[str], index: int, total: int) -> list[str]:
    """Shard ``index`` (1-based) of ``total``: every total-th file starting at index-1."""
    if not 1 <= index <= total:
        raise ValueError(f"shard {index}/{total} is out of range")
    return files[index - 1 :: total]


def targets(lane_name: str, shard_spec: str | None) -> list[str]:
    """What the lane (or its shard) hands pytest: the lane's paths, or the shard's files."""
    lane = LANES[lane_name]
    if named_tests():
        # A targeted run: only the named tests this lane selects.
        return routes(runnable_lanes()).get(lane_name, [])
    if shard_spec is None:
        return list(lane.paths)
    index, total = (int(x) for x in shard_spec.split("/"))
    return shard(test_files(lane.paths), index, total)


def _under(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == r or path.startswith(r.rstrip("/") + "/") for r in roots)


def command(lane_name: str, shard_spec: str | None, extra: list[str]) -> list[str]:
    lane = LANES[lane_name]
    cmd = [sys.executable, "-m", "pytest", *targets(lane_name, shard_spec), "-m", lane.marker]
    if lane.clear_addopts:
        cmd += ["-o", "addopts="]
    cmd += [f"--ignore={path}" for path in lane.ignore]
    # Every test is bounded: one that waits past this fails by name with every thread's stack,
    # instead of holding the shard until the job's own timeout cancels it and its log is lost.
    bound = ["--timeout", str(lane.bound), "--timeout-method", "signal"]
    # The bound's own stacks are printed in pytest's final report, which a lane cancelled by its
    # job limit never reaches: the e2e lane spent seven 15-minute bounds in module teardowns and
    # was cancelled with no stack in its log. A test phase still running a minute before its
    # bound has every thread's stack written to the log at that moment.
    stacks = ["-o", f"faulthandler_timeout={lane.bound - STACKS_BEFORE_BOUND_S}"]
    return cmd + ["-ra", "-p", "no:cacheprovider", "--durations=50", *bound, *stacks, *extra]


def main(argv: list[str]) -> int:
    if argv[:1] == ["--matrix"]:
        import json

        print(json.dumps(matrix(argv[1] if len(argv) > 1 else "all")))
        return 0
    if argv[:1] == ["--has-targets"]:
        # A job of its own (cluster, warehouse, salesforce, athena, servicenow) runs when the run
        # names no tests (the whole lane) or names one the lane selects; otherwise it would collect
        # nothing, write no results and fail its required results upload.
        print("true" if not named_tests() or targets(argv[1], None) else "false")
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("lane", choices=sorted(LANES))
    parser.add_argument("--shard", help="K/N: the Kth of N file shards of the lane")
    args, extra = parser.parse_known_args(argv)
    if named_tests() and not targets(args.lane, args.shard):
        print(f"targeted run: no named test is in lane {args.lane}; nothing to run", flush=True)
        return 0
    cmd = command(args.lane, args.shard, extra)
    print("+", " ".join(cmd), flush=True)
    code = subprocess.call(cmd, cwd=REPO, env=os.environ.copy())  # noqa: S603 - fixed argv
    if code == 5 and named_tests():
        # The run was routed here because this lane's marker selects these tests (routes): a lane
        # that then collects none of them means their markers were read wrong, and they ran
        # nowhere. Never a pass with nothing run.
        print(
            f"targeted run: lane {args.lane} was routed {' '.join(targets(args.lane, None))} "
            "but selected none of them: run_lane.py read their markers differently from pytest",
            flush=True,
        )
        return 1
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
