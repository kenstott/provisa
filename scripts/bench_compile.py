# Copyright (c) 2026 Kenneth Stott
# Canary: 8f40e587-c0b9-49e5-a507-953e5245118a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compile time per statement over the golden corpora, and what each statement produced.

``run`` times every statement of tests/fixtures/cypher_golden and tests/fixtures/graphql_golden and
writes one JSON file. Every memo in provisa is cleared between passes, so a pass is the work of a
statement seen for the first time; the first pass is warm-up and is not counted. ``compare`` reads
the files of two labels and prints median, 95th percentile, worst and total per stage, with the
statements whose output differs between the labels (those are left out of the timing).

    bench_compile.py run --label pure --passes 6 --out /path/pure-1.json
    bench_compile.py run --label pure --passes 2 --profile /path/pure.pstats --out /path/p.json
    bench_compile.py compare /path/pure-*.json -- /path/compiled-*.json
    bench_compile.py survey /path/pure.pstats
"""

from __future__ import annotations

import argparse
import functools
import gc
import hashlib
import json
import pstats
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CYPHER = REPO / "tests" / "fixtures" / "cypher_golden"
GRAPHQL = REPO / "tests" / "fixtures" / "graphql_golden"


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _attempt(work: Callable[[], Any]) -> tuple[int, str]:
    """How long ``work`` took and the digest of what it gave, a refusal being what it gave."""
    began = time.perf_counter_ns()
    try:
        result: Any = work()
    except Exception as exc:  # noqa: BLE001 -- recorded: the same statement must fail the same way
        result = {"error": type(exc).__name__, "message": str(exc)}
    return time.perf_counter_ns() - began, _digest(result)


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _cypher_statements() -> list[tuple[str, str, Callable[[], Any]]]:
    import cypher_golden

    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator import cypher_to_sql

    def translate(entry: dict) -> dict:
        label_map = cypher_golden.load_blob(entry["label_map"])
        params = cypher_golden.load_blob(entry["params"])
        ast = parse_cypher(entry["cypher"])
        if entry["call"] is not None:
            ast = ast.call_subqueries[entry["call"]].body
        return cypher_golden.rendered(cypher_to_sql(ast, label_map, params))

    entries = _jsonl(CYPHER / "entries.jsonl") + _jsonl(CYPHER / "approved.jsonl")
    return [
        ("cypher_to_sql", f"{i}:{e['origin']}", functools.partial(translate, e))
        for i, e in enumerate(entries)
    ]


def _graphql_statements() -> list[tuple[str, str, Callable[[], Any]]]:
    import graphql_golden

    from provisa.transpiler.transpile import transpile_to_duckdb, transpile_to_trino

    models: dict[str, Any] = {}
    views: dict[str, Any] = {}

    def physical(entry: dict) -> list[dict]:
        read = graphql_golden.CorpusRead(
            entry["origin"], REPO / entry["model"], entry["graphql"], entry["variables"]
        )
        schema, ctx = models[entry["model"]]
        return graphql_golden.physical_statements(schema, ctx, read, views[entry["model"]])

    def compiled(entry: dict) -> dict:
        schema, ctx = models[entry["model"]]
        return graphql_golden.compile_read(schema, ctx, entry["graphql"], entry["variables"])

    def transpiled(entry: dict, to_dialect: Callable[[str], str]) -> list[str]:
        return [to_dialect(statement["sql"]) for statement in physical(entry)]

    out: list[tuple[str, str, Callable[[], Any]]] = []
    for entry in _jsonl(GRAPHQL / "entries.jsonl"):
        model = entry["model"]
        if model not in models:
            models[model] = graphql_golden.model(REPO / model)
            views[model] = graphql_golden._view_sql(REPO / model)
        origin = entry["origin"]
        out += [
            ("graphql_compile", origin, functools.partial(compiled, entry)),
            ("graphql_physical", origin, functools.partial(physical, entry)),
            (
                "graphql_physical+duckdb",
                origin,
                functools.partial(transpiled, entry, transpile_to_duckdb),
            ),
            (
                "graphql_physical+trino",
                origin,
                functools.partial(transpiled, entry, transpile_to_trino),
            ),
        ]
    return out


def _clear_memos() -> None:
    for held in gc.get_objects():
        if isinstance(held, functools._lru_cache_wrapper):
            if getattr(held, "__module__", "").startswith(("provisa", "sqlglot")):
                held.cache_clear()


def run(label: str, passes: int, out: Path, profile: Path | None, stages: list[str] | None) -> None:
    sys.path.insert(0, str(REPO / "scripts"))
    began = time.perf_counter()
    import sqlglot
    import sqlglot.parser

    import provisa.transpiler.transpile  # noqa: F401 -- what a compile imports; its cost is reported

    import_seconds = time.perf_counter() - began
    statements = _cypher_statements() + _graphql_statements()
    if stages:
        statements = [s for s in statements if s[0] in stages]
    timings: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    digests: dict[str, dict[str, str]] = defaultdict(dict)
    profiler = None
    if profile is not None:
        import cProfile

        profiler = cProfile.Profile()
    for number in range(passes):
        _clear_memos()
        gc.collect()
        if profiler is not None and number == passes - 1:
            profiler.enable()
        for stage, key, work in statements:
            elapsed, digest = _attempt(work)
            if number:  # the first pass is warm-up
                timings[stage][key].append(elapsed)
            digests[stage][key] = digest
        if profiler is not None and number == passes - 1:
            profiler.disable()
    if profiler is not None and profile is not None:
        profiler.dump_stats(profile)
    head = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
    ).stdout.strip()
    out.write_text(
        json.dumps(
            {
                "label": label,
                "commit": head,
                "sqlglot": sqlglot.__version__,
                "compiled": str(sqlglot.parser.__file__).endswith((".so", ".pyd")),
                "python": sys.version.split()[0],
                "passes_counted": passes - 1,
                "import_seconds": import_seconds,
                "timings": timings,
                "digests": digests,
            }
        )
    )


def _load(paths: list[Path]) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, str]], list]:
    """Per statement, the median over every counted pass of every run; the digests; the imports."""
    samples: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    digests: dict[str, dict[str, str]] = {}
    imports = []
    for path in paths:
        run_ = json.loads(path.read_text())
        imports.append(run_["import_seconds"])
        for stage, by_key in run_["timings"].items():
            for key, values in by_key.items():
                samples[stage][key] += values
        for stage, by_key in run_["digests"].items():
            digests.setdefault(stage, {}).update(by_key)
    medians = {
        stage: {key: int(statistics.median(values)) for key, values in by_key.items()}
        for stage, by_key in samples.items()
    }
    return medians, digests, imports


def _ms(ns: float) -> str:
    return f"{ns / 1e6:.3f}"


def _p95(values: list[int]) -> float:
    return statistics.quantiles(values, n=20)[-1] if len(values) > 1 else values[0]


def compare(before_paths: list[Path], after_paths: list[Path]) -> None:
    before, before_digests, before_imports = _load(before_paths)
    after, after_digests, after_imports = _load(after_paths)
    print(
        f"import seconds: before {statistics.median(before_imports):.3f}, "
        f"after {statistics.median(after_imports):.3f}"
    )
    print(
        "| stage | n | diff | median ms before | after | p95 before | after "
        "| worst before | after | total s before | after | total ratio |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    differing: dict[str, list[str]] = {}
    for stage in before:
        keys = sorted(before[stage].keys() & after[stage].keys())
        differ = [k for k in keys if before_digests[stage][k] != after_digests[stage][k]]
        differing[stage] = differ
        same = [k for k in keys if k not in set(differ)]
        one = [before[stage][k] for k in same]
        two = [after[stage][k] for k in same]
        print(
            f"| {stage} | {len(same)} | {len(differ)} "
            f"| {_ms(statistics.median(one))} | {_ms(statistics.median(two))} "
            f"| {_ms(_p95(one))} | {_ms(_p95(two))} "
            f"| {_ms(max(one))} | {_ms(max(two))} "
            f"| {sum(one) / 1e9:.3f} | {sum(two) / 1e9:.3f} "
            f"| {sum(one) / sum(two):.2f}x |"
        )
    for stage, keys in differing.items():
        for key in keys[:40]:
            print(f"DIFFERS {stage}: {key}")


def survey(profile: Path, top: int) -> None:
    """Self time by source file, then the provisa files alone, then parse counts."""
    stats = pstats.Stats(str(profile))
    raw: dict[Any, Any] = stats.stats  # type: ignore[attr-defined]
    by_file: dict[str, float] = defaultdict(float)
    for (filename, _line, _name), (_cc, _nc, tottime, _ct, _callers) in raw.items():
        by_file[filename] += tottime
    total = sum(by_file.values())
    print(f"total self time {total:.2f}s")
    print("| file | self s | % |\n|---|---|---|")
    for filename, seconds in sorted(by_file.items(), key=lambda kv: -kv[1])[:top]:
        print(f"| {filename.split('site-packages/')[-1]} | {seconds:.2f} | {seconds / total:.1%} |")
    print("\nprovisa only\n| file | self s | % |\n|---|---|---|")
    ours = [(f, s) for f, s in by_file.items() if "/provisa/" in f and "site-packages" not in f]
    for filename, seconds in sorted(ours, key=lambda kv: -kv[1])[:top]:
        print(f"| {filename.split('/provisa/', 1)[-1]} | {seconds:.2f} | {seconds / total:.1%} |")
    groups: dict[str, float] = defaultdict(float)
    for filename, seconds in by_file.items():
        for name in ("sqlglot", "graphql", "pydantic", "/provisa/"):
            if name in filename:
                groups[name] += seconds
                break
        else:
            groups["other"] += seconds
    print("\ngroups:", {k: f"{v / total:.1%}" for k, v in groups.items()})
    print("\ncalls of sqlglot's top-level parse:")
    for (filename, line, name), (cc, nc, _tt, ct, _callers) in raw.items():
        if filename.endswith("sqlglot/parser.py") and name == "parse":
            print(f"  parser.py:{line} {name}: primitive {cc}, total {nc}, cumulative {ct:.2f}s")


def main(argv: list[str]) -> int:
    if argv and argv[0] == "compare":
        split = argv.index("--")
        compare([Path(p) for p in argv[1:split]], [Path(p) for p in argv[split + 1 :]])
        return 0
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--label", required=True)
    runner.add_argument("--passes", type=int, default=6)
    runner.add_argument("--out", type=Path, required=True)
    runner.add_argument("--profile", type=Path)
    runner.add_argument("--stage", action="append", help="only this stage; repeatable")
    surveyor = commands.add_parser("survey")
    surveyor.add_argument("profile", type=Path)
    surveyor.add_argument("--top", type=int, default=30)
    args = parser.parse_args(argv)
    if args.command == "run":
        run(args.label, args.passes, args.out, args.profile, args.stage)
    else:
        survey(args.profile, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
