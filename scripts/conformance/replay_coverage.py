# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1d94-6a2f-4c85-9e03-d58f2a1c7b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which statements of the Trino and ClickHouse function replays (tests/integration/*_on_engines.py)
conformance-functions already measures (REQ-2167, REQ-2171; REQ-1989 cadence, maintainer
2026-11-20): those the replays no longer run.

A statement is covered on an engine when it is a single constant call of a pg_catalog overload
whose values conformance compares on that engine, with an outcome of pass, refused, error,
wrong_answer or within_bound, or that overload's all-NULL call, which conformance makes of every
overload. Every other statement stays in its replay.

    .venv/bin/python scripts/conformance/replay_coverage.py <function_support.json>

reads the function_support.json of a green conformance-functions run (its function-support
artifact) and writes tests/integration/function_replay_covered.json (each family's covered
statements and the engines each is covered on), which the replays read
through tests/integration/replay_coverage.py."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS  # noqa: E402

OUT = REPO / "tests" / "integration" / "function_replay_covered.json"
ENGINES = ("trino", "clickhouse")
MEASURED = frozenset({"pass", "refused", "error", "wrong_answer", "within_bound"})

_ALIASES = {
    "int": "integer",
    "int4": "integer",
    "int8": "bigint",
    "int2": "smallint",
    "float8": "double precision",
    "float4": "real",
    "bool": "boolean",
    "varchar": "character varying",
    "char": "character",
    "decimal": "numeric",
    "timestamptz": "timestamp with time zone",
    "timetz": "time with time zone",
    "timestamp": "timestamp without time zone",
    "time": "time without time zone",
    "bpchar": "character",
}
_POLYMORPHIC = frozenset(
    {
        "anyelement",
        "anyarray",
        "anynonarray",
        "anyenum",
        "anyrange",
        "anymultirange",
        "any",
        '"any"',
        "anycompatible",
        "anycompatiblearray",
        "anycompatiblenonarray",
        "anycompatiblerange",
        "anycompatiblemultirange",
    }
)

Outcomes = dict[tuple[str, str], dict[str, str]]


def measured(support: dict) -> tuple[Outcomes, Outcomes]:
    """Each overload's outcome by engine: those whose values conformance compares, and every
    overload's (its all-NULL call)."""
    checked: Outcomes = {}
    called: Outcomes = {}
    for entry in support["functions"]:
        key = (entry["function"], entry["overload"])
        outcomes = {engine: said["outcome"] for engine, said in entry["outcomes"].items()}
        called[key] = outcomes
        if (
            entry["answers_checked"]
            or entry["type_and_form_checked"]
            or entry["side_effect_checked"]
        ):
            checked[key] = outcomes
    return checked, called


def _type_name(text: str) -> str:
    text = text.lower().strip()
    array = text.endswith("[]")
    base = (text[:-2] if array else text).split("(")[0].strip()
    return _ALIASES.get(base, base) + ("[]" if array else "")


def _argument_type(argument: exp.Expression) -> str | None:
    return _type_name(argument.to.sql("postgres")) if isinstance(argument, exp.Cast) else None


def _constant(argument: exp.Expression) -> bool:
    return not any(isinstance(n, (exp.Column, exp.Select, exp.Subquery)) for n in argument.walk())


def call_of(sql: str) -> tuple[str, list[exp.Expression]] | None:
    """The function and arguments of a statement that is one constant call, else None."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except sqlglot.errors.ParseError:
        return None  # not a statement the classifier reads: it stays in its replay
    if not isinstance(tree, exp.Select) or len(tree.expressions) != 1:
        return None
    if any(tree.args.get(part) for part in ("from", "where", "group", "limit")):
        return None
    node = tree.expressions[0].unalias()
    if isinstance(node, exp.Dot) and isinstance(node.expression, (exp.Anonymous, exp.Func)):
        node = node.expression
    if isinstance(node, exp.Anonymous):
        args = list(node.expressions)
    elif isinstance(node, exp.Func) and not isinstance(
        node, (exp.Cast, exp.Window, exp.WithinGroup)
    ):
        args = [v for v in node.args.values() if isinstance(v, exp.Expression)]
        args += [x for v in node.args.values() if isinstance(v, list) for x in v]
    else:
        return None
    # The name as written: sqlglot normalises some (LENGTH and the like).
    name = sql.split("(")[0].removeprefix("SELECT ").strip().split(".")[-1].strip('"')
    if not all(_constant(a) for a in args):
        return None
    return name, args


def _overloads(name: str, args: list[exp.Expression]) -> list[str]:
    types = [_argument_type(a) for a in args]
    if any(t and "." in t for t in types):
        return []  # a source's own type (a source enum): no pg_catalog overload takes it as such
    found = []
    for argtypes, _returns in FUNCTION_OVERLOADS.get(name, ()):
        if len(argtypes) != len(args):
            continue
        if all(
            got is None or want in _POLYMORPHIC or _type_name(want) == got
            for want, got in zip(argtypes, types)
        ):
            found.append(", ".join(argtypes))
    return found


def covered(sql: str, engine: str, checked: Outcomes, called: Outcomes) -> bool:
    """Whether conformance-functions measures ``sql``'s call on ``engine``."""
    found = call_of(sql)
    if found is None:
        return False
    name, args = found
    name = name if name in FUNCTION_OVERLOADS else name.lower()
    overloads = _overloads(name, args)
    if not overloads:
        return False
    all_null = all(isinstance(a, exp.Cast) and isinstance(a.this, exp.Null) for a in args)
    source = called if all_null else checked
    return all(source.get((name, o), {}).get(engine) in MEASURED for o in overloads)


def corpus() -> dict[str, dict[str, list[str]]]:
    """Each replay family's statements by engine, as its tests/integration file runs them."""
    from tests.unit import pg17_range_answers
    from tests.unit.pgwire import pg17_aggregate_answers, pg17_forms_answers, pg17_geometric_answers
    from tests.unit.pgwire import pg17_hash_answers, pg17_json_answers, pg17_system_answers
    from tests.unit.pgwire import pg17_text_search_answers, pg17_type_io_answers
    from tests.unit.pgwire.everyday_function_statements import STATEMENTS
    from tests.unit.pgwire.geometric_replay import BY_FUNCTION, column_statement
    from tests.unit.pgwire.test_pg_enum_forms import ENUM_STATEMENTS
    from tests.unit.pgwire.type_io_corpus import column_statements
    from tests.unit.test_percentile_forms_on_engines import NO_KEYS, NULL_ELEMENTS

    def both(statements: Iterable[str]) -> dict[str, list[str]]:
        return {engine: list(statements) for engine in ENGINES}

    type_io = pg17_type_io_answers.RECORDED
    columns = [found[0] for f in BY_FUNCTION if (found := column_statement(f))]
    return {
        "text_search": both(pg17_text_search_answers.RECORDED),
        "hashes": both([*pg17_hash_answers.RECORDED, *pg17_aggregate_answers.RECORDED]),
        "geometric": both([*pg17_geometric_answers.RECORDED, *columns]),
        "type_io": {engine: [*type_io, *column_statements(type_io, engine)] for engine in ENGINES},
        "system_forms": both(pg17_system_answers.RECORDED),
        "everyday": both([s for family in STATEMENTS.values() for s in family]),
        "forms": both(pg17_forms_answers.RECORDED),
        "json": both(pg17_json_answers.RECORDED),
        "enum_forms": both(ENUM_STATEMENTS),
        "percentile_null_elements": {
            "trino": [*NULL_ELEMENTS, *NO_KEYS],
            "clickhouse": list(NULL_ELEMENTS),
        },
        "ranges": both([s for by_type in pg17_range_answers.RECORDED.values() for s in by_type]),
    }


def classify(support: dict) -> dict[str, dict[str, list[str]]]:
    """Each family's covered statements, in corpus order, and the engines each is covered on."""
    checked, called = measured(support)
    found: dict[str, dict[str, list[str]]] = {}
    for family, by_engine in corpus().items():
        statements = found.setdefault(family, {})
        for engine, sqls in by_engine.items():
            for sql in dict.fromkeys(sqls):
                if covered(sql, engine, checked, called):
                    statements.setdefault(sql, []).append(engine)
    return found


def written(found: dict[str, dict[str, list[str]]]) -> str:
    """The data file's text: one statement to a line, so a recapture's diff reads by statement."""
    families = []
    for family, statements in found.items():
        lines = [
            f"  {json.dumps(s, ensure_ascii=False)}: {json.dumps(e)}" for s, e in statements.items()
        ]
        body = "{\n" + ",\n".join(lines) + "\n }" if lines else "{}"
        families.append(f" {json.dumps(family)}: {body}")
    return "{\n" + ",\n".join(families) + "\n}\n"


def main(argv: list[str]) -> int:
    (path,) = argv
    support = json.loads(Path(path).read_text())
    found = classify(support)
    OUT.write_text(written(found))
    for family, by_engine in corpus().items():
        for engine, sqls in by_engine.items():
            total = len(dict.fromkeys(sqls))
            done = sum(engine in engines for engines in found[family].values())
            print(f"{family:26s} {engine:10s} covered={done:5d} remaining={total - done:5d}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
