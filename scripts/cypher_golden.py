# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d2b8e-4a73-4c09-9e15-2d8b7c3a5f61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Cypher golden SQL set (REQ-2135): every Cypher query the repository has, with the semantic
SQL the compiler produced for it on the release it was captured from.

Captured once, on the commit the graph compiler is refactored from, and replayed by
tests/unit/test_cypher_golden_sql.py against the working tree, which must produce byte-identical
SQL, parameter order and graph-variable kinds for every entry (or the same refusal).

The corpus is every top-level translation (``_Translator.translate``):

- every one the unit tests make (``capture-tests``, a pytest plugin over the given test files);
- every Cypher statement of a named demo's tour, against the demo's own model;
- every Cypher query of the perf bench, against the perf demo's model;
- every Cypher statement of the UI e2e specs, against the e2e model (config/provisa.yaml).

Each entry keeps what the translation was given -- its Cypher text where it came from text, the
AST otherwise, the label map, the parameters -- pickled under ``blobs/`` by content hash, and what
it produced. A model's label map is built offline from its config (``model_label_map``), as the
server builds one from its schema, for the role org_admin.

Usage, on a checkout of the commit to capture from:

    python scripts/cypher_golden.py capture-tests OUT tests/unit/test_cypher_*.py ...
    python scripts/cypher_golden.py capture-corpora OUT

A result that differs from that commit's, approved query by query, is held in approved.jsonl:
captured from the checkout that produces it, each entry with the approval it was given:

    python scripts/cypher_golden.py capture-approved-tests OUT APPROVAL tests/unit/...::test ...
"""

from __future__ import annotations

import contextvars
import copy
import hashlib
import importlib
import importlib.util
import json
import pickle
import re
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
GOLDEN = REPO / "tests" / "fixtures" / "cypher_golden"

#: Where the graph compiler lived before REQ-2135 phase 1 moved it, and where it lives after.
_MOVED = (
    "translator translator_rel translator_helpers translator_union translator_types "
    "select_builder assembler path_translator path_functions group_by correlated_call "
    "graph_rewriter graph_types label_map expr_visitor expr_context expr_ast comprehension "
    "map_projection"
).split()
_IR = {
    "NodePattern", "RelPattern", "PathPattern", "PathFunction", "MatchClause", "WhereClause",
    "ReturnItem", "ReturnClause", "WithClause", "UnwindClause", "MatchStep", "CallSubquery",
    "OrderItem", "CypherAST",
}  # fmt: skip


def _module(name: str) -> Any:
    """The compiler module ``name`` where this checkout keeps it. Decided by this checkout's own
    files: ``provisa`` can be extended by another checkout's (an editable install), whose
    ``provisa.gql`` would otherwise be found first."""
    package = (
        "provisa.gql" if (REPO / "provisa" / "gql" / f"{name}.py").exists() else "provisa.cypher"
    )
    return importlib.import_module(f"{package}.{name}")


class _Unpickler(pickle.Unpickler):
    """Reads a blob pickled before the move into this checkout's modules."""

    def find_class(self, module: str, name: str) -> Any:
        if module.startswith("provisa.cypher."):
            short = module.rsplit(".", 1)[1]
            if short == "parser" and name in _IR:
                return super().find_class(_ir_module(), name)
            if short in _MOVED and (REPO / "provisa" / "gql" / f"{short}.py").exists():
                return super().find_class(f"provisa.gql.{short}", name)
        return super().find_class(module, name)


def _ir_module() -> str:
    return (
        "provisa.gql.ir" if importlib.util.find_spec("provisa.gql.ir") else "provisa.cypher.parser"
    )


def load_blob(digest: str) -> Any:
    with (GOLDEN / "blobs" / f"{digest}.pkl").open("rb") as f:
        return _Unpickler(f).load()


#: Every dialect a statement may be rendered in: one node shape can render alike in one and not in
#: another (a parameter is $1 in postgres and @1 in trino).
DIALECTS = (
    "postgres", "trino", "duckdb", "snowflake", "bigquery", "databricks", "spark", "mysql", "tsql",
    "oracle",
)  # fmt: skip


def _render(sql_ast: Any, dialect: str) -> str:
    try:
        return sql_ast.sql(dialect=dialect)
    except Exception as exc:  # a dialect that cannot render a construct: that is what it gives
        return f"!{type(exc).__name__}: {exc}"


def rendered(result: tuple) -> dict:
    """What a translation produced, as the golden set compares it."""
    sql_ast, ordered, graph_vars = result
    return {
        "sql": {dialect: _render(sql_ast, dialect) for dialect in DIALECTS},
        "params": list(ordered),
        "graph_vars": {k: getattr(v, "name", str(v)) for k, v in sorted(graph_vars.items())},
    }


def refused(exc: BaseException) -> dict:
    return {"error": type(exc).__name__, "message": str(exc)}


# --- capture -------------------------------------------------------------------------------------

_depth: contextvars.ContextVar[int] = contextvars.ContextVar("cypher_golden_depth", default=0)
_origin: contextvars.ContextVar[str] = contextvars.ContextVar("cypher_golden_origin", default="")
_parse_depth: contextvars.ContextVar[int] = contextvars.ContextVar("cypher_golden_parse", default=0)


class Recorder:
    """Records each top-level translation into ``out``: entries.jsonl and blobs/."""

    def __init__(
        self, out: Path, entries: str = "entries.jsonl", approved: str | None = None
    ) -> None:
        self.out = out
        (out / "blobs").mkdir(parents=True, exist_ok=True)
        self.entries = (out / entries).open("a")
        #: For an approved entry: why it is held, and who approved it.
        self.approved = approved
        self.seen: set[str] = set()

    def blob(self, value: Any) -> str:
        data = pickle.dumps(value, protocol=5)
        digest = hashlib.sha256(data).hexdigest()[:24]
        path = self.out / "blobs" / f"{digest}.pkl"
        if not path.exists():
            path.write_bytes(data)
        return digest

    def install(self) -> None:
        translator = _module("translator")
        parser = importlib.import_module("provisa.cypher.parser")
        recorder = self
        original_translate = translator._Translator.translate
        original_parse = parser._Parser.parse

        def parse(self_p):
            # The outermost parse tags its AST with the text, and each CALL {} body with the text
            # and its place: a body is translated on its own (cypher_calls_to_sql_list).
            depth = _parse_depth.get()
            token = _parse_depth.set(depth + 1)
            try:
                ast = original_parse(self_p)
            finally:
                _parse_depth.reset(token)
            if depth == 0:
                ast.__dict__["_golden_text"] = self_p._raw
                for i, call in enumerate(ast.call_subqueries):
                    call.body.__dict__["_golden_call"] = (self_p._raw, i)
            return ast

        def translate(self_t):
            depth = _depth.get()
            standalone = depth == 0 and not self_t._var_table and not self_t._lateral_bound
            token = _depth.set(depth + 1)
            given = None
            if standalone:
                call = getattr(self_t._ast, "_golden_call", None)
                given = (
                    getattr(self_t._ast, "_golden_text", None) or call,
                    copy.deepcopy(self_t._ast),
                    self_t._lm,
                    copy.deepcopy(self_t._params),
                )
            try:
                result = original_translate(self_t)
            except Exception as exc:
                if given is not None:
                    recorder.record(given, refused(exc))
                raise
            finally:
                _depth.reset(token)
            if given is not None:
                recorder.record(given, rendered(result))
            return result

        translator._Translator.translate = translate
        parser._Parser.parse = parse

    def record(self, given: tuple, produced: dict) -> None:
        source, ast, label_map, params = given
        if ast is not None:
            for tag in ("_golden_text", "_golden_call"):
                ast.__dict__.pop(tag, None)
        text, call = source if isinstance(source, tuple) else (source, None)
        entry = {
            "origin": _origin.get(),
            "cypher": text,
            # A CALL {} body's place in the statement the text holds, translated on its own.
            "call": call,
            "ast": None if text is not None else self.blob(ast),
            "label_map": self.blob(label_map),
            "params": self.blob(params),
            "expected": produced,
            **({"approved": self.approved} if self.approved else {}),
        }
        key = json.dumps(entry, sort_keys=True)
        if key in self.seen:
            return
        self.seen.add(key)
        self.entries.write(key + "\n")
        self.entries.flush()


class _Plugin:
    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder

    def pytest_runtest_setup(self, item: Any) -> None:
        _origin.set(f"test:{item.nodeid}")


def capture_tests(out: Path, test_args: list[str], approved: str | None = None) -> int:
    import pytest

    recorder = (
        Recorder(out, entries="approved.jsonl", approved=approved) if approved else Recorder(out)
    )
    recorder.install()
    return pytest.main(["-q", "-p", "no:cacheprovider", *test_args], plugins=[_Plugin(recorder)])


# --- the corpora a model answers -----------------------------------------------------------------


def model_label_map(config_path: Path) -> Any:
    """The label map the server builds from ``config_path``'s model for org_admin, built offline:
    its tables (views among them), relationships and declared column types."""
    from provisa.compiler import naming
    from provisa.compiler.context import build_context
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput
    from provisa.core.config_loader import read_config_with_includes, views_as_tables
    from provisa.core.models import config_table_name

    raw = read_config_with_includes(config_path)
    config = views_as_tables(copy.deepcopy(raw))
    tables, column_types = [], {}
    for i, t in enumerate(config.get("tables") or [], start=1):
        columns = [c for c in t.get("columns") or [] if c.get("data_type")]
        tables.append(
            {
                "id": i,
                "source_id": t["source_id"],
                "domain_id": t.get("domain_id", ""),
                "schema_name": t.get("schema", "default"),
                "table_name": config_table_name(t),
                "alias": t.get("alias"),
                **({"view_sql": t["view_sql"]} if "view_sql" in t else {}),
                "enable_aggregates": bool(t.get("enable_aggregates", False)),
                "enable_group_by": bool(t.get("enable_group_by", False)),
                "columns": [
                    {
                        "column_name": c["name"],
                        "visible_to": c.get("visible_to") or [],
                        "data_type": c["data_type"],
                        **({"is_primary_key": True} if c.get("is_primary_key") else {}),
                        **({"alias": c["alias"]} if c.get("alias") else {}),
                    }
                    for c in columns
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        )
        column_types[i] = [
            ColumnMetadata(column_name=c["name"], data_type=c["data_type"], is_nullable=True)
            for c in columns
        ]
    ids = {t.get("alias") or t["table_name"]: t["id"] for t in tables} | {
        t["table_name"]: t["id"] for t in tables
    }
    relationships = [
        {
            **r,
            "source_table_id": ids[r["source_table_id"]],
            "target_table_id": ids[r["target_table_id"]],
            **({"via_table_id": ids[r["via_table"]]} if r.get("via_table") in ids else {}),
        }
        for r in raw.get("relationships") or []
        if r.get("source_table_id") in ids and r.get("target_table_id") in ids
    ]
    role = {"id": "org_admin", "domain_access": ["*"], "capabilities": []}
    naming.configure(gql="apollo_graphql")
    schema_input = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=config.get("domains") or [],
        domain_prefix=(config.get("naming") or {}).get("domain_prefix", False),
    )
    ctx = build_context(schema_input)
    return _module("label_map").CypherLabelMap.from_schema(
        ctx,
        domain_access=["*"],
        all_tables=tables,
        all_relationships=relationships,
        all_column_types=column_types,
    )


def _tour_statements(tour: Path) -> list[str]:
    import yaml

    found: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "cypher" and isinstance(value, str):
                    found.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(yaml.safe_load(tour.read_text()))
    return found


def _bench_queries() -> list[str]:
    sys.path.insert(0, str(REPO / "demo" / "named" / "perf" / "bench"))
    queries = importlib.import_module("queries")
    found = []
    for value in vars(queries).values():
        for item in value if isinstance(value, (list, tuple)) else [value]:
            text = getattr(item, "cypher", None)
            if isinstance(text, str) and text.strip():
                found.append(text)
    return found


#: A Cypher statement written in an e2e spec: a string literal opening with a clause keyword.
_SPEC_CYPHER = re.compile(
    r"""(["'`])((?:MATCH|OPTIONAL MATCH|CALL|UNWIND|WITH|RETURN)\b(?:(?!\1)[^\\]|\\.)*)\1""",
    re.S,
)


def _spec_statements() -> list[str]:
    found = []
    for spec in sorted((REPO / "provisa-ui" / "e2e").glob("*.ts")):
        for quote, text in _SPEC_CYPHER.findall(spec.read_text()):
            if quote == "`" and "${" in text:
                continue  # assembled at run time from values only the test has
            found.append(text.encode().decode("unicode_escape") if "\\" in text else text)
    return found


def _translate_text(recorder: Recorder, label_map: Any, text: str, origin: str) -> None:
    parser = importlib.import_module("provisa.cypher.parser")
    _origin.set(origin)
    try:
        ast = parser.parse_cypher(text)
    except Exception as exc:  # recorded: the same text must be refused the same way
        recorder.record((text, None, label_map, {}), refused(exc))
        return
    try:
        _module("translator").cypher_to_sql(ast, label_map, {})
    except Exception:
        pass  # recorded by the translate hook


def capture_corpora(out: Path) -> None:
    recorder = Recorder(out)
    recorder.install()
    models: list[tuple[str, Path, list[str]]] = []
    for tour in sorted((REPO / "demo" / "named").glob("*/tour.yaml")):
        statements = _tour_statements(tour)
        if statements:
            models.append((f"tour:{tour.parent.name}", tour.parent / "config.yaml", statements))
    models.append(
        ("bench:perf", REPO / "demo" / "named" / "perf" / "config.yaml", _bench_queries())
    )
    models.append(("e2e", REPO / "config" / "provisa.yaml", _spec_statements()))
    for origin, config, statements in models:
        label_map = model_label_map(config)
        for i, text in enumerate(statements):
            _translate_text(recorder, label_map, text, f"{origin}#{i}")


def main(argv: list[str]) -> int:
    command, out = argv[0], Path(argv[1])
    if command == "capture-tests":
        return int(capture_tests(out, argv[2:]))
    if command == "capture-approved-tests":
        return int(capture_tests(out, argv[3:], approved=argv[2]))
    if command == "capture-corpora":
        capture_corpora(out)
        return 0
    raise SystemExit(f"unknown command {command!r}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
