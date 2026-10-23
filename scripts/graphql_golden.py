# Copyright (c) 2026 Kenneth Stott
# Canary: a2a21bd1-e243-43e4-b9eb-0622f6d4d774
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The GraphQL corpus and its semantic SQL (REQ-2135, REQ-863): the baseline GraphQL's move into
the graph IR is held to. Every GraphQL data read the repository has, with the semantic SQL each of
its root fields compiles to today, replayed by tests/unit/test_graphql_golden_sql.py against the
working tree, which must produce the same SQL byte for byte (or the same refusal).

The corpus:

- every named demo's queries (demo/named/<demo>/queries/*.graphql), against the demo's own model;
- every GraphQL read of the perf bench, against the perf demo's model;
- every GraphQL data read of the UI e2e specs, against the e2e model (config/provisa.yaml): a
  string literal opening with ``{`` or ``query`` that validates against that model's schema
  (an admin API call does not, and is no data read).

Each root's semantic SQL is held by its SHA-256 (the corpus is large); a replay that differs
prints the SQL it produced. A model's schema and compilation context are built offline from its config, as the server builds
them for the role org_admin (cypher_golden.model_inputs).

Usage:

    python scripts/graphql_golden.py capture-meta
    python scripts/graphql_golden.py capture OUT
    python scripts/graphql_golden.py counts
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
GOLDEN = REPO / "tests" / "fixtures" / "graphql_golden"
E2E_MODEL = REPO / "config" / "provisa.yaml"
PERF = REPO / "demo" / "named" / "perf"

#: A GraphQL read written in an e2e spec: a string literal opening with ``{`` or ``query``.
_SPEC_GRAPHQL = re.compile(r"""(["'`])((?:\{|query\b)(?:(?!\1)[^\\]|\\.)*)\1""", re.S)


@dataclass(frozen=True)
class CorpusRead:
    """One GraphQL read of the corpus: where it came from, its model's config, its text and the
    variables it is run with."""

    origin: str
    config: Path
    text: str
    variables: dict | None = None


def _cypher_golden() -> Any:
    spec = importlib.util.spec_from_file_location(
        "cypher_golden", REPO / "scripts" / "cypher_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["cypher_golden"] = module
    spec.loader.exec_module(module)
    return module


def model(config_path: Path) -> tuple[Any, Any]:
    """``config_path``'s model as the server builds it for org_admin: (its GraphQL schema, its
    compilation context)."""
    from provisa.compiler.context import build_context
    from provisa.compiler.schema_gen import generate_schema

    schema_input, *_ = _cypher_golden().model_inputs(config_path, parameters=True)
    return generate_schema(schema_input), build_context(schema_input)


META_MODEL = GOLDEN / "meta_model.yaml"


async def meta_model_config(db: Any) -> dict:
    """The meta domain as the server registers it (startup_seed), read back from ``db`` -- a
    fresh control plane -- as a model config: its tables with their columns, and their
    relationships. The Ontology View's queries are reads of it."""
    from sqlalchemy import select

    from provisa.api.startup_seed import _seed_meta_domain, _seed_meta_relationships
    from provisa.core.schema_org import relationships, registered_tables, sources, table_columns

    async with db.acquire() as conn:
        # The control-plane self-catalog the meta tables are registered under, as the boot's
        # built-in source seed writes it (startup_seed._seed_built_in_sources).
        await conn.upsert(
            sources,
            {
                "id": "provisa-admin",
                "type": "postgresql",
                "database": "provisa",
                "dialect": "postgresql",
            },
            index_elements=["id"],
            update_columns=[],
        )
        await _seed_meta_domain(conn, "default")
        await _seed_meta_relationships(conn)
        tables = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.domain_id == "meta")
            )
        ).fetchall()
        names = {t.id: str(t.table_name) for t in tables}
        columns = (
            await conn.execute_core(
                select(table_columns).where(table_columns.c.table_id.in_(list(names)))
            )
        ).fetchall()
        rels = (await conn.execute_core(select(relationships))).fetchall()
    out_tables = []
    for t in sorted(tables, key=lambda t: t.table_name):
        out_tables.append(
            {
                "source_id": str(t.source_id),
                "domain_id": "meta",
                "schema": str(t.schema_name),
                "table": str(t.table_name),
                "columns": [
                    {
                        "name": str(c.column_name),
                        "data_type": str(c.data_type),
                        **({"is_primary_key": True} if c.is_primary_key else {}),
                    }
                    for c in sorted(columns, key=lambda c: c.column_name)
                    if c.table_id == t.id
                ],
            }
        )
    out_rels = []
    for r in sorted(rels, key=lambda r: r.id):
        if r.source_table_id not in names or r.target_table_id not in names:
            continue  # an edge to the ops domain: not the meta model's
        row = {
            str(k): (str(v) if isinstance(v, str) else v)
            for k, v in r._asdict().items()
            if v is not None and k not in ("source_table_id", "target_table_id", "via_table_id")
        }
        row["source_table_id"] = names[r.source_table_id]
        row["target_table_id"] = names[r.target_table_id]
        if r.via_table_id is not None:
            row["via_table"] = names[r.via_table_id]
        out_rels.append(row)
    return {
        "naming": {"domain_prefix": True},
        "domains": [{"id": "meta", "description": "System metadata"}],
        "sources": [{"id": "provisa-admin", "type": "postgresql"}],
        "tables": out_tables,
        "relationships": out_rels,
    }


META_SCHEMA = GOLDEN / "meta_schema.json"


def meta_schema_doc() -> dict:
    """The meta model's GraphQL schema as a client reads it: its introspection, and each meta
    table's root field (what /data/graph-schema's root_fields says), for the UI's query builders."""
    from graphql import introspection_from_schema

    schema, ctx = model(META_MODEL)
    roots = sorted(
        (
            {
                "domain_id": meta.domain_id,
                "table_name": meta.original_table_name or meta.table_name,
                "graphql_field": field,
            }
            for field, meta in ctx.tables.items()
            if field in schema.query_type.fields
        ),
        key=lambda r: r["table_name"],
    )
    return {"introspection": introspection_from_schema(schema), "root_fields": roots}


def _demo_reads() -> list[CorpusRead]:
    found = []
    for path in sorted((REPO / "demo" / "named").glob("*/queries/*.graphql")):
        demo = path.parent.parent
        found.append(
            CorpusRead(f"demo:{demo.name}/{path.name}", demo / "config.yaml", path.read_text())
        )
    return found


def _bench_reads() -> list[CorpusRead]:
    sys.path.insert(0, str(PERF / "bench"))
    queries = importlib.import_module("queries")
    found = []
    for name, value in vars(queries).items():
        for i, item in enumerate(value if isinstance(value, (list, tuple)) else [value]):
            text = getattr(item, "graphql", None)
            if isinstance(text, str) and text.strip():
                found.append(
                    CorpusRead(f"bench:{item.id}", PERF / "config.yaml", text, dict(item.params))
                )
    return found


def _spec_reads(schema: Any) -> list[CorpusRead]:
    from graphql import parse, validate

    found = []
    for spec in sorted((REPO / "provisa-ui" / "e2e").glob("*.ts")):
        for i, (quote, text) in enumerate(_SPEC_GRAPHQL.findall(spec.read_text())):
            if quote == "`" and "${" in text:
                continue  # assembled at run time from values only the test has
            text = text.encode().decode("unicode_escape") if "\\" in text else text
            try:
                document = parse(text)
                if validate(schema, document):
                    continue  # not a read of the e2e model (an admin API call)
            except Exception:  # noqa: BLE001 -- a literal that is no GraphQL at all
                continue
            if _introspection_only(document):
                continue  # the schema asked about itself: no data read
            found.append(CorpusRead(f"e2e:{spec.name}#{i}", E2E_MODEL, text))
    return found


def _introspection_only(document: Any) -> bool:
    from graphql import OperationDefinitionNode

    fields = [
        sel.name.value
        for d in document.definitions
        if isinstance(d, OperationDefinitionNode)
        for sel in d.selection_set.selections
        if hasattr(sel, "name")
    ]
    return bool(fields) and all(f.startswith("__") for f in fields)


ONTOLOGY_QUERIES = GOLDEN / "ontology_queries.json"


def _ontology_reads() -> list[CorpusRead]:
    """The Ontology View's and the product graph's queries over the grid of their scopes, as the
    UI's builders produce them (ontologyQueryCorpus.test.ts writes and holds the file), against
    the meta model."""
    built = json.loads(ONTOLOGY_QUERIES.read_text())
    return [CorpusRead(f"ontology:{key}", META_MODEL, text) for key, text in built.items()]


def corpus() -> list[CorpusRead]:
    """Every GraphQL read of the corpus: (where it came from, its model's config, its text)."""
    schema, _ctx = model(E2E_MODEL)
    return [*_demo_reads(), *_bench_reads(), *_spec_reads(schema), *_ontology_reads()]


def compile_read(
    schema: Any, ctx: Any, text: str, variables: dict | None = None, *, sql: bool = False
) -> dict:
    """``text``'s per-root semantic SQL against the model (by its hash; ``sql``: the SQL itself
    too, to show what differs), or how it is refused."""
    from graphql import parse, validate

    from provisa.compiler.params import embed_params_comment
    from provisa.compiler.parser import coerce_variable_defaults
    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import make_semantic_sql

    try:
        document = parse(text)
        errors = validate(schema, document)
        if errors:
            return {"refused": "invalid", "error": str(errors[0].message)}
        roots = compile_query(document, ctx, coerce_variable_defaults(document, variables))
    except Exception as exc:  # noqa: BLE001 -- recorded: the same read must be refused the same way
        return {"refused": type(exc).__name__, "error": str(exc)}
    out = []
    for r in roots:
        semantic = make_semantic_sql(embed_params_comment(r.sql, r.params), ctx)
        root = {
            "root_field": r.root_field,
            "semantic_sql_sha256": hashlib.sha256(semantic.encode()).hexdigest(),
        }
        out.append({**root, "semantic_sql": semantic} if sql else root)
    return {"roots": out}


def capture(out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
    models: dict[Path, tuple[Any, Any]] = {}
    entries = []
    for read in corpus():
        if read.config not in models:
            models[read.config] = model(read.config)
        schema, ctx = models[read.config]
        entries.append(
            {
                "origin": read.origin,
                "model": str(read.config.relative_to(REPO)),
                "graphql": read.text,
                "variables": read.variables,
                "expected": compile_read(schema, ctx, read.text, read.variables),
            }
        )
    with (out / "entries.jsonl").open("w") as f:
        for e in entries:
            f.write(json.dumps(e, sort_keys=True) + "\n")
    return len(entries)


def counts() -> dict[str, int]:
    out: dict[str, int] = {}
    for read in corpus():
        kind = read.origin.split(":", 1)[0]
        out[kind] = out.get(kind, 0) + 1
    return out


def main(argv: list[str]) -> int:
    command = argv[0]
    if command == "capture":
        print(capture(Path(argv[1])), "entries")
        return 0
    if command == "capture-meta":
        import os
        import subprocess

        env = {**os.environ, "PROVISA_GRAPHQL_GOLDEN_CAPTURE": "1"}
        test = "tests/unit/test_graphql_meta_model.py"
        return subprocess.call(  # noqa: S603 -- the repository's own test, by its path
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", test],
            cwd=REPO,
            env=env,
        )
    if command == "counts":
        print(json.dumps(counts()))
        return 0
    raise SystemExit(f"unknown command {command!r}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
