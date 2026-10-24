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
    python scripts/graphql_golden.py capture-answers
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
    """The meta model's GraphQL schema as a client reads it: its types' fields and each field's
    type (the part of an introspection the UI's RoleSchema reads), and each meta table's root
    field (what /data/graph-schema's root_fields says), for the UI's query builders."""
    from graphql import GraphQLList, GraphQLNonNull, get_named_type

    schema, ctx = model(META_MODEL)

    def ref(t: Any) -> dict:
        if isinstance(t, GraphQLNonNull):
            return {"kind": "NON_NULL", "name": None, "ofType": ref(t.of_type)}
        if isinstance(t, GraphQLList):
            return {"kind": "LIST", "name": None, "ofType": ref(t.of_type)}
        named = get_named_type(t)
        kind = (
            "OBJECT" if hasattr(named, "fields") and not hasattr(named, "is_one_of") else "SCALAR"
        )
        return {"kind": kind, "name": named.name}

    types = [
        {
            "name": name,
            "fields": [{"name": f, "type": ref(field.type)} for f, field in t.fields.items()],
        }
        for name, t in schema.type_map.items()
        if not name.startswith("__") and hasattr(t, "fields") and not hasattr(t, "is_one_of")
    ]
    introspection = {"__schema": {"queryType": {"name": schema.query_type.name}, "types": types}}
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
    return {"introspection": introspection, "root_fields": roots}


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
    if command in ("capture-meta", "capture-answers"):
        import os
        import subprocess

        env = {**os.environ, "PROVISA_GRAPHQL_GOLDEN_CAPTURE": "1"}
        test = {
            "capture-meta": "tests/unit/test_graphql_meta_model.py",
            "capture-answers": "tests/unit/test_graphql_answers.py",
        }[command]
        return subprocess.call(  # noqa: S603 -- the repository's own test, by its path
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", test],
            cwd=REPO,
            env=env,
        )
    if command == "counts":
        print(json.dumps(counts()))
        return 0
    raise SystemExit(f"unknown command {command!r}")


# --- answers --------------------------------------------------------------------------------------
#
# A read whose tables are all a demo's PostgreSQL sources is answered on DuckDB, each source's
# database attached under its catalog, as the engine reads the physical SQL; a read of one source
# is answered on that database directly too, as the DIRECT route runs it. The databases are seeded
# from the demo's own seed SQL, as its docker-compose stack does. The statement run is the read's
# semantic SQL with its views expanded, made physical (view_expand, sql_rewrite), as the
# administrator: the baseline of what each read answers, not of governance (its own tests).

ANSWERS = GOLDEN / "answers.jsonl"
_ENV_REF = re.compile(r"\$\{env:[^}]*\}")
#: A seed's login: created once per server, whichever demo's seed made it first (roles are the
#: server's, and the shared test server holds every demo's databases).
_CREATE_ROLE = re.compile(r"^\s*CREATE\s+ROLE\s+(\w+)(.*);\s*$", re.I)
#: What a seed grants or sets for its logins: the test server's own login reads every database.
_ROLE_LINE = re.compile(
    r"^\s*(ALTER|DROP)\s+ROLE\b|^\s*(GRANT|REVOKE)\b|^\s*ALTER\s+DEFAULT\s+PRIVILEGES\b", re.I
)
_CREATE_DB = re.compile(r"^\s*CREATE\s+DATABASE\s+(\w+)", re.I)
_CONNECT = re.compile(r"^\s*\\c(?:onnect)?\s+(\w+)\s*$")
_COPY = re.compile(r"^\s*\\copy\s+(.+?)\s+FROM\s+'([^']+)'(.*)$", re.I)
_INITDB = "/docker-entrypoint-initdb.d"


@dataclass(frozen=True)
class _Service:
    """A PostgreSQL service of a demo's stack: its database, its seed files in the order the image
    runs them, and where each path under docker-entrypoint-initdb.d is on this machine."""

    database: str
    seeds: list[Path]
    mounts: list[tuple[str, Path]]

    def local(self, path: str) -> Path:
        target, source = max(
            ((t, s) for t, s in self.mounts if path == t or path.startswith(t + "/")),
            key=lambda m: len(m[0]),
        )
        return source if path == target else source / path[len(target) + 1 :]


def _compose_seeds(demo: Path) -> list[_Service]:
    """Each PostgreSQL service of the demo's stack, its logins' own seed (reader) left out."""
    import yaml

    compose = yaml.safe_load((demo / "docker-compose.yml").read_text())
    out = []
    for service in (compose.get("services") or {}).values():
        env = service.get("environment") or {}
        if not isinstance(env, dict) or "POSTGRES_DB" not in env:
            continue
        mounts: list[tuple[str, Path]] = []
        named: list[tuple[str, Path]] = []
        for volume in service.get("volumes") or []:
            if not isinstance(volume, str) or _INITDB not in volume:
                continue
            source, target = volume.split(":")[:2]
            path = (demo / source).resolve()
            mounts.append((target.rstrip("/"), path))
            if path.is_dir():
                if target.rstrip("/") == _INITDB:
                    named += [(f.name, f) for f in path.glob("*.sql")]
            else:
                named.append((Path(target).name, path))
        seeds = [f for name, f in sorted(named) if f.suffix == ".sql" and "reader" not in f.name]
        out.append(_Service(str(env["POSTGRES_DB"]), seeds, mounts))
    return out


def _pg_sources(config: Path) -> dict[str, str]:
    """The demo's PostgreSQL sources: each one's database, its namespace suffix left out."""
    import yaml

    raw = yaml.safe_load(config.read_text())
    return {
        s["id"]: _ENV_REF.sub("", str(s["database"]))
        for s in raw.get("sources") or []
        if s.get("type") == "postgresql"
    }


def seed_demo(pg: Any, demo: Path, prefix: str) -> dict[str, str]:
    """Seed ``demo``'s databases on ``pg`` (a tests.embedded_pg.LocalPostgres) as its stack does,
    each named ``<prefix>_<database>``: the seed's own database name to the one it was created as.
    Each seed file runs in one session per database it connects to, as psql runs it."""
    import psycopg

    created: dict[str, str] = {}

    def admin(statement: str) -> None:
        with psycopg.connect(pg.dsn(), autocommit=True) as conn:
            conn.execute(statement)

    def database(name: str) -> str:
        if name not in created:
            created[name] = f"{prefix}_{name}"
            admin(f'CREATE DATABASE "{created[name]}"')
        return created[name]

    for service in _compose_seeds(demo):
        for seed in service.seeds:
            sessions: dict[str, Any] = {}

            def session(db: str) -> Any:
                if db not in sessions:
                    sessions[db] = psycopg.connect(pg.dsn(db), autocommit=True)
                return sessions[db]

            current = database(service.database)
            chunk: list[str] = []

            def flush() -> None:
                text = "\n".join(chunk)
                chunk.clear()
                if text.strip():
                    session(current).execute(text)

            try:
                for line in seed.read_text().splitlines():
                    role = _CREATE_ROLE.match(line)
                    if role:
                        flush()
                        exists = session(current).execute(
                            "SELECT 1 FROM pg_roles WHERE rolname = %s", [role.group(1)]
                        )
                        if exists.fetchone() is None:
                            admin(line)
                        continue
                    if _ROLE_LINE.match(line):
                        continue
                    made = _CREATE_DB.match(line)
                    if made:
                        database(made.group(1))
                        continue
                    connect = _CONNECT.match(line)
                    if connect:
                        flush()
                        current = database(connect.group(1))
                        continue
                    copy = _COPY.match(line)
                    if copy:
                        flush()
                        data = service.local(copy.group(2)).read_bytes()
                        with (
                            session(current)
                            .cursor()
                            .copy(f"COPY {copy.group(1)} FROM STDIN{copy.group(3)}") as cp
                        ):
                            cp.write(data)
                        continue
                    # A server-side COPY reads the container's path: here, the file it mounts.
                    for target, source in service.mounts:
                        if f"'{target}" in line:
                            line = re.sub(
                                rf"'({re.escape(target)}[^']*)'",
                                lambda m: f"'{service.local(m.group(1))}'",
                                line,
                            )
                    chunk.append(line)
                flush()
            finally:
                for conn in sessions.values():
                    conn.close()
    return created


def _plain(value: Any) -> Any:
    """A cell as JSON holds it: a number, a string, a list, a map, or null."""
    import datetime
    import decimal
    import uuid

    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        # An instant, whichever zone the engine shows it in.
        return value.astimezone(datetime.timezone.utc).isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value.normalize()) if value == value.to_integral() else str(value)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time, uuid.UUID)):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    if isinstance(value, str) and value[:1] in "[{":
        try:
            return json.loads(value)
        except ValueError:
            return value
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def physical_statements(schema: Any, ctx: Any, read: CorpusRead, view_sql: dict) -> list[dict]:
    """Each root of ``read`` as the statement the engine runs (its views expanded, physical), with
    its parameters, the catalogs it reads and whether it orders its rows."""
    import sqlglot
    from graphql import parse

    from provisa.compiler.parser import coerce_variable_defaults
    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
    from provisa.compiler.view_expand import expand_view_refs

    document = parse(read.text)
    out = []
    for root in compile_query(document, ctx, coerce_variable_defaults(document, read.variables)):
        semantic = make_semantic_sql(root.sql, ctx)
        # Lowered the one way every surface lowers a governed statement (REQ-641).
        physical = lower_to_catalog_physical(expand_view_refs(semantic, view_sql), ctx)
        tree = sqlglot.parse_one(physical, read="postgres")
        out.append(
            {
                "root_field": root.root_field,
                "sql": physical,
                "params": list(root.params),
                "catalogs": sorted(
                    {t.catalog for t in tree.find_all(sqlglot.exp.Table) if t.catalog}
                ),
                "ordered": tree.args.get("order") is not None,
            }
        )
    return out


def _rows(cursor: Any) -> tuple[list[str], list[list]]:
    columns = [d[0] for d in cursor.description]
    return columns, [[_plain(v) for v in row] for row in cursor.fetchall()]


def answer_on_duckdb(pg: Any, databases: dict[str, str], stmt: dict, plain: bool = True) -> dict:
    """``stmt`` run on DuckDB, each catalog it reads attached from its seeded database; its cells
    as JSON holds them, or (``plain`` false) as DuckDB returns them."""
    import duckdb

    from provisa.transpiler.transpile import transpile_to_duckdb

    con = duckdb.connect()
    for catalog in stmt["catalogs"]:
        con.execute(
            f"ATTACH '{pg.dsn(databases[catalog])}' AS \"{catalog}\" (TYPE POSTGRES, READ_ONLY)"
        )
    # As the DuckDB engine writes a statement (provisa.transpiler.transpile).
    cursor = con.execute(transpile_to_duckdb(stmt["sql"]), stmt["params"])
    if plain:
        columns, rows = _rows(cursor)
    else:
        columns, rows = [d[0] for d in cursor.description], [list(r) for r in cursor.fetchall()]
    con.close()
    return {"columns": columns, "rows": rows}


def answer_on_postgres(pg: Any, databases: dict[str, str], stmt: dict) -> dict:
    """``stmt``, of one catalog, run on its database directly (the DIRECT route's statement: the
    table addressed by its schema)."""
    import psycopg
    import sqlglot

    (catalog,) = stmt["catalogs"]
    tree = sqlglot.parse_one(stmt["sql"], read="postgres")
    for table in tree.find_all(sqlglot.exp.Table):
        table.set("catalog", None)
    order: list[int] = []

    def placeholder(m: re.Match) -> str:
        order.append(int(m.group(1)) - 1)
        return "%s"

    text = re.sub(r"\$(\d+)", placeholder, tree.sql(dialect="postgres").replace("%", "%%"))
    with psycopg.connect(pg.dsn(databases[catalog])) as conn:
        columns, rows = _rows(conn.execute(text, [stmt["params"][i] for i in order]))
    return {"columns": columns, "rows": rows}


def _view_sql(config: Path) -> dict[str, str]:
    _schema_input, tables, _rels, _types = _cypher_golden().model_inputs(config, parameters=True)
    return {t["table_name"]: t["view_sql"] for t in tables if "view_sql" in t}


def demo_answers(pg: Any, demo: Path, prefix: str) -> list[dict]:
    """What each corpus read of ``demo`` answers, root by root: on DuckDB when every table it
    reads is one of the demo's PostgreSQL sources, and on PostgreSQL too when it reads one; a read
    of any other source says so (it is answered in the integration lane)."""
    config = demo / "config.yaml"
    reads = [r for r in corpus() if r.config == config]
    if not reads:
        return []
    created = seed_demo(pg, demo, prefix)
    catalogs = {
        source_id.replace("-", "_"): created[db]
        for source_id, db in _pg_sources(config).items()
        if db in created
    }
    schema, ctx = model(config)
    view_sql = _view_sql(config)
    out = []
    for read in reads:
        for stmt in physical_statements(schema, ctx, read, view_sql):
            entry = {
                "origin": read.origin,
                "root_field": stmt["root_field"],
                "ordered": stmt["ordered"],
            }
            outside = [c for c in stmt["catalogs"] if c not in catalogs]
            if outside or not stmt["catalogs"]:
                out.append({**entry, "answered": False, "reads": stmt["catalogs"]})
                continue
            engines = {"duckdb": answer_on_duckdb(pg, catalogs, stmt)}
            if len(stmt["catalogs"]) == 1:
                engines["postgres"] = answer_on_postgres(pg, catalogs, stmt)
            for engine, answer in engines.items():
                # The rows as a multiset: an ORDER BY with ties orders them as the engine likes,
                # and the order a read asks for is the semantic SQL golden's to hold.
                rows = sorted(answer["rows"], key=repr)
                out.append({**entry, "engine": engine, "columns": answer["columns"], "rows": rows})
    return out


def answered_demos() -> list[Path]:
    """The demos whose reads the answers baseline runs: each with a PostgreSQL source its own
    stack seeds."""
    return sorted(
        d
        for d in (REPO / "demo" / "named").iterdir()
        if (d / "queries").is_dir()
        and (d / "docker-compose.yml").exists()
        and _pg_sources(d / "config.yaml")
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


def _cypher_value(value: Any) -> Any:
    """A Cypher answer's cell as JSON holds it: a map's values are JSON text (MAP(VARCHAR, JSON)),
    and what that text decodes to is plain JSON already."""
    if isinstance(value, dict):
        return {k: json.loads(v) if isinstance(v, str) else v for k, v in value.items()}
    if isinstance(value, list):
        return [_cypher_value(v) for v in value]
    return value


def _compared(value: Any) -> Any:
    """A cell as the gate compares it: a number as a float, whichever type it had (a decimal sum is
    a JSON number in a Cypher map), anything else as JSON holds it."""
    import decimal

    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_compared(v) for v in value]
    if isinstance(value, dict):
        return {k: _compared(v) for k, v in value.items()}
    return _plain(value)


def _canonical(row: dict) -> str:
    """A response object as text that does not depend on its keys' order: rows held as a multiset
    are sorted by it."""
    return json.dumps(row, sort_keys=True, default=str)


def front_answers(pg: Any, demo: Path, prefix: str) -> list[dict]:
    """Each root of each corpus read of ``demo`` as the GraphQL front end lowers it, printed as
    Cypher and read back by the Cypher front end, answered on DuckDB over the seeded sources, with
    the read's own statement's answer there (the one answers/ holds) as the GraphQL endpoint
    serializes it ('expected'); a root the front end does not lower says why ('lowered')."""
    from graphql import parse

    from provisa.compiler.parser import coerce_variable_defaults
    from provisa.compiler.sql_gen import compile_query
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
    from provisa.compiler.view_expand import expand_view_refs
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.printer import print_cypher
    from provisa.executor.serialize import serialize_aggregate, serialize_rows
    from provisa.gql.assembler import assemble_rows
    from provisa.gql.graph_rewriter import apply_graph_rewrites
    from provisa.gql.translator import cypher_to_sql
    from provisa.graphql_front.lower import GraphQLLoweringRefused, lower_read

    config = demo / "config.yaml"
    reads = [r for r in corpus() if r.config == config]
    if not reads:
        return []
    created = seed_demo(pg, demo, prefix)
    catalogs = {
        source_id.replace("-", "_"): created[db]
        for source_id, db in _pg_sources(config).items()
        if db in created
    }
    schema, ctx = model(config)
    view_sql = _view_sql(config)
    lm = _cypher_golden().model_label_map(config)
    out = []
    for read in reads:
        statements = {s["root_field"]: s for s in physical_statements(schema, ctx, read, view_sql)}
        roots = list(statements)
        try:
            lowered = lower_read(parse(read.text), ctx, lm, read.variables)
        except GraphQLLoweringRefused as exc:
            out += [{"origin": read.origin, "root_field": r, "lowered": str(exc)} for r in roots]
            continue
        document = parse(read.text)
        compiled = {
            c.root_field: c
            for c in compile_query(
                document, ctx, coerce_variable_defaults(document, read.variables)
            )
        }
        for root_field, root in zip(roots, lowered, strict=True):
            cypher = print_cypher(root.statement)
            sql_ast, _params, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
            sql_ast = apply_graph_rewrites(sql_ast, graph_vars, lm)
            semantic = make_semantic_sql(sql_ast.sql(dialect="postgres"), ctx)
            physical = lower_to_catalog_physical(expand_view_refs(semantic, view_sql), ctx)
            import sqlglot

            tree = sqlglot.parse_one(physical, read="postgres")
            stmt = {
                "sql": physical,
                "params": [],
                "catalogs": sorted(
                    {t.catalog for t in tree.find_all(sqlglot.exp.Table) if t.catalog}
                ),
            }
            if [c for c in stmt["catalogs"] if c not in catalogs] or not stmt["catalogs"]:
                out.append(
                    {
                        "origin": read.origin,
                        "root_field": root_field,
                        "lowered": "reads other sources",
                    }
                )
                continue
            answer = answer_on_duckdb(pg, catalogs, stmt, plain=False)
            named = [dict(zip(answer["columns"], r, strict=True)) for r in answer["rows"]]
            rows = [
                {k: _compared(_cypher_value(v)) for k, v in r.items()}
                for r in assemble_rows(named, graph_vars)
            ]
            own = answer_on_duckdb(pg, catalogs, statements[root_field], plain=False)
            own_rows = [tuple(_compared(v) for v in r) for r in own["rows"]]
            c = compiled[root_field]
            if c.canonical_field.endswith(("Aggregate", "_aggregate")):
                # One object, the aggregate's (its statement's one row, its nodes a column of
                # it), its nodes in no order: compared as a multiset.
                one = serialize_aggregate(own_rows, c.columns, root_field, c.agg_alias)["data"][
                    root_field
                ]
                expected = [one]
                for row in (*expected, *rows):
                    if "nodes" in row:
                        row["nodes"] = sorted(row["nodes"], key=_canonical)
            else:
                expected = serialize_rows(own_rows, c.columns, root_field)["data"][root_field]
            out.append(
                {
                    "origin": read.origin,
                    "root_field": root_field,
                    "lowered": True,
                    "cypher": cypher,
                    "rows": sorted(rows, key=_canonical),
                    "expected": sorted(expected, key=_canonical),
                }
            )
    return out
