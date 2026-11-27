# Copyright (c) 2026 Kenneth Stott
# Canary: 20cf12fe-1e13-45ba-b297-3c9c1e3425bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2174 (amended 2026-11-27, THE ONE QUERY IS A REACH FILTER): an ontology snippet -- one
root field whose where keeps the terms within the chosen hops of the focus, each term read once
with its full selection and its edges' far ends by id -- compiles and answers within a bound over
a scaled ontology on the control plane's PostgreSQL, at every fixed hop count.

Nesting the hops both ways (2026-11-26) repeated a term once per path reaching it: at depth 4 a
product's answer was about 13 MB over 300 terms and took 6-9 s. The reach filter reads each term
once, so the answer is at most one row per term.

The ontology is the startup seed's views over 300 terms, 600 typed edges, 60 tables of five
columns, refs from two thirds of the terms, class terms and ten data products; the queries are
the UI's own (tests/fixtures/graphql_golden/ontology_queries.json, written by
ontologyQueryCorpus.test.ts), compiled as the golden harness compiles them."""

# Requirements: REQ-2174

from __future__ import annotations

import asyncio
import importlib.util
import json
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_READS = json.loads(
    (_REPO / "tests" / "fixtures" / "graphql_golden" / "ontology_queries.json").read_text()
)
#: Seconds each of compiling and answering a snippet may take.
_BOUND_S = 2.0
_TERMS = 300
_FOCI = ("term:7", "p:1")
_DEPTHS = (1, 2, 3, 4)


def _golden():
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", _REPO / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = module  # its dataclasses read their own module
    spec.loader.exec_module(module)
    return module


def _seed_catalog(plane) -> None:
    """The meta and ontology schemas as the server's startup seed lays them out."""
    from provisa.api.startup_seed import (
        _seed_meta_domain,
        _seed_meta_relationships,
        _seed_ontology_domain,
    )
    from provisa.core.schema_org import sources

    db = plane.org()

    async def run() -> None:
        async with db.acquire() as conn:
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
            await _seed_ontology_domain(conn, "default")
            await _seed_meta_relationships(conn)

    with ThreadPoolExecutor(1) as pool:
        pool.submit(asyncio.run, run()).result()


def _seed_ontology(conn, n: int) -> None:
    rnd = random.Random(42)
    cur = conn.cursor()
    cur.execute("SET search_path TO org_default")
    domains = ["sales", "finance", "ops"]
    for d in domains:
        cur.execute("INSERT INTO domains (id) VALUES (%s) ON CONFLICT DO NOTHING", (d,))
    cur.execute("INSERT INTO sources (id, type) VALUES ('demo', 'postgresql')")
    n_tables = n // 5
    for t in range(1, n_tables + 1):
        cur.execute(
            "INSERT INTO registered_tables (id, source_id, domain_id, schema_name, table_name,"
            " description) VALUES (%s, 'demo', %s, 'public', %s, 'a table')",
            (100000 + t, domains[t % 3], f"t{t}"),
        )
        for col in range(5):
            cur.execute(
                "INSERT INTO table_columns (table_id, column_name, data_type)"
                " VALUES (%s, %s, 'text')",
                (100000 + t, f"c{col}"),
            )
    for i in range(1, n + 1):
        cur.execute(
            "INSERT INTO glossary_terms (id, name, definition, approved) VALUES (%s, %s, %s, %s)",
            (i, f"term {i}", f"definition of term {i}", rnd.random() < 0.7),
        )
        cur.execute(
            "INSERT INTO glossary_term_experts (term_id, user_id) VALUES (%s, %s)",
            (i, f"u{i % 17}"),
        )
    types = ["KIND_OF", "PART_OF", "RELATED_TO", "SYNONYM_OF", "VALID_VALUE_OF"]
    edges: set[tuple[int, int, str]] = set()
    while len(edges) < 2 * n:
        a, b = rnd.randint(1, n), rnd.randint(1, n)
        if a != b:
            edges.add((a, b, rnd.choice(types)))
    for a, b, rel in sorted(edges):
        cur.execute(
            "INSERT INTO glossary_term_edges (from_term_id, to_term_id, rel_type)"
            " VALUES (%s, %s, %s)",
            (a, b, rel),
        )
    columns = [(100000 + t, f"c{col}") for t in range(1, n_tables + 1) for col in range(5)]
    rnd.shuffle(columns)
    for i, (table_id, column) in zip(range(1, n + 1), columns):
        if i % 3:
            cur.execute(
                "INSERT INTO glossary_term_refs (term_id, table_id, column_name)"
                " VALUES (%s, %s, %s)",
                (i, table_id, column),
            )
    for t in range(1, n_tables + 1, 5):
        cur.execute(
            "UPDATE registered_tables SET class_term_id = %s WHERE id = %s",
            (rnd.randint(1, n), 100000 + t),
        )
    for p in range(1, 11):
        cur.execute(
            "INSERT INTO data_products (id, domain_id, name, status) VALUES (%s, %s, %s, 'live')",
            (str(p), domains[p % 3], f"product {p}"),
        )
        for t in rnd.sample(range(1, n_tables + 1), 6):
            cur.execute(
                "INSERT INTO data_product_tables (product_id, table_id) VALUES (%s, %s)",
                (str(p), 100000 + t),
            )
    cur.execute("ANALYZE")
    conn.commit()


def _on_postgres(stmt: dict) -> tuple[str, list]:
    """The physical statement as the DIRECT route runs it on the control plane: each table by its
    view (the control-plane aliases), placeholders bound in order."""
    import sqlglot

    from provisa.api._ontology_views import ONTOLOGY_TABLE_ALIAS
    from provisa.api.app_loaders import _META_TABLE_ALIAS

    alias = {**_META_TABLE_ALIAS, **ONTOLOGY_TABLE_ALIAS}
    tree = sqlglot.parse_one(stmt["sql"], read="postgres")
    for table in tree.find_all(sqlglot.exp.Table):
        table.set("catalog", None)
        if table.name in alias:
            table.set("this", sqlglot.exp.to_identifier(alias[table.name], quoted=True))
    order: list[int] = []

    def placeholder(m: re.Match) -> str:
        order.append(int(m.group(1)) - 1)
        return "%s"

    text = re.sub(r"\$(\d+)", placeholder, tree.sql(dialect="postgres").replace("%", "%%"))
    return text, [stmt["params"][i] for i in order]


@pytest.fixture(scope="module")
def scaled_ontology(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    import psycopg

    plane = clone(embedded_postgres, control_plane_template)
    try:
        _seed_catalog(plane)
        with psycopg.connect(embedded_postgres.dsn(plane.name)) as conn:
            _seed_ontology(conn, _TERMS)
        yield embedded_postgres.dsn(plane.name)
    finally:
        plane.close()


@pytest.mark.parametrize("depth", _DEPTHS)
@pytest.mark.parametrize("focus", _FOCI)
def test_a_snippet_compiles_and_answers_within_the_bound(scaled_ontology, focus, depth):
    import psycopg

    golden = _golden()
    schema, ctx = golden.model(golden.META_MODEL)
    view_sql = golden._view_sql(golden.META_MODEL)
    text = _READS[f"ontology snippet: focus={focus} lanes=false depth={depth}"]
    read = golden.CorpusRead("bound", golden.META_MODEL, text)
    started = time.perf_counter()
    stmts = golden.physical_statements(schema, ctx, read, view_sql)
    compiled = time.perf_counter() - started
    assert compiled < _BOUND_S, f"compiling took {compiled:.2f}s"
    for stmt in stmts:
        sql, params = _on_postgres(stmt)
        with psycopg.connect(scaled_ontology) as conn:
            conn.execute("SET search_path TO org_default")
            conn.execute(f"SET statement_timeout = '{int(_BOUND_S * 10)}s'")
            started = time.perf_counter()
            rows = conn.execute(sql, params).fetchall()
            answered = time.perf_counter() - started
        assert answered < _BOUND_S, f"{stmt['root_field']} answered in {answered:.2f}s"
        if stmt["root_field"] == "_terms":
            # Each term within reach once: never once per path reaching it.
            assert 1 <= len(rows) <= _TERMS
