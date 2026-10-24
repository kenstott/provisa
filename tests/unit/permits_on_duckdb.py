# Copyright (c) 2026 Kenneth Stott
# Canary: 988a88a6-0488-4a32-91a0-f693274b5970
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The files demo's permits table on an in-memory DuckDB, for tests that answer a statement over
rows they choose: SQL as the engine runs it (provisa.transpiler.transpile), and Cypher through
the Cypher front end to that SQL."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import duckdb

_REPO = Path(__file__).resolve().parents[2]
CONFIG = _REPO / "demo" / "named" / "files" / "config.yaml"


def model() -> tuple[Any, Any, dict]:
    """The demo's (compilation context, Cypher label map, view SQL)."""
    spec = importlib.util.spec_from_file_location(
        "graphql_golden", _REPO / "scripts" / "graphql_golden.py"
    )
    assert spec is not None and spec.loader is not None
    golden = importlib.util.module_from_spec(spec)
    sys.modules["graphql_golden"] = golden  # its dataclass reads its own module
    spec.loader.exec_module(golden)
    _schema, ctx = golden.model(CONFIG)
    return ctx, golden._cypher_golden().model_label_map(CONFIG), golden._view_sql(CONFIG)


def answer(sql: str, params: list, model: tuple, rows: list[dict]) -> list:
    """The first column of semantic ``sql``'s answer over a permits table of ``rows``."""
    from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
    from provisa.compiler.view_expand import expand_view_refs
    from provisa.transpiler.transpile import transpile_to_duckdb

    ctx, _lm, view_sql = model
    physical = lower_to_catalog_physical(
        expand_view_refs(make_semantic_sql(sql, ctx), view_sql), ctx
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS \"drop\"")
    con.execute('CREATE SCHEMA "drop"."drop"')
    con.execute(
        'CREATE TABLE "drop"."drop".building_permits (permit_id INTEGER, address VARCHAR, '
        "neighborhood VARCHAR, permit_type VARCHAR, issued_on DATE, expires_on DATE, "
        "status VARCHAR, valuation DOUBLE)"
    )
    for i, row in enumerate(rows):
        columns = ["permit_id", *row]
        con.execute(
            f'INSERT INTO "drop"."drop".building_permits ({", ".join(columns)}) '
            f"VALUES ({', '.join('?' for _ in columns)})",
            [i, *row.values()],
        )
    out = con.execute(transpile_to_duckdb(physical), params).fetchall()
    con.close()
    return [r[0] for r in out]


def cypher_answer(text: str, model: tuple, rows: list[dict]) -> list:
    """Cypher ``text``'s answer, through the Cypher front end, over a permits table of ``rows``."""
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.graph_rewriter import apply_graph_rewrites
    from provisa.gql.translator import cypher_to_sql

    _ctx, lm, _view_sql = model
    sql, _params, graph_vars = cypher_to_sql(parse_cypher(text), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    return answer(sql.sql(dialect="postgres"), [], model, rows)
