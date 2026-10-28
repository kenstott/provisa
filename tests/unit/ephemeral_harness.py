# Copyright (c) 2026 Kenneth Stott
# Canary: 42b2e552-22a6-45fa-bfe2-0d151188026f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's ephemeral graph over one DuckDB connection, for unit tests (REQ-2176): the
session's tables are landed in it as the store holds them, the model's tables sit in its schema
``s``, and a statement runs through the seam exactly as a surface runs it."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import duckdb

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import parse_cypher
from provisa.cypher.write_translator import WriteTranslator, bind_write_params, parse_cypher_write
from provisa.ephemeral import seam
from provisa.gql.assembler import assemble_rows, to_serializable
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap, NodeMapping
from provisa.gql.temporal import bindable
from provisa.gql.translator import cypher_to_sql
from provisa.pgwire import temp_exec
from provisa.transpiler.transpile import transpile_to_duckdb

_DUCK = {
    "text": "TEXT",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "json": "JSON",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "time": "TIME",
    "zoned_time": "TIMETZ",
    "interval": "INTERVAL",
}


def _duck_type(kind: str) -> str:
    return f"{_duck_type(kind[5:-1])}[]" if kind.startswith("list<") else _DUCK[kind]


def model_node(label: str, columns: dict[str, str], domain: str | None = None) -> NodeMapping:
    return NodeMapping(
        label=f"{domain}:{label}" if domain else label,
        type_name=label,
        domain_label=domain,
        table_label=label,
        table_id=abs(hash(label)) % 1000,
        source_id="s",
        id_column="id",
        pk_columns=["id"],
        catalog_name="memory",
        schema_name="s",
        table_name=label.lower(),
        properties={c: c for c in columns},
        physical_properties={c: c for c in columns},
        property_types=tuple(columns.items()),
    )


class _Rows:
    """The store's in-place face (provisa.federation.session_rows) on the harness's DuckDB, whose
    session tables are named alone."""

    def __init__(self, duck: Any) -> None:
        self.duck = duck
        self.calls: list[str] = []

    async def insert(self, schema, table, columns, batch):
        self.calls.append(f"insert {table}")
        names = ", ".join(f'"{c}"' for c, _ in columns)
        self.duck.register("_rows", batch)
        self.duck.execute(f'INSERT INTO "{table}" ({names}) SELECT {names} FROM _rows')
        self.duck.unregister("_rows")

    async def delete(self, schema, table, key_columns, keys):
        self.calls.append(f"delete {table}")
        held = {c: [k[i] for k in keys] for i, c in enumerate(key_columns)}
        import pyarrow as pa

        self.duck.register("_keys", pa.table(held))
        same = " AND ".join(f'k."{c}" = t."{c}"' for c in key_columns)
        self.duck.execute(
            f'DELETE FROM "{table}" t WHERE EXISTS (SELECT 1 FROM _keys k WHERE {same})'
        )
        self.duck.unregister("_keys")

    def _alter(self, table, sql):
        # DuckDB alters no table an index stands on: dropped, altered, built again.
        held = self.duck.execute(
            "SELECT index_name, sql FROM duckdb_indexes() WHERE table_name = ?", [table]
        ).fetchall()
        for name, _ in held:
            self.duck.execute(f'DROP INDEX "{name}"')
        self.duck.execute(sql)
        for _, ddl in held:
            self.duck.execute(ddl)

    async def add_column(self, schema, table, column, kind):
        self.calls.append(f"add {table}.{column}")
        self._alter(table, f'ALTER TABLE "{table}" ADD COLUMN "{column}" {_duck_type(kind)}')

    async def retype(self, schema, table, column, kind):
        self.calls.append(f"retype {table}.{column}")
        self._alter(table, f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {_duck_type(kind)}')

    async def index(self, schema, table, key_columns):
        self.calls.append(f"index {table}")
        cols = ", ".join(f'"{c}"' for c in key_columns)
        self.duck.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "key_{table}" ON "{table}" ({cols})')


class Session:
    """One session: its DuckDB, its model and its temporary tables. ``in_place``: the store
    writes them in place (provisa.federation.session_rows); else each is landed whole."""

    def __init__(
        self,
        monkeypatch: Any,
        model: dict[str, tuple[dict[str, str], str]],
        *,
        in_place: bool = True,
    ) -> None:
        self.duck = duckdb.connect()
        self.duck.execute("SET TimeZone = 'UTC'")
        self.duck.execute("CREATE SCHEMA s")
        nodes = {}
        for label, (columns, values) in model.items():
            ddl = ", ".join(f"{c} {t}" for c, t in columns.items())
            self.duck.execute(f"CREATE TABLE s.{label.lower()} ({ddl})")
            if values:
                self.duck.execute(f"INSERT INTO s.{label.lower()} VALUES {values}")
            nodes[label] = model_node(label, columns)
        self.model = CypherLabelMap(
            nodes=nodes, relationships={}, nodes_by_table={k: [k] for k in nodes}
        )
        self.session = TempSession()
        self.state = SimpleNamespace(
            federation_engine=SimpleNamespace(engine=SimpleNamespace(name="duckdb"))
        )
        duck = self.duck

        async def land(state, session, name, batch, columns):
            ddl = ", ".join(f'"{c}" {_duck_type(k)}' for c, k in columns)
            duck.execute(f'CREATE OR REPLACE TABLE "{name}" ({ddl})')
            duck.register("_batch", batch)
            duck.execute(f'INSERT INTO "{name}" SELECT * FROM _batch')
            duck.unregister("_batch")
            session.stored = True

        async def address(state, session, name):
            return f'"{name}"'

        def system_rows(state, sql, name):
            result = duck.execute(sql)
            return [d[0] for d in result.description], result.fetchall()

        self.rows = _Rows(duck) if in_place else None
        monkeypatch.setattr(temp_exec, "in_place", lambda state: self.rows)
        monkeypatch.setattr(temp_exec, "_land", land)
        monkeypatch.setattr(temp_exec, "_store_address", address)
        monkeypatch.setattr(temp_exec, "_system_rows", system_rows)
        from provisa.core import settings_registry

        limits = {"temp_tables.max_per_session": 50, "temp_tables.max_rows": 1000}
        real = settings_registry.value
        monkeypatch.setattr(
            settings_registry, "value", lambda key: limits[key] if key in limits else real(key)
        )

    def sql(self, query: str, params: dict[str, Any] | None = None) -> str:
        params = params or {}
        lm = seam.label_map(self.model, self.session)
        ast, _, graph_vars = cypher_to_sql(parse_cypher(query, lm), lm, params)
        ast = apply_graph_rewrites(ast, graph_vars, lm)
        return seam.expand(ast, lm, self.model, self.session).sql(dialect="postgres")

    async def read(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        lm = seam.label_map(self.model, self.session)
        ast, ordered, graph_vars = cypher_to_sql(parse_cypher(query, lm), lm, params)
        ast = apply_graph_rewrites(ast, graph_vars, lm)
        ast = seam.expand(ast, lm, self.model, self.session)
        result = self.duck.execute(
            transpile_to_duckdb(ast.sql(dialect="postgres")),
            [bindable(params[n]) for n in ordered],
        )
        columns = [d[0] for d in result.description]
        rows = assemble_rows([dict(zip(columns, r)) for r in result.fetchall()], graph_vars)
        return [to_serializable(r, keep_values=True) for r in rows]

    async def write(self, statement: str, params: dict[str, Any]) -> None:
        """The governed write path, as a surface runs it: translated and sent to the source."""
        sql, values = bind_write_params(
            WriteTranslator(self.model).translate(parse_cypher_write(statement)), params
        )
        self.duck.execute(sql.replace('"memory".', ""), values)

    def run(self, statement: str, **params: Any) -> Any:
        made = asyncio.run(
            seam.write(
                statement, params, self.model, self.session, self.state, self.read, self.write
            )
        )
        if made is not None:
            return made
        return asyncio.run(self.read(statement, params))
