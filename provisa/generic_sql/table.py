# Copyright (c) 2026 Kenneth Stott
# Canary: 94f1d8e5-aafb-4e83-b6de-3c348d414fe7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The floor of a Generic SQL source (REQ-2191): with nothing pushed down, a table still reads, in
simple SQL, the way an API table reads.

Each column of the table may be given by equality as a parameter (``column_value``, amended
2026-11-02): ``_nf_<column>``, the value of a real column that narrows the read to the rows
having it. A steward marks which are required; a query that omits a required one is refused by
name before anything is sent, as for every parameter kind (``core.parameters``, REQ-1986). The
read is one SELECT of the table's columns with an equality filter per parameter given, written in
the source's SQL dialect, its values bound, never written into its text. Its rows land per
argument set (the materialization step), and a row limit the statement alone sets is met by
reading the cursor in batches and stopping.

A table served from its replica -- a small one replicated whole by its replicate setting -- is
read from the replica instead, and joins as any landed table does.
"""

# Requirements: REQ-2191, REQ-1986

from __future__ import annotations

from typing import Any

from provisa.core.parameters import parameter_name
from provisa.generic_sql import SOURCE_TYPE

#: A real column of the table, given by equality, that narrows the read to the rows having it.
COLUMN_VALUE = "column_value"
_BATCH_ROWS = 1000


def is_floor_table(table: dict, source_types: dict[str, str], state: Any) -> bool:
    """Whether Provisa reads ``table`` itself, per statement: a Generic SQL table its replica
    does not serve."""
    if source_types.get(table["source_id"]) != SOURCE_TYPE:
        return False
    routes = getattr(state, "replica_routes", None)
    if routes is None:
        return True
    key = (table["source_id"], table["schema_name"], table["table_name"])
    return table.get("id") not in routes.floored and key not in routes.serving


def _column_of(parameter: dict) -> str:
    """The real column a ``column_value`` parameter narrows: its ``path``, else its name."""
    return parameter.get("path") or parameter_name(parameter)


def _arguments(table: dict, given: dict | None) -> dict[str, tuple[Any, dict]]:
    """Each ``column_value`` parameter with its converted value (None where none is given)."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.parameter_values import convert

    by_canon = {apply_sql_name(k.lstrip("_")): v for k, v in (given or {}).items()}
    out: dict[str, tuple[Any, dict]] = {}
    for col in table["columns"]:
        if col["native_filter_type"] != COLUMN_VALUE:
            continue
        name = parameter_name(col)
        value = by_canon.get(apply_sql_name(name.lstrip("_")))
        out[name] = (
            convert(value, col["data_type"], table=table["table_name"], parameter=name),
            col,
        )
    return out


def cache_arguments(table: dict, given: dict | None) -> dict[str, str]:
    """The arguments a read gives, by name, as text: what its landed rows are kept under."""
    return {k: str(v) for k, (v, _) in _arguments(table, given).items() if v is not None}


def output_columns(table: dict) -> list[dict]:
    return [c for c in table["columns"] if c["native_filter_type"] is None]


def statement(table: dict, given: dict | None, sql_dialect: str) -> tuple[str, list[Any]]:
    """The read of ``table``: its columns, filtered by equality on each parameter given, in the
    source's dialect; the values as PG-canonical ``$N`` binds, which the source's driver binds in
    its own paramstyle."""
    import sqlglot
    from sqlglot import exp

    import provisa.generic_sql.dialect  # noqa: F401 -- registers "ansi"

    def quoted(name: str) -> str:
        return exp.to_identifier(name, quoted=True).sql(dialect=sql_dialect)

    select = sqlglot.select(
        *[exp.column(c["column_name"], quoted=True) for c in output_columns(table)]
    ).from_(exp.table_(table["table_name"], db=table["schema_name"], quoted=True))
    text = select.sql(dialect=sql_dialect)
    conditions: list[str] = []
    values: list[Any] = []
    for value, col in _arguments(table, given).values():
        if value is None:
            continue
        values.append(value)
        conditions.append(f"{quoted(_column_of(col))} = ${len(values)}")
    if conditions:
        text += " WHERE " + " AND ".join(conditions)
    return text, values


def row_limit(sql: str, table_name: str) -> int | None:
    """The rows the statement can use of ``table_name``'s read, when the statement alone sets
    it: a LIMIT on a statement that reads that table only, with no condition, grouping, ordering
    or distinct left for the engine (its parameters' conditions were taken out of it). None when
    the engine may need any row."""
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(sql, read="postgres")
    if not isinstance(tree, exp.Select):
        return None
    limit = tree.args.get("limit")
    if limit is None or not isinstance(limit.expression, exp.Literal):
        return None
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1 or tables[0].name != table_name:
        return None
    if any(tree.args.get(k) for k in ("where", "group", "having", "order", "distinct", "offset")):
        return None
    if any(isinstance(e, exp.AggFunc) for e in tree.find_all(exp.AggFunc)):
        return None
    return int(limit.expression.this)


async def read(state: Any, table: dict, given: dict | None, sql: str) -> Any:
    """Read ``table`` through its source's driver with the arguments ``given``: its rows, as
    they land. ``sql`` is the statement the read is for, which may set its row limit."""
    from provisa.core.ir_types import to_ir
    from provisa.mongodb.pipeline_table import PipelineRead

    source_id = table["source_id"]
    driver = state.source_pools.get(source_id)
    text, values = statement(table, given, state.source_dialects[source_id])
    outputs = output_columns(table)
    names = [c["column_name"] for c in outputs]
    limit = row_limit(sql, table["table_name"])
    rows: list[dict] = []
    stream = await driver.open_stream(text, values)
    try:
        while limit is None or len(rows) < limit:
            batch = await stream.fetch(_BATCH_ROWS)
            if not batch:
                break
            rows.extend(dict(zip(names, row)) for row in batch)
    finally:
        await stream.close()
    if limit is not None:
        rows = rows[:limit]
    return PipelineRead(
        rows=rows, columns=[(c["column_name"], to_ir(c["data_type"])) for c in outputs]
    )


def parameters_of(columns: list[dict]) -> list[dict]:
    """The ``column_value`` parameter Register Table suggests for each of a table's columns:
    optional, which a steward may make required."""
    return [
        {
            "name": "_nf_" + c["name"],
            "data_type": c["data_type"],
            "path": c["name"],
            "native_filter_type": COLUMN_VALUE,
            "native_filter_required": False,
        }
        for c in columns
    ]
