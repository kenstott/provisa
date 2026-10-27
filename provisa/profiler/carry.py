# Copyright (c) 2026 Kenneth Stott
# Canary: 6032fde5-650f-4039-bcf8-aee7d1033014
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo's profile runs, carried in its bundle (REQ-2052).

The bundle build runs a demo's setup.yaml, ``profile`` steps included, on a throwaway control plane:
whatever its steps did between two runs (a client changing the demo's data) is what the runs
measured. :func:`export_runs` writes every profile result relation of that org out, each with the
registered table it profiles, as ``seeded/profile_results.json`` beside the demo's seed. A template
of the bundle then loads them with :func:`load_runs` instead of profiling the demo's sources again:
its sources hold the data as setup left it, so a run there would measure nothing a later step
changed. Each relation is keyed by its table's (source, schema, table), since the template's table
ids are its own.
"""

# Requirements: REQ-2052

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.schema import CreateTable, DropTable

from provisa.profiler.schema import (
    is_result_table_name,
    kind_fields,
    parse_result_table_name,
    result_sa_table,
)

#: Where a bundle carries its demo's profile runs, under the demo's directory.
RESULTS_FILE = Path("seeded") / "profile_results.json"

_RESULT_RELATIONS = sa.text(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = :schema AND table_type = 'BASE TABLE' ORDER BY table_name"
)


def results_path(demo_directory: Path) -> Path:
    return demo_directory / RESULTS_FILE


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"a profile result holds a {type(value).__name__}, which has no JSON form")


def _decoded(kind: str, row: dict[str, Any]) -> dict[str, Any]:
    """A row as JSON carried it, its timestamps parsed back (every other type JSON keeps)."""
    timestamps = {name for name, data_type, _ in kind_fields(kind) if data_type == "timestamp"}
    return {
        name: datetime.fromisoformat(value) if name in timestamps and value is not None else value
        for name, value in row.items()
    }


def export_runs(url: str, schema: str, out: Path) -> int:
    """Write every profile result relation of control-plane ``schema`` (an org's) to ``out``, each
    with the table it profiles; the relations written."""
    from provisa.core.schema_org import registered_tables as rt

    engine = sa.create_engine(url)
    try:
        with engine.connect() as raw:
            conn = raw.execution_options(schema_translate_map={None: schema})
            names = [
                r[0]
                for r in conn.execute(_RESULT_RELATIONS.bindparams(schema=schema))
                if is_result_table_name(r[0])
            ]
            tables = {
                r.id: [r.source_id, r.schema_name, r.table_name]
                for r in conn.execute(
                    sa.select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name)
                )
            }
            results = []
            for name in names:
                table, table_id, kind = parse_result_table_name(name)
                relation = result_sa_table(table, table_id, kind)
                rows = [dict(r._mapping) for r in conn.execute(sa.select(relation))]
                results.append({"table": tables[table_id], "kind": kind, "rows": rows})
    finally:
        engine.dispose()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"results": results}, default=_encode, indent=1))
    return len(results)


async def load_runs(conn: Any, path: Path) -> list[str]:
    """Load the profile runs a bundle carries at ``path`` into the org ``conn`` is scoped to, each
    relation replaced whole under the id the org gave its table; the relations loaded. A table
    the org does not register is refused by name: the bundle and the demo's config disagree."""
    from provisa.core.schema_org import registered_tables as rt

    carried = json.loads(path.read_text())["results"]
    ids = {
        (r.source_id, r.schema_name, r.table_name): r.id
        for r in (
            await conn.execute_core(
                sa.select(rt.c.id, rt.c.source_id, rt.c.schema_name, rt.c.table_name)
            )
        ).fetchall()
    }
    loaded = []
    for entry in carried:
        key = tuple(entry["table"])
        if key not in ids:
            source_id, schema_name, table_name = key
            raise ValueError(
                f"the bundle's profile runs are of table {source_id}.{schema_name}.{table_name}, "
                "which this org does not register"
            )
        relation = result_sa_table(key[2], ids[key], entry["kind"])
        await conn.execute_core(DropTable(relation, if_exists=True))
        await conn.execute_core(CreateTable(relation))
        rows = [_decoded(entry["kind"], row) for row in entry["rows"]]
        if rows:
            await conn.execute_core_many(relation.insert(), rows)
        loaded.append(relation.name)
    return loaded


async def load_bundle_runs(conn: Any, directory: Path | None) -> list[str]:
    """Load the profile runs the bundle of the demo this server was started as carries
    (``directory``, :func:`provisa.core.demo.started_demo_directory`) into the org ``conn`` is
    scoped to: a bundle started on its own server has the runs its build measured, as its hosted
    template has (REQ-2052, A FIRST RUN IS SEEDED). Refused by name when the server was started as
    no named demo or the bundle carries none."""
    if directory is None:
        raise ValueError(
            "this server was started as no named demo: there are no carried profile runs"
        )
    path = results_path(directory)
    if not path.is_file():
        raise ValueError(f"the bundle carries no profile runs ({path.name}): rebuild it")
    return await load_runs(conn, path)
