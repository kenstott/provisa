# Copyright (c) 2026 Kenneth Stott
# Canary: 63c27d63-ce47-4cc3-b619-76fdd43817bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A configuration's declared replica rows: a replica's rows written beside the config, landed as
its replica so that nothing ever reads its source (REQ-2089).

A demo whose source has no tenant to read -- the insider demo's bank mail in Microsoft 365 --
declares that source's replica rows instead, one file a table, beside its config.yaml::

    replica_rows/<source_id>/<schema>.<table>.tsv

The first line names the columns, tab separated; each line after is one row. A cell ``\\N`` is
NULL; in any other cell ``\\t``, ``\\n``, ``\\r`` and ``\\\\`` stand for a tab, a newline, a
carriage return and a backslash. A cell is read as its column's type: an integer column's as a
whole number, a floating column's as a number, a boolean column's as ``true`` or ``false``, and
every other cell as its text (ISO-8601 for a date or timestamp, JSON text for a json column).

When the org's model is built, before its replicas converge to it, each declared table is
written into the org's own replica store, as a build of it would write it, and recorded as built
(``replica_state.record_adopted``, as a seed is adopted): convergence then finds it built and
asks for no build, so the source is never read -- not by a bundle build's setup, not by its
seeding run, not in a sandbox. A bundle's seed carries it from there like any landed replica.

Each declared table must be one the config serves from that source; its header must be that
table's columns, in their order; the source's ``cache_ttl`` must be -1, so that nothing refreshes
the replica from the source; and it must hold a row. Each refusal names the file.
"""

# Requirements: REQ-2089

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

#: The directory beside a config holding its declared replica rows.
ROWS_DIR = "replica_rows"

NULL = "\\N"
_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}
_INTEGERS = frozenset({"bigint", "integer", "int", "smallint", "tinyint"})
_FLOATS = frozenset({"double", "real", "float"})


class DeclaredRowsRefused(ValueError):
    """A declared replica rows file that cannot be landed; the message names the file."""


@dataclass(frozen=True)
class DeclaredTable:
    """One declared replica: its key, the file it was read from and its rows."""

    key: tuple[str, str, str]
    path: Path
    header: list[str]
    lines: list[list[str]]


def _unescaped(cell: str, path: Path, line: int) -> str:
    out: list[str] = []
    it = iter(cell)
    for ch in it:
        if ch != "\\":
            out.append(ch)
            continue
        nxt = next(it, None)
        if nxt not in _ESCAPES:
            raise DeclaredRowsRefused(f"{path}:{line}: unknown escape \\{nxt or ''} in {cell!r}")
        out.append(_ESCAPES[nxt])
    return "".join(out)


def read_file(path: Path) -> DeclaredTable:
    """The declared table ``path`` holds: its key from its place, its header and its cells."""
    source_id = path.parent.name
    schema, dot, table = path.stem.partition(".")
    if not dot or not schema or not table:
        raise DeclaredRowsRefused(f"{path}: name it <schema>.<table>.tsv")
    text = path.read_text(encoding="utf-8")
    lines = [line for line in text.split("\n") if line != ""]
    if not lines:
        raise DeclaredRowsRefused(f"{path}: no header")
    header = lines[0].split("\t")
    rows = []
    for number, line in enumerate(lines[1:], 2):
        cells = line.split("\t")
        if len(cells) != len(header):
            raise DeclaredRowsRefused(
                f"{path}:{number}: {len(cells)} cells where the header names {len(header)}"
            )
        rows.append(cells)
    return DeclaredTable((source_id, schema, table), path, header, rows)


def _value(cell: str, ir_type: str, where: str) -> Any:
    if ir_type in _INTEGERS:
        try:
            return int(cell)
        except ValueError:
            raise DeclaredRowsRefused(f"{where}: {cell!r} is not a whole number") from None
    if ir_type in _FLOATS:
        try:
            return float(cell)
        except ValueError:
            raise DeclaredRowsRefused(f"{where}: {cell!r} is not a number") from None
    if ir_type == "boolean":
        if cell not in ("true", "false"):
            raise DeclaredRowsRefused(f"{where}: {cell!r} is not true or false")
        return cell == "true"
    if ir_type == "json":
        try:
            json.loads(cell)
        except json.JSONDecodeError as e:
            raise DeclaredRowsRefused(f"{where}: not JSON: {e}") from None
    return cell


def typed_rows(declared: DeclaredTable, columns: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """The declared rows as values of ``columns`` ((name, IR type)), refused by name when the
    header is not those columns in their order."""
    names = [name for name, _ in columns]
    if declared.header != names:
        missing = [n for n in names if n not in declared.header]
        extra = [n for n in declared.header if n not in names]
        raise DeclaredRowsRefused(
            f"{declared.path}: the header must be the table's columns in their order, {names};"
            f" missing {missing}, not the table's {extra}"
        )
    rows = []
    for number, cells in enumerate(declared.lines, 2):
        row = {}
        for (name, ir_type), cell in zip(columns, cells, strict=True):
            row[name] = (
                None
                if cell == NULL
                else _value(
                    _unescaped(cell, declared.path, number),
                    ir_type,
                    f"{declared.path}:{number} {name}",
                )
            )
        rows.append(row)
    return rows


def declared_files(directory: Path) -> list[Path]:
    """The declared replica rows files beside the config in ``directory``."""
    root = directory / ROWS_DIR
    return sorted(root.glob("*/*.tsv")) if root.is_dir() else []


async def adopt_declared_rows(state: Any, directory: Path) -> list[tuple[str, str, str]]:
    """Land each table declared under ``directory``'s ``replica_rows`` as the bound org's replica
    and record it built; a table already landed here from these very rows is left as it is.
    Run within the org's model build, before its replicas converge to it. The keys
    landed."""
    from provisa.core.ir_arrow import arrow_schema, rows_to_batch
    from provisa.events.content_hash import RowSetHash
    from provisa.federation import replica_state
    from provisa.federation.data_replicator import Method
    from provisa.federation.replica_builds import ReplicaTableGone, _model_row, store_identity
    from provisa.federation.replica_converge import definition_hash
    from provisa.federation.residency import resolve_landing_args

    files = declared_files(directory)
    if not files:
        return []
    backend = state.federation_engine.engine.backend
    store = store_identity(state)
    landed: list[tuple[str, str, str]] = []
    for path in files:
        declared = read_file(path)
        key = declared.key
        try:
            source, table, _sources = await _model_row(state, key)
        except ReplicaTableGone:
            raise DeclaredRowsRefused(
                f"{path}: the config serves no table {key[1]}.{key[2]} from source {key[0]!r}"
            ) from None
        if source.cache_ttl != -1:
            raise DeclaredRowsRefused(
                f"{path}: source {source.id!r} must declare cache_ttl: -1, so that nothing"
                f" refreshes its declared replica from the source (it declares {source.cache_ttl})"
            )
        args = resolve_landing_args(source, table, platform=backend.dialect)
        rows = typed_rows(declared, list(args.columns))
        if not rows:
            raise DeclaredRowsRefused(f"{path}: no rows; a declared replica holds at least one")
        address = backend.replica_address(
            state, source_id=source.id, schema_name=table.schema_name, table_name=table.table_name
        )
        defined = definition_hash(
            source, address, args.columns, args.pk_columns, landing_filter=args.landing_filter
        )
        schema = arrow_schema(list(args.columns))
        batch = rows_to_batch(rows, list(args.columns), schema)
        digest = RowSetHash()
        digest.update(batch.to_pylist())
        content_hash = digest.hexdigest()
        async with state.tenant_db.acquire() as conn:
            record = await replica_state.read(conn, key)
        if (
            record is not None
            and record.built_store == store
            and record.definition_hash == defined
            and record.content_hash == content_hash
        ):
            continue
        engine_party = backend.replica_engine(state, source, table, address=address, args=args)
        target = backend.replica_target(state, address=address, args=args, engine=engine_party)
        await target.begin()
        try:
            await target.write(batch, rows)
            await target.swap()
        except BaseException:
            await target.abort()
            raise
        async with state.tenant_db.acquire() as conn:
            await replica_state.record_adopted(
                conn,
                [
                    {
                        "source_id": key[0],
                        "schema_name": key[1],
                        "table_name": key[2],
                        "rows_copied": len(rows),
                        "completed_rows": len(rows),
                        "content_hash": content_hash,
                        "built_columns": [[name, ir_type] for name, ir_type in args.columns],
                        "build_method": Method.STREAM_BATCHES.value,
                        "load_kind": target.caps.load.value,
                        "built_store": store,
                        "definition_hash": defined,
                        "completed_at": datetime.now(UTC),
                        "build_state": "idle",
                    }
                ],
            )
        landed.append(key)
    return landed
