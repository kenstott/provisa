# Copyright (c) 2026 Kenneth Stott
# Canary: 4109071d-c3b9-4cf1-bc65-dd7b1fd62fa1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's data choices (REQ-1942).

Every environment other than prod has one data mode -- Inherit, Unbound, Test (fake) or Test
(synthetic) -- and is read-only or read-write. Its parent is always recorded, so any environment
can be switched back to Inherit at any time. An environment's sources are its own: each one's
connection was given in it (``binding`` own), copied from the parent as the parent wrote it
(copied), or is none (unbound). Nothing is resolved through the parent at build time. Inherit copies
every connection from the parent again, Unbound clears every one, the Test modes leave each as it
is.

A change of data mode that changes row keys -- to or from Test (synthetic), or regenerating it --
discards the environment's change log, and is refused unless the caller confirms it. A change
between Inherit and Test (fake) keeps it.

Test (fake) never shows a sensitive column (REQ-1943: one carrying a tag with the Sensitive data
option, pii among them) unless that column declares a fake: entering it, and every request into
it, is refused while any sensitive column has none (:func:`uncovered_sensitive`).

Each function here acts on an environment from outside its runtime, so it names the environment's
schema in every statement.
"""

# Requirements: REQ-1942

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update

from provisa.core.env_classes import (
    BINDING_COLUMN,
    BINDING_COLUMNS,
    CLEARED_SOURCE_CONNECTION,
    COPIED,
    DATA_MODES,
    INHERIT,
    TEST_FAKE,
    TEST_SYNTHETIC,
    UNBOUND,
    UNBOUND_MODE,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

TEST_MODES = (TEST_FAKE, TEST_SYNTHETIC)


def _table(name: str, schema: str) -> Any:
    """Org table ``name`` addressed in ``schema``."""
    from provisa.core.env_copy import _scoped
    from provisa.core.schema_org import metadata

    return _scoped(metadata.tables[name], schema)


class DataChoiceRefused(ValueError):
    """A data choice the environment cannot take, said in full."""


@dataclass(frozen=True)
class Transition:
    """What a change of data mode does: the binding it gives every source -- COPIED: each copied
    again from the parent; UNBOUND: each cleared; None: each keeps its own -- whether it discards the change log, and whether it needs the right to read
    the parent's data."""

    binding: str | None
    discards_change_log: bool
    reads_parent: bool


def transition(current: str, target: str) -> Transition:
    """The change from data mode ``current`` to ``target`` (REQ-1942). Regenerating Test
    (synthetic) is ``test_synthetic`` to ``test_synthetic``."""
    for mode in (current, target):
        if mode not in DATA_MODES:
            raise DataChoiceRefused(f"unknown data mode {mode!r}; one of {DATA_MODES}")
    return Transition(
        binding={INHERIT: COPIED, UNBOUND_MODE: UNBOUND}.get(target),
        # Row keys change to or from synthetic rows, and on regenerating them.
        discards_change_log=TEST_SYNTHETIC in (current, target),
        reads_parent=target == INHERIT,
    )


async def _source_ids(conn: "Connection", schema: str, source_ids: list[str] | None) -> list[str]:
    """``source_ids`` checked against the sources the environment whose schema is ``schema`` holds
    (None: every one of them), the built-in ones excepted -- their connection is the platform's."""
    from provisa.core.models import BUILT_IN_SOURCE_IDS

    sources = _table("sources", schema)
    rows = (await conn.execute_core(select(sources.c.id))).fetchall()
    known = {r[0] for r in rows} - set(BUILT_IN_SOURCE_IDS)
    ids = sorted(known) if source_ids is None else sorted(source_ids)
    missing = sorted(set(ids) - known)
    if missing:
        raise DataChoiceRefused(
            "no source " + ", ".join(repr(m) for m in missing) + " in this environment"
        )
    return ids


async def unbind(conn: "Connection", schema: str, source_ids: list[str] | None) -> list[str]:
    """Clear the connection of ``source_ids`` (None: every source) in the environment whose schema
    is ``schema``, marking each UNBOUND; the ids it cleared."""
    ids = await _source_ids(conn, schema, source_ids)
    if ids:
        sources = _table("sources", schema)
        await conn.execute_core(
            update(sources)
            .where(sources.c.id.in_(ids))
            .values({**CLEARED_SOURCE_CONNECTION, BINDING_COLUMN: UNBOUND})
        )
    return ids


async def recopy(
    conn: "Connection", parent_schema: str, schema: str, source_ids: list[str] | None
) -> list[str]:
    """Copy the connection of ``source_ids`` (None: every source) from the parent, whose schema is
    ``parent_schema``, into the environment whose schema is ``schema`` (REQ-1942): each exactly as
    the parent wrote it -- a reference to a secret or a variable stays that reference -- marked
    COPIED, or UNBOUND where the parent has none. The ids it copied. A source the parent does not
    hold has nothing to copy, and is refused by name."""
    ids = await _source_ids(conn, schema, source_ids)
    if not ids:
        return ids
    columns = sorted(BINDING_COLUMNS["sources"])
    parent = _table("sources", parent_schema)
    rows = {
        r._mapping["id"]: dict(r._mapping)
        for r in (
            await conn.execute_core(
                select(
                    parent.c.id, parent.c[BINDING_COLUMN], *(parent.c[c] for c in columns)
                ).where(parent.c.id.in_(ids))
            )
        ).fetchall()
    }
    orphans = sorted(set(ids) - set(rows))
    if orphans:
        raise DataChoiceRefused(
            "the parent holds no source "
            + ", ".join(repr(o) for o in orphans)
            + " to copy a connection from"
        )
    sources = _table("sources", schema)
    for sid in ids:
        row = rows[sid]
        values = (
            {**CLEARED_SOURCE_CONNECTION, BINDING_COLUMN: UNBOUND}
            if row[BINDING_COLUMN] == UNBOUND
            else {**{c: row[c] for c in columns}, BINDING_COLUMN: COPIED}
        )
        await conn.execute_core(update(sources).where(sources.c.id == sid).values(values))
    return ids


async def bind_own(
    conn: "Connection", schema: str, source_id: str, connection: dict[str, Any]
) -> None:
    """Bind ``source_id`` in the environment whose schema is ``schema`` to a connection of its
    own (REQ-1942): ``connection`` gives its binding columns (host, port, database, username,
    password_ref, path); the row is marked OWN. Nothing of the parent's binding is kept."""
    from provisa.core.env_classes import OWN
    from provisa.core.models import BUILT_IN_SOURCE_IDS

    unknown = sorted(set(connection) - BINDING_COLUMNS["sources"])
    if unknown:
        raise DataChoiceRefused(f"not a source's connection: {', '.join(unknown)}")
    if source_id in BUILT_IN_SOURCE_IDS:
        raise DataChoiceRefused(f"{source_id!r} is built in; its connection is the platform's")
    sources = _table("sources", schema)
    done = await conn.execute_core(
        update(sources).where(sources.c.id == source_id).values({**connection, BINDING_COLUMN: OWN})
    )
    if done.rowcount == 0:
        raise DataChoiceRefused(f"no source {source_id!r} in this environment")


async def uncovered_sensitive(conn: "Connection", schema: str) -> list[str]:
    """Every sensitive column that declares no fake, as ``table.column``, in the environment whose
    schema is ``schema``: what Test (fake) would show real (REQ-1942, REQ-1943)."""
    from provisa.security.sensitive import sensitive_tag_ids

    rt = _table("registered_tables", schema)
    tc = _table("table_columns", schema)
    ta = _table("tag_assignments", schema)
    sensitive = await sensitive_tag_ids(conn, _table("tags", schema))
    result = await conn.execute_core(
        select(rt.c.table_name, ta.c.column_name)
        .select_from(
            ta.join(rt, rt.c.id == ta.c.table_id).join(
                tc, (tc.c.table_id == ta.c.table_id) & (tc.c.column_name == ta.c.column_name)
            )
        )
        .where(
            ta.c.object_type == "column",
            ta.c.base_tag_id.in_(sorted(sensitive)),
            tc.c.fake.is_(None),
        )
    )
    return sorted({f"{t}.{c}" for t, c in result.fetchall()})


def sensitive_refusal(env: str, columns: list[str]) -> str:
    """The refusal of Test (fake) while ``columns`` -- sensitive, with no fake -- would show
    real."""
    return (
        f"environment {env!r} is Test (fake), and these sensitive columns declare no fake, so "
        f"they would show real values: {', '.join(columns)}. A holder of the sensitive_data right "
        f"can declare a fake on each; or choose another data mode."
    )


async def refuse_uncovered_sensitive(conn: "Connection", schema: str, env: str) -> None:
    """Refuse Test (fake) in ``env`` while a sensitive column declares no fake."""
    columns = await uncovered_sensitive(conn, schema)
    if columns:
        raise DataChoiceRefused(sensitive_refusal(env, columns))


async def faked_column_count(conn: "Connection", schema: str) -> int:
    """How many columns declare a fake: what Test (fake) shows faked."""
    tc = _table("table_columns", schema)
    row = (
        await conn.execute_core(select(func.count()).select_from(tc).where(tc.c.fake.isnot(None)))
    ).fetchone()
    assert row is not None  # COUNT over a table is a row
    return int(row[0])


async def source_bindings(conn: "Connection", schema: str) -> list[dict[str, Any]]:
    """Each source's binding in the environment whose schema is ``schema``, the built-in ones
    excepted."""
    from provisa.core.models import BUILT_IN_SOURCE_IDS

    sources = _table("sources", schema)
    rows = (
        await conn.execute_core(select(sources.c.id, sources.c.type, sources.c[BINDING_COLUMN]))
    ).fetchall()
    return [
        {"id": r[0], "type": r[1], "binding": r[2]}
        for r in sorted(rows)
        if r[0] not in BUILT_IN_SOURCE_IDS
    ]
