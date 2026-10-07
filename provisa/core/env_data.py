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
can be switched back to Inherit at any time. Inheriting is a property of each source (its
``binding``: own, inherited or unbound, :mod:`provisa.core.env_bindings`); Inherit and Unbound set
every source at once, the Test modes leave each source as it is.

A change of data mode that changes row keys -- to or from Test (synthetic), or regenerating it --
discards the environment's change log, and is refused unless the caller confirms it. A change
between Inherit and Test (fake) keeps it.

Test (fake) never shows a column tagged pii unless that column declares a fake: entering it, and
every request into it, is refused while any pii column has none (:func:`uncovered_pii`).

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
    BINDINGS,
    DATA_MODES,
    INHERIT,
    INHERITED,
    TEST_FAKE,
    TEST_SYNTHETIC,
    UNBOUND,
    UNBOUND_MODE,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The tag a column is personal data by (REQ-1494, REQ-1943).
PII_TAG = "pii"

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
    """What a change of data mode does: the binding it sets on every source (None: each source
    keeps its own), whether it discards the change log, and whether it needs the right to read
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
        binding={INHERIT: INHERITED, UNBOUND_MODE: UNBOUND}.get(target),
        # Row keys change to or from synthetic rows, and on regenerating them.
        discards_change_log=TEST_SYNTHETIC in (current, target),
        reads_parent=target == INHERIT,
    )


async def set_bindings(
    conn: "Connection", schema: str, binding: str, source_ids: list[str] | None
) -> list[str]:
    """Set the binding of ``source_ids`` (None: every source the model holds, the built-in ones
    excepted) in the environment whose schema is ``schema``; the ids it set. A source's own
    binding is never set here: a source becomes its own by being given a connection."""
    from provisa.core.models import BUILT_IN_SOURCE_IDS

    if binding not in BINDINGS:
        raise DataChoiceRefused(f"unknown binding {binding!r}; one of {BINDINGS}")
    if binding not in (INHERITED, UNBOUND):
        raise DataChoiceRefused(
            "a source's own binding is its connection: bind it by giving it one"
        )
    sources = _table("sources", schema)
    rows = (await conn.execute_core(select(sources.c.id))).fetchall()
    known = {r[0] for r in rows} - set(BUILT_IN_SOURCE_IDS)
    ids = sorted(known) if source_ids is None else sorted(source_ids)
    missing = sorted(set(ids) - known)
    if missing:
        raise DataChoiceRefused(
            "no source " + ", ".join(repr(m) for m in missing) + " in this environment"
        )
    if ids:
        await conn.execute_core(
            update(sources).where(sources.c.id.in_(ids)).values({BINDING_COLUMN: binding})
        )
    return ids


async def uncovered_pii(conn: "Connection", schema: str) -> list[str]:
    """Every column tagged pii that declares no fake, as ``table.column``, in the environment
    whose schema is ``schema``: what Test (fake) would show real (REQ-1942)."""
    rt = _table("registered_tables", schema)
    tc = _table("table_columns", schema)
    ta = _table("tag_assignments", schema)
    result = await conn.execute_core(
        select(rt.c.table_name, ta.c.column_name)
        .select_from(
            ta.join(rt, rt.c.id == ta.c.table_id).join(
                tc, (tc.c.table_id == ta.c.table_id) & (tc.c.column_name == ta.c.column_name)
            )
        )
        .where(ta.c.object_type == "column", ta.c.base_tag_id == PII_TAG, tc.c.fake.is_(None))
    )
    return sorted({f"{t}.{c}" for t, c in result.fetchall()})


def pii_refusal(env: str, columns: list[str]) -> str:
    """The refusal of Test (fake) while ``columns`` -- pii with no fake -- would show real."""
    return (
        f"environment {env!r} is Test (fake), and these columns are tagged pii and declare no "
        f"fake, so they would show real values: {', '.join(columns)}. Declare a fake on each "
        f"(a holder of the pii right can), or choose another data mode."
    )


async def refuse_uncovered_pii(conn: "Connection", schema: str, env: str) -> None:
    """Refuse Test (fake) in ``env`` while a pii column declares no fake."""
    columns = await uncovered_pii(conn, schema)
    if columns:
        raise DataChoiceRefused(pii_refusal(env, columns))


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
