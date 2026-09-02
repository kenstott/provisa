# Copyright (c) 2026 Kenneth Stott
# Canary: 5e3b0d74-91c2-4a6f-bd18-0c6a7f235be1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Moving a governed model from one environment's schema into another's (REQ-1489, REQ-1490,
REQ-1491).

Two shapes, and the difference between them is what the operator asked for rather than a heuristic:

REPLACE is what a creation and a checkout do. The carried classes of the target become the source's,
wholesale — an object the incoming model does not have is gone from the target, because a checkout
is a statement about which model the environment is running and a merge-by-identity would leave
behind whatever the branch deleted.

MERGE is REQ-1490's: every carried object is matched by identity, created or updated, and an object
the source no longer has is removed only when the caller asks for removals. Both report what they
did or would do, and both apply whole or not at all — one transaction, and a report computed from
the same read the apply uses, so what is shown is what happens.

WHAT NEITHER OF THEM MOVES. The binding columns of REQ-1491 never travel in either shape, and an
IDENTITY_ONLY row is never deleted by either. A checkout keeps the environment's bindings for the
reason REQ-1491 gives — the incoming tree has nothing to overwrite them with — and dropping a source
the incoming model does not name would destroy a binding the operator established deliberately, so
those rows stay and are simply unreferenced. Rows this copy creates are marked unbound (REQ-1491):
an empty host is not an absent one, and the connection builder would read it as localhost.

WHAT A MERGE SAYS ABOUT WHAT IT OVERWROTE. Matching by identity means the source wins on every
object both sides hold, and for an object the TARGET changed since the two lines parted that is a
second person's work being carried away. The report names those objects (REQ-1555, computed in
:mod:`provisa.core.env_conflicts` against the commit both lines last held) rather than letting them
pass as ordinary changes. It does not refuse the merge: a merge into a target is the source winning,
and what was missing was not a veto but the sentence saying whose work went.

Nor do either of them move ``SEEDED_AT_CREATION`` — the roles and the assignments naming them.
Those are seeded once, by the creation, and are afterwards the environment's own answer to who may
do what (REQ-1539). A merge that carried them would let an unrestricted branch role overwrite the
restricted one in the base it merges into, which would make the review path the escalation route.
"""

# Requirements: REQ-1488, REQ-1489, REQ-1490, REQ-1491, REQ-1539, REQ-1555

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import MetaData, Table, delete, select

from provisa.core.env_classes import (
    BOUND_COLUMN,
    CARRIED,
    IDENTITY_ONLY,
    SEEDED_AT_CREATION,
    binding_columns,
    carries_setting,
)
from provisa.core.env_conflicts import Conflict
from provisa.core.env_conflicts import detect as detect_conflicts
from provisa.core.environments import org_schema
from provisa.core.schema_org import metadata as org_metadata
from provisa.core.schema_org import org_settings
from provisa.core.schema_org import roles as org_roles

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

#: How many bind parameters one INSERT is allowed to carry. PostgreSQL's protocol caps a statement
#: at 65535 of them, so a wide table copied in one multi-row VALUES would fail on a big model.
_MAX_BIND_PARAMS = 30000

_scoped_metadata: dict[str, MetaData] = {}


def _scoped(table: Table, schema: str) -> Table:
    """``table`` addressed in ``schema``.

    The org tables are declared without a schema and reached through ``search_path``, which is
    exactly what a copy cannot use: both sides are open at once and only one of them can be on the
    path. Every statement here is therefore schema-qualified explicitly.
    """
    md = _scoped_metadata.get(schema)
    if md is None:
        md = _scoped_metadata[schema] = MetaData(schema=schema)
    existing = md.tables.get(f"{schema}.{table.name}")
    if existing is not None:
        return existing
    return table.to_metadata(md, schema=schema)


def _key(table: Table, row: dict) -> tuple:
    return tuple(row[c.name] for c in table.primary_key.columns)


#: A carried row that names the environment's OWN schema. The org tables hold a self-catalog: the
#: org-registry view of REQ-1301 is registered against the schema it lives in, so a row copied
#: verbatim would tell the target environment that its registry view sits in the SOURCE
#: environment's schema — and the target's own seed, keyed on (source_id, schema_name, table_name),
#: would then add a SECOND registration of the same domain+table and every request to that runtime
#: would fail the uniqueness assertion. Only a value equal to the source schema is rewritten; a real
#: source schema (``public`` and the like) is left exactly as it is.
_SCHEMA_COLUMN = "schema_name"


def _rebase(row: dict, src_schema: str, dst_schema: str) -> dict:
    if row.get(_SCHEMA_COLUMN) != src_schema:
        return row
    return {**row, _SCHEMA_COLUMN: dst_schema}


def _carried_columns(table: Table, strip_identities: bool) -> list[str]:
    """The columns of ``table`` a copy supplies. For an IDENTITY_ONLY table, ``strip_identities``
    decides whether where it points (REQ-1491) and its boundness marker travel too: stripped for an
    ordinary branch, carried verbatim for a caller that asked for the real connection along with it
    (REQ-1602's sandbox visitor environments)."""
    names = [c.name for c in table.columns]
    if table.name in IDENTITY_ONLY and strip_identities:
        excluded = binding_columns(table.name)
        return [n for n in names if n not in excluded]
    return names


@dataclass
class TableDelta:
    """What one table's copy does, by key rather than by count alone — a report a person reads to
    decide whether to apply it has to say WHICH objects, not how many."""

    table: str
    added: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    unchanged: int = 0
    removed: list[str] = field(default_factory=list)

    @property
    def touched(self) -> bool:
        return bool(self.added or self.changed or self.removed)

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "added": self.added,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "removed": self.removed,
        }


@dataclass
class CopyReport:
    source_env: str
    target_env: str
    mode: str
    removals: bool
    tables: list[TableDelta] = field(default_factory=list)
    #: The commit both lines last held, and every object they each moved away from it differently
    #: (REQ-1555). ``base`` is None when the two environments share no ancestor, and the empty list
    #: under it then means NOTHING WAS COMPARED rather than nothing collided.
    base: str | None = None
    conflicts: list["Conflict"] = field(default_factory=list)

    @property
    def compared(self) -> bool:
        return self.base is not None

    @property
    def touches_connectivity(self) -> bool:
        """Whether the copy changed how a source is REACHED (REQ-1544).

        The same question :attr:`provisa.core.env_deploy.DeployDelta.touches_connectivity` answers,
        asked of a copy rather than of a tree, and answered here by TABLE because a copy's delta is
        already grouped by the registry it wrote. Both answers govern the same refresh, so a merge
        and a deploy that change the same registries cost the same.
        """
        from provisa.core.env_deploy import CONNECTIVITY

        return any(t.table in CONNECTIVITY and t.touched for t in self.tables)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_env": self.source_env,
            "target_env": self.target_env,
            "mode": self.mode,
            "removals": self.removals,
            "added": sum(len(t.added) for t in self.tables),
            "changed": sum(len(t.changed) for t in self.tables),
            "unchanged": sum(t.unchanged for t in self.tables),
            "removed": sum(len(t.removed) for t in self.tables),
            "tables": [t.as_dict() for t in self.tables if t.touched],
            "base": self.base,
            "compared": self.compared,
            "conflicts": [c.as_dict() for c in self.conflicts],
        }


REPLACE = "replace"
MERGE = "merge"


async def plan_copy(
    db: "Database",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    *,
    mode: str = MERGE,
    removals: bool = False,
    seed: bool = False,
    strip_identities: bool = True,
) -> CopyReport:
    """What :func:`copy_model` would do, without doing any of it (REQ-1490)."""
    async with db.acquire() as conn:
        return await _copy(
            conn,
            org_id,
            source_env,
            target_env,
            mode,
            removals,
            seed,
            strip_identities,
            apply=False,
        )


async def copy_model(
    db: "Database",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    *,
    mode: str = MERGE,
    removals: bool = False,
    seed: bool = False,
    strip_identities: bool = True,
) -> CopyReport:
    """Carry ``source_env``'s governed model into ``target_env``. One transaction (REQ-1490).

    ``seed`` is the creation path's alone: it additionally carries
    :data:`~provisa.core.env_classes.SEEDED_AT_CREATION` so a new environment opens with the roles
    and assignments of the one it came from. Every other copy leaves them where they are.

    ``strip_identities`` is REQ-1491's convenience, on by default: an IDENTITY_ONLY row a copy
    creates carries none of the source's binding columns and lands unbound. A caller that instead
    wants the real connection details copied along with the row -- REQ-1602's sandbox visitor
    environments -- passes ``strip_identities=False``.
    """
    async with db.acquire() as conn, conn.transaction():
        report = await _copy(
            conn, org_id, source_env, target_env, mode, removals, seed, strip_identities, apply=True
        )
    return report


async def adopt_role_definition(
    db: "Database",
    org_id: str,
    env: str | None,
    *,
    target: str,
    source: str,
) -> None:
    """In ``env`` alone, ``target``'s capabilities and demonstrated list become ``source``'s.

    REQ-1597 defines the sandbox visitor by subtraction from ``org_admin``, and the subtraction has
    to happen against the NAME the model's grants carry, not against a name of its own. A column is
    visible to a role when ``table_columns.visible_to`` NAMES that role
    (``schema_gen._build_visible_tables``), and every grant in a copied model names the roles the
    model was authored against. A role id no grant mentions therefore sees no column on any table,
    and ``schema_gen`` then drops each table that has none -- leaving a visitor exactly the tables
    whose API path/query parameters are exempt from the gate, and nothing else. The schema was not
    incomplete; it was correctly empty for a name nobody had granted anything to.

    So the visitor holds ``sandbox`` AND ``org_admin`` (``seat_redeemed_roles``), and the withholding
    happens HERE, against ``org_admin``, in the visitor's own environment -- capabilities resolve as
    the union over the holder's roles, so the union is the sandbox definition and no wider. The
    definition is not restated: it is read from this environment's own ``sandbox`` row, so
    REQ-1597's denylist and REQ-1602's demonstrated list keep exactly one author. Prod's
    ``org_admin`` is untouched -- REQ-1539 makes ``roles`` the environment's own answer once it has
    one, and this writes only the schema ``env`` names.
    """
    schema = org_schema(org_id, env)
    scoped = _scoped(org_roles, schema)
    async with db.acquire() as conn, conn.transaction():
        row = (
            await conn.execute_core(
                select(scoped.c.capabilities, scoped.c.demonstrated).where(scoped.c.id == source)
            )
        ).fetchone()
        if row is None:
            raise ValueError(f"{schema} has no role {source!r} to define {target!r} from")
        await conn.execute_core(
            scoped.update()
            .where(scoped.c.id == target)
            .values(
                capabilities=row._mapping["capabilities"],
                demonstrated=row._mapping["demonstrated"],
                # REQ-1624: and the derivation is RECORDED, not just applied. The tenancy seam
                # (db.apply_tenancy_role_grants) re-asserts org_admin's own rights into every
                # environment schema on every runtime build, so a subtraction applied once here was
                # given back on the visitor's next request -- environment_management among them,
                # which is how a sandbox visitor reached the org's environments surface. The column
                # is what that seam re-reads the definition from, so the subtraction survives it.
                defined_from=source,
            )
        )


async def _copy(
    conn: "Connection",
    org_id: str,
    source_env: str | None,
    target_env: str | None,
    mode: str,
    removals: bool,
    seed: bool,
    strip_identities: bool,
    *,
    apply: bool,
) -> CopyReport:
    if mode not in (REPLACE, MERGE):
        raise ValueError(f"unknown copy mode: {mode!r}")
    src_schema = org_schema(org_id, source_env)
    dst_schema = org_schema(org_id, target_env)
    if src_schema == dst_schema:
        raise ValueError(f"an environment cannot be copied onto itself ({src_schema})")
    report = CopyReport(source_env or "prod", target_env or "prod", mode, removals)

    if mode == MERGE and not seed:
        # REQ-1555: which of the target's own work this merge carries away. FIRST, because the
        # answer is about the target as it stands: once the copy has run the target holds the
        # source's model and there is nothing left to notice. Asked of a MERGE only -- a REPLACE is
        # the operator saying which model the environment runs, so the target's divergence from the
        # source is the point of the act rather than a collision inside it.
        report.base, report.conflicts = await detect_conflicts(
            conn, org_id, source_env, target_env, src_schema, dst_schema
        )

    # REQ-1539: the seeded classes travel on a creation and never again. Who may do what is the
    # target environment's own answer once it has one, so a merge cannot carry prod's `developer`
    # row away and replace it with the branch's.
    carried = CARRIED if seed else CARRIED - SEEDED_AT_CREATION
    ordered = [
        t for t in org_metadata.sorted_tables if t.name in carried or t.name in IDENTITY_ONLY
    ]
    for table in ordered:
        # An IDENTITY_ONLY row is never removed by a copy: its binding is the environment's own,
        # deliberately established fact (REQ-1491), and nothing arriving from another environment
        # is evidence that it should go.
        table_removals = (removals or mode == REPLACE) and table.name in carried
        report.tables.append(
            await _copy_table(
                conn, table, src_schema, dst_schema, table_removals, strip_identities, apply
            )
        )
    settings_removals = removals or mode == REPLACE
    report.tables.append(
        await _copy_settings(conn, src_schema, dst_schema, settings_removals, apply)
    )
    if apply:
        await _resync_sequences(conn, ordered, dst_schema)
    return report


async def _rows(conn: "Connection", table: Table) -> dict[tuple, dict]:
    result = await conn.execute_core(select(table))
    return {_key(table, dict(r._mapping)): dict(r._mapping) for r in result.fetchall()}


async def _copy_table(
    conn: "Connection",
    table: Table,
    src_schema: str,
    dst_schema: str,
    removals: bool,
    strip_identities: bool,
    apply: bool,
) -> TableDelta:
    src = _scoped(table, src_schema)
    dst = _scoped(table, dst_schema)
    columns = _carried_columns(table, strip_identities)
    delta = TableDelta(table.name)

    source_rows = {
        k: _rebase(r, src_schema, dst_schema) for k, r in (await _rows(conn, src)).items()
    }
    target_rows = await _rows(conn, dst)

    inserts: list[dict] = []
    updates: list[tuple[tuple, dict]] = []
    for key, row in source_rows.items():
        carried = {c: row[c] for c in columns}
        current = target_rows.get(key)
        if current is None:
            delta.added.append(_render(key))
            if table.name in IDENTITY_ONLY and strip_identities:
                # REQ-1491: a row this copy creates points nowhere until the environment binds it.
                carried[BOUND_COLUMN] = False
            inserts.append(carried)
        elif any(current[c] != carried[c] for c in columns):
            delta.changed.append(_render(key))
            updates.append((key, {c: carried[c] for c in columns if current[c] != carried[c]}))
        else:
            delta.unchanged += 1

    gone = [k for k in target_rows if k not in source_rows]
    if removals:
        delta.removed = [_render(k) for k in gone]

    if not apply:
        return delta
    if delta.removed:
        for key in gone:
            await conn.execute_core(delete(dst).where(_where(dst, key)))
    for key, values in updates:
        await conn.execute_core(dst.update().where(_where(dst, key)).values(**values))
    await _insert_rows(conn, dst, inserts)
    return delta


async def _copy_settings(
    conn: "Connection", src_schema: str, dst_schema: str, removals: bool, apply: bool
) -> TableDelta:
    """org_settings is classified PER KEY (REQ-1489): the governance keys travel and the keys naming
    an external target or a per-environment runtime stay with the environment that set them."""
    src = _scoped(org_settings, src_schema)
    dst = _scoped(org_settings, dst_schema)
    delta = TableDelta(org_settings.name)

    source_rows = {k: r for k, r in (await _rows(conn, src)).items() if carries_setting(k[0])}
    target_rows = await _rows(conn, dst)

    inserts: list[dict] = []
    updates: list[tuple[tuple, dict]] = []
    for key, row in source_rows.items():
        values = {"key": row["key"], "value": row["value"], "updated_by": row["updated_by"]}
        current = target_rows.get(key)
        if current is None:
            delta.added.append(_render(key))
            inserts.append(values)
        elif current["value"] != row["value"]:
            delta.changed.append(_render(key))
            updates.append((key, {"value": row["value"], "updated_by": row["updated_by"]}))
        else:
            delta.unchanged += 1

    gone = [k for k in target_rows if carries_setting(k[0]) and k not in source_rows]
    if removals:
        delta.removed = [_render(k) for k in gone]

    if not apply:
        return delta
    if delta.removed:
        for key in gone:
            await conn.execute_core(delete(dst).where(_where(dst, key)))
    for key, values in updates:
        await conn.execute_core(dst.update().where(_where(dst, key)).values(**values))
    await _insert_rows(conn, dst, inserts)
    return delta


def _where(table: Table, key: tuple) -> Any:
    from sqlalchemy import and_

    return and_(*(c == v for c, v in zip(table.primary_key.columns, key, strict=True)))


def _render(key: tuple) -> str:
    return "/".join(str(part) for part in key)


async def _insert_rows(conn: "Connection", table: Table, rows: list[dict]) -> None:
    """Insert in chunks small enough for one statement. A column absent from ``rows`` takes the
    table's server default, which is how an unbound source ends up with the empty host REQ-1491
    marks rather than a NULL the column would refuse."""
    if not rows:
        return
    width = max(len(r) for r in rows)
    chunk = max(1, _MAX_BIND_PARAMS // width)
    for start in range(0, len(rows), chunk):
        await conn.execute_core(table.insert().values(rows[start : start + chunk]))


async def _resync_sequences(conn: "Connection", tables: list[Table], schema: str) -> None:
    """Advance each copied table's identity sequence past the ids the copy carried.

    A copy carries primary keys verbatim — it has to, because the model references them — so a
    target whose sequence still sits at 1 would hand the next insert an id the copy already used.
    PostgreSQL only: it is the one backend whose serial columns have a sequence to advance, and
    the dialect is dispatched explicitly rather than attempted and swallowed.
    """
    if conn.capabilities.dialect != "postgresql":
        return
    for table in tables:
        pk = list(table.primary_key.columns)
        if len(pk) != 1 or pk[0].type.python_type is not int:
            continue
        column, qualified = pk[0].name, f"{schema}.{table.name}"
        await conn.execute(
            f"SELECT setval(pg_get_serial_sequence('{qualified}', '{column}'), "
            f"COALESCE((SELECT MAX({column}) FROM {qualified}), 0) + 1, false) "
            f"WHERE pg_get_serial_sequence('{qualified}', '{column}') IS NOT NULL"
        )
