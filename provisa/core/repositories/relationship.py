# Copyright (c) 2026 Kenneth Stott
# Canary: b8353796-185d-43e1-810b-0febb812669d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship repository — CRUD for relationships in PG config DB."""

# Requirements: REQ-018, REQ-019, REQ-020, REQ-399, REQ-400

from provisa.core import model_change, model_uri
from provisa.core.builtin_relationship_prefix import BUILTIN_RELATIONSHIP_ID_PREFIXES
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, func, or_, select, update

from provisa.compiler.sql_types import key_list
from provisa.core.models import Relationship
from provisa.core.repositories import retirement
from provisa.core.repositories import table as table_repo
from provisa.core.relationship_naming import RelationshipNameTaken
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.schema_org import registered_tables, relationships, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection


class TableNotRegistered(ValueError):
    """A relationship names a table no registration holds (yet)."""

    def __init__(self, end: str, table_name: str) -> None:
        super().__init__(f"{end} table not registered: {table_name}")
        self.table_name = table_name


class ReservedRelationshipPrefix(ValueError):
    """A relationship id carries a prefix reserved for Provisa's built-in relationships. The
    startup seed removes built-in rows it no longer declares, recognising them by that prefix, so
    a relationship a person saved under one would be removed at the next boot."""

    def __init__(self, rel_id: str, prefix: str) -> None:
        self.rel_id = rel_id
        self.prefix = prefix
        super().__init__(
            f"relationship id {rel_id!r} starts with {prefix!r}, a prefix reserved for "
            "built-in relationships"
        )


class RelationshipWithoutColumns(ValueError):
    """A relationship names no column at one end (``end``: "source" or "target"). Every
    relationship joins on a source column, and on a target column unless its target is a
    function; one without them joins nothing and is never served (REQ-019)."""

    def __init__(self, rel_id: str, end: str) -> None:
        self.rel_id = rel_id
        self.end = end
        super().__init__(f"relationship {rel_id!r} names no {end} column")


def missing_column_end(rel: Relationship) -> str | None:
    """The end of ``rel`` that names no column, or None when both do."""
    if not rel.source_column:
        return "source"
    if not rel.target_function_name and not rel.target_column:
        return "target"
    return None


def reserved_prefix_of(rel_id: str) -> str | None:
    """The reserved built-in prefix ``rel_id`` starts with, or None. The prefixes are the one
    definition in core that the seed's removal of undeclared rows uses too."""
    return next((p for p in BUILTIN_RELATIONSHIP_ID_PREFIXES if rel_id.startswith(p)), None)


#: The columns a save writes over a stored relationship (REQ-020).
_SAVED_COLUMNS = (
    "source_table_id",
    "target_table_id",
    "source_column",
    "target_column",
    "cardinality",
    "materialize",
    "refresh_interval",
    "target_function_name",
    "function_arg",
    "alias",
    "graphql_alias",
    "disable_cypher",
    "hide_target_meta",
    "same_entity",
    "via_table_id",
    "via_source_column",
    "via_target_column",
    "via_type_column",
    "via_type_value",
    "via_label_source",
    "source_json_key",
    "parameters",
)


async def upsert(
    conn: "Connection", rel: Relationship, *, derive_target_key: bool = True
) -> None:  # REQ-019, REQ-020, REQ-399, REQ-400, REQ-413, REQ-1919
    """Upsert a relationship. Resolves table names to registered_tables IDs.

    The source column is marked a foreign key. ``derive_target_key`` also marks a many-to-one or
    one-to-one target column the target's primary key (or an alternate key where the table has
    another): an accepted discovery suggestion passes False, marking the foreign key alone
    (REQ-413: no column is marked a key from a sample or an accepted suggestion).

    Refuses (:class:`RelationshipWithoutColumns`) a relationship naming no column at an end.
    Refuses (:class:`ReservedRelationshipPrefix`) an id with a built-in prefix: the seed writes
    its own relationships directly (``startup_seed._seed_meta_relationships``) and is the only
    writer of those ids; every caller of this function is not the seed."""
    prefix = reserved_prefix_of(rel.id)
    if prefix is not None:
        raise ReservedRelationshipPrefix(rel.id, prefix)
    missing = missing_column_end(rel)
    if missing is not None:
        raise RelationshipWithoutColumns(rel.id, missing)
    source_tbl = await table_repo.find_by_table_name(conn, rel.source_table_id)
    if source_tbl is None:
        raise TableNotRegistered("Source", rel.source_table_id)
    model_change.name(  # REQ-1524
        "upsert",
        model_change.uri(
            model_uri.named_relationship_uri,
            source_tbl["domain_id"],
            source_tbl["alias"] or source_tbl["table_name"],
            rel.alias,
            rel.id,
        ),
    )

    target_tbl_id = None
    if rel.target_function_name:
        # Computed relationship — no target table
        target_tbl_id = None
    else:
        target_tbl = await table_repo.find_by_table_name(conn, rel.target_table_id)
        if target_tbl is None:
            raise TableNotRegistered("Target", rel.target_table_id)
        target_tbl_id = target_tbl["id"]

    # REQ-1586: a junction-backed edge names a registered associative table; an unregistered
    # junction is a declaration error, not something to skip past.
    via_tbl_id = None
    if rel.via_table:
        via_tbl = await table_repo.find_by_table_name(conn, rel.via_table)
        if via_tbl is None:
            raise TableNotRegistered("Junction", rel.via_table)
        via_tbl_id = via_tbl["id"]

    # REQ-2248: a retired relationship keeps its id; a save under it would bring it back.
    held = await get(conn, rel.id)
    if held is not None and held["retired"]:
        raise retirement.RetiredNameTaken("relationship", rel.id)

    if await _provided_elsewhere(conn, rel, source_tbl["id"], target_tbl_id):
        return

    vals = {
        "id": rel.id,
        "source_table_id": source_tbl["id"],
        "target_table_id": target_tbl_id,
        "source_column": rel.source_column,
        "target_column": rel.target_column or None,
        "cardinality": rel.cardinality.value,
        "materialize": rel.materialize,
        "refresh_interval": rel.refresh_interval,
        "target_function_name": rel.target_function_name,
        "function_arg": rel.function_arg,
        "alias": rel.alias or None,
        "graphql_alias": rel.graphql_alias or None,
        "disable_cypher": rel.disable_cypher,
        "hide_target_meta": rel.hide_target_meta,
        "same_entity": rel.same_entity,
        "via_table_id": via_tbl_id,
        "via_source_column": rel.via_source_column or None,
        "via_target_column": rel.via_target_column or None,
        "via_type_column": rel.via_type_column or None,
        "via_type_value": rel.via_type_value or None,
        "via_label_source": rel.via_label_source or None,
        "source_json_key": rel.source_json_key or None,
        "parameters": rel.parameters,
        "version": rel.version,
        "needs_review": rel.needs_review,
    }
    # REQ-020: the version counts changes. A save of what is stored, unflagged, is none: a demo's
    # rebuild saves every relationship its configuration declares at every start (REQ-1919).
    unchanged = (
        held is not None
        and not held["needs_review"]
        and all(held[c] == vals[c] for c in _SAVED_COLUMNS)
    )
    try:
        # REQ-020: on a change bump version, clear the re-review flag (a save is an explicit
        # re-review), and preserve the original owner (owner is not in the update set).
        await conn.upsert(
            relationships,
            vals,
            index_elements=["id"],
            update_columns=list(_SAVED_COLUMNS),
            set_extra={
                "version": relationships.c.version if unchanged else relationships.c.version + 1,
                "needs_review": False,
            },
        )
    except Exception as e:
        if "relationships_source_alias_unique" in str(e):
            # REQ-415: a typed alias that is taken is refused, naming the relationship it is.
            held_by = (
                await conn.execute_core(
                    select(relationships.c.id).where(
                        relationships.c.source_table_id == source_tbl["id"],
                        relationships.c.alias == rel.alias,
                    )
                )
            ).scalar_one()
            raise RelationshipNameTaken("alias", str(rel.alias), held_by) from e
        raise

    # REQ-351: its effective names, derived once and stored, for the meta domain and every reader.
    await store_effective_names(conn, [rel.id])

    # REQ-1586: the junction's two keys are foreign keys on the junction table.
    if via_tbl_id:
        # Each end may list several columns for a composite key, and every one of them is a
        # foreign key on the junction table.
        for col in (*key_list(rel.via_source_column), *key_list(rel.via_target_column)):
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == via_tbl_id,
                    table_columns.c.column_name == col,
                )
                .values(is_foreign_key=True)
            )

    # Mark source_column as FK on source table
    if rel.source_column:
        await conn.execute_core(
            update(table_columns)
            .where(
                table_columns.c.table_id == source_tbl["id"],
                table_columns.c.column_name == rel.source_column,
            )
            .values(is_foreign_key=True)
        )

    # Mark target_column as PK (or AK if another PK already exists) on target table.
    # Applies to many-to-one and one-to-one: target_column is the PK of the target table.
    # For one-to-many, target_column is a FK in the target — do not mark as PK.
    from provisa.core.models import Cardinality

    if (
        derive_target_key
        and target_tbl_id
        and rel.target_column
        and rel.cardinality
        in (
            Cardinality.many_to_one,
            Cardinality.one_to_one,
        )
    ):
        result = await conn.execute_core(
            select(func.count())
            .select_from(table_columns)
            .where(
                table_columns.c.table_id == target_tbl_id,
                table_columns.c.is_primary_key == True,  # noqa: E712
                table_columns.c.column_name != rel.target_column,
            )
        )
        conflicting_pk = result.scalar()
        if conflicting_pk:
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == target_tbl_id,
                    table_columns.c.column_name == rel.target_column,
                )
                .values(is_alternate_key=True)
            )
        else:
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == target_tbl_id,
                    table_columns.c.column_name == rel.target_column,
                )
                # REQ-2190: a primary key is NOT NULL, as the column's registration records it.
                .values(is_primary_key=True, not_null=True)
            )
    # REQ-2141: a foreign key's ref is its target's Class term; the relationship says which.
    from provisa.core.repositories import glossary as glossary_repo

    await glossary_repo.rehome_keys(
        conn, {t for t in (source_tbl["id"], target_tbl_id, via_tbl_id) if t is not None}
    )


#: The id every relationship a remote GraphQL source's schema offers is kept under
#: (``graphql_remote.mapper._detect_relationships``): ``gql_remote__<source>__<table>__<field>``.
REMOTE_OFFERED_PREFIX = "gql_remote__"


def _offered(rel_id: str) -> bool:
    return rel_id.startswith(REMOTE_OFFERED_PREFIX)


async def _provided_elsewhere(
    conn: "Connection", rel: Relationship, source_id: int, target_id: int | None
) -> bool:
    """One relationship is kept once (REQ-313, REQ-194). A relationship a remote source's schema
    offers that joins the same tables on the same columns as one declared (in the config, or by
    an administrator) is that relationship: it is not kept beside it -- and one kept before is
    removed -- since two of one type under one field are refused by the schema build. Saving the
    declared one removes an offered one it is. True when ``rel`` is not to be saved."""
    same = (
        await conn.execute_core(
            select(relationships.c.id).where(
                relationships.c.source_table_id == source_id,
                relationships.c.target_table_id == target_id,
                relationships.c.source_column == rel.source_column,
                relationships.c.target_column == (rel.target_column or None),
                relationships.c.id != rel.id,
            )
        )
    ).fetchall()
    others = [row[0] for row in same]
    if _offered(rel.id):
        if not any(not _offered(other) for other in others):
            return False
        if await get(conn, rel.id) is not None:  # one kept before the declared one was saved
            await _remove(conn, [rel.id])
        return True
    await _remove(conn, [other for other in others if _offered(other)])
    return False


async def _remove(conn: "Connection", rel_ids: list[str]) -> None:
    """Remove offered relationships a declared one is, with their parts (their tag assignments).
    REQ-2248 (amended 2026-11-15): de-duplication may remove a true duplicate; it is not a
    person's object."""
    for rel_id in rel_ids:
        await remove_parts(conn, ObjectRef("relationship", rel_id))
        await remove_where(conn, relationships.c.id == rel_id)


async def get(conn: "Connection", rel_id: str) -> dict | None:  # REQ-018, REQ-019
    result = await conn.execute_core(select(relationships).where(relationships.c.id == rel_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection", *, retired: bool = False) -> list[dict]:  # REQ-018, REQ-019
    """Every relationship in service; ``retired`` lists the retired ones instead (REQ-2248),
    with who, when and why."""
    result = await conn.execute_core(
        select(relationships)
        .where(relationships.c.retired.is_(retired))
        .order_by(relationships.c.id)
    )
    return [dict(r._mapping) for r in result.fetchall()]


class RelationshipDeleteRefused(Exception):
    """A relationship that may not be deleted because published views rely on it;
    ``dependents`` lists them."""

    def __init__(self, rel_id: str, dependents: list[Dependent]) -> None:
        self.rel_id = rel_id
        self.dependents = dependents
        # What a refusal says before naming its dependents (REQ-2152: the API layer names only
        # those its caller may read).
        self.lead = f"Relationship {rel_id!r} is still relied on by"
        named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in dependents)
        super().__init__(f"Relationship {rel_id!r} is still relied on by: {named}")


async def _name_change(conn: "Connection", action: str, rel_id: str) -> None:  # REQ-1524
    """Name a change to relationship ``rel_id`` by its model URI: at its source table's model
    name, by its alias (REQ-1385)."""
    row = (
        await conn.execute_core(
            select(
                relationships.c.alias,
                registered_tables.c.domain_id,
                registered_tables.c.alias.label("table_alias"),
                registered_tables.c.table_name,
            )
            .join(registered_tables, registered_tables.c.id == relationships.c.source_table_id)
            .where(relationships.c.id == rel_id)
        )
    ).one()
    model_change.name(
        action,
        model_change.uri(
            model_uri.named_relationship_uri,
            row.domain_id,
            row.table_alias or row.table_name,
            row.alias,
            rel_id,
        ),
    )


async def delete(  # REQ-019, REQ-1918, REQ-2248
    conn: "Connection", rel_id: str, *, reason: str | None = None
) -> bool:
    """Delete one relationship: THE delete, for every surface. It RETIRES the relationship
    (REQ-2248): the row, its id, its alias and its tag assignments are kept, the relationship
    marked retired with ``reason``, the acting person and the time. False when there is none, or
    it is retired already.

    A relationship stands on its own — it goes before the tables it joins. It is refused
    (:class:`RelationshipDeleteRefused`) only while a materialized view is published over it:
    a view joins on the relationship's columns and no other relationship approves that join
    (REQ-1140). One transaction.
    """
    ref = ObjectRef("relationship", rel_id)
    async with conn.transaction():
        held = await get(conn, rel_id)
        if held is None or held["retired"]:
            return False
        await _name_change(conn, "retire", rel_id)
        blocking = await guard(conn, ref)
        if blocking:
            raise RelationshipDeleteRefused(rel_id, blocking)
        await conn.execute_core(
            update(relationships)
            .where(relationships.c.id == rel_id)
            .values(**retirement.values(reason))
        )
        # REQ-2141: retired, it declares no key; the refs its key derived are settled again.
        from provisa.core.repositories import glossary as glossary_repo

        joined = {
            t
            for t in (held["source_table_id"], held["target_table_id"], held["via_table_id"])
            if t is not None
        }
        await glossary_repo.rehome_keys(conn, joined)
        # REQ-2219: an object gone has no owners or stewards in object_owners.
        from provisa.entitlements.owners import write_object_owners

        await write_object_owners(conn)
    return True


async def restore(conn: "Connection", rel_id: str) -> bool:  # REQ-2248
    """Restore a retired relationship: the same relationship, its retirement cleared, traversable
    again. Its key declares again, so the refs it derives are settled again (REQ-2141). False
    when there is none, or it is not retired."""
    async with conn.transaction():
        held = await get(conn, rel_id)
        if held is None or not held["retired"]:
            return False
        await _name_change(conn, "restore", rel_id)
        await conn.execute_core(
            update(relationships).where(relationships.c.id == rel_id).values(**retirement.cleared())
        )
        from provisa.core.repositories import glossary as glossary_repo

        joined = {
            t
            for t in (held["source_table_id"], held["target_table_id"], held["via_table_id"])
            if t is not None
        }
        await glossary_repo.rehome_keys(conn, joined)
        # REQ-2219: a restored object's owners and stewards are back in object_owners.
        from provisa.entitlements.owners import write_object_owners

        await write_object_owners(conn)
    return True


async def remove_where(conn: "Connection", *where) -> None:
    """Remove every relationship matching ``where`` (clauses on ``relationships``) — for the two
    callers REQ-2248 (amended 2026-11-15) allows a removal: de-duplication removing a true
    duplicate (:func:`_remove`) and the startup seed removing a built-in relationship it no
    longer declares. Any other relationship is retired (:func:`delete`). It does not ask the
    dependency guard."""
    await conn.execute_core(_delete(relationships).where(*where))


async def store_effective_names(conn: "Connection", ids: list[str] | None = None) -> None:
    """Derive and store the effective names of the relationships ``ids`` (every one when None),
    in the naming convention now in force. Called where a relationship is saved and by every schema
    build, which follows each save, configuration load and change of naming convention. A row
    whose stored names are already these is not written, and with none to write no change is
    opened: a worker reloading another's change builds outside any model change scope. Names that
    do differ are written inside one (REQ-1524)."""
    from provisa.compiler.naming import active_gql_convention

    from provisa.core.relationship_names import (
        derive_graphql_alias,
        effective_cypher_type,
        target_field_name,
    )
    from provisa.core.schema_org import naming_rules, sources

    convention = active_gql_convention()
    # REQ-155: the derived field is named for the target's GraphQL field, which the naming rules
    # shape, in the order the schema build applies them.
    rules = [
        {"pattern": r.pattern, "replacement": r.replacement}
        for r in (
            await conn.execute_core(
                select(naming_rules.c.pattern, naming_rules.c.replacement).order_by(
                    naming_rules.c.id
                )
            )
        ).fetchall()
    ]
    target = registered_tables.alias("target")
    target_source = sources.alias("target_source")
    via = registered_tables.alias("via")
    query = select(
        relationships.c.id,
        relationships.c.alias,
        relationships.c.graphql_alias,
        relationships.c.source_column,
        relationships.c.cardinality,
        relationships.c.via_label_source,
        relationships.c.via_type_value,
        relationships.c.cypher_type,
        relationships.c.graphql_field,
        target.c.table_name.label("target_table_name"),
        target.c.alias.label("target_alias"),
        target.c.gql_naming_convention.label("target_convention"),
        target_source.c.gql_naming_convention.label("target_source_convention"),
        via.c.table_name.label("via_table_name"),
    ).select_from(
        relationships.outerjoin(target, target.c.id == relationships.c.target_table_id)
        .outerjoin(target_source, target_source.c.id == target.c.source_id)
        .outerjoin(via, via.c.id == relationships.c.via_table_id)
    )
    if ids is not None:
        query = query.where(relationships.c.id.in_(ids))
    rows = [r._mapping for r in (await conn.execute_core(query)).fetchall()]
    respelled: list[tuple[str, str | None, str | None]] = []
    for r in rows:
        cypher_type = effective_cypher_type(
            alias=r["alias"],
            source_column=r["source_column"] or "",
            cardinality=r["cardinality"] or "",
            via_label_source=r["via_label_source"],
            via_type_value=r["via_type_value"],
            via_table_name=r["via_table_name"],
        )
        graphql_field = r["graphql_alias"] or derive_graphql_alias(
            target_field_name(
                r["target_table_name"],
                r["target_alias"],
                rules,
                r["target_convention"] or r["target_source_convention"],
            )
            if r["target_table_name"]
            else "",
            r["cardinality"] or "",
            convention,
            via_label_source=r["via_label_source"],
            via_type_value=r["via_type_value"],
            via_table_name=r["via_table_name"],
            cypher_alias=r["alias"],
        )
        if (r["cypher_type"], r["graphql_field"]) != (cypher_type, graphql_field):
            respelled.append((r["id"], cypher_type, graphql_field))
    if not respelled:
        return
    # REQ-1524: a model write is part of a change. A save or a load writing these is that change
    # (the scope joins it); a build that finds them in another naming convention -- after the
    # convention changed -- writes them as a change of its own, never outside one.
    from provisa.core import model_change

    async with model_change.scope("relationship names follow the naming convention"):
        for rel_id, cypher_type, graphql_field in respelled:
            await conn.execute_core(
                update(relationships)
                .where(relationships.c.id == rel_id)
                .values(cypher_type=cypher_type, graphql_field=graphql_field)
            )


async def flag_for_review(conn: "Connection", rel_ids: list[str]) -> None:  # REQ-020, REQ-1531
    """Set ``needs_review`` on the relationships ``rel_ids``, their version unchanged: a
    cross-domain edge is the other domain's to review."""
    await conn.execute_core(
        update(relationships).where(relationships.c.id.in_(rel_ids)).values(needs_review=True)
    )


async def set_code_resolver(  # REQ-1919
    conn: "Connection", rel_ids: list[str], set_id: str
) -> None:
    """Mark the relationships ``rel_ids`` as the code resolver set ``set_id``'s."""
    await conn.execute_core(
        update(relationships).where(relationships.c.id.in_(rel_ids)).values(code_resolver=set_id)
    )


async def seed(  # REQ-1919
    conn: "Connection", row: dict[str, Any], update_columns: list[str]
) -> None:
    """Write a built-in relationship the startup seed declares: created when absent, and
    ``update_columns`` brought to ``row``'s values when present."""
    await conn.upsert(relationships, row, index_elements=["id"], update_columns=update_columns)


async def insert_discovered(  # REQ-399, REQ-413, REQ-415, REQ-1919
    conn: "Connection",
    rel_id: str,
    src_id: int,
    tgt_id: int,
    src_col: str,
    tgt_col: str,
    cardinality: str,
    alias: str | None,
    graphql_alias: str | None,
) -> bool:
    """Insert a relationship a foreign key was found for; True when it is new, False when a
    relationship with its id was already there (which is left as it is)."""
    result = await conn.execute(
        """
        INSERT INTO relationships
            (id, source_table_id, target_table_id, source_column, target_column, cardinality,
             alias, graphql_alias)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT DO NOTHING
        """,
        rel_id,
        src_id,
        tgt_id,
        src_col,
        tgt_col,
        cardinality,
        alias,
        graphql_alias,
    )
    return result == "INSERT 0 1"


async def mark_relationships_for_review(  # REQ-020
    conn: "Connection", table_id: int, valid_columns: list[str]
) -> list[str]:
    """Flag relationships whose join column on ``table_id`` is no longer present (REQ-020).

    Called after a table's columns change; any relationship whose source/target join
    column on this table is absent from ``valid_columns`` is flagged ``needs_review``
    (and its version bumped). Returns the flagged relationship ids.
    """
    result = await conn.execute_core(
        select(
            relationships.c.id,
            relationships.c.source_table_id,
            relationships.c.target_table_id,
            relationships.c.source_column,
            relationships.c.target_column,
        ).where(
            or_(
                relationships.c.source_table_id == table_id,
                relationships.c.target_table_id == table_id,
            )
        )
    )
    rows = result.fetchall()
    valid = set(valid_columns)
    flagged: list[str] = []
    for r in rows:
        stale = (
            r._mapping["source_table_id"] == table_id
            and r._mapping["source_column"]
            and r._mapping["source_column"] not in valid
        ) or (
            r._mapping["target_table_id"] == table_id
            and r._mapping["target_column"]
            and r._mapping["target_column"] not in valid
        )
        if stale:
            flagged.append(r._mapping["id"])
    if flagged:
        await conn.execute_core(
            update(relationships)
            .where(relationships.c.id.in_(flagged))
            .values(needs_review=True, version=relationships.c.version + 1)
        )
    return flagged
