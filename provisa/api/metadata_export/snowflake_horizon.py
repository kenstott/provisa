# Copyright (c) 2026 Kenneth Stott
# Canary: 3e2d3e46-e42a-46d6-b2e2-a419c58c2b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake Horizon Catalog adapter (REQ-1635).

Engine-native, unlike every other adapter in this package: OpenMetadata/Atlas/DataHub/Atlan/
Collibra all speak to a REMOTE REST catalog reached over ``MetadataExportConfig.endpoint`` with
its own credentials. Horizon Catalog is not remote — it is Snowflake's own governance surface,
living inside the SAME Snowflake account the engine already runs governed SQL against. So this
adapter opens its own :class:`SnowflakeFederationRuntime` against the engine's configured DSN
(``configured_engine_url()``) rather than reading ``endpoint``/``api_key`` — those fields describe
a second, unrelated connection this adapter has no use for.

Only a DataProduct publishes here (REQ-1592/REQ-1634): each one becomes a Snowflake SHARE over its
member tables' PHYSICAL addresses, granted USAGE/SELECT, then wrapped in an internal
``CREATE ORGANIZATION LISTING`` (distribution: ORGANIZATION) so it surfaces as a first-class Data
Product in the account's own Horizon Catalog / Data sharing UI — NOT ``CREATE EXTERNAL LISTING``,
which is Marketplace-shaped for other Snowflake organizations and never shows up there. A member
table's physical address is resolved the same way the engine resolves it for
query execution (:func:`provisa.core.catalog._to_catalog_name` on the source id) — REQ-1637 lands
a non-attachable source as a VIEW at that physical name, and a share over the view works exactly
like a share over a table.

Each ``DataProductAsset`` (REQ-1634) exposes ``id``, ``name``, ``description`` and
``members: tuple[AssetRef, ...]`` (table-kind refs, ``(source_id, schema_name, table_name)`` — the
same shape :func:`provisa.api.metadata_export.refs.table_ref` produces). A snapshot with no
DataProducts publishes nothing here, which is correct, not a fallback masking a bug.

``MetadataSnapshot.model_tags`` publish independently of DataProducts, as native Snowflake TAG
objects (``CREATE TAG`` + ``ALTER TABLE/COLUMN ... SET TAG``) applied to the tagged table/column's
physical address — see :func:`tag_statements`. A ``ModelTag`` is a steward-assigned classification,
which is what Snowflake's TAG primitive is for.

``MetadataSnapshot.governance_tags`` (REQ-1071) are a different kind of fact: which enforcement
rule fired on an asset, and the roles it restricts/exempts. That is metadata about the asset, not
a classification value, so it publishes as appended COMMENT text instead (:func:`comment_statements`,
alongside REQ-1647's description text) — never as a TAG.
"""

# Requirements: REQ-1068, REQ-1635

from __future__ import annotations

from dataclasses import dataclass

import re
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.model import AssetKind
from provisa.api.metadata_export.provider import (
    AssetError,
    AssetRefStub,
    MetadataExport,
    PublishResult,
)
from provisa.api.metadata_export.registry import register_provider
from provisa.federation.engine import configured_engine_url
from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime
from snowflake.connector.errors import ProgrammingError

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        AssetRef,
        GovernanceTag,
        MetadataSnapshot,
        ModelTag,
        RelationshipEdge,
        TableAsset,
    )

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Schema (inside the landing database) that Horizon-native TAG objects live in. A fixed, dedicated
# schema keeps governance/model tags out of any source's own schema namespace.
_TAGS_SCHEMA = "PROVISA_GOVERNANCE"


def _identifier(raw: str) -> str:
    name = raw.replace("-", "_")
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"invalid Snowflake identifier: {raw!r}")
    return name


def _escape(value: str) -> str:
    return value.replace("'", "''")


def physical_parts(ref: "AssetRef") -> tuple[str, str, str]:
    """A table ref's governed physical address — mirrors ``SnowflakeFederationRuntime._phys_parts``,
    which takes a ``Source``-like object rather than a ref; this takes the ref directly since that
    is all a published ``AssetRef`` carries."""
    from provisa.core.catalog import _to_catalog_name

    if ref.kind is not AssetKind.TABLE or len(ref.parts) != 3:
        raise ValueError(f"expected a table ref (source, schema, table); got {ref!r}")
    source_id, schema_name, table_name = ref.parts
    return _to_catalog_name(source_id), schema_name, table_name


def physical_table_and_column(ref: "AssetRef") -> tuple[tuple[str, str, str], str | None]:
    """A table OR column ref's governed physical address: ``(database, schema, table)`` plus the
    column name when ``ref`` is a column (``None`` for a table ref) — the shape
    :func:`tag_statements` needs to target either ``ALTER TABLE ... SET TAG`` or
    ``ALTER TABLE ... MODIFY COLUMN ... SET TAG``."""
    from provisa.core.catalog import _to_catalog_name

    if ref.kind is AssetKind.TABLE and len(ref.parts) == 3:
        source_id, schema_name, table_name = ref.parts
        return (_to_catalog_name(source_id), schema_name, table_name), None
    if ref.kind is AssetKind.COLUMN and len(ref.parts) == 4:
        source_id, schema_name, table_name, column_name = ref.parts
        return (_to_catalog_name(source_id), schema_name, table_name), column_name
    raise ValueError(f"expected a table or column ref; got {ref!r}")


def tag_statements(tags_database: str, model_tags: list["ModelTag"]) -> list[str]:
    """DDL creating Snowflake native TAG objects (in ``tags_database``.``_TAGS_SCHEMA``) for each
    distinct model tag id actually present, then applying them to the tagged tables/columns via
    ``SET TAG``. Pure — no I/O — so the shape is testable without a live Snowflake connection.

    Only ``ModelTag``s publish here: a steward-assigned classification is what Snowflake's TAG
    primitive is for. ``GovernanceTag`` facts (REQ-1071) — which rule fired, and the roles it
    restricts/exempts — are metadata about an asset, not a classification value, so they publish
    as COMMENT text instead (:func:`comment_statements`), never as a TAG.

    A ``ModelTag``'s value is its ``reason`` when set, else its own ``tag_id``. Relationship-scoped
    ``ModelTag``s (``relationship_id`` set, no ``asset`` — REQ-1378) have no physical asset to tag
    in Snowflake and are skipped; they ride the governance document instead."""

    def tag_fq(name: str) -> str:
        return f'"{tags_database}"."{_TAGS_SCHEMA}"."{name}"'

    def set_tag(ref: "AssetRef", tag_name: str, value: str) -> str:
        (database, schema, table), column = physical_table_and_column(ref)
        tag_ref = tag_fq(tag_name)
        escaped = _escape(value)
        if column is None:
            return (
                f'ALTER TABLE "{database}"."{schema}"."{table}" SET TAG {tag_ref} = \'{escaped}\';'
            )
        return (
            f'ALTER TABLE "{database}"."{schema}"."{table}" MODIFY COLUMN "{column}" '
            f"SET TAG {tag_ref} = '{escaped}';"
        )

    stmts = [f'CREATE SCHEMA IF NOT EXISTS "{tags_database}"."{_TAGS_SCHEMA}"']
    model_tag_ids = sorted(
        {_identifier(t.tag_id.upper()) for t in model_tags if t.asset is not None}
    )
    for tag_id in model_tag_ids:
        stmts.append(f"CREATE TAG IF NOT EXISTS {tag_fq(tag_id)};")
    for mtag in model_tags:
        if mtag.asset is None:
            continue
        stmts.append(
            set_tag(mtag.asset, _identifier(mtag.tag_id.upper()), mtag.reason or mtag.tag_id)
        )
    return stmts


def _set_comment(kind: str, parts: tuple[str, str, str], column: str | None, text: str) -> str:
    """DDL setting a description as a native Snowflake COMMENT on ``parts``' physical address.

    Unlike ``SET TAG`` (which Snowflake accepts via ``ALTER TABLE`` regardless of the target's
    actual kind), ``COMMENT`` DDL is keyword-strict: ``ALTER TABLE ... SET COMMENT`` on a VIEW
    fails with "Object found is of type 'VIEW', not specified type 'TABLE'" (confirmed live) —
    the caller must pass the object's real ``kind`` (from :func:`_object_kind`)."""
    database, schema, table = parts
    escaped = _escape(text)
    verb = "ALTER VIEW" if kind == "VIEW" else "ALTER TABLE"
    fq = f'{verb} "{database}"."{schema}"."{table}"'
    if column is None:
        return f"{fq} SET COMMENT = '{escaped}';"
    if kind == "VIEW":
        return f"{fq} ALTER COLUMN \"{column}\" COMMENT '{escaped}';"
    return f"{fq} MODIFY COLUMN \"{column}\" COMMENT '{escaped}';"


def _governance_note(tags: list["GovernanceTag"]) -> str:
    """Appended COMMENT text for the governance facts (REQ-1071) on one asset: which rule fired,
    and the roles it restricts/exempts — never the rule body itself (mirrors :class:`GovernanceTag`'s
    own docstring). One bracketed line per tag, signal-sorted so the DDL is deterministic."""
    lines = []
    for tag in sorted(tags, key=lambda t: t.signal.value):
        facts = [f"rule={tag.rule_id}"]
        if tag.restricted_roles:
            facts.append(f"restricted={','.join(sorted(tag.restricted_roles))}")
        if tag.exempt_roles:
            facts.append(f"exempt={','.join(sorted(tag.exempt_roles))}")
        lines.append(f"[provisa:governance {tag.signal.value} {' '.join(facts)}]")
    return "\n".join(lines)


def _comment_text(description: str, governance_note: str) -> str:
    if not governance_note:
        return description
    if not description:
        return governance_note
    return f"{description}\n\n{governance_note}"


def comment_statements(
    tables: list["TableAsset"],
    kinds: dict[tuple[str, str, str], str],
    governance_tags: list["GovernanceTag"],
) -> list[str]:
    """DDL applying table/column descriptions — appended with any governance facts on that same
    asset (REQ-1071) — as native Snowflake COMMENTs on each table's governed physical address
    (REQ-1647), the same address :func:`tag_statements` tags — so a source's per-source VIEW back
    onto ``_landing`` (REQ-1637) carries its description alongside its tags, not just an
    attachable source's real TABLE. Pure — no I/O — ``kinds`` (one ``_object_kind`` I/O lookup per
    table, done by the caller) is what lets it stay testable without a live connection. A
    table/column missing from ``kinds`` (not yet landed) is skipped, not an error here — the
    caller already reports missing landing terminals via ``_table_exists``.

    Governance facts publish here rather than as a Snowflake TAG (see :func:`tag_statements`'s
    docstring): a rule id and its restricted/exempt roles are metadata about the asset, not a
    classification value, and COMMENT is the channel this adapter already uses for that kind of
    fact."""
    governance_by_fqn: dict[str, list["GovernanceTag"]] = {}
    for tag in governance_tags:
        governance_by_fqn.setdefault(tag.asset.fqn(), []).append(tag)
    stmts: list[str] = []
    for table in tables:
        parts, _ = physical_table_and_column(table.ref)
        kind = kinds.get(parts)
        if kind is None:
            continue
        table_text = _comment_text(
            table.description, _governance_note(governance_by_fqn.get(table.ref.fqn(), []))
        )
        if table_text:
            stmts.append(_set_comment(kind, parts, None, table_text))
        for column in table.columns:
            column_text = _comment_text(
                column.description,
                _governance_note(governance_by_fqn.get(column.ref.fqn(), [])),
            )
            if column_text:
                stmts.append(_set_comment(kind, parts, column.name, column_text))
    return stmts


def share_statements(
    share_name: str,
    description: str,
    tables: list[tuple[str, str, str]],
    landing_database: str | None = None,
) -> list[str]:
    """DDL creating (or reusing) a share and granting it read access to ``tables``. Pure — no I/O —
    so the shape is testable without a live Snowflake connection.

    A per-source table's physical name (``_to_catalog_name(source.id)``) is a SECURE VIEW over the
    landed replica in ``landing_database`` (REQ-1637) when the source isn't attachable — a different
    database from the table's own. Snowflake refuses to share such a view unless the landing database
    has granted ``REFERENCE_USAGE`` to the share — but a share has exactly ONE database that can ever
    receive ``USAGE`` (its PRIMARY, established by the share's first ``GRANT USAGE ON DATABASE``, i.e.
    each member's own database here). A SECOND ``GRANT USAGE ON DATABASE`` for the landing database
    fails with "Database 'landing' does not belong to the database that is being shared" — the
    landing database gets ``REFERENCE_USAGE`` only, never ``USAGE``. So member ``USAGE``/schema
    grants are issued first (establishing the primary), then the landing database's
    ``REFERENCE_USAGE`` grant, then the ``SELECT`` grants that need it."""
    stmts = [
        f'CREATE SHARE IF NOT EXISTS "{share_name}" '
        f"SECURE_OBJECTS_ONLY = FALSE COMMENT = '{_escape(description)}';",
        f'ALTER SHARE "{share_name}" SET SECURE_OBJECTS_ONLY = FALSE;',
    ]
    granted_databases: set[str] = set()
    granted_schemas: set[tuple[str, str]] = set()
    for database, schema, table in tables:
        if database not in granted_databases:
            stmts.append(f'GRANT USAGE ON DATABASE "{database}" TO SHARE "{share_name}";')
            granted_databases.add(database)
        if (database, schema) not in granted_schemas:
            stmts.append(f'GRANT USAGE ON SCHEMA "{database}"."{schema}" TO SHARE "{share_name}";')
            granted_schemas.add((database, schema))
    if landing_database is not None and any(
        database != landing_database for database, _, _ in tables
    ):
        stmts.append(
            f'GRANT REFERENCE_USAGE ON DATABASE "{landing_database}" TO SHARE "{share_name}";'
        )
    for database, schema, table in tables:
        stmts.append(
            f'GRANT SELECT ON TABLE "{database}"."{schema}"."{table}" TO SHARE "{share_name}";'
        )
    return stmts


def listing_statements(
    listing_name: str,
    share_name: str,
    name: str,
    description: str,
    *,
    account: str,
    role: str,
    region: str,
    support_contact: str,
    publish: bool = False,
) -> list[str]:
    """DDL registering the share as a Horizon Catalog listing — the step that makes the DataProduct
    a first-class Data Product listing, not just a share.

    ``CREATE ORGANIZATION LISTING`` (not ``CREATE EXTERNAL LISTING``) — confirmed live against a
    Snowflake account: an EXTERNAL-distribution listing is Marketplace-shaped (for other Snowflake
    organizations) and never surfaces in this account's own Horizon Catalog / Data sharing UI, no
    matter which Snowsight view is checked. An ORGANIZATION-distribution listing (``distribution:
    ORGANIZATION``, ``organization_profile_name: INTERNAL``) is what Snowsight's own "create
    listing" wizard produces, and is what actually shows up.

    Manifest fields below are the minimal set Snowflake accepts for this account, found by
    iterating on live ``CREATE ORGANIZATION LISTING`` errors: ``organization_targets`` and
    ``locations`` are mandatory once ``organization_profile: INTERNAL`` is set, and
    ``support_contact``/``approver_contact`` are mandatory once a discovery target exists.
    ``publish=False`` (the default) keeps it DRAFT (``PUBLISH = FALSE``); organization listings,
    unlike external ones, have no ``REVIEW`` parameter — there is no Marketplace review step, so
    ``publish=True`` (``DataProductAsset.publish`` / ``DataProduct.publish``) takes it live
    immediately."""
    manifest = (
        f'title: "{_escape(name)}"\n'
        f'description: "{_escape(description)}"\n'
        'organization_profile: "INTERNAL"\n'
        "organization_targets:\n"
        "  discovery:\n"
        f'  - account: "{_escape(account)}"\n'
        "    roles:\n"
        f'    - "{_escape(role)}"\n'
        "locations:\n"
        "  access_regions:\n"
        f'  - name: "PUBLIC.{_escape(region)}"\n'
        f'support_contact: "{_escape(support_contact)}"\n'
        f'approver_contact: "{_escape(support_contact)}"\n'
    )
    return [
        f'CREATE ORGANIZATION LISTING IF NOT EXISTS "{listing_name}"\nSHARE "{share_name}"\nAS\n$$\n'
        f"{manifest}$$\nPUBLISH = {'TRUE' if publish else 'FALSE'};",
    ]


# -- constraints (REQ-1652) -------------------------------------------------------------------------


@dataclass(frozen=True)
class ForeignKeySpec:
    """One physical FOREIGN KEY a relationship publishes as: ``table(columns)`` REFERENCES
    ``referenced(referenced_columns)``. Direction follows the cardinality -- the "many" side holds
    the key -- and a junction-backed relationship (REQ-1586) yields one per hop."""

    name: str
    table: tuple[str, str, str]
    columns: tuple[str, ...]
    referenced: tuple[str, str, str]
    referenced_columns: tuple[str, ...]
    # The refs behind the two physical addresses: a junction table is rarely a published asset of
    # its own, so its landed replica (REQ-1637) has to resolve from the relationship, not from the
    # snapshot's table list.
    table_ref: "AssetRef | None" = None
    referenced_ref: "AssetRef | None" = None


def _fk_name(rel_id: str, suffix: str = "") -> str:
    return _identifier(f"provisa_fk_{rel_id}{suffix}")


def foreign_key_specs(
    relationships: list["RelationshipEdge"],
) -> tuple[list[ForeignKeySpec], list[tuple[str, str]]]:
    """The FOREIGN KEYs ``relationships`` declare, plus ``(relationship id, reason)`` for each one
    that publishes no key: a computed relationship has no target table, and a hop with no
    referenced column is not a key at all."""
    from provisa.compiler.sql_types import key_list

    specs: list[ForeignKeySpec] = []
    skipped: list[tuple[str, str]] = []
    for rel in relationships:
        if rel.target is None:
            skipped.append((rel.id, "computed relationship has no target table"))
            continue
        try:
            source, target = physical_parts(rel.source), physical_parts(rel.target)
            if rel.via is not None:
                junction = physical_parts(rel.via.table)
                specs.append(
                    ForeignKeySpec(
                        _fk_name(rel.id, "_source"),
                        junction,
                        key_list(rel.via.source_column),
                        source,
                        key_list(rel.source_column),
                        table_ref=rel.via.table,
                        referenced_ref=rel.source,
                    )
                )
                specs.append(
                    ForeignKeySpec(
                        _fk_name(rel.id, "_target"),
                        junction,
                        key_list(rel.via.target_column),
                        target,
                        key_list(rel.target_column or ""),
                        table_ref=rel.via.table,
                        referenced_ref=rel.target,
                    )
                )
                continue
            if not rel.target_column:
                skipped.append((rel.id, "relationship names no target column"))
                continue
            if rel.cardinality == "many-to-one":
                holder, held, ref, ref_cols, holder_ref, ref_ref = (
                    source,
                    key_list(rel.source_column),
                    target,
                    key_list(rel.target_column),
                    rel.source,
                    rel.target,
                )
            else:  # one-to-many: the "one" side is the source, the target holds the key
                holder, held, ref, ref_cols, holder_ref, ref_ref = (
                    target,
                    key_list(rel.target_column),
                    source,
                    key_list(rel.source_column),
                    rel.target,
                    rel.source,
                )
            specs.append(
                ForeignKeySpec(
                    _fk_name(rel.id),
                    holder,
                    held,
                    ref,
                    ref_cols,
                    table_ref=holder_ref,
                    referenced_ref=ref_ref,
                )
            )
        except ValueError as exc:
            skipped.append((rel.id, str(exc)))
    return specs, skipped


def landed_replica(
    landing_database: str, store_schema: str, ref: "AssetRef"
) -> tuple[str, str, str]:
    """Where a non-attachable source's rows actually land (REQ-1637): the store's schema inside
    the landing database, under the name ``landing_target`` mangles from the ref -- the object a
    per-source SECURE VIEW at :func:`physical_parts` selects from. Constraints can only live here:
    Snowflake accepts PRIMARY/FOREIGN KEY on tables alone, never on a view."""
    if ref.kind is not AssetKind.TABLE or len(ref.parts) != 3:
        raise ValueError(f"expected a table ref (source, schema, table); got {ref!r}")
    source_id, schema_name, table_name = ref.parts
    return landing_database, store_schema, f"{source_id}__{schema_name}__{table_name}"


def constraint_statements(
    tables: list["TableAsset"],
    targets: dict[tuple[str, str, str], tuple[str, str, str]],
    specs: list[ForeignKeySpec],
    existing_primary_keys: dict[tuple[str, str, str], tuple[str, ...]],
    existing_foreign_keys: dict[tuple[str, str, str], frozenset[str]],
) -> tuple[list[str], list[tuple[str, str]]]:
    """PRIMARY KEY and FOREIGN KEY DDL (REQ-1652) on each asset's constraint TARGET -- the physical
    object when it is a TABLE, else the landed replica its VIEW reads (:func:`landed_replica`) --
    and ``(what, reason)`` for every key withheld. ``targets`` holds only assets that resolved to
    a TABLE; anything else is withheld.

    Snowflake keeps both constraints as informational metadata (never enforced), which is exactly
    what Horizon Catalog renders as the table's keys and join paths. A PRIMARY KEY that already
    matches is left alone; one that differs is replaced (a table holds at most one). A FOREIGN KEY
    is added only when the referenced columns ARE the referenced table's published PRIMARY KEY --
    Snowflake refuses a reference to a non-key, and the demo's assignments.breed_name showed why a
    relationship can name one."""
    stmts: list[str] = []
    withheld: list[tuple[str, str]] = []
    declared: dict[tuple[str, str, str], tuple[str, ...]] = {}
    for table in tables:
        if not table.primary_key:
            continue
        try:
            physical = physical_parts(table.ref)
        except ValueError:
            continue
        target = targets.get(physical)
        if target is None:
            withheld.append((".".join(physical), "no landed TABLE to carry its PRIMARY KEY"))
            continue
        declared[physical] = table.primary_key
        current = existing_primary_keys.get(target, ())
        if current == table.primary_key:
            continue
        fq = f'"{target[0]}"."{target[1]}"."{target[2]}"'
        if current:
            stmts.append(f"ALTER TABLE {fq} DROP PRIMARY KEY;")
        cols = ", ".join(f'"{c}"' for c in table.primary_key)
        stmts.append(
            f'ALTER TABLE {fq} ADD CONSTRAINT "{_identifier(f"provisa_pk_{physical[2]}")}" '
            f"PRIMARY KEY ({cols});"
        )
    wanted: dict[tuple[str, str, str], set[str]] = {}
    for spec in specs:
        label = f"{'.'.join(spec.table)} -> {'.'.join(spec.referenced)}"
        holder, referenced = targets.get(spec.table), targets.get(spec.referenced)
        if holder is None or referenced is None:
            withheld.append((spec.name, f"{label}: both ends must be landed TABLEs"))
            continue
        if declared.get(spec.referenced) != spec.referenced_columns:
            withheld.append(
                (
                    spec.name,
                    f"{label}: referenced columns {list(spec.referenced_columns)} are not "
                    f"{'.'.join(spec.referenced)}'s primary key",
                )
            )
            continue
        wanted.setdefault(holder, set()).add(spec.name)
        if spec.name in existing_foreign_keys.get(holder, frozenset()):
            continue
        fq = f'"{holder[0]}"."{holder[1]}"."{holder[2]}"'
        ref = f'"{referenced[0]}"."{referenced[1]}"."{referenced[2]}"'
        cols = ", ".join(f'"{c}"' for c in spec.columns)
        ref_cols = ", ".join(f'"{c}"' for c in spec.referenced_columns)
        stmts.append(
            f'ALTER TABLE {fq} ADD CONSTRAINT "{spec.name}" FOREIGN KEY ({cols}) '
            f"REFERENCES {ref} ({ref_cols});"
        )
    # A key this export wrote earlier (its ``provisa_fk_`` prefix) that no relationship declares
    # any more -- or that now publishes under another name, as a junction's two hops do -- is
    # withdrawn; foreign keys of any other origin are never touched.
    for holder, names in existing_foreign_keys.items():
        for name in sorted(names):
            if name.startswith("provisa_fk_") and name not in wanted.get(holder, set()):
                fq = f'"{holder[0]}"."{holder[1]}"."{holder[2]}"'
                stmts.append(f'ALTER TABLE {fq} DROP CONSTRAINT "{name}";')
    return stmts, withheld


_PK_TAG = "PRIMARY_KEY"
_FK_TAG = "FOREIGN_KEY"


def key_tag_statements(
    tags_database: str,
    tables: list["TableAsset"],
    specs: list[ForeignKeySpec],
    views: set[tuple[str, str, str]],
    existing_tags: dict[tuple[tuple[str, str, str], str], set[str]] | None = None,
) -> list[str]:
    """Key TAGs on the per-source VIEWs (REQ-1652). A view cannot carry a constraint, so the keys
    a consumer sees on ``pet_store_sqlite.pet_store.pets`` ride the same governance TAG namespace
    its descriptions and classifications already do: ``PRIMARY_KEY`` = the column's 1-based
    position in the key, ``FOREIGN_KEY`` = the referenced physical ``"db"."schema"."table"."column"``
    (several, ``; ``-joined, when one column holds more than one). Only the specs the caller
    actually emitted (or found already present) belong here -- a withheld key is not a key."""
    if not views:
        return []

    def tag_fq(name: str) -> str:
        return f'"{tags_database}"."{_TAGS_SCHEMA}"."{name}"'

    def set_col_tag(parts: tuple[str, str, str], column: str, tag: str, value: str) -> str:
        return (
            f'ALTER VIEW "{parts[0]}"."{parts[1]}"."{parts[2]}" MODIFY COLUMN "{column}" '
            f"SET TAG {tag_fq(tag)} = '{_escape(value)}';"
        )

    stmts: list[str] = []
    for table in tables:
        try:
            physical = physical_parts(table.ref)
        except ValueError:
            continue
        if physical not in views:
            continue
        for position, column in enumerate(table.primary_key, start=1):
            stmts.append(set_col_tag(physical, column, _PK_TAG, str(position)))
    references: dict[tuple[tuple[str, str, str], str], list[str]] = {}
    for spec in specs:
        if spec.table not in views:
            continue
        for column, ref_column in zip(spec.columns, spec.referenced_columns, strict=True):
            target = ".".join(f'"{part}"' for part in (*spec.referenced, ref_column))
            references.setdefault((spec.table, column), []).append(target)
    for (parts, column), targets in references.items():
        stmts.append(set_col_tag(parts, column, _FK_TAG, "; ".join(targets)))
    # A key tag set by an earlier publish that this one does not re-set is stale -- the key moved
    # or the relationship went -- and is unset, so the view never shows a key the model lost.
    reset: set[tuple[tuple[str, str, str], str, str]] = set()
    for table in tables:
        try:
            physical = physical_parts(table.ref)
        except ValueError:
            continue
        if physical in views:
            reset.update((physical, column, _PK_TAG) for column in table.primary_key)
    reset.update((parts, column, _FK_TAG) for (parts, column) in references)
    for (parts, column), tags in sorted((existing_tags or {}).items()):
        if parts not in views:
            continue
        for tag in sorted(tags):
            if tag in (_PK_TAG, _FK_TAG) and (parts, column, tag) not in reset:
                stmts.append(
                    f'ALTER VIEW "{parts[0]}"."{parts[1]}"."{parts[2]}" MODIFY COLUMN "{column}" '
                    f"UNSET TAG {tag_fq(tag)};"
                )
    if not stmts:
        return []  # no key to mirror: no tag objects to create either
    return [
        f'CREATE SCHEMA IF NOT EXISTS "{tags_database}"."{_TAGS_SCHEMA}"',
        f"CREATE TAG IF NOT EXISTS {tag_fq(_PK_TAG)};",
        f"CREATE TAG IF NOT EXISTS {tag_fq(_FK_TAG)};",
        *stmts,
    ]


def _existing_key_tags(
    runtime: SnowflakeFederationRuntime, tags_database: str, view: tuple[str, str, str]
) -> dict[tuple[tuple[str, str, str], str], set[str]]:
    """The PRIMARY_KEY / FOREIGN_KEY tags currently set on ``view``'s columns, per column, so a
    stale one can be unset (REQ-1652)."""
    fq = ".".join(f'"{part}"' for part in view)
    cur = runtime.connection.cursor()
    try:
        cur.execute(
            f'SELECT column_name, tag_name FROM TABLE("{tags_database}".information_schema'
            f".tag_references_all_columns('{_escape(fq)}', 'table')) "
            f"WHERE tag_database = '{_escape(tags_database)}' AND tag_schema = '{_TAGS_SCHEMA}' "
            f"AND tag_name IN ('{_PK_TAG}', '{_FK_TAG}')"
        )
        out: dict[tuple[tuple[str, str, str], str], set[str]] = {}
        for column, tag in cur.fetchall():
            out.setdefault((view, str(column)), set()).add(str(tag))
        return out
    except ProgrammingError:
        return {}  # the view has no tag references yet (or no tag schema): nothing to unset
    finally:
        cur.close()


def _existing_keys(
    runtime: SnowflakeFederationRuntime, parts: tuple[str, str, str]
) -> tuple[tuple[str, ...], frozenset[str]]:
    """The table's current PRIMARY KEY columns (in key order) and the names of its FOREIGN KEYs,
    read live so the publish is idempotent -- Snowflake refuses a second PRIMARY KEY and a
    duplicate constraint name."""
    fq = f'"{parts[0]}"."{parts[1]}"."{parts[2]}"'
    cur = runtime.connection.cursor()
    try:
        cur.execute(f"SHOW PRIMARY KEYS IN TABLE {fq}")
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        pk = tuple(r["column_name"] for r in sorted(rows, key=lambda r: r["key_sequence"]))
        cur.execute(f"SHOW IMPORTED KEYS IN TABLE {fq}")
        cols = [d[0] for d in cur.description]
        fks = frozenset(dict(zip(cols, r))["fk_name"] for r in cur.fetchall())
        return pk, fks
    finally:
        cur.close()


def _organization_context(runtime: SnowflakeFederationRuntime) -> tuple[str, str, str]:
    """(account, role, region) for the CURRENT session — the ``organization_targets``/``locations``
    fields an organization listing's manifest requires (REQ-1635). Read live rather than guessed:
    a wrong region string (e.g. assuming AWS when the account is Azure-hosted) makes Snowflake treat
    the listing as cross-region and demand autofulfillment, which a same-account internal listing
    doesn't need."""
    cur = runtime.connection.cursor()
    try:
        cur.execute("SELECT CURRENT_ACCOUNT_NAME(), CURRENT_ROLE(), CURRENT_REGION()")
        row = cur.fetchone()
        assert row is not None, (
            "CURRENT_ACCOUNT_NAME()/CURRENT_ROLE()/CURRENT_REGION() returned no row"
        )
        account, role, region = row
        return account, role, region
    finally:
        cur.close()


def _object_kind(runtime: SnowflakeFederationRuntime, parts: tuple[str, str, str]) -> str | None:
    """``'TABLE'``/``'VIEW'`` for the object at ``parts``, or ``None`` if it doesn't exist yet
    (source never landed, or its database/schema doesn't exist — same "not there" outcome).
    Needed alongside :func:`_table_exists` because COMMENT DDL (:func:`_set_comment`), unlike TAG
    DDL, requires the caller to know which kind it actually is."""
    database, schema, table = parts
    cur = runtime.connection.cursor()
    try:
        cur.execute(f'SHOW OBJECTS LIKE \'{_escape(table)}\' IN SCHEMA "{database}"."{schema}"')
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        return str(dict(zip(cols, row))["kind"]).upper() if row is not None else None
    except ProgrammingError:
        return None
    finally:
        cur.close()


def _table_exists(runtime: SnowflakeFederationRuntime, parts: tuple[str, str, str]) -> bool:
    return _object_kind(runtime, parts) is not None


@register_provider
class SnowflakeHorizonExport(MetadataExport):  # REQ-1635
    """Registers each DataProduct as a Snowflake Horizon Catalog Data Product / Marketplace
    listing backed by a share over its member tables. Runs ONLY when Snowflake is the configured
    engine (``configured_engine_url()`` scheme) — on any other engine, or a snapshot with no
    DataProducts, ``publish`` is a documented no-op."""

    provider_name = "snowflake_horizon"

    def _runtime_or_none(self) -> SnowflakeFederationRuntime | None:
        url = configured_engine_url()
        if not url or not url.startswith("snowflake://"):
            return None
        return SnowflakeFederationRuntime(url=url)

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        result = PublishResult(provider_name=self.provider_name)
        products = getattr(snapshot, "data_products", None) or []
        governance_tags = getattr(snapshot, "governance_tags", None) or []
        model_tags = getattr(snapshot, "model_tags", None) or []
        tables = getattr(snapshot, "tables", None) or []
        if not products and not governance_tags and not model_tags and not tables:
            return result
        runtime = self._runtime_or_none()
        if runtime is None:
            return result
        try:
            published = sum(
                1 for product in products if self._publish_product(runtime, product, result)
            )
            if published:
                result.published["data_products"] = published
            tagged = self._publish_tags(runtime, model_tags, result)
            if tagged:
                result.published["tags"] = tagged
            commented = self._publish_descriptions(runtime, tables, governance_tags, result)
            if commented:
                result.published["descriptions"] = commented
            relationships = getattr(snapshot, "relationships", None) or []
            constrained = self._publish_constraints(runtime, tables, relationships, result)
            if constrained:
                result.published["constraints"] = constrained
        finally:
            runtime.close()
        return result

    def _publish_tags(
        self,
        runtime: SnowflakeFederationRuntime,
        model_tags: list["ModelTag"],
        result: PublishResult,
    ) -> int:
        """Applies ``model_tags`` as native Snowflake TAG objects (a steward-assigned
        classification), gated on the target table already existing (``_table_exists``) so a tag
        on a not-yet-landed source is reported as an error rather than failing the whole export.
        ``GovernanceTag`` facts (REQ-1071) publish via ``_publish_descriptions`` instead — see
        :func:`tag_statements`'s docstring."""
        if not model_tags:
            return 0
        applicable_model: list["ModelTag"] = []
        for mtag in model_tags:
            if mtag.asset is None:
                continue  # relationship-scoped (REQ-1378) — no physical asset to tag
            try:
                parts = physical_table_and_column(mtag.asset)[0]
            except ValueError as exc:
                result.errors.append(AssetError(AssetRefStub(mtag.tag_id), str(exc)))
                continue
            if _table_exists(runtime, parts):
                applicable_model.append(mtag)
            else:
                result.errors.append(
                    AssetError(
                        AssetRefStub(mtag.tag_id),
                        f"Snowflake landing terminal missing for tagged asset: {'.'.join(parts)}",
                    )
                )
        if not applicable_model:
            return 0
        tags_database = runtime.ensure_materialize_attached()
        statements = tag_statements(tags_database, applicable_model)
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
        except Exception as exc:  # noqa: BLE001 - runtime.connection is an opaque DBAPI cursor
            result.errors.append(AssetError(AssetRefStub("model_tags"), str(exc)))
            return 0
        finally:
            cur.close()
        return len(applicable_model)

    def _publish_descriptions(
        self,
        runtime: SnowflakeFederationRuntime,
        tables: list["TableAsset"],
        governance_tags: list["GovernanceTag"],
        result: PublishResult,
    ) -> int:
        """Applies table/column descriptions, appended with any governance facts on that same
        asset (REQ-1071), as native Snowflake COMMENTs (REQ-1647), gated on the target already
        existing (mirrors ``_publish_tags``'s ``_table_exists`` gate)."""
        known_fqns = {table.ref.fqn() for table in tables} | {
            column.ref.fqn() for table in tables for column in table.columns
        }
        for tag in governance_tags:
            if tag.asset.fqn() not in known_fqns:
                result.errors.append(
                    AssetError(
                        tag.asset,
                        f"Snowflake landing terminal missing for governed asset: {tag.asset.fqn()}",
                    )
                )
        if not tables:
            return 0
        governed_fqns = {tag.asset.fqn() for tag in governance_tags}
        kinds: dict[tuple[str, str, str], str] = {}
        described: list["TableAsset"] = []
        for table in tables:
            has_content = (
                table.description
                or any(c.description for c in table.columns)
                or table.ref.fqn() in governed_fqns
                or any(c.ref.fqn() in governed_fqns for c in table.columns)
            )
            if not has_content:
                continue
            try:
                parts = physical_table_and_column(table.ref)[0]
            except ValueError as exc:
                result.errors.append(AssetError(table.ref, str(exc)))
                continue
            kind = _object_kind(runtime, parts)
            if kind is None:
                result.errors.append(
                    AssetError(
                        table.ref,
                        f"Snowflake landing terminal missing for described asset: {'.'.join(parts)}",
                    )
                )
                continue
            kinds[parts] = kind
            described.append(table)
        if not described:
            return 0
        statements = comment_statements(described, kinds, governance_tags)
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
        except Exception as exc:  # noqa: BLE001 - runtime.connection is an opaque DBAPI cursor
            result.errors.append(AssetError(AssetRefStub("descriptions"), str(exc)))
            return 0
        finally:
            cur.close()
        return len(statements)

    def _publish_constraints(
        self,
        runtime: SnowflakeFederationRuntime,
        tables: list["TableAsset"],
        relationships: list["RelationshipEdge"],
        result: PublishResult,
    ) -> int:
        """Publishes each asset's declared key as a PRIMARY KEY and each relationship as a FOREIGN
        KEY (REQ-1652) on the object that can carry one -- the physical TABLE when the source is
        attachable, else the landed replica behind its per-source VIEW (REQ-1637) -- and mirrors the
        keys onto the VIEWs as column TAGs, since a Snowflake view holds no constraint. A withheld
        key is reported, never silently dropped."""
        from provisa.api.org_runtime import active_env
        from provisa.federation.store_scope import store_schema

        specs, skipped = foreign_key_specs(relationships)
        for rel_id, reason in skipped:
            result.errors.append(AssetError(AssetRefStub(rel_id), reason))
        refs: dict[tuple[str, str, str], "AssetRef"] = {}
        for table in tables:
            try:
                refs[physical_parts(table.ref)] = table.ref
            except ValueError as exc:
                result.errors.append(AssetError(table.ref, str(exc)))
        for spec in specs:
            if spec.table_ref is not None:
                refs.setdefault(spec.table, spec.table_ref)
            if spec.referenced_ref is not None:
                refs.setdefault(spec.referenced, spec.referenced_ref)
        keyed = {physical_parts(t.ref) for t in tables if t.primary_key} | {
            end for spec in specs for end in (spec.table, spec.referenced)
        }
        if not keyed:
            return 0
        landing_database = runtime.ensure_materialize_attached()
        landing_schema = store_schema(configured_engine_url() or "", active_env())
        targets: dict[tuple[str, str, str], tuple[str, str, str]] = {}
        views: set[tuple[str, str, str]] = set()
        for physical in sorted(keyed):
            kind = _object_kind(runtime, physical)
            if kind == "TABLE":
                targets[physical] = physical
                continue
            # A VIEW over the landed replica (REQ-1637), or no per-source object at all (a junction
            # table that is landed but exposed only through the relationships it joins): either way
            # the constraint's home is the replica, and only an existing VIEW gets key tags.
            if physical in refs:
                replica = landed_replica(landing_database, landing_schema, refs[physical])
                if _object_kind(runtime, replica) == "TABLE":
                    targets[physical] = replica
                    if kind == "VIEW":
                        views.add(physical)
        pks: dict[tuple[str, str, str], tuple[str, ...]] = {}
        fks: dict[tuple[str, str, str], frozenset[str]] = {}
        for target in set(targets.values()):
            pks[target], fks[target] = _existing_keys(runtime, target)
        statements, withheld = constraint_statements(tables, targets, specs, pks, fks)
        for name, reason in withheld:
            result.errors.append(AssetError(AssetRefStub(name), reason))
        withheld_names = {name for name, _ in withheld}
        emitted = [spec for spec in specs if spec.name not in withheld_names]
        existing_tags = {
            key: tags
            for view in sorted(views)
            for key, tags in _existing_key_tags(runtime, landing_database, view).items()
        }
        statements += key_tag_statements(landing_database, tables, emitted, views, existing_tags)
        if not statements:
            return 0
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
        except Exception as exc:  # noqa: BLE001 - runtime.connection is an opaque DBAPI cursor
            result.errors.append(AssetError(AssetRefStub("constraints"), str(exc)))
            return 0
        finally:
            cur.close()
        return len(statements)

    def _publish_product(
        self, runtime: SnowflakeFederationRuntime, product: Any, result: PublishResult
    ) -> bool:
        try:
            tables = [physical_parts(ref) for ref in product.members]
        except ValueError as exc:
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False

        missing = [t for t in tables if not _table_exists(runtime, t)]
        if missing:
            joined = ", ".join(".".join(t) for t in missing)
            result.errors.append(
                AssetError(
                    AssetRefStub(product.name),
                    f"Snowflake landing terminal missing for member table(s): {joined}",
                )
            )
            return False

        if not product.support_contact:
            result.errors.append(
                AssetError(
                    AssetRefStub(product.name),
                    "data product has no support_contact — required by Horizon Catalog's "
                    "organization listing manifest (set DataProduct.support_contact)",
                )
            )
            return False

        try:
            share_name = _identifier(f"provisa_{product.id}_share")
            listing_name = _identifier(f"provisa_{product.id}_listing")
        except ValueError as exc:
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False

        account, role, region = _organization_context(runtime)
        statements = share_statements(
            share_name,
            product.description,
            tables,
            landing_database=runtime.ensure_materialize_attached(),
        ) + listing_statements(
            listing_name,
            share_name,
            product.name,
            product.description,
            account=account,
            role=role,
            region=region,
            support_contact=product.support_contact,
            publish=getattr(product, "publish", False),
        )
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
        except Exception as exc:  # noqa: BLE001 - runtime.connection is an opaque DBAPI cursor
            # (REQ-1635: SnowflakeFederationRuntime is the sole owner of the snowflake.connector
            # import; this adapter must not import its driver-specific exception types to
            # distinguish them, so any DBAPI failure from this cursor is reported as-is).
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False
        finally:
            cur.close()
        return True

    async def health(self) -> None:
        url = configured_engine_url()
        if not url or not url.startswith("snowflake://"):
            raise RuntimeError(
                "snowflake_horizon export requires Snowflake as the configured engine "
                "(configured_engine_url() must be a snowflake:// DSN)"
            )
        runtime = SnowflakeFederationRuntime(url=url)
        try:
            runtime.run_arrow("SELECT 1")
        finally:
            runtime.close()
