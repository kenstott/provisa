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

# Requirements: REQ-1068, REQ-1635, REQ-1656

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import yaml

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


#: Snowflake features at most this many objects in a listing's data dictionary (REQ-1656): "You can
#: select up to five of the most important database objects within the listing."
FEATURED_LIMIT = 5


@dataclass(frozen=True)
class ListingMember:
    """One member of a listing as its manifest addresses it: the physical object the share grants,
    its kind (``TABLE``/``VIEW`` -- the manifest's ``domain``), and the columns a masking rule
    governs (published as the data preview's ``pii_columns``)."""

    database: str
    schema: str
    name: str
    kind: str
    pii_columns: tuple[str, ...] = ()


def _yaml_string(value: str) -> str:
    """A YAML double-quoted scalar. JSON string syntax is valid YAML, so ``json.dumps`` covers every
    embedded quote, backslash and newline the ``manifest`` block could otherwise break on."""
    return json.dumps(value)


def _yaml_identifier(name: str) -> str:
    """A Snowflake object name inside the manifest. The manifest resolves names like SQL does --
    unquoted is upper-cased ("Database 'GRAPHQL_DEMO' does not exist", confirmed live for a
    lower-case database) -- so the identifier is wrapped in its own double quotes, which is the
    ``"\"graphql_demo\""`` form Snowsight's own listing wizard writes back."""
    return _yaml_string(f'"{name}"')


def _member_entry(member: ListingMember, indent: str, *, pii: bool) -> str:
    lines = [
        f"{indent}- name: {_yaml_identifier(member.name)}",
        f"{indent}  schema: {_yaml_identifier(member.schema)}",
        f'{indent}  domain: "{member.kind}"',
    ]
    if pii:
        lines.append(f"{indent}  pii_columns:")
        lines.extend(f"{indent}  - {_yaml_identifier(c)}" for c in member.pii_columns)
    return "\n".join(lines) + "\n"


def listing_manifest(
    name: str,
    description: str,
    *,
    account: str,
    role: str,
    region: str,
    support_contact: str,
    members: list[ListingMember],
) -> str:
    """The organization listing's YAML manifest.

    Fields are the minimal set Snowflake accepts for this account, found by iterating on live
    ``CREATE ORGANIZATION LISTING`` errors: ``organization_targets`` and ``locations`` are mandatory
    once ``organization_profile: INTERNAL`` is set, and ``support_contact``/``approver_contact`` are
    mandatory once a discovery target exists.

    The data dictionary itself is not declared here: Snowflake generates it from the share, listing
    every granted object with its columns and COMMENTs -- so every member (and every column the
    landing reconcile described, REQ-1654) is already in it. What the manifest adds (REQ-1656):

    * ``data_dictionary.featured`` -- the members Snowsight shows first. Snowflake caps this at
      :data:`FEATURED_LIMIT` objects under ONE database, so the first members in the share's
      primary database (the first member's) are featured, in product order; a member in a second
      database is in the dictionary but cannot be featured.
    * ``data_preview`` -- ``has_pii`` and the masked columns, so the preview Snowflake samples hides
      what the model's masking rules hide. Same one-database rule as ``featured``.
    """
    manifest = (
        f"title: {_yaml_string(name)}\n"
        f"description: {_yaml_string(description)}\n"
        'organization_profile: "INTERNAL"\n'
        "organization_targets:\n"
        "  discovery:\n"
        f"  - account: {_yaml_string(account)}\n"
        "    roles:\n"
        f"    - {_yaml_string(role)}\n"
        "locations:\n"
        "  access_regions:\n"
        f"  - name: {_yaml_string(f'PUBLIC.{region}')}\n"
        f"support_contact: {_yaml_string(support_contact)}\n"
        f"approver_contact: {_yaml_string(support_contact)}\n"
    )
    if not members:
        return manifest
    primary = members[0].database
    in_primary = [m for m in members if m.database == primary]
    manifest += (
        f"data_dictionary:\n  featured:\n    database: {_yaml_identifier(primary)}\n    objects:\n"
    )
    for member in in_primary[:FEATURED_LIMIT]:
        manifest += _member_entry(member, "    ", pii=False)
    masked = [m for m in in_primary if m.pii_columns]
    has_pii = any(m.pii_columns for m in members)
    manifest += f"data_preview:\n  has_pii: {'true' if has_pii else 'false'}\n"
    if masked:
        manifest += (
            f"  metadata_overrides:\n    database: {_yaml_identifier(primary)}\n    objects:\n"
        )
        for member in masked:
            manifest += _member_entry(member, "    ", pii=True)
    return manifest


def listing_statements(
    listing_name: str, share_name: str, manifest: str, *, publish: bool = False
) -> list[str]:
    """DDL registering the share as a Horizon Catalog listing -- the step that makes the DataProduct
    a first-class Data Product listing, not just a share.

    ``CREATE ORGANIZATION LISTING`` (not ``CREATE EXTERNAL LISTING``) -- confirmed live against a
    Snowflake account: an EXTERNAL-distribution listing is Marketplace-shaped (for other Snowflake
    organizations) and never surfaces in this account's own Horizon Catalog / Data sharing UI, no
    matter which Snowsight view is checked. An ORGANIZATION-distribution listing (``distribution:
    ORGANIZATION``, ``organization_profile_name: INTERNAL``) is what Snowsight's own "create
    listing" wizard produces, and is what actually shows up.

    ``publish=False`` (the default) keeps it DRAFT (``PUBLISH = FALSE``); organization listings,
    unlike external ones, have no ``REVIEW`` parameter -- there is no Marketplace review step, so
    ``publish=True`` (``DataProductAsset.publish`` / ``DataProduct.publish``) takes it live
    immediately. ``IF NOT EXISTS`` leaves an existing listing's manifest alone; the caller converges
    it with :func:`listing_update_statement` when :func:`manifest_matches` says it drifted."""
    return [
        f'CREATE ORGANIZATION LISTING IF NOT EXISTS "{listing_name}"\nSHARE "{share_name}"\nAS\n$$\n'
        f"{manifest}$$\nPUBLISH = {'TRUE' if publish else 'FALSE'};",
    ]


def listing_update_statement(listing_name: str, manifest: str, *, publish: bool = False) -> str:
    """DDL replacing an existing listing's manifest (REQ-1656). Provisa owns the manifest: a change
    made in Snowsight is overwritten on the next publish, by design -- the Data Product is edited
    in Provisa."""
    return (
        f'ALTER LISTING "{listing_name}"\nAS\n$$\n{manifest}$$\n'
        f"PUBLISH = {'TRUE' if publish else 'FALSE'};"
    )


def manifest_matches(intended: str, live: str) -> bool:
    """Whether the listing's live manifest (``DESCRIBE LISTING ... manifest_yaml``) already says
    what ``intended`` says. Snowflake rewrites the YAML it stores -- reorders the keys of each
    object, appends ``product_types`` and ``resharing`` defaults -- so the two are compared as
    parsed documents on the keys Provisa emits, not as text."""
    wanted = yaml.safe_load(intended) or {}
    current = yaml.safe_load(live) or {}
    return all(current.get(key) == value for key, value in wanted.items())


def revoke_statements(
    share_name: str, granted: list[tuple[str, str]], wanted: set[tuple[str, str, str]]
) -> list[str]:
    """DDL withdrawing a share's SELECT grants on objects that are no longer members (REQ-1656), so
    the auto-generated data dictionary tracks the product's membership. ``granted`` is
    ``SHOW GRANTS TO SHARE``'s ``(granted_on, name)`` for its SELECT rows, ``name`` in Snowflake's
    quoted ``"db"."schema"."object"`` form."""
    stmts: list[str] = []
    for granted_on, name in granted:
        parts = tuple(p.strip('"') for p in _QUOTED_PART.findall(name))
        if len(parts) != 3 or parts in wanted:
            continue
        stmts.append(f'REVOKE SELECT ON {granted_on} {name} FROM SHARE "{share_name}";')
    return stmts


_QUOTED_PART = re.compile(r'"(?:[^"]|"")*"|[^.]+')


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


def _masked_columns(governance_tags: list["GovernanceTag"]) -> dict[str, tuple[str, ...]]:
    """Per table fqn, the columns a masking rule governs (REQ-1071's ``masked`` signal) -- the
    columns the listing's data preview must hide (REQ-1656)."""
    from provisa.api.metadata_export.model import GovernanceSignal

    out: dict[str, list[str]] = {}
    for tag in governance_tags:
        if tag.signal is not GovernanceSignal.MASKED or tag.asset.kind is not AssetKind.COLUMN:
            continue
        table_fqn = ".".join(tag.asset.parts[:-1])
        out.setdefault(table_fqn, []).append(tag.asset.parts[-1])
    return {fqn: tuple(sorted(cols)) for fqn, cols in out.items()}


def _select_grants(cur: Any, share_name: str) -> list[tuple[str, str]]:
    """``(granted_on, name)`` for each SELECT the share currently holds."""
    cur.execute(f'SHOW GRANTS TO SHARE "{share_name}"')
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
    return [(r["granted_on"], r["name"]) for r in rows if r.get("privilege") == "SELECT"]


def _live_manifest(cur: Any, listing_name: str) -> str:
    cur.execute(f'DESCRIBE LISTING "{listing_name}"')
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    assert row is not None, f"DESCRIBE LISTING {listing_name} returned no row"
    return dict(zip(cols, row, strict=True))["manifest_yaml"] or ""


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
        tables = getattr(snapshot, "tables", None) or []
        if not products and not governance_tags and not tables:
            return result
        runtime = self._runtime_or_none()
        if runtime is None:
            return result
        try:
            published = sum(
                1
                for product in products
                if self._publish_product(runtime, product, result, governance_tags)
            )
            if published:
                result.published["data_products"] = published
            commented = self._publish_descriptions(runtime, tables, governance_tags, result)
            if commented:
                result.published["descriptions"] = commented
        finally:
            runtime.close()
        return result

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

    def _publish_product(
        self,
        runtime: SnowflakeFederationRuntime,
        product: Any,
        result: PublishResult,
        governance_tags: list["GovernanceTag"] | None = None,
    ) -> bool:
        try:
            tables = [physical_parts(ref) for ref in product.members]
        except ValueError as exc:
            result.errors.append(AssetError(AssetRefStub(product.name), str(exc)))
            return False

        kinds = {t: _object_kind(runtime, t) for t in tables}
        missing = [t for t, kind in kinds.items() if kind is None]
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

        masked = _masked_columns(governance_tags or [])
        members = [
            ListingMember(
                database,
                schema,
                table,
                kinds[(database, schema, table)] or "TABLE",
                pii_columns=masked.get(ref.fqn(), ()),
            )
            for ref, (database, schema, table) in zip(product.members, tables, strict=True)
        ]
        account, role, region = _organization_context(runtime)
        manifest = listing_manifest(
            product.name,
            product.description,
            account=account,
            role=role,
            region=region,
            support_contact=product.support_contact,
            members=members,
        )
        publish = getattr(product, "publish", False)
        statements = share_statements(
            share_name,
            product.description,
            tables,
            landing_database=runtime.ensure_materialize_attached(),
        ) + listing_statements(listing_name, share_name, manifest, publish=publish)
        cur = runtime.connection.cursor()
        try:
            for stmt in statements:
                cur.execute(stmt)
            for stmt in revoke_statements(share_name, _select_grants(cur, share_name), set(tables)):
                cur.execute(stmt)
            if not manifest_matches(manifest, _live_manifest(cur, listing_name)):
                cur.execute(listing_update_statement(listing_name, manifest, publish=publish))
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
