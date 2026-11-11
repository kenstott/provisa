# Copyright (c) 2026 Kenneth Stott
# Canary: 4fd5a22c-d840-4949-a7a9-3cb14e975d4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks Unity Catalog as a data product target (REQ-2117).

Unity Catalog has no data product entity, so a product is published as the closest thing in its
own terms: one schema, ``<catalog>.<product id>``, in the catalog the Databricks engine works in,
holding a view over each member. Nothing is copied: a view reads its member where the engine keeps
it -- the member's replica (a Delta table in the org's replicas schema, REQ-1912), or for a lake
table the engine reads in place, its external table (``DatabricksFederationRuntime._phys_parts``).

- The view is named for the member's business name; its columns and the view itself carry the
  member's descriptions, and each governance fact is a ``provisa_governance:<signal>`` tag naming
  the rule, never its body.
- Unity Catalog has no glossary to link a column to, so a column's grounded term (REQ-2238) is a
  ``provisa_term`` tag naming it, and the term's definition follows the column's description in
  its comment. Only a term an agent may see is named: approved, or a defined draft on a path
  holding an approved term, marked draft.
- The schema's comment holds the product's purpose, limitations, usage, service level and
  support; its owner, team, version and status are ``provisa_product:<field>`` tags (a tag value
  holds 256 characters, so the prose stays in the comment).
- A view in the product's schema that is no longer a member is dropped.
- With ``delta_share`` set, the schema's views are also published as the Delta Share
  ``provisa_<product id>``, for other data teams of the enterprise; a recipient is granted it in
  Databricks. Sharing needs the member in Delta, so a member that is not is reported and left out.

It runs only when Databricks is the configured engine; on any other engine a publish is a no-op.
Each schema's and view's full name is recorded in the target's bindings (REQ-1389).
"""

# Requirements: REQ-2117, REQ-2238

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from provisa.api.metadata_export.model import column_terms, term_note
from provisa.api.metadata_export.provider import AssetError, MetadataExport, PublishResult
from provisa.api.metadata_export.registry import register_provider

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        ColumnAsset,
        DataProductAsset,
        GovernanceTag,
        GroundedTerm,
        MetadataSnapshot,
        TableAsset,
    )

GOVERNANCE_PREFIX = "provisa_governance:"
#: REQ-2238: the tag naming a view column's grounded term.
TERM_TAG = "provisa_term"
PRODUCT_PREFIX = "provisa_product:"
_TAG_VALUE_LIMIT = 256

Parts = tuple[str, str, str]


def lit(value: str) -> str:
    """A Databricks SQL string literal body: backslash escapes, a quote doubles."""
    return value.replace("\\", "\\\\").replace("'", "''")


def q(*parts: str) -> str:
    """A backquoted, dotted Unity Catalog name."""
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def view_name(table: "TableAsset") -> str:
    """The member's view: its business name."""
    return table.aliases[0] if table.aliases else table.name


def share_name(product: "DataProductAsset") -> str:
    return f"provisa_{product.id}"


def schema_comment(product: "DataProductAsset") -> str:
    """The product's prose fields, as the schema's comment."""
    parts = [product.description]
    for heading, text in (
        ("Limitations", product.limitations),
        ("Usage", product.usage),
        ("Support", product.support),
        ("Support contact", product.support_contact),
        ("Documentation", product.documentation_url),
    ):
        if text:
            parts.append(f"{heading}: {text}")
    return "\n\n".join(p for p in parts if p)


def schema_tags(product: "DataProductAsset") -> dict[str, str]:
    """The product's short fields, as the schema's tags."""
    fields = {
        "owner": product.owner.id if product.owner else None,
        "team": product.team_role,
        "version": product.version,
        "status": product.status,
    }
    return {
        f"{PRODUCT_PREFIX}{k}": v[:_TAG_VALUE_LIMIT] for k, v in fields.items() if v is not None
    }


def _column_comment(column: "ColumnAsset", terms_by_column: Mapping[str, "GroundedTerm"]) -> str:
    """A view column's comment: its description, then its grounded term and the term's
    definition (REQ-2238)."""
    note = term_note(terms_by_column.get(column.ref.fqn()))
    return "\n\n".join(part for part in (column.description, note) if part)


def view_statement(
    view: Parts, member: Parts, table: "TableAsset", terms_by_column: Mapping[str, "GroundedTerm"]
) -> str:
    """The view over ``member``: each column named and described, its grounded term's definition
    in its comment (REQ-2238), and the view described."""
    columns = ", ".join(
        f"{q(c.name)} COMMENT '{lit(comment)}'"
        if (comment := _column_comment(c, terms_by_column))
        else q(c.name)
        for c in table.columns
    )
    select = ", ".join(q(c.name) for c in table.columns)
    comment = f" COMMENT '{lit(table.description)}'" if table.description else ""
    return (
        f"CREATE OR REPLACE VIEW {q(*view)} ({columns}){comment} "
        f"AS SELECT {select} FROM {q(*member)}"
    )


def governance_statements(
    view: Parts, table: "TableAsset", governance_tags: list["GovernanceTag"]
) -> list[str]:
    """The governance facts on the member and its columns, as tags on its view: which rule
    governs it, never the rule."""
    table_fqn = table.ref.fqn()
    columns = {c.ref.fqn(): c.name for c in table.columns}
    stmts = []
    for tag in governance_tags:
        key = f"{GOVERNANCE_PREFIX}{tag.signal.value}"
        value = lit(tag.rule_id[:_TAG_VALUE_LIMIT])
        fqn = tag.asset.fqn()
        if fqn == table_fqn:
            stmts.append(f"ALTER VIEW {q(*view)} SET TAGS ('{key}' = '{value}')")
        elif fqn in columns:
            stmts.append(
                # A view's column is tagged through ALTER TABLE: Databricks has no ALTER VIEW
                # ... ALTER COLUMN.
                f"ALTER TABLE {q(*view)} ALTER COLUMN {q(columns[fqn])} "
                f"SET TAGS ('{key}' = '{value}')"
            )
    return stmts


def term_statements(
    view: Parts, table: "TableAsset", terms_by_column: Mapping[str, "GroundedTerm"]
) -> list[str]:
    """Each column's grounded term (REQ-2238) as a ``provisa_term`` tag on its view's column,
    naming the term."""
    return [
        # A view's column is tagged through ALTER TABLE: Databricks has no ALTER VIEW ... ALTER
        # COLUMN.
        f"ALTER TABLE {q(*view)} ALTER COLUMN {q(column.name)} "
        f"SET TAGS ('{TERM_TAG}' = '{lit(str(term.name)[:_TAG_VALUE_LIMIT])}')"
        for column in table.columns
        if (term := terms_by_column.get(column.ref.fqn())) is not None
    ]


def _owned(tag: str) -> bool:
    """A tag this export sets: the term tag or a governance tag; any other origin is never touched."""
    return tag == TERM_TAG or tag.startswith(GOVERNANCE_PREFIX)


def stale_tag_statements(
    view: Parts,
    table: "TableAsset",
    governance_tags: list["GovernanceTag"],
    terms_by_column: Mapping[str, "GroundedTerm"],
    table_tags: Iterable[str],
    column_tags: Iterable[tuple[str, str]],
) -> list[str]:
    """Unsets the tags the view still holds that its member no longer carries. ``CREATE OR REPLACE
    VIEW`` keeps the tags of the view it replaces, so a term that left a column, or a governance
    rule that no longer fires, would stay tagged. ``table_tags`` and ``column_tags``
    (``(column, tag)``) are what the view holds now."""
    table_fqn = table.ref.fqn()
    names = {c.ref.fqn(): c.name for c in table.columns}
    wanted_table: set[str] = set()
    wanted_columns: set[tuple[str, str]] = set()
    for tag in governance_tags:
        key = f"{GOVERNANCE_PREFIX}{tag.signal.value}"
        fqn = tag.asset.fqn()
        if fqn == table_fqn:
            wanted_table.add(key)
        elif fqn in names:
            wanted_columns.add((names[fqn], key))
    for column in table.columns:
        if column.ref.fqn() in terms_by_column:
            wanted_columns.add((column.name, TERM_TAG))
    stmts = [
        f"ALTER VIEW {q(*view)} UNSET TAGS ('{lit(tag)}')"
        for tag in sorted(set(table_tags))
        if _owned(tag) and tag not in wanted_table
    ]
    stmts += [
        # A view's column is untagged through ALTER TABLE, as it is tagged.
        f"ALTER TABLE {q(*view)} ALTER COLUMN {q(column)} UNSET TAGS ('{lit(tag)}')"
        for column, tag in sorted(set(column_tags))
        if _owned(tag) and (column, tag) not in wanted_columns
    ]
    return stmts


@register_provider
class UnityCatalogExport(MetadataExport):  # REQ-2117
    """Publishes each data product as a Unity Catalog schema of views over its members."""

    provider_name = "unity_catalog"
    # REQ-1912: which members the engine serves from a replica, as the reconcile decided it.
    needs_export_views = True

    def _runtime_or_none(self) -> Any:
        from provisa.federation.engine import configured_engine_url

        url = configured_engine_url()
        if not url or not url.startswith("databricks://"):
            return None
        from provisa.federation.databricks_runtime import DatabricksFederationRuntime

        return DatabricksFederationRuntime(url=url)

    async def publish(self, snapshot: "MetadataSnapshot") -> PublishResult:
        if not snapshot.data_products:
            return PublishResult(provider_name=self.provider_name)
        runtime = self._runtime_or_none()
        if runtime is None:
            return PublishResult(provider_name=self.provider_name)
        try:
            return await asyncio.to_thread(self._publish, runtime, snapshot)
        finally:
            runtime.close()

    def _member_parts(self, runtime: Any, snapshot: "MetadataSnapshot", ref: Any) -> Parts:
        """Where the engine keeps a member: its replica, or the external table it reads in
        place."""
        from provisa.federation.replica_address import replica_schema, replica_table_name

        source_id, schema_name, table_name = ref.parts
        if (source_id, schema_name, table_name) in self.export_views:
            return (
                runtime.ensure_materialize_attached(),
                replica_schema(snapshot.org_id),
                replica_table_name(source_id, schema_name, table_name),
            )
        source = SimpleNamespace(id=source_id, schema_name=schema_name, table_name=table_name)
        return runtime._phys_parts(source)

    def _publish(self, runtime: Any, snapshot: "MetadataSnapshot") -> PublishResult:
        from databricks.sql.exc import Error as DatabricksError

        from provisa.federation.runtime_support import close_cursor, open_cursor

        result = PublishResult(provider_name=self.provider_name)
        catalog = runtime.ensure_materialize_attached()
        tables = {t.ref.fqn(): t for t in snapshot.tables}
        governance = list(snapshot.governance_tags)
        conn = runtime._open()
        cur = open_cursor(conn)
        try:
            for product in snapshot.data_products:
                try:
                    self._publish_product(
                        cur, runtime, snapshot, catalog, product, tables, governance, result
                    )
                except DatabricksError as exc:
                    # A refused statement is this product's outcome, reported by name; the org's
                    # other products still publish.
                    result.errors.append(AssetError(asset=product.ref, message=str(exc)))
        finally:
            close_cursor(cur)
            conn.close()
        return result

    def _rows(self, cur: Any, sql: str) -> list[tuple]:
        cur.execute(sql)
        return list(cur.fetchall())

    def _publish_product(
        self,
        cur: Any,
        runtime: Any,
        snapshot: "MetadataSnapshot",
        catalog: str,
        product: "DataProductAsset",
        tables: dict[str, "TableAsset"],
        governance: list["GovernanceTag"],
        result: PublishResult,
    ) -> None:
        schema = product.id
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q(catalog, schema)}")
        cur.execute(f"COMMENT ON SCHEMA {q(catalog, schema)} IS '{lit(schema_comment(product))}'")
        wanted_tags = schema_tags(product)
        have_tags = {
            str(k)
            for (k,) in self._rows(
                cur,
                f"SELECT tag_name FROM {q(catalog)}.information_schema.schema_tags "
                f"WHERE schema_name = '{lit(schema)}'",
            )
            if str(k).startswith(PRODUCT_PREFIX)
        }
        for key in sorted(have_tags - set(wanted_tags)):
            cur.execute(f"ALTER SCHEMA {q(catalog, schema)} UNSET TAGS ('{lit(key)}')")
        if wanted_tags:
            pairs = ", ".join(f"'{lit(k)}' = '{lit(v)}'" for k, v in sorted(wanted_tags.items()))
            cur.execute(f"ALTER SCHEMA {q(catalog, schema)} SET TAGS ({pairs})")
        result.bindings[product.semantic_uri] = (f"{catalog}.{schema}", f"{catalog}.{schema}")
        terms = column_terms(snapshot.grounded_terms)

        views: dict[str, Parts] = {}
        members: dict[str, Parts] = {}
        for ref in product.members:
            table = tables[ref.fqn()]
            name = view_name(table)
            if name in views:
                result.errors.append(
                    AssetError(
                        asset=ref,
                        message=f"{ref.fqn()} has the business name {name!r} of another "
                        f"member of {product.id}; one view holds only one",
                    )
                )
                continue
            view = (catalog, schema, name)
            member = self._member_parts(runtime, snapshot, ref)
            cur.execute(view_statement(view, member, table, terms))
            where = f"schema_name = '{lit(schema)}' AND table_name = '{lit(name)}'"
            held_table = [
                str(k)
                for (k, _) in self._rows(
                    cur,
                    f"SELECT tag_name, tag_value FROM {q(catalog)}.information_schema.table_tags "
                    f"WHERE {where}",
                )
            ]
            held_columns = [
                (str(c), str(k))
                for (c, k, _) in self._rows(
                    cur,
                    f"SELECT column_name, tag_name, tag_value "
                    f"FROM {q(catalog)}.information_schema.column_tags WHERE {where}",
                )
            ]
            for stmt in stale_tag_statements(
                view, table, governance, terms, held_table, held_columns
            ):
                cur.execute(stmt)
            for stmt in governance_statements(view, table, governance):
                cur.execute(stmt)
            for stmt in term_statements(view, table, terms):
                cur.execute(stmt)
            views[name] = view
            members[name] = member
            result.bindings[table.semantic_uri] = (".".join(view), ".".join(view))
        for (stale,) in self._rows(
            cur,
            f"SELECT table_name FROM {q(catalog)}.information_schema.views "
            f"WHERE table_schema = '{lit(schema)}'",
        ):
            if str(stale) not in views:
                cur.execute(f"DROP VIEW IF EXISTS {q(catalog, schema, str(stale))}")
        if self._config.delta_share:
            self._share(cur, product, views, members, result)
        result.published["data_products"] = result.published.get("data_products", 0) + 1
        result.published["views"] = result.published.get("views", 0) + len(views)

    def _share(
        self,
        cur: Any,
        product: "DataProductAsset",
        views: dict[str, Parts],
        members: dict[str, Parts],
        result: PublishResult,
    ) -> None:
        """The product's views as its Delta Share: each whose member is in Delta, the rest
        reported."""
        share = share_name(product)
        cur.execute(f"CREATE SHARE IF NOT EXISTS {q(share)}")
        shared = self._shared_objects(cur, share)
        wanted: set[str] = set()
        for name, view in sorted(views.items()):
            catalog, schema, table = members[name]
            rows = self._rows(
                cur,
                f"SELECT data_source_format FROM {q(catalog)}.information_schema.tables "
                f"WHERE table_schema = '{lit(schema)}' AND table_name = '{lit(table)}'",
            )
            if not rows or str(rows[0][0]).upper() != "DELTA":
                result.errors.append(
                    AssetError(
                        asset=product.ref,
                        message=f"member {name!r} is not in Delta, so it cannot be shared",
                    )
                )
                continue
            full = ".".join(view)
            wanted.add(full)
            if full not in shared:
                cur.execute(f"ALTER SHARE {q(share)} ADD VIEW {q(*view)}")
        for stale in sorted(shared - wanted):
            cur.execute(f"ALTER SHARE {q(share)} REMOVE VIEW {q(*stale.split('.'))}")
        result.published["shares"] = result.published.get("shares", 0) + 1

    def _shared_objects(self, cur: Any, share: str) -> set[str]:
        """The full names of what ``share`` holds (``SHOW ALL IN SHARE``'s ``shared_object``;
        its ``name`` is the share-relative ``schema.object``)."""
        cur.execute(f"SHOW ALL IN SHARE {q(share)}")
        column = [d[0] for d in cur.description].index("shared_object")
        return {str(row[column]) for row in cur.fetchall()}

    async def health(self) -> None:
        runtime = self._runtime_or_none()
        if runtime is None:
            raise RuntimeError("Unity Catalog publishes only when Databricks is the engine")
        try:
            await asyncio.to_thread(runtime.ensure_materialize_attached)
        finally:
            runtime.close()
