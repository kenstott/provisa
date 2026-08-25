# Copyright (c) 2026 Kenneth Stott
# Canary: f1d64e34-ce5b-45a0-83ae-bb41849412c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CompilationContext builder for sql_gen (REQ-008, REQ-009, REQ-151).

Builds the CompilationContext (table/join/column metadata) from a SchemaInput
before query compilation. Extracted from sql_gen.py; leaf module.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence  # noqa: F401

from provisa.compiler.naming import (
    junction_field_name,
    rel_field_name as _rel_field_name,
    source_to_catalog,
)
from provisa.compiler.sql_rewrite import semantic_table_name
from provisa.compiler.sql_types import (
    CompilationContext,
    JoinMeta,
    JunctionMeta,
    TableMeta,
    key_list,
    _TableInfoProto,  # noqa: F401  (used in type annotations)
)


def _lookup_column_type(
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    table_id: int,
    column_name: str,
    catalog: str | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> str:
    """Look up a column's the engine data type.

    Checks compile-time column_types first; falls back to schema_service
    live the engine query when catalog/schema/table are provided.
    """
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    col_metas = si.column_types.get(table_id, [])
    for meta in col_metas:
        if meta.column_name == column_name:
            return meta.data_type
    if catalog and schema and table:
        from provisa.compiler import schema_service

        return schema_service.get_column_type(catalog, schema, table, column_name)
    return "varchar"


def _visible_unique_constraints(t, si) -> list[tuple[str, list[str]]]:  # REQ-1093
    """Declared UNIQUE constraints for table ``t``, filtered to columns visible in this projection.

    Sourced from the raw table config on SchemaInput (not _TableInfo) so schema_gen stays untouched.
    """
    visible = {col["column_name"] for col in t.visible_columns}
    raw = next(
        (tbl.get("unique_constraints", []) for tbl in si.tables if tbl["id"] == t.table_id), []
    )
    return [
        (uc["name"], list(uc["columns"])) for uc in raw if all(c in visible for c in uc["columns"])
    ]


def _register_table_in_ctx(
    t: _TableInfoProto,
    ctx: CompilationContext,
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    physical_map: dict[str, str],
    table_preset_map: dict[int, list],
) -> None:
    """Register one visible table and its column metadata into ctx."""
    from provisa.compiler.naming import apply_gql_name, apply_sql_name, domain_to_sql_name as _d2s
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    physical_name = physical_map.get(t.table_name, t.table_name)
    src_type = (si.source_types or {}).get(t.source_id, "")
    meta = TableMeta(
        table_id=t.table_id,
        field_name=t.field_name,
        type_name=t.type_name,
        source_id=t.source_id,
        catalog_name=(si.source_catalogs or {}).get(t.source_id) or source_to_catalog(t.source_id),
        schema_name=t.schema_name,
        table_name=physical_name,
        domain_id=t.domain_id,
        column_presets=table_preset_map.get(t.table_id, []),
        source_type=src_type,
        original_table_name=t.table_name if physical_name != t.table_name else "",
        display_name=t.alias or "",
    )
    ctx.tables[t.field_name] = meta
    _display = semantic_table_name(meta)
    ctx.virtual_columns[t.table_id] = {
        "_name_": f"{_d2s(t.domain_id)}.{_display}",
        "_domain_": t.domain_id,
    }
    ctx.tables[f"{t.field_name}_aggregate"] = meta
    ctx.tables[f"{t.field_name}Aggregate"] = meta
    ctx.tables[f"{t.field_name}_connection"] = meta
    ctx.tables[f"{t.field_name}_group_by"] = meta
    ctx.tables[f"{t.field_name}GroupBy"] = meta

    # Aggregate columns — exclude GQL object columns that have no physical DB column.
    # Virtual GQL fields (e.g. FK-resolved "pet" on inquiries) are not real columns in the engine.
    _gql_obj_cols = si.gql_object_columns.get(t.table_name, {})
    _covered_blobs = {
        blob_col for blob_col in _gql_obj_cols if blob_col.lower() not in t.column_metadata
    }
    col_info = []
    for col in t.visible_columns:
        col_name = col["column_name"]
        if col_name in _covered_blobs:
            continue
        col_meta = t.column_metadata.get(col_name.lower())
        if col_meta:
            col_info.append((col_name, col_meta.data_type))
    ctx.aggregate_columns[t.table_id] = col_info

    # REQ-1319: precompile each role-visible single-table metric into its physical inline
    # SQL, so _compile_aggregate_field/_compile_group_by_field select a metric exactly like
    # a sum/avg column — the same expansion the raw-SQL pipeline applies (REQ-1317).
    from provisa.compiler.metric_expand import inline_metric_sql

    for m in t.metrics:

        def _resolve(col_name: str, _t: "_TableInfoProto" = t, _name: str = m["name"]) -> str:
            if col_name.lower() not in _t.column_metadata:
                raise ValueError(
                    f"metric {_name!r}: column {col_name!r} is not a column of "
                    f"table {_t.table_name!r} (REQ-1319)"
                )
            return col_name

        ctx.metric_exprs[(t.table_id, m["name"])] = inline_metric_sql(
            m["name"], m["expression"], t.table_name, _resolve
        )

    ctx.pk_columns[t.table_id] = [
        col["column_name"] for col in t.visible_columns if col.get("is_primary_key")
    ]

    # REQ-1093: carry declared UNIQUE constraints (name + ordered columns) into ctx for pgwire.
    ctx.unique_constraints[t.table_id] = _visible_unique_constraints(t, si)

    def _nfc_type(nfc: dict) -> str:
        if nfc.get("data_type"):
            return nfc["data_type"]
        if nfc.get("type"):
            return nfc["type"]
        col_key = nfc["column_name"].lower()
        bare_key = col_key[4:] if col_key.startswith("_nf_") else col_key
        meta = t.column_metadata.get(col_key) or t.column_metadata.get(bare_key)
        return meta.data_type if meta else "text"

    ctx.native_filter_columns[t.table_id] = {
        nfc["column_name"]: _nfc_type(nfc) for nfc in t.native_filter_columns
    }

    for col in t.visible_columns:
        col_path = col.get("path")
        phys = col["column_name"]
        gql = col.get("alias") or apply_gql_name(phys, getattr(t, "gql_convention_override", None))
        if gql != phys:
            ctx.exposed_to_physical[(t.table_id, gql)] = phys
        sql_name = col.get("alias") or apply_sql_name(phys)
        if sql_name != phys and sql_name != gql:
            ctx.exposed_to_physical[(t.table_id, sql_name)] = phys
        ctx.physical_to_sql[(t.table_id, phys)] = sql_name
        if col_path:
            ctx.column_paths[(t.table_id, gql)] = col_path
        object_fields = col.get("object_fields")
        if object_fields and isinstance(object_fields, list):
            convention = getattr(t, "gql_convention_override", None)
            ctx.gql_json_columns.add((t.table_id, gql))
            for sf in object_fields:
                sf_name = sf["name"]
                sf_gql = sf.get("alias") or apply_gql_name(sf_name, convention)
                ctx.column_paths[(t.table_id, sf_gql)] = f"{phys}.{sf_name}"

    for col_name in si.gql_object_columns.get(t.table_name) or {}:
        ctx.gql_json_columns.add((t.table_id, col_name))


def _join_field_name(  # REQ-194, REQ-1586
    rel: Mapping,
    tgt_info: _TableInfoProto,
    table_lookup: Mapping[int, _TableInfoProto],
) -> str:
    """The field name this relationship registers its join under.

    An FK edge is named for the table it reaches. A junction-backed edge cannot be: one junction
    table backs several edges between the same pair of tables, and naming them all for the target
    leaves ``ctx.joins`` holding whichever one registered last. Its nominated label names it
    instead — the same nomination the Cypher type takes.
    """
    if rel.get("via_table_id"):
        via_info = table_lookup[rel["via_table_id"]]
        return junction_field_name(
            rel["via_label_source"],
            rel["cardinality"],
            type_value=rel.get("via_type_value"),
            via_table_name=via_info.table_name,
            cypher_alias=rel.get("alias"),
        )
    return _rel_field_name(tgt_info.field_name, rel["cardinality"])


def _build_junction_meta(  # REQ-1586
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    rel: Mapping,
    table_lookup: Mapping[int, _TableInfoProto],
    physical_map: dict[str, str],
) -> JunctionMeta | None:
    """Resolve a relationship row's junction declaration into a JunctionMeta.

    Returns None when the row declares no junction. A declared junction whose table is not in
    ``table_lookup`` is not visible to this role, so the edge itself is not registered — the caller
    distinguishes that from "no junction" by checking ``rel["via_table_id"]``.
    """
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    via_id = rel.get("via_table_id")
    if not via_id or via_id not in table_lookup:
        return None
    via_info = table_lookup[via_id]
    via_physical = physical_map.get(via_info.table_name, via_info.table_name)
    via_meta = TableMeta(
        table_id=via_info.table_id,
        field_name=via_info.field_name,
        type_name=via_info.type_name,
        source_id=via_info.source_id,
        catalog_name=(si.source_catalogs or {}).get(via_info.source_id)
        or source_to_catalog(via_info.source_id),
        schema_name=via_info.schema_name,
        table_name=via_physical,
        domain_id=via_info.domain_id,
        original_table_name=via_info.table_name if via_physical != via_info.table_name else "",
    )
    from provisa.compiler.naming import apply_gql_name

    src_keys = key_list(rel["via_source_column"])
    tgt_keys = key_list(rel["via_target_column"])
    keys = {*src_keys, *tgt_keys}
    type_column = rel.get("via_type_column") or None
    if type_column:
        # The discriminator is fixed for this edge by via_type_value, so it carries no information
        # as an attribute.
        keys.add(type_column)
    convention = getattr(via_info, "gql_convention_override", None)
    attributes = tuple(
        (col.get("alias") or apply_gql_name(col["column_name"], convention), col["column_name"])
        for col in via_info.visible_columns
        if col["column_name"] not in keys
    )
    return JunctionMeta(
        table=via_meta,
        source_columns=src_keys,
        target_columns=tgt_keys,
        type_column=type_column,
        type_value=rel.get("via_type_value") or None,
        attributes=attributes,
        label_source=rel["via_label_source"],
        label_fixed=rel.get("alias") or "",
    )


def _register_relationship_joins(
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    ctx: CompilationContext,
    table_lookup: Mapping[int, _TableInfoProto],
    physical_map: dict[str, str],
) -> None:
    """Register JoinMeta entries for all explicit relationships."""
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    for rel in si.relationships:
        src_id = rel["source_table_id"]
        tgt_id = rel["target_table_id"]
        if src_id not in table_lookup or tgt_id not in table_lookup:
            continue
        if not rel.get("source_column") and not rel.get("target_function_name"):
            continue
        # REQ-1586: a junction-backed edge is only traversable when the junction table is visible;
        # without it there is no join path at all, so the edge is not registered.
        if rel.get("via_table_id") and rel["via_table_id"] not in table_lookup:
            continue

        src_info = table_lookup[src_id]
        tgt_info = table_lookup[tgt_id]

        _tgt_physical_name = physical_map.get(tgt_info.table_name, tgt_info.table_name)
        tgt_meta = TableMeta(
            table_id=tgt_info.table_id,
            field_name=tgt_info.field_name,
            type_name=tgt_info.type_name,
            source_id=tgt_info.source_id,
            catalog_name=(si.source_catalogs or {}).get(tgt_info.source_id)
            or source_to_catalog(tgt_info.source_id),
            schema_name=tgt_info.schema_name,
            table_name=_tgt_physical_name,
            domain_id=tgt_info.domain_id,
            original_table_name=tgt_info.table_name
            if _tgt_physical_name != tgt_info.table_name
            else "",
        )

        src_col_type = _lookup_column_type(
            si,
            src_id,
            rel["source_column"],
            catalog=(si.source_catalogs or {}).get(src_info.source_id)
            or source_to_catalog(src_info.source_id),
            schema=src_info.schema_name,
            table=src_info.table_name,
        )
        tgt_col_type = _lookup_column_type(
            si,
            tgt_id,
            rel["target_column"],
            catalog=(si.source_catalogs or {}).get(tgt_info.source_id)
            or source_to_catalog(tgt_info.source_id),
            schema=tgt_info.schema_name,
            table=tgt_info.table_name,
        )

        join_field_name = rel.get("graphql_alias") or _join_field_name(rel, tgt_info, table_lookup)
        ctx.joins[(src_info.type_name, join_field_name)] = JoinMeta(
            source_column=rel["source_column"],
            target_column=rel["target_column"],
            source_column_type=src_col_type,
            target_column_type=tgt_col_type,
            target=tgt_meta,
            cardinality=rel["cardinality"],
            cypher_alias=rel.get("alias") or None,
            disable_cypher=rel.get("disable_cypher", False),
            source_json_key=rel.get("source_json_key") or None,
            via=_build_junction_meta(si, rel, table_lookup, physical_map),
        )


def _register_meta_synthetic_joins(
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    ctx: CompilationContext,
    tables: Sequence[_TableInfoProto],
    physical_map: dict[str, str],
) -> _TableInfoProto | None:
    """Inject synthetic _meta join for every non-meta table → meta:registered_tables."""
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    meta_rt = next(
        (t for t in tables if t.domain_id == "meta" and t.table_name == "registered_tables"),
        None,
    )
    if not meta_rt:
        return None
    meta_tgt = TableMeta(
        table_id=meta_rt.table_id,
        field_name=meta_rt.field_name,
        type_name=meta_rt.type_name,
        source_id=meta_rt.source_id,
        catalog_name=(si.source_catalogs or {}).get(meta_rt.source_id)
        or source_to_catalog(meta_rt.source_id),
        schema_name=meta_rt.schema_name,
        table_name=physical_map.get(meta_rt.table_name, meta_rt.table_name),
        domain_id=meta_rt.domain_id,
    )
    for t in tables:
        if t.domain_id == "meta":
            continue
        ctx.joins[(t.type_name, "_meta")] = JoinMeta(
            source_column="__table_id__",
            target_column="id",
            source_column_type="text",
            target_column_type="text",
            target=meta_tgt,
            cardinality="many-to-one",
            cypher_alias="HAS_TABLE",
            source_constant=f"{t.domain_id}.{t.table_name}",
            source_expr=f"VARCHAR '{t.domain_id}.{t.table_name}'",
            target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
            child_src_val=f"VARCHAR '{t.domain_id}.{t.table_name}'",
        )
    return meta_rt


def _register_ops_synthetic_joins(
    si: object,  # object-ok: circular import boundary — SchemaInput imported inside function body
    ctx: CompilationContext,
    tables: Sequence[_TableInfoProto],
    physical_map: dict[str, str],
    meta_rt: _TableInfoProto | None,
) -> None:
    """Inject synthetic traversal joins: registered_tables → ops tables with table_name FK."""
    from provisa.compiler.schema_gen import SchemaInput

    assert isinstance(si, SchemaInput)
    _IMPLICIT_DOMAINS = {"meta", "ops"}
    for ops_t in tables:
        if ops_t.domain_id not in _IMPLICIT_DOMAINS or ops_t.domain_id == "meta":
            continue
        if not any(c["column_name"] == "table_name" for c in ops_t.visible_columns):
            continue
        ops_tgt = TableMeta(
            table_id=ops_t.table_id,
            field_name=ops_t.field_name,
            type_name=ops_t.type_name,
            source_id=ops_t.source_id,
            catalog_name=(si.source_catalogs or {}).get(ops_t.source_id)
            or source_to_catalog(ops_t.source_id),
            schema_name=ops_t.schema_name,
            table_name=physical_map.get(ops_t.table_name, ops_t.table_name),
            domain_id=ops_t.domain_id,
        )
        _base = ops_t.field_name.split("__", 1)[1] if "__" in ops_t.field_name else ops_t.field_name
        join_field = f"_{_base}"
        if meta_rt:
            ctx.joins[(meta_rt.type_name, join_field)] = JoinMeta(
                source_column="table_name",
                source_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
                target_column="table_name",
                target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
                source_column_type="text",
                target_column_type="text",
                target=ops_tgt,
                cardinality="one-to-many",
                cypher_alias=f"HAS_{_base.upper()}",
                default_limit=10,
            )


def build_context(  # REQ-008, REQ-009, REQ-151, REQ-393
    si: object,  # object-ok: circular-import boundary — SchemaInput imported inside function body
) -> (
    CompilationContext
):  # object-ok: circular import boundary — SchemaInput imported inside function body
    """Build CompilationContext from a SchemaInput.

    This mirrors the logic in schema_gen._build_visible_tables and _assign_names
    to produce the same field_name/type_name mapping.
    """
    from provisa.compiler.naming import domain_gql_alias
    from provisa.compiler.schema_gen import SchemaInput, _build_visible_tables, _assign_names

    assert isinstance(si, SchemaInput)
    ctx = CompilationContext()

    tables = _build_visible_tables(si)
    if not tables:
        return ctx

    domain_alias_map = {
        d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias"))
        for d in si.domains
        if domain_gql_alias(d["id"], d.get("graphql_alias"))
    }
    _assign_names(
        tables, si.naming_rules, domain_prefix=si.domain_prefix, domain_alias_map=domain_alias_map
    )

    table_lookup = {t.table_id: t for t in tables}
    physical_map = getattr(si, "physical_table_map", None) or {}
    table_preset_map = {td["id"]: list(td.get("column_presets") or []) for td in si.tables}

    for t in tables:
        _register_table_in_ctx(t, ctx, si, physical_map, table_preset_map)

    _register_relationship_joins(si, ctx, table_lookup, physical_map)

    meta_rt = _register_meta_synthetic_joins(si, ctx, tables, physical_map)

    _register_ops_synthetic_joins(si, ctx, tables, physical_map, meta_rt)

    ctx.gql_governed_object_cols = si.gql_governed_object_cols or set()

    return ctx


# --- AST value extraction ---
