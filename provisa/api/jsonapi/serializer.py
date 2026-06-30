# Copyright (c) 2026 Kenneth Stott
# Canary: e7b8a766-2ce0-4e1c-95e7-548a86a39cfb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row data to JSON:API resource object serialization."""

# Requirements: REQ-257

from __future__ import annotations

from typing import Any


def row_to_resource(  # REQ-257
    row: dict[str, Any],
    resource_type: str | None = None,
    id_field: str = "id",
    relationship_fields: dict[str, str] | None = None,
    *,
    table: str | None = None,
    fields: list[str] | None = None,
) -> dict[str, Any]:
    """Convert a flat row dict to a JSON:API resource object.

    Args:
        row: flat dict of column values.
        resource_type: the JSON:API type (table name). Also accepted as ``table``.
        id_field: which key to use as the resource id.
        relationship_fields: {field_name: related_type} for FK columns.
        table: alias for ``resource_type``.
        fields: sparse fieldset — only include these attribute names. None = all.

    Returns:
        JSON:API resource object with type, id, attributes, relationships.
    """
    effective_type = resource_type or table or ""
    relationship_fields = relationship_fields or {}
    fk_columns = set(relationship_fields.keys())
    sparse = set(fields) if fields else None

    resource_id = str(row.get(id_field, ""))
    attributes: dict[str, Any] = {}
    relationships: dict[str, Any] = {}

    for key, value in row.items():
        if key == id_field:
            continue
        if key in fk_columns:
            related_type = relationship_fields[key]
            if key.endswith("_id"):
                rel_name = key[:-3]
            elif key.endswith("Id"):
                rel_name = key[:-2]
            else:
                rel_name = key
            relationships[rel_name] = {
                "data": {"type": related_type, "id": str(value)} if value is not None else None,
            }
        else:
            if sparse is None or key in sparse:
                attributes[key] = value

    resource: dict[str, Any] = {
        "type": effective_type,
        "id": resource_id,
        "attributes": attributes,
    }
    if relationships:
        resource["relationships"] = relationships
    return resource


def rows_to_jsonapi(  # REQ-257
    rows: list[dict[str, Any]],
    resource_type: str | None = None,
    id_field: str = "id",
    relationship_fields: dict[str, str] | None = None,
    included_rows: dict[str, list[dict[str, Any]]] | None = None,
    *,
    table: str | None = None,
    fields: list[str] | None = None,
    links: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert rows to a full JSON:API document.

    Args:
        rows: list of flat row dicts.
        resource_type: table name used as JSON:API type. Also accepted as ``table``.
        id_field: primary key field.
        relationship_fields: FK columns -> related type.
        included_rows: {type: [row, ...]} for sideloaded resources.
        table: alias for ``resource_type``.
        fields: sparse fieldset list of field names (None = all).
        links: pagination/self links dict to include in the document.
        meta: metadata dict to include in the document.

    Returns:
        JSON:API document with data, included, meta.
    """
    effective_type = resource_type or table or ""
    data = [
        row_to_resource(row, effective_type, id_field, relationship_fields, fields=fields)
        for row in rows
    ]

    doc: dict[str, Any] = {"data": data}

    if included_rows:
        included = []
        for inc_type, inc_rows in included_rows.items():
            for inc_row in inc_rows:
                included.append(row_to_resource(inc_row, inc_type, "id", None))
        if included:
            doc["included"] = included

    doc["meta"] = {**(meta or {}), "total": len(rows)}

    if links is not None:
        doc["links"] = links

    return doc
