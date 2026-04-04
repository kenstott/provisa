# Copyright (c) 2025 Kenneth Stott
# Canary: e7b8a766-2ce0-4e1c-95e7-548a86a39cfb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row data to JSON:API resource object serialization."""

from __future__ import annotations

from typing import Any


def row_to_resource(
    row: dict[str, Any],
    resource_type: str,
    id_field: str = "id",
    relationship_fields: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert a flat row dict to a JSON:API resource object.

    Args:
        row: flat dict of column values.
        resource_type: the JSON:API type (table name).
        id_field: which key to use as the resource id.
        relationship_fields: {field_name: related_type} for FK columns.
            e.g. {"customer_id": "customers"} -> relationship "customer".

    Returns:
        JSON:API resource object with type, id, attributes, relationships.
    """
    resource_id = str(row.get(id_field, ""))
    relationship_fields = relationship_fields or {}

    # Separate attributes from relationship FK columns
    attributes: dict[str, Any] = {}
    relationships: dict[str, Any] = {}

    fk_columns = set(relationship_fields.keys())

    for key, value in row.items():
        if key == id_field:
            continue
        if key in fk_columns:
            related_type = relationship_fields[key]
            # Derive relationship name: strip _id suffix if present
            rel_name = key[:-3] if key.endswith("_id") else key
            relationships[rel_name] = {
                "data": {
                    "type": related_type,
                    "id": str(value),
                } if value is not None else None,
            }
        else:
            attributes[key] = value

    resource: dict[str, Any] = {
        "type": resource_type,
        "id": resource_id,
        "attributes": attributes,
    }
    if relationships:
        resource["relationships"] = relationships
    return resource


def rows_to_jsonapi(
    rows: list[dict[str, Any]],
    resource_type: str,
    id_field: str = "id",
    relationship_fields: dict[str, str] | None = None,
    included_rows: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Convert rows to a full JSON:API document.

    Args:
        rows: list of flat row dicts.
        resource_type: table name used as JSON:API type.
        id_field: primary key field.
        relationship_fields: FK columns -> related type.
        included_rows: {type: [row, ...]} for sideloaded resources.

    Returns:
        JSON:API document with data, included, meta.
    """
    data = [
        row_to_resource(row, resource_type, id_field, relationship_fields)
        for row in rows
    ]

    doc: dict[str, Any] = {"data": data}

    if included_rows:
        included = []
        for inc_type, inc_rows in included_rows.items():
            for inc_row in inc_rows:
                included.append(
                    row_to_resource(inc_row, inc_type, "id", None)
                )
        if included:
            doc["included"] = included

    doc["meta"] = {"total": len(rows)}
    return doc
