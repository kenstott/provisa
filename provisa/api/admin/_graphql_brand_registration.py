# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e2a71-6f85-4d3b-a1c9-3b7e5d8f0a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a table of a branded GraphQL source (REQ-1923).

Adding a branded source registers no tables: every table its shipped schema offers is listed
by the Register Table picker, and the steward registers the ones wanted through the same
mutation every other source uses. This module is what that mutation needs from the brand --
the tables on offer, a table's columns, and the columns as they are stored once the table has
been fitted to what the source's credential may read.
"""

# Requirements: REQ-1923
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from provisa.compiler.naming import apply_sql_name
from provisa.graphql_remote.brands import BRANDS, Brand, available_tables, map_table

if TYPE_CHECKING:
    from provisa.core.models import Column

log = logging.getLogger(__name__)

# Mapper column type -> the engine type stored as table_columns.data_type (the same map the
# one-shot registration of a plain remote GraphQL source uses).
_PHYSICAL_TYPE = {
    "text": "varchar",
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "jsonb": "json",
}


def branded_registration(state, source_id: str) -> tuple[Brand, dict] | None:
    """The brand and live registration of ``source_id``, or None when it is not a branded
    source."""
    reg = getattr(state, "graphql_remote_sources", {}).get(source_id)
    if reg is None or not reg.get("brand"):
        return None
    return BRANDS[reg["brand"]], reg


def offered_tables(brand: Brand, reg: dict) -> list[dict]:
    """Every table the brand offers this source, each as ``{name, description}`` under the name
    it registers with."""
    return [
        {"name": t["sql_name"], "description": t.get("description")}
        for t in available_tables(brand, reg["namespace"])
    ]


def _column_models(table: dict) -> "list[Column]":
    """A mapped table's columns as they are stored: each selected field, then one native-filter
    column per argument the table's root field requires."""
    from provisa.api.admin.graphql_remote_router import _build_object_fields
    from provisa.core.models import Column

    columns = [
        Column(
            name=apply_sql_name(c["name"]),
            visible_to=[],
            description=c.get("description"),
            data_type=_PHYSICAL_TYPE[c["type"]],
            object_fields=_build_object_fields(c.get("gql_object_fields") or []),
            gql_selection=c.get("gql_selection"),
        )
        for c in table["columns"]
    ]
    columns += [
        Column(
            name=f"_nf_{apply_sql_name(a['name'])}",
            visible_to=[],
            native_filter_type="query_param",
            data_type=_PHYSICAL_TYPE[a["provisa_type"]],
        )
        for a in table.get("required_args") or []
    ]
    return columns


def offered_columns(brand: Brand, reg: dict, table_name: str) -> list[tuple[str, str, str | None]]:
    """The columns a not-yet-registered table offers, for the picker's column list: each as
    (name, stored type, description), native-filter columns included."""
    table = map_table(brand, reg["namespace"], reg["source_id"], "", table_name)
    return [
        (apply_sql_name(c["name"]), _PHYSICAL_TYPE[c["type"]], c.get("description"))
        for c in table["columns"]
    ] + [
        (f"_nf_{apply_sql_name(a['name'])}", _PHYSICAL_TYPE[a["provisa_type"]], None)
        for a in table.get("required_args") or []
    ]


async def columns_to_register(
    brand: Brand,
    reg: dict,
    table_name: str,
    domain_id: str,
    chosen: "list[Column]",
    page_size: int,
) -> "tuple[list[Column], list[dict]]":
    """The columns to store for a table being registered, and the fields left out of it.

    ``chosen`` is what the steward picked -- its governance (visibility, masking, alias) is
    kept, and when it names columns only those are registered. A native-filter column is always
    registered: without it the table's required arguments cannot be supplied.

    The table, with the columns picked, is offered to the remote once with the source's
    credential at ``page_size``, the size a read asks for. A field the remote refuses that
    credential is left out and reported; a query the remote prices above what it allows raises
    ``QueryTooComplex`` (provisa.graphql_remote.probe).
    """
    from provisa.graphql_remote.probe import fit_table_to_credential

    mapped = map_table(brand, reg["namespace"], reg["source_id"], domain_id, table_name)
    picked = {c.name for c in chosen if c.native_filter_type is None}
    if picked:
        kept = [c for c in mapped["columns"] if apply_sql_name(c["name"]) in picked]
        mapped = {**mapped, "columns": kept}
    fitted, omitted = await fit_table_to_credential(
        reg["url"],
        reg.get("auth"),
        mapped,
        brand.refused_error_types,
        page_size,
        brand.too_complex_messages,
    )
    offered = _column_models(fitted)
    by_choice = {c.name: c for c in chosen}
    columns = []
    for col in offered:
        pick = by_choice.get(col.name)
        if pick is None:
            columns.append(col)
            continue
        columns.append(
            pick.model_copy(
                update={
                    "data_type": col.data_type,
                    "object_fields": col.object_fields,
                    "gql_selection": col.gql_selection,
                    "native_filter_type": col.native_filter_type,
                    "description": pick.description or col.description,
                }
            )
        )
    return columns, omitted
