# Copyright (c) 2026 Kenneth Stott
# Canary: 9d3e6f0a-4c2b-4b8e-8f51-2a7c0e9d1b64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a router hands the Rust encoders, built from the objects it already holds: a compiled
query's columns, OData's entity set and expansion plans. Once per request and per plan, never per
row or value. Wiring moves these beside the routers."""

from __future__ import annotations

from typing import Any

from provisa.compiler.sql_types import ColumnRef


def plan_tuples(columns: list[ColumnRef]) -> list[tuple]:
    """``(field_name, nested_in, cardinality, is_agg, json, json_text, integer)`` per column."""
    return [
        (c.field_name, c.nested_in, c.cardinality, c.is_agg, c.json, sorted(c.json_text), c.integer)
        for c in columns
    ]


def odata_entity(entity: Any, fields: tuple[str, ...], plans: tuple[Any, ...]) -> tuple:  # noqa: ANN401
    """An OData level: ``(set name, {property: edm name}, fields, [navigation])``, a navigation
    ``(name, collection, page, parent_key, link_query, target level)``; ``link_query`` is
    ``page_link``'s query for the expansion's next page (the same for every parent row)."""
    from provisa.api.odata.expand import page_link

    navigations = [
        (
            p.navigation.name,
            p.navigation.collection,
            p.page,
            list(p.parent_key),
            page_link("", p.raw, p.options, p.page),
            odata_entity(p.target, p.fields, p.children),
        )
        for p in plans
    ]
    return (
        entity.name,
        {p.name: p.type.name for p in entity.properties},
        list(fields),
        navigations,
    )


def _has_list_view(t: Any) -> bool:  # noqa: ANN401 — pyarrow.DataType
    import pyarrow as pa

    if pa.types.is_list_view(t) or pa.types.is_large_list_view(t):
        return True
    if pa.types.is_struct(t):
        return any(_has_list_view(t.field(i).type) for i in range(t.num_fields))
    if pa.types.is_map(t):
        return _has_list_view(t.key_type) or _has_list_view(t.item_type)
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return _has_list_view(t.value_type)
    return False


def _without_list_view(a: Any) -> Any:  # noqa: ANN401 — pyarrow.Array
    """``a`` with each list view rebuilt as the list it holds: its elements taken in order (numpy
    index arithmetic, then pyarrow's take; pyarrow 24's own list-view cast writes an invalid
    offsets buffer)."""
    import numpy as np
    import pyarrow as pa

    t = a.type
    if not _has_list_view(t):
        return a
    nulls = a.is_null() if a.null_count else None
    if pa.types.is_list_view(t) or pa.types.is_large_list_view(t):
        sizes = np.where(np.asarray(a.is_valid()), np.asarray(a.sizes.fill_null(0)), 0)
        starts = np.concatenate([[0], np.cumsum(sizes)]).astype(np.int64)
        idx = np.repeat(np.asarray(a.offsets.fill_null(0)) - starts[:-1], sizes) + np.arange(
            starts[-1]
        )
        values = _without_list_view(a.values.take(pa.array(idx, pa.int64())))
        cls = pa.ListArray if pa.types.is_list_view(t) else pa.LargeListArray
        width = pa.int32() if pa.types.is_list_view(t) else pa.int64()
        return cls.from_arrays(pa.array(starts, width), values, mask=nulls)
    if pa.types.is_struct(t):
        kids = [_without_list_view(a.field(i)) for i in range(t.num_fields)]
        return pa.StructArray.from_arrays(
            kids, fields=[t.field(i).with_type(k.type) for i, k in enumerate(kids)], mask=nulls
        )
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        flat = a.values.slice(a.offsets[0].as_py(), a.offsets[-1].as_py() - a.offsets[0].as_py())
        import pyarrow.compute as pc

        offs = pc.subtract(a.offsets, a.offsets[0])
        cls = pa.ListArray if pa.types.is_list(t) else pa.LargeListArray
        return cls.from_arrays(offs, _without_list_view(flat), mask=nulls)
    raise TypeError(f"a list view inside {t} is not rebuilt")


def rust_batches(batches: list[Any]) -> list[Any]:  # noqa: ANN401 — pyarrow.RecordBatch
    """The batches as the encoders import them: arrow-rs reads no list view through the C data
    interface, so a list view column (DuckDB's ``arrow_output_list_view``, which the product does
    not set) is rebuilt as the list it holds. Any other batch is handed on as it is."""
    import pyarrow as pa

    out = []
    for b in batches:
        if not any(_has_list_view(f.type) for f in b.schema):
            out.append(b)
            continue
        cols = [_without_list_view(c) for c in b.columns]
        out.append(
            pa.RecordBatch.from_arrays(
                cols, schema=pa.schema([f.with_type(c.type) for f, c in zip(b.schema, cols)])
            )
        )
    return out


def warnings_json(warnings: list[dict] | None) -> bytes | None:
    """The request's statement warnings (``as_entries()``) as the JSON the document carries;
    None when nothing was raised."""
    from provisa.api.json_response import dumps

    return dumps(warnings) if warnings else None
