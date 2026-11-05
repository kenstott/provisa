# Copyright (c) 2026 Kenneth Stott
# Canary: 24401962-a475-4117-a1f8-99527a7739ce
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JSON surfaces' answers, written by the Rust encoders (provisa._encoders.json) straight from
the result in the form it has (REQ-2194, REQ-2195): the engine's Arrow batches, or a row result's
rows, never built into Arrow (``result_batches``): GraphQL, the REST table endpoint (JSON and
CSV), JSON:API and OData. No Python row, dict or value is built for a row; what a router hands over is the plan it
already holds (the compiled columns, OData's entity levels, JSON:API's names and page), once per
request.

Values are REQ-1867's exact values: a DECIMAL is a JSON string of its digits, PostgreSQL's
infinite dates and timestamps are ``"infinity"`` / ``"-infinity"``, an integer-typed column is a
JSON number, a number inside JSON keeps its digits, and only json/jsonb columns are parsed. A
named zone is read as zoneinfo and PostgreSQL read it (tzdata), a nanosecond value is the instant
floored to the microsecond.

What JSON cannot carry is refused by name: :class:`JsonValueRefused` (naming the field) and
:class:`JsonResponseRefused`; OData's refusals are :class:`QueryOptionRefused`. Any other failure
of the encoder is :class:`EncoderFailed`, a 500 naming its cause.
"""

# Requirements: REQ-2194, REQ-2195, REQ-1867

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import Any, TypeVar

from provisa.compiler.sql_types import ColumnRef
from provisa.executor.result import is_arrow
from provisa.core.json_refusal import JsonResponseRefused, JsonValueRefused

_T = TypeVar("_T")


class EncoderFailed(ValueError):
    """The encoder could not write the answer (a 500): a value the declared type cannot hold, a
    selection whose paths run through a value. The message names the cause."""


def _encoders() -> Any:  # noqa: ANN401 — the compiled extension module
    import provisa._encoders.json as encoders  # pyright: ignore[reportMissingModuleSource]

    return encoders


@contextmanager
def _refusals() -> Generator[None, None, None]:
    """The encoder's errors as the product answers them."""
    encoders = _encoders()
    try:
        yield
    except encoders.ValueRefused as exc:
        field, reason = exc.args
        raise JsonValueRefused(field, reason) from exc
    except encoders.ResponseRefused as exc:
        raise JsonResponseRefused(exc.args[0]) from exc
    except encoders.ODataRefused as exc:
        from provisa.api.odata.errors import QueryOptionRefused

        status, code, message = exc.args
        raise QueryOptionRefused(status, code, message) from exc
    except encoders.EncodeError as exc:
        raise EncoderFailed(str(exc)) from exc


def _encoded(call: Callable[[], _T]) -> _T:
    """``call``'s answer, timed as the request's ``encode`` stage (where a body is written)."""
    from provisa.otel_compat import get_tracer, stage

    with stage(get_tracer(__name__), "http.encode", name="encode", request_only=True):
        with _refusals():
            return call()


def plan_tuples(columns: list[ColumnRef]) -> list[tuple]:
    """``(field_name, nested_in, cardinality, is_agg, json, json_text, integer)`` per column."""
    return [
        (c.field_name, c.nested_in, c.cardinality, c.is_agg, c.json, sorted(c.json_text), c.integer)
        for c in columns
    ]


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
    offsets buffer). No Python value is built."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

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
        first = a.offsets[0].as_py()
        flat = a.values.slice(first, a.offsets[-1].as_py() - first)
        offsets = pc.call_function("subtract", [a.offsets, a.offsets[0]])
        cls = pa.ListArray if pa.types.is_list(t) else pa.LargeListArray
        return cls.from_arrays(offsets, _without_list_view(flat), mask=nulls)
    raise TypeError(f"a list view inside {t} is not rebuilt")


def arrow_batches(batches: Iterable[Any]) -> list[Any]:  # noqa: ANN401 — pyarrow.RecordBatch
    """The batches as the encoders import them (the C data interface, zero-copy): arrow-rs reads
    no list view, so a list view column (DuckDB's ``arrow_output_list_view``, which Provisa does
    not set) is rebuilt as the list it holds. Any other batch is handed on as it is."""
    import pyarrow as pa

    out = []
    for b in batches:
        if not any(_has_list_view(f.type) for f in b.schema):
            out.append(b)
            continue
        cols = [_without_list_view(c) for c in b.columns]
        schema = pa.schema([f.with_type(c.type) for f, c in zip(b.schema, cols, strict=True)])
        out.append(pa.RecordBatch.from_arrays(cols, schema=schema))
    return out


def result_batches(result: Any) -> Any:  # noqa: ANN401 — a ResultStream
    """A result as the encoders read it, in the form it has (REQ-2195): the engine's Arrow record
    batches, or a row result's batches of rows (``RowBatches``, each column's engine type name
    with them), read by the encoder as they are and never built into Arrow."""
    if is_arrow(result):
        return arrow_batches(result.arrow_batches())
    kinds = None
    if result.column_types:
        kinds = [None if t is None else str(t) for t in result.column_types]
    return _encoders().RowBatches([b for b in result.batches() if b], kinds)


def any_rows(batches: Any) -> bool:  # noqa: ANN401 — result_batches' answer
    """Whether ``result_batches``' answer holds a row."""
    if isinstance(batches, list):
        return any(b.num_rows for b in batches)
    return len(batches) > 0


def warnings_json(warnings: list[dict] | None) -> bytes | None:
    """The request's statement warnings (``as_entries()``) as the JSON a document carries; None
    when nothing was raised."""
    from provisa.api.json_response import dumps

    return dumps(warnings) if warnings else None


# ------------------------------------------------------------------ GraphQL


class FieldAnswer:
    """A root field's answer: ``body`` the JSON of its value (its rows), ``rows`` how many,
    (None for an aggregate's, which is a map), ``joined`` each relationship's matched rows (by the root key holding it: a to-many's
    elements, a to-one's objects) and ``truncated`` the to-one paths that matched several rows
    for one parent (only the first row's object was answered)."""

    __slots__ = ("body", "rows", "joined", "truncated")

    def __init__(self, body: bytes, rows: int | None, joined: dict[str, int], truncated: list[str]):
        self.body = body
        self.rows = rows
        self.joined = joined
        self.truncated = truncated


def graphql_field(
    batches: Any, columns: list[ColumnRef], result_limit: int | None = None
) -> FieldAnswer:
    """A GraphQL root field's rows (REQ-047..REQ-050: one-to-many lists, many-to-one objects)."""
    body, rows, joined, truncated = _encoded(
        lambda: _encoders().graphql.encode_field(batches, plan_tuples(columns), result_limit)
    )
    return FieldAnswer(body, rows, joined, truncated)


def graphql_aggregate(batches: Any, columns: list[ColumnRef], agg_alias: str) -> bytes:
    """A GraphQL aggregate root field's answer (REQ-196, REQ-197): its aggregates under
    ``agg_alias`` and its nodes, when selected."""
    return _encoded(
        lambda: _encoders().graphql.encode_aggregate(batches, plan_tuples(columns), agg_alias)
    )


# ------------------------------------------------------------------ REST


def rest(
    batches: Any, columns: list[ColumnRef], agg_alias: str | None = None
) -> tuple[bytes, list[str]]:
    """The REST table endpoint's ``{"data": rows}`` (a read or a group-by), or ``{"data":
    aggregate}``; and the truncated to-one paths."""
    return _encoded(lambda: _encoders().rest.encode(batches, plan_tuples(columns), agg_alias))


def rest_csv(batches: Any, columns: list[ColumnRef]) -> tuple[bytes, list[str]]:
    """The REST table endpoint's ``text/csv`` answer; and the truncated to-one paths."""
    return _encoded(lambda: _encoders().rest.encode_csv(batches, plan_tuples(columns)))


# ------------------------------------------------------------------ JSON:API


def jsonapi(batches: Any, columns: list[ColumnRef], **document: Any) -> tuple[bytes, list[str]]:  # noqa: ANN401
    """A JSON:API table read's compound document (data, relationships, included, meta, links);
    ``document`` is what the router built for it (``jsonapi.encode``'s keywords). Each truncated
    to-one path is in ``meta.warnings``; the paths are returned for the response's header."""
    from provisa.executor.serialize import TO_ONE_TRUNCATED, TO_ONE_TRUNCATED_MESSAGE

    return _encoded(
        lambda: _encoders().jsonapi.encode(
            batches,
            plan_tuples(columns),
            truncation=(TO_ONE_TRUNCATED, TO_ONE_TRUNCATED_MESSAGE),
            **document,
        )
    )


def jsonapi_group_by(batches: Any, columns: list[ColumnRef], **names: Any) -> bytes:  # noqa: ANN401
    """JSON:API ``?groupBy=``: each group as a resource, its groupKey and nodes renamed."""
    return _encoded(
        lambda: _encoders().jsonapi.encode_group_by(batches, plan_tuples(columns), **names)
    )


def jsonapi_aggregate(batches: Any, columns: list[ColumnRef], agg_alias: str) -> bytes:
    """JSON:API ``?aggregate``: ``{"data": null, "meta": {"aggregate": {...}}}``."""
    return _encoded(
        lambda: _encoders().jsonapi.encode_aggregate(batches, plan_tuples(columns), agg_alias)
    )


# ------------------------------------------------------------------ OData


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


def odata(batches: Any, names: list[str], entity: tuple, **document: Any) -> bytes:  # noqa: ANN401
    """An OData entity set read's document: ``@odata.context``, ``@odata.count``,
    ``@Provisa.warnings``, ``value`` (each expansion with its own nextLink) and
    ``@odata.nextLink``."""
    return _encoded(lambda: _encoders().odata.encode(batches, names, entity, **document))


def odata_navigation(batches: Any, names: list[str], entity: tuple, **document: Any) -> bytes:  # noqa: ANN401
    """A navigation read's document (``set(key)/navigation``): the parent row's list, the page,
    as ``value``, with the request's warnings and the list's nextLink when it goes on."""
    return _encoded(lambda: _encoders().odata.encode_navigation(batches, names, entity, **document))
