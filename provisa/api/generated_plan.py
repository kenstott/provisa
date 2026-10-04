# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1f8a3c-7b24-4d96-8c0f-2a6d9e4b1c75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The compiled form of GraphQL text a surface synthesizes from its own request (REQ-1877).

REST and JSON:API build GraphQL text from a request's path and parameters and compile it. The
compiler writes every value of that text into the statement as a BOUND parameter (``$1``), so two
requests that differ only in their values — two ids, two pages — compile to the same statement
with different bound values. One plan is therefore kept per request SHAPE: the text with its
values taken out. A request of a kept shape binds its own values into the kept statement's
parameter list and skips the parse, the validation and the compile. Values are never written into
the statement, and no value is part of a key.

The plan is kept through ``pgwire.governed_plan`` — the store, key discipline (generation,
role) and invalidation every surface keeps a governed plan with — under its own stage, anchored
to the schema object the text was validated against. Governance of the compiled SQL stays the
compiled stage's own kept plan (``_govern_and_route_compiled``), which is keyed on that one
parameterized statement.

What makes a value safe to take out of the shape:

- only a literal whose reading cannot depend on its content is a value: a 32-bit integer, a float,
  a string with no quote, backslash or newline. Anything else (a larger integer, which the schema's
  ``Int`` rejects; a string the compiler writes into the statement as a ``TIMESTAMP`` literal)
  stays in the shape verbatim, so it is validated and compiled for itself;
- a shape is kept as one plan only after it is PROVEN value-independent: the text is compiled a
  second time with a different value in every position, and the two results must be the same
  statement, with every value found in the parameter list exactly where the other compile has
  its own. A shape that fails the proof (a filter on a virtual column, which the compiler decides
  at compile time; a value an API source takes as a native argument) is kept per text instead —
  each distinct request text compiled for itself.
"""

# Requirements: REQ-1877

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from typing import Any

from graphql import GraphQLError, GraphQLSchema

from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_types import CompilationContext, CompiledQuery
from provisa.compiler.sql_where import _ISO_DATE_RE
from provisa.pgwire.governed_plan import PlanSlot

_STAGE = "generated-graphql"
_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1

# A string with nothing that needs escaping, or a number that is not part of a name.
_LITERAL = re.compile(
    r'"[^"\\\n]*"'
    r"|(?<![A-Za-z0-9_.\"])-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?![A-Za-z0-9_.\"])"
)
# Stands where a value was, in a shape: NUL (never in a shape otherwise) + the value's kind.
_MARK = "\x00"

Value = int | float | str


def _value_of(token: str) -> Value | None:
    """The value a literal token carries, or None when it stays in the shape."""
    if token.startswith('"'):
        content = token[1:-1]
        # The compiler writes an ISO date into the statement as a TIMESTAMP literal.
        return None if _ISO_DATE_RE.match(content) else content
    if "." in token or "e" in token or "E" in token:
        return float(token)
    number = int(token)
    return number if _INT32_MIN <= number <= _INT32_MAX else None


def _kind(value: Value) -> str:
    return "s" if isinstance(value, str) else "i" if isinstance(value, int) else "f"


def _literal(value: Value) -> str:
    return f'"{value}"' if isinstance(value, str) else repr(value)


def request_shape(gql_query: str) -> tuple[tuple[str, ...], list[Value]]:
    """Split synthesized text into its shape — the text between its values, each gap tagged with
    the kind of value that stood there — and the values, in order."""
    if _MARK in gql_query:
        return (gql_query,), []
    parts: list[str] = []
    values: list[Value] = []
    pending = ""
    last = 0
    for match in _LITERAL.finditer(gql_query):
        value = _value_of(match.group())
        if value is None:
            continue
        parts.append(pending + gql_query[last : match.start()])
        pending = _MARK + _kind(value)
        values.append(value)
        last = match.end()
    parts.append(pending + gql_query[last:])
    return tuple(parts), values


def _text_with(shape: tuple[str, ...], values: list[Value]) -> str:
    """The shape's text with ``values`` written back where values stood."""
    out = [shape[0]]
    for part, value in zip(shape[1:], values):
        out.append(_literal(value))
        out.append(part[2:])  # past the mark and the kind
    return "".join(out)


def _other_values(values: list[Value], taken: list[Any]) -> list[Value]:
    """A value of the same kind for every position, different from the request's own, from each
    other, and from everything the compiled statements already bind."""
    used: list[Any] = [*taken, *values]

    def _unused(candidates: Any) -> Value:
        for candidate in candidates:
            if not any(type(u) is type(candidate) and u == candidate for u in used):
                used.append(candidate)
                return candidate
        raise AssertionError("unreachable: the candidate sequence is unbounded")

    def _ints(start: int) -> Any:
        step = 1 if start < _INT32_MAX - len(used) - 1 else -1
        k = 1
        while True:
            yield start + step * k
            k += 1

    def _floats(start: float) -> Any:
        k = 1
        while True:
            yield start + k + 0.5
            k += 1

    def _strings() -> Any:
        k = 0
        while True:
            yield f"zq{k}"
            k += 1

    others: list[Value] = []
    for value in values:
        if isinstance(value, str):
            others.append(_unused(_strings()))
        elif isinstance(value, int):
            others.append(_unused(_ints(value)))
        else:
            others.append(_unused(_floats(value)))
    return others


def _same(a: Any, b: Any) -> bool:
    return type(a) is type(b) and a == b


Slots = tuple[tuple[int, int], ...]  # (index in the parameter list, index in the values)


def _slots(
    own: list, other: list, values: list[Value], others: list[Value], seen: set[int]
) -> Slots | None:
    """Where each value sits in a parameter list, read off two compiles of one shape; None when
    the two lists do not differ exactly by the values."""
    if len(own) != len(other):
        return None
    slots: list[tuple[int, int]] = []
    for j, (a, b) in enumerate(zip(own, other)):
        i = next((i for i, probe in enumerate(others) if _same(b, probe)), None)
        if i is None:
            if not _same(a, b):
                return None
            continue
        if not _same(a, values[i]):
            return None
        slots.append((j, i))
        seen.add(i)
    return tuple(slots)


@dataclass(frozen=True)
class _ShapePlan:
    """One shape's compiled statements and where a request's values are bound in them."""

    compiled: tuple[CompiledQuery, ...]
    slots: tuple[Slots, ...]
    nodes_slots: tuple[Slots, ...]

    def bound(self, values: list[Value]) -> list[CompiledQuery]:
        out = []
        for cq, slots, nodes_slots in zip(self.compiled, self.slots, self.nodes_slots):
            params, nodes_params = list(cq.params), list(cq.nodes_params)
            for j, i in slots:
                params[j] = values[i]
            for j, i in nodes_slots:
                nodes_params[j] = values[i]
            out.append(dataclasses.replace(cq, params=params, nodes_params=nodes_params))
        return out


@dataclass(frozen=True)
class _PerText:
    """Kept under a shape that is not value-independent: its requests are kept per text."""


def _shape_plan(
    compiled: tuple[CompiledQuery, ...],
    shape: tuple[str, ...],
    values: list[Value],
    schema: GraphQLSchema,
    ctx: CompilationContext,
) -> _ShapePlan | None:
    """The shape's plan, or None when the shape is not proven value-independent."""
    if not values:
        no_slots: tuple[Slots, ...] = ((),) * len(compiled)
        return _ShapePlan(compiled, no_slots, no_slots)
    taken = [p for cq in compiled for p in (*cq.params, *cq.nodes_params)]
    others = _other_values(values, taken)
    try:
        twin = tuple(compile_query(parse_query(schema, _text_with(shape, others), ctx=ctx), ctx))
    except (GraphQLValidationError, GraphQLError, ValueError):
        # The schema or the compiler reads this shape differently for another value.
        return None
    if len(twin) != len(compiled):
        return None
    seen: set[int] = set()
    all_slots: list[Slots] = []
    all_nodes_slots: list[Slots] = []
    for cq, other in zip(compiled, twin):
        # Everything but the bound values must be identical: the statement, its columns, its
        # sources, the row limit applied after grouping, an API source's native arguments.
        if dataclasses.replace(other, params=cq.params, nodes_params=cq.nodes_params) != cq:
            return None
        slots = _slots(cq.params, other.params, values, others, seen)
        nodes_slots = _slots(cq.nodes_params, other.nodes_params, values, others, seen)
        if slots is None or nodes_slots is None:
            return None
        all_slots.append(slots)
        all_nodes_slots.append(nodes_slots)
    if len(seen) != len(values):
        return None  # a value that reaches no parameter was consumed at compile time
    return _ShapePlan(compiled, tuple(all_slots), tuple(all_nodes_slots))


def _copies(compiled: tuple[CompiledQuery, ...]) -> list[CompiledQuery]:
    return [
        dataclasses.replace(cq, params=list(cq.params), nodes_params=list(cq.nodes_params))
        for cq in compiled
    ]


def compile_generated_graphql(  # REQ-1877
    state: Any, role_id: str, schema: GraphQLSchema, ctx: CompilationContext, gql_query: str
) -> list[CompiledQuery]:
    """Parse, validate and compile synthesized GraphQL text — or bind the request's values into
    the plan kept for its shape (see the module docstring).

    Raises what ``parse_query`` raises for text the schema rejects (``GraphQLValidationError``, or
    graphql-core's ``GraphQLError`` for a syntax error) — the only failures a caller may answer
    with a 400. A rejected or empty compilation is not kept. Each caller gets its own
    bound-value lists.
    """
    shape, values = request_shape(gql_query)
    shape_slot = PlanSlot(state, _STAGE, role_id, "shape", shape, extra_anchors=(schema,))
    kept = shape_slot.cached()
    if isinstance(kept, _ShapePlan):
        return kept.bound(values)

    text_slot = None
    if isinstance(kept, _PerText):
        text_slot = PlanSlot(state, _STAGE, role_id, "text", gql_query, extra_anchors=(schema,))
        kept_text = text_slot.cached()
        if kept_text is not None:
            return _copies(kept_text)

    compiled = tuple(compile_query(parse_query(schema, gql_query, ctx=ctx), ctx))
    if not compiled:
        return []
    if text_slot is not None:
        text_slot.keep(compiled)
        return _copies(compiled)

    plan = _shape_plan(compiled, shape, values, schema, ctx)
    if plan is not None:
        shape_slot.keep(plan)
    else:
        shape_slot.keep(_PerText())
        PlanSlot(state, _STAGE, role_id, "text", gql_query, extra_anchors=(schema,)).keep(compiled)
    return _copies(compiled)
