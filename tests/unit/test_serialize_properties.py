# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for result serialization (REQ-047, REQ-048, REQ-049, REQ-050).

serialize_rows turns flat SQL rows into the nested GraphQL JSON every query result
is shaped by. A bug here corrupts EVERY response — dropped/duplicated rows, missing
fields, wrong values, children under the wrong parent — with no error. Two contracts:

Flat / many-to-one (_serialize_flat):
  * the envelope is exactly {"data": {root_field: [...]}};
  * each object carries exactly the projected field names;
  * rows identical across all root columns collapse to one (dedup), and the count
    equals the distinct-row count, capped by result_limit;
  * each surviving object's values are the first occurrence's values (fidelity).

One-to-many absorption (_serialize_with_one_to_many):
  * a {root: [children]} model flattened to joined rows round-trips to exactly one
    object per root with its children nested — no child dropped, duplicated, or
    attributed to the wrong root (exact-equality oracle).
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from provisa.compiler.sql_types import ColumnRef
from provisa.executor.serialize import _convert_value, _to_hashable, serialize_rows

_ROOT = "items"
# Scalar values for which _convert_value is identity, so fidelity is exact.
_VALUES = st.one_of(st.integers(-100, 100), st.text(alphabet="abcde", max_size=3), st.none())


@st.composite
def _flat_case(draw):
    """A flat result set: n root columns (no nesting), a list of positional rows,
    and an optional result_limit. Returns (rows, columns, field_names, limit)."""
    n = draw(st.integers(min_value=1, max_value=4))
    fields = [f"f{i}" for i in range(n)]
    columns = [ColumnRef(alias="t0", column=f, field_name=f, nested_in=None) for f in fields]
    rows = draw(st.lists(st.tuples(*([_VALUES] * n)), max_size=10))
    limit = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=12)))
    return rows, columns, fields, limit


def _distinct_first(rows):
    """First occurrence of each row that is distinct across all columns — the dedup
    the flat serializer performs (root_key = every root column value)."""
    seen: set = set()
    out = []
    for r in rows:
        key = tuple(_to_hashable(_convert_value(v)) for v in r)
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


@settings(max_examples=300, deadline=None)
@given(case=_flat_case())
def test_flat_serialization_contract(case) -> None:
    rows, columns, fields, limit = case
    out = serialize_rows(rows, columns, _ROOT, result_limit=limit)

    # Envelope shape.
    assert set(out) == {"data"}
    assert set(out["data"]) == {_ROOT}
    data = out["data"][_ROOT]
    assert isinstance(data, list)

    # Every object carries exactly the projected fields.
    for obj in data:
        assert set(obj) == set(fields)

    # Dedup + limit: count matches distinct rows, capped by the limit.
    distinct = _distinct_first(rows)
    expected = distinct if limit is None else distinct[:limit]
    assert len(data) == len(expected), "row count diverged from distinct-then-limit"

    # Fidelity: each surviving object holds the first-occurrence values.
    for obj, row in zip(data, expected):
        for idx, fname in enumerate(fields):
            assert obj[fname] == _convert_value(row[idx])


# --------------------------------------------------------------------------- #
# One-to-many absorption: flat joined rows -> nested child arrays.
# --------------------------------------------------------------------------- #
_O2M_COLUMNS = [
    ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
    ColumnRef(alias="t0", column="name", field_name="name", nested_in=None),
    ColumnRef(
        alias="t1", column="cid", field_name="cid", nested_in="children", cardinality="one-to-many"
    ),
    ColumnRef(
        alias="t1",
        column="cval",
        field_name="cval",
        nested_in="children",
        cardinality="one-to-many",
    ),
]


@st.composite
def _one_to_many_case(draw):
    """A {root: [children]} model flattened to the joined rows SQL would emit (root
    repeated once per child). Returns (rows, expected_items) — an exact oracle."""
    roots = draw(
        st.lists(
            st.tuples(st.integers(0, 20), st.text(alphabet="abc", max_size=2)),
            min_size=1,
            max_size=4,
            unique_by=lambda r: r[0],  # distinct root key
        )
    )
    rows = []
    expected = []
    for rid, rname in roots:
        children = draw(
            st.lists(
                st.tuples(st.integers(0, 50), st.text(alphabet="xyz", max_size=2)),
                min_size=1,
                max_size=3,
                unique=True,
            )
        )
        for cid, cval in children:
            rows.append((rid, rname, cid, cval))
        expected.append(
            {
                "id": rid,
                "name": rname,
                "children": [{"cid": cid, "cval": cval} for cid, cval in children],
            }
        )
    return rows, expected


@settings(max_examples=300, deadline=None)
@given(case=_one_to_many_case())
def test_one_to_many_absorbs_children_exactly(case) -> None:
    """Joined rows collapse to one object per root with its children nested — no child
    dropped, duplicated, or attributed to the wrong root."""
    rows, expected = case
    out = serialize_rows(rows, _O2M_COLUMNS, _ROOT)
    assert out["data"][_ROOT] == expected
