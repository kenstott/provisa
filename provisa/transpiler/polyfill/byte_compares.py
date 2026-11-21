# Copyright (c) 2026 Kenneth Stott
# Canary: 09f5d591-abba-49bc-9c81-d44728965e29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""btree's byte-order comparisons on ClickHouse (REQ-2236; the function support measure's edge
values, REQ-2171): each answers the value PostgreSQL's source computes, not only its sign.

memcmp's: bttext_pattern_cmp, btbpchar_pattern_cmp (its trailing spaces are padding, bcTruelen),
btnametextcmp, bttextnamecmp, gin_cmp_tslexeme, gin_compare_jsonb and byteacmp compare the bytes
the two hold in common (memcmp), answering the difference of the first pair that differs as
unsigned bytes ('日' against 'a' is 230 - 97 = 133); where none differs the shorter is first, -1 or
1, and equal values 0.

strncmp's: btnamecmp compares two names in the C collation by strncmp, where the shorter's end is a
zero byte ('' against 'abc' is 0 - 97 = -97).

Every other dialect keeps the old table's forms (provisa.transpiler.pg_forms_index), which answer
the sign alone."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.numbers import ch_once, ch_only

FAMILY = "index support"


def _byte(value: str, at: str) -> str:
    return f"toInt32(reinterpretAsUInt8(substring({value}, {at}, 1)))"


def _first_difference(a: str, b: str, length: str) -> str:
    """The difference of the first pair of bytes of ``a`` and ``b`` that differs among the first
    ``length``, 0 where none does; the place is read once."""
    place = f"arrayFirstIndex(_i -> substring({a}, _i, 1) != substring({b}, _i, 1), range(1, {length} + 1))"
    return f"arrayElement(arrayMap(_k -> if(_k = 0, 0, {_byte(a, '_k')} - {_byte(b, '_k')}), [{place}]), 1)"


def _memcmp(a: str, b: str) -> str:
    common = f"least(length({a}), length({b}))"
    return (
        f"arrayElement(arrayMap(_d -> if(_d != 0, _d, if(length({a}) < length({b}), -1, "
        f"if(length({a}) > length({b}), 1, 0))), [{_first_difference(a, b, common)}]), 1)"
    )


def _strncmp(a: str, b: str) -> str:
    ended_a, ended_b = f"concat({a}, '\\0')", f"concat({b}, '\\0')"
    common = f"least(length({ended_a}), length({ended_b}))"
    return _first_difference(ended_a, ended_b, common)


def _compared(answer: str) -> str:
    return ch_once(f"CAST(if($a IS NULL OR $b IS NULL, NULL, {answer}) AS Nullable(Int32))")


#: bcTruelen: a character value's trailing spaces are padding.
def _unpadded(hole: str) -> str:
    return f"replaceRegexpOne({hole}, ' +$', '')"


def _feature(name: str, summary: str, answer: str) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(rules=(Rule(f"{name}($a, $b)", ch_only(_compared(answer))),)),
    )


_MEMCMP = _memcmp("$a", "$b")

COMPARISONS: tuple[Feature, ...] = (
    _feature(
        "btnamecmp",
        "btree's comparison of two names: strncmp's difference of their first differing bytes.",
        _strncmp("$a", "$b"),
    ),
    *(
        _feature(name, f"{summary}: memcmp's difference of their first differing bytes.", _MEMCMP)
        for name, summary in (
            ("btnametextcmp", "btree's comparison of a name and a text"),
            ("bttextnamecmp", "btree's comparison of a text and a name"),
            ("bttext_pattern_cmp", "btree's byte-order comparison of two texts"),
            ("gin_cmp_tslexeme", "GIN's comparison of two lexemes"),
            ("gin_compare_jsonb", "GIN's comparison of two jsonb keys' texts"),
            ("byteacmp", "btree's comparison of two byte strings"),
        )
    ),
    _feature(
        "btbpchar_pattern_cmp",
        "btree's byte-order comparison of two character strings, trailing spaces as padding: "
        "memcmp's difference of their first differing bytes.",
        _memcmp(_unpadded("$a"), _unpadded("$b")),
    ),
)

FEATURES: tuple[Feature, ...] = COMPARISONS
