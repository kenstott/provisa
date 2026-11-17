# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1e8a37-2c4b-4f90-b6a3-9e7f0c1d2b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Text functions ClickHouse had no form of (REQ-2236; the function support measure, REQ-2171), each
written in ClickHouse's own functions after the form DuckDB answers it with.

btrim, ltrim and rtrim of a set of characters: ClickHouse's trimBoth reads the set as bytes, and cut
a multi-byte character in half. Here the set is a bracket expression of its characters, each escaped,
matched at the start or end of the text (ClickHouse's regular expressions read UTF-8); an empty set
trims nothing.

translate: each character of the text found in the second argument is replaced by the character at
its position in the third, or dropped where the third is shorter; the first position of a repeated
character wins. ClickHouse's translateUTF8 refuses arguments of different lengths, so each character
is mapped here.

overlay: the text before the start, the replacement, then the text from the start past the count
(the replacement's length, where no count is given). A start before 1 is PostgreSQL's 22011, and a
start and count past an integer its 22003.

string_to_array: the text's pieces between each delimiter, its characters for a NULL delimiter, the
whole text for an empty one, none of an empty text, and each piece equal to the third argument NULL.
ClickHouse holds no NULL of an array: a NULL literal text answers NULL, and a NULL text that is no
literal raises 0A000."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "text functions"

_CH = "clickhouse"


def _ch(form: str | Rewrite) -> dict[str, str | Rewrite]:
    """ClickHouse's ``form``, every other dialect keeping what it had (the old table's form)."""
    return {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, _CH: form}


#: The characters of a set, each escaped for a bracket expression.
_SET = "replaceRegexpAll($c, '([\\\\\\\\\\\\]\\\\[^-])', '\\\\\\\\\\\\1')"


def _trimmed(pattern: str) -> str:
    """The text with what ``pattern`` (of ``{s}``, the escaped set) matches removed. An empty set
    trims nothing: its pattern is one that matches nothing (a character before the start), as
    ClickHouse reads every branch of a condition and an empty bracket expression is its error."""
    return f"replaceRegexpAll($t, if($c = '', 'a^', {pattern.format(s=_SET)}), '')"


_TRIMS = {
    "btrim": (
        "Text without the characters of a set at its start and end.",
        "concat('^[', {s}, ']+|[', {s}, ']+$')",
    ),
    "ltrim": ("Text without the characters of a set at its start.", "concat('^[', {s}, ']+')"),
    "rtrim": ("Text without the characters of a set at its end.", "concat('[', {s}, ']+$')"),
}

TRIMS: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        summary,
        read=Side(rules=(Rule(f"{name}($t::text, $c::text)", _ch(_trimmed(pattern))),)),
    )
    for name, (summary, pattern) in _TRIMS.items()
)

#: A text's characters (ClickHouse's regular expressions read UTF-8; (?s) takes a newline too).
_CHARACTERS = "extractAll(assumeNotNull({}), '(?s).')"
_AT = "indexOf(" + _CHARACTERS.format("$f") + ", _c)"
_TRANSLATED = (
    "if($t IS NULL OR $f IS NULL OR $r IS NULL, NULL, arrayStringConcat(arrayMap(_c -> "
    f"if({_AT} = 0, _c, if({_AT} <= lengthUTF8(assumeNotNull($r)), "
    f"{_CHARACTERS.format('$r')}[{_AT}], '')), {_CHARACTERS.format('$t')})))"
)

TRANSLATE = Feature(
    "translate",
    FAMILY,
    "Each character found in a set replaced by the one at its place in another, or dropped.",
    read=Side(rules=(Rule("translate($t::text, $f::text, $r::text)", _ch(_TRANSLATED)),)),
)


def _checked(raises: list[str], answer: str) -> str:
    """``answer`` where none of ``raises`` (ch_fail's, each 0 where it does not raise) raises:
    ClickHouse reads every branch of a condition, so each raise stands beside the answer."""
    return f"if({' + '.join(raises)} = 0, {answer}, NULL)"


_START = "toInt64($p)"
_NEGATIVE = ch_fail("22011", "negative substring length not allowed")
_PAST_INTEGER = ch_fail("22003", "integer out of range")


def _overlaid(count: str) -> str:
    end = f"{_START} + toInt64({count})"
    return _checked(
        [
            _NEGATIVE.format(condition=f"{_START} < 1"),
            _PAST_INTEGER.format(
                condition=f"{_START} >= 1 AND ({end} > 2147483647 OR {end} < -2147483648)"
            ),
        ],
        f"concat(substringUTF8($t, 1, {_START} - 1), $r, substringUTF8($t, greatest({end}, 1)))",
    )


OVERLAY = Feature(
    "overlay",
    FAMILY,
    "Text with a replacement put in place of a count of its characters from a start.",
    read=Side(
        rules=(
            Rule(
                "overlay($t::text, $r::text, $p::integer, $n::integer)",
                _ch(_overlaid("$n")),
            ),
            Rule("overlay($t::text, $r::text, $p::integer)", _ch(_overlaid("lengthUTF8($r)"))),
        )
    ),
)


def _is_null(value: exp.Expr) -> bool:
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return isinstance(node, exp.Null)


def _constant(value: exp.Expr) -> str | None:
    """The text a statement writes, through casts of it; None for any other value."""
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return node.this if isinstance(node, exp.Literal) and node.is_string else None


_NULL_TEXT = ch_fail(
    "0A000", "string_to_array() of a NULL text that is no literal has no form on clickhouse"
)
_TEXT = "ifNull($s, '')"
_PIECES = (
    f"if({_TEXT} = '', CAST([] AS Array(String)), if($d IS NULL, "
    f"{_CHARACTERS.format('$s')}, if($d = '', [{_TEXT}], "
    f"splitByString(if(ifNull($d, '') = '', ' ', assumeNotNull($d)), {_TEXT}))))"
)


def _pieces(nulled: bool) -> str:
    pieces = f"arrayMap(_x -> if(_x = $n, NULL, toNullable(_x)), {_PIECES})" if nulled else _PIECES
    return f"if({_NULL_TEXT.format(condition='$s IS NULL')} = 0, {pieces}, {pieces})"


def _to_array(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """string_to_array: NULL of a NULL literal (ClickHouse holds no NULL of an array); else the
    text's pieces between each delimiter, its characters for a NULL delimiter, the whole text for
    an empty one, and none of an empty text; each piece equal to the third argument NULL."""
    call = trino.called(node)
    if call is None or len(call[1]) not in (2, 3):
        return None
    args = call[1]
    if _is_null(args[0]):
        return exp.null()
    holes = ("s", "d", "n")[: len(args)]
    pattern = f"string_to_array({', '.join('$' + h for h in holes)})"
    form = replacement(_pieces(len(args) == 3), dialect, pattern)
    return graft(form, dict(zip(holes, args, strict=True)))


STRING_TO_ARRAY = Feature(
    "string_to_array",
    FAMILY,
    "Text split at each delimiter into an array; a piece equal to a third argument is NULL.",
    read=Side(
        rules=(
            Rule("string_to_array($s::text, $d::text)", _ch(Rewrite(_to_array))),
            Rule("string_to_array($s::text, $d::text, $n::text)", _ch(Rewrite(_to_array))),
        ),
        limitations={
            _CH: (
                Limitation(
                    "A NULL text that is no literal raises 0A000: ClickHouse holds no NULL of an "
                    "array."
                ),
            )
        },
    ),
)

_ANY_NULL = "$s IS NULL OR $m IS NULL OR $e IS NULL"
_MOST = "(toInt64($m) - 4)"
_LONG = f"$m >= 4 AND lengthUTF8($s) > {_MOST}"
_CLIPPED = f"substringUTF8($s, 1, greatest({_MOST}, 0))"
_ONLY_SPACES = f"match(substringUTF8($s, greatest({_MOST}, 0) + 1), '^ *$')"


def _too_long(type_name: str) -> str:
    return ch_fail("22001", f"value too long for type {type_name}").format(
        condition=f"{_LONG} AND NOT $e AND NOT {_ONLY_SPACES}"
    )


_VARCHAR = _checked(
    [_too_long("character varying")], f"if({_ANY_NULL}, NULL, if({_LONG}, {_CLIPPED}, $s))"
)

#: The most characters a bpchar is padded to here: PostgreSQL's longest declared length.
_MOST_PADDED = 10485760
#: PostgreSQL's most bytes of one allocation (MaxAllocSize), past which it raises XX000.
_MAX_ALLOC = 1073741823
_SHORT = f"$m >= 4 AND lengthUTF8($s) < {_MOST}"
_PAST_ALLOC = f"{_SHORT} AND {_MOST} + (length($s) - lengthUTF8($s)) + 4 > {_MAX_ALLOC}"
_PADDING = f"toUInt64(if({_MOST} > {_MOST_PADDED}, 0, greatest({_MOST} - lengthUTF8($s), 0)))"
_BPCHAR = _checked(
    [
        _too_long("character"),
        ch_fail("XX000", "invalid memory alloc request size").format(condition=_PAST_ALLOC),
        ch_fail(
            "0A000", f"bpchar() of a length past {_MOST_PADDED} has no form on clickhouse"
        ).format(condition=f"{_SHORT} AND {_MOST} > {_MOST_PADDED} AND NOT ({_PAST_ALLOC})"),
    ],
    f"if({_ANY_NULL}, NULL, if($m < 4, $s, if({_LONG}, {_CLIPPED}, "
    f"concat($s, repeat(' ', {_PADDING})))))",
)


def _held_character(value: exp.Expr) -> exp.Expr:
    """A text literal cast to character(n) as PostgreSQL holds it: its first n characters, padded
    with spaces to n (n is 1 where none is said). ClickHouse's cast to its String keeps the text
    as written. Any other value is as it stands."""
    node = value.unnest()
    if not (isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.CHAR)):
        return value
    text = _constant(node.this)
    if text is None:
        return value
    params = node.to.expressions
    length = int(params[0].this.this) if params else 1
    return exp.Literal.string(text[:length].ljust(length))


def _bpchar(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """bpchar of a text, a typmod and whether the cast is explicit, its character literal held as
    PostgreSQL holds it (:func:`_held_character`)."""
    call = trino.called(node)
    if call is None or len(call[1]) != 3:
        return None
    text, typmod, explicit = call[1]
    form = replacement(_BPCHAR, dialect, "bpchar($s, $m, $e)")
    return graft(form, {"s": _held_character(text), "m": typmod, "e": explicit})


LENGTH_CASTS: tuple[Feature, ...] = (
    Feature(
        "varchar",
        FAMILY,
        "Text cast to character varying of a declared length (its typmod).",
        read=Side(
            rules=(
                Rule("varchar($s, $m, $e)", _ch(_VARCHAR)),
                Rule("varchar($s::name)", _ch("CAST($s AS Nullable(String))")),
            )
        ),
    ),
    Feature(
        "bpchar",
        FAMILY,
        "Text cast to character of a declared length (its typmod), padded with spaces.",
        read=Side(
            rules=(Rule("bpchar($s, $m, $e)", _ch(Rewrite(_bpchar))),),
            limitations={
                _CH: (
                    Limitation(
                        f"A length past {_MOST_PADDED} characters that pads the text raises "
                        "0A000; past PostgreSQL's allocation it raises XX000, as PostgreSQL does."
                    ),
                )
            },
        ),
    ),
)

#: The most bytes of a name (NAMEDATALEN - 1).
_NAME_BYTES = 63
_SUFFIX = "concat('_', toString($o))"
_NAME_CHARS = _CHARACTERS.format("$n")
NAMECONCATOID = Feature(
    "nameconcatoid",
    FAMILY,
    "A name and an oid joined by an underscore, the name cut to fit a name's 63 bytes.",
    read=Side(
        rules=(
            Rule(
                "nameconcatoid($n, $o)",
                _ch(
                    f"if($n IS NULL OR $o IS NULL, NULL, concat(arrayStringConcat("
                    f"arrayFilter((_c, _b) -> _b <= {_NAME_BYTES} - length({_SUFFIX}), {_NAME_CHARS}, "
                    f"arrayCumSum(arrayMap(_c -> toInt64(length(_c)), {_NAME_CHARS})))), {_SUFFIX}))"
                ),
            ),
        )
    ),
)

FEATURES: tuple[Feature, ...] = (
    *TRIMS,
    TRANSLATE,
    OVERLAY,
    STRING_TO_ARRAY,
    *LENGTH_CASTS,
    NAMECONCATOID,
)
