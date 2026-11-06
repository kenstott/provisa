# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c6a7e-5d21-4b8e-9a44-71e2c8b06d13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's hash functions as forms (REQ-2171): hash_bytes (lookup3, src/common/hashfn.c) and
each type's normalization (src/backend/access/hash/hashfunc.c), so hashtext('x') is PostgreSQL's
number on an engine that has no such function.

lookup3 is a loop over a value's length, and PostgreSQL's syntax has no loop an expression can
hold, so this is the one family of forms written per engine: each engine with a fold
(:class:`Engine`) runs one definition of the algorithm in BIGINT, every word masked to 32 bits.
sqlglot cannot render the bit operators for all of them (ClickHouse has no ``a ^ b`` that is an
XOR), so they are the engine's own functions. An engine with no entry refuses the call by name.

A value reaches ``_hashed`` as hexadecimal, its bytes as PostgreSQL's hash reads them on a
little-endian server, or as a number that is its one word. ``mix`` and ``final`` are one line each
with the variables' roles rotating, so a step of the fold is one line, and the fold is the only
loop. An extended hash's seed is its first block, as hash_bytes_extended mixes it."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

from provisa.core.pg_range import MULTIRANGE_TYPES, RANGE_TYPES
from provisa.transpiler.pg_forms_types import REFUSED, refused

_MASK = 4294967295
_TWO32 = 4294967296
_SIGN = 2147483648
#: 0x9e3779b9 + 3923095, hash_bytes's start before the length.
_START = 2658358864
_MIX = (0, 4, 6, 8, 16, 19, 4)
_FINAL = (14, 11, 25, 16, 4, 14, 24)
_MIN_BIGINT = "-9223372036854775808"
_MAX_BIGINT = "9223372036854775807"
#: A float8's bytes, big-endian, where its value does not say them.
_INFINITY = "7FF0000000000000"
_MINUS_INFINITY = "FFF0000000000000"
_NAN = "7FF8000000000000"
_NAME_BYTES = 63
_ZERO = "'0'"


def _atomic(operand: object) -> bool:
    """Whether an operand holds no operator outside its parentheses."""
    text, depth = str(operand), 0
    for char in text:
        depth += (char == "(") - (char == ")")
        if char == " " and depth == 0:
            return False
    return True


class _Infix(str):
    """An operator, which parenthesizes only what needs it: a form cannot nest without limit."""

    __slots__ = ()

    def format(self, *operands: object, **_: object) -> str:
        return str.format(self, *(o if _atomic(o) else f"({o})" for o in operands))


@dataclass(frozen=True)
class Engine:
    """An engine's spelling of what a form needs: ``{}`` for each operand."""

    xor: str
    both: str  # bitwise and
    left: str
    right: str
    quotient: str  # whole-number division
    array: str  # the items, comma separated
    size: str  # a list's length
    concat: str  # two lists
    zero_to: str  # the BIGINTs 0 .. n - 1, n >= 1
    let: str  # name, value, body
    map: str  # list, name, body
    fold: str  # list, state, item, body, start
    item: str  # the list a fold reads
    item_of: str  # a fold's item as its number
    hex_word: str  # eight hexadecimal digits, as their number
    word_hex: str  # a number below 2^32, as eight hexadecimal digits
    text_hex: str  # a text's UTF-8 bytes in hexadecimal
    text: str  # a value's text
    length: str  # a text's length in bytes
    chars: str  # a text's length in characters
    part: str  # text, separator, which
    split: str  # text, separator: a list of texts
    join: str  # a list of texts, into one
    rpad: str  # text, size, fill
    lpad: str  # text, size, fill
    ltrim: str  # text, characters
    rtrim: str  # text, characters
    is_nan: str
    is_infinite: str
    float_hex: str  # a finite float8's bytes, big-endian
    not_hex: str  # text with what is no hexadecimal digit removed
    has: str  # a list of texts, whether it holds a text
    ascii: str  # the byte of a one-byte text
    int4_type: str = "INTEGER"
    int8_type: str = "BIGINT"
    number: str = "CAST({} AS BIGINT)"  # a text's number, once a CASE has chosen to read it
    empty_texts: str = "CAST([] AS VARCHAR[])"
    backslash: str = "'\\'"
    big_list: str = "{}"  # a list, of BIGINT
    value: str = "value"  # the argument, once a CASE has answered its NULL
    seed: str = "CAST(seed AS BIGINT)"
    big: str = "CAST({} AS BIGINT)"
    int4: str = "CAST({} AS INTEGER)"
    float8: str = "CAST({} AS DOUBLE)"


#: DuckDB shows no float's bytes, but %a writes them: a normalized mantissa and its exponent, which
#: a subnormal's bytes hold shifted.
_DUCKDB_EXPONENT = "CAST(regexp_extract(_a, 'p([-+][0-9]+)', 1) AS BIGINT)"
_DUCKDB_MANTISSA = "CAST('0x' || rpad(regexp_extract(_a, '[.]([0-9a-f]+)', 1), 13, '0') AS BIGINT)"
_DUCKDB_FLOAT = (
    "list_transform([format('{{:a}}', abs({0}))], LAMBDA _a : "
    f"CASE WHEN {_DUCKDB_EXPONENT} >= -1022 THEN "
    f"printf('%03x%013x', CASE WHEN {{0}} < 0 THEN 2048 ELSE 0 END + {_DUCKDB_EXPONENT} + 1023, "
    f"{_DUCKDB_MANTISSA}) "
    "ELSE printf('%03x%013x', CASE WHEN {0} < 0 THEN 2048 ELSE 0 END, "
    f"(4503599627370496 + {_DUCKDB_MANTISSA}) >> (-1022 - {_DUCKDB_EXPONENT})) END)[1]"
)

DUCKDB = Engine(
    xor="xor({}, {})",
    both=_Infix("({} & {})"),
    left=_Infix("({} << {})"),
    right=_Infix("({} >> {})"),
    quotient=_Infix("({} // {})"),
    array="[{}]",
    size="len({})",
    concat="list_concat({}, {})",
    zero_to="range(CAST({} AS BIGINT))",
    let="list_transform([{1}], LAMBDA {0} : {2})[1]",
    map="list_transform({0}, LAMBDA {1} : {2})",
    # list_reduce's start has the list's item type, so an item is a list of one number.
    fold="list_reduce({0}, LAMBDA {1}, {2} : {3}, {4})",
    item="list_transform({}, LAMBDA _n : [_n])",
    item_of="{}[1]",
    hex_word="CAST('0x' || {} AS BIGINT)",
    word_hex="printf('%08x', {})",
    text_hex="hex(encode({}))",
    text="CAST({} AS VARCHAR)",
    length="strlen({})",
    chars="length({})",
    part="split_part({}, {}, {})",
    split="string_split({}, {})",
    join="array_to_string({}, '')",
    rpad="rpad({}, CAST({} AS INTEGER), {})",
    lpad="lpad({}, CAST({} AS INTEGER), {})",
    ltrim="ltrim({}, {})",
    rtrim="rtrim({}, {})",
    is_nan="isnan({})",
    is_infinite="isinf({})",
    float_hex=_DUCKDB_FLOAT,
    not_hex="regexp_replace(lower({}), '[^0-9a-f]', '', 'g')",
    has="list_contains({}, {})",
    ascii="ascii({})",
)

TRINO = Engine(
    xor="bitwise_xor({}, {})",
    both="bitwise_and({}, {})",
    left="bitwise_left_shift({}, {})",
    right="bitwise_right_shift({}, {})",
    quotient=_Infix("({} / {})"),
    array="ARRAY[{}]",
    size="cardinality({})",
    concat="concat({}, {})",
    zero_to="sequence(CAST(0 AS BIGINT), CAST({} AS BIGINT) - 1)",
    let="transform(ARRAY[{1}], {0} -> {2})[1]",
    map="transform({0}, {1} -> {2})",
    fold="reduce({0}, {4}, ({1}, {2}) -> {3}, _f -> _f)",
    item="{}",
    item_of="{}",
    hex_word="from_base({}, 16)",
    word_hex="lpad(to_base({}, 16), 8, '0')",
    text_hex="to_hex(to_utf8({}))",
    text="CAST({} AS VARCHAR)",
    length="length({})",
    chars="length({})",
    part="coalesce(split_part({}, {}, {}), '')",
    split="split({}, {})",
    join="array_join({}, '')",
    rpad="rpad({}, {}, {})",
    lpad="lpad({}, {}, {})",
    ltrim="ltrim({}, {})",
    rtrim="rtrim({}, {})",
    is_nan="is_nan({})",
    is_infinite="is_infinite({})",
    float_hex="to_hex(to_ieee754_64({}))",
    not_hex="regexp_replace(lower({}), '[^0-9a-f]', '')",
    has="contains({}, {})",
    ascii="codepoint(CAST({} AS VARCHAR(1)))",
    empty_texts="CAST(ARRAY[] AS ARRAY(VARCHAR))",
)

CLICKHOUSE = Engine(
    xor="bitXor({}, {})",
    both="bitAnd({}, {})",
    left="bitShiftLeft({}, {})",
    right="bitShiftRight({}, {})",
    quotient="intDiv({}, {})",
    array="[{}]",
    size="length({})",
    concat="arrayConcat({}, {})",
    zero_to="arrayMap(_n -> toInt64(_n), range(toUInt64({})))",
    let="arrayMap({0} -> {2}, [{1}])[1]",
    map="arrayMap({1} -> {2}, {0})",
    fold="arrayFold(({1}, {2}) -> {3}, {0}, {4})",
    item="{}",
    item_of="{}",
    hex_word="reinterpretAsInt64(reverse(unhex(concat('00000000', {}))))",
    # hex of a number leaves out its leading zero bytes.
    word_hex="leftPad(hex(toUInt32({})), 8, '0')",
    text_hex="hex({})",
    text="toString({})",
    length="length({})",
    chars="lengthUTF8({})",
    part="splitByString({1}, {0})[{2}]",
    split="splitByString({1}, {0})",
    join="arrayStringConcat({})",
    rpad="rightPad({}, toUInt64({}), {})",
    lpad="leftPad({}, toUInt64({}), {})",
    ltrim="trimLeft({}, {})",
    rtrim="trimRight({}, {})",
    is_nan="isNaN({})",
    is_infinite="isInfinite({})",
    float_hex="leftPad(hex(reverse(reinterpretAsString(toFloat64({})))), 16, '0')",
    not_hex="replaceRegexpAll(lower({}), '[^0-9a-f]', '')",
    has="has({}, {})",
    ascii="reinterpretAsUInt8({})",
    empty_texts="emptyArrayString()",
    backslash="'\\\\'",
    int4_type="Nullable(Int32)",
    int8_type="Nullable(Int64)",
    # The form's CASE has answered a NULL, and ClickHouse keeps no bytes of a Nullable.
    number="toInt64OrZero({})",
    value="assumeNotNull(value)",
    seed="toInt64(assumeNotNull(seed))",
    big="toInt64({})",
    int4="toInt32({})",
    float8="toFloat64({})",
)


def _refusing(condition: str, function: str, answer: str) -> str:
    """``answer``, or the refusal by name of a value whose PostgreSQL answer a form cannot read."""
    message = refused("0A000", repr(f"{function} {REFUSED}"), "BIGINT")
    return f"CASE WHEN {condition} THEN {message} ELSE {answer} END"


def _masked(e: Engine, value: str) -> str:
    return e.big.format(e.both.format(value, e.big.format(_MASK)))


def _rotated(e: Engine, value: str, bits: str) -> str:
    """rot(value, bits): the high bits are cut before the shift, which DuckDB's would refuse."""
    one = e.big.format(1)
    kept = f"({e.left.format(one, f'32 - {bits}')} - 1)"
    low = e.left.format(e.both.format(value, kept), bits)
    return f"({low} + {e.right.format(value, f'32 - {bits}')})"


def _numbers(e: Engine, values: tuple[int, ...]) -> str:
    return e.array.format(", ".join(str(v) for v in values))


def _step(e: Engine, i: str, blocks: str) -> str:
    """One step of the fold, of state ``_s``: a block's words, or a line of mix or of final."""
    a, b, c = "_s[1]", "_s[2]", "_s[3]"
    block = e.quotient.format(i, 7)
    add = e.array.format(
        ", ".join(
            _masked(e, f"{word} + _w[3 * {block} + {at}]") for at, word in enumerate((a, b, c), 1)
        )
    )
    mix_bits = f"{_numbers(e, _MIX)}[{i} % 7 + 1]"
    mixed = e.array.format(
        f"{b}, {_masked(e, f'{c} + {b}')}, "
        + e.big.format(e.xor.format(_masked(e, f"{a} - {c} + {_TWO32}"), _rotated(e, c, mix_bits)))
    )
    final_bits = f"{_numbers(e, _FINAL)}[{i} - 7 * {blocks}]"
    final = e.array.format(
        f"{b}, "
        + _masked(e, f"{e.xor.format(c, b)} - {_rotated(e, b, final_bits)} + {_TWO32}")
        + f", {a}"
    )
    return f"CASE WHEN {i} > 7 * {blocks} THEN {final} WHEN {i} % 7 = 0 THEN {add} ELSE {mixed} END"


def _signed(e: Engine, word: str) -> str:
    return f"({e.big.format(e.xor.format(word, e.big.format(_SIGN)))} - {_SIGN})"


def _hashed(e: Engine, words: str, length: str, seed: str | None, *then: str) -> str:
    """hash_bytes of ``words`` (BIGINT[], three to a block, the last the tail), ``length`` bytes
    long: its BIGINT, signed, xor each of ``then``. With ``seed``, hash_bytes_extended's. The
    fold's last step is the answer, since a form cannot nest as deep as a name for it would."""
    if seed is not None:
        block = e.array.format(
            f"{_masked(e, e.right.format(seed, 32))}, {_masked(e, seed)}, {e.big.format(0)}"
        )
        words = f"CASE WHEN {seed} = 0 THEN {words} ELSE {e.concat.format(block, words)} END"
    blocks = f"({e.quotient.format(e.size.format('_w'), 3)} - 1)"
    start = _masked(e, f"{_START} + {length}")
    # Seven lines leave the roles rotated once: the state is b, c, a.
    answer = _signed(e, "_s[2]") if seed is None else f"{_signed(e, '_s[1]')} * {_TWO32} + _s[2]"
    for value in then:
        answer = e.xor.format(answer, value)
    i = e.item_of.format("_i")
    done = e.array.format(f"{e.big.format(answer)}, {e.big.format(0)}, {e.big.format(0)}")
    step = f"CASE WHEN {i} = 7 * {blocks} + 8 THEN {done} ELSE {_step(e, i, blocks)} END"
    steps = e.item.format(e.zero_to.format(f"7 * {blocks} + 9"))
    state = e.array.format(f"{start}, {start}, {start}")
    fold = e.fold.format(steps, "_s", "_i", step, state)
    return e.let.format("_w", words, f"{fold}[1]")


def _of_word(e: Engine, word: str, seed: str | None) -> str:
    """hash_uint32: a word, which is hash_bytes of its four bytes."""
    words = e.array.format(f"{_masked(e, word)}, {e.big.format(0)}, {e.big.format(0)}")
    return _hashed(e, words, "4", seed)


def _of_hex(e: Engine, digits: str, seed: str | None, *then: str) -> str:
    """hash_any: bytes in hexadecimal. A tail's third word starts at its second byte."""
    size = e.length.format("_h")
    last = e.quotient.format(size, 24)
    padded = e.rpad.format("_h", f"24 * ({last} + 1)", _ZERO)
    chunk = " || ".join(f"substr({padded}, 8 * _j + {at}, 2)" for at in (7, 5, 3, 1))
    word = e.hex_word.format(chunk)
    tailed = f"CASE WHEN _j = 3 * {last} + 2 THEN {word} * 256 ELSE {word} END"
    words = e.map.format(e.zero_to.format(f"3 * ({last} + 1)"), "_j", e.big.format(tailed))
    body = _hashed(e, words, e.big.format(e.quotient.format(size, 2)), seed, *then)
    return e.let.format("_h", digits, body)


def _little(e: Engine, number: str, size: int) -> str:
    """The low ``size`` bytes of ``number``, little-endian, in hexadecimal."""
    return e.let.format(
        "_x",
        e.word_hex.format(number),
        " || ".join(f"substr(_x, {7 - 2 * at}, 2)" for at in range(size)),
    )


def _text(e: Engine) -> str:
    return e.text.format(e.value)


def _bpchar(e: Engine) -> str:
    return e.text_hex.format(e.rtrim.format(e.value, "' '"))


def _name(e: Engine) -> str:
    """A name holds 63 bytes."""
    return f"substr({e.text_hex.format(e.value)}, 1, {2 * _NAME_BYTES})"


def _name_cut_inside_a_character(e: Engine) -> str:
    """name's input cuts between characters, which a form of bytes cannot see."""
    size = e.length.format(e.text_hex.format(e.value))
    return f"{size} > {2 * _NAME_BYTES} AND {size} <> 2 * {e.chars.format(_text(e))}"


def _word_of_int8(e: Engine) -> str:
    """hashint8: low half xor high half, or the high half's complement if the value is negative."""
    value = e.big.format(e.value)
    high = _masked(e, e.right.format(value, 32))
    return e.xor.format(
        _masked(e, value), f"CASE WHEN {value} >= 0 THEN {high} ELSE {_MASK} - {high} END"
    )


def _word_of_char(e: Engine) -> str:
    return e.ascii.format(f"substr({_text(e)}, 1, 1)")


def _char_of_no_platform(e: Engine) -> str:
    """A "char" above 127 is signed on one platform and not on another, and its octal escape
    says one."""
    text = _text(e)
    return f"{_word_of_char(e)} > 127 OR (substr({text}, 1, 1) = {e.backslash} AND {e.chars.format(text)} > 1)"


def _float(e: Engine, seed: str | None) -> str:
    value = e.float8.format(e.value)
    digits = (
        f"CASE WHEN {e.is_nan.format(value)} THEN '{_NAN}' "
        f"WHEN {e.is_infinite.format(value)} THEN "
        f"CASE WHEN {value} > 0 THEN '{_INFINITY}' ELSE '{_MINUS_INFINITY}' END "
        f"ELSE {e.float_hex.format(value)} END"
    )
    little = " || ".join(f"substr(_b, {at}, 2)" for at in range(15, 0, -2))
    zero = seed if seed is not None else "0"
    return (
        f"CASE WHEN {value} = 0 THEN {zero} ELSE "
        f"{e.let.format('_b', digits, _of_hex(e, little, seed))} END"
    )


def _mac(e: Engine) -> str:
    return e.not_hex.format(_text(e))


def _mac8(e: Engine) -> str:
    """A six-byte address reads as eight with ff:fe in its middle."""
    digits = _mac(e)
    return (
        f"CASE WHEN {e.length.format(digits)} = 12 "
        f"THEN substr({digits}, 1, 6) || 'fffe' || substr({digits}, 7, 6) ELSE {digits} END"
    )


def _tid(e: Engine) -> str:
    """Block number's high half, its low half, the offset: three uint16s."""
    inner = f"replace(replace({_text(e)}, '(', ''), ')', '')"
    block = e.number.format(e.part.format(inner, "','", 1))
    offset = e.number.format(e.part.format(inner, "','", 2))
    return " || ".join(
        (
            _little(e, e.right.format(block, 16), 2),
            _little(e, e.both.format(block, 65535), 2),
            _little(e, offset, 2),
        )
    )


def _oidvector(e: Engine) -> str:
    text = _text(e)
    words = e.map.format(e.split.format(text, "' '"), "_o", _little(e, e.number.format("_o"), 4))
    return f"CASE WHEN {text} = '' THEN '' ELSE {e.join.format(words)} END"


def _inet_v4(e: Engine, address: str) -> str:
    octets = e.map.format(
        e.split.format(address, "'.'"), "_o", _little(e, e.number.format("_o"), 1)
    )
    return e.rpad.format(e.join.format(octets), 8, "'0'")


def _inet_v6(e: Engine, address: str) -> str:
    """An IPv6 address's sixteen bytes: each group padded, ``::`` the groups it leaves out, and a
    trailing IPv4 address its two groups."""

    def groups(side: str) -> str:
        pieces = f"CASE WHEN {side} = '' THEN '' ELSE {side} END"
        return e.join.format(
            e.map.format(
                e.split.format(pieces, "':'"),
                "_q",
                f"CASE WHEN _q = '' THEN '' WHEN strpos(_q, '.') > 0 "
                f"THEN {_inet_v4(e, '_q')} ELSE {e.lpad.format('_q', 4, _ZERO)} END",
            )
        )

    return e.let.format(
        "_l",
        groups(e.part.format(f"{address} || '::'", "'::'", 1)),
        e.let.format(
            "_u",
            groups(e.part.format(f"{address} || '::'", "'::'", 2)),
            e.rpad.format(
                f"_l || {e.lpad.format('_u', f'32 - {e.length.format("_l")}', _ZERO)}",
                32,
                "'0'",
            ),
        ),
    )


def _inet(e: Engine) -> str:
    """hashinet: family, mask bits, the address. A short cidr literal states its bits or is refused."""
    text = _text(e)
    address = e.part.format(f"{text} || '/'", "'/'", 1)
    bits = e.part.format(f"{text} || '/'", "'/'", 2)
    v6 = f"strpos({text}, ':') > 0"
    default = f"CASE WHEN {v6} THEN 128 ELSE 32 END"
    masked = f"CASE WHEN {bits} = '' THEN {default} ELSE {e.number.format(bits)} END"
    family = f"CASE WHEN {v6} THEN '03' ELSE '02' END"
    return (
        f"{family} || {_little(e, masked, 1)} || "
        f"CASE WHEN {v6} THEN {_inet_v6(e, address)} ELSE {_inet_v4(e, address)} END"
    )


def _numeric_exponent(e: Engine) -> str:
    """An exponent is PostgreSQL's input and no digits of a form's; infinity's e is not one."""
    text = f"lower({_text(e)})"
    return f"strpos({text}, 'e') > 0 AND strpos({text}, 'inf') = 0"


def _numeric(e: Engine, seed: str | None) -> str:
    """hash_numeric: the base 10000 digits without the zeros at either end, xor their weight.
    ``_b`` holds the digits' hexadecimal and the weight, or nothing for zero."""
    text = _text(e)
    plain = f"replace(replace({text}, '-', ''), '+', '') || '.'"

    def fours(side: str) -> str:
        return f"({e.quotient.format(f'{e.length.format(side)} + 3', 4)} * 4)"

    whole = e.ltrim.format(e.part.format("_t", "'.'", 1), _ZERO)
    fraction = e.part.format("_t", "'.'", 2)
    aligned = (
        e.lpad.format("_u", fours("_u"), _ZERO) + " || " + e.rpad.format("_v", fours("_v"), _ZERO)
    )
    size = e.length.format("_d")
    lead = e.quotient.format(f"{size} - {e.length.format(e.ltrim.format('_d', _ZERO))}", 4)
    trail = e.quotient.format(f"{size} - {e.length.format(e.rtrim.format('_d', _ZERO))}", 4)
    count = f"({e.quotient.format(size, 4)} - _k - {trail})"
    digit = e.number.format("substr(_d, 4 * (_k + _j) + 1, 4)")
    digits = e.join.format(e.map.format(e.zero_to.format(count), "_j", _little(e, digit, 2)))
    weight = e.text.format(f"{e.quotient.format(fours('_u'), 4)} - 1 - _k")
    read = e.let.format(
        "_k",
        e.big.format(lead),
        f"CASE WHEN {e.ltrim.format('_d', _ZERO)} = '' THEN {e.array.format(chr(39) * 2 + ', ' + chr(39) * 2)} "
        f"ELSE {e.array.format(digits + ', ' + weight)} END",
    )
    parts = e.let.format(
        "_t",
        plain,
        e.let.format("_u", whole, e.let.format("_v", fraction, e.let.format("_d", aligned, read))),
    )
    zero = (
        "-1"
        if seed is None
        else f"CASE WHEN {seed} = {_MIN_BIGINT} THEN {_MAX_BIGINT} ELSE {seed} - 1 END"
    )
    hashed = _of_hex(e, "_b[1]", seed, e.number.format("_b[2]"))
    body = e.let.format("_b", parts, f"CASE WHEN _b[1] = '' THEN {zero} ELSE {hashed} END")
    special = e.array.format("'nan', 'infinity', '-infinity', '+infinity', 'inf', '-inf', '+inf'")
    return f"CASE WHEN {e.has.format(special, f'lower({text})')} THEN {seed or '0'} ELSE {body} END"


Digits = Callable[[Engine], str]
Word = Callable[[Engine], str]

#: A function, and how its value becomes the bytes it hashes.
_BYTES: dict[tuple[str, str], Digits] = {
    ("hashtext", "hashtextextended"): lambda e: e.text_hex.format(e.value),
    ("hashbpchar", "hashbpcharextended"): _bpchar,
    ("hashname", "hashnameextended"): _name,
    ("hashmacaddr", "hashmacaddrextended"): _mac,
    ("hashmacaddr8", "hashmacaddr8extended"): _mac8,
    ("hashinet", "hashinetextended"): _inet,
    ("hashtid", "hashtidextended"): _tid,
    ("hashoidvector", "hashoidvectorextended"): _oidvector,
}
#: A function, and the one word its value is.
_WORDS: dict[tuple[str, str], Word] = {
    ("hashint2", "hashint2extended"): lambda e: e.big.format(e.value),
    ("hashint4", "hashint4extended"): lambda e: e.big.format(e.value),
    ("hashoid", "hashoidextended"): lambda e: e.big.format(e.value),
    ("hashint8", "hashint8extended"): _word_of_int8,
    ("hashchar", "hashcharextended"): _word_of_char,
}
#: Hashes of a value only PostgreSQL's catalogs can say: an enum's and a role's oids, a record's
#: field types. A NULL is NULL, and a value is refused.
REFUSED_VALUES = (
    ("hashenum", "hashenumextended"),
    ("hash_aclitem", "hash_aclitem_extended"),
    ("hash_record", "hash_record_extended"),
)
#: Each function whose argument a form reads as the type the statement cast it to.
HELD = frozenset(
    name
    for pair in (
        *_BYTES,
        *REFUSED_VALUES,
        ("hash_numeric", "hash_numeric_extended"),
        ("hashoid", "hashoidextended"),
        ("hashchar", "hashcharextended"),
    )
    for name in pair
)
#: Numeric literals a form reads as written: their digits are the answer.
NUMERIC_INPUT = frozenset({"hash_numeric", "hash_numeric_extended"})
ENGINES = (("duckdb", DUCKDB), ("trino", TRINO), ("clickhouse", CLICKHOUSE))


def _strict(params: str, answer: str, result: str) -> str:
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CAST(CASE WHEN {nulls} THEN NULL ELSE {answer} END AS {result})"


def _result(e: Engine, seeded: bool) -> str:
    return e.int8_type if seeded else e.int4_type


# --- what is hashed element by element ------------------------------------------------------------


def _of_element(e: Engine, value: str) -> Engine:
    """``e`` reading ``value`` where a form reads its argument."""
    return replace(e, value=e.value.replace("value", value))


def _high(e: Engine, hash_: str) -> str:
    return _masked(e, e.right.format(hash_, 32))


def _pair(e: Engine, hi: str, lo: str, seed: str | None) -> str:
    """A hash from its 32-bit halves: PostgreSQL's uint64, or its uint32."""
    return _signed(e, lo) if seed is None else f"{_signed(e, hi)} * {_TWO32} + {lo}"


def _summed(e: Engine, hashes: str, seed: str | None) -> str:
    """hash_array's sum: 1, then 31 times the sum plus the next hash, modulo the hash's size."""
    low = f"_c[2] * 31 + {_masked(e, '_n')}"
    step = e.array.format(
        _masked(e, f"_c[1] * 31 + {_high(e, '_n')} + {e.right.format(low, 32)}")
        + ", "
        + _masked(e, low)
    )
    start = e.array.format(f"{e.big.format(0)}, {e.big.format(1)}")
    items = e.item.format(e.big_list.format(hashes))
    fold = e.fold.format(items, "_c", "_m", e.let.format("_n", e.item_of.format("_m"), step), start)
    return e.let.format("_q", fold, _pair(e, "_q[1]", "_q[2]", seed))


def _days(e: Engine, year: str, month: str, day: str) -> str:
    """A date's days since 2000-01-01: days_from_civil, in whole numbers."""
    shifted = f"({year} - CASE WHEN {month} <= 2 THEN 1 ELSE 0 END)"
    leaps = (
        f"{e.quotient.format(shifted, 4)} - {e.quotient.format(shifted, 100)} "
        f"+ {e.quotient.format(shifted, 400)}"
    )
    of_year = e.quotient.format(f"153 * (({month} + 9) % 12) + 2", 5)
    return f"(365 * {shifted} + {leaps} + {of_year} + {day} - 730426)"


def _date_days(e: Engine, text: str) -> str:
    parts = ", ".join(
        e.number.format(f"substr({text}, {at}, {size})") for at, size in ((1, 4), (6, 2), (9, 2))
    )
    return e.let.format("_c", e.array.format(parts), _days(e, "_c[1]", "_c[2]", "_c[3]"))


def _timestamp_micros(e: Engine, text: str, zoned: bool) -> str:
    """A timestamp's microseconds since 2000-01-01, from its text."""
    clock = f"substr({text}, 12)"
    zone = (
        f"CASE WHEN strpos({clock}, '+') > 0 THEN strpos({clock}, '+') "
        f"ELSE strpos({clock}, '-') END"
    )
    held = f"substr({clock}, 1, {zone} - 1)" if zoned else clock
    offset = f"substr({clock}, {zone} + 1)"
    east = (
        f"CASE WHEN substr({clock}, {zone}, 1) = '-' THEN -1 ELSE 1 END * "
        f"({e.number.format(f'substr({offset}, 1, 2)')} * 3600 + "
        f"{e.number.format(e.rpad.format(f'substr({offset}, 4, 2)', 2, _ZERO))} * 60)"
    )
    parts = ", ".join(
        (
            *(
                e.number.format(f"substr({text}, {at}, {size})")
                for at, size in ((1, 4), (6, 2), (9, 2))
            ),
            *(e.number.format(f"substr({clock}, {at}, 2)") for at in (1, 4, 7)),
            e.number.format(e.rpad.format(f"replace(substr({held}, 10), '.', '')", 6, _ZERO)),
            east if zoned else e.big.format(0),
        )
    )
    total = (
        f"({_days(e, '_c[1]', '_c[2]', '_c[3]')} * 86400 + _c[4] * 3600 + _c[5] * 60 + _c[6] "
        f"- _c[8]) * 1000000 + _c[7]"
    )
    return e.let.format("_c", e.array.format(parts), total)


def _infinite(text: str, positive: str, negative: str, finite: str) -> str:
    return (
        f"CASE WHEN {text} = 'infinity' THEN {positive} "
        f"WHEN {text} = '-infinity' THEN {negative} ELSE {finite} END"
    )


def _element_hash(e: Engine, kind: str, seed: str | None) -> str:
    """The hash of ``_e``, of PostgreSQL's type ``kind``."""
    of = _of_element(e, "_e")
    if kind in ("int2", "int4", "oid"):
        return _of_word(e, of.number.format(of.text.format(of.value)), seed)
    if kind == "int8":
        return _of_word(
            e, _word_of_int8(replace(of, value=of.number.format(of.text.format(of.value)))), seed
        )
    if kind == "bool":
        return _of_word(e, f"CASE WHEN {of.value} THEN 1 ELSE 0 END", seed)
    if kind in ("float4", "float8"):
        return _float(of, seed)
    if kind == "numeric":
        return _numeric(of, seed)
    if kind == "text":
        return _of_hex(e, of.text_hex.format(of.value), seed)
    if kind == "date":
        text = of.text.format(of.value)
        return _of_word(
            e, _infinite(text, str(2**31 - 1), f"{-(2**31)}", _date_days(e, text)), seed
        )
    text = of.text.format(of.value)
    micros = _infinite(
        text, _MAX_BIGINT, _MIN_BIGINT, _timestamp_micros(e, text, kind == "timestamptz")
    )
    word = _of_word(e, _word_of_int8(replace(of, value="_m")), seed)
    return e.let.format("_m", e.big.format(micros), word)


def _array(e: Engine, kind: str, seed: str | None) -> str:
    element = f"CASE WHEN _e IS NULL THEN 0 ELSE {_element_hash(e, kind, seed)} END"
    return _summed(e, e.map.format("value", "_e", e.big.format(element)), seed)


def _flags(e: Engine, text: str) -> str:
    """A range's flags: inclusive bounds, infinite bounds, or empty."""
    return (
        f"CASE WHEN {text} = 'empty' THEN 1 ELSE "
        f"CASE WHEN substr({text}, 1, 1) = '[' THEN 2 ELSE 0 END + "
        f"CASE WHEN substr({text}, length({text}), 1) = ']' THEN 4 ELSE 0 END + "
        f"CASE WHEN substr({text}, 2, 1) = ',' THEN 8 ELSE 0 END + "
        f"CASE WHEN substr({text}, length({text}) - 1, 1) = ',' THEN 16 ELSE 0 END END"
    )


def _range_hash(e: Engine, kind: str, seed: str | None) -> str:
    """hash_range of the text ``_t``: the flags' hash xor the lower bound's, its halves rotated
    left one bit, xor the upper bound's."""
    inner = "replace(substr(_t, 2, length(_t) - 2), '\"', '')"

    def bound(which: int, *absent: str) -> str:
        return (
            f"CASE WHEN {' OR '.join(absent)} THEN {e.big.format(0)} ELSE "
            + e.let.format("_e", e.part.format(inner, "','", which), _element_hash(e, kind, seed))
            + " END"
        )

    empty = "_t = 'empty'"
    lower = bound(1, empty, "substr(_t, 2, 1) = ','")
    upper = bound(2, empty, "substr(_t, length(_t) - 1, 1) = ','")

    def half(of: Callable[[str], str]) -> str:
        mixed = e.xor.format(of("_y[1]"), of("_y[2]"))
        rotated = _masked(e, f"{_masked(e, mixed)} * 2 + {e.right.format(mixed, 31)}")
        return _masked(e, e.xor.format(rotated, of("_y[3]")))

    answer = _pair(e, half(lambda v: _high(e, v)), half(lambda v: _masked(e, v)), seed)
    parts = ", ".join(
        e.big.format(part) for part in (_of_word(e, _flags(e, "_t"), seed), lower, upper)
    )
    return e.let.format("_y", e.array.format(parts), answer)


def _range(e: Engine, kind: str, seed: str | None) -> str:
    return e.let.format("_t", e.text.format(e.value), _range_hash(e, kind, seed))


def _multirange(e: Engine, kind: str, seed: str | None) -> str:
    """hash_multirange: hash_array's sum of its ranges' hashes."""
    text = e.text.format(e.value)
    inner = f"substr({text}, 2, length({text}) - 2)"
    marked = f"replace(replace({inner}, '),', ')|'), '],', ']|')"
    items = f"CASE WHEN {inner} = '' THEN {e.empty_texts} ELSE {e.split.format(marked, chr(39) + '|' + chr(39))} END"
    return _summed(e, e.map.format(items, "_t", e.big.format(_range_hash(e, kind, seed))), seed)


#: An array's said element type, by its PostgreSQL spelling, and the hash it reads with.
ARRAY_ELEMENTS = {
    "smallint[]": "int2",
    "int[]": "int4",
    "bigint[]": "int8",
    "oid[]": "oid",
    "real[]": "float4",
    "double precision[]": "float8",
    "decimal[]": "numeric",
    "text[]": "text",
    "varchar[]": "text",
    "boolean[]": "bool",
    "date[]": "date",
    "timestamp[]": "timestamp",
    "timestamptz[]": "timestamptz",
}
RANGE_ELEMENTS = {
    "integer": "int4",
    "bigint": "int8",
    "numeric": "numeric",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
}


_KINDS_OF_RANGES = {
    name: RANGE_ELEMENTS[rtype.element]
    for name, rtype in (*RANGE_TYPES.items(), *MULTIRANGE_TYPES.items())
}
#: (function, argument count, the said type) -> its row, for function_macros.OVERLOADS.
OVERLOADS: dict[tuple[str, int, str], str] = {
    **{
        key: row
        for said, kind in ARRAY_ELEMENTS.items()
        for key, row in (
            (("hash_array", 1, said), f"hash_array_{kind}"),
            (("hash_array_extended", 2, said), f"hash_array_extended_{kind}"),
        )
    },
    **{
        key: row
        for said, kind in _KINDS_OF_RANGES.items()
        for function in ("hash_multirange" if said in MULTIRANGE_TYPES else "hash_range",)
        for key, row in (
            ((function, 1, said), f"{function}_{kind}"),
            ((f"{function}_extended", 2, said), f"{function}_extended_{kind}"),
        )
    },
}
COMPOSITES = tuple(
    (name, params)
    for function in ("hash_array", "hash_range", "hash_multirange")
    for name, params in ((function, "value"), (f"{function}_extended", "value, seed"))
)
#: The rows that read their argument as the text of a range or multirange, canonical.
RANGE_ROWS = frozenset(
    name for (function, _, _), name in OVERLOADS.items() if not function.startswith("hash_array")
)
ARRAY_ROWS = frozenset(OVERLOADS.values()) - RANGE_ROWS
TEXT_ARRAY_ROWS = frozenset(
    row for row in ARRAY_ROWS if row.endswith(("_numeric", "_date", "_timestamp", "_timestamptz"))
)


#: What each function refuses by name, of the values that reach it.
_REFUSES: dict[str, Callable[[Engine], str]] = {
    "hashname": _name_cut_inside_a_character,
    "hashnameextended": _name_cut_inside_a_character,
    "hashchar": _char_of_no_platform,
    "hashcharextended": _char_of_no_platform,
    "hash_numeric": _numeric_exponent,
    "hash_numeric_extended": _numeric_exponent,
}


#: Every row of a hash, which is strict: a statement's NULL needs none of its form.
ROWS = frozenset(
    {
        *(name for names in (*_BYTES, *_WORDS, *REFUSED_VALUES) for name in names),
        "hashfloat4",
        "hashfloat4extended",
        "hashfloat8",
        "hashfloat8extended",
        "hash_numeric",
        "hash_numeric_extended",
        *OVERLOADS.values(),
    }
)


def forms(e: Engine) -> dict[tuple[str, str], str]:
    """Each function and its parameters, and its form on ``e``."""
    written: dict[tuple[str, str], str] = {}

    def row(name: str, seed: str | None, answer: str) -> None:
        params = "value" if seed is None else "value, seed"
        if name in _REFUSES:
            answer = _refusing(_REFUSES[name](e), name, answer)
        written[(name, params)] = _strict(params, answer, _result(e, seed is not None))

    for pair, digits in _BYTES.items():
        for name, seed in zip(pair, (None, e.seed)):
            row(name, seed, _of_hex(e, digits(e), seed))
    for pair, word in _WORDS.items():
        for name, seed in zip(pair, (None, e.seed)):
            row(name, seed, _of_word(e, word(e), seed))
    for plain, extended in (
        ("hashfloat4", "hashfloat4extended"),
        ("hashfloat8", "hashfloat8extended"),
    ):
        row(plain, None, _float(e, None))
        row(extended, e.seed, _float(e, e.seed))
    row("hash_numeric", None, _numeric(e, None))
    row("hash_numeric_extended", e.seed, _numeric(e, e.seed))
    for plain, extended in REFUSED_VALUES:
        row(plain, None, _refusing("value IS NOT NULL", plain, "0"))
        row(extended, e.seed, _refusing("value IS NOT NULL", extended, "0"))
    for function, reader, kinds in (
        ("hash_array", _array, ARRAY_ELEMENTS.values()),
        ("hash_range", _range, _KINDS_OF_RANGES.values()),
        ("hash_multirange", _multirange, _KINDS_OF_RANGES.values()),
    ):
        for kind in sorted(set(kinds)):
            row(f"{function}_{kind}", None, reader(e, kind, None))
            row(f"{function}_extended_{kind}", e.seed, reader(e, kind, e.seed))
    return written
