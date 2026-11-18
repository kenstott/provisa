# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d14-2c6a-4f85-a0e1-9d4c8b2f6e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A json or jsonb value's members on ClickHouse (REQ-2236; REQ-2171), where the value is its text
and ClickHouse's JSON functions read it: json_array_elements and json_each and their jsonb and
_text forms, json_extract_path and jsonb_extract_path and their _text forms, json_build_object and
jsonb_build_object, the object aggregates (json_object_agg, jsonb_object_agg and their _strict,
_unique and _unique_strict forms), jsonb_concat, and jsonb_delete of a key, an index or keys.

ClickHouse reads a JSON number as a double: a value holding a number with a fraction or an exponent
or of more than 18 digits raises 0A000 here rather than answer a rounded number (or, past a
bigint, nothing). A container read as text (_text) is ClickHouse's compact JSON text, the same JSON
as PostgreSQL's; a member of a jsonb object answers jsonb's: a repeated key's last value, keys in
jsonb's order (the shorter first, then by bytes). A set-returning call stands in a select list; a
record (json_each) is written as record_out writes it.

Each value is a literal, a column or a cast of one, as what a rule writes is not walked again; a
path or keys array is a literal."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import clickhouse, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.json_built import (
    TYPES,
    called_names,
    element,
    for_name,
    literal_array,
    said_of,
    sorted_by,
    value_of,
    written,
)
from provisa.transpiler.polyfill.notation import Refusal, Refused
from provisa.transpiler.polyfill.numbers import ch_fail, ch_only
from provisa.transpiler.polyfill.server import plain

FAMILY = "JSON"

_JSONS = frozenset({"json", "jsonb"})


def _first(v: str) -> str:
    """A JSON text's kind by its first character: { [ " or a scalar's."""
    return f"substring(trimLeft({v}), 1, 1)"


#: A number ClickHouse would read as a double, or past a bigint, outside a JSON text's strings.
_INEXACT = (
    'match(replaceRegexpAll({v}, \'"(?:[^"\\\\\\\\]|\\\\\\\\.)*"\', \'""\'), '
    "'[0-9][.eE]|[0-9]{{19}}')"
)


def _inexact(v: str, only: str = "") -> str:
    """0A000 where ``v`` holds a number ClickHouse would round; ``only``: a further condition."""
    condition = f"{v} IS NOT NULL AND {only + ' AND ' if only else ''}" + _INEXACT.format(v=v)
    return ch_fail(
        "0A000",
        "a JSON number with a fraction, an exponent or more than 18 digits has no exact "
        "form on clickhouse",
    ).format(condition=condition)


def _raise(sqlstate: str, message: str, condition: str) -> str:
    return ch_fail(sqlstate, message).format(condition=condition)


def _checked(raises: list[str], answer: str) -> str:
    """``answer`` where no raise of ``raises`` holds (each answers 0 where it does not)."""
    return f"if({' + '.join(raises)} = 0, {answer}, NULL)"


def _json(node: exp.Expr, function: str, dialect: str) -> exp.Expr:
    """A json or jsonb argument: a literal, a column or a cast of one the statement says is
    json."""
    if trino.said(node) not in _JSONS or not plain(node):
        raise Refused(trino.no_form(function, dialect))
    return node


def _string(e: str) -> str:
    """A JSON value's text as a _text function answers it: a string unquoted, null NULL."""
    return f"multiIf({e} = 'null', NULL, startsWith({e}, '\"'), JSONExtractString({e}), {e})"


#: A text as record_out writes a field: quoted where empty or holding a quote, a backslash, a
#: parenthesis, a comma or white space, a quote and a backslash doubled inside.
_FIELD = (
    "if({f} = '' OR match({f}, '[\"\\\\\\\\(),\\\\s]'), concat('\"', replaceAll(replaceAll({f}, "
    "'\\\\', '\\\\\\\\'), '\"', '\"\"'), '\"'), {f})"
)


def _field(f: str) -> str:
    return f"if(isNull({f}), '', {_FIELD.format(f=f'assumeNotNull({f})')})"


def _pairs(v: str) -> str:
    return f"JSONExtractKeysAndValuesRaw(ifNull({v}, '{{}}'))"


def _canonical(pairs: str) -> str:
    """An object's members as jsonb holds them: a repeated key's last, in jsonb's key order."""
    reversed_ = f"arrayReverse({pairs})"
    distinct = (
        f"arrayFilter((_p, _i) -> indexOf(arrayMap(_q -> _q.1, {reversed_}), _p.1) = _i, "
        f"{reversed_}, arrayEnumerate({reversed_}))"
    )
    return f"arraySort(_p -> (length(_p.1), _p.1), {distinct})"


def _object(pairs: str) -> str:
    """An object's text of its (key, JSON text) members."""
    members = f"arrayMap(_p -> concat(toJSONString(_p.1), ': ', _p.2), {pairs})"
    return f"concat('{{', arrayStringConcat({members}, ', '), '}}')"


def _elements(v: str) -> str:
    return f"JSONExtractArrayRaw(ifNull({v}, '[]'))"


def _array(elements: str) -> str:
    return f"concat('[', arrayStringConcat({elements}, ', '), ']')"


def _call(node: exp.Expr, names: frozenset[str]) -> tuple[str, list[exp.Expr]] | None:
    found = called_names(node) & names
    if not found:
        return None
    if isinstance(node, exp.JSONExtract | exp.JSONExtractScalar | exp.JSONBObjectAgg):
        return min(found), [node.this, node.expression]
    if isinstance(node, exp.JSONObjectAgg):
        return min(found), list(node.expressions)
    call = trino.called(node)
    assert call is not None
    return min(found), call[1]


# --- rows of a value's members ----------------------------------------------------------------

_ELEMENTS = frozenset(f"{p}_array_elements{s}" for p in ("json", "jsonb") for s in ("", "_text"))


def array_elements(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_array_elements and its kin: a row of each element of an array; 22023 of an object or
    a scalar, no row of NULL."""
    called = _call(node, _ELEMENTS)
    if called is None or len(called[1]) != 1:
        return None
    name, (arg,) = called
    value = _json(arg, name, dialect)
    if name.startswith("jsonb"):
        scalar, other = (
            "cannot extract elements from a scalar",
            "cannot extract elements from an object",
        )
    else:
        scalar, other = f"cannot call {name} on a scalar", f"cannot call {name} on a non-array"
    raises = [
        _raise("22023", scalar, f"$v IS NOT NULL AND {_first('$v')} NOT IN ('{{', '[')"),
        _raise("22023", other, f"$v IS NOT NULL AND {_first('$v')} = '{{'"),
        _inexact("$v"),
    ]
    elements = _elements("$v")
    held = "Array(String)"
    if name.endswith("_text"):
        elements, held = f"arrayMap(_e -> {_string('_e')}, {elements})", "Array(Nullable(String))"
    none = f"CAST([] AS {held})"
    form = (
        f"arrayJoin(if({' + '.join(raises)} = 0 AND $v IS NOT NULL, "
        f"CAST({elements} AS {held}), {none}))"
    )
    return written(form, {"v": value}, dialect)


_EACH = frozenset(f"{p}_each{s}" for p in ("json", "jsonb") for s in ("", "_text"))


def each(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_each and its kin, in a select list: a record (key, value) of each member of an object,
    jsonb's members (a repeated key's last, in its key order) of a jsonb; 22023 of an array or a
    scalar, no row of NULL."""
    called = _call(node, _EACH)
    if called is None or len(called[1]) != 1:
        return None
    name, (arg,) = called
    value = _json(arg, name, dialect)
    first = _first("$v")
    if name.startswith("jsonb"):
        raises = [
            _raise(
                "22023",
                f"cannot call {name} on a non-object",
                f"$v IS NOT NULL AND {first} != '{{'",
            )
        ]
        pairs = _canonical(_pairs("$v"))
    else:
        raises = [
            _raise(
                "22023",
                "cannot deconstruct a scalar",
                f"$v IS NOT NULL AND {first} NOT IN ('{{', '[')",
            ),
            _raise(
                "22023",
                "cannot deconstruct an array as an object",
                f"$v IS NOT NULL AND {first} = '['",
            ),
        ]
        pairs = _pairs("$v")
    raises.append(_inexact("$v"))
    member = _string("_p.2") if name.endswith("_text") else "_p.2"
    record = f"arrayMap(_p -> concat('(', {_field('_p.1')}, ',', {_field(member)}, ')'), {pairs})"
    form = (
        f"arrayJoin(if({' + '.join(raises)} = 0 AND $v IS NOT NULL, {record}, "
        "CAST([] AS Array(String))))"
    )
    return written(form, {"v": value}, dialect)


# --- a path's value ---------------------------------------------------------------------------

_PATHS = frozenset(f"{p}_extract_path{s}" for p in ("json", "jsonb") for s in ("", "_text"))


def _literal_texts(node: exp.Expr) -> list[str | None] | None:
    """The elements of a text[] literal as transpile reads it (ClickHouse's array of strings),
    None for anything else; a nested array is None too."""
    node = node.unnest()
    while isinstance(node, exp.Cast):
        node = node.this
    if not isinstance(node, exp.Array):
        return None
    out: list[str | None] = []
    for item in node.expressions:
        if isinstance(item, exp.Null):
            out.append(None)
        elif isinstance(item, exp.Literal) and item.is_string:
            out.append(item.this)
        else:
            return None
    return out


def _quoted(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


_INDEX = re.compile(r"\s*[+-]?\d+\s*")


def _step(current: str, step: str) -> str:
    """A path step read of a JSON text: an object's key, an array's index where the step is an
    integer's text (a negative counts from the end)."""
    key = f"nullif(JSONExtractRaw({current}, {_quoted(step)}), '')"
    if not _INDEX.fullmatch(step) or abs(int(step)) >= 1 << 31:
        return key
    index = int(step)
    at = index + 1 if index >= 0 else index
    return (
        f"if(startsWith(trimLeft({current}), '['), "
        f"nullif(JSONExtractRaw({current}, toInt64({at})), ''), {key})"
    )


def extract_path(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_extract_path and its kin of a path literal: the value at the path, NULL where it is
    not there or a step is NULL; the _text forms a string unquoted and null NULL."""
    called = _call(node, _PATHS)
    if called is None or len(called[1]) != 2:
        return None
    name, (arg, path) = called
    value = _json(arg, name, dialect)
    path = path.this if isinstance(path, exp.Variadic) else path
    if isinstance(path.unnest(), exp.Null) or (
        isinstance(path, exp.Cast) and isinstance(path.this, exp.Null)
    ):
        return written("CAST(NULL AS Nullable(String))", {}, dialect)
    steps = _literal_texts(path)
    if steps is None:
        raise Refused(trino.no_form(name, dialect))
    if any(step is None for step in steps):
        return written("CAST(NULL AS Nullable(String))", {}, dialect)
    read = "$v"
    for step in steps:
        assert step is not None
        read = _step(read, step)
    if name.endswith("_text"):
        read = _string(read) if steps else "$v"
    answer = f"CAST({read} AS Nullable(String))"
    # A scalar has no path to read: only a container's numbers are read.
    guard = _inexact("$v", f"{_first('$v')} IN ('{{', '[')")
    return written(_checked([guard], answer), {"v": value}, dialect)


# --- objects built ----------------------------------------------------------------------------

_KEYS = {
    "smallint": "toString({k})",
    "integer": "toString({k})",
    "bigint": "toString({k})",
    "boolean": "if({k}, 'true', 'false')",
    "text": "assumeNotNull({k})",
    "character varying": "assumeNotNull({k})",
    "name": "assumeNotNull({k})",
}
_NULL_KEY = "null value not allowed for object key"
_ODD = Refusal("22023", "argument list must have even number of elements")
ODD_COUNT = Limitation(
    "An odd count of arguments is refused as the statement is read, not where it runs.", _ODD
)
_OBJECTS = frozenset({"json_build_object", "jsonb_build_object"})


def _key(node: exp.Expr, function: str, dialect: str) -> tuple[str, exp.Expr]:
    said = said_of(node)
    if said in _JSONS:
        raise Refused(
            Refusal("22023", "key value must be scalar, not array, composite, or json"), ruling=True
        )
    if said not in _KEYS or not plain(node):
        raise Refused(trino.no_form(function, dialect))
    return said, clickhouse.integer_read(node, dialect) or node


def built_object(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_build_object and jsonb_build_object of keys and values the statement says the types
    of: each key's text and each value's JSON; 22004 of a NULL key, 22023 of an odd count."""
    called = _call(node, _OBJECTS)
    if called is None:
        return None
    name, args = called
    if any(isinstance(a, exp.Variadic) for a in args):
        raise Refused(trino.no_form(name, dialect))
    if len(args) % 2:
        raise Refused(_ODD, ruling=True)
    holes: dict[str, exp.Expr] = {}
    members, raises = [], []
    for i in range(0, len(args), 2):
        said_k, holes[f"k{i}"] = _key(args[i], name, dialect)
        said_v, holes[f"v{i}"] = value_of(args[i + 1], name, dialect)
        key = _KEYS[said_k].format(k=f"$k{i}")
        members.append(f"concat(toJSONString({key}), ' : ', {element(said_v, f'$v{i}')})")
        raises.append(_raise("22004", _NULL_KEY, f"isNull($k{i})"))
    if not members:
        return written("'{}'", {}, dialect)
    answer = f"concat('{{', arrayStringConcat([{', '.join(members)}], ', '), '}}')"
    return written(_checked(raises, answer), holes, dialect)


#: An object aggregate -> whether it drops a NULL value, whether a repeated key raises.
_OBJECT_AGGREGATES = {
    f"{p}_object_agg{s}": (strict, unique)
    for p in ("json", "jsonb")
    for s, strict, unique in (
        ("", False, False),
        ("_strict", True, False),
        ("_unique", False, True),
        ("_unique_strict", True, True),
    )
}


def object_aggregate(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_object_agg and its kin of a key and a value the statement says the types of: an
    object of each row's member, NULL of no rows; 22004 of a NULL key, 22030 of a repeated key
    where the aggregate is _unique. A FILTER, a window or an ORDER BY is refused by name."""
    called = _call(node, frozenset(_OBJECT_AGGREGATES))
    if called is None or len(called[1]) != 2:
        return None
    name, (key_arg, value_arg) = called
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if isinstance(outer, (exp.Filter, exp.Window)):
        raise Refused(trino.no_form(name, dialect))
    holes: dict[str, exp.Expr] = {}
    sort = None
    if isinstance(value_arg, exp.Order):
        keys = value_arg.expressions
        if len(keys) != 1 or not plain(keys[0].this):
            raise Refused(trino.no_form(name, dialect))
        holes["o"] = keys[0].this
        sort = sorted_by(keys[0], 4, 5)
        value_arg = value_arg.this
    strict, unique = _OBJECT_AGGREGATES[name]
    said_k, holes["k"] = _key(key_arg, name, dialect)
    said_v, holes["v"] = value_of(value_arg, name, dialect)
    text = _KEYS[said_k].format(k="$k")
    order = "isNull($o), assumeNotNull($o)" if sort else "0, 0"
    rows = (
        f"groupArray(tuple(if(isNull($k), '', {text}), {element(said_v, '$v')}, isNull($v), "
        f"{order}))"
    )
    every = rows  # a repeated key raises whatever its value (json.c, jsonb.c: before strict)
    if strict:
        rows = f"arrayFilter(_t -> NOT _t.3, {rows})"
    if sort:
        rows = f"{sort[0]}(_t -> {sort[1]}, {rows})"
    # jsonb's own message and SQLSTATE of a NULL key (jsonb.c), json's (json.c)
    null_key = (
        ("22023", "field name must not be null")
        if name.startswith("jsonb")
        else ("22004", _NULL_KEY)
    )
    raises = [_raise(*null_key, "countIf(isNull($k)) > 0")]
    if unique:
        keys = f"arrayMap(_t -> _t.1, {every})"
        raises.append(
            _raise(
                "22030",
                "duplicate JSON object key value",
                f"length(arrayDistinct({keys})) != length({keys})",
            )
        )
    members = f"arrayMap(_t -> concat(toJSONString(_t.1), ' : ', _t.2), {rows})"
    answer = f"if(count() = 0, NULL, concat('{{ ', arrayStringConcat({members}, ', '), ' }}'))"
    return written(_checked(raises, answer), holes, dialect)


# --- jsonb changed ----------------------------------------------------------------------------


def concat(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """jsonb_concat: two objects' members, the right's value of a key both hold; else the two as
    arrays (a value no array an array of it) joined."""
    called = _call(node, frozenset({"jsonb_concat"}))
    if called is None or len(called[1]) != 2:
        return None
    name, (left, right) = called
    holes = {"a": _json(left, name, dialect), "b": _json(right, name, dialect)}

    def as_array(v: str) -> str:
        return f"if({_first(v)} = '[', {_elements(v)}, [JSONExtractRaw(ifNull({v}, 'null'))])"

    objects = f"{_first('$a')} = '{{' AND {_first('$b')} = '{{'"
    merged = _object(_canonical(f"arrayConcat({_pairs('$a')}, {_pairs('$b')})"))
    joined = _array(f"arrayConcat({as_array('$a')}, {as_array('$b')})")
    answer = f"if(isNull($a) OR isNull($b), NULL, if({objects}, {merged}, {joined}))"
    return written(_checked([_inexact("$a"), _inexact("$b")], answer), holes, dialect)


_SCALAR_DELETE = "cannot delete from scalar"


def delete(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """jsonb_delete of a key (an object's member, an array's string elements equal to it), of an
    index (an array's element; a negative counts from the end) or of an array of keys (VARIADIC):
    22023 of a scalar, and of an object by an index; 2202E of a keys array of more than one
    dimension."""
    called = _call(node, frozenset({"jsonb_delete"}))
    if called is None or len(called[1]) != 2:
        return None
    name, (arg, removed) = called
    holes = {"v": _json(arg, name, dialect)}
    first = _first("$v")
    raises = [_raise("22023", _SCALAR_DELETE, f"$v IS NOT NULL AND {first} NOT IN ('{{', '[')")]
    pairs, elements = _canonical(_pairs("$v")), _elements("$v")
    if isinstance(removed, exp.Variadic):
        keys = _literal_texts(removed.this)
        inner = removed.this.unnest()
        while isinstance(inner, exp.Cast):
            inner = inner.this
        if isinstance(inner, exp.Null):
            return written("CAST(NULL AS Nullable(String))", {}, dialect)
        if keys is None:
            if not literal_array(removed.this) or not isinstance(inner, exp.Array):
                raise Refused(trino.no_form(name, dialect))
            # An array of arrays: PostgreSQL raises before it reads the value.
            raises.insert(0, _raise("2202E", "wrong number of array subscripts", "$v IS NOT NULL"))
            keys = []
        texts = "[" + ", ".join(_quoted(k) for k in keys if k is not None) + "]"
        texts = f"CAST({texts} AS Array(String))"
        kept_pairs = f"arrayFilter(_p -> NOT has({texts}, _p.1), {pairs})"
        kept = f"arrayFilter(_e -> NOT (startsWith(_e, '\"') AND has({texts}, JSONExtractString(_e))), {elements})"
    else:
        said = said_of(removed)
        if said in ("text", "character varying") and plain(removed):
            holes["k"] = removed
            kept_pairs = f"arrayFilter(_p -> _p.1 != $k, {pairs})"
            kept = f"arrayFilter(_e -> NOT (startsWith(_e, '\"') AND JSONExtractString(_e) = $k), {elements})"
        elif said in ("smallint", "integer") and plain(removed):
            holes["k"] = clickhouse.integer_read(removed, dialect) or removed
            raises.append(
                _raise(
                    "22023",
                    "cannot delete from object using integer index",
                    f"$v IS NOT NULL AND $k IS NOT NULL AND {first} = '{{'",
                )
            )
            kept_pairs = pairs  # an object raises above
            at = f"if($k < 0, length({elements}) + $k, toInt64($k)) + 1"
            kept = f"arrayFilter((_e, _j) -> _j != {at}, {elements}, arrayEnumerate({elements}))"
        else:
            raise Refused(trino.no_form(name, dialect))
    raises.append(_inexact("$v"))
    nulls = " OR ".join(f"isNull(${h})" for h in holes)
    answer = f"if({nulls}, NULL, if({first} = '{{', {_object(kept_pairs)}, {_array(kept)}))"
    return written(_checked(raises, answer), holes, dialect)


# --- the grid's rows --------------------------------------------------------------------------

_EXACT = Limitation(
    "A json or jsonb value holding a number with a fraction, an exponent or more than 18 digits "
    "raises 0A000: ClickHouse reads a JSON number as a double.",
    Refusal(
        "0A000",
        "a JSON number with a fraction, an exponent or more than 18 digits has no exact form on "
        "clickhouse",
    ),
)
_PLAIN = Limitation(
    "Of a value that is a literal, a column or a cast of one, a path or keys array a literal: "
    "any other is refused by name."
)
_COMPACT = Limitation(
    "A container answered as text is ClickHouse's compact JSON text: the same JSON, its "
    "whitespace not PostgreSQL's."
)
_SELECT_LIST = Limitation("In a select list: in a FROM it is refused by name.")


def _feature(
    name: str,
    summary: str,
    rewrite: Callable[[exp.Expr, str], exp.Expr | None],
    *limits: Limitation,
) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, ch_only(for_name(name, rewrite))),),
            limitations={"clickhouse": limits},
        ),
    )


FEATURES: tuple[Feature, ...] = (
    *(
        _feature(
            n,
            "A row of each element of an array.",
            array_elements,
            _EXACT,
            _PLAIN,
            _SELECT_LIST,
            _COMPACT,
        )
        for n in sorted(_ELEMENTS)
    ),
    *(
        _feature(
            n, "A record of each member of an object.", each, _EXACT, _PLAIN, _SELECT_LIST, _COMPACT
        )
        for n in sorted(_EACH)
    ),
    *(
        _feature(n, "The value at a path.", extract_path, _EXACT, _PLAIN, _COMPACT)
        for n in sorted(_PATHS)
    ),
    *(
        _feature(
            n,
            "A JSON object of an aggregate's keys and values.",
            object_aggregate,
            TYPES,
            Limitation("A FILTER, a window or an ORDER BY is refused by name."),
        )
        for n in sorted(_OBJECT_AGGREGATES)
    ),
    _feature("jsonb_concat", "Two jsonb values joined.", concat, _EXACT, _PLAIN),
    _feature(
        "jsonb_delete", "A jsonb value without a key, an index or keys.", delete, _EXACT, _PLAIN
    ),
)
