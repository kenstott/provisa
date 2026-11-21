# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b7d30-94a2-4c5f-8e07-2fa6c1d8b934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements that prove PostgreSQL's hash functions as forms (REQ-2171): each function's NULL,
its values and, for an extended hash, zero and non-zero seeds. Lengths around lookup3's twelve-byte
block and every value a type's normalization treats apart."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

_SEEDS = ("0", "1", "(-1)", "5000000000", "(-9223372036854775808)")


def _typed(type_name: str, *values: str) -> list[str]:
    return [f"'{v}'::{type_name}" for v in values]


def _numbers(type_name: str, *values: str) -> list[str]:
    return [f"({v})::{type_name}" for v in values]


_TEXTS = (
    "",
    "a",
    "abc",
    "abcd",
    "abcdefg",
    "abcdefgh",
    "abcdefghi",
    "0123456789a",
    "0123456789ab",
    "0123456789abc",
    "0123456789abcdefghijklmn",
    "0123456789abcdefghijklmno",
    "Hello World",
    "héllo wörld, ünïcode",
    "雪",
    "a  ",
)
_RANGE_BOUNDS = {
    "int4range": ("[1,5)", "empty", "(,3]", "[1,]", "(,)", "[-7,0)", "[3,3]"),
    "int8range": ("[1,5)", "empty", "(,3]", "[5000000000,)"),
    "numrange": ("[1.5,2.5)", "empty", "(,3]", "(0.25,1000]", "[0,0]"),
    "daterange": ("[2024-01-01,2024-02-01)", "empty", "(,2000-01-01]", "[2000-03-01,)"),
    "tsrange": (
        "[2024-01-01 00:00,2024-02-01 00:00)",
        "empty",
        "(,2000-01-01 00:00]",
        "[2024-02-29 12:30:01.5,)",
        "[1999-12-31 23:59:59.999999,2000-01-01 00:00:00.000001]",
    ),
    "tstzrange": (
        "[2024-01-01 00:00+00,2024-02-01 00:00+00)",
        "empty",
        "(,2000-01-01 00:00+00]",
        "[2024-02-29 12:30:01.5+05:30,)",
        "(2024-01-01 00:00-08,)",
    ),
}
_MULTIRANGES = {
    "int4multirange": ("{[1,5),[7,9)}", "{}", "{(,3]}", "{[1,3),[3,5),[9,)}"),
    "int8multirange": ("{[1,5),[7,9)}", "{}", "{(,3]}"),
    "nummultirange": ("{[1.5,2.5)}", "{}", "{(,3]}", "{[0,1],[2,3)}"),
    "datemultirange": ("{[2024-01-01,2024-02-01)}", "{}", "{(,2000-01-01]}"),
    "tsmultirange": ("{[2024-01-01 00:00,2024-02-01 00:00)}", "{}"),
    "tstzmultirange": ("{[2024-01-01 00:00+00,2024-02-01 00:00+00)}", "{}"),
}

#: The function's stem, its extended name, and the arguments it is proved with.
VALUES: dict[tuple[str, str], list[str]] = {
    ("hashint2", "hashint2extended"): _numbers("smallint", "3", "-7", "42", "0", "32767", "-32768"),
    ("hashint4", "hashint4extended"): _numbers(
        "integer", "3", "-7", "42", "0", "1", "2147483647", "-2147483648"
    ),
    ("hashint8", "hashint8extended"): _numbers(
        "bigint",
        "3",
        "-7",
        "5000000000",
        "-5000000000",
        "0",
        "9223372036854775807",
        "-9223372036854775808",
    ),
    ("hashoid", "hashoidextended"): _numbers("oid", "12345", "0", "1", "4294967295"),
    ("hashchar", "hashcharextended"): _typed('"char"', "a", "B", "1"),
    ("hashfloat4", "hashfloat4extended"): [
        *_numbers("real", "0.5", "-2.25", "100", "0", "0.1", "1e38"),
        *_typed("real", "NaN", "Infinity", "-Infinity", "-0"),
    ],
    ("hashfloat8", "hashfloat8extended"): [
        *_numbers("double precision", "0.5", "-2.25", "100", "0", "0.1", "1e308", "1e-300"),
        *_typed("double precision", "NaN", "Infinity", "-Infinity", "-0"),
    ],
    ("hashtext", "hashtextextended"): _typed("text", *_TEXTS),
    ("hashname", "hashnameextended"): _typed("name", *_TEXTS, "x" * 64),
    ("hashbpchar", "hashbpcharextended"): [
        *_typed("character", "abc", "Hello World", "12"),
        *_typed("character(6)", "abc", "abc  ", "abcdefgh"),
        "'abc  '::bpchar",
    ],
    ("hashmacaddr", "hashmacaddrextended"): _typed(
        "macaddr", "08:00:2b:01:02:03", "00:00:00:00:00:00", "FF-FF-FF-FF-FF-FF", "0800.2b01.0203"
    ),
    ("hashmacaddr8", "hashmacaddr8extended"): _typed(
        "macaddr8", "08:00:2b:ff:fe:01:02:03", "08:00:2b:01:02:03", "00:00:00:00:00:00:00:00"
    ),
    ("hashinet", "hashinetextended"): [
        *_typed(
            "inet",
            "192.168.0.1/24",
            "10.1.2.3",
            "::1",
            "0.0.0.0/0",
            "2001:db8::ff00:42:8329/64",
            "1:2:3:4:5:6:7:8",
            "::",
            "fe80::",
            "::ffff:1.2.3.4",
        ),
        *_typed("cidr", "192.168.0.0/24", "10.0.0.0/8", "::1/128"),
    ],
    ("hashtid", "hashtidextended"): _typed("tid", "(0,1)", "(70000,3)", "(4294967295,65535)"),
    ("hashoidvector", "hashoidvectorextended"): _typed(
        "oidvector", "1 2 3", "", "4294967295", "1 2 3 4 5"
    ),
    ("hash_numeric", "hash_numeric_extended"): [
        *[
            f"{v}::numeric"
            for v in (
                "2.5",
                "(-7.125)",
                "1000",
                "0",
                "0.00012",
                "10000",
                "100000000",
                "12345.6789",
                "0.0001",
                "1.10",
                "123456789012345678901234567890.123",
            )
        ],
        *_typed("numeric", "NaN", "Infinity", "-Infinity", "0.000", "+3.5"),
    ],
    ("hash_array", "hash_array_extended"): [
        *_typed(
            "integer[]",
            "{1,2,3}",
            "{}",
            "{-7,NULL,42}",
            "{NULL}",
            "{1,2,3,4,5,6,7,8}",
        ),
        *_typed("smallint[]", "{1,2,3}", "{}", "{-7,NULL,42}"),
        *_typed("bigint[]", "{1,2,3}", "{5000000000,-1}"),
        *_typed("real[]", "{0.25,0.5}", "{-1.5,100}", "{0}"),
        *_typed("double precision[]", "{0.25,0.5}", "{-1.5,100}"),
        *_typed("numeric[]", "{0.25,0.5}", "{-1.5,100}", "{0,NaN}"),
        *_typed("text[]", "{a,b,c}", "{}", "{x,NULL,z}"),
        *_typed("boolean[]", "{t,f}", "{NULL,t}"),
        *_typed("date[]", "{2024-06-15,1999-12-31}", "{2000-01-01,infinity}"),
    ],
    ("hash_range", "hash_range_extended"): [
        v for name, bounds in _RANGE_BOUNDS.items() for v in _typed(name, *bounds)
    ],
    ("hash_multirange", "hash_multirange_extended"): [
        v for name, ranges in _MULTIRANGES.items() for v in _typed(name, *ranges)
    ],
}
#: An enum the recorder creates: a hash of an enum is of its oid, so no value of one is a statement.
ENUM = "hash_probe_enum"
#: NULL of each function whose value only a server's catalogs can say.
NULLS = {
    ("hashenum", "hashenumextended"): f"NULL::{ENUM}",
    ("hash_aclitem", "hash_aclitem_extended"): "NULL::aclitem",
    ("hash_record", "hash_record_extended"): "NULL::record",
}
_FIRST_ARGUMENT_NULL = {
    "hash_array": "NULL::integer[]",
    "hash_range": "NULL::int4range",
    "hash_multirange": "NULL::int4multirange",
}


def _null_of(value: str) -> str:
    return "NULL" + value[value.rindex("::") :]


def statements() -> list[str]:
    """Every statement, each of one call."""
    out: list[str] = []
    for (plain, extended), values in VALUES.items():
        for value in values:
            out.append(f"SELECT {plain}({value}) AS v")
            out.extend(f"SELECT {extended}({value}, {seed}::bigint) AS v" for seed in _SEEDS)
        null = _FIRST_ARGUMENT_NULL.get(plain, _null_of(values[0]))
        out.append(f"SELECT {plain}({null}) AS v")
        out.append(f"SELECT {extended}({null}, 1::bigint) AS v")
        out.append(f"SELECT {extended}({values[0]}, NULL::bigint) AS v")
        out.append(f'SELECT pg_catalog."{plain}"({values[0]}) AS v')
    for (plain, extended), null in NULLS.items():
        out.append(f"SELECT {plain}({null}) AS v")
        out.append(f"SELECT {extended}({null}, 1::bigint) AS v")
    return out
