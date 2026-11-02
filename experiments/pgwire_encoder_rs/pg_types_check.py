# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3c1b84-2a9d-4f61-b0c5-8d4e6f2a9b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The encoder's text and binary for each type it writes, against a live PostgreSQL's own: its
output function and its send function (``encode(<type>_send(v), 'hex')``), byte for byte. Each
case is a column as the engine hands it over (Arrow) and the PostgreSQL expression of each value.
Covers each product defect pg_reference corrects. The server is any running PostgreSQL reached by
psql arguments; PostgreSQL 18's send and output functions for these types are 17's.

    PYTHONPATH=.:vendor/buenavista:experiments/pgwire_encoder_rs/build:experiments/pgwire_encoder_rs \\
        .venv/bin/python experiments/pgwire_encoder_rs/pg_types_check.py <psql> [psql args...]
"""

from __future__ import annotations

import ipaddress
import struct
import subprocess
import sys
from decimal import Decimal
from typing import Any

import pyarrow as pa

import pgwire_encoder_rs

_DUCKDB_INET = pa.struct(
    [("ip_type", pa.uint8()), ("address", pa.decimal128(38, 0)), ("mask", pa.uint16())]
)


def duckdb_inet(ips: list[str]) -> pa.StructArray:
    """DuckDB's INET as its Arrow carries it (an IPv6 address with its top bit flipped)."""
    kinds, addresses, masks = [], [], []
    for ip in ips:
        iface = ipaddress.ip_interface(ip)
        flipped = int(iface.ip) if iface.version == 4 else int(iface.ip) - (1 << 127)
        kinds.append(1 if iface.version == 4 else 2)
        addresses.append(flipped.to_bytes(16, "little", signed=True))
        masks.append(iface.network.prefixlen)
    address = pa.Array.from_buffers(
        pa.decimal128(38, 0), len(ips), [None, pa.py_buffer(b"".join(addresses))]
    )
    return pa.StructArray.from_arrays(
        [pa.array(kinds, pa.uint8()), address, pa.array(masks, pa.uint16())],
        fields=list(_DUCKDB_INET),
    )


def q(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def held(bvtype: str, pg_type: str, send: str, texts: list[str]) -> tuple:
    """A type held as its PostgreSQL text: the column is that text."""
    return (bvtype, pa.array(texts), [], [f"{q(t)}::{pg_type}" for t in texts], send)


# (BVType, Arrow column, coercers, PostgreSQL expression of each value, send function)
CASES: list[tuple[str, Any, list[str], list[str], str]] = [
    held("POINT", "point", "point_send", ["(1,2)", "(NaN,-Infinity)", "(1e-05,1e+20)", "(-0,0.1)"]),
    held("LINE", "line", "line_send", ["{1,-1,0}", "{0.5,2,-3.25}"]),
    held("LSEG", "lseg", "lseg_send", ["[(0,0),(1,1)]", "[(-1.5,2),(3,1e+300)]"]),
    held("BOX", "box", "box_send", ["(2,3),(0,0)", "(1,1),(-1,-1)"]),
    held("PATH", "path", "path_send", ["((0,0),(1,1),(2,0))", "[(0,0),(1,-1e+300)]", "[(5,5)]"]),
    held("POLYGON", "polygon", "poly_send", ["((0,0),(1,1),(2,0))", "((1.5,2.5))"]),
    held("CIRCLE", "circle", "circle_send", ["<(1,2),3>", "<(0,0),0.5>"]),
    held("INT4RANGE", "int4range", "range_send", ["[1,10)", "empty", "(,5)", "[3,)", "(,)"]),
    held("INT8RANGE", "int8range", "range_send", ["[-9223372036854775808,0)", "[1,2)"]),
    held(
        "NUMRANGE",
        "numrange",
        "range_send",
        ["[1.5,2.25)", "(,0.0000001]", "[-0.0000000001,100000000000000000000)"],
    ),
    held(
        "DATERANGE",
        "daterange",
        "range_send",
        [
            "[2020-01-01,2020-02-01)",
            "[2020-01-01,infinity)",
            "[-infinity,2000-01-01)",
            '["0044-03-15 BC",0001-01-01)',
            "[9999-12-31,12000-01-01)",
        ],
    ),
    held(
        "TSRANGE",
        "tsrange",
        "range_send",
        [
            '["2020-01-01 00:00:00","2020-01-02 12:30:00.5")',
            '["2000-01-01 00:00:00 BC",)',
            '["2020-01-01 00:00:00",infinity)',
            '["12000-06-01 01:02:03.000004",)',
        ],
    ),
    held(
        "TSTZRANGE",
        "tstzrange",
        "range_send",
        [
            '["2020-01-01 00:00:00+00","2020-01-02 00:00:00+00")',
            '[-infinity,"2020-01-01 05:30:00+00")',
            '["0100-01-01 00:00:00+00 BC",)',
        ],
    ),
    held("INT4MULTIRANGE", "int4multirange", "multirange_send", ["{[1,3),[5,7)}", "{}"]),
    held("DATEMULTIRANGE", "datemultirange", "multirange_send", ["{[2020-01-01,infinity)}"]),
    held(
        "TSMULTIRANGE",
        "tsmultirange",
        "multirange_send",
        ['{["2020-01-01 00:00:00","2020-01-02 00:00:00")}'],
    ),
    held("NUMMULTIRANGE", "nummultirange", "multirange_send", ["{[1.5,2),[3,4.75]}"]),
    held(
        "TSVECTOR",
        "tsvector",
        "tsvectorsend",
        ["'a':1A,2 'b''c' 'd\\\\e':3C", "'cat' 'fat':2B,3", "", "'é' 'ü':16383"],
    ),
    held(
        "TSQUERY",
        "tsquery",
        "tsquerysend",
        [
            "'a' & 'b' & 'c'",
            "( 'a' | 'b' ) & 'c'",
            "'a' <-> ( 'b' <-> 'c' )",
            "'a' <-> 'b' <-> 'c'",
            "'a' <3> 'b'",
            "!( 'a' & 'b' )",
            "!!'a'",
            "'a':AB & 'b':*",
            "'x y' & 'o''k'",
            "'a\\\\b'",
            "( 'a' | 'b' ) <-> 'c'",
            "'a' <0> 'b'",
            "'a' & !'b' | 'c' & 'd':*BCD",
            "'fat' & ( 'rat' | 'cat' ) <2> !'dog'",
        ],
    ),
    held(
        "CIDR",
        "cidr",
        "cidr_send",
        ["10.0.0.0/8", "1.2.3.4/32", "2001:db8::/32", "::ffff:1.2.3.0/120"],
    ),
    held(
        "INET",
        "inet",
        "inet_send",
        ["192.168.0.1/24", "10.1.2.3", "2001:db8::1/64", "::ffff:1.2.3.4"],
    ),
    (
        "INET",
        duckdb_inet(
            [
                "1.2.3.4/24",
                "1.2.3.4/32",
                "::ffff:1.2.3.4/128",
                "::1.2.3.4/128",
                "2001:db8::1/64",
                "::/0",
                "::1/128",
                "fe80::1:0:0:1/10",
            ]
        ),
        ["duckdb_inet"],
        [
            f"{q(ip)}::inet"
            for ip in [
                "1.2.3.4/24",
                "1.2.3.4/32",
                "::ffff:1.2.3.4/128",
                "::1.2.3.4/128",
                "2001:db8::1/64",
                "::/0",
                "::1/128",
                "fe80::1:0:0:1/10",
            ]
        ],
        "inet_send",
    ),
    held("MACADDR", "macaddr", "macaddr_send", ["08:00:2b:01:02:03"]),
    held(
        "MACADDR8",
        "macaddr8",
        "macaddr8_send",
        ["08:00:2b:01:02:03:04:05", "08:00:2b:ff:fe:01:02:03"],
    ),
    held("BIT", "bit(11)", "bit_send", ["10101010101"]),
    held("VARBIT", "varbit", "varbit_send", ["101", "", "11111111", "100000000"]),
    held("TID", "tid", "tidsend", ["(1,2)", "(4294967295,65535)"]),
    held("XID", "xid", "xidsend", ["0", "4294967295"]),
    held("CID", "cid", "cidsend", ["7"]),
    held("XID8", "xid8", "xid8send", ["9223372036854775807"]),
    held("PG_SNAPSHOT", "pg_snapshot", "pg_snapshot_send", ["10:20:12,15", "5:5:"]),
    held("TXID_SNAPSHOT", "txid_snapshot", "txid_snapshot_send", ["10:20:12,15"]),
    held("PG_LSN", "pg_lsn", "pg_lsn_send", ["16/B374D848", "0/0", "FFFFFFFF/FFFFFFFF"]),
    held("JSONPATH", "jsonpath", "jsonpath_send", ['$."a"[*]?(@ > 1)', "$"]),
    held("XML", "xml", "xml_send", ["<a>b</a>", '<?xml version="1.1"?><a/>']),
    (
        "XML",
        pa.array(
            [
                '<?xml version="1.0" encoding="UTF-8"?>\n<a/>',
                '<?xml version="1.0" standalone="yes"?><a/>',
            ]
        ),
        ["xml_out"],
        [
            q('<?xml version="1.0" encoding="UTF-8"?>\n<a/>') + "::xml",
            q('<?xml version="1.0" standalone="yes"?><a/>') + "::xml",
        ],
        "xml_send",
    ),
    held("UUID", "uuid", "uuid_send", ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]),
    held(
        "TIMETZ",
        "timetz",
        "timetz_send",
        ["12:34:56.789+05:30", "00:00:00-08", "23:59:59.999999+00"],
    ),
    held("NAME", "name", "namesend", ["pg_class", "é"]),
    held("CHAR", '"char"', "charsend", ["a"]),
    (
        "MONEY",
        pa.array(
            [
                Decimal("1234.5"),
                Decimal("-0.005"),
                Decimal("0.004"),
                Decimal("92233720368547758.07"),
                Decimal("-92233720368547758.08"),
                Decimal("1000000"),
            ],
            pa.decimal128(21, 4),
        ),
        [],
        [
            "1234.5::numeric::money",
            "(-0.005)::numeric::money",
            "0.004::numeric::money",
            "92233720368547758.07::numeric::money",
            "(-92233720368547758.08)::numeric::money",
            "1000000::numeric::money",
        ],
        "cash_send",
    ),
    (
        "DECIMAL",
        pa.array(
            [
                Decimal("1E-10"),
                Decimal("-0E-10"),
                Decimal("12345678901234567890.1234567891"),
                Decimal("99999999999999999999999999.9999999999"),
            ],
            pa.decimal128(38, 10),
        ),
        [],
        [
            "0.0000000001::numeric",
            "0.0000000000::numeric",
            "12345678901234567890.1234567891::numeric",
            "99999999999999999999999999.9999999999::numeric",
        ],
        "numeric_send",
    ),
    (
        "DECIMAL",
        pa.array([float("nan"), float("inf"), float("-inf"), 1e16, 0.1]),
        ["decimal"],
        [
            "'NaN'::numeric",
            "'Infinity'::numeric",
            "'-Infinity'::numeric",
            "'1E+16'::numeric",
            "0.1::numeric",
        ],
        "numeric_send",
    ),
    (
        "INTEGERARRAY",
        pa.array([[], [1, None, 3]], pa.list_(pa.int32())),
        [],
        ["'{}'::int4[]", "'{1,NULL,3}'::int4[]"],
        "array_send",
    ),
    (
        "INTEGERARRAY",
        pa.array([[[1, 2], [3, 4]], [[], []], [[None, 5], [6, 7]]], pa.list_(pa.list_(pa.int32()))),
        [],
        ["'{{1,2},{3,4}}'::int4[]", "'{{},{}}'::int4[]", "'{{NULL,5},{6,7}}'::int4[]"],
        "array_send",
    ),
    (
        "STRINGARRAY",
        pa.array([[[], []], [["a", "b c"], ["{", None]]], pa.list_(pa.list_(pa.string()))),
        [],
        ["'{{},{}}'::text[]", '\'{{a,"b c"},{"{",NULL}}\'::text[]'],
        "array_send",
    ),
    (
        "DECIMALARRAY",
        pa.array([[Decimal("1E-7"), None]], pa.list_(pa.decimal128(10, 7))),
        [],
        ["'{0.0000001,NULL}'::numeric[]"],
        "array_send",
    ),
    (
        "OIDVECTOR",
        pa.array([[1, 2], [], [26]], pa.list_(pa.int64())),
        [],
        ["'1 2'::oidvector", "''::oidvector", "'26'::oidvector"],
        "oidvectorsend",
    ),
    held("OIDVECTOR", "oidvector", "oidvectorsend", ["1 2 3"]),
    (
        "INT2VECTOR",
        pa.array([[3, -1]], pa.list_(pa.int16())),
        [],
        ["'3 -1'::int2vector"],
        "int2vectorsend",
    ),
    (
        "MONEYARRAY",
        pa.array([[Decimal("1.5"), None]], pa.list_(pa.decimal128(21, 4))),
        [],
        ["ARRAY[1.5::numeric::money, NULL]"],
        "array_send",
    ),
]


def _cells(b: bytes) -> list[bytes | None]:
    out, at = [], 0
    while at < len(b):
        (n,) = struct.unpack_from("!i", b, at + 7)
        out.append(None if n < 0 else b[at + 11 : at + 11 + n])
        at += 11 + max(n, 0)
    return out


def main() -> None:
    psql = sys.argv[1:] + ["-X", "-q", "-At", "-v", "ON_ERROR_STOP=1"]
    version = subprocess.run(psql + ["-c", "SHOW server_version"], capture_output=True, check=True)
    print("server", version.stdout.decode().strip())
    pgwire_encoder_rs.register_catalog_names([], [])
    checked = differ = 0
    for bvtype, column, coercers, exprs, send in CASES:
        batch = pa.RecordBatch.from_arrays([column], names=["c"])
        cells = {}
        for fmt in (0, 1):
            out = pgwire_encoder_rs.encode_batch(batch, [bvtype], [fmt], [coercers])
            cells[fmt] = _cells(out)
        for k, expr in enumerate(exprs):
            sql = (
                f"SET TimeZone = 'UTC'; SELECT encode(convert_to(format('%s', {expr}), 'UTF8'), 'hex') || '|' || "
                f"encode({send}({expr}), 'hex')"
            )
            done = subprocess.run(psql + ["-c", sql], capture_output=True, check=True)
            text_hex, binary_hex = done.stdout.decode().strip().split("|")
            for fmt, want in ((0, bytes.fromhex(text_hex)), (1, bytes.fromhex(binary_hex))):
                got = cells[fmt][k]
                checked += 1
                if got != want:
                    differ += 1
                    kind = "text" if fmt == 0 else "binary"
                    print(f"DIFF {bvtype} {kind} {expr}: postgres {want!r} encoder {got!r}")
    print(f"{checked} cells checked, {differ} differ")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
