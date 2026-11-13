# Copyright (c) 2026 Kenneth Stott
# Canary: 61d4b0e9-2a7c-4f35-9e18-b3c5a0f7d264
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The network address types on Trino (REQ-2236; the function support measure, run 43307392116,
REQ-2171).

An operation of two inet values (inetand, inetor, network_cmp and its kin) is written by the old
table (provisa.transpiler.pg_forms_types) as nested subqueries that bind each address's number and
mask length; Trino reads them as correlated subqueries and refuses them. Each is refused by name
on Trino, every other dialect keeping that form.

A macaddr's bits are its twelve hex digits read as a number: the old table writes them back with
Trino's to_hex, which reads bytes, not a number (a FUNCTION_NOT_FOUND there). Trino's form reads
the digits with from_base and writes them with to_base, both of a bigint, which holds a macaddr's
48 bits. A macaddr8's 64 bits are past a bigint's sign: its bit operations are refused by name.
trunc of either keeps the first three bytes, which a macaddr8 read from six bytes shares.

ClickHouse has no inet type either (a cast to one is held as its text there, as a cidr's is):
broadcast, family, host and network of an inet or a cidr are refused by name on it, as on Trino. Its
trunc of a MAC address is ClickHouse's form of the same bytes: ClickHouse sent its own trunc, of a
number, a String (run 43410717058)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement

FAMILY = "network addresses"

_TWO_ADDRESSES = "Refused by name: the old table's form is a subquery Trino reads as correlated."

#: The operations of two inet values whose old form Trino refuses, each with its summary.
_INET_PAIRS = {
    "inetand": "The bitwise AND of two addresses.",
    "inetor": "The bitwise OR of two addresses.",
    "network_cmp": "btree's comparison of two networks.",
    "network_eq": "Two networks equal.",
    "network_ne": "Two networks not equal.",
    "network_lt": "One network before another.",
    "network_le": "One network before or equal to another.",
    "network_gt": "One network after another.",
    "network_ge": "One network after or equal to another.",
    "network_larger": "The later of two networks.",
    "network_smaller": "The earlier of two networks.",
    "network_overlap": "Two networks that hold an address in common.",
    "network_sub": "A network strictly within another.",
    "network_subeq": "A network within or equal to another.",
    "network_sup": "A network strictly holding another.",
    "network_supeq": "A network holding or equal to another.",
}

INET_PAIRS: tuple[Feature, ...] = tuple(
    trino.by_name(name, FAMILY, summary, _TWO_ADDRESSES) for name, summary in _INET_PAIRS.items()
)

#: The functions of an address Trino has no form of: the call was sent as written, of a type
#: (INET) Trino does not have. Refused where an argument is said to be an inet or a cidr.
_OF_AN_ADDRESS = {
    "broadcast": "The broadcast address of a network.",
    "family": "An address's family: 4 or 6.",
    "host": "An address's text, without its mask length.",
    "network": "A network's address, its host bits cleared.",
}
_ADDRESSES = ("inet", "cidr")

#: The dialects with no inet type and no form of a function of one.
_NO_INET = (*trino.TRINO, "clickhouse")


def _of_an_address(name: str, summary: str) -> Feature:
    """``name`` of an inet or cidr refused by name where the dialect has no inet type."""
    refused = trino.refusing(
        name,
        f"{name} of an inet or cidr: refused by name, as the dialect has no such type.",
        trino.of_said(_ADDRESSES),
    )
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, dict.fromkeys(_NO_INET, refused)),),
            limitations=dict.fromkeys(
                _NO_INET,
                (Limitation("Refused by name: the dialect has no inet type, and no form of it."),),
            ),
        ),
    )


OF_AN_ADDRESS: tuple[Feature, ...] = tuple(
    _of_an_address(name, summary) for name, summary in _OF_AN_ADDRESS.items()
)

# --- macaddr ---------------------------------------------------------------------------------------

#: A macaddr's twelve hex digits, of any of the forms macaddr_in reads, as a number.
_NUMBER = "from_base(lower(regexp_replace(CAST(${x} AS VARCHAR), '[^0-9A-Fa-f]', '')), 16)"
#: The 48 bits of a macaddr.
_BITS = 281474976710655


def _written_as_macaddr(number: str) -> str:
    """A number of 48 bits as macaddr_out writes it: six bytes of two lowercase hex digits."""
    return (
        f"regexp_replace(lpad(to_base({number}, 16), 12, '0'), "
        "'(..)(..)(..)(..)(..)(..)', '$1:$2:$3:$4:$5:$6')"
    )


_MACADDR_FORMS = {
    "macaddr_and": (
        "macaddr_and($a, $b)",
        _written_as_macaddr(f"bitwise_and({_NUMBER.format(x='a')}, {_NUMBER.format(x='b')})"),
        "The bitwise AND of two MAC addresses.",
    ),
    "macaddr_or": (
        "macaddr_or($a, $b)",
        _written_as_macaddr(f"bitwise_or({_NUMBER.format(x='a')}, {_NUMBER.format(x='b')})"),
        "The bitwise OR of two MAC addresses.",
    ),
    "macaddr_not": (
        "macaddr_not($a)",
        _written_as_macaddr(f"bitwise_and(bitwise_not({_NUMBER.format(x='a')}), {_BITS})"),
        "The bitwise NOT of a MAC address.",
    ),
}

MACADDR_BITS: tuple[Feature, ...] = tuple(
    Feature(name, FAMILY, summary, read=Side(rules=(Rule(pattern, trino.trino_only(form)),)))
    for name, (pattern, form, summary) in _MACADDR_FORMS.items()
)

_MACADDR8_BITS = {
    "macaddr8_and": "The bitwise AND of two EUI-64 MAC addresses.",
    "macaddr8_or": "The bitwise OR of two EUI-64 MAC addresses.",
    "macaddr8_not": "The bitwise NOT of an EUI-64 MAC address.",
    "macaddr8_set7bit": "An EUI-64 MAC address with its 7th bit set.",
}

MACADDR8_BITS: tuple[Feature, ...] = tuple(
    trino.by_name(
        name,
        FAMILY,
        summary,
        "Refused by name: a macaddr8's 64 bits are past what a Trino bigint holds unsigned.",
    )
    for name, summary in _MACADDR8_BITS.items()
)

#: trunc's form of each type: the first three bytes kept, the rest zero, as macaddr_out and
#: macaddr8_out write them.
_KEPT_BYTES = "substr(lower(regexp_replace(CAST($m AS VARCHAR), '[^0-9A-Fa-f]', '')), 1, 6)"
_TRUNCATED = {
    "macaddr": f"regexp_replace({_KEPT_BYTES} || '000000', "
    "'(..)(..)(..)(..)(..)(..)', '$1:$2:$3:$4:$5:$6')",
    "macaddr8": f"regexp_replace({_KEPT_BYTES} || '0000000000', "
    "'(..)(..)(..)(..)(..)(..)(..)(..)', '$1:$2:$3:$4:$5:$6:$7:$8')",
}
_TRUNC = "trunc($m)"
#: ClickHouse's form of the same bytes: its replaceRegexpOne writes a group as \\N.
_CH_KEPT_BYTES = "substring(lower(replaceRegexpAll(toString($m), '[^0-9A-Fa-f]', '')), 1, 6)"
_CH_TRUNCATED = {
    "macaddr": f"replaceRegexpOne(concat({_CH_KEPT_BYTES}, '000000'), "
    "'(..)(..)(..)(..)(..)(..)', '\\\\1:\\\\2:\\\\3:\\\\4:\\\\5:\\\\6')",
    "macaddr8": f"replaceRegexpOne(concat({_CH_KEPT_BYTES}, '0000000000'), "
    "'(..)(..)(..)(..)(..)(..)(..)(..)', "
    "'\\\\1:\\\\2:\\\\3:\\\\4:\\\\5:\\\\6:\\\\7:\\\\8')",
}


def truncated_address(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """trunc of a MAC address: its first three bytes, the rest zero; any other trunc is kept."""
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    said = trino.said(call[1][0]) or ""
    if dialect == "clickhouse":
        form = _CH_TRUNCATED.get(said)
        if form is None:
            return None
        return graft(replacement(form, "clickhouse", _TRUNC), {"m": call[1][0]})
    form = _TRUNCATED.get(said)
    return None if form is None else trino.written(form, _TRUNC, {"m": call[1][0]})


TRUNC = Feature(
    "trunc",
    FAMILY,
    "A MAC address with its last three (macaddr) or five (macaddr8) bytes set to zero.",
    read=Side(
        rules=(
            Rule(
                _TRUNC,
                {
                    **trino.trino_only(Rewrite(truncated_address)),
                    "clickhouse": Rewrite(truncated_address),
                },
            ),
        ),
        limitations=dict.fromkeys(
            trino.TRINO,
            (
                Limitation(
                    "A MAC address whose type no statement says is read as a number, and is "
                    "Trino's own truncate."
                ),
            ),
        ),
    ),
)

FEATURES: tuple[Feature, ...] = (
    *INET_PAIRS,
    *OF_AN_ADDRESS,
    *MACADDR_BITS,
    *MACADDR8_BITS,
    TRUNC,
)
