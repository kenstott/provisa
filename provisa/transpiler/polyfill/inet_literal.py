# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7c5e93-6b1d-4f08-8e4a-3d9c1f7b5e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The inet and cidr functions on ClickHouse (REQ-2236; REQ-2171), which holds an address as its
text and has no inet type: of literal arguments, computed as the statement is read as network.c
computes them (an address's family, bits and mask length; inet_net_ntop's text, an IPv4 address
in an IPv6 one's last 32 bits written as IPv4), and written as a literal of the answer.
PostgreSQL folds an immutable function of constants as it plans the statement, so its error is
the statement's whatever rows it reads. An argument that is no literal is refused by name.

network_cmp is not here: its answer is memcmp's difference, whose value is the C library's."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import ipaddress
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Limitation
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite

_CLICKHOUSE = "clickhouse"


@dataclass(frozen=True)
class Inet:
    """An address as network.c holds one: its family (4 or 6), its bits, its mask length and
    whether it is a cidr."""

    family: int
    value: int
    bits: int
    cidr: bool = False

    @property
    def width(self) -> int:
        return 32 if self.family == 4 else 128

    def mask(self, bits: int | None = None) -> int:
        n = self.bits if bits is None else bits
        return ((1 << n) - 1) << (self.width - n) if n else 0


def _invalid(text: str, cidr: bool) -> Refused:
    kind = "cidr" if cidr else "inet"
    return Refused(Refusal("22P02", f'invalid input syntax for type {kind}: "{text}"'), ruling=True)


def parsed(text: str, cidr: bool) -> Inet:
    """inet_in and cidr_in of ``text``: an address and an optional mask length; a cidr with bits
    set past its mask raises 22P02, as cidr_in does."""
    address, _, length = text.strip().partition("/")
    try:
        ip = ipaddress.ip_address(address)
    except ValueError as refused:
        raise _invalid(text, cidr) from refused
    width = 32 if ip.version == 4 else 128
    if length:
        if not length.isdigit() or int(length) > width:
            raise _invalid(text, cidr)
        bits = int(length)
    else:
        bits = width
    held = Inet(ip.version, int(ip), bits, cidr)
    if cidr and held.value & ~held.mask() & ((1 << width) - 1):
        raise Refused(
            Refusal("22P02", f'invalid cidr value: "{text}"'),
            ruling=True,
        )
    return held


def _ipv6_text(value: int) -> str:
    """inet_net_ntop's text of an IPv6 address: the longest run of zero words compressed, an
    address whose first 96 bits are zero (but :: and ::1's kin of one word) or ::ffff:0:0/96 with
    its last 32 bits written as IPv4."""
    words = [(value >> (16 * (7 - i))) & 0xFFFF for i in range(8)]
    best_base, best_len, base, length = -1, 0, -1, 0
    for i, word in enumerate(words):
        if word == 0:
            if base < 0:
                base, length = i, 1
            else:
                length += 1
        elif base >= 0:
            if length > best_len:
                best_base, best_len = base, length
            base = -1
    if base >= 0 and length > best_len:
        best_base, best_len = base, length
    if best_len < 2:
        best_base = -1
    parts: list[str] = []
    i = 0
    while i < 8:
        if i == best_base:
            parts.append("")
            if i == 0:
                parts.append("")
            i += best_len
            if i == 8:
                parts.append("")
            continue
        if i == 6 and best_base == 0 and (best_len == 6 or (best_len == 5 and words[5] == 0xFFFF)):
            parts.append(str(ipaddress.IPv4Address(value & 0xFFFFFFFF)))
            break
        parts.append(f"{words[i]:x}")
        i += 1
    return ":".join(parts)


def _address_text(held: Inet) -> str:
    if held.family == 4:
        return str(ipaddress.IPv4Address(held.value))
    return _ipv6_text(held.value)


def written(held: Inet) -> str:
    """inet_out and cidr_out: the address, its mask length unless an inet's is the whole width."""
    text = _address_text(held)
    return text if held.bits == held.width and not held.cidr else f"{text}/{held.bits}"


def _abbreviated(held: Inet) -> str:
    """abbrev: an inet's text as inet_out writes it; a cidr's with its zero octets past the mask
    dropped (IPv4)."""
    if not held.cidr:
        return written(held)
    if held.family == 6:
        return f"{_address_text(held)}/{held.bits}"
    octets = [(held.value >> (8 * (3 - i))) & 0xFF for i in range(4)]
    kept = max(1, -(-held.bits // 8))
    while kept < 4 and any(octets[kept:]):
        kept += 1
    return ".".join(str(o) for o in octets[:kept]) + f"/{held.bits}"


def _bitncmp(left: int, right: int, bits: int, width: int) -> int:
    """The sign of the first ``bits`` bits' order (network.c's bitncmp, its sign only)."""
    shift = width - bits
    a, b = left >> shift, right >> shift
    return (a > b) - (a < b)


def _order(a: Inet, b: Inet) -> int:
    """inet_cmp_internal's sign: family, the shorter mask's bits, the mask lengths, all the bits."""
    if a.family != b.family:
        return -1 if a.family < b.family else 1
    order = _bitncmp(a.value, b.value, min(a.bits, b.bits), a.width)
    if order:
        return order
    if a.bits != b.bits:
        return -1 if a.bits < b.bits else 1
    return _bitncmp(a.value, b.value, a.width, a.width)


def _within(a: Inet, b: Inet, strict: bool) -> bool:
    """a << b (strict) or a <<= b: same family, a's mask longer, b's bits a's prefix."""
    if a.family != b.family or (a.bits <= b.bits if strict else a.bits < b.bits):
        return False
    return _bitncmp(a.value, b.value, b.bits, a.width) == 0


def _overlap(a: Inet, b: Inet) -> bool:
    return a.family == b.family and _bitncmp(a.value, b.value, min(a.bits, b.bits), a.width) == 0


def _set_masklen(held: Inet, bits: int) -> Inet:
    n = held.width if bits == -1 else bits
    if not 0 <= n <= held.width:
        raise Refused(Refusal("22023", f"invalid mask length: {bits}"), ruling=True)
    value = held.value & held.mask(n) if held.cidr else held.value
    return Inet(held.family, value, n, held.cidr)


def _as_cidr(held: Inet) -> Inet:
    return Inet(held.family, held.value & held.mask(), held.bits, True)


def _full(family: int, value: int) -> Inet:
    return Inet(family, value, 32 if family == 4 else 128)


_ONE: dict[str, Callable[[Inet], Any]] = {
    "host": _address_text,
    "family": lambda h: h.family,
    "masklen": lambda h: h.bits,
    "abbrev": _abbreviated,
    "netmask": lambda h: written(_full(h.family, h.mask())),
    "hostmask": lambda h: written(_full(h.family, ~h.mask() & ((1 << h.width) - 1))),
    "network": lambda h: written(_as_cidr(h)),
    "broadcast": lambda h: written(
        Inet(h.family, h.value | (~h.mask() & ((1 << h.width) - 1)), h.bits)
    ),
    "cidr": lambda h: written(_as_cidr(h)),
}
#: What each answers: text (an address's or its type's own text) or an integer.
_ANSWERS = {"family": "int", "masklen": "int"}
_TWO: dict[str, Callable[[Inet, Inet], Any]] = {
    "network_eq": lambda a, b: _order(a, b) == 0,
    "network_ne": lambda a, b: _order(a, b) != 0,
    "network_lt": lambda a, b: _order(a, b) < 0,
    "network_le": lambda a, b: _order(a, b) <= 0,
    "network_gt": lambda a, b: _order(a, b) > 0,
    "network_ge": lambda a, b: _order(a, b) >= 0,
    "network_larger": lambda a, b: written(a if _order(a, b) > 0 else b),
    "network_smaller": lambda a, b: written(a if _order(a, b) < 0 else b),
    "network_overlap": _overlap,
    "network_sub": lambda a, b: _within(a, b, strict=True),
    "network_subeq": lambda a, b: _within(a, b, strict=False),
    "network_sup": lambda a, b: _within(b, a, strict=True),
    "network_supeq": lambda a, b: _within(b, a, strict=False),
}
NAMES = frozenset({*_ONE, *_TWO, "set_masklen"})
_ADDRESSES = frozenset({"inet", "cidr"})


def _peeled(node: exp.Expr) -> exp.Expr:
    node = node.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return node


def _address(node: exp.Expr, name: str, dialect: str) -> Inet | None:
    """An inet or cidr the statement writes: its value, None for a NULL; refused by name for any
    other value."""
    said = trino.said(node)
    if said not in _ADDRESSES:
        raise Refused(trino.no_form(name, dialect))
    inner = _peeled(node)
    if isinstance(inner, exp.Null):
        return None
    if not (isinstance(inner, exp.Literal) and inner.is_string):
        raise Refused(trino.no_form(name, dialect))
    return parsed(inner.this, said == "cidr")


def _integer(node: exp.Expr, name: str, dialect: str) -> int | None:
    inner = node.unnest()
    while isinstance(inner, exp.Cast):
        inner = inner.this.unnest()
    negative = False
    while isinstance(inner, (exp.Neg, exp.Paren)):
        negative ^= isinstance(inner, exp.Neg)
        inner = inner.this.unnest()
    if isinstance(inner, exp.Null):
        return None
    text = inner.this.strip() if isinstance(inner, exp.Literal) else ""
    if not text.lstrip("-").isdigit():
        raise Refused(trino.no_form(name, dialect))
    return -int(text) if negative else int(text)


def _literal(answer: Any, answers: str) -> exp.Expr:
    if answers == "bool":
        to, value = "Nullable(Bool)", exp.Null() if answer is None else exp.Boolean(this=answer)
    elif answers == "int":
        to, value = "Nullable(Int32)", exp.Null() if answer is None else exp.Literal.number(answer)
    else:
        to = "Nullable(String)"
        value = exp.Null() if answer is None else exp.Literal.string(str(answer))
    return exp.cast(value, to, dialect=_CLICKHOUSE)


def literal_call(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call of one of ``NAMES`` of literal addresses: its answer, computed as the statement is
    read, as a literal."""
    call = trino.called(node)
    if call is None or not (found := call[0] & NAMES):
        return None
    name, args = min(found), call[1]
    if name in _ONE and len(args) == 1:
        held = _address(args[0], name, dialect)
        answer = None if held is None else _ONE[name](held)
        return _literal(answer, _ANSWERS.get(name, "text"))
    if name in _TWO and len(args) == 2:
        a, b = (_address(arg, name, dialect) for arg in args)
        answer = None if a is None or b is None else _TWO[name](a, b)
        answers = "text" if name in ("network_larger", "network_smaller") else "bool"
        return _literal(answer, answers)
    if name == "set_masklen" and len(args) == 2:
        held, bits = _address(args[0], name, dialect), _integer(args[1], name, dialect)
        answer = None if held is None or bits is None else written(_set_masklen(held, bits))
        return _literal(answer, "text")
    return None


def rewrite_of(name: str) -> Rewrite:
    """:func:`literal_call` of a call of ``name`` alone: each row answers its own function."""

    def only(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if dialect != _CLICKHOUSE:
            return None
        call = trino.called(node)
        return literal_call(node, dialect) if call is not None and name in call[0] else None

    only.__doc__ = literal_call.__doc__
    only.__qualname__ = only.__name__ = f"{name}_of_literal_addresses"
    return Rewrite(only, mentions=rf"\b{name}\s*\(")  # a call by its name alone is read too


LITERALS = Limitation(
    "Of literal addresses (and a literal mask length) only, computed as the statement is read as "
    "network.c computes them: ClickHouse has no inet type, and holds an address as its text. Any "
    "other argument is refused by name."
)
