# Copyright (c) 2026 Kenneth Stott
# Canary: 4b1d7e93-6a2c-4f85-9e30-c8a5f1d26b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's exp, ln and tanh of a double on ClickHouse, computed as glibc computes them
(REQ-2236).

PostgreSQL's dexp, dlog1 and tanh are the C library's exp, log and tanh. ClickHouse's own exp, log
and tanh are not: the 24.3 server answers each with a vectorized approximation (exp(0.5) is
1.6487212707014907 there, 1.6487212707001282 in glibc). So each is written here as glibc's own
algorithm, step for step, in ClickHouse's double arithmetic, whose +, -, * and / each round once to
nearest as C's do: exp and log are Arm's optimized-routines (glibc's e_exp.c and e_log.c, with
their 128-entry tables), tanh is fdlibm's s_tanh.c of fdlibm's s_expm1.c.

glibc runs each in the build its loader picks for the processor: on x86-64 with FMA (every x86-64
since 2013) and on AArch64, a build in which the compiler fuses each product into the sum it feeds
(GCC's -ffp-contract=fast), so that sum rounds once. Each fused multiply-add here is computed
exactly as the processor's fma is: the product split into two doubles (Dekker), added to the
addend (Knuth's two-sum), and their tail rounded to odd before the one rounding to nearest
(Boldo and Melquiond, "Emulation of FMA and correctly rounded sums: proved algorithms using
rounding to odd", IEEE TC 2008), exact while no step underflows or overflows, which no operand of
these algorithms reaches.

Each value an algorithm names is bound once, as a lambda's parameter (an arrayMap over a one
element array); a value is bound in the lambda right inside the one that binds the last value it
reads, so the nesting is as deep as the longest chain of steps, not as long as the program."""

# Requirements: REQ-2236

from __future__ import annotations

import re
import struct
import sys
from collections.abc import Callable

# sqlglot reads and writes these forms by descending about 40 Python frames per lambda, and a form
# nests up to 53 (tanh's), past the default limit of 1000. The frames are Python's own, not the C
# stack's (Python 3.12), so the limit is raised as provisa.cypher.expr_parser raises it.
_RECURSION_LIMIT = 20000
if sys.getrecursionlimit() < _RECURSION_LIMIT:
    sys.setrecursionlimit(_RECURSION_LIMIT)

#: A value a program names: ``_libm`` and its number.
_NAMED = re.compile(r"\b_libm\d+\b")


def _bits(value: float) -> int:
    return struct.unpack("<Q", struct.pack("<d", value))[0]


def _double(value: float) -> str:
    """``value`` as ClickHouse reads it exactly: the double of its 64 bits."""
    return f"reinterpretAsFloat64(toUInt64({_bits(value)}))"


def _table(entries: tuple[int, ...]) -> str:
    return "[" + ", ".join(str(entry) for entry in entries) + "]"


#: 2^64, the modulus of C's uint64_t arithmetic: a - b is written a + (2^64 - b), a sum of two
#: UInt64, which ClickHouse wraps as C does.
_WRAP = 1 << 64


class _Program:
    """A straight-line program of named doubles and integers, written as nested lambdas.

    A value read once, or marked cheap (one step of values already named), is written where it is
    read instead of named: ClickHouse nests lambdas 64 deep at most."""

    def __init__(self, argument: str) -> None:
        self._count = 0
        self._values: dict[str, str] = {}
        self._cheap: set[str] = set()
        self.x = self.let(f"toFloat64({argument})")

    def let(self, value: str, *, cheap: bool = False) -> str:
        """A name bound to ``value``, which reads the names bound before it."""
        name = f"_libm{self._count}"
        self._count += 1
        self._values[name] = value
        if cheap:
            self._cheap.add(name)
        return name

    def fma(self, a: str, b: str, c: str) -> str:
        """a * b + c rounded once, as the processor's fused multiply-add rounds it."""
        let = self.let
        product = let(f"{a} * {b}")
        a_hi = let(f"134217729.0 * {a} - (134217729.0 * {a} - {a})")
        b_hi = let(f"134217729.0 * {b} - (134217729.0 * {b} - {b})")
        a_lo, b_lo = let(f"({a} - {a_hi})", cheap=True), let(f"({b} - {b_hi})", cheap=True)
        product_lo = let(
            f"(({a_hi} * {b_hi} - {product}) + {a_hi} * {b_lo} + {a_lo} * {b_hi}) + {a_lo} * {b_lo}"
        )
        total = let(f"{c} + {product}")
        virtual = let(f"({total} - {c})", cheap=True)
        total_lo = let(f"({c} - ({total} - {virtual})) + ({product} - {virtual})")
        tail = let(f"{total_lo} + {product_lo}")
        tail_virtual = let(f"({tail} - {total_lo})", cheap=True)
        tail_error = let(
            f"({total_lo} - ({tail} - {tail_virtual})) + ({product_lo} - {tail_virtual})"
        )
        tail_bits = let(f"reinterpretAsUInt64({tail})", cheap=True)
        odd = let(
            f"if({tail_error} = 0 OR bitAnd({tail_bits}, 1) = 1, {tail}, reinterpretAsFloat64("
            f"if(({tail_error} > 0) = ({tail} > 0), {tail_bits} + 1, {tail_bits} + {_WRAP - 1})))"
        )
        return let(f"{total} + {odd}")

    def written(self, answer: str) -> str:
        """The program, answering ``answer`` (of its names) inside its innermost lambda."""
        values = dict(self._values)
        while True:
            read = [n for text in (*values.values(), answer) for n in _NAMED.findall(text)]
            once = [
                name
                for name in reversed(values)
                if name != self.x and (read.count(name) == 1 or name in self._cheap)
            ]
            if not once:
                break
            name = once[0]
            value = values.pop(name)
            pattern = re.compile(rf"\b{name}\b")
            values = {n: pattern.sub(f"({value})", text) for n, text in values.items()}
            answer = pattern.sub(f"({value})", answer)
        level: dict[str, int] = {}
        levels: list[list[tuple[str, str]]] = []
        for name, value in values.items():
            depth = 1 + max((level[n] for n in _NAMED.findall(value)), default=-1)
            level[name] = depth
            while len(levels) <= depth:
                levels.append([])
            levels[depth].append((name, value))
        written = answer
        for bound in reversed(levels):
            names = ", ".join(name for name, _ in bound)
            arrays = ", ".join(f"[{value}]" for _, value in bound)
            written = f"arrayElement(arrayMap(({names}) -> {written}, {arrays}), 1)"
        return written


def _top12(p: _Program, x: str) -> str:
    return p.let(f"bitAnd(bitShiftRight(reinterpretAsUInt64({x}), 52), 2047)")


def planned(sql: str, dialect: str) -> str:
    """A ClickHouse statement holding one of these forms, as ClickHouse plans it in a tenth of a
    second: by its old analyzer. The new one (24.3's default) takes seconds over these nested
    lambdas (log's 4 s on chDB 26, more on a constant), the old one 0.07 s, with the same answers,
    as pg_forms_geometric.planned plans a shape's form. A statement that already says its settings
    is left as it is."""
    if dialect != "clickhouse" or not _NAMED.search(sql) or "SETTINGS" in sql.upper():
        return sql
    return f"{sql.rstrip().rstrip(';')} SETTINGS allow_experimental_analyzer = 0"


#: What a form answers, given the name of its argument and the name of the function's value.
Answer = Callable[[str, str], str]

_INFINITY = "reinterpretAsFloat64(toUInt64(9218868437227405312))"


def _constant(p: _Program, value: float) -> str:
    return p.let(_double(value), cheap=True)


# --- exp: optimized-routines' exp.c, N = 128, a polynomial of order 5 ---------------------------

_INVLN2N = float.fromhex("0x1.71547652b82fep0") * 128
_NEGLN2HIN = float.fromhex("-0x1.62e42fefa0000p-8")
_NEGLN2LON = float.fromhex("-0x1.cf79abc9e3b3ap-47")
_SHIFT = float.fromhex("0x1.8p52")
_EXP_POLY = tuple(
    float.fromhex(c)
    for c in ("0x1.ffffffffffdbdp-2", "0x1.555555555543cp-3", "0x1.55555cf172b91p-5",
              "0x1.1111167a4d017p-7")
)  # fmt: skip


def exp(argument: str, answer: Answer) -> str:
    """glibc's exp of ``argument``: NaN of NaN, 0 of -Infinity, Infinity of Infinity and of a
    value of 1024 or more, 0 of one of -1024 or less, as exp.c answers each."""
    p = _Program(argument)
    let, fma, x = p.let, p.fma, p.x
    c2, c3, c4, c5 = (_constant(p, c) for c in _EXP_POLY)
    shift = _constant(p, _SHIFT)
    abstop = _top12(p, x)
    kd_shifted = fma(_constant(p, _INVLN2N), x, shift)
    ki = let(f"reinterpretAsUInt64({kd_shifted})")
    kd = let(f"{kd_shifted} - {shift}")
    # kd * NegLn2hiN is exact (kd an integer below 2^18, NegLn2hiN of 36 bits): fused or not alike
    r = fma(kd, _constant(p, _NEGLN2LON), let(f"{x} + {kd} * {_double(_NEGLN2HIN)}"))
    index = let(f"bitAnd({ki}, 127) * 2 + 1")
    table = let(_table(_EXP_TABLE), cheap=True)
    tail = let(f"reinterpretAsFloat64(arrayElement({table}, {index}))")
    sbits = let(f"arrayElement({table}, {index} + 1) + bitShiftLeft({ki}, 45)")
    r2 = let(f"{r} * {r}")
    tmp = fma(let(f"{r2} * {r2}"), fma(r, c5, c4), fma(r2, fma(r, c3, c2), let(f"{tail} + {r}")))
    scale = let(f"reinterpretAsFloat64({sbits})")
    normal = fma(scale, tmp, scale)
    # specialcase, k > 0: the scale's exponent lowered by 1009, raised again after the sum
    high_scale = let(f"reinterpretAsFloat64({sbits} + {_WRAP - (1009 << 52)})")
    high = let(f"{_double(2.0**1009)} * {fma(high_scale, tmp, high_scale)}")
    # k < 0: raised by 1022, the sum rounded at a subnormal's precision before it is lowered
    low_scale = let(f"reinterpretAsFloat64({sbits} + {1022 << 52})")
    # here the product is not fused: it feeds a sum in another block too (GCC fuses within one)
    low_product = let(f"{low_scale} * {tmp}")
    low_y = let(f"{low_scale} + {low_product}")
    low_lo = let(f"({low_scale} - {low_y}) + {low_product}")
    low_hi = let(f"1.0 + {low_y}")
    low_sum = let(f"((1.0 - {low_hi}) + {low_y}) + {low_lo}")
    low = let(f"{_double(2.0**-1022)} * if({low_y} < 1.0, ({low_hi} + {low_sum}) - 1.0, {low_y})")
    special = let(f"if(bitAnd({ki}, 2147483648) = 0, {high}, {low})")
    value = let(
        f"multiIf(isNaN({x}), {x}, {abstop} < 969, 1.0 + {x}, "
        f"{abstop} >= 1033, if({x} > 0, {_INFINITY}, 0.0), {abstop} >= 1032, {special}, {normal})"
    )
    return p.written(answer(x, value))


# --- log: optimized-routines' log.c, N = 128, polynomials of order 6 and 12 ---------------------

_LN2HI = float.fromhex("0x1.62e42fefa3800p-1")
_LN2LO = float.fromhex("0x1.ef35793c76730p-45")
_OFF = 0x3FE6000000000000
_LOG_POLY = tuple(
    float.fromhex(c)
    for c in ("-0x1.0000000000001p-1", "0x1.555555551305bp-2", "-0x1.fffffffeb4590p-3",
              "0x1.999b324f10111p-3", "-0x1.55575e506c89fp-3")
)  # fmt: skip
_LOG_POLY1 = tuple(
    float.fromhex(c)
    for c in ("-0x1.0000000000000p-1", "0x1.5555555555577p-2", "-0x1.ffffffffffdcbp-3",
              "0x1.999999995dd0cp-3", "-0x1.55555556745a7p-3", "0x1.24924a344de30p-3",
              "-0x1.fffffa4423d65p-4", "0x1.c7184282ad6cap-4", "-0x1.999eb43b068ffp-4",
              "0x1.78182f7afd085p-4", "-0x1.5521375d145cdp-4")
)  # fmt: skip
#: The bits of 1 - 2^-4 and of 1 + 0x1.09p-4: log.c's interval about 1 of its own polynomial.
_NEAR_ONE = (_bits(1.0 - 2.0**-4), _bits(1.0 + float.fromhex("0x1.09p-4")))


def log(argument: str, answer: Answer) -> str:
    """glibc's log of ``argument`` above zero: NaN of NaN, Infinity of Infinity. A form raises
    PostgreSQL's error of zero and of a negative before it reads this value."""
    p = _Program(argument)
    let, fma, x = p.let, p.fma, p.x
    ix = let(f"reinterpretAsUInt64({x})")
    # close to 1: r = x - 1 in the order 12 polynomial, r^2/2 split so its square is exact
    b = [_constant(p, c) for c in _LOG_POLY1]
    r = let(f"{x} - 1.0")
    r2, r3 = let(f"{r} * {r}"), let(f"{r} * {r} * {r}")
    inner = fma(r3, b[10], fma(r2, b[9], fma(r, b[8], b[7])))
    inner = fma(r3, inner, fma(r2, b[6], fma(r, b[5], b[4])))
    inner = fma(r3, inner, fma(r2, b[3], fma(r, b[2], b[1])))
    w = let(f"{r} * 134217728.0")
    rhi = let(f"({r} + {w}) - {w}")
    rlo = let(f"{r} - {rhi}")
    w2 = let(f"{rhi} * {rhi} * -0.5")
    near_hi = let(f"{r} + {w2}")
    near_lo = fma(let(f"-0.5 * {rlo}"), let(f"{rhi} + {r}"), let(f"({r} - {near_hi}) + {w2}"))
    near = let(f"{fma(r3, inner, near_lo)} + {near_hi}")
    # elsewhere: x = 2^k z, z in [OFF, 2 OFF); a subnormal first raised by 2^52
    normalized = let(
        f"if({x} < {_double(2.0**-1022)}, "
        f"reinterpretAsUInt64({x} * 4503599627370496.0) + {_WRAP - (52 << 52)}, {ix})"
    )
    tmp = let(f"{normalized} + {_WRAP - _OFF}")
    i = let(f"bitAnd(bitShiftRight({tmp}, 45), 127) * 2 + 1")
    kd = let(f"toFloat64(bitShiftRight({tmp}, 52)) - if(bitShiftRight({tmp}, 63) = 1, 4096.0, 0.0)")
    table = let(_table(_LOG_TABLE), cheap=True)
    invc = let(f"reinterpretAsFloat64(arrayElement({table}, {i}))")
    logc = let(f"reinterpretAsFloat64(arrayElement({table}, {i} + 1))")
    z = let(f"reinterpretAsFloat64(bitAnd({tmp}, 4503599627370495) + {_OFF})")
    rr = fma(z, invc, "-1.0")
    # kd * Ln2hi is exact (Ln2hi's last 11 bits are zero): fused or not alike
    wk = let(f"{kd} * {_double(_LN2HI)} + {logc}")
    hi = let(f"{wk} + {rr}")
    lo = fma(kd, _constant(p, _LN2LO), let(f"({wk} - {hi}) + {rr}"))
    a = [_constant(p, c) for c in _LOG_POLY]
    rr2 = let(f"{rr} * {rr}")
    poly = fma(rr2, fma(rr, a[4], a[3]), fma(rr, a[2], a[1]))
    far = let(f"{fma(let(f'{rr} * {rr2}'), poly, fma(rr2, a[0], lo))} + {hi}")
    value = let(
        f"multiIf(isNaN({x}) OR {x} = {_INFINITY}, {x}, {ix} = {_bits(1.0)}, 0.0, "
        f"{ix} >= {_NEAR_ONE[0]} AND {ix} < {_NEAR_ONE[1]}, {near}, {far})"
    )
    return p.written(answer(x, value))


# --- tanh: fdlibm's s_tanh.c of s_expm1.c, as glibc holds them ----------------------------------

_LN2_HI = float.fromhex("0x1.62e42fee00000p-1")
_LN2_LO = float.fromhex("0x1.a39ef35793c76p-33")
_INVLN2 = float.fromhex("0x1.71547652b82fep0")
_EXPM1_Q = (
    -3.33333333333331316428e-02,
    1.58730158725481460165e-03,
    -7.93650757867487942473e-05,
    4.00821782732936239552e-06,
    -2.01099218183624371326e-07,
)


def _high(p: _Program, x: str) -> str:
    """The high word of |x|: its exponent and the top 20 bits of its fraction."""
    return p.let(f"bitShiftRight(bitAnd(reinterpretAsUInt64({x}), 9223372036854775807), 32)")


def _expm1(p: _Program, x: str) -> str:
    """s_expm1.c of ``x``, 2^-54 <= |x| < 44: tanh's arguments."""
    let, fma = p.let, p.fma
    hx = _high(p, x)
    positive = f"{x} > 0"
    near = let(f"{hx} < 1072734898")  # |x| < 1.5 ln2: k is -1 or 1
    reduced = let(f"{hx} > 1071001154")  # |x| > 0.5 ln2: k is not 0
    k_far = let(f"trunc({fma(_constant(p, _INVLN2), x, let(f'if({positive}, 0.5, -0.5)'))})")
    k = let(f"if({reduced}, if({near}, if({positive}, 1.0, -1.0), {k_far}), 0.0)")
    # t * ln2_hi is exact (ln2_hi's last 32 bits are zero)
    hi = let(f"{x} - {k} * {_double(_LN2_HI)}")
    lo = let(f"{k} * {_double(_LN2_LO)}")
    xr = let(f"if({reduced}, {hi} - {lo}, {x})")
    c = let(f"if({reduced}, ({hi} - {xr}) - {lo}, 0.0)")
    q = [_constant(p, value) for value in _EXPM1_Q]
    hfx = let(f"0.5 * {xr}")
    hxs = let(f"{xr} * {hfx}")
    h2 = let(f"{hxs} * {hxs}")
    h4 = let(f"{h2} * {h2}")
    r1 = fma(h4, fma(hxs, q[4], q[3]), fma(h2, fma(hxs, q[2], q[1]), fma(hxs, q[0], "1.0")))
    t = fma(let(f"-{r1}"), hfx, "3.0")
    e = let(f"{hxs} * (({r1} - {t}) / {fma(let(f'-{xr}'), t, '6.0')})")
    k0 = let(f"{xr} - {fma(xr, e, let(f'-{hxs}'))}")
    ek = let(f"{fma(xr, let(f'{e} - {c}'), let(f'-{c}'))} - {hxs}")
    k_minus_one = let(f"0.5 * ({xr} - {ek}) - 0.5")
    k_one = let(f"if({xr} < -0.25, -2.0 * ({ek} - ({xr} + 0.5)), 1.0 + 2.0 * ({xr} - {ek}))")
    two_k = let(f"reinterpretAsFloat64(bitShiftLeft(toUInt64(1023.0 + {k}), 52))")
    two_minus_k = let(f"reinterpretAsFloat64(bitShiftLeft(toUInt64(1023.0 - {k}), 52))")
    wide = let(f"(1.0 - ({ek} - {xr})) * {two_k} - 1.0")
    small = let(f"((1.0 - {two_minus_k}) - ({ek} - {xr})) * {two_k}")
    large = let(f"(({xr} - ({ek} + {two_minus_k})) + 1.0) * {two_k}")
    return let(
        f"multiIf({k} = 0, {k0}, {k} = -1, {k_minus_one}, {k} = 1, {k_one}, "
        f"{k} <= -2 OR {k} > 56, {wide}, {k} < 20, {small}, {large})"
    )


def tanh(argument: str, answer: Answer) -> str:
    """glibc's tanh of ``argument``: NaN of NaN, 1 and -1 of the infinities and past 22."""
    p = _Program(argument)
    let, x = p.let, p.x
    hx = _high(p, x)
    magnitude = let(f"abs({x})")
    t = _expm1(p, let(f"if({hx} >= 1072693248, 2.0 * {magnitude}, -2.0 * {magnitude})"))
    z = let(f"if({hx} >= 1072693248, 1.0 - 2.0 / ({t} + 2.0), -{t} / ({t} + 2.0))")
    signed = let(f"if({x} < 0, -{z}, {z})")
    value = let(
        f"multiIf(isNaN({x}), {x}, {hx} >= 1077280768, if({x} < 0, -1.0, 1.0), {x} = 0, {x}, "
        f"{hx} < 1015021568, {x} * (1.0 + {x}), {signed})"
    )
    return p.written(answer(x, value))


# --- the tables: optimized-routines exp_data.c and log_data.c, N = 128, as 64-bit patterns ----

#: exp_data.c tab: tab[2k] the bits of T[k], tab[2k+1] those of 2^(k/128) less k << 45.
_EXP_TABLE = (
    0,
    4607182418800017408,
    4367149474599452526,
    4607171688528556853,
    13578739988850859101,
    4607161091040444513,
    13587463954133116625,
    4607150627056680837,
    4363093389540324883,
    4607140297302181236,
    13586179750647926522,
    4607130102505797342,
    4350749156061923425,
    4607120043400338376,
    4364168240100004761,
    4607110120722592643,
    4366128403083131757,
    4607100335213349135,
    4363624214708482044,
    4607090687617419255,
    4341116261916187857,
    4607081178683658658,
    13576667550842908171,
    4607071809164989211,
    13588862361555418382,
    4607062579818421073,
    13569487443606825557,
    4607053491405074892,
    13587862706032342038,
    4607044544690204128,
    4359717415120819631,
    4607035740443217489,
    13582886257094398792,
    4607027079437701499,
    13587113717893313085,
    4607018562451443178,
    4362731899485115327,
    4607010190266452853,
    13589288191656061734,
    4607001963668987092,
    4367063335334195040,
    4606993883449571754,
    13587333149746411677,
    4606985950403025181,
    4362421515177083789,
    4606978165328481494,
    4359618604155798653,
    4606970529029414038,
    4365834109879625876,
    4606963042313658936,
    4361558225371547773,
    4606955705993438783,
    4359562691856033778,
    4606948520885386461,
    13589251765121910878,
    4606941487810569089,
    4360319936817113905,
    4606934607594512097,
    13564034602756980730,
    4606927881067223440,
    4362641697437125300,
    4606921309063217931,
    4354440509323660175,
    4606914892421541718,
    4360414030434708406,
    4606908631985796885,
    4361245865020257356,
    4606902528604166193,
    4363949802402347082,
    4606896583129437951,
    13584725097160006404,
    4606890796419031025,
    13588005410925400168,
    4606885169335019979,
    13589902112758771086,
    4606879702744160358,
    13578865146911274706,
    4606874397517914103,
    13589920948748688149,
    4606869254532475109,
    4361066948569222253,
    4606864274668794914,
    13588224706282547594,
    4606859458812608540,
    13571781710912653659,
    4606854807854460458,
    13581779622207003163,
    4606850322689730711,
    4342424336897059857,
    4606846004218661165,
    4365789078613288175,
    4606841853346381911,
    4354643160121541497,
    4606837870982937808,
    4365113450260533808,
    4606834058043315166,
    4356828907110576048,
    4606830415447468583,
    4364865165386375287,
    4606826944120347917,
    13587241551866733454,
    4606823644991925415,
    13585071772441748297,
    4606820518997222978,
    13580849621492512863,
    4606817567076339586,
    13584565421834198262,
    4606814790174478863,
    4364890759862540174,
    4606812189241976794,
    4358244976955136773,
    4606809765234329597,
    4364097860734309385,
    4606807519112221737,
    4338311716453074666,
    4606805451841554103,
    4365169902980743221,
    4606803564393472328,
    13586485183986103967,
    4606801857744395273,
    13586291098077056424,
    4606800332876043653,
    13584074153218795950,
    4606798990775468832,
    13588431951308937304,
    4606797832435081765,
    4347962289735735652,
    4606796858852682099,
    13588402342996091432,
    4606796071031487437,
    13576684563168803202,
    4606795469980162751,
    13574807981113846254,
    4606795056712849967,
    13583098632124532817,
    4606794832249197700,
    13580639192812764879,
    4606794797614391156,
    13584807330835037616,
    4606794953839182196,
    13584573029057685130,
    4606795301959919561,
    4364160436192007379,
    4606795843018579263,
    13581505024848930077,
    4606796578062795143,
    4365374511615819883,
    4606797508145889586,
    4352143969406577749,
    4606798634326904418,
    13585286144244043586,
    4606799957670631954,
    13588320735159072564,
    4606801479247646227,
    13582960398152892190,
    4606803200134334381,
    13588719419377473390,
    4606805121412928237,
    4364178698342926848,
    4606807244171536025,
    4363345029737015988,
    4606809569504174299,
    13575457177841559551,
    4606812098510800012,
    4358348108421357380,
    4606814832297342774,
    13588214532259331459,
    4606817771975737286,
    13573191462042738217,
    4606820918663955941,
    13577266531058891097,
    4606824273486041615,
    13587996981358557610,
    4606827837572140624,
    13584486419852070475,
    4606831612058535866,
    4355455812241575463,
    4606835598087680144,
    4354367543587795899,
    4606839796808229667,
    13585043422533788259,
    4606844209375077733,
    4362612029712904661,
    4606848836949388595,
    13587848060628417301,
    4606853680698631517,
    4364212552380330450,
    4606858741796615000,
    4360726356711135923,
    4606864021423521215,
    13583665500206833076,
    4606869520765940602,
    4362891239881388935,
    4606875241016906669,
    13587495931142593018,
    4606881183375930982,
    13570968390383480506,
    4606887349049038331,
    13587540456847636032,
    4606893739248802106,
    13576337421119623988,
    4606900355194379847,
    13587904206984481333,
    4606907198111549002,
    4356388038631218637,
    4606914269232742866,
    13587850440691849388,
    4606921569797086729,
    4355946959017544883,
    4606929101050434204,
    4363674894410448276,
    4606936864245403770,
    4360857247493478828,
    4606944860641415499,
    4351547817417644647,
    4606953091504727989,
    4352565403421120872,
    4606961558108475497,
    13583008314044495314,
    4606970261732705274,
    13587269216817379951,
    4606979203664415100,
    13587375117699492169,
    4606988385197591026,
    4356286533989107623,
    4606997807633245319,
    13583579534434771471,
    4607007472279454616,
    4363524392198705836,
    4607017380451398277,
    4360674496013757259,
    4607027533471396960,
    13583653763942727199,
    4607037932668951391,
    13585436725991640521,
    4607048579380781353,
    4356306869484685523,
    4607059474950864886,
    4360546482996998769,
    4607070620730477699,
    13587350259894555120,
    4607082018078232794,
    13586707622498890877,
    4607093668360120305,
    4363640302291525640,
    4607105572949547559,
    4360658257139329842,
    4607117733227379351,
    4362409866909812206,
    4607130150581978432,
    13586858401655940077,
    4607142826409246228,
    4357102052301720989,
    4607155762112663768,
    4346824392926350473,
    4607168959103332849,
)
#: log_data.c tab: invc and logc of each of 128 subintervals of [OFF, 2 OFF).
_LOG_TABLE = (
    4609210952320081567,
    13823742429115092992,
    4609174151535323423,
    13823640652485558272,
    4609137762941711776,
    13823539447239663616,
    4609101780076432509,
    13823438808184027136,
    4609066195948516561,
    13823338728338257920,
    4609031003647708346,
    13823239200618053632,
    4608996196778918435,
    13823140219078086656,
    4608961770493910745,
    13823041781908066304,
    4608927715981993156,
    13822943875669114880,
    4608894029497490452,
    13822846501147660288,
    4608860704429464652,
    13822749650639759360,
    4608827733622991951,
    13822653314525870080,
    4608795113711964382,
    13822557493987864576,
    4608762837695273592,
    13822462179321683968,
    4608730900845713031,
    13822367367239729152,
    4608699297346055123,
    13822273050981228544,
    4608668021725560723,
    13822179224546084864,
    4608637069496305027,
    13822085884644134912,
    4608606436072799711,
    13821993027486939136,
    4608576115407380183,
    13821900644610078720,
    4608546104468711488,
    13821808736507762688,
    4608516395921613768,
    13821717290277343232,
    4608486988337653130,
    13821626310950369280,
    4608457874380981300,
    13821524313851228160,
    4608429050692185355,
    13821344166540980224,
    4608400513723603028,
    13821164919435833344,
    4608372259037676634,
    13820986562460626944,
    4608344281559988475,
    13820809081066614784,
    4608316577295561905,
    13820632467097620480,
    4608289143243931279,
    13820456718410780672,
    4608261974399864041,
    13820281819674775552,
    4608235066966982955,
    13820107762892525568,
    4608208418496699064,
    13819934548492980224,
    4608182024348356610,
    13819762162364444672,
    4608155880746416668,
    13819590595620384768,
    4608129984310884066,
    13819419841628848128,
    4608104331790373952,
    13819249894299545600,
    4608078919386516260,
    13819080743594389504,
    4608053745083813193,
    13818912391057334272,
    4608028803162061200,
    13818744813135568896,
    4608004092140582977,
    13818578014346424320,
    4607979607715064273,
    13818411979930480640,
    4607955348240203050,
    13818246712844746752,
    4607931308525231019,
    13818082191617490944,
    4607907488200963376,
    13817918427446706176,
    4607883882914095605,
    13817755403999789056,
    4607860489364257727,
    13817593111812612096,
    4607837304735659453,
    13817431544482799616,
    4607814327318677563,
    13817270703104663552,
    4607791553269285046,
    13817110573604577280,
    4607768980718633266,
    13816858654215471104,
    4607746606204386459,
    13816541216839319552,
    4607724427722704907,
    13816225174501539840,
    4607702441940444521,
    13815910503988895744,
    4607680648182201980,
    13815597219701547008,
    4607659041870259945,
    13815285279565053952,
    4607637621113100982,
    13814974679547559936,
    4607616384371159229,
    13814665420431163392,
    4607595329002388662,
    13814357486602813440,
    4607574451424080346,
    13814050848089014272,
    4607553751206027229,
    13813745520761495552,
    4607533225893614156,
    13813441490475196416,
    4607512871202519514,
    13813138715335548928,
    4607492687924081969,
    13812837228489089024,
    4607472672061011119,
    13812533925724160000,
    4607452821607836595,
    13811935932589752320,
    4607433134784028622,
    13811340407893983232,
    4607413609963496737,
    13810747343165898752,
    4607394244387313690,
    13810156694749233152,
    4607375037409146577,
    13809568482684469248,
    4607355985284043577,
    13808982631454703616,
    4607337087808759775,
    13808399173480169472,
    4607318342423685371,
    13807599678720278528,
    4607299746773962521,
    13806442102614294528,
    4607281300320876844,
    13805289202348294144,
    4607263000134495376,
    13804140868705845248,
    4607244845192359337,
    13802461362887655424,
    4607226833383535273,
    13800182875165425664,
    4607208962367583048,
    13796797265813962752,
    4607191232174928277,
    13790026497742077952,
    4607147371718441498,
    4571135975984463872,
    4607078088715659892,
    4577869718869573632,
    4607009867155216686,
    4581232448741998592,
    4606942683337764301,
    4583432697401638912,
    4606876514109166567,
    4585140340856520704,
    4606811335652258219,
    4586223922483150848,
    4606747126592769179,
    4587299414251962368,
    4606683865489775029,
    4588366935595909120,
    4606621530588224086,
    4589297319838007296,
    4606560103363492760,
    4589823285785337856,
    4606499562142339331,
    4590345449255731200,
    4606439889401765070,
    4590863851743330304,
    4606381066660273821,
    4591378546364645376,
    4606323074200710349,
    4591889600642572288,
    4606265896982771970,
    4592397044266393600,
    4606209516050516077,
    4592900944725221376,
    4606153914748590084,
    4593401352421269504,
    4606099078592044940,
    4593784960540819456,
    4606044990914488127,
    4594031733641490432,
    4605991636799559361,
    4594276827212165120,
    4605939000208149032,
    4594520269598838784,
    4605887068574268972,
    4594762074469634048,
    4605835828034898059,
    4595002262718939136,
    4605785262199222346,
    4595240868508725248,
    4605735360665939838,
    4595477899084623872,
    4605686108857056674,
    4595713382617092096,
    4605637494364657831,
    4595947338221678592,
    4605589505339650955,
    4596179783459962880,
    4605542130267147047,
    4596410735360753664,
    4605495355974611665,
    4596640220181168128,
    4605449172481162267,
    4596868249655824384,
    4605403567997038992,
    4597094845431726080,
    4605358530648067631,
    4597320030710423552,
    4605314051900459893,
    4597543812985176064,
    4605270121620391992,
    4597766208615342080,
    4605226727711920870,
    4597987244934402048,
    4605183861247170461,
    4598191076868601856,
    4605141513151447164,
    4598300254877765632,
    4605099672569037582,
    4598408778231476224,
    4605058332693305339,
    4598516648831078400,
    4605017483302377992,
    4598623877854349312,
    4604977115906260565,
    4598730472393834496,
    4604937222866862688,
    4598836437669079040,
    4604897794226955049,
    4598941785460615168,
    4604858822028998061,
    4599046522653597696,
    4604820300194325954,
    4599150651412060160,
    4604782219042948695,
    4599254183977103360,
    4604744573281243916,
    4599357121047289856,
)
