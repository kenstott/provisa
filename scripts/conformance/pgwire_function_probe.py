# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1e7f0a-52b4-4d86-9f27-b6a08d4e1f53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How the pgwire conformance probe calls a pg_catalog function (REQ-2167, amended 2026-10-29, PG_CATALOG
FUNCTIONS ARE GAPS).

Every overload pg_proc lists in pg_catalog is a case: nothing is left out for its argument types,
its result type or its kind. ``call`` writes the statement PostgreSQL requires for the function: a
NULL of each argument's type, a concrete type in place of a pseudo-type, the VARIADIC form, WITHIN
GROUP, OVER, a column definition list, and a real argument where PostgreSQL refuses the NULL. An
overload taking ``internal`` has no statement: no SQL expression has that type, so ``call`` returns
None and the overload is failed by name, as is one PostgreSQL refuses in every form it is called.

``family`` names the family a function belongs to (function_families.yaml), so the failures can be
taken one family at a time."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

FAMILIES = Path(__file__).resolve().parent / "function_families.yaml"

#: The enum the job seeds beside its fixture: PostgreSQL ships none, and anyenum needs one.
PROBE_ENUM = "conformance.probe_enum"

#: The sequence the job seeds for lastval.
PROBE_SEQUENCE = "conformance.probe_sequence"

#: The concrete type a call gives each pseudo-type, consistent across one call's arguments:
#: PostgreSQL resolves anyelement with anyarray, anyrange and anymultirange to one element type.
CONCRETE = {
    '"any"': "integer",
    "anyelement": "integer",
    "anynonarray": "integer",
    "anycompatible": "integer",
    "anycompatiblenonarray": "integer",
    "anyarray": "integer[]",
    "anycompatiblearray": "integer[]",
    "anyrange": "int4range",
    "anycompatiblerange": "int4range",
    "anymultirange": "int4multirange",
    "anycompatiblemultirange": "int4multirange",
    "anyenum": PROBE_ENUM,
}

#: An ``internal`` argument: PostgreSQL refuses ``NULL::internal`` ("cannot cast type unknown to
#: internal") and a bare NULL finds no function, and nothing a statement can write returns one.
UNCALLABLE = "internal"

#: Why an overload taking an internal argument has no call.
NO_CALL = "takes an internal argument, which no SQL expression can supply"

#: A row type both servers have, for the functions whose first argument PostgreSQL refuses to take
#: as anything else.
ROW_TYPE = "pg_catalog.pg_namespace"


def _populate(*rest: str) -> tuple[str, ...]:
    return (f"NULL::{ROW_TYPE}", *rest)


#: The overloads PostgreSQL refuses a NULL for, each with the arguments it accepts. By signature,
#: never by type: a real argument given to any function would run that function for real.
ARGUMENTS: dict[str, tuple[str, ...]] = {
    **{
        f"{name}({element}, {element}, text)": (
            f"NULL::{element}",
            f"NULL::{element}",
            "'[]'::text",
        )
        for name, element in (
            ("daterange", "date"),
            ("int4range", "integer"),
            ("int8range", "bigint"),
            ("numrange", "numeric"),
            ("tsrange", "timestamp without time zone"),
            ("tstzrange", "timestamp with time zone"),
        )
    },
    **{
        f'{name}(key "any", value "any")': ("'k'::text", "NULL::integer")
        for prefix in ("json", "jsonb")
        for name in (
            f"{prefix}_object_agg",
            f"{prefix}_object_agg_strict",
            f"{prefix}_object_agg_unique",
            f"{prefix}_object_agg_unique_strict",
        )
    },
    'json_build_object(VARIADIC "any")': ("'k'::text", "NULL::integer"),
    'jsonb_build_object(VARIADIC "any")': ("'k'::text", "NULL::integer"),
    "json_populate_record(base anyelement, from_json json, use_json_as_text boolean)": _populate(
        "NULL::json", "NULL::boolean"
    ),
    "json_populate_recordset(base anyelement, from_json json, use_json_as_text boolean)": _populate(
        "NULL::json", "NULL::boolean"
    ),
    "jsonb_populate_record(anyelement, jsonb)": _populate("NULL::jsonb"),
    "jsonb_populate_record_valid(anyelement, jsonb)": _populate("NULL::jsonb"),
    "jsonb_populate_recordset(anyelement, jsonb)": _populate("NULL::jsonb"),
    "array_agg(anyarray)": ("'{1}'::integer[]",),
    "array_fill(anyelement, integer[])": ("NULL::integer", "'{1}'::integer[]"),
    "array_fill(anyelement, integer[], integer[])": (
        "NULL::integer",
        "'{1}'::integer[]",
        "'{1}'::integer[]",
    ),
    'pg_collation_for("any")': ("NULL::text",),
    "pg_notify(text, text)": ("'conformance'::text", "NULL::text"),
    "set_config(text, text, boolean)": ("'application_name'::text", "'conformance'::text", "false"),
}

#: The statements that are no plain call: a set-returning function taking its row type from a
#: column definition list, and lastval, which needs a nextval earlier in the same session.
STATEMENTS = {
    "json_to_recordset(json)": "SELECT * FROM pg_catalog.\"json_to_recordset\"('[]') AS t(v integer)",
    "jsonb_to_recordset(jsonb)": (
        "SELECT * FROM pg_catalog.\"jsonb_to_recordset\"('[]') AS t(v integer)"
    ),
    "lastval()": (
        f"SELECT pg_catalog.\"lastval\"() AS v FROM (SELECT nextval('{PROBE_SEQUENCE}') OFFSET 0) s"
    ),
}


@dataclass(frozen=True)
class Function:
    """One pg_catalog overload as pg_proc gives it."""

    name: str
    overload: str  # pg_get_function_identity_arguments
    kind: str = "f"  # prokind: f, a or w
    argtypes: tuple[str, ...] = ()  # format_type of each of proargtypes
    variadic: bool = False  # the last argument is an array passed as VARIADIC
    aggkind: str = "n"  # pg_aggregate: n, or o (ordered-set) or h (hypothetical-set)
    direct: int = 0  # pg_aggregate.aggnumdirectargs
    rettype: str = ""  # format_type of prorettype
    volatility: str = "v"  # provolatile: i, s or v

    @property
    def signature(self) -> str:
        return f"{self.name}({self.overload})"


def _null(argtype: str) -> str:
    return f"NULL::{CONCRETE.get(argtype, argtype)}"


def _args(fn: Function) -> list[str]:
    given = ARGUMENTS.get(fn.signature)
    if given is not None:
        return list(given)
    args = [_null(t) for t in fn.argtypes]
    if fn.variadic:
        args[-1] = f"VARIADIC {args[-1]}"
    return args


def call(fn: Function) -> str | None:
    """The statement that calls ``fn``, its one result column first; None if none can."""
    if UNCALLABLE in fn.argtypes:
        return None
    if fn.signature in STATEMENTS:
        return STATEMENTS[fn.signature]
    return statement(fn, _args(fn))


def statement(fn: Function, args: list[str]) -> str:
    """The statement calling ``fn`` with ``args``, in the form its kind takes."""
    named = f'pg_catalog."{fn.name}"'
    if fn.aggkind == "h":  # the one VARIADIC "any" is both the direct and the ordered arguments
        return f"SELECT {named}({args[0]}) WITHIN GROUP (ORDER BY {args[0]}) AS v"
    if fn.aggkind == "o":
        direct = ", ".join(args[: fn.direct])
        ordered = ", ".join(args[fn.direct :])
        return f"SELECT {named}({direct}) WITHIN GROUP (ORDER BY {ordered}) AS v"
    if fn.kind == "a" and not args:  # count(*)
        return f"SELECT {named}(*) AS v"
    if fn.kind == "w":
        return f"SELECT {named}({', '.join(args)}) OVER () AS v"
    return f"SELECT {named}({', '.join(args)}) AS v"


@dataclass
class Family:
    family: str
    names: frozenset[str] = field(default_factory=frozenset)
    pattern: re.Pattern[str] | None = None
    argument: re.Pattern[str] | None = None  # matches any argument type
    returns: re.Pattern[str] | None = None

    def matches(self, name: str, argtypes: tuple[str, ...], rettype: str) -> bool:
        return (
            name in self.names
            or (self.pattern is not None and self.pattern.search(name) is not None)
            or (self.argument is not None and any(self.argument.search(t) for t in argtypes))
            or (self.returns is not None and self.returns.search(rettype) is not None)
        )


def families() -> list[Family]:
    """The families, first match wins; the last one matches every name."""
    out = []
    for entry in yaml.safe_load(FAMILIES.read_text()):
        out.append(
            Family(
                family=entry["family"],
                names=frozenset(entry.get("names", ())),
                pattern=re.compile(entry["pattern"]) if entry.get("pattern") else None,
                argument=re.compile(entry["argument"]) if entry.get("argument") else None,
                returns=re.compile(entry["returns"]) if entry.get("returns") else None,
            )
        )
    return out


def family(fn: Function, declared: list[Family]) -> str:
    for candidate in declared:
        if candidate.matches(fn.name, fn.argtypes, fn.rettype):
            return candidate.family
    raise ValueError(f"{fn.signature}: in no family of {FAMILIES.name}, whose last names every one")
