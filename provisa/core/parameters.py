# Copyright (c) 2026 Kenneth Stott
# Canary: 10dfb26e-d7fe-470c-8ea0-09b02c88fe47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's parameters and the one rule for a required one (REQ-1986).

A parameter is a value a caller passes to the source to read the table, held as a parameter
column (``_nf_<name>``, ``native_filter_type`` naming how it reaches the source). It is required
when the source requires it (``native_filter_required``, recorded at registration) or when a
steward made it required for the table (``required_by_steward``). A read that leaves a required
parameter without a value is refused before anything is sent to the source, naming the table and
the parameter -- the same for every source kind.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

# How a parameter reaches the source (core/models.py Column.native_filter_type).
# partition_key (REQ-1982): a Cosmos DB container's partition key, which narrows a read to one
# partition. column_value (REQ-2191): a real column of the table, given by equality, that narrows
# the read to the rows having that value (a JDBC source's floor).
PARAMETER_KINDS = frozenset(
    {
        "path_param",
        "query_param",
        "graphql_arg",
        "grpc_input",
        "query_arg",
        "partition_key",
        "column_value",
    }
)

# The kinds that are a real column of the table, whose meaning is known: the source requires or
# takes a value for it and the rows returned hold that value. Every other kind is opaque: an input
# to the source's call, of which the product knows only that the source takes it (REQ-1986, AN
# OPAQUE PARAMETER IS BOUND BY EQUALITY ONLY).
REAL_COLUMN_KINDS = frozenset({"partition_key", "column_value"})

NF_PREFIX = "_nf_"
# The key under which a native GraphQL argument records the parameter it fills (its argument name
# is ``_<name>`` when ``<name>`` is also one of the field's own).
PARAMETER_EXTENSION = "provisa_parameter"


def _get(column: Any, key: str) -> Any:
    return column.get(key) if isinstance(column, Mapping) else getattr(column, key, None)


def column_name(column: Any) -> str:
    name = _get(column, "column_name")
    return name if name is not None else _get(column, "name")


def is_parameter(column: Any) -> bool:
    return _get(column, "native_filter_type") is not None


def is_required(column: Any) -> bool:
    """Whether a parameter must be given: the source requires it, or a steward made it
    required."""
    return bool(_get(column, "native_filter_required")) or bool(_get(column, "required_by_steward"))


def parameter_name(column: Any) -> str:
    """The parameter as a caller names it: the column's name without its ``_nf_`` prefix."""
    name = column_name(column)
    return name[len(NF_PREFIX) :] if name.startswith(NF_PREFIX) else name


def bound_names(column: Any) -> list[str]:
    """Every name a statement binds a parameter under (REQ-1986, a parameter is bound under its
    own name and under its alias alike): its column's name, its own name (without ``_nf_``), its
    declared alias and the alias without ``_nf_``; each name without ``_nf_`` also in its one
    canonical spelling (:func:`canonical_name`)."""
    alias = _get(column, "alias")
    spelled = [column_name(column), parameter_name(column)]
    if alias:
        spelled += [alias, alias[len(NF_PREFIX) :] if alias.startswith(NF_PREFIX) else alias]
    plain = [n for n in spelled if not n.startswith(NF_PREFIX)]
    return list(dict.fromkeys([*spelled, *(canonical_name(n) for n in plain)]))


def parameter_names(columns: Iterable[Any], taken: Iterable[str] = ()) -> dict[str, str]:
    """Each name the parameters of a table's ``columns`` are bound under (:func:`bound_names`),
    lowercased as an unquoted SQL identifier folds, to its parameter column: the one map every
    front resolves a parameter name by. A name in ``taken`` -- one of the table's own result
    columns' names -- stays that column's."""
    held = {t.lower() for t in taken}
    names: dict[str, str] = {}
    for c in columns:
        if not is_parameter(c):
            continue
        for name in bound_names(c):
            key = name.lower()
            if key not in held:
                names.setdefault(key, column_name(c))
    return names


def parameter_column(names: Mapping[str, str], name: str) -> str | None:
    """The parameter column ``name`` binds in a table's :func:`parameter_names`, or None for no
    parameter: the name as written, or in its canonical spelling."""
    return names.get(name.lower()) or names.get(canonical_name(name).lower())


def given_names(fact: ParameterFact) -> list[str]:
    """The names a caller gives a parameter by: its own, and its alias (without ``_nf_``) where
    it declares one -- what a vocabulary publishes it under."""
    alias = (
        fact.alias[len(NF_PREFIX) :]
        if fact.alias and fact.alias.startswith(NF_PREFIX)
        else fact.alias
    )
    return list(dict.fromkeys([fact.name, *([alias] if alias else [])]))


def required_parameters(columns: Iterable[Any]) -> list[str]:
    """The parameters of ``columns`` a read must give, by the name a caller gives each: what the
    Register Table picker names a table by before it is registered."""
    return [parameter_name(c) for c in columns if is_parameter(c) and is_required(c)]


def is_opaque(column: Any) -> bool:
    """Whether a parameter is an input to the source's call (a path, query, GraphQL, gRPC or
    native-query argument) rather than a real column of the table (a partition key, a column
    value)."""
    kind = _get(column, "native_filter_type")
    return kind is not None and kind not in REAL_COLUMN_KINDS


@dataclass(frozen=True)
class ParameterFact:
    """One parameter of a table as its metadata states it (REQ-1986, ITS LIMITS ARE STATED IN ITS
    METADATA): what every catalog publishes and every reader asks before reading the table."""

    name: str  # as a caller names it
    column: str  # the parameter column that holds it
    type: str | None  # the column's registered data type
    kind: str  # how it reaches the source (PARAMETER_KINDS)
    opaque: bool  # an input to the source's call; False: a real column of the table
    required_by_source: bool
    required_by_steward: bool
    alias: str | None = None  # the parameter column's declared alias

    @property
    def required(self) -> bool:
        return self.required_by_source or self.required_by_steward

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "column": self.column,
            "type": self.type,
            "kind": self.kind,
            "opaque": self.opaque,
            "required": self.required,
            "required_by_source": self.required_by_source,
            "required_by_steward": self.required_by_steward,
        }


def parameter_facts(columns: Iterable[Any]) -> list[ParameterFact]:
    """Each parameter of a table's ``columns``, once: a remote field that is also an argument is
    held both as its column and as its ``_nf_`` column, and is one parameter, stated by the
    column a caller names."""
    facts: dict[str, ParameterFact] = {}
    for c in columns:
        if not is_parameter(c):
            continue
        key = canonical_name(parameter_name(c))
        if key in facts and column_name(c).startswith(NF_PREFIX):
            continue
        facts[key] = ParameterFact(
            name=parameter_name(c),
            column=column_name(c),
            type=_get(c, "data_type"),
            kind=_get(c, "native_filter_type"),
            opaque=is_opaque(c),
            required_by_source=bool(_get(c, "native_filter_required")),
            required_by_steward=bool(_get(c, "required_by_steward")),
            alias=_get(c, "alias"),
        )
    return list(facts.values())


# The words a catalog comment states a table's limits in (:func:`limits_statement`); the meta
# schema's views write the same sentence in SQL from these (api/_meta_views.py).
LIMITS_OPENING = "Requires parameters: "
LIMITS_CLOSING = ". Gives no row count, profile or sample without them."
OPAQUE_WORD = "opaque"
REAL_COLUMN_WORD = "real column"


def limits_statement(facts: Iterable[ParameterFact]) -> str:
    """What a table that requires parameters cannot do, as one sentence of its catalog comment:
    the parameters it requires, each with its type and whether it is opaque or a real column, and
    that it gives no row count, profile or sample without them. Empty for a table that requires
    none."""
    required = [
        f"{f.name} ({', '.join(x for x in (f.type, OPAQUE_WORD if f.opaque else REAL_COLUMN_WORD) if x)})"
        for f in facts
        if f.required
    ]
    return f"{LIMITS_OPENING}{', '.join(required)}{LIMITS_CLOSING}" if required else ""


def describe_limits(description: str | None, columns: Iterable[Any]) -> str | None:
    """A table's ``description`` followed by its limits (:func:`limits_statement` over its
    ``columns``): what every catalog shows as the table's comment. Unchanged for a table that
    requires no parameter."""
    statement = limits_statement(parameter_facts(columns))
    if not statement:
        return description
    return f"{description} {statement}" if description else statement


def required_of(facts: Iterable[ParameterFact]) -> list[str]:
    """The parameters among ``facts`` a read must give, by the name a caller gives each."""
    return [f.name for f in facts if f.required]


class RequiresParameters(ValueError):
    """A whole read of a table that requires parameters -- a count, a profile, a sample, a scan,
    a preview without a value -- refused from the table's metadata, before anything is read
    (REQ-1986): the one refusal, under one code, wherever such a read is asked of one table."""

    code = "table.requires_parameters"

    def __init__(self, table: str, parameters: list[str]) -> None:
        self.table = table
        self.parameters = parameters
        super().__init__(f"Table {table!r} {not_read_reason(parameters)}")

    @property
    def params(self) -> dict[str, str]:
        """What the catalogued message of :attr:`code` is filled with."""
        return {"table": self.table, "parameters": ", ".join(self.parameters)}


def not_read_reason(parameters: list[str]) -> str:
    """Why a table is not read whole, after its name: what a sweep over many tables reports for
    one it leaves unread, and what :class:`RequiresParameters` says."""
    return f"cannot be counted or profiled: it requires {', '.join(parameters)}"


def whole_read_refusal(table: str, required: list[str]) -> RequiresParameters | None:
    """The refusal of a whole read of ``table``, whose metadata states ``required`` parameters;
    None for a table that requires none. A reader that sweeps many tables asks this and reports
    the table as not read; a reader asked for this one table raises it."""
    return RequiresParameters(table, required) if required else None


def canonical_name(name: str) -> str:
    """One parameter's one name, however it is spelled (``breedName``, ``breed_name``,
    ``_breed_name``)."""
    from provisa.compiler.naming import apply_sql_name

    # A statement names a parameter by its column, which the naming authority may have
    # re-cased and GraphQL may have prefixed with ``_`` (a collision with a field): both sides
    # reduce to one sql name.
    return apply_sql_name(name.lstrip("_"))


def missing_parameters(
    columns: Iterable[Any], given: Mapping[str, Any] | None, fed: Callable[[str], bool]
) -> list[str]:
    """The required parameters of a table's ``columns`` that a read gives no value: neither in
    ``given`` (the statement's arguments, by parameter name) nor through a join (``fed`` answers
    for a parameter column's name)."""
    have = {canonical_name(k) for k, v in (given or {}).items() if v is not None}
    # Each parameter once, by its declared column's name: a remote field that is also an argument
    # is held both as its column and as its ``_nf_`` column, under the argument's own spelling
    # ("requires the parameter(s) ['breed_name', 'breedName']").
    missing: dict[str, str] = {}
    for c in columns:
        if not is_parameter(c) or not is_required(c) or fed(column_name(c)):
            continue
        key = canonical_name(parameter_name(c))
        if key in have:
            continue
        if key not in missing or not column_name(c).startswith(NF_PREFIX):
            missing[key] = parameter_name(c)
    return list(missing.values())


def missing_reason(missing: list[str]) -> str:
    """Why a read of a table is refused, after the table's name (``apply_dropped_tables``)."""
    return (
        f"requires the parameter(s) {missing}: give each a value (a WHERE condition on its "
        "column in SQL, an argument in GraphQL)"
    )
