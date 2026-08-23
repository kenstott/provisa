# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3d51f6-9b02-4e7a-a1d4-72f6e0b95c38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a checker can be asked to check, and how one check is written down (REQ-1443 clause 7).

The contract builder panel offers a check picker scoped to the column's type, a threshold editor and
a per-check severity. All three need the same thing: a description of the checks a checker actually
implements, keyed by the check type its own parser expects. That description lives HERE, beside the
parse/serialize pair in :mod:`provisa.dq.contract`, rather than in the browser — a picker holding its
own copy of soda's or Great Expectations' vocabulary is a second dialect that drifts from the one the
worker runs, and the operator finds out at scan time.

The catalog is authored against the shipped checkers:

* Soda v4 registers its check types in ``soda_core.contracts.impl.check_types.check_types`` — schema,
  missing, invalid, duplicate, row_count, aggregate, metric, freshness, failed_rows — and constrains
  each one's body in ``soda_data_contract_json_schema_1_0_0.json``. Severity is
  ``threshold.level: fail | warn`` (``ThresholdYaml``), and ``threshold.metric: count | percent``
  chooses the units the comparator reads, which is why the two ride in the threshold rather than
  beside it.
* Great Expectations 1.x registers its expectations by name; the tolerance is the per-expectation
  ``mostly`` kwarg. GX has NO warn level — an expectation succeeds or it does not — so its severity
  choice is ``fail`` alone rather than a warn that the results table would never report.

Only the checks whose bodies the panel can fully author are listed. ``failed_rows``, ``metric`` and
``schema`` take hand-written SQL or a column list, so they stay the raw editor's business — the raw
text is the source of truth and the builder is a view of it, so a check the picker cannot offer is
still perfectly authorable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import yaml

from provisa.dq.contract import CHECKERS, ContractError

# The IR type families the picker scopes on. ``data_type`` on a registered column is an IR name
# (provisa.core.ir_types), so scoping is expressed in that vocabulary rather than in any source's
# native spelling — the same check offer holds whichever store the observed table came from.
NUMERIC_TYPES: tuple[str, ...] = ("smallint", "integer", "bigint", "float", "double", "numeric")
TEXT_TYPES: tuple[str, ...] = ("text", "uuid")
TEMPORAL_TYPES: tuple[str, ...] = ("date", "timestamp", "time")

# Soda's comparators (``ThresholdYaml``). ``must_be_between`` / ``must_be_not_between`` take a range
# rather than a scalar and are left to the raw editor.
_SODA_COMPARATORS: tuple[str, ...] = (
    "must_be",
    "must_not_be",
    "must_be_greater_than",
    "must_be_greater_than_or_equal",
    "must_be_less_than",
    "must_be_less_than_or_equal",
)


@dataclass(frozen=True)
class CheckParam:
    """One editable field of a check's body, in the checker's own spelling."""

    name: str
    # number | string | number_list | string_list | column | enum
    value_type: str
    required: bool = False
    # The permitted values when ``value_type`` is ``enum``; empty otherwise.
    choices: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckKind:
    """One check the picker may offer, and everything the editors need to render it."""

    check_type: str
    # column | dataset — whether the check hangs off one column or the whole table.
    scope: str
    # The IR types this check may be offered for; empty = every type. Dataset checks carry ().
    applies_to: tuple[str, ...] = ()
    params: tuple[CheckParam, ...] = ()
    # The comparators the threshold editor offers; empty = this check takes no threshold.
    comparators: tuple[str, ...] = ()
    # The units a comparator's value is read in; empty = the check has no choice of metric.
    metrics: tuple[str, ...] = ()
    # The severity choices. ("fail",) means the checker has no warn level.
    levels: tuple[str, ...] = ("fail",)
    # Extra threshold keys beyond the comparator (soda freshness reads its unit there).
    threshold_units: tuple[str, ...] = field(default=())


_SODA_KINDS: tuple[CheckKind, ...] = (
    CheckKind(
        check_type="missing",
        scope="column",
        params=(CheckParam("missing_values", "string_list"),),
        comparators=_SODA_COMPARATORS,
        metrics=("count", "percent"),
        levels=("fail", "warn"),
    ),
    CheckKind(
        check_type="invalid",
        scope="column",
        params=(
            CheckParam("valid_values", "string_list"),
            CheckParam("valid_min", "number"),
            CheckParam("valid_max", "number"),
            CheckParam("valid_min_length", "number"),
            CheckParam("valid_max_length", "number"),
        ),
        comparators=_SODA_COMPARATORS,
        metrics=("count", "percent"),
        levels=("fail", "warn"),
    ),
    CheckKind(
        check_type="duplicate",
        scope="column",
        comparators=_SODA_COMPARATORS,
        metrics=("count", "percent"),
        levels=("fail", "warn"),
    ),
    CheckKind(
        check_type="aggregate",
        scope="column",
        applies_to=NUMERIC_TYPES + TEXT_TYPES,
        params=(
            CheckParam(
                "function",
                "enum",
                required=True,
                choices=("avg", "sum", "min", "max", "minLength", "maxLength", "avgLength"),
            ),
        ),
        comparators=_SODA_COMPARATORS,
        levels=("fail", "warn"),
    ),
    CheckKind(
        check_type="row_count",
        scope="dataset",
        comparators=_SODA_COMPARATORS,
        levels=("fail", "warn"),
    ),
    CheckKind(
        check_type="freshness",
        scope="dataset",
        params=(CheckParam("column", "column", required=True),),
        comparators=_SODA_COMPARATORS,
        levels=("fail", "warn"),
        threshold_units=("minute", "hour", "day"),
    ),
)

# GX's tolerance: the fraction of rows that may fail before the expectation does. Every row-wise
# expectation takes it; the aggregate ones (mean, unique value count, table row count) do not.
_MOSTLY = CheckParam("mostly", "number")

_GX_KINDS: tuple[CheckKind, ...] = (
    CheckKind(check_type="expect_column_values_to_not_be_null", scope="column", params=(_MOSTLY,)),
    CheckKind(check_type="expect_column_values_to_be_unique", scope="column", params=(_MOSTLY,)),
    CheckKind(
        check_type="expect_column_values_to_be_in_set",
        scope="column",
        params=(CheckParam("value_set", "string_list", required=True), _MOSTLY),
    ),
    CheckKind(
        check_type="expect_column_values_to_match_regex",
        scope="column",
        applies_to=TEXT_TYPES,
        params=(CheckParam("regex", "string", required=True), _MOSTLY),
    ),
    CheckKind(
        check_type="expect_column_values_to_be_between",
        scope="column",
        applies_to=NUMERIC_TYPES + TEMPORAL_TYPES,
        params=(CheckParam("min_value", "number"), CheckParam("max_value", "number"), _MOSTLY),
    ),
    CheckKind(
        check_type="expect_column_value_lengths_to_be_between",
        scope="column",
        applies_to=TEXT_TYPES,
        params=(CheckParam("min_value", "number"), CheckParam("max_value", "number"), _MOSTLY),
    ),
    CheckKind(
        check_type="expect_column_mean_to_be_between",
        scope="column",
        applies_to=NUMERIC_TYPES,
        params=(CheckParam("min_value", "number"), CheckParam("max_value", "number")),
    ),
    CheckKind(
        check_type="expect_column_unique_value_count_to_be_between",
        scope="column",
        params=(CheckParam("min_value", "number"), CheckParam("max_value", "number")),
    ),
    CheckKind(
        check_type="expect_table_row_count_to_be_between",
        scope="dataset",
        params=(CheckParam("min_value", "number"), CheckParam("max_value", "number")),
    ),
)

_CATALOG: dict[str, tuple[CheckKind, ...]] = {
    "soda": _SODA_KINDS,
    "great_expectations": _GX_KINDS,
}


def check_catalog(checker: str) -> tuple[CheckKind, ...]:
    """Every check ``checker`` offers the builder. Raises for an unknown checker rather than
    returning an empty catalog, which a panel would render as "this checker has no checks"."""
    if checker not in CHECKERS:
        raise ContractError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    return _CATALOG[checker]


def checks_for_column(checker: str, data_type: str | None) -> tuple[CheckKind, ...]:
    """The column-scoped checks offerable on a column of ``data_type``.

    A column whose IR type is unresolved is not a reason to offer everything — ``data_type`` is
    resolved at registration and never lazily backfilled (REQ-846), so a null one means the column
    genuinely has no type and only the type-agnostic checks apply.
    """
    return tuple(
        kind
        for kind in check_catalog(checker)
        if kind.scope == "column" and (not kind.applies_to or data_type in kind.applies_to)
    )


def _kind(checker: str, check_type: str) -> CheckKind:
    for kind in check_catalog(checker):
        if kind.check_type == check_type:
            return kind
    raise ContractError(
        f"{checker} has no builder-authorable check {check_type!r}; author it in the raw contract"
    )


def _validated_params(kind: CheckKind, params: dict[str, Any]) -> dict[str, Any]:
    """``params`` narrowed to the check's own fields, with the required ones present.

    An unknown field is an error rather than a silently dropped key: soda's contract parser and GX's
    expectation models both reject bodies they do not recognize, so passing one through would build
    text that fails at scan time instead of at build time.
    """
    known = {param.name: param for param in kind.params}
    for name in params:
        if name not in known:
            raise ContractError(
                f"check {kind.check_type!r} has no parameter {name!r}; expected one of "
                f"{sorted(known) or 'none'}"
            )
    for param in kind.params:
        if param.required and params.get(param.name) in (None, "", [], {}):
            raise ContractError(f"check {kind.check_type!r} requires {param.name!r}")
        if param.value_type == "enum" and param.name in params:
            value = params[param.name]
            if value not in param.choices:
                raise ContractError(
                    f"check {kind.check_type!r} parameter {param.name!r} must be one of "
                    f"{list(param.choices)}, got {value!r}"
                )
    return {name: value for name, value in params.items() if value not in (None, "", [])}


def build_check_definition(
    checker: str,
    check_type: str,
    *,
    column_name: str | None = None,
    params: dict[str, Any] | None = None,
    comparator: str | None = None,
    threshold_value: Any = None,
    metric: str | None = None,
    unit: str | None = None,
    level: str = "fail",
) -> str:
    """One check's ``definition`` text, in ``checker``'s dialect.

    The panel's picker/threshold/severity editors produce these arguments and get back the same
    ``definition`` string :func:`provisa.dq.contract.contract_checks` reads out of authored text, so
    a builder-made check and a hand-typed one are the same object by the time the contract is
    assembled. Nothing here is optional-by-omission: a comparator without a value (or the reverse)
    is an error, because a threshold that half-exists is a check that silently never fails.
    """
    kind = _kind(checker, check_type)
    if kind.scope == "column" and not column_name:
        raise ContractError(f"check {check_type!r} is column-scoped and names no column")
    if kind.scope == "dataset" and column_name:
        raise ContractError(f"check {check_type!r} is dataset-scoped and takes no column")
    body: dict[str, Any] = dict(_validated_params(kind, dict(params or {})))
    if level not in kind.levels:
        raise ContractError(
            f"check {check_type!r} supports severity {list(kind.levels)}, got {level!r}"
        )
    if checker == "great_expectations":
        # A GX suite is flat — every expectation names its own column in kwargs, where soda nests
        # its column checks under the column entry and never repeats the name inside the check.
        if kind.scope == "column":
            body = {"column": column_name, **body}
        # GX carries no threshold beside its kwargs — min_value/max_value/mostly ARE the tolerance,
        # and there is no warn, so `level` has already been checked against ("fail",).
        if comparator is not None or metric is not None or unit is not None:
            raise ContractError(
                "great_expectations expectations carry no threshold block; the bound is the "
                "expectation's own min_value / max_value / mostly kwargs"
            )
        # Args alone — the ``type``/``kwargs`` envelope is restated by the contract serializer
        # (REQ-1443 clause 7), so the panel's rows show what an operator can actually change.
        return json.dumps(body, sort_keys=True)
    threshold = _soda_threshold(kind, comparator, threshold_value, metric, unit, level)
    if threshold:
        body["threshold"] = threshold
    # Args alone, as above: soda's wrapper is the single key naming the type, which the row already
    # carries. A check with no args serializes to empty text.
    if not body:
        return ""
    return yaml.safe_dump(body, sort_keys=False, default_flow_style=False).strip()


def _soda_threshold(
    kind: CheckKind,
    comparator: str | None,
    threshold_value: Any,
    metric: str | None,
    unit: str | None,
    level: str,
) -> dict[str, Any]:
    if comparator is None:
        if threshold_value is not None:
            raise ContractError(
                f"check {kind.check_type!r} was given a threshold value with no comparator"
            )
        return {}
    if comparator not in kind.comparators:
        raise ContractError(
            f"check {kind.check_type!r} supports comparators {list(kind.comparators)}, "
            f"got {comparator!r}"
        )
    if threshold_value is None:
        raise ContractError(f"comparator {comparator!r} was given no value to compare against")
    threshold: dict[str, Any] = {}
    if metric is not None:
        if metric not in kind.metrics:
            raise ContractError(
                f"check {kind.check_type!r} supports metrics {list(kind.metrics)}, got {metric!r}"
            )
        threshold["metric"] = metric
    if unit is not None:
        if unit not in kind.threshold_units:
            raise ContractError(
                f"check {kind.check_type!r} supports units {list(kind.threshold_units)}, "
                f"got {unit!r}"
            )
        threshold["unit"] = unit
    threshold[comparator] = threshold_value
    # Soda defaults an unstated level to fail (ThresholdYaml), so only warn is written down —
    # emitting "level: fail" everywhere would churn every hand-authored contract on first save.
    if level == "warn":
        threshold["level"] = "warn"
    return threshold
