# Copyright (c) 2026 Kenneth Stott
# Canary: c6e90f2b-71d4-4a35-8e0c-5a2b7f13d946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The differences between two versions of a contract's fields and checks (REQ-2158, REQ-2180).

A product's contracts are derived from its offers (provisa.core.offer_contracts), and each keeps
its versions on itself; this module reads what changed between two of them in the fields a
contract states and the checks the product promises.

WHAT IS BREAKING. A difference that can break a consumer: a field removed or renamed, a type
changed so it no longer holds what it did, a field made required, a promised check weakened. A
change that cannot be read as harmless is marked breaking: a check changed other than by a
stricter bound.
"""

# Requirements: REQ-2158, REQ-2180

from __future__ import annotations

from typing import Any

import yaml

from provisa.compiler.supertype import holds
from provisa.core.models import (
    ContractChange,
    ContractCheck,
    VersionColumn,
)

#: Which way a check's bound runs (+1 stronger as it rises, -1 stronger as it falls), as Soda's
#: thresholds and Great Expectations' kwargs name them.
_BOUND_DIRECTION = {
    "must_be_greater_than": 1,
    "must_be_greater_than_or_equal": 1,
    "min_value": 1,
    "mostly": 1,
    "must_be_less_than": -1,
    "must_be_less_than_or_equal": -1,
    "max_value": -1,
}


def _number(value: Any) -> float | None:
    """A value as a number, or None when it is not one; a numeral as text is one."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _weaker(direction: int | None, before: Any, after: Any) -> bool:
    """Whether a value moved the weaker way; one that does not say which way it runs did."""
    old, new = _number(before), _number(after)
    if direction is None or old is None or new is None:
        return True
    return (new - old) * direction < 0


def _bounds(definition: str) -> dict[str, Any]:
    """A check's definition as its settings: a threshold's lifted beside the rest, its level left
    out (a check's severity says it)."""
    body = yaml.safe_load(definition) if definition else None
    if body is None:
        return {}
    if not isinstance(body, dict):
        return {"value": body}
    threshold = body.get("threshold")
    lifted = threshold if isinstance(threshold, dict) else {}
    rest = {k: v for k, v in body.items() if k != "threshold"}
    return {k: v for k, v in {**rest, **lifted}.items() if k != "level"}


def _check_weakened(before: ContractCheck, after: ContractCheck) -> bool:
    if before.severity == "fail" and after.severity == "warn":
        return True
    if before.definition == after.definition:
        return False
    old, new = _bounds(before.definition), _bounds(after.definition)
    for key in old.keys() | new.keys():
        if old.get(key) == new.get(key):
            continue
        if key not in old and key in _BOUND_DIRECTION:
            continue  # a bound added holds to more
        if key not in new or _weaker(_BOUND_DIRECTION.get(key), old[key], new[key]):
            return True
    return False


def _renames(
    removed: list[VersionColumn], added: list[VersionColumn]
) -> list[tuple[VersionColumn, VersionColumn]]:
    """The removed field each added one replaces: the one of its type, where there is only one
    each way. With two of a type, which became which cannot be told."""
    return [
        (gone, new)
        for gone in removed
        for new in added
        if gone.type.lower() == new.type.lower()
        and sum(c.type.lower() == gone.type.lower() for c in removed) == 1
        and sum(c.type.lower() == gone.type.lower() for c in added) == 1
    ]


def _field_changes(before: list[VersionColumn], after: list[VersionColumn]) -> list[ContractChange]:
    old = {c.name: c for c in before}
    new = {c.name: c for c in after}
    removed = [c for name, c in old.items() if name not in new]
    added = [c for name, c in new.items() if name not in old]
    renames = _renames(removed, added)
    renamed = {gone.name for gone, _ in renames} | {replacement.name for _, replacement in renames}
    changes = [
        ContractChange(
            kind="field_renamed", subject=gone.name, after=replacement.name, breaking=True
        )
        for gone, replacement in renames
    ]
    changes += [
        ContractChange(kind="field_removed", subject=c.name, before=c.type, breaking=True)
        for c in removed
        if c.name not in renamed
    ]
    changes += [
        ContractChange(kind="field_added", subject=c.name, after=c.type)
        for c in added
        if c.name not in renamed
    ]
    for name, column in new.items():
        was = old.get(name)
        if was is None:
            continue
        if was.type.lower() != column.type.lower():
            changes.append(
                ContractChange(
                    kind="field_type_changed",
                    subject=name,
                    before=was.type,
                    after=column.type,
                    breaking=not holds(was.type, column.type),
                )
            )
        if column.required and not was.required:
            changes.append(ContractChange(kind="field_required", subject=name, breaking=True))
        if was.required and not column.required:
            changes.append(ContractChange(kind="field_optional", subject=name))
        if was.description != column.description:
            changes.append(
                ContractChange(
                    kind="field_described",
                    subject=name,
                    before=was.description,
                    after=column.description,
                )
            )
    return changes


def _check_subject(check: ContractCheck) -> str:
    return f"{check.column}: {check.name}" if check.column else check.name


def _check_changes(before: list[ContractCheck], after: list[ContractCheck]) -> list[ContractChange]:
    old = {(c.column, c.name): c for c in before}
    new = {(c.column, c.name): c for c in after}
    changes = [
        ContractChange(
            kind="check_removed", subject=_check_subject(c), before=c.severity, breaking=True
        )
        for key, c in old.items()
        if key not in new
    ]
    changes += [
        ContractChange(kind="check_added", subject=_check_subject(c), after=c.severity)
        for key, c in new.items()
        if key not in old
    ]
    for key, check in new.items():
        was = old.get(key)
        if was is not None and was != check:
            changes.append(
                ContractChange(
                    kind="check_changed",
                    subject=_check_subject(check),
                    before=was.definition or was.severity,
                    after=check.definition or check.severity,
                    breaking=_check_weakened(was, check),
                )
            )
    return changes


def differences(
    before_columns: list[VersionColumn],
    after_columns: list[VersionColumn],
    before_checks: list[ContractCheck],
    after_checks: list[ContractCheck],
) -> list[ContractChange]:
    """How a contract's fields and checks changed, each difference marked breaking or not."""
    return [
        *_field_changes(before_columns, after_columns),
        *_check_changes(before_checks, after_checks),
    ]
