# Copyright (c) 2026 Kenneth Stott
# Canary: c6e90f2b-71d4-4a35-8e0c-5a2b7f13d946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A contract's saved versions and the differences between them (REQ-2158).

WHERE THEY ARE KEPT. On the contract itself, in ``registered_tables.contract``: the model store
holds them, every save of a contract passes through :func:`recorded`, and they travel, merge and
undo with the model as the rest of the contract does. The environment's branch (REQ-1524) records
every save too, but it cannot be asked: it is a projection, never an authority, it may be squashed
and a drifted one is rebuilt whole, and a contract's row needs its versions without walking it.

WHAT IS BREAKING. A difference that can break a consumer: a field removed or renamed, a type
changed so it no longer holds what it did, a field made required, a promised service level or
check weakened. A change that cannot be read as harmless is marked breaking: a service level of a
property or unit that does not say which way it runs, a check changed other than by a stricter
bound.
"""

# Requirements: REQ-2158

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import yaml

from provisa.compiler.supertype import holds
from provisa.core import model_change
from provisa.core.models import (
    ContractChange,
    ContractCheck,
    ContractColumn,
    ContractSlaProperty,
    ContractVersion,
    DataContract,
)

#: Which way a service level runs: +1 stronger as it rises, -1 stronger as it falls.
_SLA_DIRECTION = {
    "availability": 1,
    "retention": 1,
    "throughput": 1,
    "latency": -1,
    "errorRate": -1,
    "frequency": -1,
}
#: The same of a check's bounds, as Soda's thresholds and Great Expectations' kwargs name them.
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


def _sla_text(sla: ContractSlaProperty) -> str:
    return f"{sla.value}{sla.unit or ''}"


def _sla_weakened(before: ContractSlaProperty, after: ContractSlaProperty) -> bool:
    if (before.value, before.unit) == (after.value, after.unit):
        return False
    if before.unit != after.unit:
        return True
    return _weaker(_SLA_DIRECTION.get(before.property), before.value, after.value)


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
    removed: list[ContractColumn], added: list[ContractColumn]
) -> list[tuple[ContractColumn, ContractColumn]]:
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


def _field_changes(before: DataContract, after: DataContract) -> list[ContractChange]:
    old = {c.name: c for c in before.columns}
    new = {c.name: c for c in after.columns}
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


def _sla_changes(before: DataContract, after: DataContract) -> list[ContractChange]:
    old = {s.property: s for s in before.sla}
    new = {s.property: s for s in after.sla}
    changes = [
        ContractChange(kind="sla_removed", subject=name, before=_sla_text(s), breaking=True)
        for name, s in old.items()
        if name not in new
    ]
    changes += [
        ContractChange(kind="sla_added", subject=name, after=_sla_text(s))
        for name, s in new.items()
        if name not in old
    ]
    for name, sla in new.items():
        was = old.get(name)
        if was is not None and was != sla:
            changes.append(
                ContractChange(
                    kind="sla_changed",
                    subject=name,
                    before=_sla_text(was),
                    after=_sla_text(sla),
                    breaking=_sla_weakened(was, sla),
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


def _prose_changes(before: DataContract, after: DataContract) -> list[ContractChange]:
    changes = []
    if before.description != after.description:
        changes.append(
            ContractChange(
                kind="description_changed",
                subject="description",
                before=before.description,
                after=after.description,
            )
        )
    old, new = before.terms.model_dump(), after.terms.model_dump()
    changes += [
        ContractChange(kind="terms_changed", subject=term, before=old[term], after=new[term])
        for term in new
        if old[term] != new[term]
    ]
    return changes


def differences(
    before: DataContract,
    before_checks: list[ContractCheck],
    after: DataContract,
    after_checks: list[ContractCheck],
) -> list[ContractChange]:
    """How ``after`` differs from ``before``, each difference marked breaking or not. A version
    number or a status is no difference: it says which contract this is, not what it promises."""
    return [
        *_field_changes(before, after),
        *_sla_changes(before, after),
        *_check_changes(before_checks, after_checks),
        *_prose_changes(before, after),
    ]


def _unchanged(a: DataContract, b: DataContract) -> bool:
    omitted = {"created_at", "versions"}
    return a.model_dump(exclude=omitted) == b.model_dump(exclude=omitted)


def recorded(
    previous: DataContract | None, contract: DataContract, checks: list[ContractCheck]
) -> DataContract:
    """``contract`` replacing ``previous`` with the version this save made recorded on it: a new
    number adds one, the same number replaces it, and saving what is stored records nothing.
    ``checks``: what the product promises of the member now. The history is the stored one
    whatever ``contract`` carries. A contract stored before versions were kept starts from this
    save, with no differences to record."""
    kept = previous.versions if previous is not None else []
    if previous is not None and kept and _unchanged(previous, contract):
        return contract.model_copy(update={"versions": kept})
    earlier = [v for v in kept if v.contract.version != contract.version]
    base = earlier[-1] if earlier else None
    version = ContractVersion(
        contract=contract.model_copy(update={"versions": []}),
        checks=checks,
        changes=differences(base.contract, base.checks, contract, checks) if base else [],
        saved_at=datetime.now(timezone.utc),
        saved_by=model_change.actor(),
    )
    return contract.model_copy(update={"versions": [*earlier, version]})
