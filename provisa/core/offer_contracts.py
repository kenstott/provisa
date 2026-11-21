# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1f3a92-5c48-4e0b-9a6d-2b8e4f1c0d75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The contracts derived from a product's offers (REQ-2180, CONTRACTS ARE DERIVED FROM THE OFFERS).

For every offer, each dataset of the product has one contract: the offer's terms applied to that
dataset. Its schema is the dataset's columns as the offer's role reads them -- a masked column
stated as masked, a row rule stated in words -- its data level (the full dataset, the offer's
sample views over it, or synthetic), the data quality promises (always; the offer decides whether
their results can be read), and its profiles where the offer includes them. Nothing here is
written by hand: what all of a product's contracts share (owner, description, freshness, support)
is entered once on the product and joined in when a contract is exported.

VERSIONS. A contract's version changes when its terms do. A change that removes or masks a column
is a breaking one, as is any change :mod:`provisa.core.contract_versions` reads as breaking, and
it takes the next major version; any other change takes the next minor one.
"""

# Requirements: REQ-2180

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from provisa.core import contract_versions, model_change
from provisa.core.models import (
    ContractChange,
    ContractCheck,
    DataProductOffer,
    OfferContract,
    OfferContractVersion,
    OfferField,
    TERMS_OF_USE,
    OfferTerms,
    VersionColumn,
)

if TYPE_CHECKING:
    from datetime import datetime

#: The version an offer's contract on a dataset is created at.
FIRST_OFFER_CONTRACT_VERSION = "1.0.0"


@dataclass(frozen=True)
class SeenColumn:
    """A dataset column, and how the offer's role reads it: ``read`` False is a column it does not
    read, which is no field of its contract; ``mask`` the mask's kind, None for unmasked."""

    name: str
    type: str
    description: str | None = None
    read: bool = True
    mask: str | None = None


def terms(
    offer: DataProductOffer,
    columns: Iterable[SeenColumn],
    *,
    row_rules: Iterable[str],
    sample_views: Iterable[str],
    checks: Iterable[ContractCheck],
    profiles: Iterable[str],
) -> OfferTerms:
    """``offer``'s terms on one dataset. ``columns``: the dataset's, as the offer's role reads
    them; ``row_rules``: the role's row rules on it, each in words; ``sample_views``: the offer's
    views over it (a sample offer's); ``checks``: the product's promises on it; ``profiles``: its
    profile tables, given only where the offer includes them."""
    return OfferTerms(
        fields=[
            OfferField(name=c.name, type=c.type, description=c.description, masked=c.mask)
            for c in columns
            if c.read
        ],
        row_rules=sorted(set(row_rules)),
        data_level=offer.data_level,
        sample_views=sorted(set(sample_views)) if offer.data_level == "sample" else [],
        checks=list(checks),
        dq_results=offer.dq_results,
        profiles=sorted(set(profiles)) if offer.profiles else [],
        duration_days=offer.duration_days,
        purpose=offer.purpose,
        usage=offer.usage,
        limitations=offer.limitations,
    )


def _columns(t: OfferTerms) -> list[VersionColumn]:
    """``t``'s fields as :mod:`contract_versions` compares them."""
    return [VersionColumn(name=f.name, type=f.type, description=f.description) for f in t.fields]


def _changed(
    kind: str,
    subject: str,
    before: str | int | bool | None,
    after: str | int | bool | None,
    *,
    breaking: bool,
) -> ContractChange:
    return ContractChange(
        kind=kind,  # type: ignore[arg-type]
        subject=subject,
        before=None if before is None else str(before),
        after=None if after is None else str(after),
        breaking=breaking,
    )


def differences(before: OfferTerms, after: OfferTerms) -> list[ContractChange]:
    """How ``after`` differs from ``before``, each difference marked breaking or not: the fields'
    and checks' differences as :func:`contract_versions.differences` reads them; a column masked
    (breaking) or unmasked; a row rule added (breaking: fewer rows) or removed; and the data
    level, data quality results, profiles and duration, each breaking when it gives less; and each
    term of use changed, never breaking."""
    changes = contract_versions.differences(
        _columns(before), _columns(after), before.checks, after.checks
    )
    old = {f.name: f for f in before.fields}
    for field in after.fields:
        was = old.get(field.name)
        if was is None or was.masked == field.masked:
            continue
        if field.masked is not None:
            changes.append(
                _changed("field_masked", field.name, was.masked, field.masked, breaking=True)
            )
        else:
            changes.append(_changed("field_unmasked", field.name, was.masked, None, breaking=False))
    changes += [
        _changed("row_rule_added", rule, None, rule, breaking=True)
        for rule in after.row_rules
        if rule not in before.row_rules
    ]
    changes += [
        _changed("row_rule_removed", rule, rule, None, breaking=False)
        for rule in before.row_rules
        if rule not in after.row_rules
    ]
    if (before.data_level, before.sample_views) != (after.data_level, after.sample_views):
        changes.append(
            _changed(
                "data_level_changed",
                "data",
                _level(before),
                _level(after),
                breaking=_LEVEL_RANK[after.data_level] < _LEVEL_RANK[before.data_level]
                or (
                    after.data_level == "sample"
                    and before.data_level == "sample"
                    and not set(before.sample_views) <= set(after.sample_views)
                ),
            )
        )
    if before.dq_results != after.dq_results:
        changes.append(
            _changed(
                "dq_results_changed",
                "dq_results",
                before.dq_results,
                after.dq_results,
                breaking=before.dq_results,
            )
        )
    if before.profiles != after.profiles:
        changes.append(
            _changed(
                "profiles_changed",
                "profiles",
                ", ".join(before.profiles) or None,
                ", ".join(after.profiles) or None,
                breaking=not set(before.profiles) <= set(after.profiles),
            )
        )
    if before.duration_days != after.duration_days:
        changes.append(
            _changed(
                "duration_changed",
                "duration_days",
                before.duration_days,
                after.duration_days,
                breaking=after.duration_days is not None
                and (before.duration_days is None or after.duration_days < before.duration_days),
            )
        )
    # The terms of use are prose: a change is recorded, and read by a person, not judged here.
    changes += [
        _changed(
            "terms_of_use_changed",
            term,
            getattr(before, term) or None,
            getattr(after, term) or None,
            breaking=False,
        )
        for term in TERMS_OF_USE
        if getattr(before, term) != getattr(after, term)
    ]
    return changes


#: How much data each level gives: a move down is breaking.
_LEVEL_RANK = {"synthetic": 0, "sample": 1, "full": 2}


def _level(t: OfferTerms) -> str:
    return f"sample ({', '.join(t.sample_views)})" if t.data_level == "sample" else t.data_level


def next_version(version: str, changes: Iterable[ContractChange]) -> str:
    """The version after ``version`` (MAJOR.MINOR.PATCH): the next major for a breaking change,
    else the next minor."""
    major, minor = (int(p) for p in version.split(".")[:2])
    if any(c.breaking for c in changes):
        return f"{major + 1}.0.0"
    return f"{major}.{minor + 1}.0"


def recorded(previous: OfferContract | None, now: OfferTerms, *, at: datetime) -> OfferContract:
    """The contract with terms ``now``: ``previous`` itself when its terms are these, else the
    next version of it, its differences recorded; the first version when there is none."""
    by = model_change.actor()
    if previous is None:
        first = OfferContractVersion(
            version=FIRST_OFFER_CONTRACT_VERSION, terms=now, saved_at=at, saved_by=by
        )
        return OfferContract(
            version=FIRST_OFFER_CONTRACT_VERSION, terms=now, created_at=at, versions=[first]
        )
    if previous.terms == now:
        return previous
    changes = differences(previous.terms, now)
    version = next_version(previous.version, changes)
    saved = OfferContractVersion(
        version=version, terms=now, changes=changes, saved_at=at, saved_by=by
    )
    return previous.model_copy(
        update={"version": version, "terms": now, "versions": [*previous.versions, saved]}
    )


def breaking(contract: OfferContract) -> bool:
    """Whether the contract's latest version broke the one before it."""
    return bool(contract.versions) and any(c.breaking for c in contract.versions[-1].changes)
