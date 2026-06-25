# Copyright (c) 2026 Kenneth Stott
# Canary: 3c4d5e6f-7081-92a3-c4d5-e6f708192031
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GovData subscription access gating."""

from __future__ import annotations

from fastapi import HTTPException

from provisa.core.models import GovDataSource, GovDataSubject
from provisa.govdata.subjects import subjects_cover_schema

# Requirements: REQ-540, REQ-541


def check_source_access(  # REQ-540
    subscribed: list[GovDataSubject],
    source: GovDataSource,
) -> None:
    """Raise HTTP 403 if *subscribed* does not cover any schema in *source*.

    Access is granted if the tenant has subscribed to ALL, or to the subject
    tag on *source*, or to any individual schema within the source.
    """
    if GovDataSubject.all in subscribed:
        return

    if source.subject in subscribed:
        return

    if any(subjects_cover_schema(subscribed, s) for s in source.govdata_schemas):
        return

    raise HTTPException(
        status_code=403,
        detail=(
            f"Tenant not subscribed to GovData subject {source.subject.value!r}. "
            f"Subscribe via the billing portal to access this dataset."
        ),
    )


def subjects_from_plan(plan: str) -> list[GovDataSubject]:  # REQ-540
    """Return the default GovData subjects granted for a billing plan.

    trial   → no GovData access
    starter → DEMOGRAPHICS, PUBLIC_SAFETY, CYBER (all free/public)
    pro     → ALL
    """
    if plan == "trial":
        return []
    if plan == "starter":
        return [
            GovDataSubject.demographics,
            GovDataSubject.public_safety,
            GovDataSubject.cyber,
        ]
    return [GovDataSubject.all]
