# Copyright (c) 2026 Kenneth Stott
# Canary: 1a2b3c4d-5e6f-7089-a1b2-c3d4e5f60718
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GovData subject taxonomy helpers."""

from __future__ import annotations

from provisa.core.models import GOVDATA_SUBJECT_SCHEMAS, GovDataSubject

# Requirements: REQ-540, REQ-541

# Reverse map: govdata schema name → subject
_SCHEMA_TO_SUBJECT: dict[str, GovDataSubject] = {}
for _subj, _schemas in GOVDATA_SUBJECT_SCHEMAS.items():
    for _schema in _schemas:
        _SCHEMA_TO_SUBJECT[_schema] = GovDataSubject(_subj)


def schemas_for_subject(subject: GovDataSubject) -> list[str]:  # REQ-540, REQ-541
    """Return govdata schema names covered by *subject*.

    GovDataSubject.all returns every known schema.
    """
    if subject == GovDataSubject.all:
        return [s for schemas in GOVDATA_SUBJECT_SCHEMAS.values() for s in schemas]
    return GOVDATA_SUBJECT_SCHEMAS.get(subject.value, [])


def subject_for_schema(schema: str) -> GovDataSubject | None:  # REQ-540
    """Return the subject that owns *schema*, or None if unknown."""
    return _SCHEMA_TO_SUBJECT.get(schema)


def subjects_cover_schema(subjects: list[GovDataSubject], schema: str) -> bool:  # REQ-540
    """Return True if *subjects* grants access to *schema*."""
    if GovDataSubject.all in subjects:
        return True
    owning = subject_for_schema(schema)
    return owning is not None and owning in subjects
