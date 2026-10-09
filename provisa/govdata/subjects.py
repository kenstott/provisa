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

import json
import re
from functools import lru_cache
from pathlib import Path

from provisa.core.models import (
    GOVDATA_LINKER_SCHEMAS,
    GOVDATA_SUBJECT_LABELS,
    GOVDATA_SUBJECT_SCHEMAS,
    GovDataSubject,
)

_CATALOG_PATH = Path(__file__).resolve().parent / "catalog_metadata.json"
_BUNDLE_SCHEMAS_PATH = Path(__file__).resolve().parent / "bundle_schemas.json"

# Requirements: REQ-540, REQ-541

# Reverse map: govdata schema name → subject
_SCHEMA_TO_SUBJECT: dict[str, GovDataSubject] = {}
for _subj, _schemas in GOVDATA_SUBJECT_SCHEMAS.items():
    for _schema in _schemas:
        _SCHEMA_TO_SUBJECT[_schema] = GovDataSubject(_subj)


@lru_cache(maxsize=1)
def bundle_schemas() -> tuple[str, frozenset[str]]:  # REQ-540
    """``(release, schemas)``: the schemas the adapter bundle of ``release`` serves, as recorded
    from that bundle's own model by ``scripts/record_govdata_bundle_schemas.py``. The record is
    held to the bundle pin by a unit test and to the bundle itself wherever one is started
    (:func:`require_recorded_schemas`)."""
    record = json.loads(_BUNDLE_SCHEMAS_PATH.read_text())
    return record["release"], frozenset(record["schemas"])


class BundleSchemasChanged(RuntimeError):
    """The adapter bundle being started does not serve the schemas recorded for it."""


def require_recorded_schemas(bundle_model: dict, release: str) -> None:  # REQ-540
    """Refuse, by name, a bundle whose model serves other schemas than the record says — the
    record is what the subject map is held to, so a bundle that has moved on from it means a
    schema with no subject or a subject with no schema."""
    recorded_release, recorded = bundle_schemas()
    served = {entry["name"] for entry in bundle_model["schemas"]}
    if release == recorded_release and served == recorded:
        return
    raise BundleSchemasChanged(
        f"the pgwire-govdata bundle {release} serves schemas that differ from the record of "
        f"{recorded_release} (provisa/govdata/bundle_schemas.json): only in the bundle "
        f"{sorted(served - recorded)}, only in the record {sorted(recorded - served)}. "
        "Run scripts/record_govdata_bundle_schemas.py and place any new schema under a subject."
    )


def subject_catalog() -> list[dict]:  # REQ-540
    """Every subject a source can be given, as the Sources form offers it: its value, its
    label and the schemas it brings. ``ALL`` is not offered: it is a subscription's shorthand
    for every subject, not a choice of schemas."""
    return [
        {"value": value, "label": GOVDATA_SUBJECT_LABELS[value], "schemas": list(schemas)}
        for value, schemas in GOVDATA_SUBJECT_SCHEMAS.items()
    ]


def schemas_for_subjects(subjects: list[str]) -> list[str]:  # REQ-540, REQ-541
    """The schemas a source given ``subjects`` serves: each subject's, then the linker schemas,
    once each, in that order. An unknown subject is refused by name."""
    unknown = [s for s in subjects if s not in GOVDATA_SUBJECT_SCHEMAS]
    if unknown:
        raise ValueError(f"unknown GovData subject(s) {unknown}")
    ordered: list[str] = []
    for schema in (
        *(schema for subject in subjects for schema in GOVDATA_SUBJECT_SCHEMAS[subject]),
        *GOVDATA_LINKER_SCHEMAS,
    ):
        if schema not in ordered:
            ordered.append(schema)
    return ordered


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


@lru_cache(maxsize=1)
def _load_catalog() -> dict[str, dict]:
    """Static per-schema {comment, tables:[{name, comment}]} snapshot (REQ-1798), built by
    scripts/build_govdata_catalog.py from the govdata engine's *-schema.yaml files (real
    schema/table descriptions — e.g. econ's `inflation_metrics`/`metro_cpi` tables) and checked
    into the repo, so keyword search works without a live askamerica connection. Re-run that
    script by hand when the upstream schema YAML changes; this file does not regenerate itself."""
    try:
        return json.loads(_CATALOG_PATH.read_text())
    except FileNotFoundError:
        return {}


# Dropped from search terms — common enough to inflate every schema's score without signaling
# anything about the topic (e.g. "data"/"the" appear in nearly every schema's own description).
_STOPWORDS = frozenset(
    {
        "data",
        "the",
        "and",
        "for",
        "of",
        "in",
        "on",
        "a",
        "an",
        "to",
        "with",
        "by",
        "me",
        "find",
        "get",
    }
)


def _tokenize(text: str) -> list[str]:
    return [t for t in re.split(r"\W+", text.lower()) if t and t not in _STOPWORDS]


def search_catalog(query: str) -> list[dict]:  # REQ-1798
    """Keyword-match GovData's static table catalog against a free-text topic.

    Scores each schema by term hits in its own description plus its tables' names/descriptions
    (a table hit counts double — it's a more specific signal than a schema-level description
    match), and returns the schemas with any hit, ranked highest score first, each carrying its
    owning GovDataSubject and up to 5 matching table names.
    """
    terms = _tokenize(query)
    if not terms:
        return []
    results: list[dict] = []
    for schema_name, entry in _load_catalog().items():
        normalized = schema_name.replace("-", "_")
        schema_comment = entry.get("comment", "").lower()
        score = sum(1 for t in terms if t in schema_comment)
        matched_tables = []
        for table in entry.get("tables", []):
            haystack = f"{table['name']} {table.get('comment', '')}".lower()
            if any(t in haystack for t in terms):
                score += 2
                matched_tables.append(table["name"])
        if score:
            subject = subject_for_schema(normalized)
            results.append(
                {
                    "schema": normalized,
                    "subject": subject.value if subject else None,
                    "tables": matched_tables[:5],
                    "score": score,
                }
            )
    results.sort(key=lambda r: -r["score"])
    return results


def search_subjects(query: str) -> list[GovDataSubject]:  # REQ-540, REQ-1798
    """Match a free-text topic against GovData's subject taxonomy, via search_catalog's
    schema/table keyword hits (real descriptions, not a guessed synonym list)."""
    seen: list[GovDataSubject] = []
    for hit in search_catalog(query):
        if hit["subject"] is None:
            continue
        subject = GovDataSubject(hit["subject"])
        if subject not in seen:
            seen.append(subject)
    return seen
