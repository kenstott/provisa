# Copyright (c) 2026 Kenneth Stott
# Canary: 4a1276ed-6152-4ec0-bbf5-42ae4c5f440f
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD step implementations for REQ-540 — GovData Sources (subject-grouped)."""

import pytest
from pytest_bdd import given, when, then, scenario

from provisa.core.models import (
    GOVDATA_SUBJECT_SCHEMAS,
    GovDataSource,
    GovDataSubject,
)


@pytest.fixture
def shared_data() -> dict:
    return {}


@scenario(
    "../features/REQ-540.feature",
    "REQ-540 default behaviour",
)
def test_req540_default_behaviour():
    """Sources of type govdata expose schemas partitioned by subject grouping."""


def _resolve_subject():
    """Pick a real GovData subject that has schemas defined."""
    for subject in GovDataSubject:
        schemas = GOVDATA_SUBJECT_SCHEMAS.get(subject)
        if schemas:
            return subject, schemas
    raise AssertionError("No GovData subject with schemas is defined")


@given("a govdata source configured with a subject grouping")
def govdata_source_configured(shared_data):
    subject, expected_schemas = _resolve_subject()
    source = GovDataSource(
        id="gov-data-1",
        subject=subject,
        govdata_schemas=list(expected_schemas),
        domain_id="gov",
    )
    assert source.subject == subject
    shared_data["source"] = source
    shared_data["subject"] = subject
    shared_data["expected_schemas"] = list(expected_schemas)


@when("the source is registered")
def register_source(shared_data):
    _source = shared_data["source"]
    subject = shared_data["subject"]
    # Registration resolves the subject grouping into the set of exposed schemas.
    exposed_schemas = GOVDATA_SUBJECT_SCHEMAS.get(subject)
    assert exposed_schemas, f"Subject {subject!r} exposed no schemas on registration"
    shared_data["exposed_schemas"] = list(exposed_schemas)


@then("all schemas for that subject are automatically exposed as governed tables")
def all_schemas_exposed(shared_data):
    expected = set(shared_data["expected_schemas"])
    exposed = set(shared_data["exposed_schemas"])
    assert expected, "Expected at least one schema for the configured subject"
    assert exposed == expected, (
        f"Exposed schemas {sorted(exposed)} do not match "
        f"expected subject schemas {sorted(expected)}"
    )
    # Every exposed schema must be a non-empty governed identifier.
    for schema in exposed:
        assert isinstance(schema, str) and schema, f"Invalid schema entry: {schema!r}"
