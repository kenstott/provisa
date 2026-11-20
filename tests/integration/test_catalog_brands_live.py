# Copyright (c) 2026 Kenneth Stott
# Canary: 151ffb9d-e8c6-4ec4-a78c-fc5489dfa969
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2265: the DataHub brand against a running GMS (the test stack's datahub-gms).

DataHub's GMS generates its OpenAPI v3 spec from its entity registry, so the brand reads it from
the instance. These tests read the spec the GMS serves where the brand reads it, check that the
generic OpenAPI source offers each catalog entity as a scroll-paged list of rows, and check that
the recording the unit tests read (tests/fixtures/catalog_brands/datahub_api_docs.json.gz) is
this GMS's spec trimmed to those lists. When the recording differs, the failure carries the
trimmed spec as gzip in base64, to replace the recording with.
"""

# Requirements: REQ-2265

from __future__ import annotations

import base64
import gzip
import json
import os

import httpx
import pytest

from provisa.openapi.brands import BRANDS
from provisa.openapi.loader import load_spec
from provisa.openapi.mapper import parse_spec
from tests.fixtures.catalog_brands.trim_datahub_spec import OUT, PATHS, trimmed

pytestmark = [pytest.mark.integration, pytest.mark.requires_datahub]

DATAHUB = BRANDS["datahub"]


def _gms() -> str:
    return f"http://localhost:{os.environ['DATAHUB_GMS_PORT']}"


@pytest.fixture(scope="module")
def served() -> dict:
    """The spec as the brand reads it: from the address its setup names."""
    return load_spec(DATAHUB.source_spec_path(DATAHUB.setup({"base_url": _gms()})))


def test_each_catalog_entity_is_offered_as_a_scroll_paged_list_of_rows(served):
    offered = {t.path: t for t in parse_spec(served)[0]}
    for path in PATHS:
        table = offered[path]
        assert (table.is_list, table.rows_field) == (True, "entities"), path
        assert table.pagination is not None, path
        paging = table.pagination.model_dump(mode="json", exclude_unset=True)
        assert paging == {
            "rows_field": "entities",
            "type": "cursor",
            "cursor_param": "scrollId",
            "cursor_field": "scrollId",
            "page_size_param": "count",
        }, path


def test_the_brands_credential_check_is_answered():
    resp = httpx.get(_gms() + DATAHUB.verify_path, timeout=60)
    assert resp.status_code == 200, resp.text[:500]
    assert "entities" in resp.json()


def test_the_recording_the_unit_tests_read_is_this_gms_spec(served):
    recorded = trimmed(served)
    shipped = json.loads(gzip.open(OUT).read()) if OUT.exists() else None
    if shipped != recorded:
        packed = gzip.compress(json.dumps(recorded, sort_keys=True).encode())
        pytest.fail(
            f"{OUT.name} is not the test stack's GMS spec trimmed; replace it with:\n"
            f"RECORDING-BEGIN {base64.b64encode(packed).decode()} RECORDING-END"
        )
