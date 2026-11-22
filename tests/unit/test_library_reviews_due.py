# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e9b14-83f7-4a5d-b0e1-d47a92c3f518
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2259, REQ-2208: Context Health's documents card counts each document's review items --
drifted term citations and tag or assignment citations that need review -- among the citations
the caller reads, through the one gated read of what cites a document."""

# Requirements: REQ-2259, REQ-2208

from __future__ import annotations

import re
from pathlib import Path

from provisa.api.admin.reference_library_router import reviews_due

_ROUTER = Path(__file__).resolve().parents[2] / "provisa/api/admin/reference_library_router.py"


def _tag(needs_review: bool) -> dict:
    return {
        "tag_id": "pii",
        "state": "drifted" if needs_review else "current",
        "needs_review": needs_review,
    }


def test_review_items_are_drifted_terms_and_tag_citations_needing_review():
    cited = {
        "terms": [],
        "drifted": 2,
        "tags": [_tag(True), _tag(False)],
        "tag_assignments": [_tag(True), _tag(True)],
    }
    assert reviews_due(cited) == 5


def test_a_document_whose_citations_are_current_has_none_due():
    cited = {"terms": [], "drifted": 0, "tags": [_tag(False)], "tag_assignments": []}
    assert reviews_due(cited) == 0


def test_the_endpoint_reads_through_the_gated_scope_and_cited_by():
    source = _ROUTER.read_text()
    block = re.search(r'@router\.get\("/reviews-due"\).*?(?=\n@router\.|\Z)', source, re.S)
    assert block is not None
    body = block.group(0)
    assert "_read_scope(request)" in body
    assert "_may_read(scope, d.domains)" in body
    assert "await cited_by(d.id, request)" in body
