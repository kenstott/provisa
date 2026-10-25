# Copyright (c) 2026 Kenneth Stott
# Canary: b5fa11cd-039e-4022-a91b-066923c55cac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A step's first error names the request it answered, as the transport sent it."""

from __future__ import annotations

import sys
from pathlib import Path

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import optimistic_load  # noqa: E402
from queries import Query  # noqa: E402


def test_the_request_text_is_the_transports_own_text():
    q = Query(id="q", category="c", description="d", cypher="MATCH (a:A), (b:B) RETURN a.x")
    assert optimistic_load._request_text(q) == "MATCH (a:A), (b:B) RETURN a.x"


def test_a_structured_request_is_its_json():
    q = Query(id="q", category="c", description="d", grpc={"mode": "scan", "type_name": "PbT"})
    assert optimistic_load._request_text(q) == '{"mode": "scan", "type_name": "PbT"}'
