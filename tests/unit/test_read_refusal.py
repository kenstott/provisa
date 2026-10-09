# Copyright (c) 2026 Kenneth Stott
# Canary: c1e7a4d9-5b02-4f36-8a9d-e60f3b7c2d15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read the deployment cannot answer now is refused by name, not answered as a server fault.

A read that needs every table resident (Cypher ``MATCH (n)``) answered HTTP 500 "Internal server
error" when one table's replica could not be built — a checker that is not installed, say. The
statement is good and the server has not failed: an operator has a table to fix. The build
failure is now a member of the one read-refusal family (``core/read_refusal.py``), which the
pipeline raises and every transport answers in one place.
"""

# Requirements: REQ-1661, REQ-1922

from __future__ import annotations

import json
import types
from pathlib import Path

import grpc
import pytest

import provisa
from provisa.core.read_refusal import ReadRefused
from provisa.core.region_stores import HomeRegionUnavailable
from provisa.federation.replica_state import ReplicaBuildFailed

_TABLE = "dq-checker.quality.pets_scan"
_REASON = "checker 'great_expectations' exited 2: it is not installed"


def test_a_replica_that_could_not_be_built_is_a_read_refusal_naming_the_table_and_the_reason():
    refused = ReplicaBuildFailed(_TABLE, _REASON)
    assert isinstance(refused, ReadRefused)
    assert refused.code == "query.replica_build_failed"
    assert refused.params == {"table": _TABLE, "reason": _REASON}
    assert _TABLE in str(refused) and _REASON in str(refused)


def test_a_build_that_recorded_no_error_says_so():
    refused = ReplicaBuildFailed(_TABLE, None)
    assert refused.params["reason"] == "the build recorded no error"
    assert "None" not in str(refused)


def test_the_home_region_refusal_is_the_same_family_and_keeps_its_answer():
    refused = HomeRegionUnavailable("orders", "eu", "is not built")
    assert isinstance(refused, ReadRefused)
    assert refused.code == "query.home_region_unavailable"
    assert refused.params == {"table": "orders", "region": "eu"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refused",
    [ReplicaBuildFailed(_TABLE, _REASON), HomeRegionUnavailable("orders", "eu", "is not built")],
)
async def test_http_answers_the_family_503_with_its_code_and_what_it_is_about(refused):
    """One handler for the family: not the catch-all's 500 "Internal server error"."""
    from provisa.api.app import create_app

    handler = create_app().exception_handlers[ReadRefused]
    response = await handler(None, refused)  # type: ignore[arg-type]
    assert response.status_code == 503
    assert json.loads(response.body) == {
        "detail": str(refused),
        "code": refused.code,
        "params": refused.params,
    }


@pytest.mark.asyncio
async def test_the_cypher_route_lets_the_refusal_through_to_its_handler(monkeypatch):
    """The failure as it was met: the run of a whole-model Cypher read raised the build failure
    and the route answered it as an unexpected 500."""
    from provisa.api.rest import cypher_router

    async def _execute(plan, state):  # noqa: ARG001
        raise ReplicaBuildFailed(_TABLE, _REASON)

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    monkeypatch.setattr("provisa.pgwire._pipeline.require_governed_plan", lambda plan: None)
    plan = types.SimpleNamespace(physical_sql="SELECT 1", exec_sql="SELECT 1")
    with pytest.raises(ReplicaBuildFailed, match="pets_scan"):
        await cypher_router._run_plan(plan, types.SimpleNamespace())


def test_grpc_answers_the_family_unavailable_not_internal():
    from provisa.grpc.server import _status_for_exception

    assert _status_for_exception(ReplicaBuildFailed(_TABLE, _REASON)) == grpc.StatusCode.UNAVAILABLE
    assert (
        _status_for_exception(HomeRegionUnavailable("orders", "eu", "is not built"))
        == grpc.StatusCode.UNAVAILABLE
    )


def test_no_surface_names_a_member_of_the_family():
    """A surface lets the FAMILY through; one that named a member would miss the next member,
    which is how the build failure came to be a 500."""
    api = Path(provisa.__file__).parent
    members = ("HomeRegionUnavailable", "ReplicaBuildFailed")
    for package in ("api", "grpc", "pgwire", "bolt"):
        for source in sorted((api / package).rglob("*.py")):
            text = source.read_text(encoding="utf-8")
            for member in members:
                assert member not in text, f"{source.relative_to(api)} names {member}"


# The HTTP data routes each end in a catch-all of their own (a query can raise any driver's
# error), which answers what it does not know as 400 or 500. Each lets the family through to
# the app's handler -- or, for JSON:API, answers it in that format -- so the refusal is the same
# 503 on all of them.
_HTTP_DATA_ROUTES = (
    "api/data/endpoint.py",  # GraphQL
    "api/data/endpoint_dev.py",  # SQL
    "api/data/endpoint_grpc_proxy.py",  # the gRPC proxy
    "api/rest/generator.py",  # REST
    "api/rest/cypher_router.py",  # Cypher
    "api/jsonapi/generator.py",  # JSON:API
    "api/rest/neo4j_compat_router.py",  # the Neo4j HTTP-compatible endpoint
    "api/admin/table_profile_router.py",  # a table's sampled profile
)


@pytest.mark.parametrize("route", _HTTP_DATA_ROUTES)
def test_each_http_data_route_lets_the_family_past_its_catch_all(route):
    source = (Path(provisa.__file__).parent / route).read_text(encoding="utf-8")
    assert "except Exception" in source, "the route has a catch-all to get past"
    assert "ReadRefused" in source, f"{route} would answer the refusal from its catch-all"


def test_json_api_answers_the_family_in_its_own_error_format():
    source = (Path(provisa.__file__).parent / "api/jsonapi/generator.py").read_text()
    assert source.count("except ReadRefused as e:") == source.count("except Exception as e:")
    assert '_jsonapi_error_response(503, "Service Unavailable", str(e))' in source


def test_the_neo4j_endpoint_answers_the_family_in_its_own_error_format():
    """503 with the refusal's message, not "Execution failed: …" as a 400."""
    from provisa.api.rest.neo4j_compat_router import _read_refused

    response = _read_refused(ReplicaBuildFailed(_TABLE, _REASON))
    assert response.status_code == 503
    (error,) = json.loads(response.body)["errors"]
    assert _TABLE in error["message"] and _REASON in error["message"]
    assert not error["message"].startswith("Execution failed")
    source = (Path(provisa.__file__).parent / "api/rest/neo4j_compat_router.py").read_text()
    assert source.count("except ReadRefused as exc:") == 2  # governance, and execution


def test_a_table_profile_does_not_retry_or_reword_a_refusal():
    """Its catch-all retries the sample without TABLESAMPLE and answers a second failure as a
    400; a refusal is neither retried nor reworded."""
    source = (Path(provisa.__file__).parent / "api/admin/table_profile_router.py").read_text()
    assert source.count("except ReadRefused:") == source.count("except Exception")
