# Copyright (c) 2026 Kenneth Stott
# Canary: 090c22b1-8121-4d9f-b541-1a0dd2626124
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered parameter column records whether its source requires a value for it (#204).

Each registration path sets it from what the source states: an OpenAPI path parameter, or one
the spec marks required; a GraphQL argument that is non-null with no default. Preview and
Profile ask for the required values first, so a table is never read without them.
"""

from __future__ import annotations

from provisa.api.admin import _graphql_table_registration as registration
from provisa.core.models import Column
from provisa.openapi.mapper import _extract_params


def test_an_openapi_parameter_is_required_when_the_spec_says_so():
    path, query = _extract_params(
        [
            {"name": "petId", "in": "path", "required": True, "schema": {"type": "integer"}},
            # A path parameter is always required, whatever a loose spec leaves out.
            {"name": "ownerId", "in": "path", "schema": {"type": "string"}},
            {"name": "status", "in": "query", "required": True, "schema": {"type": "string"}},
            {"name": "limit", "in": "query", "schema": {"type": "integer"}},
            {"name": "tag", "in": "query", "required": False, "schema": {"type": "string"}},
        ]
    )
    assert {p["name"]: p["required"] for p in path} == {"petId": True, "ownerId": True}
    assert {p["name"]: p["required"] for p in query} == {
        "status": True,
        "limit": False,
        "tag": False,
    }


def test_a_remote_graphql_tables_required_argument_is_recorded_required():
    columns = registration._column_models(
        {
            "columns": [{"name": "id", "type": "integer"}],
            "required_args": [{"name": "owner", "provisa_type": "text"}],
        }
    )
    by_name = {c.name: c for c in columns}
    assert by_name["_nf_owner"].native_filter_type == "query_param"
    assert by_name["_nf_owner"].native_filter_required is True
    assert by_name["id"].native_filter_required is None  # not a parameter


def test_a_column_says_nothing_until_a_registration_records_it():
    assert (
        Column(name="_nf_id", visible_to=[], native_filter_type="path_param").native_filter_required
        is None
    )


def test_an_old_row_is_resolved_only_where_required_is_certain():
    from provisa.api.admin.schema_helpers import native_filter_required as resolved

    # Recorded: it stands, whatever the kind.
    assert resolved(False, "query_param", "graphql_remote") is False
    assert resolved(True, "query_param", "openapi") is True
    # Nothing recorded: a remote GraphQL table registers only its required arguments, and a
    # path cannot be built without its parameter.
    assert resolved(None, "query_param", "graphql_remote") is True
    assert resolved(None, "path_param", "openapi") is True
    # An OpenAPI query parameter and a gRPC input may be optional: not made required.
    assert resolved(None, "query_param", "openapi") is None
    assert resolved(None, "grpc_input", "grpc_remote") is None
    assert resolved(None, None, "graphql_remote") is None  # not a parameter
