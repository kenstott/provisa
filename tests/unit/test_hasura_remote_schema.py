# Copyright (c) 2026 Kenneth Stott
# Canary: 13d4e111-7fb2-487d-ab06-01abf0f06b44
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-417: Hasura Remote Schemas map to Provisa graphql_remote sources."""

from __future__ import annotations

from provisa.hasura_v2.mapper import _map_remote_schema, convert_metadata
from provisa.hasura_v2.models import HasuraMetadata, HasuraRemoteSchema


def test_map_remote_schema_url_and_headers():
    rs = HasuraRemoteSchema(
        name="gql-shop",
        definition={
            "url": "https://shop.example/graphql",
            "headers": [{"name": "X-Api-Key", "value": "k"}],
            "forward_client_headers": True,
            "timeout_seconds": 30,
        },
    )
    src = _map_remote_schema(rs)
    assert src.id == "gql-shop"
    assert src.type.value == "graphql_remote"
    assert src.base_url == "https://shop.example/graphql"
    assert src.mapping["headers"] == {"X-Api-Key": "k"}
    assert src.mapping["forward_client_headers"] is True
    assert src.mapping["timeout_seconds"] == 30


def test_map_remote_schema_url_from_env():
    rs = HasuraRemoteSchema(name="r2", definition={"url_from_env": "REMOTE_URL"})
    assert _map_remote_schema(rs).base_url == "${env:REMOTE_URL}"


def test_convert_metadata_includes_remote_schema_source():
    md = HasuraMetadata()
    md.remote_schemas.append(
        HasuraRemoteSchema(name="gql-shop", definition={"url": "https://x/graphql"})
    )
    cfg = convert_metadata(md)
    remote = [s for s in cfg.sources if s.type.value == "graphql_remote"]
    assert [s.id for s in remote] == ["gql-shop"]
