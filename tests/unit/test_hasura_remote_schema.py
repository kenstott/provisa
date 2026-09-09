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
    assert src.path == "https://shop.example/graphql"
    assert src.mapping["headers"] == {"X-Api-Key": "k"}
    assert src.mapping["forward_client_headers"] is True
    assert src.mapping["timeout_seconds"] == 30


def test_map_remote_schema_url_from_env():
    rs = HasuraRemoteSchema(name="r2", definition={"url_from_env": "REMOTE_URL"})
    assert _map_remote_schema(rs).path == "${env:REMOTE_URL}"


def test_convert_metadata_includes_remote_schema_source():
    md = HasuraMetadata()
    md.remote_schemas.append(
        HasuraRemoteSchema(name="gql-shop", definition={"url": "https://x/graphql"})
    )
    cfg = convert_metadata(md)
    remote = [s for s in cfg.sources if s.type.value == "graphql_remote"]
    assert [s.id for s in remote] == ["gql-shop"]


# --- REQ-1681: the remote schema is landed as tables from its role SDLs ---

_SDL = """
schema { query: Query }
type Continent { code: ID! name: String! countries: [Country!]! }
type Country { code: ID! name: String! capital: String continent: Continent! languages: [Language!]! }
type Language { code: ID! name: String rtl: Boolean! }
type Query {
  continent(code: ID!): Continent
  countries(filter: CountryFilterInput): [Country!]!
  languages: [Language!]!
}
input CountryFilterInput { code: String }
"""


def _countries(perms):
    return HasuraRemoteSchema(
        name="countries", definition={"url": "https://x/graphql"}, permissions=perms
    )


def test_remote_schema_lands_one_table_per_root_field():
    from provisa.import_shared.warnings import WarningCollector

    col = WarningCollector()
    md = HasuraMetadata()
    md.remote_schemas.append(_countries([{"role": "user", "definition": {"schema": _SDL}}]))
    cfg = convert_metadata(md, collector=col, domain_map={"countries": "geo"})
    by_name = {t.table_name: t for t in cfg.tables if t.source_id == "countries"}
    assert sorted(by_name) == ["continent", "countries", "languages"]
    countries = by_name["countries"]
    assert countries.schema_name == "graphql" and countries.domain_id == "geo"
    cols = {c.name: c for c in countries.columns}
    assert cols["name"].data_type == "varchar" and cols["name"].visible_to == ["org_admin", "user"]
    assert cols["capital"].data_type == "varchar"
    assert "continent" not in cols and "languages" not in cols
    assert by_name["languages"].columns[-1].name == "rtl"
    assert {c.name: c.data_type for c in by_name["languages"].columns}["rtl"] == "boolean"
    nested = [w for w in col.warnings if "nested fields not landed" in w.message]
    assert any("countries" in w.message and "continent, languages" in w.message for w in nested)


def test_required_root_argument_becomes_native_filter_column():
    md = HasuraMetadata()
    md.remote_schemas.append(_countries([{"role": "user", "definition": {"schema": _SDL}}]))
    cfg = convert_metadata(md)
    continent = next(t for t in cfg.tables if t.table_name == "continent")
    nf = continent.columns[0]
    assert nf.name == "_nf_code" and nf.native_filter_type == "query_param"
    assert nf.visible_to == [] and nf.data_type == "varchar"


def test_visibility_is_the_union_of_roles_that_expose_the_field():
    narrow = _SDL.replace("capital: String ", "")
    md = HasuraMetadata()
    md.remote_schemas.append(
        _countries(
            [
                {"role": "user", "definition": {"schema": _SDL}},
                {"role": "guest", "definition": {"schema": narrow}},
            ]
        )
    )
    cfg = convert_metadata(md)
    cols = {c.name: c for c in next(t for t in cfg.tables if t.table_name == "countries").columns}
    assert cols["name"].visible_to == ["guest", "org_admin", "user"]
    assert cols["capital"].visible_to == ["org_admin", "user"]


def test_remote_schema_without_permissions_lands_nothing_and_warns():
    from provisa.import_shared.warnings import WarningCollector

    col = WarningCollector()
    md = HasuraMetadata()
    md.remote_schemas.append(_countries([]))
    cfg = convert_metadata(md, collector=col)
    assert [t for t in cfg.tables if t.source_id == "countries"] == []
    assert any("carries no role permissions" in w.message for w in col.warnings)
