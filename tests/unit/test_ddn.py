# Copyright (c) 2025 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for DDN (Hasura v3) HML converter."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from provisa.core.models import ProvisaConfig
from provisa.ddn.mapper import convert_hml
from provisa.ddn.models import (
    DDNCommand,
    DDNConnector,
    DDNFieldMapping,
    DDNMetadata,
    DDNModel,
    DDNModelPermission,
    DDNObjectType,
    DDNRelationship,
    DDNTypeMapping,
    DDNTypePermission,
    DDNAggregateExpression,
)
from provisa.ddn.parser import parse_hml_dir
from provisa.import_shared.warnings import WarningCollector


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_hml_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    """Create a temporary HML project directory."""
    for rel_path, content in files.items():
        fpath = tmp_path / rel_path
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(textwrap.dedent(content), encoding="utf-8")
    return tmp_path


def _chinook_metadata() -> DDNMetadata:
    """Minimal Chinook-style metadata for mapper tests."""
    connector = DDNConnector(
        name="chinook_connector",
        subgraph="chinook",
        url="http://localhost:8080/postgres",
    )
    artist_type = DDNObjectType(
        name="Artist",
        subgraph="chinook",
        fields={"artistId": "Int", "name": "String"},
        type_mappings=[DDNTypeMapping(
            connector_name="chinook_connector",
            source_type="artist",
            field_mappings=[
                DDNFieldMapping(graphql_field="artistId", column="artist_id"),
                DDNFieldMapping(graphql_field="name", column="name"),
            ],
        )],
    )
    album_type = DDNObjectType(
        name="Album",
        subgraph="chinook",
        fields={"albumId": "Int", "title": "String", "artistId": "Int"},
        type_mappings=[DDNTypeMapping(
            connector_name="chinook_connector",
            source_type="album",
            field_mappings=[
                DDNFieldMapping(graphql_field="albumId", column="album_id"),
                DDNFieldMapping(graphql_field="title", column="title"),
                DDNFieldMapping(graphql_field="artistId", column="artist_id"),
            ],
        )],
    )
    artist_model = DDNModel(
        name="Artist",
        subgraph="chinook",
        object_type="Artist",
        connector_name="chinook_connector",
        collection="artist",
        graphql_type_name="Artist",
    )
    album_model = DDNModel(
        name="Album",
        subgraph="chinook",
        object_type="Album",
        connector_name="chinook_connector",
        collection="album",
        graphql_type_name="Album",
        aggregate_expression="AlbumAggregate",
    )
    rel = DDNRelationship(
        name="artist",
        subgraph="chinook",
        source_type="Album",
        target_model="Artist",
        rel_type="Object",
        field_mapping={"artistId": "artistId"},
    )
    tp_artist = DDNTypePermission(
        type_name="Artist", role="viewer",
        allowed_fields=["artistId", "name"],
    )
    tp_album = DDNTypePermission(
        type_name="Album", role="viewer",
        allowed_fields=["albumId", "title", "artistId"],
    )
    mp = DDNModelPermission(
        model_name="Album", role="viewer",
        filter={"artistId": {"_eq": "1"}},
    )
    agg = DDNAggregateExpression(
        name="AlbumAggregate",
        subgraph="chinook",
        operand_type="Album",
        count_enabled=True,
        count_distinct=True,
        aggregatable_fields={"title": ["count"]},
    )
    cmd = DDNCommand(
        name="GetArtistById",
        subgraph="chinook",
        connector_name="chinook_connector",
        command_type="function",
        source_name="get_artist_by_id",
        return_type="Artist",
        arguments={"id": "Int"},
        graphql_root_field="getArtistById",
    )

    return DDNMetadata(
        connectors=[connector],
        object_types=[artist_type, album_type],
        models=[artist_model, album_model],
        relationships=[rel],
        type_permissions=[tp_artist, tp_album],
        model_permissions=[mp],
        aggregate_expressions=[agg],
        commands=[cmd],
        subgraphs={"chinook"},
    )


# ── Parser Tests ──────────────────────────────────────────────────────


class TestParser:
    def test_parse_empty_dir(self, tmp_path: Path) -> None:
        collector = WarningCollector()
        md = parse_hml_dir(tmp_path, collector)
        assert len(md.models) == 0
        assert len(md.connectors) == 0

    def test_parse_connector(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/connector.hml": """\
                kind: DataConnectorLink
                version: v1
                definition:
                  name: my_pg
                  url:
                    singleUrl:
                      value: http://localhost:8080/postgres
                  schema: {}
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.connectors) == 1
        assert md.connectors[0].name == "my_pg"
        assert "postgres" in md.connectors[0].url

    def test_parse_object_type_with_field_mapping(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/artist.hml": """\
                kind: ObjectType
                version: v1
                definition:
                  name: Artist
                  fields:
                    - name: artistId
                      type: Int
                    - name: name
                      type: String
                  dataConnectorTypeMapping:
                    - dataConnectorName: my_pg
                      dataConnectorObjectType: artist
                      fieldMapping:
                        artistId:
                          column:
                            name: artist_id
                        name:
                          column:
                            name: name
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.object_types) == 1
        ot = md.object_types[0]
        assert ot.name == "Artist"
        assert len(ot.type_mappings) == 1
        tm = ot.type_mappings[0]
        assert tm.source_type == "artist"
        fm_dict = {fm.graphql_field: fm.column for fm in tm.field_mappings}
        assert fm_dict["artistId"] == "artist_id"
        assert fm_dict["name"] == "name"

    def test_parse_model(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/artist_model.hml": """\
                kind: Model
                version: v1
                definition:
                  name: Artist
                  objectType: Artist
                  source:
                    dataConnectorName: my_pg
                    collection: artist
                  graphql:
                    typeName: Artist
                    selectMany:
                      queryRootField: artists
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.models) == 1
        m = md.models[0]
        assert m.name == "Artist"
        assert m.object_type == "Artist"
        assert m.connector_name == "my_pg"
        assert m.collection == "artist"
        assert m.graphql_select_many == "artists"

    def test_parse_relationship(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/album_artist_rel.hml": """\
                kind: Relationship
                version: v1
                definition:
                  name: artist
                  sourceType: Album
                  target:
                    model:
                      name: Artist
                      relationshipType: Object
                  mapping:
                    - source:
                        fieldPath:
                          - artistId
                      target:
                        fieldPath:
                          - artistId
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.relationships) == 1
        r = md.relationships[0]
        assert r.name == "artist"
        assert r.source_type == "Album"
        assert r.target_model == "Artist"
        assert r.rel_type == "Object"
        assert r.field_mapping == {"artistId": "artistId"}

    def test_parse_type_permissions(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/artist_perms.hml": """\
                kind: TypePermissions
                version: v1
                definition:
                  typeName: Artist
                  permissions:
                    - role: viewer
                      output:
                        allowedFields:
                          - artistId
                          - name
                    - role: admin
                      output:
                        allowedFields:
                          - artistId
                          - name
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.type_permissions) == 2
        assert md.type_permissions[0].role == "viewer"
        assert md.type_permissions[1].role == "admin"

    def test_parse_model_permissions(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/album_model_perms.hml": """\
                kind: ModelPermissions
                version: v1
                definition:
                  modelName: Album
                  permissions:
                    - role: viewer
                      filter:
                        artistId:
                          _eq: "1"
                    - role: admin
                      filter: null
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.model_permissions) == 2
        assert md.model_permissions[0].role == "viewer"
        assert md.model_permissions[0].filter == {"artistId": {"_eq": "1"}}
        assert md.model_permissions[1].filter == {}

    def test_parse_command(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/get_artist.hml": """\
                kind: Command
                version: v1
                definition:
                  name: GetArtistById
                  source:
                    dataConnectorName: my_pg
                    function:
                      - get_artist_by_id
                  outputType: Artist
                  arguments:
                    - name: id
                      type: Int
                  graphql:
                    rootFieldName: getArtistById
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.commands) == 1
        cmd = md.commands[0]
        assert cmd.name == "GetArtistById"
        assert cmd.command_type == "function"
        assert cmd.source_name == "get_artist_by_id"
        assert cmd.arguments == {"id": "Int"}

    def test_parse_command_procedure(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/create_artist.hml": """\
                kind: Command
                version: v1
                definition:
                  name: CreateArtist
                  source:
                    dataConnectorName: my_pg
                    procedure:
                      - create_artist
                  outputType: Artist
                  arguments:
                    - name: name
                      type: String
                  graphql:
                    rootFieldName: createArtist
            """,
        })
        md = parse_hml_dir(hml)
        assert md.commands[0].command_type == "procedure"
        assert md.commands[0].source_name == "create_artist"

    def test_parse_skipped_kinds(self, tmp_path: Path) -> None:
        collector = WarningCollector()
        _make_hml_dir(tmp_path, {
            "app/metadata/order_by.hml": """\
                kind: OrderByExpression
                version: v1
                definition:
                  name: ArtistOrderBy
            """,
            "app/metadata/bool_expr.hml": """\
                kind: BooleanExpressionType
                version: v1
                definition:
                  name: ArtistBoolExp
            """,
            "app/metadata/auth.hml": """\
                kind: AuthConfig
                version: v1
                definition:
                  mode: noAuth
            """,
        })
        md = parse_hml_dir(tmp_path, collector)
        assert md.skipped_kinds["OrderByExpression"] == 1
        assert md.skipped_kinds["BooleanExpressionType"] == 1
        assert md.skipped_kinds["AuthConfig"] == 1
        assert collector.has_warnings()
        # BooleanExpressionType and AuthConfig should have warnings
        categories = {w.category for w in collector.warnings}
        assert "BooleanExpressionType" in categories
        assert "AuthConfig" in categories

    def test_parse_aggregate_expression(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/agg.hml": """\
                kind: AggregateExpression
                version: v1
                definition:
                  name: AlbumAggregate
                  operand:
                    object:
                      aggregatedType: Album
                      aggregatableFields:
                        - fieldName: title
                          aggregateExpression:
                            enabledAggregationFunctions:
                              - name: count
                  count:
                    enable: true
                    enableDistinct: true
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.aggregate_expressions) == 1
        agg = md.aggregate_expressions[0]
        assert agg.name == "AlbumAggregate"
        assert agg.count_enabled is True
        assert agg.count_distinct is True
        assert "title" in agg.aggregatable_fields

    def test_parse_multi_doc_file(self, tmp_path: Path) -> None:
        """Multiple YAML documents in one HML file."""
        hml = _make_hml_dir(tmp_path, {
            "app/metadata/combined.hml": """\
                kind: DataConnectorLink
                version: v1
                definition:
                  name: my_pg
                  url:
                    singleUrl:
                      value: http://localhost/pg
                  schema: {}
                ---
                kind: ObjectType
                version: v1
                definition:
                  name: Track
                  fields:
                    - name: trackId
                      type: Int
                  dataConnectorTypeMapping:
                    - dataConnectorName: my_pg
                      dataConnectorObjectType: track
                      fieldMapping:
                        trackId:
                          column:
                            name: track_id
            """,
        })
        md = parse_hml_dir(hml)
        assert len(md.connectors) == 1
        assert len(md.object_types) == 1

    def test_subgraph_detection(self, tmp_path: Path) -> None:
        hml = _make_hml_dir(tmp_path, {
            "sales/metadata/order.hml": """\
                kind: Model
                version: v1
                definition:
                  name: Order
                  objectType: Order
                  source:
                    dataConnectorName: pg
                    collection: orders
            """,
            "globals/metadata/auth.hml": """\
                kind: AuthConfig
                version: v1
                definition:
                  mode: noAuth
            """,
        })
        collector = WarningCollector()
        md = parse_hml_dir(hml, collector)
        assert "sales" in md.subgraphs
        assert "globals" not in md.subgraphs


# ── Mapper Tests ──────────────────────────────────────────────────────


class TestMapper:
    def test_convert_sources(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        assert len(config.sources) == 1
        assert config.sources[0].id == "chinook_connector"
        assert config.sources[0].type.value == "postgresql"

    def test_convert_domains_from_subgraphs(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        domain_ids = {d.id for d in config.domains}
        assert "chinook" in domain_ids
        assert "default" in domain_ids

    def test_convert_tables(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        assert len(config.tables) == 2
        table_names = {t.table_name for t in config.tables}
        assert "artist" in table_names
        assert "album" in table_names

    def test_field_to_column_resolution(self) -> None:
        """GraphQL field artistId -> physical column artist_id."""
        md = _chinook_metadata()
        config = convert_hml(md)
        artist_table = next(t for t in config.tables if t.table_name == "artist")
        col_names = {c.name for c in artist_table.columns}
        # Physical column names, not GraphQL field names
        assert "artist_id" in col_names
        assert "name" in col_names
        # Alias should be the GraphQL field name when different
        id_col = next(c for c in artist_table.columns if c.name == "artist_id")
        assert id_col.alias == "artistId"

    def test_type_permissions_to_visible_to(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        artist_table = next(t for t in config.tables if t.table_name == "artist")
        id_col = next(c for c in artist_table.columns if c.name == "artist_id")
        assert "viewer" in id_col.visible_to

    def test_model_permissions_to_rls(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        assert len(config.rls_rules) == 1
        rule = config.rls_rules[0]
        assert rule.role_id == "viewer"
        assert "artist_id" in rule.filter
        assert "= '1'" in rule.filter

    def test_relationship_mapping(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        assert len(config.relationships) == 1
        rel = config.relationships[0]
        assert rel.source_column == "artist_id"
        assert rel.target_column == "artist_id"
        assert rel.cardinality == "many-to-one"

    def test_roles_collected(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        role_ids = {r.id for r in config.roles}
        assert "viewer" in role_ids

    def test_command_to_function(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        assert len(config.functions) == 1
        fn = config.functions[0]
        assert fn.name == "getArtistById"
        assert fn.function_name == "get_artist_by_id"
        assert fn.source_id == "chinook_connector"
        assert len(fn.arguments) == 1
        assert fn.arguments[0].name == "id"

    def test_aggregate_expression_annotation(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md)
        album_table = next(t for t in config.tables if t.table_name == "album")
        assert album_table.description is not None
        assert "aggregates" in album_table.description
        assert "count" in album_table.description

    def test_config_validates(self) -> None:
        """Output passes ProvisaConfig.model_validate()."""
        md = _chinook_metadata()
        config = convert_hml(md)
        dumped = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(dumped)
        assert len(validated.tables) == 2

    def test_domain_map_override(self) -> None:
        md = _chinook_metadata()
        config = convert_hml(md, domain_map={"chinook": "music"})
        domain_ids = {d.id for d in config.domains}
        assert "music" in domain_ids
        tables_domain = {t.domain_id for t in config.tables}
        assert "music" in tables_domain

    def test_source_overrides(self) -> None:
        md = _chinook_metadata()
        overrides = {
            "chinook_connector": {
                "host": "prod-db.example.com",
                "port": 5433,
            },
        }
        config = convert_hml(md, source_overrides=overrides)
        assert config.sources[0].host == "prod-db.example.com"
        assert config.sources[0].port == 5433

    def test_warning_collection(self) -> None:
        md = _chinook_metadata()
        md.skipped_kinds["BooleanExpressionType"] = 3
        md.skipped_kinds["AuthConfig"] = 1
        collector = WarningCollector()
        # Warnings for BooleanExpressionType/AuthConfig are emitted by parser,
        # not mapper. Mapper only warns on unknown skipped kinds.
        # Add an unknown skipped kind to test mapper warnings
        md.skipped_kinds["UnknownKind"] = 2
        convert_hml(md, collector=collector)
        assert collector.has_warnings()

    def test_empty_metadata(self) -> None:
        md = DDNMetadata()
        config = convert_hml(md)
        assert len(config.tables) == 0
        assert len(config.sources) == 0
        dumped = config.model_dump(by_alias=True)
        ProvisaConfig.model_validate(dumped)

    def test_missing_object_type_warns(self) -> None:
        md = DDNMetadata(
            models=[DDNModel(
                name="Ghost", object_type="NonExistent",
                connector_name="pg", collection="ghost",
            )],
            connectors=[DDNConnector(name="pg")],
            subgraphs={"default"},
        )
        collector = WarningCollector()
        config = convert_hml(md, collector=collector)
        assert len(config.tables) == 0
        assert collector.has_warnings()
        assert any("NonExistent" in w.message for w in collector.warnings)


# ── Integration Parser + Mapper ───────────────────────────────────────


class TestIntegration:
    def test_parse_and_convert_chinook_project(self, tmp_path: Path) -> None:
        """Full round-trip: parse HML files -> convert -> validate."""
        hml = _make_hml_dir(tmp_path, {
            "chinook/metadata/connector.hml": """\
                kind: DataConnectorLink
                version: v1
                definition:
                  name: chinook_pg
                  url:
                    singleUrl:
                      value: http://localhost:8080/postgres
                  schema: {}
            """,
            "chinook/metadata/artist_type.hml": """\
                kind: ObjectType
                version: v1
                definition:
                  name: Artist
                  fields:
                    - name: artistId
                      type: Int
                    - name: name
                      type: String
                  dataConnectorTypeMapping:
                    - dataConnectorName: chinook_pg
                      dataConnectorObjectType: artist
                      fieldMapping:
                        artistId:
                          column:
                            name: artist_id
                        name:
                          column:
                            name: name
            """,
            "chinook/metadata/artist_model.hml": """\
                kind: Model
                version: v1
                definition:
                  name: Artist
                  objectType: Artist
                  source:
                    dataConnectorName: chinook_pg
                    collection: artist
                  graphql:
                    typeName: Artist
                    selectMany:
                      queryRootField: artists
            """,
            "chinook/metadata/artist_perms.hml": """\
                kind: TypePermissions
                version: v1
                definition:
                  typeName: Artist
                  permissions:
                    - role: user
                      output:
                        allowedFields:
                          - artistId
                          - name
            """,
            "chinook/metadata/artist_model_perms.hml": """\
                kind: ModelPermissions
                version: v1
                definition:
                  modelName: Artist
                  permissions:
                    - role: user
                      filter: null
            """,
            "chinook/metadata/bool_expr.hml": """\
                kind: BooleanExpressionType
                version: v1
                definition:
                  name: ArtistBoolExp
            """,
        })
        collector = WarningCollector()
        md = parse_hml_dir(hml, collector)
        config = convert_hml(md, collector=collector)

        # Validate
        dumped = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(dumped)

        assert len(validated.sources) == 1
        assert validated.sources[0].id == "chinook_pg"
        assert len(validated.tables) == 1
        assert validated.tables[0].table_name == "artist"

        # Field resolution
        id_col = next(
            c for c in validated.tables[0].columns if c.name == "artist_id"
        )
        assert id_col.alias == "artistId"
        assert "user" in id_col.visible_to

        # Warnings for BooleanExpressionType
        assert collector.has_warnings()
        assert any(
            w.category == "BooleanExpressionType" for w in collector.warnings
        )

    def test_yaml_output_roundtrip(self, tmp_path: Path) -> None:
        """Config can be serialized to YAML and re-loaded."""
        md = _chinook_metadata()
        config = convert_hml(md)
        dumped = config.model_dump(by_alias=True, exclude_none=True, mode="json")
        yaml_str = yaml.dump(dumped, default_flow_style=False, sort_keys=False)

        reloaded = yaml.safe_load(yaml_str)
        validated = ProvisaConfig.model_validate(reloaded)
        assert len(validated.tables) == 2
