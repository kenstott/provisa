# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Hasura Migration Converters.

REQ-182 — Hasura v2 metadata converter -- CLI tool that reads a Hasura v2 metadata
export directory and emits valid Provisa YAML config. Converts tracked tables,
relationships, permissions, roles, and auth.

REQ-183 — Hasura DDN (v3) HML converter -- CLI tool that reads a DDN supergraph
project and emits valid Provisa YAML config. Converts ObjectTypes, Models,
Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks.

REQ-184 — Shared boolean expression-to-SQL converter for Hasura filter expressions.
Supports `_eq`, `_neq`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_nin`, `_like`,
`_ilike`, `_regex`, `_is_null`, `_and`, `_or`, `_not`. Session variable mapping:
`X-Hasura-<Name>` -> `current_setting('provisa.<name>')`.

REQ-185 — v2 converter maps `select_permissions[].columns` per role -> Provisa column
`visible_to`. `columns: "*"` means all columns visible to that role.

REQ-186 — v2 converter maps `insert/update_permissions[].columns` per role to the
Provisa column `writable_by`. Column write permissions from Hasura v2 are preserved
automatically on import: a column is writable by a role if that role's insert or
update permission lists the column.

REQ-187 — v2 converter maps `select_permissions[].filter` -> Provisa `rls_rules[]`
via boolean expression-to-SQL conversion. `filter: {}` (empty) means no RLS filter.

REQ-188 — v2 converter maps `object_relationships` -> cardinality=many-to-one and
`array_relationships` -> cardinality=one-to-many. Physical column names used directly
(no GraphQL resolution needed).

REQ-189 — DDN converter resolves GraphQL field names to physical column names through
`ObjectType.dataConnectorTypeMapping[].fieldMapping` for all field references in
relationships, permissions, and column definitions.

REQ-190 — v2 auth conversion via optional `--auth-env-file` flag. JWT with `jwk_url`
-> Provisa `provider: oauth`. JWT `claims_map` -> Provisa `role_mapping[]`. Admin
secret -> Provisa `superuser`. Webhook auth emits warning (no Provisa equivalent).

REQ-191 — DDN AggregateExpression metadata preserved in sidecar
`provisa-aggregates.yaml` and converted to Provisa aggregate config.

REQ-192 — Converters emit warnings for unmappable features (event_triggers,
remote_schemas, cron_triggers, BooleanExpressionType) without failing conversion.
v2 Actions and DDN Commands convert to Provisa `functions` config where backed by
stored procedures; webhook-backed actions emit warning with handler URL.

REQ-621 — Both Hasura v2 and DDN converters emit placeholder connection credentials
in the output config (host: localhost, password: ${env:DB_PASSWORD}). Operators must
update connection details before starting Provisa.

REQ-623 — v2 converter maps Hasura source `kind` to Provisa `SourceType`:
`pg`/`postgres` -> `postgresql`, `mssql` -> `sqlserver`, `bigquery` -> `bigquery`,
`citus` -> `postgresql`, `mysql` -> `mysql`. Connection URL (`database_url`) is
parsed into host/port/database/username/password components. Pool settings
(`pool_settings.min_connections`, `pool_settings.max_connections`) are preserved as
`pool_min`/`pool_max`.

REQ-624 — v2 converter upgrades a role to `write` capability when that role has any
`delete_permissions` entry on any table. No per-table delete mapping is produced —
the capability upgrade is the only artefact.

REQ-625 — When a Hasura v2 source `database_url` is an environment variable reference
(`{"from_env": "VAR"}`) or cannot be parsed, the converter substitutes placeholder
values: `host: localhost`, `port: 5432`, `database: default`, `username: postgres`,
`password: ${env:DB_PASSWORD}`. Operators must fix these via `--source-overrides`.

REQ-626 — Roles are collected exclusively from permission entries (select, insert,
update, delete permissions on tables, action permissions, and inherited role
definitions). A Hasura role that exists without any permission entry on any table or
action does not appear in the converter output.

REQ-627 — v2 converter derives the Provisa table `alias` from
`custom_root_fields.select` (first priority), then `custom_root_fields.select_by_pk`
(second priority), then `custom_name` (third priority). All other custom root fields
(`select_aggregate`, `insert`, `update`, `delete`) are not mapped to any Provisa
equivalent.

REQ-628 — When converting DDN HML, if a Model references an ObjectType name that is
not found in any scanned .hml file, that table is skipped and a warning is emitted to
the WarningCollector; the conversion continues rather than aborting.
"""

import io
import os
import subprocess
import sys
import textwrap
import warnings
from pathlib import Path

import pytest
import yaml
from pytest_bdd import given, scenario, then, when

from provisa.core.models import ProvisaConfig
from provisa.ddn.mapper import convert_hml
from provisa.ddn.models import (
    DDNAggregateExpression,
    DDNConnector,
    DDNFieldMapping,
    DDNMetadata,
    DDNModel,
    DDNModelPermission,
    DDNObjectType,
    DDNRelationship,
    DDNTypeMapping,
    DDNTypePermission,
)
from provisa.ddn.parser import parse_hml_dir
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraActionDefinition,
    HasuraCronTrigger,
    HasuraEventTrigger,
    HasuraMetadata,
    HasuraPermission,
    HasuraRemoteSchema,
    HasuraSource,
    HasuraTable,
)
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.filters import bool_expr_to_sql
from provisa.import_shared.warnings import WarningCollector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Scenario bindings
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-182.feature",

    "REQ-182 default behaviour",
)
def test_req_182_default_behaviour():
    """Hasura v2 metadata converter CLI emits valid Provisa YAML config."""


@scenario(
    "../features/REQ-183.feature",

    "REQ-183 default behaviour",
)
def test_req_183_default_behaviour():
    """Hasura DDN HML converter CLI emits valid Provisa YAML config."""


@scenario(
    "../features/REQ-184.feature",

    "REQ-184 default behaviour",
)
def test_req_184_default_behaviour():
    """Shared boolean expression-to-SQL converter produces valid SQL."""


@scenario(
    "../features/REQ-185.feature",

    "REQ-185 default behaviour",
)
def test_req_185_default_behaviour():
    """v2 converter maps select_permissions columns to visible_to."""


@scenario(
    "../features/REQ-186.feature",

    "REQ-186 default behaviour",
)
def test_req_186_default_behaviour():
    """v2 converter maps insert/update column permissions to writable_by."""


@scenario(
    "../features/REQ-187.feature",

    "REQ-187 default behaviour",
)
def test_req_187_default_behaviour():
    """v2 converter maps select_permissions filter to rls_rules via bool_expr_to_sql."""


@scenario(
    "../features/REQ-188.feature",

    "REQ-188 default behaviour",
)
def test_req_188_default_behaviour():
    """v2 converter maps object_relationships to many-to-one and array_relationships to one-to-many."""


@scenario(
    "../features/REQ-189.feature",

    "REQ-189 default behaviour",
)
def test_req_189_default_behaviour():
    """DDN converter resolves GraphQL field names to physical column names via fieldMapping."""


@scenario(
    "../features/REQ-190.feature",

    "REQ-190 default behaviour",
)
def test_req_190_default_behaviour():
    """v2 auth conversion via --auth-env-file produces correct Provisa auth config."""


@scenario(
    "../features/REQ-191.feature",

    "REQ-191 default behaviour",
)
def test_req_191_default_behaviour():
    """DDN AggregateExpression metadata preserved in provisa-aggregates.yaml."""


@scenario(
    "../features/REQ-192.feature",

    "REQ-192 default behaviour",
)
def test_req_192_default_behaviour():
    """Converters emit warnings for unmappable features without aborting."""


@scenario(
    "../features/REQ-621.feature",

    "REQ-621 default behaviour",
)
def test_req_621_default_behaviour():
    """Both converters emit placeholder credentials; Provisa refuses to start without real values."""


@scenario(
    "../features/REQ-623.feature",

    "REQ-623 default behaviour",
)
def test_req_623_default_behaviour():
    """v2 converter maps source kind and parses connection URL with pool settings."""


@scenario(
    "../features/REQ-624.feature",

    "REQ-624 default behaviour",
)
def test_req_624_default_behaviour():
    """v2 converter upgrades role to write capability when delete_permissions exist; no per-table delete mapping."""


@scenario(
    "../features/REQ-625.feature",

    "REQ-625 default behaviour",
)
def test_req_625_default_behaviour():
    """Placeholder connection values substituted for env-var or unparseable database_url."""


@scenario(
    "../features/REQ-626.feature",

    "REQ-626 default behaviour",
)
def test_req_626_default_behaviour():
    """Permission-driven role collection excludes roles with no permission entries."""


@scenario(
    "../features/REQ-627.feature",

    "REQ-627 default behaviour",
)
def test_req_627_default_behaviour():
    """v2 converter derives table alias with select > select_by_pk > custom_name priority."""


@scenario(
    "../features/REQ-628.feature",

    "REQ-628 default behaviour",
)
def test_req_628_default_behaviour():
    """Missing ObjectType tables are skipped with a warning; conversion continues."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_table(config: ProvisaConfig, table_name: str):
    """Locate a converted Provisa table by source table name."""
    for domain in config.domains:
        for table in getattr(domain, "tables", []):
            candidates = {
                getattr(table, "name", None),
                getattr(table, "alias", None),
                getattr(table, "source_table", None),
                getattr(table, "table", None),
            }
            if table_name in candidates:
                return table
    for table in getattr(config, "tables", []) or []:
        candidates = {
            getattr(table, "name", None),
            getattr(table, "alias", None),
            getattr(table, "source_table", None),
            getattr(table, "table", None),
        }
        if table_name in candidates:
            return table
    return None


def _all_tables(config: ProvisaConfig):
    """Yield every table in the converted config regardless of nesting."""
    seen = []
    for domain in getattr(config, "domains", []) or []:
        for table in getattr(domain, "tables", []) or []:
            seen.append(table)
    for table in getattr(config, "tables", []) or []:
        seen.append(table)
    return seen


def _column_visible_to(column) -> set[str]:
    """Return the visible_to set for a converted column."""
    value = getattr(column, "visible_to", None)
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    return set(value)


def _column_writable_by(column) -> set[str]:
    """Return the writable_by set for a converted column."""
    value = getattr(column, "writable_by", None)
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    return set(value)


def _all_warning_messages(warnings_obj: WarningCollector) -> list[str]:
    """Return all warning message strings from a WarningCollector."""
    messages = []
    raw = getattr(warnings_obj, "warnings", None) or getattr(warnings_obj, "_warnings", None)
    if raw is None:
        try:
            raw = list(warnings_obj)
        except TypeError:
            raw = []
    for w in raw:
        if isinstance(w, str):
            messages.append(w)
        else:
            msg = getattr(w, "message", None) or getattr(w, "msg", None) or str(w)
            messages.append(msg)
    return messages


def _make_hml_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    """Create a temporary HML project directory."""
    for rel_path, content in files.items():
        fpath = tmp_path / rel_path
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(textwrap.dedent(content), encoding="utf-8")
    return tmp_path


def _build_hasura_v2_metadata_dir(base: Path) -> Path:
    """
    Write a minimal but complete Hasura v2 metadata export directory structure
    to *base* and return it.
    """
    metadata_dir = base / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")

    db_dir = metadata_dir / "databases" / "default"
    tables_dir = db_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    users_yaml = {
        "table": {"schema": "public", "name": "users"},
        "object_relationships": [
            {
                "name": "profile",
                "using": {
                    "foreign_key_constraint_on": {
                        "column": "user_id",
                        "table": {"schema": "public", "name": "profiles"},
                    }
                },
            }
        ],
        "select_permissions": [
            {
                "role": "viewer",
                "permission": {
                    "columns": ["id", "email", "created_at"],
                    "filter": {},
                },
            },
            {
                "role": "admin",
                "permission": {
                    "columns": "*",
                    "filter": {},
                },
            },
        ],
        "insert_permissions": [
            {
                "role": "admin",
                "permission": {
                    "columns": ["email", "created_at"],
                    "check": {},
                },
            }
        ],
        "update_permissions": [
            {
                "role": "admin",
                "permission": {
                    "columns": ["email"],
                    "filter": {},
                },
            }
        ],
        "delete_permissions": [
            {
                "role": "admin",
                "permission": {"filter": {}},
            }
        ],
    }
    (tables_dir / "public_users.yaml").write_text(
        yaml.dump(users_yaml), encoding="utf-8"
    )

    posts_yaml = {
        "table": {"schema": "public", "name": "posts"},
        "array_relationships": [
            {
                "name": "comments",
                "using": {
                    "foreign_key_constraint_on": {
                        "column": "post_id",
                        "table": {"schema": "public", "name": "comments"},
                    }
                },
            }
        ],
        "select_permissions": [
            {
                "role": "viewer",
                "permission": {
                    "columns": ["id", "title", "body", "author_id"],
                    "filter": {"author_id": {"_eq": "X-Hasura-User-Id"}},
                },
            }
        ],
        "insert_permissions": [
            {
                "role": "editor",
                "permission": {
                    "columns": ["title", "body", "author_id"],
                    "check": {},
                },
            }
        ],
    }
    (tables_dir / "public_posts.yaml").write_text(
        yaml.dump(posts_yaml), encoding="utf-8"
    )

    tables_index = [
        "!include public_users.yaml",
        "!include public_posts.yaml",
    ]
    (tables_dir / "tables.yaml").write_text(
        "\n".join(f"- {entry}" for entry in tables_index) + "\n",
        encoding="utf-8",
    )

    databases_yaml = [
        {
            "name": "default",
            "kind": "postgres",
            "configuration": {
                "connection_info": {
                    "database_url": {
                        "from_env": "PG_DATABASE_URL",
                    }
                }
            },
            "tables": "!include default/tables/tables.yaml",
            "functions": [],
        }
    ]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8"
    )

    actions_yaml = {
        "actions": [
            {
                "name": "registerUser",
                "definition": {
                    "handler": "https://api.example.com/register",
                    "kind": "synchronous",
                    "arguments": [
                        {"name": "email", "type": "String!"},
                    ],
                    "output_type": "RegisterOutput",
                },
            }
        ],
        "custom_types": {
            "input_objects": [],
            "objects": [
                {
                    "name": "RegisterOutput",
                    "fields": [{"name": "id", "type": "Int!"}],
                }
            ],
        },
    }
    (metadata_dir / "actions.yaml").write_text(
        yaml.dump(actions_yaml), encoding="utf-8"
    )
    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")

    remote_schemas_yaml = [
        {
            "name": "payments",
            "definition": {
                "url": "https://payments.example.com/graphql",
                "headers": [],
                "forward_client_headers": True,
            },
        }
    ]
    (metadata_dir / "remote_schemas.yaml").write_text(
        yaml.dump(remote_schemas_yaml), encoding="utf-8"
    )

    cron_triggers_yaml = [
        {
            "name": "cleanup",
            "webhook": "https://api.example.com/cron/cleanup",
            "schedule": "0 0 * * *",
            "payload": {},
        }
    ]
    (metadata_dir / "cron_triggers.yaml").write_text(
        yaml.dump(cron_triggers_yaml), encoding="utf-8"
    )

    for stub_file in (
        "allow_list.yaml",
        "query_collections.yaml",
        "rest_endpoints.yaml",
    ):
        (metadata_dir / stub_file).write_text("[]\n", encoding="utf-8")

    inherited_roles_yaml = [
        {
            "role_name": "user",
            "role_set": ["viewer", "editor"],
        }
    ]
    (metadata_dir / "inherited_roles.yaml").write_text(
        yaml.dump(inherited_roles_yaml), encoding="utf-8"
    )

    return metadata_dir


def _build_ddn_supergraph_project(base: Path) -> Path:
    """
    Write a minimal but complete DDN supergraph project directory to *base*
    and return it.
    """
    project_dir = base / "supergraph"
    project_dir.mkdir(parents=True, exist_ok=True)

    subgraph_dir = project_dir / "subgraphs" / "chinook"

    connector_dir = subgraph_dir / "dataConnectorLinks"
    connector_dir.mkdir(parents=True, exist_ok=True)
    connector_hml = textwrap.dedent(
        """\
        kind: DataConnectorLink
        version: v1
        definition:
          name: chinook_connector
          url:
            singleUrl:
              value: http://localhost:8080/postgres
          schema:
            version: v0.1
            schema:
              scalar_types: {}
              object_types:
                artist:
                  fields:
                    artist_id:
                      type:
                        name: Int
                        type: named
                    name:
                      type:
                        name: String
                        type: named
                album:
                  fields:
                    album_id:
                      type:
                        name: Int
                        type: named
                    title:
                      type:
                        name: String
                        type: named
                    artist_id:
                      type:
                        name: Int
                        type: named
              collections:
                - name: artist
                  type: artist
                - name: album
                  type: album
              functions: []
              procedures: []
        """
    )
    (connector_dir / "chinook_connector.hml").write_text(
        connector_hml, encoding="utf-8"
    )

    models_dir = subgraph_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    artist_object_type_hml = textwrap.dedent(
        """\
        kind: ObjectType
        version: v1
        definition:
          name: Artist
          fields:
            - name: artistId
              type: Int!
            - name: name
              type: String!
          graphql:
            typeName: Artist
          dataConnectorTypeMapping:
            - dataConnectorName: chinook_connector
              dataConnectorObjectType: artist
              fieldMapping:
                artistId:
                  column:
                    name: artist_id
                name:
                  column:
                    name: name
        """
    )
    (models_dir / "Artist.hml").write_text(artist_object_type_hml, encoding="utf-8")

    album_object_type_hml = textwrap.dedent(
        """\
        kind: ObjectType
        version: v1
        definition:
          name: Album
          fields:
            - name: albumId
              type: Int!
            - name: title
              type: String!
            - name: artistId
              type: Int!
          graphql:
            typeName: Album
          dataConnectorTypeMapping:
            - dataConnectorName: chinook_connector
              dataConnectorObjectType: album
              fieldMapping:
                albumId:
                  column:
                    name: album_id
                title:
                  column:
                    name: title
                artistId:
                  column:
                    name: artist_id
        """
    )
    (models_dir / "Album.hml").write_text(album_object_type_hml, encoding="utf-8")

    artist_model_hml = textwrap.dedent(
        """\
        kind: Model
        version: v1
        definition:
          name: Artist
          objectType: Artist
          source:
            dataConnectorName: chinook_connector
            collection: artist
          graphql:
            selectMany:
              queryRootField: artists
            selectUniques:
              - queryRootField: artistById
                uniqueIdentifier:
                  - artistId
            filterInputTypeName: Artist_bool_exp
            orderByExpressionType: Artist_order_by
          orderableFields:
            - fieldName: artistId
              orderByDirections:
                enableAll: true
            - fieldName: name
              orderByDirections:
                enableAll: true
        """
    )
    (models_dir / "ArtistModel.hml").write_text(artist_model_hml, encoding="utf-8")

    album_model_hml = textwrap.dedent(
        """\
        kind: Model
        version: v1
        definition:
          name: Album
          objectType: Album
          source:
            dataConnectorName: chinook_connector
            collection: album
          graphql:
            selectMany:
              queryRootField: albums
            selectUniques:
              - queryRootField: albumById
                uniqueIdentifier:
                  - albumId
            filterInputTypeName: Album_bool_exp
            orderByExpressionType: Album_order_by
          orderableFields:
            - fieldName: albumId
              orderByDirections:
                enableAll: true
            - fieldName: title
              orderByDirections:
                enableAll: true
            - fieldName: artistId
              orderByDirections:
                enableAll: true
        """
    )
    (models_dir / "AlbumModel.hml").write_text(album_model_hml, encoding="utf-8")

    relationships_dir = subgraph_dir / "relationships"
    relationships_dir.mkdir(parents=True, exist_ok=True)

    album_artist_rel_hml = textwrap.dedent(
        """\
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
                  - fieldName: artistId
              target:
                modelField:
                  - fieldName: artistId
        """
    )
    (relationships_dir / "AlbumArtist.hml").write_text(
        album_artist_rel_hml, encoding="utf-8"
    )

    permissions_dir = subgraph_dir / "permissions"
    permissions_dir.mkdir(parents=True, exist_ok=True)

    artist_perms_hml = textwrap.dedent(
        """\
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
        ---
        kind: ModelPermissions
        version: v1
        definition:
          modelName: Artist
          permissions:
            - role: viewer
              select:
                filter: null
        """
    )
    (permissions_dir / "ArtistPermissions.hml").write_text(
        artist_perms_hml, encoding="utf-8"
    )

    album_perms_hml = textwrap.dedent(
        """\
        kind: TypePermissions
        version: v1
        definition:
          typeName: Album
          permissions:
            - role: viewer
              output:
                allowedFields:
                  - albumId
                  - title
                  - artistId
        ---
        kind: ModelPermissions
        version: v1
        definition:
          modelName: Album
          permissions:
            - role: viewer
              select:
                filter:
                  fieldComparison:
                    field: artistId
                    operator: _eq
                    value:
                      literal: 1
        """
    )
    (permissions_dir / "AlbumPermissions.hml").write_text(
        album_perms_hml, encoding="utf-8"
    )

    return project_dir


def _build_hasura_v2_metadata_with_select_permissions(base: Path) -> Path:
    """
    Build a Hasura v2 metadata directory specifically designed to test REQ-185:
    select_permissions[].columns -> Provisa column visible_to.
    """
    metadata_dir = base / "metadata_185"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")

    db_dir = metadata_dir / "databases" / "default"
    tables_dir = db_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    products_yaml = {
        "table": {"schema": "public", "name": "products"},
        "select_permissions": [
            {
                "role": "viewer",
                "permission": {
                    "columns": ["id", "name", "price"],
                    "filter": {},
                },
            },
            {
                "role": "admin",
                "permission": {
                    "columns": "*",
                    "filter": {},
                },
            },
            {
                "role": "auditor",
                "permission": {
                    "columns": ["id", "internal_cost"],
                    "filter": {},
                },
            },
        ],
    }
    (tables_dir / "public_products.yaml").write_text(
        yaml.dump(products_yaml), encoding="utf-8"
    )

    (tables_dir / "tables.yaml").write_text(
        "- !include public_products.yaml\n",
        encoding="utf-8",
    )

    databases_yaml = [
        {
            "name": "default",
            "kind": "postgres",
            "configuration": {
                "connection_info": {
                    "database_url": {"from_env": "PG_DATABASE_URL"},
                }
            },
            "tables": "!include default/tables/tables.yaml",
            "functions": [],
        }
    ]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8"
    )

    for stub_file in (
        "actions.yaml",
        "allow_list.yaml",
        "cron_triggers.yaml",
        "inherited_roles.yaml",
        "query_collections.yaml",
        "remote_schemas.yaml",
        "rest_endpoints.yaml",
    ):
        (metadata_dir / stub_file).write_text("[]\n", encoding="utf-8")

    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")

    return metadata_dir


def _build_partial_ddn_project_missing_object_type(tmp_path: Path) -> Path:
    """
    Build a DDN supergraph project where one Model references an ObjectType
    whose HML file is intentionally absent (GhostType), while another Model
    (Artist) has its ObjectType present.  This exercises REQ-628.
    """
    project_dir = tmp_path / "partial_supergraph"
    project_dir.mkdir(parents=True, exist_ok=True)

    # Supergraph config — references two subgraph dirs, one missing its HML.
    (project_dir / "supergraph.yaml").write_text(
        "kind: Supergraph\nversion: v2\ndefinition:\n  subgraphs:\n"
        "    - globals/subgraph.yaml\n    - app/subgraph.yaml\n",
        encoding="utf-8",
    )
    return project_dir
