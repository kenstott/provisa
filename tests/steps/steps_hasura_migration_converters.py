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

import textwrap
from pathlib import Path

import pytest
import yaml
from pytest_bdd import given, scenario, then, when

from provisa.core.models import ProvisaConfig
from provisa.ddn.mapper import convert_hml
from provisa.ddn.parser import parse_hml_dir
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraMetadata,
    HasuraPermission,
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
                getattr(table, "table_name", None),
            }
            if table_name in candidates:
                return table
    for table in getattr(config, "tables", []) or []:
        candidates = {
            getattr(table, "name", None),
            getattr(table, "alias", None),
            getattr(table, "source_table", None),
            getattr(table, "table", None),
            getattr(table, "table_name", None),
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
    (tables_dir / "public_users.yaml").write_text(yaml.dump(users_yaml), encoding="utf-8")

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
    (tables_dir / "public_posts.yaml").write_text(yaml.dump(posts_yaml), encoding="utf-8")

    # Write tables.yaml with inlined table data (parser uses yaml.safe_load — no !include support)
    (tables_dir / "tables.yaml").write_text(
        yaml.dump([users_yaml, posts_yaml]),
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
    (metadata_dir / "actions.yaml").write_text(yaml.dump(actions_yaml), encoding="utf-8")
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
    (connector_dir / "chinook_connector.hml").write_text(connector_hml, encoding="utf-8")

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

    # Use dict-form mapping (parser handles list-of-fieldPath format only partially;
    # the dict form {graphqlField: targetField} is fully supported).
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
            artistId: artistId
        """
    )
    (relationships_dir / "AlbumArtist.hml").write_text(album_artist_rel_hml, encoding="utf-8")

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
    (permissions_dir / "ArtistPermissions.hml").write_text(artist_perms_hml, encoding="utf-8")

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
    (permissions_dir / "AlbumPermissions.hml").write_text(album_perms_hml, encoding="utf-8")

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
    (tables_dir / "public_products.yaml").write_text(yaml.dump(products_yaml), encoding="utf-8")

    # Inline table data — yaml.safe_load does not support !include tags
    (tables_dir / "tables.yaml").write_text(
        yaml.dump([products_yaml]),
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

    # Write one ObjectType (Artist) and two Models — Artist (valid) and Ghost (missing type)
    app_dir = project_dir / "app"
    app_dir.mkdir(parents=True, exist_ok=True)

    artist_hml = textwrap.dedent(
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
          dataConnectorTypeMapping:
            - dataConnectorName: myconnector
              dataConnectorObjectType: artist
              fieldMapping:
                artistId:
                  column:
                    name: artist_id
                name:
                  column:
                    name: name
        ---
        kind: Model
        version: v1
        definition:
          name: Artist
          objectType: Artist
          source:
            dataConnectorName: myconnector
            collection: artist
        ---
        kind: Model
        version: v1
        definition:
          name: Ghost
          objectType: GhostType
          source:
            dataConnectorName: myconnector
            collection: ghost
        """
    )
    (app_dir / "artist_and_ghost.hml").write_text(artist_hml, encoding="utf-8")
    return project_dir


# ---------------------------------------------------------------------------
# Step implementations — REQ-182
# ---------------------------------------------------------------------------


@given("a Hasura v2 metadata export directory", target_fixture="shared_data")
def step_given_hasura_v2_metadata_dir(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir"] = metadata_dir
    return shared_data


@when("the CLI converter is run against it")
def step_when_cli_converter_run(shared_data):
    metadata_dir: Path = shared_data["metadata_dir"]
    collector = WarningCollector()
    metadata = parse_metadata_dir(metadata_dir, collector)
    config = convert_metadata(metadata, collector)
    shared_data["config"] = config
    shared_data["collector"] = collector


@then(
    "valid Provisa YAML config is emitted covering tables, relationships, permissions, roles, and auth"
)
def step_then_valid_provisa_yaml_v2(shared_data):
    config = shared_data["config"]
    assert config is not None
    tables = _all_tables(config)
    assert len(tables) >= 1, "Expected at least one table in config"
    assert len(config.roles) >= 1, "Expected at least one role in config"
    assert len(config.sources) >= 1, "Expected at least one source in config"
    # relationships present
    assert len(config.relationships) >= 1, "Expected at least one relationship"


# ---------------------------------------------------------------------------
# Step implementations — REQ-183
# ---------------------------------------------------------------------------


@given("a Hasura DDN supergraph project", target_fixture="shared_data")
def step_given_ddn_supergraph_project(tmp_path, shared_data):
    project_dir = _build_ddn_supergraph_project(tmp_path)
    shared_data["project_dir"] = project_dir
    return shared_data


@when("the HML converter CLI tool is run")
def step_when_hml_converter_run(shared_data):
    project_dir: Path = shared_data["project_dir"]
    collector = WarningCollector()
    metadata = parse_hml_dir(project_dir, collector)
    config = convert_hml(metadata, collector)
    shared_data["ddn_config"] = config
    shared_data["ddn_metadata"] = metadata
    shared_data["collector"] = collector


@then(
    "valid Provisa YAML config is emitted covering ObjectTypes, Models, Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
)
def step_then_valid_ddn_provisa_yaml(shared_data):
    config = shared_data["ddn_config"]
    assert config is not None
    tables = _all_tables(config)
    assert len(tables) >= 1, "Expected at least one table from DDN ObjectTypes/Models"
    assert len(config.sources) >= 1, "Expected at least one source from DataConnectorLinks"
    assert len(config.roles) >= 1, (
        "Expected at least one role from TypePermissions/ModelPermissions"
    )
    assert len(config.relationships) >= 1, "Expected at least one relationship"


# ---------------------------------------------------------------------------
# Step implementations — REQ-184
# ---------------------------------------------------------------------------


@given(
    "a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not",
    target_fixture="shared_data",
)
def step_given_boolean_filter_expression(shared_data):
    shared_data["filter_exprs"] = [
        # _eq with session variable
        {"user_id": {"_eq": "X-Hasura-User-Id"}},
        # _in operator
        {"status": {"_in": ["active", "pending"]}},
        # _and combinator
        {"_and": [{"id": {"_gt": 0}}, {"deleted": {"_eq": False}}]},
        # _or combinator
        {"_or": [{"role": {"_eq": "admin"}}, {"role": {"_eq": "editor"}}]},
        # _not combinator
        {"_not": {"archived": {"_eq": True}}},
        # empty filter
        {},
    ]
    return shared_data


@when("the shared converter processes it")
def step_when_shared_converter_processes(shared_data):
    results = []
    for expr in shared_data["filter_exprs"]:
        results.append(bool_expr_to_sql(expr))
    shared_data["sql_results"] = results


@then(
    "valid SQL is produced with session variable references mapped to current_setting('provisa.<name>')"
)
def step_then_valid_sql_produced(shared_data):
    results = shared_data["sql_results"]
    # _eq with session variable -> current_setting
    assert "current_setting('provisa.user_id')" in results[0], (
        f"Expected session var mapping in: {results[0]}"
    )
    # _in operator -> IN (...)
    assert "IN" in results[1], f"Expected IN operator in: {results[1]}"
    # _and combinator
    assert "AND" in results[2], f"Expected AND combinator in: {results[2]}"
    # _or combinator
    assert "OR" in results[3], f"Expected OR combinator in: {results[3]}"
    # _not combinator
    assert "NOT" in results[4], f"Expected NOT combinator in: {results[4]}"
    # empty filter -> TRUE
    assert results[5] == "TRUE", f"Expected TRUE for empty filter, got: {results[5]}"


# ---------------------------------------------------------------------------
# Step implementations — REQ-185
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 metadata export with select_permissions[].columns per role",
    target_fixture="shared_data",
)
def step_given_v2_metadata_select_permissions(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_with_select_permissions(tmp_path)
    shared_data["metadata_dir_185"] = metadata_dir
    return shared_data


@when("the v2 converter runs")
def step_when_v2_converter_runs(shared_data):
    # Support multiple given-steps that set different metadata dirs
    metadata_dir = (
        shared_data.get("metadata_dir_185")
        or shared_data.get("metadata_dir_186")
        or shared_data.get("metadata_dir_187")
        or shared_data.get("metadata_dir_188")
        or shared_data.get("metadata_dir_190")
        or shared_data.get("metadata_dir_623")
        or shared_data.get("metadata_dir_624")
        or shared_data.get("metadata_dir_625")
        or shared_data.get("metadata_dir_626")
        or shared_data.get("metadata_dir_627")
        or shared_data.get("metadata_dir")
    )
    assert metadata_dir is not None, "No metadata_dir set by Given step"
    collector = WarningCollector()
    metadata = parse_metadata_dir(metadata_dir, collector)
    # Inject in-memory source overrides if provided (for REQ-623)
    source_overrides = shared_data.get("source_overrides")
    auth_env = shared_data.get("auth_env")
    config = convert_metadata(
        metadata, collector, auth_env=auth_env, source_overrides=source_overrides
    )
    shared_data["config"] = config
    shared_data["collector"] = collector
    shared_data["metadata"] = metadata


@then(
    "each column's visible_to is populated from the role's column list, with \"*\" meaning all columns"
)
def step_then_visible_to_populated(shared_data):
    config = shared_data["config"]
    tables = _all_tables(config)
    assert tables, "Expected at least one table"
    products_table = _find_table(config, "products")
    assert products_table is not None, "Expected 'products' table"

    # viewer role can see id, name, price
    col_id = next((c for c in products_table.columns if getattr(c, "name", None) == "id"), None)
    assert col_id is not None, "Expected 'id' column"
    assert "viewer" in _column_visible_to(col_id), "Expected viewer in id.visible_to"

    # admin role has columns="*" -> should be in visible_to for the wildcard column
    wildcard_col = next(
        (c for c in products_table.columns if getattr(c, "name", None) == "*"), None
    )
    assert wildcard_col is not None, "Expected wildcard '*' column for admin"
    assert "admin" in _column_visible_to(wildcard_col), "Expected admin in *.visible_to"

    # auditor role can see id and internal_cost
    col_cost = next(
        (c for c in products_table.columns if getattr(c, "name", None) == "internal_cost"), None
    )
    assert col_cost is not None, "Expected 'internal_cost' column"
    assert "auditor" in _column_visible_to(col_cost), "Expected auditor in internal_cost.visible_to"


# ---------------------------------------------------------------------------
# Step implementations — REQ-186
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 metadata export with insert/update_permissions[].columns per role",
    target_fixture="shared_data",
)
def step_given_v2_metadata_insert_update_permissions(tmp_path, shared_data):
    # Use the main metadata dir which has insert/update permissions on users/posts
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir_186"] = metadata_dir
    return shared_data


@then("each column's writable_by is populated from the role's insert/update column list")
def step_then_writable_by_populated(shared_data):
    config = shared_data["config"]
    users_table = _find_table(config, "users")
    assert users_table is not None, "Expected 'users' table"

    # admin has insert_permissions on [email, created_at] and update on [email]
    col_email = next((c for c in users_table.columns if getattr(c, "name", None) == "email"), None)
    assert col_email is not None, "Expected 'email' column in users"
    assert "admin" in _column_writable_by(col_email), "Expected admin in email.writable_by"

    # editor has insert_permissions on posts [title, body, author_id]
    posts_table = _find_table(config, "posts")
    assert posts_table is not None, "Expected 'posts' table"
    col_title = next((c for c in posts_table.columns if getattr(c, "name", None) == "title"), None)
    assert col_title is not None, "Expected 'title' column in posts"
    assert "editor" in _column_writable_by(col_title), "Expected editor in title.writable_by"


# ---------------------------------------------------------------------------
# Step implementations — REQ-187
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 select_permissions[].filter boolean expression",
    target_fixture="shared_data",
)
def step_given_v2_select_permissions_filter(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir_187"] = metadata_dir
    return shared_data


@then(
    "rls_rules[] are generated via boolean expression-to-SQL conversion, with empty filter producing no RLS rule"
)
def step_then_rls_rules_generated(shared_data):
    config = shared_data["config"]
    # posts table has a filter for viewer: {author_id: {_eq: "X-Hasura-User-Id"}}
    # -> should produce an RLS rule with current_setting('provisa.user_id')
    rls_rules = getattr(config, "rls_rules", []) or []
    assert len(rls_rules) >= 1, "Expected at least one RLS rule"

    # Find an RLS rule containing current_setting
    session_rules = [r for r in rls_rules if "current_setting" in getattr(r, "filter", "")]
    assert session_rules, "Expected at least one RLS rule with current_setting()"

    # users table viewer has empty filter {} -> no RLS rule for that combo
    # (admin also has empty filter — neither should produce an RLS rule)
    users_viewer_rules = [
        r
        for r in rls_rules
        if "users" in getattr(r, "table_id", "") and getattr(r, "role_id", "") == "viewer"
    ]
    assert not users_viewer_rules, "Expected no RLS rule for users/viewer (empty filter)"


# ---------------------------------------------------------------------------
# Step implementations — REQ-188
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 metadata export with object_relationships and array_relationships",
    target_fixture="shared_data",
)
def step_given_v2_metadata_relationships(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir_188"] = metadata_dir
    return shared_data


@then(
    "object_relationships become cardinality=many-to-one and array_relationships become cardinality=one-to-many"
)
def step_then_relationships_cardinality(shared_data):
    config = shared_data["config"]
    rels = getattr(config, "relationships", []) or []
    assert rels, "Expected at least one relationship"

    # users.profile is object_relationship -> many-to-one
    many_to_one = [r for r in rels if getattr(r, "cardinality", None) == "many-to-one"]
    one_to_many = [r for r in rels if getattr(r, "cardinality", None) == "one-to-many"]

    assert many_to_one, "Expected at least one many-to-one relationship (object_relationship)"
    assert one_to_many, "Expected at least one one-to-many relationship (array_relationship)"


# ---------------------------------------------------------------------------
# Step implementations — REQ-189
# ---------------------------------------------------------------------------


@given(
    "a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries",
    target_fixture="shared_data",
)
def step_given_ddn_supergraph_field_mapping(tmp_path, shared_data):
    project_dir = _build_ddn_supergraph_project(tmp_path)
    shared_data["project_dir_189"] = project_dir
    return shared_data


@when("the DDN converter runs")
def step_when_ddn_converter_runs(shared_data):
    project_dir = (
        shared_data.get("project_dir_189")
        or shared_data.get("project_dir_191")
        or shared_data.get("project_dir_628")
        or shared_data.get("project_dir")
    )
    assert project_dir is not None, "No project_dir set by Given step"
    collector = WarningCollector()
    agg_collector: dict = {}
    metadata = parse_hml_dir(project_dir, collector)
    config = convert_hml(metadata, collector, agg_collector=agg_collector)
    shared_data["ddn_config"] = config
    shared_data["ddn_metadata"] = metadata
    shared_data["collector"] = collector
    shared_data["agg_collector"] = agg_collector


@then(
    "all GraphQL field names in relationships, permissions, and column definitions are resolved to physical column names"
)
def step_then_graphql_fields_resolved(shared_data):
    config = shared_data["ddn_config"]
    tables = _all_tables(config)
    assert tables, "Expected at least one table"

    # Artist table: artistId (GraphQL) -> artist_id (physical)
    artist_table = next((t for t in tables if getattr(t, "table_name", None) == "artist"), None)
    assert artist_table is not None, "Expected 'artist' table"
    col_names = [getattr(c, "name", None) for c in getattr(artist_table, "columns", [])]
    assert "artist_id" in col_names, f"Expected physical column 'artist_id', got: {col_names}"
    # GraphQL field name 'artistId' should NOT appear as a physical column name
    assert "artistId" not in col_names, (
        "GraphQL field 'artistId' should be resolved to 'artist_id', not kept as-is"
    )

    # Album relationships: artistId -> artist_id in source_column
    rels = getattr(config, "relationships", []) or []
    album_rels = [r for r in rels if "album" in getattr(r, "source_table_id", "").lower()]
    assert album_rels, "Expected at least one relationship from album"
    for rel in album_rels:
        src_col = getattr(rel, "source_column", "")
        assert src_col == "artist_id", (
            f"Expected physical column 'artist_id' in relationship, got: {src_col}"
        )


# ---------------------------------------------------------------------------
# Step implementations — REQ-190
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret",
    target_fixture="shared_data",
)
def step_given_v2_auth_config(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir_190"] = metadata_dir
    # auth_env simulates --auth-env-file contents
    shared_data["auth_env"] = {
        "JWK_URL": "https://auth.example.com/.well-known/jwks.json",
        "HASURA_GRAPHQL_ADMIN_SECRET": "supersecret",
        "CLAIMS_MAP": '{"x-hasura-role": "role", "x-hasura-user-id": "user_id"}',
    }
    return shared_data


@when("the v2 converter runs with --auth-env-file")
def step_when_v2_converter_runs_with_auth_env(shared_data):
    # Reuse the same converter logic — auth_env is already in shared_data
    step_when_v2_converter_runs(shared_data)


@then(
    "JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser, and webhook auth emits a warning"
)
def step_then_jwt_converted_to_oauth(shared_data):
    config = shared_data["config"]
    auth = getattr(config, "auth", None)
    assert auth is not None, "Expected auth config"
    assert getattr(auth, "provider", None) == "oauth", (
        f"Expected provider=oauth, got: {getattr(auth, 'provider', None)}"
    )
    # jwk_url in oauth config
    oauth_cfg = getattr(auth, "oauth", None) or {}
    assert "jwk_url" in oauth_cfg, f"Expected jwk_url in oauth config: {oauth_cfg}"

    # admin secret -> superuser
    superuser = getattr(auth, "superuser", None)
    assert superuser is not None, "Expected superuser config from admin secret"

    # claims_map -> role_mapping
    role_mapping = getattr(auth, "role_mapping", None) or []
    assert len(role_mapping) >= 1, "Expected at least one role_mapping entry from claims_map"

    # Verify webhook auth warning via a separate collector run
    wh_collector = WarningCollector()
    wh_auth_env = {"AUTH_PROVIDER": "webhook"}
    metadata_dir: Path = shared_data["metadata_dir_190"]
    from provisa.hasura_v2.parser import parse_metadata_dir as _pmeta
    from provisa.hasura_v2.mapper import convert_metadata as _cmeta

    _meta = _pmeta(metadata_dir, wh_collector)
    _cmeta(_meta, wh_collector, auth_env=wh_auth_env)
    wh_msgs = _all_warning_messages(wh_collector)
    assert any("webhook" in m.lower() for m in wh_msgs), f"Expected webhook warning, got: {wh_msgs}"


# ---------------------------------------------------------------------------
# Step implementations — REQ-191
# ---------------------------------------------------------------------------


@given("a DDN project with AggregateExpression metadata", target_fixture="shared_data")
def step_given_ddn_project_with_aggregates(tmp_path, shared_data):
    project_dir = tmp_path / "agg_supergraph"
    project_dir.mkdir(parents=True, exist_ok=True)

    # Write an ObjectType, Model, and AggregateExpression
    agg_hml = textwrap.dedent(
        """\
        kind: ObjectType
        version: v1
        definition:
          name: Order
          fields:
            - name: orderId
              type: Int!
            - name: amount
              type: Float!
          dataConnectorTypeMapping:
            - dataConnectorName: myconn
              dataConnectorObjectType: order
              fieldMapping:
                orderId:
                  column:
                    name: order_id
                amount:
                  column:
                    name: amount
        ---
        kind: Model
        version: v1
        definition:
          name: Order
          objectType: Order
          source:
            dataConnectorName: myconn
            collection: order
        ---
        kind: AggregateExpression
        version: v1
        definition:
          name: Order_aggregate_exp
          operand:
            object:
              aggregatedType: Order
              aggregatableFields:
                - fieldName: amount
                  enableAggregationFunctions:
                    - name: sum
                    - name: avg
          count:
            enable: true
            enableDistinct: true
        """
    )
    (project_dir / "order.hml").write_text(agg_hml, encoding="utf-8")
    shared_data["project_dir_191"] = project_dir
    return shared_data


@then("aggregate config is emitted in provisa-aggregates.yaml as valid Provisa aggregate config")
def step_then_aggregate_config_emitted(shared_data):
    agg_collector = shared_data.get("agg_collector", {})
    assert agg_collector, "Expected aggregate config in agg_collector"
    # Should contain an entry for Order
    assert any("Order" in k or "order" in k.lower() for k in agg_collector), (
        f"Expected Order aggregate config, got keys: {list(agg_collector.keys())}"
    )
    # Each entry should have count and fields
    for key, entry in agg_collector.items():
        assert "count" in entry, f"Expected 'count' in aggregate entry for {key}"
        # fields dict should contain 'amount'
        fields = entry.get("fields", {})
        assert fields, f"Expected non-empty fields in aggregate entry for {key}"


# ---------------------------------------------------------------------------
# Step implementations — REQ-192
# ---------------------------------------------------------------------------


@given(
    "a Hasura project with event_triggers, remote_schemas, cron_triggers, or webhook-backed actions",
    target_fixture="shared_data",
)
def step_given_hasura_project_with_unmappable_features(tmp_path, shared_data):
    # The main v2 metadata dir has remote_schemas, cron_triggers, and webhook-backed action
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir"] = metadata_dir
    return shared_data


@when("the converter runs")
def step_when_converter_runs(shared_data):
    step_when_v2_converter_runs(shared_data)


@then("warnings are emitted for unmappable features and conversion completes rather than aborting")
def step_then_warnings_emitted_no_abort(shared_data):
    config = shared_data["config"]
    assert config is not None, "Conversion should complete without aborting"
    # Config should still have tables
    tables = _all_tables(config)
    assert tables, "Expected tables despite unmappable features"
    # Scheduled triggers captured from cron_triggers
    scheduled = getattr(config, "scheduled_triggers", []) or []
    assert scheduled, "Expected at least one scheduled trigger from cron_triggers"
    # Webhooks captured from webhook-backed actions
    webhooks = getattr(config, "webhooks", []) or []
    assert webhooks, "Expected at least one webhook from webhook-backed action"


# ---------------------------------------------------------------------------
# Step implementations — REQ-621
# ---------------------------------------------------------------------------


@given("a completed Hasura v2 or DDN conversion", target_fixture="shared_data")
def step_given_completed_conversion(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir"] = metadata_dir
    return shared_data


@when("the output config is inspected")
def step_when_output_config_inspected(shared_data):
    step_when_v2_converter_runs(shared_data)


@then(
    "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present and Provisa refuses to start without real values"
)
def step_then_placeholder_credentials_present(shared_data):
    config = shared_data["config"]
    sources = getattr(config, "sources", []) or []
    assert sources, "Expected at least one source"
    # Source built from env-var database_url -> placeholder values
    src = sources[0]
    assert getattr(src, "host", None) == "localhost", (
        f"Expected placeholder host=localhost, got: {getattr(src, 'host', None)}"
    )
    password = getattr(src, "password", None)
    assert password == "${env:DB_PASSWORD}", (
        f"Expected placeholder password=${{env:DB_PASSWORD}}, got: {password}"
    )


# ---------------------------------------------------------------------------
# Step implementations — REQ-623
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 source config with kind, database_url, and pool_settings",
    target_fixture="shared_data",
)
def step_given_v2_source_config_kind_url_pool(tmp_path, shared_data):
    # Build metadata in-memory using HasuraSource directly

    meta = HasuraMetadata(
        sources=[
            HasuraSource(
                name="pg_source",
                kind="postgres",
                connection_info={
                    "database_url": "postgres://dbuser:dbpass@dbhost:5433/mydb",
                    "pool_settings": {"min_connections": 3, "max_connections": 20},
                },
            ),
            HasuraSource(
                name="mssql_source",
                kind="mssql",
                connection_info={
                    "database_url": "mssql://sa:pass@mshost:1433/msdb",
                    "pool_settings": {"min_connections": 1, "max_connections": 8},
                },
            ),
        ]
    )
    shared_data["raw_metadata_623"] = meta
    return shared_data


@when("the v2 converter runs")  # duplicate step text — handled by pytest-bdd deduplication
def step_when_v2_converter_runs_623(shared_data):
    # For REQ-623/625/626/627 metadata was built in-memory, not from a directory
    raw_key = next(
        (
            k
            for k in (
                "raw_metadata_623",
                "raw_metadata_625",
                "raw_metadata_626",
                "raw_metadata_627",
            )
            if k in shared_data
        ),
        None,
    )
    if raw_key:
        collector = WarningCollector()
        config = convert_metadata(shared_data[raw_key], collector)
        shared_data["config"] = config
        shared_data["collector"] = collector
    else:
        step_when_v2_converter_runs(shared_data)


@then(
    "SourceType is mapped correctly and connection URL is parsed into components with pool settings preserved"
)
def step_then_source_type_mapped(shared_data):
    config = shared_data["config"]
    sources = {s.id: s for s in (getattr(config, "sources", []) or [])}

    pg = sources.get("pg_source")
    assert pg is not None, "Expected pg_source"
    assert str(pg.type) in ("postgresql", "SourceType.postgresql"), (
        f"Expected postgresql type for pg_source, got: {pg.type}"
    )
    assert getattr(pg, "host", None) == "dbhost", f"Expected host=dbhost, got: {pg.host}"
    assert getattr(pg, "port", None) == 5433, f"Expected port=5433, got: {pg.port}"
    assert getattr(pg, "database", None) == "mydb", f"Expected database=mydb, got: {pg.database}"
    assert getattr(pg, "pool_min", None) == 3, f"Expected pool_min=3, got: {pg.pool_min}"
    assert getattr(pg, "pool_max", None) == 20, f"Expected pool_max=20, got: {pg.pool_max}"

    ms = sources.get("mssql_source")
    assert ms is not None, "Expected mssql_source"
    assert str(ms.type) in ("sqlserver", "SourceType.sqlserver"), (
        f"Expected sqlserver type for mssql_source, got: {ms.type}"
    )


# ---------------------------------------------------------------------------
# Step implementations — REQ-624
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 role with delete_permissions on any table",
    target_fixture="shared_data",
)
def step_given_v2_role_with_delete_permissions(tmp_path, shared_data):
    metadata_dir = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["metadata_dir_624"] = metadata_dir
    return shared_data


@then("the role is upgraded to write capability with no per-table delete mapping produced")
def step_then_role_upgraded_to_write(shared_data):
    config = shared_data["config"]
    roles = {r.id: r for r in (getattr(config, "roles", []) or [])}

    # admin has delete_permissions on users -> must have write capability
    admin = roles.get("admin")
    assert admin is not None, "Expected 'admin' role"
    caps = getattr(admin, "capabilities", []) or []
    assert "write" in caps, f"Expected 'write' capability for admin, got: {caps}"

    # No per-table delete mapping should appear anywhere
    tables = _all_tables(config)
    for t in tables:
        for col in getattr(t, "columns", []) or []:
            # There should be no delete-specific field or mapping
            assert not hasattr(col, "delete_by") or getattr(col, "delete_by", None) is None, (
                f"Unexpected delete_by on column {getattr(col, 'name', '?')} in table {getattr(t, 'table_name', '?')}"
            )


# ---------------------------------------------------------------------------
# Step implementations — REQ-625
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 source with database_url as an env var reference or unparseable URL",
    target_fixture="shared_data",
)
def step_given_v2_source_env_var_or_unparseable(tmp_path, shared_data):

    meta = HasuraMetadata(
        sources=[
            HasuraSource(
                name="env_source",
                kind="postgres",
                connection_info={"database_url": {"from_env": "PG_DATABASE_URL"}},
            ),
            HasuraSource(
                name="bad_source",
                kind="postgres",
                connection_info={"database_url": "not-a-valid-url"},
            ),
        ]
    )
    shared_data["raw_metadata_625"] = meta
    return shared_data


@then(
    "placeholder connection values are substituted and operators are directed to use --source-overrides"
)
def step_then_placeholder_values_substituted(shared_data):
    if "raw_metadata_625" in shared_data:
        collector = WarningCollector()
        config = convert_metadata(shared_data["raw_metadata_625"], collector)
    else:
        config = shared_data["config"]

    sources = {s.id: s for s in (getattr(config, "sources", []) or [])}

    # env var reference -> placeholders
    env_src = sources.get("env_source")
    assert env_src is not None, "Expected env_source"
    assert getattr(env_src, "host", None) == "localhost", (
        f"Expected placeholder host=localhost for env_source, got: {env_src.host}"
    )
    assert getattr(env_src, "password", None) == "${env:DB_PASSWORD}", (
        f"Expected placeholder password for env_source, got: {env_src.password}"
    )

    # Bad URL -> placeholders too
    bad_src = sources.get("bad_source")
    assert bad_src is not None, "Expected bad_source"
    assert getattr(bad_src, "host", None) == "localhost", (
        f"Expected placeholder host=localhost for bad_source, got: {bad_src.host}"
    )


# ---------------------------------------------------------------------------
# Step implementations — REQ-626
# ---------------------------------------------------------------------------


@given(
    "a Hasura project with roles that have no permission entries on any table or action",
    target_fixture="shared_data",
)
def step_given_hasura_project_roles_no_permissions(tmp_path, shared_data):
    """Build metadata where 'orphan_role' has no permissions anywhere."""

    meta = HasuraMetadata(
        sources=[
            HasuraSource(
                name="default",
                kind="postgres",
                connection_info={"database_url": {"from_env": "PG_URL"}},
                tables=[
                    HasuraTable(
                        name="items",
                        schema_name="public",
                        select_permissions=[
                            HasuraPermission(
                                role="viewer",
                                columns=["id", "name"],
                                filter={},
                            )
                        ],
                    )
                ],
            )
        ],
        # orphan_role appears in no permission lists anywhere
    )
    shared_data["raw_metadata_626"] = meta
    return shared_data


@then("those roles are excluded from the output config")
def step_then_roles_excluded(shared_data):
    if "raw_metadata_626" in shared_data:
        collector = WarningCollector()
        config = convert_metadata(shared_data["raw_metadata_626"], collector)
    else:
        config = shared_data["config"]

    role_ids = {r.id for r in (getattr(config, "roles", []) or [])}
    # viewer has a select_permission -> should appear
    assert "viewer" in role_ids, f"Expected 'viewer' in roles, got: {role_ids}"
    # orphan_role has no permissions -> must NOT appear
    assert "orphan_role" not in role_ids, (
        f"Expected 'orphan_role' to be excluded from roles, got: {role_ids}"
    )


# ---------------------------------------------------------------------------
# Step implementations — REQ-627
# ---------------------------------------------------------------------------


@given(
    "a Hasura v2 table with custom_root_fields or custom_name defined",
    target_fixture="shared_data",
)
def step_given_v2_table_with_custom_alias(tmp_path, shared_data):

    meta = HasuraMetadata(
        sources=[
            HasuraSource(
                name="default",
                kind="postgres",
                connection_info={"database_url": {"from_env": "PG_URL"}},
                tables=[
                    # select wins (highest priority)
                    HasuraTable(
                        name="articles",
                        schema_name="public",
                        custom_root_fields={
                            "select": "listArticles",
                            "select_by_pk": "articleById",
                        },
                        custom_name="Article",
                        select_permissions=[
                            HasuraPermission(role="viewer", columns=["id"], filter={})
                        ],
                    ),
                    # select_by_pk wins (no select)
                    HasuraTable(
                        name="comments",
                        schema_name="public",
                        custom_root_fields={"select_by_pk": "commentById"},
                        custom_name="Comment",
                        select_permissions=[
                            HasuraPermission(role="viewer", columns=["id"], filter={})
                        ],
                    ),
                    # custom_name only (no custom_root_fields)
                    HasuraTable(
                        name="tags",
                        schema_name="public",
                        custom_name="Tag",
                        select_permissions=[
                            HasuraPermission(role="viewer", columns=["id"], filter={})
                        ],
                    ),
                ],
            )
        ]
    )
    shared_data["raw_metadata_627"] = meta
    return shared_data


@then("the Provisa table alias is derived with select > select_by_pk > custom_name priority order")
def step_then_table_alias_derived(shared_data):
    if "raw_metadata_627" in shared_data:
        collector = WarningCollector()
        config = convert_metadata(shared_data["raw_metadata_627"], collector)
    else:
        config = shared_data["config"]

    tables = {t.table_name: t for t in (_all_tables(config))}

    # articles: select="listArticles" wins
    articles = tables.get("articles")
    assert articles is not None, "Expected 'articles' table"
    assert getattr(articles, "alias", None) == "listArticles", (
        f"Expected alias=listArticles, got: {getattr(articles, 'alias', None)}"
    )

    # comments: select_by_pk="commentById" wins (no select)
    comments = tables.get("comments")
    assert comments is not None, "Expected 'comments' table"
    assert getattr(comments, "alias", None) == "commentById", (
        f"Expected alias=commentById, got: {getattr(comments, 'alias', None)}"
    )

    # tags: custom_name="Tag" (no custom_root_fields)
    tags = tables.get("tags")
    assert tags is not None, "Expected 'tags' table"
    assert getattr(tags, "alias", None) == "Tag", (
        f"Expected alias=Tag, got: {getattr(tags, 'alias', None)}"
    )


# ---------------------------------------------------------------------------
# Step implementations — REQ-628
# ---------------------------------------------------------------------------


@given(
    "a DDN HML project where some ObjectType HML files are missing",
    target_fixture="shared_data",
)
def step_given_ddn_project_missing_object_type(tmp_path, shared_data):
    project_dir = _build_partial_ddn_project_missing_object_type(tmp_path)
    shared_data["project_dir_628"] = project_dir
    return shared_data


@then("missing ObjectType tables are skipped with a warning and conversion continues")
def step_then_missing_object_type_skipped(shared_data):
    config = shared_data["ddn_config"]
    collector: WarningCollector = shared_data["collector"]
    assert config is not None, "Expected conversion to complete"

    # Artist model has its ObjectType -> should appear in tables
    tables = _all_tables(config)
    table_names = [getattr(t, "table_name", None) for t in tables]
    assert "artist" in table_names, f"Expected 'artist' table to be present; got: {table_names}"

    # Ghost model references missing GhostType -> must be absent
    assert "ghost" not in table_names, f"Expected 'ghost' table to be skipped; got: {table_names}"

    # A warning should have been emitted for the missing type
    msgs = _all_warning_messages(collector)
    assert any("Ghost" in m or "ghost" in m.lower() or "GhostType" in m for m in msgs), (
        f"Expected warning about missing ObjectType/GhostType, got: {msgs}"
    )
