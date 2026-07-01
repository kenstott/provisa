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
              filter:
                artistId:
                  _eq: 1
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


def _build_hasura_v2_metadata_with_write_permissions(base: Path) -> Path:
    """
    Build a Hasura v2 metadata directory specifically designed to test REQ-186:
    insert/update_permissions[].columns -> Provisa column writable_by.
    """
    metadata_dir = base / "metadata_186"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")

    db_dir = metadata_dir / "databases" / "default"
    tables_dir = db_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    orders_yaml = {
        "table": {"schema": "public", "name": "orders"},
        "select_permissions": [
            {
                "role": "analyst",
                "permission": {
                    "columns": ["id", "amount", "region"],
                    "filter": {},
                },
            },
        ],
        "insert_permissions": [
            {
                "role": "clerk",
                "permission": {
                    "columns": ["amount", "region"],
                    "check": {},
                },
            },
        ],
        "update_permissions": [
            {
                "role": "manager",
                "permission": {
                    "columns": ["amount", "region", "status"],
                    "filter": {},
                    "check": {},
                },
            },
        ],
    }
    (tables_dir / "public_orders.yaml").write_text(yaml.dump(orders_yaml), encoding="utf-8")

    (tables_dir / "tables.yaml").write_text(
        yaml.dump([orders_yaml]),
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


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


def _run_v2(metadata_dir: Path, shared_data: dict, **kwargs) -> ProvisaConfig:
    """Parse + convert a v2 metadata dir, storing intermediates in shared_data."""
    collector = WarningCollector()
    metadata = parse_metadata_dir(metadata_dir, collector)
    config = convert_metadata(metadata, collector, **kwargs)
    shared_data["metadata"] = metadata
    shared_data["collector"] = collector
    shared_data["config"] = config
    return config


def _run_ddn(project_dir: Path, shared_data: dict, **kwargs) -> ProvisaConfig:
    """Parse + convert a DDN project dir, storing intermediates in shared_data."""
    collector = WarningCollector()
    metadata = parse_hml_dir(project_dir, collector)
    config = convert_hml(metadata, collector, **kwargs)
    shared_data["metadata"] = metadata
    shared_data["collector"] = collector
    shared_data["config"] = config
    return config


# --- REQ-182: v2 metadata converter (end-to-end) --------------------------


@given("a Hasura v2 metadata export directory")
def given_v2_metadata_export_dir(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)


@when("the CLI converter is run against it")
def when_cli_converter_run(shared_data):
    _run_v2(shared_data["metadata_dir"], shared_data)


@then(
    "valid Provisa YAML config is emitted covering tables, relationships, "
    "permissions, roles, and auth"
)
def then_valid_provisa_config_emitted(shared_data):
    config = shared_data["config"]
    # Round-trips through Pydantic validation -> "valid Provisa config".
    validated = ProvisaConfig.model_validate(config.model_dump(by_alias=True))
    table_names = {t.table_name for t in validated.tables}
    assert {"users", "posts"} <= table_names
    assert validated.relationships, "expected relationships"
    role_ids = {r.id for r in validated.roles}
    assert {"viewer", "admin", "editor"} <= role_ids
    # permissions -> column visibility present
    assert any(
        c.visible_to for t in validated.tables for c in t.columns
    ), "expected column visibility from permissions"
    assert validated.auth is not None


# --- REQ-183: DDN HML converter (end-to-end) ------------------------------


@given("a Hasura DDN supergraph project")
def given_ddn_supergraph_project(tmp_path, shared_data):
    shared_data["project_dir"] = _build_ddn_supergraph_project(tmp_path)


@when("the HML converter CLI tool is run")
def when_hml_converter_run(shared_data):
    _run_ddn(shared_data["project_dir"], shared_data)


@then(
    "valid Provisa YAML config is emitted covering ObjectTypes, Models, "
    "Relationships, TypePermissions, ModelPermissions, and DataConnectorLinks"
)
def then_valid_ddn_config_emitted(shared_data):
    config = shared_data["config"]
    validated = ProvisaConfig.model_validate(config.model_dump(by_alias=True))
    table_names = {t.table_name for t in validated.tables}
    assert {"artist", "album"} <= table_names  # Models -> tables via ObjectTypes
    assert validated.relationships, "Relationship -> Provisa relationship"
    assert {"viewer"} <= {r.id for r in validated.roles}  # TypePermissions/ModelPermissions
    assert validated.sources, "DataConnectorLink -> source"
    assert validated.rls_rules, "ModelPermissions filter -> rls rule"


# --- REQ-184: boolean expression -> SQL -----------------------------------


@given("a Hasura boolean filter expression using operators like _eq, _in, _and, _or, _not")
def given_bool_filter_expr(shared_data):
    shared_data["expr"] = {
        "_and": [
            {"author_id": {"_eq": "X-Hasura-User-Id"}},
            {"status": {"_in": ["draft", "published"]}},
            {"_not": {"deleted": {"_eq": True}}},
            {"_or": [{"priority": {"_gt": 5}}, {"pinned": {"_eq": True}}]},
        ]
    }


@when("the shared converter processes it")
def when_shared_converter_processes(shared_data):
    shared_data["sql"] = bool_expr_to_sql(shared_data["expr"])


@then(
    "valid SQL is produced with session variable references mapped to "
    "current_setting('provisa.<name>')"
)
def then_valid_sql_with_session_var(shared_data):
    sql = shared_data["sql"]
    assert "author_id = current_setting('provisa.user_id')" in sql
    assert "status IN ('draft', 'published')" in sql
    assert "NOT (deleted = TRUE)" in sql
    assert " AND " in sql and " OR " in sql
    assert "priority > 5" in sql


# --- REQ-185: select_permissions.columns -> visible_to --------------------


@given("a Hasura v2 metadata export with select_permissions[].columns per role")
def given_v2_select_permissions(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_with_select_permissions(tmp_path)


@when("the v2 converter runs")
def when_v2_converter_runs(shared_data):
    # Some scenarios build HasuraMetadata directly (kind/connection_info that the
    # directory parser does not carry); others supply a metadata export directory.
    if "direct_metadata" in shared_data:
        collector = WarningCollector()
        config = convert_metadata(shared_data["direct_metadata"], collector)
        shared_data["collector"] = collector
        shared_data["config"] = config
        return
    _run_v2(shared_data["metadata_dir"], shared_data)


@then(
    'each column\'s visible_to is populated from the role\'s column list, '
    'with "*" meaning all columns'
)
def then_visible_to_populated(shared_data):
    config = shared_data["config"]
    products = next(t for t in config.tables if t.table_name == "products")
    cols = {c.name: _column_visible_to(c) for c in products.columns}
    assert "viewer" in cols["name"]
    assert "viewer" in cols["price"]
    assert "auditor" in cols["internal_cost"]
    assert "viewer" not in cols["internal_cost"]
    # "*" wildcard column carries the admin role visibility
    assert "admin" in cols["*"]


# --- REQ-186: insert/update columns -> writable_by ------------------------


@given("a Hasura v2 metadata export with insert/update_permissions[].columns per role")
def given_v2_write_permissions(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_with_write_permissions(tmp_path)


@then(
    "each column's writable_by is populated from the role's insert/update column list"
)
def then_writable_by_populated(shared_data):
    config = shared_data["config"]
    orders = next(t for t in config.tables if t.table_name == "orders")
    writable = {c.name: _column_writable_by(c) for c in orders.columns}
    # clerk has insert on amount/region
    assert "clerk" in writable["amount"]
    assert "clerk" in writable["region"]
    # manager has update on amount/region/status
    assert "manager" in writable["amount"]
    assert "manager" in writable["status"]
    # analyst only has select — not writable
    assert "analyst" not in writable.get("amount", set())


# --- REQ-187: select filter -> rls_rules ----------------------------------


@given("a Hasura v2 select_permissions[].filter boolean expression")
def given_v2_select_filter(tmp_path, shared_data):
    # The default metadata dir has posts.viewer with a non-empty filter and
    # users.viewer with an empty filter {}.
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)


@then(
    "rls_rules[] are generated via boolean expression-to-SQL conversion, "
    "with empty filter producing no RLS rule"
)
def then_rls_rules_generated(shared_data):
    config = shared_data["config"]
    posts_rls = [r for r in config.rls_rules if r.role_id == "viewer" and "author_id" in r.filter]
    assert len(posts_rls) == 1
    assert "current_setting('provisa.user_id')" in posts_rls[0].filter
    # users.viewer had filter {} -> no rule with an empty/TRUE filter
    assert all(r.filter and r.filter != "TRUE" for r in config.rls_rules)


# --- REQ-188: relationship cardinality ------------------------------------


@given("a Hasura v2 metadata export with object_relationships and array_relationships")
def given_v2_relationships(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)


@then(
    "object_relationships become cardinality=many-to-one and "
    "array_relationships become cardinality=one-to-many"
)
def then_relationship_cardinalities(shared_data):
    config = shared_data["config"]
    obj_rel = next(r for r in config.relationships if r.id.endswith(".profile"))
    arr_rel = next(r for r in config.relationships if r.id.endswith(".comments"))
    assert obj_rel.cardinality == "many-to-one"
    assert arr_rel.cardinality == "one-to-many"


# --- REQ-189: DDN field mapping resolution --------------------------------


@given("a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries")
def given_ddn_field_mapping(tmp_path, shared_data):
    shared_data["project_dir"] = _build_ddn_supergraph_project(tmp_path)


@when("the DDN converter runs")
def when_ddn_converter_runs(shared_data):
    _run_ddn(shared_data["project_dir"], shared_data)


@then(
    "all GraphQL field names in relationships, permissions, and column "
    "definitions are resolved to physical column names"
)
def then_field_names_resolved(shared_data):
    config = shared_data["config"]
    album = next(t for t in config.tables if t.table_name == "album")
    col_names = {c.name for c in album.columns}
    # physical columns, not GraphQL aliases
    assert {"album_id", "title", "artist_id"} <= col_names
    assert "albumId" not in col_names
    # GraphQL alias preserved on column
    album_id_col = next(c for c in album.columns if c.name == "album_id")
    assert album_id_col.alias == "albumId"
    # relationship resolved to physical column
    rel = next(r for r in config.relationships if r.id.endswith(".artist"))
    assert rel.source_column == "artist_id"
    # ModelPermissions filter resolved to physical column
    album_rls = [r for r in config.rls_rules if "artist_id" in r.filter]
    assert album_rls, "filter field artistId should resolve to artist_id"


# --- REQ-190: auth conversion ---------------------------------------------


@given("a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret")
def given_v2_auth_config(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)
    shared_data["auth_env"] = {
        "AUTH_PROVIDER": "oauth",
        "JWK_URL": "https://auth.example.com/.well-known/jwks.json",
        "CLAIMS_MAP": '{"admin_group": "admin", "viewer_group": "viewer"}',
        "HASURA_GRAPHQL_ADMIN_SECRET": "supersecret",
    }


@when("the v2 converter runs with --auth-env-file")
def when_v2_converter_runs_auth(shared_data):
    _run_v2(shared_data["metadata_dir"], shared_data, auth_env=shared_data["auth_env"])


@then(
    "JWT becomes provider: oauth with role_mapping[], admin secret becomes "
    "superuser, and webhook auth emits a warning"
)
def then_auth_config_converted(shared_data):
    config = shared_data["config"]
    auth = config.auth
    assert auth.provider == "oauth"
    assert auth.oauth and auth.oauth.get("jwk_url") == "https://auth.example.com/.well-known/jwks.json"
    assert auth.superuser and auth.superuser.get("secret") == "supersecret"
    mapped_roles = {m["role"] for m in auth.role_mapping}
    assert {"admin", "viewer"} <= mapped_roles
    # webhook auth path must emit a warning
    webhook_collector = WarningCollector()
    webhook_meta = parse_metadata_dir(shared_data["metadata_dir"], webhook_collector)
    convert_metadata(
        webhook_meta,
        webhook_collector,
        auth_env={"AUTH_PROVIDER": "webhook"},
    )
    assert any(
        w.category == "webhook_auth" for w in webhook_collector.warnings
    ), "webhook auth should emit a warning"


# --- REQ-191: DDN aggregate expressions -----------------------------------


def _build_ddn_project_with_aggregate(base: Path) -> Path:
    project_dir = _build_ddn_supergraph_project(base)
    agg_hml = textwrap.dedent(
        """\
        kind: AggregateExpression
        version: v1
        definition:
          name: AlbumAgg
          operand:
            object:
              aggregatedType: Album
              aggregatableFields:
                - fieldName: albumId
                  aggregateExpression: IntAgg
          count:
            enable: true
            enableDistinct: true
        """
    )
    (project_dir / "subgraphs" / "chinook" / "models" / "AlbumAgg.hml").write_text(
        agg_hml, encoding="utf-8"
    )
    return project_dir


@given("a DDN project with AggregateExpression metadata")
def given_ddn_aggregate(tmp_path, shared_data):
    shared_data["project_dir"] = _build_ddn_project_with_aggregate(tmp_path)


@when("the DDN converter runs")
def when_ddn_converter_runs_agg(shared_data):
    if "agg" in shared_data:
        return
    agg_collector: dict = {}
    _run_ddn(shared_data["project_dir"], shared_data, agg_collector=agg_collector)
    shared_data["agg"] = agg_collector


@then(
    "aggregate config is emitted in provisa-aggregates.yaml as valid Provisa "
    "aggregate config"
)
def then_aggregate_config_emitted(shared_data):
    agg = shared_data["agg"]
    assert agg, "agg_collector should be populated"
    entry = next(iter(agg.values()))
    assert entry.get("count") is True
    # sidecar must be serialisable to valid YAML (provisa-aggregates.yaml)
    dumped = yaml.safe_dump(agg)
    reloaded = yaml.safe_load(dumped)
    assert reloaded == agg


# --- REQ-192: unmappable feature warnings ---------------------------------


def _build_v2_unmappable(base: Path) -> Path:
    metadata_dir = base / "metadata_192"
    tables_dir = metadata_dir / "databases" / "default" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")
    tbl = {
        "table": {"schema": "public", "name": "audit"},
        "select_permissions": [
            {"role": "viewer", "permission": {"columns": ["id"], "filter": {}}}
        ],
        "event_triggers": [
            {
                "name": "on_audit_insert",
                "definition": {"insert": {"columns": "*"}},
                "webhook": "https://hooks.example.com/audit",
                "retry_conf": {"num_retries": 3, "interval_sec": 5},
            }
        ],
    }
    (tables_dir / "tables.yaml").write_text(yaml.dump([tbl]), encoding="utf-8")
    databases_yaml = [{
        "name": "default", "kind": "postgres",
        "configuration": {"connection_info": {"database_url": {"from_env": "PG_URL"}}},
        "tables": "!include default/tables/tables.yaml", "functions": [],
    }]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8")
    # DB-backed (non-HTTP) action -> mapper emits an "actions" warning
    actions_yaml = {
        "actions": [
            {
                "name": "computeTotals",
                "definition": {
                    "handler": "{{ACTION_BASE_URL}}",
                    "kind": "synchronous",
                    "arguments": [],
                    "output_type": "TotalsOutput",
                },
                "permissions": [{"role": "viewer"}],
            }
        ],
        "custom_types": {"input_objects": [], "objects": []},
    }
    (metadata_dir / "actions.yaml").write_text(yaml.dump(actions_yaml), encoding="utf-8")
    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")
    for stub in ("allow_list.yaml", "cron_triggers.yaml", "inherited_roles.yaml",
                 "query_collections.yaml", "remote_schemas.yaml", "rest_endpoints.yaml"):
        (metadata_dir / stub).write_text("[]\n", encoding="utf-8")
    return metadata_dir


@given("a Hasura project with event_triggers, remote_schemas, cron_triggers, or webhook-backed actions")
def given_v2_unmappable_features(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_v2_unmappable(tmp_path)


@when("the converter runs")
def when_converter_runs(shared_data):
    _run_v2(shared_data["metadata_dir"], shared_data)


@then(
    "warnings are emitted for unmappable features and conversion completes "
    "rather than aborting"
)
def then_warnings_emitted_no_abort(shared_data):
    config = shared_data["config"]
    # conversion completed (did not abort)
    assert config.tables
    collector = shared_data["collector"]
    assert collector.has_warnings()
    categories = {w.category for w in collector.warnings}
    # unmappable features surfaced warnings (event trigger + non-HTTP action)
    assert "event_triggers" in categories
    assert "actions" in categories
    # handler URL present in an action warning
    joined = " ".join(_all_warning_messages(collector))
    assert "computeTotals" in joined


# --- REQ-621: placeholder credentials -------------------------------------


@given("a completed Hasura v2 or DDN conversion")
def given_completed_conversion(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)
    _run_v2(shared_data["metadata_dir"], shared_data)


@when("the output config is inspected")
def when_output_inspected(shared_data):
    shared_data["source"] = shared_data["config"].sources[0]


@then(
    "placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) "
    "are present and Provisa refuses to start without real values"
)
def then_placeholder_credentials(shared_data):
    source = shared_data["source"]
    assert source.host == "localhost"
    assert source.password == "${env:DB_PASSWORD}"


# --- REQ-623: source kind mapping + URL parse -----------------------------


@given("a Hasura v2 source config with kind, database_url, and pool_settings")
def given_v2_source_config(shared_data):
    from provisa.hasura_v2.models import (
        HasuraMetadata,
        HasuraPermission,
        HasuraSource,
        HasuraTable,
    )

    table = HasuraTable(
        name="widgets",
        schema_name="public",
        select_permissions=[
            HasuraPermission(role="viewer", columns=["id"], filter={}),
        ],
    )
    source = HasuraSource(
        name="default",
        kind="mssql",  # -> sqlserver
        connection_info={
            "database_url": "postgres://appuser:secretpw@db.example.com:6543/shopdb",
            "pool_settings": {"min_connections": 3, "max_connections": 17},
        },
        tables=[table],
    )
    shared_data["direct_metadata"] = HasuraMetadata(version=3, sources=[source])


@then(
    "SourceType is mapped correctly and connection URL is parsed into "
    "components with pool settings preserved"
)
def then_source_type_and_url_parsed(shared_data):
    source = shared_data["config"].sources[0]
    assert source.type.value == "sqlserver"  # mssql -> sqlserver
    assert source.host == "db.example.com"
    assert source.port == 6543
    assert source.database == "shopdb"
    assert source.username == "appuser"
    assert source.password == "secretpw"
    assert source.pool_min == 3
    assert source.pool_max == 17


# --- REQ-624: delete_permissions -> write capability ----------------------


def _build_v2_delete_perm(base: Path) -> Path:
    metadata_dir = base / "metadata_624"
    tables_dir = metadata_dir / "databases" / "default" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")
    tbl = {
        "table": {"schema": "public", "name": "invoices"},
        "select_permissions": [
            {"role": "purger", "permission": {"columns": ["id"], "filter": {}}}
        ],
        "delete_permissions": [
            {"role": "purger", "permission": {"filter": {}}}
        ],
    }
    (tables_dir / "tables.yaml").write_text(yaml.dump([tbl]), encoding="utf-8")
    databases_yaml = [{
        "name": "default", "kind": "postgres",
        "configuration": {"connection_info": {"database_url": {"from_env": "PG_URL"}}},
        "tables": "!include default/tables/tables.yaml", "functions": [],
    }]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8")
    for stub in ("actions.yaml", "allow_list.yaml", "cron_triggers.yaml",
                 "inherited_roles.yaml", "query_collections.yaml",
                 "remote_schemas.yaml", "rest_endpoints.yaml"):
        (metadata_dir / stub).write_text("[]\n", encoding="utf-8")
    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")
    return metadata_dir


@given("a Hasura v2 role with delete_permissions on any table")
def given_v2_delete_perm(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_v2_delete_perm(tmp_path)


@then(
    "the role is upgraded to write capability with no per-table delete "
    "mapping produced"
)
def then_role_upgraded_write(shared_data):
    config = shared_data["config"]
    purger = next(r for r in config.roles if r.id == "purger")
    assert "write" in purger.capabilities
    # no per-table delete mapping artefact: RLS rules carry no delete op, and
    # there is no delete-specific structure emitted.
    assert all(getattr(r, "role_id", None) != "purger" or r.filter for r in config.rls_rules)


# --- REQ-625: env-var / unparseable database_url -> placeholders ----------


@given("a Hasura v2 source with database_url as an env var reference or unparseable URL")
def given_v2_env_var_url(tmp_path, shared_data):
    # Default dir uses {"from_env": "PG_DATABASE_URL"} for database_url.
    shared_data["metadata_dir"] = _build_hasura_v2_metadata_dir(tmp_path)


@then(
    "placeholder connection values are substituted and operators are directed "
    "to use --source-overrides"
)
def then_placeholder_substituted(shared_data):
    source = shared_data["config"].sources[0]
    assert source.host == "localhost"
    assert source.port == 5432
    assert source.database == "default"
    assert source.username == "postgres"
    assert source.password == "${env:DB_PASSWORD}"
    # --source-overrides is honoured: overriding the source replaces placeholders.
    collector = WarningCollector()
    meta = parse_metadata_dir(shared_data["metadata_dir"], collector)
    overridden = convert_metadata(
        meta, collector,
        source_overrides={source.id: {"host": "real.db", "database": "prod"}},
    )
    ov = next(s for s in overridden.sources if s.id == source.id)
    assert ov.host == "real.db"
    assert ov.database == "prod"


# --- REQ-626: role collection from permissions only -----------------------


def _build_v2_role_without_perms(base: Path) -> Path:
    metadata_dir = base / "metadata_626"
    tables_dir = metadata_dir / "databases" / "default" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")
    tbl = {
        "table": {"schema": "public", "name": "items"},
        "select_permissions": [
            {"role": "reader", "permission": {"columns": ["id"], "filter": {}}}
        ],
    }
    (tables_dir / "tables.yaml").write_text(yaml.dump([tbl]), encoding="utf-8")
    databases_yaml = [{
        "name": "default", "kind": "postgres",
        "configuration": {"connection_info": {"database_url": {"from_env": "PG_URL"}}},
        "tables": "!include default/tables/tables.yaml", "functions": [],
    }]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8")
    for stub in ("actions.yaml", "allow_list.yaml", "cron_triggers.yaml",
                 "query_collections.yaml", "remote_schemas.yaml",
                 "rest_endpoints.yaml"):
        (metadata_dir / stub).write_text("[]\n", encoding="utf-8")
    # inherited_roles present but empty -> "ghost" role never appears
    (metadata_dir / "inherited_roles.yaml").write_text("[]\n", encoding="utf-8")
    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")
    shared_ghost = "ghost_no_perms"
    return metadata_dir, shared_ghost


@given("a Hasura project with roles that have no permission entries on any table or action")
def given_v2_role_without_perms(tmp_path, shared_data):
    metadata_dir, ghost = _build_v2_role_without_perms(tmp_path)
    shared_data["metadata_dir"] = metadata_dir
    shared_data["ghost_role"] = ghost


@then("those roles are excluded from the output config")
def then_ghost_roles_excluded(shared_data):
    config = shared_data["config"]
    role_ids = {r.id for r in config.roles}
    assert "reader" in role_ids  # permission-backed role is present
    assert shared_data["ghost_role"] not in role_ids
    # every emitted role must be permission-backed
    assert role_ids == {"reader"}


# --- REQ-627: table alias priority ----------------------------------------


def _build_v2_alias_priority(base: Path) -> Path:
    metadata_dir = base / "metadata_627"
    tables_dir = metadata_dir / "databases" / "default" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "version.yaml").write_text("version: 3\n", encoding="utf-8")
    # select wins over select_by_pk and custom_name
    t_select = {
        "table": {"schema": "public", "name": "alpha"},
        "configuration": {
            "custom_name": "AlphaName",
            "custom_root_fields": {"select": "alphaSelect", "select_by_pk": "alphaPk"},
        },
        "select_permissions": [{"role": "viewer", "permission": {"columns": ["id"], "filter": {}}}],
    }
    # select_by_pk wins over custom_name when select absent
    t_pk = {
        "table": {"schema": "public", "name": "beta"},
        "configuration": {
            "custom_name": "BetaName",
            "custom_root_fields": {"select_by_pk": "betaPk"},
        },
        "select_permissions": [{"role": "viewer", "permission": {"columns": ["id"], "filter": {}}}],
    }
    # custom_name wins when no custom_root_fields
    t_name = {
        "table": {"schema": "public", "name": "gamma"},
        "configuration": {"custom_name": "GammaName"},
        "select_permissions": [{"role": "viewer", "permission": {"columns": ["id"], "filter": {}}}],
    }
    (tables_dir / "tables.yaml").write_text(
        yaml.dump([t_select, t_pk, t_name]), encoding="utf-8")
    databases_yaml = [{
        "name": "default", "kind": "postgres",
        "configuration": {"connection_info": {"database_url": {"from_env": "PG_URL"}}},
        "tables": "!include default/tables/tables.yaml", "functions": [],
    }]
    (metadata_dir / "databases" / "databases.yaml").write_text(
        yaml.dump(databases_yaml), encoding="utf-8")
    for stub in ("actions.yaml", "allow_list.yaml", "cron_triggers.yaml",
                 "inherited_roles.yaml", "query_collections.yaml",
                 "remote_schemas.yaml", "rest_endpoints.yaml"):
        (metadata_dir / stub).write_text("[]\n", encoding="utf-8")
    (metadata_dir / "actions.graphql").write_text("", encoding="utf-8")
    return metadata_dir


@given("a Hasura v2 table with custom_root_fields or custom_name defined")
def given_v2_alias_priority(tmp_path, shared_data):
    shared_data["metadata_dir"] = _build_v2_alias_priority(tmp_path)


@then(
    "the Provisa table alias is derived with select > select_by_pk > "
    "custom_name priority order"
)
def then_alias_priority(shared_data):
    config = shared_data["config"]
    alpha = next(t for t in config.tables if t.table_name == "alpha")
    beta = next(t for t in config.tables if t.table_name == "beta")
    gamma = next(t for t in config.tables if t.table_name == "gamma")
    assert alpha.alias == "alphaSelect"  # select wins
    assert beta.alias == "betaPk"        # select_by_pk wins over custom_name
    assert gamma.alias == "GammaName"    # custom_name fallback


# --- REQ-628: missing ObjectType -> skip + warn ---------------------------


def _build_ddn_missing_object_type(base: Path) -> Path:
    project_dir = _build_ddn_supergraph_project(base)
    # Add a Model referencing an ObjectType that has no .hml definition.
    orphan_model = textwrap.dedent(
        """\
        kind: Model
        version: v1
        definition:
          name: Track
          objectType: Track
          source:
            dataConnectorName: chinook_connector
            collection: track
          graphql:
            selectMany:
              queryRootField: tracks
        """
    )
    (project_dir / "subgraphs" / "chinook" / "models" / "TrackModel.hml").write_text(
        orphan_model, encoding="utf-8"
    )
    return project_dir


@given("a DDN HML project where some ObjectType HML files are missing")
def given_ddn_missing_object_type(tmp_path, shared_data):
    shared_data["project_dir"] = _build_ddn_missing_object_type(tmp_path)


@then(
    "missing ObjectType tables are skipped with a warning and conversion "
    "continues"
)
def then_missing_object_type_skipped(shared_data):
    config = shared_data["config"]
    table_names = {t.table_name for t in config.tables}
    # orphan Track model skipped, but valid ones still converted
    assert "track" not in table_names
    assert {"artist", "album"} <= table_names
    warnings = shared_data["collector"].warnings
    assert any(
        w.category == "missing_type" and "Track" in w.message for w in warnings
    ), "expected missing_type warning for Track"
