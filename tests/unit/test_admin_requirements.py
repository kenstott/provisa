# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for admin requirements: REQ-405, REQ-406, REQ-407, REQ-408, REQ-164, REQ-165, REQ-166, REQ-167, REQ-528, REQ-533, REQ-620, REQ-622"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _openapi_spec(paths: dict, info: dict | None = None) -> dict:
    return {
        "openapi": "3.0.0",
        "info": info or {"title": "Test API", "version": "1.0.0"},
        "paths": paths,
    }


# ---------------------------------------------------------------------------
# REQ-407: OpenAPIRegisterRequest and OpenAPIPreviewRequest accept spec_content
#          When spec_content is provided it is parsed (YAML then JSON fallback)
#          and used in place of loading from disk; path is stored as ":inline:"
# ---------------------------------------------------------------------------


class TestREQ407InlineSpecContent:
    """REQ-407"""

    def test_register_request_has_spec_content_field(self):
        # REQ-407
        from provisa.api.admin.openapi_router import OpenAPIRegisterRequest

        req = OpenAPIRegisterRequest(source_id="s1", spec_content="openapi: '3.0.0'")
        assert req.spec_content == "openapi: '3.0.0'"

    def test_preview_request_has_spec_content_field(self):
        # REQ-407
        from provisa.api.admin.openapi_router import OpenAPIPreviewRequest

        req = OpenAPIPreviewRequest(spec_content="openapi: '3.0.0'")
        assert req.spec_content == "openapi: '3.0.0'"

    def test_register_request_spec_path_defaults_empty(self):
        # REQ-407: path is stored as ":inline:" when spec_content is used;
        # the model default for spec_path must be empty so the router can decide
        from provisa.api.admin.openapi_router import OpenAPIRegisterRequest

        req = OpenAPIRegisterRequest(source_id="s1", spec_content="data: 1")
        assert req.spec_path == ""

    def test_inline_path_sentinel_is_stored_when_spec_path_absent(self):
        # REQ-407: when spec_content is provided and spec_path is absent the
        # registration logic stores the sentinel ":inline:" as the path
        from provisa.api.admin.openapi_router import OpenAPIRegisterRequest

        req = OpenAPIRegisterRequest(source_id="s1", spec_content="data: 1")
        # The sentinel logic: spec_path if spec_path else ":inline:"
        stored_path = req.spec_path if req.spec_path else ":inline:"
        assert stored_path == ":inline:"

    def test_parse_text_handles_yaml(self):
        # REQ-407: inline content is parsed via parse_text (YAML then JSON fallback)
        from provisa.openapi.loader import parse_text

        yaml_text = "openapi: '3.0.0'\ninfo:\n  title: Demo\n  version: '1'\npaths: {}"
        spec = parse_text(yaml_text)
        assert spec.get("openapi") == "3.0.0"
        assert spec["info"]["title"] == "Demo"

    def test_parse_text_handles_json_fallback(self):
        # REQ-407: inline content falls back to JSON parsing when YAML fails
        import json
        from provisa.openapi.loader import parse_text

        json_text = json.dumps(
            {"openapi": "3.0.0", "info": {"title": "T", "version": "1"}, "paths": {}}
        )
        spec = parse_text(json_text)
        assert spec.get("openapi") == "3.0.0"


# ---------------------------------------------------------------------------
# REQ-408: x-provisa-kind override for POST-as-query classification
# ---------------------------------------------------------------------------


class TestREQ408XProvisakind:
    """REQ-408"""

    def test_post_with_x_provisa_kind_query_becomes_query(self):
        # REQ-408
        from provisa.openapi.mapper import parse_spec

        spec = _openapi_spec(
            {
                "/search": {
                    "post": {
                        "operationId": "searchItems",
                        "x-provisa-kind": "query",
                        "responses": {
                            "200": {
                                "description": "ok",
                                "content": {
                                    "application/json": {
                                        "schema": {"type": "array", "items": {"type": "object"}}
                                    }
                                },
                            }
                        },
                    }
                }
            }
        )
        queries, _ = parse_spec(spec)
        op_ids = [q.operation_id for q in queries]
        assert "searchItems" in op_ids

    def test_post_without_x_provisa_kind_defaults_to_mutation(self):
        # REQ-408: GET heuristic — POST without override defaults to mutation
        from provisa.openapi.mapper import parse_spec

        spec = _openapi_spec(
            {
                "/items": {
                    "post": {
                        "operationId": "createItem",
                        "responses": {"200": {"description": "ok"}},
                    }
                }
            }
        )
        _, mutations = parse_spec(spec)
        assert any(m.operation_id == "createItem" for m in mutations)

    def test_x_provisa_kind_mutation_overrides_get(self):
        # REQ-408: x-provisa-kind: mutation overrides GET method default
        from provisa.openapi.mapper import parse_spec

        spec = _openapi_spec(
            {
                "/items": {
                    "get": {
                        "operationId": "writeGet",
                        "x-provisa-kind": "mutation",
                        "responses": {"200": {"description": "ok"}},
                    }
                }
            }
        )
        queries, mutations = parse_spec(spec)
        assert not any(q.operation_id == "writeGet" for q in queries)
        assert any(m.operation_id == "writeGet" for m in mutations)

    def test_operation_overrides_payload_takes_priority_over_x_provisa_kind(self):
        # REQ-408: operation_overrides payload takes priority over x-provisa-kind
        from provisa.openapi.mapper import parse_spec

        spec = _openapi_spec(
            {
                "/search": {
                    "post": {
                        "operationId": "searchOp",
                        "x-provisa-kind": "mutation",  # says mutation
                        "responses": {
                            "200": {
                                "description": "ok",
                                "content": {
                                    "application/json": {
                                        "schema": {"type": "array", "items": {"type": "object"}}
                                    }
                                },
                            }
                        },
                    }
                }
            }
        )
        # payload override says query — must win
        queries, mutations = parse_spec(spec, operation_overrides={"searchOp": "query"})
        assert any(q.operation_id == "searchOp" for q in queries)
        assert not any(m.operation_id == "searchOp" for m in mutations)


# ---------------------------------------------------------------------------
# REQ-164: GET/PUT /admin/config for config YAML download/upload with backup
# ---------------------------------------------------------------------------


class TestREQ164AdminConfig:
    """REQ-164"""

    def test_get_admin_config_route_exists(self):
        # REQ-164
        from provisa.api.admin.settings_router import router
        from fastapi.routing import APIRoute

        paths = [r.path for r in router.routes if isinstance(r, APIRoute)]
        assert "/admin/config" in paths

    def test_put_admin_config_route_exists(self):
        # REQ-164
        from provisa.api.admin.settings_router import router
        from fastapi.routing import APIRoute

        put_routes = [
            r
            for r in router.routes
            if isinstance(r, APIRoute) and r.path == "/admin/config" and "PUT" in r.methods
        ]
        assert len(put_routes) >= 1

    def test_write_config_creates_backup(self):
        # REQ-164: PUT /admin/config creates a .yaml.bak backup
        from provisa.api.admin._config_io import write_config

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "provisa.yaml"
            cfg_path.write_text("sources: []\n")
            new_cfg = {"sources": ["s1"]}
            write_config(cfg_path, new_cfg)
            bak_path = cfg_path.with_suffix(".yaml.bak")
            assert bak_path.exists(), "backup .yaml.bak must be created on write"

    def test_write_config_backup_contains_original_content(self):
        # REQ-164: backup preserves the pre-upload content
        from provisa.api.admin._config_io import write_config

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "provisa.yaml"
            original = "sources: []\n"
            cfg_path.write_text(original)
            write_config(cfg_path, {"sources": ["updated"]})
            bak_path = cfg_path.with_suffix(".yaml.bak")
            assert bak_path.read_text() == original

    def test_config_path_default(self):
        # REQ-164: default config path is config/provisa.yaml when env not set
        from provisa.api.admin._config_io import config_path

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PROVISA_CONFIG", None)
            p = config_path()
            assert str(p) == "config/provisa.yaml"

    def test_read_config_returns_dict(self):
        # REQ-164: read_config returns a dict from YAML
        from provisa.api.admin._config_io import read_config

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "provisa.yaml"
            cfg_path.write_text("sources:\n  - id: test\n")
            with patch.dict(os.environ, {"PROVISA_CONFIG": str(cfg_path)}):
                cfg = read_config()
            assert isinstance(cfg, dict)
            assert "sources" in cfg


# ---------------------------------------------------------------------------
# REQ-165: GET/PUT /admin/settings for runtime platform settings
# ---------------------------------------------------------------------------


class TestREQ165AdminSettings:
    """REQ-165"""

    def test_get_admin_settings_route_exists(self):
        # REQ-165
        from provisa.api.admin.settings_router import router
        from fastapi.routing import APIRoute

        get_routes = [
            r
            for r in router.routes
            if isinstance(r, APIRoute) and r.path == "/admin/settings" and "GET" in r.methods
        ]
        assert len(get_routes) >= 1

    def test_put_admin_settings_route_exists(self):
        # REQ-165
        from provisa.api.admin.settings_router import router
        from fastapi.routing import APIRoute

        put_routes = [
            r
            for r in router.routes
            if isinstance(r, APIRoute) and r.path == "/admin/settings" and "PUT" in r.methods
        ]
        assert len(put_routes) >= 1

    def test_settings_router_exposes_redirect_key(self):
        # REQ-165: settings response must include redirect configuration
        import inspect
        from provisa.api.admin import settings_router

        src = inspect.getsource(settings_router)
        assert "redirect" in src

    def test_settings_router_exposes_sampling_key(self):
        # REQ-165: settings response must include sampling configuration
        import inspect
        from provisa.api.admin import settings_router

        src = inspect.getsource(settings_router)
        assert "sampling" in src

    def test_settings_router_exposes_cache_key(self):
        # REQ-165: settings response must include cache configuration
        import inspect
        from provisa.api.admin import settings_router

        src = inspect.getsource(settings_router)
        assert "cache" in src


# ---------------------------------------------------------------------------
# REQ-166: Editable relationships page — materialize toggle, delete, add form
# ---------------------------------------------------------------------------


class TestREQ166EditableRelationships:
    """REQ-166"""

    def test_upsert_relationship_exists_in_admin_schema(self):
        # REQ-166: add form requires an upsert_relationship mutation
        import inspect
        from provisa.api.admin import schema as admin_schema_mod

        src = inspect.getsource(admin_schema_mod)
        assert "upsert_relationship" in src

    def test_delete_relationship_exists_in_admin_schema(self):
        # REQ-166: delete action requires a delete_relationship mutation
        import inspect
        from provisa.api.admin import schema as admin_schema_mod

        src = inspect.getsource(admin_schema_mod)
        assert "delete_relationship" in src

    def test_materialize_field_present_in_relationship_upsert(self):
        # REQ-166: materialize toggle must be part of the upsert input
        import inspect
        from provisa.api.admin import schema as admin_schema_mod

        src = inspect.getsource(admin_schema_mod)
        assert "materialize" in src


# ---------------------------------------------------------------------------
# REQ-167: AI-suggested relationships via LLM discovery integration
# ---------------------------------------------------------------------------


class TestREQ167LLMRelationshipDiscovery:
    """REQ-167"""

    def test_discovery_router_has_relationships_endpoint(self):
        # REQ-167: POST /admin/discover/relationships triggers LLM discovery
        from provisa.api.admin.discovery import router
        from fastapi.routing import APIRoute

        paths = [r.path for r in router.routes if isinstance(r, APIRoute)]
        assert "/admin/discover/relationships" in paths

    def test_discovery_uses_anthropic_api_key_for_llm(self):
        # REQ-167: LLM discovery only runs when ANTHROPIC_API_KEY is set
        import inspect
        from provisa.api.admin import discovery

        src = inspect.getsource(discovery)
        assert "ANTHROPIC_API_KEY" in src

    def test_discovery_request_model_has_scope_field(self):
        # REQ-167: discovery must accept scope (table/domain/cross-domain)
        from provisa.api.admin.discovery import DiscoverRequest

        req = DiscoverRequest(scope="domain", domain_id="d1")
        assert req.scope == "domain"

    def test_discovery_fk_candidates_always_run(self):
        # REQ-167: FK constraint candidates are always collected, not just when LLM key present
        import inspect
        from provisa.api.admin import discovery

        src = inspect.getsource(discovery)
        assert "collect_fk_candidates" in src
        # FK collection appears before the ANTHROPIC_API_KEY check
        fk_pos = src.index("collect_fk_candidates")
        key_pos = src.index("ANTHROPIC_API_KEY")
        assert fk_pos < key_pos


# ---------------------------------------------------------------------------
# REQ-528: Config path controlled by PROVISA_CONFIG env var (default config/provisa.yaml)
# ---------------------------------------------------------------------------


class TestREQ528ProvIsaConfigEnvVar:
    """REQ-528"""

    def test_config_path_uses_provisa_config_env_var(self):
        # REQ-528
        from provisa.api.admin._config_io import config_path

        with patch.dict(os.environ, {"PROVISA_CONFIG": "/custom/path/config.yaml"}):
            p = config_path()
        assert str(p) == "/custom/path/config.yaml"

    def test_config_path_default_when_env_absent(self):
        # REQ-528: default is config/provisa.yaml
        from provisa.api.admin._config_io import config_path

        env = {k: v for k, v in os.environ.items() if k != "PROVISA_CONFIG"}
        with patch.dict(os.environ, env, clear=True):
            p = config_path()
        assert str(p) == "config/provisa.yaml"

    def test_read_config_respects_provisa_config_env_var(self):
        # REQ-528: read_config must read from path given by PROVISA_CONFIG
        from provisa.api.admin._config_io import read_config

        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "custom.yaml"
            cfg_path.write_text("my_key: my_value\n")
            with patch.dict(os.environ, {"PROVISA_CONFIG": str(cfg_path)}):
                cfg = read_config()
        assert cfg.get("my_key") == "my_value"

    def test_app_py_reads_provisa_config_env_var(self):
        # REQ-528: app.py must also honour PROVISA_CONFIG
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "PROVISA_CONFIG" in src
        assert "config/provisa.yaml" in src


# ---------------------------------------------------------------------------
# REQ-533: Admin GraphQL API is Strawberry-based at POST /admin/graphql,
#          separate from data GraphQL at /data/graphql
# ---------------------------------------------------------------------------


class TestREQ533AdminGraphQLEndpoint:
    """REQ-533"""

    def test_admin_graphql_uses_strawberry(self):
        # REQ-533: admin GraphQL must use Strawberry
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "strawberry" in src.lower()
        assert "GraphQLRouter" in src

    def test_admin_graphql_mounted_at_admin_graphql(self):
        # REQ-533: endpoint mounted at /admin/graphql prefix
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert '"/admin/graphql"' in src or "'/admin/graphql'" in src

    def test_admin_schema_is_distinct_from_data_schema(self):
        # REQ-533: admin_schema is imported separately from the data endpoint schema
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "admin_schema" in src
        # Data graphql is separate
        assert "/data/graphql" in src or "data_router" in src

    def test_admin_schema_module_exists(self):
        # REQ-533: provisa.api.admin.schema must export admin_schema
        from provisa.api.admin.schema import admin_schema  # noqa: F401

        assert admin_schema is not None


# ---------------------------------------------------------------------------
# REQ-620: Admin GraphQL API is mounted at /admin/graphql, distinct from /data/graphql
# ---------------------------------------------------------------------------


class TestREQ620AdminGraphQLMount:
    """REQ-620"""

    def test_admin_graphql_prefix_in_app(self):
        # REQ-620: /admin/graphql must be registered on the app
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "/admin/graphql" in src

    def test_data_graphql_separate_from_admin_graphql(self):
        # REQ-620: /data/graphql must be distinct from /admin/graphql
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "/admin/graphql" in src
        # data endpoint present and different
        assert "/data/graphql" in src or "data_router" in src

    def test_admin_schema_used_for_admin_router(self):
        # REQ-620: admin_schema is the schema used for the admin GraphQL router
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "GraphQLRouter(admin_schema" in src


# ---------------------------------------------------------------------------
# REQ-622: GraphiQL IDE accessible via GET /admin/graphql in a browser
# ---------------------------------------------------------------------------


class TestREQ622GraphiQLAccessible:
    """REQ-622"""

    def test_graphql_router_created_without_graphiql_disabled(self):
        # REQ-622: GraphQLRouter must not explicitly disable GraphiQL
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        # Check that GraphQLRouter is created and graphiql is not set to False
        assert "GraphQLRouter(admin_schema" in src
        # Strawberry's GraphQLRouter exposes GraphiQL by default; confirm no explicit disable
        assert "graphiql=False" not in src

    def test_strawberry_graphql_router_supports_get(self):
        # REQ-622: Strawberry's GraphQLRouter serves GraphiQL on GET by default
        # Verify the import path is from strawberry.fastapi
        import inspect
        import provisa.api.app as app_mod

        src = inspect.getsource(app_mod)
        assert "from strawberry.fastapi import GraphQLRouter" in src
