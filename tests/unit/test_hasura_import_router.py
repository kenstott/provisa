# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7d81f3-2ce6-4d70-9b45-6c0f1d9a3e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The interactive Hasura v2 / DDN importer (REQ-1483).

What is pinned here is the two-step contract the admin UI depends on: preview converts and writes
NOTHING, apply loads exactly the YAML it was handed. Plus the staging rules that let one uploaded
file reach converters that read directories.
"""

from __future__ import annotations

import base64
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from provisa.api.admin import import_router as ir
from provisa.api.errors import ApiError
from provisa.import_shared.upload import DDN, HASURA_V2, UploadError, detect_flavor, staged_upload

# A consolidated v2 export: exactly what `export_metadata` returns, which is what an administrator
# has on hand when they are not zipping a metadata directory.
V2_DOCUMENT = {
    "version": 3,
    "sources": [
        {
            "name": "pg1",
            "kind": "postgres",
            "configuration": {
                "connection_info": {
                    "database_url": "postgres://u:p@db.example.com:5432/shop",
                }
            },
            "tables": [
                {
                    "table": {"schema": "public", "name": "orders"},
                    "select_permissions": [
                        {"role": "customer", "permission": {"columns": ["id", "total"]}}
                    ],
                }
            ],
        }
    ],
    "query_collections": [{"name": "allowed-queries"}],
}


def _request(caps: tuple[str, ...] = ("org_settings",)):
    return SimpleNamespace(
        state=SimpleNamespace(
            identity=SimpleNamespace(user_id="admin@example.com", roles=list(caps))
        )
    )


def _preview_req(doc, filename="metadata.yaml", **kw):
    return ir.ImportPreviewRequest(
        filename=filename,
        content_b64=base64.b64encode(yaml.dump(doc).encode()).decode(),
        **kw,
    )


def _grant(monkeypatch, caps: set[str]) -> None:
    """The gate resolves capabilities through the app state; give it a fixed answer."""
    import provisa.api.admin.capabilities as capmod

    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: caps)


@pytest.mark.asyncio
async def test_preview_converts_a_consolidated_v2_document(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    resp = await ir.preview_import(_preview_req(V2_DOCUMENT), _request())

    assert resp.flavor == HASURA_V2
    assert resp.summary.sources == 1
    assert resp.summary.source_ids == ["pg1"]
    assert resp.summary.tables == 1
    assert "customer" in resp.summary.role_ids
    parsed = yaml.safe_load(resp.config_yaml)
    assert [s["id"] for s in parsed["sources"]] == ["pg1"]


@pytest.mark.asyncio
async def test_preview_reports_export_sections_it_cannot_convert(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    resp = await ir.preview_import(_preview_req(V2_DOCUMENT), _request())
    assert "query_collections" in {w.category for w in resp.warnings}


@pytest.mark.asyncio
async def test_preview_accepts_the_api_envelope(monkeypatch):
    """`export_metadata` wraps the metadata in {resource_version, metadata}; both forms convert."""
    _grant(monkeypatch, {"org_settings"})
    resp = await ir.preview_import(
        _preview_req({"resource_version": 7, "metadata": V2_DOCUMENT}), _request()
    )
    assert resp.summary.source_ids == ["pg1"]


@pytest.mark.asyncio
async def test_preview_rejects_an_upload_it_cannot_read(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    req = ir.ImportPreviewRequest(
        filename="metadata.yaml", content_b64=base64.b64encode(b"[1, 2, 3]").decode()
    )
    with pytest.raises(ApiError) as exc:
        await ir.preview_import(req, _request())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_preview_requires_org_settings(monkeypatch):
    _grant(monkeypatch, {"observability"})
    with pytest.raises(ApiError) as exc:
        await ir.preview_import(_preview_req(V2_DOCUMENT), _request(("observability",)))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_preview_never_writes(monkeypatch):
    """The whole point of the split: nothing reaches the org until apply is called."""
    _grant(monkeypatch, {"org_settings"})
    called: list[str] = []
    monkeypatch.setattr(
        "provisa.core.config_loader.load_config",
        lambda *a, **k: called.append("load_config"),
    )
    await ir.preview_import(_preview_req(V2_DOCUMENT), _request())
    assert called == []


class _FakeConn:
    pass


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _cm():
            yield self.conn

        return _cm()


def _wire_apply(monkeypatch) -> dict:
    """Stand in for the org runtime, recording the settled apply sequence."""
    seen: dict = {"order": []}

    async def _load_config(config, conn, engine, replace, catalog_names):  # noqa: ARG001
        seen["order"].append("load_config")
        seen["config"] = config
        seen["replace"] = replace
        seen["catalog_names"] = catalog_names

    async def _pools(config):  # noqa: ARG001
        seen["order"].append("pools")

    async def _pk():
        seen["order"].append("pk")

    async def _rebuild():
        seen["order"].append("rebuild")

    import provisa.api.app as app_mod
    import provisa.api.app_loaders as loaders
    import provisa.api.startup_seed as seed
    import provisa.core.config_loader as cl

    monkeypatch.setattr(cl, "load_config", _load_config)
    monkeypatch.setattr(loaders, "_build_source_pools_and_enums", _pools)
    monkeypatch.setattr(
        loaders, "_populate_source_catalog_names", lambda c: seen["order"].append("catalogs")
    )
    monkeypatch.setattr(seed, "_resolve_pk_from_sources", _pk)
    monkeypatch.setattr(app_mod, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(app_mod.state, "tenant_db", _FakePool(), raising=False)
    monkeypatch.setattr(app_mod.state, "federation_engine", None, raising=False)
    monkeypatch.setattr(app_mod.state, "source_catalogs", {"pg1": "org_7_pg1"}, raising=False)
    return seen


CONFIG_YAML = yaml.dump(
    {
        "sources": [{"id": "pg1", "type": "postgresql", "host": "db", "database": "shop"}],
        "domains": [{"id": "default"}],
        "tables": [],
        "roles": [{"id": "customer", "capabilities": [], "domain_access": ["default"]}],
    }
)


@pytest.mark.asyncio
async def test_apply_runs_the_settled_sequence(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    seen = _wire_apply(monkeypatch)

    resp = await ir.apply_import(
        ir.ImportApplyRequest(config_yaml=CONFIG_YAML, replace=False), _request()
    )

    # Catalog names FIRST — sources must register under the org's own engine catalogs (REQ-1266).
    assert seen["order"] == ["catalogs", "load_config", "pools", "pk", "rebuild"]
    assert seen["catalog_names"] == {"pg1": "org_7_pg1"}
    assert seen["replace"] is False
    assert resp.summary.source_ids == ["pg1"]


@pytest.mark.asyncio
async def test_apply_passes_replace_through(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    seen = _wire_apply(monkeypatch)
    resp = await ir.apply_import(
        ir.ImportApplyRequest(config_yaml=CONFIG_YAML, replace=True), _request()
    )
    assert seen["replace"] is True
    assert resp.replace is True


@pytest.mark.asyncio
async def test_apply_rejects_an_invalid_config(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    _wire_apply(monkeypatch)
    with pytest.raises(ApiError) as exc:
        await ir.apply_import(ir.ImportApplyRequest(config_yaml="sources: [{}]"), _request())
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_apply_requires_org_settings(monkeypatch):
    _grant(monkeypatch, {"observability"})
    with pytest.raises(ApiError) as exc:
        await ir.apply_import(ir.ImportApplyRequest(config_yaml=CONFIG_YAML), _request())
    assert exc.value.status_code == 403


def _zip(entries: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in entries.items():
            z.writestr(name, body)
    return buf.getvalue()


def test_staged_zip_descends_the_wrapper_directory():
    """The DDN parser names subgraphs from the first path component under the root."""
    data = _zip({"my-project/app/models/Orders.hml": "kind: Model\ndefinition:\n  name: Orders\n"})
    with staged_upload("project.zip", data) as staged:
        assert staged.flavor == DDN
        assert staged.root is not None
        assert (staged.root / "app" / "models" / "Orders.hml").exists()


def test_staged_zip_detects_hasura_v2_layout():
    data = _zip({"metadata/tables.yaml": "- table:\n    schema: public\n    name: orders\n"})
    with staged_upload("metadata.zip", data) as staged:
        assert staged.flavor == HASURA_V2
        assert staged.document is None


def test_staged_upload_refuses_an_entry_that_escapes():
    data = _zip({"../evil.yaml": "x: 1"})
    with pytest.raises(UploadError, match="escapes"):
        with staged_upload("evil.zip", data):
            pass


def test_staged_upload_refuses_a_non_archive():
    with pytest.raises(UploadError, match="not a readable zip"):
        with staged_upload("metadata.zip", b"not a zip"):
            pass


def test_staged_upload_refuses_empty():
    with pytest.raises(UploadError, match="empty"):
        with staged_upload("metadata.yaml", b""):
            pass


def test_single_hml_stages_as_a_ddn_project():
    with staged_upload("Orders.hml", b"kind: Model\ndefinition:\n  name: Orders\n") as staged:
        assert staged.flavor == DDN
        assert staged.root is not None
        assert list(staged.root.glob("*.hml"))


def test_detect_flavor_rejects_an_unrecognized_directory(tmp_path: Path):
    (tmp_path / "readme.txt").write_text("nothing to convert")
    with pytest.raises(UploadError):
        detect_flavor(tmp_path)


@pytest.mark.asyncio
async def test_preview_converts_a_ddn_project(monkeypatch):
    _grant(monkeypatch, {"org_settings"})
    hml = (
        "kind: DataConnectorLink\n"
        "version: v1\n"
        "definition:\n"
        "  name: pgconn\n"
        "  url:\n"
        "    singleUrl:\n"
        "      value: http://connector:8080\n"
    )
    data = _zip({"proj/app/connector.hml": hml})
    req = ir.ImportPreviewRequest(filename="proj.zip", content_b64=base64.b64encode(data).decode())
    resp = await ir.preview_import(req, _request())
    assert resp.flavor == DDN
    assert "pgconn" in resp.summary.source_ids
