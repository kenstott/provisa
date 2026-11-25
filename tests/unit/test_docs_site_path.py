# Copyright (c) 2026 Kenneth Stott
# Canary: 1386ab3c-b5b8-435a-b70a-a5515b89efb7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without ethical written
# permission from the copyright holder.

"""REQ-2114: docs_site_path resolves to the directory actually served at /docs-site/ in each mode.

Three modes:
  Wheel:    provisa/_ui/docs-site
  Dev tree: provisa-ui/public/docs-site
  Image:    static/docs-site

Both provisa/api/mcp/docs_tools.py and provisa/ui_server.py import DOCS_SITE from here,
so Polly reads and the server serves the same directory in every run mode.
"""

from __future__ import annotations

from pathlib import Path

from provisa.docs_site_path import resolve_docs_site


class TestResolveDocsSite:
    def test_wheel_mode_uses_packaged_ui(self, tmp_path: Path) -> None:
        """When provisa/_ui exists (wheel install), the site is provisa/_ui/docs-site."""
        packaged_ui = tmp_path / "_ui"
        packaged_ui.mkdir()
        result = resolve_docs_site(
            packaged_ui=packaged_ui,
            vite_dir=tmp_path / "no-provisa-ui",
            repo_static=tmp_path / "no-static",
        )
        assert result == packaged_ui / "docs-site"

    def test_dev_tree_mode_uses_vite_public(self, tmp_path: Path) -> None:
        """In the dev tree (no wheel, provisa-ui/ present), Vite serves public/docs-site."""
        vite_dir = tmp_path / "provisa-ui"
        vite_dir.mkdir()
        result = resolve_docs_site(
            packaged_ui=tmp_path / "no-ui",  # not a dir → not wheel
            vite_dir=vite_dir,
            repo_static=tmp_path / "no-static",
        )
        assert result == vite_dir / "public" / "docs-site"

    def test_image_mode_uses_repo_static(self, tmp_path: Path) -> None:
        """In the image (no wheel, no provisa-ui/), the site is static/docs-site."""
        repo_static = tmp_path / "static"
        result = resolve_docs_site(
            packaged_ui=tmp_path / "no-ui",
            vite_dir=tmp_path / "no-provisa-ui",
            repo_static=repo_static,
        )
        assert result == repo_static / "docs-site"

    def test_wheel_takes_priority_over_vite_dir(self, tmp_path: Path) -> None:
        """When both _ui and provisa-ui/ exist (unusual), the wheel's _ui wins."""
        packaged_ui = tmp_path / "_ui"
        packaged_ui.mkdir()
        vite_dir = tmp_path / "provisa-ui"
        vite_dir.mkdir()
        result = resolve_docs_site(
            packaged_ui=packaged_ui,
            vite_dir=vite_dir,
            repo_static=tmp_path / "static",
        )
        assert result == packaged_ui / "docs-site"

    def test_missing_site_is_not_refused_here(self, tmp_path: Path) -> None:
        """resolve_docs_site returns the path regardless of whether it exists.
        The caller (DocsUnavailable) handles the missing case."""
        result = resolve_docs_site(
            packaged_ui=tmp_path / "no-ui",
            vite_dir=tmp_path / "no-provisa-ui",
            repo_static=tmp_path / "static",
        )
        assert not result.exists()


class TestBothModulesUseSharedResolution:
    def test_docs_tools_imports_docs_site_from_shared_module(self) -> None:
        """docs_tools.DOCS_SITE is the same object as docs_site_path.DOCS_SITE."""
        from provisa.api.mcp import docs_tools
        from provisa import docs_site_path

        assert docs_tools.DOCS_SITE is docs_site_path.DOCS_SITE

    def test_ui_server_imports_docs_site_from_shared_module(self) -> None:
        """ui_server.DOCS_SITE is the same object as docs_site_path.DOCS_SITE."""
        import provisa.ui_server as ui_server
        from provisa import docs_site_path

        assert ui_server.DOCS_SITE is docs_site_path.DOCS_SITE

    def test_ui_server_serves_docs_site_from_docs_site_not_static_dir(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """ui_server serves /docs-site/ from DOCS_SITE, not STATIC_DIR.

        In the dev tree STATIC_DIR is static/ (stale) while DOCS_SITE is
        provisa-ui/public/docs-site (the Vite-served copy). The request must hit DOCS_SITE.
        """
        from fastapi.testclient import TestClient

        import provisa.ui_server as ui_server

        # Set up two separate trees: STATIC_DIR has an OLD page, DOCS_SITE has the CURRENT page.
        static_dir = tmp_path / "static"
        static_dir.mkdir()
        (static_dir / "index.html").write_text("<!doctype html><title>shell</title>")
        # Stale docs-site in static/ — simulates the Sep 6 build in the dev tree.
        stale_docs = static_dir / "docs-site" / "search"
        stale_docs.mkdir(parents=True)
        (stale_docs / "search_index.json").write_text('{"stale": true}')

        docs_site = tmp_path / "public" / "docs-site"
        docs_site.mkdir(parents=True)
        (docs_site / "index.html").write_text("<html>current docs</html>")

        monkeypatch.setattr(ui_server, "STATIC_DIR", static_dir)
        monkeypatch.setattr(ui_server, "DOCS_SITE", docs_site)

        with TestClient(ui_server.app) as client:
            resp = client.get("/docs-site/", headers={"sec-fetch-dest": "empty"})

        assert resp.status_code == 200
        assert "current docs" in resp.text
        assert "stale" not in resp.text
