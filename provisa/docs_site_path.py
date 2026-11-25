# Copyright (c) 2026 Kenneth Stott
# Canary: f3e2b1a0-9c8d-4e7f-b6a5-2d1c0e9f8b7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolve the docs-site directory for each run mode (REQ-2114).

Three modes, each named by the directory actually served at /docs-site/:

  Wheel:    provisa/_ui/docs-site  — staged from provisa-ui/dist by build-wheel.sh
  Dev tree: provisa-ui/public/docs-site  — served by Vite from public/, built by
            provisa-ui/scripts/build-docs-site.mjs (the prebuild hook)
  Image:    static/docs-site  — copied from provisa-ui/dist by the Dockerfile

No fallbacks: each mode maps to exactly one directory. A missing site is refused by
the caller (DocsUnavailable or similar), never patched with another mode's copy.

Imported by both provisa/api/mcp/docs_tools.py (Polly reads it) and provisa/ui_server.py
(serves it at /docs-site/).
"""

from __future__ import annotations

from pathlib import Path

_PKG_DIR: Path = Path(__file__).resolve().parent
_REPO_ROOT: Path = _PKG_DIR.parent

# Named path roots for each mode; exposed for testing.
_PACKAGED_UI: Path = _PKG_DIR / "_ui"  # wheel: staged by build-wheel.sh
_REPO_VITE_DIR: Path = _REPO_ROOT / "provisa-ui"  # dev tree: source checkout with Vite
_REPO_STATIC: Path = _REPO_ROOT / "static"  # image: Dockerfile's COPY static/


def resolve_docs_site(
    *,
    packaged_ui: Path = _PACKAGED_UI,
    vite_dir: Path = _REPO_VITE_DIR,
    repo_static: Path = _REPO_STATIC,
) -> Path:
    """The docs-site directory for this run mode.

    Wheel:    ``packaged_ui / "docs-site"``
              provisa/_ui was staged from provisa-ui/dist by build-wheel.sh.
    Dev tree: ``vite_dir / "public" / "docs-site"``
              Vite dev server serves provisa-ui/public/ at the root; the prebuild hook
              (provisa-ui/scripts/build-docs-site.mjs) builds docs into public/docs-site.
              Detected by: provisa-ui/ exists (the full source checkout), and provisa/_ui
              does not (not a wheel).
    Image:    ``repo_static / "docs-site"``
              The Dockerfile copies static/ from the build host; provisa-ui/ is not present
              in the runtime image.

    Returns the Path regardless of whether it exists. The caller is responsible for
    raising DocsUnavailable when the site is absent.
    """
    if packaged_ui.is_dir():
        # Wheel mode: provisa/_ui was staged; Vite-dist and static/ are absent or stale.
        return packaged_ui / "docs-site"
    if vite_dir.is_dir():
        # Dev tree: Vite serves public/docs-site at /docs-site/; static/docs-site is a
        # stale gitignored copy that must not be read instead.
        return vite_dir / "public" / "docs-site"
    # Image: static/docs-site, copied from provisa-ui/dist by the Dockerfile.
    # provisa-ui/ is absent in the runtime image so the branch above never fires there.
    return repo_static / "docs-site"


#: The docs-site directory for this run, resolved at import time.
DOCS_SITE: Path = resolve_docs_site()
