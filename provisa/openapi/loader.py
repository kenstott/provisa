# Copyright (c) 2026 Kenneth Stott
# Canary: 7239de29-8635-4ae8-b706-ab6afe19acc7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Load an OpenAPI spec from a local file or remote URL."""

from __future__ import annotations

from provisa.core import org_files
from provisa.security import sandbox_egress  # REQ-2006
import json
import pathlib
import yaml

# Requirements: REQ-314, REQ-315


def parse_text(content: str) -> dict:  # REQ-315, REQ-407
    """Parse an inline YAML or JSON spec string.

    Tries YAML first, falls back to JSON.
    """
    try:
        return yaml.safe_load(content)
    except Exception:
        return json.loads(content)


def load_spec(spec_path: str) -> dict:  # REQ-314, REQ-315
    """Load OpenAPI spec from a local file path or remote URL (http/https), or the spec a
    branded source ships with (``brand:<id>``, REQ-1923).

    Raises FileNotFoundError for missing local paths.
    Raises httpx.HTTPError for HTTP failures.
    Raises ValueError for parse errors.
    """
    from provisa.openapi.brands import brand_spec, spec_brand

    brand = spec_brand(spec_path)
    if brand is not None:
        return brand_spec(brand)
    from provisa.core.source_uploads import on_own_disk, require_own_disk

    # REQ-1997: what the location is decides how it is read, never whether a file of that name
    # happens to exist; a held org's specification on the server's own disk is in its own folder.
    if not on_own_disk(spec_path) and spec_path.startswith(("http://", "https://")):
        r = sandbox_egress.checked_get(spec_path, timeout=30)
        r.raise_for_status()
        text = r.text
    else:
        require_own_disk(spec_path)
        p = pathlib.Path(org_files.resolve_path(spec_path))
        if not p.exists():
            raise FileNotFoundError(f"Spec not found: {spec_path!r}")
        text = p.read_text()

    if spec_path.endswith(".yaml") or spec_path.endswith(".yml"):
        return yaml.safe_load(text)
    if spec_path.endswith(".json"):
        return json.loads(text)
    # Heuristic: try YAML first, fall back to JSON
    try:
        return yaml.safe_load(text)
    except Exception:
        return json.loads(text)
