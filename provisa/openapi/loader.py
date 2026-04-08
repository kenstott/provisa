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
import json
import pathlib
import httpx
import yaml


def load_spec(spec_path: str) -> dict:
    """Load OpenAPI spec from a local file path or remote URL (http/https).

    Raises FileNotFoundError for missing local paths.
    Raises httpx.HTTPError for HTTP failures.
    Raises ValueError for parse errors.
    """
    p = pathlib.Path(spec_path)
    if p.exists():
        text = p.read_text()
    elif spec_path.startswith("http://") or spec_path.startswith("https://"):
        r = httpx.get(spec_path, timeout=30, follow_redirects=True)
        r.raise_for_status()
        text = r.text
    else:
        raise FileNotFoundError(f"Spec not found: {spec_path!r}")

    if spec_path.endswith(".yaml") or spec_path.endswith(".yml"):
        return yaml.safe_load(text)
    if spec_path.endswith(".json"):
        return json.loads(text)
    # Heuristic: try YAML first, fall back to JSON
    try:
        return yaml.safe_load(text)
    except Exception:
        return json.loads(text)
