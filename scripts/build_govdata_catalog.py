#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build provisa/govdata/catalog_metadata.json (REQ-1798) from the govdata engine's
*-schema.yaml files — a static, checked-in snapshot of schema/table descriptions so
search_govdata_subjects can do real keyword matching (e.g. "inflation" -> the econ
schema's `inflation_metrics`/`metro_cpi` tables) without a live askamerica connection
or the sibling Java repo being present at runtime.

Re-run this manually whenever the govdata schema YAML files change; it is not part
of the app's runtime or CI (the sibling repo isn't guaranteed to be checked out
everywhere Provisa runs).

Usage: python3 scripts/build_govdata_catalog.py [path to govdata resources dir]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

DEFAULT_SRC = Path(
    "/Volumes/main/Users/kennethstott/IdeaProjects/calcite/govdata/src/main/resources"
)
OUT_PATH = Path(__file__).resolve().parent.parent / "provisa" / "govdata" / "catalog_metadata.json"


def _tables_from(doc: dict, key: str) -> list[dict]:
    out = []
    for entry in doc.get(key) or []:
        if not isinstance(entry, dict) or "name" not in entry:
            continue
        comment = entry.get("comment") or ""
        out.append({"name": entry["name"], "comment": " ".join(comment.split())})
    return out


def build(src_dir: Path) -> dict[str, dict]:
    catalog: dict[str, dict] = {}
    for schema_yaml in sorted(src_dir.glob("*/*-schema.yaml")):
        with open(schema_yaml) as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict):
            continue
        schema_name = schema_yaml.stem.removesuffix("-schema")
        comment = doc.get("comment") or ""
        tables = _tables_from(doc, "tables") + _tables_from(doc, "partitionedTables")
        if not comment and not tables:
            continue
        catalog[schema_name] = {"comment": " ".join(comment.split()), "tables": tables}
    return catalog


def main() -> None:
    src_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
    if not src_dir.is_dir():
        raise SystemExit(f"govdata resources dir not found: {src_dir}")
    catalog = build(src_dir)
    OUT_PATH.write_text(json.dumps(catalog, indent=2, sort_keys=True) + "\n")
    n_tables = sum(len(v["tables"]) for v in catalog.values())
    print(f"Wrote {OUT_PATH} — {len(catalog)} schemas, {n_tables} tables.")


if __name__ == "__main__":
    main()
