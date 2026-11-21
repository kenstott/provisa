# Copyright (c) 2026 Kenneth Stott
# Canary: 29b7ebe5-a431-495d-a1a7-970571aa9a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trim a recorded catalog spec to the GET operations its tests read (REQ-2265).

A catalog brand reads its spec from the running instance, so nothing is shipped and the tests
read a recording. A whole spec runs to megabytes, so a recording keeps the named GET operations
and every schema and parameter component they reach, unchanged."""

import gzip
import json
from pathlib import Path

_PREFIXES = {"#/components/schemas/": "schemas", "#/components/parameters/": "parameters"}


def _refs(node, found: set[tuple[str, str]]) -> None:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            for prefix, section in _PREFIXES.items():
                if ref.startswith(prefix):
                    found.add((section, ref[len(prefix) :]))
        for v in node.values():
            _refs(v, found)
    elif isinstance(node, list):
        for v in node:
            _refs(v, found)


def trim(spec: dict, paths: tuple[str, ...]) -> dict:
    missing = [p for p in paths if "get" not in spec["paths"].get(p, {})]
    if missing:
        raise KeyError(f"the recorded spec has no GET at {missing}")
    kept_paths = {p: {"get": spec["paths"][p]["get"]} for p in paths}
    components = spec["components"]
    kept: set[tuple[str, str]] = set()
    todo: set[tuple[str, str]] = set()
    _refs(kept_paths, todo)
    while todo:
        item = todo.pop()
        if item in kept:
            continue
        kept.add(item)
        section, name = item
        _refs(components[section][name], todo)
    trimmed: dict = {}
    for section in ("schemas", "parameters"):
        names = sorted(name for s, name in kept if s == section)
        if names:
            trimmed[section] = {n: components[section][n] for n in names}
    if "securitySchemes" in components:
        trimmed["securitySchemes"] = components["securitySchemes"]
    return {
        **{k: v for k, v in spec.items() if k not in ("paths", "components", "tags")},
        "paths": kept_paths,
        "components": trimmed,
    }


def write(spec: dict, out: Path) -> None:
    with gzip.open(out, "wt", encoding="utf-8") as fh:
        json.dump(spec, fh, sort_keys=True)
