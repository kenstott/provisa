# Copyright (c) 2026 Kenneth Stott
# Canary: 1aad82d6-8ec5-4fbe-9161-9e7686346514
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model as it stands, written as a configuration file, and its diff against the file
(REQ-164, REQ-1096, REQ-1919).

The deployment's configuration file seeds the model store once; from then on the store alone owns
the model, so the file goes stale — a materialized view created in the UI never appears in it.
``build_live_config`` writes the configuration the store holds now: the file's settings, with every
model section (sources, domains, roles, tables and their columns, relationships, row filters,
metrics, data products, tags and their assignments, glossary terms, commands, webhooks, scheduled
triggers, stores and regions, naming rules) taken from the store (``provisa.core.store_config``),
or only the sections an admin chose. Applied to an empty store it builds the same model.

Two things make the diff meaningful rather than noise:
  * Faithful projection — each row is projected onto the configuration's own models, by their
    config names, with table-id references resolved back to the table names the config uses.
  * Normalization — both sides run through the SAME deep key-sort + stable entity-sort, so section /
    key / entity ORDER never differs.
"""

# Requirements: REQ-164, REQ-1096, REQ-1919

import difflib
import json
from typing import Any

import yaml

from provisa.api.admin._config_io import config_path, read_config


def _plain(obj: Any) -> Any:
    """Coerce a value tree to plain YAML/JSON-safe types. DB rows carry SQLAlchemy ``quoted_name``
    (a str subclass) in keys AND values; ``yaml.dump`` would emit those as ``!!python/object`` tags. A
    JSON round-trip (str subclasses serialize as plain strings; ``default=str`` catches the rest)
    flattens the tree to str/int/float/bool/None/list/dict."""
    return json.loads(json.dumps(obj, default=str))


def _entity_sort_key(item: Any) -> str:
    if isinstance(item, dict):
        for k in ("id", "table", "table_name", "name", "role_id", "alias"):
            if k in item and item[k] is not None:
                return f"{k}={item[k]}"
        return json.dumps(item, sort_keys=True, default=str)
    return str(item)


def normalize_config(cfg: Any) -> Any:
    """Deterministic shape for diffing: deep-sort dict keys and sort every list-of-dicts by a stable
    entity key. Applied IDENTICALLY to both sides so ordering never contributes diff noise."""
    if isinstance(cfg, dict):
        return {k: normalize_config(cfg[k]) for k in sorted(cfg, key=str)}
    if isinstance(cfg, list):
        items = [normalize_config(i) for i in cfg]
        if items and all(isinstance(i, dict) for i in items):
            items = sorted(items, key=_entity_sort_key)
        return items
    return cfg


async def build_live_config(sections: list[str] | None = None) -> dict:
    """The current configuration: the deployment file's settings with every model section the
    store holds (REQ-1919). ``sections`` names the part of the model an admin chose; then only
    those sections are written (``store_config.only_sections``)."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.core.store_config import only_sections, store_model, with_store_model

    pool = await _get_pool()
    async with pool.acquire() as conn:
        model = await store_model(conn)
    cfg = with_store_model(read_config(), model)
    if sections is not None:
        cfg = only_sections(cfg, sections)
    return _plain(cfg)


def _dump(cfg: Any) -> str:
    return yaml.dump(normalize_config(cfg), default_flow_style=False, sort_keys=False)


async def build_live_config_yaml(sections: list[str] | None = None) -> str:
    """The current config — or the chosen sections of its model — as normalized YAML."""
    return _dump(await build_live_config(sections))


def _baseline() -> str:
    """The diff/patch baseline: the boot snapshot (state at startup) when captured, else the on-disk
    file. This is the ``original`` side both the diff view and the patch are computed against."""
    from provisa.api.app import state

    snapshot = getattr(state, "config_boot_snapshot", None)
    return snapshot if snapshot is not None else _dump(read_config())


async def config_diff() -> dict[str, str]:
    """Both sides of the diff, normalized identically. ``original`` is the BOOT SNAPSHOT — the config
    generated once at startup, after all runtime auto-derivation (FK tracking, graphql-remote) — so
    the diff shows only changes made SINCE startup (e.g. an MV created in the UI), not derived entities
    that were never in the file. Falls back to the on-disk file when no snapshot was captured.
    ``current`` is live state."""
    return {"original": _baseline(), "current": _dump(await build_live_config())}


def make_config_patch(revised: str) -> str:
    """A unified-diff patch (git-apply / ``patch`` compatible) from the baseline to ``revised`` — the
    curated current config from the diff view. CI/CD applies this to a config matching the baseline to
    reproduce the changes made in the UI. Returns '' when there is no difference."""
    name = config_path().name
    diff = difflib.unified_diff(
        _baseline().splitlines(keepends=True),
        revised.splitlines(keepends=True),
        fromfile=f"a/{name}",
        tofile=f"b/{name}",
    )
    patch = "".join(diff)
    # difflib omits a trailing newline when the last line lacks one — ensure the patch ends cleanly.
    if patch and not patch.endswith("\n"):
        patch += "\n"
    return patch
