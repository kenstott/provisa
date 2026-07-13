# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generate the CURRENT config from live state and diff it against the on-disk file (REQ-164).

The on-disk config is only the boot seed; admin mutations write to the control-plane DB, so the file
goes stale — a materialized view created in the UI never appears in it. ``build_live_config`` rebuilds
the DB-backed sections (tables incl views/MVs, relationships, roles, rls_rules, domains) from live
state and overlays them on the file base; file-only sections and source credentials are preserved.

Two things make the diff meaningful rather than noise:
  * Faithful projection — each DB row is projected to ONLY the config-schema fields (DB-only columns
    like ``disable_cypher``/``version``/``prefer_materialized`` are dropped), and integer ``*_table_id``
    references are resolved back to the table NAMES the config uses (the DB stores int ids).
  * Normalization — both sides run through the SAME deep key-sort + stable entity-sort, so section /
    key / entity ORDER never differs.

Internal ``meta``/``ops`` entities and the unassigned (empty-id) domain — never in a user file — are
dropped so the current matches the file's scope.
"""

import difflib
import json
from typing import Any

import yaml

from provisa.api.admin._config_io import config_path, read_config

_INTERNAL_DOMAINS = frozenset({"meta", "ops"})

# Config-schema field whitelists per section — the ONLY keys that belong in the exported config. DB
# rows carry extra control-plane columns; anything outside these sets is dropped.
_TABLE_KEYS = frozenset(
    {
        "source_id",
        "domain_id",
        "schema",
        "table",
        "alias",
        "description",
        "governance",
        "columns",
        "view_sql",
        "materialize",
        "enable_aggregates",
        "enable_group_by",
    }
)
_COLUMN_KEYS = frozenset({"name", "description", "visible_to", "governance"})
_REL_KEYS = frozenset(
    {
        "id",
        "alias",
        "graphql_alias",
        "cardinality",
        "source_table_id",
        "target_table_id",
        "source_column",
        "target_column",
    }
)
_ROLE_KEYS = frozenset({"id", "capabilities", "domain_access"})
_RLS_KEYS = frozenset({"table_id", "domain_id", "role_id", "filter"})
_DOMAIN_KEYS = frozenset({"id", "description"})


def _plain(obj: Any) -> Any:
    """Coerce a value tree to plain YAML/JSON-safe types. DB rows carry SQLAlchemy ``quoted_name``
    (a str subclass) in keys AND values; ``yaml.dump`` would emit those as ``!!python/object`` tags. A
    JSON round-trip (str subclasses serialize as plain strings; ``default=str`` catches the rest)
    flattens the tree to str/int/float/bool/None/list/dict."""
    return json.loads(json.dumps(obj, default=str))


def _project(row: dict, allowed: frozenset[str], *, id_to_name: dict[int, str]) -> dict:
    """Keep only config-schema keys; drop null/empty; resolve integer ``*_table_id`` refs to the table
    name the config uses (the DB stores int ids, the config stores names)."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        key = str(k)
        if key not in allowed or v is None or v == [] or v == "":
            continue
        if key.endswith("_table_id"):
            try:
                v = id_to_name.get(int(v), v)
            except (TypeError, ValueError):
                pass
        out[key] = v
    return out


def _table_to_config(row: dict, id_to_name: dict[int, str]) -> dict:
    row = {**row, "schema": row.get("schema_name"), "table": row.get("table_name")}
    out = _project(row, _TABLE_KEYS, id_to_name=id_to_name)
    if isinstance(out.get("columns"), list):
        out["columns"] = [_project(c, _COLUMN_KEYS, id_to_name=id_to_name) for c in out["columns"]]
    return out


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


async def build_live_config() -> dict:
    """The current config: file base with its DB-backed sections rebuilt from live state, each row
    projected to the config schema and table-id refs resolved to names. File-only sections and source
    credentials are preserved; internal meta/ops and unassigned-domain entities are excluded."""
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.core.repositories import domain as domain_repo
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.repositories import role as role_repo
    from provisa.core.repositories import table as table_repo

    base = read_config()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        tables = await table_repo.list_all(conn)
        rels = await rel_repo.list_all(conn)
        roles = await role_repo.list_all(conn)
        rls = await rls_repo.list_all(conn)
        domains = await domain_repo.list_all(conn)

    def _internal_domain(d: Any) -> bool:
        return not d or str(d) in _INTERNAL_DOMAINS

    def _is_internal_table(t: dict) -> bool:
        # meta/ops tables are seeded with those domain_ids (startup_seed) — the ONE reliable signal. A
        # user view/MV lives under org_*_mv_cache but keeps its real domain, so schema-prefix checks
        # would wrongly exclude it.
        return _internal_domain(t.get("domain_id"))

    # meta/ops tables are implied by the internal model and MUST NOT appear in the external config —
    # nor may relationships/rls that reference them.
    internal_table_ids: set[int] = {
        int(t["id"]) for t in tables if t.get("id") is not None and _is_internal_table(t)
    }

    # int table id → the VIRTUAL name the config references: alias when set, else table_name (matches
    # table_repo.find_by_table_name, the loader's resolver). Getting this order wrong made every
    # aliased relationship diff.
    id_to_name: dict[int, str] = {}
    for t in tables:
        tid = t.get("id")
        name = t.get("alias") or t.get("table_name")
        if tid is not None and name is not None:
            id_to_name[int(tid)] = str(name)

    def _refs_internal(row: dict) -> bool:
        for k in ("source_table_id", "target_table_id", "table_id"):
            v = row.get(k)
            try:
                if v is not None and int(v) in internal_table_ids:
                    return True
            except (TypeError, ValueError):
                pass
        return False

    base["tables"] = [_table_to_config(t, id_to_name) for t in tables if not _is_internal_table(t)]
    base["relationships"] = [
        _project(r, _REL_KEYS, id_to_name=id_to_name) for r in rels if not _refs_internal(r)
    ]
    base["roles"] = [_project(r, _ROLE_KEYS, id_to_name=id_to_name) for r in roles]
    base["rls_rules"] = [
        _project(r, _RLS_KEYS, id_to_name=id_to_name)
        for r in rls
        if not _refs_internal(r) and not _internal_domain(r.get("domain_id"))
    ]
    base["domains"] = [
        _project(d, _DOMAIN_KEYS, id_to_name=id_to_name)
        for d in domains
        if not _internal_domain(d.get("id"))
    ]
    return _plain(base)


def _dump(cfg: Any) -> str:
    return yaml.dump(normalize_config(cfg), default_flow_style=False, sort_keys=False)


async def build_live_config_yaml() -> str:
    """The current config as normalized YAML (for standalone download)."""
    return _dump(await build_live_config())


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
