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

``address_config`` writes the same configuration as an interchange document (REQ-1364): each
internal cross-reference also carries a JSON Pointer to what it names, and each table and column its
physical ``resourcePath``.
"""

# Requirements: REQ-164, REQ-1096, REQ-1364, REQ-1919

import difflib
import json
import re
from typing import Any
from urllib.parse import quote

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


# ---------------------------------------------------------------------------------------------
# REQ-1364: the addressed export. An interchange projection of the configuration above: every
# internal cross-reference also carries an RFC 6901 JSON Pointer to the object it names, and every
# table and column carries the physical ``resourcePath`` URI it is read from, so an external
# validator or format converter resolves both without a second pass over the model.
#
# The configuration's own name references stay where they are: the model definition is name-based
# (REQ-1364 design notes), and every reader of the configuration (the loader, the Ossie converter,
# the diff and the patch) keeps reading it unchanged. The pointers sit beside the names, under one
# ``$refs`` map per object keyed by the referring field, and the configuration's models ignore both
# ``$refs`` and ``resourcePath``, so the addressed document loads as the plain one does.
#
# A pointer indexes the document as it is written, so the projection starts from the normalized
# configuration and is dumped without normalizing again: normalizing sorts some lists by a key the
# ``$refs`` map would change, and the pointers would then name the wrong entries.
# ---------------------------------------------------------------------------------------------

_REFS = "$refs"
_RESOURCE_PATH = "resourcePath"

# ``table.column`` inside a metric's expression (REQ-1317: references are semantic table.column).
_QUALIFIED_COLUMN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")


def _pointer(*parts: str | int) -> str:
    """An RFC 6901 pointer in URI-fragment form (``#/tables/3/columns/0``): each reference token
    escaped (``~`` → ``~0``, ``/`` → ``~1``), then percent-encoded for the fragment."""
    tokens = (str(p).replace("~", "~0").replace("/", "~1") for p in parts)
    return "#" + "".join("/" + quote(t, safe="~") for t in tokens)


class _Addresses:
    """The pointer of every addressable object in one configuration document, by the names the
    configuration refers to it by."""

    def __init__(self, cfg: dict) -> None:
        self.sources = self._by_id(cfg, "sources", "id")
        self.domains = self._by_id(cfg, "domains", "id")
        self.roles = self._by_id(cfg, "roles", "id")
        self.tags = self._by_id(cfg, "tags", "id")
        # Tables are named as the config names them: the virtual name (alias when set, else the
        # table name), bare or schema-qualified, or ``source.schema.table`` (a command's returns).
        self._tables: dict[str, list[int]] = {}
        self._columns: dict[int, dict[str, int]] = {}
        for i, table in enumerate(cfg.get("tables") or []):
            schema, source = table.get("schema"), table.get("source_id")
            for name in {table.get("table"), table.get("alias")} - {None}:
                for key in {name, f"{schema}.{name}", f"{source}.{schema}.{name}"}:
                    self._tables.setdefault(key, []).append(i)
            self._columns[i] = {c["name"]: j for j, c in enumerate(table.get("columns") or [])}

    @staticmethod
    def _by_id(cfg: dict, section: str, key: str) -> dict[str, str]:
        return {
            str(item[key]): _pointer(section, i)
            for i, item in enumerate(cfg.get(section) or [])
            if isinstance(item, dict) and item.get(key) is not None
        }

    def table_index(self, name: str, context: str) -> int | None:
        """The index of the table ``name`` names; None when no table in this document has that
        name. A reference may name a table the document leaves out by design (a seeded table,
        REQ-1634, or a section the admin did not choose, REQ-1919): it is not an internal
        reference and gets no pointer. A name two tables answer addresses neither, and the export
        refuses rather than pointing at one (the rule ``metadata_export.refs.TableIndex`` keeps)."""
        found = sorted(set(self._tables.get(name, [])))
        if len(found) > 1:
            raise ValueError(f"{context} names table {name!r}, which matches tables {found}")
        return found[0] if found else None

    def table(self, name: str, context: str) -> str | None:
        index = self.table_index(name, context)
        return None if index is None else _pointer("tables", index)

    def column(self, table_index: int, column: str) -> str | None:
        j = self._columns[table_index].get(column)
        return None if j is None else _pointer("tables", table_index, "columns", j)


def _set_ref(item: dict, field: str, pointer: str | list[str] | None) -> None:
    if pointer:
        item.setdefault(_REFS, {})[field] = pointer


def _named(index: dict[str, str], names: Any) -> list[str]:
    return [index[n] for n in names or [] if n in index]


def _column_refs(addr: _Addresses, table_index: int | None, columns: str) -> str | list[str] | None:
    """A relationship end's column pointer, or one per column of a composite end (REQ-1586: an
    end is a comma-separated ordered column list)."""
    if table_index is None or not columns:
        return None
    names = [c.strip() for c in columns.split(",")]
    pointers = [p for p in (addr.column(table_index, n) for n in names) if p]
    if len(names) == 1:
        return pointers[0] if pointers else None
    return pointers or None


def _connection_base(source: dict) -> str:
    """The physical location a source's connection declares, as a URI: its type as the scheme,
    then the host and port with the database (a network source), the file path (a file source),
    or the base URL itself (an API source). A source that declares no locator (an embedded
    engine) has an empty authority, as ``file:///`` does."""
    scheme = str(source["type"])
    if source.get("base_url"):
        return str(source["base_url"]).rstrip("/")
    if source.get("host"):
        authority = str(source["host"]) + (f":{source['port']}" if source.get("port") else "")
        database = f"/{quote(str(source['database']), safe='')}" if source.get("database") else ""
        return f"{scheme}://{authority}{database}"
    if source.get("path"):
        return f"{scheme}://{quote(str(source['path']), safe='/:')}"
    return f"{scheme}://"


def table_resource_path(source: dict, table: dict) -> str:
    """A table's physical resourcePath: its source's connection location, then schema and table
    (``postgresql://host:5432/db/schema/table``)."""
    segments = [table.get("schema"), table["table"]]
    path = "/".join(quote(str(s), safe="") for s in segments if s)
    return f"{_connection_base(source)}/{path}"


def column_resource_path(source: dict, table: dict, column: str) -> str:
    """A column's physical resourcePath: its table's, with the column as the fragment."""
    return f"{table_resource_path(source, table)}#{quote(column, safe='')}"


def _address_tables(cfg: dict, addr: _Addresses) -> None:
    sources = {s["id"]: s for s in cfg.get("sources") or []}
    for table in cfg.get("tables") or []:
        _set_ref(table, "source_id", addr.sources.get(table.get("source_id", "")))
        _set_ref(table, "domain_id", addr.domains.get(table.get("domain_id", "")))
        source = sources.get(table.get("source_id"))
        # A table whose source is not in this document (a derived view's internal source, or the
        # sources section left out of a chosen export) has no connection here to compose from.
        if source is None:
            continue
        table[_RESOURCE_PATH] = table_resource_path(source, table)
        for column in table.get("columns") or []:
            column[_RESOURCE_PATH] = column_resource_path(source, table, column["name"])


def _address_relationships(cfg: dict, addr: _Addresses) -> None:
    for rel in cfg.get("relationships") or []:
        context = f"relationship {rel.get('id')!r}"
        source_i = addr.table_index(rel.get("source_table_id") or "", context)
        target_i = addr.table_index(rel.get("target_table_id") or "", context)
        _set_ref(rel, "source_table_id", None if source_i is None else _pointer("tables", source_i))
        _set_ref(rel, "target_table_id", None if target_i is None else _pointer("tables", target_i))
        _set_ref(rel, "source_column", _column_refs(addr, source_i, rel.get("source_column", "")))
        _set_ref(rel, "target_column", _column_refs(addr, target_i, rel.get("target_column", "")))
        if rel.get("via_table"):
            _set_ref(rel, "via_table", addr.table(rel["via_table"], context))


def _address_governance(cfg: dict, addr: _Addresses) -> None:
    for domain in cfg.get("domains") or []:
        _set_ref(domain, "parent_domain_id", addr.domains.get(domain.get("parent_domain_id") or ""))
    for role in cfg.get("roles") or []:
        _set_ref(role, "parents", _named(addr.roles, role.get("parents")))
        _set_ref(role, "domain_access", _named(addr.domains, role.get("domain_access")))
    for rule in cfg.get("rls_rules") or []:
        context = f"row rule of role {rule.get('role_id')!r}"
        _set_ref(rule, "role_id", addr.roles.get(rule.get("role_id") or ""))
        _set_ref(rule, "domain_id", addr.domains.get(rule.get("domain_id") or ""))
        _set_ref(rule, "tag_id", addr.tags.get(rule.get("tag_id") or ""))
        if rule.get("table_id"):
            _set_ref(rule, "table_id", addr.table(rule["table_id"], context))


def _address_metrics(cfg: dict, addr: _Addresses) -> None:
    for metric in cfg.get("metrics") or []:
        context = f"metric {metric.get('name')!r}"
        pointers: list[str] = []
        for table_name, column in _QUALIFIED_COLUMN.findall(metric.get("expression") or ""):
            index = addr.table_index(table_name, context)
            pointer = None if index is None else addr.column(index, column)
            if pointer and pointer not in pointers:
                pointers.append(pointer)
        _set_ref(metric, "expression", pointers)
        _set_ref(metric, "visible_to", _named(addr.roles, metric.get("visible_to")))


def _address_commands_and_products(cfg: dict, addr: _Addresses) -> None:
    for function in cfg.get("functions") or []:
        context = f"command {function.get('name')!r}"
        _set_ref(function, "returns", addr.table(function.get("returns") or "", context))
        _set_ref(function, "source_id", addr.sources.get(function.get("source_id") or ""))
        _set_ref(function, "domain_id", addr.domains.get(function.get("domain_id") or ""))
    for webhook in cfg.get("webhooks") or []:
        if webhook.get("returns"):
            context = f"webhook {webhook.get('name')!r}"
            _set_ref(webhook, "returns", addr.table(webhook["returns"], context))
    for product in cfg.get("data_products") or []:
        context = f"data product {product.get('id')!r}"
        _set_ref(product, "domain_id", addr.domains.get(product.get("domain_id") or ""))
        tables = [addr.table(name, context) for name in product.get("tables") or []]
        _set_ref(product, "tables", [p for p in tables if p])


def address_config(cfg: dict) -> dict:
    """The addressed projection of a plain configuration (REQ-1364): normalized, then each
    internal cross-reference given its JSON Pointer under ``$refs`` and each table and column of a
    source in the document given its ``resourcePath``. Dump it without normalizing again."""
    doc = normalize_config(cfg)
    addr = _Addresses(doc)
    _address_tables(doc, addr)
    _address_relationships(doc, addr)
    _address_governance(doc, addr)
    _address_metrics(doc, addr)
    _address_commands_and_products(doc, addr)
    return doc


async def build_addressed_config(sections: list[str] | None = None) -> dict:
    """The current configuration (or its chosen sections) as the addressed projection."""
    return address_config(await build_live_config(sections))


async def build_addressed_config_yaml(sections: list[str] | None = None) -> str:
    """The addressed projection as YAML, in the order the pointers index."""
    doc = await build_addressed_config(sections)
    return yaml.dump(doc, default_flow_style=False, sort_keys=False)
