# Copyright (c) 2026 Kenneth Stott
# Canary: 4f5a2d18-9b3c-4e77-a0d5-6c81e2b47f93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed model as a DIRECTORY OF FILES, one file per entity (REQ-1526).

WHY A TREE AND NOT A DOCUMENT. The environment already versions its model in git (REQ-1524), and a
single document makes every change to any entity a change to one file: two people editing two
unrelated tables conflict, a diff of "what changed in sales" is a diff of everything, and blame for
one column names whoever last touched the document. One file per entity turns each of those back
into what it is.

THE PATH IS THE ADDRESS. A file's location is its REQ-1385 URI with the scheme and org stripped, so
``provisa://acme/sales/tables/Order`` is ``sales/tables/Order.yaml`` and nothing has to hold a
second mapping from address to storage. Fragments are NOT paths: ``#field:`` and ``#rel:`` address
parts of a table, so a column and a relationship are written INSIDE the table's file, where an
editor changing a column is editing the concept it belongs to.

WHAT IS AN ENTITY. A row whose lifetime is its own gets a file; a row that CASCADES with another one
is written inside that one's file. That rule is read off the schema rather than chosen per table:
``table_columns`` cascades from ``registered_tables``, so columns are part of the table's file, and
the surrogate ``table_id`` that joined them never appears anywhere.

NO SURROGATE KEYS. ``registered_tables.id`` is an autoincrementing integer, so the same model copied
into a second environment gets different numbers and a naive dump diffs against itself. Every
surrogate is dropped and every reference to one is written as the target's PATH, which is stable
across environments because it is derived from the model's own names.

NO BINDINGS AND NO TENANCY. Where a source points stays out of the tree entirely (REQ-1491,
REQ-1525): the columns :mod:`provisa.core.env_classes` classifies as bindings are excluded here, so
a credential cannot reach a commit even by being pasted into one. ``tenant_id``/``org_id`` and the
created/updated stamps are excluded for a different reason -- they say where a row is stored and
when it was written, not what the model says.

DETERMINISM IS THE POINT. Two environments holding the same model must produce byte-identical trees,
or every diff is noise. Keys are emitted in one fixed (alphabetical) order, child collections are
sorted by their own address, and the YAML style is fixed. Values are NOT re-ordered: a list inside a
model field -- ``column_presets``, ``unique_constraints`` -- is ordered by the model itself, and
sorting it would change what it means.
"""

# Requirements: REQ-1385, REQ-1489, REQ-1491, REQ-1524, REQ-1525, REQ-1526

from __future__ import annotations

from typing import Any, NamedTuple
from urllib.parse import quote, unquote

import yaml

from provisa.core.env_classes import BINDING_COLUMNS, BOUND_COLUMN

#: Written by storage, not by the model: a surrogate key, the tenancy a row lives under, or a stamp
#: recording when the row was written. None of them survive a copy into another environment, and
#: emitting them would make an identical model diff against itself.
STORAGE_COLUMNS: frozenset[str] = frozenset(
    {"id", "tenant_id", "org_id", "created_at", "updated_at"}
)

#: Derived caches that a rebuild recomputes: they describe THIS environment's clustering run.
DERIVED_COLUMNS: frozenset[str] = frozenset(
    {"l1_cluster", "l2_cluster", "l3_cluster", "clusters_computed_at"}
)

#: The file extension. One extension, because a loader that accepts two has to decide which wins
#: when both exist.
SUFFIX = ".yaml"


class FileLayoutError(Exception):
    """The tree cannot express this model, or the tree on disk is not a model."""


def _segment(name: str) -> str:
    """One path segment, quoted the way REQ-1385 quotes a URI segment.

    The same quoting, so that a path IS the URI's tail rather than a second encoding of the same
    name: a table called ``Order/Line`` addresses one entity, and its file must be one file.
    """
    return quote(name, safe="")


def _unsegment(segment: str) -> str:
    return unquote(segment)


def _domain_prefix(domain_id: str | None) -> str:
    """``sales/eu/`` for a hierarchical domain id, and ``""`` for a table in no domain."""
    if not domain_id:
        return ""
    return "/".join(_segment(part) for part in domain_id.split("/")) + "/"


def domain_path(domain_id: str) -> str:
    """A domain's own attributes live in ``domain.yaml`` INSIDE its directory.

    The directory has to exist anyway to hold the domain's tables, and a sibling file next to it
    (``sales.yaml`` beside ``sales/``) puts one concept in two places that can be moved apart.
    """
    return f"{_domain_prefix(domain_id)}domain{SUFFIX}"


def table_path(domain_id: str | None, business_name: str) -> str:
    """REQ-1385's table URI as a path: the BUSINESS name, never the physical coordinates.

    ``alias or table_name`` is the business name, and which source and schema the table is read from
    is a binding — it stays out of the tree with the rest of them.
    """
    return f"{_domain_prefix(domain_id)}tables/{_segment(business_name)}{SUFFIX}"


def kind_path(kind_dir: str, *parts: str) -> str:
    """A root-level kind directory, e.g. ``sources/pg_main.yaml`` or ``calendars/fiscal/2026.yaml``."""
    return f"{kind_dir}/" + "/".join(_segment(p) for p in parts) + SUFFIX


def _model_columns(table: str, row: dict[str, Any]) -> dict[str, Any]:
    """``row`` less everything that is storage, derivation, or a binding."""
    excluded = STORAGE_COLUMNS | DERIVED_COLUMNS
    if table in BINDING_COLUMNS:
        excluded = excluded | BINDING_COLUMNS[table] | {BOUND_COLUMN}
    return {k: v for k, v in row.items() if k not in excluded and v is not None}


def dump(tree: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Render an addressed tree to file text, deterministically.

    ``sort_keys`` is what gives the fixed key order: alphabetical is not a better order than any
    other, it is a FIXED one, which is the whole requirement. Flow style is off so that every scalar
    lands on its own line and a one-field change diffs as one line.
    """
    return {
        path: yaml.safe_dump(
            tree[path], sort_keys=True, default_flow_style=False, allow_unicode=True, width=100
        )
        for path in sorted(tree)
    }


def load(files: dict[str, str]) -> dict[str, dict[str, Any]]:
    """The inverse of :func:`dump`: file text back to an addressed tree.

    A file that is not a mapping is refused rather than coerced. The tree's shape is the model's
    shape, so a list or a scalar where an entity belongs means the file was written by something
    else, and guessing what it meant would put that guess into the environment.
    """
    tree: dict[str, dict[str, Any]] = {}
    for path in sorted(files):
        if not path.endswith(SUFFIX):
            raise FileLayoutError(
                f"{path!r} is not a model file: every entity file ends {SUFFIX!r}"
            )
        parsed = yaml.safe_load(files[path])
        if not isinstance(parsed, dict):
            raise FileLayoutError(
                f"{path!r} holds {type(parsed).__name__}, not an entity mapping; a model file "
                f"describes exactly one entity"
            )
        tree[path] = parsed
    return tree


def address_of(path: str, org_id: str) -> str:
    """The REQ-1385 URI a path addresses — the inverse of stripping scheme and org.

    Exists so the tree can be read back by an exporter that speaks URIs without either side
    re-deriving the other's rules.
    """
    if not path.endswith(SUFFIX):
        raise FileLayoutError(f"{path!r} is not a model file: every entity file ends {SUFFIX!r}")
    return f"provisa://{_segment(org_id)}/{path[: -len(SUFFIX)]}"


def path_of(uri: str) -> str:
    """The path an ``provisa://`` URI addresses, with the scheme and org stripped.

    A fragment is refused: ``#field:`` and ``#rel:`` address a PART of a table, and the part is
    written inside the table's file, so there is no path that names it.
    """
    if not uri.startswith("provisa://"):
        raise FileLayoutError(f"{uri!r} is not a provisa URI")
    if "#" in uri:
        raise FileLayoutError(
            f"{uri!r} addresses a fragment; a column or relationship is written inside its table's "
            f"file and has no file of its own"
        )
    rest = uri[len("provisa://") :]
    if "/" not in rest:
        raise FileLayoutError(f"{uri!r} addresses an organization, which is the tree itself")
    return rest.split("/", 1)[1] + SUFFIX


def _sorted_children(children: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Child collections are sorted BY ADDRESS, which is the one order both environments agree on.

    Not by the surrogate id they arrived in: insertion order differs between two environments
    holding the same model, and sorting by it would make the trees differ too.
    """
    missing = [c for c in children if key not in c]
    if missing:
        raise FileLayoutError(
            f"a child collection sorted by {key!r} has {len(missing)} member(s) without it; the "
            f"sort key is the child's address and every child has one"
        )
    return sorted(children, key=lambda c: str(c[key]))


class Child(NamedTuple):
    """One nested collection: how it is ordered, what joined it, and its rows.

    ``parent_key`` is named rather than inferred because inferring it means guessing which of a
    child's ``*_id`` columns points at the parent — and a child can hold more than one
    (``glossary_term_edges`` holds two term ids, ``rls_rules`` holds a table and a role). A guess
    that picked wrong would delete a real reference from the file.

    ``table`` names the child's OWN table so that its binding columns are the ones excluded from it.
    Defaulting to the parent's table would exclude the parent's binding names from the child, which
    both misses a child's own binding and can delete an innocent child column that happens to share
    a name with one of the parent's (``sources`` excludes ``path`` and ``mapping``).

    ``keep`` re-admits a column :data:`STORAGE_COLUMNS` drops. It exists for ``id``, which is storage
    on a table whose keys the database hands out and MODEL on a table whose keys the caller chooses:
    a relationship's id is a name somebody typed, and a nested child has no path to carry it.
    """

    sort_key: str
    parent_key: str | tuple[str, ...]
    rows: list[dict[str, Any]]
    table: str | None = None
    keep: tuple[str, ...] = ()


def _child_columns(
    child: "Child", parent_table: str, row: dict[str, Any], dropped: set[str]
) -> dict[str, Any]:
    """One nested row: model columns, less the keys that joined it, plus what ``keep`` re-admits."""
    body = {
        k: v
        for k, v in _model_columns(child.table or parent_table, row).items()
        if k not in dropped
    }
    for column in child.keep:
        if row.get(column) is not None:
            body[column] = row[column]
    return body


def entity(table: str, row: dict[str, Any], **children: Child) -> dict[str, Any]:
    """One entity's file body: its model columns, plus each child collection sorted by its address.

    ``children`` maps the name the collection takes in the file to its :class:`Child`. Child rows are
    stripped the same way the parent is, and the surrogate FK that joined them is dropped as well —
    the NESTING is what records the relationship now, and the integer it replaced differs between two
    environments holding the same model.
    """
    body = _model_columns(table, row)
    for field, child in children.items():
        if not child.rows:
            continue
        dropped = {child.parent_key} if isinstance(child.parent_key, str) else set(child.parent_key)
        body[field] = [
            _child_columns(child, table, c, dropped)
            for c in _sorted_children(child.rows, child.sort_key)
        ]
    return body
