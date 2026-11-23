# Copyright (c) 2026 Kenneth Stott
# Canary: 45fe9bed-c94b-48a7-a030-21aee36b1d7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a source's bundled pgwire server serves, for a type whose server serves only the
tables registered from the source (REQ-1954). Read by the server's model (``pgwire_replica``,
``servicenow``) and the Trino catalog; set from the model on every schema build
(``api/app_rebuild.serve_registered_tables``)."""

# Requirements: REQ-1954

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

#: Source id -> the tables registered from it that its server serves, for a type whose server
#: serves only those (``SERVES_REGISTERED_TABLES``). Set from the model on every schema build.
_SERVED_TABLES: dict[str, tuple[str, ...]] = {}
#: Source id -> its registered tables bound to an import set: table -> the import set (staging
#: table) its inserts go through.
_IMPORT_SETS: dict[str, dict[str, str]] = {}
#: Source id -> the forms its registered tables are (REQ-2069, a Jira source): each the table's
#: name, its project and issue type, and the fields its columns read.
_FORMS: dict[str, tuple[dict[str, Any], ...]] = {}
#: Types whose server is given the tables registered from the source and serves only those
#: (REQ-1954): a ServiceNow server resolves every table it serves before it listens, and an
#: instance has thousands.
SERVES_REGISTERED_TABLES = frozenset({"servicenow", "jira"})


def served_tables(source_id: str) -> tuple[str, ...]:
    """The registered tables ``source_id``'s server serves, by name, in order."""
    return _SERVED_TABLES.get(source_id, ())


def served_import_sets(source_id: str) -> dict[str, str]:
    """The registered tables of ``source_id`` bound to an import set: table -> import set."""
    return dict(_IMPORT_SETS.get(source_id, {}))


def served_forms(source_id: str) -> tuple[dict[str, Any], ...]:
    """The forms ``source_id``'s registered tables are (REQ-2069), in table-name order."""
    return _FORMS.get(source_id, ())


def serve_tables(
    source_id: str,
    names: Iterable[str],
    import_sets: Mapping[str, str] | None = None,
    forms: Iterable[dict[str, Any]] = (),
) -> bool:
    """Record the tables registered from ``source_id``, the import sets those bound to one go
    through and the forms they are; True when any differs from what its server was last given
    (nothing, before the first record), so a server already running must be started again to
    serve them."""
    wanted = tuple(sorted(set(names)))
    bound = dict(import_sets or {})
    shaped = tuple(sorted(forms, key=lambda f: f["name"]))
    before = (
        _SERVED_TABLES.get(source_id, ()),
        _IMPORT_SETS.get(source_id, {}),
        _FORMS.get(source_id, ()),
    )
    _SERVED_TABLES[source_id] = wanted
    _IMPORT_SETS[source_id] = bound
    _FORMS[source_id] = shaped
    return before != (wanted, bound, shaped)


def file_glob_specs(tables: list[Any]) -> dict[str, list[dict]]:  # REQ-788
    """{source_id: [the file adapter's glob-table spec, ...]} for every registered table that
    declares a ``file_glob`` -- a config Table or a registered-table row, one spec per table."""
    by_source: dict[str, list[dict]] = {}
    for table in tables:
        row = table if isinstance(table, dict) else vars(table)
        if not row.get("file_glob"):
            continue
        specs = by_source.setdefault(row["source_id"], [])
        if all(spec["name"] != row["table_name"] for spec in specs):
            specs.append(
                {
                    "name": row["table_name"],
                    "file_glob": row["file_glob"],
                    "source_file_column": row.get("source_file_column"),
                }
            )
    return by_source


# REQ-788: a files source's glob tables -- each merged by its server's file adapter from every file
# its glob matches -- are in that server's model, built when it starts. A glob table registered or
# changed after the server started is not in it until it is started again. Keyed by the source's
# org and id (``source_key``): two orgs may each hold a source of the same id.
_GLOB_TABLES: dict[tuple[str, str], tuple[tuple[str, str, str | None], ...]] = {}


def serve_glob_tables(source_id: str, specs: Iterable[Mapping[str, Any]]) -> bool:
    """Record the glob tables registered from the bound org's files source ``source_id`` (each a
    ``{"name", "file_glob", "source_file_column"}`` spec); True when they differ from what its
    server was last given (nothing, before the first record), so a server already running must be
    started again to serve them."""
    from provisa.core.org_scope import source_key

    key = source_key(source_id)
    wanted = tuple(sorted((s["name"], s["file_glob"], s.get("source_file_column")) for s in specs))
    before = _GLOB_TABLES.get(key, ())
    _GLOB_TABLES[key] = wanted
    return before != wanted


def served_glob_tables(source_id: str) -> list[dict]:
    """The glob tables the bound org's files source ``source_id``'s server is started with: those
    the last schema build recorded as registered from it (``serve_glob_tables``). Every server
    start reads them here, whoever asks for the server -- a discovery call, a replica build, the
    engine's attach -- so the model a server is built with never depends on which caller started
    it. None recorded is none registered: a source's glob tables are registered through the
    model, and each registration is a schema build, which records them and starts a server
    already running again (``api/app_rebuild._serve_glob_tables``)."""
    from provisa.core.org_scope import source_key

    return [
        {"name": name, "file_glob": glob, "source_file_column": column}
        for name, glob, column in _GLOB_TABLES.get(source_key(source_id), ())
    ]
