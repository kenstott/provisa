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
