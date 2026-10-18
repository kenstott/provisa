# Copyright (c) 2026 Kenneth Stott
# Canary: 8feead0f-16be-4372-bd3c-c7837255a6dd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Jira source's connection settings (REQ-2069), read by the pgwire server's model
(``pgwire_replica``).

Jira is read through its own Calcite adapter (module ``jira``). A lookup by key is fetched by key,
and a date or number comparison is sent to Jira as JQL; every other filter, and every sort, is
applied to the rows read. A multi-value field (labels, a multi-select, components) is text holding
a JSON array, read and written so.
A table of a Jira source is a FORM: one project and one issue type, and the fields its columns
read. The server is given the forms the registered tables are, and serves those alone beside its
lookups: the account's forms (``_jira_forms``), one form's fields (``_jira_fields``, read for one
project and issue type at a time) and the issue links (``issue_links``), each link once.

A form's table names its project and issue type in its ``arguments`` (REQ-2061). Each column reads
the field its ``path`` names (a field id such as ``customfield_10021``, or a field's name), or the
field of its own name where it names none; the fixed columns (``FIXED_COLUMNS``) are every form's
and read no chosen field."""

# Requirements: REQ-2069

from __future__ import annotations

from typing import Any

from provisa.core.secrets import resolve_secrets
from provisa.federation.connector_config import MissingConnectorConfig

SITE_URL_SCHEME = "https://"

#: Every project and issue type the account can create an issue of: ``(project_key, issue_type,
#: name)``. The Register Table list is read from it.
FORM_LIST = "_jira_forms"

#: One form's fields: ``(project_key, issue_type, field_id, field_name, data_type, required,
#: ordinal_position)``, read for one project and issue type at a time.
FIELD_LIST = "_jira_fields"

#: The issue links between the issues of the served forms: ``(link_id, link_type, outward_key,
#: inward_key)``, each link once. Inserted and deleted; a link is not updated.
LINKS_TABLE = "issue_links"

#: The columns every form has, in this order; ``key`` is its primary key.
FIXED_COLUMNS = ("key", "id", "summary", "status", "created", "updated")

#: The arguments a form's table fixes (REQ-2061): its project's key and its issue type.
FORM_ARGUMENTS = frozenset({"project", "issue_type"})


def jira_site_url(source: Any) -> str:
    """The site's address, from ``base_url`` or ``host`` with its secret reference resolved. It
    must be the full ``https://…`` URL; a missing one, or one without the scheme, is a config
    error naming the field. The scheme is never added for the steward."""
    who = f"jira source {source.id!r}"
    raw = source.base_url or source.host
    url = resolve_secrets(raw) if raw else ""
    if not url:
        raise MissingConnectorConfig(f"{who}: requires baseUrl (the site's address)")
    if not url.startswith(SITE_URL_SCHEME) or url == SITE_URL_SCHEME:
        raise MissingConnectorConfig(
            f"{who}: baseUrl must be the full {SITE_URL_SCHEME}… address "
            f"(for example https://acme.atlassian.net), got {url!r}"
        )
    return url.rstrip("/")


def form_of(table: Any) -> dict[str, Any]:
    """The form ``table`` (a registered table of a Jira source) is, as the adapter's ``forms``
    operand takes it: its name, project and issue type, and the field each chosen column reads.
    A table that does not fix its project and issue type is refused by name."""
    arguments = dict(getattr(table, "arguments", {}) or {})
    missing = sorted(FORM_ARGUMENTS - set(arguments))
    extra = sorted(set(arguments) - FORM_ARGUMENTS)
    if missing or extra:
        raise ValueError(
            f"jira table {table.table_name!r}: its arguments are its form's project and "
            f"issue_type"
            + (f"; it lacks {', '.join(missing)}" if missing else "")
            + (f"; {', '.join(extra)} is not one" if extra else "")
        )
    fields = [
        {"field": column.path or column.name, "column": column.name}
        for column in table.columns
        if column.name not in FIXED_COLUMNS
    ]
    return {
        "name": table.table_name,
        "project": str(arguments["project"]),
        "issueType": str(arguments["issue_type"]),
        "fields": fields,
    }


def jira_settings(source: Any) -> dict:
    """The adapter's settings for ``source``: its site, the account's email and API token, each
    resolved at use, and the forms its registered tables are (``served_tables``). A missing email
    or token is a config error naming it."""
    who = f"jira source {source.id!r}"
    email = resolve_secrets(source.username) if source.username else ""
    token = resolve_secrets(source.password) if source.password else ""
    if not email:
        raise MissingConnectorConfig(f"{who}: requires the account's email")
    if not token:
        raise MissingConnectorConfig(f"{who}: requires the API token")
    settings: dict[str, Any] = {"baseUrl": jira_site_url(source), "email": email, "apiToken": token}
    # Only the registered forms are served as tables; with none, the server serves its lookups
    # and the links alone.
    from provisa.federation.served_tables import served_forms

    forms = served_forms(source.id)
    if forms:
        settings["forms"] = [dict(f) for f in forms]
    return settings


#: The fixed columns a write may set: the issue's summary, and its status, which a write moves
#: through its workflow's transition.
WRITTEN_FIXED_COLUMNS = frozenset({"summary", "status"})

#: The fixed columns' PostgreSQL types, as the adapter serves them.
FIXED_COLUMN_TYPES = {
    "key": "character varying",
    "id": "character varying",
    "summary": "character varying",
    "status": "character varying",
    "created": "timestamp",
    "updated": "timestamp",
}


def column_name(field_name: str) -> str:
    """A field's column name: its name, snake-cased ("Story Points" -> ``story_points``)."""
    import re

    snake = re.sub(r"[^0-9a-zA-Z]+", "_", field_name).strip("_").lower()
    return snake if snake and not snake[0].isdigit() else f"f_{snake}"


def form_columns(
    fields: list[tuple[str, str, str, str, bool, bool]],
) -> list[tuple[str, str, str | None, bool]]:
    """``[(column, data_type, field_id, read_only), ...]`` of a form: the fixed columns (no field
    id; read only but for ``summary`` and ``status``), then one per field of ``fields``
    (``jira_fields``' rows) that is not one of them, named as the adapter names it. A field whose
    column is already a column's -- two custom fields named alike, common on a large site, or one
    named like a fixed column -- is named by its field id instead, so every field can be chosen
    and each column names one field."""
    columns: list[tuple[str, str, str | None, bool]] = [
        (name, data_type, None, name not in WRITTEN_FIXED_COLUMNS)
        for name, data_type in FIXED_COLUMN_TYPES.items()
    ]
    taken = set(FIXED_COLUMN_TYPES)
    for field_id, field_name, column, data_type, _required, read_only in fields:
        if field_id in FIXED_COLUMN_TYPES:
            continue  # a fixed column's own field
        name = column or column_name(field_name)
        if name in taken:
            name = column_name(field_id)
        taken.add(name)
        columns.append((name, data_type, field_id, read_only))
    return columns
