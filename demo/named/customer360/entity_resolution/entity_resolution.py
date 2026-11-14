# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""entity_resolution: the customer360 demo's code source for entity resolution (REQ-2034).

ENTITY_SOURCES, at the top, is the one place a table that refers to an organization or a person
is declared, as govdata's OrgSource / PersonSource registry is (EntityBridgeListener). Adding a
source is an entry here, not new match logic: the matching job (er_job.py, beside this module)
reads every entry's rows and matches each by its entity type's algorithm, and nothing else names
Salesforce, ServiceNow, Jira or Stripe.

Each entry names:

  entity_type   organization or person: which matching algorithm reads it
  source, table the registered Provisa source and table it is (its source id and table name)
  key           the column that identifies one mention, and its type: a mention is one distinct
                value of it, and the wide tables' typed key column for the entry is this one
  authority     True on exactly one entry per entity type: the system whose records are the
                golden records, every other mention matched to one of them
  reference     the columns the algorithm reads, by role: an organization's name, domain (a
                website) and email (the domain after its @); a person's first_name and last_name,
                or name (first word and last word), and email
  match_text    the string the fuzzy tier compares, a template over the row's columns
                (normalised after it is formatted)
  label         how a steward sees the mention, a template over the row's columns
  organization  a person's: the organization entry whose key one of the row's columns holds
                (OrgRef(source, table, column)), the org context the person is blocked in
  order_by      an authority's: the column that says which of its records sharing an identifier
                is the anchor, the earliest first (a tie goes to the lower key)

A template is data, not code: it is applied with str.format_map over the row's own columns, each
field a bare column name. A template naming a column the table does not have is refused, and so is
one that asks for an attribute, an index, a conversion or a format of a field. Nothing is eval'd.

In production the registry is a file of its own, read by the job and the code source alike; the
demo keeps it here, at the top of the module, so the two cannot disagree.
"""

# Requirements: REQ-2034

from __future__ import annotations

import string
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

ORGANIZATION = "organization"
PERSON = "person"
ENTITY_TYPES = (ORGANIZATION, PERSON)

#: The roles each entity type's algorithm reads a column in, and the ones it needs.
ROLES = {
    ORGANIZATION: ("name", "domain", "email"),
    PERSON: ("first_name", "last_name", "name", "email"),
}


@dataclass(frozen=True)
class Key:
    """The column that identifies a mention, and its Provisa data type."""

    column: str
    type: str


@dataclass(frozen=True)
class OrgRef:
    """A person's organization context: the organization entry (source, table) whose key the
    person's row holds in ``column``."""

    source: str
    table: str
    column: str


@dataclass(frozen=True)
class EntitySource:
    """One table that refers to an organization or a person."""

    entity_type: str
    source: str
    table: str
    key: Key
    reference: Mapping[str, str]
    match_text: str | None = None
    label: str | None = None
    authority: bool = False
    organization: OrgRef | None = None
    order_by: str | None = None

    def __post_init__(self) -> None:
        if self.entity_type not in ENTITY_TYPES:
            raise RegistryRefused(
                self, f"entity_type {self.entity_type!r} is not one of {ENTITY_TYPES}"
            )
        unknown = sorted(set(self.reference) - set(ROLES[self.entity_type]))
        if unknown:
            raise RegistryRefused(
                self, f"reference roles {unknown} are not {self.entity_type} roles"
            )
        if self.entity_type == ORGANIZATION:
            if "name" not in self.reference:
                raise RegistryRefused(self, "an organization names its name column")
            if self.organization is not None:
                raise RegistryRefused(self, "an organization has no organization context")
        else:
            has_parts = {"first_name", "last_name"} <= set(self.reference)
            if has_parts == ("name" in self.reference):
                raise RegistryRefused(
                    self, "a person names first_name and last_name, or name, and not both"
                )

    @property
    def identity(self) -> str:
        """The entry's name: ``source.table``."""
        return f"{self.source}.{self.table}"

    @property
    def match_template(self) -> str:
        """The match_text template, or the reference's own name columns when it gives none."""
        if self.match_text is not None:
            return self.match_text
        if "name" in self.reference:
            return "{" + self.reference["name"] + "}"
        return "{" + self.reference["first_name"] + "} {" + self.reference["last_name"] + "}"

    @property
    def label_template(self) -> str:
        """The label template, or the match_text template when it gives none."""
        return self.label if self.label is not None else self.match_template


class RegistryRefused(ValueError):
    """A registry entry, or a template of one, that cannot be used: said naming the entry."""

    def __init__(self, entry: EntitySource | str, reason: str) -> None:
        name = (
            entry if isinstance(entry, str) else f"{entry.entity_type} {entry.source}.{entry.table}"
        )
        super().__init__(f"entity source {name}: {reason}")


# --------------------------------------------------------------------------------------------- #
# The registry.                                                                                 #
# --------------------------------------------------------------------------------------------- #

ENTITY_SOURCES = [
    # Organizations. Salesforce's accounts are the golden records.
    EntitySource(
        entity_type=ORGANIZATION,
        source="salesforce",
        table="Account",
        key=Key("Id", "varchar"),
        authority=True,
        reference={"name": "Name", "domain": "Website"},
        match_text="{Name}",
        label="{Name} ({Website})",
        order_by="CreatedDate",
    ),
    EntitySource(
        entity_type=ORGANIZATION,
        source="servicenow",
        table="core_company",
        key=Key("sys_id", "varchar"),
        reference={"name": "name", "domain": "website"},
        label="{name}",
    ),
    EntitySource(
        entity_type=ORGANIZATION,
        source="jira",
        table="bug",
        key=Key("customer", "varchar"),
        reference={"name": "customer"},
    ),
    EntitySource(
        entity_type=ORGANIZATION,
        source="stripe",
        table="get_customers",
        key=Key("id", "varchar"),
        reference={"name": "name", "email": "email"},
        label="{name} <{email}>",
    ),
    # People. Salesforce's contacts are the golden records.
    EntitySource(
        entity_type=PERSON,
        source="salesforce",
        table="Contact",
        key=Key("Id", "varchar"),
        authority=True,
        reference={"first_name": "FirstName", "last_name": "LastName", "email": "Email"},
        match_text="{FirstName} {LastName}",
        label="{FirstName} {LastName} <{Email}>",
        organization=OrgRef("salesforce", "Account", "AccountId"),
    ),
    EntitySource(
        entity_type=PERSON,
        source="servicenow",
        table="sys_user",
        key=Key("sys_id", "varchar"),
        reference={"first_name": "first_name", "last_name": "last_name", "email": "email"},
        label="{first_name} {last_name} <{email}>",
        organization=OrgRef("servicenow", "core_company", "company"),
    ),
    EntitySource(
        entity_type=PERSON,
        source="jira",
        table="bug",
        key=Key("reporter_email", "varchar"),
        reference={"name": "reporter_name", "email": "reporter_email"},
        label="{reporter_name} <{reporter_email}>",
        organization=OrgRef("jira", "bug", "customer"),
    ),
    EntitySource(
        entity_type=PERSON,
        source="stripe",
        table="get_customers",
        key=Key("id", "varchar"),
        reference={"name": "billing_contact_name", "email": "email"},
        label="{billing_contact_name} <{email}>",
        organization=OrgRef("stripe", "get_customers", "id"),
    ),
]


# --------------------------------------------------------------------------------------------- #
# Reading the registry.                                                                         #
# --------------------------------------------------------------------------------------------- #


def entries(entity_type: str, registry: Iterable[EntitySource] = ()) -> list[EntitySource]:
    """The entries of one entity type, in the registry's order."""
    return [e for e in (registry or ENTITY_SOURCES) if e.entity_type == entity_type]


def authority(entity_type: str, registry: Iterable[EntitySource] = ()) -> EntitySource:
    """The one entry of ``entity_type`` marked authority; refused unless there is exactly one."""
    found = [e for e in entries(entity_type, registry) if e.authority]
    if len(found) != 1:
        raise RegistryRefused(entity_type, f"needs exactly one authority entry, has {len(found)}")
    return found[0]


def check_registry(registry: Iterable[EntitySource]) -> None:
    """Refuse a registry that cannot be matched: an entity type without exactly one authority, an
    entry declared twice, or a person whose organization context names no organization entry."""
    registry = list(registry)
    for entity_type in ENTITY_TYPES:
        authority(entity_type, registry)
    seen: set[tuple[str, str]] = set()
    for e in registry:
        if (e.entity_type, e.identity) in seen:
            raise RegistryRefused(e, "is declared twice")
        seen.add((e.entity_type, e.identity))
    orgs = {e.identity for e in entries(ORGANIZATION, registry)}
    for e in entries(PERSON, registry):
        if (
            e.organization is not None
            and f"{e.organization.source}.{e.organization.table}" not in orgs
        ):
            raise RegistryRefused(e, f"organization {e.organization} is no organization entry")


def template_fields(template: str, entry: EntitySource | str = "template") -> list[str]:
    """The column names a template formats, in order; a field that is not a bare column name (an
    attribute, an index, a conversion, a format spec, or a positional field) is refused."""
    names: list[str] = []
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError as bad:
        raise RegistryRefused(
            entry, f"template {template!r} is not a format string: {bad}"
        ) from bad
    for _, name, spec, conversion in parsed:
        if name is None:
            continue
        if not name or name.isdigit() or not _is_column_name(name):
            raise RegistryRefused(entry, f"template {template!r}: {{{name}}} is not a column name")
        if spec or conversion:
            raise RegistryRefused(
                entry, f"template {template!r}: {{{name}}} may not carry a conversion or a format"
            )
        names.append(name)
    return names


def _is_column_name(name: str) -> bool:
    """A column name a template may name: no attribute or index access, which format would run."""
    return not any(ch in name for ch in ".[]!:{}")


def check_columns(entry: EntitySource, columns: Iterable[str]) -> None:
    """Refuse ``entry`` when its key, a reference column or a template field names a column its
    table does not have. Checked before any row is read."""
    have = set(columns)
    wanted = [
        ("key", entry.key.column),
        *((f"reference {role}", column) for role, column in entry.reference.items()),
        *(("match_text", c) for c in template_fields(entry.match_template, entry)),
        *(("label", c) for c in template_fields(entry.label_template, entry)),
    ]
    if entry.organization is not None:
        wanted.append(("organization", entry.organization.column))
    if entry.order_by is not None:
        wanted.append(("order_by", entry.order_by))
    missing = [f"{what} {column!r}" for what, column in wanted if column not in have]
    if missing:
        raise RegistryRefused(entry, f"names columns its table does not have: {', '.join(missing)}")


def render(template: str, row: Mapping[str, str | int | float | bool | None]) -> str:
    """``template`` formatted over the row's own columns, an empty column as nothing. Only the
    row's columns are visible to it."""
    values = {k: "" if v is None else str(v) for k, v in row.items()}
    return template.format_map(_Columns(values)).strip()


class _Columns(dict):
    """The row's columns as format_map sees them: a name the row lacks is refused, not guessed."""

    def __missing__(self, key: str) -> str:
        raise RegistryRefused("template", f"names column {key!r}, which the row does not have")
