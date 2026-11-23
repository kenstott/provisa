# Copyright (c) 2026 Kenneth Stott
# Canary: 8da2b732-253f-4f6e-8082-7af7080aa4bd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's entity-resolution code source: its entry module (REQ-2034).

Two ``@query`` functions do the matching inside Provisa, so the rerun can be triggered from
Provisa: ``match_organizations`` answers the rows of er.entity_org_bridge and ``match_people``
those of er.entity_person_bridge, for a new match_run_id. Each is given every registry table as a
``TableRef``, which it reads through Provisa as the calling role, and runs the same job
(er_job.run) the shell does over the rows the demo declares. Neither writes anything: two
registered statement commands, one per bridge, each MERGE the bridge from one of them, replacing
its rows in one statement (config.yaml, ``rerun_org_bridge`` and ``rerun_person_bridge``).

The tables a function is given are the registry's own (entity_resolution.ENTITY_SOURCES), one
parameter per table, named as the demo registers it, and each parameter's row type is the columns
the matching reads of that table (entity_resolution.columns_read). The demo's tests hold the three
to each other.

A table is read over Arrow Flight with the minted token its reference carries (``TableRef.read``),
through the host's own pyarrow, so the package has no dependencies to install.
"""

# Requirements: REQ-2034, REQ-2226

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from er_job import Bridges, run
from provisa_services import TableRef  # pyright: ignore[reportMissingImports] -- the host provides it

# --------------------------------------------------------------------------------------------- #
# The tables the matching reads: one row type per registry table, its fields the columns read.  #
# --------------------------------------------------------------------------------------------- #


@dataclass
class Account:
    CreatedDate: datetime | None
    Id: str | None
    Name: str | None
    Website: str | None


@dataclass
class Contact:
    AccountId: str | None
    Email: str | None
    FirstName: str | None
    Id: str | None
    LastName: str | None


@dataclass
class Company:
    name: str | None
    sys_id: str | None
    website: str | None


@dataclass
class ServiceDeskUser:
    company: str | None
    email: str | None
    first_name: str | None
    last_name: str | None
    sys_id: str | None


@dataclass
class Bug:
    customer: str | None
    reporter_email: str | None
    reporter_name: str | None


@dataclass
class StripeCustomer:
    billing_contact_name: str | None
    email: str | None
    id: str | None
    name: str | None


#: Each parameter of the two functions, and the registry table, (source, table), it is given.
TABLES = {
    "accounts": ("salesforce", "Account"),
    "contacts": ("salesforce", "Contact"),
    "companies": ("servicenow", "core_company"),
    "service_desk_users": ("servicenow", "sys_user"),
    "bugs": ("jira", "bug"),
    "stripe_customers": ("stripe", "get_customers"),
}

# --------------------------------------------------------------------------------------------- #
# The rows each function answers: a bridge's columns.                                           #
# --------------------------------------------------------------------------------------------- #


@dataclass
class OrgBridgeRow:
    source_schema: str
    source_table: str
    source_column: str
    source_key: str
    name_raw: str
    name_norm: str
    match_text: str
    label: str
    canonical_id: str
    golden_source: str | None
    match_method: str | None
    match_confidence: str
    match_score: float | None
    unresolved_reason: str | None
    candidate_id: str | None
    candidate_score: float | None
    match_run_id: str


@dataclass
class PersonBridgeRow:
    source_schema: str
    source_table: str
    source_column: str
    source_key: str
    name_raw: str
    name_norm: str
    match_text: str
    label: str
    canonical_id: str
    golden_source: str | None
    org_canonical_id: str | None
    match_method: str | None
    match_confidence: str
    match_score: float | None
    unresolved_reason: str | None
    candidate_id: str | None
    candidate_score: float | None
    match_run_id: str


# --------------------------------------------------------------------------------------------- #
# Reading a table through Provisa.                                                              #
# --------------------------------------------------------------------------------------------- #


def read_table(table: TableRef) -> list[dict[str, str | None]]:
    """Every row ``table``'s statement answers, each value as text or None, read as the calling
    role over Arrow Flight with the token minted for this call (REQ-2226)."""
    return [
        {name: None if value is None else str(value) for name, value in row.items()}
        for row in table.read().to_pylist()
    ]


# --------------------------------------------------------------------------------------------- #
# The matching.                                                                                 #
# --------------------------------------------------------------------------------------------- #


def _bridges(given: dict[str, TableRef]) -> Bridges:
    """One run of the job over the tables given, for a run id of the time it ran."""
    rows = {TABLES[name]: read_table(table) for name, table in given.items()}
    columns = {TABLES[name]: list(table.columns) for name, table in given.items()}
    run_id = "er-" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return run(rows, columns, run_id)


def match_organizations(
    accounts: TableRef[Account],
    contacts: TableRef[Contact],
    companies: TableRef[Company],
    service_desk_users: TableRef[ServiceDeskUser],
    bugs: TableRef[Bug],
    stripe_customers: TableRef[StripeCustomer],
) -> list[OrgBridgeRow]:
    """Each mention of an organization in the four systems, matched to its Salesforce account:
    the rows of er.entity_org_bridge for a new run.

    @query
    """
    given = {
        "accounts": accounts,
        "contacts": contacts,
        "companies": companies,
        "service_desk_users": service_desk_users,
        "bugs": bugs,
        "stripe_customers": stripe_customers,
    }
    return [OrgBridgeRow(**row) for row in _bridges(given).org]


def match_people(
    accounts: TableRef[Account],
    contacts: TableRef[Contact],
    companies: TableRef[Company],
    service_desk_users: TableRef[ServiceDeskUser],
    bugs: TableRef[Bug],
    stripe_customers: TableRef[StripeCustomer],
) -> list[PersonBridgeRow]:
    """Each mention of a person in the four systems, matched within its organization to its
    Salesforce contact: the rows of er.entity_person_bridge for a new run.

    @query
    """
    given = {
        "accounts": accounts,
        "contacts": contacts,
        "companies": companies,
        "service_desk_users": service_desk_users,
        "bugs": bugs,
        "stripe_customers": stripe_customers,
    }
    return [PersonBridgeRow(**row) for row in _bridges(given).person]
