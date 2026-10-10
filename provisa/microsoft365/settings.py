# Copyright (c) 2026 Kenneth Stott
# Canary: 34f35188-6109-433a-986c-10cd13df94c2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a Microsoft 365 source is set up with (REQ-1923): its mapping, read here and nowhere
else -- when the source is saved, when a person connects it, and when its mail is fetched.

A source reads one mailbox with its owner's approval of the organisation's Microsoft client
(``provisa.core.mail_platforms``). It keeps the mailbox's address, what it reads, and the
reference that names the refresh token the approval produced in the organisation's vault.
"""

# Requirements: REQ-1923
from __future__ import annotations

import re
from dataclasses import dataclass

MAIL = "mail"
#: What a source can read. Calendar and tasks are named by the product and not yet read.
RESOURCES: tuple[str, ...] = (MAIL,)

LOGIN = "https://login.microsoftonline.com"
_GRAPH = "https://graph.microsoft.com/"
#: What Microsoft is asked for, by what the source reads. Read-only.
RESOURCE_SCOPES: dict[str, str] = {MAIL: f"{_GRAPH}Mail.Read"}
#: Asked for by every source: a refresh token, and the name of the account that approved.
BASE_SCOPES: tuple[str, ...] = ("offline_access", f"{_GRAPH}User.Read")

#: The mapping keys that hold a credential, kept as references into the vault.
SECRET_KEYS: tuple[str, ...] = ("refresh_token",)
_KEYS = frozenset({"accounts", "resources", "refresh_token", "mailboxes"})
#: What the organisation's own credential is asked for (the client-credentials grant): every
#: permission an administrator consented to for the application.
ORGANISATION_SCOPE = f"{_GRAPH}.default"
EVERYONE, GROUP, LIST = "everyone", "group", "list"
_ADDRESS = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
_SECRET_REFERENCE = re.compile(r"\$\{secret:([^}#]+)\}")
# A directory is named by its id (a GUID) or by one of its domains.
_TENANT = re.compile(
    r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
    r"|[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)+"
)


class InvalidMicrosoft365Source(ValueError):
    """The settings cannot be a Microsoft 365 source; the message says which and why."""


@dataclass(frozen=True)
class Mailboxes:
    """Which mailboxes a source of the organisation's reads: everyone in the directory, the
    members of a group (named by its id or its address), or the addresses listed."""

    kind: str  # EVERYONE | GROUP | LIST
    group: str | None = None
    addresses: tuple[str, ...] = ()


@dataclass(frozen=True)
class Settings:
    """A source is one of two shapes: one mailbox its owner connected (``account`` and
    ``refresh_token``), or the organisation's mailboxes read with the organisation's own
    credential (``mailboxes``)."""

    resources: tuple[str, ...]
    account: str | None = None  # the mailbox's address
    refresh_token: str | None = None  # a ${secret:NAME} reference
    mailboxes: Mailboxes | None = None

    @property
    def organisation(self) -> bool:
        return self.mailboxes is not None

    @property
    def refresh_token_name(self) -> str:
        """The name the refresh token has in the organisation's vault."""
        return _SECRET_REFERENCE.fullmatch(self.refresh_token or "").group(1)  # type: ignore[union-attr]  # checked by parse

    def scopes(self) -> list[str]:
        return scopes(self.resources)


def scopes(resources: tuple[str, ...] | list[str]) -> list[str]:
    """What Microsoft is asked for to read ``resources``, and nothing else."""
    return [*(RESOURCE_SCOPES[r] for r in resources), *BASE_SCOPES]


def tenant_path(tenant: str | None) -> str:
    """``tenant`` as it goes into Microsoft's sign-in addresses, or a refusal: only a directory
    id or domain is put into an address."""
    stated = (tenant or "").strip()
    if not _TENANT.fullmatch(stated):
        raise InvalidMicrosoft365Source(
            "the organisation's Microsoft 365 setting names no directory (tenant): it is the "
            "directory's ID or its domain"
        )
    return stated


def authorization_url(tenant: str | None) -> str:
    return f"{LOGIN}/{tenant_path(tenant)}/oauth2/v2.0/authorize"


def token_url(tenant: str | None) -> str:
    return f"{LOGIN}/{tenant_path(tenant)}/oauth2/v2.0/token"


def _resources(mapping: dict) -> tuple[str, ...]:
    resources = mapping.get("resources")
    if not isinstance(resources, list) or not resources:
        raise InvalidMicrosoft365Source("resources must say what is read: mail")
    unread = [r for r in resources if r not in RESOURCES]
    if unread:
        raise InvalidMicrosoft365Source(
            f"cannot read {', '.join(map(str, unread))}; a source reads {', '.join(RESOURCES)}"
        )
    return tuple(resources)


def _address(value: object) -> str:
    address = value.strip() if isinstance(value, str) else ""
    if not _ADDRESS.fullmatch(address):
        raise InvalidMicrosoft365Source(f"{value!r} is not a mailbox address")
    return address


def _mailboxes(stated: object) -> Mailboxes:
    """Which of the organisation's mailboxes: exactly one of everyone, a group, a list."""
    if not isinstance(stated, dict) or len(stated) != 1:
        raise InvalidMicrosoft365Source(
            "mailboxes must say one of: everyone, a group, or a list of addresses"
        )
    ((kind, value),) = stated.items()
    if kind == EVERYONE:
        if value is not True:
            raise InvalidMicrosoft365Source("mailboxes.everyone is stated as true or left out")
        return Mailboxes(EVERYONE)
    if kind == GROUP:
        if not isinstance(value, str) or not value.strip() or any(c in value for c in "'/\\ "):
            raise InvalidMicrosoft365Source(
                "mailboxes.group names one group, by its ID or its address"
            )
        return Mailboxes(GROUP, group=value.strip())
    if kind == LIST:
        if not isinstance(value, list) or not value:
            raise InvalidMicrosoft365Source("mailboxes.list names at least one address")
        addresses = tuple(dict.fromkeys(_address(a).lower() for a in value))
        return Mailboxes(LIST, addresses=addresses)
    raise InvalidMicrosoft365Source(
        f"mailboxes.{kind} is not a way to choose mailboxes: everyone, group or list"
    )


def parse(mapping: dict) -> Settings:
    """The settings ``mapping`` states, or a refusal naming what is wrong with it."""
    unknown = sorted(set(mapping) - _KEYS)
    if unknown:
        raise InvalidMicrosoft365Source(f"unknown setting(s): {', '.join(unknown)}")
    if "mailboxes" in mapping:
        own = sorted({"accounts", "refresh_token"} & set(mapping))
        if own:
            raise InvalidMicrosoft365Source(
                f"a source of the organisation's mailboxes states no {' or '.join(own)}: "
                "nobody connects it"
            )
        return Settings(_resources(mapping), mailboxes=_mailboxes(mapping["mailboxes"]))
    accounts = mapping.get("accounts")
    if not isinstance(accounts, list) or len(accounts) != 1 or not isinstance(accounts[0], str):
        raise InvalidMicrosoft365Source("accounts must name one mailbox, by its address")
    account = _address(accounts[0])
    resources = _resources(mapping)
    token = mapping.get("refresh_token")
    if not isinstance(token, str) or not token.strip():
        raise InvalidMicrosoft365Source(
            "the mailbox is not connected: its owner has not approved the sign-in"
        )
    if not _SECRET_REFERENCE.fullmatch(token.strip()):
        raise InvalidMicrosoft365Source(
            "refresh_token must be a reference to the organisation's vault (${secret:NAME})"
        )
    return Settings(resources, account=account, refresh_token=token.strip())
