# Copyright (c) 2026 Kenneth Stott
# Canary: 095f3c21-cc49-4ddb-bb8e-07a0b655d5cb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a Google Workspace source is set up with (REQ-1923).

One source reads the resources its operator ticks -- mail, calendar, tasks -- of the accounts it
names, and asks Google only for the scope of a ticked resource. It signs in one of two ways,
both with a Google Cloud client the operator owns: as a person who approved the client (a
refresh token, one account), or as a service account a Workspace domain has delegated to (the
same key reads as each account named).

The settings name a list of accounts and every table leads with the account it was read from,
so a source of many accounts is this one with a longer list. Today the list holds one.

A setting this module does not read is refused by name, as is a combination Google refuses.
"""

# Requirements: REQ-1923
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

from provisa.core.auth_models import ApiAuthGoogleServiceAccount, ApiAuthOAuth2RefreshToken

if TYPE_CHECKING:
    from provisa.core.mail_platforms import Configured


#: Google's token endpoint, as its OpenID configuration document publishes it.
TOKEN_URL = "https://oauth2.googleapis.com/token"

MAIL, CALENDAR, TASKS = "mail", "calendar", "tasks"
#: The resources Google's APIs offer under one credential, in the order the setup shows them.
RESOURCES: tuple[str, ...] = (MAIL, CALENDAR, TASKS)
#: The resources this build reads. Another is refused by name until it is built.
RESOURCES_READ: frozenset[str] = frozenset({MAIL})

#: How much of a message is read. Headers and labels only is Google's gmail.metadata scope:
#: no body, and Google refuses a search under it.
MAIL_FULL, MAIL_HEADERS = "full", "headers"

_SCOPE = "https://www.googleapis.com/auth/"
MAIL_SCOPES: dict[str, str] = {
    MAIL_FULL: f"{_SCOPE}gmail.readonly",
    MAIL_HEADERS: f"{_SCOPE}gmail.metadata",
}
RESOURCE_SCOPES: dict[str, tuple[str, ...]] = {
    CALENDAR: (f"{_SCOPE}calendar.calendarlist.readonly", f"{_SCOPE}calendar.events.readonly"),
    TASKS: (f"{_SCOPE}tasks.readonly",),
}

#: Signing in as a person who approved the operator's client, or as a service account a
#: Workspace domain has delegated to.
GOOGLE_ACCOUNT, SERVICE_ACCOUNT = "google_account", "service_account"

#: The settings that are credentials: kept in the org's vault, the source holding a reference.
SECRET_KEYS: tuple[str, ...] = ("refresh_token", "service_account_key")

#: What each sign-in keeps in the source. A person's approval is of the ORGANISATION's client
#: (``core.mail_platforms``), which the source does not hold: it keeps the approval alone.
_SIGN_IN_KEYS: dict[str, tuple[str, ...]] = {
    GOOGLE_ACCOUNT: ("refresh_token",),
    SERVICE_ACCOUNT: ("service_account_key",),
}
_MAIL_KEYS = (
    "mail_content",
    "mail_search",
    "mail_labels",
    "mail_since",
    "mail_include_spam_trash",
)
_KEYS = frozenset(
    (
        "accounts",
        "resources",
        "sign_in",
        *_MAIL_KEYS,
        *(k for ks in _SIGN_IN_KEYS.values() for k in ks),
    )
)


class InvalidGoogleWorkspaceSource(ValueError):
    """A Google Workspace source's settings that cannot be a source, said in the setup's terms."""


@dataclass(frozen=True)
class MailSettings:
    """Which mail a source holds. Each narrows what Google is asked for; none is a filter a
    statement adds."""

    content: str  # MAIL_FULL | MAIL_HEADERS
    search: str | None  # as typed in Gmail's own search box
    labels: tuple[str, ...]  # by name; a message carries every one
    since: dt.date | None  # nothing received before it
    include_spam_trash: bool


@dataclass(frozen=True)
class Settings:
    accounts: tuple[str, ...]
    resources: tuple[str, ...]
    sign_in: str
    credential: dict[str, str]  # the sign-in's own settings, secrets as references
    mail: MailSettings | None  # None when mail is not ticked

    def scopes(self) -> list[str]:
        """What Google is asked for: the scope of each ticked resource and of no other."""
        scopes: list[str] = []
        for resource in self.resources:
            if resource == MAIL:
                assert self.mail is not None  # parse: mail ticked states its content
                scopes.append(MAIL_SCOPES[self.mail.content])
            else:
                scopes.extend(RESOURCE_SCOPES[resource])
        return scopes

    def auth(
        self, account: str, client: "Configured | None" = None
    ) -> ApiAuthOAuth2RefreshToken | ApiAuthGoogleServiceAccount:
        """The credential ``account`` is read with. ``client`` is the organisation's Google
        client, which a person's approval was given to and is renewed with."""
        if account not in self.accounts:
            raise InvalidGoogleWorkspaceSource(f"{account} is not an account of this source")
        if self.sign_in == GOOGLE_ACCOUNT:
            if client is None:
                raise InvalidGoogleWorkspaceSource(
                    "A source signed in by its owner's approval is read with the "
                    "organisation's Google client, and none was given"
                )
            return ApiAuthOAuth2RefreshToken(
                client_id=client.client_id,
                client_secret=client.client_secret,
                refresh_token=self.credential["refresh_token"],
                token_url=TOKEN_URL,
            )
        return ApiAuthGoogleServiceAccount(
            key=self.credential["service_account_key"], subject=account, scopes=self.scopes()
        )


def _text(mapping: dict, key: str) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidGoogleWorkspaceSource(f"{key} must be text")
    return value.strip() or None


def _names(mapping: dict, key: str) -> tuple[str, ...]:
    value = mapping.get(key, [])
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise InvalidGoogleWorkspaceSource(f"{key} must be a list of text")
    names = tuple(v.strip() for v in value if v.strip())
    repeated = sorted({n for n in names if names.count(n) > 1})
    if repeated:
        raise InvalidGoogleWorkspaceSource(f"{key} names {', '.join(repeated)} more than once")
    return names


def _accounts(mapping: dict) -> tuple[str, ...]:
    accounts = _names(mapping, "accounts")
    if not accounts:
        raise InvalidGoogleWorkspaceSource("Name the account to read")
    for account in accounts:
        name, at, domain = account.partition("@")
        if not (name and at and "." in domain) or " " in account or "@" in domain:
            raise InvalidGoogleWorkspaceSource(f"{account!r} is not an email address")
    if len(accounts) > 1:
        raise InvalidGoogleWorkspaceSource(
            f"A source reads one account; {len(accounts)} were named ({', '.join(accounts)})"
        )
    return accounts


def _resources(mapping: dict) -> tuple[str, ...]:
    ticked = _names(mapping, "resources")
    if not ticked:
        raise InvalidGoogleWorkspaceSource("Choose what the source reads: " + ", ".join(RESOURCES))
    unknown = [r for r in ticked if r not in RESOURCES]
    if unknown:
        raise InvalidGoogleWorkspaceSource(
            f"{', '.join(unknown)} is not something a Google Workspace source reads "
            f"({', '.join(RESOURCES)})"
        )
    unread = [r for r in ticked if r not in RESOURCES_READ]
    if unread:
        raise InvalidGoogleWorkspaceSource(f"{', '.join(unread)} is not read by this version")
    return tuple(r for r in RESOURCES if r in ticked)


def _credential(mapping: dict) -> tuple[str, dict[str, str]]:
    sign_in = _text(mapping, "sign_in")
    if sign_in not in _SIGN_IN_KEYS:
        raise InvalidGoogleWorkspaceSource(
            f"sign_in must be one of {', '.join(_SIGN_IN_KEYS)}, not {sign_in!r}"
        )
    credential: dict[str, str] = {}
    for key in _SIGN_IN_KEYS[sign_in]:
        value = _text(mapping, key)
        if value is None:
            raise InvalidGoogleWorkspaceSource(f"{sign_in} sign-in needs {key}")
        credential[key] = value
    other = [
        key
        for kind, keys in _SIGN_IN_KEYS.items()
        if kind != sign_in
        for key in keys
        if mapping.get(key)
    ]
    if other:
        raise InvalidGoogleWorkspaceSource(f"{sign_in} sign-in does not take {', '.join(other)}")
    return sign_in, credential


def _mail(mapping: dict) -> MailSettings:
    content = _text(mapping, "mail_content")
    if content not in MAIL_SCOPES:
        raise InvalidGoogleWorkspaceSource(
            f"mail_content must be one of {', '.join(MAIL_SCOPES)}, not {content!r}"
        )
    search = _text(mapping, "mail_search")
    since_text = _text(mapping, "mail_since")
    since: dt.date | None = None
    if since_text is not None:
        try:
            since = dt.date.fromisoformat(since_text)
        except ValueError:
            raise InvalidGoogleWorkspaceSource(
                f"mail_since must be a date as YYYY-MM-DD, not {since_text!r}"
            ) from None
    # Google: a search "cannot be used when accessing the api using the gmail.metadata scope",
    # and a date is asked for as a search.
    if content == MAIL_HEADERS and (search is not None or since is not None):
        raise InvalidGoogleWorkspaceSource(
            "Google does not search mail read as headers and labels only: leave out "
            "mail_search and mail_since, or read whole messages"
        )
    spam_trash = mapping.get("mail_include_spam_trash", False)  # Google's own default
    if not isinstance(spam_trash, bool):
        raise InvalidGoogleWorkspaceSource("mail_include_spam_trash must be true or false")
    return MailSettings(content, search, _names(mapping, "mail_labels"), since, spam_trash)


def parse(mapping: dict) -> Settings:
    """A Google Workspace source's settings, as its mapping states them."""
    unread = sorted(set(mapping) - _KEYS)
    if unread:
        raise InvalidGoogleWorkspaceSource(
            f"{', '.join(unread)} is not a setting of a Google Workspace source"
        )
    resources = _resources(mapping)
    sign_in, credential = _credential(mapping)
    accounts = _accounts(mapping)
    if MAIL in resources:
        mail = _mail(mapping)
    else:
        stated = [key for key in _MAIL_KEYS if key in mapping]
        if stated:
            raise InvalidGoogleWorkspaceSource(
                f"{', '.join(stated)} is a setting of mail, which this source does not read"
            )
        mail = None
    return Settings(accounts, resources, sign_in, credential, mail)
