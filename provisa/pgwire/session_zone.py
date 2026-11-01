# Copyright (c) 2026 Kenneth Stott
# Canary: 7c31e0a4-58d2-4b19-a6f7-c24e9d0b1f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The TimeZone a session is on (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED). No engine is told it,
so what a form writes in UTC is right only for a session on UTC: a statement that calls one of those
forms from any other zone is refused by name.

A zone comes from the startup packet, SET, SET LOCAL or set_config, and SET is undone by ROLLBACK
as in PostgreSQL. Where the zone cannot be told it is ``UNKNOWN_ZONE``, which is not UTC."""

# Requirements: REQ-2171

from __future__ import annotations

import re

from provisa.core.pg_error import PostgresError
from provisa.transpiler.pg_forms_type_io import zoned_call
from provisa.transpiler.pg_forms_types import REFUSED

#: A zone no statement of this session names: a set_config of a parameter, a ROLLBACK TO.
UNKNOWN_ZONE = "unknown"
_UTC = frozenset({"utc", "etc/utc", "gmt", "etc/gmt"})
_SET = re.compile(
    r"^\s*SET\s+(?:(?P<scope>SESSION|LOCAL)\s+)?TIME\s*ZONE\b\s*(?:TO\b|=)?\s*(?P<value>.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RESET = re.compile(r"^\s*(?:RESET\s+(?:TIME\s*ZONE|ALL)|DISCARD\s+ALL)\s*;?\s*$", re.IGNORECASE)
_ROLLED_BACK = re.compile(r"^\s*(?:ROLLBACK|ABORT)\b", re.IGNORECASE)
_TO_SAVEPOINT = re.compile(r"^\s*ROLLBACK\s+(?:(?:WORK|TRANSACTION)\s+)?TO\b", re.IGNORECASE)
_SET_CONFIG = re.compile(r"\bset_config\"?\s*\(\s*(?P<name>'(?:[^']|'')*')?", re.IGNORECASE)
_OF_ZONE = re.compile(
    r"\s*(?:::\s*\w+\s*)?,\s*'(?P<value>(?:[^']|'')*)'\s*(?:::\s*\w+\s*)?,\s*(?P<local>\w+)\s*\)",
    re.IGNORECASE,
)


class SessionZone:
    """The zone in force, ``current``, and the session's own, which a SET LOCAL leaves alone. None
    is the server's, which is UTC."""

    def __init__(self, started: str | None = None) -> None:
        self.started = started
        self.current = started
        self._session = started
        self._at_begin = started

    def _set(self, value: str | None, local: bool, in_block: bool) -> None:
        if local and not in_block:
            return  # PostgreSQL warns, and nothing changes
        self.current = value
        if not local:
            self._session = value

    def _said(self, value: str) -> str | None:
        word = value.strip().strip("'\"")
        return self.started if word.upper() in ("DEFAULT", "LOCAL") else word

    def _set_config(self, sql: str, in_block: bool) -> None:
        for call in _SET_CONFIG.finditer(sql):
            name = call.group("name")
            if name is not None and name.lower() != "'timezone'":
                continue
            given = None if name is None else _OF_ZONE.match(sql, call.end())
            if given is None:
                self._set(UNKNOWN_ZONE, False, in_block)
            else:
                local = given.group("local").lower() == "true"
                self._set(given.group("value").replace("''", "'"), local, in_block)

    def note(self, sql: str, was_open: bool, is_open: bool) -> None:
        """What ``sql`` does to the zone; the statement itself is answered as any is."""
        if not was_open and is_open:
            self._at_begin = self._session
        elif was_open and not is_open:
            if _ROLLED_BACK.match(sql):
                self._session = self._at_begin
            self.current = self._session
        elif _TO_SAVEPOINT.match(sql):
            if (self.current, self._session) != (self._at_begin, self._at_begin):
                self.current = self._session = UNKNOWN_ZONE
        if _RESET.match(sql):
            self._set(self.started, False, was_open)
            return
        written = _SET.match(sql)
        if written is None:
            self._set_config(sql, was_open)
        elif written.group("value").strip("'\""):  # PostgreSQL refuses an empty one
            local = (written.group("scope") or "").upper() == "LOCAL"
            self._set(self._said(written.group("value")), local, was_open)


def refuse_other_zone(sql: str, zone: str | None) -> None:
    """Refuse, by name, a statement that calls a form bound to UTC in a session on another zone."""
    if zone is None or zone.lower() in _UTC:
        return
    name = zoned_call(sql)
    if name is not None:
        raise PostgresError(
            "0A000",
            f"{name} {REFUSED}: its form answers in UTC, and the session TimeZone is {zone}",
        )
