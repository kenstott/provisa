# Copyright (c) 2026 Kenneth Stott
# Canary: 50336d19-5491-42a3-be30-72b405386271
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The options of a Provisa read (REQ-2270).

``spark.read.format("provisa").option("uri", ...).option("token", ...).load("sales.orders")``:

- ``uri``: Provisa's Arrow Flight SQL endpoint, ``grpc://host:port`` or ``grpc+tls://host:port``.
- ``token``: the credential of the principal the read runs as (a personal access token or a
  provider token), sent as ``authorization: Bearer``. Provisa decides the role it may act as, the
  rows and columns it is served and the masks applied, and audits the read as that principal.
- ``role``: a role to act as, a request Provisa honors only when the principal holds it.
- the table, as ``load``'s path or the ``table`` option, never both: its name as Provisa's SQL
  names it (``orders``, ``sales.orders``; a part with a dot in it double-quoted).

Any other option is refused by name, so a misspelt ``token`` is not a read as no one.
"""

# Requirements: REQ-2270

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

_SCHEMES = ("grpc://", "grpc+tcp://", "grpc+tls://")
_KNOWN = frozenset({"uri", "token", "role", "table", "path"})


class OptionError(ValueError):
    """A Provisa read's options do not name one read."""


@dataclass(frozen=True)
class ReadOptions:
    """One read: where, as whom, and which table."""

    uri: str
    table: tuple[str, ...]
    token: str | None
    role: str | None

    @classmethod
    def parse(cls, options: Mapping[str, str]) -> ReadOptions:
        given = {key.lower(): value for key, value in options.items()}
        unknown = sorted(set(given) - _KNOWN)
        if unknown:
            raise OptionError(
                f"unknown option(s) {', '.join(unknown)}; a Provisa read takes uri, token, role "
                "and the table (load's path or the table option)"
            )
        uri = given.get("uri", "").strip()
        if not uri:
            raise OptionError(
                "option uri is required: Provisa's Flight SQL endpoint, grpc://host:port"
            )
        if not uri.startswith(_SCHEMES):
            raise OptionError(
                f"option uri {uri!r} is not a Flight endpoint; it starts grpc://, grpc+tcp:// or "
                "grpc+tls://"
            )
        path, table = given.get("path"), given.get("table")
        if path and table:
            raise OptionError(
                "the table is named twice: give load's path or the table option, not both"
            )
        name = (path or table or "").strip()
        if not name:
            raise OptionError('no table is named: spark.read.format("provisa").load("<table>")')
        return cls(
            uri=uri,
            table=table_parts(name),
            token=_present(given, "token"),
            role=_present(given, "role"),
        )

    def statement(self) -> str:
        """The statement whose rows are the table's: every column the principal is served."""
        return "SELECT * FROM " + ".".join(_quoted(part) for part in self.table)

    def headers(self) -> list[tuple[bytes, bytes]]:
        """The call headers naming the caller: the credential, and the role it asks for."""
        headers: list[tuple[bytes, bytes]] = []
        if self.token is not None:
            headers.append((b"authorization", f"Bearer {self.token}".encode()))
        if self.role is not None:
            headers.append((b"x-provisa-role", self.role.encode()))
        return headers


def _present(given: Mapping[str, str], key: str) -> str | None:
    """The option's value; an option given empty is refused rather than read as absent."""
    if key not in given:
        return None
    value = given[key].strip()
    if not value:
        raise OptionError(f"option {key} is empty")
    return value


def table_parts(name: str) -> tuple[str, ...]:
    """The parts of a dotted table name; a double-quoted part keeps its dots (``""`` is a quote)."""
    parts: list[str] = []
    i, n = 0, len(name)
    while True:
        if i < n and name[i] == '"':
            i += 1
            part: list[str] = []
            while True:
                if i >= n:
                    raise OptionError(f"table {name!r} has an unclosed double quote")
                if name[i] == '"':
                    if i + 1 < n and name[i + 1] == '"':
                        part.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                part.append(name[i])
                i += 1
            parts.append("".join(part))
        else:
            end = name.find(".", i)
            end = n if end == -1 else end
            bare = name[i:end]
            if '"' in bare:
                raise OptionError(f"table {name!r} has a double quote inside an unquoted part")
            parts.append(bare)
            i = end
        if not parts[-1]:
            raise OptionError(f"table {name!r} has an empty part")
        if i == n:
            return tuple(parts)
        if name[i] != ".":
            raise OptionError(
                f"table {name!r}: a quoted part is followed by {name[i]!r}, not a dot"
            )
        i += 1


def _quoted(part: str) -> str:
    return '"' + part.replace('"', '""') + '"'
