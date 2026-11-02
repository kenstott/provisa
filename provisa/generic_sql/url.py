# Copyright (c) 2026 Kenneth Stott
# Canary: b327f138-71a1-421d-a26f-9adbb7ccb9b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Generic SQL source's connection URL, as it is kept and as it is connected to (REQ-2191).

The URL's scheme names the SQLAlchemy dialect and driver. A scheme no installed package provides
fails the connection naming what is missing (:class:`MissingDialect`).

A file database (SQLite, DuckDB) is named by its path, written with the org's tokens of REQ-1997
(``sqlite:///{files}/shop.db``) and opened where the token resolves; a path that leaves the org's
folder is refused by name, by the check REQ-1997 applies. On a server of many organizations that is
the only file URL allowed: the maintainer, on the customer scratch files area, "that's the only
file:// you are allowed". A DuckDB file is opened there with DuckDB's external access off, so a
statement on it reaches no other file and no network address of the server's.

A network URL's host is checked where every source's is (``provisa.security.sandbox_egress``) and
connected to at the address checked. For an org whose sources are so held, the URL carries only
the driver options of :data:`HELD_ORG_OPTIONS`: a driver option can name another address than the
URL's host (psycopg2 connects to a ``?host=`` instead of it), a file of the server's own
(``sslkey``, ``read_default_file``), a service, an init command or a local file load. The
maintainer, 2026-11-02: a per-dialect allowlist, anything else refused by name; a self-hosted
server and the deployment's own org give any option their driver takes.
"""

# Requirements: REQ-2191, REQ-1997, REQ-2006

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.generic_sql import SOURCE_TYPE
from provisa.generic_sql.settings import InvalidGenericSqlSource, backend

if TYPE_CHECKING:
    from sqlalchemy.engine import URL

#: The backends whose URL names a file on the server's disk.
FILE_BACKENDS = frozenset({"sqlite", "duckdb"})


class MissingDialect(RuntimeError):
    """The URL's scheme names a dialect or driver no installed package provides."""

    def __init__(self, scheme: str, missing: str) -> None:
        super().__init__(
            f"the {scheme!r} connection scheme needs {missing}, which Provisa does not ship"
        )
        self.scheme = scheme
        self.missing = missing


#: The driver options a held org's URL may give, by the dialect its scheme names; none for any
#: other dialect (module docstring).
HELD_ORG_OPTIONS: dict[str, frozenset[str]] = {
    "postgresql": frozenset(
        {"sslmode", "connect_timeout", "application_name", "target_session_attrs"}
    ),
    "mysql": frozenset({"ssl_mode", "connect_timeout", "charset"}),
    "mssql": frozenset({"driver", "Encrypt", "TrustServerCertificate"}),
}


class DriverOptionsRefused(PermissionError):
    """A held org's URL that carries a driver option it may not give (module docstring)."""

    def __init__(self, backend_name: str, options: list[str]) -> None:
        allowed = sorted(HELD_ORG_OPTIONS.get(backend_name, frozenset()))
        super().__init__(
            f"this org's {backend_name} URL may not give the driver option "
            f"{', '.join(options)}; "
            + (f"it may give {', '.join(allowed)}" if allowed else "it may give none")
        )
        self.options = options


class FileUrlRefused(PermissionError):
    """A file URL that names a file this server may not open for the org."""

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"{path or '(no path)'}: {reason}")
        self.path = path


def is_file_url(url: str) -> bool:
    """Whether ``url`` names a file database."""
    return backend(url) in FILE_BACKENDS


def _parsed(url: str) -> "URL":
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import ArgumentError

    try:
        parsed = make_url(url)
    except ArgumentError as exc:
        raise InvalidGenericSqlSource(f"{url!r} is not a connection URL: {exc}") from None
    if parsed.password:
        raise InvalidGenericSqlSource(
            "the connection URL carries a password; give it in the source's password field"
        )
    return parsed


def _file_path(path: str) -> str:
    """The file ``path`` names, its org token resolved; refused by name where it may not be
    opened (module docstring)."""
    from provisa.core import org_files
    from provisa.core.secrets_providers import many_organisations

    if org_files.is_org_path(path):
        try:
            return org_files.resolve_path(path)
        except org_files.OrgPathRefused:
            raise FileUrlRefused(path, "leaves the organization's folder") from None
    if many_organisations():
        raise FileUrlRefused(
            path,
            "a server of many organizations opens only a file in the organization's files "
            "area, written {files}/... or {org}/...",
        )
    return path


def check(url: str) -> None:
    """Refuse, by name, a URL that cannot be kept: one that is no URL, carries a password, or
    names a file this server may not open for the org."""
    if backend(url) == "jdbc":
        from provisa.generic_sql.jdbc import refuse_where_not_allowed

        refuse_where_not_allowed()
        return
    parsed = _parsed(url)
    if backend(url) in FILE_BACKENDS:
        _file_path(parsed.database or "")
    _no_options_when_held(parsed)


def _no_options_when_held(parsed: "URL") -> None:
    from provisa.security.sandbox_egress import held_here

    if not parsed.query or not held_here():
        return
    name = parsed.get_backend_name()
    refused = sorted(set(parsed.query) - HELD_ORG_OPTIONS.get(name, frozenset()))
    if refused:
        raise DriverOptionsRefused(name, refused)


def destination(url: str) -> tuple[str, int | None] | None:
    """The network host and port ``url`` connects to, or None for a file database and for a jdbc:
    URL, which no held org may give (``provisa.generic_sql.jdbc``)."""
    if backend(url) in FILE_BACKENDS or backend(url) == "jdbc":
        return None
    parsed = _parsed(url)
    return (parsed.host.lower(), parsed.port) if parsed.host else None


def connection_url(url: str, user: str, password: str) -> tuple["URL", str]:
    """The URL the source is connected at, and the database as the source names it (for
    messages): a file's token resolved, the source's own user and password set, and a network
    host replaced by the address it was checked at (``sandbox_egress.pin_host``)."""
    parsed = _parsed(url)
    _no_options_when_held(parsed)
    shown = parsed.database or ""
    if backend(url) in FILE_BACKENDS:
        parsed = parsed.set(database=_file_path(shown))
    if user:
        parsed = parsed.set(username=user)
    if password:
        parsed = parsed.set(password=password)
    if parsed.host:
        from provisa.security.sandbox_egress import pin_host

        parsed = parsed.set(host=pin_host(parsed.host, parsed.port, SOURCE_TYPE))
    return parsed, shown


def create_engine(url: "URL") -> Any:
    """The SQLAlchemy engine for ``url``; a scheme no installed package provides is refused
    naming what is missing."""
    import sqlalchemy
    from sqlalchemy.exc import NoSuchModuleError

    connect_args: dict[str, Any] = {}
    if url.get_backend_name() == "duckdb":
        from provisa.core.secrets_providers import many_organisations

        if many_organisations():
            connect_args["config"] = {"enable_external_access": False}
    try:
        return sqlalchemy.create_engine(url, connect_args=connect_args)
    except NoSuchModuleError:
        raise MissingDialect(
            url.drivername, f"a package providing the SQLAlchemy dialect {url.drivername!r}"
        ) from None
    except ModuleNotFoundError as exc:
        raise MissingDialect(url.drivername, f"the Python package {exc.name!r}") from None
