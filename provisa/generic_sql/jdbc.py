# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6c1430-45d3-462e-8bc8-391240825a7a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A customer's JDBC driver behind a Generic SQL source (REQ-2191, amended 2026-11-01).

A ``jdbc:`` URL with a driver jar and its class name selects it. Calcite is not involved: Provisa
opens the driver directly through JPype, in its own JVM, and reads it through JPype's DB-API
(``jpype.dbapi2``), behind the same driver the rest of Generic SQL uses. The SQL dialect, the
capability settings, Detect and the floor apply as for a SQLAlchemy URL. A jdbc: URL has no
engine attach.

A customer's jar is the customer's code in Provisa's JVM, so this is for self-hosted installs
only: on a server of many organizations it is refused by name, and so is it for an org whose
sources are held to public destinations (a sandbox), whose destination a JDBC URL cannot be
checked for. Each jar is loaded by a class loader of its own, so two sources' drivers never see
each other's classes, and a jar is loaded once per process.
"""

# Requirements: REQ-2191

from __future__ import annotations

import re
import threading
from typing import Any

from provisa.generic_sql.settings import InvalidGenericSqlSource

#: The settings a jdbc: URL needs beside it (the source's federation hints).
DRIVER_JAR = "jdbc_driver_jar"
DRIVER_CLASS = "jdbc_driver_class"

_lock = threading.Lock()
_loaders: dict[str, Any] = {}


class JdbcRefused(PermissionError):
    """A JDBC driver this server does not run for the org (module docstring)."""

    def __init__(self) -> None:
        super().__init__(
            "a customer's JDBC driver runs as code in Provisa's own JVM, which only a "
            "self-hosted install allows; this server does not run one for this organization"
        )


def is_jdbc(url: str) -> bool:
    return url.strip().lower().startswith("jdbc:")


def refuse_where_not_allowed() -> None:
    """Refuse, by name, a JDBC driver on a server of many organizations or for a held org."""
    from provisa.core.secrets_providers import many_organisations
    from provisa.security.sandbox_egress import held_here

    if many_organisations() or held_here():
        raise JdbcRefused()


def driver_settings(hints: dict[str, str]) -> tuple[str, str]:
    """The driver jar and class a jdbc: URL is read through, refused by name when missing."""
    jar = (hints.get(DRIVER_JAR) or "").strip()
    cls = (hints.get(DRIVER_CLASS) or "").strip()
    missing = [name for name, value in ((DRIVER_JAR, jar), (DRIVER_CLASS, cls)) if not value]
    if missing:
        raise InvalidGenericSqlSource(f"a jdbc: URL needs {' and '.join(missing)}")
    return jar, cls


def start_jvm() -> Any:
    """Provisa's JVM, started once per process. Called on the event loop's thread before any JDBC
    work is offloaded: a JVM started on a worker thread outlives it, and the interpreter's exit
    then waits on the JVM's shutdown for good."""
    import jpype

    if not jpype.isJVMStarted():
        jpype.startJVM()
    return jpype


def _jvm() -> Any:
    import jpype

    if not jpype.isJVMStarted():
        raise RuntimeError("the JVM is started on the event loop's thread first (start_jvm)")
    return jpype


def _loader(jar: str) -> Any:
    """The class loader of jar (its token resolved), made once per process."""
    from pathlib import Path

    from provisa.core.org_files import resolve_path

    path = resolve_path(jar)
    if not Path(path).is_file():
        raise InvalidGenericSqlSource(f"the JDBC driver jar {jar} is not a file")
    with _lock:
        loader = _loaders.get(path)
        if loader is None:
            jpype = _jvm()
            url = jpype.JClass("java.io.File")(path).toURI().toURL()
            parent = jpype.JClass("java.lang.ClassLoader").getSystemClassLoader()
            loader = jpype.JClass("java.net.URLClassLoader")([url], parent)
            _loaders[path] = loader
        return loader


def connect(url: str, jar: str, driver_class: str, user: str, password: str) -> Any:
    """A DB-API connection (jpype.dbapi2) to url through the driver driver_class of
    jar. Refused by name where this server does not run a customer's driver."""
    refuse_where_not_allowed()
    loader = _loader(jar)
    jpype = _jvm()
    import jpype.dbapi2 as dbapi

    try:
        cls = jpype.JClass("java.lang.Class").forName(driver_class, True, loader)
    except jpype.JException as exc:
        raise InvalidGenericSqlSource(
            f"the JDBC driver jar {jar} has no class {driver_class}: {exc}"
        ) from None
    driver = cls.getDeclaredConstructor().newInstance()
    props = jpype.JClass("java.util.Properties")()
    if user:
        props.setProperty("user", user)
    if password:
        props.setProperty("password", password)
    jconn = driver.connect(url, props)
    if jconn is None:
        raise InvalidGenericSqlSource(f"the JDBC driver {driver_class} does not accept {url}")
    return dbapi.Connection(
        jconn,
        dbapi._default_adapters,
        dbapi._default_converters,
        dbapi.SETTERS_BY_TYPE,
        dbapi.GETTERS_BY_TYPE,
    )


_DOLLAR = re.compile(r"\$(\d+)")


def qmark(sql: str, params: list | None) -> tuple[str, list]:
    """sql with its PG-canonical $N placeholders as JDBC's ?, and the values in the
    order they now appear (a placeholder used twice is bound twice)."""
    if not params:
        return sql, []
    order: list[int] = []

    def _q(match: re.Match[str]) -> str:
        order.append(int(match.group(1)) - 1)
        return "?"

    return _DOLLAR.sub(_q, sql), [params[i] for i in order]
