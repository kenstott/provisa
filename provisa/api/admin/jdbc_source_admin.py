# Copyright (c) 2026 Kenneth Stott
# Canary: 38c474ed-ef3e-4fda-aa5d-58a21101d10a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin API of a JDBC source's form (REQ-2191): what the form offers -- the engine its
capabilities are set for, the standard drivers to pick from, the SQL dialects, the capabilities --
and Detect, the checks the admin runs from the form against the connection it describes. Detect
saves nothing."""

# Requirements: REQ-2191

from __future__ import annotations

from typing import Any

import strawberry

from provisa.jdbc_source.settings import CAPABILITIES, NAMED_TYPES, form_scheme, sql_dialects


@strawberry.type
class JdbcDriverType:
    """A standard driver the form offers: a backend of ``NAMED_TYPES``."""

    backend: str
    #: The built-in source type of the same database, which names it on the form.
    source_type: str
    #: The URL scheme the form writes for it.
    scheme: str
    #: Whether its URL names a file (SQLite, DuckDB) rather than a host.
    file: bool
    #: Whether the active engine can attach the built-in type, so live attach is offered.
    attach: bool


@strawberry.type
class JdbcSourceSetupType:
    engine: str
    drivers: list[JdbcDriverType]
    sql_dialects: list[str]
    capabilities: list[str]


@strawberry.input
class JdbcSourceDetectInput:
    url: str
    sql_dialect: str
    username: str = ""
    # As the form holds it: a value, or a secret reference the operator typed.
    password: str = ""
    # A jdbc: URL's driver (provisa.jdbc_source.jdbc); empty for any other URL.
    jdbc_driver_jar: str = ""
    jdbc_driver_class: str = ""


@strawberry.type
class JdbcSourceCheckType:
    capability: str
    passed: bool
    reason: str


@strawberry.type
class JdbcSourceDetectionType:
    engine: str
    checks: list[JdbcSourceCheckType]


def setup(engine: Any) -> JdbcSourceSetupType:
    """What the form offers on ``engine``, the active federation engine."""
    from provisa.federation.strategy import engine_attaches
    from provisa.jdbc_source.url import FILE_BACKENDS

    return JdbcSourceSetupType(
        engine=engine.name,
        drivers=[
            JdbcDriverType(
                backend=name,
                source_type=kind,
                scheme=form_scheme(name),
                file=name in FILE_BACKENDS,
                attach=engine_attaches(engine, kind),
            )
            for name, kind in NAMED_TYPES.items()
        ],
        sql_dialects=list(sql_dialects()),
        capabilities=list(CAPABILITIES),
    )


async def detect(input: JdbcSourceDetectInput, engine: Any) -> JdbcSourceDetectionType:
    """The checks against the connection input describes, on the active engine. Its
    settings are refused by name as a save of them would be, before anything is connected to."""
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.jdbc_source import detect as checks
    from provisa.jdbc_source.jdbc import DRIVER_CLASS, DRIVER_JAR
    from provisa.jdbc_source.settings import SQL_DIALECT, URL, parse
    from provisa.jdbc_source.url import check

    hints = {
        URL: input.url,
        SQL_DIALECT: input.sql_dialect,
        **({DRIVER_JAR: input.jdbc_driver_jar} if input.jdbc_driver_jar else {}),
        **({DRIVER_CLASS: input.jdbc_driver_class} if input.jdbc_driver_class else {}),
    }
    settings = parse(hints)
    async with bound_to_request_org():
        check(settings.url)
        found = await checks.detect(
            hints,
            input.username,
            str(resolve_secrets(input.password)) if input.password else "",
            engine,
        )
    return JdbcSourceDetectionType(
        engine=engine.name,
        checks=[
            JdbcSourceCheckType(capability=c.capability, passed=c.passed, reason=c.reason)
            for c in found
        ],
    )
