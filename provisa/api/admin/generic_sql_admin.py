# Copyright (c) 2026 Kenneth Stott
# Canary: 38c474ed-ef3e-4fda-aa5d-58a21101d10a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin API of a Generic SQL source's form (REQ-2191): what the form offers -- the engine its
capabilities are set for, the SQL dialects, the capabilities -- and Detect, the checks the admin
runs from the form against the connection it describes. Detect saves nothing."""

# Requirements: REQ-2191

from __future__ import annotations

import strawberry

from provisa.generic_sql.settings import CAPABILITIES, sql_dialects


@strawberry.type
class GenericSqlSetupType:
    engine: str
    sql_dialects: list[str]
    capabilities: list[str]


@strawberry.input
class GenericSqlDetectInput:
    url: str
    sql_dialect: str
    username: str = ""
    # As the form holds it: a value, or a secret reference the operator typed.
    password: str = ""


@strawberry.type
class GenericSqlCheckType:
    capability: str
    passed: bool
    reason: str


@strawberry.type
class GenericSqlDetectionType:
    engine: str
    checks: list[GenericSqlCheckType]


def setup(engine: str) -> GenericSqlSetupType:
    return GenericSqlSetupType(
        engine=engine, sql_dialects=list(sql_dialects()), capabilities=list(CAPABILITIES)
    )


async def detect(input: GenericSqlDetectInput, engine: str) -> GenericSqlDetectionType:
    """The checks against the connection input describes, on the active engine. Its
    settings are refused by name as a save of them would be, before anything is connected to."""
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.generic_sql import detect as checks
    from provisa.generic_sql.settings import SQL_DIALECT, URL, parse
    from provisa.generic_sql.url import check

    settings = parse({URL: input.url, SQL_DIALECT: input.sql_dialect})
    async with bound_to_request_org():
        check(settings.url)
        found = await checks.detect(
            settings.url,
            settings.sql_dialect,
            input.username,
            str(resolve_secrets(input.password)) if input.password else "",
        )
    return GenericSqlDetectionType(
        engine=engine,
        checks=[
            GenericSqlCheckType(capability=c.capability, passed=c.passed, reason=c.reason)
            for c in found
        ],
    )
