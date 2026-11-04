# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6b4d52-7f1e-4a0b-9d3e-5a8e2c71f946
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgvector in the control plane (REQ-2185, THE LIBRARY'S STORE AND RETRIEVAL).

The reference library keeps each passage's embedding in a pgvector column under a vector index, and
its governed retrieval searches that index; there is no unindexed search. The extension is created
once per database, in ``public`` (:data:`SCHEMA`). An org's connections have only the org's schema
on their search_path, so the type, its functions and its operators are always named with that
schema. A control plane that cannot carry pgvector is refused when an org's schema is laid out
(:func:`require`), naming pgvector.
"""

# Requirements: REQ-2185

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import cast, text
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.types import UserDefinedType

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The schema pgvector is created in, and every reference to it is qualified with.
SCHEMA = "public"
#: The most dimensions pgvector's HNSW index takes for a ``vector``.
MAX_INDEXED_DIMENSIONS = 2000


class PgvectorUnavailable(RuntimeError):
    """The control plane's PostgreSQL cannot carry pgvector."""


class Vector(UserDefinedType):
    """pgvector's ``vector`` of any dimension: a list of floats in Python, ``[x,y,...]`` on the
    wire. Bound values are cast to the type, so a parameter never arrives as text."""

    cache_ok = True

    def get_col_spec(self, **_kw: Any) -> str:
        return f"{SCHEMA}.vector"

    def bind_expression(self, bindvalue: Any) -> Any:
        return cast(bindvalue, self)

    def bind_processor(self, dialect: Any) -> Any:
        def process(value: Any) -> str | None:
            return None if value is None else literal(value)

        return process

    def result_processor(self, dialect: Any, coltype: Any) -> Any:
        def process(value: Any) -> list[float] | None:
            return None if value is None else parse(value)

        return process


# Reflection (db.add_missing_columns inspects every org table) knows the column as this type, by
# its qualified name while an org's schema is the search_path, or bare while public is.
ischema_names[f"{SCHEMA}.vector"] = Vector
ischema_names["vector"] = Vector


def literal(vector: Any) -> str:
    """``vector`` in pgvector's text form."""
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


def parse(held: str) -> list[float]:
    """pgvector's text form read back as floats."""
    inner = held.strip()[1:-1]
    return [float(x) for x in inner.split(",")] if inner else []


async def require(conn: Connection) -> None:
    """pgvector in the database ``conn`` reaches, created in :data:`SCHEMA` if it is not there
    yet; refused (:class:`PgvectorUnavailable`) when the server does not have it, the role may
    not create it, or it is in another schema."""
    held = (
        await conn.execute_core(
            text(
                "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n "
                "ON n.oid = e.extnamespace WHERE e.extname = 'vector'"
            )
        )
    ).fetchone()
    if held is None:
        available = (
            await conn.execute_core(
                text("SELECT 1 FROM pg_available_extensions WHERE name = 'vector'")
            )
        ).fetchone()
        if available is None:
            raise PgvectorUnavailable(_refusal("this PostgreSQL server does not have pgvector"))
        try:
            async with conn.transaction():
                await conn.execute_core(
                    text(f'CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA "{SCHEMA}"')
                )
        except Exception as exc:
            reason = str(getattr(exc, "orig", None) or exc).strip().splitlines()[0]
            raise PgvectorUnavailable(_refusal(f"pgvector could not be created: {reason}")) from exc
        return
    if held[0] != SCHEMA:
        raise PgvectorUnavailable(
            _refusal(f"pgvector is installed in schema {held[0]!r}; Provisa reads it in {SCHEMA!r}")
        )


def _refusal(why: str) -> str:
    return (
        f"the reference library's governed retrieval needs the pgvector extension in the control "
        f"plane's PostgreSQL, and {why}. Run the control plane on PostgreSQL with pgvector (the "
        "pgvector/pgvector:pg16 image) and a role that may create it; the library is not searched "
        "without its index"
    )
