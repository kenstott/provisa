# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic SQL's dialect: PostgreSQL text that keeps what the read means (REQ-778).

A statement is held as PostgreSQL-dialect text and read back and written again at every rewrite
(governance, RLS, routing). sqlglot writes a null-on-failure cast (``TryCast``) in that dialect as
a plain CAST, PostgreSQL having none, and the cast that answers null for a value that does not
convert (Cypher's toInteger, toFloat, toBoolean) then fails the statement instead. So the text
keeps TRY_CAST, which sqlglot reads back as the same cast, and each target writes its own: TRY_CAST,
SAFE_CAST, or for a PostgreSQL target ``pg_input_is_valid`` (``transpile.postgres_try_casts``)."""

# Requirements: REQ-778

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generator import Generator

Postgres.Generator.TRANSFORMS[exp.TryCast] = Generator.trycast_sql
