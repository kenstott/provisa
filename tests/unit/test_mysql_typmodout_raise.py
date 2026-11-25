# Copyright (c) 2026 Kenneth Stott
# Canary: 7f3a1d22-b48c-4e9d-a5b1-8c2f0e5d9b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""intervaltypmodout('0'::integer) must raise on MySQL-family dialects, not answer '0' (REQ-2236).

Since 718d1fe359 (DuckDB J6), _int_said reads a quoted integer cast, so the typmod fold fires on
every dialect. For typmod 0, interval_typmod_out raises XX000. On MySQL, the fold emitted
CAST(CAST(text AS SIGNED) AS CHAR) which silently returns '0' — a wrong answer (REQ-2236:
never a wrong answer). The fix uses provisa_raise so MySQL-family dialects raise instead."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest

from provisa.core.source_registry import SOURCE_TO_DIALECT
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_type_io import readable
from provisa.transpiler.transpile import transpile

#: The call that raises XX000 on PostgreSQL.
_SQL = "SELECT pg_catalog.\"intervaltypmodout\"('0'::integer) AS v"

#: Dialects in the MySQL family.
_MYSQL_FAMILY = ("mysql", SOURCE_TO_DIALECT["mariadb"], SOURCE_TO_DIALECT["tidb"])


class TestIntervalTypmodoutZeroRaisesOnMysqlFamily:
    """intervaltypmodout('0'::integer) must raise, not answer '0'."""

    @pytest.mark.parametrize("dialect", _MYSQL_FAMILY)
    def test_readable_emits_provisa_raise_not_signed_cast(self, dialect: str) -> None:
        """After the fold, the intermediate postgres SQL must contain provisa_raise, not SIGNED."""
        folded = readable(_SQL, dialect)
        assert "provisa_raise" in folded.lower(), (
            f"expected provisa_raise in folded SQL for {dialect!r}, got: {folded!r}"
        )
        assert "SIGNED" not in folded, (
            f"broken SIGNED form still present for {dialect!r}: {folded!r}"
        )

    @pytest.mark.parametrize("dialect", _MYSQL_FAMILY)
    def test_transpile_raises_function_refused_not_zero(self, dialect: str) -> None:
        """transpile() must raise FunctionRefused for provisa_raise on MySQL, not return '0' SQL."""
        with pytest.raises(FunctionRefused):
            transpile(_SQL, dialect)


class TestIntervalTypmodoutOtherDialectsUnaffected:
    """DuckDB and Trino still raise PostgreSQL's XX000, not '0'."""

    @pytest.mark.parametrize("dialect", ("duckdb", "trino"))
    def test_transpile_still_raises_on_duckdb_and_trino(self, dialect: str) -> None:
        """DuckDB and Trino transpile to an expression that raises, not to a silent form."""
        result = transpile(_SQL, dialect)
        # Neither should produce a silent '0'-returning form
        # DuckDB uses error(), Trino uses fail()
        assert "SIGNED" not in result, f"broken SIGNED form in {dialect!r} output: {result!r}"
        # The error marker must be in the transpiled SQL
        assert "postgresql-error" in result, (
            f"postgresql-error marker missing from {dialect!r} output: {result!r}"
        )
