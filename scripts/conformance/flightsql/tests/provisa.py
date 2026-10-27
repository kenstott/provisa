# Copyright (c) 2026 Kenneth Stott
# Canary: aa94b1af-041f-4f9a-9167-5f57dc110704
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa, as the ADBC driver validation suite describes a backend (REQ-2167): the ADBC Flight
SQL driver against Provisa's Flight SQL endpoint, the features Provisa claims, and how the suite
reaches it. A feature claimed here and not delivered fails the suite's tests for it; one not
claimed is skipped by the suite. Creating tables is not claimed: a Provisa schema is its model,
so the tables the suite's cases would create are declared in the job's fixture model instead
(``fixtures.py``), and the suite reads them there."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from pathlib import Path

from adbc_drivers_validation import model


class ProvisaQuirks(model.DriverQuirks):
    name = "provisa"
    driver = "adbc_driver_flightsql"
    # The name adbc-driver-flightsql 1.12 reports (the suite expects the unsuffixed one).
    driver_name = "ADBC Flight SQL Driver - Go"
    vendor_name = "Provisa"
    vendor_version = re.compile(r".*")
    short_version = "flightsql"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        get_objects=True,
        select_fixture_setup=False,
        statement_bind=True,
        statement_bind_test_mode="select",
        statement_execute_schema=True,
        statement_prepare=True,
        # An update is answered its affected count. The statement tests' CREATE TABLE is replaced
        # by emptying the seeded table (query_override), which affects none.
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        current_catalog=model.FromEnv("PROVISA_FLIGHT_CATALOG"),
        current_schema=model.FromEnv("PROVISA_FLIGHT_SCHEMA"),
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("PROVISA_FLIGHT_URI"),
            # The job user's credential ("Bearer <token>") and the role it acts as.
            "adbc.flight.sql.authorization_header": model.FromEnv("CONFORMANCE_AUTHORIZATION"),
            "adbc.flight.sql.rpc.call_header.x-provisa-role": model.FromEnv("PROVISA_ROLE"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        return f"${index}"

    #: The statement tests' CREATE TABLE, as each is adapted: its seeded empty table emptied
    #: (fixtures.WRITTEN_TABLES).
    _ADAPTED = {
        "TestStatement.sample_table": "DELETE FROM sample_table",
        "TestStatement.test_rows_affected.create_table": "DELETE FROM test_rows_affected",
    }

    def query_override(self, context: str, default: str) -> str:
        return self._ADAPTED.get(context, default)

    def try_drop_table(self, cursor, **_table) -> None:  # noqa: ARG002  # the suite's signature
        """Nothing to drop: the fixture model holds every table the suite reads, and a query
        surface creates or drops none (adapted, by_design/flight-sql.yaml)."""

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        text = str(error).lower()
        return table_name.lower() in text and ("not found" in text or "does not exist" in text)


def get_quirks() -> ProvisaQuirks:
    return ProvisaQuirks()
