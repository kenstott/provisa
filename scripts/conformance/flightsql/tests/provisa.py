# Copyright (c) 2026 Kenneth Stott
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
so the suite's queries that build their fixture with DDL are skipped, not run."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from pathlib import Path

from adbc_drivers_validation import model


class ProvisaQuirks(model.DriverQuirks):
    name = "provisa"
    driver = "adbc_driver_flightsql"
    driver_name = "ADBC Flight SQL Driver"
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
        current_catalog=model.FromEnv("PROVISA_FLIGHT_CATALOG"),
        current_schema=model.FromEnv("PROVISA_FLIGHT_SCHEMA"),
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("PROVISA_FLIGHT_URI"),
            # No auth provider on the instance: the acting role travels as a call header.
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

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        text = str(error).lower()
        return table_name.lower() in text and ("not found" in text or "does not exist" in text)


def get_quirks() -> ProvisaQuirks:
    return ProvisaQuirks()
