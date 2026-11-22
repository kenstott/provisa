# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e7d14-8c3a-4f61-9e05-a7d1c6b4f382
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""postgresql_fdw_validator (REQ-2236): whether the options of a foreign-data wrapper are valid.

Its form (provisa.transpiler.metadata_server_forms, and DuckDB's catalog answer,
provisa.pgwire.catalog_server_answers) is written for the call before the grid reads the statement,
so the row holds no rule: it states where the form differs from PostgreSQL. The form is foreign.c's:
an option array holding a NULL element is 22004, as untransformRelOptions deconstructs the array
before it reads an option; otherwise the first option the catalog does not list is 42601 invalid
option naming it, an options array of any dimensions read as its elements in storage order. That is
exact on DuckDB and on Trino (whose SQL is Presto's and Athena's too). ClickHouse 24.3's throwIf
takes only a constant message, so there an unlisted option is refused by name instead (maintainer
2026-11-21: keep 24.3, state the refusal). Db2 and SAP HANA have no form of the array filter the
form is written with, and refuse it by name."""

# Requirements: REQ-2236

from __future__ import annotations

from provisa.transpiler.metadata_server_forms import FDW_OPTION_REFUSAL
from provisa.transpiler.polyfill.grid import Feature, Limitation, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "foreign-data wrapper options"

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 175

_NO_ARRAY_FILTER = (
    "Refused with 0A000 by name: the engine has no form of the array filter the validator's form "
    "is written with."
)

FEATURES: tuple[Feature, ...] = (
    Feature(
        "postgresql_fdw_validator",
        FAMILY,
        "Whether the options of a foreign-data wrapper are valid: 22004 for an option array "
        "holding a NULL element, else 42601 invalid option naming the first option the catalog "
        "does not list.",
        read=Side(
            limitations={
                "clickhouse": (
                    Limitation(
                        "An option the catalog does not list is refused with 0A000 by name, not "
                        "PostgreSQL's 42601 naming it: ClickHouse 24.3's throwIf takes only a "
                        "constant message.",
                        Refusal("0A000", FDW_OPTION_REFUSAL),
                    ),
                ),
                **{
                    dialect: (
                        Limitation(
                            _NO_ARRAY_FILTER,
                            Refusal("0A000", f"array_length() has no form on {dialect}"),
                        ),
                    )
                    for dialect in ("db2", "saphana")
                },
            }
        ),
    ),
)
