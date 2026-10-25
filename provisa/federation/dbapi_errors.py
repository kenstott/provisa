# Copyright (c) 2026 Kenneth Stott
# Canary: bd99591f-4394-4abf-8cb5-00c8c6749561
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DB-API driver's errors by what they say of a statement (PEP 249, REQ-778)."""

# Requirements: REQ-778

from __future__ import annotations

from types import ModuleType


def dbapi_errors(
    module: ModuleType,
) -> tuple[tuple[type[BaseException], ...], tuple[type[BaseException], ...]]:
    """``module``'s connection errors (OperationalError, InterfaceError), then its statement
    errors (DataError, ProgrammingError, IntegrityError, NotSupportedError)."""
    return (
        (module.OperationalError, module.InterfaceError),
        (
            module.DataError,
            module.ProgrammingError,
            module.IntegrityError,
            module.NotSupportedError,
        ),
    )
