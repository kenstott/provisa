# Copyright (c) 2026 Kenneth Stott
# Canary: 0e991b94-370b-454d-a6ab-7cf7d4755c5a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rows of the function table (REQ-2167, REQ-2171), one module per family of functions
(REQ-2236, G10): each family's module holds its rows as ``ROWS``, and :data:`ROWS` collects every
module of this package but :mod:`base`, the rows' notation. A family's rows are edited in its own
module alone; a new family is a new module, collected with no edit here.

An overload (a name and an argument count) is one family's, so the table
(provisa.transpiler.function_macros.MACROS) is the same in whatever order the families are read."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

import importlib
import pkgutil

from provisa.transpiler.rows.base import Macro

#: The package's module that holds no rows: the notation every family's rows are written in.
_NOTATION = "base"


def _collected() -> tuple[Macro, ...]:
    """Every family's rows, refusing an overload two families both hold."""
    held_by: dict[tuple[str, int], str] = {}
    collected: list[Macro] = []
    for module in sorted(m.name for m in pkgutil.iter_modules(__path__) if m.name != _NOTATION):
        family: tuple[Macro, ...] = importlib.import_module(f"{__name__}.{module}").ROWS
        for key in {(macro.name, len(macro.params)) for macro in family}:
            if key in held_by:
                raise ValueError(f"{key} is a row of both {held_by[key]} and {module}")
            held_by[key] = module
        collected.extend(family)
    return tuple(collected)


#: Every family's rows.
ROWS = _collected()
