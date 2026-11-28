# Copyright (c) 2026 Kenneth Stott
# Canary: 5d3f8a26-71c4-4e9b-a0d2-8b6e1f4c7a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The grid's hints (REQ-2236, A HINT IS A REWRITE FOR AN ENGINE AND SOURCE PAIR; REQ-2352): each
a difference from PostgreSQL that one engine shows, on its own tables (an engine hint) or reading
one source it attaches (a source hint), which its dialect's rows do not, with its fix and the test
that reproduced it. A hint is added when such a difference is found, never in advance; its
dialect's page lists it under its source (provisa.transpiler.polyfill.docs)."""

# Requirements: REQ-2236, REQ-2352

from __future__ import annotations

from provisa.transpiler import opengauss_bytea_like
from provisa.transpiler.polyfill.grid import Hint, Rule
from provisa.transpiler.polyfill.notation import Rewrite

#: openGauss writes PostgreSQL's dialect, where bytea LIKE is native, but matches it as multibyte
#: text (REQ-2236, amended 2026-11-24).
OPENGAUSS_BYTEA_LIKE = Hint(
    name="bytea LIKE, matched byte-wise",
    dialect="postgres",
    engine=opengauss_bytea_like.KIND,
    source=None,
    difference=(
        "openGauss matches bytea LIKE (like, notlike, bytealike, byteanlike, ~~ and !~~ on bytea)"
        " as multibyte text on a UTF-8 database, reading past the end of a value that ends in an"
        " incomplete UTF-8 sequence, so its answer varies from run to run. Its rawlike and"
        " rawnlike match byte-wise, as PostgreSQL does."
    ),
    rule=Rule(None, {"postgres": Rewrite(opengauss_bytea_like.rewritten)}),
    features=(
        "like(bytea, bytea)",
        "notlike(bytea, bytea)",
        "bytealike(bytea, bytea)",
        "byteanlike(bytea, bytea)",
    ),
    test="tests/integration/test_opengauss_bytea_like.py",
    service="opengauss",
)

#: Every hint, in no order: each page sorts its own (Grid.hints_on).
HINTS: tuple[Hint, ...] = (OPENGAUSS_BYTEA_LIKE,)
