# Copyright (c) 2026 Kenneth Stott
# Canary: de9b6751-6eed-45b1-bea9-b8d4aa3ed167
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``ansi`` SQLGlot dialect (REQ-2191): SQLGlot's own generic SQL, under the name a Generic SQL
source's admin picks for a database closest to no named dialect. Defining the class registers the
name, so a source's dialect is always a name SQLGlot answers to."""

# Requirements: REQ-2191

from __future__ import annotations

from sqlglot.dialects.dialect import Dialect


class Ansi(Dialect):
    """SQLGlot's generic dialect, named ``ansi``."""
