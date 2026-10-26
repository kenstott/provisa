# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pg engine's fake functions import in the database's own Python (REQ-1494).

PostgreSQL runs them as PL/Python (``plpython3u``), whose Python holds Faker and Provisa's source
and nothing more of Provisa's dependencies. The package's root registered a sqlglot transform
(REQ-778), so every ``provisa.*`` import -- the fake functions' among them -- needed sqlglot, and
every fake read on a pg engine failed ``No module named 'sqlglot'``.
"""

# Requirements: REQ-1494, REQ-778

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_the_fake_functions_import_with_no_sqlglot():
    # sqlglot made unimportable, as in the database's Python.
    script = (
        "import sys; sys.modules['sqlglot'] = None\n"
        "from provisa.fakes.pg_functions import FUNCTIONS_SQL\n"
        "assert FUNCTIONS_SQL\n"
    )
    ran = subprocess.run(
        [sys.executable, "-c", script], cwd=_ROOT, capture_output=True, text=True, check=False
    )
    assert ran.returncode == 0, ran.stderr


def test_each_package_writing_the_semantic_sql_keeps_its_null_on_failure_casts():
    # REQ-778: registered by the package that writes or rewrites the statement, whichever a
    # process imports first, so the text keeps TRY_CAST through every rewrite.
    for package in ("provisa.gql", "provisa.compiler", "provisa.transpiler"):
        script = (
            f"import {package}\n"
            "import sqlglot\n"
            "said = sqlglot.parse_one('SELECT TRY_CAST(x AS INT)', read='postgres')\n"
            "assert 'TRY_CAST' in said.sql(dialect='postgres'), said.sql(dialect='postgres')\n"
        )
        ran = subprocess.run(
            [sys.executable, "-c", script], cwd=_ROOT, capture_output=True, text=True, check=False
        )
        assert ran.returncode == 0, (package, ran.stderr)
