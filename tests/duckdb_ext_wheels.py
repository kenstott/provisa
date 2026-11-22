# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisa-duckdb-ext wheels directory, as an image build is given it (REQ-294).

Dockerfile and Dockerfile.dev stage every DuckDB extension at build time
(``python -m provisa.runtime_deps.stage_assets``), and provisa-duckdb-ext's wheels are on the
release's Pages index only once a release has published them: an image of any other commit finds
none there, and its build stops by name. A test that builds either image gives the build the
directory this commit's wheels were built into, and the stage step gives pip that directory as
find-links.

The directory is the one PROVISA_DUCKDB_EXT_WHEELS names. In CI .github/actions/duckdb-ext-wheels
fills it, from its cache, the run's artifact or one build, and sets the variable. Anywhere else,
mirror and build the wheels for the machine's Linux platform (as the action does) and name the
directory they went into.
"""

from __future__ import annotations

import os

ENV = "PROVISA_DUCKDB_EXT_WHEELS"
#: The Dockerfiles' stage the directory takes the place of.
STAGE = "duckdb-ext-wheels"


def build_context_args() -> list[str]:
    """``docker build`` arguments that put the wheels directory in the Dockerfile's
    ``duckdb-ext-wheels`` stage's place. Raises KeyError, naming the variable, when the
    environment does not name the directory."""
    return ["--build-context", f"{STAGE}={os.environ[ENV]}"]
