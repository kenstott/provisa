# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Pyodide downloads directory, as an image build is given it.

Dockerfile and Dockerfile.dev build provisa-notebook's assets, which are Pyodide and its packages:
about seventy files from Pyodide's CDN. A test that builds either image gives the build the
directory those files were already downloaded into, so the build asks the CDN for nothing (one
refused request there failed every test of the cluster lane, run 43879947072).

The directory is the one PROVISA_NOTEBOOK_DOWNLOADS names. In CI .github/actions/pyodide-downloads
fills it, from its cache or with one download, and sets the variable. Anywhere else:

    export PROVISA_NOTEBOOK_DOWNLOADS=~/.cache/provisa-notebook-downloads
    PYTHONPATH=provisa-notebook python -m provisa_notebook.build --download-only
"""

from __future__ import annotations

import os

ENV = "PROVISA_NOTEBOOK_DOWNLOADS"
#: The Dockerfiles' stage the directory takes the place of.
STAGE = "pyodide-downloads"


def build_context_args() -> list[str]:
    """``docker build`` arguments that put the downloads directory in the Dockerfile's
    ``pyodide-downloads`` stage's place. Raises KeyError, naming the variable, when the
    environment does not name the directory."""
    return ["--build-context", f"{STAGE}={os.environ[ENV]}"]
