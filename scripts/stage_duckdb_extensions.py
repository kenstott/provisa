#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
# Canary: c8d11db1-1d9c-45d3-8507-93f58757e6a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage every DuckDB extension on a source checkout or in an image build (REQ-294).

The step itself is packaged (provisa/federation/stage_duckdb_extensions.py), so a pip install can run
it. Here, without PROVISA_DUCKDB_EXT_DIR, the extensions land in DuckDB's per-user directory.
"""

from __future__ import annotations

import sys

from provisa.federation.stage_duckdb_extensions import main

if __name__ == "__main__":
    sys.exit(main(default_directory=None))
