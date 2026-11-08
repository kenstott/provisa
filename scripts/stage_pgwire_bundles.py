#!/usr/bin/env python
# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4c27e8-5b61-4f03-a8e7-2c1b6f90d3a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Stage the pgwire connector bundles on a source checkout or in an image build (REQ-294).

The step itself is packaged (provisa/runtime_deps/stage_pgwire_bundles.py), so an install can run it.
"""

from __future__ import annotations

import sys

from provisa.runtime_deps.stage_pgwire_bundles import main, stage

__all__ = ["main", "stage"]

if __name__ == "__main__":
    sys.exit(main())
