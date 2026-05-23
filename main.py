# Copyright (c) 2026 Kenneth Stott
# Canary: 0b22329f-1002-4c71-89f3-fda1a402e402
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
from pathlib import Path

_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            if not os.environ.get(_k.strip()):
                os.environ[_k.strip()] = _v.strip()

from provisa.api.app import create_app

app = create_app()
