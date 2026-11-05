# Copyright (c) 2026 Kenneth Stott
# Canary: eadd664f-6e7f-4677-8f45-415a2c732f2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The checked-in Flight SQL bindings are what scripts/gen_flightsql_proto.py generates from
provisa/proto/FlightSql.proto (REQ-2175). Regenerate with:

    PYTHONPATH=. .venv/bin/python3 scripts/gen_flightsql_proto.py
"""

# Requirements: REQ-2175

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "gen_flightsql_proto.py"


def test_the_flightsql_bindings_are_current():
    spec = importlib.util.spec_from_file_location("gen_flightsql_proto", _SCRIPT)
    assert spec is not None and spec.loader is not None
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    for path, generated in gen.render().items():
        # The canary stamp's line is the repository's, not protoc's.
        checked_in = re.sub(r"(?m)^# Canary: .*\n", "", path.read_text(encoding="utf-8"))
        assert checked_in == generated, f"{path.name} is not current"
