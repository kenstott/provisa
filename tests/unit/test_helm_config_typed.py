# Copyright (c) 2026 Kenneth Stott
# Canary: bc434c40-28f6-4d15-8416-a1ce98c98cfd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The chart's own config loads: every column carries its type (REQ-1426).

A column's data_type is design-time metadata the config carries; config load infers nothing and
refuses an untyped column. The chart's provisa.yaml had none, so in the cluster lane the API's boot
stopped at "column orders.id has no data_type".
"""

# Requirements: REQ-1426

from __future__ import annotations

import yaml

from provisa.core.ir_types import to_ir
from tests.unit.test_helm_auth import _LOCAL, _documents, _render


def test_every_column_in_the_charts_config_has_a_type_the_ir_knows():
    rendered = _render(*_LOCAL, "mongodb.enabled=true")
    assert rendered.returncode == 0, rendered.stderr
    config = next(
        d
        for d in _documents(rendered.stdout)
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"].endswith("-config")
    )
    tables = yaml.safe_load(config["data"]["provisa.yaml"])["tables"]
    assert tables
    for table in tables:
        for column in table["columns"]:
            assert column.get("data_type"), (table["table"], column["name"])
            to_ir(column["data_type"])  # raises on a type outside the vocabulary
