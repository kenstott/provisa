# Copyright (c) 2026 Kenneth Stott
# Canary: af018fc5-5c14-4792-a9b2-041c4c61672a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Helm chart starts the API in a process mode the product accepts (REQ-1916).

PROVISA_MODE names the launch's process mode (every, query, coordinator). The chart still set
``PROVISA_MODE: production`` from an older meaning of the variable; the API refused it at boot
("unknown process mode 'production'") and crash-looped, and the cluster lane's helm install
timed out waiting for it.
"""

# Requirements: REQ-1916

from __future__ import annotations

from provisa.core import process_mode
from tests.unit.test_helm_auth import _LOCAL, _documents, _render


def test_the_api_container_is_started_in_a_process_mode_the_product_accepts():
    rendered = _render(*_LOCAL)
    assert rendered.returncode == 0, rendered.stderr
    deployment = next(
        d
        for d in _documents(rendered.stdout)
        if d.get("kind") == "Deployment" and d["metadata"]["name"].endswith("-provisa")
    )
    (api,) = [
        c for c in deployment["spec"]["template"]["spec"]["containers"] if c["name"] == "provisa"
    ]
    modes = [e.get("value") for e in api.get("env", []) if e["name"] == "PROVISA_MODE"]
    assert all(m in process_mode.MODES for m in modes), modes
