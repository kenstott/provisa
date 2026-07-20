# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8d6e4b-9213-4a0d-8174-3d9f8a6b14ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build a transport adapter from a declarative spec (REQ-940).

The processor spec selects the transport by ``transport`` and carries its address; the toolkit owns
everything else. Keeps the config surface one small dict so a node can declare an external processor
without the caller knowing each adapter's constructor.
"""

from __future__ import annotations

from provisa.processors.contract import TransportAdapter
from provisa.processors.http import HttpAdapter
from provisa.processors.shell import ShellAdapter


def build_adapter(spec: dict) -> TransportAdapter:
    """Construct the transport adapter a processor ``spec`` declares (REQ-940).

    ``{"transport": "shell", "argv": [...], "timeout"?: float}`` or
    ``{"transport": "http", "url": "https://..."}``. gRPC is constructed directly with its injected
    bidi stream (no static address form). An unknown/missing transport fails loud — never a silent
    no-op processor."""
    transport = spec.get("transport")
    if transport == "shell":
        argv = spec.get("argv")
        if not isinstance(argv, list) or not argv:
            raise ValueError("shell processor spec requires a non-empty 'argv' list")
        return ShellAdapter(argv, timeout=float(spec.get("timeout", 30.0)))
    if transport == "http":
        url = spec.get("url")
        if not isinstance(url, str) or not url:
            raise ValueError("http processor spec requires a 'url'")
        return HttpAdapter(url)
    raise ValueError(
        f"unknown processor transport {transport!r}; expected 'shell' or 'http' "
        "(gRPC is constructed with an injected bidi stream)"
    )
