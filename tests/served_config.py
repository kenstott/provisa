# Copyright (c) 2026 Kenneth Stott
# Canary: dc825f1c-bc41-4cda-a811-2a2a61478346
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config a served app always holds, for a test that drives the app (or its middleware)
without booting it.

A booted node loads its config before the first request (``_load_and_build``), and every request
reads what it declares -- the claim types its pseudo-roles come from (REQ-2057) among them. A test
that wires the app state by hand gives it this: a deployment that declares nothing of its own.
"""

from __future__ import annotations

from typing import Any


def served_config(**declared: Any):
    """A parsed config with no sources, domains, tables or roles, plus whatever ``declared``
    adds (``multitenancy=True``, ``claim_roles=[...]``)."""
    from provisa.core.config_loader import parse_config_dict

    return parse_config_dict({"sources": [], "domains": [], "tables": [], "roles": [], **declared})
