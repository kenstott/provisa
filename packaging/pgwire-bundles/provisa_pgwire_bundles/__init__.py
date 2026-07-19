# Copyright (c) 2026 Kenneth Stott
# Canary: e602848b-f6c0-418d-adc1-24869790002b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Locator for the bundled Calcite pgwire connector bundles.

The bundles live under ``_bundles/<version>/<connector>/bin/pgwire-<connector>`` (+ model/, lib/) —
the tree ``provisa.runtime_deps.pgwire_bundles.BundleResolver`` caches. Reading ``bundle_root()``
lets resolution stage the connector offline — no github.com/kenstott/calcite release round trip.
"""

from __future__ import annotations

from pathlib import Path

__version__ = "0.1.0"


def bundle_root() -> Path:
    """Absolute path to the embedded bundle tree (``<version>/<connector>/bin/pgwire-<connector>``)."""
    return Path(__file__).resolve().parent / "_bundles"
