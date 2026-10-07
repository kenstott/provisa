# Copyright (c) 2026 Kenneth Stott
# Canary: c4e19a70-8d25-4b3f-9e61-2a7f0d5c8b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The config error every bundled-connector source raises (REQ-955)."""

from __future__ import annotations


class MissingConnectorConfig(Exception):  # REQ-955
    """A pgwire-replica source is missing required creds/paths for its ``model.json`` operand. Raised
    on fail-loud config resolution; never a partial or defaulted operand."""
