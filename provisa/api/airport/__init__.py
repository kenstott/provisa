# Copyright (c) 2026 Kenneth Stott
# Canary: 3ca67afe-b5d0-43d3-b483-c47eb9f5caef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa airport Flight service (REQ-1106) — serves the DuckDB `airport`
community-extension protocol over the governed query pipeline."""

from provisa.api.airport.service import start_airport_server

__all__ = ["start_airport_server"]
