# Copyright (c) 2026 Kenneth Stott
# Canary: 92aee53f-953e-40ed-8745-f78e3061cb97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A real platform plane for tests, on PostgreSQL (REQ-2014).

The simple provider keeps each user's id there (provisa/auth/simple_user_ids.py), so a test that
signs a simple user in needs one: the platform plane of its control_plane fixture
(``platform_db(control_plane)``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.core.database import Database

if TYPE_CHECKING:
    from tests.pg_control_plane import ControlPlane


def platform_db(plane: ControlPlane) -> Database:
    """The platform-plane Database of the test's control plane."""
    return plane.platform


def simple_user_ids(plane: ControlPlane):
    """The simple provider's id store over a platform plane."""
    from provisa.auth.simple_user_ids import SimpleUserIds

    return SimpleUserIds(platform_db(plane))
