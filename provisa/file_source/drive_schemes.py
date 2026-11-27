# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6d8b13-7c4e-4a95-9e01-b3d5a7c2f846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The path schemes of the OneDrive and Google Drive File Crawler transports (REQ-2280).

Standard library only, so a module that only needs to know whether a path is in a drive
(an engine's connector) does not import the transports' sign-in (``cloud_drive``)."""

# Requirements: REQ-2280
from __future__ import annotations

ONEDRIVE = "onedrive"
GDRIVE = "gdrive"
#: The drive path schemes.
DRIVE_SCHEMES: frozenset[str] = frozenset({ONEDRIVE, GDRIVE})


def scheme_of(path: str) -> str | None:
    """The drive scheme ``path`` is in, or None when it is not a drive path."""
    scheme = path.split("://", 1)[0] if "://" in path else None
    return scheme if scheme in DRIVE_SCHEMES else None
