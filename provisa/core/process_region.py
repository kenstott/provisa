# Copyright (c) 2026 Kenneth Stott
# Canary: 83c08ccf-cacd-4afb-8e81-b37f043d1242
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The region a process runs in, and its launch (REQ-1916, REQ-1922).

A node is launched in a mode (``process_mode``) and — when the platform declares regions — in one
of them: ``provisa run --mode <every|query|coordinator> --region <id>``, or ``PROVISA_MODE`` /
``PROVISA_REGION`` for a launch that does not go through the command. The launch is checked before
any store is opened: a platform that declares regions refuses a node without one, listing them;
a region the platform does not declare is refused; and a platform that declares none refuses a
region. The node then serves its region of every org that selects it.
"""

# Requirements: REQ-1916, REQ-1922

from __future__ import annotations

import os
from typing import Any

from provisa.core import process_mode
from provisa.core.regions import DEFAULT_REGION, PlatformConfig

_region: str | None = None
#: The regions the platform declares, bound with the launch.
_platform_regions: tuple[str, ...] = ()


class LaunchRefused(SystemExit):
    """The node's launch does not fit the platform; it does not start. A ``SystemExit`` so the
    launch ends with the message, not a traceback."""

    def __init__(self, message: str) -> None:
        super().__init__(f"provisa: {message}")
        self.message = message

    def __str__(self) -> str:
        return self.message


def bind_launch(platform: dict[str, Any] | None, *, requested: str | None) -> str:
    """Check the region the node was launched with (``requested``, None for none) against the
    platform's declared regions (``platform``: the config's ``platform`` key) and make it the
    node's. Returns the node's region."""
    global _region, _platform_regions
    declared = PlatformConfig.model_validate(platform or {}).regions
    _platform_regions = tuple(r.id for r in declared)
    listed = ", ".join(f"{r.id} ({r.address})" for r in declared)
    if not declared:
        if requested is not None:
            raise LaunchRefused(
                f"--region {requested!r} was given, but the platform declares no regions"
            )
        # REQ-1922 amendment (2026-10-03): with no platform regions a node runs in the ONE
        # implicit region, the way single-tenant runs as the implicit "default" org.
        _region = DEFAULT_REGION
        return DEFAULT_REGION
    if requested is None:
        raise LaunchRefused(
            f"the platform declares regions, so a node is started with --region <id>: {listed}"
        )
    if requested not in {r.id for r in declared}:
        raise LaunchRefused(f"region {requested!r} is not one of the platform's: {listed}")
    _region = requested
    return requested


def bind_from_environment(raw_config: dict[str, Any]) -> None:
    """Bind the launch's mode and region from ``PROVISA_MODE`` / ``PROVISA_REGION`` (set by
    ``provisa run --mode/--region``), checked against the config's ``platform`` key."""
    requested_mode = os.environ.get("PROVISA_MODE")
    if requested_mode is not None:
        if requested_mode not in process_mode.MODES:
            raise LaunchRefused(
                f"unknown process mode {requested_mode!r}; expected one of "
                f"{', '.join(process_mode.MODES)}"
            )
        process_mode.set_mode(requested_mode)
    bind_launch(raw_config.get("platform"), requested=os.environ.get("PROVISA_REGION") or None)


def platform_regions() -> tuple[str, ...]:
    """The regions the platform declares (empty for none), bound with the launch."""
    if _region is None:
        raise RuntimeError("the platform's regions are read before the launch bound them")
    return _platform_regions


def region() -> str:
    """The region this process serves. Bound at launch; asking before the launch is a defect."""
    if _region is None:
        raise RuntimeError("the process region is read before the launch bound it")
    return _region
