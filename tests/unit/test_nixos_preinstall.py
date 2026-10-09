# Copyright (c) 2026 Kenneth Stott
# Canary: 56c9ba59-b28d-4aa1-95c0-99172db75260
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""install.sh on NixOS passes a host whose settings are the version it expects. The settings
(packaging/nixos/preinstall.nix) record their version on the host; the installer reads it."""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).parents[2]


def test_the_installer_expects_the_version_the_settings_record():
    settings = (_ROOT / "packaging/nixos/preinstall.nix").read_text()
    installer = (_ROOT / "install.sh").read_text()
    (recorded,) = re.findall(
        r'environment\.etc\."provisa/nixos-preinstall"\.text = "(\d+)\\n";', settings
    )
    (expected,) = re.findall(r'^NIXOS_PREINSTALL_VERSION="(\d+)"$', installer, re.M)
    assert recorded == expected


def test_the_installer_reads_the_file_the_settings_write():
    installer = (_ROOT / "install.sh").read_text()
    assert 'NIXOS_PREINSTALL_MARKER="/etc/provisa/nixos-preinstall"' in installer
