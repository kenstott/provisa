# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every shell script a named demo ships is executable in git.

A store's init scripts are bind-mounted into /docker-entrypoint-initdb.d. On Docker Desktop for Mac
a mounted 0644 script reads as executable, so the entrypoint runs it, fails with "Permission
denied", and the store restarts on an initialized directory without its init marker: unhealthy for
good (churnprod's 99-complete.sh, 2026-10-20).
"""

import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_every_demo_shell_script_is_executable_in_git():
    listing = subprocess.run(
        ["git", "ls-files", "-s", "demo/named"],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    not_executable = [
        line.split("\t", 1)[1]
        for line in listing.splitlines()
        if line.split("\t", 1)[1].endswith(".sh") and not line.startswith("100755")
    ]
    assert not_executable == []
