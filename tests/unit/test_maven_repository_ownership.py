# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1f74df-17d0-4933-903d-2414908b30a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Nothing the test harness runs leaves the machine's Maven repository owned by another user.

scripts/build_trino_functions.sh builds a plugin in a Maven container with the host's ~/.m2
mounted. Run as the image's root, it left root-owned directories there, and the JDBC driver's
integration test -- Maven on the host, later in the same lane -- failed with
"AccessDeniedException: /home/runner/.m2/repository/..." (run 37824835227, core 4/6). (That
test also builds in a repository of its own now; its owner made that change.)"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_the_plugin_build_container_runs_as_the_calling_user():
    script = (REPO / "scripts" / "build_trino_functions.sh").read_text()
    assert '--user "$(id -u):$(id -g)"' in script
    assert ":/root/.m2" not in script  # root's home is not where the repository is mounted
    assert '-v "$HOME/.m2:/var/maven/.m2"' in script
    assert "-e MAVEN_CONFIG=/var/maven/.m2" in script and "-Duser.home=/var/maven" in script
    # Made before the run: Docker creates a missing mount point itself, as root.
    assert script.index('mkdir -p "$HOME/.m2"') < script.index("docker run")


def test_no_script_mounts_the_maven_repository_into_a_root_home():
    offenders = [
        str(path.relative_to(REPO))
        for path in (*(REPO / "scripts").rglob("*.sh"), *(REPO / "scripts").rglob("*.py"))
        if re.search(r"\.m2:/root/", path.read_text(encoding="utf-8", errors="replace"))
    ]
    assert offenders == []
