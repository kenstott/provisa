# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A demo reset takes its environments' model history, and only the demo's (REQ-1554, REQ-1535).

The reset drops the demo's control-plane database, every environment with it, and kept each org's
model repository: the next start's environments met branches with no environment behind them and
were refused. The demo now keeps its own data directory beside its control plane, and its reset
removes the model repositories there and nothing else -- not the orgs' files, and never a native
install's, which keeps ``~/.provisa``.

start-ui-install.sh is never run: the reset's own script runs against a temporary PROVISA_HOME, and
the start script's demo block is checked as text, in order.
"""

# Requirements: REQ-1535, REQ-1554

from __future__ import annotations

import subprocess
from pathlib import Path

from dulwich.repo import Repo

ROOT = Path(__file__).resolve().parents[2]
RESET = ROOT / "scripts" / "demo_reset_model_history.sh"
START = (ROOT / "start-ui-install.sh").read_text()


def _repo(path: Path) -> Path:
    path.parent.mkdir(parents=True)
    Repo.init_bare(str(path), mkdir=True).close()
    return path


def test_the_reset_takes_every_model_repository_of_the_demo_and_nothing_else(tmp_path):
    home = tmp_path / "provisa-home"
    demo = home / "demo"
    repos = [_repo(demo / "orgs" / org / "repo.git") for org in ("default", "starthere")]
    supplies = demo / "orgs" / "default" / "files" / "supplies.parquet"
    supplies.parent.mkdir(parents=True)
    supplies.write_bytes(b"PAR1")
    native = _repo(home / "orgs" / "default" / "repo.git")

    subprocess.run(["bash", str(RESET), str(demo)], check=True)

    assert not any(repo.exists() for repo in repos)
    assert supplies.read_bytes() == b"PAR1"  # the org's files are not model history
    assert (native / "objects").is_dir()  # a native install's history is never the demo's


def test_a_demo_with_no_organisations_yet_resets_cleanly(tmp_path):
    subprocess.run(["bash", str(RESET), str(tmp_path / "provisa-home" / "demo")], check=True)


def test_the_reset_names_no_directory_by_default():
    done = subprocess.run(["bash", str(RESET)], capture_output=True, text=True)
    assert done.returncode != 0
    assert "DEMO_DATA_DIR" in done.stderr


def test_the_demo_runs_on_its_own_data_directory_and_resets_its_history_there():
    block = START[START.index('if [ "$DEMO" = true ]; then\n  # A demo is native') :]
    export = block.index('export PROVISA_DATA_DIR="${PROVISA_HOME:-$HOME/.provisa}/demo"')
    plane = block.index("control_plane_pg reset")
    history = block.index('scripts/demo_reset_model_history.sh" "$PROVISA_DATA_DIR"')
    assert export < plane < history
    # The backend is started after, so it inherits the demo's data directory.
    assert START.index("demo_reset_model_history.sh") < START.index('/.venv/bin/uvicorn" main:app')
