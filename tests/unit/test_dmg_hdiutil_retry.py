# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The macOS DMG builds retry `hdiutil create` on "Resource busy" alone, a bounded number of times.

GitHub's macOS runners fail it intermittently while XProtect holds the image being written
(actions/runner-images#7522, where retrying is the maintainers' workaround). Any other failure is
raised at once.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPTS = ("packaging/macos/build-dmg-obs.sh", "packaging/macos/build-dmg-demo.sh")


def _run(script: str, tmp_path: Path, answers: list[str]) -> subprocess.CompletedProcess:
    """Call the script's hdiutil_create_retry against a fake hdiutil answering ``answers`` in turn
    ("ok", or the error text it prints before failing)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (tmp_path / "answers").write_text("\n".join(answers) + "\n")
    (tmp_path / "calls").write_text("")
    fake = bin_dir / "hdiutil"
    fake.write_text(
        "#!/bin/bash\n"
        f'echo "$*" >> {tmp_path}/calls\n'
        f"answer=$(head -n1 {tmp_path}/answers); sed -i.bak 1d {tmp_path}/answers\n"
        '[ "$answer" = ok ] && { echo created; exit 0; }\n'
        'echo "hdiutil: create failed - $answer" >&2; exit 1\n'
    )
    fake.chmod(0o755)
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n")
    (bin_dir / "sleep").chmod(0o755)
    source = (_REPO / script).read_text()
    function = source[source.index("hdiutil_create_retry() {") :]
    function = function[: function.index("\n}\n") + 3]
    program = (
        'info() { echo "INFO $*"; }\nerr() { echo "ERR $*" >&2; }\n'
        + function
        + 'hdiutil_create_retry -volname "V" -srcfolder /src out.dmg\necho returned\n'
    )
    return subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
    )


@pytest.mark.parametrize("script", _SCRIPTS)
def test_the_dmg_is_created_through_the_retry(script):
    source = (_REPO / script).read_text()
    assert "\n  hdiutil create " not in source
    assert "hdiutil_create_retry -volname" in source


@pytest.mark.parametrize("script", _SCRIPTS)
def test_resource_busy_is_retried_until_it_clears(script, tmp_path):
    result = _run(script, tmp_path, ["Resource busy", "Resource busy", "ok"])
    assert result.returncode == 0, result.stderr
    assert "returned" in result.stdout
    assert len((tmp_path / "calls").read_text().splitlines()) == 3


@pytest.mark.parametrize("script", _SCRIPTS)
def test_any_other_failure_is_raised_at_once(script, tmp_path):
    result = _run(script, tmp_path, ["No space left on device", "ok"])
    assert result.returncode == 1
    assert "returned" not in result.stdout
    assert len((tmp_path / "calls").read_text().splitlines()) == 1


@pytest.mark.parametrize("script", _SCRIPTS)
def test_resource_busy_is_retried_a_bounded_number_of_times(script, tmp_path):
    result = _run(script, tmp_path, ["Resource busy"] * 6)
    assert result.returncode == 1
    assert len((tmp_path / "calls").read_text().splitlines()) == 5
