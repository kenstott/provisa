# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41e257-533e-484b-9c91-723d924e5033
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The macOS DMG notarization staples through a bounded retry and read Apple's notarization log.

`stapler staple` fetches the ticket from Apple's CloudKit ticket-delivery service, and that
connection drops on GitHub's macOS runners (NSURLErrorDomain -1005, then "Error 68"): the Core
DMG on v0.1.0-alpha.505 and the Container DMG on v0.1.0-alpha.506. Apple's notarization log is
printed for every submission and any issue it lists fails the notarization.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_LIB = _REPO / "packaging/macos/notary-lib.sh"
_SCRIPTS = ("packaging/macos/notarize-dmg.sh",)


def _run(
    tmp_path: Path, call: str, staple_answers: list[int], log_json: str = ""
) -> subprocess.CompletedProcess:
    """Run ``call`` with notary-lib.sh sourced, against a fake xcrun.

    ``staple_answers`` are the exit codes `xcrun stapler staple` returns in turn; `xcrun notarytool
    log` prints ``log_json``.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (tmp_path / "answers").write_text("".join(f"{a}\n" for a in staple_answers))
    (tmp_path / "calls").write_text("")
    (tmp_path / "log.json").write_text(log_json)
    fake = bin_dir / "xcrun"
    fake.write_text(
        "#!/bin/bash\n"
        f'echo "$*" >> {tmp_path}/calls\n'
        'if [ "$1 $2" = "notarytool log" ]; then\n'
        f"  cat {tmp_path}/log.json; exit 0\n"
        "fi\n"
        f"answer=$(head -n1 {tmp_path}/answers); sed -i.bak 1d {tmp_path}/answers\n"
        '[ "$answer" = 0 ] && { echo "The staple and validate action worked!"; exit 0; }\n'
        'echo "The staple and validate action failed! Error $answer." >&2; exit "$answer"\n'
    )
    fake.chmod(0o755)
    (bin_dir / "sleep").write_text("#!/bin/sh\nexit 0\n")
    (bin_dir / "sleep").chmod(0o755)
    program = (
        "set -euo pipefail\n"
        'info() { echo "INFO $*"; }\nok() { echo "OK $*"; }\nerr() { echo "ERR $*" >&2; }\n'
        f'source "{_LIB}"\n' + call + "\necho returned\n"
    )
    return subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
    )


def _staple_calls(tmp_path: Path) -> list[str]:
    return [
        c for c in (tmp_path / "calls").read_text().splitlines() if c.startswith("stapler staple")
    ]


@pytest.mark.parametrize("script", _SCRIPTS)
def test_every_notarizing_script_staples_through_the_retry_and_reads_the_log(script):
    source = (_REPO / script).read_text()
    assert 'source "${SCRIPT_DIR}/notary-lib.sh"' in source
    assert "xcrun stapler staple" not in source
    assert "staple_retry " in source
    # Read on acceptance and on rejection alike.
    assert source.count('notary_log_check "$submission_id"') == 2
    assert "notarytool log" not in source


def test_a_dropped_staple_connection_is_retried_until_it_staples(tmp_path):
    result = _run(tmp_path, "staple_retry /out/Provisa.app", [68, 68, 0])
    assert result.returncode == 0, result.stderr
    assert "returned" in result.stdout
    assert len(_staple_calls(tmp_path)) == 3
    assert "stapler attempt 1 failed (exit 68)" in result.stdout
    assert "stapler attempt 2 failed (exit 68)" in result.stdout


def test_stapling_is_retried_a_bounded_number_of_times_then_fails(tmp_path):
    result = _run(tmp_path, "staple_retry /out/Provisa.app", [68] * 6)
    assert result.returncode == 1
    assert "returned" not in result.stdout
    assert len(_staple_calls(tmp_path)) == 5
    assert "stapler staple failed 5 times for /out/Provisa.app (last exit 68)" in result.stderr


def test_a_log_without_issues_is_printed_and_passes(tmp_path):
    log = '{"status": "Accepted", "issues": null}'
    result = _run(tmp_path, "notary_log_check sub-1 --team-id T", [], log)
    assert result.returncode == 0, result.stderr
    assert log in result.stdout
    assert "returned" in result.stdout


def test_a_log_listing_any_issue_fails_the_build_even_when_accepted(tmp_path):
    log = '{"status": "Accepted", "issues": [{"severity": "warning", "message": "no secure timestamp"}]}'
    result = _run(tmp_path, "notary_log_check sub-1 --team-id T", [], log)
    assert result.returncode == 1
    assert "no secure timestamp" in result.stdout
    assert "lists 1 blocking issue(s)" in result.stderr
    assert "returned" not in result.stdout


_IMAGE_WARNING = (
    '{"severity": "warning", "path": "Provisa-Demo.dmg/images/petstore-demo-local.tar.gz/'
    'petstore-demo-local.tar/blobs/sha256/acd4/acd4/var/lib/dpkg/alternatives/builtins.7.gz", '
    '"message": "The archive at Provisa-Demo.dmg/images/petstore-demo-local.tar.gz/x could not be '
    'unpacked. Any executables contained in the archive will not be notarized."}'
)


def test_an_unpackable_archive_inside_a_bundled_container_image_is_allowed_and_logged(tmp_path):
    log = '{"status": "Accepted", "issues": [' + _IMAGE_WARNING + "]}"
    result = _run(tmp_path, "notary_log_check sub-1 --team-id T", [], log)
    assert result.returncode == 0, result.stderr
    assert "allowed (Linux container image content, not executed on macOS)" in result.stderr
    assert "lists no blocking issues" in result.stdout
    assert "returned" in result.stdout


def test_any_other_issue_beside_an_allowed_one_still_fails(tmp_path):
    other = '{"severity": "warning", "path": "Provisa.app/Contents/MacOS/x", "message": "no secure timestamp"}'
    log = '{"status": "Accepted", "issues": [' + _IMAGE_WARNING + ", " + other + "]}"
    result = _run(tmp_path, "notary_log_check sub-1 --team-id T", [], log)
    assert result.returncode == 1
    assert "lists 1 blocking issue(s)" in result.stderr


def test_an_error_inside_a_container_image_is_never_allowed(tmp_path):
    log = (
        '{"status": "Accepted", "issues": [' + _IMAGE_WARNING.replace('"warning"', '"error"') + "]}"
    )
    result = _run(tmp_path, "notary_log_check sub-1 --team-id T", [], log)
    assert result.returncode == 1
    assert "lists 1 blocking issue(s)" in result.stderr
