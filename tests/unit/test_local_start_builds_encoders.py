# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A start from source builds provisa._encoders when provisa/_encoders.so is missing or older than
its Rust sources, and skips the build when it is fresh (REQ-2194).

rust/ensure-local-build.sh is run in a scratch checkout whose rust/build-local.sh is a stub that
records it ran and writes the module; cargo is a stub on a PATH of its own, or absent. The start
scripts are never run: that they call it before the API starts is checked as text, in order.
"""

# Requirements: REQ-2194

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ENSURE = REPO / "rust" / "ensure-local-build.sh"


def _checkout(tmp_path: Path, *, build_fails: bool = False) -> Path:
    """A scratch checkout: rust/ with the real ensure script, a stub build-local.sh and a few Rust
    sources, and provisa/."""
    root = tmp_path / "checkout"
    (root / "rust" / "encoders" / "src").mkdir(parents=True)
    (root / "rust" / "pgwire" / "third_party" / "ryu-pg").mkdir(parents=True)
    (root / "rust" / "encoders" / "target" / "release").mkdir(parents=True)
    (root / "provisa").mkdir()
    shutil.copy(ENSURE, root / "rust" / "ensure-local-build.sh")
    build = root / "rust" / "build-local.sh"
    build.write_text(
        "#!/bin/bash\n"
        f'echo built >> "{tmp_path}/builds"\n'
        + ("exit 3\n" if build_fails else f'echo module > "{root}/provisa/_encoders.so"\n')
    )
    build.chmod(0o755)
    for name in (
        "rust/encoders/Cargo.toml",
        "rust/encoders/Cargo.lock",
        "rust/encoders/src/lib.rs",
        "rust/pgwire/third_party/ryu-pg/Cargo.toml",
        "rust/encoders/target/release/lib_encoders.dylib",
    ):
        (root / name).write_text("x\n")
        os.utime(root / name, (1_000_000, 1_000_000))
    return root


def _run(tmp_path: Path, root: Path, *, cargo: bool = True) -> tuple[int, str, int]:
    """Run the ensure script with only find and dirname on PATH, plus a stub cargo when ``cargo``:
    (exit code, output, how many times build-local.sh ran)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for tool in ("find", "dirname"):
        link = bin_dir / tool
        if not link.exists():
            link.symlink_to(shutil.which(tool) or f"/usr/bin/{tool}")
    stub = bin_dir / "cargo"
    if cargo:
        stub.write_text("#!/bin/bash\nexit 0\n")
        stub.chmod(0o755)
    elif stub.exists():
        stub.unlink()
    done = subprocess.run(
        ["/bin/bash", str(root / "rust" / "ensure-local-build.sh")],
        capture_output=True,
        text=True,
        env={"PATH": str(bin_dir)},
    )
    builds = tmp_path / "builds"
    count = len(builds.read_text().splitlines()) if builds.exists() else 0
    return done.returncode, done.stdout + done.stderr, count


def _module(root: Path, mtime: int) -> Path:
    so = root / "provisa" / "_encoders.so"
    so.write_text("module\n")
    os.utime(so, (mtime, mtime))
    return so


def test_a_missing_module_is_built(tmp_path: Path) -> None:
    root = _checkout(tmp_path)
    code, out, builds = _run(tmp_path, root)
    assert (code, builds) == (0, 1), out
    assert "is missing" in out
    assert (root / "provisa" / "_encoders.so").exists()


def test_a_fresh_module_is_not_rebuilt(tmp_path: Path) -> None:
    root = _checkout(tmp_path)
    _module(root, 2_000_000)
    code, out, builds = _run(tmp_path, root)
    assert (code, builds, out) == (0, 0, "")


def test_a_module_older_than_a_source_file_is_rebuilt(tmp_path: Path) -> None:
    for source in (
        "rust/encoders/src/lib.rs",
        "rust/encoders/Cargo.toml",
        "rust/encoders/Cargo.lock",
        "rust/pgwire/third_party/ryu-pg/Cargo.toml",
    ):
        case = tmp_path / source.replace("/", "_")
        case.mkdir()
        root = _checkout(case)
        _module(root, 2_000_000)
        os.utime(root / source, (3_000_000, 3_000_000))
        code, out, builds = _run(case, root)
        assert (code, builds) == (0, 1), (source, out)
        assert f"older than {root / source}" in out


def test_cargo_output_newer_than_the_module_does_not_rebuild_it(tmp_path: Path) -> None:
    root = _checkout(tmp_path)
    _module(root, 2_000_000)
    os.utime(root / "rust/encoders/target/release/lib_encoders.dylib", (3_000_000, 3_000_000))
    code, out, builds = _run(tmp_path, root)
    assert (code, builds, out) == (0, 0, "")


def test_no_cargo_stops_the_start_naming_cargo(tmp_path: Path) -> None:
    root = _checkout(tmp_path)
    code, out, builds = _run(tmp_path, root, cargo=False)
    assert (code, builds) == (1, 0)
    assert "cargo not found" in out
    assert "provisa._encoders" in out


def test_no_cargo_is_no_matter_when_the_module_is_fresh(tmp_path: Path) -> None:
    root = _checkout(tmp_path)
    _module(root, 2_000_000)
    code, out, builds = _run(tmp_path, root, cargo=False)
    assert (code, builds, out) == (0, 0, "")


def test_a_failed_build_stops_the_start(tmp_path: Path) -> None:
    root = _checkout(tmp_path, build_fails=True)
    code, out, builds = _run(tmp_path, root)
    assert (code, builds) == (1, 1)
    assert "building provisa._encoders failed" in out


def _ensure_before_backend(script: str, launch: str) -> None:
    text = (REPO / script).read_text()
    call = text.index('"$SCRIPT_DIR/rust/ensure-local-build.sh" || exit 1')
    assert call < text.index(launch)
    assert "rust/build-local.sh" in text[: text.index("set -euo pipefail")]


def test_start_ui_install_builds_the_module_before_the_api_starts() -> None:
    _ensure_before_backend("start-ui-install.sh", "\nstart_backend\n")


def test_start_ui_builds_the_module_before_the_api_starts() -> None:
    _ensure_before_backend("start-ui.sh", "\n_start_backend\n")
