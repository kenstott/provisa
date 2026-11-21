# Copyright (c) 2026 Kenneth Stott
# Canary: 1f8c4d27-b903-4e6a-a1d7-52e0c9b37f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Builds the wheel of the Spark Connect client this connector pins (REQ-2225).

PyPI carries pyspark-client as an sdist only, and a connector image is installed from wheels only.
This builds that wheel, from the sdist the project's uv.lock names and checked against its hash,
and writes the requirements.txt the image installs from: the wheel by its own hash, the rest as
uv.lock pins them. uv.lock is removed from the project being built, because the image build reads
it ahead of requirements.txt and it holds the sdist's hash, which no wheel matches.

Two ways to use it. Run from the repository's root, as a connector's build command:

    provisa connector build ... --path connectors/spark \\
        --build-command "python3 connectors/spark/_build_client.py"

which keeps the wheel in the project, so the build needs no private index. Or, for a private
index, ``--for-index DIR`` puts the wheel in DIR to upload, and names the client by version:

    python3 connectors/spark/_build_client.py --for-index dist/
    provisa connector build ... --package-registries registries.yaml

The wheel of one sdist is the same bytes on every build (SOURCE_DATE_EPOCH), so its hash holds.
Needs only uv on the PATH.
"""

# Requirements: REQ-2225

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from pathlib import Path

CLIENT = "pyspark-client"
_PROJECT = Path(__file__).resolve().parent
_WHEELS = "wheels"


def _locked_sdist(project: Path) -> tuple[str, str, str]:
    """The client's version, and its sdist's address and SHA-256, from the project's uv.lock."""
    packages = tomllib.loads((project / "uv.lock").read_text())["package"]
    found = next(p for p in packages if p["name"] == CLIENT)
    sdist = found["sdist"]
    algorithm, digest = sdist["hash"].split(":", 1)
    if algorithm != "sha256":
        raise SystemExit(f"uv.lock holds a {algorithm} hash for {CLIENT}, not sha256")
    return found["version"], sdist["url"], digest


def _download(url: str, digest: str, dest: Path) -> None:
    with urllib.request.urlopen(url) as response:  # noqa: S310 -- the https address uv.lock pins
        body = response.read()
    actual = hashlib.sha256(body).hexdigest()
    if actual != digest:
        raise SystemExit(f"{url} has sha256 {actual}, not the {digest} uv.lock pins")
    dest.write_bytes(body)


def _build_wheel(uv: str, sdist: Path, out: Path) -> Path:
    env = {**os.environ, "SOURCE_DATE_EPOCH": "315532800"}  # the earliest a zip entry can hold
    subprocess.run([uv, "build", "--wheel", "--out-dir", str(out), str(sdist)], check=True, env=env)
    (wheel,) = out.glob("*.whl")
    return wheel


def _others(uv: str, project: Path) -> str:
    """Everything but the client, as uv.lock pins it, each with its hashes."""
    return subprocess.run(
        [uv, "export", "--project", str(project), "--frozen", "--no-emit-project",
         "--no-emit-package", CLIENT, "--format", "requirements-txt", "--no-header"],
        check=True, capture_output=True, text=True,
    ).stdout  # fmt: skip


def build(project: Path, for_index: Path | None) -> Path:
    """Builds the wheel and writes ``project``'s requirements.txt; the path of the wheel."""
    uv = shutil.which("uv")
    if uv is None:
        raise SystemExit("uv is not on the PATH")
    version, url, digest = _locked_sdist(project)
    with tempfile.TemporaryDirectory(prefix="provisa-spark-client-") as scratch:
        sdist = Path(scratch) / url.rsplit("/", 1)[1]
        _download(url, digest, sdist)
        wheel = _build_wheel(uv, sdist, Path(scratch) / "out")
        out = for_index if for_index is not None else project / _WHEELS
        out.mkdir(parents=True, exist_ok=True)
        kept = out / wheel.name
        shutil.copyfile(wheel, kept)
    hashed = hashlib.sha256(kept.read_bytes()).hexdigest()
    name = f"{CLIENT}=={version}" if for_index is not None else f"./{_WHEELS}/{kept.name}"
    (project / "requirements.txt").write_text(
        f"{name} --hash=sha256:{hashed}\n{_others(uv, project)}"
    )
    (project / "uv.lock").unlink()
    return kept


def main() -> int:
    parser = argparse.ArgumentParser(description="Builds the Spark Connect client's wheel.")
    parser.add_argument("--project", type=Path, default=_PROJECT, help="The connector project")
    parser.add_argument(
        "--for-index",
        type=Path,
        default=None,
        metavar="DIR",
        help="Put the wheel in DIR to upload to a private index, and name the client by version",
    )
    args = parser.parse_args()
    print(build(args.project.resolve(), args.for_index), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
