# Copyright (c) 2026 Kenneth Stott
# Canary: 1808e9b5-b468-4fac-bb32-f93d7b0905df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The release's Python dependency set for scripts/deploy-cloud.sh.

deploy-cloud pushes the source tree onto a node's provisa/provisa:local image; it never rebuilds
the image. A release that adds a dependency therefore reached cloud-dev as code importing a package
the image did not have, and the coordinator and the sandbox VM crash-looped (511, 2026-11-23). This
helper gives the script the dependency set the Dockerfile installs and the means to tell whether a
node's image already holds it:

  stage REPO OUTDIR   export the set as the Dockerfile does (same uv export, same extras, same torch
                      split) and write OUTDIR/{requirements.txt,torch.txt,Dockerfile,fingerprint,
                      deploy_deps.py}; print the fingerprint
  plan WANT HAVE      "install" or "current" for one node, from the fingerprint this checkout wants
                      and the one the node's image records (empty: the image records none)
  check REQUIREMENTS  run inside the node's image: exit 1 naming every requirement the interpreter
                      does not satisfy at its locked version
"""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path

# The file the dependency layer writes into the image. It lives in the image rather than beside it:
# a node whose image is replaced (first-launch reloads it from the release on every boot) loses the
# record along with the packages, so the next deploy installs again.
RECORD_PATH = "/etc/provisa-deps.sha256"

# The Dockerfile's filter: torch (and the CUDA stack it would pull) comes from the CPU index, and
# the editable provisa-notebook is built into the image by its own stage.
_EXCLUDED = re.compile(r"^(torch|triton|nvidia-|cuda-|-e )")
_TORCH_CPU = re.compile(r"^torch==\S+\+cpu ")
_EXTRAS_ARG = re.compile(r"^ARG PROVISA_EXTRAS=(\S*)$", re.MULTILINE)
_REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==(\S+)(?:\s*;\s*(.+))?$")


def dockerfile_extras(dockerfile: str) -> list[str]:
    """The extra set the release image is built with: the Dockerfile's PROVISA_EXTRAS default."""
    match = _EXTRAS_ARG.search(dockerfile)
    if match is None:
        raise ValueError("Dockerfile declares no ARG PROVISA_EXTRAS default")
    return [extra for extra in match.group(1).split(",") if extra]


def fingerprint(pyproject: bytes, lock: bytes, extras: list[str]) -> str:
    """One hash over everything that decides the exported set."""
    digest = hashlib.sha256()
    for part in (pyproject, lock, ",".join(extras).encode()):
        digest.update(hashlib.sha256(part).digest())
    return digest.hexdigest()


def split_locked(locked: str) -> tuple[str, list[str]]:
    """(the Linux CPU torch pin, every other requirement line), as the Dockerfile splits them."""
    lines = [line.strip() for line in locked.splitlines() if line.strip()]
    torch = [line.split(" ", 1)[0] for line in lines if _TORCH_CPU.match(line)]
    if len(torch) != 1:
        raise ValueError(f"expected one Linux torch+cpu pin in the export, found {len(torch)}")
    return torch[0], [line for line in lines if not _EXCLUDED.match(line)]


def plan(want: str, have: str) -> str:
    """Whether a node's image needs the set installed."""
    if not re.fullmatch(r"[0-9a-f]{64}", want):
        raise ValueError(f"not a fingerprint: {want!r}")
    return "current" if have == want else "install"


def layer_dockerfile() -> str:
    """The layer that installs the set onto a node's image and records its fingerprint."""
    return (
        "ARG BASE\n"
        "FROM ${BASE}\n"
        "COPY requirements.txt torch.txt fingerprint /tmp/provisa-deps/\n"
        'RUN pip install --no-cache-dir "$(cat /tmp/provisa-deps/torch.txt)" \\\n'
        "      --index-url https://download.pytorch.org/whl/cpu \\\n"
        " && pip install --no-cache-dir --no-deps -r /tmp/provisa-deps/requirements.txt \\\n"
        f" && cp /tmp/provisa-deps/fingerprint {RECORD_PATH} \\\n"
        " && rm -rf /tmp/provisa-deps\n"
    )


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def unmet(
    requirements: list[str],
    installed: Mapping[str, str],
    applies: Callable[[str], bool],
) -> list[str]:
    """Each requirement whose marker applies here but whose locked version is not installed.

    `installed` maps canonical distribution names to versions; `applies(marker)` evaluates a PEP 508
    marker for this interpreter.
    """
    missing: list[str] = []
    for line in requirements:
        match = _REQUIREMENT.match(line)
        if match is None:
            raise ValueError(f"not a pinned requirement: {line!r}")
        name, version, marker = match.groups()
        if marker and not applies(marker):
            continue
        have = installed.get(_canonical(name))
        if have != version:
            missing.append(f"{name}=={version} (installed: {have or 'none'})")
    return missing


def _stage(repo: Path, out: Path) -> str:
    extras = dockerfile_extras((repo / "Dockerfile").read_text())
    fp = fingerprint(
        (repo / "pyproject.toml").read_bytes(), (repo / "uv.lock").read_bytes(), extras
    )
    locked = subprocess.run(
        [
            "uv",
            "export",
            "--frozen",
            "--no-dev",
            "--no-emit-project",
            "--no-hashes",
            "--no-annotate",
            "--no-header",
            *[a for e in extras for a in ("--extra", e)],
        ],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    torch, requirements = split_locked(locked)
    out.mkdir(parents=True, exist_ok=True)
    (out / "torch.txt").write_text(torch + "\n")
    (out / "requirements.txt").write_text("\n".join(requirements) + "\n")
    (out / "Dockerfile").write_text(layer_dockerfile())
    (out / "fingerprint").write_text(fp + "\n")
    shutil.copy(Path(__file__), out / "deploy_deps.py")
    return fp


def _check(requirements_file: Path) -> list[str]:
    from importlib.metadata import distributions

    from packaging.markers import Marker

    installed = {_canonical(d.metadata["Name"]): d.version for d in distributions()}
    lines = [line.strip() for line in requirements_file.read_text().splitlines() if line.strip()]
    return unmet(lines, installed, lambda marker: Marker(marker).evaluate())


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[0] == "stage":
        print(_stage(Path(argv[1]), Path(argv[2])))
        return 0
    if len(argv) == 3 and argv[0] == "plan":
        print(plan(argv[1], argv[2]))
        return 0
    if len(argv) == 2 and argv[0] == "check":
        missing = _check(Path(argv[1]))
        for item in missing:
            print(f"unmet: {item}", file=sys.stderr)
        return 1 if missing else 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
