# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0c0d0e-5a39-4f0e-9d0f-3c1d3a7a6e11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Developer prerequisites for setup.sh: reads the toolchain pins from the dev container's own files
(.devcontainer/devcontainer.json and Dockerfile), detects what is installed, and installs what is
missing or older than the pin, per tool after a yes/no (``--yes`` installs without asking).

Standard library only: setup.sh runs it before the project environment exists."""

from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping

REPO = Path(__file__).resolve().parents[1]
TOOLS = ("uv", "rust", "node", "go", "docker")
# Docker has no pin in the dev container (docker-in-docker is unversioned): present is enough, and it
# is needed only for the tests and demo stacks that start containers, so its absence does not stop setup.
OPTIONAL = {"docker"}
VERSION_COMMANDS = {
    "uv": ["uv", "--version"],
    "rust": ["cargo", "--version"],
    "node": ["node", "--version"],
    "go": ["go", "version"],
    "docker": ["docker", "--version"],
}


def parse_version(text: str) -> tuple[int, ...] | None:
    """The first dotted version in a tool's --version output, or None when there is none."""
    m = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", text)
    if m is None:
        return None
    return tuple(int(g) if g is not None else 0 for g in m.groups())


def read_pins(root: Path = REPO) -> dict[str, str]:
    """uv from the Dockerfile's COPY --from line; rust, go and node from devcontainer.json's features."""
    dc = json.loads((root / ".devcontainer" / "devcontainer.json").read_text())
    pins: dict[str, str] = {}
    for key, tool in (("rust", "rust"), ("go", "go"), ("node", "node")):
        feature = next(k for k in dc["features"] if k.split("/")[-1].split(":")[0] == key)
        pins[tool] = dc["features"][feature]["version"]
    m = re.search(
        r"ghcr\.io/astral-sh/uv:(\d+\.\d+\.\d+)",
        (root / ".devcontainer" / "Dockerfile").read_text(),
    )
    if m is None:
        raise ValueError("no ghcr.io/astral-sh/uv:<version> in .devcontainer/Dockerfile")
    pins["uv"] = m.group(1)
    return pins


def detect(
    run: Callable[[list[str]], str | None] | None = None,
) -> dict[str, tuple[int, ...] | None]:
    """Installed version per tool; None when the tool is absent or prints no version."""
    run = run or _run_version
    return {t: (parse_version(out) if (out := run(VERSION_COMMANDS[t])) else None) for t in TOOLS}


def _run_version(cmd: list[str]) -> str | None:
    if shutil.which(cmd[0]) is None:
        return None
    done = subprocess.run(cmd, capture_output=True, text=True)
    return done.stdout if done.returncode == 0 else None


@dataclass(frozen=True)
class Finding:
    tool: str
    status: str  # "ok" | "missing" | "old"
    found: tuple[int, ...] | None
    pin: str | None

    @property
    def optional(self) -> bool:
        return self.tool in OPTIONAL


def findings(
    pins: Mapping[str, str], detected: Mapping[str, tuple[int, ...] | None]
) -> list[Finding]:
    out = []
    for tool in TOOLS:
        found, pin = detected[tool], pins.get(tool)
        if found is None:
            status = "missing"
        elif pin is not None and found < (parse_version(pin) or (0,)):
            status = "old"
        else:
            status = "ok"
        out.append(Finding(tool, status, found, pin))
    return out


def _arch() -> str:
    return "arm64" if platform.machine().lower() in ("arm64", "aarch64") else "amd64"


def install_command(tool: str, pin: str | None, system: str, have_brew: bool) -> str:
    """The shell command that installs one tool at its pin (docker: current) on this platform."""
    arch = _arch()
    if system not in ("Darwin", "Linux"):
        raise ValueError(f"unsupported platform {system}: setup.sh supports macOS and Linux")
    mac = system == "Darwin"
    if tool == "rust":
        return (
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | "
            f"sh -s -- -y --profile minimal --default-toolchain {pin}"
        )
    if tool == "uv":
        if mac and have_brew:
            return "brew install uv"
        return f"curl -LsSf https://astral.sh/uv/{pin}/install.sh | sh"
    if tool == "node":
        major = (pin or "24").split(".")[0]
        if mac and have_brew:
            return f"brew install node@{major} && brew link --overwrite --force node@{major}"
        if mac:
            raise ValueError(
                "Node on macOS without Homebrew: install Homebrew (https://brew.sh) or Node "
                f"{pin} from https://nodejs.org"
            )
        node_arch = "arm64" if arch == "arm64" else "x64"
        return (
            f"mkdir -p ~/.local/node && curl -fsSL https://nodejs.org/dist/v{pin}/node-v{pin}-linux-{node_arch}.tar.xz"
            " | tar -xJ --strip-components=1 -C ~/.local/node"
        )
    if tool == "go":
        if mac and have_brew:
            return "brew install go"
        if mac:
            raise ValueError(
                f"Go on macOS without Homebrew: install Homebrew (https://brew.sh) or Go {pin} from https://go.dev/dl"
            )
        return f"rm -rf ~/.local/go && mkdir -p ~/.local && curl -fsSL https://go.dev/dl/go{pin}.linux-{arch}.tar.gz | tar -xz -C ~/.local"
    if tool == "docker":
        if mac:
            if not have_brew:
                raise ValueError(
                    "Docker on macOS without Homebrew: install Docker Desktop from https://docker.com"
                )
            return "brew install --cask docker"
        return "curl -fsSL https://get.docker.com | sh"
    raise ValueError(f"unknown tool {tool}")


def plan(
    pins: Mapping[str, str],
    detected: Mapping[str, tuple[int, ...] | None],
    system: str,
    have_brew: bool,
    assume_yes: bool,
    ask: Callable[[str], bool],
) -> tuple[list[tuple[str, str]], list[Finding]]:
    """(tool, command) to run, in order, and the findings left unresolved: declined or not installable.

    ``ask`` gets the prompt and returns the answer; it is not called when ``assume_yes``."""
    installs: list[tuple[str, str]] = []
    unresolved: list[Finding] = []
    for f in findings(pins, detected):
        if f.status == "ok":
            continue
        cmd = install_command(f.tool, f.pin, system, have_brew)
        what = (
            "missing"
            if f.status == "missing"
            else f"too old ({'.'.join(map(str, f.found))}, need {f.pin})"
        )
        if assume_yes or ask(f"{f.tool} is {what}. Install with `{cmd}`?"):
            installs.append((f.tool, cmd))
        else:
            unresolved.append(f)
    return installs, unresolved


def _ask_tty(prompt: str) -> bool:
    if not sys.stdin.isatty():
        return False
    return input(f"{prompt} [y/N] ").strip().lower() in ("y", "yes")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--yes", action="store_true", help="install every missing or too-old tool without asking"
    )
    args = ap.parse_args(argv)
    pins = read_pins()
    system = platform.system()
    have_brew = shutil.which("brew") is not None
    installs, _ = plan(pins, detect(), system, have_brew, args.yes, _ask_tty)
    for tool, cmd in installs:
        print(f"==> installing {tool}: {cmd}", flush=True)
        if subprocess.run(["bash", "-c", f"set -o pipefail; {cmd}"]).returncode != 0:
            print(f"{tool}: install failed", file=sys.stderr)
            return 1
    stop = False
    for f in findings(pins, detect()):
        if f.status == "ok":
            continue
        need = f" {f.pin}+" if f.pin else ""
        note = f"{f.tool}{need} is {f.status}"
        if f.optional:
            print(f"note: {note}; it is needed only for container tests and the demo stacks")
        else:
            print(
                f"error: {note}; install it (see docs/quickstart.md) or rerun with --yes",
                file=sys.stderr,
            )
            stop = True
    if any(t == "docker" for t, _ in installs):
        print("note: Docker Desktop needs a first launch before `docker` works")
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main())
