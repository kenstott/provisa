"""No installer or start script installs the ``provisa`` package from a package index.

Nothing goes to PyPI since 2026-11-21 (the newest there is 0.1.0a509): every install takes the
Provisa it carries, a wheel in its own wheelhouse (``--no-index --find-links``) or a runtime it
copies. Third-party packages from an index stay allowed (maintainer 2026-11-22).
"""

from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]

#: The commands that fetch a Python package, as the words that start them.
_INSTALLERS = (
    ("pip", "install"),
    ("pip", "download"),
    ("pip3", "install"),
    ("pip3", "download"),
    ("pipx", "install"),
    ("uv", "pip", "install"),
    ("uv", "tool", "install"),
)

#: A requirement on the provisa distribution by name: provisa, provisa[extras], provisa==1.2 ...
_PROVISA_REQUIREMENT = re.compile(r"^provisa(\[[^\]]*\])?([=<>!~$].*)?$")

_SCRIPT_SUFFIXES = {".sh", ".ps1", ".cmd", ".bat", ".nix"}


def _scripts() -> list[Path]:
    """Every tracked installer, launcher and start script, and every Dockerfile."""
    tracked = subprocess.run(
        ["git", "-C", str(_ROOT), "ls-files"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    picked = []
    for name in tracked:
        path = Path(name)
        if path.name.startswith("Dockerfile"):
            picked.append(path)
        elif path.suffix in _SCRIPT_SUFFIXES and (
            path.parts[0] in {"packaging", "scripts"} or len(path.parts) == 1
        ):
            picked.append(path)
    return picked


def _commands(text: str) -> list[str]:
    """The script's command lines, with continuations (``\\`` and PowerShell's backtick) joined
    and comment lines dropped."""
    lines: list[str] = []
    pending = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not pending and (line.startswith("#") or line.upper().startswith("REM ")):
            continue
        if line.endswith("\\") or line.endswith("`"):
            pending += line[:-1] + " "
            continue
        lines.append(pending + line)
        pending = ""
    if pending:
        lines.append(pending)
    return lines


def _words(command: str) -> list[str]:
    try:
        return shlex.split(command, comments=True)
    except ValueError:
        return command.split()


def _index_installs_of_provisa(text: str) -> list[str]:
    """The commands that fetch provisa by name with an index in reach (no ``--no-index``)."""
    found = []
    for command in _commands(text):
        words = [w.strip("'\"") for w in _words(command)]
        for start in range(len(words)):
            for installer in _INSTALLERS:
                if not _matches(words, start, installer):
                    continue
                args = words[start + len(installer) :]
                args = args[: _end_of_command(args)]
                if "--no-index" in args:
                    continue
                if any(_PROVISA_REQUIREMENT.match(arg) for arg in args):
                    found.append(command)
    return found


def _matches(words: list[str], start: int, installer: tuple[str, ...]) -> bool:
    head = words[start : start + len(installer)]
    if len(head) != len(installer):
        return False
    # "${venv}/bin/pip", "$pip" and pip.exe are all pip.
    first = re.split(r"[/\\]", head[0])[-1].strip("${}").removesuffix(".exe")
    return first == installer[0] and head[1:] == list(installer[1:])


def _end_of_command(args: list[str]) -> int:
    for i, arg in enumerate(args):
        if arg in {";", "&&", "||", "|"} or arg.endswith(";"):
            return i
    return len(args)


@pytest.mark.parametrize(
    ("script", "caught"),
    [
        ('"$pip" install --quiet "provisa[${extras}]${pin}" uvicorn mcp-proxy', True),
        ("pip install provisa==0.1.0a511", True),
        ("uv pip install provisa", True),
        ("pipx install provisa", True),
        ('& $Py -m pip install "provisa[embedded]"', True),
        ('"$pip" install \\\n  "provisa[embedded]"', True),
        (
            '"$pip" install --quiet --no-index --find-links "$wheels" "provisa[${extras}]" uvicorn',
            False,
        ),
        ('"$pip" install --index-url https://download.pytorch.org/whl/cpu torch', False),
        ('pip install --find-links "$nb" "${built_wheel}[embedded]" uvicorn mcp-proxy', False),
        ('& $Py -m pip install --quiet --find-links $Nb "$RepoRoot[vector]" uvicorn', False),
        ("pip install provisa-client mcp-proxy", False),
        ("# pip install provisa[embedded] from PyPI", False),
    ],
)
def test_the_detector_tells_an_index_install_of_provisa_from_the_rest(script, caught):
    assert bool(_index_installs_of_provisa(script)) is caught


def test_the_scan_covers_every_installer():
    scanned = {str(p) for p in _scripts()}
    for installer in (
        "packaging/linux/first-launch.sh",
        "packaging/linux/build-appimage.sh",
        "packaging/macos/first-launch.sh",
        "packaging/macos/build-dmg.sh",
        "packaging/windows/first-launch-native.ps1",
        "packaging/windows/build-sfx.ps1",
        "start-ui-install.sh",
        "Dockerfile",
    ):
        assert installer in scanned


def test_no_installer_installs_provisa_from_an_index():
    offenders = {
        str(path): hits
        for path in _scripts()
        if (hits := _index_installs_of_provisa((_ROOT / path).read_text(errors="replace")))
    }
    assert offenders == {}
