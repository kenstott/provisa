# Copyright (c) 2026 Kenneth Stott
# Canary: e2fc01b3-b9f1-4d52-a0ea-5dbe6c344ffb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's code, as Provisa reads and runs it (REQ-1993).

- Its entry module is the code's single top-level import; when there is more than one, the
  source's registration names it (decision P1). The organisation's code is never changed for
  Provisa: no key in its files.
- Its functions are read by the reader (``reader.py``), run under the source's own Python inside
  the worker's sandbox, and judged by the shared rules (:mod:`provisa.codesource.manifest`).
- Its worker runs the host (``host.py``) under that Python, inside the same sandbox, speaking
  every code source's protocol (:mod:`provisa.codesource.worker`).

``python_home`` is the runtime's prefix (its ``bin`` and its standard library), which the sandbox
lets it read.
"""

# Requirements: REQ-1993
from __future__ import annotations

import json
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import PYTHON_RULES, Manifest, read_manifest
from provisa.codesource.sandbox import sandboxed, sandboxed_once
from provisa.codesource.worker import HostCommand, private_environment
from provisa.python.install import wheel_top_level

READER = Path(__file__).with_name("reader.py")
HOST = Path(__file__).with_name("host.py")

#: Top-level files a project carries that are not its code.
_NOT_CODE = frozenset({"setup.py", "conftest.py", "noxfile.py", "tasks.py", "manage.py"})

#: The environment a Python worker or reader gets beside PATH: it writes no bytecode (it may
#: write nowhere) and its output is not buffered (what it says before a crash is kept).
_PYTHON_ENV = {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}


def _top_level(code: Path) -> list[str]:
    names = []
    for path in sorted(code.iterdir()):
        if path.name.startswith((".", "_")) or path.name in _NOT_CODE:
            continue
        if path.is_file() and path.suffix == ".py" and not path.name.startswith("test"):
            names.append(path.stem)
        elif path.is_dir() and (path / "__init__.py").is_file() and path.name != "tests":
            names.append(path.name)
    return names


def code_path(installed: Path) -> Path:
    """Where the code's modules are, in its install: ``src/`` for a project laid out that way,
    else the install itself (an unpacked wheel, or a flat project)."""
    src = installed / "src"
    return src if src.is_dir() and _top_level(src) else installed


def entry_module(code: Path, named: str | None = None) -> str:
    """The module the code is entered by: ``named`` (from the source's registration) when given,
    which must be one of the code's; else its single top-level import -- a wheel's from its
    ``top_level.txt`` or ``RECORD``, a project's its single top-level module or package. None,
    or more than one with none named, is refused by name."""
    found = wheel_top_level(code) or _top_level(code)
    if named is not None:
        top = named.split(".")[0]
        if top not in found:
            raise BundleRefused(
                "no_entry",
                f"the source names the module {named}, and the code has no module {top}",
                entry=named,
            )
        return named
    if len(found) == 1:
        return found[0]
    if not found:
        raise BundleRefused("no_entry", "the code has no top-level Python module or package")
    raise BundleRefused(
        "entry_ambiguous",
        f"the code has {len(found)} top-level modules ({', '.join(found)}); the source names "
        "which one it is entered by",
        candidates=", ".join(found),
    )


#: Run the reader's argv with exactly ``env``; its stdout, refused by name when it fails.
ReaderRunner = Callable[[Sequence[str], dict[str, str]], dict]


def run_reader(argv: Sequence[str], env: dict[str, str]) -> dict:
    # The source's pinned Python and Provisa's own reader, with a fixed argv and no shell.
    done = subprocess.run(list(argv), capture_output=True, text=True, env=env)  # nosec B603
    if done.returncode != 0:
        raise BundleRefused(
            "reader_failed",
            f"the code's functions could not be read: {done.stderr.strip()[-2000:]}",
        )
    return json.loads(done.stdout)


def read_code(
    *,
    python: Path,
    python_home: Path,
    code: Path,
    entry: str,
    egress: Iterable[str] = (),
    run: ReaderRunner = run_reader,
) -> Manifest:
    """The code's tables, commands and streams, read from its functions; refused with every
    problem by name (:func:`provisa.codesource.manifest.read_manifest`)."""
    argv, env = sandboxed_once(
        [str(python), "-I", str(READER), str(code), entry],
        executable=python,
        read=[READER.parent, code, python_home],
        env=private_environment(**_PYTHON_ENV),
        egress=egress,
    )
    return read_manifest(run(argv, env), PYTHON_RULES)


def host_command(
    *,
    python: Path,
    python_home: Path,
    code: Path,
    entry: str,
    egress: Sequence[str],
    host_lib: Path,
) -> HostCommand:
    """How the source's Python runs the host on its code, sandboxed. ``host_lib`` is where the
    host's own pyarrow is (REQ-2098), which it reads its rows' schema with and sends them in."""
    return sandboxed(
        lambda address: [str(python), "-I", str(HOST), address, str(code), entry],
        executable=python,
        read=[HOST.parent, code, python_home, host_lib],
        env=private_environment(**_PYTHON_ENV, PROVISA_HOST_LIB=str(host_lib)),
        cwd=code,
        egress=egress,
    )
