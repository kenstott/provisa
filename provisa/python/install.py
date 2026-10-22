# Copyright (c) 2026 Kenneth Stott
# Canary: 6cc4d1e0-6cdb-46a9-a48a-0b16e26785a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's install (REQ-1993): its code fetched from a pinned reference and hashed
before anything is installed; its dependencies installed once by the pinned uv from the code's
lockfile, binary wheels only; the result cached by content hash (the shared cache,
:func:`provisa.codesource.install.ensure_installed`).

The source names its code in one of three ways:

- a package on a PyPI index at an exact version, fetched as its wheel from the organisation's
  default index, else the public PyPI, and checked against the index's published SHA-256. A
  package with no wheel for the pinned Python is refused: building an sdist runs its build
  backend, the counterpart of an install script (decision P2);
- a Git repository at a commit, with a subdirectory;
- an archive uploaded to Provisa.

A code that declares dependencies carries a lockfile (decision P4): ``uv.lock`` (with its
``pyproject.toml``), ``pylock.toml``, or a ``requirements.txt`` with a ``--hash`` on every
requirement. They are installed into a virtual environment inside the install
(``<install>/.venv``), from the pinned Python, with no build of any package. The index addresses
and credentials reach uv in the install's own environment only, never in a file; the install runs
with a HOME of its own and none of the process's environment but its PATH. An index's certificate
authority (REQ-2088) is trusted by the fetches through it and, through SSL_CERT_FILE naming a file
in the install's own temporary directory, by uv.
"""

# Requirements: REQ-1993, REQ-2088
from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import re
import tempfile
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

from provisa.codesource import install as shared
from provisa.codesource.bundle import BundleRefused, unpack
from provisa.codesource.install import (
    GitCode,
    Http,
    Installed,
    Lockfile,
    Runner,
    UploadCode,
    get,
    gitconfig,
    http_get,
    run_command,
)
from provisa.core.package_registries import PYPI, Resolved

#: The index a package comes from when the organisation names no default index.
PYPI_PUBLIC = "https://pypi.org/simple/"

#: The user name a bare index token is sent with (PyPI's convention for an API token).
TOKEN_USER = "__token__"  # nosec B105 - the user name a token is sent with, not a secret

#: The Python every wheel must suit: the pinned CPython's (provisa/runtime_deps/code_runtimes.py).
PYTHON_TAGS = frozenset({"py3", "py312", "cp312"})
ABI_TAGS = frozenset({"none", "abi3", "cp312"})

LOCKFILES = ("uv.lock", "pylock.toml", "requirements.txt")
#: Where the code's dependencies are installed, inside its install.
VENV = ".venv"

_EXACT_VERSION = re.compile(r"^\d+(\.\d+)*([abc]|rc)?\d*(\.post\d+)?(\.dev\d+)?$")


@dataclass(frozen=True)
class PypiCode:
    package: str
    version: str


Code = PypiCode | GitCode | UploadCode


# -- indexes ---------------------------------------------------------------------------------------


def default_index(registries: Sequence[Resolved]) -> Resolved | None:
    """The organisation's default PyPI index (its mirror); None for the public PyPI."""
    return next((r for r in registries if r.kind == PYPI and r.mirror), None)


def _credentials(credential: str) -> tuple[str, str]:
    """``user:password`` as entered, or a bare token sent as PyPI's ``__token__`` user."""
    user, sep, password = credential.partition(":")
    return (user, password) if sep else (TOKEN_USER, credential)


def _with_credentials(address: str, credential: str) -> str:
    user, password = _credentials(credential)
    parts = urlsplit(address)
    netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{parts.netloc}"
    return urlunsplit(parts._replace(netloc=netloc))


def uv_index_environment(registries: Sequence[Resolved]) -> dict[str, str]:
    """The index settings uv reads from its environment: the default index and every other
    PyPI index, each with its credential in its URL. They live only in the install's
    environment."""
    env: dict[str, str] = {}
    default = default_index(registries)
    if default is not None:
        env["UV_DEFAULT_INDEX"] = _with_credentials(default.address, default.credential)
    others = [r for r in registries if r.kind == PYPI and not r.mirror]
    if others:
        env["UV_INDEX"] = " ".join(_with_credentials(r.address, r.credential) for r in others)
    return env


# -- fetching the code ------------------------------------------------------------------------------


def _normalized(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _platform_suits(tag: str) -> bool:
    if tag == "any":
        return True
    system, machine = platform.system(), platform.machine().lower()
    if system == "Darwin":
        return tag.startswith("macosx_") and tag.endswith(("_arm64", "_universal2"))
    if system == "Linux":
        arch = {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
        return tag.startswith("manylinux") and tag.endswith(f"_{arch}")
    return False


def _of_version(filename: str, code: PypiCode) -> bool:
    """Whether ``filename`` (a wheel or a source distribution) is ``code``'s release, by its
    normalized name and its exact version."""
    if filename.endswith(".whl"):
        name, version = filename.split("-")[:2]
    else:
        stem = re.sub(r"\.(tar\.gz|tar\.bz2|zip)$", "", filename)
        name, _, version = stem.rpartition("-")
    return _normalized(name) == _normalized(code.package) and version == code.version


def wheel_suits(filename: str) -> bool:
    """Whether the wheel ``filename`` installs on the pinned Python on this platform."""
    parts = filename[: -len(".whl")].split("-")
    if len(parts) not in (5, 6):
        return False
    python, abi, plat = parts[-3], parts[-2], parts[-1]
    return (
        bool(PYTHON_TAGS.intersection(python.split(".")))
        and bool(ABI_TAGS.intersection(abi.split(".")))
        and any(_platform_suits(p) for p in plat.split("."))
    )


def _auth_header(credential: str) -> dict[str, str]:
    user, password = _credentials(credential)
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()}


def fetch_wheel(code: PypiCode, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    """The package's wheel at its exact version for the pinned Python, checked against the
    index's SHA-256, unpacked under ``dest``."""
    if not _EXACT_VERSION.match(code.version):
        raise BundleRefused(
            "version_not_exact",
            f"{code.package}=={code.version} is not an exact version",
            package=code.package,
            version=code.version,
        )
    index = default_index(registries)
    base = index.address if index is not None else PYPI_PUBLIC
    base = base if base.endswith("/") else base + "/"
    headers = {"Accept": "application/vnd.pypi.simple.v1+json"}
    if index is not None:
        headers.update(_auth_header(index.credential))
    ca_bundle = index.ca_bundle if index is not None else None
    listing = json.loads(get(http, f"{base}{_normalized(code.package)}/", headers, ca_bundle))
    files = [f for f in listing.get("files", []) if _of_version(f["filename"], code)]
    if not files:
        raise BundleRefused(
            "version_not_found",
            f"{code.package}=={code.version} is not in {base}",
            package=code.package,
            version=code.version,
        )
    wheels = [f for f in files if f["filename"].endswith(".whl") and wheel_suits(f["filename"])]
    if not wheels:
        raise BundleRefused(
            "no_wheel",
            f"{code.package}=={code.version} has no wheel for Python 3.12 on this platform; a "
            "source distribution is never built",
            package=code.package,
        )
    chosen = sorted(wheels, key=lambda f: not f["filename"].endswith("-any.whl"))[0]
    expected = (chosen.get("hashes") or {}).get("sha256")
    if not expected:
        raise BundleRefused(
            "no_hash",
            f"{base} gives {chosen['filename']} no SHA-256 to check",
            package=code.package,
        )
    url = chosen["url"]
    if not url.startswith("https://"):
        url = base + url  # a relative link, beside the listing
    same_host = index is not None and urlsplit(url).netloc == urlsplit(base).netloc
    data = get(http, url, _auth_header(index.credential) if same_host and index else {}, ca_bundle)
    if hashlib.sha256(data).hexdigest() != expected:
        raise BundleRefused(
            "integrity_mismatch",
            f"{chosen['filename']} from {url} does not match the index's SHA-256",
            package=code.package,
        )
    # A wheel is a zip; its root is the code's top-level packages and its .dist-info.
    return unpack(chosen["filename"][: -len(".whl")] + ".zip", data, dest)


def fetch(code: Code, registries: Sequence[Resolved], http: Http, dest: Path) -> Path:
    if isinstance(code, PypiCode):
        return fetch_wheel(code, registries, http, dest)
    return shared.fetch_kept(code, registries, dest)


def wheel_top_level(root: Path) -> list[str]:
    """A wheel's top-level imports: its ``top_level.txt``, else the first part of every path its
    ``RECORD`` lists outside its ``.dist-info``. None when ``root`` is not an unpacked wheel."""
    infos = sorted(root.glob("*.dist-info"))
    if not infos:
        return []
    top = infos[0] / "top_level.txt"
    if top.is_file():
        return sorted({n.strip() for n in top.read_text().splitlines() if n.strip()})
    names: set[str] = set()
    for line in (infos[0] / "RECORD").read_text().splitlines():
        first = line.split(",", 1)[0].split("/", 1)[0]
        if first and not first.endswith((".dist-info", ".data")):
            names.add(first[: -len(".py")] if first.endswith(".py") else first)
    return sorted(names)


# -- installing its dependencies --------------------------------------------------------------------


def _wheel_requirements(root: Path) -> list[str]:
    for info in root.glob("*.dist-info"):
        metadata = info / "METADATA"
        if metadata.is_file():
            return [
                line.split(":", 1)[1].strip()
                for line in metadata.read_text().splitlines()
                if line.startswith("Requires-Dist:") and "extra ==" not in line
            ]
    return []


def _project_requirements(root: Path) -> list[str]:
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        return []
    return list(
        (tomllib.loads(pyproject.read_text()).get("project") or {}).get("dependencies") or []
    )


def _unhashed(requirements: Path) -> list[str]:
    """The requirement lines of a requirements.txt that carry no ``--hash``."""
    joined = re.sub(r"\\\n", " ", requirements.read_text())
    lines = [ln.split("#", 1)[0].strip() for ln in joined.splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("-") and "--hash=" not in ln]


def lockfile(root: Path) -> Path | None:
    """The lockfile the code's dependencies are installed from; None when it declares none.
    Code that declares dependencies with no lockfile, or a requirements.txt with a requirement
    that has no hash, is refused by name."""
    found = next((root / name for name in LOCKFILES if (root / name).is_file()), None)
    declared = _wheel_requirements(root) or _project_requirements(root)
    if found is None:
        if declared:
            raise BundleRefused(
                "no_lockfile",
                f"the code declares dependencies ({', '.join(declared[:5])}) but carries no "
                f"{', '.join(LOCKFILES)}; add one beside the code on the source's form",
            )
        return None
    if found.name == "uv.lock" and not (root / "pyproject.toml").is_file():
        raise BundleRefused(
            "no_project", "uv.lock is installed with its pyproject.toml, and there is none"
        )
    if found.name == "requirements.txt":
        unhashed = _unhashed(found)
        if unhashed:
            raise BundleRefused(
                "unhashed_requirement",
                f"requirements.txt has requirements without --hash: {', '.join(unhashed[:5])}",
            )
    return found


def install_commands(uv: Path, python: Path, root: Path, lock: Path) -> list[list[str]]:
    """The pinned uv's commands: a virtual environment from the pinned Python, then the lockfile
    installed into it exactly, binary wheels only."""
    venv = root / VENV
    make = [str(uv), "venv", "--python", str(python), "--no-python-downloads", str(venv)]
    if lock.name == "uv.lock":
        sync = [
            str(uv),
            "sync",
            "--frozen",
            "--no-build",
            "--no-install-project",
            "--no-python-downloads",
        ]
    else:
        sync = [
            str(uv), "pip", "sync", "--python", str(venv / "bin" / "python"), "--no-build",
            *(["--require-hashes"] if lock.name == "requirements.txt" else []), str(lock),
        ]  # fmt: skip
    return [make, sync]


def _install(
    root: Path, uv: Path, python: Path, registries: Sequence[Resolved], run: Runner
) -> None:
    lock = lockfile(root)
    if lock is None:
        return
    with tempfile.TemporaryDirectory(prefix="provisa-python-install-") as scratch:
        home = Path(scratch)
        git = home / ".gitconfig"
        git.write_text(gitconfig(registries))
        ca_file = shared.install_trust(registries, home)
        env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(home),
            "USERPROFILE": str(home),
            "UV_CACHE_DIR": str(home / "uv-cache"),
            "UV_NO_CONFIG": "1",  # no uv.toml of the host's or the user's
            "UV_PYTHON_DOWNLOADS": "never",
            "UV_PROJECT_ENVIRONMENT": str(root / VENV),
            "GIT_CONFIG_GLOBAL": str(git),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_TERMINAL_PROMPT": "0",
            **uv_index_environment(registries),
            # uv trusts the file SSL_CERT_FILE names in place of its own authorities; the file
            # holds the publicly trusted ones with the indexes' own (trust_bundle).
            **({"SSL_CERT_FILE": str(ca_file)} if ca_file is not None else {}),
            **shared.trust_environment(ca_file),
        }
        for argv in install_commands(uv, python, root, lock):
            run(argv, root, env)


def ensure_installed(
    *,
    source_id: str,
    code: Code,
    expected_hash: str | None,
    registries: Sequence[Resolved],
    python: Path,
    uv: Path,
    cache_root: Path,
    lockfile: Lockfile | None = None,
    http: Http = http_get,
    run: Runner = run_command,
) -> Installed:
    """The source's code, installed and cached by content hash
    (:func:`provisa.codesource.install.ensure_installed`), with ``lockfile`` -- given beside the
    code -- put into it first."""
    return shared.ensure_installed(
        source_id=source_id,
        runtime="python",
        fetch=lambda dest: fetch(code, registries, http, dest),
        install=lambda root: _install(root, uv, python, registries, run),
        expected_hash=expected_hash,
        cache_root=cache_root,
        lockfile=lockfile,
        lockfiles=LOCKFILES,
    )


def environment_python(installed: Path, python: Path) -> Path:
    """The interpreter a worker runs: the install's virtual environment's, when it has one (its
    dependencies are there), else the pinned Python."""
    venv_python = installed / VENV / "bin" / "python"
    return venv_python if venv_python.exists() else python
