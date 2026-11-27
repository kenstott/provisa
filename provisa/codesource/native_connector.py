# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8e1f52-a947-4d06-b2e8-6f0d9a71c435
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checkout's demo runs the Spark connector natively (REQ-2225, amended 2026-11-24).

Spark is a built-in source type, not a connector an organisation offers (REQ-2225, amended
2026-11-24): a ``spark`` source joins the Spark host, listed with the code source types. A release
carries that host as an image named in its platform block. A demo run from a checkout carries it
too, but runs it as a native Python code host from ``connectors/spark``, like the demo's other
native services: no Docker, no image build and no registry.

THE HOST'S ROW. :func:`platform_rows` is the host a ``spark`` source joins: its name, its standard file's
description, icon, values and settings (read as ``provisa connector build`` reads them), what its
code ``@requires`` (read from the checkout's code), and ``native``. Its code hash is the hash of
``connectors/spark`` as checked out: the code the host is built from.

BUILT. At the first join (:func:`start_for_join`) the project is copied under
``<PROVISA_HOME>/connectors/spark/<code hash>``, its Spark Connect client is built as
``connectors/spark/_build_client.py`` builds it (the wheel from the sdist its uv.lock pins, checked
against that hash, and a hashed requirements.txt), and the project is installed as a Python code
source's is (binary wheels only), on the interpreter this Provisa runs on.

RUN. Each source gets a host of its own: the code host front end (provisa/codehost/front.py), run
by this Provisa's interpreter, with the Python host program on the project's environment. Its state
-- its identity and its pairing -- is under the data directory, and the
source is paired with it on loopback exactly as with a host its operator runs. After a restart of
Provisa the host is started again from its state, on the port the source keeps
(:func:`ensure_running`). A host that fails to start fails the source by name. A deleted source's
host stops and its state goes (:func:`stop`). The hosts stop with the demo (:func:`stop_all`, and
the interpreter's exit as a backstop).
"""

# Requirements: REQ-2225
from __future__ import annotations

import json
import os
import shutil
import subprocess  # nosec B404 - this checkout's own build script and the code host, argv lists
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from provisa.codesource.bundle import BundleRefused, content_hash

CONNECTOR = "spark"
#: The key that marks a platform row as run natively here.
NATIVE = "native"
LOOPBACK = "127.0.0.1"
_PROJECT_PATH = "connectors/spark"
#: The checkout this package runs from: the provisa package's parent.
_ROOT = Path(__file__).resolve().parents[2]
#: What a build or an interpreter leaves in a project: never part of the code it is built from.
_SKIPPED = frozenset({".git", "__pycache__", ".venv", "wheels"})
#: How long a host has to say which port it listens on.
START_TIMEOUT_S = 120.0
STOP_TIMEOUT_S = 15.0
_BUILT_MARKER = ".built"


class NativeHostFailed(BundleRefused):
    """The native Spark connector could not be built or its host started, said by name."""

    def __init__(self, message: str, **params: str) -> None:
        super().__init__("connector_native_host", message, connector=CONNECTOR, **params)


# -- where it applies ------------------------------------------------------------------------------


def checkout_root() -> Path | None:
    """The checkout this package runs from, or None for an installed package (a release's)."""
    if (_ROOT / ".git").exists() and (_ROOT / _PROJECT_PATH / "connector.yaml").is_file():
        return _ROOT
    return None


def applies() -> bool:
    """Whether the Spark connector is run natively here: in a demo run from a checkout."""
    from provisa.core.demo import is_demo

    return is_demo() and checkout_root() is not None


def is_native(row: dict) -> bool:
    return bool(row.get(NATIVE))


def _project() -> Path:
    root = checkout_root()
    if root is None:
        raise NativeHostFailed("the native Spark connector runs only from a checkout")
    return root / _PROJECT_PATH


def code_hash(project: Path) -> str:
    return content_hash(project, _SKIPPED)


# -- offered -----------------------------------------------------------------------------------------

_rows: dict[str, dict[str, Any]] = {}
_rows_lock = threading.Lock()


def _git(root: Path, *args: str) -> str:
    done = subprocess.run(  # noqa: S603  # nosec B603 B607 - git, a fixed argv
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
    )
    if done.returncode != 0:
        raise NativeHostFailed(f"git {' '.join(args)} failed in {root}: {done.stderr.strip()}")
    return done.stdout.strip()


def _row(project: Path, hashed: str) -> dict[str, Any]:
    from provisa.cli_connector import (  # pyright: ignore[reportPrivateUsage]
        _read_standard_file,  # pyright: ignore[reportPrivateUsage]
        _Refused,  # pyright: ignore[reportPrivateUsage]
        _with_file_values,  # pyright: ignore[reportPrivateUsage]
    )
    from provisa.core.models import ConnectorRequirement
    from provisa.python.source import read_code
    from provisa.runtime_deps.code_runtimes import running_interpreter

    runtime = running_interpreter()
    manifest = read_code(
        python=runtime.executable, python_home=runtime.home, code=project, entry="provisa_spark"
    )
    requires = [ConnectorRequirement(name=r.name, secret=r.secret) for r in manifest.requires]
    try:
        standard = _read_standard_file(project)
        if standard is None:
            raise NativeHostFailed(f"{project} has no connector.yaml")
        requires = _with_file_values(requires, standard)
    except _Refused as refused:
        raise NativeHostFailed(f"{project}: {refused}") from None
    root = project.parents[1]
    return {
        "name": CONNECTOR,
        "git": str(root),
        "ref": _git(root, "rev-parse", "--abbrev-ref", "HEAD"),
        "commit": _git(root, "rev-parse", "HEAD"),
        "language": "python",
        "image": "",
        "digest": "",
        "code_hash": hashed,
        "arch": "",
        "requires": [r.model_dump(mode="json", exclude_defaults=True) for r in requires],
        "description": standard.description,
        "icon": standard.icon,
        "settings": [s.model_dump(mode="json", exclude_defaults=True) for s in standard.settings],
        NATIVE: True,
    }


def platform_rows() -> list[dict[str, Any]]:
    """The connectors run natively here, as platform rows; none outside a checkout's demo."""
    if not applies():
        return []
    project = _project()
    hashed = code_hash(project)
    with _rows_lock:
        found = _rows.get(hashed)
        if found is None:
            found = _rows[hashed] = _row(project, hashed)
    return [dict(found)]


# -- built -------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Install:
    """The connector's code installed: the interpreter its host runs on, its code, its manifest."""

    python: Path
    code: Path
    entry: str
    manifest: dict


def _build_root(hashed: str) -> Path:
    from provisa.core.demo_spark import provisa_home

    return provisa_home() / "connectors" / CONNECTOR / hashed


def _uv_env(scratch: Path) -> dict[str, str]:
    from provisa.runtime_deps.code_runtimes import uv_executable

    uv = uv_executable()
    return {
        **os.environ,
        "PATH": os.pathsep.join([str(uv.parent), os.environ.get("PATH", "")]),
        "UV_CACHE_DIR": str(scratch / "uv-cache"),
        "UV_NO_CONFIG": "1",
    }


def _built_project(hashed: str) -> Path:
    """The project copied and its client built (``_build_client.py``), once per code hash."""
    root = _build_root(hashed)
    project = root / "project"
    if (root / _BUILT_MARKER).is_file():
        return project
    source = _project()
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=root, prefix=".build-"))
    try:
        copy = staging / "project"
        shutil.copytree(source, copy, ignore=shutil.ignore_patterns(*_SKIPPED))
        if code_hash(copy) != hashed:
            raise NativeHostFailed(
                f"{source} changed while it was read: its code is now {code_hash(copy)}, not "
                f"{hashed}; create the source again"
            )
        done = subprocess.run(  # noqa: S603  # nosec B603 - the checkout's own build script
            [sys.executable, "-I", str(copy / "_build_client.py"), "--project", str(copy)],
            cwd=str(copy),
            env=_uv_env(staging),
            capture_output=True,
            text=True,
            check=False,
        )
        if done.returncode != 0:
            raise NativeHostFailed(
                f"the Spark Connect client could not be built: {done.stderr.strip()[-2000:]}"
            )
        if project.exists():  # a build cut short before its marker was written
            shutil.rmtree(project)
        os.replace(copy, project)
        (root / _BUILT_MARKER).write_text(hashed)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return project


def install(hashed: str) -> Install:
    """The connector's project built and installed as a Python code source's is."""
    from provisa.codesource.install import DirectoryCode
    from provisa.codesource.manifest import manifest_json
    from provisa.python.install import ensure_installed, environment_python
    from provisa.python.source import code_path, entry_module, read_code
    from provisa.runtime_deps.code_runtimes import (
        RuntimeUnavailable,
        running_interpreter,
        uv_executable,
    )
    from provisa.runtime_deps.pgwire_bundles import default_cache_root

    try:
        project = _built_project(hashed)
        runtime = running_interpreter()
        installed = ensure_installed(
            source_id=CONNECTOR,
            code=DirectoryCode(str(project)),
            expected_hash=None,
            registries=[],
            python=runtime.executable,
            uv=uv_executable(),
            cache_root=default_cache_root(),
        )
        code = code_path(installed.path)
        entry = entry_module(code, "provisa_spark")
        python = environment_python(installed.path)
        manifest = read_code(python=python, python_home=runtime.home, code=code, entry=entry)
    except NativeHostFailed:
        raise
    except (BundleRefused, RuntimeUnavailable, OSError) as failed:
        raise NativeHostFailed(f"the Spark connector could not be installed: {failed}") from None
    return Install(python, code, entry, manifest_json(manifest))


def labels(hashed: str, manifest: dict) -> dict[str, str]:
    """What an image's labels would say of this host (image_build.labels), unprefixed."""
    from provisa.codesource.manifest import manifest_hash

    return {"code_hash": hashed, "manifest_hash": manifest_hash(manifest), "runtime": "python"}


# -- run -----------------------------------------------------------------------------------------------


@dataclass
class _Host:
    process: subprocess.Popen
    port: int


@dataclass(frozen=True)
class Started:
    """A host started for a join: where it listens and its one-time pairing code."""

    host: str
    port: int
    pairing: str
    labels: dict[str, str]


_hosts: dict[tuple[str, str], _Host] = {}
_hosts_lock = threading.Lock()
_atexit_registered = False


def state_dir(org_id: str, source_id: str) -> Path:
    from provisa.core.org_files import data_dir

    return data_dir() / "connector-hosts" / org_id / source_id


def _command(work: Path, built: Install, port: int) -> list[str]:
    from provisa.codehost import front
    from provisa.python.source import HOST

    return [
        sys.executable, front.__file__, str(port), str(work / "state"), str(work / "spill"),
        str(work / "names.json"),
        str(built.python), "-I", str(HOST), "{fd}", str(built.code), built.entry,
    ]  # fmt: skip


def _environment(work: Path, hashed: str) -> dict[str, str]:
    from provisa.codehost.front import BIND_ENV
    from provisa.python.host import DISCOVERED_ENV

    return {
        **os.environ,
        BIND_ENV: LOOPBACK,  # REQ-2225: a native host is never reachable from the network
        "PROVISA_CODE_HASH": hashed,
        DISCOVERED_ENV: "{}",
        "PROVISA_MANIFEST_FILE": str(work / "manifest.json"),
    }


def _tail(log: Path, size: int = 4000) -> str:
    return log.read_text(errors="replace")[-size:] if log.is_file() else ""


def _await_listening(process: subprocess.Popen, log: Path, offset: int) -> tuple[int, str | None]:
    """The port the host says it listens on, and the pairing code it printed before it (None
    when it is paired already)."""
    deadline = time.monotonic() + START_TIMEOUT_S
    pairing: str | None = None
    while True:
        text = log.read_text(errors="replace")[offset:] if log.is_file() else ""
        for line in text.splitlines():
            if line.startswith("pairing code: "):
                pairing = line.removeprefix("pairing code: ").strip()
            elif line.startswith("remote host listening on "):
                return int(line.rsplit(" ", 1)[1]), pairing
        if process.poll() is not None:
            raise NativeHostFailed(
                f"the Spark connector's host exited with {process.returncode} before it "
                f"listened:\n{_tail(log)}"
            )
        if time.monotonic() > deadline:
            _stop_process(process)
            raise NativeHostFailed(
                f"the Spark connector's host did not listen within {START_TIMEOUT_S:.0f}s:\n"
                f"{_tail(log)}"
            )
        time.sleep(0.2)


def _launch(work: Path, built: Install, hashed: str, port: int) -> tuple[_Host, str | None]:
    from provisa.codesource.image_build import _names_text  # pyright: ignore[reportPrivateUsage]
    from provisa.codesource.manifest import manifest_from_json
    from provisa.codesource.names import name_tables

    work.mkdir(parents=True, exist_ok=True)
    (work / "manifest.json").write_text(json.dumps(built.manifest))
    (work / "names.json").write_text(_names_text(name_tables(manifest_from_json(built.manifest))))
    log = work / "host.log"
    offset = log.stat().st_size if log.is_file() else 0
    with open(log, "a") as out:  # the child keeps its own descriptor
        process = subprocess.Popen(  # noqa: S603  # nosec B603 - code-built arguments
            _command(work, built, port),
            cwd=str(work),
            env=_environment(work, hashed),
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    bound, pairing = _await_listening(process, log, offset)
    _register_atexit()
    return _Host(process, bound), pairing


def _register_atexit() -> None:
    global _atexit_registered
    if not _atexit_registered:
        import atexit

        # The backstop for an exit that never reaches the app's shutdown; the app stops them.
        atexit.register(stop_all)
        _atexit_registered = True


def start_for_join(org_id: str, source_id: str, row: dict) -> Started:
    """A fresh host for ``source_id``, built on a miss, started and waiting to be paired. Any
    host or state an earlier source of this id left is gone first."""
    hashed = row["code_hash"]
    built = install(hashed)
    work = state_dir(org_id, source_id)
    with _hosts_lock:
        earlier = _hosts.pop((org_id, source_id), None)
        if earlier is not None:
            _stop_process(earlier.process)
        if work.exists():
            shutil.rmtree(work)
        host, pairing = _launch(work, built, hashed, 0)
        if pairing is None:
            _stop_process(host.process)
            raise NativeHostFailed("the Spark connector's new host printed no pairing code")
        _hosts[(org_id, source_id)] = host
    return Started(LOOPBACK, host.port, pairing, labels(hashed, built.manifest))


def ensure_running(org_id: str, source_id: str, hashed: str, port: int) -> None:
    """The source's host running on ``port``: started again from its state when it is not (a
    restart of Provisa ends it). Its state holds its pairing, so it is not paired again."""
    work = state_dir(org_id, source_id)
    with _hosts_lock:
        found = _hosts.get((org_id, source_id))
        if found is not None and found.process.poll() is None:
            return
        if not (work / "state").is_dir():
            raise NativeHostFailed(
                f"source {source_id}: its Spark connector host has no state at {work}; "
                "create the source again",
                source=source_id,
            )
        host, pairing = _launch(work, install(hashed), hashed, port)
        if pairing is not None:
            _stop_process(host.process)
            raise NativeHostFailed(
                f"source {source_id}: its Spark connector host lost its pairing; create the "
                "source again",
                source=source_id,
            )
        _hosts[(org_id, source_id)] = host


def running(org_id: str, source_id: str) -> subprocess.Popen | None:
    """The source's host process while it runs."""
    with _hosts_lock:
        found = _hosts.get((org_id, source_id))
        return found.process if found is not None and found.process.poll() is None else None


def _stop_process(process: subprocess.Popen) -> None:
    from provisa.core.demo_spark import stop_process

    stop_process(process, STOP_TIMEOUT_S)


def stop(org_id: str, source_id: str) -> None:
    """The source was deleted: its host stopped and its state removed, so nothing starts it again
    (REQ-2225)."""
    work = state_dir(org_id, source_id)
    with _hosts_lock:
        found = _hosts.pop((org_id, source_id), None)
        if found is not None:
            _stop_process(found.process)
        if work.exists():
            shutil.rmtree(work)


def stop_all() -> None:
    """Stop every host this process started."""
    with _hosts_lock:
        hosts = list(_hosts.values())
        _hosts.clear()
    for host in hosts:
        _stop_process(host.process)


__all__ = [
    "CONNECTOR",
    "NATIVE",
    "NativeHostFailed",
    "Started",
    "applies",
    "checkout_root",
    "ensure_running",
    "install",
    "is_native",
    "platform_rows",
    "running",
    "start_for_join",
    "state_dir",
    "stop",
    "stop_all",
]
