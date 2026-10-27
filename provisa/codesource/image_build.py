# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1f4a62-9b3e-4c75-a0e8-3f6c2d5b9a14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build image (REQ-2098): a code source's remote host, as a Linux image Provisa assembles itself.

The image is the source's code at its pinned content hash, its dependencies installed for the
image's platform from the code's lockfile through the organisation's package registries (wheels
only; npm and Bun with no install scripts), the pinned runtime for that platform, the host
program's Arrow library, and the front end every image runs (REQ-2100, provisa/codehost/front.py)
-- the pinned Python, its own libraries apart from the code's, and the code's name tables -- on a
distroless base pinned by digest. The front end serves the port and runs the language's host
program beside it. Nothing in the image runs while it is built (provisa/codesource/oci.py): no
builder program, no daemon, no root.

The container runs as an unprivileged user and listens on one fixed internal port, 7443, with no
variable; its state (its identity and the Provisa it is paired to) is under /var/lib/provisa,
which the operator keeps on a volume. Its tag carries the code hash and Provisa's version; its
labels say what it is: ``dev.provisa.codehost=1``, the protocol, the runtime, the code hash, the
exports and the name tables.

Deno installs npm packages for the machine it runs on and has no way to name another platform, so
a Deno source's image is built from the server's own install, and is refused by name when the
server's platform is not the image's and the code's lockfile holds packages for a platform.
"""

# Requirements: REQ-2098
from __future__ import annotations

import json
import os
import platform
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.bundle import BundleRefused, content_hash
from provisa.codesource.install import Lockfile, Runner, gitconfig, place_lockfile, run_command
from provisa.codesource.remote import HOST_PORT
from provisa.codesource.worker import PROTOCOL
from provisa.runtime_deps.code_runtimes import LINUX_ARM64, LINUX_X64, CodeRuntimeResolver

#: The base every image is built on: glibc and libstdc++, nothing else (no shell, no package
#: manager), pinned per platform by its manifest's digest.
BASE_HOST = "gcr.io"
BASE_REPOSITORY = "distroless/cc-debian12"
BASE_DIGESTS = {
    "amd64": "sha256:6dc8478fd8e790ae427aeff51e588573dec83b13adb7e5d10505a717927009b5",
    "arm64": "sha256:4bed9c8eff0e70bf6547aa04a3b1ea5bba63fb910707b3f94f45556edc2a9238",
}
ARCHITECTURES = tuple(BASE_DIGESTS)
_PLATFORM = {"amd64": LINUX_X64, "arm64": LINUX_ARM64}
_UV_PLATFORM = {"amd64": "x86_64-manylinux_2_28", "arm64": "aarch64-manylinux_2_28"}
_CPU = {"amd64": "x64", "arm64": "arm64"}

#: Where everything is in the image.
ROOT = "/opt/provisa"
STATE = "/var/lib/provisa"
#: REQ-2100: where the image's DuckDB spills, a volume of its own the operator sizes to the
#: spill cap (codesource.duckdb_spill_cap_mb) with the platform's quota.
SPILL = "/var/lib/provisa-spill"
#: distroless's unprivileged user and group.
NONROOT = 65532

LABEL = "dev.provisa."


class ImageBuildRefused(BundleRefused):
    """A build that cannot be made, named with why."""


@dataclass(frozen=True)
class Built:
    """An image assembled for a source: what to push or archive, and what names it."""

    image: oci.Image
    repository: str
    tag: str
    exports: list[str]


def repository_of(source_id: str) -> str:
    """The repository a source's images are named in, under a registry's namespace."""
    return "provisa-code-" + re.sub(r"[^a-z0-9._-]+", "-", source_id.lower()).strip("-.")


def tag_of(code_hash: str, version: str) -> str:
    """The tag: the code hash and Provisa's version (a tag carries no "+")."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", f"{code_hash[:12]}-{version}")[:128]


def labels(runtime: str, code_hash: str, exports: list[str], names: dict) -> dict[str, str]:
    return {
        f"{LABEL}codehost": "1",
        f"{LABEL}protocol": str(PROTOCOL),
        f"{LABEL}runtime": runtime,
        f"{LABEL}code_hash": code_hash,
        f"{LABEL}exports": ",".join(sorted(exports)),
        f"{LABEL}names": _names_text(names),
    }


def _names_text(names: dict) -> str:
    """The name tables as the image carries them (REQ-2100): one compact JSON document."""
    return json.dumps(names, sort_keys=True, separators=(",", ":"))


def compose_snippet(image_ref: str, service: str) -> str:
    """The compose service the UI shows after a build (and the docs): the fixed internal port
    published on a host port the operator must name, the state on a volume, and the spill on a
    volume of its own, which bounds the spill only when it is sized to the cap."""
    return (
        "services:\n"
        f"  {service}:\n"
        f"    image: {image_ref}\n"
        "    restart: unless-stopped\n"
        "    ports:\n"
        f'      - "${{PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}}:{HOST_PORT}"\n'
        "    volumes:\n"
        f"      - {service}-state:{STATE}\n"
        f"      - {service}-spill:{SPILL}\n"
        "volumes:\n"
        f"  {service}-state: {{}}\n"
        "  # Size this volume to codesource.duckdb_spill_cap_mb with the platform's quota\n"
        "  # (Kubernetes ephemeral-storage or an emptyDir sizeLimit, an xfs project quota, or a\n"
        "  # volume driver's size option). Without a quota the disk the image spills to is not\n"
        "  # bounded.\n"
        f"  {service}-spill: {{}}\n"
    )


# -- the build ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Plan:
    """What a build needs, read from the source and the organisation before it runs."""

    source_id: str
    language: str  # node | python
    runtime: str  # node | bun | deno | python
    code_hash: str
    entry: str | None
    exports: list[str]
    #: REQ-2100: the code's name tables (provisa/codesource/names.py), by function.
    names: dict
    registries: list[Any]  # core.package_registries.Resolved
    cache_root: Path
    version: str


def assemble(
    plan: Plan,
    code: Any,  # the language's Code reference (install.py)
    lock: Lockfile | None,
    arch: str,
    workdir: Path,
    *,
    run: Runner = run_command,
    base: oci.Registry | None = None,
) -> Built:
    """The source's image for ``arch``: blocking (fetches, installs and writes layers)."""
    if arch not in ARCHITECTURES:
        raise ImageBuildRefused(
            "image_arch",
            f"an image is built for {', '.join(ARCHITECTURES)}, not {arch!r}",
            arch=arch,
        )
    root = _fetched(plan, code, lock, workdir / "code")
    target = CodeRuntimeResolver(cache_root=plan.cache_root, host=_PLATFORM[arch])
    layers, base_config = oci.pull_base(
        base or oci.Registry(BASE_HOST),
        BASE_REPOSITORY,
        BASE_DIGESTS[arch],
        plan.cache_root / "oci" / "base",
    )
    layers_dir = workdir / "layers"
    state = oci.write_layer(
        [
            oci.Entry(STATE, directory=True, mode=0o700, owner=NONROOT),
            oci.Entry(SPILL, directory=True, mode=0o700, owner=NONROOT),
        ],
        layers_dir,
    )
    if plan.language == "python":
        own, worker, env = _python(plan, root, target, arch, workdir, run)
        front_python = None  # the code's own runtime runs the front end too
    else:
        own, worker, env = _node(plan, root, target, arch, workdir, run)
        front_python = _runtime(target, "python", FRONT_RUNTIME)
    front, entrypoint = _front(plan, target, arch, workdir, run, worker, front_python)
    own = [*own, *front]
    image = oci.Image(
        architecture=arch,
        layers=[*layers, *(oci.write_layer(entries, layers_dir) for entries in own), state],
        entrypoint=entrypoint,
        env={
            "PROVISA_CODE_HASH": plan.code_hash,
            "PROVISA_HOST_LIB": f"{ROOT}/hostlib",
            "PROVISA_FRONT_LIB": f"{ROOT}/frontlib",
            **env,
        },
        labels=labels(plan.runtime, plan.code_hash, plan.exports, plan.names),
        exposed=[HOST_PORT],
        working_dir=f"{ROOT}/code",
        user=f"{NONROOT}:{NONROOT}",
        base_config=base_config,
    )
    return Built(
        image, repository_of(plan.source_id), tag_of(plan.code_hash, plan.version), plan.exports
    )


def _fetched(plan: Plan, code: Any, lock: Lockfile | None, dest: Path) -> Path:
    """The code at its pinned content hash, fetched afresh (the server's install holds its own
    platform's dependencies), with its lockfile beside it."""
    from provisa.codesource.install import http_get

    from provisa.node import install as node_install
    from provisa.python import install as python_install

    lockfiles: tuple[str, ...]
    if plan.language == "python":
        root = python_install.fetch(code, plan.registries, http_get, dest)
        lockfiles = python_install.LOCKFILES
    else:
        root = node_install.fetch(code, plan.registries, http_get, dest)
        lockfiles = tuple(n for names in node_install.LOCKFILES.values() for n in names)
    if lock is not None:
        place_lockfile(root, lock, lockfiles)
    found = content_hash(root)
    if found != plan.code_hash:
        raise ImageBuildRefused(
            "content_changed",
            f"source {plan.source_id!r}: the code fetched has hash {found}, not the pinned "
            f"{plan.code_hash}",
            source=plan.source_id,
        )
    return root


def _runtime(resolver: CodeRuntimeResolver, name: str, at: str) -> list[oci.Entry]:
    """A pinned runtime (or library) for the image's platform, from its checked archive."""
    asset, data = resolver.archive(name)
    executable = f"{at}/{asset.executable}"
    # A zip may keep no permission bits: the runtime's own executable is made executable.
    return [
        oci.Entry(e.path, source=e.source, data=e.data, mode=0o755, link=e.link)
        if e.path == executable and e.link is None
        else e
        for e in oci.archive_entries(asset.filename, data, at)
    ]


def _files(root: Path, at: str, names: list[str]) -> list[oci.Entry]:
    return [oci.Entry(f"{at}/{n}", source=root / n) for n in names]


def _uv_install_env(plan: Plan, home: Path) -> dict[str, str]:
    from provisa.python import install as python_install

    return {
        **_scratch_env(plan.registries, home),
        "UV_CACHE_DIR": str(home / "uv-cache"),
        "UV_NO_CONFIG": "1",
        "UV_PYTHON_DOWNLOADS": "never",
        **python_install.uv_index_environment(plan.registries),
    }


def _uv_install(arch: str, site: Path) -> list[str]:
    return [
        "pip", "install", "--target", str(site), "--python-version", "3.12",
        "--python-platform", _UV_PLATFORM[arch], "--only-binary", ":all:", "--no-deps",
    ]  # fmt: skip


#: Where a Node image's front end Python runs from (a Python image's front end runs on the code's).
FRONT_RUNTIME = f"{ROOT}/front-runtime"
#: The front end's programs (provisa/codehost), with the code's name tables beside them.
FRONT = f"{ROOT}/front"
#: engine_memory.py is core's (provisa/codehost/engine_memory.py links it): every DuckDB the front
#: end opens is capped (REQ-2006).
FRONT_PROGRAMS = (
    "front.py",
    "residual.py",
    "duckdb_sql.py",
    "engine_memory.py",
    "pgfront.py",
    "pgcatalog.py",
)


def _front(
    plan: Plan,
    target: CodeRuntimeResolver,
    arch: str,
    workdir: Path,
    run: Runner,
    worker: list[str],
    front_python: list[oci.Entry] | None,
) -> tuple[list[list[oci.Entry]], list[str]]:
    """REQ-2100: the front end's layers, and the entrypoint that runs it with ``worker`` (the
    host program's argv, ``{fd}`` where its socket goes)."""
    import buenavista

    from provisa.codehost import front as front_program

    here = Path(front_program.__file__).parent
    protocol = Path(buenavista.__file__).parent
    server = CodeRuntimeResolver(cache_root=plan.cache_root)
    uv = server.resolve("uv")
    lib = workdir / "frontlib"
    lib.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="provisa-image-front-") as scratch:
        env = _uv_install_env(plan, Path(scratch))
        requirements = here / "front_requirements.txt"
        run(
            [str(uv), *_uv_install(arch, lib), "--require-hashes", "-r", str(requirements)],
            workdir,
            env,
        )
    executable = target.pin("python").asset(_PLATFORM[arch]).executable
    python = (
        f"{ROOT}/runtime/{executable}" if front_python is None else f"{FRONT_RUNTIME}/{executable}"
    )
    layers = [
        *([front_python] if front_python is not None else []),
        oci.tree(lib, f"{ROOT}/frontlib"),
        # The PostgreSQL protocol, as provisa/pgwire serves it (REQ-2100, pgfront.py): its
        # modules, never what an interpreter cached beside them.
        _files(
            protocol, f"{ROOT}/frontlib/buenavista", sorted(p.name for p in protocol.glob("*.py"))
        ),
        [
            *_files(here, FRONT, list(FRONT_PROGRAMS)),
            oci.Entry(f"{FRONT}/names.json", data=_names_text(plan.names).encode()),
        ],
    ]
    entrypoint = [
        python, "-I", f"{FRONT}/front.py", str(HOST_PORT), STATE, SPILL, f"{FRONT}/names.json",
        *worker,
    ]  # fmt: skip
    return layers, entrypoint


def _scratch_env(registries: list[Any], home: Path) -> dict[str, str]:
    from provisa.codesource import install as shared

    git = home / ".gitconfig"
    git.write_text(gitconfig(registries))
    ca_file = shared.install_trust(registries, home)
    return {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(home),
        "USERPROFILE": str(home),
        "GIT_CONFIG_GLOBAL": str(git),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        **({"SSL_CERT_FILE": str(ca_file)} if ca_file is not None else {}),
        **shared.trust_environment(ca_file),
    }


# -- Python -------------------------------------------------------------------------------------


def _python(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> tuple[list[list[oci.Entry]], list[str], dict[str, str]]:
    from provisa.python import install as python_install
    from provisa.python.source import HOST, code_path, entry_module

    server = CodeRuntimeResolver(cache_root=plan.cache_root)
    uv = server.resolve("uv")
    site = workdir / "site-packages"
    site.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="provisa-image-python-") as scratch:
        home = Path(scratch)
        env = _uv_install_env(plan, home)
        install = [str(uv), *_uv_install(arch, site)]
        lock = python_install.lockfile(root)
        if lock is not None:
            requirements = lock
            if lock.name == "uv.lock":
                requirements = home / "requirements.txt"
                run(
                    [str(uv), "export", "--frozen", "--no-emit-project", "--format",
                     "requirements-txt", "--output-file", str(requirements)],
                    root,
                    env,
                )  # fmt: skip
            hashed = ["--require-hashes"] if requirements.name.endswith(".txt") else []
            run([*install, *hashed, "-r", str(requirements)], root, env)
    version = target.pin("python").version.split("+", 1)[0]
    venv = [
        oci.Entry(
            f"{ROOT}/venv/pyvenv.cfg",
            data=(
                f"home = {ROOT}/runtime/python/bin\ninclude-system-site-packages = false\n"
                f"version = {version}\n"
            ).encode(),
        ),
        oci.Entry(f"{ROOT}/venv/bin/python", link=f"{ROOT}/runtime/python/bin/python3.12"),
        *oci.tree(site, f"{ROOT}/venv/lib/python3.12/site-packages"),
    ]
    code = code_path(root)
    entry = entry_module(code, plan.entry)
    layers = [
        _runtime(target, "python", f"{ROOT}/runtime"),
        _runtime(target, "pyarrow", f"{ROOT}/hostlib"),
        _files(HOST.parent, f"{ROOT}/host", ["host.py"]),
        # REQ-2106: Provisa's services module, beside the host as it is on the server.
        oci.tree(HOST.parent / "services", f"{ROOT}/host/services"),
        venv,
        oci.tree(code, f"{ROOT}/code"),
    ]
    worker = [
        f"{ROOT}/venv/bin/python",
        "-I",
        f"{ROOT}/host/host.py",
        "{fd}",
        f"{ROOT}/code",
        entry,
    ]
    return layers, worker, {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}


# -- Node.js, Bun, Deno -------------------------------------------------------------------------


def _deno_platform_packages(root: Path) -> list[str]:
    """The npm packages a Deno lockfile holds for one platform only."""
    lock = root / "deno.lock"
    if not lock.is_file():
        return []
    packages = (json.loads(lock.read_text()).get("npm") or {}).items()
    return sorted(name for name, entry in packages if entry.get("os") or entry.get("cpu"))


def _node(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> tuple[list[list[oci.Entry]], list[str], dict[str, str]]:
    from provisa.node import install as node_install
    from provisa.node.worker import HOST, entry_module

    runtime = plan.runtime
    if node_install.needs_install(root, runtime):
        server = CodeRuntimeResolver(cache_root=plan.cache_root)
        executable = server.resolve(runtime)
        with tempfile.TemporaryDirectory(prefix="provisa-image-node-") as scratch:
            home = Path(scratch)
            rc = home / ".npmrc"
            env = _scratch_env(plan.registries, home)
            cafile = env.get("SSL_CERT_FILE")
            rc.write_text(node_install.npmrc(plan.registries, Path(cafile) if cafile else None))
            (root / ".npmrc").write_text(rc.read_text())
            env.update(
                {
                    "NPM_CONFIG_USERCONFIG": str(rc),
                    "npm_config_cache": str(home / "npm-cache"),
                    "BUN_INSTALL_CACHE_DIR": str(home / "bun-cache"),
                    "DENO_DIR": str(root / ".deno"),
                }
            )
            if runtime == "node":
                argv = [
                    *node_install.install_command("node", executable),
                    "--os=linux", f"--cpu={_CPU[arch]}", "--libc=glibc",
                ]  # fmt: skip
            elif runtime == "bun":
                argv = [
                    *node_install.install_command("bun", executable),
                    "--os=linux",
                    f"--cpu={_CPU[arch]}",
                ]
            else:
                here = (platform.system(), platform.machine().replace("arm64", "aarch64"))
                held = _deno_platform_packages(root)
                if here != _PLATFORM[arch] and held:
                    raise ImageBuildRefused(
                        "deno_platform",
                        "Deno installs npm packages for the machine it runs on; this server is "
                        f"not {arch} Linux, and the lockfile holds packages for one platform: "
                        f"{', '.join(held[:5])}",
                    )
                argv = node_install.install_command("deno", executable)
            try:
                run(argv, root, env)
            finally:
                (root / ".npmrc").unlink(missing_ok=True)
    exe = f"{ROOT}/runtime/{target.pin(runtime).asset(_PLATFORM[arch]).executable}"
    entry = entry_module(root).relative_to(root.resolve()).as_posix()
    host = [f"{ROOT}/host/host.mjs", "{fd}", f"{ROOT}/code/{entry}"]
    if runtime == "deno":
        worker = [
            exe, "run", "--no-prompt", "--allow-net", f"--allow-read={ROOT}", "--allow-env",
            f"--import-map={ROOT}/host/services-import-map.json", *host,
        ]  # fmt: skip
        env = {"DENO_DIR": f"{ROOT}/code/.deno", "NO_COLOR": "1"}
    else:
        worker = [*node_install.code_command(runtime, exe), *host]
        env = {"NO_COLOR": "1"}
    layers = [
        _runtime(target, runtime, f"{ROOT}/runtime"),
        _runtime(target, "apache-arrow", f"{ROOT}/hostlib"),
        _files(
            HOST.parent,
            f"{ROOT}/host",
            # REQ-2106: "@provisa/services", and Deno's import map naming it.
            ["host-core.mjs", "host.mjs", "services.mjs", "services-import-map.json"],
        ),
        oci.tree(root, f"{ROOT}/code"),
    ]
    return layers, worker, env


def clean(workdir: Path) -> None:
    shutil.rmtree(workdir, ignore_errors=True)
