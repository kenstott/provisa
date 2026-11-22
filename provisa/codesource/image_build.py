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

What each language puts in the image is its own (its ``image`` module, provisa/codesource/
languages.py): Python's interpreter and wheels, Node's runtime and packages, Go's host compiled
for the image's platform with the code linked in.
"""

# Requirements: REQ-2098, REQ-2200
from __future__ import annotations

import importlib
import json
import os
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.bundle import BundleRefused, content_hash
from provisa.codesource.install import Lockfile, Runner, gitconfig, place_lockfile, run_command
from provisa.codesource.manifest import manifest_hash
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
PLATFORM = {"amd64": LINUX_X64, "arm64": LINUX_ARM64}
_UVPLATFORM = {"amd64": "x86_64-manylinux_2_28", "arm64": "aarch64-manylinux_2_28"}
CPU = {"amd64": "x64", "arm64": "arm64"}

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


@dataclass(frozen=True)
class ImageParts:
    """What a language puts in a source's image: its layers, its host program's argv (``{fd}``
    where its socket goes), its environment, and whether its runtime is a Python the front end
    runs on too."""

    layers: list[list[oci.Entry]]
    worker: list[str]
    env: dict[str, str]
    runs_front: bool = False


def language_image(source_type: str) -> Any:  # the language's image module
    """The language's part of an image (its ``image`` module): ``fetch``, ``LOCKFILES`` and
    ``layers`` (provisa/codesource/languages.py)."""
    from provisa.codesource.languages import LANGUAGES

    return importlib.import_module(LANGUAGES[source_type].image)


def repository_of(source_id: str) -> str:
    """The repository a source's images are named in, under a registry's namespace."""
    return "provisa-code-" + re.sub(r"[^a-z0-9._-]+", "-", source_id.lower()).strip("-.")


def tag_of(code_hash: str, version: str) -> str:
    """The tag: the code hash and Provisa's version (a tag carries no "+")."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", f"{code_hash[:12]}-{version}")[:128]


def labels(
    runtime: str, code_hash: str, exports: list[str], names: dict, manifest: dict
) -> dict[str, str]:
    return {
        f"{LABEL}codehost": "1",
        f"{LABEL}protocol": str(PROTOCOL),
        f"{LABEL}runtime": runtime,
        f"{LABEL}code_hash": code_hash,
        f"{LABEL}exports": ",".join(sorted(exports)),
        f"{LABEL}names": _names_text(names),
        # REQ-2221: what a host's handshake is checked by, against the image at its digest.
        f"{LABEL}manifest_hash": manifest_hash(manifest),
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
    language: str  # a source type of provisa.codesource.languages.LANGUAGES
    runtime: str  # one of its language's runtimes
    code_hash: str
    entry: str | None
    exports: list[str]
    #: REQ-2100: the code's name tables (provisa/codesource/names.py), by function.
    names: dict
    #: REQ-2221: the code's manifest (manifest.manifest_json), which a paired host hands over.
    manifest: dict
    registries: list[Any]  # core.package_registries.Resolved
    cache_root: Path
    version: str
    #: REQ-2225: the @requires value that is the connector's script, for a connector whose tables
    #: and commands come from a script given at setup; None for any other.
    script: str | None = None


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
    target = CodeRuntimeResolver(cache_root=plan.cache_root, host=PLATFORM[arch])
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
    parts = language_image(plan.language).layers(plan, root, target, arch, workdir, run)
    own, worker, env = parts.layers, parts.worker, parts.env
    # A language whose runtime is a Python runs the front end on it; any other image carries the
    # front end's own.
    front_python = None if parts.runs_front else runtime_layer(target, "python", FRONT_RUNTIME)
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
            "PROVISA_MANIFEST_FILE": f"{FRONT}/manifest.json",
            **({SCRIPT_ENV: plan.script} if plan.script else {}),
            **env,
        },
        labels=labels(plan.runtime, plan.code_hash, plan.exports, plan.names, plan.manifest),
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

    language = language_image(plan.language)
    root = language.fetch(code, plan.registries, http_get, dest)
    lockfiles = language.LOCKFILES
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


def runtime_layer(resolver: CodeRuntimeResolver, name: str, at: str) -> list[oci.Entry]:
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


def files(root: Path, at: str, names: list[str]) -> list[oci.Entry]:
    return [oci.Entry(f"{at}/{n}", source=root / n) for n in names]


def uv_install_env(plan: Plan, home: Path) -> dict[str, str]:
    from provisa.python import install as python_install

    return {
        **scratch_env(plan.registries, home),
        "UV_CACHE_DIR": str(home / "uv-cache"),
        "UV_NO_CONFIG": "1",
        "UV_PYTHON_DOWNLOADS": "never",
        **python_install.uv_index_environment(plan.registries),
    }


def uv_install(arch: str, site: Path) -> list[str]:
    return [
        "pip", "install", "--target", str(site), "--python-version", "3.12",
        "--python-platform", _UVPLATFORM[arch], "--only-binary", ":all:", "--no-deps",
    ]  # fmt: skip


#: Where a Node image's front end Python runs from (a Python image's front end runs on the code's).
FRONT_RUNTIME = f"{ROOT}/front-runtime"
#: REQ-2225: names the @requires value that is the connector's script, for the front end
#: (provisa/codehost/front.py).
SCRIPT_ENV = "PROVISA_CONNECTOR_SCRIPT"
#: The front end's programs (provisa/codehost), with the code's name tables beside them.
FRONT = f"{ROOT}/front"
FRONT_PROGRAMS = ("front.py", "residual.py", "duckdb_sql.py", "pgfront.py", "pgcatalog.py")
#: Provisa's own modules the front end runs with, copied beside its programs: each image path's
#: source, relative to the provisa package. Every DuckDB the front end opens is capped (REQ-2006).
FRONT_MODULES = {"engine_memory.py": "core/engine_memory.py"}
#: Provisa's own modules the PostgreSQL protocol (buenavista) imports, relative to the provisa
#: package: placed in the front end's ``provisa`` package, under the names it imports them by.
PROTOCOL_MODULES = ("pgwire/catalog_pg17.py", "transpiler/pg_literals.py")
#: REQ-2194: where the Linux provisa wheels are (PROVISA_WHEELHOUSE, else the build cache's
#: ``wheels``): the front end writes its DataRows and binary COPY with the encoders
#: (provisa._encoders) compiled into the wheel for the image's architecture.
WHEELHOUSE_ENV = "PROVISA_WHEELHOUSE"
_WHEEL_ARCH = {"amd64": "x86_64", "arm64": "aarch64"}


def wheelhouse(plan: Plan) -> Path:
    return Path(os.environ.get(WHEELHOUSE_ENV) or plan.cache_root / "wheels")


def _same_version(wheel: Path, version: str) -> bool:
    from packaging.utils import InvalidWheelFilename, parse_wheel_filename
    from packaging.version import InvalidVersion, Version

    try:
        _, found, _, _ = parse_wheel_filename(wheel.name)
        return found == Version(version)
    except (InvalidWheelFilename, InvalidVersion):
        return False


def _encoders(plan: Plan, arch: str) -> list[oci.Entry]:
    """provisa._encoders for ``arch`` as a package of its own in the front end's library: its
    compiled module, from the Linux provisa wheel of this Provisa's version, and an empty
    ``provisa/__init__.py`` (the rest of provisa the front end runs is PROTOCOL_MODULES). A build
    for an architecture whose wheel is not there is refused by name; there is no other DataRow
    writer."""
    house = wheelhouse(plan)
    tag = f"manylinux_2_28_{_WHEEL_ARCH[arch]}"
    found = sorted(house.glob(f"provisa-*-{tag}.whl")) if house.is_dir() else []
    wheels = [w for w in found if _same_version(w, plan.version)]
    if not wheels:
        raise ImageBuildRefused(
            "image_encoders",
            f"no provisa {plan.version} wheel for linux/{arch} ({tag}) in {house}: the code "
            "host's PostgreSQL front end writes its rows with the encoders compiled into it",
            arch=arch,
            wheelhouse=str(house),
        )
    with zipfile.ZipFile(wheels[-1]) as wheel:
        modules = [n for n in wheel.namelist() if re.fullmatch(r"provisa/_encoders[^/]*\.so", n)]
        if len(modules) != 1:
            raise ImageBuildRefused(
                "image_encoders",
                f"{wheels[-1].name} carries {len(modules)} provisa._encoders modules, not one",
                arch=arch,
                wheelhouse=str(house),
            )
        (module,) = modules
        compiled = wheel.read(module)
    lib = f"{ROOT}/frontlib/provisa"
    return [
        oci.Entry(lib, directory=True, mode=0o755),
        oci.Entry(f"{lib}/__init__.py", data=b""),
        oci.Entry(f"{lib}/{module.rsplit('/', 1)[1]}", data=compiled, mode=0o755),
    ]


def _protocol_modules(package: Path) -> list[oci.Entry]:
    """The provisa modules the PostgreSQL protocol imports, in the front end's ``provisa`` package.
    Each subpackage's ``__init__.py`` is empty, as the package's own is: provisa's would import
    what the image does not hold."""
    lib = f"{ROOT}/frontlib/provisa"
    packages = sorted({Path(module).parent.as_posix() for module in PROTOCOL_MODULES})
    return [
        *(oci.Entry(f"{lib}/{name}", directory=True, mode=0o755) for name in packages),
        *(oci.Entry(f"{lib}/{name}/__init__.py", data=b"") for name in packages),
        *(oci.Entry(f"{lib}/{module}", source=package / module) for module in PROTOCOL_MODULES),
    ]


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
        env = uv_install_env(plan, Path(scratch))
        requirements = here / "front_requirements.txt"
        run(
            [str(uv), *uv_install(arch, lib), "--require-hashes", "-r", str(requirements)],
            workdir,
            env,
        )
    executable = target.pin("python").asset(PLATFORM[arch]).executable
    python = (
        f"{ROOT}/runtime/{executable}" if front_python is None else f"{FRONT_RUNTIME}/{executable}"
    )
    layers = [
        *([front_python] if front_python is not None else []),
        oci.tree(lib, f"{ROOT}/frontlib"),
        _encoders(plan, arch),
        _protocol_modules(here.parent),
        # The PostgreSQL protocol, as provisa/pgwire serves it (REQ-2100, pgfront.py): its
        # modules, never what an interpreter cached beside them.
        files(
            protocol, f"{ROOT}/frontlib/buenavista", sorted(p.name for p in protocol.glob("*.py"))
        ),
        [
            *files(here, FRONT, list(FRONT_PROGRAMS)),
            *(
                oci.Entry(f"{FRONT}/{name}", source=here.parent / source)
                for name, source in FRONT_MODULES.items()
            ),
            oci.Entry(f"{FRONT}/names.json", data=_names_text(plan.names).encode()),
            oci.Entry(f"{FRONT}/manifest.json", data=json.dumps(plan.manifest).encode()),
        ],
    ]
    entrypoint = [
        python, "-I", f"{FRONT}/front.py", str(HOST_PORT), STATE, SPILL, f"{FRONT}/names.json",
        *worker,
    ]  # fmt: skip
    return layers, entrypoint


def scratch_env(registries: list[Any], home: Path) -> dict[str, str]:
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


def clean(workdir: Path) -> None:
    shutil.rmtree(workdir, ignore_errors=True)
