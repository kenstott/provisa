# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e7d02-3a94-4f6b-9b27-8e40d1c6a7f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa connector build|check|rebuild`` -- an organization's own code connectors (REQ-2221).

Runs wherever the operator runs it (any CI), with no Provisa server: it needs git, the project, and
the container registry. The server never builds a connector or reads its build settings. A build
clones the project at one commit, runs the entry's build command if it has one, detects the language
from the project's standard files, and hands the rest to the code source image build (REQ-2098). The
operator's own deployment runs the image.

Credentials stay the operator's: git's own configuration signs in to the repository, and the
registry password is read from an environment variable the operator names.

THE CONTRACT WITH A SCRIPT: stdout is the result (a ``connectors:`` block, or one line per
connector), stderr says what happened.

* ``0`` -- done; ``check``: every connector is at its ref's latest commit.
* ``1`` -- a build was refused or failed; ``check``: a connector is behind its ref, or its ref could
  not be read. ``rebuild`` builds the others and leaves a failed connector's entry as it was.
* ``2`` -- usage error or an unusable config; nothing was built.
"""

# Requirements: REQ-2221

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml

if (
    TYPE_CHECKING
):  # the config models are imported when a command runs: `provisa --help` stays quick
    from pydantic import ValidationError

    from provisa.core.models import ConnectorConfig, ConnectorRequirement

#: What a standard project of each language has at its root.
_STANDARD_FILES = {
    "go": ("go.mod",),
    "node": ("package.json",),
    "python": ("pyproject.toml", "requirements.txt"),
}
#: Stand-ins that pass validation while the flags are checked, before anything is built.
_UNBUILT_COMMIT = "0" * 40
_UNBUILT_DIGEST = "sha256:" + "0" * 64
_UNBUILT_CODE_HASH = "0" * 64
_UNBUILT_LANGUAGE = "python"


class _Usage(Exception):
    """The command cannot start: exit 2, nothing built."""


class _Refused(Exception):
    """A connector could not be built or read, with why."""


def _problems(exc: ValidationError) -> str:
    # Each error's location and message, never str(exc): that repeats the input it refused.
    return "; ".join(
        f"{'.'.join(str(part) for part in e['loc'])}: {e['msg']}" for e in exc.errors()
    )


def _git(*args: str, cwd: Path | None = None) -> str:
    try:
        done = subprocess.run(  # nosec B603 B607 -- git by name from the operator's PATH, no shell
            ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
        )
    except FileNotFoundError as exc:
        raise _Refused("git is not on the PATH; a connector is built from a git project") from exc
    if done.returncode != 0:
        raise _Refused(f"git {args[0]} failed: {done.stderr.strip()}")
    return done.stdout


def _latest_commit(git: str, ref: str) -> str:
    """The commit ``ref`` is at now, asking the repository. A branch and a tag of one name are
    refused rather than guessed between."""
    branch, tag = f"refs/heads/{ref}", f"refs/tags/{ref}"
    listed = _git("ls-remote", "--", git, branch, tag, f"{tag}^{{}}")
    found = {name: sha for sha, name in (line.split("\t") for line in listed.splitlines())}
    at_branch = found.get(branch)
    at_tag = found.get(f"{tag}^{{}}") or found.get(tag)  # an annotated tag's commit is its peeled
    if at_branch and at_tag:
        raise _Refused(f"{ref} is both a branch and a tag in {git}")
    if not at_branch and not at_tag:
        raise _Refused(f"{git} has no branch or tag {ref}")
    return cast(str, at_branch or at_tag)


def _checkout(git: str, commit: str, dest: Path) -> None:
    _git("clone", "--quiet", "--no-checkout", "--", git, str(dest))
    _git("checkout", "--quiet", "--detach", commit, cwd=dest)


def _run_build_command(command: str, checkout: Path) -> None:
    """The entry's build command, at the repository's root, its output on stderr so stdout stays
    the result. Split into words and run with no shell: the operator's machine may have none."""
    try:
        argv = shlex.split(command)
    except ValueError as exc:
        raise _Refused(f"the build command cannot be read: {exc}") from exc
    if not argv:
        raise _Refused("the build command is empty")
    try:
        done = subprocess.run(argv, cwd=checkout, stdout=2, check=False)  # nosec B603
    except OSError as exc:
        raise _Refused(f"the build command could not run: {exc.strerror}: {argv[0]}") from exc
    if done.returncode != 0:
        raise _Refused(f"the build command exited with status {done.returncode}")


def _project_root(checkout: Path, path: str) -> Path:
    root = (checkout / path).resolve()
    if not root.is_relative_to(checkout.resolve()):
        raise _Refused(f"path {path} leaves the repository")
    if not root.is_dir():
        raise _Refused(f"the repository has no directory {path}")
    return root


def _detect_language(root: Path, path: str) -> str:
    """The project's language, from its standard files; refused saying what is missing."""
    where = path or "the repository's root"
    found = [
        language
        for language, names in _STANDARD_FILES.items()
        if any((root / name).is_file() for name in names)
    ]
    if not found:
        wanted = ", ".join(sorted(n for names in _STANDARD_FILES.values() for n in names))
        raise _Refused(f"{where} is not a standard project: it has none of {wanted}")
    if len(found) > 1:
        raise _Refused(
            f"{where} has the standard files of {' and '.join(found)}; "
            "name the one project with --path"
        )
    return found[0]


def _runtime(language: str, root: Path) -> str:
    """The language's runtime: a Node project's is the one its lockfile is for."""
    from provisa.codesource.languages import LANGUAGES
    from provisa.node.install import LOCKFILES

    if language != "node":
        return LANGUAGES[language].runtimes[0]
    found = [r for r, names in LOCKFILES.items() if any((root / n).is_file() for n in names)]
    if len(found) > 1:
        raise _Refused(f"the project has lockfiles for {' and '.join(found)}; keep one")
    return found[0] if found else "node"


def _build_and_push(
    entry: ConnectorConfig,
    root: Path,
    language: str,
    workdir: Path,
    registry: dict[str, str],
    package_registries: list[Any],
) -> tuple[str, list[ConnectorRequirement], str]:
    """The code source image build (REQ-2098) on the project at ``root``: its digest once pushed,
    what its code declares it requires, and the hash of that code. With no server there are no
    organization package registries: ``package_registries`` are the indexes the operator named
    for private dependencies (:func:`_package_registries`); the rest come from the public ones."""
    from provisa.codesource import image_build, oci
    from provisa.codesource.install import DirectoryCode
    from provisa.codesource.languages import LANGUAGES
    from provisa.codesource.manifest import manifest_json
    from provisa.codesource.names import name_tables
    from provisa.codesource.source import CodeConfig, Context
    from provisa.core.demo_bundles import running_provisa
    from provisa.core.models import ConnectorRequirement
    from provisa.runtime_deps.pgwire_bundles import default_cache_root

    code = DirectoryCode(str(root))
    runtime = _runtime(language, root)
    # admin_db and org_id serve a source's uploads and service grant; a project has neither here.
    ctx = Context(default_cache_root(), package_registries, [], cast(Any, None), "")
    config = CodeConfig(
        language, runtime, {"kind": "directory", "path": str(root)}, None, None, None
    )
    prepared = LANGUAGES[language].prepare()(entry.name, config, code, None, ctx)
    plan = image_build.Plan(
        source_id=entry.name,
        language=language,
        runtime=runtime,
        code_hash=prepared.content_hash,
        entry=None,
        exports=[e.name for e in prepared.manifest.exports],
        names=name_tables(prepared.manifest),
        manifest=manifest_json(prepared.manifest),
        registries=list(package_registries),
        cache_root=ctx.cache_root,
        version=running_provisa(),
    )
    workdir.mkdir(parents=True)
    built = image_build.assemble(plan, code, None, entry.arch, workdir)
    host, _, repository = entry.image.partition("/")
    digest = oci.Registry(host, registry.get("username"), registry.get("password")).push(
        repository, built.tag, built.image
    )
    requires = [
        ConnectorRequirement(name=r.name, secret=r.secret) for r in prepared.manifest.requires
    ]
    return digest, requires, prepared.content_hash


def _build(
    entry: ConnectorConfig,
    commit: str,
    registry: dict[str, str],
    package_registries: list[Any],
) -> ConnectorConfig:
    """``entry`` built at ``commit``: the build system's answer, taken as it comes."""
    with tempfile.TemporaryDirectory(prefix="provisa-connector-") as scratch:
        work = Path(scratch)
        checkout = work / "checkout"
        _checkout(entry.git, commit, checkout)
        if entry.build_command:
            _run_build_command(entry.build_command, checkout)
        root = _project_root(checkout, entry.path)
        language = _detect_language(root, entry.path)
        digest, requires, code_hash = _build_and_push(
            entry, root, language, work / "image", registry, package_registries
        )
    return entry.model_copy(
        update={
            "commit": commit,
            "digest": digest,
            "requires": requires,
            "code_hash": code_hash,
            "language": language,
        }
    )


def _failures() -> tuple[type[Exception], ...]:
    """What a build can fail with: reported by connector, where anything else is a defect."""
    import httpx

    from provisa.codesource.bundle import BundleRefused
    from provisa.codesource.linux_sandbox import SandboxUnavailable
    from provisa.codesource.oci import ImageRefused

    return (
        _Refused,
        BundleRefused,
        ImageRefused,
        SandboxUnavailable,
        OSError,
        subprocess.SubprocessError,
        httpx.HTTPError,
    )


def _registry_sign_in(args: argparse.Namespace) -> dict[str, str]:
    """The registry sign-in the flags name; none is an anonymous push."""
    if bool(args.registry_user) != bool(args.registry_password_env):
        raise _Usage("--registry-user and --registry-password-env are given together")
    if not args.registry_user:
        return {}
    password = os.environ.get(args.registry_password_env)
    if not password:
        raise _Usage(f"the environment variable {args.registry_password_env} is not set")
    return {"username": args.registry_user, "password": password}


def _package_registries(path: str | None) -> list[Any]:
    """The indexes ``--package-registries`` names for private dependencies: a YAML file with one
    ``registries:`` list, each entry its ``kind``, ``address``, and -- the file carries no secret
    -- ``credential_env``, the environment variable holding its credential. A CI's secret store
    fills the variable; the credential stays with the operator and is written nowhere. Refused,
    naming the entry and never a value, when one cannot be used."""
    from pydantic import BaseModel, ConfigDict, Field, ValidationError

    from provisa.core.package_registries import KINDS, Resolved

    if path is None:
        return []

    class _Named(BaseModel):
        model_config = ConfigDict(extra="forbid")

        kind: str
        address: str = Field(min_length=1)
        scope: str | None = None
        mirror: bool = False
        credential_env: str = Field(min_length=1)
        ca_file: str | None = None

    try:
        raw = yaml.safe_load(Path(path).read_text())
    except OSError as exc:
        raise _Usage(f"cannot read {path}: {exc.strerror}") from exc
    except yaml.YAMLError as exc:
        # The position only: the parser's message quotes the file.
        mark = getattr(exc, "problem_mark", None)
        raise _Usage(
            f"{path} is not valid YAML{f' (line {mark.line + 1})' if mark else ''}"
        ) from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("registries"), list):
        raise _Usage(f"{path} has no registries: list")
    out: list[Resolved] = []
    for at, given in enumerate(raw["registries"]):
        try:
            named = _Named.model_validate(given)
        except ValidationError as exc:
            raise _Usage(f"{path}: registries[{at}]: {_problems(exc)}") from exc
        if named.kind not in KINDS:
            raise _Usage(f"{path}: registries[{at}]: kind is one of {', '.join(KINDS)}")
        credential = os.environ.get(named.credential_env)
        if not credential:
            raise _Usage(
                f"{path}: registries[{at}]: the environment variable {named.credential_env} "
                "is not set"
            )
        ca_bundle = None
        if named.ca_file is not None:
            try:
                ca_bundle = Path(named.ca_file).read_text()
            except OSError as exc:
                raise _Usage(
                    f"{path}: registries[{at}]: cannot read {named.ca_file}: {exc.strerror}"
                ) from exc
        out.append(
            Resolved(
                f"registries[{at}]",
                named.kind,
                named.address,
                named.scope,
                credential,
                named.mirror,
                ca_bundle,
            )
        )
    return out


def _block(connectors: list[ConnectorConfig]) -> str:
    return yaml.safe_dump(
        {"connectors": [c.model_dump(exclude_defaults=True) for c in connectors]}, sort_keys=False
    )


def _load_connectors(path: str) -> list[ConnectorConfig]:
    """The ``connectors:`` block of ``path``, alone: the rest of the file is not read, so a build
    needs none of the model's secrets, and the file may be an include holding only the block."""
    from pydantic import TypeAdapter, ValidationError

    from provisa.core.models import ConnectorConfig, repeated_connector_names

    try:
        raw = yaml.safe_load(Path(path).read_text())
    except OSError as exc:
        raise _Usage(f"cannot read {path}: {exc.strerror}") from exc
    except yaml.YAMLError as exc:
        # The position only: the parser's message quotes the file, which may hold anything.
        mark = getattr(exc, "problem_mark", None)
        raise _Usage(
            f"{path} is not valid YAML{f' (line {mark.line + 1})' if mark else ''}"
        ) from exc
    if not isinstance(raw, dict) or "connectors" not in raw:
        raise _Usage(f"{path} has no connectors: block")
    try:
        connectors = TypeAdapter(list[ConnectorConfig]).validate_python(raw["connectors"] or [])
    except ValidationError as exc:
        raise _Usage(f"{path}: {_problems(exc)}") from exc
    repeated = repeated_connector_names(connectors)
    if repeated:
        raise _Usage(f"{path}: connector names used more than once: {repeated}")
    return connectors


def _replace_block(text: str, connectors: list[ConnectorConfig]) -> str:
    """``text`` with its top-level ``connectors:`` block replaced, every other line as it was;
    refused when the result would not read back as exactly those connectors."""
    lines = text.splitlines(keepends=True)
    start = next(i for i, line in enumerate(lines) if re.match(r"connectors\s*:", line))
    end = start + 1
    while end < len(lines) and not lines[end].startswith("---"):
        if lines[end].strip() and lines[end][0] not in " \t-":
            break
        end += 1
    while not lines[end - 1].strip():
        end -= 1  # blank lines before the next key stay with it
    replaced = "".join([*lines[:start], _block(connectors), *lines[end:]])
    wanted = yaml.safe_load(_block(connectors))["connectors"]
    try:
        reads_back = yaml.safe_load(replaced)
    except yaml.YAMLError as exc:
        raise _Refused("the connectors: block could not be replaced in place") from exc
    if not isinstance(reads_back, dict) or reads_back.get("connectors") != wanted:
        raise _Refused("the connectors: block could not be replaced in place")
    return replaced


def _cmd_build(args: argparse.Namespace) -> int:
    from pydantic import ValidationError

    from provisa.core.models import ConnectorConfig

    try:
        planned = ConnectorConfig(
            name=args.name,
            git=args.git,
            ref=args.ref,
            commit=args.commit or _UNBUILT_COMMIT,
            path=args.path,
            build_command=args.build_command,
            language=_UNBUILT_LANGUAGE,
            image=args.image,
            digest=_UNBUILT_DIGEST,
            code_hash=_UNBUILT_CODE_HASH,
            arch=args.arch,
        )
    except ValidationError as exc:
        raise _Usage(_problems(exc)) from exc
    registry = _registry_sign_in(args)
    package_registries = _package_registries(args.package_registries)
    try:
        commit = args.commit or _latest_commit(planned.git, planned.ref)
        built = _build(planned, commit, registry, package_registries)
    except _failures() as exc:
        print(f"connector {planned.name}: {exc}", file=sys.stderr)
        return 1
    sys.stdout.write(_block([built]))
    return 0


def _cmd_check(args: argparse.Namespace) -> int:
    code = 0
    for connector in _load_connectors(args.config):
        try:
            latest = _latest_commit(connector.git, connector.ref)
        except _Refused as exc:
            print(f"{connector.name}\tunknown\t{connector.commit}")
            print(f"connector {connector.name}: {exc}", file=sys.stderr)
            code = 1
            continue
        if latest == connector.commit:
            print(f"{connector.name}\tcurrent\t{connector.commit}")
        else:
            print(f"{connector.name}\tbehind\t{connector.commit}\t{latest}")
            code = 1
    return code


def _cmd_rebuild(args: argparse.Namespace) -> int:
    connectors = _load_connectors(args.config)
    registry = _registry_sign_in(args)
    package_registries = _package_registries(args.package_registries)
    failures = _failures()
    updated: list[ConnectorConfig] = []
    code = 0
    for connector in connectors:
        try:
            commit = (
                _latest_commit(connector.git, connector.ref) if args.latest else connector.commit
            )
            updated.append(_build(connector, commit, registry, package_registries))
            print(f"connector {connector.name}: built at {commit}", file=sys.stderr)
        except failures as exc:
            print(f"connector {connector.name}: {exc}", file=sys.stderr)
            updated.append(connector)
            code = 1
    if not args.write:
        sys.stdout.write(_block(updated))
        return code
    target = Path(args.config)
    try:
        target.write_text(_replace_block(target.read_text(), updated))
    except (_Refused, OSError) as exc:
        print(f"{args.config}: {exc}; its block is printed instead", file=sys.stderr)
        sys.stdout.write(_block(updated))
        return 1
    return code


def _running(command: Any) -> Any:
    """``command`` with a usage error reported as exit 2."""

    def run(args: argparse.Namespace) -> int:
        try:
            return command(args)
        except _Usage as exc:
            print(f"connector: {exc}", file=sys.stderr)
            return 2

    return run


def _registry_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--registry-user", default=None, help="Container registry user")
    parser.add_argument(
        "--registry-password-env",
        default=None,
        metavar="VARIABLE",
        help="The environment variable holding the container registry password",
    )
    parser.add_argument(
        "--package-registries",
        default=None,
        metavar="FILE",
        help=(
            "A YAML file naming the indexes for private dependencies; each entry names, in "
            "credential_env, the environment variable holding its credential"
        ),
    )


def register(sub) -> None:
    parser = sub.add_parser("connector", help="Build and update an organization's code connectors")
    actions = parser.add_subparsers(dest="connector_command", required=True)

    build = actions.add_parser(
        "build", help="Build a connector from a git project; prints its config block"
    )
    build.add_argument("git", help="The git project's address")
    build.add_argument("--name", required=True, help="The connector's name")
    build.add_argument("--ref", required=True, help="The branch or tag it follows")
    build.add_argument(
        "--commit", default=None, help="Build this commit (default: the ref's latest)"
    )
    build.add_argument(
        "--image", required=True, help="Where to push the image: registry-host/repository"
    )
    build.add_argument(
        "--arch", required=True, choices=("amd64", "arm64"), help="The image's architecture"
    )
    build.add_argument(
        "--path", default="", help="The project's directory in the repository (a monorepo)"
    )
    build.add_argument(
        "--build-command",
        default="",
        help="Run at the repository's root, before the build, to produce the project at --path",
    )
    _registry_flags(build)
    build.set_defaults(func=_running(_cmd_build))

    check = actions.add_parser("check", help="Report the connectors behind their git ref")
    check.add_argument(
        "--config", required=True, help="The file with the connectors: block (config or include)"
    )
    check.set_defaults(func=_running(_cmd_check))

    rebuild = actions.add_parser("rebuild", help="Build every connector in the config again")
    rebuild.add_argument(
        "--config", required=True, help="The file with the connectors: block (config or include)"
    )
    rebuild.add_argument(
        "--latest",
        action="store_true",
        help="Build each at its ref's latest, not its pinned commit",
    )
    rebuild.add_argument(
        "--write", action="store_true", help="Update the file's block instead of printing it"
    )
    _registry_flags(rebuild)
    rebuild.set_defaults(func=_running(_cmd_rebuild))
