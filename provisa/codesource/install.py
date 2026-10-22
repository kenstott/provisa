# Copyright (c) 2026 Kenneth Stott
# Canary: 39a3a31b-571b-47de-8868-b2c52d1b0859
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What every code source's install shares (REQ-1984, REQ-1993): its code fetched from a pinned
Git commit or an uploaded archive, hashed BEFORE any install, its dependencies installed by its
language, and the result cached by content hash.

A language adds its own package reference (an npm package, a PyPI wheel) and its own dependency
install; :func:`ensure_installed` takes both as callables. The content hash is what a source
records: a later install that finds other content is refused, naming both hashes.

Credentials come from the organisation's package registries only (core/package_registries). A
Git host's token is given to the fetch for that host and to nothing else. A registry's certificate
authority (REQ-2088) is trusted beside the publicly trusted ones, by the fetches made through that
registry and by the language's install, from a file in the install's own temporary directory.
"""

# Requirements: REQ-1984, REQ-1993, REQ-2088
from __future__ import annotations

import os
import re
import shutil
import ssl
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit

from provisa.codesource.bundle import BundleRefused, content_hash, unpack
from provisa.core.package_registries import GIT, Resolved

#: Written last into a complete install: the cache's mark that it may be used.
INSTALLED_MARK = ".provisa-installed"

#: The user name a Git host's HTTPS token is sent with.
GIT_TOKEN_USER = "x-access-token"  # nosec B105 - the user name a token is sent with, not a secret

_COMMIT = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class GitCode:
    repository: str
    #: The commit fetched: a tag is resolved to its commit when the source is saved.
    commit: str
    subdirectory: str = ""


@dataclass(frozen=True)
class UploadCode:
    """An archive uploaded to Provisa, read from the organisation's store by the caller."""

    filename: str
    data: bytes


#: What a code directory's copy leaves out (REQ-2121): what the developer's tools make, not code.
DIRECTORY_SKIPPED = frozenset({"node_modules", ".venv", "__pycache__", ".git"})


@dataclass(frozen=True)
class DirectoryCode:
    """A directory on this server, read where it is (REQ-2121): copied afresh each time the source
    is prepared, so an edit there is read by the next query."""

    path: str


@dataclass(frozen=True)
class Lockfile:
    """A lockfile given beside the code on the source's form, kept with the source: Git or uploaded
    code may carry none, and a package from npm or PyPI never does (``npm publish`` leaves out
    package-lock.json, and a wheel has no place for one). With it, every install is frozen. It is put into the code before
    the content hash is taken, so it is hashed like the code."""

    filename: str
    data: bytes


@dataclass(frozen=True)
class Installed:
    #: The installed bundle: its files and, where it has dependencies, what its language installed.
    path: Path
    content_hash: str


#: GET ``url`` with ``headers``; the body. Injected so tests never touch the network. A fetch
#: through a registry that carries a certificate authority passes it as ``ca_bundle=`` (REQ-2088).
Http = Callable[..., bytes]
#: Run ``argv`` in ``cwd`` with exactly ``env``; raises :class:`InstallFailed` on a non-zero exit.
Runner = Callable[[Sequence[str], Path, dict[str, str]], None]


class InstallFailed(BundleRefused):
    """The language's install exited with an error; its output is the message."""


def _publicly_trusted() -> str:
    """The publicly trusted certificate authorities, as PEM: the ones every fetch trusts."""
    import certifi

    return Path(certifi.where()).read_text()


def trust_context(ca_bundle: str) -> ssl.SSLContext:
    """A TLS context that trusts ``ca_bundle`` (PEM) beside the publicly trusted authorities."""
    import certifi

    context = ssl.create_default_context(cafile=certifi.where())
    try:
        context.load_verify_locations(cadata=ca_bundle)
    except ssl.SSLError as exc:
        raise BundleRefused(
            "ca_bundle_unreadable", f"the certificate authority could not be read: {exc}"
        ) from exc
    return context


def trust_bundle(ca_bundles: Sequence[str], scratch: Path) -> Path | None:
    """The file a language's install reads its trusted authorities from (uv's SSL_CERT_FILE,
    npm's cafile), written into ``scratch``, the install's own temporary directory: the publicly
    trusted authorities with every registry's own after them, since each of those settings
    replaces the authorities the tool trusts rather than adding to them. None when no registry
    carries one, and the tool trusts what it always does."""
    if not ca_bundles:
        return None
    for ca_bundle in ca_bundles:
        trust_context(ca_bundle)  # refused by name here, not as a TLS failure mid-install
    path = scratch / "registries-ca.pem"
    path.write_text(
        _publicly_trusted().rstrip("\n") + "\n" + "\n".join(c.strip() for c in ca_bundles) + "\n"
    )
    path.chmod(0o600)
    return path


def install_trust(registries: Sequence[Resolved], scratch: Path) -> Path | None:
    """The trusted-authorities file an install reads (:func:`trust_bundle`) with every
    registry's own certificate authority: one install reaches npm registries or PyPI indexes and
    Git hosts alike. None when no registry carries one."""
    return trust_bundle([r.ca_bundle for r in registries if r.ca_bundle], scratch)


def trust_environment(ca_file: Path | None) -> dict[str, str]:
    """What Git reads its trusted authorities from, in an install's own environment: a Git
    dependency is fetched by the git the language's install runs."""
    return {} if ca_file is None else {"GIT_SSL_CAINFO": str(ca_file)}


def http_get(url: str, headers: dict[str, str], ca_bundle: str | None = None) -> bytes:
    import httpx

    if not url.startswith("https://"):
        raise BundleRefused("not_https", f"refusing a non-https address: {url}", url=url)
    verify: ssl.SSLContext | bool = True if ca_bundle is None else trust_context(ca_bundle)
    try:
        response = httpx.get(
            url, headers=headers, follow_redirects=True, timeout=300.0, verify=verify
        )
    except httpx.HTTPError as exc:
        raise BundleRefused("fetch_failed", f"could not fetch {url}: {exc}", url=url) from exc
    if response.status_code != 200:
        raise BundleRefused(
            "fetch_failed", f"could not fetch {url}: HTTP {response.status_code}", url=url
        )
    return response.content


def get(http: Http, url: str, headers: dict[str, str], ca_bundle: str | None) -> bytes:
    """``http`` for a fetch through a registry: with its certificate authority when it has one."""
    return http(url, headers) if ca_bundle is None else http(url, headers, ca_bundle=ca_bundle)


def run_command(argv: Sequence[str], cwd: Path, env: dict[str, str]) -> None:
    # The language's own install, from the pinned runtime, with a fixed argv and no shell.
    done = subprocess.run(list(argv), cwd=cwd, env=env, capture_output=True, text=True)  # nosec B603
    if done.returncode != 0:
        output = (done.stdout + done.stderr).strip().splitlines()[-40:]
        raise InstallFailed(
            "install_failed",
            f"{Path(argv[0]).name} {' '.join(argv[1:3])} exited {done.returncode}: "
            + "\n".join(output),
        )


# -- Git hosts --------------------------------------------------------------------------------------


def git_credentials(credential: str) -> tuple[str, str]:
    """A Git host's credential as the user and token it is sent with: ``user:token`` as entered
    (GitLab, Bitbucket), else a bare token sent as ``x-access-token`` (GitHub) (REQ-2088)."""
    user, sep, token = credential.partition(":")
    return (user, token) if sep else (GIT_TOKEN_USER, credential)


def git_prefix(address: str) -> str:
    """A Git host entry's address as the prefix of the repository URLs it covers: the host, with
    its port and a path prefix where it has them (``bitbucket.acme.com:7990/bitbucket``)."""
    address = address.strip()
    if address.startswith("https://"):
        address = address[len("https://") :]
    return address.rstrip("/")


def gitconfig(registries: Sequence[Resolved]) -> str:
    """The Git configuration a dependency fetched by Git reads: each Git host's prefix rewritten
    to carry its user and token."""
    lines: list[str] = []
    for registry in registries:
        if registry.kind != GIT:
            continue
        prefix = git_prefix(registry.address)
        user, token = git_credentials(registry.credential)
        lines += [
            f'[url "https://{quote(user, safe="")}:{quote(token, safe="")}@{prefix}/"]',
            f"\tinsteadOf = https://{prefix}/",
        ]
    return "\n".join(lines) + "\n"


def git_host_for(url: str, registries: Sequence[Resolved]) -> Resolved | None:
    """The Git host entry whose prefix covers ``url``: the host with its port and path; the
    longest such prefix when several do."""
    parts = urlsplit(url)
    target = parts.netloc.rsplit("@", 1)[-1] + parts.path.rstrip("/")
    covering = [
        registry
        for registry in registries
        if registry.kind == GIT
        and (
            target == git_prefix(registry.address)
            or target.startswith(git_prefix(registry.address) + "/")
        )
    ]
    return max(covering, key=lambda registry: len(git_prefix(registry.address)), default=None)


# -- fetching the code ------------------------------------------------------------------------------


def _trusting_config(ca_file: Path):
    """The Git configuration a fetch reads, with ``http.sslCAInfo`` naming ``ca_file`` above
    whatever else configures it: the fetch's own, never written to a configuration file."""
    from dulwich.config import ConfigFile, StackedConfig

    trusting = ConfigFile()
    trusting.set((b"http",), b"sslCAInfo", str(ca_file).encode())
    config = StackedConfig.default()
    config.backends.insert(0, trusting)
    return config


def fetch_git(code: GitCode, registries: Sequence[Resolved], dest: Path) -> Path:
    """The repository's tree at the commit, and the bundle's subdirectory of it."""
    from dulwich import porcelain

    if not _COMMIT.match(code.commit):
        raise BundleRefused(
            "commit_not_pinned",
            f"{code.commit!r} is not a commit; a tag is resolved to its commit when the source is saved",
            commit=code.commit,
        )
    if not code.repository.startswith("https://"):
        raise BundleRefused(
            "not_https", f"refusing a non-https repository: {code.repository}", url=code.repository
        )
    host = git_host_for(code.repository, registries)
    try:
        with (
            open(os.devnull, "wb") as quiet,
            tempfile.TemporaryDirectory(prefix="provisa-git-fetch-") as scratch,
        ):
            options: dict = {}
            if host is not None:
                options["username"], options["password"] = git_credentials(host.credential)
            ca_file = trust_bundle(
                [host.ca_bundle] if host and host.ca_bundle else [], Path(scratch)
            )
            if ca_file is not None:
                options["config"] = _trusting_config(ca_file)
            repo = porcelain.clone(
                code.repository, str(dest), checkout=False, errstream=quiet, **options
            )
            porcelain.reset(repo, "hard", code.commit)
    except Exception as exc:  # noqa: BLE001 -- dulwich raises many kinds; each is a fetch that failed, named
        raise BundleRefused(
            "fetch_failed",
            f"could not fetch {code.repository} at {code.commit}: {type(exc).__name__}: {exc}",
            url=code.repository,
        ) from exc
    shutil.rmtree(dest / ".git", ignore_errors=True)
    root = (dest / code.subdirectory).resolve() if code.subdirectory else dest.resolve()
    if not root.is_relative_to(dest.resolve()) or not root.is_dir():
        raise BundleRefused(
            "no_subdirectory",
            f"{code.repository} at {code.commit} has no directory {code.subdirectory!r}",
            subdirectory=code.subdirectory,
        )
    return root


def fetch_directory(code: DirectoryCode, dest: Path) -> Path:
    """A copy of the code directory under ``dest``, without :data:`DIRECTORY_SKIPPED`."""
    shutil.copytree(
        code.path, dest, symlinks=True, ignore=shutil.ignore_patterns(*DIRECTORY_SKIPPED)
    )
    return dest.resolve()


def fetch_kept(
    code: GitCode | UploadCode | DirectoryCode, registries: Sequence[Resolved], dest: Path
) -> Path:
    """The code of a Git, uploaded or server directory reference, those every language shares."""
    if isinstance(code, GitCode):
        return fetch_git(code, registries, dest)
    if isinstance(code, DirectoryCode):
        return fetch_directory(code, dest)
    return unpack(code.filename, code.data, dest)


def place_lockfile(root: Path, lockfile: Lockfile, allowed: Sequence[str]) -> None:
    """Put the lockfile given beside the code into it. ``allowed`` is the lockfiles the language
    reads, in the order its installer reads them. One it does not read is refused by name, and so
    is one beside code that carries its own: a lockfile of the same name, or one the installer
    reads first (an npm package's published npm-shrinkwrap.json)."""
    if lockfile.filename not in allowed:
        raise BundleRefused(
            "lockfile_kind",
            f"{lockfile.filename} is not a lockfile this source installs from "
            f"({', '.join(allowed)})",
            filename=lockfile.filename,
        )
    ahead = allowed[: allowed.index(lockfile.filename) + 1]
    carried = next((name for name in ahead if (root / name).exists()), None)
    if carried is not None:
        raise BundleRefused(
            "lockfile_twice",
            f"the code already carries {carried}; it installs from that, not the "
            f"{lockfile.filename} given beside it",
            filename=carried,
        )
    (root / lockfile.filename).write_bytes(lockfile.data)


# -- the cache --------------------------------------------------------------------------------------


def install_root(cache_root: Path) -> Path:
    return cache_root / "code-bundles"


def ensure_installed(
    *,
    source_id: str,
    runtime: str,
    fetch: Callable[[Path], Path],
    install: Callable[[Path], None],
    expected_hash: str | None,
    cache_root: Path,
    lockfile: Lockfile | None = None,
    lockfiles: Sequence[str] = (),
) -> Installed:
    """The source's bundle, installed and cached under ``<cache_root>/code-bundles/<runtime>/
    <content hash>/``. A complete install already cached is used as it is.

    ``fetch(dest)`` puts the code under ``dest`` and returns the bundle's root; ``install(root)``
    installs its dependencies in place, or does nothing when it has none. ``expected_hash`` is the
    hash the source recorded when it was saved; None when it is being saved now, and the hash
    found is then the one to record. Content whose hash is not the recorded one is refused by the
    source's name and both hashes. ``lockfile``, given beside the reference, is put into the code
    (one of the language's ``lockfiles``) before the hash is taken."""
    base = install_root(cache_root) / runtime
    base.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=base))
    try:
        root = fetch(staging / "code")
        if lockfile is not None:
            place_lockfile(root, lockfile, lockfiles)
        found = content_hash(root)
        if expected_hash is not None and found != expected_hash:
            raise BundleRefused(
                "content_changed",
                f"source {source_id!r}: the code fetched has hash {found}, not the {expected_hash} "
                "recorded when the source was saved",
                source=source_id,
                expected=expected_hash,
                found=found,
            )
        target = base / found
        if (target / INSTALLED_MARK).exists():
            return Installed(target, found)
        install(root)
        (root / INSTALLED_MARK).write_text(found)
        if (target / INSTALLED_MARK).exists():  # another process completed it meanwhile
            return Installed(target, found)
        if target.exists():  # an incomplete install from a process that died: replaced
            shutil.rmtree(target)
        os.replace(root, target)
        return Installed(target, found)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
