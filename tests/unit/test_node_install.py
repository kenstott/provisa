# Copyright (c) 2026 Kenneth Stott
# Canary: 35078339-9619-4124-a41d-0c9d9d23bac4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node source's install (REQ-1984): its code fetched from a pinned reference and hashed before
anything is installed; its dependencies installed once, in the runtime's frozen mode with no
install scripts, through the organisation's registries, with configuration that lives in the
install's own directory only while it runs; the result cached by content hash.

No network and no runtime: a registry is a stand-in answering by URL, a Git repository is a real
local one, and the install command is recorded by a stand-in runner."""

# Requirements: REQ-1984
from __future__ import annotations

import base64
import hashlib
import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest

from provisa.core.package_registries import Resolved
from provisa.codesource import install as shared
from provisa.codesource.bundle import BundleRefused, content_hash, unpack
from provisa.codesource.install import GitCode, UploadCode
from provisa.node import install
from provisa.node.install import NpmCode, ensure_installed

pytestmark = pytest.mark.unit


def _tgz(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for name, data in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _zip(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


def _sri(data: bytes) -> str:
    return "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode()


ACME = Resolved("r1", "npm", "https://npm.acme.test/", "@acme", "acme-token", False)
MIRROR = Resolved("r2", "npm", "https://mirror.acme.test/npm/", None, "mirror-token", True)
GITHOST = Resolved("g1", "git", "git.acme.test", None, "git-token", False)

PLAIN = {"package/index.js": b"export function rows() { return [] }\n", "package/index.d.ts": b"x"}


class _Registry:
    """An npm registry answering one package, recording what each request carried."""

    def __init__(self, base: str, package: str, version: str, tarball: bytes, *, integrity=None):
        self.base, self.package, self.version, self.tarball = base, package, version, tarball
        self.tarball_url = f"{base}{package}/-/pkg-{version}.tgz"
        self.integrity = integrity if integrity is not None else _sri(tarball)
        self.requests: list[tuple[str, dict]] = []

    def __call__(self, url: str, headers: dict[str, str]) -> bytes:
        self.requests.append((url, dict(headers)))
        if url == self.tarball_url:
            return self.tarball
        # The npm registry API addresses a scoped package as @scope%2Fname.
        assert url == self.base + self.package.replace("/", "%2F"), url
        dist = {"tarball": self.tarball_url, "integrity": self.integrity}
        return json.dumps({"versions": {self.version: {"dist": dist}}}).encode()


class _Runner:
    """Records each install, and what the configuration said while it ran."""

    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[dict] = []
        self.fail = fail

    def __call__(self, argv, cwd: Path, env: dict[str, str]) -> None:
        self.calls.append(
            {
                "argv": list(argv),
                "cwd": cwd,
                "env": dict(env),
                "npmrc": Path(env["NPM_CONFIG_USERCONFIG"]).read_text(),
                "project_npmrc": (cwd / ".npmrc").read_text(),
                "gitconfig": Path(env["GIT_CONFIG_GLOBAL"]).read_text(),
            }
        )
        if self.fail:
            raise shared.InstallFailed("install_failed", "npm ci exited 1: lockfile out of date")
        (cwd / "node_modules").mkdir()
        (cwd / "node_modules" / "dep.js").write_text("// installed")


# -- the bundle's files ---------------------------------------------------------------------------


def test_the_content_hash_is_the_files_whatever_order_they_were_written_in(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    for root, order in ((a, ["x.js", "y/z.d.ts"]), (b, ["y/z.d.ts", "x.js"])):
        for name in order:
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            (root / name).write_text(f"// {name}")
    (b / ".git").mkdir()
    (b / ".git" / "HEAD").write_text("ref: refs/heads/main")  # never part of the bundle
    assert content_hash(a) == content_hash(b)
    (b / "x.js").write_text("// x.js ")
    assert content_hash(a) != content_hash(b)


def test_an_archive_is_unpacked_to_its_single_top_level_directory(tmp_path):
    root = unpack("pkg.tgz", _tgz(PLAIN), tmp_path / "t")
    assert root.name == "package" and (root / "index.d.ts").exists()
    flat = unpack("b.zip", _zip({"index.js": b"1", "index.d.ts": b"2"}), tmp_path / "z")
    assert flat == tmp_path / "z"


@pytest.mark.parametrize(
    ("filename", "data", "code"),
    [
        ("b.zip", _zip({"../escape.js": b"x"}), "codesource.archive_escapes"),
        ("b.tgz", _tgz({"../escape.js": b"x"}), "codesource.archive_escapes"),
        ("b.rar", b"rar", "codesource.archive_kind"),
        ("b.zip", b"not a zip", "codesource.archive_unreadable"),
    ],
    # Archive bytes carry a timestamp: named ids keep collection the same on every worker.
    ids=["zip-escapes", "tgz-escapes", "unknown-kind", "unreadable-zip"],
)
def test_an_archive_that_cannot_be_used_is_refused_by_name(tmp_path, filename, data, code):
    with pytest.raises(BundleRefused) as refused:
        unpack(filename, data, tmp_path / "x")
    assert refused.value.code == code
    assert not (tmp_path / "escape.js").exists()


def test_an_archive_that_expands_past_the_bound_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr("provisa.codesource.bundle.MAX_EXPANDED_BYTES", 10)
    with pytest.raises(BundleRefused) as refused:
        unpack("b.zip", _zip({"big.js": b"x" * 11}), tmp_path / "x")
    assert refused.value.code == "codesource.archive_too_large"


# -- the organisation's registries ----------------------------------------------------------------


def test_a_package_comes_from_its_scope_then_the_mirror_then_the_public_registry():
    assert install.npm_registry_for("@acme/rows", [ACME, MIRROR]) == ACME
    assert install.npm_registry_for("left-pad", [ACME, MIRROR]) == MIRROR
    assert install.npm_registry_for("@other/x", [ACME, MIRROR]) == MIRROR
    assert install.npm_registry_for("left-pad", [ACME]) is None


def test_the_npmrc_names_the_mirror_each_scope_and_each_registrys_token():
    assert install.npmrc([ACME, MIRROR, GITHOST]).splitlines() == [
        "@acme:registry=https://npm.acme.test/",
        "//npm.acme.test/:_authToken=acme-token",
        "registry=https://mirror.acme.test/npm/",
        "//mirror.acme.test/npm/:_authToken=mirror-token",
    ]


def test_the_git_configuration_carries_each_hosts_token():
    assert shared.gitconfig([ACME, GITHOST]).splitlines() == [
        '[url "https://x-access-token:git-token@git.acme.test/"]',
        "\tinsteadOf = https://git.acme.test/",
    ]


# -- fetching the code ----------------------------------------------------------------------------


def test_an_npm_package_is_fetched_at_its_exact_version_and_checked(tmp_path):
    registry = _Registry("https://npm.acme.test/", "@acme/rows", "1.2.3", _tgz(PLAIN))
    root = install.fetch_npm(NpmCode("@acme/rows", "1.2.3"), [ACME], registry, tmp_path / "c")
    assert (root / "index.d.ts").exists()
    # The organisation's token goes to its registry, for the metadata and the tarball alike.
    assert [h.get("Authorization") for _u, h in registry.requests] == ["Bearer acme-token"] * 2


def test_a_public_package_is_fetched_with_no_credential(tmp_path):
    registry = _Registry(install.NPM_PUBLIC, "left-pad", "1.3.0", _tgz(PLAIN))
    install.fetch_npm(NpmCode("left-pad", "1.3.0"), [ACME], registry, tmp_path / "c")
    assert all("Authorization" not in h for _u, h in registry.requests)


def test_a_tarball_that_is_not_the_registrys_is_refused(tmp_path):
    registry = _Registry(
        "https://npm.acme.test/", "@acme/rows", "1.2.3", _tgz(PLAIN), integrity=_sri(b"other")
    )
    with pytest.raises(BundleRefused) as refused:
        install.fetch_npm(NpmCode("@acme/rows", "1.2.3"), [ACME], registry, tmp_path / "c")
    assert refused.value.code == "codesource.integrity_mismatch"


@pytest.mark.parametrize("version", ["^1.2.3", "latest", "1.2", "~1.2.3"])
def test_a_version_that_is_not_exact_is_refused(tmp_path, version):
    with pytest.raises(BundleRefused) as refused:
        install.fetch_npm(NpmCode("left-pad", version), [], lambda u, h: b"", tmp_path / "c")
    assert refused.value.code == "codesource.version_not_exact"


@pytest.fixture
def git_repo(tmp_path, monkeypatch):
    """A real repository with two commits; the bundle in ``bundle/``. An https address of the
    organisation's Git host is cloned from it, and what the clone was given is recorded."""
    from dulwich import porcelain

    repo_dir = tmp_path / "origin"
    repo = porcelain.init(str(repo_dir))
    (repo_dir / "bundle").mkdir()
    (repo_dir / "bundle" / "index.d.ts").write_text("// first")
    porcelain.add(repo, [str(repo_dir / "bundle" / "index.d.ts")])
    first = porcelain.commit(repo, b"first", author=b"a <a@x>", committer=b"a <a@x>").decode()
    (repo_dir / "bundle" / "index.d.ts").write_text("// second")
    porcelain.add(repo, [str(repo_dir / "bundle" / "index.d.ts")])
    porcelain.commit(repo, b"second", author=b"a <a@x>", committer=b"a <a@x>")
    seen: dict = {}
    real_clone = porcelain.clone

    def clone(source, target, **kwargs):
        seen.update(
            source=source, **{k: v for k, v in kwargs.items() if k in ("username", "password")}
        )
        return real_clone(str(repo_dir), target, checkout=False, errstream=kwargs["errstream"])

    monkeypatch.setattr(porcelain, "clone", clone)
    return {"first": first, "seen": seen}


def test_a_git_repository_is_fetched_at_its_commit_and_subdirectory(tmp_path, git_repo):
    code = GitCode("https://git.acme.test/acme/rows.git", git_repo["first"], "bundle")
    root = shared.fetch_git(code, [GITHOST], tmp_path / "c")
    assert (root / "index.d.ts").read_text() == "// first"  # the pinned commit, not the head
    assert not (tmp_path / "c" / ".git").exists()
    assert git_repo["seen"] == {
        "source": "https://git.acme.test/acme/rows.git",
        "username": "x-access-token",
        "password": "git-token",
    }


def test_a_git_reference_that_is_not_a_commit_or_not_https_is_refused(tmp_path, git_repo):
    with pytest.raises(BundleRefused) as tag:
        shared.fetch_git(GitCode("https://git.acme.test/r.git", "v1.0.0"), [], tmp_path / "a")
    with pytest.raises(BundleRefused) as scheme:
        shared.fetch_git(GitCode("git@git.acme.test:r.git", git_repo["first"]), [], tmp_path / "b")
    with pytest.raises(BundleRefused) as missing:
        shared.fetch_git(
            GitCode("https://git.acme.test/r.git", git_repo["first"], "nope"), [], tmp_path / "c"
        )
    assert (tag.value.code, scheme.value.code, missing.value.code) == (
        "codesource.commit_not_pinned",
        "codesource.not_https",
        "codesource.no_subdirectory",
    )


# -- installing -----------------------------------------------------------------------------------


def _bundle(**files: str) -> UploadCode:
    return UploadCode("b.zip", _zip({name: text.encode() for name, text in files.items()}))


WITH_DEPS = {
    "index.js": "import x from 'dep'",
    "index.d.ts": "export {}",
    "package.json": json.dumps({"name": "b", "dependencies": {"dep": "1.0.0"}}),
    "package-lock.json": "{}",
}


def _ensure(
    tmp_path,
    code,
    *,
    runtime="node",
    expected=None,
    runner=None,
    registries=(ACME, MIRROR, GITHOST),
    lockfile=None,
):
    node = tmp_path / "rt" / "bin" / "node"
    npm = tmp_path / "rt" / "lib" / "node_modules" / "npm" / "bin" / "npm-cli.js"
    for path in (node, npm):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    return ensure_installed(
        source_id="rows",
        runtime=runtime,
        code=code,
        expected_hash=expected,
        registries=list(registries),
        runtime_executable=node,
        cache_root=tmp_path / "cache",
        lockfile=lockfile,
        run=runner or _Runner(),
    )


def test_a_bundle_with_no_dependencies_needs_no_install(tmp_path):
    runner = _Runner()
    got = _ensure(tmp_path, _bundle(**{"index.js": "1", "index.d.ts": "2"}), runner=runner)
    assert runner.calls == []
    assert got.path == tmp_path / "cache" / "code-bundles" / "node" / got.content_hash
    assert (got.path / "index.d.ts").exists()


def test_dependencies_install_frozen_without_scripts_through_the_organisations_registries(tmp_path):
    runner = _Runner()
    got = _ensure(tmp_path, _bundle(**WITH_DEPS), runner=runner)
    (call,) = runner.calls
    assert call["argv"][2:] == ["ci", "--ignore-scripts", "--no-audit", "--no-fund"]
    assert "//npm.acme.test/:_authToken=acme-token" in call["npmrc"]
    assert call["project_npmrc"] == call["npmrc"]
    assert "x-access-token:git-token@git.acme.test" in call["gitconfig"]
    # Nothing of the process's environment but PATH, and a HOME of the install's own.
    assert set(call["env"]) == {
        "PATH", "HOME", "USERPROFILE", "NPM_CONFIG_USERCONFIG", "npm_config_cache",
        "BUN_INSTALL_CACHE_DIR", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_NOSYSTEM", "GIT_TERMINAL_PROMPT",
        "DENO_DIR",
    }  # fmt: skip
    # The configuration is gone with the install; the installed bundle keeps no credential.
    assert not Path(call["env"]["NPM_CONFIG_USERCONFIG"]).exists()
    assert not Path(call["env"]["GIT_CONFIG_GLOBAL"]).exists()
    assert not (got.path / ".npmrc").exists()
    assert (got.path / "node_modules" / "dep.js").exists()
    assert all("token" not in p.read_text() for p in got.path.rglob("*") if p.is_file())


@pytest.mark.parametrize(
    ("runtime", "lockfile", "command"),
    [
        ("bun", "bun.lock", ["install", "--frozen-lockfile", "--ignore-scripts"]),
        ("deno", "deno.lock", ["install", "--frozen"]),
    ],
)
def test_each_runtime_installs_frozen_from_its_own_lockfile(tmp_path, runtime, lockfile, command):
    files = {k: v for k, v in WITH_DEPS.items() if k != "package-lock.json"}
    files[lockfile] = "{}"
    runner = _Runner()
    _ensure(tmp_path, _bundle(**files), runtime=runtime, runner=runner)
    assert runner.calls[0]["argv"][1:] == command


def test_dependencies_without_the_runtimes_lockfile_are_refused(tmp_path):
    files = {k: v for k, v in WITH_DEPS.items() if k != "package-lock.json"}
    with pytest.raises(BundleRefused) as refused:
        _ensure(tmp_path, _bundle(**files), runtime="bun")
    assert refused.value.code == "codesource.no_lockfile"
    assert "bun.lock or bun.lockb" in str(refused.value)
    assert "add one beside the package on the source's form" in str(refused.value)


NO_LOCK = {k: v for k, v in WITH_DEPS.items() if k != "package-lock.json"}


def test_a_lockfile_given_beside_the_package_is_installed_from_and_hashed_with_it(tmp_path):
    """A published npm package carries no package-lock.json (npm publish leaves it out): the
    source's form gives one beside the reference, and it is part of what the source records."""
    runner = _Runner()
    beside = shared.Lockfile("package-lock.json", b'{"lockfileVersion": 3}')
    got = _ensure(tmp_path, _bundle(**NO_LOCK), runner=runner, lockfile=beside)
    assert (got.path / "package-lock.json").read_bytes() == beside.data
    assert runner.calls[0]["argv"][2] == "ci"
    with_own = _ensure(
        tmp_path / "b", _bundle(**{**NO_LOCK, "package-lock.json": '{"lockfileVersion": 3}'})
    )
    assert got.content_hash == with_own.content_hash  # hashed exactly as if the code carried it


@pytest.mark.parametrize(
    ("beside", "files", "code"),
    [
        (shared.Lockfile("yarn.lock", b""), NO_LOCK, "codesource.lockfile_kind"),
        (shared.Lockfile("package-lock.json", b"{}"), WITH_DEPS, "codesource.lockfile_twice"),
    ],
)
def test_a_lockfile_beside_the_package_is_one_its_runtime_reads_and_replaces_nothing(
    tmp_path, beside, files, code
):
    with pytest.raises(BundleRefused) as refused:
        _ensure(tmp_path, _bundle(**files), lockfile=beside)
    assert refused.value.code == code


def test_a_cached_install_is_used_and_not_installed_again(tmp_path):
    runner = _Runner()
    first = _ensure(tmp_path, _bundle(**WITH_DEPS), runner=runner)
    again = _ensure(tmp_path, _bundle(**WITH_DEPS), expected=first.content_hash, runner=runner)
    assert again == first and len(runner.calls) == 1


def test_content_that_is_not_what_the_source_recorded_is_refused_by_both_hashes(tmp_path):
    with pytest.raises(BundleRefused) as refused:
        _ensure(tmp_path, _bundle(**WITH_DEPS), expected="0" * 64)
    assert refused.value.code == "codesource.content_changed"
    assert refused.value.params["expected"] == "0" * 64 and refused.value.params["source"] == "rows"


def test_a_failed_install_caches_nothing_and_leaves_no_configuration(tmp_path):
    runner = _Runner(fail=True)
    with pytest.raises(shared.InstallFailed):
        _ensure(tmp_path, _bundle(**WITH_DEPS), runner=runner)
    (call,) = runner.calls
    assert not Path(call["env"]["NPM_CONFIG_USERCONFIG"]).exists()
    assert [p.name for p in (tmp_path / "cache" / "code-bundles" / "node").iterdir()] == []


# -- uploads --------------------------------------------------------------------------------------


@pytest.fixture
def plane(control_plane, tmp_path, monkeypatch):
    from sqlalchemy import insert

    # The org's folder on this node, where an upload is unpacked (REQ-1992).
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    (tmp_path / "data" / "orgs" / "acme").mkdir(parents=True)

    from provisa.core.schema_admin import orgs

    with control_plane.engine().begin() as conn:
        conn.execute(insert(orgs).values(id="acme", name="acme"))  # org_code_bundles' foreign key
    return control_plane.platform


async def test_an_upload_is_kept_by_its_bundles_content_hash(plane, tmp_path):
    from provisa.codesource import uploads

    data = _zip({"index.js": b"1", "index.d.ts": b"2"})
    found = await uploads.put(plane, "acme", "b.zip", data, actor="uid-ada")
    assert found == content_hash(unpack("b.zip", data, tmp_path / "x"))
    assert await uploads.put(plane, "acme", "b.zip", data, actor="uid-ada") == found  # once
    kept = await uploads.read(plane, "acme", found)
    assert (kept.filename, kept.data) == ("b.zip", data)
    with pytest.raises(BundleRefused) as other_org:
        await uploads.read(plane, "globex", found)
    assert other_org.value.code == "codesource.upload_not_found"


async def test_an_upload_that_cannot_be_used_is_refused_and_not_kept(plane):
    from provisa.codesource import uploads

    with pytest.raises(BundleRefused):
        await uploads.put(plane, "acme", "b.zip", _zip({"../x": b"1"}), actor=None)
    async with plane.acquire() as conn:
        from sqlalchemy import func, select

        from provisa.core.schema_admin import org_code_bundles

        count = (
            await conn.execute_core(select(func.count()).select_from(org_code_bundles))
        ).scalar()
    assert count == 0
