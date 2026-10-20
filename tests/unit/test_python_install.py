# Copyright (c) 2026 Kenneth Stott
# Canary: 753d4116-8eef-4bd7-b0b3-d6a079c4a577
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's install (REQ-1993): its code fetched as a wheel for the pinned Python from
the organisation's default index (else the public PyPI) and checked against the index's SHA-256,
never built from an sdist (P2); its dependencies installed by the pinned uv from its lockfile (P4),
binary wheels only, with the indexes' credentials in the install's environment alone; the result
cached by content hash.

No network and no uv: an index is a stand-in answering by URL, and uv's commands are recorded by
a stand-in runner."""

# Requirements: REQ-1993
from __future__ import annotations

import base64
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import UploadCode
from provisa.core.package_registries import Resolved
from provisa.python import install
from provisa.python.install import PypiCode, ensure_installed, wheel_suits, wheel_top_level

pytestmark = pytest.mark.unit

INDEX = Resolved("i1", "pypi", "https://pypi.acme.test/simple/", None, "made-up-index-token", True)
EXTRA = Resolved("i2", "pypi", "https://extra.acme.test/simple/", None, "ada:made-up-pw", False)
NPM = Resolved("n1", "npm", "https://npm.acme.test/", "@acme", "made-up-npm", False)


def _wheel(
    files: dict[str, str], *, requires: tuple[str, ...] = (), top_level: bool = True
) -> bytes:
    info = "rates-1.2.0.dist-info"
    entries = dict(files)
    entries[f"{info}/METADATA"] = "Metadata-Version: 2.1\nName: rates\nVersion: 1.2.0\n" + "".join(
        f"Requires-Dist: {r}\n" for r in requires
    )
    if top_level:
        entries[f"{info}/top_level.txt"] = "rates\n"
    entries[f"{info}/RECORD"] = "".join(f"{name},,\n" for name in entries) + f"{info}/RECORD,,\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in entries.items():
            z.writestr(name, text)
    return buf.getvalue()


WHEEL = _wheel({"rates/__init__.py": "def rate() -> float:\n    return 1.0\n"})


class _Index:
    """An index answering its simple JSON listing and its files, recording what it was asked."""

    def __init__(self, base: str, files: dict[str, bytes], hashes: dict[str, str] | None = None):
        self.base, self.files, self.asked = base, files, []
        self.hashes = hashes or {n: hashlib.sha256(d).hexdigest() for n, d in files.items()}

    def __call__(self, url: str, headers: dict[str, str]) -> bytes:
        self.asked.append((url, headers))
        if url == f"{self.base}rates/":
            listing = [
                {
                    "filename": n,
                    "url": f"{self.base}files/{n}",
                    "hashes": {"sha256": self.hashes[n]},
                }
                for n in self.files
            ]
            return json.dumps({"files": listing}).encode()
        return self.files[url.rsplit("/", 1)[1]]


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


# -- the code -----------------------------------------------------------------------------------


def test_the_package_is_its_wheel_from_the_organisations_default_index(tmp_path):
    index = _Index(
        INDEX.address, {"rates-1.2.0-py3-none-any.whl": WHEEL, "rates-1.2.0.tar.gz": b"x"}
    )
    root = install.fetch_wheel(PypiCode("rates", "1.2.0"), [INDEX, NPM], index, tmp_path / "c")
    assert (root / "rates" / "__init__.py").is_file()
    assert wheel_top_level(root) == ["rates"]
    # The token goes to the index, as PyPI's __token__ user.
    assert all(h["Authorization"] == _basic("__token__", INDEX.credential) for _, h in index.asked)


def test_a_package_comes_from_the_public_pypi_with_no_credential_when_no_default_index(tmp_path):
    index = _Index(install.PYPI_PUBLIC, {"rates-1.2.0-py3-none-any.whl": WHEEL})
    install.fetch_wheel(PypiCode("rates", "1.2.0"), [EXTRA], index, tmp_path / "c")
    assert all("Authorization" not in h for _, h in index.asked)


def test_a_wheel_that_is_not_the_indexs_is_refused(tmp_path):
    name = "rates-1.2.0-py3-none-any.whl"
    index = _Index(INDEX.address, {name: WHEEL}, hashes={name: "0" * 64})
    with pytest.raises(BundleRefused) as refused:
        install.fetch_wheel(PypiCode("rates", "1.2.0"), [INDEX], index, tmp_path / "c")
    assert refused.value.code == "codesource.integrity_mismatch"


def test_a_package_with_only_a_source_distribution_is_never_built(tmp_path):
    index = _Index(
        INDEX.address, {"rates-1.2.0.tar.gz": b"x", "rates-1.2.0-cp311-cp311-win_amd64.whl": WHEEL}
    )
    with pytest.raises(BundleRefused) as refused:
        install.fetch_wheel(PypiCode("rates", "1.2.0"), [INDEX], index, tmp_path / "c")
    assert refused.value.code == "codesource.no_wheel"
    assert "a source distribution is never built" in str(refused.value)


@pytest.mark.parametrize(
    ("version", "code"), [("1.2", "version_not_found"), ("~=1.2", "version_not_exact")]
)
def test_a_version_that_is_not_there_or_not_exact_is_refused(tmp_path, version, code):
    index = _Index(INDEX.address, {"rates-1.2.0-py3-none-any.whl": WHEEL})
    with pytest.raises(BundleRefused) as refused:
        install.fetch_wheel(PypiCode("rates", version), [INDEX], index, tmp_path / "c")
    assert refused.value.code == f"codesource.{code}"


@pytest.mark.parametrize(
    ("filename", "suits"),
    [
        ("rates-1.2.0-py3-none-any.whl", True),
        ("rates-1.2.0-py2.py3-none-any.whl", True),
        ("rates-1.2.0-cp312-abi3-any.whl", True),
        ("rates-1.2.0-cp311-cp311-any.whl", False),  # another Python
        ("rates-1.2.0-cp312-cp312-win_amd64.whl", False),  # no platform Provisa runs code on
    ],
)
def test_a_wheel_suits_the_pinned_python(filename, suits):
    assert wheel_suits(filename) is suits


def test_a_wheel_without_top_level_names_its_imports_by_its_record(tmp_path):
    data = _wheel({"rates/__init__.py": "", "rates_cli.py": ""}, top_level=False)
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        z.extractall(tmp_path)
    assert wheel_top_level(tmp_path) == ["rates", "rates_cli"]


# -- the lockfile -------------------------------------------------------------------------------


def test_code_with_no_dependencies_needs_no_lockfile(tmp_path):
    (tmp_path / "rates.py").write_text("")
    assert install.lockfile(tmp_path) is None


def test_dependencies_without_a_lockfile_are_refused(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "rates"\ndependencies = ["httpx>=0.27"]\n'
    )
    with pytest.raises(BundleRefused) as refused:
        install.lockfile(tmp_path)
    assert refused.value.code == "codesource.no_lockfile"
    assert "httpx>=0.27" in str(refused.value)


def test_a_wheels_dependencies_without_a_lockfile_are_refused(tmp_path):
    with zipfile.ZipFile(
        io.BytesIO(_wheel({"rates/__init__.py": ""}, requires=("httpx>=0.27",)))
    ) as z:
        z.extractall(tmp_path)
    with pytest.raises(BundleRefused, match="httpx>=0.27"):
        install.lockfile(tmp_path)


def test_a_requirement_without_a_hash_is_refused(tmp_path):
    (tmp_path / "requirements.txt").write_text(
        "httpx==0.27.0 \\\n    --hash=sha256:aa\nsix==1.16.0\n"
    )
    with pytest.raises(BundleRefused) as refused:
        install.lockfile(tmp_path)
    assert refused.value.code == "codesource.unhashed_requirement"
    assert "six==1.16.0" in str(refused.value)
    assert "httpx" not in str(refused.value)


def test_uv_lock_is_installed_with_its_project(tmp_path):
    (tmp_path / "uv.lock").write_text("")
    with pytest.raises(BundleRefused) as refused:
        install.lockfile(tmp_path)
    assert refused.value.code == "codesource.no_project"


# -- the install --------------------------------------------------------------------------------


class _Runner:
    def __init__(self):
        self.ran: list[tuple[list[str], Path, dict[str, str]]] = []

    def __call__(self, argv, cwd, env):
        self.ran.append((list(argv), cwd, dict(env)))


def _project(files: dict[str, str]) -> UploadCode:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(f"rates/{name}", text)
    return UploadCode("rates.zip", buf.getvalue())


HASHED = "httpx==0.27.0 \\\n    --hash=sha256:aa\n"


def _ensure(tmp_path, code, runner, registries=(INDEX, EXTRA)):
    return ensure_installed(
        source_id="rates-src",
        code=code,
        expected_hash=None,
        registries=list(registries),
        python=Path("/rt/python/bin/python3.12"),
        uv=Path("/rt/uv/uv"),
        cache_root=tmp_path / "cache",
        run=runner,
    )


@pytest.mark.parametrize(
    ("lock", "files", "sync"),
    [
        ("requirements.txt", {"requirements.txt": HASHED}, ["pip", "sync", "--python"]),
        ("pylock.toml", {"pylock.toml": "lock-version = '1.0'\n"}, ["pip", "sync", "--python"]),
        (
            "uv.lock",
            {"uv.lock": "", "pyproject.toml": '[project]\nname = "rates"\n'},
            ["sync", "--frozen"],
        ),
    ],
)
def test_dependencies_install_from_the_lockfile_into_a_venv_from_the_pinned_python(
    tmp_path, lock, files, sync
):
    runner = _Runner()
    got = _ensure(tmp_path, _project({"rates.py": "", **files}), runner)
    assert got.path == tmp_path / "cache" / "code-bundles" / "python" / got.content_hash
    (make, cwd, _), (installing, _, env) = runner.ran
    assert make[:4] == ["/rt/uv/uv", "venv", "--python", "/rt/python/bin/python3.12"]
    assert installing[1 : 1 + len(sync)] == sync
    assert "--no-build" in installing
    assert ("--require-hashes" in installing) is (lock == "requirements.txt")
    # The indexes' credentials are in the install's environment, and nothing else of this process.
    assert env["UV_DEFAULT_INDEX"] == "https://__token__:made-up-index-token@pypi.acme.test/simple/"
    assert env["UV_INDEX"] == "https://ada:made-up-pw@extra.acme.test/simple/"
    assert env["UV_NO_CONFIG"] == "1" and env["UV_PYTHON_DOWNLOADS"] == "never"
    assert not any(
        "made-up" in p.read_text(errors="ignore") for p in got.path.rglob("*") if p.is_file()
    )


def test_code_with_no_dependencies_installs_nothing_and_runs_on_the_pinned_python(tmp_path):
    runner = _Runner()
    got = _ensure(tmp_path, _project({"rates.py": ""}), runner)
    assert runner.ran == []
    assert install.environment_python(got.path, Path("/rt/python")) == Path("/rt/python")


def test_a_failed_install_caches_nothing(tmp_path):
    def _fails(argv, cwd, env):
        raise install.shared.InstallFailed("install_failed", "uv pip sync exited 2: no wheel for x")

    with pytest.raises(install.shared.InstallFailed):
        _ensure(tmp_path, _project({"requirements.txt": HASHED}), _fails)
    assert [p.name for p in (tmp_path / "cache" / "code-bundles" / "python").iterdir()] == []


def test_a_wheels_dependencies_install_from_a_lockfile_given_beside_the_package(tmp_path):
    """A wheel has no place for a lockfile: the source's form gives one beside the reference,
    hashed with the code, and the install is frozen to it."""
    name = "rates-1.2.0-py3-none-any.whl"
    wheel = _wheel({"rates/__init__.py": "import httpx\n"}, requires=("httpx>=0.27",))
    index = _Index(INDEX.address, {name: wheel})
    runner = _Runner()
    beside = install.shared.Lockfile("requirements.txt", HASHED.encode())
    got = ensure_installed(
        source_id="rates-src",
        code=PypiCode("rates", "1.2.0"),
        expected_hash=None,
        registries=[INDEX],
        python=Path("/rt/python/bin/python3.12"),
        uv=Path("/rt/uv/uv"),
        cache_root=tmp_path / "cache",
        lockfile=beside,
        http=index,
        run=runner,
    )
    assert (got.path / "requirements.txt").read_bytes() == beside.data
    assert "--require-hashes" in runner.ran[1][0]


def test_a_wheels_dependencies_with_no_lockfile_beside_it_are_refused_saying_to_add_one(tmp_path):
    name = "rates-1.2.0-py3-none-any.whl"
    index = _Index(INDEX.address, {name: _wheel({"rates/__init__.py": ""}, requires=("httpx",))})
    with pytest.raises(BundleRefused) as refused:
        ensure_installed(
            source_id="rates-src",
            code=PypiCode("rates", "1.2.0"),
            expected_hash=None,
            registries=[INDEX],
            python=Path("/rt/python"),
            uv=Path("/rt/uv"),
            cache_root=tmp_path / "cache",
            http=index,
            run=_Runner(),
        )
    assert refused.value.code == "codesource.no_lockfile"
    assert "add one beside the code on the source's form" in str(refused.value)
