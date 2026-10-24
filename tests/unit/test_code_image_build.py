# Copyright (c) 2026 Kenneth Stott
# Canary: 4d363b9c-0253-4d23-8726-1a72f0965bbd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build image (REQ-2098): a code source's remote host image, assembled for a Linux platform from
the code at its pinned hash, its dependencies installed for that platform (wheels only; npm with no
install scripts), the pinned runtime and Arrow library read from their archives, and the front end
every image runs with the language's host program as its worker (REQ-2100), on the pinned base --
here with a base registry answered in-process, the runtimes' archives already in the cache, and the
installs' commands recorded rather than run."""

# Requirements: REQ-2098, REQ-2100

from __future__ import annotations

import gzip
import hashlib
import io
import json
import platform
import shutil
import tarfile
import zipfile
from pathlib import Path

import httpx
import pytest

from provisa.codesource import image_build, oci
from provisa.codesource.bundle import content_hash
from provisa.codesource.worker import PROTOCOL
from provisa.runtime_deps.code_runtimes import RUNTIMES, LINUX_ARM64, LINUX_X64

from tests.unit.test_code_image_oci import _Registry

pytestmark = pytest.mark.unit


def _tgz(files: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o755
            tar.addfile(info, io.BytesIO(data))
    return raw.getvalue()


def _zip(files: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    return raw.getvalue()


def _cache(root: Path, platform_key: tuple[str, str]) -> None:
    """The pinned archives a build reads, already in the cache under their pinned sums."""
    archives = root / "code" / "archives"
    archives.mkdir(parents=True, exist_ok=True)
    for name in ("python", "pyarrow", "node", "apache-arrow"):
        asset = RUNTIMES[name].asset(platform_key)
        data = (
            _zip({asset.executable: b"lib"})
            if asset.filename.endswith(".whl")
            else _tgz({asset.executable: b"exe"})
        )
        (archives / asset.sha256).write_bytes(data)
    # The server's own uv and node, as resolved on this machine.
    here = (platform.system(), platform.machine())
    for name in ("uv", "node"):
        pin = RUNTIMES[name]
        exe = root / "code" / name / pin.version / pin.asset(here).executable
        exe.parent.mkdir(parents=True, exist_ok=True)
        exe.write_text("")
    npm = (
        root / "code" / "node" / RUNTIMES["node"].version
        / Path(RUNTIMES["node"].asset(here).executable).parent.parent
        / "lib" / "node_modules" / "npm" / "bin" / "npm-cli.js"
    )  # fmt: skip
    npm.parent.mkdir(parents=True, exist_ok=True)
    npm.write_text("")


@pytest.fixture
def base(tmp_path: Path) -> oci.Registry:
    """The base image, pushed to an in-process registry under the pinned digests' places."""
    served = _Registry()
    registry = oci.Registry("reg.test", "bot", "x", transport=httpx.MockTransport(served))
    layer = oci.write_layer([oci.Entry("/usr/lib/libc.so.6", data=b"glibc")], tmp_path / "base")
    image = oci.Image("amd64", [layer], ["/bin/false"], base_config={"Env": ["PATH=/usr/bin"]})
    registry.push(image_build.BASE_REPOSITORY, "latest", image)
    digest = "sha256:" + hashlib.sha256(image.manifest()).hexdigest()
    served.manifests[digest] = served.manifests["latest"]  # pulled by its digest
    return registry, digest


NAMES = {"rates": {"columns": [], "parameters": [], "pushdown": {}}}
FRONT = [
    "-I",
    "/opt/provisa/front/front.py",
    "7443",
    "/var/lib/provisa",
    "/opt/provisa/front/names.json",
]


def _plan(root: Path, language: str, runtime: str, cache: Path, entry: str | None = None):
    return image_build.Plan(
        source_id="Rates Source",
        language=language,
        runtime=runtime,
        code_hash=content_hash(root),
        entry=entry,
        exports=["rates"],
        names=NAMES,
        registries=[],
        cache_root=cache,
        version="0.1.0a7+local",
    )


def _layers_paths(built: image_build.Built) -> set[str]:
    out: set[str] = set()
    for layer in built.image.layers:
        with gzip.open(layer.blob) as plain, tarfile.open(fileobj=io.BytesIO(plain.read())) as tar:
            out |= {m.name for m in tar.getmembers()}
    return out


@pytest.fixture
def pinned_base(base, monkeypatch):
    registry, digest = base
    monkeypatch.setattr(image_build, "BASE_DIGESTS", {"amd64": digest, "arm64": digest})
    return registry


def _fetched_from(src: Path):
    def fetched(plan, code, lock, dest):
        shutil.copytree(src, dest)
        return dest

    return fetched


def test_a_python_source_is_built_for_linux_with_its_wheels_and_its_host(
    tmp_path: Path, pinned_base, monkeypatch
):
    code = tmp_path / "src"
    code.mkdir()
    (code / "rates.py").write_text(
        "def rates(n: int) -> list[dict]:\n    '''@table'''\n    return []\n"
    )
    (code / "requirements.txt").write_text("six==1.17.0 --hash=sha256:" + "a" * 64 + "\n")
    cache = tmp_path / "cache"
    _cache(cache, LINUX_X64)
    monkeypatch.setattr(image_build, "_fetched", _fetched_from(code))
    ran: list[list[str]] = []

    def run(argv, cwd, env):
        ran.append(list(argv))
        target = Path(argv[argv.index("--target") + 1])
        front = argv[-1].endswith("front_requirements.txt")
        (target / ("duckdb.py" if front else "six.py")).write_text("")

    built = image_build.assemble(
        _plan(code, "python", "python", cache, "rates"), None, None, "amd64", tmp_path / "w",
        run=run, base=pinned_base,
    )  # fmt: skip
    assert built.repository == "provisa-code-rates-source"
    assert built.tag == f"{content_hash(code)[:12]}-0.1.0a7-local"
    for argv in ran:
        assert argv[argv.index("--python-platform") + 1] == "x86_64-manylinux_2_28"
        assert "--only-binary" in argv and ":all:" in argv and "--require-hashes" in argv
    assert ran[-1][-1].endswith("front_requirements.txt")
    config = json.loads(built.image.config())["config"]
    python = RUNTIMES["python"].asset(LINUX_X64).executable
    assert config["Entrypoint"] == [
        f"/opt/provisa/runtime/{python}", *FRONT,
        "/opt/provisa/venv/bin/python", "-I", "/opt/provisa/host/host.py", "{fd}",
        "/opt/provisa/code", "rates",
    ]  # fmt: skip
    assert config["User"] == "65532:65532" and config["ExposedPorts"] == {"7443/tcp": {}}
    assert config["Labels"] == {
        "dev.provisa.codehost": "1",
        "dev.provisa.protocol": str(PROTOCOL),
        "dev.provisa.runtime": "python",
        "dev.provisa.code_hash": content_hash(code),
        "dev.provisa.exports": "rates",
        "dev.provisa.names": json.dumps(NAMES, sort_keys=True, separators=(",", ":")),
    }
    assert "PROVISA_CODE_HASH=" + content_hash(code) in config["Env"]
    paths = _layers_paths(built)
    assert {
        "usr/lib/libc.so.6",
        "opt/provisa/runtime/python/bin/python3.12",
        "opt/provisa/hostlib/pyarrow/__init__.py",
        "opt/provisa/host/host.py",
        "opt/provisa/front/front.py",
        "opt/provisa/front/residual.py",
        "opt/provisa/front/duckdb_sql.py",
        "opt/provisa/front/pgfront.py",
        "opt/provisa/front/pgcatalog.py",
        "opt/provisa/front/names.json",
        "opt/provisa/frontlib/duckdb.py",
        "opt/provisa/frontlib/buenavista/postgres.py",
        "opt/provisa/venv/pyvenv.cfg",
        "opt/provisa/venv/bin/python",
        "opt/provisa/venv/lib/python3.12/site-packages/six.py",
        "opt/provisa/code/rates.py",
        "var/lib/provisa",
    } <= paths


def test_a_node_source_installs_its_packages_for_the_images_platform(
    tmp_path: Path, pinned_base, monkeypatch
):
    code = tmp_path / "src"
    code.mkdir()
    (code / "package.json").write_text(
        json.dumps(
            {"name": "r", "type": "module", "main": "index.js", "dependencies": {"dayjs": "1"}}
        )
    )
    (code / "package-lock.json").write_text("{}")
    (code / "index.js").write_text("export function rates() { return []; }\n")
    cache = tmp_path / "cache"
    _cache(cache, LINUX_ARM64)
    monkeypatch.setattr(image_build, "_fetched", _fetched_from(code))
    ran: list[list[str]] = []

    def run(argv, cwd, env):
        ran.append(list(argv))
        if "--target" in argv:  # the front end's own libraries
            (Path(argv[argv.index("--target") + 1]) / "duckdb.py").write_text("")
            return
        (cwd / "node_modules" / "dayjs").mkdir(parents=True)

    built = image_build.assemble(
        _plan(code, "node", "node", cache),
        None,
        None,
        "arm64",
        tmp_path / "w",
        run=run,
        base=pinned_base,
    )
    npm, front = ran
    assert front[-1].endswith("front_requirements.txt")
    assert "ci" in npm and "--ignore-scripts" in npm
    assert {"--os=linux", "--cpu=arm64", "--libc=glibc"} <= set(npm)
    config = json.loads(built.image.config())
    node = RUNTIMES["node"].asset(LINUX_ARM64).executable
    python = RUNTIMES["python"].asset(LINUX_ARM64).executable
    assert config["architecture"] == "arm64"
    assert config["config"]["Entrypoint"] == [
        f"/opt/provisa/front-runtime/{python}", *FRONT,
        f"/opt/provisa/runtime/{node}", "/opt/provisa/host/host.mjs", "{fd}",
        "/opt/provisa/code/index.js",
    ]  # fmt: skip
    paths = _layers_paths(built)
    assert {
        f"opt/provisa/runtime/{node}",
        "opt/provisa/hostlib/package/Arrow.es2015.min.js",
        "opt/provisa/host/host-core.mjs",
        "opt/provisa/host/host.mjs",
        f"opt/provisa/front-runtime/{python}",
        "opt/provisa/front/front.py",
        "opt/provisa/frontlib/duckdb.py",
        "opt/provisa/code/node_modules/dayjs",
    } <= paths
    assert not (code / ".npmrc").exists()


def test_an_image_is_built_for_amd64_or_arm64_only(tmp_path: Path, pinned_base):
    code = tmp_path / "src"
    code.mkdir()
    with pytest.raises(image_build.ImageBuildRefused) as refused:
        image_build.assemble(
            _plan(code, "python", "python", tmp_path), None, None, "riscv64", tmp_path / "w"
        )
    assert "riscv64" in str(refused.value)


def test_code_whose_hash_is_not_the_pinned_one_is_refused(tmp_path: Path, pinned_base, monkeypatch):
    code = tmp_path / "src"
    code.mkdir()
    (code / "rates.py").write_text("x = 1\n")
    plan = _plan(code, "python", "python", tmp_path)
    (code / "rates.py").write_text("x = 2\n")  # the code changed after it was pinned
    from provisa.python import install as language  # the real check, on a fetched copy

    monkeypatch.setattr(
        language, "fetch", lambda code_, registries, http, dest: shutil.copytree(code, dest)
    )
    with pytest.raises(image_build.ImageBuildRefused) as refused:
        image_build.assemble(plan, None, None, "amd64", tmp_path / "w", base=pinned_base)
    assert refused.value.code == "codesource.content_changed"


def test_a_deno_lockfile_with_platform_packages_is_refused_off_its_platform(
    tmp_path: Path, pinned_base, monkeypatch
):
    code = tmp_path / "src"
    code.mkdir()
    (code / "package.json").write_text(json.dumps({"name": "r", "dependencies": {"x": "1"}}))
    (code / "deno.lock").write_text(
        json.dumps(
            {
                "version": "5",
                "npm": {"@esbuild/linux-x64@0.25.0": {"os": ["linux"], "cpu": ["x64"]}},
            }
        )
    )
    (code / "index.js").write_text("export const x = 1;\n")
    cache = tmp_path / "cache"
    _cache(cache, LINUX_ARM64)
    pin = RUNTIMES["deno"]
    exe = (
        cache
        / "code"
        / "deno"
        / pin.version
        / pin.asset((platform.system(), platform.machine())).executable
    )
    exe.parent.mkdir(parents=True, exist_ok=True)
    exe.write_text("")
    monkeypatch.setattr(image_build, "_fetched", _fetched_from(code))
    with pytest.raises(image_build.ImageBuildRefused) as refused:
        image_build.assemble(
            _plan(code, "node", "deno", cache), None, None, "arm64", tmp_path / "w",
            run=lambda *a: None, base=pinned_base,
        )  # fmt: skip
    assert "@esbuild/linux-x64@0.25.0" in str(refused.value)


def test_the_compose_service_requires_its_host_port_and_keeps_its_state():
    snippet = image_build.compose_snippet(
        "ghcr.io/acme/provisa-code-rates:abc", "provisa-code-rates"
    )
    assert (
        '"${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"' in snippet
    )
    assert "provisa-code-rates-state:/var/lib/provisa" in snippet
    assert "image: ghcr.io/acme/provisa-code-rates:abc" in snippet
