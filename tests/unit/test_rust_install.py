# Copyright (c) 2026 Kenneth Stott
# Canary: 5f1a9c63-0e2b-47d8-9b34-c6e8d2a7f145
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source's install and its host's build (REQ-2227), without compiling: the generated
calls of the code's functions, the joined Cargo.lock, the build's Cargo.toml, how cargo reaches
the organisation's registry, where a crate is downloaded from, the static target, and the
pinned toolchain. Compiling and serving are tests/integration/test_rust_worker.py's."""

# Requirements: REQ-2227, REQ-2088
from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import RUST_RULES, read_manifest
from provisa.core.package_registries import CARGO, KINDS, Resolved
from provisa.runtime_deps.code_runtimes import RUNTIMES
from provisa.rustlang import build
from provisa.rustlang.install import (
    CrateCode,
    check_lockfile,
    download_url,
    fetch_crate,
    registry_setup,
)
from provisa.rustlang.reader import cargo_environment
from provisa.rustlang.toolchain import Toolchain, host_triple, static_target

pytestmark = pytest.mark.unit


def _signature(parameters: list[dict], returns: dict, fails: bool = False) -> dict:
    return {"parameters": parameters, "returns": returns, "context": False, "fails": fails}


def _parameter(name: str, kind: str = "string") -> dict:
    return {"name": name, "optional": False, "rest": False, "type": {"kind": kind}}


ROW = {"kind": "ref", "name": "Row"}
DOCUMENT = {
    "exports": [
        {
            "name": "orders",
            "tags": ["table"],
            "tag_arguments": [],
            "description": "",
            "signatures": [
                _signature(
                    [{**_parameter("inputs"), "type": {"kind": "ref", "name": "Inputs"}}],
                    {"kind": "array", "element": ROW},
                    fails=True,
                )
            ],
        },
        {
            "name": "all",
            "tags": ["table"],
            "tag_arguments": [],
            "description": "",
            "signatures": [
                _signature(
                    [],
                    {"kind": "iterable", "async": False, "errors": False, "element": ROW},
                )
            ],
        },
        {
            "name": "ticks",
            "tags": ["stream"],
            "tag_arguments": [],
            "description": "",
            "signatures": [
                _signature([], {"kind": "iterable", "async": False, "errors": True, "element": ROW})
            ],
        },
        {
            "name": "refund",
            "tags": ["command"],
            "tag_arguments": [],
            "description": "",
            "signatures": [
                _signature([_parameter("id"), _parameter("amount", "number")], ROW, fails=True)
            ],
        },
        {
            "name": "type",
            "tags": ["command"],
            "tag_arguments": [],
            "description": "",
            "signatures": [_signature([_parameter("id")], {"kind": "undefined"})],
        },
    ],
    "types": {
        "Row": {"name": "Row", "fields": [_parameter("id") | {"description": ""}]},
        "Inputs": {"name": "Inputs", "fields": [_parameter("region") | {"description": ""}]},
    },
}


# -- the generated calls --------------------------------------------------------------------------


def test_each_function_is_called_as_its_signature_was_read():
    source = build.exports_source(DOCUMENT, read_manifest(DOCUMENT, RUST_RULES))
    assert '("orders", |a: &Args| { rows(code::r#orders(inputs(a)?).map_err(failed)?) }),' in (
        source
    )
    assert '("all", |a: &Args| { rows(code::r#all()) }),' in source
    assert '("ticks", |a: &Args| { fallible_rows(code::r#ticks()) }),' in source
    assert (
        '("refund", |a: &Args| { expect(a, 2)?; '
        "value(code::r#refund(arg(a, 0)?, arg(a, 1)?).map_err(failed)?) }),"
    ) in source
    # A keyword is a raw identifier, and a function returning nothing answers nothing.
    assert (
        '("type", |a: &Args| { expect(a, 1)?; code::r#type(arg(a, 0)?); Ok(Answer::Nothing) }),'
        in (source)
    )


def test_the_build_is_the_hosts_crate_with_the_codes_crate_added(tmp_path):
    made = tomllib.loads(build.build_manifest(tmp_path / "orders", "orders-lib"))
    host = tomllib.loads((build.HOST / "Cargo.toml").read_text())
    assert made["dependencies"]["code"] == {
        "package": "orders-lib",
        "path": str(tmp_path / "orders"),
    }
    assert {k: v for k, v in made["dependencies"].items() if k != "code"} == host["dependencies"]
    assert made["workspace"] == {}
    assert made["bin"] == [{"name": "host", "path": "src/main.rs"}]


# -- the joined lock ------------------------------------------------------------------------------

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def _lock(*packages: tuple[str, str, list[str]], root: str = "root") -> str:
    lines = ["version = 4", "", "[[package]]", f'name = "{root}"', 'version = "0.1.0"']
    for name, version, dependencies in packages:
        lines += ["", "[[package]]", f'name = "{name}"', f'version = "{version}"']
        lines += [f'source = "{REGISTRY}"', f'checksum = "{name}-{version}"']
        if dependencies:
            lines.append("dependencies = [" + ", ".join(f'"{d}"' for d in dependencies) + "]")
    return "\n".join(lines) + "\n"


def test_a_crate_both_lock_in_one_range_is_the_codes_and_the_hosts_depend_on_it():
    host = _lock(
        ("serde", "1.0.229", []), ("serde_json", "1.0.154", ["serde"]), ("arrow", "56.2.1", [])
    )
    code = _lock(("serde", "1.0.228", []), ("syn", "1.0.109", []), ("syn", "2.0.111", []))
    joined = tomllib.loads(build.joined_lock(host, code))
    packages = {(p["name"], p["version"]): p for p in joined["package"]}
    assert set(packages) == {
        ("serde", "1.0.228"),
        ("serde_json", "1.0.154"),
        ("arrow", "56.2.1"),
        ("syn", "1.0.109"),
        ("syn", "2.0.111"),
    }
    # Each dependency names its version, which two versions of one crate need.
    assert packages[("serde_json", "1.0.154")]["dependencies"] == ["serde 1.0.228"]
    assert packages[("serde", "1.0.228")]["checksum"] == "serde-1.0.228"
    # The path packages are cargo's to write.
    assert "root" not in {p["name"] for p in joined["package"]}


def test_versions_in_different_ranges_are_both_kept():
    host = _lock(("base64", "0.22.1", []))
    code = _lock(("base64", "0.21.7", []))
    joined = tomllib.loads(build.joined_lock(host, code))
    assert sorted(p["version"] for p in joined["package"]) == ["0.21.7", "0.22.1"]


def test_code_with_no_lock_builds_on_the_hosts():
    joined = tomllib.loads(build.joined_lock(_lock(("libc", "0.2.191", [])), None))
    assert [p["name"] for p in joined["package"]] == ["libc"]


def test_a_workspace_members_lock_is_its_workspaces(tmp_path):
    (tmp_path / "Cargo.toml").write_text('[workspace]\nmembers = ["rows"]\n')
    (tmp_path / "rows").mkdir()
    (tmp_path / "rows" / "Cargo.toml").write_text('[package]\nname = "rows"\nversion = "0.1.0"\n')
    assert build.lock_of(tmp_path / "rows") == tmp_path / "Cargo.lock"
    (tmp_path / "rows" / "Cargo.lock").write_text("")
    assert build.lock_of(tmp_path / "rows") == tmp_path / "rows" / "Cargo.lock"


# -- the install ----------------------------------------------------------------------------------


def _crate(tmp_path: Path, manifest: str) -> Path:
    (tmp_path / "Cargo.toml").write_text('[package]\nname = "rows"\nversion = "0.1.0"\n' + manifest)
    return tmp_path


def test_a_crate_that_depends_on_others_carries_its_lock(tmp_path):
    crate = _crate(tmp_path, '\n[dependencies]\nserde = "1"\n')
    with pytest.raises(BundleRefused) as missing:
        check_lockfile(crate, "1.95.0")
    assert missing.value.code == "codesource.lockfile_missing"
    (crate / "Cargo.lock").write_text("version = 4\n")
    assert check_lockfile(crate, "1.95.0")["package"]["name"] == "rows"


def test_a_crate_of_no_dependencies_needs_no_lock(tmp_path):
    assert check_lockfile(_crate(tmp_path, ""), "1.95.0")["package"]["name"] == "rows"


def _registry(address: str, *, mirror: bool = True, ca: str | None = None) -> Resolved:
    return Resolved("r1", CARGO, address, None, "tok-123", mirror, ca)


def test_crates_io_is_used_when_the_organisation_names_no_registry(tmp_path):
    setup = registry_setup([], tmp_path)
    assert setup.args == () and setup.env == {}


def test_the_default_registry_stands_in_for_crates_io_with_its_token(tmp_path):
    setup = registry_setup([_registry("sparse+https://cargo.acme.test/index/")], tmp_path)
    (flag, path) = setup.args
    assert flag == "--config"
    config = tomllib.loads(Path(path).read_text())
    assert config == {
        "registries": {"provisa-mirror": {"index": "sparse+https://cargo.acme.test/index/"}},
        "source": {"crates-io": {"replace-with": "provisa-mirror"}},
    }
    assert setup.env == {"CARGO_REGISTRIES_PROVISA_MIRROR_TOKEN": "tok-123"}
    # The token is in cargo's environment, never in the config file.
    assert "tok-123" not in Path(path).read_text()


def test_a_registrys_certificate_authority_is_trusted(tmp_path):
    import certifi

    end = "-----END CERTIFICATE-----"
    ca = Path(certifi.where()).read_text().split(end, 1)[0].split("-----BEGIN", 1)[1]
    ca = f"-----BEGIN{ca}{end}\n"
    setup = registry_setup(
        [_registry("sparse+https://cargo.acme.test/", mirror=False, ca=ca)], tmp_path
    )
    assert setup.args == ()
    assert ca.strip() in Path(setup.env["CARGO_HTTP_CAINFO"]).read_text()


@pytest.mark.parametrize(
    ("dl", "crate", "url"),
    [
        (
            "https://static.crates.io/crates",
            "serde",
            "https://static.crates.io/crates/serde/1.2.3/download",
        ),
        ("https://c.test/{crate}/{version}.crate", "ab", "https://c.test/ab/1.2.3.crate"),
        ("https://c.test/{prefix}/{crate}", "abc", "https://c.test/3/a/abc"),
        ("https://c.test/{lowerprefix}/{crate}", "Serde", "https://c.test/se/rd/Serde"),
    ],
)
def test_a_crate_file_is_where_its_index_says(dl, crate, url):
    assert download_url(dl, crate, "1.2.3") == url


def test_a_crate_is_fetched_from_the_registrys_index_with_its_token(tmp_path):
    import io
    import tarfile

    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        data = b'[package]\nname = "rows"\nversion = "1.2.3"\n'
        info = tarfile.TarInfo("rows-1.2.3/Cargo.toml")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    asked: list[tuple[str, dict]] = []

    def http(url, headers):
        asked.append((url, headers))
        if url.endswith("config.json"):
            return json.dumps({"dl": "https://dl.acme.test/crates"}).encode()
        return archive.getvalue()

    root = fetch_crate(
        CrateCode("rows", "1.2.3"),
        [_registry("sparse+https://cargo.acme.test/index/")],
        http,
        tmp_path,
    )
    assert (root / "Cargo.toml").is_file()
    assert asked == [
        ("https://cargo.acme.test/index/config.json", {"Authorization": "tok-123"}),
        ("https://dl.acme.test/crates/rows/1.2.3/download", {"Authorization": "tok-123"}),
    ]


def test_a_crate_is_named_at_an_exact_version_from_a_sparse_index(tmp_path):
    with pytest.raises(BundleRefused) as inexact:
        fetch_crate(CrateCode("rows", "1"), [], lambda *_: b"", tmp_path)
    assert inexact.value.code == "codesource.crate_version"
    with pytest.raises(BundleRefused) as git_index:
        fetch_crate(
            CrateCode("rows", "1.2.3"),
            [_registry("https://git.acme.test/index")],
            lambda *_: b"",
            tmp_path,
        )
    assert git_index.value.code == "codesource.cargo_index"


# -- the toolchain --------------------------------------------------------------------------------


def test_a_host_is_built_static_for_linux_and_for_the_mac_itself():
    assert static_target("aarch64-apple-darwin") == "aarch64-apple-darwin"
    assert static_target("x86_64-unknown-linux-gnu") == "x86_64-unknown-linux-musl"
    assert static_target("aarch64-apple-darwin", "amd64") == "x86_64-unknown-linux-musl"
    assert static_target("aarch64-apple-darwin", "arm64") == "aarch64-unknown-linux-musl"
    assert host_triple("Linux", "aarch64") == "aarch64-unknown-linux-gnu"
    with pytest.raises(BundleRefused, match="no Rust toolchain is pinned for Windows/AMD64"):
        host_triple("Windows", "AMD64")
    musl = build._linking("x86_64-unknown-linux-musl")
    assert musl["CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER"] == "rust-lld"
    assert "link-self-contained=yes" in musl["CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS"]
    assert build._linking("aarch64-apple-darwin") == {}


def test_every_component_of_the_toolchain_is_pinned_at_one_version():
    versions = {
        RUNTIMES[n].version for n in RUNTIMES if n.startswith(("rustc", "cargo", "rust-std"))
    }
    assert versions == {RUNTIMES["rustc"].version}
    for name in ("rust-std-x86_64-unknown-linux-musl", "rust-std-aarch64-unknown-linux-musl"):
        assets = {a.filename for a in RUNTIMES[name].assets.values()}
        assert len(assets) == 1  # one file, whatever platform the build runs on


def test_a_cargo_command_inherits_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("CARGO_HOME", "/elsewhere")
    monkeypatch.setenv("RUSTFLAGS", "-C opt-level=0")
    toolchain = Toolchain(
        cargo=tmp_path / "cargo" / "bin" / "cargo",
        rustc=tmp_path / "sysroot" / "bin" / "rustc",
        sysroot=tmp_path / "sysroot",
        host="x86_64-unknown-linux-gnu",
        version="1.95.0",
    )
    env = cargo_environment(toolchain, cargo_home=tmp_path / "home", target_dir=tmp_path / "t")
    assert env["CARGO_HOME"] == str(tmp_path / "home")
    assert env["RUSTC"] == str(tmp_path / "sysroot" / "bin" / "rustc")
    assert "RUSTFLAGS" not in env
    assert env["PATH"].split(":")[:2] == [
        str(tmp_path / "cargo" / "bin"),
        str(tmp_path / "sysroot" / "bin"),
    ]


def test_a_cargo_registry_is_a_kind_of_package_registry():
    assert CARGO in KINDS
