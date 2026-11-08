# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8b1e93-2f07-4d6a-b5e8-7a3d0c9f2b61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source's install (REQ-2200) against the real Go module proxy: the pinned Go toolchain is
downloaded, Provisa's reader and host are compiled with their modules fetched from
proxy.golang.org, and a dependent's modules are fetched and checked against its go.sum. These
reach the public internet, so they are integration tests; the install's logic that needs none is
in tests/unit/test_go_install.py."""

# Requirements: REQ-2200
from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import DirectoryCode, Lockfile
from provisa.golang.install import MODULES, PUBLIC_PROXY, ensure_installed, host_binary
from provisa.golang.reader import ensure_reader
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent.parent / "fixtures" / "go_source"
UUID_SUM = (
    "github.com/google/uuid v1.6.0 h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=\n"
    "github.com/google/uuid v1.6.0/go.mod h1:TIyPZe4MgqvfeYDBFedMoGGpEw/LqOeaOT+nhxU+yHo=\n"
)


# -- installs, with the pinned Go and the public proxy ---------------------------------------------


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> tuple[Path, Path, Path]:
    cache = tmp_path_factory.mktemp("cache")
    go = CodeRuntimeResolver(cache_root=cache).resolve("go")
    return go, ensure_reader(go, RUNTIMES["go"].version, cache, PUBLIC_PROXY), cache


def _dependent(tmp_path: Path) -> Path:
    code = tmp_path / "rows"
    code.mkdir()
    (code / "go.mod").write_text(
        "module example.com/rows\n\ngo 1.23\n\nrequire github.com/google/uuid v1.6.0\n"
    )
    (code / "rows.go").write_text(
        textwrap.dedent(
            """
            package rows

            import "github.com/google/uuid"

            type Row struct {
                ID string `json:"id"`
            }

            // @table
            func Rows() []Row {
                return []Row{{uuid.NewSHA1(uuid.NameSpaceURL, []byte("x")).String()}}
            }
            """
        )
    )
    return code


def _install(toolchain, code, tmp_path, **given):
    go, reader, cache = toolchain
    return ensure_installed(
        source_id="rows-src",
        code=DirectoryCode(str(code)),
        package=given.pop("package", "."),
        expected_hash=given.pop("expected_hash", None),
        registries=[],
        go=go,
        reader_binary=reader,
        cache_root=tmp_path / "cache",
        **given,
    )


def test_the_code_its_modules_and_its_host_are_installed_and_cached(toolchain, tmp_path):
    installed = _install(toolchain, FIXTURE, tmp_path)
    assert host_binary(installed.path).is_file()
    assert installed.path == tmp_path / "cache" / "code-bundles" / "go" / installed.content_hash
    again = _install(toolchain, FIXTURE, tmp_path, expected_hash=installed.content_hash)
    assert again == installed
    with pytest.raises(BundleRefused) as changed:
        _install(toolchain, FIXTURE, tmp_path, expected_hash="0" * 64)
    assert changed.value.code == "codesource.content_changed"


def test_a_dependency_is_fetched_through_the_proxy_and_checked_against_go_sum(toolchain, tmp_path):
    code = _dependent(tmp_path)
    with pytest.raises(BundleRefused) as no_sum:
        _install(toolchain, code, tmp_path)
    assert no_sum.value.code == "codesource.lockfile_missing"
    # The go.sum given beside the code on the source's form.
    installed = _install(toolchain, code, tmp_path, lockfile=Lockfile("go.sum", UUID_SUM.encode()))
    assert (installed.path / MODULES / "github.com" / "google" / "uuid@v1.6.0").is_dir()
    assert host_binary(installed.path).is_file()


def test_a_module_go_sum_does_not_cover_is_refused_by_the_go_command(toolchain, tmp_path):
    code = _dependent(tmp_path)
    (code / "go.sum").write_text(UUID_SUM.replace("NIvaJDMOsjHA8n1", "AAAAAAAAAAAAAAA"))
    with pytest.raises(BundleRefused) as refused:
        _install(toolchain, code, tmp_path)
    assert refused.value.code == "codesource.go_failed"
    assert "github.com/google/uuid" in str(refused.value)
    assert "checksum mismatch" in str(refused.value)


def test_a_package_of_the_module_is_its_entry(toolchain, tmp_path):
    code = tmp_path / "code"
    shutil.copytree(FIXTURE, code / "feeds")
    (code / "feeds" / "go.mod").unlink()
    (code / "go.mod").write_text("module example.com/orders\n\ngo 1.23\n")
    installed = _install(toolchain, code, tmp_path, package="feeds")
    assert host_binary(installed.path).is_file()


@pytest.mark.parametrize(("goarch", "machine"), [("amd64", 62), ("arm64", 183)])
def test_an_images_host_is_one_static_linux_binary_of_its_architecture(
    toolchain, tmp_path, goarch, machine
):
    """REQ-2098: for an image, the host is compiled for linux and the image's architecture, with no
    cgo: an ELF of that machine with no interpreter to load it, which runs on the distroless
    base with nothing beside it."""
    import struct

    from provisa.golang.build import build_host
    from provisa.golang.reader import read_code

    go, reader, cache = toolchain
    gomodcache = cache / "gomod"
    manifest = read_code(
        reader=reader, go=go, code=FIXTURE, package=".", gomodcache=gomodcache, scratch=tmp_path
    )
    binary = build_host(
        go=go,
        code=FIXTURE,
        package=".",
        exports=[e.name for e in manifest.exports],
        gomodcache=gomodcache,
        proxy=PUBLIC_PROXY,
        dest=tmp_path / goarch / "host",
        extra_env={"GOOS": "linux", "GOARCH": goarch},
    )
    elf = binary.read_bytes()
    assert elf[:4] == b"\x7fELF" and elf[4] == 2  # 64-bit
    assert struct.unpack_from("<H", elf, 18)[0] == machine
    (phoff,) = struct.unpack_from("<Q", elf, 32)
    entsize, count = struct.unpack_from("<HH", elf, 54)
    kinds = [struct.unpack_from("<I", elf, phoff + i * entsize)[0] for i in range(count)]
    assert 3 not in kinds  # PT_INTERP: no dynamic loader, a static binary
