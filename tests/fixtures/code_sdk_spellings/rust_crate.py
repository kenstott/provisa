# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust author's crate that depends on Provisa's Rust SDK (REQ-1984), checked and read the way
Provisa reads a Rust source: the pinned toolchain, Provisa's own reader, the SDK the repository's
copy (``provisa-sdk/rust``)."""

# Requirements: REQ-1984
from __future__ import annotations

import subprocess  # nosec B404 - argv-list invocation of the pinned cargo
from dataclasses import dataclass
from pathlib import Path

from provisa.codesource.manifest import ManifestRefused, manifest_json
from provisa.runtime_deps.pgwire_bundles import default_cache_root
from provisa.rustlang.reader import cargo_environment, ensure_reader, read_code
from provisa.rustlang.toolchain import Toolchain, ensure_toolchain

REPO = Path(__file__).resolve().parents[3]
SDK = REPO / "provisa-sdk" / "rust"
#: The serde a crate using it is given: the one the Rust source fixture pins.
SERDE = '{ version = "=1.0.228", features = ["derive"] }'


def cargo_toml(code: str, dependency: str = f'provisa-sdk = {{ path = "{SDK}" }}') -> str:
    """A crate whose library is ``lib.rs``, depending on the SDK as ``dependency`` writes it, and
    on serde when ``code`` uses it. An empty ``[workspace]`` keeps it out of any workspace around
    it."""
    serde = f"serde = {SERDE}\n" if "serde" in code else ""
    return (
        '[package]\nname = "source"\nversion = "0.1.0"\nedition = "2021"\n\n'
        '[lib]\npath = "lib.rs"\n\n'
        f"[dependencies]\n{dependency}\n{serde}\n[workspace]\n"
    )


def write_crate(directory: Path, code: str, **toml: str) -> Path:
    """``directory`` holding ``code`` as ``lib.rs`` and its ``Cargo.toml``."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "lib.rs").write_text(code)
    (directory / "Cargo.toml").write_text(cargo_toml(code, **toml))
    return directory


@dataclass(frozen=True)
class RustToolchain:
    toolchain: Toolchain
    reader: Path
    cache: Path

    @classmethod
    def resolve(cls, cache: Path) -> RustToolchain:
        """The pinned toolchain, shared with every Rust test through the runtime cache; the
        reader, cargo's home and its target directory under ``cache``."""
        toolchain = ensure_toolchain(default_cache_root(), [])
        return cls(toolchain, ensure_reader(toolchain, cache), cache)

    def check(self, crate: Path) -> subprocess.CompletedProcess[str]:
        """``cargo check`` of the crate, as an author's editor or CI would run it."""
        env = cargo_environment(
            self.toolchain,
            cargo_home=self.cache / "code" / "cargo-home",
            target_dir=self.cache / "target",
        )
        return subprocess.run(  # nosec B603 - the pinned cargo, no shell
            [str(self.toolchain.cargo), "check", "--quiet"],
            capture_output=True,
            text=True,
            env=env,
            cwd=crate,
        )

    def read(self, crate: Path) -> dict | tuple[str, ...]:
        """The crate's manifest as JSON, or the manifest's problems when it is refused."""
        try:
            manifest = read_code(
                reader=self.reader, code=crate, entry=".", toolchain=self.toolchain.version
            )
        except ManifestRefused as refused:
            return tuple(refused.problems)
        return manifest_json(manifest)
