# Copyright (c) 2026 Kenneth Stott
# Canary: 57930cbf-5056-41b2-93c2-92eb21dfd534
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Rust SDK as a package (REQ-1984): a crate with no dependencies and no proc-macro, whose
spelling is the one the reader reads; and the names a code's Cargo.toml gives it, which the reader
is told."""

# Requirements: REQ-1984
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from provisa.rustlang.reader import READER, sdk_crates

pytestmark = pytest.mark.unit

SDK = Path(__file__).resolve().parents[2] / "provisa-sdk" / "rust"


def test_the_sdk_is_a_crate_with_no_dependencies_and_no_proc_macro():
    manifest = tomllib.loads((SDK / "Cargo.toml").read_text())
    assert manifest["package"]["name"] == "provisa-sdk"
    assert manifest["lib"]["name"] == "provisa_sdk"
    assert manifest["dependencies"] == {}
    assert "proc-macro" not in manifest["lib"]
    assert not any(k.endswith("dependencies") for k in manifest if k != "dependencies")


def test_the_sdks_spelling_is_the_one_the_reader_reads():
    sdk = re.search(r"pub const SPELLING: u32 = (\d+);", (SDK / "src" / "lib.rs").read_text())
    reader = re.search(
        r"const SDK_SPELLING: u32 = (\d+);", (READER / "src" / "main.rs").read_text()
    )
    assert sdk is not None and reader is not None
    assert sdk.group(1) == reader.group(1)


@pytest.mark.parametrize(
    ("toml", "names"),
    [
        ('[dependencies]\nprovisa-sdk = "0.1"\n', ["provisa_sdk"]),
        ('[dependencies]\nprovisa-sdk = { path = "../sdk" }\n', ["provisa_sdk"]),
        ('[dependencies]\np = { package = "provisa-sdk", version = "0.1" }\n', ["p"]),
        (
            "[target.'cfg(unix)'.dependencies]\nmy-sdk = { package = \"provisa-sdk\" }\n",
            ["my_sdk"],
        ),
        ('[dependencies]\nprovisa-sdk = { package = "other" }\nserde = "1"\n', []),
        ('[dev-dependencies]\nprovisa-sdk = "0.1"\n', []),
        ('[package]\nname = "x"\n', []),
    ],
)
def test_the_names_cargo_toml_gives_the_sdk(toml, names):
    assert sdk_crates(tomllib.loads(toml)) == names
