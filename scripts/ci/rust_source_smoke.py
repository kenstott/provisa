# Copyright (c) 2026 Kenneth Stott
# Canary: 6dd05491-d2b1-41af-99da-59ff0e877db1
#
# This source code is licensed under the Business Source License 1.1
"""Inside the built image: a Rust source is prepared as the server prepares one (REQ-2227). The
pinned toolchain is fetched, the reader and the source's host are compiled, and the crate's
functions are read. The crate is tests/fixtures/rust_source, mounted read-only at the path given
as the first argument. It derives serde's traits, so its procedural macro and serde's build script
are compiled and linked by the image's C compiler. Run as
`docker run -i -v <fixture>:/fixture:ro <image> python - /fixture < this`."""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

from provisa.codesource.install import DirectoryCode
from provisa.codesource.languages import LANGUAGES
from provisa.codesource.source import CodeConfig, Context
from provisa.rustlang.install import host_binary

if shutil.which("cc") is None:
    raise SystemExit("the image has no C compiler (cc): a Rust source cannot be prepared")

fixture = Path(sys.argv[1])
cache = Path(tempfile.mkdtemp())
config = CodeConfig("rust", "rust", {"kind": "directory", "path": str(fixture)}, None, None, None)
ctx = Context(cache, [], [], cast(Any, None), "org")
prepared = LANGUAGES["rust"].prepare()("smoke-rust", config, DirectoryCode(str(fixture)), None, ctx)

names = sorted(e.name for e in prepared.manifest.exports)
assert {"orders", "refund"} <= set(names), names
binary = host_binary(cache / "code-bundles" / "rust" / prepared.content_hash)
head = binary.read_bytes()[:4]
assert head == b"\x7fELF", (binary, head)
print(f"rust source prepared in the image: host {binary.stat().st_size} bytes, exports {names}")
