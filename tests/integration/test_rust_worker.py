# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2f6b41-9c37-4e05-a1d8-5b3e7c9f0a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Rust source joined and served inside the worker's sandbox (REQ-2227): the language's prepare
reads the crate, compiles its host once with the code linked in (the pinned toolchain, its crates
from crates.io checked against the Cargo.lock), and the host serves a table's rows as Arrow and a
command's value, sandboxed. A second source of the same code reuses the compiled host: its code
hash is the same, so nothing is compiled again. On Linux the host is one static binary, as it is
in a source's image."""

# Requirements: REQ-2227
from __future__ import annotations

import platform
import struct
from pathlib import Path
from typing import Any, cast

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.install import DirectoryCode
from provisa.codesource.languages import LANGUAGES
from provisa.codesource.source import CodeConfig, Context, Prepared
from provisa.rustlang import install
from provisa.rustlang.install import host_binary

pytestmark = [pytest.mark.integration]

FIXTURE = Path(__file__).parent.parent / "fixtures" / "rust_source"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}


def _join(source_id: str, cache: Path) -> Prepared:
    """The fixture joined as a source: the Rust language's own prepare, as a source's save and
    the connector build run it."""
    config = CodeConfig(
        "rust", "rust", {"kind": "directory", "path": str(FIXTURE)}, None, None, None
    )
    ctx = Context(cache, [], [], cast(Any, None), "org")
    return LANGUAGES["rust"].prepare()(source_id, config, DirectoryCode(str(FIXTURE)), None, ctx)


@pytest.fixture(scope="module")
def cache(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("cache")


@pytest.fixture(scope="module")
def joined(cache) -> Prepared:
    return _join("orders-src", cache)


def _rows(started: w.Worker, prepared: Prepared, export: str, args: list) -> list[list[dict]]:
    table = prepared.manifest.export(export)
    names = [p.name for p in table.parameters]
    given = [*args, *([w.ABSENT] * (len(names) - len(args)))]
    schema = schema_text(row_schema(prepared.manifest, table))
    return list(
        started.rows(export, "table", given, names, schema=schema, takes=table.takes, context=EMPTY)
    )


def test_a_table_is_read_and_a_command_run_inside_the_sandbox(joined):
    started = joined.worker()
    started.start()
    try:
        [[order]] = _rows(started, joined, "orders", ["acme", 2])
        assert order == {
            "id": "acme-1",
            "status": "open",
            "total": 12.5,
            "note": None,
            "lines": [{"sku": "acme", "qty": 2}, {"sku": "acme", "qty": 3}],
        }
        # An iterator's rows go in batches of 1000.
        assert [len(b) for b in _rows(started, joined, "many", [1500])] == [1000, 500]
        assert started.command("refund", ["o-1", 3.5], ["order_id", "amount"]) == {
            "orderId": "o-1",
            "amount": 3.5,
        }
        # An Option parameter left out is None.
        assert started.command("price", ["abc", w.ABSENT], ["sku", "quantity"]) == 3.0
        # The code's own error fails the call with its words.
        with pytest.raises(w.CodeFailed, match="a refund of -1 is not one"):
            started.command("refund", ["o-1", -1.0], ["order_id", "amount"])
    finally:
        started.stop()


def test_the_code_reads_nothing_outside_it(joined):
    started = joined.worker()
    started.start()
    try:
        with pytest.raises(w.CodeFailed, match="(?i)permission denied|operation not permitted"):
            _rows(started, joined, "outside", [])
    finally:
        started.stop()


def test_a_second_join_of_the_same_code_reuses_its_compiled_host(joined, cache, monkeypatch):
    binary = host_binary(cache / "code-bundles" / "rust" / joined.content_hash)
    before = binary.stat()

    def compile_again(*_: object, **__: object) -> None:
        raise AssertionError("the host was compiled again for code it was compiled for")

    monkeypatch.setattr(install, "install_host", compile_again)
    again = _join("orders-copy", cache)
    assert again.content_hash == joined.content_hash
    after = binary.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    started = again.worker()
    started.start()
    try:
        [[order]] = _rows(started, again, "orders", ["beta"])
        assert order["id"] == "beta-1"
    finally:
        started.stop()


def _elf_is_static(binary: bytes) -> bool:
    """A 64-bit ELF with no interpreter (PT_INTERP) to load it."""
    assert binary[:4] == b"\x7fELF" and binary[4] == 2
    (phoff,) = struct.unpack_from("<Q", binary, 32)
    entsize, count = struct.unpack_from("<HH", binary, 54)
    kinds = [struct.unpack_from("<I", binary, phoff + i * entsize)[0] for i in range(count)]
    return 3 not in kinds


def _macho_libraries(binary: bytes) -> list[str]:
    """The libraries a 64-bit Mach-O loads (its LC_LOAD_DYLIB commands)."""
    assert struct.unpack_from("<I", binary, 0)[0] == 0xFEEDFACF
    (count,) = struct.unpack_from("<I", binary, 16)
    at, out = 32, []
    for _ in range(count):
        command, size = struct.unpack_from("<II", binary, at)
        if command == 0xC:
            (offset,) = struct.unpack_from("<I", binary, at + 8)
            out.append(binary[at + offset : at + size].split(b"\0", 1)[0].decode())
        at += size
    return out


def test_the_host_needs_nothing_beside_it(joined, cache):
    """On Linux the host is one static binary (as in a source's image); a Mac has no static
    binaries, and the host loads the system's libraries alone, as a Go host does."""
    binary = host_binary(cache / "code-bundles" / "rust" / joined.content_hash).read_bytes()
    if platform.system() == "Linux":
        assert _elf_is_static(binary)
    else:
        libraries = _macho_libraries(binary)
        assert libraries and all(lib.startswith(("/usr/lib/", "/System/")) for lib in libraries)
