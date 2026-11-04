# Copyright (c) 2026 Kenneth Stott
# Canary: e8a3c6d1-4f27-4b09-9d5e-0b6f2a8c7e13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source's host (REQ-2200): compiled by the pinned Go toolchain with the code's package
linked in, and serving every code source's protocol. The host runs here unsandboxed; the
sandboxed runs are the integration tests' (tests/integration/test_go_worker.py)."""

# Requirements: REQ-2200
from __future__ import annotations

import shutil
import textwrap
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import Manifest
from provisa.core import request_deadline
from provisa.golang import source
from provisa.golang.build import (
    HOST,
    build_host,
    build_module,
    directives,
    exports_source,
    import_path,
    requirements,
)
from provisa.golang.reader import ensure_reader, read_code
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "go_source"
PROXY = "https://proxy.golang.org"
EMPTY = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}


@pytest.fixture(scope="module")
def toolchain(tmp_path_factory) -> tuple[Path, Path, Path]:
    """The pinned go, the compiled reader, and a module cache the builds share."""
    go = CodeRuntimeResolver().resolve("go")
    cache = tmp_path_factory.mktemp("cache")
    reader = ensure_reader(go, RUNTIMES["go"].version, cache, PROXY)
    return go, reader, cache / "gomod"


def _manifest(toolchain, code: Path, scratch: Path) -> Manifest:
    go, reader, gomodcache = toolchain
    return read_code(
        reader=reader, go=go, code=code, package=".", gomodcache=gomodcache, scratch=scratch
    )


def _build(toolchain, code: Path, manifest: Manifest, dest: Path) -> Path:
    go, _reader, gomodcache = toolchain
    return build_host(
        go=go,
        code=code,
        package=".",
        exports=[e.name for e in manifest.exports],
        gomodcache=gomodcache,
        proxy=PROXY,
        dest=dest,
    )


def _start(binary: Path) -> w.Worker:
    command = w.HostCommand(
        argv=lambda address: [str(binary), address],
        env=w.private_environment(),
        cwd=binary.parent,
    )
    started = w.Worker("go-src", "h", command)
    started.start()
    return started


@pytest.fixture(scope="module")
def fixture_manifest(toolchain, tmp_path_factory) -> Manifest:
    return _manifest(toolchain, FIXTURE, tmp_path_factory.mktemp("read"))


@pytest.fixture(scope="module")
def binary(toolchain, fixture_manifest, tmp_path_factory) -> Path:
    return _build(toolchain, FIXTURE, fixture_manifest, tmp_path_factory.mktemp("bin") / "host")


@pytest.fixture
def worker(binary):
    started = _start(binary)
    yield started
    started.stop()


def _read_of(manifest: Manifest, export: str) -> dict:
    """A @table or @stream call's schema, what it takes and an empty read context."""
    found = manifest.export(export)
    return {
        "schema": schema_text(row_schema(manifest, found)),
        "takes": found.takes,
        "context": EMPTY,
    }


# -- the protocol ---------------------------------------------------------------------------------


def test_the_host_says_which_functions_it_has(worker):
    assert sorted(worker.exports) == [
        "Boom", "CancelOrder", "CleanAddress", "Direct", "Fetched", "Forever", "Greet", "Many",
        "Orders", "Outside", "Placed", "Ticks",
    ]  # fmt: skip


def test_rows_and_their_values_come_back_in_their_own_types(worker, fixture_manifest):
    since = datetime(2025, 12, 1, tzinfo=UTC)
    [[row]] = list(
        worker.rows(
            "Orders",
            "table",
            ["acme", since],
            ["customer_id", "since"],
            **_read_of(fixture_manifest, "Orders"),
        )
    )
    assert row == {
        "id": 2**60,
        "placed": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        "tier": "gold",
        "lines": [{"sku": "acme", "qty": 2}],
        "note": "2025-12-01T00:00:00Z",
    }


def test_a_large_table_comes_in_batches(worker, fixture_manifest):
    batches = worker.rows("Many", "table", [2500], ["count"], **_read_of(fixture_manifest, "Many"))
    assert [len(b) for b in batches] == [1000, 1000, 500]


def test_a_commands_value_and_the_codes_own_error(worker, fixture_manifest):
    assert worker.command("CancelOrder", [7]) == {"cancelled": 7}
    with pytest.raises(w.CodeFailed, match="no such customer"):
        list(worker.rows("Boom", "table", [], **_read_of(fixture_manifest, "Boom")))
    assert worker.ping()


@pytest.mark.parametrize("export", ["Ticks", "Placed"])  # no context, and one that takes it
def test_a_stream_its_subscriber_leaves_is_stopped_and_the_worker_serves_on(
    worker, fixture_manifest, export
):
    given = (["eu"], ["region"]) if export == "Placed" else ([], [])
    rows = worker.rows(export, "stream", *given, **_read_of(fixture_manifest, export))
    first, second = next(rows), next(rows)
    assert len(first) == len(second) == 1
    rows.close()
    assert worker.ping()


def test_a_deadline_cancels_a_call_that_never_answers(worker, fixture_manifest):
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        with request_deadline.within(0.3):
            list(worker.rows("Forever", "stream", [], **_read_of(fixture_manifest, "Forever")))
    assert time.monotonic() - started < 10
    assert worker.ping()


def test_a_tables_argument_sets_are_served_in_one_call_in_order(worker, fixture_manifest):
    """REQ-2099: the host runs the function once per argument set of the one call, in order."""
    calls = [(["ada", w.ABSENT, p], ["name", "greeting", "punctuation"]) for p in ("!", "?")]
    rows = [
        r
        for b in worker.rows_each("Greet", calls, **_read_of(fixture_manifest, "Greet"))
        for r in b
    ]
    assert rows == [{"text": "hello ada!"}, {"text": "hello ada?"}]


def test_an_input_left_out_is_nil(worker, fixture_manifest):
    names = ["name", "greeting", "punctuation"]
    read = _read_of(fixture_manifest, "Greet")
    assert list(worker.rows("Greet", "table", ["ada", "hi", w.ABSENT], names, **read)) == [
        [{"text": "hi ada!"}]
    ]


def test_a_columns_call_answers_one_result_a_row_in_order(worker, fixture_manifest):
    transform = fixture_manifest.export("CleanAddress")
    schema = schema_text(row_schema(fixture_manifest, transform))
    assert worker.command(
        "CleanAddress", [["1 main st, springfield, us", "2 elm st, shelbyville, ca"]], schema=schema
    ) == [
        {"street": "1 main st", "city": "Springfield", "country": "US"},
        {"street": "2 elm st", "city": "Shelbyville", "country": "CA"},
    ]


def test_a_panic_a_yielded_error_and_a_value_with_no_form_fail_the_call_by_name(
    toolchain, tmp_path
):
    code = tmp_path / "code"
    code.mkdir()
    (code / "go.mod").write_text("module example.com/rows\n\ngo 1.23\n")
    (code / "rows.go").write_text(
        textwrap.dedent(
            """
            package rows

            import (
                "errors"
                "iter"
            )

            type Row struct {
                ID int64 `json:"id"`
            }

            // @table
            func Panics() []Row { panic("the code gave up") }

            // @stream
            func Fails() iter.Seq2[Row, error] {
                return func(yield func(Row, error) bool) {
                    if yield(Row{1}, nil) {
                        yield(Row{}, errors.New("the feed broke"))
                    }
                }
            }

            // @command
            func Channel() any { return make(chan int) }

            // @table
            func Big() []map[string]any { return []map[string]any{{"n": uint64(1 << 63)}} }
            """
        )
    )
    manifest = _manifest(toolchain, code, tmp_path)
    started = _start(_build(toolchain, code, manifest, tmp_path / "bin" / "host"))
    try:
        with pytest.raises(w.CodeFailed, match="panic: the code gave up") as panicked:
            list(started.rows("Panics", "table", [], **_read_of(manifest, "Panics")))
        assert "rows.Panics" in panicked.value.stack
        stream = started.rows("Fails", "stream", [], **_read_of(manifest, "Fails"))
        assert next(stream) == [{"id": 1}]
        with pytest.raises(w.CodeFailed, match="the feed broke"):
            next(stream)
        with pytest.raises(w.CodeFailed, match="a chan int has no form a row or a value can take"):
            started.command("Channel", [])
        # A loose row is sent whole, as the wire's JSON: an integer beyond its range by its digits.
        assert list(started.rows("Big", "table", [], **_read_of(manifest, "Big"))) == [
            [{"n": 2**63}]
        ]
        assert started.ping()
    finally:
        started.stop()


# -- the build ------------------------------------------------------------------------------------


def test_the_build_joins_the_hosts_module_and_the_codes(tmp_path, fixture_manifest):
    code = tmp_path / "code"
    shutil.copytree(FIXTURE, code)
    (code / "go.mod").write_text(
        "module example.com/orders\n\ngo 1.27.1\n\n"
        "require (\n\tgolang.org/x/sys v0.99.0 // indirect\n\texample.com/dep v1.0.0\n)\n\n"
        "replace example.com/dep => ./vendored/dep\n"
    )
    (code / "go.sum").write_text("example.com/dep v1.0.0 h1:abc=\n")
    into = tmp_path / "build"
    into.mkdir()
    build_module(code, ".", [e.name for e in fixture_manifest.exports], into)
    module = (into / "go.mod").read_text()
    host_requires = requirements((HOST / "go.mod").read_text())
    assert module.startswith("module provisa/codehost\n\ngo 1.27.1\n")  # the newer go line
    required = dict(w for w in directives(module, "require"))
    # Every module either requires, the code's module itself replaced by its directory.
    assert required == {
        **host_requires,
        "golang.org/x/sys": "v0.99.0",  # the higher of the host's and the code's
        "example.com/dep": "v1.0.0",
        "example.com/orders": "v0.0.0-00010101000000-000000000000",
    }
    assert [" ".join(w) for w in directives(module, "replace")] == [
        f"example.com/orders => {code}",
        f"example.com/dep => {(code / 'vendored' / 'dep').resolve()}",
    ]
    sums = (into / "go.sum").read_text().splitlines()
    assert "example.com/dep v1.0.0 h1:abc=" in sums
    assert set((HOST / "go.sum").read_text().splitlines()) - {""} <= set(sums)
    assert sorted(p.name for p in into.iterdir()) == [
        "arrow.go", "call.go", "exports.go", "go.mod", "go.sum", "host.go", "values.go",
    ]  # fmt: skip


def test_the_higher_of_two_required_versions_is_kept_as_semver_orders_them():
    assert requirements(
        "require (\n\ta v1.2.0\n\tb v0.0.0-20260101000000-abcdef123456\n\tc v2.0.0-rc.2\n)\n",
        "require a v1.10.0\nrequire b v0.1.0\nrequire c v2.0.0-rc.10 // indirect\n",
    ) == {"a": "v1.10.0", "b": "v0.1.0", "c": "v2.0.0-rc.10"}


def test_the_generated_exports_register_each_tagged_function(fixture_manifest):
    text = exports_source("example.com/orders", [e.name for e in fixture_manifest.exports])
    assert 'import code "example.com/orders"' in text
    assert [line.strip() for line in text.splitlines() if line.strip().startswith('"')] == [
        f'"{e.name}": code.{e.name},' for e in fixture_manifest.exports
    ]


def test_a_package_is_a_directory_of_the_module(tmp_path):
    (tmp_path / "go.mod").write_text("module example.com/rows\n\ngo 1.23\n")
    (tmp_path / "feeds").mkdir()
    assert import_path(tmp_path, ".") == "example.com/rows"
    assert import_path(tmp_path, "feeds") == "example.com/rows/feeds"
    for outside in ("../elsewhere", "/etc", "missing"):
        with pytest.raises(BundleRefused) as refused:
            import_path(tmp_path, outside)
        assert refused.value.code == "codesource.no_package"


def test_a_build_the_go_command_refuses_is_refused_with_its_words(tmp_path, fixture_manifest):
    code = tmp_path / "code"
    shutil.copytree(FIXTURE, code)

    def failing(argv, env, cwd):
        assert argv[1] == "build" and env["GOFLAGS"] == "-mod=readonly -modcacherw"
        assert env["GOTOOLCHAIN"] == "local" and env["CGO_ENABLED"] == "0"
        raise BundleRefused("go_failed", "go build failed: missing go.sum entry for module x")

    with pytest.raises(BundleRefused, match="missing go.sum entry"):
        build_host(
            go=tmp_path / "go",
            code=code,
            package=".",
            exports=[e.name for e in fixture_manifest.exports],
            gomodcache=tmp_path / "mod",
            proxy="off",
            dest=tmp_path / "bin" / "host",
            run=failing,
        )
    assert not (tmp_path / "bin" / "host").exists()


def test_on_macos_the_host_verifies_tls_with_the_systems_certificate_bundle(tmp_path, monkeypatch):
    seen = {}

    def sandboxed(argv, **kwargs):
        seen.update(kwargs, argv=argv("/sock"))
        return "command"

    monkeypatch.setattr(source, "sandboxed", sandboxed)
    for system, expected in (
        ("Darwin", {
            "PROVISA_CERTIFICATES": "/private/etc/ssl/cert.pem",
            "GODEBUG": "x509usefallbackroots=1",
        }),
        ("Linux", {}),
    ):  # fmt: skip
        monkeypatch.setattr(source.platform, "system", lambda system=system: system)
        assert source.host_command(binary=tmp_path / "host", egress=["example.com"]) == "command"
        assert seen["argv"] == [str(tmp_path / "host"), "/sock"]
        assert seen["executable"] == tmp_path / "host" and seen["read"] == []
        assert seen["egress"] == ["example.com"]
        assert {k: v for k, v in seen["env"].items() if k != "PATH"} == expected
