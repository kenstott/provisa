# Copyright (c) 2026 Kenneth Stott
# Canary: 024b6612-d674-4583-a433-9fa3691cd1f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source read and served inside the worker's sandbox (REQ-1993): the reader imports the
code under the sandbox and its tables, commands and streams are the shared manifest's; the host
serves rows sandboxed; the code reads nothing outside it and reaches the network only through the
egress proxy, to the hosts on the allow-list.

The interpreter is the pinned Python, fetched as a source's is; its dependencies the pinned uv's."""

# Requirements: REQ-1993
from __future__ import annotations

import shutil
import socket
import threading
from pathlib import Path

import pytest

from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.manifest import Named, Scalar
from provisa.python.source import host_command, read_code

pytestmark = [pytest.mark.integration]

FIXTURE = Path(__file__).parent.parent / "fixtures" / "python_source"


@pytest.fixture(scope="module")
def runtimes(tmp_path_factory):
    """The pinned Python (its interpreter and its prefix) and the pinned uv."""
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

    resolver = CodeRuntimeResolver(cache_root=tmp_path_factory.mktemp("runtime-deps"))
    python = resolver.resolve("python")
    host_lib = resolver.resolve("pyarrow").parent.parent  # the host's own pyarrow (REQ-2098)
    return python, python.parent.parent, resolver.resolve("uv"), host_lib


@pytest.fixture
def code(tmp_path):
    copied = tmp_path / "code"
    shutil.copytree(FIXTURE, copied)
    return copied


def _start(code: Path, runtimes, egress=()) -> w.Worker:
    python, python_home, _uv, host_lib = runtimes
    command = host_command(
        python=python,
        python_home=python_home,
        code=code,
        entry="orders",
        egress=list(egress),
        host_lib=host_lib,
    )
    started = w.Worker("python-src", "h", command)
    started.start()
    return started


def _rows(started: w.Worker, code: Path, runtimes, export: str, args: list) -> list[list[dict]]:
    """The export's rows, batch by batch, sent against its row type's schema (REQ-2098)."""
    python, python_home, _uv, _lib = runtimes
    manifest = read_code(python=python, python_home=python_home, code=code, entry="orders")
    table = manifest.export(export)
    schema = schema_text(row_schema(manifest, table))
    # REQ-2100: its inputs object's fields, those not given absent; and an empty read context.
    names = [p.name for p in table.parameters]
    given = [*args, *([w.ABSENT] * (len(names) - len(args)))]
    empty = {"filters": [], "order_by": [], "limit": None, "columns": [], "keys": None}
    return list(
        started.rows(export, "table", given, names, schema=schema, takes=table.takes, context=empty)
    )


def test_the_code_is_read_inside_the_sandbox(code, runtimes):
    python, python_home, _uv, _lib = runtimes
    manifest = read_code(python=python, python_home=python_home, code=code, entry="orders")
    orders = manifest.export("orders")
    assert orders.returns == Named("Order")
    assert [(p.name, p.shape) for p in orders.parameters] == [
        ("customer_id", Scalar("text")),
        ("since", Scalar("timestamp")),
    ]


def test_rows_are_served_inside_the_sandbox(code, runtimes):
    started = _start(code, runtimes)
    try:
        [[row]] = _rows(started, code, runtimes, "orders", ["acme"])
        assert row["lines"] == [{"sku": "acme", "qty": 2}]
        assert [len(b) for b in _rows(started, code, runtimes, "many", [1500])] == [1000, 500]
    finally:
        started.stop()


def test_the_code_reads_nothing_outside_it(code, runtimes):
    started = _start(code, runtimes)
    try:
        with pytest.raises(w.CodeFailed, match="PermissionError|Operation not permitted"):
            _rows(started, code, runtimes, "outside", [])
    finally:
        started.stop()


def _outward_address() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.connect(("192.0.2.1", 9))  # TEST-NET-1: routed, never answered
        return probe.getsockname()[0]


@pytest.fixture
def upstream():
    server = socket.socket()
    server.bind((_outward_address(), 0))
    server.listen(8)

    def _serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")

    threading.Thread(target=_serve, daemon=True).start()
    yield server.getsockname()
    server.close()


def test_a_host_on_the_allow_list_is_reached_through_the_proxy_and_no_other(
    code, upstream, runtimes
):
    host, port = upstream
    started = _start(code, runtimes, egress=[f"{host}:{port}"])
    try:
        [[reached]] = _rows(started, code, runtimes, "fetched", [f"http://{host}:{port}/rates"])
        assert reached == {"status": 200, "body": "ok"}
        [[refused]] = _rows(started, code, runtimes, "fetched", [f"http://{host}:{port + 1}/x"])
        assert refused["status"] == 403
        assert "not on the egress allow-list" in refused["body"]
        with pytest.raises(w.CodeFailed):
            _rows(started, code, runtimes, "direct", [host, port])  # around the proxy
    finally:
        started.stop()


# -- the pinned Python and uv (part P2) ----------------------------------------------------------

SIX_WHEEL_SHA256 = "8abb2f1d86890a2dfb989f9a77cfcfd3e47c2a354b01111771326f8aa26e0254"


def test_dependencies_are_installed_from_the_lockfile_and_imported_in_the_sandbox(
    tmp_path, runtimes
):
    """The pinned uv installs a hashed requirements.txt from the public PyPI into the install's
    own environment, from the pinned Python, binary wheels only; the sandboxed worker runs that
    environment's interpreter and imports the dependency."""
    import io
    import zipfile

    from provisa.codesource.install import UploadCode
    from provisa.python.install import ensure_installed, environment_python
    from provisa.python.source import code_path, entry_module

    python, python_home, uv, _lib = runtimes
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(
            "rates/requirements.txt", f"six==1.16.0 --hash=sha256:{SIX_WHEEL_SHA256}\n"
        )
        archive.writestr(
            "rates/rates.py",
            "import six\n\n"
            "def versions() -> list[dict]:\n"
            '    """@table"""\n'
            '    return [{"six": six.__version__}]\n',
        )
    installed = ensure_installed(
        source_id="rates-src",
        code=UploadCode("rates.zip", buf.getvalue()),
        expected_hash=None,
        registries=[],
        python=python,
        uv=uv,
        cache_root=tmp_path / "cache",
    )
    code = code_path(installed.path)
    entry = entry_module(code)
    interpreter = environment_python(installed.path, python)
    assert interpreter != python  # the install's own environment
    manifest = read_code(python=interpreter, python_home=python_home, code=code, entry=entry)
    assert [e.name for e in manifest.exports] == ["versions"]
    command = host_command(
        python=interpreter,
        python_home=python_home,
        code=code,
        entry=entry,
        egress=[],
        host_lib=runtimes[3],
    )
    started = w.Worker("rates-src", installed.content_hash, command)
    started.start()
    schema = schema_text(row_schema(manifest, manifest.export("versions")))
    try:
        assert list(started.rows("versions", "table", [], schema=schema)) == [[{"six": "1.16.0"}]]
    finally:
        started.stop()
