# Copyright (c) 2026 Kenneth Stott
# Canary: ecc9f147-2691-44eb-a2b0-8477bfdacee1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node source on a remote host (REQ-2098, REQ-2100): the front end every built image runs, with
Node's host program as its worker; its identity a self-signed certificate it writes itself,
paired with and dialled by Provisa over mutual TLS, serving the same calls a local worker serves.
Runs on the ``node`` the machine has."""

# Requirements: REQ-2098, REQ-2100

from __future__ import annotations

import os
import shutil
import socket
import ssl
import subprocess
import sys
import time

import pytest

from provisa.codehost import front
from provisa.codesource import remote
from provisa.codesource.worker import WorkerFailed
from provisa.node.worker import HOST

pytestmark = pytest.mark.unit

BOUNDS = {
    "memory_mb": 256,
    "threads": 2,
    "spill_cap_mb": 1024,
}  # the host's DuckDB, given at pairing

_CODE = """
export function add(a, b) { return a + b; }
export async function greet(name) { return { greeting: `hi ${name}` }; }
"""


def _node() -> str:
    found = shutil.which("node")
    assert found is not None, "these tests run the remote host on the machine's node"
    return found


@pytest.fixture
def started(tmp_path):
    """A Node remote host on a free port, its state in a directory of its own; its pairing code."""
    (tmp_path / "index.mjs").write_text(_CODE)
    port = 0
    names = tmp_path / "names.json"
    names.write_text("{}")
    worker = [_node(), str(HOST), "{fd}", str(tmp_path / "index.mjs")]
    process = subprocess.Popen(  # noqa: S603 -- the machine's node and Provisa's front end
        [
            sys.executable,
            front.__file__,
            str(port),
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *worker,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "h1"},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing = None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            # Bound by the host itself (port 0): no other test can take it first.
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None, "the host printed no pairing code"
    yield port, pairing, tmp_path / "state"
    process.terminate()
    process.wait(timeout=10)


def test_its_certificate_is_a_self_signed_p256_certificate_its_code_names(started):
    port, pairing, _state = started
    with socket.create_connection(("127.0.0.1", port), timeout=10) as raw:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with context.wrap_socket(raw) as tls:
            der = tls.getpeercert(binary_form=True)
            assert tls.version() == "TLSv1.3"
    from cryptography import x509
    from cryptography.hazmat.primitives.asymmetric import ec

    assert der is not None
    certificate = x509.load_der_x509_certificate(der)
    assert certificate.issuer == certificate.subject
    key = certificate.public_key()
    assert isinstance(key, ec.EllipticCurvePublicKey) and key.curve.name == "secp256r1"
    algorithm = certificate.signature_hash_algorithm
    assert algorithm is not None
    # Self-signed: its own key signs it.
    key.verify(certificate.signature, certificate.tbs_certificate_bytes, ec.ECDSA(algorithm))
    import hashlib

    assert pairing.split(".")[0] == hashlib.sha256(der).hexdigest()


def test_provisa_pairs_dials_and_calls_it(started):
    port, pairing, state = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    assert pin == pairing.split(".")[0]
    assert (state / "provisa.crt").read_text() == identity.certificate
    assert not (state / "pairing.secret").exists()
    worker = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    worker.start()
    try:
        assert worker.exports == ["add", "greet"]
        assert worker.command("add", [2, 40]) == 42
        assert worker.command("greet", ["ada"]) == {"greeting": "hi ada"}
    finally:
        worker.stop()


def test_another_secret_is_refused_and_the_host_stays_unpaired(started):
    port, pairing, state = started
    fingerprint = pairing.split(".")[0]
    with pytest.raises(remote.PairingRefused) as refused:
        remote.pair(
            "127.0.0.1", port, f"{fingerprint}.not-the-secret", remote.new_identity(), BOUNDS
        )
    assert "not this host's pairing code" in str(refused.value)
    assert not (state / "provisa.crt").exists()


def test_once_paired_only_the_paired_provisa_is_served(started):
    port, pairing, _state = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    stranger = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=remote.new_identity()
    )
    with pytest.raises(WorkerFailed):
        stranger.start(timeout=5)
    with pytest.raises(remote.PairingRefused):
        remote.pair(
            "127.0.0.1", port, pairing, identity, BOUNDS
        )  # paired: it takes no pair message


def test_a_host_running_other_code_is_refused_by_name(started):
    port, pairing, _state = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    other = remote.RemoteWorker(
        "shop", "h2", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    with pytest.raises(WorkerFailed) as refused:
        other.start()
    assert "h1" in str(refused.value) and "h2" in str(refused.value)


_ARROW_CODE = """
import { createRequire } from "node:module";
const A = createRequire(import.meta.url)(`${process.env.PROVISA_HOST_LIB}/package/Arrow.es2015.min.js`);
export function table(n) {
  return A.tableFromArrays({ extra: Array.from({ length: n }, () => "-"), n: BigInt64Array.from({ length: n }, (_, i) => BigInt(i)) });
}
export function* yielded(n) {
  yield { n: -1 };
  for (let i = 0; i < n; i += 2) {
    yield A.tableFromArrays({ n: BigInt64Array.from({ length: Math.min(2, n - i) }, (_, k) => BigInt(i + k)) });
  }
}
"""


def test_arrow_the_code_returns_or_yields_is_sent_as_it_is(tmp_path):
    """REQ-2100: an Arrow Table the code returns, or yields between plain rows, goes as it is,
    its columns the declared ones by name; over each of the front's connections to the host."""
    import pyarrow as pa

    from provisa.codesource.arrow_rows import schema_text
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

    lib = CodeRuntimeResolver().resolve("apache-arrow").parent.parent
    (tmp_path / "index.mjs").write_text(_ARROW_CODE)
    names = tmp_path / "names.json"
    names.write_text("{}")
    host = [_node(), str(HOST), "{fd}", str(tmp_path / "index.mjs")]
    process = subprocess.Popen(  # noqa: S603 -- the machine's node and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *host,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "h1", "PROVISA_HOST_LIB": str(lib)},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing, port = None, 0
    until = time.monotonic() + 20
    while time.monotonic() < until:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None and port, "the host did not start"
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    worker = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    worker.start()
    schema = schema_text(pa.schema([("n", pa.int64())]))
    try:
        returned = [r for b in worker.rows("table", "table", [3], ["n"], schema=schema) for r in b]
        assert returned == [{"n": 0}, {"n": 1}, {"n": 2}]
        yielded = [r for b in worker.rows("yielded", "table", [5], ["n"], schema=schema) for r in b]
        assert [r["n"] for r in yielded] == [-1, 0, 1, 2, 3, 4]
    finally:
        worker.stop()
        process.terminate()
        process.wait(timeout=10)


_CONTEXT_CODE = """
export function asked({ region = "EU", n }, context) {
  const filters = context.filters.map((f) => `${f.column} ${f.op} ${f.value}`).join(",");
  return [{ asked: `region=${region};n=${n};filters=${filters};limit=${context.limit}` }];
}
"""


def test_a_table_is_called_with_its_inputs_object_and_its_read_context(tmp_path):
    """REQ-2100: a Node @table function takes (inputs, context): an object of the inputs the read
    gives -- one it leaves out absent, so the function's default applies -- and the read context."""
    import pyarrow as pa

    from provisa.codesource.arrow_rows import schema_text
    from provisa.codesource.worker import ABSENT
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

    lib = CodeRuntimeResolver().resolve("apache-arrow").parent.parent
    (tmp_path / "index.mjs").write_text(_CONTEXT_CODE)
    names = tmp_path / "names.json"
    names.write_text("{}")
    host = [_node(), str(HOST), "{fd}", str(tmp_path / "index.mjs")]
    process = subprocess.Popen(  # noqa: S603 -- the machine's node and Provisa's front end
        [
            sys.executable,
            front.__file__,
            "0",
            str(tmp_path / "state"),
            str(tmp_path / "spill"),
            str(names),
            *host,
        ],
        env={**os.environ, "PROVISA_CODE_HASH": "h1", "PROVISA_HOST_LIB": str(lib)},
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stderr is not None
    pairing, port = None, 0
    until = time.monotonic() + 20
    while time.monotonic() < until:
        line = process.stderr.readline()
        if line.startswith("pairing code: "):
            pairing = line.removeprefix("pairing code: ").strip()
        if line.startswith("remote host listening on "):
            port = int(line.rsplit(" ", 1)[1])
            break
    assert pairing is not None and port, "the host did not start"
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity, BOUNDS)
    worker = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    worker.start()
    schema = schema_text(pa.schema([("asked", pa.utf8())]))
    context = {
        "filters": [{"column": "asked", "op": "ne", "value": "x"}],
        "order_by": [],
        "limit": 3,
        "columns": ["asked"],
        "keys": None,
    }
    try:
        rows = [
            r
            for b in worker.rows(
                "asked",
                "table",
                [ABSENT, 7],
                ["region", "n"],
                schema=schema,
                takes=("inputs", "context"),
                context=context,
            )
            for r in b
        ]
        assert rows == [{"asked": "region=EU;n=7;filters=asked ne x;limit=3"}]
    finally:
        worker.stop()
        process.terminate()
        process.wait(timeout=10)
