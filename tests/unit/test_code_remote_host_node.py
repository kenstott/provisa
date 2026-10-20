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

_CODE = """
export function add(a, b) { return a + b; }
export async function greet(name) { return { greeting: `hi ${name}` }; }
"""


def _node() -> str:
    found = shutil.which("node")
    assert found is not None, "these tests run the remote host on the machine's node"
    return found


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def started(tmp_path):
    """A Node remote host on a free port, its state in a directory of its own; its pairing code."""
    (tmp_path / "index.mjs").write_text(_CODE)
    port = _free_port()
    names = tmp_path / "names.json"
    names.write_text("{}")
    worker = [_node(), str(HOST), "{fd}", str(tmp_path / "index.mjs")]
    process = subprocess.Popen(  # noqa: S603 -- the machine's node and Provisa's front end
        [sys.executable, front.__file__, str(port), str(tmp_path / "state"), str(names), *worker],
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
        if line.startswith("remote host listening"):
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
    pin = remote.pair("127.0.0.1", port, pairing, identity)
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
        remote.pair("127.0.0.1", port, f"{fingerprint}.not-the-secret", remote.new_identity())
    assert "not this host's pairing code" in str(refused.value)
    assert not (state / "provisa.crt").exists()


def test_once_paired_only_the_paired_provisa_is_served(started):
    port, pairing, _state = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity)
    stranger = remote.RemoteWorker(
        "shop", "h1", address="127.0.0.1", port=port, pin=pin, identity=remote.new_identity()
    )
    with pytest.raises(WorkerFailed):
        stranger.start(timeout=5)
    with pytest.raises(remote.PairingRefused):
        remote.pair("127.0.0.1", port, pairing, identity)  # paired: it takes no pair message


def test_a_host_running_other_code_is_refused_by_name(started):
    port, pairing, _state = started
    identity = remote.new_identity()
    pin = remote.pair("127.0.0.1", port, pairing, identity)
    other = remote.RemoteWorker(
        "shop", "h2", address="127.0.0.1", port=port, pin=pin, identity=identity
    )
    with pytest.raises(WorkerFailed) as refused:
        other.start()
    assert "h1" in str(refused.value) and "h2" in str(refused.value)
