# Copyright (c) 2026 Kenneth Stott
# Canary: c28b0d79-f4be-4a8d-8d13-0a4e9b1f58d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1135–1139: the offline trial + license subsystem.

Covers first-use anchors surviving deletion (REQ-1135), rollback-resistant trial elapsed time
(REQ-1136), the once-per-connection nag (REQ-1137), offline Ed25519 license verification
(REQ-1138), and license application with machine-id matching (REQ-1139). No network anywhere.
"""

from __future__ import annotations

import datetime
import json

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from provisa.licensing import anchors, monotonic
from provisa.licensing.license import _canonical_payload, apply_license, verify_license
from provisa.licensing.nag import NagRateLimiter, nag_message
from provisa.licensing.state import evaluate

MID = "testmachine0000000000000000000000"


def _epoch(y, m, d):
    return datetime.date(y, m, d).toordinal() * 86400


# ------------------------------------------------------- REQ-1135 anchors


def test_anchor_records_first_seen(tmp_path):
    paths = [tmp_path / "a.json", tmp_path / "sub" / "b.json", tmp_path / ".m"]
    fs = anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-01-01", paths=paths)
    assert fs == "2026-01-01"
    assert all(p.exists() for p in paths)  # written to every location


def test_anchor_survives_deletion(tmp_path):
    paths = [tmp_path / "a.json", tmp_path / "b.json", tmp_path / "c.json"]
    anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-01-01", paths=paths)
    # delete two anchors, reconcile "later": the survivor re-seeds the others with the ORIGINAL date
    paths[0].unlink()
    paths[2].unlink()
    fs = anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-06-01", paths=paths)
    assert fs == "2026-01-01"  # not reset to the later date
    assert all(p.exists() for p in paths)


def test_anchor_tamper_is_rejected(tmp_path):
    p = tmp_path / "a.json"
    anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-01-01", paths=[p])
    data = json.loads(p.read_text())
    data["first_seen"] = "2020-01-01"  # forge an earlier date without re-signing
    p.write_text(json.dumps(data))
    assert anchors.read_anchor(p, MID) is None  # signature no longer verifies


def test_anchor_earliest_wins(tmp_path):
    a, b = tmp_path / "a.json", tmp_path / "b.json"
    anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-03-01", paths=[a])
    anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-01-01", paths=[b])
    fs = anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-09-09", paths=[a, b])
    assert fs == "2026-01-01"  # min across all anchors


# ------------------------------------------------------- REQ-1136 monotonic


def test_highwater_is_monotonic(tmp_path):
    hw = tmp_path / "hw.json"
    monotonic.update_highwater(hw, _epoch(2026, 2, 10))
    # a backwards clock does not lower the mark
    assert monotonic.update_highwater(hw, _epoch(2026, 1, 1)) == _epoch(2026, 2, 10)


def test_trial_expiry_resists_rollback(tmp_path):
    hw = tmp_path / "hw.json"
    monotonic.update_highwater(hw, _epoch(2026, 2, 15))  # 45 days after Jan 1
    high_water = monotonic.read_highwater(hw)
    # even though "now" was set back to Jan 5, elapsed uses the high-water mark → still expired
    assert monotonic.trial_expired(
        now_epoch=_epoch(2026, 1, 5), high_water=high_water, first_seen_iso="2026-01-01"
    )


def test_trial_active_within_30_days():
    assert not monotonic.trial_expired(
        now_epoch=_epoch(2026, 1, 20), high_water=_epoch(2026, 1, 20), first_seen_iso="2026-01-01"
    )


# ------------------------------------------------------- REQ-1138/1139 license


@pytest.fixture
def issuer(monkeypatch):
    """A test license issuer keypair pinned as the embedded public key."""
    priv = Ed25519PrivateKey.generate()
    pub_hex = (
        priv.public_key()
        .public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        .hex()
    )
    monkeypatch.setenv("PROVISA_LICENSE_PUBKEY", pub_hex)
    return priv


def _sign_license(priv, machine_id, **overrides):
    payload = {
        "company": "Acme",
        "position": "Engineer",
        "role": "admin",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@acme.test",
        "machine_id": machine_id,
        "issued_at": "2026-07-20",
    }
    payload.update(overrides)
    payload["sig"] = priv.sign(_canonical_payload(payload)).hex()
    return payload


def test_valid_license_verifies(issuer):
    lic = verify_license(_sign_license(issuer, MID), machine_id=MID)
    assert lic.valid


def test_tampered_license_rejected(issuer):
    payload = _sign_license(issuer, MID)
    payload["email"] = "evil@x.test"  # change a field after signing
    lic = verify_license(payload, machine_id=MID)
    assert not lic.valid and "signature" in lic.reason


def test_machine_mismatch_rejected(issuer):
    # validly signed, but for a different machine → rejected on the machine_id check
    payload = _sign_license(issuer, "othermachine")
    lic = verify_license(payload, machine_id=MID)
    assert not lic.valid and "machine_id" in lic.reason


def test_unsigned_license_rejected(issuer):
    payload = _sign_license(issuer, MID)
    del payload["sig"]
    assert not verify_license(payload, machine_id=MID).valid


def test_apply_valid_license_stores_it(issuer, tmp_path):
    src = tmp_path / "license.json"
    src.write_text(json.dumps(_sign_license(issuer, MID)))
    dest = tmp_path / "installed.json"
    result = apply_license(src, dest=dest, machine_id=MID)
    assert result.valid and dest.exists()


def test_apply_invalid_license_not_stored(issuer, tmp_path):
    src = tmp_path / "license.json"
    src.write_text(json.dumps(_sign_license(issuer, "wrong")))
    dest = tmp_path / "installed.json"
    assert not apply_license(src, dest=dest, machine_id=MID).valid
    assert not dest.exists()  # rejected license is never installed


# ------------------------------------------------------- REQ-1137 nag


def test_nag_message_is_self_contained():
    msg = nag_message(MID)
    assert "FREE" in msg
    assert "no telemetry" in msg.lower()
    assert MID in msg  # includes the machine id
    assert "provisa license apply" in msg
    assert "provisa.dev" in msg


def test_nag_rate_limited_once_per_connection():
    rl = NagRateLimiter()
    assert rl.should_emit("conn-1")
    assert not rl.should_emit("conn-1")  # second time on same connection: suppressed
    assert rl.should_emit("conn-2")  # a different connection is nagged once


# ------------------------------------------------------- REQ-1135–1139 state


def test_evaluate_nags_after_expiry_without_license(tmp_path, monkeypatch):
    monkeypatch.setattr("provisa.licensing.state.stable_machine_id", lambda: MID)
    paths = [tmp_path / "a.json"]
    # seed first-use far in the past so the trial is expired
    anchors.reconcile_first_seen(machine_id=MID, today_iso="2026-01-01", paths=paths)
    st = evaluate(
        now_epoch=_epoch(2026, 3, 1),
        today_iso="2026-03-01",
        anchor_paths=paths,
        highwater_path=tmp_path / "hw.json",
        license_path=tmp_path / "none.json",
    )
    assert st.trial_expired and not st.licensed and st.should_nag


def test_evaluate_no_nag_within_trial(tmp_path, monkeypatch):
    monkeypatch.setattr("provisa.licensing.state.stable_machine_id", lambda: MID)
    paths = [tmp_path / "a.json"]
    st = evaluate(
        now_epoch=_epoch(2026, 1, 10),
        today_iso="2026-01-10",
        anchor_paths=paths,
        highwater_path=tmp_path / "hw.json",
        license_path=tmp_path / "none.json",
    )
    assert not st.trial_expired and not st.should_nag


# ------------------------------------------------------- REQ-1137 emit seam


def _expired_state():
    from provisa.licensing.state import LicensingState

    return LicensingState(
        machine_id=MID, first_seen="2026-01-01", elapsed_days=99.0,
        trial_expired=True, licensed=False, license_reason="no license present",
    )


def test_emit_nag_once_per_connection(monkeypatch):
    from provisa.licensing import emit

    emit.set_state(_expired_state())
    assert emit.should_nag()
    assert emit.nag_for_connection("c1") is not None
    assert emit.nag_for_connection("c1") is None  # once per connection
    assert emit.nag_for_connection("c2") is not None
    emit.set_state(None)  # reset for other tests


def test_emit_no_nag_when_licensed():
    from provisa.licensing import emit
    from provisa.licensing.state import LicensingState

    emit.set_state(
        LicensingState(machine_id=MID, first_seen="2026-01-01", elapsed_days=99.0,
                       trial_expired=True, licensed=True, license_reason="")
    )
    assert not emit.should_nag()
    assert emit.nag_for_connection("c1") is None
    emit.set_state(None)


# ------------------------------------------------------- REQ-1137 per-surface channels


def test_bolt_success_metadata_notification(monkeypatch):
    from provisa.bolt.session import BoltSession
    from provisa.licensing import emit

    emit.set_state(_expired_state())
    sess = BoltSession(writer=object(), bolt_version=(5, 4))
    note = sess._license_nag_notification()
    assert note is not None
    assert note["severity"] == "WARNING"
    assert MID in note["description"]
    assert sess._license_nag_notification() is None  # once per connection
    emit.set_state(None)


def test_grpc_trailing_metadata_nag(monkeypatch):
    from provisa.grpc.server import ProvisaServicer
    from provisa.licensing import emit

    emit.set_state(_expired_state())
    captured = {}

    class _Ctx:
        def peer(self):
            return "ipv4:1.2.3.4:5"

        def set_trailing_metadata(self, md):
            captured["md"] = md

    servicer = ProvisaServicer(state=object(), pb2_module=object(), pb2_grpc_module=object())
    servicer._emit_license_nag(_Ctx())
    assert captured["md"][0][0] == "x-provisa-license-notice"
    assert MID in captured["md"][0][1]
    emit.set_state(None)


def test_flight_license_stream_attaches_app_metadata(monkeypatch):
    import pyarrow as pa

    from provisa.api.flight.server import ProvisaFlightServer
    from provisa.licensing import emit

    emit.set_state(_expired_state())
    # build the server object without opening a port
    srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
    table = pa.table({"a": [1, 2]})
    stream = srv._license_stream(table, "analyst")
    # nagging → a GeneratorStream (carries app_metadata), not a plain RecordBatchStream
    import pyarrow.flight as flight

    assert isinstance(stream, flight.GeneratorStream)
    emit.set_state(None)
    # not nagging → plain RecordBatchStream
    stream2 = srv._license_stream(table, "analyst")
    assert isinstance(stream2, flight.RecordBatchStream)


def test_pgwire_notice_frame_encoding():
    import io
    import struct

    from provisa.pgwire.server import ProvisaHandler

    h = ProvisaHandler.__new__(ProvisaHandler)
    h.wfile = io.BytesIO()
    h._send_pg_notice("trial expired")
    data = h.wfile.getvalue()
    assert data[:1] == b"N"  # NoticeResponse frame tag
    length = struct.unpack("!i", data[1:5])[0]
    assert length == len(data) - 1
    assert b"trial expired" in data
    assert b"NOTICE" in data
