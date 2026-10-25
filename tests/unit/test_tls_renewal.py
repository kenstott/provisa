# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2e9c41-3d5a-4f8e-a1c6-9e0d2b4f7a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The deployment's wildcard certificate is kept in Secret Manager and renewed there (REQ-1239).
cloud.provisa.dev's certificate expired: it was renewed by hand only, and each VM rewrote the
copy in its metadata at every boot, so a renewal installed by hand was undone at the next wake.
Every VM now installs it from Secret Manager at boot and hourly (provisa-tls-sync), and the front
door renews it there daily once few days remain (provisa-tls-renew). The scripts run here with
gcloud and lego stood in for and openssl real."""

# Requirements: REQ-1239

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_TF = _REPO / "terraform" / "gcp-saas"
_SYNC = _TF / "tls" / "provisa-tls-sync.sh"
_RENEW = _TF / "tls" / "provisa-tls-renew.sh"


def _cert(path: Path, days: int) -> None:
    """A self-signed certificate valid for ``days``, and its key beside it (``.key``)."""
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", str(days)]
        + ["-subj", "/CN=*.example.test", "-keyout", str(path.with_suffix(".key"))]
        + ["-out", str(path)],
        check=True,
        capture_output=True,
    )


def _stubs(tmp: Path) -> Path:
    """A bin dir with a gcloud that answers `secrets versions access latest --secret S` from
    ``secrets/S`` and records `secrets versions add`, and a lego that issues a certificate."""
    bin_ = tmp / "bin"
    bin_.mkdir()
    (bin_ / "gcloud").write_text(
        f"""#!/bin/bash
set -e
secret=""
args=("$@")
for ((i = 0; i < ${{#args[@]}}; i++)); do
  case "${{args[$i]}}" in
    --secret) secret="${{args[$((i + 1))]}}" ;;
    --data-file) data="${{args[$((i + 1))]}}" ;;
  esac
done
case "$3" in
  access)
    [ -f "{tmp}/secrets/$secret" ] || {{ echo "NOT_FOUND: $secret" >&2; exit 1; }}
    cat "{tmp}/secrets/$secret" ;;
  add)
    secret="$4"
    echo "$secret" >> "{tmp}/added"
    cp "$data" "{tmp}/secrets/$secret" ;;
esac
"""
    )
    (bin_ / "lego").write_text(
        f"""#!/bin/bash
set -e
path=""
args=("$@")
for ((i = 0; i < ${{#args[@]}}; i++)); do
  [ "${{args[$i]}}" = --path ] && path="${{args[$((i + 1))]}}"
done
echo "$CLOUDFLARE_DNS_API_TOKEN" > "{tmp}/lego-token"
mkdir -p "$path/certificates"
openssl req -x509 -newkey rsa:2048 -nodes -days 90 -subj '/CN=*.example.test' \\
  -keyout "$path/certificates/_.example.test.key" -out "$path/certificates/_.example.test.crt" \\
  2>/dev/null
"""
    )
    (bin_ / "provisa-tls-sync").write_text(f"#!/bin/bash\necho synced >> '{tmp}/synced'\n")
    for f in bin_.iterdir():
        f.chmod(0o755)
    (tmp / "secrets").mkdir()
    return bin_


def _run(script: Path, tmp: Path, env_file: Path, *args: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "PATH": f"{tmp / 'bin'}:{os.environ['PATH']}",
        "PROVISA_TLS_SYNC_ENV": str(env_file),
        "PROVISA_TLS_RENEW_ENV": str(env_file),
    }
    return subprocess.run(
        ["bash", str(script), *args], env=env, capture_output=True, text=True, check=False
    )


@pytest.fixture
def sync(tmp_path):
    _stubs(tmp_path)
    _cert(tmp_path / "issued.crt", 90)
    shutil.copy(tmp_path / "issued.crt", tmp_path / "secrets" / "provisa-tls-cert")
    shutil.copy(tmp_path / "issued.key", tmp_path / "secrets" / "provisa-tls-key")
    env = tmp_path / "sync.env"
    env.write_text(
        "PROJECT=p\nCERT_SECRET=provisa-tls-cert\nKEY_SECRET=provisa-tls-key\n"
        f"TARGETS='{tmp_path}/a/node.crt:{tmp_path}/a/node.key {tmp_path}/b/c.crt:{tmp_path}/b/c.key'\n"
        f"RESTART='echo restarted >> {tmp_path}/restarts'\n"
    )
    return tmp_path, env


def test_a_new_certificate_is_installed_everywhere_and_what_serves_it_restarted_once(sync):
    tmp, env = sync
    done = _run(_SYNC, tmp, env)
    assert done.returncode == 0, done.stderr
    for crt, key in (("a/node.crt", "a/node.key"), ("b/c.crt", "b/c.key")):
        assert (tmp / crt).read_bytes() == (tmp / "issued.crt").read_bytes()
        assert (tmp / key).read_bytes() == (tmp / "issued.key").read_bytes()
        assert oct((tmp / key).stat().st_mode & 0o777) == "0o600"
    assert (tmp / "restarts").read_text() == "restarted\n"
    # Installed already: nothing is written or restarted again.
    again = _run(_SYNC, tmp, env)
    assert again.returncode == 0 and "certificate current" in again.stdout
    assert (tmp / "restarts").read_text() == "restarted\n"


def test_at_boot_it_installs_and_restarts_nothing(sync):
    tmp, env = sync
    assert _run(_SYNC, tmp, env, "--boot").returncode == 0
    assert (tmp / "a" / "node.crt").exists()
    assert not (tmp / "restarts").exists()


def test_a_certificate_and_key_that_do_not_match_are_never_installed(sync):
    tmp, env = sync
    _cert(tmp / "other.crt", 90)
    shutil.copy(tmp / "other.key", tmp / "secrets" / "provisa-tls-key")
    done = _run(_SYNC, tmp, env)
    assert done.returncode == 1 and "do not match" in done.stderr
    assert not (tmp / "a" / "node.crt").exists()


def test_no_stored_certificate_fails_saying_where_one_comes_from(sync):
    tmp, env = sync
    (tmp / "secrets" / "provisa-tls-cert").unlink()
    done = _run(_SYNC, tmp, env)
    assert done.returncode == 1
    assert "provisa-tls-cert" in done.stderr and "issue-wildcard-cert.sh" in done.stderr


@pytest.fixture
def renew(tmp_path):
    _stubs(tmp_path)
    (tmp_path / "secrets" / "provisa-cloudflare-dns-token").write_text("cf-token")
    env = tmp_path / "renew.env"
    env.write_text(
        "PROJECT=p\nCERT_SECRET=provisa-tls-cert\nKEY_SECRET=provisa-tls-key\n"
        "TOKEN_SECRET=provisa-cloudflare-dns-token\nACME_EMAIL=ops@example.test\n"
        f"DOMAIN=example.test\nRENEW_DAYS=30\nLEGO_PATH={tmp_path}/lego\n"
    )
    return tmp_path, env


def _store(tmp: Path, days: int) -> None:
    _cert(tmp / "stored.crt", days)
    shutil.copy(tmp / "stored.crt", tmp / "secrets" / "provisa-tls-cert")
    shutil.copy(tmp / "stored.key", tmp / "secrets" / "provisa-tls-key")


def test_a_certificate_with_time_left_is_not_renewed(renew):
    tmp, env = renew
    _store(tmp, 60)
    done = _run(_RENEW, tmp, env)
    assert done.returncode == 0, done.stderr
    assert not (tmp / "lego-token").exists() and not (tmp / "added").exists()


def test_one_near_its_end_is_renewed_stored_key_first_and_installed_here(renew):
    tmp, env = renew
    _store(tmp, 10)
    done = _run(_RENEW, tmp, env)
    assert done.returncode == 0, done.stderr
    assert (tmp / "lego-token").read_text() == "cf-token\n"
    # The key first: a reader between the two writes finds a pair that does not match.
    assert (tmp / "added").read_text().split() == ["provisa-tls-key", "provisa-tls-cert"]
    issued = tmp / "lego" / "certificates" / "_.example.test.crt"
    assert (tmp / "secrets" / "provisa-tls-cert").read_bytes() == issued.read_bytes()
    assert (tmp / "synced").read_text() == "synced\n"


# -- where terraform puts it ------------------------------------------------------------------


def test_no_vm_carries_the_certificate_in_its_metadata():
    for tf in _TF.glob("*.tf"):
        assert "tls_cert_pem" not in tf.read_text(), tf.name
        assert "tls_key_pem" not in tf.read_text(), tf.name


def test_every_vm_installs_it_from_secret_manager_and_the_front_door_renews_it():
    main = (_TF / "main.tf").read_text()
    secrets = (_TF / "secrets.tf").read_text()
    for name in ("provisa-tls-cert", "provisa-tls-key", "provisa-cloudflare-dns-token"):
        assert f'"{name}"' in secrets
    # The coordinator and the sandbox VM boot from base_startup; the front door from its own.
    base = main[main.index("base_startup = <<-SHELL") :]
    assert "${local.node_tls_sync}" in base[: base.index("\n  SHELL\n")]
    door = main[main.index('resource "google_compute_instance" "front_door"') :]
    assert "${local.front_door_tls_sync}" in door and "${local.front_door_tls_renew}" in door
    # lego is a pinned release checked against its published checksum.
    assert "lego_v5.4.1_linux_amd64.tar.gz" in main
    assert "sha256sum -c" in (_TF / "tls" / "install-renew.sh.tftpl").read_text()


def test_each_vms_install_steps_are_a_valid_script(tmp_path):
    """The templates the startup scripts embed, filled as terraform fills them, parse as bash."""
    import re

    values = {
        "script": "c2NyaXB0", "project": "p", "cert_secret": "c", "key_secret": "k",
        "token_secret": "t", "targets": "/a.crt:/a.key", "before": "docker.service",
        "restart": "systemctl restart x", "lego_url": "https://example.test/lego.tar.gz",
        "lego_sha256": "0" * 64, "acme_email": "ops@example.test", "domain": "example.test",
        "renew_days": "30",
    }  # fmt: skip
    for name in ("install-sync.sh.tftpl", "install-renew.sh.tftpl"):
        text = (_TF / "tls" / name).read_text()
        filled = re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], text)
        script = tmp_path / name
        script.write_text(filled)
        checked = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert checked.returncode == 0, (name, checked.stderr)
