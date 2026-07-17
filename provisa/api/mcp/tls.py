# Copyright (c) 2026 Kenneth Stott
# Canary: 061a0671-429d-4897-8d61-f66da3b19ccd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-machine self-signed TLS for the local MCP server (REQ-1106).

Claude Desktop's "Add custom connector" only accepts an https:// URL, so the native tier can serve
its loopback MCP server over TLS and let the user paste https://localhost:<port>/mcp straight into
that dialog -- no stdio bridge, no Node.

Design rules:
  - NEVER bundle a cert: a shipped cert is a shipped PRIVATE KEY, trivially extractable and usable to
    MITM every install. The cert is generated ONCE per machine, in ~/.provisa/certs, and is
    user-replaceable (delete the files to regenerate, or drop in your own).
  - Generation is cross-platform (cryptography). Trust-store install IS OS-specific and best-effort.
  - EVERYTHING here is best-effort and NEVER raises: if a cert can't be created the caller serves
    plain HTTP instead and the mcp-proxy stdio bridge remains the working path. TLS is an
    enhancement, never a hard requirement (the fallback is design-mandated, hence the broad guards).
"""

from __future__ import annotations

import datetime
import ipaddress
import logging
import os
import subprocess  # nosec B404 - only fixed-arg trust-store CLIs (certutil/security), no shell
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def _cert_dir() -> Path:
    home = os.environ.get("PROVISA_HOME") or str(Path.home() / ".provisa")
    return Path(home) / "certs"


def _generate(cert_path: Path, key_path: Path) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)  # self-signed: subject == issuer
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    x509.IPAddress(ipaddress.ip_address("::1")),
                ]
            ),
            critical=False,
        )
        # ca=True so the same self-signed cert can be added to a Root trust store (Docker-Desktop
        # style self-trust); serverAuth EKU + digitalSignature/keyCertSign for TLS server use.
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([x509.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    try:
        os.chmod(key_path, 0o600)  # private key: owner-only where the OS honors it
    except OSError:
        pass


def ensure_cert() -> tuple[str, str] | None:
    """Return ``(certfile, keyfile)`` paths, generating a per-machine self-signed pair if absent.

    Returns None on ANY failure so the caller falls back to plain HTTP. Never raises."""
    try:
        cert, key = _cert_dir() / "mcp-cert.pem", _cert_dir() / "mcp-key.pem"
        if not (cert.exists() and key.exists()):
            _generate(cert, key)
        return (str(cert), str(key))
    except Exception as exc:  # noqa: BLE001 - TLS is optional; any failure -> HTTP fallback
        log.warning("MCP TLS cert could not be created (%s); serving plain HTTP", exc)
        return None


def _trust_windows_pure(cert_path: str) -> bool:
    """Add the cert to the CurrentUser ROOT store via crypt32 in pure Python (ctypes) - no certutil,
    no admin. Returns True on success. Raises on any Win32 error (caller catches)."""
    import ctypes

    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding

    der = x509.load_pem_x509_certificate(Path(cert_path).read_bytes()).public_bytes(Encoding.DER)

    crypt32 = ctypes.WinDLL("crypt32.dll")  # type: ignore[attr-defined]  # win32-only
    CERT_STORE_PROV_SYSTEM_W = 10
    CERT_SYSTEM_STORE_CURRENT_USER = 1 << 16  # per-user store -> no elevation
    X509_ASN_ENCODING = 0x1
    PKCS_7_ASN_ENCODING = 0x10000
    CERT_STORE_ADD_REPLACE_EXISTING = 3

    crypt32.CertOpenStore.restype = ctypes.c_void_p
    store = crypt32.CertOpenStore(
        ctypes.c_void_p(CERT_STORE_PROV_SYSTEM_W),
        0,
        None,
        CERT_SYSTEM_STORE_CURRENT_USER,
        ctypes.c_wchar_p("ROOT"),
    )
    if not store:
        raise OSError("CertOpenStore(ROOT) failed")
    try:
        ok = crypt32.CertAddEncodedCertificateToStore(
            ctypes.c_void_p(store),
            X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
            der,
            len(der),
            CERT_STORE_ADD_REPLACE_EXISTING,
            None,
        )
        if not ok:
            raise OSError(f"CertAddEncodedCertificateToStore failed ({ctypes.GetLastError()})")
        return True
    finally:
        crypt32.CertCloseStore(ctypes.c_void_p(store), 0)


def trust_cert(cert_path: str) -> bool:
    """Best-effort: add the self-signed cert to the OS *user* trust store so Claude Desktop trusts
    https://localhost. Per-user stores only -> no admin elevation. Returns True on success.

    A failure is not fatal: the cert still serves TLS, and the mcp-proxy bridge (--no-verify-ssl)
    remains a fallback. Windows is done in PURE PYTHON (ctypes -> crypt32); macOS uses the one
    `security` call (a pure-ctypes Security.framework path isn't worth the weight); Linux trust
    stores vary (nss vs ca-certificates) and are skipped rather than guessed."""
    try:
        if sys.platform == "win32":
            try:
                return _trust_windows_pure(cert_path)
            except Exception as exc:  # noqa: BLE001 - fall back to certutil if the ctypes path fails
                log.info("pure-python cert trust failed (%s); trying certutil", exc)
                subprocess.run(  # nosec B603 B607 - fixed args, no shell, our own cert file
                    ["certutil", "-user", "-addstore", "-f", "Root", cert_path],
                    check=True,
                    capture_output=True,
                )
                return True
        if sys.platform == "darwin":
            # Add to the user login keychain as a trusted root (no admin; may prompt for the login
            # password when it flips trust settings — the same one-time consent Docker Desktop uses).
            keychain = str(Path.home() / "Library/Keychains/login.keychain-db")
            subprocess.run(  # nosec B603 B607
                ["security", "add-trusted-cert", "-r", "trustRoot", "-k", keychain, cert_path],
                check=True,
                capture_output=True,
            )
            return True
        if sys.platform.startswith("linux"):
            # Claude Desktop on Linux is Electron/Chromium, which reads the per-user NSS DB at
            # ~/.pki/nssdb (NOT the system CA bundle). Add the cert there via libnss3-tools `certutil`
            # (the NSS tool — distinct from Windows certutil). No root. Best-effort: create the DB if
            # absent, and skip if the tool isn't installed.
            nssdb = Path.home() / ".pki" / "nssdb"
            nssdb.mkdir(parents=True, exist_ok=True)
            db = f"sql:{nssdb}"
            if not (nssdb / "cert9.db").exists():
                subprocess.run(  # nosec B603 B607 - create an empty per-user NSS DB
                    ["certutil", "-N", "-d", db, "--empty-password"],
                    check=True,
                    capture_output=True,
                )
            subprocess.run(  # nosec B603 B607 - trust as a CA for SSL ("C,,")
                ["certutil", "-A", "-d", db, "-n", "Provisa MCP (localhost)", "-t", "C,,",
                 "-i", cert_path],
                check=True,
                capture_output=True,
            )
            return True
        return False  # unknown platform: trust manually or use the bridge
    except Exception as exc:  # noqa: BLE001 - trust is best-effort; serving TLS does not depend on it
        log.info("Could not auto-trust the MCP cert (%s); trust it manually or use the bridge", exc)
        return False
