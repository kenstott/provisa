# Copyright (c) 2026 Kenneth Stott
# Canary: 5b9e7d24-c1a3-4f68-9e02-7d4a1c8b3f65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Package registries behind a proxy such as Artifactory, or a Git server such as GitLab or
Bitbucket (REQ-2088).

- An npm registry's ``user:password`` credential is npm's ``_auth`` and is sent as Basic; a token
  stays ``_authToken`` and Bearer.
- A Git host's ``user:token`` credential is the user and token of both the rewritten URL and the
  fetch; a bare token is sent as ``x-access-token``. A Git host entry may carry a port and a path
  prefix, and a repository URL is matched to the entry with the longest prefix that covers it.
- An entry's certificate authority is trusted beside the publicly trusted ones: by the direct
  fetches through it, verified here against a local HTTPS server whose certificate only that
  authority signs; by uv (SSL_CERT_FILE), npm (cafile), Deno (DENO_CERT) and Git (GIT_SSL_CAINFO,
  and the fetch's own ``http.sslCAInfo``), each reading one file in the install's own temporary
  directory, gone when the install is."""

from __future__ import annotations

import base64
import datetime
import hashlib
import http.server
import io
import ipaddress
import json
import ssl
import tarfile
import threading
from pathlib import Path

import certifi
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from provisa.codesource import install as shared
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import GitCode, git_host_for, gitconfig
from provisa.core.package_registries import Resolved
from provisa.node import install as node
from provisa.node.install import NpmCode
from provisa.python import install as python

pytestmark = pytest.mark.unit

TOKEN = "made-up-npm-token"
USER_PASSWORD = "ada:made-up-api-key"


# -- a certificate authority of the proxy's own, and a server only it vouches for -----------------


def _authority() -> tuple[str, bytes]:
    """A self-signed CA's PEM, and a localhost server certificate with its key that it signs."""
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Acme Proxy CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(1)
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(ca_name)
        .public_key(key.public_key())
        .serial_number(2)
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=2))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    chain = leaf.public_bytes(serialization.Encoding.PEM) + key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return ca.public_bytes(serialization.Encoding.PEM).decode(), chain


def _tgz(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for name, data in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class _Proxy:
    """An npm registry on 127.0.0.1 behind TLS that only its own CA vouches for, answering one
    package to a request carrying the credential it expects."""

    def __init__(self, tmp_path: Path, authorization: str) -> None:
        self.ca, chain = _authority()
        (tmp_path / "server.pem").write_bytes(chain)
        self.tarball = _tgz({"package/index.js": b"export default 1;\n"})
        self.seen: list[tuple[str, str | None]] = []
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 -- http.server's name
                proxy.seen.append((self.path, self.headers.get("Authorization")))
                if self.headers.get("Authorization") != authorization:
                    self.send_response(401)
                    self.end_headers()
                    return
                if self.path.endswith(".tgz"):
                    body = proxy.tarball
                else:
                    integrity = (
                        "sha512-"
                        + base64.b64encode(hashlib.sha512(proxy.tarball).digest()).decode()
                    )
                    body = json.dumps(
                        {
                            "versions": {
                                "1.0.0": {
                                    "dist": {
                                        "tarball": f"{proxy.url}rows/-/rows-1.0.0.tgz",
                                        "integrity": integrity,
                                    }
                                }
                            }
                        }
                    ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 -- the base's name
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(tmp_path / "server.pem"))
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.url = f"https://localhost:{self.server.server_address[1]}/"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def basic_proxy(tmp_path):
    proxy = _Proxy(tmp_path, "Basic " + base64.b64encode(USER_PASSWORD.encode()).decode())
    yield proxy
    proxy.close()


# -- npm credentials --------------------------------------------------------------------------------


def test_an_npm_user_and_password_is_npms_auth_and_a_token_stays_its_auth_token():
    artifactory = Resolved(
        "r1", "npm", "https://art.acme.test/api/npm/npm/", None, USER_PASSWORD, True
    )
    github = Resolved("r2", "npm", "https://npm.pkg.github.com/", "@acme", TOKEN, False)
    rc = node.npmrc([artifactory, github])
    expected = base64.b64encode(USER_PASSWORD.encode()).decode()
    assert f"//art.acme.test/api/npm/npm/:_auth={expected}" in rc
    assert f"//npm.pkg.github.com/:_authToken={TOKEN}" in rc
    assert "_authToken=ada" not in rc
    assert node.npm_authorization(USER_PASSWORD) == f"Basic {expected}"
    assert node.npm_authorization(TOKEN) == f"Bearer {TOKEN}"


@pytest.mark.parametrize(
    ("credential", "sent"),
    [
        (USER_PASSWORD, "Basic " + base64.b64encode(USER_PASSWORD.encode()).decode()),
        (TOKEN, f"Bearer {TOKEN}"),
    ],
)
def test_the_package_is_fetched_with_basic_for_a_user_and_bearer_for_a_token(
    tmp_path, credential, sent
):
    tarball = _tgz({"package/index.js": b"1"})
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(tarball).digest()).decode()
    asked: list[tuple[str, dict]] = []

    def http(url, headers):
        asked.append((url, headers))
        if url.endswith(".tgz"):
            return tarball
        tar_url = "https://art.acme.test/rows/-/rows-1.0.0.tgz"
        return json.dumps(
            {"versions": {"1.0.0": {"dist": {"tarball": tar_url, "integrity": integrity}}}}
        ).encode()

    mirror = Resolved("r1", "npm", "https://art.acme.test/", None, credential, True)
    node.fetch_npm(NpmCode("rows", "1.0.0"), [mirror], http, tmp_path / "out")
    assert [h["Authorization"] for _, h in asked] == [sent, sent]


# -- the direct fetches verify against the registry's own authority ---------------------------------


def test_a_direct_fetch_through_a_registry_verifies_against_its_authority(tmp_path, basic_proxy):
    without = Resolved("r1", "npm", basic_proxy.url, None, USER_PASSWORD, True)
    with pytest.raises(BundleRefused) as refused:
        node.fetch_npm(NpmCode("rows", "1.0.0"), [without], shared.http_get, tmp_path / "a")
    assert refused.value.code == "codesource.fetch_failed"
    assert "CERTIFICATE_VERIFY_FAILED" in str(refused.value)

    trusted = Resolved("r1", "npm", basic_proxy.url, None, USER_PASSWORD, True, basic_proxy.ca)
    root = node.fetch_npm(NpmCode("rows", "1.0.0"), [trusted], shared.http_get, tmp_path / "b")
    assert (root / "index.js").read_text() == "export default 1;\n"
    # Metadata and tarball each came with the credential, as Basic.
    assert [auth for _, auth in basic_proxy.seen[-2:]] == [
        "Basic " + base64.b64encode(USER_PASSWORD.encode()).decode()
    ] * 2


def test_the_authority_is_trusted_beside_the_publicly_trusted_ones():
    ca, _ = _authority()
    context = shared.trust_context(ca)
    trusted = {c["subject"] for c in context.get_ca_certs()}
    assert ((("commonName", "Acme Proxy CA"),),) in trusted
    assert len(trusted) > 100  # certifi's authorities are still there


def test_an_authority_that_is_not_a_certificate_is_refused_by_name(tmp_path):
    bad = "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"
    with pytest.raises(BundleRefused) as refused:
        shared.trust_bundle([bad], tmp_path)
    assert refused.value.code == "codesource.ca_bundle_unreadable"


# -- the installs read one trust file, in their own temporary directory -----------------------------


class _Runner:
    """Records the environment and the trust file and configuration as they were while it ran."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, argv, cwd: Path, env: dict[str, str]) -> None:
        seen: dict[str, object] = {"env": dict(env)}
        for name in ("SSL_CERT_FILE", "GIT_SSL_CAINFO", "DENO_CERT"):
            if name in env:
                seen[name] = Path(env[name]).read_text()
        if "NPM_CONFIG_USERCONFIG" in env:
            seen["npmrc"] = Path(env["NPM_CONFIG_USERCONFIG"]).read_text()
        self.calls.append(seen)


def _bundle_with_dependencies(root: Path) -> None:
    root.mkdir()
    (root / "package.json").write_text(json.dumps({"dependencies": {"left-pad": "1.3.0"}}))
    (root / "bun.lock").write_text("{}")


def test_npm_bun_deno_and_git_read_the_registries_authority_from_the_installs_own_file(tmp_path):
    ca, _ = _authority()
    registries = [
        Resolved("r1", "npm", "https://art.acme.test/api/npm/npm/", None, USER_PASSWORD, True, ca),
        Resolved("g1", "git", "gitlab.acme.test", None, "ada:glpat-made-up", False),
    ]
    root = tmp_path / "code"
    _bundle_with_dependencies(root)
    runner = _Runner()
    node._install(root, "bun", tmp_path / "bun", registries, runner)
    (call,) = runner.calls
    cafile = call["env"]["DENO_CERT"]
    assert f"cafile={cafile}" in call["npmrc"]
    assert call["env"]["GIT_SSL_CAINFO"] == cafile
    held = call["DENO_CERT"]
    assert ca.strip() in held
    assert Path(certifi.where()).read_text().strip()[:200] in held  # the public ones too
    # Written only in the install's own temporary directory, and gone with it.
    assert Path(cafile).parent == Path(call["env"]["HOME"])
    assert not Path(cafile).exists()
    assert not (root / ".npmrc").exists()


def test_an_install_through_registries_with_no_authority_names_no_trust_file(tmp_path):
    registries = [Resolved("r1", "npm", "https://npm.acme.test/", None, TOKEN, True)]
    root = tmp_path / "code"
    _bundle_with_dependencies(root)
    runner = _Runner()
    node._install(root, "bun", tmp_path / "bun", registries, runner)
    (call,) = runner.calls
    assert "cafile" not in call["npmrc"]
    assert not {"DENO_CERT", "GIT_SSL_CAINFO"} & set(call["env"])


def test_uv_and_git_read_the_indexes_authority_from_the_installs_own_file(tmp_path):
    ca, _ = _authority()
    index = Resolved(
        "i1", "pypi", "https://art.acme.test/api/pypi/pypi/simple/", None, USER_PASSWORD, True, ca
    )
    root = tmp_path / "code"
    root.mkdir()
    (root / "requirements.txt").write_text("httpx==0.27.0 --hash=sha256:aa\n")
    runner = _Runner()
    python._install(root, Path("/rt/uv"), Path("/rt/python3.12"), [index], runner)
    venv, sync = runner.calls
    for call in (venv, sync):
        ca_file = call["env"]["SSL_CERT_FILE"]
        assert call["env"]["GIT_SSL_CAINFO"] == ca_file
        assert ca.strip() in call["SSL_CERT_FILE"]
        assert Path(ca_file).parent == Path(call["env"]["HOME"])
        assert not Path(ca_file).exists()
    assert sync["env"]["UV_DEFAULT_INDEX"] == (
        "https://ada:made-up-api-key@art.acme.test/api/pypi/pypi/simple/"
    )


def test_a_wheel_is_fetched_through_the_indexes_authority(monkeypatch):
    ca, _ = _authority()
    index = Resolved("i1", "pypi", "https://art.acme.test/simple/", None, TOKEN, True, ca)
    asked: list[str | None] = []

    def http(url, headers, ca_bundle=None):
        asked.append(ca_bundle)
        raise BundleRefused("fetch_failed", "stop here")

    with pytest.raises(BundleRefused):
        python.fetch_wheel(python.PypiCode("rows", "1.0.0"), [index], http, Path("/nowhere"))
    assert asked == [ca]


# -- Git hosts: user:token, a port and a path prefix ------------------------------------------------

GITHUB = Resolved("g1", "git", "github.com", None, "ghp_made-up", False)
BITBUCKET_DC = Resolved(
    "g2", "git", "bitbucket.acme.test:7990/bitbucket", None, "ada:made-up-http-token", False
)
BITBUCKET_PROJECT = Resolved(
    "g3",
    "git",
    "bitbucket.acme.test:7990/bitbucket/scm/data",
    None,
    "x-token-auth:made-up-project-token",
    False,
)
GITLAB = Resolved("g4", "git", "https://gitlab.acme.test/", None, "ada:glpat-made-up", False)


@pytest.mark.parametrize(
    ("url", "entry"),
    [
        ("https://github.com/acme/rows.git", GITHUB),
        ("https://bitbucket.acme.test:7990/bitbucket/scm/ops/rows.git", BITBUCKET_DC),
        ("https://bitbucket.acme.test:7990/bitbucket/scm/data/rows.git", BITBUCKET_PROJECT),
        ("https://gitlab.acme.test/acme/rows.git", GITLAB),
        ("https://bitbucket.acme.test/bitbucket/scm/ops/rows.git", None),  # another port
        ("https://bitbucket.acme.test:7990/bitbucketx/rows.git", None),  # not a path prefix
        ("https://github.com.evil.test/acme/rows.git", None),
    ],
)
def test_a_repository_is_matched_to_the_longest_prefix_that_covers_it(url, entry):
    registries = [GITHUB, BITBUCKET_DC, BITBUCKET_PROJECT, GITLAB]
    assert git_host_for(url, registries) == entry


def test_a_user_and_token_and_a_prefix_are_written_into_the_git_configuration():
    config = gitconfig([GITHUB, BITBUCKET_DC, GITLAB])
    assert (
        '[url "https://x-access-token:ghp_made-up@github.com/"]\n\tinsteadOf = https://github.com/'
        in config
    )
    assert (
        '[url "https://ada:made-up-http-token@bitbucket.acme.test:7990/bitbucket/"]\n'
        "\tinsteadOf = https://bitbucket.acme.test:7990/bitbucket/"
    ) in config
    assert '[url "https://ada:glpat-made-up@gitlab.acme.test/"]' in config


def test_the_fetch_sends_the_user_and_token_and_trusts_the_hosts_authority(tmp_path, monkeypatch):
    from dulwich import porcelain

    ca, _ = _authority()
    host = Resolved(
        "g2", "git", "bitbucket.acme.test:7990/bitbucket", None, "ada:made-up-http-token", False, ca
    )
    seen: dict = {}

    def clone(source, target, **options):
        seen.update(options)
        config = options.get("config")
        if config is not None:
            ca_file = config.get((b"http",), b"sslCAInfo").decode()
            seen["ca_file"] = ca_file
            seen["trusted"] = Path(ca_file).read_text()
        raise OSError("stop here")

    monkeypatch.setattr(porcelain, "clone", clone)
    code = GitCode("https://bitbucket.acme.test:7990/bitbucket/scm/ops/rows.git", "a" * 40, "")
    with pytest.raises(BundleRefused):
        shared.fetch_git(code, [GITHUB, host], tmp_path / "out")
    assert (seen["username"], seen["password"]) == ("ada", "made-up-http-token")
    assert ca.strip() in seen["trusted"]
    assert not Path(seen["ca_file"]).exists()  # the fetch's own, gone with it


def test_a_bare_git_token_is_sent_as_x_access_token(tmp_path, monkeypatch):
    from dulwich import porcelain

    seen: dict = {}

    def clone(source, target, **options):
        seen.update(options)
        raise OSError("stop here")

    monkeypatch.setattr(porcelain, "clone", clone)
    with pytest.raises(BundleRefused):
        shared.fetch_git(
            GitCode("https://github.com/acme/rows.git", "a" * 40, ""), [GITHUB], tmp_path / "o"
        )
    assert (seen["username"], seen["password"]) == ("x-access-token", "ghp_made-up")
    assert "config" not in seen  # no authority of its own: git trusts what it always does
