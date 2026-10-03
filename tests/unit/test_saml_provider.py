# Copyright (c) 2026 Kenneth Stott
# Canary: 712c97c9-5bdf-4aa4-a063-68a2a6e5c790
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1265: what the SAML provider accepts and what it refuses.

The responses here are built and signed in the test with a key the test generates, so each
refusal can be provoked exactly. The sign-in against a real identity provider is
tests/integration/test_saml_auth_provider.py.
"""

from __future__ import annotations

import base64
import datetime
import zlib
from urllib.parse import parse_qs, urlparse

import jwt
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]
from signxml import DigestAlgorithm, SignatureMethod, XMLSigner
from signxml.algorithms import CanonicalizationMethod

from provisa.auth.providers.saml import (
    SamlAuthProvider,
    SamlRejected,
    SamlSettings,
    parse_idp_metadata,
)

IDP = "https://idp.example/metadata"
SSO = "https://idp.example/sso"
SP = "https://provisa.example/auth/saml/metadata"
ACS = "https://provisa.example/auth/saml/acs"
LOGIN = "https://provisa.example/login"
SECRET = "saml-session-signing-secret-of-48-bytes-or-more!"
NOW = datetime.datetime(2026, 10, 3, 12, 0, 0, tzinfo=datetime.timezone.utc)

SAML = "urn:oasis:names:tc:SAML:2.0:assertion"
SAMLP = "urn:oasis:names:tc:SAML:2.0:protocol"
DS = "http://www.w3.org/2000/09/xmldsig#"


def _keypair() -> tuple[rsa.RSAPrivateKey, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "idp.example")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOW - datetime.timedelta(days=1))
        .not_valid_after(NOW + datetime.timedelta(days=3650))
        .sign(key, hashes.SHA256())
    )
    return key, cert.public_bytes(serialization.Encoding.PEM).decode()


IDP_KEY, IDP_CERT = _keypair()
OTHER_KEY, OTHER_CERT = _keypair()


def _cert_body(pem: str) -> str:
    return "".join(pem.strip().splitlines()[1:-1])


def _metadata(cert_pem: str = IDP_CERT) -> bytes:
    return f"""<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
        xmlns:ds="{DS}" entityID="{IDP}">
      <md:IDPSSODescriptor protocolSupportEnumeration="{SAMLP}">
        <md:KeyDescriptor use="signing"><ds:KeyInfo><ds:X509Data>
          <ds:X509Certificate>{_cert_body(cert_pem)}</ds:X509Certificate>
        </ds:X509Data></ds:KeyInfo></md:KeyDescriptor>
        <md:SingleSignOnService
          Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="{SSO}"/>
      </md:IDPSSODescriptor>
    </md:EntityDescriptor>""".encode()


@pytest.fixture()
def provider(tmp_path):
    return _provider(tmp_path)


def _provider(tmp_path, now=NOW, **overrides) -> SamlAuthProvider:
    metadata = tmp_path / "idp.xml"
    metadata.write_bytes(_metadata())
    settings = SamlSettings(
        **{
            "sp_entity_id": SP,
            "acs_url": ACS,
            "login_page_url": LOGIN,
            "idp_metadata_file": str(metadata),
            "email_attribute": "email",
            "display_name_attribute": "displayName",
            "groups_attribute": "groups",
            **overrides,
        }
    )
    return SamlAuthProvider(settings, SECRET, now=lambda: now)


def _start(provider: SamlAuthProvider) -> tuple[str, str]:
    """Begin a sign-in; return (request id, RelayState) as the identity provider receives them."""
    query = parse_qs(urlparse(provider.login_redirect()).query)
    inflated = zlib.decompress(base64.b64decode(query["SAMLRequest"][0]), wbits=-15)
    return etree.fromstring(inflated).get("ID"), query["RelayState"][0]


def _ts(moment: datetime.datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _assertion_xml(
    request_id: str,
    *,
    issuer: str = IDP,
    audience: str = SP,
    recipient: str = ACS,
    name_id: str = "alice@corp.example",
    not_before: datetime.datetime = NOW - datetime.timedelta(minutes=1),
    not_on_or_after: datetime.datetime = NOW + datetime.timedelta(minutes=5),
    assertion_id: str = "_assertion1",
    groups: tuple[str, ...] = ("analysts", "stewards"),
    extra: str = "",
) -> bytes:
    group_values = "".join(f"<saml:AttributeValue>{g}</saml:AttributeValue>" for g in groups)
    return f"""<saml:Assertion xmlns:saml="{SAML}" ID="{assertion_id}" Version="2.0"
        IssueInstant="{_ts(NOW)}">
      <saml:Issuer>{issuer}</saml:Issuer>
      <saml:Subject>
        <saml:NameID>{name_id}</saml:NameID>
        <saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
          <saml:SubjectConfirmationData Recipient="{recipient}"
            InResponseTo="{request_id}" NotOnOrAfter="{_ts(not_on_or_after)}"/>
        </saml:SubjectConfirmation>
      </saml:Subject>
      <saml:Conditions NotBefore="{_ts(not_before)}" NotOnOrAfter="{_ts(not_on_or_after)}">
        <saml:AudienceRestriction><saml:Audience>{audience}</saml:Audience>
        </saml:AudienceRestriction>
      </saml:Conditions>
      <saml:AuthnStatement AuthnInstant="{_ts(NOW)}"/>
      <saml:AttributeStatement>
        <saml:Attribute Name="email">
          <saml:AttributeValue>alice@corp.example</saml:AttributeValue></saml:Attribute>
        <saml:Attribute Name="displayName">
          <saml:AttributeValue>Alice Analyst</saml:AttributeValue></saml:Attribute>
        <saml:Attribute Name="groups">{group_values}</saml:Attribute>
      </saml:AttributeStatement>{extra}
    </saml:Assertion>""".encode()


def _response_xml(
    request_id: str,
    assertions: bytes,
    *,
    destination: str = ACS,
    status: str = "urn:oasis:names:tc:SAML:2.0:status:Success",
) -> bytes:
    """A Response around already-serialized assertions.

    Assembled as text, never by moving nodes between trees: lxml renames namespace prefixes
    when a node changes document, and a signature does not survive that.
    """
    return (
        f"""<samlp:Response xmlns:samlp="{SAMLP}" xmlns:saml="{SAML}" ID="_response1"
            Version="2.0" IssueInstant="{_ts(NOW)}" Destination="{destination}"
            InResponseTo="{request_id}"><saml:Issuer>{IDP}</saml:Issuer>
          <samlp:Status><samlp:StatusCode Value="{status}"/></samlp:Status>""".encode()
        + assertions
        + b"</samlp:Response>"
    )


def _sign(
    document: bytes,
    key=IDP_KEY,
    cert: str = IDP_CERT,
) -> bytes:
    """Sign the document's root element, the signature placed after its Issuer."""
    element = etree.fromstring(document)
    placeholder = etree.Element(f"{{{DS}}}Signature", nsmap={"ds": DS}, Id="placeholder")
    element.find(f"{{{SAML}}}Issuer").addnext(placeholder)
    signer = XMLSigner(
        signature_algorithm=SignatureMethod.RSA_SHA256,
        digest_algorithm=DigestAlgorithm.SHA256,
        c14n_algorithm=CanonicalizationMethod.EXCLUSIVE_XML_CANONICALIZATION_1_0,
    )
    return etree.tostring(signer.sign(element, key=key, cert=cert, reference_uri=element.get("ID")))


_RESPONSE_KW = ("destination", "status")


def _signed_assertion_response(request_id: str, key=IDP_KEY, cert=IDP_CERT, **kw) -> bytes:
    """A Response whose assertion (not the Response) is signed."""
    response_kw = {k: kw.pop(k) for k in _RESPONSE_KW if k in kw}
    signed = _sign(_assertion_xml(request_id, **kw), key=key, cert=cert)
    return _response_xml(request_id, signed, **response_kw)


def _signed_response(request_id: str, **kw) -> bytes:
    """A Response that is itself signed, around an unsigned assertion."""
    response_kw = {k: kw.pop(k) for k in _RESPONSE_KW if k in kw}
    return _sign(_response_xml(request_id, _assertion_xml(request_id, **kw), **response_kw))


def _post(document: bytes) -> str:
    return base64.b64encode(document).decode()


def _edited(document: bytes, edit) -> bytes:
    """The document after ``edit`` changes it in place (text and attributes; no moved nodes)."""
    root = etree.fromstring(document)
    edit(root)
    return etree.tostring(root)


class TestAcceptedSignIn:
    async def test_a_signed_assertion_becomes_a_session(self, provider):
        request_id, relay = _start(provider)
        token = provider.consume_response(_post(_signed_assertion_response(request_id)), relay)

        identity = await provider.validate_token(token)
        assert identity.user_id == "alice@corp.example"
        assert identity.email == "alice@corp.example"
        assert identity.display_name == "Alice Analyst"
        assert identity.roles == ["analysts", "stewards"]
        assert identity.raw_claims["groups"] == ["analysts", "stewards"]

    async def test_a_signed_response_is_accepted_too(self, provider):
        request_id, relay = _start(provider)
        token = provider.consume_response(_post(_signed_response(request_id)), relay)
        assert (await provider.validate_token(token)).user_id == "alice@corp.example"

    async def test_the_user_id_can_come_from_an_attribute(self, tmp_path):
        provider = _provider(tmp_path, user_id_attribute="email")
        request_id, relay = _start(provider)
        token = provider.consume_response(
            _post(_signed_assertion_response(request_id, name_id="transient-123")), relay
        )
        identity = await provider.validate_token(token)
        assert identity.user_id == "alice@corp.example"
        assert identity.raw_claims["name_id"] == "transient-123"

    def test_the_request_goes_to_the_identity_providers_sign_on_url(self, provider):
        url = provider.login_redirect()
        assert url.startswith(SSO + "?")
        query = parse_qs(urlparse(url).query)
        request = etree.fromstring(
            zlib.decompress(base64.b64decode(query["SAMLRequest"][0]), wbits=-15)
        )
        assert request.get("AssertionConsumerServiceURL") == ACS
        assert request.get("Destination") == SSO
        assert request.findtext(f"{{{SAML}}}Issuer") == SP
        assert len(query["RelayState"][0].encode()) <= 80  # the binding's limit


class TestRefusedSignatures:
    def _refused(self, provider, element, relay, match):
        with pytest.raises(SamlRejected, match=match):
            provider.consume_response(_post(element), relay)

    def test_an_unsigned_response(self, provider):
        request_id, relay = _start(provider)
        unsigned = _response_xml(request_id, _assertion_xml(request_id))
        self._refused(provider, unsigned, relay, "neither .* signed")

    def test_a_signature_by_another_key(self, provider):
        request_id, relay = _start(provider)
        forged = _signed_assertion_response(request_id, key=OTHER_KEY, cert=OTHER_CERT)
        self._refused(provider, forged, relay, "signature not valid")

    def test_content_changed_after_signing(self, provider):
        request_id, relay = _start(provider)

        def rename(root):
            root.find(f".//{{{SAML}}}NameID").text = "mallory@corp.example"

        changed = _edited(_signed_assertion_response(request_id), rename)
        self._refused(provider, changed, relay, "signature not valid")

    def test_a_group_added_after_signing(self, provider):
        request_id, relay = _start(provider)

        def promote(root):
            groups = root.find(f".//{{{SAML}}}Attribute[@Name='groups']")
            etree.SubElement(groups, f"{{{SAML}}}AttributeValue").text = "org_admin"

        changed = _edited(_signed_assertion_response(request_id, groups=("analysts",)), promote)
        self._refused(provider, changed, relay, "signature not valid")

    def test_a_forged_assertion_beside_the_signed_one(self, provider):
        request_id, relay = _start(provider)
        forged = _assertion_xml(request_id, name_id="mallory@corp.example", assertion_id="_forged")
        genuine = _sign(_assertion_xml(request_id))
        response = _response_xml(request_id, forged + genuine)
        self._refused(provider, response, relay, "expected one assertion, found 2")

    def test_a_signed_assertion_wrapped_inside_a_forged_one(self, provider):
        """The forged assertion carries the genuine one, and a copy of its signature."""
        request_id, relay = _start(provider)
        genuine = _sign(_assertion_xml(request_id))
        signature = etree.tostring(
            etree.fromstring(genuine).find(f"{{{DS}}}Signature"), with_tail=False
        )
        forged = _assertion_xml(
            request_id,
            name_id="mallory@corp.example",
            assertion_id="_forged",
            extra=f"<saml:Advice>{genuine.decode()}</saml:Advice>",
        ).replace(b"</saml:Issuer>", b"</saml:Issuer>" + signature, 1)
        with pytest.raises(SamlRejected):
            provider.consume_response(_post(_response_xml(request_id, forged)), relay)

    def test_sha1_is_not_an_accepted_algorithm(self):
        """The signing library cannot produce a SHA-1 signature to post, so this checks the
        accepted sets the verifier is given rather than a refused document."""
        from provisa.auth.providers import saml

        accepted = {m.name for m in saml._SIGNATURE_METHODS | saml._DIGEST_ALGORITHMS}
        assert not [name for name in accepted if "SHA1" in name]
        assert not [name for name in accepted if name.startswith(("HMAC", "DSA"))]

    def test_a_comment_cannot_shorten_the_name(self, provider):
        """The signed NameID is read whole: text after a comment is not dropped."""
        request_id, relay = _start(provider)
        posted = _signed_assertion_response(
            request_id, name_id="alice@corp.example<!--x-->.evil.example"
        )
        token = provider.consume_response(_post(posted), relay)
        claims = jwt.decode(token, SECRET, algorithms=["HS256"], audience="saml")
        assert claims["sub"] == "alice@corp.example.evil.example"


class TestRefusedConditions:
    def _refused(self, provider, match, **response_kw):
        request_id, relay = _start(provider)
        with pytest.raises(SamlRejected, match=match):
            provider.consume_response(
                _post(_signed_assertion_response(request_id, **response_kw)), relay
            )

    def test_another_issuer(self, provider):
        self._refused(provider, "not issued by", issuer="https://other-idp.example/metadata")

    def test_another_audience(self, provider):
        self._refused(provider, "not this service", audience="https://other-sp.example")

    def test_another_recipient(self, provider):
        self._refused(provider, "no bearer confirmation", recipient="https://evil.example/acs")

    def test_an_expired_assertion(self, provider):
        self._refused(provider, "expired", not_on_or_after=NOW - datetime.timedelta(minutes=2))

    def test_an_assertion_from_the_future(self, provider):
        self._refused(provider, "not yet valid", not_before=NOW + datetime.timedelta(minutes=5))

    def test_a_failed_status(self, provider):
        self._refused(provider, "reported", status="urn:oasis:names:tc:SAML:2.0:status:Responder")

    def test_a_signed_response_addressed_elsewhere(self, provider):
        request_id, relay = _start(provider)
        response = _signed_response(request_id, destination="https://evil.example/acs")
        with pytest.raises(SamlRejected, match="addressed to"):
            provider.consume_response(_post(response), relay)


class TestRequestBinding:
    def test_a_response_to_no_request_is_refused(self, provider):
        _request_id, relay = _start(provider)
        with pytest.raises(SamlRejected, match="does not answer a request"):
            provider.consume_response(_post(_signed_assertion_response("_never-issued")), relay)

    def test_a_missing_relay_state_is_refused(self, provider):
        request_id, _relay = _start(provider)
        with pytest.raises(SamlRejected, match="RelayState"):
            provider.consume_response(_post(_signed_assertion_response(request_id)), "")

    def test_an_altered_expiry_in_relay_state_is_refused(self, provider):
        request_id, relay = _start(provider)
        expires, _, mac = relay.partition(".")
        with pytest.raises(SamlRejected, match="does not answer a request"):
            provider.consume_response(
                _post(_signed_assertion_response(request_id)), f"{int(expires) + 3600}.{mac}"
            )

    def test_a_request_that_took_too_long_is_refused(self, tmp_path):
        early = _provider(tmp_path, now=NOW - datetime.timedelta(minutes=11))
        request_id, relay = _start(early)
        late = _provider(tmp_path, now=NOW)
        with pytest.raises(SamlRejected, match="request has expired"):
            late.consume_response(_post(_signed_assertion_response(request_id)), relay)

    def test_the_same_assertion_is_refused_a_second_time(self, provider):
        request_id, relay = _start(provider)
        posted = _post(_signed_assertion_response(request_id))
        provider.consume_response(posted, relay)
        with pytest.raises(SamlRejected, match="already been used"):
            provider.consume_response(posted, relay)


class TestRefusedDocuments:
    def test_not_base64(self, provider):
        with pytest.raises(SamlRejected, match="base64"):
            provider.consume_response("<<<not base64>>>", "x.y")

    def test_not_xml(self, provider):
        with pytest.raises(SamlRejected, match="well-formed"):
            provider.consume_response(base64.b64encode(b"not xml").decode(), "x.y")

    def test_a_doctype(self, provider):
        document = b'<!DOCTYPE r [<!ENTITY e "x">]><r>&e;</r>'
        with pytest.raises(SamlRejected, match="DOCTYPE"):
            provider.consume_response(base64.b64encode(document).decode(), "x.y")

    def test_an_encrypted_assertion(self, provider):
        document = (
            f'<samlp:Response xmlns:samlp="{SAMLP}" xmlns:saml="{SAML}">'
            "<saml:EncryptedAssertion/></samlp:Response>"
        ).encode()
        with pytest.raises(SamlRejected, match="encrypted"):
            provider.consume_response(base64.b64encode(document).decode(), "x.y")

    def test_too_large(self, provider):
        with pytest.raises(SamlRejected, match="too large"):
            provider.consume_response("A" * 1_000_001, "x.y")


class TestSessionToken:
    async def test_a_token_signed_with_another_key_is_refused(self, provider):
        forged = jwt.encode(
            {"sub": "mallory", "aud": "saml", "exp": NOW.timestamp() + 10**9},
            "another-signing-secret-of-48-bytes-or-more!!!!!!",
            algorithm="HS256",
        )
        with pytest.raises(jwt.InvalidSignatureError):
            await provider.validate_token(forged)

    async def test_a_token_minted_for_another_provider_is_refused(self, provider):
        other = jwt.encode(
            {"sub": "alice", "aud": "ldap", "exp": NOW.timestamp() + 10**9},
            SECRET,
            algorithm="HS256",
        )
        with pytest.raises(jwt.InvalidAudienceError):
            await provider.validate_token(other)


class TestConfiguration:
    def test_a_signing_secret_is_required(self, tmp_path):
        metadata = tmp_path / "idp.xml"
        metadata.write_bytes(_metadata())
        settings = SamlSettings(
            sp_entity_id=SP, acs_url=ACS, login_page_url=LOGIN, idp_metadata_file=str(metadata)
        )
        with pytest.raises(ValueError, match="jwt_secret"):
            SamlAuthProvider(settings, "")

    @pytest.mark.parametrize(
        "sources",
        [{}, {"idp_metadata_url": "https://idp.example/md", "idp_metadata_file": "/x.xml"}],
    )
    def test_exactly_one_metadata_source(self, sources):
        with pytest.raises(ValueError, match="exactly one"):
            SamlSettings(sp_entity_id=SP, acs_url=ACS, login_page_url=LOGIN, **sources)

    def test_metadata_without_a_signing_certificate_is_refused(self):
        document = _metadata().replace(b'use="signing"', b'use="encryption"')
        with pytest.raises(ValueError, match="signing certificate"):
            parse_idp_metadata(document)

    def test_sp_metadata_names_the_acs(self, provider):
        root = etree.fromstring(provider.sp_metadata())
        assert root.get("entityID") == SP
        acs = root.find(".//{urn:oasis:names:tc:SAML:2.0:metadata}AssertionConsumerService")
        assert acs.get("Location") == ACS


class TestRoutesServeTheBoundProvider:
    def _bind(self, monkeypatch, tmp_path, provider_name="saml", generation=7):
        from provisa.api.app import state

        metadata = tmp_path / "idp.xml"
        metadata.write_bytes(_metadata())
        monkeypatch.setattr(
            state,
            "auth_config",
            {
                "provider": provider_name,
                "jwt_secret": SECRET,
                "saml": {
                    "sp_entity_id": SP,
                    "acs_url": ACS,
                    "login_page_url": LOGIN,
                    "idp_metadata_file": str(metadata),
                },
            },
        )
        monkeypatch.setattr(state, "auth_reconfig_generation", generation)
        return state

    def test_a_deployment_not_using_saml_answers_404(self, monkeypatch, tmp_path):
        from provisa.api.errors import ApiError
        from provisa.auth.providers.saml import _require_provider

        self._bind(monkeypatch, tmp_path, provider_name="ldap")
        with pytest.raises(ApiError) as refused:
            _require_provider()
        assert refused.value.status_code == 404

    def test_one_provider_per_generation_so_a_replay_is_remembered(self, monkeypatch, tmp_path):
        from provisa.auth.providers.saml import _require_provider

        state = self._bind(monkeypatch, tmp_path)
        first = _require_provider()
        assert _require_provider() is first
        monkeypatch.setattr(state, "auth_reconfig_generation", 8)
        assert _require_provider() is not first
