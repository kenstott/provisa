# Copyright (c) 2026 Kenneth Stott
# Canary: 31b7b390-1c57-48dc-aaa4-4b8cf4d1511c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SAML 2.0 service-provider auth (REQ-1265). Web Browser SSO, SP-initiated.

1. ``GET /auth/saml/login`` redirects the browser to the identity provider with an
   AuthnRequest (HTTP-Redirect binding).
2. The identity provider signs the user in and posts a signed Response to
   ``POST /auth/saml/acs`` (HTTP-POST binding).
3. The assertion is verified and the browser is sent to the login page with a session token in
   the URL fragment. The fragment is never sent to a server.
4. ``GET /auth/saml/metadata`` describes this service provider, for registering it at the
   identity provider.

What is verified, and in this order:

* The XML signature, against the signing certificates in the identity provider's metadata. A
  signature is accepted on the Response or on the Assertion. Claims are read only from the
  element the signature covers, never from the document as posted.
* The assertion's Issuer is the identity provider; its Audience is this service provider; its
  bearer SubjectConfirmation names this service provider's ACS URL and has not expired; its
  Conditions hold now.
* The assertion answers a request this deployment issued: the request id comes back in the
  signed ``InResponseTo`` and RelayState carries an expiry and an HMAC over that id.

Refused outright: an unsolicited (IdP-initiated) response, an encrypted assertion, a document
with a DOCTYPE, more than one assertion, a SHA-1 signature.

Limits, by design and stated in the operator documentation:

* An assertion is refused a second time only by the process that saw it first. With several
  API replicas, a captured response can be presented once per replica until it expires.
* A session is not re-checked with the identity provider. A user removed there keeps access
  until the session token expires.
* pgwire and Bolt carry a username and a password, which SAML has no use for. Under this
  provider they accept personal access tokens only.
"""

from __future__ import annotations

import base64
import binascii
import datetime
import hashlib
import hmac
import logging
import secrets
import threading
import time
import zlib
from dataclasses import dataclass
from typing import Callable
from urllib.parse import quote, urlencode

import httpx
from fastapi import APIRouter, Form
from fastapi.responses import RedirectResponse, Response
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]  # lxml ships no stubs for its C module
from signxml import DigestAlgorithm, SignatureMethod, XMLVerifier
from signxml.exceptions import SignXMLException
from signxml.verifier import SignatureConfiguration

from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.auth.session_token import SessionTokens

# Requirements: REQ-1265

logger = logging.getLogger(__name__)

_NS = {
    "samlp": "urn:oasis:names:tc:SAML:2.0:protocol",
    "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
    "md": "urn:oasis:names:tc:SAML:2.0:metadata",
    "ds": "http://www.w3.org/2000/09/xmldsig#",
}
_RESPONSE = f"{{{_NS['samlp']}}}Response"
_ASSERTION = f"{{{_NS['saml']}}}Assertion"
_STATUS_SUCCESS = "urn:oasis:names:tc:SAML:2.0:status:Success"
_BEARER = "urn:oasis:names:tc:SAML:2.0:cm:bearer"
_BINDING_REDIRECT = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
_BINDING_POST = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"

# How long a sign-in may take between the redirect to the identity provider and its response.
_REQUEST_TTL_SECONDS = 600
# Tolerated difference between this host's clock and the identity provider's.
_CLOCK_SKEW = datetime.timedelta(seconds=60)
# The identity provider's metadata is re-read after this long, which is how a rotated signing
# certificate is picked up.
_METADATA_TTL_SECONDS = 3600.0
_METADATA_TIMEOUT_SECONDS = 10
# A posted SAMLResponse larger than this (base64) is refused before it is parsed.
_MAX_RESPONSE_BYTES = 1_000_000

# SHA-1 is not accepted for the signature or for a digest.
_SIGNATURE_METHODS = frozenset(
    {
        SignatureMethod.RSA_SHA256,
        SignatureMethod.RSA_SHA384,
        SignatureMethod.RSA_SHA512,
        SignatureMethod.ECDSA_SHA256,
        SignatureMethod.ECDSA_SHA384,
        SignatureMethod.ECDSA_SHA512,
    }
)
_DIGEST_ALGORITHMS = frozenset(
    {DigestAlgorithm.SHA256, DigestAlgorithm.SHA384, DigestAlgorithm.SHA512}
)

router = APIRouter(prefix="/auth/saml", tags=["auth"])

# The provider the routes serve, with the auth generation it was built for. One instance per
# generation, not one per request: it holds the replay record and the IdP metadata cache, which a
# fresh instance per request would discard (state.auth_reconfig_generation, REQ-1267).
_bound: "tuple[int, SamlAuthProvider] | None" = None
_bound_lock = threading.Lock()


class SamlRejected(ValueError):
    """The posted response is not an acceptable sign-in. The message is for the server log."""


def _require_provider() -> "SamlAuthProvider":
    """The configured SAML provider, or a 404 where the deployment does not sign in by SAML."""
    global _bound
    from provisa.api.app import state

    auth_config = state.auth_config
    if auth_config is None or auth_config.get("provider") != "saml":
        raise ApiError(404, "auth.saml_not_configured", "This deployment does not use SAML")
    generation = state.auth_reconfig_generation
    with _bound_lock:
        if _bound is None or _bound[0] != generation:
            from provisa.auth.wiring import build_auth_provider

            provider = build_auth_provider(auth_config)
            assert isinstance(provider, SamlAuthProvider)
            _bound = (generation, provider)
        return _bound[1]


@router.get("/login")
async def saml_login():  # REQ-1265
    """Send the browser to the identity provider."""
    return RedirectResponse(_require_provider().login_redirect(), status_code=302)


@router.post("/acs")
async def saml_acs(  # REQ-1265
    SAMLResponse: str = Form(...),  # noqa: N803 - the form field name the SAML binding defines
    RelayState: str = Form(""),  # noqa: N803
):
    """Receive the identity provider's response and start the browser session."""
    provider = _require_provider()
    try:
        token = provider.consume_response(SAMLResponse, RelayState)
    except SamlRejected as refused:
        # The browser learns only that sign-in failed; the reason is the operator's to read.
        logger.warning("SAML response refused: %s", refused)
        return RedirectResponse(f"{provider.login_page_url}#saml_error=rejected", status_code=303)
    return RedirectResponse(
        f"{provider.login_page_url}#saml_token={quote(token, safe='')}", status_code=303
    )


@router.get("/metadata")
async def saml_metadata():  # REQ-1265
    """This service provider's metadata, for registration at the identity provider."""
    return Response(_require_provider().sp_metadata(), media_type="application/samlmetadata+xml")


@dataclass(frozen=True)
class SamlSettings:  # REQ-1265
    """The ``auth.saml`` block.

    idp_metadata_url / idp_metadata_file: where the identity provider's metadata is read from.
        Exactly one is set. It supplies the IdP's entity id, sign-on URL and signing certificates.
    sp_entity_id: this service provider's name at the identity provider.
    acs_url: the public URL of ``/auth/saml/acs``.
    login_page_url: the public URL of the sign-in page, where the browser returns.
    user_id_attribute: the assertion attribute holding the stable user id. Unset means the
        assertion's NameID is the user id.
    email_attribute / display_name_attribute / groups_attribute: read when set. Group values
        become the identity's roles.
    """

    sp_entity_id: str
    acs_url: str
    login_page_url: str
    idp_metadata_url: str | None = None
    idp_metadata_file: str | None = None
    user_id_attribute: str | None = None
    email_attribute: str | None = None
    display_name_attribute: str | None = None
    groups_attribute: str | None = None

    def __post_init__(self) -> None:
        if bool(self.idp_metadata_url) == bool(self.idp_metadata_file):
            raise ValueError(
                "auth.saml takes exactly one of idp_metadata_url and idp_metadata_file"
            )


@dataclass(frozen=True)
class _IdpMetadata:
    entity_id: str
    sso_url: str
    certificates: tuple[str, ...]


def _parser() -> etree.XMLParser:
    # No entity expansion, no DTD, no network. Comments are dropped at parse: they are outside
    # the canonical form a signature covers, and an element's text must not be readable as
    # something shorter than what was signed.
    return etree.XMLParser(
        resolve_entities=False,
        no_network=True,
        load_dtd=False,
        dtd_validation=False,
        huge_tree=False,
        remove_comments=True,
        remove_pis=True,
    )


def _parse(document: bytes) -> etree._Element:
    try:
        root = etree.fromstring(document, _parser())
    except etree.XMLSyntaxError as exc:
        raise SamlRejected(f"not well-formed XML: {exc}")
    if root.getroottree().docinfo.doctype:
        raise SamlRejected("a DOCTYPE is not accepted")
    return root


def _text(element: etree._Element | None) -> str:
    return "" if element is None else "".join(element.itertext()).strip()


def _instant(value: str, what: str) -> datetime.datetime:
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError:
        raise SamlRejected(f"{what} is not a timestamp: {value!r}")
    if parsed.tzinfo is None:
        raise SamlRejected(f"{what} carries no time zone: {value!r}")
    return parsed


def parse_idp_metadata(document: bytes) -> _IdpMetadata:
    """The identity provider's entity id, redirect sign-on URL and signing certificates."""
    root = etree.fromstring(document, _parser())
    descriptor = (
        root
        if root.tag == f"{{{_NS['md']}}}EntityDescriptor"
        else root.find("md:EntityDescriptor", _NS)
    )
    if descriptor is None or not descriptor.get("entityID"):
        raise ValueError("SAML IdP metadata has no EntityDescriptor with an entityID")
    idp = descriptor.find("md:IDPSSODescriptor", _NS)
    if idp is None:
        raise ValueError("SAML IdP metadata has no IDPSSODescriptor")
    sso = [
        s.get("Location")
        for s in idp.findall("md:SingleSignOnService", _NS)
        if s.get("Binding") == _BINDING_REDIRECT and s.get("Location")
    ]
    if not sso:
        raise ValueError("SAML IdP metadata offers no HTTP-Redirect SingleSignOnService")
    certificates = tuple(
        "".join(_text(cert).split())
        for key in idp.findall("md:KeyDescriptor", _NS)
        if key.get("use") in (None, "signing")
        for cert in key.findall("ds:KeyInfo/ds:X509Data/ds:X509Certificate", _NS)
    )
    if not certificates:
        raise ValueError("SAML IdP metadata carries no signing certificate")
    return _IdpMetadata(descriptor.get("entityID"), sso[0], certificates)


class _SeenAssertions:
    """Assertion ids already accepted by this process, kept until each expires."""

    def __init__(self) -> None:
        self._expiry: dict[str, datetime.datetime] = {}
        self._lock = threading.Lock()

    def claim(self, assertion_id: str, expires: datetime.datetime, now: datetime.datetime) -> bool:
        """True the first time an id is seen; False for a replay."""
        with self._lock:
            for stale in [i for i, e in self._expiry.items() if e <= now]:
                del self._expiry[stale]
            if assertion_id in self._expiry:
                return False
            self._expiry[assertion_id] = expires
            return True


class SamlAuthProvider(AuthProvider):  # REQ-1265
    """Signs users in through a SAML 2.0 identity provider."""

    provider_name: str = "saml"

    def __init__(
        self,
        settings: SamlSettings,
        session_secret: str,
        now: Callable[[], datetime.datetime] | None = None,
    ) -> None:
        if not session_secret:
            raise ValueError(
                "auth.jwt_secret is required for provider 'saml': it signs the browser session "
                "and binds each response to the request that asked for it"
            )
        self._settings = settings
        self._secret = session_secret.encode("utf-8")
        self._sessions = SessionTokens(session_secret, audience=self.provider_name)
        self._now = now or (lambda: datetime.datetime.now(datetime.timezone.utc))
        self._seen = _SeenAssertions()
        self._metadata: _IdpMetadata | None = None
        self._metadata_read_at = 0.0
        self._metadata_lock = threading.Lock()

    @property
    def login_page_url(self) -> str:
        return self._settings.login_page_url

    # -- identity provider metadata -----------------------------------------------------------

    def _read_metadata(self) -> bytes:
        if self._settings.idp_metadata_file:
            with open(self._settings.idp_metadata_file, "rb") as handle:
                return handle.read()
        assert self._settings.idp_metadata_url is not None
        resp = httpx.get(self._settings.idp_metadata_url, timeout=_METADATA_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.content

    def idp(self) -> _IdpMetadata:
        with self._metadata_lock:
            stale = (time.monotonic() - self._metadata_read_at) > _METADATA_TTL_SECONDS
            if self._metadata is None or stale:
                self._metadata = parse_idp_metadata(self._read_metadata())
                self._metadata_read_at = time.monotonic()
            return self._metadata

    # -- the request ---------------------------------------------------------------------------

    def _relay_mac(self, request_id: str, expires: int) -> str:
        digest = hmac.new(self._secret, f"{request_id}.{expires}".encode(), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()

    def login_redirect(self) -> str:
        """The identity provider URL that starts a sign-in, with a fresh AuthnRequest."""
        idp = self.idp()
        now = self._now()
        request_id = "_" + secrets.token_hex(16)
        request = etree.Element(
            f"{{{_NS['samlp']}}}AuthnRequest",
            nsmap={"samlp": _NS["samlp"], "saml": _NS["saml"]},
            ID=request_id,
            Version="2.0",
            IssueInstant=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            Destination=idp.sso_url,
            ProtocolBinding=_BINDING_POST,
            AssertionConsumerServiceURL=self._settings.acs_url,
        )
        etree.SubElement(request, f"{{{_NS['saml']}}}Issuer").text = self._settings.sp_entity_id
        deflater = zlib.compressobj(wbits=-15)
        deflated = deflater.compress(etree.tostring(request)) + deflater.flush()
        expires = int(now.timestamp()) + _REQUEST_TTL_SECONDS
        query = urlencode(
            {
                "SAMLRequest": base64.b64encode(deflated).decode(),
                "RelayState": f"{expires}.{self._relay_mac(request_id, expires)}",
            }
        )
        return f"{idp.sso_url}{'&' if '?' in idp.sso_url else '?'}{query}"

    def _check_relay_state(self, relay_state: str, request_id: str) -> None:
        expires_text, _, mac = relay_state.partition(".")
        if not expires_text.isdigit() or not mac:
            raise SamlRejected("RelayState is not one this deployment issued")
        expires = int(expires_text)
        if not hmac.compare_digest(mac, self._relay_mac(request_id, expires)):
            raise SamlRejected("the response does not answer a request this deployment issued")
        if expires < int(self._now().timestamp()):
            raise SamlRejected("the sign-in request has expired")

    def sp_metadata(self) -> bytes:
        md = _NS["md"]
        entity = etree.Element(
            f"{{{md}}}EntityDescriptor", nsmap={"md": md}, entityID=self._settings.sp_entity_id
        )
        sp = etree.SubElement(
            entity,
            f"{{{md}}}SPSSODescriptor",
            AuthnRequestsSigned="false",
            WantAssertionsSigned="true",
            protocolSupportEnumeration=_NS["samlp"],
        )
        etree.SubElement(
            sp,
            f"{{{md}}}AssertionConsumerService",
            Binding=_BINDING_POST,
            Location=self._settings.acs_url,
            index="0",
            isDefault="true",
        )
        return etree.tostring(entity, xml_declaration=True, encoding="UTF-8")

    # -- the response --------------------------------------------------------------------------

    def _verified(self, element: etree._Element, idp: _IdpMetadata) -> etree._Element | None:
        """What ``element``'s own signature covers, or None when it carries no signature.

        The element is verified on its own, detached from the document, so the signature found
        is its direct child and the reference can only resolve inside it.
        """
        if element.find("ds:Signature", _NS) is None:
            return None
        detached = etree.tostring(element)
        config = SignatureConfiguration(
            require_x509=True,
            location="./",
            expect_references=1,
            signature_methods=_SIGNATURE_METHODS,
            digest_algorithms=_DIGEST_ALGORITHMS,
        )
        failures = []
        for certificate in idp.certificates:
            try:
                result = XMLVerifier().verify(
                    detached, x509_cert=certificate, parser=_parser(), expect_config=config
                )
            except (SignXMLException, ValueError) as exc:
                failures.append(str(exc))
                continue
            if isinstance(result, list):
                raise SamlRejected("the signature carries more than one reference")
            signed = result.signed_xml
            if signed is None or signed.tag != element.tag or signed.get("ID") != element.get("ID"):
                raise SamlRejected("the signature does not cover the element that carries it")
            return signed
        raise SamlRejected(f"signature not valid for the identity provider: {'; '.join(failures)}")

    def _signed_assertion(self, root: etree._Element, idp: _IdpMetadata) -> etree._Element:
        if root.tag != _RESPONSE:
            raise SamlRejected("the document is not a SAML Response")
        if root.find("saml:EncryptedAssertion", _NS) is not None:
            raise SamlRejected("encrypted assertions are not supported")
        status = root.find("samlp:Status/samlp:StatusCode", _NS)
        if status is None or status.get("Value") != _STATUS_SUCCESS:
            raise SamlRejected(
                f"the identity provider reported {None if status is None else status.get('Value')}"
            )
        signed_response = self._verified(root, idp)
        # From here only signed content is read: the verified Response when it is signed,
        # otherwise the one assertion whose own signature verifies.
        scope = signed_response if signed_response is not None else root
        assertions = scope.findall("saml:Assertion", _NS)
        if len(assertions) != 1:
            raise SamlRejected(f"expected one assertion, found {len(assertions)}")
        if signed_response is not None:
            destination = signed_response.get("Destination")
            if destination and destination != self._settings.acs_url:
                raise SamlRejected(f"the response is addressed to {destination!r}")
            return assertions[0]
        signed_assertion = self._verified(assertions[0], idp)
        if signed_assertion is None:
            raise SamlRejected("neither the response nor its assertion is signed")
        return signed_assertion

    def _check_assertion(self, assertion: etree._Element, idp: _IdpMetadata) -> str:
        """Check the signed assertion's conditions; return the id of the request it answers."""
        s = self._settings
        now = self._now()
        if _text(assertion.find("saml:Issuer", _NS)) != idp.entity_id:
            raise SamlRejected("the assertion was not issued by the configured identity provider")

        conditions = assertion.find("saml:Conditions", _NS)
        if conditions is None:
            raise SamlRejected("the assertion carries no Conditions")
        not_before = conditions.get("NotBefore")
        if not_before and now + _CLOCK_SKEW < _instant(not_before, "Conditions NotBefore"):
            raise SamlRejected("the assertion is not yet valid")
        not_on_or_after = conditions.get("NotOnOrAfter")
        if not not_on_or_after:
            raise SamlRejected("the assertion's Conditions carry no NotOnOrAfter")
        if now - _CLOCK_SKEW >= _instant(not_on_or_after, "Conditions NotOnOrAfter"):
            raise SamlRejected("the assertion has expired")
        audiences = {
            _text(a) for a in conditions.findall("saml:AudienceRestriction/saml:Audience", _NS)
        }
        if s.sp_entity_id not in audiences:
            raise SamlRejected(f"the assertion is for {sorted(audiences)}, not this service")

        for confirmation in assertion.findall("saml:Subject/saml:SubjectConfirmation", _NS):
            data = confirmation.find("saml:SubjectConfirmationData", _NS)
            if confirmation.get("Method") != _BEARER or data is None:
                continue
            expires = data.get("NotOnOrAfter")
            if (
                data.get("Recipient") == s.acs_url
                and data.get("InResponseTo")
                and expires
                and now - _CLOCK_SKEW < _instant(expires, "SubjectConfirmationData NotOnOrAfter")
            ):
                return data.get("InResponseTo")
        raise SamlRejected(
            "no bearer confirmation names this service, answers a request and is still valid"
        )

    def _attributes(self, assertion: etree._Element) -> dict[str, list[str]]:
        values: dict[str, list[str]] = {}
        for attribute in assertion.findall("saml:AttributeStatement/saml:Attribute", _NS):
            name = attribute.get("Name")
            if name:
                values.setdefault(name, []).extend(
                    _text(v) for v in attribute.findall("saml:AttributeValue", _NS)
                )
        return values

    def _claims(self, assertion: etree._Element) -> dict:
        s = self._settings
        attributes = self._attributes(assertion)

        def single(name: str | None) -> str | None:
            found = attributes.get(name) if name else None
            return found[0] if found else None

        name_id = _text(assertion.find("saml:Subject/saml:NameID", _NS))
        if s.user_id_attribute:
            user_id = single(s.user_id_attribute)
            if not user_id:
                raise SamlRejected(
                    f"the assertion carries no {s.user_id_attribute!r} attribute "
                    "(auth.saml.user_id_attribute)"
                )
        else:
            user_id = name_id
            if not user_id:
                raise SamlRejected("the assertion carries no NameID")
        return {
            "sub": user_id,
            "name_id": name_id,
            "email": single(s.email_attribute),
            "name": single(s.display_name_attribute),
            "groups": sorted(attributes.get(s.groups_attribute, [])) if s.groups_attribute else [],
        }

    def consume_response(self, saml_response: str, relay_state: str) -> str:
        """Verify a posted SAMLResponse and mint the browser's session token."""
        if len(saml_response) > _MAX_RESPONSE_BYTES:
            raise SamlRejected("the response is too large")
        try:
            document = base64.b64decode(saml_response, validate=True)
        except (binascii.Error, ValueError):
            raise SamlRejected("the response is not base64")
        idp = self.idp()
        assertion = self._signed_assertion(_parse(document), idp)
        request_id = self._check_assertion(assertion, idp)
        self._check_relay_state(relay_state, request_id)

        assertion_id = assertion.get("ID")
        conditions = assertion.find("saml:Conditions", _NS)
        assert conditions is not None  # _check_assertion refused an assertion without them
        expires = _instant(conditions.get("NotOnOrAfter"), "Conditions NotOnOrAfter") + _CLOCK_SKEW
        if not assertion_id or not self._seen.claim(assertion_id, expires, self._now()):
            raise SamlRejected("the assertion has already been used")
        return self._sessions.issue(self._claims(assertion))

    # -- the provider interface ----------------------------------------------------------------

    async def validate_token(self, token: str) -> AuthIdentity:
        """Validate a session token minted by ``consume_response``."""
        claims = self._sessions.verify(token)
        return AuthIdentity(
            user_id=claims["sub"],
            email=claims.get("email"),
            display_name=claims.get("name"),
            roles=list(claims.get("groups", [])),
            raw_claims={
                "name_id": claims.get("name_id"),
                "email": claims.get("email"),
                "groups": list(claims.get("groups", [])),
            },
        )
