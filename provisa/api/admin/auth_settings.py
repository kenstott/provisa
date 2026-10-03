# Copyright (c) 2026 Kenneth Stott
# Canary: 4a6b1840-2dc6-4222-9d1f-433417b3c966
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The admin auth registry and how a saved form becomes the ``auth`` config block.

REQ-919: the registry lists each provider with its config fields, secret fields marked. Only
the keys the chosen provider declares are persisted. REQ-1265 adds the LDAP and SAML providers.
"""

from __future__ import annotations

from provisa.api.errors import ApiError

# Requirements: REQ-919, REQ-1265

# Role settings every provider shares; saved at the top level of the auth block.
COMMON_KEYS = (
    "default_role",
    "assignments_source",
    "trust_upstream",
    "allow_simple_auth",
    "allow_registration",
)

# The browser-session signing key. One per deployment, so it is stored at the top level of the
# auth block and shown under each provider that declares it.
_SESSION_SECRET_KEY = "jwt_secret"

AUTH_PROVIDERS = [
    {
        "key": "none",
        "label": "None",
        "description": "No authentication — open access. Development only.",
        "config_fields": [],
    },
    {
        "key": "basic",
        "label": "Local accounts",
        "description": "Usernames and passwords kept by this deployment.",
        "config_fields": [
            {
                "config_key": "jwt_secret",
                "label": "Browser session signing secret",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "firebase",
        "label": "Firebase",
        "description": "Google Firebase ID-token verification.",
        "config_fields": [
            {"config_key": "project_id", "label": "Project ID", "type": "string", "required": True},
            {
                "config_key": "service_account_key",
                "label": "Service account key",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "keycloak",
        "label": "Keycloak",
        "description": "Keycloak OIDC (realm + client).",
        "config_fields": [
            {
                "config_key": "server_url",
                "label": "Server URL",
                "type": "string",
                "required": True,
                "placeholder": "https://keycloak.example.com",
            },
            {"config_key": "realm", "label": "Realm", "type": "string", "required": True},
            {"config_key": "client_id", "label": "Client ID", "type": "string", "required": True},
            {
                "config_key": "client_secret",
                "label": "Client secret",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "oauth",
        "label": "OAuth / OIDC",
        "description": "Generic OIDC provider via a discovery URL.",
        "config_fields": [
            {
                "config_key": "discovery_url",
                "label": "Discovery URL",
                "type": "string",
                "required": True,
                "placeholder": "https://issuer/.well-known/openid-configuration",
            },
            {"config_key": "client_id", "label": "Client ID", "type": "string", "required": True},
            {"config_key": "audience", "label": "Audience", "type": "string", "required": False},
            {
                "config_key": "role_claim",
                "label": "Role claim",
                "type": "string",
                "required": False,
                "placeholder": "roles",
            },
        ],
    },
    {
        "key": "ldap",
        "label": "LDAP",
        "description": "Directory username and password (LDAP or Active Directory).",
        "config_fields": [
            {
                "config_key": "server_url",
                "label": "Server URL",
                "type": "string",
                "required": True,
                "placeholder": "ldaps://directory.example.com:636",
            },
            {
                "config_key": "start_tls",
                "label": "Upgrade an ldap:// connection with StartTLS",
                "type": "boolean",
                "required": False,
            },
            {
                "config_key": "ca_cert_file",
                "label": "CA certificate file",
                "type": "string",
                "required": False,
                "placeholder": "/etc/provisa/ldap-ca.pem",
            },
            {
                "config_key": "bind_dn",
                "label": "Service account DN",
                "type": "string",
                "required": True,
                "placeholder": "cn=provisa,ou=services,dc=example,dc=com",
            },
            {
                "config_key": "bind_password",
                "label": "Service account password",
                "type": "string",
                "required": True,
                "secret": True,
            },
            {
                "config_key": "user_base_dn",
                "label": "User base DN",
                "type": "string",
                "required": True,
                "placeholder": "ou=people,dc=example,dc=com",
            },
            {
                "config_key": "user_filter",
                "label": "User filter",
                "type": "string",
                "required": True,
                "placeholder": "(uid={username})",
            },
            {
                "config_key": "user_id_attribute",
                "label": "User id attribute",
                "type": "string",
                "required": True,
                "placeholder": "uid",
            },
            {
                "config_key": "email_attribute",
                "label": "Email attribute",
                "type": "string",
                "required": False,
                "placeholder": "mail",
            },
            {
                "config_key": "display_name_attribute",
                "label": "Display name attribute",
                "type": "string",
                "required": False,
                "placeholder": "cn",
            },
            {
                "config_key": "group_base_dn",
                "label": "Group base DN",
                "type": "string",
                "required": False,
                "placeholder": "ou=groups,dc=example,dc=com",
            },
            {
                "config_key": "group_filter",
                "label": "Group filter",
                "type": "string",
                "required": False,
                "placeholder": "(member={user_dn})",
            },
            {
                "config_key": "group_name_attribute",
                "label": "Group name attribute",
                "type": "string",
                "required": False,
                "placeholder": "cn",
            },
            {
                "config_key": "jwt_secret",
                "label": "Browser session signing secret",
                "type": "string",
                "required": False,
                "secret": True,
            },
        ],
    },
    {
        "key": "saml",
        "label": "SAML 2.0",
        "description": "Single sign-on through a SAML 2.0 identity provider.",
        "config_fields": [
            {
                "config_key": "idp_metadata_url",
                "label": "Identity provider metadata URL",
                "type": "string",
                "required": False,
                "placeholder": "https://idp.example.com/metadata",
            },
            {
                "config_key": "idp_metadata_file",
                "label": "Identity provider metadata file (instead of a URL)",
                "type": "string",
                "required": False,
                "placeholder": "/etc/provisa/idp-metadata.xml",
            },
            {
                "config_key": "sp_entity_id",
                "label": "Service provider entity ID",
                "type": "string",
                "required": True,
                "placeholder": "https://provisa.example.com/auth/saml/metadata",
            },
            {
                "config_key": "acs_url",
                "label": "Assertion consumer service URL",
                "type": "string",
                "required": True,
                "placeholder": "https://provisa.example.com/auth/saml/acs",
            },
            {
                "config_key": "login_page_url",
                "label": "Sign-in page URL",
                "type": "string",
                "required": True,
                "placeholder": "https://provisa.example.com/login",
            },
            {
                "config_key": "user_id_attribute",
                "label": "User id attribute (empty uses the NameID)",
                "type": "string",
                "required": False,
            },
            {
                "config_key": "email_attribute",
                "label": "Email attribute",
                "type": "string",
                "required": False,
            },
            {
                "config_key": "display_name_attribute",
                "label": "Display name attribute",
                "type": "string",
                "required": False,
            },
            {
                "config_key": "groups_attribute",
                "label": "Groups attribute",
                "type": "string",
                "required": False,
            },
            {
                "config_key": "jwt_secret",
                "label": "Browser session signing secret",
                "type": "string",
                "required": True,
                "secret": True,
            },
        ],
    },
    {
        "key": "simple",
        "label": "Simple (username/password)",
        "description": "Built-in username/password. NOT for production — requires the production guard.",
        "config_fields": [
            {
                "config_key": "jwt_secret",
                "label": "JWT signing secret",
                "type": "string",
                "required": True,
                "secret": True,
            },
        ],
    },
]


def _fields_of(provider: str) -> list[dict]:
    return [f for p in AUTH_PROVIDERS if p["key"] == provider for f in p["config_fields"]]


def provider_config_view(auth: dict) -> dict[str, dict]:
    """Each provider's stored config block, as the admin form reads it (before redaction)."""
    view: dict[str, dict] = {}
    for entry in AUTH_PROVIDERS:
        block = dict(auth.get(entry["key"], {}) or {})
        if any(f["config_key"] == _SESSION_SECRET_KEY for f in entry["config_fields"]):
            block[_SESSION_SECRET_KEY] = auth.get(_SESSION_SECRET_KEY, "")
        view[entry["key"]] = block
    return view


def apply_auth_settings(auth: dict, body: dict) -> dict:
    """The auth block after saving ``body`` over ``auth``. Raises a 400 for a refused save."""
    provider = body.get("provider")
    valid = {p["key"] for p in AUTH_PROVIDERS}
    if not isinstance(provider, str) or provider not in valid:
        raise ApiError(
            400,
            "settings.unknown_auth_provider",
            f"unknown auth provider {provider!r}; valid: {sorted(valid)}",
            provider=str(provider),
            valid=sorted(valid),
        )
    result = dict(auth)
    result["provider"] = provider

    fields = {f["config_key"]: f for f in _fields_of(provider)}
    block = dict(result.get(provider, {}) or {})
    for key, value in (body.get("config") or {}).items():
        if key not in fields:
            continue
        if fields[key]["type"] == "boolean" and not isinstance(value, bool):
            raise ApiError(
                400,
                "settings.auth_field_type",
                f"auth field {key!r} takes true or false",
                field=key,
            )
        if key == _SESSION_SECRET_KEY:
            result[_SESSION_SECRET_KEY] = value
        else:
            block[key] = value
    if provider != "simple" or block:
        result[provider] = block

    for key in COMMON_KEYS:
        if key in (body.get("common") or {}):
            result[key] = body["common"][key]
    return result
