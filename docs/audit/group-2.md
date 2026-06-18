# Audit — Group 2: Authentication & Identity

Date: 2026-06-18
Scope: **Group 2 — Authentication & Identity** (REQ-120–125), all under `provisa/auth/`.
Method: read implementation against requirement text with file:line evidence.
Companion to the Group-1 audit ([group-1.md](group-1.md)) and the as-found
snapshot ([overview.md](overview.md), Group 1 only).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Area | Status | Finding |
| --- | --- | --- | --- |
| 120 | Authentication | To spec | `AuthProvider` ABC → `AuthIdentity`; one provider selected from YAML via `auth.provider` |
| 121 | Authentication | To spec | `FirebaseAuthProvider.verify_id_token` via firebase-admin; token-agnostic (all sign-in methods); optional `[firebase]` extra |
| 122 | Authentication | Incomplete | Keycloak realm roles only — **client roles (`resource_access`) not extracted**; JWKS URI by convention, not OIDC discovery |
| 123 | Authentication | To spec | `OAuthProvider`: discovery URL → JWKS → RS256 JWT; configurable `role_claim` + audience |
| 124 | Authentication | Not to spec | bcrypt + short-lived HS256 JWT present, but the **`allow_simple_auth: true` production guard is missing** — no such config field, no gate in wiring |
| 125 | Authentication | Not added | `check_superuser` + `AuthConfig.superuser` defined but **never invoked** anywhere; superuser bootstrap is inert; password not resolved from env secret |

3 of 6 to spec. Gaps: REQ-122 (client roles), REQ-124 (production guard),
REQ-125 (superuser unwired).

## Detail

### REQ-120 — Pluggable auth provider → AuthIdentity (To spec)

`AuthProvider` abstract base with `validate_token(token) -> AuthIdentity`
([models.py:44](../../provisa/auth/models.py#L44)); `AuthIdentity` carries
`user_id, email, display_name, roles, raw_claims, active_org_id`
([models.py:18](../../provisa/auth/models.py#L18)) — a superset of the spec's
`(user_id, email, roles, claims)`. One provider is chosen at a time from YAML via
`auth.provider` ([models.py:527](../../provisa/core/models.py#L527),
[wiring.py:23](../../provisa/auth/wiring.py#L23)). Claims map to Provisa roles via
`resolve_role`/`resolve_assignments`
([role_mapping.py](../../provisa/auth/role_mapping.py)) inside `AuthMiddleware`
([middleware.py:108-110](../../provisa/auth/middleware.py#L108-L110)). A `basic`
provider (`local_users` table) exists beyond the six named providers
([basic.py](../../provisa/auth/providers/basic.py)) — additive, not a violation.

### REQ-121 — Firebase Authentication (To spec)

`FirebaseAuthProvider` initializes firebase-admin (service-account key or
application default) and validates via `firebase_auth.verify_id_token`
([firebase.py:51-58](../../provisa/auth/providers/firebase.py#L51-L58)). All
Firebase sign-in methods are supported implicitly — the provider only consumes the
issued ID token. Optional dependency declared as `firebase = ["firebase-admin"]`
([pyproject.toml:61](../../pyproject.toml#L61)); import guarded with a clear error.
No unit test exercises this provider (see test note).

### REQ-122 — Keycloak OIDC (Incomplete)

`KeycloakAuthProvider` validates RS256 JWTs against Keycloak JWKS with a cached
`PyJWKClient` ([keycloak.py:48-56](../../provisa/auth/providers/keycloak.py#L48-L56)).
Two gaps versus the requirement:

1. **Client roles not mapped.** Only `realm_access.roles` is read
   ([keycloak.py:57-58](../../provisa/auth/providers/keycloak.py#L57-L58)); the
   requirement is "Realm roles **+ client roles** → Provisa role mapping," which
   live under `resource_access.{client_id}.roles`. No `resource_access` reference
   exists in the module.
2. **No OIDC discovery.** The JWKS URI is built by Keycloak convention
   (`/realms/{realm}/protocol/openid-connect/certs`,
   [keycloak.py:36](../../provisa/auth/providers/keycloak.py#L36)) rather than from
   the realm's OIDC discovery document. Functional for Keycloak, but narrower than
   "via OIDC discovery + JWKS."

### REQ-123 — Generic OAuth 2.0 / OIDC (To spec)

`OAuthProvider` fetches the discovery document, extracts `jwks_uri`
([oauth.py:42-48](../../provisa/auth/providers/oauth.py#L42-L48)), and validates
RS256 JWTs against it. Role claim is configurable (`role_claim`, default `roles`,
string coerced to list) and audience is configurable
([oauth.py:31-36](../../provisa/auth/providers/oauth.py#L31-L36),
[oauth.py:67-69](../../provisa/auth/providers/oauth.py#L67-L69)). Works with any
OIDC-compliant IdP. No dedicated unit test.

### REQ-124 — Simple username/password (Not to spec)

`SimpleAuthProvider` validates bcrypt password hashes and issues a 30-minute HS256
JWT ([simple.py:56-72](../../provisa/auth/providers/simple.py#L56-L72)); users come
from `auth.simple.users` in YAML; a `/auth/login` route is mounted only for this
provider ([wiring.py:101-105](../../provisa/auth/wiring.py#L101-L105)). The
requirement's production guard — "**requires `allow_simple_auth: true` flag**" — is
absent: `AuthConfig` has no such field
([models.py:526-534](../../provisa/core/models.py#L526-L534)), and
`build_auth_provider` instantiates the provider whenever `auth.provider == "simple"`
with no opt-in check ([wiring.py:30-41](../../provisa/auth/wiring.py#L30-L41)). The
only mention of `allow_simple_auth` in the codebase is a comment in
`actions_router.py` describing the pattern, not an implementation.

### REQ-125 — Superuser bootstrap (Not added)

`check_superuser` returns an admin `AuthIdentity(roles=["admin"])` on credential
match ([superuser.py:18-33](../../provisa/auth/superuser.py#L18-L33)) and
`AuthConfig.superuser: dict | None` exists
([models.py:532](../../provisa/core/models.py#L532)), but **nothing calls
`check_superuser`** — there is no reference in `middleware.py`, `wiring.py`, the
`/auth/login` route, or any provider. The superuser path is therefore inert:
superuser credentials are never honored, and the "always admin + all capabilities
regardless of auth provider" guarantee does not hold. Additionally the password is
compared against the raw config value
([superuser.py:22-26](../../provisa/auth/superuser.py#L22-L26)) with no
`resolve_secrets`, so "password from env secret" is unimplemented.

## Named tests

Spec names `tests/unit/test_auth_providers.py` and
`tests/unit/test_auth_middleware.py` (both exist). Current coverage is limited to
`SimpleAuthProvider` (login, JWT round-trip, tamper/secret rejection) and role
mapping ([test_auth_providers.py](../../tests/unit/test_auth_providers.py)).
Firebase, Keycloak, OAuth, Basic, superuser, and the `allow_simple_auth` guard have
no unit coverage.

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 122 | Incomplete | S | Extract Keycloak client roles from `resource_access.{client_id}.roles` and merge with realm roles |
| 2 | 124 | Not to spec | S | Add `auth.allow_simple_auth` (default false) and refuse to build `SimpleAuthProvider` unless set; cover with a test |
| 3 | 125 | Not added | M | Wire `check_superuser` into the auth path (login/middleware), resolve the password from env via `resolve_secrets`, ensure admin role + all capabilities regardless of provider |
| 4 | 121/123/122 | Test debt | M | Add provider unit tests for Firebase, Keycloak (realm + client roles), and generic OAuth |

Effort: S ≈ <½ day, M ≈ ~1 day, L ≈ multi-day.

## Phased implementation plan

Branch `feat/group2-auth` off `main`. Each phase ends with unit tests + a commit
(the Group-1 cadence). Ordered by blast radius: provider-local hardening first, the
middleware-touching superuser wiring last before tests. Verification command for
every phase: `pytest tests/unit/test_auth_providers.py tests/unit/test_auth_middleware.py`.

### Phase 1 — REQ-124: `allow_simple_auth` production guard (S)

- `AuthConfig`: add `allow_simple_auth: bool = False`
  ([models.py:526](../../provisa/core/models.py#L526)).
- `build_auth_provider`: when `provider == "simple"`, raise a config error unless
  `allow_simple_auth` is true — hard fail, no silent disable
  ([wiring.py:30](../../provisa/auth/wiring.py#L30)).
- Tests: simple + flag false → raises; flag true → builds; other providers
  unaffected.

### Phase 2 — REQ-122: Keycloak client roles (S)

- `validate_token`: merge `resource_access.{client_id}.roles` with
  `realm_access.roles` ([keycloak.py:57](../../provisa/auth/providers/keycloak.py#L57)).
- Tests: realm-only, client-only, and both → resolved roles include each; collision
  deduped.
- Optional, not blocking: switch the JWKS URI to OIDC discovery (currently built by
  Keycloak convention, [keycloak.py:36](../../provisa/auth/providers/keycloak.py#L36)).

### Phase 3 — REQ-125: superuser bootstrap wiring (M, highest blast radius)

- Resolve the superuser password via `resolve_secrets` (env secret), not the raw
  config value ([superuser.py:22](../../provisa/auth/superuser.py#L22)).
- Wire `check_superuser` into the auth path so it works **regardless of provider**
  and yields admin role + all capabilities.
- **Design decision — superuser attachment point (to be settled before Phase 3):**
  1. *Always-mounted `/auth/superuser-login`* that bypasses the configured provider
     and issues a JWT (recommended — works even with an IdP configured; mirrors
     `/auth/login`).
  2. *Middleware short-circuit* on HTTP Basic credentials checked against the
     superuser config before provider validation.
- Tests: superuser creds → admin identity + all caps; wrong creds → reject; works
  with a non-simple provider configured.

### Phase 4 — Provider test debt (M, after Phase 2)

- Unit tests: Firebase (mock `verify_id_token`), Keycloak (realm + client roles,
  mocked signing key/decode), generic OAuth (mock discovery + decode), Basic (mock
  `local_users`).
