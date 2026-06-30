# Multi-Tenant SaaS Mode

Provisa ships in single-tenant mode by default. Every deployment shares one configuration namespace, one set of metadata tables, and one Postgres connection pool. For most enterprise on-prem deployments, this is correct — you own the machine, you own the data.

Multi-tenant mode flips three things simultaneously: it activates PostgreSQL Row Level Security (RLS) on all metadata tables, installs `TenantMiddleware` into the request pipeline, and creates a per-tenant AWS KMS Customer Master Key (CMK) for config encryption. Use it when you are building a SaaS product on top of Provisa and need hard isolation between paying customers running on shared infrastructure.

---

## Enabling SaaS Mode

Set `multitenancy: true` in `config/provisa.yaml` [tool-verified: `provisa/core/models.py` line 623]:

```yaml
multitenancy: true
```

At startup, when `config.multitenancy` is `True`, the application does two things [tool-verified: `provisa/api/app.py` lines 769–774]:

1. Calls `_init_meta_rls()`, which enables RLS on every metadata table.
2. Registers `TenantMiddleware` via `app.add_middleware(TenantMiddleware)`.

The billing router (`/billing/*`) is always included regardless of this flag [tool-verified: `provisa/api/app.py` lines 2255–2256].

---

## JWT Requirements

Every authenticated request must carry a JWT with a `tenant_id` claim. `TenantMiddleware` reads `identity.raw_claims.get("tenant_id")` [tool-verified: `provisa/api/middleware/tenant_middleware.py` line 35]. If the claim is absent, the middleware returns HTTP 401 with `{"detail": "tenant_id claim missing"}`.

The `tenant_id` value must match a row in the `tenants` table created during signup. It is a UUID string.

Your auth provider (Firebase, Auth0, or any OIDC-compatible IdP) must inject this claim at token issuance time. How you do that is IdP-specific, but the claim name must be `tenant_id` — no aliases.

---

## Database Setup

### tenant_id columns

The schema migration at the bottom of `provisa/core/schema.sql` adds a `tenant_id UUID` column (nullable) to seven metadata tables [tool-verified: `provisa/core/schema.sql` lines 520–529]:

- `registered_tables`
- `table_columns`
- `domains`
- `relationships`
- `rls_rules`
- `persisted_queries`
- `roles`

Existing rows have `tenant_id = NULL`. The RLS policy treats `NULL` as a system row visible to all tenants, which allows shared baseline configuration alongside per-tenant customization.

### Row Level Security

When multitenancy is enabled, `_init_meta_rls()` runs the following pattern for each metadata table [tool-verified: `provisa/api/app.py` lines 512–530]:

```sql
ALTER TABLE <table> ENABLE ROW LEVEL SECURITY;
ALTER TABLE <table> FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_<table>
    ON <table>
    USING (tenant_id IS NULL OR tenant_id = current_setting('app.tenant_id', true)::uuid);
```

`FORCE ROW LEVEL SECURITY` applies the policy even to the table owner, preventing privilege escalation through ownership.

### SET LOCAL mechanism

At the start of each database transaction, `set_tenant_context()` injects the tenant's UUID as a Postgres session variable [tool-verified: `provisa/core/db.py` lines 66–69]:

```python
await conn.execute(f"SET LOCAL app.tenant_id = '{tenant_id}'")
```

`SET LOCAL` scopes the variable to the current transaction. When the transaction commits or rolls back, the variable resets. The RLS policy then reads `current_setting('app.tenant_id', true)` to filter rows. This means tenant isolation is enforced at the database level, not just in application logic — a query that bypasses middleware cannot read another tenant's rows.

### orgs table

Each tenant maps to an org. The `orgs` table [tool-verified: `provisa/core/schema.sql` lines 453–466] stores org namespaces. The `root` org is seeded automatically for single-tenant deployments. In multi-tenant mode you create one org per customer via the admin API. `user_org_memberships` tracks which users belong to which org.

---

## AWS KMS Setup

Status: planned, not yet wired. The KMS envelope-encryption model described here is the designed model for at-rest per-tenant config encryption (tracked by REQ-684, REQ-685, REQ-694, all status `proposed`). The cryptographic primitives and the decrypt/read path exist in code, but the encryption write path is not yet wired: no code path generates or stores a DEK or writes encrypted `tenant_config`. As a result, at-rest per-tenant config encryption is not yet active.

The designed model gives each tenant a dedicated Customer Master Key (CMK). At signup, `create_tenant_key(tenant_id)` calls `kms.create_key()` with `KeyUsage="ENCRYPT_DECRYPT"` and returns the key ARN [tool-verified: `provisa/api/billing/kms.py` lines 21–31]:

```python
response = kms_client.create_key(
    Description=f"provisa-tenant-{tenant_id}",
    KeyUsage="ENCRYPT_DECRYPT",
)
return response["KeyMetadata"]["Arn"]
```

The designed per-request encryption uses envelope encryption: `generate_data_key()` calls KMS to produce a 256-bit AES data encryption key (DEK). The plaintext DEK encrypts the config payload with AES-256-GCM via `aes_encrypt()`. Only the encrypted DEK is persisted in `tenant_config` alongside the ciphertext and IV. These primitives exist [tool-verified: `provisa/api/billing/kms.py` lines 34–65, `provisa/api/billing/tenant_db.py` lines 15–37], but `generate_data_key`, `aes_encrypt`, and `upsert_config_entity` currently have no callers, so `tenant_config` is never populated.

### Required IAM permissions

The Provisa process role needs these KMS permissions:

```json
{
  "Effect": "Allow",
  "Action": [
    "kms:CreateKey",
    "kms:GenerateDataKey",
    "kms:Decrypt"
  ],
  "Resource": "*"
}
```

Restrict `Resource` to `arn:aws:kms:<region>:<account>:key/*` in production, or tighten further with a key tag condition.

### Region configuration

The KMS client reads `AWS_KMS_REGION` and defaults to `us-east-1` [tool-verified: `provisa/api/billing/kms.py` line 17]:

```bash
export AWS_KMS_REGION=us-east-1
```

Standard AWS credential chain applies: environment variables, instance profile, or ECS task role. No custom credential configuration is needed beyond what boto3 already supports.

---

## Billing / Stripe Setup

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `STRIPE_API_KEY` | Yes | Stripe secret key (`sk_live_…` or `sk_test_…`) |
| `STRIPE_WEBHOOK_SECRET` | Yes | Signing secret from the Stripe webhook endpoint config |
| `STRIPE_BASE_URL` | No | Override for Stripe API base URL (used in tests) |

`STRIPE_API_KEY` and `STRIPE_WEBHOOK_SECRET` are read directly from `os.environ` — missing either raises a `KeyError` at the point of first use [tool-verified: `provisa/api/billing/stripe_client.py` line 15, `provisa/api/billing/router.py` line 83].

### Webhook configuration

In your Stripe dashboard, create a webhook endpoint pointing to `https://<your-host>/billing/webhook`. This path bypasses `TenantMiddleware` [tool-verified: `provisa/api/middleware/tenant_middleware.py` line 21].

The webhook handler verifies the `Stripe-Signature` header using `stripe.WebhookSignature.verify_header()`. It processes three events [tool-verified: `provisa/api/billing/router.py` lines 79–122]:

- `checkout.session.completed` — links the Stripe customer ID to the tenant record
- `customer.subscription.updated` — updates the tenant's plan and source limit
- `customer.subscription.deleted` — downgrades the tenant to `trial`

### Plans and limits

Three plans are defined [tool-verified: `provisa/api/billing/models.py` lines 15–21]:

| Plan | Source limit |
|---|---|
| `trial` | 2 |
| `starter` | 10 |
| `pro` | 100 |

Plan matching during `customer.subscription.updated` is case-insensitive substring matching on the Stripe price nickname. A nickname containing `"pro"` maps to the `pro` plan, `"starter"` to `starter`, `"trial"` to `trial`.

### Billing endpoints

| Method | Path | Auth required |
|---|---|---|
| `POST` | `/billing/signup` | No |
| `POST` | `/billing/checkout` | No |
| `POST` | `/billing/webhook` | No (Stripe signature) |
| `GET` | `/billing/portal` | No |
| `GET` | `/billing/status` | No |

`/billing/signup` creates the tenant record and KMS key. Call it once per customer onboarding. It returns `tenant_id`, `plan`, and `source_limit`. Pass `tenant_id` to `/billing/checkout` along with a Stripe `price_id` to initiate a Stripe Checkout session.

---

## Redis Cache Isolation

No configuration change is needed. When `tenant_id` is present, `RedisCacheStore` automatically prefixes every key [tool-verified: `provisa/cache/store.py` lines 111–119]:

```
Single-tenant: provisa:cache:<sha256_key>
Multi-tenant:  provisa:cache:<tenant_id>:<sha256_key>
```

Table invalidation keys follow the same pattern: `provisa:table:<tenant_id>:<table_id>`. This means one tenant's cache flush never touches another tenant's cached results.

The APQ (Automatic Persisted Queries) cache uses the same convention [tool-verified: `provisa/apq/cache.py` lines 90–93]:

```
Single-tenant: provisa:apq:<sha256_hex>
Multi-tenant:  provisa:apq:<tenant_id>:<sha256_hex>
```

Default APQ TTL is 86400 seconds. Override with `PROVISA_APQ_TTL`.

---

## Federated Query Isolation

In multi-tenant mode, the federated query engine scopes every connection to the requesting tenant's UUID [tool-verified: `provisa/api/trino_setup.py` lines 112–126]:

```python
if tenant_id is not None:
    kwargs["user"] = tenant_id
```

The federated engine's resource group configuration uses `${USER}` to route queries to per-tenant groups. Configure your resource group rules to match on `user` to enforce per-tenant memory and concurrency limits.

### Warm table schema isolation

Each `WarmTableManager` instance scoped to a tenant writes promoted tables to a tenant-specific schema [tool-verified: `provisa/cache/warm_tables.py` lines 63–75]:

```python
if tenant_id is not None:
    self._iceberg_schema = f"warm_cache_{tenant_id.replace('-', '_')}"
```

A tenant with ID `550e8400-e29b-41d4-a716-446655440000` gets schema `warm_cache_550e8400_e29b_41d4_a716_446655440000`. Schemas never overlap across tenants.

---

## Org Management

Orgs are the logical namespace for tenants and their users. All org management endpoints live under `/admin/orgs` [tool-verified: `provisa/api/admin/orgs_router.py`].

### Org CRUD

| Method | Path | Description |
|---|---|---|
| `GET` | `/admin/orgs/` | List all orgs |
| `POST` | `/admin/orgs/` | Create org (`id`, `name`) |
| `PUT` | `/admin/orgs/{org_id}` | Rename org |
| `DELETE` | `/admin/orgs/{org_id}` | Delete org (blocked for `root`) |

### Membership

| Method | Path | Description |
|---|---|---|
| `GET` | `/admin/orgs/{org_id}/members` | List members with profile data |
| `POST` | `/admin/orgs/{org_id}/members` | Add member (`user_id`) |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Remove member |

### Invite flow

Org invites let administrators onboard new users without direct IdP access. The invite system is only meaningful when `auth.provider` is `basic` — invite tokens bypass normal registration flow to place a new user into a specific org.

**Create an invite** (`POST /admin/invites/`):

```json
{
  "org_id": "acme-corp",
  "role_id": "analyst",
  "expires_in_days": 7
}
```

Returns a `token` (UUID). Send this token to the invitee out-of-band.

**Redeem the invite** (`POST /register`):

```json
{
  "username": "alice",
  "password": "…",
  "email": "alice@acme.com",
  "invite_token": "<token>"
}
```

On success, the user is created in `local_users`, added to `user_org_memberships` for the invite's org, and the invite is stamped `used_at` [tool-verified: `provisa/api/auth_router.py` lines 152–170]. Used tokens cannot be redeemed again.

**List invites** (`GET /admin/invites/`) — returns all invites with org name, expiry, and redemption status.

**Revoke an invite** (`DELETE /admin/invites/{token}`) — only succeeds if the invite has not yet been used.

Invites expire after the configured `expires_in_days` (default 7). An expired or used token returns HTTP 400 [tool-verified: `provisa/api/auth_router.py` line 161].

---

## Audit Logging

Every query is recorded in `query_audit_log` [tool-verified: `provisa/audit/query_log.py` lines 19–45]:

```sql
CREATE TABLE IF NOT EXISTS query_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID,
    user_id     TEXT NOT NULL,
    role_id     TEXT NOT NULL,
    query_hash  TEXT NOT NULL,   -- SHA-256 of query text
    table_ids   TEXT[] NOT NULL DEFAULT '{}',
    source      TEXT NOT NULL,
    status_code INT NOT NULL,
    duration_ms INT NOT NULL,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Two database rules block destructive operations [tool-verified: `provisa/audit/query_log.py` lines 33–41]:

```sql
CREATE RULE no_delete_audit AS ON DELETE TO query_audit_log DO INSTEAD NOTHING;
CREATE RULE no_update_audit AS ON UPDATE TO query_audit_log DO INSTEAD NOTHING;
```

The table is append-only. Deletes and updates silently no-op at the database level — they cannot be circumvented by application code.

Query text is never stored verbatim — only its SHA-256 hash. This satisfies data minimization requirements while preserving the ability to detect repeated queries.

Two indexes cover the primary access patterns [tool-verified: `provisa/audit/query_log.py` lines 43–44]:

- `(tenant_id, logged_at DESC)` — tenant-scoped time-range queries
- `(user_id, logged_at DESC)` — per-user activity audits

---

## Paths That Bypass TenantMiddleware

The following paths skip tenant resolution entirely [tool-verified: `provisa/api/middleware/tenant_middleware.py` line 21]:

```python
_SKIP_PATHS = {"/billing/signup", "/billing/webhook", "/health", "/docs", "/openapi.json"}
```

- `/billing/signup` — tenant does not exist yet at signup time
- `/billing/webhook` — Stripe calls this; Stripe does not carry a tenant JWT
- `/health` — infrastructure health checks must not require auth
- `/docs` and `/openapi.json` — OpenAPI UI and spec, typically blocked at the load balancer in production

All other paths require a valid JWT with a `tenant_id` claim. A missing identity returns HTTP 401 before tenant lookup begins [tool-verified: `provisa/api/middleware/tenant_middleware.py` lines 32–33].
