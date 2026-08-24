# Secrets

**Names go in. Values never come back out.**

No API endpoint returns a stored secret value. No UI offers a "show" button. A person who has lost a value replaces it — that is the same call that created it, through the same form. This is not a policy decision: the read path simply does not exist in the code. (REQ-1558)

---

## Reference syntax

Three reference forms are valid wherever Provisa resolves credentials:

| Form | Resolves from | Who can use it |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | The server process's environment | Deployment configuration only |
| `${secret:NAME}` | The org vault — shared by all members | Any field that accepts a credential reference |
| `${user:NAME}` | The acting person's personal vault | Any field that accepts a credential reference |

Resolution is fail-closed throughout. An unknown provider name, an unset name, and an unreachable backend all raise an error. A reference that could not be resolved is never silently replaced with an empty string. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Name format

Secret names must match `[A-Za-z_][A-Za-z0-9_]*` — letters, digits, and underscores, starting with a letter or underscore. The constraint is practical: `${secret:NAME}` is parsed by the reference grammar, which reads up to the closing `}`. A name containing a brace, space, or colon would produce a reference that parses as something else. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Two vaults, one service

Every org has two vaults. Both live inside the same secrets service. (REQ-1560)

**Org vault** — The credential an org admin stores here is shared. Every member who references `${secret:DATABASE_TOKEN}` gets the same value. This is for credentials the *organization* owns: a shared database password, a service account key, a deployment token. The org vault requires the `org_settings` capability to read or write.

**Personal vault** — A credential stored here belongs to exactly one person. When two people each hold a `GIT_TOKEN`, `${user:GIT_TOKEN}` resolves to whichever of them is acting. The same reference text hands each person their own credential. A person who has stored nothing gets an error, not someone else's value. No capability gates the personal vault — holding your own credential is not a privilege an administrator grants. And there is no request syntax for naming another person's vault. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

The scope is part of the reference, not a permission around it. `${secret:NAME}` and `${user:NAME}` never answer for each other.

---

## Choosing a secrets service

**Admin → Security → Secrets service.** The panel is visible to anyone holding the `platform_settings` capability. Every backend the build knows is listed, whether or not the SDK is installed. A greyed-out row tells you which Python package is missing — the panel names it rather than hiding the option entirely.

Five backends ship:

| Key | Label | Needs |
| ----- | ------- | ------- |
| `provisa` | Provisa (built-in, encrypted) | Nothing; this is the default |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

Selection is fail-closed: an unknown or unavailable backend raises at startup rather than silently falling back to another. (REQ-1557)

### The backend's own credential

A central backend's connection credential is process configuration. It comes from `${env:...}` only — never from `${secret:...}`. A secrets service whose own credential lives inside itself cannot be opened, so the chain of trust terminates in the host environment by design. The registry enforces this: any config value on a backend spec is resolved with `providers=("env",)` before the backend is constructed. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Example — Vault config in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Central service vs. built-in

When a central service is configured, Provisa reads from it but does not write to it. The central service owns creating and deleting entries — those operations belong to its own tooling. The Secrets page says so and does not offer a create button. (REQ-1557)

When the built-in `provisa` backend is active, the Secrets page is fully writable: create, replace, and delete from the UI or via the API.

---

## Provisa's built-in store

The default when no central service is configured. Every row in `secrets_store` holds an encrypted envelope blob — the `value` column is binary, not text, and the decryption key lives in the process environment, not the database. A copy of the control plane without the deployment's master key holds ciphertext and nothing else. (REQ-1558)

Encryption is never optional. When no process-wide encryption key is configured, the store falls back to a local keychain. If the host has no keychain to hold a key, the store refuses to write rather than storing the value in the clear. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Storage shape** [tool-verified: `provisa/core/schema_admin.py:493-505`]:

| Column | Type | Purpose |
| -------- | ------ | --------- |
| `org_id` | Text | The org that owns this secret |
| `owner_id` | Text | `"*"` for org vault; user id for personal vault |
| `name` | Text | The reference name |
| `value` | LargeBinary | Encrypted envelope blob |
| `description` | Text | What the secret is for — never derived from the value |
| `updated_by` | Text | Who last set it |

The `value` column is not selected in any listing query. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## API endpoints

All routes are under `/admin/orgs/{org_id}`. The org vault requires `org_settings` in that org. The personal vault requires no capability — the owner is read off the authenticated identity; there is no request parameter for naming someone else's vault.

| Method | Path | What it does |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | List org vault names and references |
| `PUT` | `/secrets/{name}` | Create or replace one org secret |
| `DELETE` | `/secrets/{name}` | Delete one org secret |
| `GET` | `/my-secrets` | List the caller's personal names and references |
| `PUT` | `/my-secrets/{name}` | Create or replace one of the caller's secrets |
| `DELETE` | `/my-secrets/{name}` | Delete one of the caller's secrets |

Every response returns metadata — name, description, `updated_at`, `updated_by`, and the `reference` string to paste — but never the value. The `PUT` body carries `value` (required) and `description` (optional). A replace is the same call as a create: the name is the identity, not a separate ID.

Every write is recorded in the audit log. The log entry names the actor and the secret name. The value is not recorded, not even its length. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Where `${secret:NAME}` resolves

Resolution happens inside a context-bound operation, not at import time or at startup. The store reads and decrypts the org's secrets once at the start of that operation and holds the map in a `ContextVar` for its duration. Outside a bound operation, `${secret:NAME}` raises. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Two call sites establish the binding:

**Git remote operations.** When an org's repository remote URL contains a `${secret:...}` or `${user:...}` reference — for example, a push token embedded in the URL — the environments router binds both the org vault and the acting user's personal vault around the git call. The `${user:GIT_TOKEN}` form means a commit lands under the credential of whoever pushed it, not a shared service account. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**AI vendor API key reads.** When Provisa reads an org's LLM vendor key and that key is stored as a `${secret:NAME}` reference, `bound_to_request_org` establishes the org vault for that request. The reference is resolved on the way out; the reference text itself is never sent to the vendor. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## Org AI vendor keys as secret references

An org's AI vendor key (Anthropic, OpenAI, and others) can be stored as a `${secret:NAME}` reference instead of a literal key. (REQ-1580)

Store the key in the org vault first:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Then set the org's AI configuration to reference it:

```
vendor key field → ${secret:OPENAI_KEY}
```

The reference is stored encrypted in `org_secrets`. At query time Provisa resolves `${secret:OPENAI_KEY}` against the org vault and hands the literal key to the vendor SDK. Rotating the vault entry takes effect immediately — no configuration change on the org settings side. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Platform admin access

A platform admin operating the control plane has no read of any org's secret values. The `org_settings` guard explicitly refuses `cross_org` and the platform bypass: administering an org's lifecycle is not a read of the credentials that org keeps. The server enforces this independently of the UI. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## See also

- [Security Model](security.md) — layered access control, authentication, and audit logging
- [Configuration Reference](configuration.md) — `${env:VAR}` syntax for process-level credentials
