# Secrets

**Names go in. Values never come back out.**

No API endpoint returns a stored secret value. No UI offers a "show" button. A person who has lost a value replaces it — that is the same call that created it, through the same form. This is not a policy decision: the read path simply does not exist in the code.

---

## Reference syntax

Three reference forms are valid wherever Provisa resolves credentials:

| Form | Resolves from | Who can use it |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | The server process's environment | Deployment configuration only |
| `${secret:NAME}` | The org vault — shared by all members | Any field that accepts a credential reference |
| `${user:NAME}` | The acting person's personal vault | Any field that accepts a credential reference |

On a multi-tenant deployment, keep platform credentials in a secrets service rather than in environment variables. A tenant admin can write `${env:...}` into a credential field of their own, so any value in the process environment is within reach of every tenant. Leave only the secrets backend's own credential in the environment.

Resolution is fail-closed throughout. An unknown provider name, an unset name, and an unreachable backend all raise an error. A reference that could not be resolved is never silently replaced with an empty string.

### Name format

Secret names must match `[A-Za-z_][A-Za-z0-9_]*` — letters, digits, and underscores, starting with a letter or underscore. The constraint is practical: `${secret:NAME}` is parsed by the reference grammar, which reads up to the closing `}`. A name containing a brace, space, or colon would produce a reference that parses as something else.

---

## Two vaults, one service

Every org has two vaults. Both live inside the same secrets service.

**Org vault** — The credential an org admin stores here is shared. Every member who references `${secret:DATABASE_TOKEN}` gets the same value. This is for credentials the *organization* owns: a shared database password, a service account key, a deployment token. The org vault requires the `org_settings` capability to read or write.

**Personal vault** — A credential stored here belongs to exactly one person. When two people each hold a `GIT_TOKEN`, `${user:GIT_TOKEN}` resolves to whichever of them is acting. The same reference text hands each person their own credential. A person who has stored nothing gets an error, not someone else's value. No capability gates the personal vault — holding your own credential is not a privilege an administrator grants. And there is no request syntax for naming another person's vault.

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

Selection is fail-closed: an unknown or unavailable backend raises at startup rather than silently falling back to another.

### The backend's own credential

A central backend's connection credential is process configuration. It comes from `${env:...}` only — never from `${secret:...}`. A secrets service whose own credential lives inside itself cannot be opened, so the chain of trust terminates in the host environment by design. The registry enforces this: any config value on a backend spec is resolved with `providers=("env",)` before the backend is constructed.

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

When a central service is configured, Provisa reads from it but does not write to it. The central service owns creating and deleting entries — those operations belong to its own tooling. The Secrets page says so and does not offer a create button.

When the built-in `provisa` backend is active, the Secrets page is fully writable: create, replace, and delete from the UI or via the API.

---

### One organization's secrets in a central service

A deployment that holds many organizations and reads a central service reads it for each organization inside that organization's own prefix. A reference stays the short name (`${secret:db#password}`); the prefix is added when it is read.

| Service | Where organization `acme`'s secret `db` is kept |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, the length of the organization id, the id with `_` written as `-`, then the name) |

A reference is refused, by name, when:

- its name would leave the prefix: it starts with `/`, holds a `..` segment or a backslash, or holds `/` in GCP or Azure, whose names are flat;
- its name is too long for the service once prefixed (GCP 255 characters, Azure 127); this is refused when the reference is saved, with the limit stated;
- no organization is bound to the operation that reads it.

A deployment of one organization reads a reference exactly as written, with no prefix: the service it is wired to is that organization's own, and nothing has to be moved.

**Moving to the prefix.** On a deployment of many organizations, every secret an organization references has to sit under that organization's prefix. Copy each one there with the service's own tooling, then shorten the reference to the name inside the prefix (`${secret:teams/data/db}` kept at `orgs/acme/db` becomes `${secret:db}`). A reference whose secret has not been moved fails as not found; it is not read from where it was.

## Provisa's built-in store

The default when no central service is configured. Every row in `secrets_store` holds an encrypted envelope blob — the `value` column is binary, not text, and the decryption key lives in the process environment, not the database. A copy of the control plane without the deployment's master key holds ciphertext and nothing else.

Encryption is never optional. When no process-wide encryption key is configured, the store falls back to a local keychain. If the host has no keychain to hold a key, the store refuses to write rather than storing the value in the clear.

**Storage shape**:

| Column | Type | Purpose |
| -------- | ------ | --------- |
| `org_id` | Text | The org that owns this secret |
| `owner_id` | Text | `"*"` for org vault; user id for personal vault |
| `name` | Text | The reference name |
| `value` | LargeBinary | Encrypted envelope blob |
| `description` | Text | What the secret is for — never derived from the value |
| `updated_by` | Text | Who last set it |

An organization that has created its own encryption key has its secrets wrapped under that key; otherwise they are wrapped under the deployment's. See [Encryption at Rest](security.md#encryption-at-rest-and-the-organizations-own-keys).

The `value` column is not selected in any listing query.

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

Every write is recorded in the audit log. The log entry names the actor and the secret name. The value is not recorded, not even its length.

---

## Where `${secret:NAME}` resolves

Resolution happens inside a context-bound operation, not at import time or at startup. The store reads and decrypts the org's secrets once at the start of that operation and holds the map in a `ContextVar` for its duration. Outside a bound operation, `${secret:NAME}` raises.

Two call sites establish the binding:

**Git remote operations.** When an org's repository remote URL contains a `${secret:...}` or `${user:...}` reference — for example, a push token embedded in the URL — the environments router binds both the org vault and the acting user's personal vault around the git call. The `${user:GIT_TOKEN}` form means a commit lands under the credential of whoever pushed it, not a shared service account.

**AI vendor API key reads.** When Provisa reads an org's LLM vendor key and that key is stored as a `${secret:NAME}` reference, `bound_to_request_org` establishes the org vault for that request. The reference is resolved on the way out; the reference text itself is never sent to the vendor.

---

## Org AI vendor keys as secret references

An org's AI vendor key (Anthropic, OpenAI, and others) can be stored as a `${secret:NAME}` reference instead of a literal key.

Store the key in the org vault first:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Then set the org's AI configuration to reference it:

```
vendor key field → ${secret:OPENAI_KEY}
```

The reference is stored encrypted in `org_secrets`. At query time Provisa resolves `${secret:OPENAI_KEY}` against the org vault and hands the literal key to the vendor SDK. Rotating the vault entry takes effect immediately — no configuration change on the org settings side.

---

## The deployment's own store credentials

The passwords of the stores Provisa runs (PostgreSQL, Redis, MinIO, Grafana's admin account and the Trino cluster's shared secret) are not secrets in either vault. They are generated per deployment and kept by the deployment: in `~/.provisa/store-credentials.env` (mode 600), in the Terraform state, or in the Kubernetes Secret the Helm chart's `credentials.existingSecret` names. See [Deployment Hardening](security.md#deployment-hardening).

---

## Platform admin access

A platform admin operating the control plane has no read of any org's secret values. The `org_settings` guard explicitly refuses `cross_org` and the platform bypass: administering an org's lifecycle is not a read of the credentials that org keeps. The server enforces this independently of the UI.

---

## See also

- [Security Model](security.md) — layered access control, authentication, and audit logging
- [Configuration Reference](configuration.md) — `${env:VAR}` syntax for process-level credentials
