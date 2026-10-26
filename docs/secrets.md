# Secrets

**Names go in. Values never come back out.**

No API endpoint returns a stored secret value, and no UI offers a "show" button. Someone who has lost a value replaces it, with the same call and the same form that created it. This isn't a policy choice: the read path simply doesn't exist in the code.

---

## Reference syntax

Three reference forms are valid wherever Provisa resolves credentials:

| Form | Resolves from | Who can use it |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | The server process's environment | Deployment configuration only |
| `${secret:NAME}` | The org vault — shared by all members | Any field that accepts a credential reference |
| `${user:NAME}` | The acting person's personal vault | Any field that accepts a credential reference |

On a multi-tenant deployment, keep platform credentials in a secrets service and out of environment variables. A tenant admin can write `${env:...}` into a credential field of their own, so any value in the process environment is within every tenant's reach. Leave only the secrets backend's own credential in the environment.

Resolution is fail-closed throughout. An unknown provider name, an unset name, and an unreachable backend all raise an error. A reference that could not be resolved is never silently replaced with an empty string.

### Name format

Secret names must match `[A-Za-z_][A-Za-z0-9_]*`: letters, digits and underscores, starting with a letter or underscore. The reason is practical. `${secret:NAME}` is parsed by the reference grammar, which reads up to the closing `}`, so a name with a brace, space or colon would produce a reference that parses as something else.

---

## Two vaults, one service

Every org has two vaults. Both live inside the same secrets service.

**Org vault.** A credential an org admin stores here is shared, so every member who references `${secret:DATABASE_TOKEN}` gets the same value. Use it for credentials the *organization* owns, such as a shared database password, a service account key or a deployment token. Reading or writing the org vault requires the `org_settings` capability.

**Personal vault.** A credential stored here belongs to exactly one person. When two people each hold a `GIT_TOKEN`, `${user:GIT_TOKEN}` resolves to whichever of them is acting, so the same reference text hands each person their own credential. Someone who has stored nothing gets an error and never someone else's value. No capability gates the personal vault, because holding your own credential isn't a privilege an administrator grants, and no request syntax names another person's vault.

The scope is part of the reference itself, and no permission wraps around it. `${secret:NAME}` and `${user:NAME}` never answer for each other.

---

## Choosing a secrets service

**Admin → Security → Secrets service.** Anyone holding the `platform_settings` capability sees the panel. It lists every backend the build knows, whether or not its SDK is installed, and a greyed-out row names the Python package that's missing instead of hiding the option.

Five backends ship:

| Key | Label | Needs |
| ----- | ------- | ------- |
| `provisa` | Provisa (built-in, encrypted) | Nothing; this is the default |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

Selection fails closed: an unknown or unavailable backend stops startup with an error, and Provisa never falls back to another one quietly.

### The backend's own credential

A central backend's connection credential is process configuration, and it comes from `${env:...}` alone. `${secret:...}` can't supply it, because a secrets service whose own credential lives inside itself could never be opened. The chain of trust ends in the host environment by design. Provisa resolves every config value of a backend from the environment before it builds the backend.

For example, a Vault configuration in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Central service vs. built-in

With a central service configured, Provisa reads from it and never writes to it. Creating and deleting entries belongs to the service's own tooling, so the Secrets page says so and offers no create button.

With the built-in `provisa` backend active, the Secrets page is fully writable. You create, replace and delete secrets from the UI or through the API.

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

This is the default when no central service is configured. Every row in `secrets_store` holds an encrypted envelope blob. The `value` column is binary, and the decryption key lives in the process environment, outside the database. A copy of the model store without the deployment's master key holds ciphertext and nothing else.

Encryption is never optional. When no process-wide encryption key is configured, the store uses a local keychain. If the host has no keychain to hold a key, the store refuses to write, and the value is never stored in the clear.

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

All routes sit under `/admin/orgs/{org_id}`. The org vault requires `org_settings` in that org. The personal vault requires no capability, because the owner is read off the authenticated identity, and no request parameter names someone else's vault.

| Method | Path | What it does |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | List org vault names and references |
| `PUT` | `/secrets/{name}` | Create or replace one org secret |
| `DELETE` | `/secrets/{name}` | Delete one org secret |
| `GET` | `/my-secrets` | List the caller's personal names and references |
| `PUT` | `/my-secrets/{name}` | Create or replace one of the caller's secrets |
| `DELETE` | `/my-secrets/{name}` | Delete one of the caller's secrets |

Every response returns metadata and never the value: the name, description, `updated_at`, `updated_by`, and the `reference` string to paste. The `PUT` body carries `value` (required) and `description` (optional). A replace is the same call as a create, because the name is the identity and there is no separate ID.

Every write is recorded in the audit log. The entry names the actor and the secret. Neither the value nor its length is recorded.

---

## Where `${secret:NAME}` resolves

Resolution happens inside a bound operation. Nothing resolves at import time or at startup. The store reads and decrypts the org's secrets once, when the operation starts, and holds the map for as long as it runs. Outside a bound operation, `${secret:NAME}` raises an error.

Two call sites establish the binding:

**Git remote operations.** An org's repository remote URL can contain a `${secret:...}` or `${user:...}` reference, such as a push token embedded in the URL. The environments router then binds both the org vault and the acting user's personal vault around the git call. With the `${user:GIT_TOKEN}` form, a commit lands under the credential of whoever pushed it. No shared service account is involved.

**AI vendor API key reads.** When Provisa reads an org's LLM vendor key and that key is stored as a `${secret:NAME}` reference, it binds the org vault for that request. The reference is resolved on the way out, and the reference text itself never reaches the vendor.

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

The reference is stored encrypted in `org_secrets`. At query time Provisa resolves `${secret:OPENAI_KEY}` against the org vault and hands the literal key to the vendor SDK. Rotating the vault entry takes effect immediately, with no configuration change on the org settings side.

---

## The deployment's own store credentials

The passwords of the stores Provisa runs (PostgreSQL, Redis, MinIO, Grafana's admin account and the Trino cluster's shared secret) are not secrets in either vault. They are generated per deployment and kept by the deployment: in `~/.provisa/store-credentials.env` (mode 600), in the Terraform state, or in the Kubernetes Secret the Helm chart's `credentials.existingSecret` names. See [Deployment Hardening](security.md#deployment-hardening).

---

## Platform admin access

A platform admin operating the control plane can't read any org's secret values. The `org_settings` guard refuses `cross_org` and the platform bypass outright, because administering an org's lifecycle gives no read of the credentials that org keeps. The server enforces this on its own, whatever the UI shows.

---

## See also

- [Security Model](security.md) — layered access control, authentication, and audit logging
- [Configuration Reference](configuration.md) — `${env:VAR}` syntax for process-level credentials
