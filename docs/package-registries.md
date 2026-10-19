# Package Registries

Node and Python sources install their own code and their dependencies. **Admin → Package registries** says where they install from: the organization's npm registries, PyPI indexes and Git hosts. An organization administrator enters them once, and every Node and Python install of the organization uses them. The public npm registry and the public PyPI need no entry.

Every field holds what was typed: a value, or a reference such as `${secret:npm_token}` or `${env:NPM_TOKEN}`, resolved in the organization's own vault each time an install runs. A reference that resolves to nothing stops the install and names the registry and the field. Provisa never writes to the vault or makes up a secret's name. See [Secrets](secrets.md).

---

## npm registries

| Field | Holds |
|---|---|
| Registry URL | The registry's address, such as `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Scope | A scope such as `@acme`, whose packages come from this registry. Empty for none |
| Token or user:password | An access token, or `user:password` |
| Mirror every install | Every package without a scope of its own is installed through this registry. One per organization |
| Certificate authority (PEM) | See [A private certificate authority](#a-private-certificate-authority) |

A token is written to the install's `.npmrc` as `_authToken` and sent as a bearer token. `user:password`, such as an Artifactory user and its API key, is written as `_auth` (base64) and sent as Basic. Either goes only to the registry's own host.

## PyPI indexes

| Field | Holds |
|---|---|
| Index URL | The index's simple-index address, such as `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Token or user:password | An access token, sent as the user `__token__`, or `user:password` |
| Default index for every install | Every Python package is installed from this index instead of the public PyPI. One per organization |
| Certificate authority (PEM) | See [A private certificate authority](#a-private-certificate-authority) |

The indexes and their credentials reach uv in the install's own environment, never in a file.

## Git hosts

A Git host entry covers the repositories a source's code or its Git dependencies are fetched from.

| Field | Holds |
|---|---|
| Host | The host, with its port and a path prefix where the server has them: `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Token or user:token | A token, or `user:token` |
| Certificate authority (PEM) | See [A private certificate authority](#a-private-certificate-authority) |

A repository URL is matched to the entry whose host, port and path prefix it starts with. When several match, the longest wins, so an entry for `bitbucket.example.com:7990/bitbucket/scm/data` covers that project and one for `bitbucket.example.com:7990/bitbucket` covers the rest.

A token alone is sent with the user `x-access-token`. Other providers expect a user, so enter `user:token`:

| Provider | User |
|---|---|
| GitHub | `x-access-token`, the default: enter the token alone |
| GitLab | Any non-blank user name |
| Bitbucket Cloud | `x-token-auth` for a repository or workspace access token; your user name or email for an API token |
| Bitbucket Data Center | Your user name; `x-token-auth` for a project or repository token |

## A private certificate authority

A registry, index or Git server behind your own certificate authority, such as an Artifactory proxy inside your network, needs that authority to be trusted. Paste its PEM certificate into the entry's **Certificate authority (PEM)** field, or a reference to a secret that holds it. Leave the field empty for a server whose certificate is publicly trusted. A value that holds no PEM certificate is refused when the entry is saved, and a reference that resolves to none is refused when an install runs.

The authority is trusted alongside the publicly trusted ones, never instead of them:

- Provisa's own fetches through the entry (a package's metadata and tarball, a wheel, a Git repository) verify the server against it.
- During an install, uv reads it through `SSL_CERT_FILE`, npm and Bun through `cafile` in the install's own `.npmrc`, Deno through `DENO_CERT`, and Git through `GIT_SSL_CAINFO`.

Those settings name one file holding the publicly trusted authorities and every entry's own. The file is written into the install's temporary directory and removed with it, so nothing outside the install reads it.
