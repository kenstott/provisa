# VS Code

The Provisa extension for Visual Studio Code works on Node and Python code sources. It shows what Provisa would serve from the folder you have open, runs that folder in dev mode on a Provisa instance, and builds its image. For queries, use the VS Code extensions you already know, pointed at Provisa. The extension has no query editor of its own, and it never changes your model.

## The extension {: #the-extension }

### What Provisa would serve

Open a folder that holds a code source. The **Provisa** view in the Explorer lists each function Provisa would serve as a table or a command. Expand one to see the parameters and columns Provisa infers from its types.

Nothing is registered yet. The extension runs `provisa code check` on the folder, the same reader the server uses, so the list matches what the server reads. It refreshes each time you save a file. When Provisa refuses the code, the view says why, and the full refusal is in the **Provisa** output channel.

Set two things in your settings:

- `provisa.cli`: the Provisa command, as a list. The default is `["provisa"]`.
- `provisa.python`: the interpreter a Python folder is read under. Left empty, it is the folder's `.venv/bin/python`.

### Marks in the editor

In the editor, each Provisa form is marked where you wrote it:

- a `@table`, `@command` or other tag in a docstring or JSDoc comment;
- a decorator from `provisa-sdk`, or a wrapper from `@provisa/sdk`;
- an `Annotated[...]` type hint in Python.

Hover over a mark to see what Provisa serves the function as, with its parameters and its columns. An `Annotated[T, ...]` hint is read as `T`. Provisa ignores the rest of the hint.

### Dev mode on an instance

List the instances you work against in `provisa.instances`. Each one has a `url`, the `org` your sources go in, and an optional `name`:

```json
"provisa.instances": [
  { "name": "Local", "url": "http://localhost:8000", "org": "acme" }
]
```

1. Run **Provisa: Sign In** from the command palette. Pick an instance and paste a personal access token. VS Code keeps the token in its secret storage.
2. Run **Provisa: Run in Dev Mode** and name the source. The extension registers a code source that reads your folder in place, so each edit you save is read by the next query.

The instance reads your folder from its own disk, so this works when Provisa runs on your machine, as a single-organization install, and the folder is under a directory the operator allows. Otherwise the instance refuses, and the extension shows its reason. A source of the same name that already reads other code is left alone; pick another name.

### Build image

Run **Provisa: Build Image** on a source in dev mode. Choose the architecture, then a container registry of your organization, or **Download as a tarball**. The extension calls the same build the Provisa UI runs, waits for it, and copies the compose snippet to your clipboard. See [Running on a remote host](code-sources.md#running-on-a-remote-host) for what to do with it.

## Queries with the extensions you have {: #queries }

Provisa speaks GraphQL, SQL over the PostgreSQL wire protocol, and Cypher over Bolt. Point an existing extension at it.

### GraphQL

A GraphQL extension that reads a schema from a URL, such as GraphQL: Language Feature Support, gives you completion and checks against your role's schema. Point its schema at `/data/sdl` and send your role in the `X-Provisa-Role` header. A `graphql.config.yml` at the root of your folder:

```yaml
schema:
  - http://localhost:8000/data/sdl:
      headers:
        X-Provisa-Role: analyst
documents: "**/*.graphql"
```

The schema is the one your role sees. Run queries against `/data/graphql`.

### SQL

Provisa serves SQL over the PostgreSQL wire protocol when the operator sets `PROVISA_PGWIRE_PORT`. Any PostgreSQL extension connects, such as SQLTools with its PostgreSQL driver. Use the host and that port. For the user and password, give your user name and a personal access token, or your password. The catalog the extension browses shows only the tables your role can read. See [pgwire](pgwire.md).

### Cypher

Provisa serves Bolt when the operator sets `PROVISA_BOLT_PORT`. A Neo4j extension for VS Code connects with `bolt://<host>:<port>` and your credentials. Each database it lists is a role you hold. Pick the one you want to query as. See [Cypher](cypher.md).
