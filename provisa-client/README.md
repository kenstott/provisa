# provisa-client

Python client for [Provisa](https://github.com/kenstott/provisa), the config-driven data
virtualization platform. Query a Provisa server in SQL, GraphQL or Cypher and stream results back
as Arrow tables over Arrow Flight, without hand-rolling HTTP calls or Arrow IPC decoding.

## Features

- GraphQL query execution against a Provisa server
- Arrow Flight streaming for large result sets
- Optional pandas, a SQLAlchemy dialect (`provisa+http://` and `provisa+https://` URLs) and ADBC
- AES-GCM payload encryption support

## Install

```bash
pip install provisa-client
```

Optional extras: `provisa-client[pandas]`, `provisa-client[sqlalchemy]`, `provisa-client[adbc]`.

## Notebooks

`provisa_client.query(text, role=None)` answers a SQL, GraphQL or Cypher statement as a pandas
DataFrame. The server detects the statement's language.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
eu = provisa_client.query("SELECT region, amount FROM shop.orders", role="analyst_eu")
```

On a desktop Python the read goes over Arrow Flight (or HTTP with `transport="http"`). The
connection is read from keywords to `provisa_client.sign_in(...)`, then the environment, then
`~/.provisa/credentials`:

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst
```

The environment names are `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`,
`PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`
and `PROVISA_PROFILE` (the profile to read). A username and password are exchanged for a session
token at `POST /auth/login`, on a deployment whose provider signs users in with a password.

In the browser (Pyodide, the notebook in Provisa's UI) a read goes to the server that served the
page, as the page's signed-in user. `query` blocks, which a notebook's kernel (a Web Worker) can
do; on the page's main thread use `await provisa_client.aquery(...)`.

Cell magics (`pip install provisa-client[notebook]`):

```text
%load_ext provisa_client.magics

%%sql orders --role analyst_eu
SELECT region, amount FROM shop.orders
```

`%%graphql` and `%%cypher` take the same arguments: an optional name the DataFrame is bound to,
and `--role`.

JupySQL (`%load_ext sql`) also defines `%%sql`. With both extensions loaded, `%%sql` is the one
whose extension was loaded last. Provisa's adds `--role` and, on a desktop Python, reads over
Arrow Flight. JupySQL reaches Provisa through the `provisa+http` SQLAlchemy dialect.
