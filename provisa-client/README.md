# provisa-client

Python client for [Provisa](https://github.com/kenstott/provisa) — a config-driven data
virtualization platform. Query a Provisa server's governed GraphQL API and stream results back
as Arrow tables over Arrow Flight, without hand-rolling HTTP calls or Arrow IPC decoding.

## Features

- GraphQL query execution against a Provisa server
- Arrow Flight streaming for large result sets
- Optional pandas, SQLAlchemy dialect (`provisa.http`/`provisa.https`), and ADBC integration
- AES-GCM payload encryption support

## Install

```bash
pip install provisa-client
```

Optional extras: `provisa-client[pandas]`, `provisa-client[sqlalchemy]`, `provisa-client[adbc]`.
