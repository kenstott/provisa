# Python Client (`provisa-client`)

Thin Python client for querying Provisa via GraphQL or Arrow Flight.

## Install

```bash
pip install provisa-client
```

With pandas support:
```bash
pip install "provisa-client[pandas]"
```

## Quick Start

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

## GraphQL Queries

```python
# Raw response dict
result = client.query("{ orders { id amount region } }")

# With variables
result = client.query(
    "query Q($region: String!) { orders(region: $region) { id amount } }",
    variables={"region": "west"},
)

# pandas DataFrame (first root field is flattened)
df = client.query_df("{ orders { id amount region } }")
```

### Async

```python
result = await client.aquery("{ orders { id amount } }")
```

## Arrow Flight (high-throughput columnar)

Use Flight for large result sets — data streams as Arrow record batches without materializing in the server.

```python
import pyarrow as pa

# pyarrow Table
table: pa.Table = client.flight("{ orders { id amount region } }")

# pandas DataFrame
df = client.flight_df("{ orders { id amount region } }")
```

Flight connects to port 8815 by default. Override with `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

## Catalog Exploration

```python
# List all semantic layer tables
tables_df = client.list_tables()
# schema_name  table_name
# sales        orders
# sales        customers

# List approved persisted queries
approved_df = client.list_approved()
# stable_id
# monthly_revenue_by_region
```

## Connection Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `url` | `http://localhost:8001` | Provisa server base URL |
| `token` | `None` | Bearer token; omit for open auth |
| `role` | `"admin"` | Role sent with every request |
| `flight_port` | `8815` | Arrow Flight gRPC port |

## Error Handling

`query()` raises `httpx.HTTPStatusError` on HTTP errors.

`query_df()` raises `RuntimeError` if the response contains GraphQL errors.
