# Python Client (`provisa-client`)

Cliente de Python para Provisa. Ofrece cuatro interfaces:

| Interfaz | Caso de uso |
| ----------- | ---------- |
| `ProvisaClient` | Consultas GraphQL, Arrow Flight, salida en DataFrame |
| DB-API 2.0 (`connect`) | Interfaz estándar de base de datos de Python (PEP 249) (REQ-268) |
| Dialecto de SQLAlchemy | Herramientas de BI, ORM, `read_sql` de Pandas (REQ-270) |
| ADBC | Transmisión columnar nativa de Arrow mediante Flight (REQ-271) |

## Instalación

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Inicio rápido

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

### Consultas de GraphQL

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

### Asíncrono

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (columnar de alto rendimiento)

Use Flight para conjuntos de resultados grandes: los datos se transmiten como lotes de registros Arrow sin materializarse en el servidor. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight se conecta al puerto 8815 de forma predeterminada. (REQ-143) Sobrescríbalo con `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Exploración del catálogo

```python
tables_df = client.list_tables()
```

### Referencia de conexión

| Parámetro | Valor predeterminado | Descripción |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base del servidor Provisa |
| `token` | `None` | Token Bearer; omítalo para autenticación con contraseña (REQ-606) |
| `role` | `"admin"` | Rol enviado con cada solicitud (REQ-273) |
| `flight_port` | `8815` | Puerto gRPC de Arrow Flight (REQ-143) |

### Manejo de errores

`query()` lanza `httpx.HTTPStatusError` en errores HTTP. (REQ-607)  
`query_df()` lanza `RuntimeError` si la respuesta contiene errores de GraphQL. (REQ-607)

---

## DB-API 2.0

Interfaz estándar [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Funciona con cualquier herramienta que acepte una conexión DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### Ejecución de consultas

El cursor acepta GraphQL o SQL: se detecta automáticamente. (REQ-268, REQ-274)

```python
cur = conn.cursor()

# GraphQL
cur.execute("{ orders { id amount region } }")
rows = cur.fetchall()           # list of tuples
one  = cur.fetchone()           # single tuple or None
many = cur.fetchmany(size=50)   # up to N tuples

# SQL (routed through Stage 2 governance)
cur.execute("SELECT id, amount FROM orders WHERE region = 'west'")
rows = cur.fetchall()
```

### Metadatos de columnas

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parámetros con nombre

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gestores de contexto

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialecto de SQLAlchemy

```bash
pip install "provisa-client[sqlalchemy]"
```

Esquema de URL: `provisa+http://` o `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Con pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Parámetros de URL

| Parámetro | Descripción | Valor predeterminado |
| ----------- | ------------- | --------- |
| `role` | Rol de Provisa | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspección del esquema

El dialecto implementa `get_table_names()`, `get_columns()` y `has_table()`; las herramientas de catálogo (DBeaver, SQLAlchemy automap) pueden inspeccionar el esquema. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity respaldado por Arrow Flight. (REQ-271) Devuelve `pyarrow.Table` directamente, sin deserialización JSON. (REQ-271)

```bash
pip install "provisa-client[adbc]"
```

```python
from provisa_client.adbc import adbc_connect

conn = adbc_connect(
    "http://localhost:8001",
    user="alice",
    password="secret",
    role="analyst",   # optional; server validates the requested role
    port=8815,        # Arrow Flight port (REQ-711)
)
```

### Obtención como tabla Arrow

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Obtención como tuplas

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadatos de columnas

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gestor de contexto

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC se conecta al servidor Flight en el puerto 8815 de forma predeterminada. (REQ-143) Pase `port=` para alcanzar un servidor Flight vinculado a un puerto no predeterminado. (REQ-711)
