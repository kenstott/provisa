# Cliente Python (`provisa-client`)

Cliente Python para o Provisa. Fornece quatro interfaces:

| Interface | Caso de uso |
| ----------- | ---------- |
| `ProvisaClient` | Consultas GraphQL, Arrow Flight, saída DataFrame |
| DB-API 2.0 (`connect`) | Interface padrão de banco de dados Python (PEP 249) (REQ-268) |
| Dialeto SQLAlchemy | Ferramentas de BI, ORM, `read_sql` do Pandas (REQ-270) |
| ADBC | Streaming colunar Arrow-nativo via Flight (REQ-271) |

## Instalação

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Início Rápido

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

### Consultas GraphQL

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

### Assíncrono

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (colunar de alta vazão)

Use Flight para grandes conjuntos de resultados — os dados fazem streaming como batches de registro Arrow sem materializar no servidor. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

O Flight se conecta à porta 8815 por padrão. (REQ-143) Sobreponha com `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Exploração de Catálogo

```python
tables_df = client.list_tables()
```

### Referência de Conexão

| Parâmetro | Padrão | Descrição |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base do servidor Provisa |
| `token` | `None` | Token Bearer; omita para autenticação por senha (REQ-606) |
| `role` | `"admin"` | Função enviada com cada requisição (REQ-273) |
| `flight_port` | `8815` | Porta gRPC do Arrow Flight (REQ-143) |

### Tratamento de Erros

`query()` lança `httpx.HTTPStatusError` em erros HTTP. (REQ-607)
`query_df()` lança `RuntimeError` se a resposta contiver erros GraphQL. (REQ-607)

---

## DB-API 2.0

Interface padrão [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Funciona com qualquer ferramenta que aceite uma conexão DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### Executando consultas

O cursor aceita GraphQL ou SQL — detectado automaticamente. (REQ-268, REQ-274)

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

### Metadados de coluna

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parâmetros nomeados

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Gerenciadores de contexto

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialeto SQLAlchemy

```bash
pip install "provisa-client[sqlalchemy]"
```

Esquema de URL: `provisa+http://` ou `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Com pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Parâmetros de URL

| Parâmetro | Descrição | Padrão |
| ----------- | ------------- | --------- |
| `role` | Função do Provisa | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspecção de esquema

O dialeto implementa `get_table_names()`, `get_columns()`, e `has_table()` — ferramentas de catálogo (DBeaver, SQLAlchemy automap) conseguem inspecionar o esquema. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity apoiado por Arrow Flight. (REQ-271) Retorna `pyarrow.Table` diretamente — sem desserialização JSON. (REQ-271)

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

### Buscar como Arrow Table

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Buscar como tuplas

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadados de coluna

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Gerenciador de contexto

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

O ADBC se conecta ao servidor Flight na porta 8815 por padrão. (REQ-143) Passe `port=` para alcançar um servidor Flight vinculado a uma porta não padrão. (REQ-711)
