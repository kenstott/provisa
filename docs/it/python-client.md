# Client Python (`provisa-client`)

Client Python per Provisa. Fornisce quattro interfacce:

| Interfaccia | Caso d'uso |
| ----------- | ---------- |
| `ProvisaClient` | Query GraphQL, Arrow Flight, output DataFrame |
| DB-API 2.0 (`connect`) | Interfaccia database Python standard (PEP 249) (REQ-268) |
| Dialetto SQLAlchemy | Strumenti BI, ORM, `read_sql` di Pandas (REQ-270) |
| ADBC | Streaming columnar Arrow-native via Flight (REQ-271) |

## Installazione

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Avvio rapido

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` accetta una credenziale, non un nome utente e una password: non ha alcun passaggio di login proprio. Un token di accesso personale è la credenziale a cui ricorrere quando uno script deve girare senza supervisione — viene emesso dal profilo dell'utente stesso, porta una scadenza ed è revocabile senza toccare l'account. (REQ-1263) Un token bearer del provider funziona allo stesso modo. L'uno o l'altro va in `token`, e il client lo presenta sia sul percorso HTTP sia su quello Arrow Flight.

Per scambiare una password con un token, fai POST su `/auth/login` e leggi `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

I punti di ingresso DB-API e ADBC eseguono questo scambio per te — vedi sotto.

### Query GraphQL

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

### Arrow Flight (columnar ad alto throughput)

Usa Flight per grandi result set — i dati vengono trasmessi in streaming come Arrow record batch senza essere materializzati sul server. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight si connette alla porta 8815 per default. (REQ-143) Sovrascrivi con `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Esplorazione del catalogo

```python
tables_df = client.list_tables()
```

### Riferimento connessione

| Parametro | Default | Descrizione |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base del server Provisa |
| `token` | `None` | Credenziale bearer — un token del provider o un token di accesso personale; ometterlo per l'autenticazione con password (REQ-606, REQ-1263) |
| `role` | `"admin"` | Ruolo inviato con ogni richiesta (REQ-273) |
| `flight_port` | `8815` | Porta gRPC di Arrow Flight (REQ-143) |

### Gestione errori

`query()` solleva `httpx.HTTPStatusError` in caso di errori HTTP. (REQ-607)
`query_df()` solleva `RuntimeError` se la risposta contiene errori GraphQL. (REQ-607)

---

## DB-API 2.0

Interfaccia standard [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Funziona con qualsiasi strumento che accetta una connessione DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` invia nome utente e password in POST a `/auth/login` e conserva l'`access_token` che riceve, così la connessione porta una credenziale vera anziché un nome. `role` *richiede* un ruolo e il server lo onora solo se l'identità lo ha assegnato (REQ-273); se omesso, la connessione gira con il ruolo risolto dal login.

### Esecuzione di query

Il cursore accetta GraphQL o SQL — rilevato automaticamente. (REQ-268, REQ-274)

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

### Metadati colonna

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parametri nominati

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Context manager

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialetto SQLAlchemy

```bash
pip install "provisa-client[sqlalchemy]"
```

Schema URL: `provisa+http://` o `provisa+https://` (REQ-270)

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

### Parametri URL

| Parametro | Descrizione | Default |
| ----------- | ------------- | --------- |
| `role` | Ruolo da richiedere; validato lato server (REQ-273) | il ruolo risolto dal login |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspezione dello schema

Il dialetto implementa `get_table_names()`, `get_columns()`, e `has_table()` — gli strumenti di catalogo (DBeaver, SQLAlchemy automap) possono ispezionare lo schema. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity basata su Arrow Flight. (REQ-271) Restituisce direttamente `pyarrow.Table` — nessuna deserializzazione JSON. (REQ-271)

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

`adbc_connect` esegue prima il login su HTTP e inserisce il token ottenuto in ogni ticket Flight, così il server Flight autentica la connessione allo stesso modo della superficie REST. (REQ-1263) L'argomento `role` è una richiesta, validata lato server rispetto alle assegnazioni dell'identità — non diventa mai l'identità. (REQ-273)

### Recupero come Arrow Table

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Recupero come tuple

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadati colonna

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Context manager

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC si connette al server Flight sulla porta 8815 per default. (REQ-143) Passa `port=` per raggiungere un server Flight collegato a una porta non predefinita. (REQ-711)
