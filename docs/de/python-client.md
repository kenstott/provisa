# Python-Client (`provisa-client`)

Python-Client für Provisa. Stellt vier Schnittstellen bereit:

| Schnittstelle | Anwendungsfall |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL-Abfragen, Arrow Flight, DataFrame-Ausgabe |
| DB-API 2.0 (`connect`) | Standard-Python-Datenbankschnittstelle (PEP 249) (REQ-268) |
| SQLAlchemy-Dialekt | BI-Tools, ORM, Pandas `read_sql` (REQ-270) |
| ADBC | Arrow-natives spaltenorientiertes Streaming über Flight (REQ-271) |

## Installation

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Schnellstart

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` nimmt ein Credential entgegen, keinen Benutzernamen mit Passwort: Der Client kennt keinen eigenen Login-Schritt. Ein Personal Access Token ist das Credential der Wahl, wenn ein Skript unbeaufsichtigt laufen soll — es wird aus dem eigenen Profil des Benutzers ausgestellt, hat ein Ablaufdatum und lässt sich widerrufen, ohne das Konto anzufassen. (REQ-1263) Ein Provider-Bearer-Token funktioniert genauso. Beides gehört in `token`, und der Client legt es sowohl auf dem HTTP- als auch auf dem Arrow-Flight-Pfad vor.

Um ein Passwort gegen ein Token einzutauschen, senden Sie ein POST an `/auth/login` und lesen `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Die DB-API- und ADBC-Einstiegspunkte erledigen diesen Austausch für Sie — siehe unten.

### GraphQL-Abfragen

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

### Arrow Flight (Hochdurchsatz, spaltenorientiert)

Verwenden Sie Flight für große Ergebnismengen — Daten strömen als Arrow-Record-Batches, ohne serverseitig materialisiert zu werden. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight verbindet sich standardmäßig mit Port 8815. (REQ-143) Überschreiben mit `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Katalogexploration

```python
tables_df = client.list_tables()
```

### Verbindungsreferenz

| Parameter | Standard | Beschreibung |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Basis-URL des Provisa-Servers |
| `token` | `None` | Bearer-Credential — ein Provider-Token oder ein Personal Access Token; bei Passwort-Auth weglassen (REQ-606, REQ-1263) |
| `role` | `"admin"` | Mit jeder Anfrage gesendete Rolle (REQ-273) |
| `flight_port` | `8815` | Arrow-Flight-gRPC-Port (REQ-143) |

### Fehlerbehandlung

`query()` wirft `httpx.HTTPStatusError` bei HTTP-Fehlern. (REQ-607)
`query_df()` wirft `RuntimeError`, wenn die Antwort GraphQL-Fehler enthält. (REQ-607)

---

## DB-API 2.0

Standard-[PEP-249](https://peps.python.org/pep-0249/)-Schnittstelle. (REQ-268) Funktioniert mit jedem Tool, das eine DB-API-Verbindung akzeptiert.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` sendet Benutzernamen und Passwort per POST an `/auth/login` und behält das zurückgegebene `access_token`, sodass die Verbindung ein echtes Credential führt und nicht bloß einen Namen. `role` *fordert* eine Rolle an, und der Server erfüllt die Anfrage nur, wenn der Identität diese Rolle zugewiesen ist (REQ-273); ohne Angabe läuft die Verbindung unter der Rolle, die der Login aufgelöst hat.

### Abfragen ausführen

Der Cursor akzeptiert entweder GraphQL oder SQL — automatisch erkannt. (REQ-268, REQ-274)

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

### Spaltenmetadaten

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Benannte Parameter

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Kontextmanager

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## SQLAlchemy-Dialekt

```bash
pip install "provisa-client[sqlalchemy]"
```

URL-Schema: `provisa+http://` oder `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Mit pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL-Parameter

| Parameter | Beschreibung | Standard |
| ----------- | ------------- | --------- |
| `role` | Anzufordernde Rolle; serverseitig validiert (REQ-273) | die Rolle, die der Login auflöst |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Schemaerkennung

Der Dialekt implementiert `get_table_names()`, `get_columns()` und `has_table()` — Katalog-Tools (DBeaver, SQLAlchemy automap) können das Schema inspizieren. (REQ-363, REQ-270)

---

## ADBC

Arrow Database Connectivity, gestützt auf Arrow Flight. (REQ-271) Gibt direkt `pyarrow.Table` zurück — keine JSON-Deserialisierung. (REQ-271)

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

`adbc_connect` meldet sich zunächst über HTTP an und legt das erhaltene Token in jedes Flight-Ticket, sodass der Flight-Server die Verbindung genauso authentifiziert wie die REST-Oberfläche. (REQ-1263) Das Argument `role` ist eine Anfrage, die serverseitig gegen die Zuweisungen der Identität geprüft wird — es wird niemals zur Identität. (REQ-273)

### Als Arrow Table abrufen

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Als Tupel abrufen

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Spaltenmetadaten

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Kontextmanager

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC verbindet sich standardmäßig mit dem Flight-Server auf Port 8815. (REQ-143) Übergeben Sie `port=`, um einen Flight-Server auf einem nicht standardmäßigen Port zu erreichen. (REQ-711)
