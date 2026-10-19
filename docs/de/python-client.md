# Python-Client (`provisa-client`)

Python-Client für Provisa. Stellt vier Schnittstellen bereit:

| Schnittstelle | Anwendungsfall |
| ----------- | ---------- |
| `ProvisaClient` | GraphQL-Abfragen, Arrow Flight, DataFrame-Ausgabe |
| DB-API 2.0 (`connect`) | Standard-Python-Datenbankschnittstelle (PEP 249) |
| SQLAlchemy-Dialekt | BI-Tools, ORM, Pandas `read_sql` |
| ADBC | Arrow-natives spaltenorientiertes Streaming über Flight |

## Installation {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Schnellstart {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` nimmt eine Anmeldung entgegen. Es hat keinen eigenen Login-Schritt und fragt deshalb nie nach Benutzername und Passwort. Muss ein Skript unbeaufsichtigt laufen, nutzen Sie ein Personal Access Token: Sie stellen es in Ihrem eigenen Profil aus, es hat ein Ablaufdatum, und Sie können es widerrufen, ohne das Konto anzufassen. Ein Bearer-Token des Providers funktioniert genauso. Beides gehört in `token`, und der Client legt es sowohl auf dem HTTP- als auch auf dem Arrow-Flight-Weg vor.

Um ein Passwort gegen ein Token einzutauschen, senden Sie ein POST an `/auth/login` und lesen `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Die Einstiegspunkte DB-API und ADBC erledigen diesen Austausch für Sie, wie unten beschrieben.

### GraphQL-Abfragen {: #graphql-queries }

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

### Arrow Flight (Hochdurchsatz, spaltenorientiert) {: #arrow-flight-high-throughput-columnar }

Nutzen Sie Flight für große Ergebnismengen. Die Daten fließen als Arrow-Record-Batches und werden nie auf dem Server materialisiert.

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight verbindet sich standardmäßig mit Port 8815. Überschreiben mit `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Katalogexploration {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Verbindungsreferenz {: #connection-reference }

| Parameter | Standard | Beschreibung |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Basis-URL des Provisa-Servers |
| `token` | `None` | Bearer-Credential — ein Provider-Token oder ein Personal Access Token; bei Passwort-Auth weglassen |
| `role` | `None` | Rolle, unter der gehandelt wird; als `X-Provisa-Role` gesendet und nur berücksichtigt, wenn sie dem Benutzer zugewiesen ist; ohne Angabe läuft die Anfrage unter der eigenen Rolle der Identität |
| `flight_port` | `8815` | Arrow-Flight-gRPC-Port |

### Fehlerbehandlung {: #error-handling }

`query()` wirft `httpx.HTTPStatusError` bei HTTP-Fehlern.
`query_df()` wirft `RuntimeError`, wenn die Antwort GraphQL-Fehler enthält.

---

## Provisa in Jupyter verwenden {: #using-provisa-in-jupyter }

Installieren Sie das Paket vor dem Import in den laufenden Kernel. Das Extra `pandas` deckt die DataFrame-Ausgabe ab; fügen Sie `sqlalchemy` hinzu, wenn Sie auch `pandas.read_sql` über den Dialekt brauchen.

```python
%pip install "provisa-client[pandas]"
```

### Zugangsdaten {: #credentials }

Notebooks speichern die Ausgabe ihrer Zellen in der `.ipynb`-Datei. Ein in eine Zelle getipptes Token wird zusammen mit Ihrem Code auf die Festplatte geschrieben. Tun Sie das nicht.

`ProvisaClient` liest keine Umgebung: Übergeben Sie seine Zugangsdaten ausdrücklich, aus Umgebungsvariablen. (`provisa_client.query()`, unten, liest sie für Sie.)

```python
import os
from provisa_client import ProvisaClient

client = ProvisaClient(
    os.environ["PROVISA_URL"],           # choose your own variable name
    token=os.environ["PROVISA_TOKEN"],   # personal access token
    role=os.environ.get("PROVISA_ROLE"), # optional; omit to run as your own role
    org=os.environ.get("PROVISA_ORG"),   # required on a multi-tenant deployment; None otherwise
)
```

Setzen Sie diese Variablen in Ihrer Shell, bevor Sie Jupyter starten, oder laden Sie eine `.env`-Datei mit `python-dotenv`. In beiden Fällen bleiben sie aus dem Notebook heraus.

### Ein Aufruf: `query()` {: #one-call-query }

`provisa_client.query()` nimmt eine SQL-, GraphQL- oder Cypher-Anweisung entgegen und gibt einen pandas-DataFrame zurück. Der Server erkennt, welche Sprache es ist. Vorher wird nichts erzeugt: Die Verbindung kommt aus der Umgebung oder aus einer Profildatei.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` handelt für einen Lesevorgang unter dieser Rolle, gesendet als `X-Provisa-Role`. Der Server berücksichtigt sie nur für eine Rolle, die Sie innehaben. Gelesen wird über Arrow Flight; setzen Sie `transport` auf `http`, um stattdessen an `/data/query` zu senden. `await provisa_client.aquery(...)` ist die awaitbare Form.

Die Verbindung wird von der ersten Stelle gelesen, die die jeweilige Einstellung nennt:

1. Schlüsselwörter an `provisa_client.sign_in(...)`, das die Verbindung einmal für alle späteren Lesevorgänge auflöst;
2. Umgebungsvariablen: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. ein Abschnitt von `~/.provisa/credentials`: `[default]` oder der, den `PROVISA_PROFILE` (oder `sign_in(profile=...)`) nennt.

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

Ein `token` ist ein Personal Access Token oder ein Sitzungstoken. Geben Sie stattdessen `username` und `password` an, tauscht der Client sie unter `POST /auth/login` gegen ein Sitzungstoken. Das funktioniert in einem Deployment, dessen Anmelde-Provider ein Passwort annimmt (basic, simple, LDAP). Halten Sie das Passwort in `PROVISA_PASSWORD` und aus der Datei heraus. Eine abgelehnte Anmeldung, eine fehlende URL oder ein benanntes Profil, das nicht in der Datei steht, löst einen Fehler mit dem Grund aus. Provisa hat keinen Anmeldeablauf, den ein Notebook über einen Browser steuern könnte, also nutzen Sie bei Single-Sign-on-Deployments ein Personal Access Token.

### Zell-Magics {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` und `%%cypher` beantworten die Zelle mit `query()`. Der DataFrame ist das Ergebnis der Zelle. Ein Name nach dem Magic bindet ihn zusätzlich im Notebook (`eu` oben); `--role` handelt für die Zelle unter dieser Rolle.

JupySQL (`%load_ext sql`) definiert ebenfalls ein `%%sql`. Laden Sie beide, gehört `%%sql` der zuletzt geladenen Erweiterung. Die von Provisa nimmt `--role` und liest über Arrow Flight. JupySQL funktioniert auch, über den SQLAlchemy-Dialekt: `%sql provisa+http://user:password@host:port` verbindet, `%sql`/`%%sql` führen gesteuerte Anweisungen aus, und `%sqlcmd tables -s <domain>` und `%sqlcmd columns -t <table> -s <domain>` listen den Katalog auf.

### Im Browser (Pyodide) {: #in-the-browser-pyodide }

Unter Pyodide liest `query()` von dem Server, der die Seite ausgeliefert hat, als der angemeldete Benutzer der Seite. Die Host-Seite hält diesen Benutzer im globalen Scope des Kernels:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` ist für den eigenen Origin der Seite leer. Der Client liest `provisaAuth` bei jeder Anfrage, sodass ein rotiertes Token ohne Neustart wirkt. Ergebnisse kommen als Arrow, wenn `pyarrow` geladen ist; ohne es kommen sie als JSON. Eine Cypher-Anweisung kommt immer als JSON zurück. `query()` blockiert, was ein Web Worker (in dem ein Notebook-Kernel läuft) kann; im Haupt-Thread der Seite löst es einen Fehler aus, und `await provisa_client.aquery(...)` ist der zu verwendende Aufruf.

### DataFrames aus GraphQL, SQL und Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` akzeptiert alle drei Abfragesprachen und gibt einen pandas-DataFrame zurück.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

GraphQL-Antworten tragen die Daten unter einem Root-Feldnamen; `query_df()` extrahiert sie. SQL- und Cypher-Antworten tragen ausdrückliche Spaltennamen vom Server und werden direkt abgebildet.

### Arrow Flight für große Abrufe {: #arrow-flight-for-large-pulls }

`flight_df()` streamt Daten als Arrow-Record-Batches, statt JSON zu deserialisieren. Für breite Tabellen oder Ergebnismengen über einige hunderttausend Zeilen ist es schneller und braucht weniger Speicher.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow bewahrt Spaltentypen. Ganzzahlen bleiben Ganzzahlen und Zeitstempel bleiben Zeitstempel, was JSON nicht zusichern kann. Wenn das Schema zählt, ist das allein Grund genug, Flight vorzuziehen.

Flight verbindet standardmäßig auf Port 8815. Übergeben Sie `flight_port=` an den Konstruktor von `ProvisaClient`, wenn Ihr Server einen anderen Port verwendet.

### `pandas.read_sql` über den SQLAlchemy-Dialekt {: #pandasread_sql-through-the-sqlalchemy-dialect }

Installieren Sie das Extra `sqlalchemy` in den Kernel:

```python
%pip install "provisa-client[sqlalchemy]"
```

Bauen Sie dann eine Engine mit einer URL `provisa+http://`. Lesen Sie die Anmeldedaten aus Umgebungsvariablen, damit sie nie in Ihrem Notebook stehen.

```python
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.environ["PROVISA_HOST"]       # e.g. "localhost:8001"
user = os.environ["PROVISA_USER"]
pwd  = os.environ["PROVISA_PASSWORD"]

engine = create_engine(f"provisa+http://{user}:{pwd}@{host}")

df = pd.read_sql("SELECT id, amount FROM orders", engine)
```

Der Dialekt sendet Benutzername und Passwort einmal an `/auth/login` und hält das erhaltene Token für die Lebensdauer der Engine. Die Anmeldedaten werden nicht mit jeder Abfrage erneut gesendet.

---

## DB-API 2.0

Standard-[PEP-249](https://peps.python.org/pep-0249/)-Schnittstelle. Funktioniert mit jedem Tool, das eine DB-API-Verbindung akzeptiert.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` sendet Benutzername und Passwort an `/auth/login` und hält das zurückgegebene `access_token`, sodass die Verbindung eine echte Anmeldung trägt. `role` *fordert* eine Rolle an, und der Server gewährt sie nur, wenn die Identität diese Rolle zugewiesen hat. Lassen Sie es weg, läuft die Verbindung in der Rolle, die der Login ergeben hat.

### Abfragen ausführen {: #executing-queries }

Der Cursor nimmt GraphQL oder SQL an und erkennt selbst, was er bekommen hat.

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

### Spaltenmetadaten {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Benannte Parameter {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Kontextmanager {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## SQLAlchemy-Dialekt {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

URL-Schema: `provisa+http://` oder `provisa+https://`

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### Mit pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### URL-Parameter {: #url-parameters }

| Parameter | Beschreibung | Standard |
| ----------- | ------------- | --------- |
| `role` | Anzufordernde Rolle; serverseitig validiert | die Rolle, die der Login auflöst |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Schemaerkennung {: #schema-introspection }

Der Dialekt bildet den Katalog ab, der Ihrer Rolle ausgeliefert wird, gelesen von `GET /data/catalog` als Benutzer, Rolle und Organisation der Verbindung. Jede Domain ist ein Schema, und jede Tabelle liegt in einem: `get_schema_names()` listet die Domains, `get_table_names(schema="shop")` listet die Tabellen dieser Domain unter ihren SQL-Namen, und `get_columns("orders", schema="shop")` liefert Typ, Nullbarkeit und Beschreibung jeder Spalte. Ohne genanntes Schema listet `get_table_names()` keine. Eine unbekannte Tabelle löst `NoSuchTableError` aus; ein vom Server verweigerter Katalog löst einen Fehler aus.

---

## ADBC

Arrow Database Connectivity auf Basis von Arrow Flight. Es liefert direkt eine `pyarrow.Table`, ohne JSON-Deserialisierung.

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
    port=8815,        # Arrow Flight port
)
```

`adbc_connect` meldet sich zuerst über HTTP an und legt das erhaltene Token in jedes Flight-Ticket, sodass der Flight-Server die Verbindung genauso authentifiziert wie die REST-Oberfläche. Das Argument `role` ist eine Anfrage, die der Server gegen die Zuweisungen der Identität prüft. Es wird nie zur Identität.

### Als Arrow Table abrufen {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Als Tupel abrufen {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Spaltenmetadaten {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Kontextmanager {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC verbindet sich standardmäßig mit dem Flight-Server auf Port 8815. Übergeben Sie `port=`, um einen Flight-Server auf einem nicht standardmäßigen Port zu erreichen.
