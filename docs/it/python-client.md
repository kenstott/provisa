# Client Python (`provisa-client`) {: #python-client-provisa-client }

Client Python per Provisa. Fornisce quattro interfacce:

| Interfaccia | Caso d'uso |
| ----------- | ---------- |
| `ProvisaClient` | Query GraphQL, Arrow Flight, output DataFrame |
| DB-API 2.0 (`connect`) | Interfaccia database Python standard (PEP 249) (REQ-268) |
| Dialetto SQLAlchemy | Strumenti BI, ORM, `read_sql` di Pandas (REQ-270) |
| ADBC | Streaming columnar Arrow-native via Flight (REQ-271) |

## Installazione {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Avvio rapido {: #quick-start }

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

### Query GraphQL {: #graphql-queries }

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

### Arrow Flight (columnar ad alto throughput) {: #arrow-flight-high-throughput-columnar }

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

### Esplorazione del catalogo {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Riferimento connessione {: #connection-reference }

| Parametro | Default | Descrizione |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | URL base del server Provisa |
| `token` | `None` | Credenziale bearer — un token del provider o un token di accesso personale; ometterlo per l'autenticazione con password (REQ-606, REQ-1263) |
| `role` | `None` | Ruolo con cui agire, inviato come `X-Provisa-Role` e rispettato solo se assegnato all'utente; se omesso, la richiesta viene eseguita con il ruolo proprio dell'identità (REQ-273) |
| `flight_port` | `8815` | Porta gRPC di Arrow Flight (REQ-143) |

### Gestione errori {: #error-handling }

`query()` solleva `httpx.HTTPStatusError` in caso di errori HTTP. (REQ-607)
`query_df()` solleva `RuntimeError` se la risposta contiene errori GraphQL. (REQ-607)

---

## Usare Provisa in Jupyter {: #using-provisa-in-jupyter }

Installate nel kernel in esecuzione prima di importare. L'extra `pandas` copre l'output come DataFrame; aggiungete `sqlalchemy` se vi serve anche `pandas.read_sql` tramite il dialetto.

```python
%pip install "provisa-client[pandas]"
```

### Credenziali {: #credentials }

I notebook salvano l'output delle celle nel file `.ipynb`. Un token digitato in una cella viene salvato su disco insieme al vostro codice. Non fatelo.

`ProvisaClient` non legge l'ambiente: passategli le credenziali esplicitamente, da variabili d'ambiente. (`provisa_client.query()`, più sotto, le legge per voi.)

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

Impostate queste variabili nella shell prima di avviare Jupyter, oppure caricate un file `.env` con `python-dotenv`. In entrambi i casi, tenetele fuori dal notebook.

### Una sola chiamata: `query()` {: #one-call-query }

`provisa_client.query()` accetta un'istruzione SQL, GraphQL o Cypher e restituisce un DataFrame pandas. Il server riconosce di quale linguaggio si tratta. Non si costruisce nulla prima: la connessione viene dall'ambiente o da un file di profilo.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` agisce con quel ruolo per una lettura, inviato come `X-Provisa-Role`. Il server lo rispetta solo per un ruolo che possedete. La lettura passa per Arrow Flight; impostate `transport` su `http` per inviarla a `/data/query`. `await provisa_client.aquery(...)` ne è la forma awaitable.

La connessione si legge dal primo posto che nomina ciascuna impostazione:

1. le parole chiave di `provisa_client.sign_in(...)`, che risolve la connessione una volta per tutte le letture successive;
2. le variabili d'ambiente: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. una sezione di `~/.provisa/credentials`: `[default]`, o quella che nomina `PROVISA_PROFILE` (o `sign_in(profile=...)`).

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

Un `token` è un token di accesso personale o un token di sessione. Date invece `username` e `password` e il client li scambia con un token di sessione presso `POST /auth/login`. Funziona su un deployment il cui provider di accesso accetta una password (basic, simple, LDAP). Tenete la password in `PROVISA_PASSWORD` invece che nel file. Un accesso rifiutato, un URL mancante o un profilo nominato che non è nel file sollevano un errore con il motivo. Provisa non ha un flusso di accesso che un notebook possa guidare tramite un browser, quindi per i deployment con single sign-on usate un token di accesso personale.

### Magic di cella {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` e `%%cypher` rispondono alla cella con `query()`. Il DataFrame è il risultato della cella. Un nome dopo il magic lo associa anche nel notebook (`eu` sopra); `--role` agisce con quel ruolo per la cella.

Anche JupySQL (`%load_ext sql`) definisce un `%%sql`. Caricandoli entrambi, `%%sql` appartiene all'estensione caricata per ultima. Quella di Provisa accetta `--role` e legge tramite Arrow Flight. Anche JupySQL funziona, tramite il dialetto SQLAlchemy: `%sql provisa+http://user:password@host:port` si connette, `%sql`/`%%sql` eseguono istruzioni governate, e `%sqlcmd tables -s <domain>` e `%sqlcmd columns -t <table> -s <domain>` elencano il catalogo.

### Nel browser (Pyodide) {: #in-the-browser-pyodide }

Con Pyodide, `query()` legge dal server che ha servito la pagina, come l'utente autenticato della pagina. La pagina ospite tiene quell'utente nello scope globale del kernel:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` è vuoto per l'origine stessa della pagina. Il client legge `provisaAuth` a ogni richiesta, quindi un token ruotato ha effetto senza riavvio. I risultati arrivano come Arrow quando `pyarrow` è caricato; senza, arrivano come JSON. Un'istruzione Cypher torna sempre come JSON. `query()` è bloccante, cosa che un Web Worker (dove gira il kernel di un notebook) può fare; sul thread principale della pagina solleva un errore e la chiamata da usare è `await provisa_client.aquery(...)`.

### DataFrame da GraphQL, SQL e Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` accetta tutti e tre i linguaggi di query e restituisce un DataFrame pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

Le risposte GraphQL portano i dati sotto il nome di un campo radice; `query_df()` li estrae. Le risposte SQL e Cypher portano nomi di colonna espliciti dal server e vengono mappate direttamente.

### Arrow Flight per estrazioni grandi {: #arrow-flight-for-large-pulls }

`flight_df()` trasmette i dati come batch di record Arrow invece di deserializzare JSON. Per tabelle larghe o risultati oltre qualche centinaio di migliaia di righe è più veloce e usa meno memoria.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow preserva i tipi di colonna: gli interi restano interi, i timestamp restano timestamp. JSON no. Già questo basta per preferire Flight quando lo schema conta.

Flight si connette per impostazione predefinita alla porta 8815. Passate `flight_port=` al costruttore di `ProvisaClient` se il vostro server usa un'altra porta.

### `pandas.read_sql` tramite il dialetto SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

Installate l'extra `sqlalchemy` nel kernel:

```python
%pip install "provisa-client[sqlalchemy]"
```

Poi create un engine con un URL `provisa+http://`. Mettete le credenziali in variabili d'ambiente, non nell'URL.

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

Il dialetto invia nome utente e password a `/auth/login` e conserva il token ottenuto per tutta la vita dell'engine: le credenziali non vengono ritrasmesse a ogni query.

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

### Esecuzione di query {: #executing-queries }

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

### Metadati colonna {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Parametri nominati {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Context manager {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Dialetto SQLAlchemy {: #sqlalchemy-dialect }

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

### Con pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Parametri URL {: #url-parameters }

| Parametro | Descrizione | Default |
| ----------- | ------------- | --------- |
| `role` | Ruolo da richiedere; validato lato server (REQ-273) | il ruolo risolto dal login |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Introspezione dello schema {: #schema-introspection }

Il dialetto riflette il catalogo servito al vostro ruolo, letto da `GET /data/catalog` come utente, ruolo e organizzazione della connessione. Ogni dominio è uno schema, e ogni tabella vive in uno: `get_schema_names()` elenca i domini, `get_table_names(schema="shop")` elenca le tabelle di quel dominio con i loro nomi SQL, e `get_columns("orders", schema="shop")` dà tipo, nullabilità e descrizione di ogni colonna. Senza schema, `get_table_names()` non ne elenca nessuna. Una tabella sconosciuta solleva `NoSuchTableError`; un catalogo che il server rifiuta solleva un errore. (REQ-363, REQ-270, REQ-2070)

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

### Recupero come Arrow Table {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Recupero come tuple {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Metadati colonna {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Context manager {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC si connette al server Flight sulla porta 8815 per default. (REQ-143) Passa `port=` per raggiungere un server Flight collegato a una porta non predefinita. (REQ-711)
