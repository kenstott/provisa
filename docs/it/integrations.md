# Integrazioni {: #integrations }

## Scegliere un percorso di connessione {: #choosing-a-connection-path }

| Tipo di client | Percorso consigliato | Perché |
| ------------- | ----------------- | ----- |
| Strumenti di BI (Tableau, Power BI, Looker) | JDBC | Streaming columnar Arrow Flight sulla connessione; gli strumenti di BI dispongono di una procedura guidata JDBC integrata e beneficiano della consegna columnar ad alto throughput per grandi set di risultati |
| psql, DBeaver, qualsiasi strumento compatibile con PG | pgwire (driver PG nativo) | Opzione predefinita senza attriti — nessun driver personalizzato necessario; usa quello che hai già |
| Stack dati Python (pandas, pyarrow) | `provisa-client` o ADBC diretto | Batch Arrow in streaming; nessun overhead di serializzazione per riga |
| Spark, DuckDB, pipeline ad alto throughput | Arrow Flight (ADBC) | Streaming columnar illimitato direttamente in memoria Arrow |
| Servizio a servizio (contratti tipizzati) | Protobuf gRPC | Proto generato per ruolo; righe in streaming; sicurezza dei tipi |
| Applicazioni web, scripting | HTTP (`/data/graphql`, `/data/sql`) | Nessun driver; HTTP standard; scelta completa del linguaggio di query |
| Client REST (standard JSON:API) | `GET /data/jsonapi/{table}` | Busta JSON:API v1.0; set di campi parziali, paginazione, filtraggio tramite parametri di query; nessun driver |

---

## pgwire — Driver PostgreSQL nativo {: #pgwire-native-postgresql-driver }

Provisa implementa il protocollo di rete di PostgreSQL (versione protocollo 3.0). Qualsiasi client che parla PostgreSQL si connette senza un driver personalizzato.

Abilitalo impostando `PROVISA_PGWIRE_PORT` (ad esempio `5433`) prima di avviare Provisa. Disabilitato se non impostato o `0`.

### Perché pgwire invece di JDBC? {: #why-pgwire-instead-of-jdbc }

Il driver JDBC porta le sue query su Arrow Flight, e per usarlo dovete distribuire `provisa-jdbc.jar`. pgwire non ha bisogno di altro. Se avete già `psql`, DBeaver, SQLAlchemy o un driver JDBC PG, siete pronti, e per questo è la strada più semplice quando inviate solo SQL.

Scegliete JDBC per gli strumenti di BI con una procedura guidata di connessione JDBC integrata che scaricano grandi set di risultati, dove lo streaming columnar di Arrow Flight ripaga. pgwire accetta lo stesso SQL libero sull'intero schema pubblicato, con meno da configurare.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. Nuova connessione → PostgreSQL
2. Host: `localhost`, Porta: `5433`
3. Nome utente / password come configurati in Provisa
4. Nessun download di driver aggiuntivo richiesto

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Oppure con `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Autenticazione {: #authentication }

Il campo `password` del pacchetto di avvio trasporta la credenziale, e *cosa sia* la credenziale determina il metodo: un token di accesso personale, un token bearer OIDC oppure una password verso il provider configurato. Con il provider `basic` e `auth.scram: true` la password viene dimostrata tramite SCRAM-SHA-256 anziché inviata. I certificati client sono supportati. In modalità trust (`none`) il nome utente viene mappato direttamente a un ruolo e la password viene ignorata.

La tabella completa interfaccia × metodo si trova nel [Modello di sicurezza](security.md#surfaces-and-credentials). MD5 non è supportato; abilita TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) quando operi su una rete non attendibile.

### Limitazioni {: #limitations }

- Solo SQL. GraphQL e Cypher non sono accettati tramite pgwire.
- Non di sola lettura. Le righe delle tabelle registrate vengono scritte con `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` e `COPY ... FROM STDIN`, ciascuna ammessa dal diritto `write` del ruolo, dalle sue concessioni di colonna e dal suo filtro di righe; vedere [Scritture di dati](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Nessuna definizione: `CREATE`, `ALTER` e `DROP` vengono rifiutati con SQLSTATE 0A000, tranne la tabella temporanea propria di una sessione; vedere [Definizioni](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` esporta in `text`, `csv` e `binary`; `COPY ... FROM STDIN` legge `text` e `csv`.
- Le query a `information_schema` e `pg_catalog` vengono intercettate e risolte da uno shim di catalogo DuckDB, quindi gli strumenti di individuazione dello schema funzionano correttamente.

---

## Driver JDBC {: #jdbc-driver }

Il driver JDBC di Provisa usa Arrow Flight come trasporto sottostante. È il percorso consigliato per gli strumenti di BI con una procedura guidata di connessione JDBC.

Le query passano per Arrow Flight ogni volta che la porta Flight risponde all'apertura della connessione, e i risultati arrivano un batch alla volta con i loro tipi di colonna. Come porta Flight si assume quella accanto alla porta HTTP (8815 accanto a 8001); impostate la proprietà `flight_port` quando non è così. Se la porta Flight non è raggiungibile alla connessione, la connessione usa l'API HTTP per le sue query e lo registra a livello INFO. Una volta che una connessione è su Flight, un errore del server — una credenziale rifiutata, un ruolo che l'utente non detiene, un'istruzione fallita — viene sollevato al chiamante con il messaggio del server; non viene ritentato via HTTP.

La navigazione dello schema (`getTables`, `getColumns`, chiavi primarie ed esterne) viene letta dalla stessa porta Flight, con la credenziale di accesso: il driver elenca il catalogo del ruolo con cui agisce, quindi una tabella o colonna non concessa a quel ruolo non compare. Non viene elencata nemmeno una chiave che fa riferimento a una tabella o colonna non concessa al ruolo. Su una connessione la cui porta Flight non era raggiungibile alla connessione, il driver legge lo stesso catalogo via HTTP (`GET /data/catalog`): il server costruisce entrambi gli elenchi con un'unica funzione, quindi tabelle, nomi di colonna, tipi e chiavi non dipendono dalla porta che ha risposto.

### Connessione {: #connection }

Scarica [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre l'ultima versione) e aggiungilo al percorso dei driver del tuo strumento.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

L'autenticazione usa le proprietà JDBC standard `user` / `password`. Il driver le scambia con un token di sessione e lo invia a ogni richiesta; un accesso rifiutato fa fallire la connessione con lo stato e il motivo del server. Provisa ricava il ruolo dall'identità che ha effettuato l'accesso. La proprietà facoltativa `role` (o `?role=` nell'URL) chiede di agire come un ruolo che l'utente detiene, o un insieme di tali ruoli separati da virgole; un ruolo che l'utente non detiene viene rifiutato. Su un deployment senza provider di autenticazione non c'è accesso, e il nome utente viene preso come ruolo richiesto.

### Configurazione degli strumenti di BI {: #bi-tool-setup }

**Tableau**

1. Gestisci → Driver → Installa Provisa JDBC
2. Connetti → Altri database (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Inserisci nome utente e password quando richiesto

**DBeaver** (il percorso JDBC; il percorso pgwire è più sopra)

1. Database → Nuova connessione → JDBC
2. Driver: aggiungi `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Inserisci nome utente e password nella scheda Autenticazione

**Power BI** — usa il gateway ODBC con il ponte Provisa JDBC-ODBC (incluso nel programma di installazione).

---

## Client Arrow Flight {: #arrow-flight-clients }

Arrow Flight (porta 8815) è il percorso consigliato per gli strumenti dati che lo supportano. I risultati fluiscono in streaming come RecordBatch Arrow senza essere materializzati nella memoria di Provisa.

### Python (`provisa-client`)

Il percorso Python consigliato incapsula sia GraphQL che Arrow Flight:

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

Consulta [docs/python-client.md](python-client.md) per il riferimento completo, incluso DB-API 2.0, il dialetto SQLAlchemy e ADBC.

### Python (PyArrow puro) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight trasporta la propria credenziale nel payload JSON, nel campo `token`, che contiene un token bearer del provider o un token di accesso personale. Sia l'handshake sia ogni ticket lo accettano, ed entrambi lo validano allo stesso modo, quindi un client che si è autenticato all'handshake presenta comunque il token a ogni `do_get`. Un campo `role` accanto *richiede* un ruolo; il server deriva i ruoli consentiti dell'identità e sostituisce il valore autorizzato, quindi una stringa di ruolo in un ticket non è mai l'identità. Vedi [Modello di sicurezza](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

Anche la navigazione ha bisogno della credenziale. `list_flights`, `get_flight_info` e `get_schema` non portano un ticket, quindi leggono la credenziale dall'intestazione di chiamata `authorization: Bearer <token>` e il ruolo richiesto da `x-provisa-role` (un ruolo, o un insieme di ruoli detenuti separati da virgole). Con l'autenticazione attiva, una chiamata di navigazione senza credenziale viene rifiutata, e il catalogo restituito è quello del ruolo autorizzato: le tabelle, le colonne e i comandi concessi a quel ruolo, e nient'altro. Un ticket di catalogo (un ticket senza `query`) riceve la stessa risposta dal proprio `token` e `role`. Un client deve quindi inviare la sua credenziale prima di navigare; `provisa-client` lo fa in `list_tables()`. Su un deployment senza provider di autenticazione queste chiamate elencano l'intero catalogo e non richiedono credenziali.

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (porta 50051) {: #protobuf-grpc-port-50051 }

Percorso da servizio a servizio. Provisa genera un `.proto` per ruolo e lo rigenera ogni volta che il modello o il ruolo cambia, e ogni ruolo vede solo le tabelle e le colonne a cui ha accesso. Il servizio ha una RPC per ogni tabella alla radice dello schema del ruolo; le tabelle correlate si raggiungono attraverso le loro relazioni, come in GraphQL.

Scarica il proto per il tuo ruolo:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Usare `grpc_server_reflection` per scoprire lo schema in modo programmatico. La reflection descrive il servizio del ruolo con cui la chiamata agisce, lo stesso file del download qui sopra. Con l'autenticazione disattivata, una chiamata di reflection indica il proprio ruolo come qualsiasi altra chiamata:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Ogni RPC deve portare una credenziale nella chiave di metadati `authorization`, cioè un token del provider o un token di accesso personale. `x-provisa-role` richiede un ruolo dall'insieme consentito dell'identità; non è una credenziale e non lo è mai stata. I certificati client sono supportati. Vedi [Modello di sicurezza](security.md#surfaces-and-credentials).

Le query in streaming emettono un messaggio per riga; le mutazioni sono unarie.

---

## Invocare comandi tra protocolli {: #invoking-commands-across-protocols }

Un **comando** è una funzione tracciata registrata o un webhook: un elemento invocabile registrato nel modello di Provisa con un `kind` (`query` o `mutation`) e un `impl_kind` che descrive come viene eseguito. Ogni superficie instrada le invocazioni attraverso un unico esecutore governato (`invoke_tracked_function`) che applica l'assegnazione del comando (`visible_to`), il diritto `write` per una mutazione e la governance in modo uniforme.

| `impl_kind` | Cosa viene eseguito | Campi di binding |
| ------------ | ----------- | --------------- |
| `source_procedure` | Stored procedure su un'origine registrata (predefinito) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script lato server | `script` |
| `http` | Chiamata HTTP in uscita | `url`, `method` |
| `grpc` | Chiamata gRPC in uscita verso un server esterno | `target`, `method` |
| `python` | Elemento invocabile Python ospitato da Provisa | `callable` (ad esempio `demo.py_functions:random_dataset`) |

Quando un comando dichiara un `return_schema` (JSON Schema con `type: array, items: object`), è a restituzione di insieme, e ogni superficie lo proietta come un set di righe tipizzato. I comandi dimostrativi `random_python_set` (impl_kind `python`) e `random_grpc_set` (impl_kind `grpc`) illustrano sia un elemento invocabile ospitato sia un ponte gRPC esterno che restituisce righe con valori casuali; entrambi sono registrati in `config/provisa-install.yaml`.

### Matrice dei protocolli {: #protocol-matrix }

| Superficie | Sintassi | Esempio |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → campo Query; `kind=mutation` → campo Mutation; con prefisso di dominio quando `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` o `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — gli argomenti posizionali corrispondono ai nomi degli argomenti dichiarati | `CALL random_python_set(3, 7)` |
| Provisa gRPC (porta 50051) | Unario `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Il campo `kind` controlla solo il posizionamento in GraphQL. Le superfici SQL, Cypher, Bolt e gRPC accettano allo stesso modo comandi `query` e `mutation`.

---

## Apollo Federation

Provisa può fungere da subgraph Federation v2, esponendo il proprio schema pubblicato a un Apollo Router o Apollo Gateway.

### Configurazione {: #setup }

Abilita la federazione in `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa genera automaticamente le direttive `@key` sulle colonne di chiave primaria e `@external`/`@provides` sulle relazioni tra subgraph.

### Registrazione con Apollo Router {: #register-with-apollo-router }

Nel tuo `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Esegui `rover supergraph compose --config supergraph.yaml` per generare lo schema del supergraph.

### Entità {: #entities }

Provisa risponde alle query `_entities` per i join tra subgraph. Qualsiasi tabella con una chiave primaria è automaticamente risolvibile come entità Federation.

---

## Import Hasura v2 / DDN {: #hasura-v2-ddn-import }

Consulta [docs/import.md](import.md) per la migrazione da Hasura a Provisa.

---

## Kafka

Consulta [docs/sources.md](sources.md#kafka-sources) per la configurazione dei topic Kafka come tabelle di sola lettura e destinazioni dei risultati delle query.

---

## Controlli di qualità dei dati {: #data-quality-checkers }

Soda Core e Great Expectations si connettono a Provisa tramite pgwire, come qualsiasi altro client postgres. Questa è l'intera integrazione: il checker mantiene un unico driver postgres e analizza la vista federata, così una tabella Snowflake, una tabella Iceberg e una collection Mongo vengono tutte controllate dallo stesso dialetto di contratto senza un checker per sistema.

La scansione viene eseguita in un interprete figlio — `python -m provisa.dq.worker` — l'unico
punto in cui vengono importati `soda_core` o `great_expectations`. Nulla viene collegato al
processo del server, e un crash del checker abbatte un sottoprocesso anziché l'event loop.

I risultati della scansione atterrano come normali righe di origine, quindi cadenza, freschezza,
eventi, derivazione, governance, RLS, la griglia e l'esportazione si applicano tutti senza un
secondo meccanismo. La stesura del contratto, l'involucro del risultato e la registrazione
derivata sono trattati in [docs/sources.md](sources.md#data-quality-checkers).

### Installare un checker {: #installing-a-checker }

Nessuna delle due librerie viene distribuita per impostazione predefinita. Il programma di
installazione chiede quale si desidera, e la risposta diventa `dq_checker: none|soda|gx` in
`~/.provisa/config.yaml`. Sul livello Docker `scripts/provisa` la trasforma nell'argomento di
build `PROVISA_EXTRAS`; sul livello nativo `first-launch.sh` installa nel venv l'extra pyproject
corrispondente.

| `dq_checker` | Libreria | Licenza | Piano cloud ospitato |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Rifiutato (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Consentito |

La Elastic License 2.0 vieta di fornire il software a terzi come servizio ospitato, che è
esattamente ciò che significherebbe eseguire Soda all'interno del piano SaaS per conto di un
tenant. Una distribuzione ospitata che desideri Soda punta a un endpoint Soda gestito
direttamente dall'operatore. Vedere [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations)
per le chiavi di connessione.

---

## Interscambio semantico Apache Ossie {: #apache-ossie-semantic-interchange }

Provisa scambia modelli semantici con Apache Ossie (specifica 0.2.0.dev0, in incubazione; in precedenza Open Semantic Interchange) tramite un adattatore di confine. Il vocabolario interno di Provisa non viene mai rinominato in quello di Ossie. La specifica dichiara probabili modifiche incompatibili, quindi l'accoppiamento resta confinato all'adattatore.

La specifica in bozza ha cambiato forma senza cambiare la sua stringa di versione. L'adattatore segue la
specifica com'è al commit `642cb894` di `apache/ossie` (2026-10-10): un modello per documento,
con `version`, `name`, `datasets`, `relationships` e `metrics` alla radice del documento, e
un `datatype` enumerato. L'export viene verificato nei test unitari rispetto al
`core-spec/ossie-schema.json` di quel commit.

### Export

La superficie di export canonica è un endpoint HTTP attivo. Deriva il documento Ossie dallo stato attivo a ogni richiesta, senza cache e senza passi di generazione.

```http
GET /admin/ossie
```

La risposta è un documento YAML con `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

La pagina Metriche offre anche un pulsante **Scarica** e un URL dell'endpoint copiabile nel pannello Ossie
Interchange, entrambi diretti allo stesso endpoint.

#### Cosa viene esportato {: #what-is-exported }

L'adattatore fa corrispondere gli oggetti Provisa agli oggetti Ossie come segue:

| Oggetto Provisa | Oggetto Ossie | Note |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; chiavi primarie/uniche dalla configurazione delle colonne e da `UniqueConstraint` |
| `Column` | `field` | `expression` = riferimento di colonna (dialetto ANSI_SQL); le colonne temporali ricevono `dimension.is_time: true` |
| `Relationship` | `relationship` | L'alias viene usato come nome quando è impostato; le relazioni calcolate (con destinazione funzione) vengono saltate |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (mappato come quello di una colonna), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Solo andata e ritorno; altri strumenti possono ignorarlo |

Una relazione su più colonne elenca ogni colonna in `from_columns` e `to_columns`. Un
modello senza tabelle viene rifiutato con `422`: un documento Ossie ha almeno un dataset.

Governance, RLS, lineage e semantica dei grafi non vengono esportati. Possono viaggiare nello slot facoltativo
`provisa` di custom_extensions per la fedeltà dell'andata e ritorno, ma l'interscambio non dipende mai dalla lettura che ne fanno altri
strumenti.

Un tipo di colonna fuori dal vocabolario di Ossie viene scritto come `Opaque`, con il nome proprio del tipo nell'estensione
`provisa` del campo, come la specifica prescrive per un tipo noto per cui non ha un
membro. Una colonna senza tipo registrato non indica alcun `datatype`. L'adattatore non fa mai corrispondere un tipo che la
colonna non ha.

#### Mappatura dei tipi {: #type-mapping }

| Tipo Provisa / sorgente | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| qualsiasi altro | `Opaque`, con il nome del tipo nell'estensione `provisa` |

Il `Decimal` di Ossie non porta precisione né scala, quindi `numeric(10,2)` viene esportato come `Decimal`
e torna da un import come `decimal`.

### Import

L'import accetta un documento Ossie (YAML o JSON) e restituisce proposte di registrazione. Nulla viene registrato automaticamente, e le definizioni importate non saltano mai il passo di revisione.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Il server analizza il documento con `parse_ossie_model`, che convalida la struttura e restituisce una
dataclass `OssieImport` contenente le tabelle, le relazioni e le metriche proposte come semplici dizionari.
Qualsiasi problema strutturale è un `400` con un errore indicato per percorso, ad es.
`ossie import: missing $.datasets[1].source`.

L'importatore legge solo la forma piatta del documento. Tre cose vengono rifiutate per nome:

- Un documento che avvolge il suo modello in `semantic_model`, la forma che la specifica in bozza aveva prima di
  settembre 2026. L'errore dice che il documento è precedente alla forma piatta e come convertirlo.
- Una `version` diversa da `0.2.0.dev0`.
- Un `datatype` che non è membro del vocabolario di Ossie; l'errore indica il percorso ed elenca
  i membri.

Una colonna o una metrica proposta porta il tipo che rappresenta il suo datatype Ossie (`Integer` propone
`bigint`, `DateTime` propone `timestamp`). `Opaque` propone il tipo che l'estensione `provisa` nomina
quando il documento proviene da Provisa, e nessun tipo altrimenti. Un `ai_context` dato come
oggetto viene letto tramite le sue `instructions`.

Un campo viene proposto come colonna solo quando la sua espressione è una colonna della sorgente. Un campo
la cui espressione è un calcolo non viene proposto; la schermata di revisione lo indica sotto la sua tabella
come non importato. Un campo che rinomina la sua colonna viene proposto come quella colonna, con il nome del
campo come alias della colonna. La `source` di un dataset deve avere tre parti,
`source.schema.table`; una query o un nome più breve viene rifiutato, e l'errore lo dice.

#### Da dbt, Cube, Snowflake e altri livelli semantici {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Il progetto Apache Ossie mantiene convertitori tra Ossie e i formati di modello di altri strumenti. Eseguite
il convertitore del vostro strumento, poi inviate il suo output all'endpoint di import o scegliete il file nella
schermata di revisione qui sotto. Provisa legge il documento Ossie; non legge i file dell'altro strumento.

| Provenienza | Comando del convertitore | Cosa legge |
|---|---|---|
| dbt (livello semantico) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | Il `semantic_manifest.json` che dbt scrive per MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Una directory di modello Cube, o singoli file di cubi e viste |

Lo stesso repository contiene convertitori per Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData e altri; ognuno ha il proprio README.

Due limiti da conoscere prima di convertire:

- Il convertitore dbt legge solo il manifesto del livello semantico. Un progetto dbt senza modelli
  semantici non ha nulla da convertire. Scarta le metriche di conversione, le metriche private e le
  entità a chiave naturale, e stampa un avviso per ciascuna.

- L'output di un convertitore segue la specifica come la ha il commit proprio di quel convertitore. Provisa legge
  la forma piatta del commit `642cb894` di `apache/ossie`; un convertitore precedente a settembre 2026
  scrive l'involucro `semantic_model`, che Provisa rifiuta per nome.

Entrambi i percorsi sono stati eseguiti il 2026-10-10 con i convertitori installati dal commit `642cb894` di `apache/ossie`
(non sono pubblicati come pacchetti; si installano dalle directory `converters/dbt` e
`converters/cube` del repository e richiedono Python 3.11 o successivo). Un modello di
due tabelle, una relazione e una metrica è stato esportato da Provisa, convertito in un manifesto semantico dbt
e in un modello Cube, riconvertito in Ossie da `ossie-dbt msi-to-ossie` e
`ossie-cube import`, e letto da `parse_ossie_model`:

| Percorso | Risultato |
|---|---|
| Modello Cube → Ossie → Provisa | Letto. Tabelle, chiavi, tipi di colonna, la relazione e la metrica sono tornati tutti; il nome della relazione era quello del convertitore (`orders_to_customers`). |
| Manifesto semantico dbt → Ossie → Provisa | Letto. Tabelle, chiavi primarie e la metrica sono tornate. I tipi di colonna no (l'output del convertitore non ne indica), e la relazione è tornata come il convertitore l'aveva derivata dalle entità dbt, che non era quella registrata. Rivedete le relazioni prima di applicare. |

Il convertitore dbt rifiuta un modello in cui una tabella ha un vincolo di unicità su più
colonne («MetricFlow entities cannot represent composite keys losslessly»).

#### La schermata di revisione {: #the-review-screen }

Nell'interfaccia, il pulsante **Importa** (pagina Metriche → pannello Ossie Interchange) apre un selettore di file. Dopo che il documento è stato inviato e analizzato, si apre una finestra di revisione con ogni tabella, relazione e metrica proposta elencata come elemento selezionato. Il modellatore può deselezionare qualsiasi cosa per escluderla. Facendo clic su **Applica** si registrano gli elementi selezionati tramite le mutazioni di registrazione esistenti: prima le tabelle, poi le relazioni (che fanno riferimento alle tabelle), poi le metriche.

Il ruolo di modellazione e la cronologia memorizzati in un documento Ossie esportato da Provisa superano correttamente
l'import.

### File di sources dbt {: #dbt-sources-file }

Per un progetto dbt che non usa il livello semantico, Provisa scrive direttamente un `sources.yml` dbt.
Viene derivato dal modello attivo a ogni richiesta.

```http
GET /admin/dbt/sources?role=<role id>
```

La risposta è YAML con `Content-Disposition: attachment; filename=provisa.sources.yml`. Il
chiamante ha bisogno di `org_settings`, e `role` deve essere un ruolo che detiene; il titolare di
`access_config` può nominare qualsiasi ruolo. Il pannello di interscambio della pagina Metriche offre lo stesso download
per il ruolo in uso.

Il file elenca ogni tabella servita al ruolo, sotto una source dbt per sorgente e schema (nominata
con l'id della sorgente, o `<source>_<schema>` quando una sorgente ha tabelle in più di uno schema),
con la descrizione della tabella, le descrizioni delle colonne servite e questi test:

| Il modello indica | Il file scrive |
|---|---|
| Una chiave primaria di una colonna | `unique` e `not_null` su quella colonna |
| Una chiave primaria di più colonne | `not_null` su ogni colonna |
| Un vincolo di unicità di una colonna | `unique` su quella colonna |
| Una relazione tra due tabelle entrambe servite al ruolo | `relationships` sulla colonna di origine, verso `source('<source>', '<table>')` |

Una colonna non servita al ruolo è assente, così come una tabella senza colonne servite. Una chiave su
più colonne non ha un test dbt standard; il commento iniziale del file le nomina tutte, e per esse non viene scritto
alcun test. Il modello non registra la nullabilità, quindi `not_null` compare solo sulle colonne chiave.
Il file non porta metriche, sicurezza di riga, mascheramento, ruoli o lineage, e il suo commento iniziale
lo dice.

Un file generato da un modello di due tabelle e una relazione è stato analizzato da dbt-core 1.12.5
con dbt-duckdb 1.11.0: due source e cinque test, nessun avviso. dbt-core 1.9.11 non legge
i suoi test `relationships`, che annidano i loro argomenti sotto `arguments`; le versioni intermedie
non sono state provate.

---

## Metriche tra protocolli {: #metrics-across-protocols }

La definizione di una metrica governata — la sua espressione, descrizione e `ai_context` — viaggia
con il valore verso ogni superficie di query attraverso un'unica espansione del compilatore. Non ci
sono copie. Il compilatore riserva lo schema `metrics` per l'accesso SQL; ogni protocollo aggiunge
poi il proprio canale di metadati.

### SQL / pgwire

Indirizza qualsiasi metrica come una relazione virtuale nello schema `metrics`. Le colonne di
dimensione selezionate diventano il GROUP BY:

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

Il compilatore espande la forma `metrics.<name>` nell'aggregato raggruppato reale prima che venga
eseguita la governance. Le descrizioni delle colonne vengono esposte come voci `pg_description`,
quindi DBeaver e `\d+` di psql le mostrano.

`SELECT *` viene rifiutato, quindi nominate le colonne che vi servono.

### GraphQL

Le metriche vengono proiettate all'interno del campo radice `_aggregate` come blocco `metrics`.

Il testo della definizione (`description`, `ai_context`) appare nella documentazione di introspezione
GraphQL, quindi gli strumenti sensibili allo schema e la generazione di codice lo recepiscono
automaticamente.

### MCP (agenti IA) {: #mcp-ai-agents }

Due strumenti espongono le metriche ai client MCP:

- **`list_metrics`** — restituisce tutte le metriche governate visibili per la sessione, con `name`,
  `description` e `ai_context`.
- **`query_metric`** — accetta un nome di metrica più un elenco di dimensioni e richiama il percorso
  SQL semantico del compilatore, restituendo il risultato aggregato.

Gli agenti che chiamano `list_metrics` prima di costruire una query selezionano una metrica governata
per nome anziché scrivere SQL di aggregazione a mano. Il campo `ai_context` è il punto in cui inserire
il testo di definizione che guida la selezione corretta.

### Arrow Flight

Le metriche sono indirizzabili come descrittori di volo di metrica che restituiscono tabelle Arrow.

Usa la stessa forma SQL `metrics.<name>` tramite il percorso standard del ticket Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Richiama una metrica usando la procedura `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Le tabelle di fatti e dimensioni portano le etichette di nodo `:Fact` e `:Dimension` nel grafo
federato, quindi Bloom rappresenta automaticamente la forma a stella.

### Query in linguaggio naturale {: #natural-language-queries }

Il comparatore di schema in linguaggio naturale risolve il vocabolario delle metriche nelle domande
in linguaggio naturale direttamente in una metrica più dimensioni, quindi genera SQL semantico.

Le tabelle di fatti sono etichettate `[fact]` nel prompt in linguaggio naturale; le tabelle di
dimensione sono etichettate `[dimension]`. Il comparatore privilegia i percorsi di join da fatto a
dimensione durante la risoluzione delle domande.

### Streaming

Combina `view_metrics` con `materialize` per mantenere aggiornata una vista di metriche quando
cambiano i suoi input, usando la macchina di materializzazione esistente. Non è richiesta alcuna
nuova pipeline.

### Osservabilità (OTel) {: #observability-otel }

Le valutazioni delle metriche vengono tracciate ed esportabili come metriche OpenTelemetry.
