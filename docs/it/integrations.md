# Integrazioni

## Scegliere un percorso di connessione

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

## pgwire — Driver PostgreSQL nativo

Provisa implementa il protocollo di rete di PostgreSQL (versione protocollo 3.0). Qualsiasi client che parla PostgreSQL si connette senza un driver personalizzato.

Abilitalo impostando `PROVISA_PGWIRE_PORT` (ad esempio `5433`) prima di avviare Provisa. Disabilitato se non impostato o `0`.

### Perché pgwire invece di JDBC?

Il driver JDBC usa Arrow Flight come trasporto e richiede la distribuzione di `provisa-jdbc.jar`. pgwire non richiede nulla — se hai già `psql`, DBeaver, SQLAlchemy o un driver JDBC PG, hai finito. È il percorso a minor attrito per i carichi di lavoro solo SQL.

JDBC è la scelta giusta per gli strumenti di BI che dispongono di una procedura guidata di connessione JDBC integrata e beneficiano dello streaming columnar di Arrow Flight per grandi set di risultati. pgwire accetta SQL libero sull'intero schema pubblicato — le stesse query, con un costo di configurazione inferiore.

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

### Autenticazione

Il campo `password` del pacchetto di avvio trasporta la credenziale, e *cosa sia* la credenziale determina il metodo: un token di accesso personale, un token bearer OIDC oppure una password verso il provider configurato. Con il provider `basic` e `auth.scram: true` la password viene dimostrata tramite SCRAM-SHA-256 anziché inviata. I certificati client sono supportati. In modalità trust (`none`) il nome utente viene mappato direttamente a un ruolo e la password viene ignorata.

La tabella completa interfaccia × metodo si trova nel [Modello di sicurezza](security.md#superfici-e-credenziali). MD5 non è supportato; abilita TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) quando operi su una rete non attendibile.

### Limitazioni

- Solo SQL. GraphQL e Cypher non sono accettati tramite pgwire.
- Non è di sola lettura. `COPY ... FROM STDIN` inserisce righe nelle origini `postgresql`, `mysql`, `sqlite` e `mariadb`, e il DDL è supportato (vedi sotto).
- Il DDL (`CREATE`, `ALTER`, `DROP`) è supportato e viene instradato verso il percorso Trino o diretto; la nuova tabella viene registrata nel contesto di compilazione ed è immediatamente interrogabile. `COPY ... TO STDOUT` (esportazione) e `COPY ... FROM STDIN` (importazione) sono supportati nei formati `text` e `csv`.
- Le query a `information_schema` e `pg_catalog` vengono intercettate e risolte da uno shim di catalogo DuckDB — gli strumenti di individuazione dello schema funzionano correttamente.

---

## Driver JDBC

Il driver JDBC di Provisa usa Arrow Flight come trasporto sottostante. È il percorso consigliato per gli strumenti di BI con una procedura guidata di connessione JDBC.

### Connessione

Scarica [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre l'ultima versione) e aggiungilo al percorso dei driver del tuo strumento.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

L'autenticazione usa le proprietà JDBC standard `user` / `password`. Provisa autentica le credenziali rispetto al provider di autenticazione configurato e assegna il ruolo — il client non sceglie il proprio ruolo.

### Configurazione degli strumenti di BI

**Tableau**

1. Gestisci → Driver → Installa Provisa JDBC
2. Connetti → Altri database (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Inserisci nome utente e password quando richiesto

**DBeaver** (percorso JDBC — per il percorso pgwire vedi sopra)

1. Database → Nuova connessione → JDBC
2. Driver: aggiungi `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Inserisci nome utente e password nella scheda Autenticazione

**Power BI** — usa il gateway ODBC con il ponte Provisa JDBC-ODBC (incluso nel programma di installazione).

---

## Client Arrow Flight

Arrow Flight (porta 8815) è il percorso consigliato per gli strumenti dati che lo supportano. I risultati fluiscono in streaming come RecordBatch Arrow senza essere materializzati nella memoria di Provisa.

### Python (`provisa-client`)

Il percorso Python consigliato — incapsula sia GraphQL che Arrow Flight:

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

### Python (PyArrow puro)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight trasporta la propria credenziale nel payload JSON, come campo `token` — un token bearer del provider o un token di accesso personale. Sia l'handshake sia ogni ticket lo accettano, ed entrambi lo validano allo stesso modo, quindi un client che si è autenticato all'handshake presenta comunque il token a ogni `do_get`. Un campo `role` accanto *richiede* un ruolo; il server deriva i ruoli consentiti dell'identità e sostituisce il valore autorizzato, quindi una stringa di ruolo in un ticket non è mai l'identità. (REQ-1263) Vedi [Modello di sicurezza](security.md#superfici-e-credenziali).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
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

## Protobuf gRPC (porta 50051)

Percorso servizio a servizio. Provisa genera un `.proto` per ruolo all'avvio — ogni ruolo vede solo le tabelle e le colonne a cui ha accesso.

Scarica il proto per il tuo ruolo:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Usa `grpc_server_reflection` per individuare lo schema programmaticamente.

Ogni RPC deve portare una credenziale nella chiave di metadati `authorization` — un token del provider o un token di accesso personale. `x-provisa-role` richiede un ruolo dall'insieme consentito dell'identità; non è una credenziale e non lo è mai stata. I certificati client sono supportati. Vedi [Modello di sicurezza](security.md#superfici-e-credenziali).

Le query in streaming emettono un messaggio per riga; le mutazioni sono unarie.

---

## Invocare comandi tra protocolli

Un **comando** è una funzione tracciata registrata o un webhook — un elemento invocabile registrato nel livello semantico di Provisa con un `kind` (`query` o `mutation`) e un `impl_kind` che descrive come viene eseguito. Ogni superficie instrada le invocazioni attraverso un unico esecutore governato (`invoke_tracked_function`) che applica `writable_by` e la governance in modo uniforme (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Cosa viene eseguito | Campi di binding |
| ------------ | ----------- | --------------- |
| `source_procedure` | Stored procedure su un'origine registrata (predefinito) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script lato server | `script` |
| `http` | Chiamata HTTP in uscita | `url`, `method` |
| `grpc` | Chiamata gRPC in uscita verso un server esterno | `target`, `method` |
| `python` | Elemento invocabile Python ospitato da Provisa (REQ-885) | `callable` (ad esempio `demo.py_functions:random_dataset`) |

Quando un comando dichiara un `return_schema` (JSON Schema con `type: array, items: object`), è a restituzione di insieme — ogni superficie lo proietta come un set di righe tipizzato. I comandi dimostrativi `random_python_set` (impl_kind `python`) e `random_grpc_set` (impl_kind `grpc`) illustrano sia un elemento invocabile ospitato sia un ponte gRPC esterno che restituisce righe con valori casuali; entrambi sono registrati in `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Matrice dei protocolli

| Superficie | Sintassi | Esempio |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → campo Query; `kind=mutation` → campo Mutation; con prefisso di dominio quando `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` o `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — gli argomenti posizionali corrispondono ai nomi degli argomenti dichiarati | `CALL random_python_set(3, 7)` |
| Provisa gRPC (porta 50051) | Unario `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Il campo `kind` controlla solo il posizionamento in GraphQL — le superfici SQL, Cypher, Bolt e gRPC accettano allo stesso modo comandi `query` e `mutation`.

---

## Apollo Federation

Provisa può fungere da subgraph Federation v2, esponendo il proprio schema pubblicato a un Apollo Router o Apollo Gateway.

### Configurazione

Abilita la federazione in `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa genera automaticamente le direttive `@key` sulle colonne di chiave primaria e `@external`/`@provides` sulle relazioni tra subgraph.

### Registrazione con Apollo Router

Nel tuo `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Esegui `rover supergraph compose --config supergraph.yaml` per generare lo schema del supergraph.

### Entità

Provisa risponde alle query `_entities` per i join tra subgraph. Qualsiasi tabella con una chiave primaria è automaticamente risolvibile come entità Federation.

---

## Import Hasura v2 / DDN

Consulta [docs/import.md](import.md) per la migrazione da Hasura a Provisa.

---

## Kafka

Consulta [docs/sources.md](sources.md#origini-kafka) per la configurazione dei topic Kafka come tabelle di sola lettura e destinazioni dei risultati delle query.

---

## Controlli di qualità dei dati (REQ-1443)

Soda Core e Great Expectations si connettono a Provisa nello stesso modo di qualsiasi altro client
postgres — tramite pgwire. Questa è l'intera integrazione: il checker mantiene un unico driver
postgres e analizza la vista federata, così una tabella Snowflake, una tabella Iceberg e una
collection Mongo vengono tutte controllate dallo stesso dialetto di contratto senza un checker per
sistema. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

La scansione viene eseguita in un interprete figlio — `python -m provisa.dq.worker` — l'unico
punto in cui vengono importati `soda_core` o `great_expectations`. Nulla viene collegato al
processo del server, e un crash del checker abbatte un sottoprocesso anziché l'event loop.
[tool-verified: `provisa/dq/runner.py` `build_command`]

I risultati della scansione atterrano come normali righe di origine, quindi cadenza, freschezza,
eventi, derivazione, governance, RLS, la griglia e l'esportazione si applicano tutti senza un
secondo meccanismo. La stesura del contratto, l'involucro del risultato e la registrazione
derivata sono trattati in [docs/sources.md](sources.md#controlli-di-qualita-dei-dati-req-1443).

### Installare un checker

Nessuna delle due librerie viene distribuita per impostazione predefinita. Il programma di
installazione chiede quale si desidera, e la risposta diventa `dq_checker: none|soda|gx` in
`~/.provisa/config.yaml`. Sul livello Docker `scripts/provisa` la trasforma nell'argomento di
build `PROVISA_EXTRAS`; sul livello nativo `first-launch.sh` installa nel venv l'extra pyproject
corrispondente. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh`
`_native_extras`]

| `dq_checker` | Libreria | Licenza | Piano cloud ospitato |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Rifiutato (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Consentito |

La Elastic License 2.0 vieta di fornire il software a terzi come servizio ospitato, che è
esattamente ciò che significherebbe eseguire Soda all'interno del piano SaaS per conto di un
tenant. Una distribuzione ospitata che desideri Soda punta a un endpoint Soda gestito
direttamente dall'operatore. Vedere [docs/configuration.md](configuration.md#controlli-di-qualita-dei-dati-soda-great_expectations)
per le chiavi di connessione.

---

## Interscambio semantico Apache Ossie (REQ-1316)

Provisa scambia modelli semantici con Apache Ossie (specifica 0.2.0.dev0, in incubazione;
in precedenza Open Semantic Interchange) tramite un adattatore di confine. Il vocabolario interno di
Provisa non viene mai rinominato secondo quello di Ossie — la specifica dichiara probabili modifiche
non retrocompatibili, quindi l'accoppiamento è confinato all'adattatore.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Esportazione

La superficie di esportazione canonica è un endpoint HTTP live. Deriva il documento Ossie dallo
stato live a ogni richiesta — nessuna cache, nessun passaggio di generazione.

```http
GET /admin/ossie
```

La risposta è un documento YAML con `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

La pagina Metrics offre anche un pulsante **Download** e un URL dell'endpoint copiabile nel pannello
Ossie Interchange, entrambi rivolti allo stesso endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Cosa viene esportato

L'adattatore mappa gli oggetti Provisa su oggetti Ossie come segue:

| Oggetto Provisa | Oggetto Ossie | Note |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; chiavi primarie/univoche dalla configurazione delle colonne e da `UniqueConstraint` |
| `Column` | `field` | `expression` = riferimento di colonna (dialetto ANSI_SQL); le colonne temporali ottengono `dimension.is_time: true` |
| `Relationship` | `relationship` | L'alias viene usato come nome quando impostato; le relazioni calcolate (target-funzione) vengono saltate |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — senza perdita per progettazione |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Solo per il round-trip; altri strumenti possono ignorarlo |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governance, sicurezza a livello di riga, derivazione e semantica del grafo non vengono esportate.
Possono viaggiare nello slot opzionale `provisa` di custom_extensions per la fedeltà del round-trip,
ma l'interscambio non dipende mai dal fatto che altri strumenti lo leggano. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

I tipi di colonna Provisa sconosciuti passano invariati; l'adattatore non mappa mai silenziosamente
su un tipo errato. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Mappatura dei tipi

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Tipo Provisa / origine | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| qualsiasi altro | passato invariato |

### Importazione

L'importazione accetta un documento Ossie (YAML o JSON) e restituisce proposte di registrazione.
Nulla viene registrato automaticamente — le definizioni importate non aggirano mai il passaggio di
revisione.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Il server analizza il documento con `parse_ossie_model`, che valida la struttura e restituisce una
dataclass `OssieImport` contenente tabelle, relazioni e metriche proposte come dizionari semplici.
Qualsiasi problema strutturale genera un `400` con un errore con percorso nominato, ad esempio
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### La schermata di revisione

Nell'interfaccia, il pulsante **Importa** (pagina Metrics → pannello Ossie Interchange) apre un
selettore di file. Dopo che il documento è stato inviato e analizzato, si apre un modal di revisione
con ogni tabella, relazione e metrica proposta elencata come elemento selezionato. Il modellatore può
deselezionare qualsiasi elemento per escluderlo. Facendo clic su **Applica** vengono registrati gli
elementi selezionati tramite le mutazioni di registrazione esistenti — prima le tabelle, poi le
relazioni (che fanno riferimento alle tabelle), quindi le metriche.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Il ruolo di modellazione e la cronologia memorizzati in un documento Ossie esportato da Provisa
effettuano correttamente il round-trip attraverso l'importazione. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Metriche tra protocolli (REQ-1319)

La definizione di una metrica governata — la sua espressione, descrizione e `ai_context` — viaggia
con il valore verso ogni superficie di query attraverso un'unica espansione del compilatore. Non ci
sono copie. Il compilatore riserva lo schema `metrics` per l'accesso SQL; ogni protocollo aggiunge
poi il proprio canale di metadati.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

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
quindi DBeaver e `\d+` di psql le mostrano. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` viene rifiutato — nomina esplicitamente le colonne.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Le metriche vengono proiettate all'interno del campo radice `_aggregate` come blocco `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Il testo della definizione (`description`, `ai_context`) appare nella documentazione di introspezione
GraphQL, quindi gli strumenti sensibili allo schema e la generazione di codice lo recepiscono
automaticamente.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (agenti IA)

Due strumenti espongono le metriche ai client MCP:

- **`list_metrics`** — restituisce tutte le metriche governate visibili per la sessione, con `name`,
  `description` e `ai_context`.
- **`query_metric`** — accetta un nome di metrica più un elenco di dimensioni e richiama il percorso
  SQL semantico del compilatore, restituendo il risultato aggregato.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Gli agenti che chiamano `list_metrics` prima di costruire una query selezionano una metrica governata
per nome anziché scrivere SQL di aggregazione a mano. Il campo `ai_context` è il punto in cui inserire
il testo di definizione che guida la selezione corretta.

### Arrow Flight

Le metriche sono indirizzabili come descrittori di volo di metrica che restituiscono tabelle Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Usa la stessa forma SQL `metrics.<name>` tramite il percorso standard del ticket Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Richiama una metrica usando la procedura `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Le tabelle di fatti e dimensioni portano le etichette di nodo `:Fact` e `:Dimension` nel grafo
federato, quindi Bloom rappresenta automaticamente la forma a stella.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Query in linguaggio naturale

Il comparatore di schema in linguaggio naturale risolve il vocabolario delle metriche nelle domande
in linguaggio naturale direttamente in una metrica più dimensioni, quindi genera SQL semantico. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Le tabelle di fatti sono etichettate `[fact]` nel prompt in linguaggio naturale; le tabelle di
dimensione sono etichettate `[dimension]`. Il comparatore privilegia i percorsi di join da fatto a
dimensione durante la risoluzione delle domande.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Combina `view_metrics` con `materialize` e un sink Kafka per produrre un output di metrica push-on-
change usando la macchina di materializzazione esistente. Non è richiesta alcuna nuova pipeline.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Osservabilità (OTel)

Le valutazioni delle metriche vengono tracciate ed esportabili come metriche OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
