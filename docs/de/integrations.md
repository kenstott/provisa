# Integrationen

## Den richtigen Verbindungspfad wählen

| Client-Typ | Empfohlener Pfad | Warum |
| ------------- | ----------------- | ----- |
| BI-Tools (Tableau, Power BI, Looker) | JDBC | Arrow-Flight-Columnar-Streaming über die Leitung; BI-Tools haben einen eingebauten JDBC-Assistenten und profitieren von der Hochdurchsatz-Columnar-Zustellung bei großen Ergebnismengen |
| psql, DBeaver, jedes PG-kompatible Tool | pgwire (nativer PG-Treiber) | Reibungsloser Standard — kein spezieller Treiber nötig; nutzen Sie, was Sie bereits haben |
| Python-Data-Stack (pandas, pyarrow) | `provisa-client` oder rohes ADBC | Streaming von Arrow-Batches; kein Overhead durch Zeilenserialisierung |
| Spark, DuckDB, Hochdurchsatz-Pipelines | Arrow Flight (ADBC) | Unbegrenztes Columnar-Streaming direkt in den Arrow-Speicher |
| Service-zu-Service (typisierte Contracts) | Protobuf gRPC | Pro Rolle generiertes Proto; Streaming von Zeilen; Typsicherheit |
| Webanwendungen, Scripting | HTTP (`/data/graphql`, `/data/sql`) | Kein Treiber; Standard-HTTP; volle Wahl der Abfragesprache |
| REST-Clients (JSON:API-Standard) | `GET /data/jsonapi/{table}` | JSON:API-v1.0-Envelope; Sparse Fieldsets, Paginierung, Filterung über Query-Parameter; kein Treiber |

---

## pgwire — Nativer PostgreSQL-Treiber

Provisa implementiert das PostgreSQL-Wire-Protokoll (Protokollversion 3.0). Jeder Client, der PostgreSQL spricht, verbindet sich ohne speziellen Treiber.

Aktivieren Sie es, indem Sie `PROVISA_PGWIRE_PORT` (z. B. `5433`) setzen, bevor Sie Provisa starten. Deaktiviert, wenn nicht gesetzt oder `0`.

### Warum pgwire statt JDBC?

Der JDBC-Treiber nutzt Arrow Flight als Transport und erfordert das Deployment von `provisa-jdbc.jar`. pgwire erfordert nichts — wenn Sie bereits `psql`, DBeaver, SQLAlchemy oder einen PG-JDBC-Treiber haben, sind Sie fertig. Es ist der reibungsärmere Pfad für reine SQL-Workloads.

JDBC ist die richtige Wahl für BI-Tools mit eingebautem JDBC-Verbindungsassistenten, die vom Columnar-Streaming von Arrow Flight bei großen Ergebnismengen profitieren. pgwire akzeptiert freies SQL gegen das gesamte veröffentlichte Schema — dieselben Abfragen, geringere Einrichtungskosten.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Benutzername/Passwort wie in Provisa konfiguriert
4. Kein zusätzlicher Treiber-Download erforderlich

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Oder mit `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Authentifizierung

Das `password`-Feld des Startup-Pakets trägt das Credential, und was das Credential *ist*,
bestimmt die Methode: ein Personal Access Token, ein OIDC-Bearer-Token oder ein Passwort gegen
den konfigurierten Provider. Unter dem Provider `basic` mit `auth.scram: true` wird das Passwort
über SCRAM-SHA-256 nachgewiesen, statt gesendet zu werden. Client-Zertifikate werden
unterstützt. Im Trust-Modus (`none`) wird der Benutzername direkt auf eine Rolle abgebildet und
das Passwort ignoriert.

Die vollständige Oberfläche-×-Methode-Tabelle steht im [Sicherheitsmodell](security.md#oberflachen-und-anmeldeinformationen). MD5 wird nicht unterstützt; aktivieren Sie TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`), wenn Sie über ein nicht vertrauenswürdiges Netzwerk laufen.

### Einschränkungen

- Nur SQL. GraphQL und Cypher werden über pgwire nicht akzeptiert.
- Nicht schreibgeschützt. `COPY ... FROM STDIN` fügt Zeilen in `postgresql`-, `mysql`-, `sqlite`- und `mariadb`-Quellen ein, und DDL wird unterstützt (siehe unten).
- DDL (`CREATE`, `ALTER`, `DROP`) wird unterstützt und an den Trino- oder Direktpfad weitergeleitet; die neue Tabelle wird in den Kompilierungskontext registriert und ist sofort abfragbar. `COPY ... TO STDOUT` (Export) und `COPY ... FROM STDIN` (Import) werden in den Formaten `text` und `csv` unterstützt.
- `information_schema`- und `pg_catalog`-Abfragen werden abgefangen und aus einem DuckDB-Katalog-Shim beantwortet — Schema-Erkennungstools funktionieren korrekt.

---

## JDBC-Treiber

Der Provisa-JDBC-Treiber nutzt Arrow Flight als zugrunde liegenden Transport. Er ist der empfohlene Pfad für BI-Tools mit JDBC-Verbindungsassistenten.

### Verbindung

Laden Sie [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) herunter (immer die neueste Version) und fügen Sie sie dem Treiberpfad Ihres Tools hinzu.

JDBC-URL:

```yaml
jdbc:provisa://<host>:8815
```

Die Authentifizierung nutzt die Standard-JDBC-Eigenschaften `user`/`password`. Provisa authentifiziert die Anmeldedaten gegen den konfigurierten Auth-Provider und weist die Rolle zu — der Client wählt seine Rolle nicht selbst.

### BI-Tool-Einrichtung

**Tableau**

1. Manage → Drivers → Provisa JDBC installieren
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie bei Aufforderung Ihren Benutzernamen und Ihr Passwort ein

**DBeaver** (JDBC-Pfad — für den pgwire-Pfad siehe oben)

1. Database → New Connection → JDBC
2. Treiber: `provisa-jdbc.jar` hinzufügen
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie Ihren Benutzernamen und Ihr Passwort im Authentication-Tab ein

**Power BI** — nutzen Sie das ODBC-Gateway mit der Provisa-JDBC-ODBC-Brücke (im Installer enthalten).

---

## Arrow-Flight-Clients

Arrow Flight (Port 8815) ist der empfohlene Pfad für Datentools, die es unterstützen. Ergebnisse streamen als Arrow-RecordBatches, ohne sich im Provisa-Speicher zu materialisieren.

### Python (`provisa-client`)

Der empfohlene Python-Pfad — kapselt sowohl GraphQL als auch Arrow Flight:

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

Die vollständige Referenz einschließlich DB-API 2.0, SQLAlchemy-Dialekt und ADBC finden Sie unter [docs/python-client.md](python-client.md).

### Python (rohes PyArrow)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight trägt sein Credential in der JSON-Payload als `token`-Feld — ein Provider-Bearer-Token
oder ein Personal Access Token. Sowohl der Handshake als auch jedes Ticket akzeptieren es, und
beide validieren es auf dieselbe Weise, sodass ein Client, der sich beim Handshake authentifiziert
hat, das Token weiterhin bei jedem `do_get` vorlegt. Ein `role`-Feld daneben *fordert* eine Rolle
an; der Server leitet die zulässigen Rollen der Identität ab und ersetzt den autorisierten Wert,
sodass ein Rollenstring in einem Ticket nie die Identität ist. (REQ-1263) Siehe [Sicherheitsmodell](security.md#oberflachen-und-anmeldeinformationen).

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

## Protobuf gRPC (Port 50051)

Service-zu-Service-Pfad. Provisa generiert beim Start ein `.proto` pro Rolle — jede Rolle sieht nur die Tabellen und Spalten, auf die sie Zugriff hat.

Das Proto für Ihre Rolle herunterladen:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Nutzen Sie `grpc_server_reflection`, um das Schema programmatisch zu erkunden.

Jeder RPC muss ein Credential im Metadaten-Schlüssel `authorization` tragen — ein Provider-Token
oder ein Personal Access Token. `x-provisa-role` fordert eine Rolle aus der zulässigen Menge der
Identität an; es ist kein Credential und war es nie. Client-Zertifikate werden unterstützt. Siehe
[Sicherheitsmodell](security.md#oberflachen-und-anmeldeinformationen).

Streaming-Abfragen liefern eine Nachricht pro Zeile; Mutationen sind unär.

---

## Commands über Protokolle hinweg aufrufen

Ein **Command** ist eine registrierte getrackte Funktion oder ein Webhook — ein in der
semantischen Schicht von Provisa registrierter Callable mit einem `kind` (`query` oder
`mutation`) und einem `impl_kind`, der beschreibt, wie er läuft. Jede Oberfläche leitet Aufrufe
durch einen einzigen governten Executor (`invoke_tracked_function`), der `writable_by` und
Governance einheitlich durchsetzt (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Was ausgeführt wird | Bindungsfelder |
| ------------ | ----------- | --------------- |
| `source_procedure` | Stored Procedure auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Script | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehosteter Python-Callable (REQ-885) | `callable` (z. B. `demo.py_functions:random_dataset`) |

Wenn ein Command ein `return_schema` (JSON Schema mit `type: array, items: object`) deklariert,
ist er set-returning — jede Oberfläche projiziert ihn als typisierte Zeilenmenge. Die
Demo-Commands `random_python_set` (impl_kind `python`) und `random_grpc_set` (impl_kind `grpc`)
veranschaulichen sowohl einen gehosteten Callable als auch eine externe gRPC-Brücke, die Zeilen
mit Zufallswerten liefert; beide sind in `config/provisa-install.yaml` registriert.
[tool-verified: `config/provisa-install.yaml:809-856`]

### Protokollmatrix

| Oberfläche | Syntax | Beispiel |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query-Feld; `kind=mutation` → Mutation-Feld; domänenpräfixiert bei `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` oder `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / Treiber) | `CALL fn(args)` — positionale Argumente werden auf deklarierte Argumentnamen abgebildet | `CALL random_python_set(3, 7)` |
| Provisa gRPC (Port 50051) | Unärer `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Das Feld `kind` steuert nur die GraphQL-Platzierung — SQL-, Cypher-, Bolt- und
gRPC-Oberflächen akzeptieren `query`- und `mutation`-Commands identisch.

---

## Apollo Federation

Provisa kann als Federation-v2-Subgraph fungieren und sein veröffentlichtes Schema einem Apollo Router oder Apollo Gateway exponieren.

### Einrichtung

Federation in `config.yaml` aktivieren:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa generiert automatisch `@key`-Direktiven auf Primärschlüsselspalten und `@external`/`@provides` auf spaltenübergreifende Subgraph-Relationships.

### Bei Apollo Router registrieren

In Ihrer `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Führen Sie `rover supergraph compose --config supergraph.yaml` aus, um das Supergraph-Schema zu generieren.

### Entities

Provisa beantwortet `_entities`-Abfragen für subgraphübergreifende Joins. Jede Tabelle mit einem Primärschlüssel ist automatisch als Federation-Entity auflösbar.

---

## Hasura v2 / DDN Import

Siehe [docs/import.md](import.md) für die Migration von Hasura zu Provisa.

---

## Kafka

Siehe [docs/sources.md](sources.md#kafka-quellen) für die Konfiguration von Kafka-Topics als schreibgeschützte Tabellen und Abfrageergebnis-Senken.

---

## Data-Quality-Checker (REQ-1443)

Soda Core und Great Expectations verbinden sich mit Provisa genauso wie jeder andere
Postgres-Client — über pgwire. Das ist die gesamte Integration: Der Checker hält einen
Postgres-Treiber und durchsucht die föderierte View, sodass eine Snowflake-Tabelle, eine
Iceberg-Tabelle und eine Mongo-Collection alle mit demselben Contract-Dialekt geprüft werden,
ohne einen systemspezifischen Checker. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

Der Scan läuft in einem Kind-Interpreter — `python -m provisa.dq.worker` —, der der einzige Ort
ist, an dem `soda_core` oder `great_expectations` importiert wird. Nichts wird in den
Serverprozess gelinkt, und ein Checker-Absturz reißt einen Subprozess nieder, nicht die
Event-Loop. [tool-verified: `provisa/dq/runner.py` `build_command`]

Scan-Ergebnisse landen als gewöhnliche Quellzeilen, sodass Taktung, Aktualität, Events, Lineage,
Governance, RLS, das Grid und der Export alle ohne einen zweiten Mechanismus greifen.
Contract-Autorisierung, der Ergebnis-Envelope und die abgeleitete Registrierung werden in
[docs/sources.md](sources.md#data-quality-checker-req-1443) behandelt.

### Einen Checker installieren

Keine der beiden Bibliotheken wird standardmäßig ausgeliefert. Der Installer fragt, welche Sie
möchten, und die Antwort wird zu `dq_checker: none|soda|gx` in `~/.provisa/config.yaml`. In der
Docker-Stufe wandelt `scripts/provisa` dies in das Build-Argument `PROVISA_EXTRAS` um; in der
nativen Stufe installiert `first-launch.sh` das passende pyproject-Extra in das venv.
[tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | Bibliothek | Lizenz | Gehostete Cloud-Ebene |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Abgelehnt (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Erlaubt |

Die Elastic License 2.0 untersagt es, die Software Dritten als gehosteten Dienst
bereitzustellen — genau das wäre es, Soda innerhalb der SaaS-Ebene im Namen eines Mandanten
auszuführen. Ein gehostetes Deployment, das Soda nutzen möchte, zeigt auf einen Soda-Endpunkt,
den der Betreiber selbst betreibt. Die Verbindungsschlüssel finden Sie in
[docs/configuration.md](configuration.md#data-quality-checker-soda-great_expectations).

---

## Apache-Ossie-Semantic-Interchange (REQ-1316)

Provisa tauscht semantische Modelle mit Apache Ossie (Spec 0.2.0.dev0, incubating; ehemals Open
Semantic Interchange) über einen Grenzadapter aus. Das interne Vokabular von Provisa wird nie in
das von Ossie umbenannt — die Spezifikation erklärt Breaking Changes für wahrscheinlich, sodass
die Kopplung auf den Adapter beschränkt bleibt.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Export

Die kanonische Export-Oberfläche ist ein live HTTP-Endpunkt. Er leitet das Ossie-Dokument bei
jeder Anfrage aus dem Live-Zustand ab — kein Caching, kein Generierungsschritt.

```http
GET /admin/ossie
```

Die Antwort ist ein YAML-Dokument mit `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Die Metrics-Seite bietet zudem eine **Download**-Schaltfläche und eine kopierbare
Endpunkt-URL im Ossie-Interchange-Panel, die beide auf denselben Endpunkt verweisen.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Was exportiert wird

Der Adapter bildet Provisa-Objekte wie folgt auf Ossie-Objekte ab:

| Provisa-Objekt | Ossie-Objekt | Anmerkungen |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; Primär-/Unique-Keys aus der Spaltenkonfiguration und `UniqueConstraint` |
| `Column` | `field` | `expression` = Spaltenreferenz (ANSI_SQL-Dialekt); Zeitspalten erhalten `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias wird als Name verwendet, wenn gesetzt; berechnete (funktionsbasierte) Relationships werden übersprungen |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — verlustfrei per Design |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Nur Round-Trip; andere Tools können es ignorieren |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governance, RLS, Lineage und Graph-Semantik werden nicht exportiert. Sie können zur
Round-Trip-Treue im optionalen `provisa`-custom_extensions-Slot mitreisen, aber der Austausch
hängt nie davon ab, dass andere Tools diesen lesen. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Unbekannte Provisa-Spaltentypen werden unverändert durchgereicht; der Adapter mappt nie
stillschweigend auf einen falschen Typ. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Typ-Mapping

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Provisa-/Quelltyp | Ossie-`datatype` |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| alles andere | unverändert durchgereicht |

### Import

Der Import akzeptiert ein Ossie-Dokument (YAML oder JSON) und liefert Registrierungsvorschläge.
Nichts wird automatisch registriert — importierte Definitionen umgehen nie den
Überprüfungsschritt.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Der Server parst das Dokument mit `parse_ossie_model`, das die Struktur validiert und eine
`OssieImport`-Dataclass mit vorgeschlagenen Tabellen, Relationships und Metriken als
einfache Dicts zurückgibt. Jedes strukturelle Problem ist ein `400` mit einem pfadbenannten
Fehler, z. B. `ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### Der Überprüfungsbildschirm

In der UI öffnet die Schaltfläche **Import** (Metrics-Seite → Ossie-Interchange-Panel) einen
Datei-Auswahldialog. Nachdem das Dokument gesendet und geparst wurde, öffnet sich ein
Überprüfungsmodal mit jeder vorgeschlagenen Tabelle, Relationship und Metrik als angehaktem
Element. Der Modellierer kann jedes Element abwählen, um es auszuschließen. Ein Klick auf
**Apply** registriert die angehakten Elemente über die bestehenden Registrierungsmutationen —
zuerst Tabellen, dann Relationships (die auf Tabellen verweisen), dann Metriken.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Die Modellierungsrolle und der Verlauf, die in einem von Provisa exportierten Ossie-Dokument
gespeichert sind, durchlaufen den Import korrekt im Round-Trip. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Metriken über Protokolle hinweg (REQ-1319)

Die Definition einer governten Metrik — ihr Ausdruck, ihre Beschreibung und ihr `ai_context` —
reist mit dem Wert in jede Abfrageoberfläche durch eine einzige Compiler-Expansion. Es gibt
keine Kopien. Der Compiler reserviert das Schema `metrics` für den SQL-Zugriff; jedes Protokoll
fügt dann seinen eigenen Metadatenkanal hinzu.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Sprechen Sie jede Metrik als virtuelle Relation im Schema `metrics` an. Die von Ihnen
ausgewählten Dimensionsspalten werden zur GROUP BY:

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

Der Compiler expandiert die Form `metrics.<name>` zum echten gruppierten Aggregat, bevor die
Governance läuft. Spaltenbeschreibungen werden als `pg_description`-Einträge exponiert, sodass
DBeaver und psql `\d+` sie anzeigen. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` wird abgelehnt — benennen Sie die Spalten explizit.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Metriken projizieren innerhalb des `_aggregate`-Root-Felds als `metrics`-Block.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Der Definitionstext (`description`, `ai_context`) erscheint in der GraphQL-Introspektions-Doku,
sodass schema-bewusste Tools und Codegen ihn automatisch aufgreifen.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (KI-Agenten)

Zwei Tools exponieren Metriken für MCP-Clients:

- **`list_metrics`** — liefert alle governten Metriken, die für die Sitzung sichtbar sind, mit
  `name`, `description` und `ai_context`.
- **`query_metric`** — akzeptiert einen Metriknamen plus eine Dimensionsliste und ruft den
  semantischen SQL-Pfad des Compilers auf, wobei das Aggregatergebnis zurückgegeben wird.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Agenten, die `list_metrics` vor dem Konstruieren einer Abfrage aufrufen, wählen eine governte
Metrik nach Namen aus, statt Aggregations-SQL von Hand zu schreiben. Das Feld `ai_context` ist
der Ort für den Definitionstext, der die korrekte Auswahl leitet.

### Arrow Flight

Metriken sind als Metric-Flight-Deskriptoren adressierbar, die Arrow-Tabellen zurückgeben.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Verwenden Sie dieselbe SQL-Form `metrics.<name>` über den Standard-Flight-SQL-Ticket-Pfad.

### Bolt / Cypher (Neo4j Browser)

Rufen Sie eine Metrik über die Prozedur `provisa.metric()` auf:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Fact- und Dimension-Tabellen tragen im föderierten Graphen die Knotenlabels `:Fact` und
`:Dimension`, sodass Bloom die Sternform automatisch rendert.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Natürlichsprachliche Abfragen

Der NL-Schema-Matcher löst Metrik-Vokabular in natürlichsprachlichen Fragen direkt zu einer
Metrik plus Dimensionen auf und generiert dann semantisches SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Fact-Tabellen sind im NL-Prompt mit `[fact]` getaggt; Dimension-Tabellen mit `[dimension]`. Der
Matcher bevorzugt Join-Pfade von Fact zu Dimension bei der Auflösung von Fragen.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Kombinieren Sie `view_metrics` mit `materialize` und einer Kafka-Senke, um mit der bestehenden
Materialisierungsmaschinerie eine Push-on-Change-Metrikausgabe zu erzeugen. Keine neue Pipeline
ist erforderlich.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observability (OTel)

Metrikauswertungen werden getraced und sind als OpenTelemetry-Metriken exportierbar.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
