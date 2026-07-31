# Integrationen

## Die richtige Verbindungsart wählen

| Client-Typ | Empfohlene Methode | Warum |
| ------------- | ----------------- | ----- |
| BI-Tools (Tableau, Power BI, Looker) | JDBC | Spaltenbasiertes Arrow-Flight-Streaming über die Leitung; BI-Tools haben einen integrierten JDBC-Assistenten und profitieren von der spaltenbasierten Zustellung mit hohem Durchsatz für große Ergebnismengen |
| psql, DBeaver, jedes PG-kompatible Tool | pgwire (nativer PG-Treiber) | Reibungslose Standardoption — kein spezieller Treiber erforderlich; nutzen Sie, was Sie bereits haben |
| Python-Datenstack (pandas, pyarrow) | `provisa-client` oder rohes ADBC | Arrow-Batches im Streaming; kein Overhead durch Zeilenserialisierung |
| Spark, DuckDB, Pipelines mit hohem Durchsatz | Arrow Flight (ADBC) | Unbegrenztes spaltenbasiertes Streaming direkt in den Arrow-Speicher |
| Service-zu-Service (typisierte Verträge) | Protobuf gRPC | Pro Rolle generiertes Proto; Zeilen im Streaming; Typsicherheit |
| Webanwendungen, Skripting | HTTP (`/data/graphql`, `/data/sql`) | Kein Treiber; Standard-HTTP; volle Auswahl an Abfragesprachen |
| REST-Clients (JSON:API-Standard) | `GET /data/jsonapi/{table}` | JSON:API-v1.0-Umschlag; partielle Feldmengen, Paginierung, Filterung über Query-Parameter; kein Treiber |

---

## pgwire — Nativer PostgreSQL-Treiber

Provisa implementiert das PostgreSQL-Drahtprotokoll (Protokollversion 3.0). Jeder Client, der PostgreSQL spricht, verbindet sich ohne speziellen Treiber.

Aktivieren Sie es, indem Sie `PROVISA_PGWIRE_PORT` (z. B. `5433`) vor dem Start von Provisa setzen. Deaktiviert, wenn nicht gesetzt oder `0`.

### Warum pgwire statt JDBC?

Der JDBC-Treiber nutzt Arrow Flight als Transport und erfordert die Bereitstellung von `provisa-jdbc.jar`. pgwire erfordert nichts — wenn Sie bereits `psql`, DBeaver, SQLAlchemy oder einen PG-JDBC-Treiber haben, sind Sie fertig. Es ist die Variante mit dem geringsten Aufwand für reine SQL-Workloads.

JDBC ist die richtige Wahl für BI-Tools mit integriertem JDBC-Verbindungsassistenten, die vom spaltenbasierten Streaming von Arrow Flight für große Ergebnismengen profitieren. pgwire akzeptiert freies SQL gegen das vollständig veröffentlichte Schema — dieselben Abfragen, geringere Einrichtungskosten.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. Neue Verbindung → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Benutzername / Passwort wie in Provisa konfiguriert
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

pgwire verwendet eine Klartext-Passwortauthentifizierung, die mit dem konfigurierten Authentifizierungsanbieter von Provisa (`none` oder `simple`) verbunden ist. Im Trust-Modus (`none`) wird der Benutzername direkt einer Rolle zugeordnet — das Passwort wird ignoriert. MD5 wird nicht unterstützt; aktivieren Sie TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`), wenn Sie in einem nicht vertrauenswürdigen Netzwerk arbeiten.

### Einschränkungen

- Nur SQL. GraphQL und Cypher werden über pgwire nicht akzeptiert.
- Nicht schreibgeschützt. `COPY ... FROM STDIN` fügt Zeilen in die Quellen `postgresql`, `mysql`, `sqlite` und `mariadb` ein, und DDL wird unterstützt (siehe unten).
- DDL (`CREATE`, `ALTER`, `DROP`) wird unterstützt und an den Trino- oder den direkten Pfad weitergeleitet; die neue Tabelle wird im Kompilierungskontext registriert und ist sofort abfragbar. `COPY ... TO STDOUT` (Export) und `COPY ... FROM STDIN` (Import) werden in den Formaten `text` und `csv` unterstützt.
- Abfragen an `information_schema` und `pg_catalog` werden abgefangen und aus einem DuckDB-Katalog-Shim beantwortet — Tools zur Schemaerkennung funktionieren korrekt.

---

## JDBC-Treiber

Der JDBC-Treiber von Provisa verwendet Arrow Flight als zugrunde liegenden Transport. Er ist der empfohlene Weg für BI-Tools mit einem JDBC-Verbindungsassistenten.

### Verbindung

Laden Sie [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) herunter (immer die neueste Version) und fügen Sie sie dem Treiberpfad Ihres Tools hinzu.

JDBC-URL:

```yaml
jdbc:provisa://<host>:8815
```

Die Authentifizierung verwendet die standardmäßigen JDBC-Eigenschaften `user` / `password`. Provisa authentifiziert die Anmeldedaten beim konfigurierten Authentifizierungsanbieter und weist die Rolle zu — der Client wählt seine Rolle nicht selbst.

### Einrichtung von BI-Tools

**Tableau**

1. Verwalten → Treiber → Provisa JDBC installieren
2. Verbinden → Andere Datenbanken (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie bei Aufforderung Ihren Benutzernamen und Ihr Passwort ein

**DBeaver** (JDBC-Weg — für den pgwire-Weg siehe oben)

1. Datenbank → Neue Verbindung → JDBC
2. Treiber: `provisa-jdbc.jar` hinzufügen
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie Ihren Benutzernamen und Ihr Passwort im Tab „Authentifizierung“ ein

**Power BI** — verwenden Sie das ODBC-Gateway mit der Provisa-JDBC-ODBC-Brücke (im Installationsprogramm enthalten).

---

## Arrow-Flight-Clients

Arrow Flight (Port 8815) ist der empfohlene Weg für Datentools, die es unterstützen. Ergebnisse werden als Arrow-RecordBatches gestreamt, ohne im Speicher von Provisa materialisiert zu werden.

### Python (`provisa-client`)

Der empfohlene Python-Weg — kapselt sowohl GraphQL als auch Arrow Flight:

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

Die vollständige Referenz, einschließlich DB-API 2.0, SQLAlchemy-Dialekt und ADBC, finden Sie unter [docs/python-client.md](python-client.md).

### Python (rohes PyArrow)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Das Ticket trägt keine Rolle. Der Server weist die Rolle aus dem konfigurierten Authentifizierungsanbieter zu. Wo eine Rollenauswahl erlaubt ist, übergeben Sie diese in den gRPC-Aufrufmetadaten unter dem Schlüssel `x-provisa-role` (zum Beispiel `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), nicht im Ticket-JSON.

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

Service-zu-Service-Weg. Provisa generiert beim Start ein `.proto` pro Rolle — jede Rolle sieht nur die Tabellen und Spalten, auf die sie Zugriff hat.

Laden Sie das Proto für Ihre Rolle herunter:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Verwenden Sie `grpc_server_reflection`, um das Schema programmatisch zu erkennen.

Die Rolle wird über den Metadatenschlüssel `x-provisa-role` bei jedem RPC übergeben. Streaming-Abfragen geben eine Nachricht pro Zeile aus; Mutationen sind unär.

---

## Befehle über Protokolle hinweg aufrufen

Ein **Befehl** ist eine registrierte nachverfolgte Funktion oder ein Webhook — ein aufrufbares Element, das in der semantischen Schicht von Provisa mit einem `kind` (`query` oder `mutation`) und einem `impl_kind` registriert ist, der beschreibt, wie er ausgeführt wird. Jede Oberfläche leitet Aufrufe über einen einzigen gesteuerten Executor (`invoke_tracked_function`), der `writable_by` und die Governance einheitlich durchsetzt (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Was ausgeführt wird | Bindungsfelder |
| ------------ | ----------- | --------------- |
| `source_procedure` | Gespeicherte Prozedur auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Skript | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehostetes Python-Callable (REQ-885) | `callable` (z. B. `demo.py_functions:random_dataset`) |

Wenn ein Befehl ein `return_schema` deklariert (JSON Schema mit `type: array, items: object`), liefert er eine Ergebnismenge — jede Oberfläche projiziert ihn dann als typisierten Zeilensatz. Die Demo-Befehle `random_python_set` (impl_kind `python`) und `random_grpc_set` (impl_kind `grpc`) veranschaulichen sowohl ein gehostetes Callable als auch eine externe gRPC-Brücke, die Zeilen mit Zufallswerten zurückgibt; beide sind in `config/provisa-install.yaml` registriert. [tool-verified: `config/provisa-install.yaml:809-856`]

### Protokollmatrix

| Oberfläche | Syntax | Beispiel |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query-Feld; `kind=mutation` → Mutation-Feld; mit Domänenpräfix, wenn `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` oder `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / Treiber) | `CALL fn(args)` — positionale Argumente werden den deklarierten Argumentnamen zugeordnet | `CALL random_python_set(3, 7)` |
| Provisa gRPC (Port 50051) | Unär `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Das Feld `kind` steuert nur die Platzierung in GraphQL — die SQL-, Cypher-, Bolt- und gRPC-Oberflächen akzeptieren `query`- und `mutation`-Befehle identisch.

---

## Apollo Federation

Provisa kann als Federation-v2-Subgraph fungieren und sein veröffentlichtes Schema einem Apollo Router oder Apollo Gateway zur Verfügung stellen.

### Einrichtung

Aktivieren Sie Federation in `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa generiert automatisch `@key`-Direktiven auf Primärschlüsselspalten sowie `@external`/`@provides` auf Beziehungen zwischen Subgraphen.

### Registrierung bei Apollo Router

In Ihrer `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Führen Sie `rover supergraph compose --config supergraph.yaml` aus, um das Supergraph-Schema zu generieren.

### Entitäten

Provisa beantwortet `_entities`-Abfragen für Joins zwischen Subgraphen. Jede Tabelle mit einem Primärschlüssel ist automatisch als Federation-Entität auflösbar.

---

## Hasura v2 / DDN Import

Siehe [docs/import.md](import.md) für die Migration von Hasura zu Provisa.

---

## Kafka

Siehe [docs/sources.md](sources.md#kafka-quellen) für die Konfiguration von Kafka-Topics als schreibgeschützte Tabellen und Senken für Abfrageergebnisse.

---

## Apache-Ossie-Semantikaustausch (REQ-1316)

Provisa tauscht semantische Modelle mit Apache Ossie (Spezifikation 0.2.0.dev0, im Inkubationsstatus;
früher Open Semantic Interchange) über einen Grenzadapter aus. Das interne Vokabular von Provisa wird
niemals in das von Ossie umbenannt — die Spezifikation erklärt Breaking Changes für wahrscheinlich,
daher ist die Kopplung auf den Adapter beschränkt.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Export

Die kanonische Export-Oberfläche ist ein Live-HTTP-Endpunkt. Er leitet das Ossie-Dokument bei jeder
Anfrage aus dem Live-Zustand ab — kein Caching, kein Generierungsschritt.

```http
GET /admin/ossie
```

Die Antwort ist ein YAML-Dokument mit `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Die Metrics-Seite bietet außerdem eine Schaltfläche **Download** und eine kopierbare Endpunkt-URL im
Ossie-Interchange-Panel, beide verweisen auf denselben Endpunkt.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Was exportiert wird

Der Adapter bildet Provisa-Objekte wie folgt auf Ossie-Objekte ab:

| Provisa-Objekt | Ossie-Objekt | Hinweise |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; Primär-/Eindeutigkeitsschlüssel aus der Spaltenkonfiguration und `UniqueConstraint` |
| `Column` | `field` | `expression` = Spaltenreferenz (ANSI_SQL-Dialekt); Zeitspalten erhalten `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias wird als Name verwendet, wenn gesetzt; berechnete (funktionsbasierte) Beziehungen werden übersprungen |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — verlustfrei nach Design |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Nur für Roundtrip; andere Tools können dies ignorieren |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governance, Sicherheit auf Zeilenebene, Lineage und Graph-Semantik werden nicht exportiert. Sie
können zur Roundtrip-Treue im optionalen `provisa`-Slot der custom_extensions mitreisen, aber der
Austausch hängt nie davon ab, dass andere Tools dies lesen. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Unbekannte Provisa-Spaltentypen werden unverändert durchgereicht; der Adapter mappt niemals
stillschweigend auf einen falschen Typ. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Typzuordnung

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

Der Import akzeptiert ein Ossie-Dokument (YAML oder JSON) und liefert Registrierungsvorschläge
zurück. Nichts wird automatisch registriert — importierte Definitionen umgehen niemals den
Überprüfungsschritt.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Der Server analysiert das Dokument mit `parse_ossie_model`, das die Struktur validiert und eine
`OssieImport`-Datenklasse zurückgibt, die vorgeschlagene Tabellen, Beziehungen und Metriken als
einfache Dictionaries enthält. Jedes strukturelle Problem führt zu einem `400`-Fehler mit einem
pfadbenannten Fehler, z. B. `ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### Der Überprüfungsbildschirm

In der Benutzeroberfläche öffnet die Schaltfläche **Import** (Metrics-Seite → Ossie-Interchange-Panel)
eine Dateiauswahl. Nachdem das Dokument gesendet und analysiert wurde, öffnet sich ein
Überprüfungsdialog mit jeder vorgeschlagenen Tabelle, Beziehung und Metrik als angehaktes Element.
Der Modellierer kann jedes Element abwählen, um es auszuschließen. Ein Klick auf **Anwenden**
registriert die angehakten Elemente über die bestehenden Registrierungsmutationen — zuerst die
Tabellen, dann die Beziehungen (die auf Tabellen verweisen), dann die Metriken.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Die in einem von Provisa exportierten Ossie-Dokument gespeicherte Modellierungsrolle und -historie
durchlaufen den Roundtrip beim Import korrekt. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Metriken über Protokolle hinweg (REQ-1319)

Die Definition einer gesteuerten Metrik — ihr Ausdruck, ihre Beschreibung und ihr `ai_context` —
reist mit dem Wert zu jeder Abfrageoberfläche über eine einzige Compiler-Expansion. Es gibt keine
Kopien. Der Compiler reserviert das Schema `metrics` für den SQL-Zugriff; jedes Protokoll fügt dann
seinen eigenen Metadatenkanal hinzu.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Adressieren Sie jede Metrik als virtuelle Relation im Schema `metrics`. Die von Ihnen ausgewählten
Dimensionsspalten werden zum GROUP BY:

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

Der Compiler erweitert die Form `metrics.<name>` zum tatsächlichen gruppierten Aggregat, bevor die
Governance ausgeführt wird. Spaltenbeschreibungen werden als `pg_description`-Einträge dargestellt,
sodass DBeaver und `\d+` von psql sie anzeigen. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` wird abgelehnt — benennen Sie die Spalten explizit.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Metriken werden innerhalb des Wurzelfelds `_aggregate` als `metrics`-Block projiziert.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Der Definitionstext (`description`, `ai_context`) erscheint in der GraphQL-Introspektionsdokumentation,
sodass schema-bewusste Tools und Codegenerierung ihn automatisch aufnehmen.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (KI-Agenten)

Zwei Tools stellen Metriken für MCP-Clients bereit:

- **`list_metrics`** — liefert alle für die Sitzung sichtbaren gesteuerten Metriken mit `name`,
  `description` und `ai_context`.
- **`query_metric`** — akzeptiert einen Metriknamen sowie eine Dimensionsliste und ruft den
  semantischen SQL-Pfad des Compilers auf, wobei das Aggregatergebnis zurückgegeben wird.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Agenten, die `list_metrics` vor dem Erstellen einer Abfrage aufrufen, wählen eine gesteuerte Metrik
nach Namen aus, anstatt Aggregations-SQL von Hand zu schreiben. Das Feld `ai_context` ist der Ort,
an dem der Definitionstext platziert wird, der die korrekte Auswahl leitet.

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

Fakten- und Dimensionstabellen tragen im föderierten Graphen die Knotenbeschriftungen `:Fact` und
`:Dimension`, sodass Bloom die Sternform automatisch darstellt.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Anfragen in natürlicher Sprache

Der NL-Schema-Matcher löst das Metrik-Vokabular in Fragen in natürlicher Sprache direkt zu einer
Metrik plus Dimensionen auf und generiert dann semantisches SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Faktentabellen werden im NL-Prompt mit `[fact]` gekennzeichnet; Dimensionstabellen mit `[dimension]`.
Der Matcher bevorzugt bei der Auflösung von Fragen Join-Pfade von Fakt zu Dimension.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Streaming

Kombinieren Sie `view_metrics` mit `materialize` und einer Kafka-Senke, um eine Push-on-Change-
Metrikausgabe mit der bestehenden Materialisierungsmaschinerie zu erzeugen. Es ist keine neue
Pipeline erforderlich.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Observability (OTel)

Metrikauswertungen werden verfolgt und können als OpenTelemetry-Metriken exportiert werden.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
