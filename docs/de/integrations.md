# Integrationen {: #integrations }

## Den richtigen Verbindungspfad wählen {: #choosing-a-connection-path }

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

## pgwire — Nativer PostgreSQL-Treiber {: #pgwire-native-postgresql-driver }

Provisa implementiert das PostgreSQL-Wire-Protokoll (Protokollversion 3.0). Jeder Client, der PostgreSQL spricht, verbindet sich ohne speziellen Treiber.

Aktivieren Sie es, indem Sie `PROVISA_PGWIRE_PORT` (z. B. `5433`) setzen, bevor Sie Provisa starten. Deaktiviert, wenn nicht gesetzt oder `0`.

### Warum pgwire statt JDBC? {: #why-pgwire-instead-of-jdbc }

Der JDBC-Treiber transportiert seine Abfragen über Arrow Flight, und Sie müssen `provisa-jdbc.jar` bereitstellen, um ihn zu nutzen. pgwire braucht nichts weiter. Haben Sie schon `psql`, DBeaver, SQLAlchemy oder einen PG-JDBC-Treiber, sind Sie startklar. Wenn Sie nur SQL senden, ist das der einfachere Weg.

Nehmen Sie JDBC für BI-Tools mit eingebautem JDBC-Verbindungsassistenten, die große Ergebnismengen ziehen; dort zahlt sich das Columnar-Streaming von Arrow Flight aus. pgwire nimmt dasselbe freie SQL gegen das gesamte veröffentlichte Schema entgegen, mit weniger Einrichtung.

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

### Authentifizierung {: #authentication }

Das `password`-Feld des Startup-Pakets trägt das Credential, und was das Credential *ist*,
bestimmt die Methode: ein Personal Access Token, ein OIDC-Bearer-Token oder ein Passwort gegen
den konfigurierten Provider. Unter dem Provider `basic` mit `auth.scram: true` wird das Passwort
über SCRAM-SHA-256 nachgewiesen, statt gesendet zu werden. Client-Zertifikate werden
unterstützt. Im Trust-Modus (`none`) wird der Benutzername direkt auf eine Rolle abgebildet und
das Passwort ignoriert.

Die vollständige Oberfläche-×-Methode-Tabelle steht im [Sicherheitsmodell](security.md#surfaces-and-credentials). MD5 wird nicht unterstützt; aktivieren Sie TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`), wenn Sie über ein nicht vertrauenswürdiges Netzwerk laufen.

### Einschränkungen {: #limitations }

- Nur SQL. GraphQL und Cypher werden über pgwire nicht akzeptiert.
- Nicht schreibgeschützt. Zeilen registrierter Tabellen werden mit `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` und `COPY ... FROM STDIN` geschrieben, jeweils zugelassen durch das Recht `write` der Rolle, ihre Spaltenfreigaben und ihren Zeilenfilter; siehe [Datenschreibvorgänge](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Keine Definitionen: `CREATE`, `ALTER` und `DROP` werden mit SQLSTATE 0A000 abgelehnt, außer der eigenen temporären Tabelle einer Sitzung; siehe [Definitionen](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` exportiert in `text`, `csv` und `binary`; `COPY ... FROM STDIN` liest `text` und `csv`.
- `information_schema`- und `pg_catalog`-Abfragen werden abgefangen und aus einem DuckDB-Katalog-Shim beantwortet, sodass Schema-Erkennungstools korrekt funktionieren.

---

## JDBC-Treiber {: #jdbc-driver }

Der Provisa-JDBC-Treiber nutzt Arrow Flight als zugrunde liegenden Transport. Er ist der empfohlene Pfad für BI-Tools mit JDBC-Verbindungsassistenten.

Abfragen laufen über Arrow Flight, sobald der Flight-Port beim Öffnen der Verbindung antwortet, und Ergebnisse werden batchweise mit ihren Spaltentypen gestreamt. Als Flight-Port wird der neben dem HTTP-Port angenommen (8815 neben 8001); setzen Sie die Eigenschaft `flight_port`, wenn das nicht zutrifft. Ist der Flight-Port beim Verbinden nicht erreichbar, nutzt die Verbindung die HTTP-API für ihre Abfragen und protokolliert das auf INFO. Ist eine Verbindung einmal auf Flight, wird ein Fehler des Servers — ein abgelehntes Credential, eine Rolle, die der Benutzer nicht hält, eine fehlgeschlagene Anweisung — mit der Meldung des Servers an den Aufrufer weitergegeben; er wird nicht über HTTP wiederholt.

Das Durchsuchen des Schemas (`getTables`, `getColumns`, Primär- und Fremdschlüssel) wird über denselben Flight-Port gelesen, mit dem angemeldeten Credential: Der Treiber listet den Katalog der Rolle, als die er handelt, sodass eine Tabelle oder Spalte, die dieser Rolle nicht freigegeben ist, nicht erscheint. Ein Schlüssel, der auf eine nicht freigegebene Tabelle oder Spalte verweist, wird ebenfalls nicht gelistet. Auf einer Verbindung, deren Flight-Port beim Verbinden nicht erreichbar war, liest der Treiber denselben Katalog über HTTP (`GET /data/catalog`): Der Server baut beide Listen mit einer Funktion, sodass Tabellen, Spaltennamen, Typen und Schlüssel nicht davon abhängen, welcher Port geantwortet hat.

### Verbindung {: #connection }

Laden Sie [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) herunter (immer die neueste Version) und fügen Sie sie dem Treiberpfad Ihres Tools hinzu.

JDBC-URL:

```yaml
jdbc:provisa://<host>:8815
```

Die Authentifizierung nutzt die Standard-JDBC-Eigenschaften `user`/`password`. Der Treiber tauscht sie gegen ein Sitzungstoken und sendet es bei jeder Anfrage; eine abgelehnte Anmeldung lässt die Verbindung mit Status und Grund des Servers fehlschlagen. Provisa leitet die Rolle aus der angemeldeten Identität ab. Die optionale Eigenschaft `role` (oder `?role=` in der URL) bittet darum, als eine Rolle zu handeln, die der Benutzer hält, oder als eine kommagetrennte Menge solcher Rollen; eine Rolle, die der Benutzer nicht hält, wird abgelehnt. Auf einem Deployment ohne Auth-Provider gibt es keine Anmeldung, und der Benutzername wird als angeforderte Rolle genommen.

### BI-Tool-Einrichtung {: #bi-tool-setup }

**Tableau**

1. Manage → Drivers → Provisa JDBC installieren
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie bei Aufforderung Ihren Benutzernamen und Ihr Passwort ein

**DBeaver** (der JDBC-Pfad; der pgwire-Pfad steht oben)

1. Database → New Connection → JDBC
2. Treiber: `provisa-jdbc.jar` hinzufügen
3. URL: `jdbc:provisa://localhost:8815`
4. Geben Sie Ihren Benutzernamen und Ihr Passwort im Authentication-Tab ein

**Power BI** — nutzen Sie das ODBC-Gateway mit der Provisa-JDBC-ODBC-Brücke (im Installer enthalten).

---

## Arrow-Flight-Clients

Arrow Flight (Port 8815) ist der empfohlene Pfad für Datentools, die es unterstützen. Ergebnisse streamen als Arrow-RecordBatches, ohne sich im Provisa-Speicher zu materialisieren.

### Python (`provisa-client`)

Der empfohlene Python-Pfad kapselt sowohl GraphQL als auch Arrow Flight:

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

### Python (rohes PyArrow) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight trägt sein Credential in der JSON-Payload als `token`-Feld, das ein Provider-Bearer-Token oder ein Personal Access Token enthält. Sowohl der Handshake als auch jedes Ticket akzeptieren es, und beide validieren es auf dieselbe Weise, sodass ein Client, der sich beim Handshake authentifiziert hat, das Token weiterhin bei jedem `do_get` vorlegt. Ein `role`-Feld daneben *fordert* eine Rolle an; der Server leitet die zulässigen Rollen der Identität ab und ersetzt den autorisierten Wert, sodass ein Rollenstring in einem Ticket nie die Identität ist. Siehe [Sicherheitsmodell](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

Auch das Durchsuchen braucht das Credential. `list_flights`, `get_flight_info` und `get_schema` tragen kein Ticket, also lesen sie das Credential aus dem Header `authorization: Bearer <token>` des Aufrufs und die angeforderte Rolle aus `x-provisa-role` (eine Rolle oder eine kommagetrennte Menge gehaltener Rollen). Mit eingeschalteter Authentifizierung wird ein Durchsuchungsaufruf ohne Credential abgelehnt, und der zurückgegebene Katalog ist der der autorisierten Rolle: die Tabellen, Spalten und Commands, die dieser Rolle freigegeben sind, und nichts anderes. Ein Katalog-Ticket (ein Ticket ohne `query`) wird ebenso aus seinem eigenen `token` und `role` beantwortet. Ein Client muss sein Credential daher senden, bevor er durchsucht; `provisa-client` tut das in `list_tables()`. Auf einem Deployment ohne Auth-Provider listen diese Aufrufe den ganzen Katalog und nehmen kein Credential.

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

## Protobuf gRPC (Port 50051)

Service-zu-Service-Pfad. Provisa generiert ein `.proto` pro Rolle und erzeugt es neu, sobald sich das Modell oder die Rolle ändert, und jede Rolle sieht nur die Tabellen und Spalten, auf die sie Zugriff hat. Der Service hat eine RPC für jede Tabelle an der Wurzel des Schemas der Rolle; verknüpfte Tabellen werden wie in GraphQL über ihre Beziehungen erreicht.

Das Proto für Ihre Rolle herunterladen:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Nutzen Sie `grpc_server_reflection`, um das Schema programmatisch zu erkunden. Reflection beschreibt den Service der Rolle, als die der Aufruf handelt, dieselbe Datei wie der Download oben. Ohne Authentifizierung nennt ein Reflection-Aufruf seine Rolle wie jeder andere Aufruf:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Jeder RPC muss ein Credential im Metadaten-Schlüssel `authorization` tragen, entweder ein Provider-Token oder ein Personal Access Token. `x-provisa-role` fordert eine Rolle aus der zulässigen Menge der Identität an; es ist kein Credential und war es nie. Client-Zertifikate werden unterstützt. Siehe [Sicherheitsmodell](security.md#surfaces-and-credentials).

Streaming-Abfragen liefern eine Nachricht pro Zeile; Mutationen sind unär.

---

## Commands über Protokolle hinweg aufrufen {: #invoking-commands-across-protocols }

Ein **Command** ist eine registrierte getrackte Funktion oder ein Webhook, also ein im Modell von Provisa registrierter Callable mit einem `kind` (`query` oder `mutation`) und einem `impl_kind`, der beschreibt, wie er läuft. Jede Oberfläche leitet Aufrufe durch einen einzigen governten Executor (`invoke_tracked_function`), der die Befehlszuweisung (`visible_to`), das Recht `write` für Mutationen und Governance einheitlich durchsetzt.

| `impl_kind` | Was ausgeführt wird | Bindungsfelder |
| ------------ | ----------- | --------------- |
| `source_procedure` | Stored Procedure auf einer registrierten Quelle (Standard) | `sourceId`, `schemaName`, `functionName` |
| `script` | Serverseitiges Script | `script` |
| `http` | Ausgehender HTTP-Aufruf | `url`, `method` |
| `grpc` | Ausgehender gRPC-Aufruf an einen externen Server | `target`, `method` |
| `python` | Von Provisa gehosteter Python-Callable | `callable` (z. B. `demo.py_functions:random_dataset`) |

Wenn ein Command ein `return_schema` (JSON Schema mit `type: array, items: object`) deklariert, ist er set-returning, und jede Oberfläche projiziert ihn als typisierte Zeilenmenge. Die Demo-Commands `random_python_set` (impl_kind `python`) und `random_grpc_set` (impl_kind `grpc`) veranschaulichen sowohl einen gehosteten Callable als auch eine externe gRPC-Brücke, die Zeilen mit Zufallswerten liefert; beide sind in `config/provisa-install.yaml` registriert.

### Protokollmatrix {: #protocol-matrix }

| Oberfläche | Syntax | Beispiel |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → Query-Feld; `kind=mutation` → Mutation-Feld; domänenpräfixiert bei `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` oder `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / Treiber) | `CALL fn(args)` — positionale Argumente werden auf deklarierte Argumentnamen abgebildet | `CALL random_python_set(3, 7)` |
| Provisa gRPC (Port 50051) | Unärer `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Das Feld `kind` steuert nur die GraphQL-Platzierung. Die SQL-, Cypher-, Bolt- und gRPC-Oberflächen akzeptieren `query`- und `mutation`-Commands identisch.

---

## Apollo Federation

Provisa kann als Federation-v2-Subgraph fungieren und sein veröffentlichtes Schema einem Apollo Router oder Apollo Gateway exponieren.

### Einrichtung {: #setup }

Federation in `config.yaml` aktivieren:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa generiert automatisch `@key`-Direktiven auf Primärschlüsselspalten und `@external`/`@provides` auf spaltenübergreifende Subgraph-Relationships.

### Bei Apollo Router registrieren {: #register-with-apollo-router }

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

Siehe [docs/sources.md](sources.md#kafka-sources) für die Konfiguration von Kafka-Topics als schreibgeschützte Tabellen und Abfrageergebnis-Senken.

---

## Data-Quality-Checker {: #data-quality-checkers }

Soda Core und Great Expectations verbinden sich wie jeder andere Postgres-Client über pgwire mit Provisa. Das ist die gesamte Integration: Der Checker hält einen Postgres-Treiber und durchsucht die föderierte View, sodass eine Snowflake-Tabelle, eine Iceberg-Tabelle und eine Mongo-Collection alle mit demselben Contract-Dialekt geprüft werden, ohne einen systemspezifischen Checker.

Der Scan läuft in einem Kind-Interpreter — `python -m provisa.dq.worker` —, der der einzige Ort
ist, an dem `soda_core` oder `great_expectations` importiert wird. Nichts wird in den
Serverprozess gelinkt, und ein Checker-Absturz reißt einen Subprozess nieder, nicht die
Event-Loop.

Scan-Ergebnisse landen als gewöhnliche Quellzeilen, sodass Taktung, Aktualität, Events, Lineage,
Governance, RLS, das Grid und der Export alle ohne einen zweiten Mechanismus greifen.
Contract-Autorisierung, der Ergebnis-Envelope und die abgeleitete Registrierung werden in
[docs/sources.md](sources.md#data-quality-checkers) behandelt.

### Einen Checker installieren {: #installing-a-checker }

Keine der beiden Bibliotheken wird standardmäßig ausgeliefert. Der Installer fragt, welche Sie
möchten, und die Antwort wird zu `dq_checker: none|soda|gx` in `~/.provisa/config.yaml`. In der
Docker-Stufe wandelt `scripts/provisa` dies in das Build-Argument `PROVISA_EXTRAS` um; in der
nativen Stufe installiert `first-launch.sh` das passende pyproject-Extra in das venv.

| `dq_checker` | Bibliothek | Lizenz | Gehostete Cloud-Ebene |
| -------------- | --------- | --------- | -------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Abgelehnt (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Erlaubt |

Die Elastic License 2.0 untersagt es, die Software Dritten als gehosteten Dienst
bereitzustellen — genau das wäre es, Soda innerhalb der SaaS-Ebene im Namen eines Mandanten
auszuführen. Ein gehostetes Deployment, das Soda nutzen möchte, zeigt auf einen Soda-Endpunkt,
den der Betreiber selbst betreibt. Die Verbindungsschlüssel finden Sie in
[docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations).

---

## Apache-Ossie-Semantic-Interchange {: #apache-ossie-semantic-interchange }

Provisa tauscht semantische Modelle mit Apache Ossie (Spezifikation 0.2.0.dev0, Incubating; vormals Open Semantic Interchange) über einen Grenzadapter aus. Das interne Vokabular von Provisa wird nie in das von Ossie umbenannt. Die Spezifikation erklärt Breaking Changes für wahrscheinlich, daher bleibt die Kopplung auf den Adapter beschränkt.

Die Entwurfsspezifikation hat ihre Form geändert, ohne ihren Versionsstring zu ändern. Der Adapter folgt der
Spezifikation, wie sie im Commit `642cb894` von `apache/ossie` (2026-10-10) steht: ein Modell pro Dokument,
mit `version`, `name`, `datasets`, `relationships` und `metrics` an der Wurzel des Dokuments, und
einem aufgezählten `datatype`. Der Export wird in den Unit-Tests gegen das
`core-spec/ossie-schema.json` dieses Commits geprüft.

### Export

Die kanonische Export-Oberfläche ist ein lebender HTTP-Endpunkt. Er leitet das Ossie-Dokument bei jeder Anfrage aus dem aktuellen Zustand ab, ohne Caching und ohne Generierungsschritt.

```http
GET /admin/ossie
```

Die Antwort ist ein YAML-Dokument mit `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

Die Seite Metriken bietet im Panel Ossie Interchange außerdem eine Schaltfläche **Herunterladen** und eine kopierbare
Endpunkt-URL, beide auf denselben Endpunkt.

#### Was exportiert wird {: #what-is-exported }

Der Adapter bildet Provisa-Objekte wie folgt auf Ossie-Objekte ab:

| Provisa-Objekt | Ossie-Objekt | Hinweise |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; Primär-/Unique-Schlüssel aus Spaltenkonfiguration und `UniqueConstraint` |
| `Column` | `field` | `expression` = Spaltenreferenz (ANSI_SQL-Dialekt); Zeitspalten erhalten `dimension.is_time: true` |
| `Relationship` | `relationship` | Alias als Name, wenn gesetzt; berechnete (Funktionsziel-)Beziehungen werden übersprungen |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (abgebildet wie bei einer Spalte), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Nur für den Round-Trip; andere Tools können sie ignorieren |

Eine Beziehung über mehrere Spalten listet jede Spalte in `from_columns` und `to_columns`. Ein
Modell ohne Tabelle wird mit `422` abgelehnt: Ein Ossie-Dokument hat mindestens ein Dataset.

Governance, RLS, Lineage und Graphsemantik werden nicht exportiert. Sie können für die Round-Trip-Treue im optionalen
`provisa`-Slot von custom_extensions mitreisen, aber der Austausch hängt nie davon ab, dass andere
Tools ihn lesen.

Ein Spaltentyp außerhalb des Ossie-Vokabulars wird als `Opaque` geschrieben, mit dem eigenen Namen des Typs in der
`provisa`-Erweiterung des Feldes; so schreibt es die Spezifikation für einen bekannten Typ vor, für den sie kein Element
hat. Eine Spalte ohne erfassten Typ nennt keinen `datatype`. Der Adapter bildet nie auf einen Typ ab, den die
Spalte nicht hat.

#### Typ-Mapping {: #type-mapping }

| Provisa-/Quelltyp | Ossie-`datatype` |
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
| alles andere | `Opaque`, mit dem Namen des Typs in der `provisa`-Erweiterung |

Ossies `Decimal` trägt weder Genauigkeit noch Skala, daher wird `numeric(10,2)` als `Decimal` exportiert
und kommt aus einem Import als `decimal` zurück.

### Import

Der Import nimmt ein Ossie-Dokument (YAML oder JSON) entgegen und liefert Registrierungsvorschläge. Nichts wird automatisch registriert, und importierte Definitionen umgehen nie den Prüfschritt.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Der Server parst das Dokument mit `parse_ossie_model`, das die Struktur validiert und eine
`OssieImport`-Dataclass mit vorgeschlagenen Tabellen, Beziehungen und Metriken als einfache Dicts liefert.
Jedes Strukturproblem ist ein `400` mit einem nach Pfad benannten Fehler, z. B.
`ossie import: missing $.datasets[1].source`.

Der Importer liest nur die flache Dokumentform. Drei Dinge werden namentlich abgelehnt:

- Ein Dokument, das sein Modell in `semantic_model` einhüllt, die Form, die die Entwurfsspezifikation vor
  September 2026 hatte. Der Fehler sagt, dass das Dokument älter als die flache Form ist und wie man es umstellt.
- Eine `version` außer `0.2.0.dev0`.
- Ein `datatype`, der kein Element des Ossie-Vokabulars ist; der Fehler nennt den Pfad und listet
  die Elemente.

Eine vorgeschlagene Spalte oder Metrik trägt den Typ, für den ihr Ossie-Datentyp steht (`Integer` schlägt
`bigint` vor, `DateTime` schlägt `timestamp` vor). `Opaque` schlägt den Typ vor, den die `provisa`-Erweiterung
nennt, wenn das Dokument von Provisa stammt, und sonst keinen Typ. Ein als Objekt angegebener `ai_context`
wird über seine `instructions` gelesen.

Ein Feld wird nur dann als Spalte vorgeschlagen, wenn sein Ausdruck eine Spalte der Quelle ist. Ein Feld,
dessen Ausdruck eine Berechnung ist, wird nicht vorgeschlagen; der Prüfbildschirm nennt es unter seiner Tabelle
als nicht importiert. Ein Feld, das seine Spalte umbenennt, wird als diese Spalte vorgeschlagen, mit dem Namen des
Feldes als Alias der Spalte. Die `source` eines Datasets muss drei Teile haben,
`source.schema.table`; eine Abfrage oder ein kürzerer Name wird abgelehnt, und der Fehler sagt es.

#### Von dbt, Cube, Snowflake und anderen semantischen Schichten kommend {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Das Projekt Apache Ossie pflegt Konverter zwischen Ossie und den Modellformaten anderer Tools. Führen Sie
den Konverter für Ihr Tool aus und senden Sie dann seine Ausgabe an den Import-Endpunkt, oder wählen Sie die Datei im
Prüfbildschirm unten. Provisa liest das Ossie-Dokument; es liest nicht die Dateien des anderen Tools.

| Herkunft | Konverterbefehl | Was er liest |
|---|---|---|
| dbt (Semantic Layer) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | Das `semantic_manifest.json`, das dbt für MetricFlow schreibt |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Ein Cube-Modellverzeichnis oder einzelne Cube- und View-Dateien |

Dasselbe Repository enthält Konverter für Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData und weitere; jeder hat sein eigenes README.

Zwei Grenzen, die man vor dem Konvertieren kennen sollte:

- Der dbt-Konverter liest nur das Manifest der semantischen Schicht. Ein dbt-Projekt ohne semantische
  Modelle hat nichts, was er konvertieren könnte. Er verwirft Konversionsmetriken, private Metriken und
  Natural-Key-Entities und gibt für jede eine Warnung aus.

- Die Ausgabe eines Konverters folgt der Spezifikation, wie sie der eigene Commit des Konverters hat. Provisa liest
  die flache Form des Commits `642cb894` von `apache/ossie`; ein Konverter, der älter als September 2026 ist,
  schreibt die `semantic_model`-Hülle, die Provisa namentlich ablehnt.

Beide Pfade wurden am 2026-10-10 mit den aus Commit `642cb894` von `apache/ossie` installierten Konvertern ausgeführt
(sie werden nicht als Pakete veröffentlicht; sie werden aus den Verzeichnissen `converters/dbt` und
`converters/cube` des Repositorys installiert und brauchen Python 3.11 oder neuer). Ein Modell aus
zwei Tabellen, einer Beziehung und einer Metrik wurde aus Provisa exportiert, in ein dbt-Semantic-Manifest
und in ein Cube-Modell konvertiert, von `ossie-dbt msi-to-ossie` und
`ossie-cube import` zurück nach Ossie konvertiert und von `parse_ossie_model` gelesen:

| Pfad | Ergebnis |
|---|---|
| Cube-Modell → Ossie → Provisa | Gelesen. Tabellen, Schlüssel, Spaltentypen, die Beziehung und die Metrik kamen alle zurück; der Name der Beziehung war der des Konverters (`orders_to_customers`). |
| dbt-Semantic-Manifest → Ossie → Provisa | Gelesen. Tabellen, Primärschlüssel und die Metrik kamen zurück. Spaltentypen nicht (die Ausgabe des Konverters nennt keine), und die Beziehung kam so zurück, wie der Konverter sie aus den dbt-Entities abgeleitet hatte, was nicht die registrierte war. Prüfen Sie die Beziehungen vor dem Anwenden. |

Der dbt-Konverter lehnt ein Modell ab, in dem irgendeine Tabelle eine Unique-Constraint über mehrere
Spalten hat („MetricFlow entities cannot represent composite keys losslessly").

#### Der Prüfbildschirm {: #the-review-screen }

In der UI öffnet die Schaltfläche **Importieren** (Seite Metriken → Panel Ossie Interchange) eine Dateiauswahl. Nachdem das Dokument gesendet und geparst ist, öffnet sich ein Prüfdialog, in dem jede vorgeschlagene Tabelle, Beziehung und Metrik als angehakter Eintrag aufgeführt ist. Der Modellierer kann alles abwählen, um es auszuschließen. Ein Klick auf **Anwenden** registriert die angehakten Einträge über die bestehenden Registrierungsmutationen: zuerst Tabellen, dann Beziehungen (die auf Tabellen verweisen), dann Metriken.

Die in einem von Provisa exportierten Ossie-Dokument gespeicherte Modellierungsrolle und Historie überstehen den Import
unverändert.

### dbt-sources-Datei {: #dbt-sources-file }

Für ein dbt-Projekt, das die semantische Schicht nicht nutzt, schreibt Provisa direkt eine dbt-`sources.yml`.
Sie wird bei jeder Anfrage aus dem lebenden Modell abgeleitet.

```http
GET /admin/dbt/sources?role=<role id>
```

Die Antwort ist YAML mit `Content-Disposition: attachment; filename=provisa.sources.yml`. Der
Aufrufer braucht `org_settings`, und `role` muss eine Rolle sein, die der Aufrufer hält; wer
`access_config` hält, darf jede Rolle nennen. Das Interchange-Panel der Seite Metriken bietet denselben Download
für die verwendete Rolle an.

Die Datei listet jede Tabelle, die der Rolle bereitgestellt wird, unter einer dbt-Source pro Quelle und Schema (benannt
nach der Quell-ID, oder `<source>_<schema>`, wenn eine Quelle Tabellen in mehr als einem Schema hat),
mit der Beschreibung der Tabelle, den Beschreibungen der bereitgestellten Spalten und diesen Tests:

| Das Modell nennt | Die Datei schreibt |
|---|---|
| Einen Primärschlüssel aus einer Spalte | `unique` und `not_null` auf dieser Spalte |
| Einen Primärschlüssel aus mehreren Spalten | `not_null` auf jeder Spalte |
| Eine Unique-Constraint aus einer Spalte | `unique` auf dieser Spalte |
| Eine Beziehung zwischen zwei Tabellen, die beide der Rolle bereitgestellt werden | `relationships` auf der Quellspalte, zu `source('<source>', '<table>')` |

Eine Spalte, die der Rolle nicht bereitgestellt wird, fehlt, ebenso eine Tabelle ohne bereitgestellte Spalte. Ein Schlüssel über
mehrere Spalten hat keinen dbt-Standardtest; der einleitende Kommentar der Datei nennt jeden, und für ihn wird kein
Test geschrieben. Das Modell erfasst keine Nullbarkeit, daher erscheint `not_null` nur auf Schlüsselspalten.
Die Datei trägt keine Metriken, Zeilensicherheit, Maskierung, Rollen oder Lineage, und ihr einleitender
Kommentar sagt das.

Eine aus einem Modell mit zwei Tabellen und einer Beziehung erzeugte Datei wurde von dbt-core 1.12.5
mit dbt-duckdb 1.11.0 geparst: zwei Sources und fünf Tests, keine Warnungen. dbt-core 1.9.11 liest
ihre `relationships`-Tests nicht, die ihre Argumente unter `arguments` verschachteln; Versionen dazwischen
wurden nicht ausprobiert.

---

## Metriken über Protokolle hinweg {: #metrics-across-protocols }

Die Definition einer governten Metrik — ihr Ausdruck, ihre Beschreibung und ihr `ai_context` —
reist mit dem Wert in jede Abfrageoberfläche durch eine einzige Compiler-Expansion. Es gibt
keine Kopien. Der Compiler reserviert das Schema `metrics` für den SQL-Zugriff; jedes Protokoll
fügt dann seinen eigenen Metadatenkanal hinzu.

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
DBeaver und psql `\d+` sie anzeigen.

`SELECT *` wird abgelehnt, also benennen Sie die gewünschten Spalten.

### GraphQL

Metriken projizieren innerhalb des `_aggregate`-Root-Felds als `metrics`-Block.

Der Definitionstext (`description`, `ai_context`) erscheint in der GraphQL-Introspektions-Doku,
sodass schema-bewusste Tools und Codegen ihn automatisch aufgreifen.

### MCP (KI-Agenten) {: #mcp-ai-agents }

Zwei Tools exponieren Metriken für MCP-Clients:

- **`list_metrics`** — liefert alle governten Metriken, die für die Sitzung sichtbar sind, mit
  `name`, `description` und `ai_context`.
- **`query_metric`** — akzeptiert einen Metriknamen plus eine Dimensionsliste und ruft den
  semantischen SQL-Pfad des Compilers auf, wobei das Aggregatergebnis zurückgegeben wird.

Agenten, die `list_metrics` vor dem Konstruieren einer Abfrage aufrufen, wählen eine governte
Metrik nach Namen aus, statt Aggregations-SQL von Hand zu schreiben. Das Feld `ai_context` ist
der Ort für den Definitionstext, der die korrekte Auswahl leitet.

### Arrow Flight

Metriken sind als Metric-Flight-Deskriptoren adressierbar, die Arrow-Tabellen zurückgeben.

Verwenden Sie dieselbe SQL-Form `metrics.<name>` über den Standard-Flight-SQL-Ticket-Pfad.

### Bolt / Cypher (Neo4j Browser)

Rufen Sie eine Metrik über die Prozedur `provisa.metric()` auf:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

Fact- und Dimension-Tabellen tragen im föderierten Graphen die Knotenlabels `:Fact` und
`:Dimension`, sodass Bloom die Sternform automatisch rendert.

### Natürlichsprachliche Abfragen {: #natural-language-queries }

Der NL-Schema-Matcher löst Metrik-Vokabular in natürlichsprachlichen Fragen direkt zu einer
Metrik plus Dimensionen auf und generiert dann semantisches SQL.

Fact-Tabellen sind im NL-Prompt mit `[fact]` getaggt; Dimension-Tabellen mit `[dimension]`. Der
Matcher bevorzugt Join-Pfade von Fact zu Dimension bei der Auflösung von Fragen.

### Streaming

Kombinieren Sie `view_metrics` mit `materialize`, um eine Metrik-View aktuell zu halten, wenn sich
ihre Eingaben ändern, mit der bestehenden Materialisierungsmaschinerie. Keine neue Pipeline ist
erforderlich.

### Observability (OTel)

Metrikauswertungen werden getraced und sind als OpenTelemetry-Metriken exportierbar.
