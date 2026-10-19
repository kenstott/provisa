# Provisa

**Verbinden Sie Ihre Datenbanken. Abfragen mit GraphQL, gRPC, SQL oder MCP — über jede API oder jedes Protokoll — in 5 Minuten.**

Provisa legt eine API über alle Ihre Daten. Richten Sie es auf Ihre Datenbanken und Geschäftsanwendungen, beschreiben Sie, wie sie zusammenhängen, und fragen Sie das verknüpfte Ergebnis über REST, GraphQL, SQL, gRPC, MCP und mehr ab.

Das funktioniert, weil alles aus einem einzigen Modell kommt: Ihren Domänen, den Beziehungen zwischen Ihren Tabellen und den Richtlinien darauf, über alle Quellen hinweg. Provisa läuft aus diesem Modell und regelt aus ihm, also gibt es nichts synchron zu halten. Die registrierten Beziehungen sind die einzigen Joins, die eine Abfrage machen darf, und die Zugriffsrichtlinien werden in jeden Abfrageplan kompiliert. Aus dem einen Modell folgen vier Dinge:

- **Föderation.** 73 Konnektoren erreichen mehr als 85 Arten von Quellen, darunter Salesforce, ServiceNow und Stripe. Die SaaS-Systeme, auf denen Ihr Geschäft läuft, lesen sich als Tabellen neben Ihren Datenbanken, an Ort und Stelle, ohne vorherige Pipeline in ein Warehouse. Fragen Sie sie in SQL, GraphQL oder Cypher ab, über neun Protokolle.
- **Metriken.** Eine Metrik ist ein benanntes, reguliertes Aggregat. Sie definieren sie einmal, und jedes Werkzeug und jede Sprache, die danach fragt, bekommt dieselbe Definition.
- **Ontologie.** Das Business-Glossar ist ein Graph aus Begriffen, ihren Definitionen und ihren Beziehungen. Provisa bildet ihn auf die Tabellen und Spalten ab, die die Werte halten, und kann deshalb Fragen zu Ihren Daten beantworten. Ein Begriff bleibt ein Entwurf, bis er Daten erreicht, direkt oder über andere Begriffe. Das Glossar sagt, was ein Begriff ist; eine Metrik sagt, wie eine Zahl berechnet wird.
- **Governance.** Sechs Prüfebenen werden in jeden Abfrageplan kompiliert, in jeder Sprache und über jedes Protokoll, und jede Anfrage wird gleich festgehalten: wer was abgefragt hat, als welche Rolle, unter welcher Richtlinie. Traces, Metriken und Logs sind als abfragbare Tabellen neben Ihren Geschäftsdaten registriert.

Sie können in **GraphQL, Cypher oder SQL** fragen und sich über **pgwire, Bolt, gRPC, REST, Arrow Flight oder JDBC** verbinden. Jede Abfragesprache wird auf eine einzige Zwischendarstellung abgesenkt, und dort wird die Governance angewendet, einmal, sodass eine Richtlinie nicht zwischen Sprachen auseinanderlaufen kann. Auf dem Weg hinaus wird diese Form in den Dialekt jeder Quelle übersetzt. Eine neue Abfragesprache ist ein neues Frontend auf demselben Kern.

Lesen und Schreiben laufen beide hindurch. Eine quellenübergreifende Abfrage verteilt sich über die Föderationsschicht. Schreibvorgänge und Lesezugriffe auf eine einzige Quelle gehen direkt an deren Treiber, gleich reguliert und typischerweise in unter 100 ms zurück. Spaltenbasiertes Streaming über Arrow Flight ist eingebaut.

Das Modell hat ein kleines Vokabular: Domänen, Beziehungen, Rollen und Richtlinien. Das hält es leicht lesbar und prüfbar. Sie können den Richtliniensatz lesen und wissen, was er tut. Provisa selbst ist ein schlanker Abfrage-Compiler, keine Laufzeitumgebung in Ihrem Datenpfad. Es macht aus einer Anfrage native Abfragen, leitet sie weiter und tritt beiseite, und deshalb bleibt die Datenlandschaft schnell.

Sie können es auf zwei Arten nutzen, und beide lassen sich kombinieren:

- **Zum Modernisieren.** Modellieren Sie Ihre Landschaft, lassen Sie Provisa das native SQL für jede Quelle erzeugen, und übernehmen Sie dieses SQL dann im Zielsystem. Provisa ist die Brücke für den Übergang, und Sie können es danach weglassen.
- **Für dauerhafte Governance.** Behalten Sie es als den Weg, den jede Abfrage nimmt, damit Definition, Durchsetzung und Audit zusammenbleiben, solange die Landschaft existiert.

## Eine Demo ausprobieren {: #try-a-demo }

Am schnellsten geht es mit einer Live-Demo im Browser, ohne Installation: [provisa.dev/demos](https://provisa.dev/demos). Jede öffnet eine private Sandbox mit einer geführten Tour.

In einem Checkout dieses Repositorys können Sie jede Demo auf Ihrem eigenen Rechner ausführen:

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

Eine veröffentlichte Demo wird als fertiges Bundle geladen; die anderen laufen aus `demo/named/<name>/`. Alle Optionen finden Sie in [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md).

## Das Föderationsmodell {: #the-federation-model }

Vier Ideen tragen das ganze Design. Jede Quelle wird auf Tabellen zurückgeführt. Jede Abfrage wird auf eine SQL-ähnliche Form zurückgeführt. Die Erreichbarkeit entscheidet, was live abgefragt und was zuerst kopiert wird. Und eine Aktualitätsstrategie hält jede Kopie und jeden abgeleiteten Datensatz so aktuell, wie es nötig ist.

Beginnen wir mit den Quellen. Jede wird als Menge zweidimensionaler Tabellen über einem gemeinsamen Typsystem ausgedrückt, und das ist der einzige Vertrag, den eine Quelle erfüllen muss, um zur Datenlandschaft zu gehören. Eine MySQL- oder PostgreSQL-Tabelle ist schon eine. Ein GraphQL-Abfrageergebnis wird eine, sobald es abgeflacht ist. SPARQL-Triplestores und Neo4j sehen überhaupt nicht wie Tabellen aus, aber Sie liefern eine Abfrage mit tabellarischem Ergebnis, und die Abfrage wird zum Adapter. Gleich welche Quelle: Die Landschaft sieht Zeilen, Spalten und Typen. Eine neue Art von Quelle anzubinden heißt, diesen einen Vertrag zu erfüllen, manchmal mit etwas menschlicher Hilfe, statt eine Spezialintegration zu schreiben.

Abfragen werden genauso behandelt. SQL ist bei allen Dialekten die Sprache der Analyse über Tabellen, und das macht eine SQL-ähnliche Form zum natürlichen gemeinsamen Ziel. Also wird jede Anfrage, in welcher Sprache sie auch kommt, zuerst auf diese Form abgesenkt. SQL und GraphQL lassen sich leicht absenken. Die Pfade und die Graph-Semantik von Cypher kosten echte Arbeit, aber sie kommen an. Weil alles durch eine Form läuft, bevor irgendetwas anderes passiert, greift die Governance an einer Stelle, gleich in welcher Sprache die Anfrage kam.

Mit tabellarischen Quellen und einer Abfrageform kann die Föderation beides abdecken, Live-Abfragen und Warehousing: was eine Live-Query-Engine wie Trino tut, plus die Materialisierung, auf die solche Engines angewiesen sind. Die **Erreichbarkeit** verbindet beides. Kann die Engine eine Quelle dort abfragen, wo sie ist, oder müssen ihre Daten zuerst an einen abfragbaren Ort kopiert werden? Die Antwort teilt die Landschaft in das, was live gelesen, und das, was kopiert wird.

Die meisten Datenbanken haben schon eine Art Live-Verbindung: `ATTACH` in DuckDB, `postgres_fdw` in PostgreSQL, externe Links in Databricks. Daher können die meisten Datenbanken bis zu einem gewissen Grad als Föderations-Engine dienen. Keine deckt alles ab. Jede erreicht eine bestimmte Menge von Quellen und kopiert den Rest, und keine sagt klar, was was ist. Das Modell macht die Erreichbarkeit ausdrücklich, als festgelegte Menge von Methoden pro Quelle, die sagen, was die Engine live erreicht und, im Umkehrschluss, was kopiert werden muss.

Bleibt die Aktualität. Wie aktuell muss die Kopie jeder Quelle sein, die die Engine nicht erreicht? In der Praxis gibt es nur wenige Antworten: auf Abruf, nach Zeitplan, auf ein Änderungssignal (CDC, ein Wasserzeichen, ein Snapshot) oder fixiert. Eine pro Quelle zu wählen, ist die ganze Aktualitätsrichtlinie.

Abgeleitete Datensätze passen in dieselbe Form: abgeleitete Tabellen, Aggregate, die Ausgabe einer Transformation. Sie werden in derselben Form geschrieben, also bekommen Sie die Lineage umsonst. Der Weg von jedem Ursprungssystem bis zur Endausgabe ist die Definition, die sie erzeugt hat, von Anfang bis Ende lesbar. Beim Aufbau stellt sich die Aktualitätsfrage einen Schritt weiter: Wird der Datensatz nach Zeitplan aktualisiert, erst wenn seine Vorbedingungen erfüllt sind, fortlaufend oder als fixierter historischer Snapshot? Die Möglichkeiten sind dieselbe kleine Menge wie bei einer Quellkopie, also nutzt die Aufbaurichtlinie eines abgeleiteten Datensatzes dasselbe Vokabular.

Dimensionale Modelle folgen direkt daraus. Fakten und Dimensionen eines Sternschemas sind abgeleitete Datensätze wie alle anderen. Eine Dimension ist eine abgestimmte, deduplizierte Projektion, eine Faktentabelle ein auf eine Granularität reduzierter Join mit Aggregat, jede mit eigener Aufbau- und Aktualitätsrichtlinie. Slowly Changing Dimensions brauchen nichts Besonderes: Ein fixierter Snapshot ist Typ-2-Historie, ein geplanter Neuaufbau Typ 1. Weil das Schema im Modell definiert ist statt an die Tabellen eines Warehouse gebunden, lassen sich dieselben Fakten und Dimensionen in Oracle oder Databricks aufbauen oder virtuell über einer MPP-Engine belassen, ohne Neumodellierung.

Data Vault passt genauso, eine Schicht früher. Hubs sind deduplizierte Datensätze aus Geschäftsschlüsseln, Links die registrierten Beziehungen zwischen ihnen, und Satelliten sind nur anhängende, zeitgestempelte Attributdatensätze, die die Historie halten. Ein Satellit ist schlicht ein abgeleiteter Datensatz mit der Änderungssignal-Strategie: Ladedatum plus Hashdiff ist Änderungserfassung auf beschreibenden Attributen, und rein anhängende Historie ist die Strategie des fixierten Snapshots. Point-in-Time- und Bridge-Tabellen sind weitere abgeleitete Datensätze, gebaut für Abfragegeschwindigkeit. Ein Raw Vault ist also eine Menge abgeleiteter Datensätze im Modell und ein Sternschema eine Projektion daraus, beide erzeugt und beide zwischen Engines übertragbar. Die Methodik entscheiden weiter Sie: was ein Hub wird, die Granularität eines Satelliten, wie aufgeteilt wird. Ist das entschieden, lebt es im Modell statt in ETL, das an ein Warehouse geschweißt ist.

Beide Muster deklarieren Sie mit **zwei Abkürzungen** statt handgeschriebener Views. Sie sind die Bausteine, aus denen jedes Sternschema und jeder Data Vault besteht, und legen Sie auf keine der beiden Methoden fest:

- **`entity`** ist eine geschlüsselte, deduplizierte Projektion einer Quelle, die Historie halten kann. Deklarieren Sie den Schlüssel, die Attribute und einen Historienmodus, und Provisa baut eine Materialized View. Verlangen Sie Historie, baut es stattdessen eine **bitemporale MV** (`scd2` → delta, `snapshot` → snapshot). Ein Konstrukt liefert eine Kimball-**Dimension** (SCD1 oder SCD2) oder einen Data-Vault-**Hub mit Satellit**.
- **`fact`** verknüpft mit Entitätsschlüsseln, reduziert auf eine deklarierte Granularität und aggregiert Kennzahlen. Provisa baut eine Aggregat-MV plus die registrierten Beziehungen zu den Entitäten. Ein Konstrukt liefert eine Stern-**Faktentabelle** oder einen Data-Vault-**Link**; ein Fakt ohne Kennzahlen ist ein reiner Schlüsselmengen-Link.

Jede `entity` und jeder `fact` wird genau zu den Materialized Views, bitemporalen Einstellungen und Beziehungen, die ein Modellierer sonst von Hand schreiben würde, sodass das Warehouse im Modell bleibt und ohne Neumodellierung zwischen Engines wechselt. Deklarieren Sie eines in der Admin-Oberfläche (das Formular **Model**) oder über die Admin-API (`registerEntity` / `registerFact`). Das Modell erzeugt einen Kimball-Stern oder einen Data Vault für Sie; es zwingt keines von beiden auf.

### Time Travel

Time Travel heißt, jede Version einer Zeile zu behalten, statt sie zu überschreiben, damit Sie fragen können, wie die Daten zu einem früheren Zeitpunkt waren. Engines unterscheiden sich stark darin, wie effizient sie das können, deshalb macht Provisa es zum Teil der **Definition** der Materialized View statt der Speicher-Engine. Einmal deklariert, funktioniert es auf jedem Speicher, der eine Materialized View halten kann.

Übertragbar bleibt es, weil es **nur anhängend** ist. Eine einmal geschriebene Version wird nie geändert oder gelöscht. Der übliche bitemporale Trick, eine Zeile durch Zurückschreiben eines „gültig bis“-Datums zu schließen, braucht ein UPDATE, das viele Engines über einen föderierten Speicher nicht günstig oder gar nicht können. Also **hängt** jede Aktualisierung stattdessen an, und welche Version zu einem Zeitpunkt galt, wird beim Lesen aus dem Log ermittelt. Es gibt zwei Arten anzuhängen:

- **Snapshot** hängt den ganzen frischen Datensatz an, gestempelt mit der Systemzeit der Aktualisierung. Es gibt kein Diffing, und es ist auf jeder Engine korrekt, aber der Speicher wächst um eine volle Kopie pro Aktualisierung.
- **Delta** hängt nur an, was sich geändert hat, plus Grabsteine für entfernte Schlüssel. Die Engine ermittelt das Delta selbst, mit Anti-Joins in einem `INSERT … SELECT`, statt dass Provisa es Zeile für Zeile zusammenführt. Es ist kleiner und braucht einen Entitätsschlüssel.

Die Systemzeit, also wann Provisa eine Version aufgezeichnet hat, verwaltet Provisa auf diese Weise. Die gültige Zeit, wann ein Sachverhalt im Geschäft zutrifft, kommt aus dem eigenen SELECT der View und bleibt erhalten. Engines, die mehr können, etwa native Iceberg-Snapshots oder ein MERGE, das weniger Zeilen berührt, lassen sich hinter derselben Deklaration nutzen; der rein anhängende Weg ist die Untergrenze, die überall korrekt ist.

Das Lesen ist transparent. Eine einfache Abfrage auf eine bitemporale MV liefert den **aktuellen** Zustand, aus dem Log wiederhergestellt. Um in der Zeit zurückzugehen, senden Sie einen Header `X-Provisa-As-Of: <timestamp>`, und die ganze Abfrage wird so beantwortet, wie die Landschaft damals aussah, auf jedem Speicher gleich. Fügen Sie `X-Provisa-As-Recorded` hinzu, um die Daten so zu lesen, wie Provisa sie zu einem Zeitpunkt aufgezeichnet hatte, damit eine später erfasste Korrektur eine Wiedergabe der Vergangenheit nicht ändert. Schalten Sie es für jede Materialized View in der Admin-Oberfläche ein (das Steuerelement **Time Travel**: aus, Snapshot oder Delta, plus ein Entitätsschlüssel) oder über die Admin-API.

Erreichbarkeit und Aktualität zusammen ergeben ein allgemeines Modell für Föderation: eine Definition dessen, was live ist, was kopiert wird und wie aktuell jede Kopie bleibt, unabhängig davon, was eine einzelne Engine erreicht. Das Modell ist übertragbar, also ist Ihre Datenlandschaft nicht an den Anbieter gebunden, dessen Föderation heute zufällig die meisten Quellen erreicht.

## Funktionen {: #features }

### Abfrageschnittstellen {: #query-interfaces }

Dies sind die Sprachen und strukturierten APIs, in denen Sie Abfragen schreiben. Jede hat ihre eigene Syntax und Semantik; Governance (RLS, Maskierung, Spaltensichtbarkeit, Beziehungsdurchsetzung) gilt einheitlich über alle hinweg, unabhängig davon, welches Wire-Protokoll sie liefert.

- **GraphQL** — Rollenspezifische Schemas mit feldgenauer Sichtbarkeit, Filterung, Cursor-basierter Paginierung und Aggregatabfragen (`count`, `sum`, `avg`, `min`, `max`). Schema-eingeschränkt auf registrierte Beziehungen — durch Konstruktion strukturell gültig, der schnellste Weg zu einer korrekten einfachen Abfrage. Apollo APQ inklusive: Abfragen werden gehasht und serverseitig registriert; nachfolgende Aufrufe senden nur den Hash über HTTP GET, was Antworten CDN-cachebar macht, ohne dass Client-Änderungen nötig sind. Lookup-Tabellen unterhalb eines konfigurierbaren Zeilenschwellenwerts werden als Enum-Typen exponiert.
- **SQL** — Vollständiges SQL über föderierte Daten; uneingeschränkt und ausdrucksstärker als GraphQL. Schreiben Sie Standard-SQL — korrelierte Unterabfragen inklusive — und es läuft unverändert über Quellen hinweg. Einzelquellen-Abfragen umgehen die Föderationsschicht vollständig (unter 100 ms).
- **Cypher** — Graph-Abfragesprache über demselben föderierten Schema. Beziehungen als Graph-Kanten traversieren; Quellen vereinigen; Pfade variabler Länge. Governance gilt identisch wie bei GraphQL und SQL.
- **gRPC-Model-API** — Automatisch generiertes `.proto` aus dem registrierten Schema; typisierte Query- und Insert-RPCs für jede Tabelle an der Wurzel des Schemas einer Rolle, gestreamte Antworten. Schema-getrieben im selben Sinne wie GraphQL — das Registrierungsmodell ist der Vertrag, Protobuf ist die Wire-Kodierung. Anders als Arrow Flight (das ein Columnar-Streaming-Transport ist) ist dies eine vollständige Pro-Tabelle-Abfrageschnittstelle.
- **JSON:API** — Strukturierte Abfrage-API unter `/data/jsonapi/{table}`, per Design nur HTTP. Unterstützt JSON:API 1.1: Sparse Fieldsets (`fields[table]=col1,col2`), Filterausdrücke (`filter[field][op]=value`), zusammengesetzte Dokumente (`include=relation`) und Sortierung. Keine Allzweck-Abfragesprache — fragt eine Tabelle nach der anderen mit standardisierter Filtersyntax ab statt mit einem Ad-hoc-Abfragestring.
- **Query Language Explorer** — Schreiben Sie eine GraphQL-Abfrage und sehen Sie live **Semantic-SQL**- und **Cypher**-Übersetzungen in Seitenpanels; kopieren Sie eine davon oder springen Sie direkt in den SQL- oder Graph-Editor. Ein praktischer Workflow ist, Abfragefragmente in GraphQL zu skizzieren und dann das resultierende SQL in komplexe Views oder Reports einzufügen.

Der Explorer zeigt eine GraphQL-Abfrage neben ihren live SQL- und Cypher-Übersetzungen:

![Query Language Explorer](docs/images/query-explorer.png)

Dasselbe föderierte Schema ist als lebender Graph erkundbar — Domänen- und Knotenlabels, Beziehungstypen und Traversierungen variabler Länge:

![Graph Visualization](docs/images/graph-view.png)

### Werkzeuge zur Abfragekomposition {: #query-composition-tools }

Diese Werkzeuge helfen Ihnen, Abfragen in den obigen Sprachen zu schreiben — sie sind selbst keine Abfragesprachen.

- **Natural-Language-Abfrage** — NL→SQL/Cypher/GraphQL-Pipeline, angetrieben von Claude. Beschreiben Sie in einfachem Englisch, was Sie wollen; die Pipeline erzeugt eine Abfrage in Ihrer gewählten Sprache mit einer interaktiven Validierungsschleife vor der Ausführung.

![Natural Language Query](docs/images/natural-language.png)

### Wire-Protokolle {: #wire-protocols }

Dies sind die Verbindungsprotokolle. SQL, GraphQL und Cypher reiten über ihnen — die Wahl des Wire-Protokolls ändert die Abfrageschnittstelle oder das Governance-Verhalten nicht.

- **pgwire** — Jeder PostgreSQL-Client (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) verbindet sich auf Port 5439, als wäre es ein Postgres-Server. Akzeptiert nur SQL. Volle Governance-Pipeline gilt. `pg_catalog` und `information_schema` werden aus einem In-Memory-Katalog beantwortet, sodass Schema-Browser ohne Föderations-Roundtrip funktionieren. TLS optional.
- **Bolt (Neo4j)** — Jeder Neo4j-Client (Neo4j Browser, Bloom, offizielle Treiber) verbindet sich über das Bolt-Protokoll und führt Cypher gegen den föderierten Graphen aus. Jede Rolle, die der Nutzer hält, erscheint als `provisa_<role>`-Datenbank. Dieselbe Governance wie jeder andere Transport. TLS optional.
- **Arrow Flight** — Durchsatzstarkes Columnar-Streaming über gRPC; akzeptiert GraphQL oder SQL als Abfrageeingabe. Unbegrenzte Ergebnismengen, keine serverseitige Materialisierung, keine separate Infrastruktur erforderlich.
- **JDBC** — BI-Tool-Integration (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Subscriptions: Near-Real-Time-Änderungsereignisse; Backends: PG-nativ, MongoDB-nativ, CDC, Polling. Auch über Kafka exponiert.

### Datenquellen {: #data-sources }

- **73 Konnektoren, 85+ Arten von Quellen** — Die größere Zahl zählt die gehosteten PostgreSQL-Dienste (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) und die Dateiformate des File Crawlers einzeln. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL-Triplestores, Kafka, Google Sheets und mehr über eine einzige API; Graph- und RDF-Quellen sind erstklassig, keine Adapter
- **Smart Routing** — Einzelquellen-Abfragen umgehen die Föderation (unter 100 ms); Multi-Quellen-Abfragen routen durch die Föderationsschicht — eigenen Cluster mitbringen oder die eingebetteten Worker nutzen
- **API-Quellen** — REST-, GraphQL-, gRPC-, WebSocket- oder RSS-Endpunkte als abfragbare Tabellen registrieren; SPARQL-Helfer inklusive; föderierte Joins über API-Quellen und relationale Quellen hinweg funktionieren transparent
- **Remote-Schema-Introspektion** — Auf jeden GraphQL-, OpenAPI- oder gRPC-Endpunkt zeigen; dokumentierte Operationen werden automatisch als abfragbare Tabellen, Graph-Knoten und -Kanten mit voller Governance obendrauf exponiert
- **Dateiquellen** — CSV-, Parquet- und SQLite-Dateien als abfragbare Tabellen; unterstützt lokale Pfade und Remote-Objektspeicher (`s3://`, `ftp://`, `sftp://`)
- **Kafka-Integration** — Topics als schreibgeschützte Tabellen; Abfrageergebnisse als Kafka-Sinks
- **Geplante Trigger** — Cron- und Interval-Trigger (APScheduler), die Webhooks, Mutationen oder Kafka-Sink-Publishes auslösen
- **Föderations-Performance-Hints** — SQL-Kommentar-Routing-Hints überschreiben automatische Routing-Entscheidungen

![Data Sources](docs/images/data-sources.png)

Quellen, Dateien und Remote-Endpunkte werden als regulierte Tabellen aus der UI registriert:

![Table Registration](docs/images/table-registration.png)

### Sicherheit & Governance {: #security-governance }

- **Sicherheit auf Zeilenebene** — Injektion von WHERE-Klauseln pro Tabelle, pro Rolle
- **Spaltenmaskierung** — Maskierung pro Spalte (Regex, Konstante, Trunkierung) mit rollenbasiertem Bypass
- **Spalten-Presets** — Serverseitige statische oder sitzungsvariablenbasierte Werte, injiziert bei Insert/Update; nicht in Mutation-Input-Typen exponiert
- **Schreibberechtigungen** — Zugriffskontrolle pro Spalte für Mutationen (`writable_by`)
- **Vererbte Rollen** — Rollen erben RLS, Sichtbarkeit und Maskierung rekursiv von einer übergeordneten Rolle
- **Getrackte Funktionen & Webhooks** — DB-Funktionen und ausgehende Webhooks, exponiert als GraphQL-Mutationen mit typisierten Rückgabeformen
- **ABAC-Genehmigungs-Hook** — Vor-Ausführungs-Autorisierungs-Hook; Webhook-, gRPC- oder unix_socket-Transport; Umfang pro Tabelle, pro Quelle oder global; konfigurierbare Fallback-Richtlinie
- **Steckbare Authentifizierung** — Firebase, Keycloak, OAuth 2.0, Simple (für Tests)

![Security Roles](docs/images/security-roles.png)

### Delivery & Performance

- **Materialisierte Sichten als aufgezeichnete Transforms** — Eine MV erfasst den Transform, der sie erzeugt hat: ihre Join-Form oder SQL, die Eingangssignale pro Quelle (Iceberg-Snapshot, RDB-Watermark), aus denen sie gebaut wurde, und eine Determinismusprüfung bei der Registrierung. Weil der Transform aufgezeichnet ist, werden Abfragen (oder Teilausdrücke) transparent auf eine frische MV umgeschrieben — strukturelles Join-Pattern-Matching mit Partial-Match-Unterstützung, sodass eine MV, die eine Teilmenge von Joins abdeckt, weiterhin anwendbar ist, wobei verbleibende Joins erhalten bleiben
- **Hot-Table-Inlining** — Kleine, häufig gejointe Lookup-Tabellen werden als VALUES-CTEs direkt in den Abfrageplan inline eingebettet, was quellübergreifende Roundtrips für Dimensionsdaten eliminiert
- **Query-Caching** — Rollen- und RLS-partitionierter Redis-Ergebniscache; APQ-Hash-Cache inklusive
- **Observability als Daten** — Verteilte Traces, Metriken und Protokolle werden über OpenTelemetry gesammelt, in Iceberg auf S3 kompaktiert und automatisch als abfragbare Tabellen (`traces`, `metrics`, `logs`, `queries`) im föderierten Schema registriert; fragen Sie sie mit SQL, GraphQL oder Cypher neben Ihren Geschäftsdaten ab — joinen Sie eine `customers`-Tabelle mit der `queries`-Tabelle, um zu sehen, wer was ausgeführt hat und wie lange es gedauert hat

### Administration & Integration

- **Admin-API** — GraphQL unter `/admin/graphql`; Konfigurations-Upload/-Download, Beziehungsbearbeitung, Abfragegenehmigung
- **Reports-Viewer** — `/admin/reports` listet die eingebauten Ops-Domain-Management-Views und jeden registrierten benutzerdefinierten Report auf; erfordert die Capability `observability`
- **Table Preview** — jede registrierte Tabelle hat einen server-seitig paginierten regulierten Datenviewer mit pushed-down Filtern, mehrstufigem Group-by und CSV-Export
- **GraphQL Voyager** — Interaktive rollenspezifische Schema-Visualisierung als Entity-Relationship-Diagramm
- **LLM-Beziehungserkennung** — Von Claude angetriebene Fremdschlüssel-Kandidatenvorschläge
- **Python-Client** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → pyarrow Tables, SQLAlchemy-Dialekt, ADBC-Unterstützung
- **Datenaufnahme** — HTTP-Endpunkte zum Einspeisen von JSON-Ereignisdaten in die Plattform
- **Hasura v2 / DDN Import** — Hasura-v2-Metadaten oder DDN-Supergraph-YAML in Provisa-Konfiguration umwandeln
- **Apollo Federation** — Provisa als Apollo-Federation-v2-Subgraph exponieren

Rollenspezifisches Schema, visualisiert als Entity-Relationship-Diagramm (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Beziehungen werden als die einzigen legalen JOIN-Pfade registriert, genehmigt und durchgesetzt:

![Relationships](docs/images/relationships.png)

## Sicherheitsmodell {: #security-model }

Hier hört „auf dem Pfad, den jede Abfrage ohnehin nimmt" auf, ein Slogan zu sein. Provisa setzt ein mehrschichtiges Sicherheitsmodell über jede Abfragesprache (GraphQL, SQL, Cypher) und jeden Transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket) durch. Governance wird einheitlich angewendet — es gibt keinen Abfragepfad, der sie umgeht. Abdeckung ist durch Konstruktion vollständig, nicht durch Sorgfalt: Fügen Sie eine Quelle, Spalte oder Beziehung hinzu, und jede Schicht gilt automatisch dafür, ohne dass etwas zu registrieren zu merken wäre.

Die Schichten gelten der Reihe nach. Eine Anfrage muss jede Schicht passieren, bevor die nächste ausgewertet wird.

### Schicht 0 — Introspektionsfilterung {: #layer-0-introspection-filtering }

Das Schema und der Katalog, die einer Rolle präsentiert werden, enthalten nur die Tabellen in ihrer `domain_access`-Liste und die Spalten, die pro-Spalte-`visible_to`-Regeln bestehen. Objekte außerhalb des Zugriffs einer Rolle sind zum Zeitpunkt der Entdeckung unsichtbar — sie können nicht abgefragt, autovervollständigt oder als existent erschlossen werden. Dies gilt für das GraphQL-Schema, den SQL-Katalog und den Schema-Browser des Abfrage-Editors.

### Schicht 1 — Öffentlicher Zugriff {: #layer-1-public-access }

Tabellen in Domänen ohne `domain_access`-Beschränkung sind für alle authentifizierten Identitäten ohne zusätzliche Konfiguration sichtbar. Keine Reibung für genuin öffentliche Daten.

### Schicht 2 — Domänenzugriff {: #layer-2-domain-access }

Jede Rolle trägt eine `domain_access`-Liste von Domänen-IDs. Eine Abfrage, die eine Tabelle außerhalb dieser Domänen berührt, wird vor der Ausführung abgelehnt. Dies ist die grobe Eigentumsgrenze — eine HR-Rolle kann Finanztabellen nicht erreichen, unabhängig davon, wie das SQL geschrieben ist.

### Schicht 3 — Sicherheit auf Zeilenebene {: #layer-3-row-level-security }

Nachdem der Domänenzugriff bestätigt ist, werden pro-Tabelle, pro-Rolle `WHERE`-Prädikate zur Ausführungszeit in jedes `SELECT` injiziert. Die Prädikate werten gegen Rohdaten aus. Ein regionaler Manager, der eine gemeinsam genutzte Orders-Tabelle abfragt, sieht selbst bei einem `SELECT *` nur die Zeilen seiner Region.

### Schicht 4 — Spaltensichtbarkeit und Maskierung {: #layer-4-column-visibility-and-masking }

Spalten mit einer `visible_to`-Liste, die die anfragende Rolle ausschließt, werden aus der Abfrageausgabe entfernt. Spalten mit einer Maskierungsregel haben ihre Werte ersetzt — Regex-Schwärzung, Konstantenersetzung oder Trunkierung — bevor Ergebnisse den Server verlassen. Maskierung gilt in allen Abfragesprachen und Ausgabeformaten.

### Schicht 5 — Prädikat-Guard {: #layer-5-predicate-guard }

Maskierte Spalten werden aus `WHERE`- und `HAVING`-Klauseln abgelehnt. Ohne dies könnte ein Aufrufer den unmaskierten Wert durch Binärsuche in einem Filter erschließen, selbst wenn die Ausgabe maskiert ist. Die Ablehnung wird zur Abfrage-Parse-Zeit durchgesetzt, vor der Ausführung.

### Beziehungs-Governance {: #relationship-governance }

JOIN-Bedingungen in SQL müssen einer registrierten, genehmigten Beziehung zwischen Tabellen entsprechen. Nicht genehmigte Joins werden abgelehnt. Jede Beziehung trägt einen menschenlesbaren Grund und eine Beschreibung — eine Orientierung für sowohl Nutzer als auch autonome Agenten darüber, warum ein Traversierungspfad existiert. Dies ist Governance-Richtlinie, keine harte Sicherheitsgrenze: Die Schichten 2–5 gelten unabhängig von der Join-Struktur, sodass eine gezielte Umgehung keine Daten exponiert, die die Rolle nicht über zwei separate Abfragen erreichen könnte. Umgehungsversuche werden protokolliert und sind auditierbar.

Keine ungültige Verknüpfung läuft jemals auf Ihren Daten. Rollen sind standardmäßig reguliert; der Ausstieg ist eine bewusste Vergabe des Rechts `ignore_relationships` an eine Rolle, der Sie vertrauen. Das OAuth-Token eines Agenten wird einer Rolle zugeordnet, daher gelten für einen Agenten dieselben Schichten wie für eine Person. Datenkataloge, die eine Kontextschicht bereitstellen, sagen einem Agenten, was er tun sollte; Provisa erzwingt, was er tun kann.

---

Diese Schichten komponieren. Eine Rolle mit Domänenzugriff, RLS und maskierten Spalten hat alle fünf Constraints gleichzeitig aktiv. Eine neue Datenquelle, Spalte oder Beziehung hinzuzufügen erfordert nicht, jede Regel zu aktualisieren — jede Schicht ist unabhängig konfiguriert und gilt automatisch für jede Abfrage, die regulierte Objekte berührt.

### macOS

1. [Provisa-macOS.dmg](https://provisa.dev/dl/macos) herunterladen (immer die neueste Version)
2. **Provisa.app** in `/Applications` ziehen und zum Starten doppelklicken
3. Der erste Start schließt eine einmalige Einrichtung ab (~2 Min., keine Internetverbindung erforderlich)
4. Terminal öffnen:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) herunterladen (immer die neueste Version)
2. Ausführbar machen und starten — der erste Start schließt eine einmalige Einrichtung ab (keine Internetverbindung erforderlich):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) herunterladen (immer die neueste Version)
2. Den Installer ausführen — keine Administratorrechte erforderlich
3. **Provisa First Launch** aus dem Startmenü öffnen — schließt eine einmalige Einrichtung ab (~5 Min., keine Internetverbindung erforderlich)
4. Ein neues Terminal öffnen:

```bash
provisa start
```

### Erste Abfrage {: #first-query }

In der lokalen Entwicklung (`PROVISA_MODE=test`) sind keine Anmeldedaten erforderlich. In der Produktion authentifizieren Sie sich mit einem Bearer-Token — die Rolle wird automatisch daraus extrahiert.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

[provisa-jdbc.jar](https://provisa.dev/dl/jdbc) herunterladen (immer die neueste Version) und dem Treiberpfad Ihres BI-Tools hinzufügen.

```text
jdbc:provisa://localhost:8815
```

Authentifizieren Sie sich mit Ihrem Provisa-Benutzernamen und -Passwort — der Server weist Ihre Rolle zu.

Siehe [docs/integrations.md](docs/integrations.md) für Tableau- und Power-BI-Einrichtungsschritte.

### PostgreSQL Wire Protocol (pgwire)

Provisa spricht das PostgreSQL-Wire-Protokoll auf Port 5439. Jeder Client, der sich mit Postgres verbinden kann, verbindet sich mit Provisa — kein Treiber, kein Adapter, keine Änderungen an bestehendem Tooling.

**Der PostgreSQL-Benutzername wählt die Provisa-Rolle.** Mit `provider: none` (Trust-Modus) wird das Passwort ignoriert, und jeder konfigurierte Rollenname wird als Benutzername akzeptiert — verbinden Sie sich als `analyst`, `admin` oder jede Rolle, um die regulierte Sicht dieser Rolle auf die Daten zu sehen. Mit `provider: simple` wird das Passwort bcrypt-validiert. Andere Provider (`firebase`, `keycloak`, `oauth`) werden über pgwire nicht unterstützt.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Alle Abfragen laufen durch die vollständige Governance-Pipeline — Domänenzugriff, RLS, Maskierung und Prädikat-Guard gelten genau wie bei GraphQL und REST. Schema-Browser (DBeaver, DataGrip, pgAdmin) funktionieren sofort: `pg_catalog`- und `information_schema`-Abfragen werden aus einem In-Memory-Katalog beantwortet, der auf den Domänenzugriff der Rolle beschränkt ist, sodass Nutzer nur die Tabellen und Spalten sehen, die sie abfragen dürfen.

DataGrip beim Durchsuchen des regulierten Schemas und seines Fremdschlüssel-Diagramms über pgwire — kein Treiber, kein Adapter:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS wird durch Setzen von `PROVISA_PGWIRE_CERT` und `PROVISA_PGWIRE_KEY` aktiviert. Der Port ist über `PROVISA_PGWIRE_PORT` konfigurierbar (Standard `5439`).

### Bolt (Neo4j Wire Protocol)

Provisa spricht auch das Neo4j-**Bolt**-Protokoll, sodass graph-native Tools sich direkt verbinden und Cypher gegen den föderierten Graphen ausführen — kein Export, keine separate Graphdatenbank. Richten Sie **Neo4j Browser** oder **Bloom** auf Provisa und traversieren Sie Beziehungen über Quellen hinweg mit derselben angewendeten Governance (Domänenzugriff, RLS, Maskierung).

Neo4j Browser, der Cypher gegen Provisa ausführt — Knotenlabels, Beziehungstypen und Property-Keys stammen direkt aus dem registrierten Schema:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Aktivieren Sie es durch Setzen von `PROVISA_BOLT_PORT` (Neo4js Standard ist `7687`). TLS wird mit `PROVISA_BOLT_CERT` und `PROVISA_BOLT_KEY` aktiviert. Jede Provisa-Rolle, die der authentifizierte Nutzer hält, erscheint als auswählbare `provisa_<role>`-Datenbank (der `provisa_admin`-Selektor oben) — die Auswahl einer davon grenzt die Sitzung auf die Domänenrechte dieser Rolle ein; der Nutzer kann die Rollen, die er hält, nie überschreiten.

### Python-Client

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Siehe [docs/python-client.md](docs/python-client.md) für die vollständige Referenz.

## Dokumentation {: #documentation }

| Thema | Doc |
| --- | --- |
| Entwickler-Schnellstart (aus dem Quellcode ausführen) | [docs/quickstart.md](docs/quickstart.md) |
| Vollständige YAML-Konfigurationsreferenz | [docs/configuration.md](docs/configuration.md) |
| Endpunktreferenz (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Systemdesign und Komponentenübersicht | [docs/architecture.md](docs/architecture.md) |
| Sicherheitsmodell (RLS, Maskierung, Auth) | [docs/security.md](docs/security.md) |
| Secret-Speicherung und `${secret:NAME}`-Referenzen | [docs/secrets.md](docs/secrets.md) |
| Business-Glossar und Begriffskuratierung | [docs/glossary.md](docs/glossary.md) |
| Umgebungen (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Testumgebungen aufbauen (Profiling, Fakes, synthetische Daten) | [docs/test-data.md](docs/test-data.md) |
| Referenz der Fake-Methoden | [docs/fake-methods.md](docs/fake-methods.md) |
| Demos auf einem Laptop ausführen und bauen (Sales Engineers) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Unterstützte Quelltypen | [docs/sources.md](docs/sources.md) |
| SSE-Subscriptions | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, BI-Tools, Arrow-Flight-Clients, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python-Client (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin-API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Hasura v2 / DDN Import | [docs/import.md](docs/import.md) |
| Release-Workflow (Alpha/Beta/Stable-Tags) | [docs/releasing.md](docs/releasing.md) |

## Sizing

Provisa enthält eine eingebaute Föderations-Engine für Multi-Quellen-Abfragen. Beim ersten Start wählen Sie ein RAM-Budget; Provisa leitet die Anzahl der lokalen Föderations-Worker automatisch ab.

| Host-RAM | Worker | Typische Arbeitslast |
| --- | --- | --- |
| < 24 GB | 0 | Entwicklung, Einzelquellen-Abfragen, kleine Teams |
| 24–47 GB | 1 | Kleines Team, moderate quellübergreifende Abfragen |
| 48–95 GB | 2 | Abteilungsweites Deployment, gemischte BI- + Notebook-Nutzung |
| 96 GB+ | 4 | Große Abteilung, starke gleichzeitige Föderation |

Die Worker-Anzahl kann jederzeit durch Bearbeiten von `~/.provisa/config.yaml` (`federation_workers: N`) und Ausführen von `provisa restart` geändert werden. Auf `0` setzen für reinen Koordinationsbetrieb (Single-Node).

### Über eine einzelne Box hinaus skalieren {: #scaling-beyond-a-single-box }

**Horizontale Skalierung** — Mehrere Provisa-Instanzen hinter einem Load Balancer betreiben. Jede Instanz ist ein vollständig funktionierendes System. Alle Instanzen müssen auf dieselbe Konfigurations-DB zeigen (`CONFIG_DB_HOST` auf sekundären Boxen setzen) und optional auf eine gemeinsame Redis-Instanz (`REDIS_URL`) für einen vereinheitlichten Cache. Die meisten Abfragen verteilen sich transparent; sehr große quellübergreifende Joins können die Ressourcen einer einzelnen Instanz übersteigen und eine größere Box oder einen externen Föderationscluster erfordern.

**Gemeinsam genutztes Redis** — `REDIS_URL` auf jeder Instanz setzen, um auf ein externes Redis zu zeigen. Gemeinsam genutztes Redis bedeutet, dass Cache-Einträge einer Instanz für alle verfügbar sind, was die Trefferquote clusterweit verbessert.

**Eigenen Föderationscluster mitbringen** — Provisa auf einen bestehenden externen Föderationscluster statt der eingebetteten Worker zeigen. Empfohlen für großskalige oder Cloud-Deployments; siehe [docs/deployment.md](docs/deployment.md) für die Konfiguration.

## Lizenz {: #license }

Business Source License 1.1 (unverändert, gemäß den Licensor-Covenants von MariaDB). Jede
veröffentlichte Version konvertiert am 4. Jahrestag ihrer öffentlichen Veröffentlichung zur
Change License (GPL v2.0 oder später); aktueller und neuerer Code bleibt unter BSL.
Produktiver Einsatz oberhalb der Additional-Use-Grant-Schwellenwerte (weniger als 100
Mitarbeiter/Auftragnehmer und unter 1 Mio. USD Vorjahresumsatz) erfordert eine kommerzielle
Lizenz. Siehe [LICENSE](LICENSE).

Der Licensor stimmt der Nutzung dieses Werks für KI-/ML-Training nicht zu. Siehe
[NOTICE](NOTICE), [ai.txt](ai.txt) und [robots.txt](robots.txt). Für kommerzielle
Lizenzen oder KI-Trainingslizenzen: <kennethstott@gmail.com>
