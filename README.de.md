# Provisa

**Verbinden Sie Ihre Datenbanken. Abfragen mit GraphQL, gRPC, SQL oder MCP — über jede API oder jedes Protokoll — in 5 Minuten.**

Provisa bedient jede API-Oberfläche (REST, GraphQL, SQL, gRPC, MCP und mehr) über das zusammengeführte Ergebnis Ihrer Quellen. Das kann es, weil es eine **aktive semantische Schicht** ist: eine einzige Definition Ihres Datenbestands — jede Domäne, Beziehung und Richtlinie über Ihre Quellen hinweg, ausgenommen nur die Ursprungssysteme selbst — die den Bestand gleichzeitig betreibt und regelt. Die Definition ist keine Dokumentation, die eine Engine konsultieren kann; sie *ist* die Engine. Registrierte Domänen und Beziehungen sind die einzigen legalen Join-Pfade, und Zugriffsrichtlinien werden in jeden Abfrageplan hineinkompiliert. Ein Modell, drei Aufgaben:

- **Definieren** — Domänen, Spalten und Beziehungen werden einmal deklariert. Diese Deklaration ist das Schema, das jeder Konsument sieht, und die einzige Menge von Join-Pfaden, die eine Abfrage nehmen darf.
- **Durchsetzen** — Sicherheit auf Zeilenebene, Spaltenmaskierung, Spaltensichtbarkeit und Abfragegenehmigung werden inline auf dem Ausführungspfad angewendet. Keine Abfrage erreicht Daten, ohne sie zu durchlaufen, sodass die Abdeckung durch Konstruktion vollständig ist, nicht durch Sorgfalt.
- **Auditieren** — Da jede Anfrage denselben regulierten Pfad durchläuft, wird einheitlich erfasst, wer was unter welcher Rolle und gegen welche Richtlinie abgefragt hat. Verteilte Traces, Metriken und Protokolle werden selbst als abfragbare Tabellen neben Ihren Geschäftsdaten registriert.

Ein regulierter Kern bedient jede Sprache und jeden Transport. Abfragen mit **GraphQL, Cypher oder SQL**; Konsum über **pgwire, Bolt, gRPC, REST, Arrow Flight oder JDBC**. Jede Abfragesprache wird auf eine einzige Zwischendarstellung heruntergebrochen, in der Governance einmal injiziert wird — sodass eine Richtlinie zwischen Sprachen nicht abweichen kann — und diese IR wird beim Verlassen auf den nativen Dialekt jeder Quelle zurückgemappt. Eine Sprache hinzuzufügen ist ein neues Front-End auf dem gemeinsamen Kern, keine neue Engine.

Der Bestand ist sowohl analytisch als auch transaktional. Quellübergreifende Lesevorgänge fächern sich über die Föderationsschicht auf; Schreibvorgänge und Einzelquellen-Lesevorgänge routen direkt zum Quelltreiber — identisch reguliert, aber transaktional und unter 100 ms. Arrow-Flight-Columnar-Streaming ist eingebaut.

Das gesamte Modell besteht aus einer Handvoll Primitiven — Domänen, Beziehungen, Rollen und Richtlinien. Kleines Vokabular, sodass die Definition leicht verständlich sowie einfach zu evaluieren und zu auditieren ist: Sie können das Richtlinien-Set lesen und wissen, was es tut. Provisa ist ein leichter Abfrage-Compiler, keine Runtime, die im Datenpfad sitzt. Es wandelt eine Anfrage in native Abfragen um, routet sie und tritt zur Seite — deshalb performt der Bestand.

Dieses Design unterstützt zwei Nutzungsarten, die sich nicht ausschließen:

- **Als Gerüst für die Modernisierung** — Modellieren Sie Ihren Bestand, lassen Sie Provisa das native SQL für jede Quelle generieren, erfassen Sie dieses SQL dann und übernehmen Sie es direkt im Zielsystem. Provisa ist die Übergangsschicht, keine dauerhafte Abhängigkeit.
- **Als dauerhafte richtliniendurchsetzende Infrastruktur** — Behalten Sie es als den regulierten Pfad bei, den jede Abfrage nimmt, sodass Definition, Durchsetzung und Audit vereint bleiben, solange der Bestand existiert.

## Das Föderationsmodell

Das gesamte Modell läuft auf zwei Verträge und zwei Richtlinien hinaus: Quellen reduzieren sich auf 2-D-Tabellen über ein Typsystem, Abfragen reduzieren sich auf eine SQL-ähnliche IR, Erreichbarkeit entscheidet, was live abgefragt versus materialisiert wird, und eine Aktualitätsstrategie regelt jede materialisierte Kopie und jedes abgeleitete Dataset. Datenform rein, Abfrageform rein, Governance am Join, native Abfragen raus. Der Rest dieses Abschnitts geht jeden Teil durch.

Das Modell ruht auf einer Reduktion: Jede Quelle wird als Sammlung zweidimensionaler Tabellen über einem einzigen, generalisierten Typsystem ausgedrückt. Das ist der Vertrag, den eine Quelle erfüllen muss, um dem Bestand beizutreten, und es ist derselbe Vertrag für alle. Manche Quellen passen bereits — eine MySQL- oder PostgreSQL-Tabelle *ist* eine typisierte 2-D-Relation. Manche passen mit einer Projektion: Ein GraphQL-Abfrageergebnis ist, einmal abgeflacht, eine Tabelle. Manche sind der Form fremd — SPARQL-Triplestores, Neo4j — bleiben aber handhabbar, weil der Nutzer eine Abfrage liefert, deren Ergebnismenge tabellarisch ist; die Abfrage ist der Adapter. Unabhängig von der Quelle sieht der Bestand Zeilen, Spalten und generalisierte Typen — sonst nichts. Eine neue Art von Quelle einzubinden bedeutet, diesen einen Vertrag zu erfüllen, manchmal mit einem Schritt menschlichen Eingriffs, nicht eine maßgeschneiderte Integration zu schreiben.

Diese Reduktion hat ein Pendant auf der Abfrageseite. SQL — über all seine Dialekte und Eigenheiten hinweg — ist im Wesentlichen die Sprache für Analysen über 2-D-Datasets, was eine SQL-ähnliche Form zum natürlichen universellen Ziel für Abfragen macht. Also wird jede Anfrage, in welcher Sprache auch immer sie ankommt, als allerersten Schritt auf diese Zwischendarstellung heruntergebrochen. Manche lassen sich sauber herunterbrechen — SQL selbst, sogar GraphQL; manche sind schwierig — Cyphers Pfad- und Graphsemantik erfordert echte Arbeit — aber alle sind machbar. Jede Anfrage vor allem anderen in eine IR zu trichtern ist, was es erlaubt, Governance an genau einer Stelle anzuwenden, auf einer Form, unabhängig von der Sprache, in der sie ankam.

Aufbauend auf diesen beiden einheitlichen Formen — tabellarische Quellen und eine einzige Abfrageform — bedeutet Föderation hier sowohl Live-Abfrage als auch Warehousing — dieselbe Spanne, die eine Live-Query-Engine wie Trino abdeckt, plus die Materialisierung, auf die sich solche Engines stützen. Das Konzept, das sie vereint, ist **Erreichbarkeit**: Kann die Engine eine Quelle für eine gegebene Quelle direkt abfragen, oder müssen ihre Daten zuerst irgendwo abfragbar materialisiert werden? Erreichbarkeit teilt den Bestand in das, was live abgefragt wird, und das, was zuerst kopiert wird.

Die meisten Datenbanken haben bereits eine gewisse Vorstellung von einer Live-Verbindung — DuckDB `ATTACH`, PostgreSQL `postgres_fdw`, Databricks External Links. Die meisten Datenbanken können also bis zu einem gewissen Grad als Föderations-Engine fungieren. Keine ist umfassend: Jede erreicht eine bestimmte Menge von Quellen und materialisiert den Rest, ohne eine einheitliche Aussage darüber, welche welche ist. Das Modell schließt diese Lücke, indem es Erreichbarkeit explizit macht — eine definierte Menge von Methoden pro Quelle, die festlegen, was die Engine live erreichen kann und, durch Ausschluss, was materialisiert werden muss.

Was bleibt, ist Aktualität: Wie aktuell muss die materialisierte Kopie jeder nicht erreichbaren Quelle sein? In der Praxis reduziert sich das auf eine kleine Menge von Strategien — bei Bedarf, nach Zeitplan, bei einem Änderungssignal (CDC, Watermark, Snapshot) oder festgepinnt. Eine pro Quelle zu wählen ist die gesamte Aktualitätsrichtlinie.

Analytische Datasets — abgeleitete Tabellen, Aggregate, die Ausgaben eines Transforms — fügen sich in dieselbe Form. Auch sie müssen in der IR ausgedrückt werden, und weil sie es sind, ist Lineage kein separat zu pflegendes System: Der Pfad von jedem Ursprungssystem zu einer finalen Ausgabe *ist* die IR, die ihn erzeugt hat, durchgängig lesbar. Ihre Erstellung wirft die Aktualitätsfrage einen Schritt entfernt auf — aktualisiert sich das Dataset nach Zeitplan, erst wenn seine Vorbedingungen erfüllt sind, kontinuierlich als Near-Real-Time, oder als festgepinnter historischer Snapshot? Die Arten, wie und wann ein Dataset aufgebaut wird, sind dieselbe kleine, aufzählbare Menge, sodass ein abgeleitetes Dataset eine Build-Richtlinie im exakt gleichen Vokabular trägt wie eine Quellkopie.

Dimensionale Modelle sind eine direkte Anwendung. Die Fakt- und Dimensionstabellen eines Sternschemas sind analytische Datasets wie jedes andere — eine Dimension ist eine konformierte, deduplizierte Projektion; eine Fakttabelle ist ein auf eine Granularität reduzierter Join mit Aggregat — jede mit eigener Build- und Aktualitätsrichtlinie. Langsam wechselnde Dimensionen benötigen keine Sondermaschinerie: Ein festgepinnter Snapshot ist Typ-2-Historie, ein geplanter Rebuild ist Typ 1. Und weil das Schema in der IR definiert ist statt physisch an die Tabellen eines Warehouses gebunden, lassen sich dieselben Fakt- und Dimensionsdefinitionen umzielen — materialisiert in Oracle, in Databricks oder virtuell über einer MPP-Engine belassen — ohne Remodellierung. Das Modell generiert das Sternschema; es sperrt es nicht auf eine Engine fest.

Data Vault passt auf dieselbe Weise, eine Schicht früher. Seine Hubs sind deduplizierte Business-Key-Datasets, seine Links sind die registrierten Beziehungen zwischen ihnen, und seine Satelliten sind insert-only, zeitgestempelte Attribut-Datasets — der historische Datensatz. Ein Satellit ist einfach ein abgeleitetes Dataset auf der Änderungssignal-Aktualitätsstrategie: Load-Date plus Hashdiff ist CDC angewendet auf beschreibende Attribute, und Insert-only-Historie ist die festgepinnte-Snapshot-Strategie. Point-in-Time- und Bridge-Tabellen sind weitere abgeleitete Datasets, die für Abfrageperformance gebaut sind. Ein Raw Vault ist also eine Menge analytischer Datasets in der IR, und ein Sternschema ist eine Projektion davon — beide generiert, beide über Engines hinweg portabel. Was das Modell nicht tut, ist die Methodik zu entscheiden: was zum Hub wird, die Granularität eines Satelliten, die Split-Strategie. Das bleiben Modellierungsentscheidungen; einmal getroffen, leben sie als portable IR statt als an ein Warehouse geschweißtes ETL.

Beide Muster werden über **zwei erstklassige Shortcuts** deklariert, statt handgeschriebene Views — die Primitiven, aus denen jedes Sternschema und jeder Data Vault gebaut sind, methodikneutral gehalten:

- **`entity`** — eine geschlüsselte, deduplizierte, optional historisierte Projektion einer Quelle. Deklarieren Sie einen Entitätsschlüssel, die Attribute und einen Historisierungsmodus; Provisa bricht sie auf eine materialisierte Sicht herunter und, wenn Historie angefragt ist, auf eine **bitemporale MV** (`scd2` → Delta, `snapshot` → Snapshot). Ein Konstrukt bedient eine Kimball-**Dimension** (SCD1/SCD2) und einen Data-Vault-**Hub + Satellit**.
- **`fact`** — ein Join zu Entitätsschlüsseln, auf eine deklarierte Granularität reduziert, mit aggregierten Measures. Provisa bricht ihn auf eine Aggregat-MV plus registrierte Beziehungen zu den Entitäten herunter. Ein Konstrukt bedient eine Stern-**Fakttabelle** und einen Data-Vault-**Link** (ein maßloser Fakt ist ein reiner Key-Set-Link).

Weil das Herunterbrechen rein ist — eine `entity`/`fact`-Spezifikation wird genau zu den MV-, bitemporalen und Beziehungsdefinitionen, die ein Modellierer sonst von Hand schreiben würde —, ist das Warehouse durchgängig IR und zielt über Engines hinweg um, ohne Remodellierung. Deklarieren Sie ein Warehouse in der Admin-UI (ein **Model**-Formular für Entities und Facts) oder über die Admin-API (`registerEntity` / `registerFact`); das Modell *generiert* den Kimball-Stern oder den Data Vault, es erzwingt keinen.

### Time Travel

Time Travel ist eine einfache Idee — jede Version einer Zeile aufbewahren statt sie zu überschreiben, sodass Sie fragen können, was die Daten zu jedem vergangenen Zeitpunkt *waren*. Was sich unterscheidet, ist, wie effizient jede Engine das kann, weshalb Provisa es genau deshalb zu einer Eigenschaft der Definition der materialisierten Sicht macht (REQ-1162), nicht der Storage-Engine. Einmal deklariert, funktioniert es auf jedem materialisierenden Backend.

Die Regel, die es portabel hält, ist **Append-only**: Eine einmal geschriebene Version wird nie aktualisiert oder gelöscht. Eine Zeile durch Zurückschreiben eines „Valid-to"-Datums zu retiren — der übliche bitemporale Trick — erfordert ein UPDATE, das viele Engines über einem föderierten Store nicht günstig (oder überhaupt) ausführen können, also tut Provisa das nicht. Stattdessen **hängt** jede Aktualisierung an, und „welche Version zum Zeitpunkt T galt" wird zur Lesezeit aus dem unveränderlichen Log abgeleitet. Es gibt genau zwei Arten anzuhängen:

- **Snapshot** — das gesamte frische Dataset anhängen, mit der Systemzeit dieser Aktualisierung gestempelt. Kein Diffing; auf jeder Engine korrekt; Speicherbedarf wächst um eine vollständige Kopie pro Aktualisierung.
- **Delta** — nur das Geänderte anhängen, plus Tombstones für entfernte Schlüssel. Das Delta wird **von der Engine berechnet** (Anti-Joins innerhalb eines `INSERT … SELECT`), niemals zeilenweise in Provisa gefaltet. Kleiner, und es benötigt einen Entitätsschlüssel.

Systemzeit (wann Provisa eine Version aufgezeichnet hat) wird auf diese Weise verwaltet; Valid Time (wann ein Fakt im Business wahr ist) wird vom eigenen SELECT der Sicht geliefert und erhalten. Engines, die mehr bieten — native Iceberg-Snapshots, ein MERGE, das weniger Zeilen pflegt — können hinter derselben Deklaration auf Effizienz gezielt werden; der Append-only-Pfad ist der überall korrekte Boden.

Lesen ist transparent. Eine schlichte Abfrage gegen eine bitemporale MV rekonstruiert standardmäßig den **aktuellen** Zustand aus dem Append-Log; um in der Zeit zu reisen, senden Sie einen `X-Provisa-As-Of: <timestamp>`-Header, und die gesamte Abfrage wird beantwortet, wie der Bestand zu diesem Zeitpunkt war — identische Semantik auf jedem Substrat. Aktivieren Sie es für jede materialisierte Sicht in der Admin-UI (ein **Time-Travel**-Steuerelement: aus / Snapshot / Delta plus ein Entitätsschlüssel) oder über die Admin-API.

Erreichbarkeit plus Aktualität ist ein allgemeines Modell für Datenföderation: eine Definition, die sagt, was live ist, was materialisiert ist und wie aktuell jede Kopie bleibt — unabhängig von der Reichweite einer einzelnen Engine. Das Ergebnis ist Freiheit von proprietärem Lock-in. Das Modell ist portabel; der Bestand ist nicht an die Föderation des einen Anbieters gefesselt, der heute zufällig die meisten Quellen erreicht.

## Funktionen

### Abfrageschnittstellen

Dies sind die Sprachen und strukturierten APIs, in denen Sie Abfragen schreiben. Jede hat ihre eigene Syntax und Semantik; Governance (RLS, Maskierung, Spaltensichtbarkeit, Beziehungsdurchsetzung) gilt einheitlich über alle hinweg, unabhängig davon, welches Wire-Protokoll sie liefert.

- **GraphQL** — Rollenspezifische Schemas mit feldgenauer Sichtbarkeit, Filterung, Cursor-basierter Paginierung und Aggregatabfragen (`count`, `sum`, `avg`, `min`, `max`). Schema-eingeschränkt auf registrierte Beziehungen — durch Konstruktion strukturell gültig, der schnellste Weg zu einer korrekten einfachen Abfrage. Apollo APQ inklusive: Abfragen werden gehasht und serverseitig registriert; nachfolgende Aufrufe senden nur den Hash über HTTP GET, was Antworten CDN-cachebar macht, ohne dass Client-Änderungen nötig sind. Lookup-Tabellen unterhalb eines konfigurierbaren Zeilenschwellenwerts werden als Enum-Typen exponiert.
- **SQL** — Vollständiges SQL über föderierte Daten; uneingeschränkt und ausdrucksstärker als GraphQL. Schreiben Sie Standard-SQL — korrelierte Unterabfragen inklusive — und es läuft unverändert über Quellen hinweg. Einzelquellen-Abfragen umgehen die Föderationsschicht vollständig (unter 100 ms).
- **Cypher** — Graph-Abfragesprache über demselben föderierten Schema. Beziehungen als Graph-Kanten traversieren; Quellen vereinigen; Pfade variabler Länge. Governance gilt identisch wie bei GraphQL und SQL.
- **gRPC-Model-API** — Automatisch generiertes `.proto` aus dem registrierten Schema; typisierte Query- und Insert-RPCs pro Tabelle, gestreamte Antworten. Schema-getrieben im selben Sinne wie GraphQL — das Registrierungsmodell ist der Vertrag, Protobuf ist die Wire-Kodierung. Anders als Arrow Flight (das ein Columnar-Streaming-Transport ist) ist dies eine vollständige Pro-Tabelle-Abfrageschnittstelle.
- **JSON:API** — Strukturierte Abfrage-API unter `/data/jsonapi/{table}`, per Design nur HTTP. Unterstützt JSON:API 1.1: Sparse Fieldsets (`fields[table]=col1,col2`), Filterausdrücke (`filter[field][op]=value`), zusammengesetzte Dokumente (`include=relation`) und Sortierung. Keine Allzweck-Abfragesprache — fragt eine Tabelle nach der anderen mit standardisierter Filtersyntax ab statt mit einem Ad-hoc-Abfragestring.
- **Query Language Explorer** — Schreiben Sie eine GraphQL-Abfrage und sehen Sie live **Semantic-SQL**- und **Cypher**-Übersetzungen in Seitenpanels; kopieren Sie eine davon oder springen Sie direkt in den SQL- oder Graph-Editor. Ein praktischer Workflow ist, Abfragefragmente in GraphQL zu skizzieren und dann das resultierende SQL in komplexe Views oder Reports einzufügen.

Der Explorer zeigt eine GraphQL-Abfrage neben ihren live SQL- und Cypher-Übersetzungen:

![Query Language Explorer](docs/images/query-explorer.png)

Dasselbe föderierte Schema ist als lebender Graph erkundbar — Domänen- und Knotenlabels, Beziehungstypen und Traversierungen variabler Länge:

![Graph Visualization](docs/images/graph-view.png)

### Werkzeuge zur Abfragekomposition

Diese Werkzeuge helfen Ihnen, Abfragen in den obigen Sprachen zu schreiben — sie sind selbst keine Abfragesprachen.

- **Natural-Language-Abfrage** — NL→SQL/Cypher/GraphQL-Pipeline, angetrieben von Claude. Beschreiben Sie in einfachem Englisch, was Sie wollen; die Pipeline erzeugt eine Abfrage in Ihrer gewählten Sprache mit einer interaktiven Validierungsschleife vor der Ausführung.

![Natural Language Query](docs/images/natural-language.png)

### Wire-Protokolle

Dies sind die Verbindungsprotokolle. SQL, GraphQL und Cypher reiten über ihnen — die Wahl des Wire-Protokolls ändert die Abfrageschnittstelle oder das Governance-Verhalten nicht.

- **pgwire** — Jeder PostgreSQL-Client (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) verbindet sich auf Port 5439, als wäre es ein Postgres-Server. Akzeptiert nur SQL. Volle Governance-Pipeline gilt. `pg_catalog` und `information_schema` werden aus einem In-Memory-Katalog beantwortet, sodass Schema-Browser ohne Föderations-Roundtrip funktionieren. TLS optional.
- **Bolt (Neo4j)** — Jeder Neo4j-Client (Neo4j Browser, Bloom, offizielle Treiber) verbindet sich über das Bolt-Protokoll und führt Cypher gegen den föderierten Graphen aus. Jede Rolle, die der Nutzer hält, erscheint als `provisa_<role>`-Datenbank. Dieselbe Governance wie jeder andere Transport. TLS optional.
- **Arrow Flight** — Durchsatzstarkes Columnar-Streaming über gRPC; akzeptiert GraphQL oder SQL als Abfrageeingabe. Unbegrenzte Ergebnismengen, keine serverseitige Materialisierung, keine separate Infrastruktur erforderlich.
- **JDBC** — BI-Tool-Integration (Tableau, Power BI, DBeaver) im `approved`- oder `catalog`-Modus.
- **WebSocket / SSE** — Subscriptions: Near-Real-Time-Änderungsereignisse; Backends: PG-nativ, MongoDB-nativ, CDC, Polling. Auch über Kafka exponiert.

### Datenquellen

- **53 Quelltypen** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL-Triplestores, Kafka, Google Sheets und mehr über eine einzige API; Graph- und RDF-Quellen sind erstklassig, keine Adapter
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

### Sicherheit & Governance

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

## Sicherheitsmodell

Hier hört „auf dem Pfad, den jede Abfrage ohnehin nimmt" auf, ein Slogan zu sein. Provisa setzt ein mehrschichtiges Sicherheitsmodell über jede Abfragesprache (GraphQL, SQL, Cypher) und jeden Transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket) durch. Governance wird einheitlich angewendet — es gibt keinen Abfragepfad, der sie umgeht. Abdeckung ist durch Konstruktion vollständig, nicht durch Sorgfalt: Fügen Sie eine Quelle, Spalte oder Beziehung hinzu, und jede Schicht gilt automatisch dafür, ohne dass etwas zu registrieren zu merken wäre.

Die Schichten gelten der Reihe nach. Eine Anfrage muss jede Schicht passieren, bevor die nächste ausgewertet wird.

### Schicht 0 — Introspektionsfilterung

Das Schema und der Katalog, die einer Rolle präsentiert werden, enthalten nur die Tabellen in ihrer `domain_access`-Liste und die Spalten, die pro-Spalte-`visible_to`-Regeln bestehen. Objekte außerhalb des Zugriffs einer Rolle sind zum Zeitpunkt der Entdeckung unsichtbar — sie können nicht abgefragt, autovervollständigt oder als existent erschlossen werden. Dies gilt für das GraphQL-Schema, den SQL-Katalog und den Schema-Browser des Abfrage-Editors.

### Schicht 1 — Öffentlicher Zugriff

Tabellen in Domänen ohne `domain_access`-Beschränkung sind für alle authentifizierten Identitäten ohne zusätzliche Konfiguration sichtbar. Keine Reibung für genuin öffentliche Daten.

### Schicht 2 — Domänenzugriff

Jede Rolle trägt eine `domain_access`-Liste von Domänen-IDs. Eine Abfrage, die eine Tabelle außerhalb dieser Domänen berührt, wird vor der Ausführung abgelehnt. Dies ist die grobe Eigentumsgrenze — eine HR-Rolle kann Finanztabellen nicht erreichen, unabhängig davon, wie das SQL geschrieben ist.

### Schicht 3 — Sicherheit auf Zeilenebene

Nachdem der Domänenzugriff bestätigt ist, werden pro-Tabelle, pro-Rolle `WHERE`-Prädikate zur Ausführungszeit in jedes `SELECT` injiziert. Die Prädikate werten gegen Rohdaten aus. Ein regionaler Manager, der eine gemeinsam genutzte Orders-Tabelle abfragt, sieht selbst bei einem `SELECT *` nur die Zeilen seiner Region.

### Schicht 4 — Spaltensichtbarkeit und Maskierung

Spalten mit einer `visible_to`-Liste, die die anfragende Rolle ausschließt, werden aus der Abfrageausgabe entfernt. Spalten mit einer Maskierungsregel haben ihre Werte ersetzt — Regex-Schwärzung, Konstantenersetzung oder Trunkierung — bevor Ergebnisse den Server verlassen. Maskierung gilt in allen Abfragesprachen und Ausgabeformaten.

### Schicht 5 — Prädikat-Guard

Maskierte Spalten werden aus `WHERE`- und `HAVING`-Klauseln abgelehnt. Ohne dies könnte ein Aufrufer den unmaskierten Wert durch Binärsuche in einem Filter erschließen, selbst wenn die Ausgabe maskiert ist. Die Ablehnung wird zur Abfrage-Parse-Zeit durchgesetzt, vor der Ausführung.

### Beziehungs-Governance

JOIN-Bedingungen in SQL müssen einer registrierten, genehmigten Beziehung zwischen Tabellen entsprechen. Nicht genehmigte Joins werden abgelehnt. Jede Beziehung trägt einen menschenlesbaren Grund und eine Beschreibung — eine Orientierung für sowohl Nutzer als auch autonome Agenten darüber, warum ein Traversierungspfad existiert. Dies ist Governance-Richtlinie, keine harte Sicherheitsgrenze: Die Schichten 2–5 gelten unabhängig von der Join-Struktur, sodass eine gezielte Umgehung keine Daten exponiert, die die Rolle nicht über zwei separate Abfragen erreichen könnte. Umgehungsversuche werden protokolliert und sind auditierbar.

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

### Erste Abfrage

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

- **`catalog`-Modus** — vollständiges Schema sichtbar; für Katalog-Tools verwenden (Collibra, Atlan, DBeaver)

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

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Siehe [docs/python-client.md](docs/python-client.md) für die vollständige Referenz.

## Dokumentation

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

### Über eine einzelne Box hinaus skalieren

**Horizontale Skalierung** — Mehrere Provisa-Instanzen hinter einem Load Balancer betreiben. Jede Instanz ist ein vollständig funktionierendes System. Alle Instanzen müssen auf dieselbe Konfigurations-DB zeigen (`CONFIG_DB_HOST` auf sekundären Boxen setzen) und optional auf eine gemeinsame Redis-Instanz (`REDIS_URL`) für einen vereinheitlichten Cache. Die meisten Abfragen verteilen sich transparent; sehr große quellübergreifende Joins können die Ressourcen einer einzelnen Instanz übersteigen und eine größere Box oder einen externen Föderationscluster erfordern.

**Gemeinsam genutztes Redis** — `REDIS_URL` auf jeder Instanz setzen, um auf ein externes Redis zu zeigen. Gemeinsam genutztes Redis bedeutet, dass Cache-Einträge einer Instanz für alle verfügbar sind, was die Trefferquote clusterweit verbessert.

**Eigenen Föderationscluster mitbringen** — Provisa auf einen bestehenden externen Föderationscluster statt der eingebetteten Worker zeigen. Empfohlen für großskalige oder Cloud-Deployments; siehe [docs/deployment.md](docs/deployment.md) für die Konfiguration.

## Lizenz

Business Source License 1.1 (unverändert, gemäß den Licensor-Covenants von MariaDB). Jede
veröffentlichte Version konvertiert am 4. Jahrestag ihrer öffentlichen Veröffentlichung zur
Change License (GPL v2.0 oder später); aktueller und neuerer Code bleibt unter BSL.
Produktiver Einsatz oberhalb der Additional-Use-Grant-Schwellenwerte (weniger als 100
Mitarbeiter/Auftragnehmer und unter 1 Mio. USD Vorjahresumsatz) erfordert eine kommerzielle
Lizenz. Siehe [LICENSE](LICENSE).

Der Licensor stimmt der Nutzung dieses Werks für KI-/ML-Training nicht zu. Siehe
[NOTICE](NOTICE), [ai.txt](ai.txt) und [robots.txt](robots.txt). Für kommerzielle
Lizenzen oder KI-Trainingslizenzen: <kennethstott@gmail.com>
