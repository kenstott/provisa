# Provisa

**Verbinden Sie Ihre Datenbanken. Fragen Sie ab mit GraphQL, gRPC, SQL oder MCP — über jede beliebige API oder jedes Protokoll — in 5 Minuten.**

Provisa bedient jede API-Oberfläche (REST, GraphQL, SQL, gRPC, MCP und mehr) über das zusammengeführte Ergebnis Ihrer Quellen hinweg. Das ist möglich, weil es sich um eine **aktive semantische Schicht** handelt: eine einzige Definition Ihres Datenbestands — jede Domäne, Beziehung und Richtlinie über all Ihre Quellen hinweg, ausgenommen nur die Ursprungssysteme selbst — die den Bestand sowohl betreibt als auch regiert. Die Definition ist keine Dokumentation, die eine Engine konsultieren kann; sie *ist* die Engine. Registrierte Domänen und Beziehungen sind die einzigen zulässigen Join-Pfade, und Zugriffsrichtlinien werden in jeden Abfrageplan hineinkompiliert. Ein Modell, drei Aufgaben:

- **Definieren** — Domänen, Spalten und Beziehungen werden einmal deklariert. Diese Deklaration ist das Schema, das jeder Konsument sieht, und die einzige Menge von Join-Pfaden, die eine Abfrage nehmen darf.
- **Durchsetzen** — Sicherheit auf Zeilenebene, Spaltenmaskierung, Spaltensichtbarkeit und Abfragefreigabe werden inline auf dem Ausführungspfad angewendet. Keine Abfrage erreicht Daten, ohne diese zu durchlaufen, sodass die Abdeckung durch Konstruktion vollständig ist, nicht durch Sorgfalt.
- **Auditieren** — Weil jede Anfrage denselben regierten Pfad durchläuft, wird einheitlich aufgezeichnet, wer was unter welcher Rolle und gegen welche Richtlinie abgefragt hat. Verteilte Traces, Metriken und Protokolle sind selbst als abfragbare Tabellen neben Ihren Geschäftsdaten registriert.

Ein einziger regierter Kern bedient jede Sprache und jeden Transport. Fragen Sie ab mit **GraphQL, Cypher oder SQL**; konsumieren Sie über **pgwire, Bolt, gRPC, REST, Arrow Flight oder JDBC**. Jede Abfragesprache wird auf eine einzige Zwischendarstellung heruntergebrochen, in die Governance einmal injiziert wird — sodass eine Richtlinie zwischen den Sprachen nicht abweichen kann — und diese IR retargetiert beim Verlassen auf den nativen Dialekt jeder Quelle. Das Hinzufügen einer Sprache ist ein neues Front-End auf dem gemeinsamen Kern, keine neue Engine.

Der Bestand ist sowohl analytisch als auch transaktional. Quellenübergreifende Lesevorgänge fächern durch die Föderationsschicht auf; Schreibvorgänge und Einzelquellen-Lesevorgänge routen direkt zum Quellentreiber — identisch regiert, aber transaktional und unter 100 ms. Arrow-Flight-Columnar-Streaming ist eingebaut.

Das gesamte Modell ist aus einer Handvoll Primitiven aufgebaut — Domänen, Beziehungen, Rollen und Richtlinien. Ein kleines Vokabular, sodass die Definition leicht zu erfassen und einfach zu bewerten und zu auditieren ist: Sie können das Regelwerk lesen und wissen, was es tut. Provisa ist ein leichter Abfragecompiler, keine Laufzeitumgebung, die im Datenpfad sitzt. Es wandelt eine Anfrage in native Abfragen um, routet sie und geht aus dem Weg — deshalb performt der Bestand.

Dieses Design unterstützt zwei Nutzungsarten, die sich nicht ausschließen:

- **Als Gerüst für die Modernisierung** — Modellieren Sie Ihren Bestand, lassen Sie Provisa das native SQL für jede Quelle generieren, erfassen Sie dieses SQL dann und übernehmen Sie es direkt im Zielsystem. Provisa ist die Übergangsschicht, keine dauerhafte Abhängigkeit.
- **Als dauerhafte, richtliniendurchsetzende Infrastruktur** — Behalten Sie es als den regierten Pfad bei, den jede Abfrage nimmt, sodass Definition, Durchsetzung und Audit vereinheitlicht bleiben, solange der Bestand existiert.

## Das Föderationsmodell

Das gesamte Modell läuft auf zwei Verträge und zwei Richtlinien hinaus: Quellen reduzieren sich auf 2D-Tabellen über einem Typsystem, Abfragen reduzieren sich auf eine SQL-artige IR, Erreichbarkeit entscheidet, was live abgefragt und was materialisiert wird, und eine Aktualitätsstrategie regiert jede materialisierte Kopie und jedes abgeleitete Dataset. Datenform hinein, Abfrageform hinein, Governance am Join, native Abfragen hinaus. Der Rest dieses Abschnitts geht jeden einzelnen Baustein durch.

Das Modell beruht auf einer Reduktion: Jede Quelle wird als Sammlung zweidimensionaler Tabellen über einem einzigen, generalisierten Typsystem ausgedrückt. Das ist der Vertrag, den eine Quelle erfüllen muss, um dem Bestand beizutreten, und es ist derselbe Vertrag für alle. Manche Quellen passen bereits — eine MySQL- oder PostgreSQL-Tabelle *ist* eine typisierte 2D-Relation. Manche passen mit einer Projektion: Ein GraphQL-Abfrageergebnis ist, einmal abgeflacht, eine Tabelle. Manche sind der Form fremd — SPARQL-Triplestores, Neo4j — bleiben aber handhabbar, weil der Nutzer eine Abfrage liefert, deren Ergebnismenge tabellarisch ist; die Abfrage ist der Adapter. Ganz gleich welche Quelle, der Bestand sieht Zeilen, Spalten und generalisierte Typen — sonst nichts. Eine neue Art von Quelle anzubinden bedeutet, diesen einen Vertrag zu erfüllen, manchmal mit einem Schritt manuellen Eingreifens, nicht eine maßgeschneiderte Integration zu schreiben.

Diese Reduktion hat ein Pendant auf der Abfrageseite. SQL — über all seine Dialekte und Eigenheiten hinweg — ist im Wesentlichen die Sprache für Analysen über 2D-Datensätzen, was eine SQL-artige Form zum natürlichen universellen Ziel für Abfragen macht. Also wird jede Anfrage, in welcher Sprache sie auch eintrifft, als allerersten Schritt auf diese Zwischendarstellung heruntergebrochen. Manche brechen sauber herunter — SQL selbst, sogar GraphQL; manche sind schwierig — die Pfad- und Graphsemantik von Cypher erfordert echte Arbeit — aber alle sind machbar. Jede Anfrage vor allem anderen in eine IR zu trichtern, ist das, was Governance erlaubt, an genau einer Stelle, auf einer Form, unabhängig von der Sprache, in der sie ankam, angewendet zu werden.

Auf diesen beiden einheitlichen Formen aufbauend — tabellarische Quellen und eine einzige Abfrageform — bedeutet Föderation hier sowohl Live-Abfrage als auch Warehousing — dieselbe Spanne, die eine Live-Abfrage-Engine wie Trino abdeckt, plus die Materialisierung, auf die sich solche Engines stützen. Das Konzept, das sie vereint, ist **Erreichbarkeit**: Kann die Engine für eine Quelle direkt abfragen, oder müssen ihre Daten zuerst irgendwo abfragbar materialisiert werden? Erreichbarkeit teilt den Bestand in das, was live abgefragt wird, und das, was zuerst kopiert wird.

Die meisten Datenbanken tragen bereits eine gewisse Vorstellung einer Live-Verbindung — DuckDB `ATTACH`, PostgreSQL `postgres_fdw`, Databricks External Links. Also können die meisten Datenbanken bis zu einem gewissen Grad als Föderations-Engine agieren. Keine ist umfassend: Jede erreicht eine bestimmte Menge an Quellen und materialisiert den Rest, ohne eine einheitliche Übersicht darüber, was was ist. Das Modell schließt diese Lücke, indem es Erreichbarkeit explizit macht — eine definierte Menge von Methoden pro Quelle, die festlegen, was die Engine live erreichen kann und, durch Ausschluss, was materialisiert werden muss.

Was bleibt, ist Aktualität: Wie aktuell muss die materialisierte Kopie jeder nicht erreichbaren Quelle sein? In der Praxis reduziert sich das auf eine kleine Menge von Strategien — auf Anfrage, nach Zeitplan, bei einem Änderungssignal (CDC, Watermark, Snapshot) oder festgepinnt. Eine pro Quelle zu wählen, ist die gesamte Aktualitätsrichtlinie.

Analytische Datasets — abgeleitete Tabellen, Aggregate, die Ausgaben einer Transformation — fügen sich in dieselbe Form ein. Auch sie müssen in der IR ausgedrückt werden, und weil sie es sind, ist Lineage kein separates System, das gepflegt werden muss: Der Pfad von jedem Ursprungssystem zu einer Endausgabe *ist* die IR, die ihn erzeugt hat, durchgehend lesbar. Beim Aufbau solcher Datasets stellt sich die Aktualitätsfrage einen Schritt weiter entfernt — wird das Dataset nach Zeitplan aktualisiert, erst wenn seine Voraussetzungen erfüllt sind, kontinuierlich als Near-Realtime, oder als festgepinnter historischer Snapshot? Die Wege, um auszudrücken, wie und wann ein Dataset erstellt wird, sind dieselbe kleine, aufzählbare Menge, sodass ein abgeleitetes Dataset eine Build-Richtlinie in genau demselben Vokabular trägt wie eine Quellenkopie.

Dimensionale Modelle sind eine direkte Anwendung. Die Fakten- und Dimensionstabellen eines Sternschemas sind analytische Datasets wie jedes andere — eine Dimension ist eine konformierte, deduplizierte Projektion; eine Faktentabelle ist ein auf die Granularität reduzierter Join und Aggregat — jede mit eigener Build- und Aktualitätsrichtlinie. Langsam veränderliche Dimensionen (Slowly Changing Dimensions) benötigen keine Sondermaschinerie: Ein festgepinnter Snapshot ist Typ-2-Historie, ein geplanter Rebuild ist Typ 1. Und weil das Schema in der IR definiert ist, statt physisch an die Tabellen eines Warehouses gebunden zu sein, retargetieren dieselben Fakten- und Dimensionsdefinitionen — materialisiert in Oracle, in Databricks, oder virtuell über einer MPP-Engine belassen — ohne Neumodellierung. Das Modell erzeugt das Sternschema; es sperrt es nicht auf eine Engine fest.

Data Vault passt auf dieselbe Weise, eine Schicht früher. Seine Hubs sind deduplizierte Geschäftsschlüssel-Datasets, seine Links sind die registrierten Beziehungen zwischen ihnen, und seine Satellites sind insert-only, zeitgestempelte Attribut-Datasets — der historische Datensatz. Ein Satellite ist einfach ein abgeleitetes Dataset auf der Änderungssignal-Aktualitätsstrategie: Load-Date plus Hashdiff ist CDC angewendet auf beschreibende Attribute, und Insert-only-Historie ist die Pinned-Snapshot-Strategie. Point-in-Time- und Bridge-Tabellen sind weitere abgeleitete Datasets, die für Abfrageperformance erstellt werden. Also ist ein Raw Vault eine Menge analytischer Datasets in der IR, und ein Sternschema ist eine Projektion davon — beide generiert, beide über Engines hinweg portabel. Was das Modell nicht tut, ist die Methodik zu entscheiden: was zu einem Hub wird, die Granularität eines Satellite, die Split-Strategie. Das bleiben Modellierungsentscheidungen; einmal getroffen, leben sie als portable IR statt als an ein Warehouse geschweißtes ETL.

Beide Muster werden über **zwei erstklassige Shortcuts** deklariert, statt handgeschriebener Views — die Primitiven, aus denen jedes Sternschema und jedes Data Vault aufgebaut sind, methodikneutral gehalten:

- **`entity`** — eine schlüsselbasierte, deduplizierte, optional historisierte Projektion einer Quelle. Deklarieren Sie einen Entitätsschlüssel, die Attribute und einen Historisierungsmodus; Provisa bricht das auf eine materialisierte View herunter und, wenn Historie angefordert wird, auf eine **bitemporale MV** (`scd2` → Delta, `snapshot` → Snapshot). Ein Konstrukt bedient eine Kimball-**Dimension** (SCD1/SCD2) und einen Data-Vault-**Hub + Satellite**.
- **`fact`** — ein Join zu Entitätsschlüsseln, reduziert auf eine deklarierte Granularität, mit aggregierten Kennzahlen. Provisa bricht das auf eine Aggregat-MV plus registrierte Beziehungen zu den Entitäten herunter. Ein Konstrukt bedient eine Stern-**Faktentabelle** und einen Data-Vault-**Link** (ein kennzahlloser Fact ist ein reiner Schlüsselmengen-Link).

Weil das Herunterbrechen rein ist — eine `entity`/`fact`-Spezifikation wird genau zu den MV-, Bitemporal- und Beziehungsdefinitionen, die ein Modellierer sonst von Hand schreiben würde — ist das Warehouse durchgehend IR und retargetiert über Engines hinweg ohne Neumodellierung. Deklarieren Sie ein Warehouse in der Admin-UI (ein **Model**-Formular für Entities und Facts) oder über die Admin-API (`registerEntity` / `registerFact`); das Modell *generiert* den Kimball-Stern oder das Data Vault, es erzwingt keines von beiden.

### Time Travel

Time Travel ist eine einfache Idee — jede Version einer Zeile behalten, statt sie zu überschreiben, sodass Sie fragen können, wie die Daten zu einem beliebigen vergangenen Zeitpunkt *waren*. Was sich unterscheidet, ist, wie effizient jede Engine das kann, weshalb Provisa es genau deshalb zu einer Eigenschaft der Definition der **materialisierten View** macht statt der Speicher-Engine (REQ-1162). Deklarieren Sie es einmal; es funktioniert auf jedem materialisierenden Backend.

Die Regel, die es portabel hält, ist **Append-only**: Eine Version, einmal geschrieben, wird nie aktualisiert oder gelöscht. Eine Zeile durch Zurückschreiben eines "Valid-to"-Datums stillzulegen — der übliche bitemporale Trick — braucht ein UPDATE, das viele Engines über einem föderierten Store nicht günstig (oder überhaupt nicht) ausführen können, also tut Provisa das nicht. Stattdessen **hängt** jede Aktualisierung an, und "welche Version zum Zeitpunkt T gültig war" wird zur Lesezeit aus dem unveränderlichen Log abgeleitet. Es gibt genau zwei Arten anzuhängen:

- **Snapshot** — den gesamten frischen Datensatz anhängen, gestempelt mit der Systemzeit dieser Aktualisierung. Kein Diffing; auf jeder Engine korrekt; Speicherbedarf wächst um eine volle Kopie pro Aktualisierung.
- **Delta** — nur das anhängen, was sich geändert hat, plus Tombstones für entfernte Schlüssel. Das Delta wird **von der Engine berechnet** (Anti-Joins innerhalb eines `INSERT … SELECT`), niemals Zeile für Zeile in Provisa zusammengefaltet. Kleiner, und es braucht einen Entitätsschlüssel.

Die Systemzeit (wann Provisa eine Version aufgezeichnet hat) wird auf diese Weise verwaltet; die Gültigkeitszeit (wann ein Fakt geschäftlich zutrifft) wird vom eigenen SELECT der View geliefert und erhalten. Engines, die mehr bieten — native Iceberg-Snapshots, ein MERGE, das weniger Zeilen pflegt — können hinter derselben Deklaration auf Effizienz gezielt werden; der Append-only-Pfad ist der Boden, der überall korrekt ist.

Das Lesen ist transparent. Eine einfache Abfrage gegen eine bitemporale MV rekonstruiert standardmäßig den **aktuellen** Zustand aus dem Append-Log; um in der Zeit zu reisen, senden Sie einen `X-Provisa-As-Of: <timestamp>`-Header, und die gesamte Abfrage wird beantwortet, wie der Bestand zu diesem Zeitpunkt war — identische Semantik auf jedem Substrat. Aktivieren Sie es für jede materialisierte View in der Admin-UI (ein **Time-Travel**-Steuerelement: aus / Snapshot / Delta plus ein Entitätsschlüssel) oder über die Admin-API.

Erreichbarkeit plus Aktualität ist ein allgemeines Modell für Datenföderation: eine Definition, die festlegt, was live ist, was materialisiert ist und wie aktuell jede Kopie bleibt — unabhängig von der Reichweite einer einzelnen Engine. Das Ergebnis ist Freiheit von proprietärer Bindung. Das Modell ist portabel; der Bestand ist nicht gefangen in der Reichweite des Anbieters, der heute zufällig die meisten Quellen erreicht.

## Features

### Query Interfaces

Dies sind die Sprachen und strukturierten APIs, in denen Sie Abfragen schreiben. Jede hat ihre eigene Syntax und Semantik; Governance (RLS, Maskierung, Spaltensichtbarkeit, Beziehungsdurchsetzung) gilt einheitlich über alle hinweg, unabhängig davon, welches Wire-Protokoll sie liefert.

- **GraphQL** — Rollenspezifische Schemas mit feldbasierter Sichtbarkeit, Filterung, Cursor-basierter Paginierung und Aggregatabfragen (`count`, `sum`, `avg`, `min`, `max`). Schemabeschränkt auf registrierte Beziehungen — durch Konstruktion strukturell gültig, der schnellste Weg zu einer korrekten einfachen Abfrage. Apollo APQ inklusive: Abfragen werden gehasht und serverseitig registriert; nachfolgende Aufrufe senden nur den Hash über HTTP GET, wodurch Antworten CDN-cachefähig werden, ohne dass Client-Änderungen nötig sind. Nachschlagetabellen unterhalb einer konfigurierbaren Zeilenschwelle werden als Enum-Typen exponiert.
- **SQL** — Vollständiges SQL über föderierte Daten; uneingeschränkt und ausdrucksstärker als GraphQL. Schreiben Sie Standard-SQL — korrelierte Unterabfragen und alles — und es läuft unverändert über Quellen hinweg. Abfragen über eine einzelne Quelle umgehen die Föderationsschicht vollständig (unter 100 ms).
- **Cypher** — Graphabfragesprache über demselben föderierten Schema. Durchqueren Sie Beziehungen als Graphkanten; vereinigen Sie Quellen; Pfade variabler Länge. Governance gilt identisch zu GraphQL und SQL.
- **gRPC-Modell-API** — Automatisch generiertes `.proto` aus dem registrierten Schema; typisierte Abfrage- und Insert-RPCs pro Tabelle, gestreamte Antworten. Schemagetrieben im selben Sinne wie GraphQL — das Registrierungsmodell ist der Vertrag, Protobuf ist die Wire-Kodierung. Anders als Arrow Flight (ein Columnar-Streaming-Transport) ist dies eine vollständige Pro-Tabelle-Abfrageschnittstelle.
- **JSON:API** — Strukturierte Abfrage-API unter `/data/jsonapi/{table}`, per Design nur HTTP. Unterstützt JSON:API 1.1: Sparse Fieldsets (`fields[table]=col1,col2`), Filterausdrücke (`filter[field][op]=value`), Compound Documents (`include=relation`) und Sortierung. Keine allgemeine Abfragesprache — fragt jeweils eine Tabelle mit standardisierter Filtersyntax ab statt mit einem Ad-hoc-Abfragestring.
- **Query Language Explorer** — Schreiben Sie eine GraphQL-Abfrage und sehen Sie live **Semantic-SQL**- und **Cypher**-Übersetzungen in Seitenpanels; kopieren Sie eine davon oder springen Sie direkt in den SQL- oder Graph-Editor. Ein praktischer Workflow ist, Abfragefragmente in GraphQL zu skizzieren und das resultierende SQL dann in komplexe Views oder Berichte einzuweben.

Der Explorer zeigt eine GraphQL-Abfrage neben ihren Live-SQL- und Cypher-Übersetzungen:

![Query Language Explorer](docs/images/query-explorer.png)

Dasselbe föderierte Schema ist als Live-Graph erkundbar — Domänen- und Knotenbezeichnungen, Beziehungstypen und Pfade variabler Länge:

![Graph Visualization](docs/images/graph-view.png)

### Query-Composition-Tools

Diese Werkzeuge helfen Ihnen, Abfragen in den oben genannten Sprachen zu schreiben — sie sind selbst keine Abfragesprachen.

- **Natürlichsprachliche Abfrage** — NL→SQL/Cypher/GraphQL-Pipeline, angetrieben von Claude. Beschreiben Sie in einfachem Englisch, was Sie möchten; die Pipeline erzeugt eine Abfrage in Ihrer gewählten Sprache mit einer interaktiven Validierungsschleife vor der Ausführung.

![Natural Language Query](docs/images/natural-language.png)

### Wire Protocols

Dies sind die Verbindungsprotokolle. SQL, GraphQL und Cypher reiten über sie hinweg — die Wahl des Wire-Protokolls ändert nicht die Abfrageschnittstelle oder das Governance-Verhalten.

- **pgwire** — Jeder PostgreSQL-Client (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) verbindet sich auf Port 5439, als wäre es ein Postgres-Server. Akzeptiert nur SQL. Die vollständige Governance-Pipeline gilt. `pg_catalog` und `information_schema` werden aus einem In-Memory-Katalog beantwortet, sodass Schema-Browser ohne Föderations-Round-Trip funktionieren. TLS optional.
- **Bolt (Neo4j)** — Jeder Neo4j-Client (Neo4j Browser, Bloom, offizielle Treiber) verbindet sich über das Bolt-Protokoll und führt Cypher gegen den föderierten Graphen aus. Jede Rolle, die der Nutzer innehat, erscheint als `provisa_<role>`-Datenbank. Dieselbe Governance wie bei jedem anderen Transport. TLS optional.
- **Arrow Flight** — Hochdurchsatz-Columnar-Streaming über gRPC; akzeptiert GraphQL oder SQL als Abfrageeingabe. Unbegrenzte Ergebnismengen, keine serverseitige Materialisierung, keine separate Infrastruktur erforderlich.
- **JDBC** — BI-Tool-Integration (Tableau, Power BI, DBeaver) im Modus `approved` oder `catalog`.
- **WebSocket / SSE** — Subscriptions: Near-Realtime-Änderungsereignisse; Backends: PG-nativ, MongoDB-nativ, CDC, Polling. Ebenfalls über Kafka exponiert.

### Data Sources

- **46 Quellentypen** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, SPARQL-Triplestores, Kafka, Google Sheets und mehr über eine einzige API; Graph- und RDF-Quellen sind erstklassig, keine Adapter
- **Smart Routing** — Abfragen über eine einzelne Quelle umgehen die Föderation (unter 100 ms); Abfragen über mehrere Quellen routen durch die Föderationsschicht — bringen Sie Ihren eigenen Cluster mit oder nutzen Sie die eingebetteten Worker
- **API-Quellen** — Registrieren Sie REST-, GraphQL-, gRPC-, WebSocket- oder RSS-Endpunkte als abfragbare Tabellen; SPARQL-Hilfsmittel inklusive; föderierte Joins über API-Quellen und relationale Quellen hinweg funktionieren transparent
- **Remote-Schema-Introspektion** — Zeigen Sie auf einen beliebigen GraphQL-, OpenAPI- oder gRPC-Endpunkt; dokumentierte Operationen werden automatisch als abfragbare Tabellen, Graphknoten und -kanten exponiert, mit vollständig darübergelegter Governance
- **Dateiquellen** — CSV-, Parquet- und SQLite-Dateien als abfragbare Tabellen; unterstützt lokale Pfade und entfernten Objektspeicher (`s3://`, `ftp://`, `sftp://`)
- **Kafka-Integration** — Topics als schreibgeschützte Tabellen; Abfrageergebnisse als Kafka-Senken
- **Geplante Trigger** — Cron- und Intervall-Trigger (APScheduler), die Webhooks, Mutationen oder Kafka-Senken-Veröffentlichungen auslösen
- **Föderations-Performance-Hints** — SQL-Kommentar-Routing-Hints überschreiben automatische Routing-Entscheidungen

![Data Sources](docs/images/data-sources.png)

Quellen, Dateien und Remote-Endpunkte werden über die UI als regierte Tabellen registriert:

![Table Registration](docs/images/table-registration.png)

### Security & Governance

- **Sicherheit auf Zeilenebene** — Pro Tabelle, pro Rolle injizierte WHERE-Klausel
- **Spaltenmaskierung** — Pro-Spalten-Maskierung (Regex, Konstante, Kürzung) mit rollenbasierter Umgehung
- **Spalten-Presets** — Serverseitig statisch oder sitzungsvariablenbasiert injizierte Werte bei Insert/Update; nicht in Mutations-Eingabetypen exponiert
- **Schreibberechtigungen** — Pro-Spalten-Mutationszugriffskontrolle (`writable_by`)
- **Vererbte Rollen** — Rollen erben RLS, Sichtbarkeit und Maskierung rekursiv von einer übergeordneten Rolle
- **Verfolgte Funktionen & Webhooks** — DB-Funktionen und ausgehende Webhooks, exponiert als GraphQL-Mutationen mit typisierten Rückgabeformen
- **ABAC-Genehmigungshook** — Autorisierungshook vor der Ausführung; Webhook-, gRPC- oder Unix-Socket-Transport; pro Tabelle, pro Quelle oder global begrenzt; konfigurierbare Fallback-Richtlinie
- **Steckbare Authentifizierung** — Firebase, Keycloak, OAuth 2.0, Simple (Testen)

![Security Roles](docs/images/security-roles.png)

### Delivery & Performance

- **Materialisierte Views als aufgezeichnete Transformationen** — Eine MV erfasst die Transformation, die sie erzeugt hat: ihre Join-Form oder ihr SQL, die pro-Quellen-Eingangssignale (Iceberg-Snapshot, RDB-Watermark), aus denen sie erstellt wurde, und eine Determinismusprüfung bei der Registrierung. Weil die Transformation aufgezeichnet ist, werden Abfragen (oder Teilausdrücke) transparent auf eine frische MV umgeschrieben — strukturelles Join-Pattern-Matching mit Teiltreffer-Unterstützung, sodass eine MV, die eine Teilmenge der Joins abdeckt, weiterhin angewendet wird, mit erhaltenen verbleibenden Joins
- **Hot-Table-Inlining** — Kleine, häufig verknüpfte Nachschlagetabellen werden als VALUES-CTEs direkt in den Abfrageplan eingebettet, was quellenübergreifende Round-Trips für Dimensionsdaten eliminiert
- **Abfrage-Caching** — Rollen+RLS-partitionierter Redis-Ergebnis-Cache; APQ-Hash-Cache inklusive
- **Observability als Daten** — Verteilte Traces, Metriken und Protokolle werden über OpenTelemetry gesammelt, in Iceberg auf S3 verdichtet und automatisch als abfragbare Tabellen (`traces`, `metrics`, `logs`, `queries`) im föderierten Schema registriert; fragen Sie sie mit SQL, GraphQL oder Cypher neben Ihren Geschäftsdaten ab — verknüpfen Sie eine `customers`-Tabelle mit der `queries`-Tabelle, um zu sehen, wer was ausgeführt hat und wie lange es dauerte

### Administration & Integration

- **Admin-API** — GraphQL unter `/admin/graphql`; Konfigurations-Upload/-Download, Beziehungsbearbeitung, Abfragefreigabe
- **GraphQL Voyager** — Interaktive rollenspezifische Schemavisualisierung als Entity-Relationship-Diagramm
- **LLM-Beziehungserkennung** — Claude-gestützte Vorschläge für Fremdschlüssel-Kandidaten
- **Python-Client** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → pyarrow Tables, SQLAlchemy-Dialekt, ADBC-Unterstützung
- **Datenaufnahme** — HTTP-Endpunkte zum Einspielen von JSON-Ereignisdaten in die Plattform
- **Hasura-v2-/DDN-Import** — Konvertiert Hasura-v2-Metadaten oder DDN-Supergraph-YAML in Provisa-Konfiguration
- **Apollo Federation** — Exponiert Provisa als Apollo-Federation-v2-Subgraph

Rollenspezifisches Schema, visualisiert als Entity-Relationship-Diagramm (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Beziehungen werden registriert, genehmigt und als einzige zulässige JOIN-Pfade durchgesetzt:

![Relationships](docs/images/relationships.png)

## Security Model

Hier hört "auf dem Pfad, den jede Abfrage ohnehin nimmt" auf, ein Slogan zu sein. Provisa setzt ein mehrschichtiges Sicherheitsmodell über jede Abfragesprache (GraphQL, SQL, Cypher) und jeden Transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket) durch. Governance wird einheitlich angewendet — es gibt keinen Abfragepfad, der sie umgeht. Die Abdeckung ist durch Konstruktion vollständig, nicht durch Sorgfalt: Fügen Sie eine Quelle, Spalte oder Beziehung hinzu, und jede Schicht gilt automatisch dafür, ohne dass etwas zur Registrierung gemerkt werden müsste.

Die Schichten gelten der Reihe nach. Eine Anfrage muss jede Schicht bestehen, bevor die nächste bewertet wird.

### Layer 0 — Introspektionsfilterung

Das Schema und der Katalog, die einer Rolle präsentiert werden, enthalten nur die Tabellen in ihrer `domain_access`-Liste und die Spalten, die die spaltenweisen `visible_to`-Regeln bestehen. Objekte außerhalb des Zugriffs einer Rolle sind zum Zeitpunkt der Entdeckung unsichtbar — sie können nicht abgefragt, autovervollständigt oder als existierend inferiert werden. Dies gilt für das GraphQL-Schema, den SQL-Katalog und den Schema-Browser des Abfrage-Editors.

### Layer 1 — Öffentlicher Zugriff

Tabellen in Domänen ohne `domain_access`-Beschränkung sind für alle authentifizierten Identitäten ohne zusätzliche Konfiguration sichtbar. Keine Reibung für genuin öffentliche Daten.

### Layer 2 — Domänenzugriff

Jede Rolle trägt eine `domain_access`-Liste von Domänen-IDs. Eine Abfrage, die eine Tabelle außerhalb dieser Domänen berührt, wird vor der Ausführung abgelehnt. Dies ist die grobe Eigentumsgrenze — eine HR-Rolle kann Finanztabellen nicht erreichen, unabhängig davon, wie das SQL geschrieben ist.

### Layer 3 — Sicherheit auf Zeilenebene

Nachdem der Domänenzugriff bestätigt ist, werden pro-Tabelle, pro-Rolle `WHERE`-Prädikate zur Ausführungszeit in jedes `SELECT` injiziert. Die Prädikate werten gegen Rohdaten aus. Ein regionaler Manager, der eine gemeinsam genutzte Bestelltabelle abfragt, sieht selbst bei einem `SELECT *` nur die Zeilen seiner Region.

### Layer 4 — Spaltensichtbarkeit und -maskierung

Spalten mit einer `visible_to`-Liste, die die anfragende Rolle ausschließt, werden aus der Abfrageausgabe entfernt. Spalten mit einer Maskierungsregel haben ihre Werte ersetzt — Regex-Schwärzung, Konstantenersatz oder Kürzung — bevor Ergebnisse den Server verlassen. Maskierung gilt in allen Abfragesprachen und Ausgabeformaten.

### Layer 5 — Prädikat-Guard

Maskierte Spalten werden aus `WHERE`- und `HAVING`-Klauseln abgelehnt. Ohne dies könnte ein Aufrufer den unmaskierten Wert ableiten, indem er ihn in einem Filter binär sucht, obwohl die Ausgabe maskiert ist. Die Ablehnung wird zum Zeitpunkt des Abfrage-Parsings durchgesetzt, vor der Ausführung.

### Beziehungsgovernance

JOIN-Bedingungen in SQL müssen einer registrierten, genehmigten Beziehung zwischen Tabellen entsprechen. Nicht genehmigte Joins werden abgelehnt. Jede Beziehung trägt einen menschenlesbaren Grund und eine Beschreibung — eine Orientierung sowohl für Nutzer als auch für autonome Agenten, warum ein Traversierungspfad existiert. Dies ist Governance-Richtlinie, keine harte Sicherheitsgrenze: Die Layer 2–5 gelten unabhängig von der Join-Struktur, sodass eine gezielte Umgehung keine Daten offenlegt, die die Rolle nicht durch zwei separate Abfragen erreichen könnte. Umgehungsversuche werden protokolliert und sind auditierbar.

---

Diese Schichten kombinieren sich. Eine Rolle mit Domänenzugriff, RLS und maskierten Spalten hat alle fünf Beschränkungen gleichzeitig aktiv. Das Hinzufügen einer neuen Datenquelle, Spalte oder Beziehung erfordert keine Aktualisierung jeder Regel — jede Schicht wird unabhängig konfiguriert und gilt automatisch für jede Abfrage, die regierte Objekte berührt.

### macOS

1. Laden Sie [Provisa-macOS.dmg](https://provisa.dev/dl/macos) herunter (immer die neueste Version)
2. Ziehen Sie **Provisa.app** nach `/Applications` und doppelklicken Sie, um zu starten
3. Der erste Start schließt eine einmalige Einrichtung ab (~2 Min., kein Internet erforderlich)
4. Öffnen Sie das Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Laden Sie [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) herunter (immer die neueste Version)
2. Machen Sie es ausführbar und führen Sie es aus — der erste Start schließt eine einmalige Einrichtung ab (kein Internet erforderlich):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Laden Sie [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) herunter (immer die neueste Version)
2. Führen Sie den Installer aus — keine Administratorrechte erforderlich
3. Öffnen Sie **Provisa First Launch** aus dem Startmenü — schließt eine einmalige Einrichtung ab (~5 Min., kein Internet erforderlich)
4. Öffnen Sie ein neues Terminal:

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

Laden Sie [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) herunter (immer die neueste Version) und fügen Sie es dem Treiberpfad Ihres BI-Tools hinzu.

```text
jdbc:provisa://localhost:8815
```

Authentifizieren Sie sich mit Ihrem Provisa-Benutzernamen und -Passwort — der Server weist Ihre Rolle zu.

- **`catalog`-Modus** — vollständiges Schema sichtbar; für Katalog-Tools verwenden (Collibra, Atlan, DBeaver)

Siehe [docs/integrations.md](docs/integrations.md) für Einrichtungsschritte zu Tableau und Power BI.

### PostgreSQL Wire Protocol (pgwire)

Provisa spricht das PostgreSQL-Wire-Protokoll auf Port 5439. Jeder Client, der sich mit Postgres verbinden kann, verbindet sich mit Provisa — kein Treiber, kein Adapter, keine Änderungen an vorhandenem Tooling.

**Der PostgreSQL-Benutzername wählt die Provisa-Rolle.** Mit `provider: none` (Trust-Modus) wird das Passwort ignoriert, und jeder konfigurierte Rollenname wird als Benutzername akzeptiert — verbinden Sie sich als `analyst`, `admin` oder eine beliebige Rolle, um die regierte Ansicht dieser Rolle auf die Daten zu sehen. Mit `provider: simple` wird das Passwort bcrypt-validiert. Andere Anbieter (`firebase`, `keycloak`, `oauth`) werden über pgwire nicht unterstützt.

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

Alle Abfragen laufen durch die vollständige Governance-Pipeline — Domänenzugriff, RLS, Maskierung und Prädikat-Guard gelten exakt wie für GraphQL und REST. Schema-Browser (DBeaver, DataGrip, pgAdmin) funktionieren sofort: `pg_catalog`- und `information_schema`-Abfragen werden aus einem In-Memory-Katalog beantwortet, der auf den Domänenzugriff der Rolle beschränkt ist, sodass Nutzer nur die Tabellen und Spalten sehen, die sie abfragen dürfen.

DataGrip beim Durchsuchen des regierten Schemas und seines Fremdschlüssel-Diagramms über pgwire — kein Treiber, kein Adapter:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS wird durch Setzen von `PROVISA_PGWIRE_CERT` und `PROVISA_PGWIRE_KEY` aktiviert. Der Port ist über `PROVISA_PGWIRE_PORT` konfigurierbar (Standard `5439`).

### Bolt (Neo4j Wire Protocol)

Provisa spricht auch das Neo4j-**Bolt**-Protokoll, sodass graphnative Tools sich direkt verbinden und Cypher gegen den föderierten Graphen ausführen — kein Export, keine separate Graphdatenbank. Richten Sie **Neo4j Browser** oder **Bloom** auf Provisa und durchqueren Sie Beziehungen über Quellen hinweg mit derselben angewendeten Governance (Domänenzugriff, RLS, Maskierung).

Neo4j Browser, der Cypher gegen Provisa ausführt — Knotenbezeichnungen, Beziehungstypen und Eigenschaftsschlüssel stammen direkt aus dem registrierten Schema:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Aktivieren Sie es durch Setzen von `PROVISA_BOLT_PORT` (der Standard von Neo4j ist `7687`). TLS wird mit `PROVISA_BOLT_CERT` und `PROVISA_BOLT_KEY` aktiviert. Jede Provisa-Rolle, die der authentifizierte Nutzer innehat, erscheint als auswählbare `provisa_<role>`-Datenbank (der `provisa_admin`-Selektor oben) — die Wahl einer davon schränkt die Sitzung auf die Domänenrechte dieser Rolle ein; der Nutzer kann nie die Rollen überschreiten, die er innehat.

### Python Client

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

## Documentation

| Thema | Doc |
| --- | --- |
| Entwickler-Schnellstart (Ausführung aus dem Quellcode) | [docs/quickstart.md](docs/quickstart.md) |
| Vollständige YAML-Konfigurationsreferenz | [docs/configuration.md](docs/configuration.md) |
| Endpunktreferenz (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Systemdesign und Komponentenübersicht | [docs/architecture.md](docs/architecture.md) |
| Sicherheitsmodell (RLS, Maskierung, Auth) | [docs/security.md](docs/security.md) |
| Unterstützte Quellentypen | [docs/sources.md](docs/sources.md) |
| SSE-Subscriptions | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, BI-Tools, Arrow-Flight-Clients, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Python-Client (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin-API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Hasura-v2-/DDN-Import | [docs/import.md](docs/import.md) |
| Release-Workflow (Alpha-/Beta-/Stable-Tags) | [docs/releasing.md](docs/releasing.md) |

## Sizing

Provisa enthält eine eingebaute Föderations-Engine für quellenübergreifende Abfragen. Beim ersten Start wählen Sie ein RAM-Budget; Provisa leitet die Anzahl lokaler Föderations-Worker automatisch ab.

| Host-RAM | Worker | Typische Arbeitslast |
| --- | --- | --- |
| < 24 GB | 0 | Entwicklung, Abfragen über eine einzelne Quelle, kleine Teams |
| 24–47 GB | 1 | Kleines Team, moderate quellenübergreifende Abfragen |
| 48–95 GB | 2 | Abteilungs-Deployment, gemischte BI- + Notebook-Nutzung |
| 96 GB+ | 4 | Große Abteilung, starke gleichzeitige Föderation |

Die Worker-Anzahl kann jederzeit geändert werden, indem `~/.provisa/config.yaml` bearbeitet wird (`federation_workers: N`) und `provisa restart` ausgeführt wird. Auf `0` setzen, um nur mit Koordination zu laufen (Single-Node).

### Skalierung über eine einzelne Box hinaus

**Horizontale Skalierung** — Führen Sie mehrere Provisa-Instanzen hinter einem Load Balancer aus. Jede Instanz ist ein vollständig funktionierendes System. Alle Instanzen müssen auf dieselbe Konfigurations-DB zeigen (setzen Sie `CONFIG_DB_HOST` auf sekundären Boxen) und optional eine gemeinsame Redis-Instanz (`REDIS_URL`) für einen vereinheitlichten Cache. Die meisten Abfragen verteilen sich transparent; sehr große quellenübergreifende Joins können die Ressourcen einer einzelnen Instanz übersteigen und eine größere Box oder einen externen Föderations-Cluster erfordern.

**Gemeinsamer Redis** — Setzen Sie `REDIS_URL` auf jeder Instanz, um auf einen externen Redis zu zeigen. Gemeinsamer Redis bedeutet, dass Cache-Einträge einer Instanz allen zur Verfügung stehen, was die Trefferquoten im gesamten Cluster verbessert.

**Eigenen Föderations-Cluster mitbringen** — Zeigen Sie Provisa auf einen bestehenden externen Föderations-Cluster statt auf die eingebetteten Worker. Empfohlen für großflächige oder Cloud-Deployments; siehe [docs/deployment.md](docs/deployment.md) für die Konfiguration.

## License

Business Source License 1.1 (unmodifiziert, gemäß den Licensor-Covenants von MariaDB). Jede
veröffentlichte Version wechselt zur Change License (GPL v2.0 oder später) am 4. Jahrestag
ihrer öffentlichen Veröffentlichung; aktueller und neuerer Code bleibt unter BSL.
Produktiver Einsatz oberhalb der Schwellenwerte des Additional Use Grant (weniger als 100
Mitarbeiter/Auftragnehmer und unter 1 Mio. USD Vorjahresumsatz) erfordert eine kommerzielle
Lizenz. Siehe [LICENSE](LICENSE).

Der Licensor stimmt der Nutzung dieser Arbeit für KI-/ML-Training nicht zu. Siehe
[NOTICE](NOTICE), [ai.txt](ai.txt) und [robots.txt](robots.txt). Für kommerzielle
Lizenzen oder KI-Trainingslizenzen: <kennethstott@gmail.com>
