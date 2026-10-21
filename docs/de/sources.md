# Quelltypen {: #source-types }

## Ausführungsmodell {: #execution-model }

Jede Abfrage läuft am Ende über die Federation-Engine, und genau das lässt jede Quelle mit jeder anderen joinen. Wie eine Quelle dorthin kommt, hängt davon ab, wie Provisa sie erreicht, und danach gehört jede Quelle zu einer von fünf Gruppen.

| Kategorie | Hat direkten Treiber | Hat föderierten Connector | Beispiele |
| --- | --- | --- | --- |
| **Direktfähig** | Ja | Ja | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Nur Federation** | Nein | Ja | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-gestützt) |
| **Direktlesen (Replikat)** | Ja | Ja | Snowflake, Databricks, ClickHouse — Treiber liest Daten und landet ein Replikat; Abfragen laufen gegen das Replikat in der aktiven Engine |
| **Direkt (kein quellenübergreifender Connector)** | Ja | Nein | Athena — Einzelquellen-Anweisungen laufen vollständig in Athena; quellenübergreifende Joins landen ein vollständiges Replikat |
| **Materialisieren → Federation** | Nein | Nein | REST/OpenAPI, Remote-GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (Push-Empfänger), GovData, SharePoint, Salesforce, ServiceNow, Cloud-Inventar, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

**Direktfähige** Quellen beantworten eine Abfrage, die nur sie betrifft, über ihren eigenen Treiber, in unter 100 ms und ohne Umweg über die Federation-Engine. Ihren vollen Connector behalten sie, also joinen sie über die Engine weiterhin mit anderen Quellen.

**Nur-Federation**-Quellen haben keinen direkten Treiber. Jede Abfrage erreicht sie über die Federation-Schicht.

**Direktlesen (Replikat)**-Quellen haben einen DirectDriver, der nativ aus dem Warehouse liest (Arrow-nativ, wo es geht) und ein Replikat im Materialisierungs-Store der aktiven Engine landet. Abfragen laufen dann gegen dieses Replikat. Siehe [Warehouses als benannte Quellen](#warehouses-as-named-sources).

**Direkt (kein quellenübergreifender Connector)**-Quellen haben einen DirectDriver, der eine Einzelquellen-Anweisung vollständig in der Quelle ausführt, mit jedem Filter, jeder Spalte und jedem Limit weitergereicht. Einen Trino-Connector gibt es für sie nicht. Joint eine Anweisung eine solche Quelle mit einer anderen, liest der Treiber deshalb die Daten und landet ein Replikat, und der Join läuft gegen dieses Replikat in der aktiven Engine.

**Materialize**-Quellen haben ebenfalls keinen föderierten Connector. Provisa holt ihre Daten beim Start oder zur Abfragezeit und cacht sie als Parquet in S3 oder in PostgreSQL. Von dort erreicht die Federation-Engine sie für quellenübergreifende Abfragen.

---

## Alle Quellen {: #all-sources }

Provisa registriert **73** Quelltypen, die das Sources-Formular anbietet. Die folgenden Tabellen decken alle 73 ab; der Index ist die Zählung.

| # | Gruppe | Quelltypen |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Cloud-Data-Warehouses](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data-Lake / offene Tabellenformate](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Code-Quellen](code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Push-Empfänger](#push-receiver) | `ingest` |
| 45–46 | [Graph & Semantic](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Dateibasiert](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observability & Sonstiges](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [Enterprise-SaaS](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [API-Quellen](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Data-Quality-Checker](#data-quality-checkers) | `soda`, `great_expectations` |
| 71 | [Data-Profiler](#data-profiler) | `data_profiler` |
| 72 | [Kaggle-Datasets](#kaggle-datasets) | Kaggle (über das Sources-Formular bereitgestellt; registriert als `files`-Quelle — siehe [Kaggle-Datasets](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (registriert als `files`-Quelle, die die benannten Seiten crawlt — siehe [Wikipedia](#wikipedia)) |

Referenz für jeden Quelltyp, den Provisa unterstützt. „Direkter Treiber" bedeutet, dass Einzelquellen-Abfragen nativ gegen die Quelle ausgeführt werden (unter 100 ms). „Connector-Name" ist der föderierte Connector, der verwendet wird, wenn die Quelle an Mehrquellen-JOINs teilnimmt.

### RDBMS

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Ja |
| `mysql` | aiomysql | mysql | mysql | Ja |
| `mariadb` | aiomysql | mariadb | mysql | Ja |
| `singlestore` | — | singlestore | singlestore | Federiert |
| `sqlserver` | aioodbc | sqlserver | tsql | Ja |
| `oracle` | oracledb | oracle | oracle | Ja |
| `duckdb` | duckdb | memory | duckdb | Ja |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Ja |
| `firebird` | — | — (DuckDB-Erweiterung) | — | Nein |
| `airport` | — | — (DuckDB-Erweiterung) | — | Nein |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | Nein |
| `hiveserver2` | impyla (HiveServer2 über Thrift) | — | hive | Nein |

Wire-kompatible Datenbanken nutzen den JDBC-Treiber, den nativen Async-Treiber und den Dialekt eines Basisprotokolls mit. CockroachDB, YugabyteDB und Greenplum laufen über das PostgreSQL-Protokoll, TiDB über das MySQL-Protokoll. Sie brauchen nur Registry-Einträge und keinen neuen Connector-Code.

`firebird` (Firebird 3/4/5) und `airport` (Arrow-Flight-Server) sind registrierte Quelltypen, die DuckDB-Community-Erweiterungen an Ort und Stelle erreichen, wenn DuckDB die aktive Engine ist. Sie haben weder einen direkten Treiber noch einen föderierten Connector.

#### `saphana`

SAP HANA wird über einen eigenen Treiber gelesen und in den Store der aktiven Engine repliziert, ist also auf jeder Engine erreichbar. Das Formular verlangt Host, Port (Standard 39015), Benutzername, Passwort und Datenbank; bei einem Multi-Tenant-System benennt die Datenbank den Tenant. Register Table listet die Schemas auf, die der Benutzer lesen darf; die Systemschemas von HANA bleiben außen vor.

#### `hiveserver2`

Ein HiveServer2-Endpunkt, direkt über Thrift gelesen, für ein Hive-Deployment, in dem nur dieser Endpunkt erreichbar ist. Er ist von `hive` und `hive_s3` zu unterscheiden, die die Dateien und den Metastore des Warehouse lesen und nie über HiveServer2 laufen. Zeilen werden über den Endpunkt gelesen und in den Store der aktiven Engine repliziert. Das Formular verlangt Host, Port (Standard 10000), Benutzername, Passwort und Datenbank; `federation_hints.auth_mechanism` wählt die Authentifizierung (Standard `PLAIN`, zum Beispiel `GSSAPI` für Kerberos).

### Cloud-Data-Warehouses

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federiert | Liest über snowflake-connector-python; landet Replikat; `account`/`warehouse`/`role` in `federation_hints` |
| `bigquery` | — | bigquery | bigquery | Federiert | Kein DirectDriver; erreicht über Federation-Engine oder BigQuery-Engine-ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federiert | Liest über databricks-sql-connector (Cloud Fetch, Arrow); landet Replikat; `http_path` erforderlich in `federation_hints` |
| `redshift` | — | redshift | redshift | Federiert | — |
| `athena` | AthenaDriver | — | athena | Partiell (INSERT immer; UPDATE/DELETE/MERGE nur für Iceberg) | Serverless-S3-Abfrage; Einzelquellen-Anweisungen laufen vollständig in Athena; quellenübergreifende Abfragen landen ein Replikat; `region` in `federation_hints` erforderlich |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federiert | Microsoft Fabric Warehouse; T-SQL über TDS, Azure-AD-Auth; landet Replikat |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federiert | Azure Synapse SQL; T-SQL über TDS, Azure-AD-Auth; landet Replikat |
| `trino` | SQLAlchemyDriver | — | — | Federiert | Entfernter Trino-/Presto-Koordinator, gelesen über den SQLAlchemy-Trino-Dialekt; landet Replikat auf jeder Engine |

#### `athena`

Amazon Athena ist ein serverloser Abfragedienst über Daten in S3, abgerechnet pro gescanntem Byte. Eine Anweisung nur auf Athenas Tabellen geht direkt an Athena, mit weitergereichten Filtern, Spaltenlisten und `LIMIT`, sodass Athena nur die Bytes scannt und berechnet, die diese Anweisung braucht. Eine Anweisung, die eine Athena-Tabelle mit einer anderen Quelle joint, landet zuerst ein vollständiges Replikat aus Athena. Dabei wird die ganze Tabelle gelesen, und Athena berechnet das; danach läuft der Join gegen das Replikat. Die Einstellungen Replicate und Lastschutz der Quelle bestimmen, wie oft gelandet wird, und dieser Hinweis ist die einzige Warnung vor diesen Kosten.

**Erkennung.** Register Table ruft Athenas Metadaten-API auf: `ListDatabases`, `ListTableMetadata` und `GetTableMetadata`. Diese Aufrufe führen keine Abfrage aus, daher kostet das Auflisten von Datenbanken und Tabellen und das Lesen der Spalten einer Tabelle nichts pro Byte. Die Spalten einer Tabelle werden erst geholt, wenn Sie diese Tabelle auswählen, mit den Partitionsschlüsseln nach den regulären Spalten. `array<…>`, `map<…>` und `struct<…>` werden zu `text`, und ein unbekannter Typ wird mit Namen abgelehnt.

**Quellfelder.**

| Feld | Wo | Erforderlich | Standard | Hinweise |
| --- | --- | --- | --- | --- |
| `database` | Quelle | Ja | — | Die standardmäßig abgefragte Glue-Datenbank |
| `username` | Quelle | Nein | — | AWS-Zugriffsschlüssel-ID; kann `${secret:…}` oder `${env:…}` sein |
| `password` | Quelle | Nein | — | Geheimer AWS-Zugriffsschlüssel; kann `${secret:…}` oder `${env:…}` sein |
| `region` | `federation_hints` | Ja | — | AWS-Region, z.B. `us-east-1` |
| `workgroup` | `federation_hints` | Nein | `primary` | Athena-Arbeitsgruppe |
| `s3_staging_dir` | `federation_hints` | Nein | Workgroup-Standard | Wo Athena Abfrageergebnisse schreibt; erforderlich, wenn die Arbeitsgruppe keinen Ort festlegt |
| `catalog` | `federation_hints` | Nein | `AwsDataCatalog` | Athena-Datenkatalog |
| `role_arn` | `federation_hints` | Nein | — | IAM-Rollen-ARN, der für jeden Aufruf übernommen wird |

Wenn sowohl `username` als auch `password` leer sind, wird die Credential-Chain von boto3 verwendet: Instanzrolle, Umgebungsvariablen (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) oder ein benanntes Profil. Eines ohne das andere wird abgelehnt.

**Schreiben.** Jede registrierte Tabelle nimmt `INSERT` an. `UPDATE`, `DELETE` und `MERGE` funktionieren nur auf einer Iceberg-Tabelle, und das Format wird bei der Registrierung festgehalten (`registered_tables.table_format`). Auf einer Hive-Tabelle wird ein Schreibvorgang, der `UPDATE`, `DELETE` oder `MERGE` verlangt, unter Nennung von Tabelle und Operation abgelehnt. Athena gibt keine geschriebenen Zeilen zurück, daher wird eine `RETURNING`-Klausel abgelehnt, und GraphQL bietet für diese Tabellen keine Mutationsfelder. Die Schreibwege sind `INSERT`, `UPDATE`, `DELETE` und `COPY`. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` und `TRUNCATE` werden unter Nennung der Form abgelehnt.

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

Mit einer pro Aufruf übernommenen Rolle (Credential-Chain für die Basis-Sitzung):

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```

### MotherDuck

`motherduck` ist eine bei MotherDuck gehostete DuckDB-Datenbank. Auf der DuckDB-Engine wird sie direkt angehängt; auf der Postgres-Engine bleibt sie der Connector über den Foreign Data Wrapper `pg_duckdb`.

**Verbindung.** `database` benennt die MotherDuck-Datenbank oder eine Freigabe über ihre Freigabeadresse. `password` ist das MotherDuck-Token und wird wie jedes Quellen-Secret aufgelöst.

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

Die DuckDB-Engine setzt `ATTACH 'md:<database>'` ab; bei einer Freigabe ist `database` die Freigabeadresse (`_share/...`), das Anhängen lautet also `md:_share/...`.

- **Token-Behandlung.** Die Engine meldet sich mit `SET GLOBAL motherduck_token = ?` an, wobei das Token als gebundener Wert übergeben wird. Es steht im Text keiner Anweisung, daher enthält es kein Log, kein Trace und kein Fehler.
- **Kein Token.** Eine Quelle ohne `password` wird unter Nennung des Grundes abgelehnt, bevor etwas angehängt wird (`a MotherDuck source signs in with its token (the password field); none is set`). Ohne diese Prüfung würde die Erweiterung eine interaktive Anmeldung starten und etwa eine Minute warten. Eine Quelle ohne `database` wird ebenfalls abgelehnt.
- **Ein Konto pro Engine.** Eine Engine meldet sich bei einem MotherDuck-Konto an. Eine zweite MotherDuck-Quelle mit einem anderen Token wird unter Nennung des Grundes abgelehnt: "one DuckDB engine reads one MotherDuck account".
- **Schreiboperationen** laufen über die Engine, wie auf der Postgres-Engine.

**Hybride (duale) Ausführung** braucht keine Einstellung. Auf der DuckDB-Engine ist jede Quelle an einer Verbindung angehängt, daher läuft eine Anweisung, die eine Tabelle einer DuckDB-Dateiquelle mit einer MotherDuck-Tabelle verknüpft, als eine DuckDB-Anweisung, und MotherDuck entscheidet, wo jeder Teil läuft.

> **Datenbewegung.** Bei dualer Ausführung kann MotherDuck lokale Zeilen für die Teile, die es ausführt, auf seine Seite verschieben. Eine solche Anweisung kann daher Daten aus der lokalen Datei in das MotherDuck-Konto der Organisation senden. Verknüpfen Sie eine Datei mit Daten, die auf dem Knoten bleiben müssen, nicht mit einer MotherDuck-Quelle, es sei denn, das Senden an dieses Konto ist akzeptabel.

Eine MotherDuck-Quelle wird nur von der DuckDB- und der Postgres-Engine bedient. Keine andere Engine hat einen Connector dafür, und nichts landet ihre Tabellen dort als Replikate.

### Analytics / OLAP

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federiert | Liest über clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` für TLS |
| `druid` | — | druid | druid | Nein | — |
| `exasol` | — | exasol | exasol | Nein | — |
| `elasticsearch` | HTTP (native Engines) | elasticsearch (Trino) | — | Nein | Auf Trino liest der Connector sie, seine Eigenschaften aus der Mapping-DSL des Typs; auf jeder anderen Engine liest Provisa den Index über HTTP (Indizes und Mapping für Register Table, ein Scroll-Read zum Landen) und landet die Zeilen |
| `pinot` | — | pinot | — | Nein | Trino-`pinot`-Connector; `pinot.controller-urls` = Host:Port des Pinot-Controllers |

### Data-Lake / offene Tabellenformate {: #data-lake-open-table-formats }

Diese Quelltypen gibt es nur über die Federation. Sie haben weder einen direkten Treiber noch einen Dialekt.

| Quelltyp | Connector-Name | Time Travel | Hinweise |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Ja (`as_of`-Argument) | — |
| `delta_lake` | delta_lake | Ja (`as_of`-Argument) | — |
| `hive` | hive | Nein | — |
| `hive_s3` | hive | Nein | S3-gestütztes Hive |

### NoSQL

`mongodb`, `cassandra` und `redis` haben Trino-Connectors (`redis` baut seine Eigenschaften aus der Mapping-DSL des Typs).

| Quelltyp | Connector-Name | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Nein |
| `cassandra` | cassandra (Trino); CQL-Read über cassandra-driver auf jeder anderen Engine | Nein | Keyspaces sind Schemas; Register Table listet die Tabellen eines Keyspace und typisiert Spalten aus den Schema-Metadaten des Clusters (Partition Keys als Primärschlüssel); das `cassandra`-Extra installiert den Treiber |
| `redis` | redis (Trino); HTTP-freier redis-py-Read auf jeder anderen Engine | Nein | Ein Key-Präfix `<table>:*` ist eine Tabelle, und ein Hash ist eine Zeile; Register Table listet die vorhandenen Präfixe und typisiert die Spalten eines Präfixes aus dessen Hashes (ein `mapping.tables`-Eintrag überschreibt Muster, Key-Spalte, Werttyp und Spalten) |

**Redis auf den gehosteten Engines.** Redis ist nicht verfügbar, wenn die Engine der Organisation eine der bereitgestellten Engines der gehosteten Plattform ist. Trino liest die Tabellen eines Redis-Katalogs aus Beschreibungsdateien in einem eigenen Verzeichnis des Katalogs, und einer gehosteten Engine kann im laufenden Betrieb keines gegeben werden. Das Speichern oder Anwenden einer Redis-Quelle wird dort mit Namen abgelehnt (HTTP 422, `sources.redis_needs_engine_files`); ein selbst gehostetes Deployment liest Redis wie oben beschrieben.

### Qdrant

`qdrant` wird vom eigenen Client von Qdrant gelesen und geschrieben, `qdrant-client`. Qdrant ist **nur live**: eine Eigenschaft des Quelltyps, keine Einstellung. Antworten gibt es mit seinem Vektorindex und seiner Query API, und kein Replikat kann dasselbe beantworten; seine Tabellen werden daher nie repliziert. Für eine Qdrant-Quelle oder ihre Tabellen gibt es keine Replicate-Auswahl; ein `replicate`-Wert oder `row_materialize` darauf wird abgelehnt.

**Verbindung.** `host` ist die URL der Instanz mit Port (`https://<cluster>.cloud.qdrant.io:6333` oder ein selbst gehostetes `http://qdrant:6333`); `password` ist ihr API-Schlüssel, eine Secret-Referenz, die beim Lesen der Quelle aufgelöst wird, leer bei einer Instanz ohne Schlüssel. `federation_hints.prefer_grpc: true` liest über den gRPC-Port von Qdrant statt über REST. Die Quelle hat ein Schema, `default`.

**Collection-Tabellen.** Eine Collection ist eine Tabelle. Register Table bietet die Spalten der Collection an: `id` (die Point-ID, ihr Schlüssel, Pfad `@id`), die Felder des Payloads (ein indiziertes Feld nach seinem Index typisiert, jedes andere so, wie die Stichproben-Points es nahelegen; verschachtelte Felder mit ihrem Pfad abgeflacht) und eine `json`-Spalte je Vektor (Pfad `@vector`, bei einem benannten Vektor `@vector.<name>`), nicht ausgewählt angeboten. Eine Vektorspalte nennt das registrierte Embedding-Modell, dessen Embeddings sie enthält, gewählt aus den Modellen, deren Dimensionen der Größe des Vektors entsprechen (in Register Table und im Tab „Typ und Schlüssel“ des Editors); mit der Wahl eines Modells wird der Vektor registriert.

**Anweisungen auf einer Qdrant-Quelle.** Eine Anweisung, die nur eine Collection liest, läuft in Qdrant, wenn die API von Qdrant sie exakt ausdrücken kann:

- Payload-Bedingungen werden zu einem Qdrant-Filter (`=`, `<>`, `IN`, Bereiche auf Zahlen und Datumswerten, `IS NULL`, `AND`, `OR`, `NOT`); die NULL-Semantik von SQL bleibt erhalten, sodass `lang <> 'en'` Points ohne `lang` überspringt;
- eine Bedingung allein auf `id` (`=` oder `IN`) ist ein `retrieve`; jedes andere Lesen ist ein gefilterter `scroll`;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` ist ein `query_points` über einen Vektor mit der Distanz Cosine, der Filter wird vor dem Top-k angewendet. Ein Text als Abfrage wird vom Modell des Vektors eingebettet. Die Antwort trägt eine Warnung, dass das Top-k approximativ ist und über den Index von Qdrant gewählt wird.

Alles andere (ein Aggregat, ein `GROUP BY`, eine berechnete oder maskierte Spalte, eine andere Sortierung als „ähnlichste zuerst“) beantwortet die Engine, wie unten beschrieben.

**Anweisungen, die die Engine beantwortet.** Verbindet eine Anweisung eine Collection mit einer anderen Quelle oder rechnet sie über deren Spalten, wird der Teil der Collection für diese Anweisung live aus Qdrant gelesen und der Engine als Zeilen übergeben. Der Teil besteht aus den Spalten der Collection, den Bedingungen der Anweisung allein auf der Collection (darunter ihre Zeilenregeln) und, wenn die Collection in ihrem `SELECT` allein gelesen wird, der Ähnlichkeitssortierung und dem `LIMIT` dieses `SELECT`. Ein Teil, den Qdrant nicht exakt ausdrücken kann, wird abgelehnt, unter Nennung von Tabelle und Grund. Ein Teil ohne Bedingung, ohne Schlüssel und ohne Top-k würde die ganze Collection lesen und wird ebenfalls abgelehnt. Eine Bedingung auf einen Parameter (`$1`) bleibt der Engine überlassen.

**Schreibzugriffe.** `INSERT`, `UPDATE` und `DELETE` auf eine Collection laufen in Qdrant:

- `INSERT … VALUES` nennt die Spalte `id` und schreibt neue Points. Eine `id`, die die Collection bereits enthält, wird unter Nennung abgelehnt. Die Prüfung liest zuerst die IDs und ist daher nicht atomar mit dem Schreiben.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` ist ein Upsert. Spalten, die die Anweisung nicht setzt, behalten ihre Werte.
- `UPDATE` und `DELETE` wirken auf die Points, die ihr `WHERE` auswählt, als Qdrant-Filter; eine Bedingung ohne Filterform wird abgelehnt.
- Ein in eine Vektorspalte geschriebener Text wird vom Modell der Spalte eingebettet. Ein dorthin geschriebener Vektor muss die Dimensionen des Modells haben.

**Suchtabellen.** Die Vektorsuche ist eine Suchtabelle: Ihr `query_template` enthält eine Query-API-Anfrage.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` ist immer ein Argument; `using`, `filter`, `limit`, `score_threshold` und `model` sind optional. Jeder Wert in `using`, `filter`, `limit` oder `score_threshold`, der genau `"@name"` lautet, ist ebenfalls ein Argument.
- Jedes Argument ist ein Parameter der Tabelle (`query_arg`), streng in seinen deklarierten Typ umgewandelt und als JSON-Wert in die Anfrage eingesetzt, nie in deren Text. Das Argument von `query` ist `array(double)` und erforderlich.
- Ein Aufrufer übergibt Argumente wie jeden Parameter: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` in SQL, Feldargumente in GraphQL.
- Ein nicht übergebenes optionales Argument filtert nichts: Seine Bedingung entfällt im Filter. Ohne `limit` liefert eine Suche den Standard von Qdrant, 10 Points.
- Der Filter wird von Qdrant angewendet, bevor die nächsten Points gewählt werden, ist also exakt. Jede andere Bedingung der Anweisung schränkt die Points, die die Suche geliefert hat, erst danach ein; sie holt keine weiteren.
- `score` ist die eigene Metrik der Collection für den durchsuchten Vektor: bei einer Cosine-Collection die Kosinus-Ähnlichkeit. Register Table listet die Vektoren der Collection mit Größe und Distanz.
- `model` nennt ein registriertes Embedding-Modell (`vector_models`, Admin → AI Models). Ein für die Abfrage übergebener Text wird dann von diesem Modell eingebettet, und ein übergebener Vektor muss die Dimensionen dieses Modells haben. Ohne `model` muss die Abfrage ein Vektor sein.

Eine Suche wird pro Anweisung unter ihren Argumenten gelesen, ihre Zeilen für die TTL der Tabelle unter diesen Argumenten gehalten. Eine Suchtabelle ist schreibgeschützt.

### Streaming

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Föderierter Kafka-Connector; Schema über Confluent Schema Registry (Avro, Protobuf, JSON Schema), manuelle Definition oder Stichproben-Inferenz | Nein; die Live-Ausgabe einer Tabelle veröffentlicht nach Kafka |
| `websocket` | Externer WebSocket-Feed — verbinden, abonnieren, Ereignisse empfangen; Ergebnisse materialisiert | Nein |
| `rss` | RSS-2.0-/Atom-Feed — pollen, Watermark nach pubDate/updated; Ergebnisse materialisiert | Nein |

**Kafka auf den gehosteten Engines.** Auf den bereitgestellten Engines der gehosteten Plattform muss eine Kafka-Quelle eine Schema-Registry nennen (`schema_registry_url`). Eine Quelle, deren Topic-Spalten nur das Modell deklariert, wird beim Speichern oder Anwenden mit Namen abgelehnt (HTTP 422, `sources.kafka_needs_schema_registry`), da ihre Tabellenbeschreibungen ein Verzeichnis bräuchten, das der laufenden Engine nicht gegeben werden kann. SASL-Zugangsdaten erreichen diese Engines im Speicher (siehe [Sicherheit](security.md#the-federation-engine-trino)).

### Push-Empfänger {: #push-receiver }

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | Externe Dienste POSTen JSON-Ereignisse; Ergebnisse materialisiert | Nein |

### Graph & Semantic

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher über HTTP-API, Ergebnisse in PostgreSQL gecacht | Nein |
| `sparql` | SPARQL-1.1-POST, Ergebnisse in PostgreSQL gecacht | Nein |

### Dateibasiert {: #file-based }

Drei Mechanismen decken Dateien ab. Einzeldatei-Quellen und der Datei-Crawler verwenden das Feld `path` statt `host`/`port`. Der dritte, [Logs](#logs), benennt seine Dateien durch einen Glob in seinem `mapping` und liest nur Dateien auf dem Provisa-Server.

**Dateidatenbank- und Einzeldatei-Quellen.** Eine `sqlite`-, `duckdb`-, `csv`- oder `parquet`-Quelle richtet `path` auf eine einzige Datei. (Eine `duckdb`-Quelle hält ihren Speicherort im Feld `database` des Formulars.)

| Quelltyp | Speicherorte | Mutations |
| --- | --- | --- |
| `sqlite` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` | Ja bei einer lokalen Datei; keine bei einer entfernten |
| `duckdb` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` | Ja bei einer lokalen Datei; keine bei einer entfernten |
| `csv` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` (DuckDB-Engine) | Nein |
| `parquet` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` (DuckDB-Engine) | Nein |

Ein Pfad mit einem anderen Schema (etwa `ftp://`) wird abgelehnt: "DuckDB reads no file at a `ftp://` location".

**Zugangsdaten für einen entfernten Speicherort.** Legen Sie sie in den `federation_hints` der Quelle ab. Jede gilt nur für den Bucket oder Container des eigenen Pfads, daher werden die Zugangsdaten einer Quelle nie für den Speicherort einer anderen Quelle verwendet. Eine Quelle ohne Zugangsdaten wird als öffentlicher Speicherort gelesen.

| Speicherort | `federation_hints`-Schlüssel |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; optional `region`, `endpoint` (ein S3-kompatibler Speicher wie R2 oder MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (ein HMAC-Schlüssel) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Entfernte SQLite- und DuckDB-Dateien sind schreibgeschützt.** Die DuckDB-Engine hängt sie direkt mit `READ_ONLY` an, und eine Tabelle darauf bietet kein Insert, Update oder Delete. Auf einer Engine, die SQLite nicht anhängt, wird eine entfernte SQLite-Datei über eine temporäre DuckDB-Verbindung gelesen, die sie schreibgeschützt anhängt, und ihre Zeilen landen wie üblich.

**Eine Datei hochladen.** Im Quellformular einer `sqlite`-, `duckdb`-, `csv`- oder `parquet`-Quelle laden Sie die Datei hoch, statt einen Pfad einzugeben. Die Datei wird auf die Platte gestreamt, nie im Speicher gehalten, in den Ordner der Organisation unter `files/<source id>/<file name>`, und der Pfad der Quelle wird darauf gesetzt.

- **Ersetzen.** Ein erneuter Upload tritt an die Stelle der früheren Datei, gleich welchen Namen sie hat, und die Quelle wird neu bereitgestellt, sodass der nächste Lesevorgang die neue Datei sieht.
- **Entfernen.** Das Entfernen der Quelle entfernt ihre hochgeladene Datei. Nichts anderes entfernt sie.
- **Wer.** Nur Rollen mit der Fähigkeit `source_registration` dürfen hochladen.
- **Größenlimit.** Die Einstellung `sources.max_upload_bytes` (Umgebung `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) begrenzt die Datei, standardmäßig 1 GiB (1073741824 Bytes), Minimum 1, gelesen beim Beginn eines Uploads. Eine größere Datei wird mit HTTP 413 unter Nennung des Limits abgelehnt, und nichts davon wird behalten.
- **Pfad.** Solange der Pfad der Upload der Quelle ist, ist das Pfadfeld des Formulars schreibgeschützt.
- **Namen.** Quell-ID und Dateiname dürfen nur Buchstaben, Ziffern, `.`, `_` und `-` enthalten.
- **Speicherung.** Ein Upload wird wie jede Org-Datei gespeichert: im Ordner der Organisation, verschlüsselt, wenn der Ordner der Organisation verschlüsselt ist, und für jeden Knoten auf einem gemeinsam genutzten Mount sichtbar.

**Eine Datei herunterladen.** Eine `csv`- oder `parquet`-Quelle kann ihre Datei stattdessen per Download erhalten. Wählen Sie im Quellformular **Download a copy** und nennen Sie eine Datei: einen Pfad auf dem Server, eine `https://`-Adresse oder eine `s3://`-Adresse, gelesen mit den Zugangsdaten, die das Formular für den Speicher hält. Provisa streamt die Datei über den Upload-Pfad nach `files/<source id>/<file name>` im Ordner der Organisation, mit derselben Größengrenze, Speicherung und Ersetzung wie ein Upload, und die Quelle liest diese Kopie.

- **Wann sie erneut geholt wird.** Das entscheidet der Cache-TTL der Quelle. Eine aus der Quelle gebaute Tabelle holt die Datei zuerst erneut, sobald ihr TTL seit dem Anlegen der Kopie abgelaufen ist. Bei einem TTL von -1 wird die Datei einmal geholt und behalten.
- **Pfade auf dem Server.** Ein Serverpfad muss eine Datei unter einem Verzeichnis sein, das in der Einstellung `sources.download_roots` (Umgebung `PROVISA_SOURCES_DOWNLOAD_ROOTS`) aufgeführt ist, nachdem seine Symlinks aufgelöst wurden. Standardmäßig ist keines aufgeführt. Ein Pfad außerhalb davon wird mit Namen abgelehnt, und nichts wird kopiert.
- **Viele Organisationen.** Auf einem Server mit vielen Organisationen (`multitenancy: true`) wird ein Serverpfad mit Namen abgelehnt, was immer die Einstellung enthält, und nur `https://` und `s3://` werden angeboten. Eine Adresse, deren Host sich im eigenen Netzwerk des Servers auflöst (privat, Loopback oder Link-Local), wird ebenfalls mit Namen abgelehnt, bei jeder Weiterleitung.
- **Ihre Tabelle, als Entwurf.** Beim Anlegen der Quelle bietet das Formular an, ihre Tabelle sofort als Entwurf einzurichten, in der Domäne Ihrer Wahl. Die Tabelle wird nach der Quelle benannt und aus der Datei typisiert, wie Register Table es vorschlagen würde, und sie bleibt außer Betrieb, bis ein Steward sie aus dem Entwurf nimmt. Die Option ist standardmäßig aktiv.
- **Über Polly.** Geben Sie Polly einen vollständig aufgelösten Pfad zu einer einzelnen CSV- oder Parquet-Datei, und sie schlägt die Quelle mit Download und Entwurfstabelle vor und legt sie an, sobald Sie bestätigen. Ein Verzeichnis, ein Glob oder mehrere Dateien werden stattdessen zu einer Dateiquelle.

**Schreiboperationen auf eine lokale SQLite- oder DuckDB-Datei, und Sperren.** Eine Schreiboperation nimmt die Sperre der Datei und wartet nie darauf. Hält ein anderer Prozess oder Knoten die Datei, wird die Schreiboperation abgelehnt, unter Nennung der Quelle und der Tatsache, dass ihre Datei gesperrt ist.

- **SQLite.** Die Schreiboperation setzt `PRAGMA busy_timeout = 0`, sodass eine gesperrte Datei sofort fehlschlägt.
- **DuckDB.** DuckDB teilt eine Datei nur zwischen schreibgeschützten Öffnungen. Daher halten das Anhängen der Engine und die eigene Verbindung der Quelle die Datei schreibgeschützt, und jeder Knoten liest sie gleichzeitig. Eine Schreiboperation pausiert die Halter dieses Prozesses, öffnet die Datei nur für diese Anweisung lesend und schreibend und schließt sie wieder. Kann das Öffnen die Sperre nicht nehmen, wird die Schreiboperation abgelehnt.

**Gemeinsam genutzte Mounts.** In einem verschlüsselten Org-Ordner, dessen Mount von mehreren Knoten gemeinsam genutzt wird (Einstellung `org_files.shared_mount`), wird eine Schreiboperation auf eine SQLite- oder DuckDB-Datei darin grundsätzlich abgelehnt, unter Nennung der Quelle. Die Ordnerverschlüsselungsschicht (gocryptfs) überträgt Dateisperren nicht an die anderen Knoten, sodass nichts zwei Knoten daran hindern würde, gleichzeitig zu schreiben. Lesen funktioniert weiterhin.

Ein ohne Verschlüsselung gemeinsam genutzter Ordner muss mit Sperren gemountet werden, die jeden Knoten erreichen: ein NFS-Mount mit aktiviertem Locking, nicht `nolock` und nicht `local_lock`. Keine Prüfung kontrolliert das; es ist eine Anforderung an das Deployment.

**Datei-Crawler (`files`-Quelle).** Er richtet `path` auf ein Glob-Muster, durchläuft es rekursiv und registriert das Verzeichnis als föderierten Katalog von Tabellen. Er liest viele Formate über viele Transporte:

| Formate | Transporte |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow und in Tabellen konvertierte Dokumente — HTML, Markdown, DOCX, PPTX | Lokales Dateisystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST und Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Jede `files`-Quelle kann in ihrem Mapping auch ein `crawl` deklarieren, mit denselben Einstellungen, die unter [Wikipedia](#wikipedia) aufgeführt sind: Die Quelle liest dann zuerst Webseiten und landet jede HTML-Datentabelle und jede verlinkte Datendatei in ihrem Verzeichnis als Tabelle.

Auf der DuckDB-Engine wird `files` nativ gelesen, mit einer `read_csv_auto`-Scanner-View pro `<table>.csv` unter dem aufgelösten Verzeichnis. Auf einer Engine ohne eigenen `files`-Connector landen die Zeilen über denselben mit dem Connector gebündelten Calcite-pgwire-Server (`pgwire-file`), den auch sharepoint und splunk nutzen. Siehe [Enterprise-SaaS-Connectors](#enterprise-saas-connectors) unten.

#### Logs {: #logs }

Eine `logs`-Quelle macht Logdateien auf dem Provisa-Server zu einer Tabelle. Sie benennen die Dateien mit einem vollständigen Pfad oder einem Glob und wählen das Format, in dem die Einträge geschrieben sind. Jeder Eintrag wird zu einer Zeile. Ein Eintrag, den das Format nicht lesen kann, bleibt mit seinem Rohtext und dem Grund erhalten und wird nie verworfen.

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

Das Mapping kennt vier Schlüssel und weist jeden anderen mit Namen zurück: `glob` und `format` (beide erforderlich) sowie `pattern` und `record_start`, die nur das Format `regex` annimmt.

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

Im Sources-Formular erscheint eine Logs-Quelle als „Log files“ unter File. Die Schaltfläche **Preview** zeigt, noch vor dem Speichern, die Dateien, auf die der Glob passt, die Spalten, die das Format liefert, und die ersten 20 Zeilen der neuesten Datei.

**Der Glob.** Er muss ein vollständiger Pfad sein; `~` wird aufgelöst, ein relativer Pfad wird zurückgewiesen („name the files by their full path“). Er wird mit Pythons `glob` im nicht rekursiven Modus ausgewertet, daher verhält sich `**` wie `*` und passt innerhalb einer Verzeichnisebene. Nur reguläre Dateien werden übernommen, in Namensreihenfolge. Eine leere Datei wird übersprungen, bis sie Bytes enthält. Der Glob darf eine Umgebungsvariable nennen, etwa `${env:LOG_DIR}/*.log`. Die Quelle speichert den Verweis so, wie er geschrieben wurde, und löst ihn bei jedem Lesen der Dateien auf; eine nicht gesetzte Variable wird mit ihrem Namen abgelehnt.

**Die eigenen Spalten jeder Zeile.** Sie stehen vor den Spalten des Formats:

| Spalte | Typ | Enthält |
| --- | --- | --- |
| `_file_id` | text | Die Identität der Datei: eine Prüfsumme ihrer ersten 4096 Bytes, dekomprimiert |
| `_file_name` | text | Der Pfad, unter dem der Eintrag gelesen wurde |
| `_line_offset` | bigint | Byte-Offset der ersten Zeile des Eintrags im dekomprimierten Text |
| `_entry` | bigint | Der wievielte Eintrag des Datensatzes die Zeile ist: 0, außer bei einem Sammeldatensatz wie dem `Records` einer CloudTrail-Datei |
| `_raw` | text | Der Eintrag, wie er geschrieben wurde |
| `_parse_error` | text | Warum der Eintrag oder einer seiner Werte nicht gelesen werden konnte; NULL, wenn er gelesen wurde |
| `_landed_at` | timestamp | Wann die Zeile gelandet ist |

`_file_id`, `_line_offset` und `_entry` bilden den Schlüssel der Zeile, sodass ein zweimal gelesener Eintrag eine Zeile bleibt.

**Formate und Presets.** Die Formatauswahl gruppiert die Formate nach Familien; die letzte Wahl, `regex`, ist Ihr eigenes Muster.

| Familie (Formularbezeichnung) | Formatnamen |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

Das sind 56 eingebaute Formate und `regex`.

**Wie ein Format zu Spalten kommt.** Formate bekommen ihre Spalten auf vier Arten.

- **Feste Spalten.** Die meisten Formate nennen ihre Spalten und Typen. `rfc3164` liefert zum Beispiel `timestamp`, `facility`, `severity`, `host`, `process`, `pid` und `message`; `facility` und `severity` werden aus dem `<PRI>`-Wert berechnet. Eine BSD-Syslog-Zeile enthält kein Jahr, daher gilt das Jahr der letzten Schreibzeit der Datei, oder das Jahr davor, wenn der Monat der Zeile nach diesem Schreibzeitpunkt liegt.
- **Vom Eintrag benannte Felder.** `jsonl` und `logfmt` übernehmen ihre Spalten aus den Feldern der ersten Zeilen der Dateien (Einstellung `logs.preview_lines`, Standard 1000). Jede Spalte erhält den engsten Typ, in den alle gesehenen Werte passen: boolean, dann integer, float, timestamp und json, sonst text. Ein Feld, das ein späterer Eintrag trägt und für das es keine Spalte gibt, landet in einer json-Spalte `extra`.
- **Von einem Header benannte Felder.** `w3c` und die darauf aufbauenden Presets lesen die `#Fields:`-Zeile jeder Datei; `date` und `time` werden zu einer Spalte `timestamp` verbunden. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` und `zeek_tsv` lesen `#`-Headerzeilen auf dieselbe Weise, und `aws_vpc_flow` liest eine Headerzeile, die mit `version` beginnt. Wo eine Headerzeile gelesen wurde, bleibt bei der Datei, sodass eine später weitergelesene Datei ihre Felder behält.
- **`regex`.** Jede benannte Gruppe, etwa `(?P<level>…)`, ist eine Spalte vom Typ text.

Der Bildschirm Register Table listet diese Spalten mit ihren Typen auf und markiert den Schlüssel. Beim Lesen der Zeilen wird jeder Wert in den Typ umgewandelt, den seine Spalte in der registrierten Tabelle hat; ein dort geänderter Typ ist also der angewandte. Ein Wert, den seine Spalte nicht aufnehmen kann, wird zu NULL, und sein Grund steht in `_parse_error`.

Ein Eintrag, der länger als `logs.max_line_bytes` (Standard 65536) ist, behält seine ersten Bytes in `_raw` und wird nicht geparst; `_parse_error` nennt seine Länge. Eine Datei, die mit einer UTF-8- oder UTF-16-Byte-Order-Mark beginnt, wird in dieser Kodierung gelesen, jede andere als UTF-8.

**Einträge über mehrere Zeilen.** Ein Format mit Eintragsbeginn-Muster bildet einen Eintrag aus einer Zeile, die darauf passt, plus allen folgenden Zeilen, die nicht passen. Ein Stacktrace bleibt bei seiner Exception, ein Slow-Query-Eintrag bleibt ganz. Zeilen vor dem ersten Treffer bilden einen eigenen Eintrag; das Format weist ihn meist zurück, und die Zeile bleibt erhalten. Die eingebauten Formate mit Eintragsbeginn sind `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` und `azure_monitor`. Bei `regex` setzen Sie `record_start` selbst.

Zwei Formate lesen ihre Einträge selbst statt zeilenweise. `kubernetes_cri` fügt die partiellen Zeilen (`P`), die die Runtime zerlegt hat, zu einem Eintrag zusammen, und `windows_evtx` liest die binäre `.evtx`-Datei.

Eine Datei, in die noch geschrieben wird, wird bis zu ihrer letzten vollständigen Zeile gelesen. Bei einem mehrzeiligen Format wird sie bis zum Beginn ihres letzten Eintrags gelesen, den die nächste Zeile fortsetzen könnte. Sobald die Datei 2 Sekunden ohne Schreibvorgang geblieben ist, gilt dieser letzte Eintrag als vollständig.

**Rotation.** Eine Datei wird an einer Prüfsumme ihrer ersten 4096 Bytes, dekomprimiert, erkannt, nicht an ihrem Namen. Wird `system.log` zu `system.log.0` rotiert und dann zu `system.log.0.gz` komprimiert, ist jede Kopie dieselbe Datei. Das Lesen setzt bei dem Byte fort, das der letzte Lesevorgang erreicht hat, sodass keine Zeile doppelt gelesen wird.

- Kompression wird an den ersten Bytes der Datei erkannt, nie an ihrem Namen: gzip und bzip2. Eine komprimierte Datei gilt als vollständig.
- Eine rotierte Kopie und ihr komprimierter Zwilling, die beide passen, werden einmal gelesen, und zwar die komprimierte.
- Eine Datei, die kürzer ist als der bereits gelesene Offset, wurde abgeschnitten: Sie ist eine neue Datei und wird von vorn gelesen. Die Zeilen der alten bleiben.
- Dateien unter 4096 Bytes, die gleich beginnen, werden über die Inode unterschieden und bleiben daher zwei Dateien.
- Eine Datei, auf die der Glob nicht mehr passt, wird vergessen; ihre Zeilen bleiben als Historie.

**Aktualisierung und Live-Zustellung.** Eine Logs-Tabelle ist ein Replikat. Nichts überwacht die Dateien. Stattdessen bildet, wenn das Änderungssignal der Quelle `probe` oder `ttl_probe` ist, ihre Probe einen Digest über Pfad, Inode, Größe und letzte Schreibzeit jeder passenden Datei; weicht der Digest ab, wird eine Aktualisierung angefordert. Die Probe-Typen, die eine Logs-Quelle annimmt, sind `hash` und `none`.

Eine Aktualisierung liest nur, was neu ist. Jede Datei wird dort weitergelesen, wo der letzte Durchlauf stand, und die Zeilen werden nach ihrem Schlüssel geschrieben, 5000 auf einmal, bevor die neue Position gespeichert wird. Ein Durchlauf, der zwischen beiden Schritten stoppt, liest diese Einträge erneut, was dieselben Zeilen ergibt. Jeder Durchlauf stempelt seine Zeilen in `_landed_at`, der Spalte, die die Live-Zustellung als Wasserzeichen nutzt. Eine geänderte Tabellendefinition ist ein vollständiger Neuaufbau, der jede Datei von vorn liest.

**Erlaubte Wurzeln.** Die Operator-Einstellung `logs.allowed_roots` listet die Verzeichnisse auf, in die ein Glob auflösen darf. Ihr Standard ist `["/var/log"]`; sie ist eine Liste auf der Karte Security unter Admin → Einstellungen, auch über `PROVISA_LOGS_ALLOWED_ROOTS` setzbar, und eine Änderung verlangt eine Bestätigung. Jede Wurzel wird nach der `~`-Auflösung und nach dem Auflösen von Symlinks ausgewertet.

Zwei Prüfungen gelten. Der feste Teil des Globs bis zum ersten Platzhalter muss innerhalb einer Wurzel auflösen; das wird beim Speichern der Quelle und bei jeder Vorschau und jedem Lesen geprüft. Jede passende Datei muss außerdem nach dem Folgen ihrer Symlinks innerhalb einer Wurzel auflösen, sodass ein Symlink aus `/var/log` heraus zurückgewiesen wird. Die Zurückweisung nennt den Pfad, das Auflösungsziel und die erlaubten Wurzeln:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Beim Speichern lautet der Code `schema.logs_path_refused`; bei der Vorschau ist es ein 403 mit `logs.path_refused`.

**Zurückweisung im gehosteten Betrieb (SaaS).** Ein Deployment, das viele Organisationen hält, liest keine eigene Logdatei. Jede Pfadprüfung weist zuerst zurück, beim Speichern, bei der Vorschau und beim Lesen, mit „`<path>`: a server that holds many organizations reads no log file of its own“. Die Dateien gehören dem Server, nicht einer einzelnen Organisation, daher darf kein Mandant sie lesen. Ob das Deployment viele Organisationen hält, entnimmt es seinem Multitenancy-Zustand.

#### Kaggle-Datasets {: #kaggle-datasets }

Kaggle ist ein Ort zum Herunterladen von Datendateien. Ein Kaggle-Datensatz, den Sie hinzufügen, wird als `files`-Quelle registriert, und Sie fragen ihn genauso ab wie jede andere `files`-Quelle.

**Einen Datensatz hinzufügen.** Öffnen Sie Sources → Subscriptions → Kaggle. Das Formular arbeitet in zwei Schritten, weil Kaggles eigene Datensatzsuche ein Token braucht; die Auswahl erscheint also erst, wenn Ihres geprüft ist.

1. **Token.** Geben Sie ein Kaggle-API-Token ein und klicken Sie auf „Validate“. Die Prüfung ruft `POST https://www.kaggle.com/api/v1/datasets/create/new` mit leerem Body auf. Ein `401` bedeutet, dass das Token ungültig ist; jede andere Antwort bedeutet, dass es gültig ist. Kaggle prüft den leeren Body, bevor es etwas anlegt, daher wird nichts gespeichert.
2. **Auswahl.** Ein Suchfeld fragt beim Tippen `GET /api/v1/datasets/list` ab, und jedes Ergebnis zeigt Titel und Beschreibung des Datensatzes. Wählen Sie einen aus und klicken Sie auf „Add Dataset“.

„Add Dataset“ lädt das Paket von `GET /api/v1/datasets/download/{owner}/{ref}` herunter, entpackt seine CSV- und Parquet-Dateien nach `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` und legt **eine** `files`-Quelle an, deren `path` dieses Wurzelverzeichnis ist. Der Server speichert das Token nie.

Registrieren Sie dann die gewünschten Tabellen auf dem normalen Register-Table-Bildschirm. Jede Datei erscheint als eigene Tabelle, wie bei jeder anderen `files`-Quelle.

**Nur CSV und Parquet.** Ein Paket mit einer `.sqlite`- oder `.db`-Datei wird mit einer Fehlermeldung abgelehnt, die das sagt. Bereitgestellt werden nur CSV- und Parquet-Dateien.

**Aktualisieren.** Wenn Kaggle eine neue Version eines Datensatzes veröffentlicht, rufen Sie die GraphQL-Mutation `refreshKaggleSource` mit der Quell-ID und einem gültigen Token auf. Sie stellt die Dateien an Ort und Stelle neu bereit und leert den Cache des Datei-Endpunkts, sodass die nächste Abfrage jede Schemaänderung aufnimmt. `kaggle_owner` und `kaggle_ref`, beim Anlegen der Quelle in `federation_hints` abgelegt, sagen, welcher Datensatz geholt wird.

**Kein eigenes YAML.** Eine Kaggle-Quelle legen Sie nur über das Sources-Formular an. Nach YAML exportiert erscheint sie als `type: files` mit `kaggle_owner` und `kaggle_ref` in `federation_hints`. Erneut von Kaggle holen geht über die Aktualisierung in der UI oder die Mutation `refreshKaggleSource`. In einer abgeschotteten Umgebung lassen Sie das YAML-`path` auf ein Verzeichnis zeigen, das Sie selbst bereitgestellt haben.

#### Wikipedia {: #wikipedia }

Wikipedia crawlt die Seiten, die Sie benennen. Jede Datentabelle einer Seite und jede verlinkte CSV-, TSV-, Excel-, JSON- oder Parquet-Datei wird zu einer Tabelle der Quelle. Wie Kaggle registriert es sich als `files`-Quelle mit einem `crawl` in ihrem Mapping. Das Hinzufügen registriert nichts: Ein Steward registriert die gewünschten Tabellen über den normalen Register-Table-Bildschirm.

**Einrichtungsfelder.**

| Feld | Standard | Bedeutung |
| --- | --- | --- |
| Pages to start from | keiner (erforderlich) | Eine pro Zeile: ein Seitentitel oder die Adresse einer Seite in der gewählten Ausgabe |
| Language edition | `en` | Der Code der Ausgabe, zum Beispiel `en`, `de` oder `ja` |
| Link depth | `1` | Wie viele Links von einer Startseite entfernt gelesen wird; `0` liest nur die Startseiten |
| Page limit | `25` | Die meisten Seiten, die ein Crawl liest |
| Contact | leer | Wird mit jeder Anfrage gesendet, damit Wikipedia Sie erreichen kann, wie seine Richtlinie es verlangt |
| Follow "See also" links | aus | Aus lässt diesen Abschnitt aus dem Artikel weg |
| What this edition calls its "See also" section | leer | Wird nur gefragt, wenn das Feld darüber aus ist und Provisa den Abschnitt für die gewählte Ausgabe nicht benennen kann. Der Titel des Abschnitts, wie eine Seite ihn zeigt, zum Beispiel `Voir aussi` |
| Directory for the crawl's files | leer | Wohin Tabellen und Datendateien geschrieben werden. Leer: `<PROVISA_DATA_DIR>/crawl/<source id>` |

Provisa kennt die Überschrift „See also" dieser Ausgaben: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Bei jeder anderen Ausgabe fragt das Formular, wenn „See also" aus ist, wie die Ausgabe diesen Abschnitt nennt; geben Sie seinen Titel ein, wie eine Seite ihn zeigt (zum Beispiel `Voir aussi`). Eine Quelle, die ohne ihn gespeichert wird, wird abgelehnt (`wikipedia.see_also_heading_needed`).

**Welche Links verfolgt werden.** Ein Link wird danach gewählt, was das Markup der Seite selbst über ihn sagt, nie nach einem Namen in seiner Adresse: Solche Namen unterscheiden sich in jeder Sprachausgabe und kommen auch in gewöhnlichen Titeln vor.

- Verfolgt: Links auf andere Artikel, die im Fließtext, in Listen und in Datentabellen des Artikels stehen. Die Markup-Markierung ist `a[rel='mw:WikiLink']`.
- Nicht verfolgt: noch nicht geschriebene Seiten, die Seite selbst, Buchquellen-Nachschläge, Dateiseiten, andere Wikis und externe Sites.
- Gar nicht gelesen (entfernt, bevor Links und Tabellen gesammelt werden): Navigationskästen, Infoboxen, Anmerkungen und Belege, Kategorien, das Inhaltsverzeichnis, Bilder und Wartungshinweise.
- Nicht gelesen, solange Sie die Option nicht einschalten: der Abschnitt „See also".

Nur Tabellen, die Redakteure als Datentabellen markieren (`table.wikitable`), werden zu Tabellen, und eine Tabelle mit weniger als 2 Zeilen wird nicht angeboten.

**Erweiterte Crawl-Einstellungen.** Das Formular hält diese unter „Crawl settings" eingeklappt und zeigt jede mit ihrem Standard; lassen Sie ein Feld leer, um den Standard zu behalten. In einem `files`-Mapping stehen dieselben Namen unter `crawl:`, und ein Name außerhalb dieser Liste wird abgelehnt.

| Einstellung | Wikipedia-Standard | Bedeutung |
| --- | --- | --- |
| `start_urls` | die Seiten, die Sie benannt haben | Seiten, bei denen der Crawl beginnt |
| `max_depth` | `1` | Linktiefe |
| `max_pages` | `25` | Seitenlimit |
| `request_delay` | `1 seconds` | Pause zwischen Anfragen |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, mit angehängtem Kontakt | Der Name, den sich der Crawl gibt |
| `content_selector` | `#mw-content-text .mw-parser-output` | CSS-Selektor: Links und Tabellen werden nur darin gelesen |
| `remove_selectors` | die Liste der oben beschriebenen Teile | CSS-Selektoren für ausgelassene Teile. Wer sie angibt, ersetzt die ganze Liste, einschließlich der „See also"-Regel |
| `link_selector` | `a[rel='mw:WikiLink']` mit Ausschlüssen | CSS-Selektor, dem ein Link entsprechen muss, um verfolgt zu werden |
| `link_exclude_patterns` | leer | Reguläre Ausdrücke; ein Link, dessen Adresse einen findet, wird nicht verfolgt |
| `follow_external_links` | `false` | Links auf andere Sites verfolgen |
| `allowed_domains` | nicht gesetzt | Domains, die der Crawl besuchen darf |
| `table_selector` | `table.wikitable` | CSS-Selektor, dem eine Tabelle entsprechen muss |
| `generate_tables_from_html` | nicht gesetzt | Ob HTML-Tabellen zu Tabellen werden |
| `html_table_min_rows` | `2` | Mindestzahl Zeilen einer Tabelle |
| `html_table_max_rows` | nicht gesetzt | Höchstzahl Zeilen einer Tabelle |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Verlinkte Dateitypen, die zu Tabellen werden |
| `html_cache_ttl` | `1 days` | Wie lange ein Crawl aufbewahrt wird |
| `max_html_size` | nicht gesetzt | Größte gelesene Seite, zum Beispiel `10MB` |
| `max_data_file_size` | nicht gesetzt | Größte gelesene Datendatei, zum Beispiel `100MB` |

**Tabellennamen.** Eine gecrawlte Tabelle heißt `<page>__<name>`. Der Name ist die Beschriftung (caption) der Tabelle oder, wenn sie keine hat, die letzte Überschrift darüber; eine Tabelle ohne beides heißt `table_1`, `table_2` und so weiter. Beide Teile werden kleingeschrieben, und jede Folge anderer Zeichen wird zu `_`. Eine Tabelle unter der Überschrift „Tallest buildings“ auf der Seite `List_of_tallest_buildings` wird zum Beispiel zu `list_of_tallest_buildings__tallest_buildings`. Eine verlinkte Datendatei behält ihren eigenen Namen.

**Aktualisieren.** Ein Crawl wird für die Cache-Dauer aufbewahrt, standardmäßig `1 days` (`html_cache_ttl`). Seiten werden erst nach Ablauf dieser Zeit erneut abgerufen.

### Observability & Sonstiges {: #observability-other }

`prometheus` hat einen Trino-Connector (Eigenschaften aus der Mapping-DSL des Typs gebaut). `google_sheets` ist ein registrierter Quelltyp ohne Trino-Connector und materialisiert über die API-Cache-Pipeline.

| Quelltyp | Connector-Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialisiert) | Nein |
| `prometheus` | prometheus | Nein | Eine Metrik ist eine Tabelle, und ein Sample ist eine Zeile (`timestamp`, `value`, eine Spalte pro Label); auf jeder Engine ohne Live-Connector liest Provisa die HTTP-API — Metriknamen und Labels für Register Table, `query_range` über den Bereich der Tabelle zum Landen

### Enterprise-SaaS-Connectors

Google Workspace und Microsoft 365 werden von eigenen Loadern gelesen; siehe [Google Workspace und Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, Cloud-Inventar und Splunk werden über Treiber gelesen, die als eigener Prozess laufen und das PostgreSQL-Wire-Protokoll sprechen (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`); keiner von ihnen hat also einen direkten Treiber. Auf der DuckDB-Engine wird der Endpunkt des Treibers über die postgres-Erweiterung live angebunden: Register Table listet die Tabellen des Connectors aus dem angebundenen Katalog, Abfragen lesen den Connector an Ort und Stelle, und Filter und Projektionen werden an ihn weitergereicht. Jede andere Engine landet die Zeilen für die Federation im Materialisierungs-Store. Die Treiberpakete werden pro Betriebssystem und Architektur aus einem festgelegten Release geholt (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64). Ein Betreiber kann stattdessen pro Connector einen für diesen Host gebauten Paketordner unter Admin → Einstellungen → Connector-Server angeben (`connectors.<connector>.bundle_dir`); siehe [Admin](admin.md). Alle fünf Trino-Connectors (Jira hat keinen) vergleichen Namen immer ohne Rücksicht auf Groß- und Kleinschreibung, wie es jedes Produkt selbst tut.

#### `sharepoint`

SharePoint-Listen werden als Schemas aufgezählt und als abfragbare Tabellen offengelegt. Zwei Auth-Methoden: `CLIENT_CREDENTIALS` (Standard) und zertifikatbasiert über ein PFX-Zertifikat. Secret-Werte in `mapping` werden über die Secrets-Engine aufgelöst, bevor sie den Connector erreichen.

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` oder `host` | `site-url` | SharePoint-Site-URL |
| `username` | `client-id` | Azure-App-Client-ID |
| `password` | `client-secret` | Azure-App-Client-Secret |
| `database` | `tenant-id` | Azure-Tenant-UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (Standard) oder `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | PFX-Pfad bei `auth_type: CERTIFICATE` — muss ABSOLUT sein |
| `mapping.certificate_password` | `certificate-password` | PFX-Passwort — der Key muss vorhanden sein, leerer String bei einer passwortlosen PFX |

Zertifikats-Auth auf den Nicht-Trino-Engines trägt zwei zusätzliche Regeln, beide beim Aufbau des `model.json`-Operanden des Calcite-pgwire-Servers durchgesetzt. `certificate_path` muss absolut sein: Der Server läuft mit seinem Bundle-Verzeichnis als Arbeitsverzeichnis, sodass ein relativer Pfad innerhalb des Runtime-Deps-Caches aufgelöst wird und die PFX nicht gefunden wird. `certificate_password` muss in `mapping` vorhanden sein, selbst wenn die PFX kein Passwort hat (in dem Fall ist es der leere String), denn der Calcite-Adapter lehnt ein Null-Passwort rundweg ab, und ein fehlender Key wird als Konfigurationsfehler behandelt statt stillschweigend als leeres Passwort gelesen. Ein fehlender oder relativer Wert löst `MissingConnectorConfig` mit Nennung des Felds aus.

Wenn der Connector `information_schema.columns` nicht offenlegt, registrieren Sie die Tabelle mit expliziten Spaltendefinitionen (aus der Microsoft-Graph-API) über die `registerTable`-Mutation.

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

Zertifikats-Auth, mit dem absoluten Pfad und dem immer vorhandenen Passwort:

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

Jedes sObject, das die Anmeldeinformationen abfragen können, ist eine Tabelle, die der Steward registrieren kann (`Account`, `Opportunity`, benutzerdefinierte `*__c`-Objekte); nichts wird automatisch registriert. Spalten und Typen stammen aus dem Describe des sObjects. Filter, Projektionen, Sortierungen und Limits werden als SOQL an Salesforce weitergegeben.

Eine Salesforce-Quelle nennt die „Meine Domäne“-Anmelde-URL der Organisation und genau einen vollständigen Satz Anmeldeinformationen, gewählt über `mapping.auth_type`. Die Anmelde-URL ist die vollständige `https://…`-URL (zum Beispiel `https://acme.my.salesforce.com`); ein bloßer Host oder ein anderes Schema wird abgelehnt und das Feld benannt. Ein unvollständiger Satz wird bei der Verwendung der Quelle abgelehnt, mit Angabe des fehlenden Werts. Jedes Geheimnis kann eine `${secret:…}`- oder `${env:…}`-Referenz sein. Ein als Literal eingegebenes Geheimnis wird im Tresor der Organisation abgelegt, und die Quelle behält ihre Referenz.

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` oder `host` | `login-url` | Die „Meine Domäne“-URL der Organisation (Setup, dann Meine Domäne). `login.salesforce.com` funktioniert für den Client-Credentials-Flow nicht |
| `username` | `client-id` | Verbraucherschlüssel der verbundenen Anwendung |
| `password` | `client-secret` | Verbrauchergeheimnis der verbundenen Anwendung |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (Standard), `USERNAME_PASSWORD` oder `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | Nur `USERNAME_PASSWORD`, zusätzlich zu Verbraucherschlüssel und -geheimnis |
| `mapping.security_token` | `security-token` | Nur `USERNAME_PASSWORD`; optional |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | Nur `ACCESS_TOKEN`; kein Verbraucherschlüssel oder -geheimnis |
| `mapping.api_version` | `api-version` | Optional, zum Beispiel `v61.0` |

Für `CLIENT_CREDENTIALS` braucht die verbundene Anwendung *Enable Client Credentials Flow* und einen *Run As*-Benutzer; jeder Lese- und Schreibvorgang läuft mit den Berechtigungen dieses Benutzers. Seit Summer '23 angelegte Organisationen sperren den Benutzername-Passwort-Flow standardmäßig.

**Lesen.** Auf Trino wird die Quelle über den `salesforce`-Katalog gelesen, der nur liest. Auf jeder anderen Engine bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle.

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einem registrierten sObject sind Mutationen: gesteuert, auditiert und je Umgebung behandelt wie jede andere Mutation. Auf jeder Engine, auch Trino, läuft ein Schreibvorgang auf dem pgwire-Server der Quelle; auf Trino läuft dieser Server nur für Schreibvorgänge. Jede Anweisung wird an Salesforce gesendet und bei der Ausführung festgeschrieben, kann also nicht zurückgerollt werden. Der Server gibt die geschriebenen Zeilen nicht zurück: Eine Anweisung mit `RETURNING`-Klausel wird abgelehnt, und GraphQL bietet für diese Tabellen keine Mutationsfelder an; SQL-`INSERT`, -`UPDATE`, -`DELETE` und -`COPY` sind die Schreibpfade. Ein Schreibvorgang ist eine einzelne Anweisung auf einer Tabelle: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` und `TRUNCATE` werden abgelehnt und das jeweilige Formular benannt. Eine SharePoint-Quelle wird auf dieselbe Weise geschrieben.

**Start.** Bevor er Verbindungen annimmt, liest der Server der Quelle die Spalten jedes sObjects, etwa vier Minuten bei einer Organisation mit 1.200 davon. Das fällt einmal an: Die Describe-Ergebnisse liegen im eigenen Zustandsverzeichnis der Quelle unter dem Datenverzeichnis der Instanz. Der Server wird gestartet, wenn die Quelle registriert oder geladen wird. Auf Trino hält der `salesforce`-Katalog seine eigenen Describe-Ergebnisse, ein Verzeichnis pro Quelle unter `/data/trino/cache/salesforce-describe` im Coordinator, sodass der Aufwand auch dort nur einmal anfällt, wenn dieser Pfad ein Volume ist, das den Container überdauert: das Volume `trino_cache` des Compose-Stacks oder das Coordinator-Cache-Volume des Helm-Charts (`trino.coordinator.cache.persistence`). Bis er Verbindungen annimmt, zeigt „Tabelle registrieren“ den Connector als startend an, und ein Schreibvorgang erhält dieselbe Meldung, statt zu warten.

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Benutzername-Passwort-Flow:

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

Jede Tabelle der Instanz steht zur Registrierung bereit, und nichts wird automatisch registriert. Eine viel genutzte Instanz hat Tausende Tabellen, daher gruppiert Register Table sie nach Thema. Jedes Thema ist der Anzeigename eines ServiceNow-Pakets oder „(no package)“ für eine Tabelle, die zu keinem gehört. Jede Tabelle wird als `name — label` aufgeführt. Die Auswahl einer Tabelle liest deren Spalten von der Instanz; das Auflisten der Tabellen tut das nicht.

Eine ServiceNow-Quelle nennt die vollständige `https://…`-URL der Instanz sowie Benutzername und Passwort. Die URL stammt aus `base_url` oder `host`; ein blosser Host oder ein anderes Schema wird abgelehnt und das Feld benannt (`instanceUrl`). Ein fehlender Benutzername oder ein fehlendes Passwort wird abgelehnt und das jeweilige Feld benannt. Jeder Wert kann eine `${secret:…}`- oder `${env:…}`-Referenz sein. Ein als Literal eingegebenes Geheimnis wird im Tresor der Organisation abgelegt, und die Quelle behält ihre Referenz.

| Quellfeld | Adapter-Eigenschaft | Hinweis |
| --- | --- | --- |
| `base_url` oder `host` | `instanceUrl` | Vollständige `https://…`-URL der Instanz (z. B. `https://acme.service-now.com`). Ein blosser Host oder ein anderes Schema wird abgelehnt |
| `username` | `username` | Basic-Auth-Benutzername |
| `password` | `password` | Basic-Auth-Passwort |

**Lesen.** Auf Trino wird die Quelle über den `servicenow`-Katalog gelesen, der nur liest. Auf jeder anderen Engine bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle. Nur die ausgewählten Spalten werden von der Instanz angefordert (`sysparm_fields`). Ein Filter wird nur in einer Form an die Instanz gesendet (`sysparm_query`), die der Adapter an einer Live-Instanz verifiziert hat; jeder andere Filter sowie jede Sortierung und jedes Limit werden auf die gelesenen Zeilen angewendet.

Ein Referenzfeld erscheint als zwei Spalten: `<field>` enthält die sys_id des referenzierten Eintrags, und `<field>__display` enthält den Anzeigewert. Ein leerer Feldwert ist NULL. Ein Feld, dessen Typ der Adapter nicht abbilden kann, erzeugt einen Fehler, der Tabelle, Feld und Typ benennt.

Die Tabellensuche (natürliche Sprache) stimmt auf Name, Label und Thema jeder Tabelle ab. Sobald eine Tabelle registriert ist, nehmen ihre Spalten an der Suche teil.

**Server-Lebenszyklus.** Der Server bedient nur die aus ihm registrierten Tabellen. Das Registrieren oder Entfernen einer Tabelle startet den Server neu und provisioniert die Quelle erneut auf der Engine; eine Anweisung, die während dieses Neustarts eintrifft, schlägt fehl.

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einer registrierten Tabelle sind Mutationen: gesteuert, auditiert und je Umgebung behandelt wie jede andere Mutation. Auf jeder Engine, auch Trino, läuft ein Schreibvorgang auf dem pgwire-Server der Quelle. Der Server gibt die Zeile zurück, die die Instanz nach dem Schreibvorgang enthält; eine `RETURNING`-Klausel wird daher akzeptiert, und GraphQL bietet Mutationsfelder für diese Tabellen an. Ein Schreibvorgang ist eine einzelne Anweisung auf einer Tabelle: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` und `TRUNCATE` werden abgelehnt und das jeweilige Formular benannt.

**Import-Sets.** Wenn eine Tabelle über eine aktive Transform-Map verfügt, bietet Register Table deren Import-Sets an, und der Steward kann die Tabelle an eines davon binden (eine Tabelle mit mehreren Maps benennt jedes einzeln). Einfügungen in eine gebundene Tabelle laufen über die Import-Set-API der Instanz und die Transformation der jeweiligen Map; Einfügungen in eine ungebundene Tabelle sowie alle Updates und Deletes werden direkt in die Tabelle geschrieben. Eine Zeile, die die Transformation ablehnt, wird zurückgewiesen und die betreffende Zeile benannt; bereits geschriebene Zeilen bleiben erhalten. Die Bindung wird beim Speichern der Tabelle geprüft: Sie muss das Import-Set einer aktiven Transform-Map in die Tabelle sein, auf einer ServiceNow-Quelle.

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Eine Jira-Cloud-Site wird über einen eigenen Calcite-Adapter gelesen und geschrieben. Eine Tabelle einer Jira-Quelle ist ein **Formular**: ein Projekt und ein Vorgangstyp, dazu die Felder, die ihre Spalten lesen. Register Table listet jedes Formular auf, für das das Konto einen Vorgang anlegen kann, nach Projekt gruppiert; die Auswahl eines Formulars liest dessen Felder aus Jira, das bloße Auflisten der Formulare liest sie nicht. Jedes Formular hat die Spalten `key` (sein Primärschlüssel), `id`, `summary`, `status`, `created` und `updated`, danach je eine Spalte pro gewähltem Feld, benannt, wie der Adapter sie benennt: der Feldname in Snake Case (`Story Points` wird zu `story_points`). Ein Feld, dessen Name bereits der Name einer Spalte ist, wird nach seiner Feld-ID benannt (`customfield_10099`), sodass sich jedes Feld auswählen lässt. Ein Benutzerfeld enthält die Konto-ID; sein Anzeigename ist ein eigenes Feld (`assignee.displayName`, Spalte `assignee_name`). Spalten, die Jira selbst schreibt, sind schreibgeschützt: `key`, `id`, `created`, `updated`, der Anzeigename eines Benutzerfelds sowie ein Feld in einer Form, die der Adapter nicht kennt und die er als JSON-Text liefert. Bei der Registrierung wird eine solche Spalte in ihrer Beschreibung mit „Read only: Jira sets it“ gekennzeichnet, sofern sie keine hat, und ein Schreibvorgang, der sie setzt, wird vom Server abgelehnt, wobei die Spalte benannt wird.

Die Tabelle eines Formulars nennt ihr Projekt und ihren Vorgangstyp in ihren `arguments` (`project`, `issue_type`), und jede Spalte liest das Feld, das ihr `path` nennt (eine Feld-ID oder ein Feldname), oder das Feld mit ihrem eigenen Namen. Eine im Admin registrierte Tabelle erhält beides von Jira; eine Konfiguration gibt sie an. Eine Jira-Tabelle, die etwas anderes festlegt oder eines von beiden nicht enthält, wird abgelehnt und benannt.

| Quellfeld | Adapter-Eigenschaft | Hinweis |
| --- | --- | --- |
| `base_url` oder `host` | `baseUrl` | Vollständige `https://…`-Adresse der Site (z. B. `https://acme.atlassian.net`). Ein blosser Host oder ein anderes Schema wird abgelehnt |
| `username` | `email` | E-Mail-Adresse des Atlassian-Kontos |
| `password` | `apiToken` | Ein API-Token dieses Kontos, erstellt unter id.atlassian.com unter Security, API tokens |

**Verknüpfungen.** Die Tabelle `issue_links` der Quelle enthält die Verknüpfungen zwischen den Vorgängen ihrer Formulare: `link_id`, `link_type`, `outward_key` und `inward_key`, jede Verknüpfung einmal. Eine Beziehung zwischen zwei Formularen wird darüber als Junction deklariert, mit `link_type` als Diskriminator, sodass GraphQL und Cypher vom Vorgang zum verknüpften Vorgang nach Verknüpfungstyp navigieren.

**Lesen.** Eine Suche nach Schlüssel (`key =`, `key IN`) wird per Schlüssel abgerufen, sodass ein Schlüssel, der keinen Vorgang benennt, keine Zeile ergibt. Ein Vergleich von Datum oder Zahl wird als JQL an Jira gesendet. Jeder andere Filter und jede Sortierung wird auf die gelesenen Zeilen angewendet: Eine Gleichheit auf Text, einer Option, einem Status oder einem Benutzer wird nicht gesendet, da JQL einen Wert ablehnt, der nichts benennt, wo SQL keine Zeilen zurückgibt. Ein Mehrwertfeld (Labels, Mehrfachauswahl, Komponenten, Versionen) ist Text mit einem JSON-Array und wird genauso geschrieben.

Auf DuckDB, PostgreSQL und ClickHouse bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle. Auf Trino hat die Quelle keinen eigenen Katalog und wird über ihren Server geladen.

**Server-Lebenszyklus.** Der Server bedient nur die aus ihm registrierten Formulare, daneben seine Formularliste, seine Feldliste und die Verknüpfungen. Das Registrieren, Ändern oder Entfernen eines Formulars startet den Server neu und provisioniert die Quelle erneut auf der Engine.

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einem Formular sind Mutationen, auf jeder Engine auf dem pgwire-Server der Quelle geschrieben. Zeilen werden in Stapeln von 50 eingefügt; ein Update oder Delete erfolgt Vorgang für Vorgang, per Schlüssel. Das Setzen von `status` führt den Workflow-Übergang in diesen Status aus und wird abgelehnt, wobei die erreichbaren Status benannt werden, wenn der Workflow des Vorgangs keinen hat. Ein Vorgang mit Teilaufgaben wird nicht gelöscht. Der Server gibt jede geschriebene Zeile per Schlüssel zurück; eine `RETURNING`-Klausel wird daher akzeptiert, und GraphQL bietet Mutationsfelder an. Auf `issue_links` erstellt `INSERT` eine Verknüpfung und `DELETE` entfernt eine; `UPDATE` wird abgelehnt.

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

Das Cloud-Inventar stellt die Ressourcen von Azure, AWS und GCP als einen Satz Tabellen dar: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` und `compute_security_groups`, das eine Compute-Ressource mit ihren Sicherheitsgruppen verknüpft. Jede Tabelle hat über die drei Clouds hinweg dieselben Spalten, und ihre Spalte `cloud_provider` nennt die Cloud, aus der eine Zeile stammt. Die Quelle liest nur, und ihre Tabellen werden wie alle anderen von einem Steward registriert.

Eine Quelle nennt eine oder mehrere Clouds. Für jede Cloud gilt alles oder nichts: Eine Cloud mit nur einem Teil ihrer erforderlichen Werte wird abgelehnt, mit Angabe der fehlenden Werte, ebenso eine Quelle, die keine Cloud nennt. Die Quelle liest genau die Clouds, die sie nennt. Alle Werte stehen in `mapping`, und jedes Geheimnis kann eine `${secret:…}`- oder `${env:…}`-Referenz sein. Ein als Literal eingegebenes Geheimnis wird im Tresor der Organisation abgelegt, und die Quelle behält ihre Referenz.

| Cloud | `mapping`-Schlüssel | Connector-Eigenschaft | Hinweise |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Erforderlich |
| Azure | `azure_client_id` | `azure.client-id` | Erforderlich; die Client-ID der App-Registrierung |
| Azure | `azure_client_secret` | `azure.client-secret` | Erforderlich; Geheimnis |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Erforderlich; durch Kommas getrennt |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Erforderlich |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Erforderlich; Geheimnis |
| AWS | `aws_region` | `aws.region` | Erforderlich, zum Beispiel `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Erforderlich; durch Kommas getrennt |
| AWS | `aws_role_arn` | `aws.role-arn` | Optional; eine Rolle, die für kontoübergreifenden Zugriff übernommen wird |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Erforderlich; ABSOLUTER Pfad zu einer JSON-Schlüsseldatei eines Dienstkontos |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Erforderlich; durch Kommas getrennt |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Optional; wie lange ein Ergebnis wiederverwendet wird, bevor die Clouds erneut gefragt werden (Adapter-Standard 5) |

Die GCP-Schlüsseldatei wird von dem geöffnet, was die Quelle liest. Auf Trino ist das der Trino-Server, der Pfad muss dort also existieren. Auf jeder anderen Engine ist es der pgwire-Server der Quelle auf dem Provisa-Host, dessen Arbeitsverzeichnis sein eigenes Zustandsverzeichnis ist, sodass ein relativer Pfad abgelehnt wird.

Auf Trino wird die Quelle über den `cloudops`-Katalog gelesen. Auf jeder anderen Engine bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle, wie eine SharePoint-Quelle.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Splunk-Suchergebnisse sind als Tabellen abfragbar (z. B. `internal_server`). Die Connector-URL stammt aus `base_url` oder wird als `https://{host}:{port}` mit Standardport `8089` konstruiert. Auth: Ist `mapping.use_token` `true` (Standard), wird `password` als API-Token übergeben; ist es `false`, werden `username` und `password` als separate Zugangsdaten übergeben.

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, sonst `https://host:port` (Standardport 8089) |
| `password` | `token` oder `password` | Token, wenn `use_token: true` |
| `username` | `user` | nur, wenn `use_token: false` |
| `database` | `app` | auf eine Splunk-App einschränken |
| `mapping.datamodel_filter` | `datamodel-filter` | auf ein Data Model filtern |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | für selbstsignierte Zertifikate |

Auf dem pgwire-Replikat-Pfad (jede Engine außer Trino) werden dieselben vier optionalen Einstellungen zu den Calcite-`model.json`-Operandenschlüsseln `app`, `token`/`username`+`password`, `datamodelFilter` und `disableSslValidation`. Die letzten beiden haben die Typen, in die `SplunkSchemaFactory` sie umwandelt: einen String und einen Boolean.

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

#### Google Workspace und Microsoft 365 {: #google-workspace-and-microsoft-365 }

`google_workspace` und `microsoft_365` lesen E-Mails, Kalender und Aufgaben der Konten, die eine Quelle benennt, und landen sie als Replikate. Es sind keine Calcite-Connectors: Jeder hat einen eigenen Loader, der die API des Anbieters aufruft, auf der einen Seite Gmail, Google Calendar und Google Tasks, auf der anderen Microsoft Graph. Das Formular führt sie unter Enterprise als „Google Workspace (Gmail)“ und „Microsoft 365 (Outlook, Exchange)“.

**Tabellen.** Beide Typen erzeugen dieselben kanonischen Tabellen: dieselben Namen, Spaltentypen und dieselbe Reihenfolge. Eine Vereinigung beider braucht weder Cast noch Umbenennung; eine Spalte, für die ein Anbieter keinen Wert hat, ist NULL. Die Tabellen stehen im Schema `default`, und jede Zeile beginnt mit dem Konto, aus dem sie gelesen wurde. Eine Quelle bietet die Tabellen der Ressourcen an, die sie liest:

| Ressource | Tabellen | Durch Anweisungen beschrieben |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Nein |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Die Spalten stehen im [kanonischen Mail-Schema](arch/canonical-mail-schema.md). `events` enthält Einzeltermine, Serienmaster mit ihrer Wiederholung als RFC-5545-Zeilen und Ausnahmen; die aufgelösten Termine einer Serie enthält die Tabelle nie.

**Registrierung.** Der Administrator einer Organisation trägt zuerst den Client der Organisation für die Plattform unter Admin → Email ein: eine Client-ID und ein Secret, bei Microsoft 365 dazu die Verzeichnis-ID (Tenant-ID) oder die Domain. Wer eine Quelle hinzufügt, sieht und tippt ihn nie. Eine Quelle hat dann eine von zwei Formen:

- **Ein Postfach.** Die Quelle nennt eine Adresse in `accounts`. Ihr Inhaber klickt im Formular auf **Connect** und genehmigt den Client der Organisation. Die daraus entstehende Genehmigung liegt im State-Store der Organisation, nicht in den Einstellungen der Quelle. Bei Google Workspace nennt diese Form zusätzlich `sign_in: google_account`.
- **Die Postfächer der Organisation.** Die Quelle nennt `mailboxes` als genau eines von `everyone: true`, `group: <address or id>` oder `list: [addresses]`. Sie hält keine Zugangsdaten, und niemand verbindet sie. Die Organisation liest mit ihrem eigenen Zugriff, und zwar erst, wenn der Administrator „Read the organization's mailboxes“ eingeschaltet hat. Bei Google ist dieser Zugriff ein Service-Account-Schlüssel mit domänenweiter Delegierung, dazu eine Administratoradresse für Verzeichnisaufrufe. Bei Microsoft ist es der Client-Credentials-Grant der App-Registrierung.

Bei der zweiten Form hält eine Tabelle die Zeilen aller Postfächer. Eine Rolle, die die Tabelle lesen darf, liest jedes Postfach, es sei denn, eine Zeilenregel auf der Spalte `account` schränkt sie ein.

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**Einstellungen.** Jedes Modul weist einen Schlüssel, den es nicht liest, mit Namen zurück.

| `mapping`-Schlüssel | Typ | Bedeutung |
| --- | --- | --- |
| `resources` | beide | Welche von `mail`, `calendar`, `tasks` die Quelle liest; mindestens eine |
| `accounts` | beide | Die Adresse des einen Postfachs (Form mit einem Postfach) |
| `mailboxes` | beide | `everyone`, `group` oder `list` (Organisationsform) |
| `sign_in` | `google_workspace` | `google_account` (Form mit einem Postfach) |
| `mail_since` | beide | `YYYY-MM-DD`; nichts, was früher eingegangen ist, wird gehalten |
| `mail_content` | `google_workspace` | `full`, oder `headers` nur für Header und Labels (Gmails Scope `gmail.metadata`). Erforderlich, wenn E-Mail gelesen wird |
| `mail_search` | `google_workspace` | Eine Suche, wie sie in Gmails Suchfeld getippt wird |
| `mail_labels` | `google_workspace` | Label-Namen; nur E-Mails mit allen davon werden gelesen |
| `mail_include_spam_trash` | `google_workspace` | Standardmäßig `false` |
| `calendar_send_updates` | beide | Ob ein Schreibvorgang die Teilnehmer eines Termins benachrichtigt; standardmäßig aus |
| `calendar_check_days` | `microsoft_365` | Tage vor und nach heute, die die Änderungsprüfung ansieht; eine ganze Zahl, 1 oder mehr, erforderlich, wenn der Kalender gelesen wird |

Google weist eine Suche unter `gmail.metadata` zurück, daher wird `mail_content: headers` mit `mail_search` oder `mail_since` zurückgewiesen.

**Scopes.** Eine Anmeldung fragt nur die Scopes der angekreuzten Ressourcen an:

| Ressource | Google Workspace | Microsoft 365 (delegiert) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, oder `gmail.metadata` nur für Header | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| jede Quelle | `userinfo.email` | `offline_access`, `User.Read` |

Für die Organisationsform braucht Microsoft die Anwendungsberechtigungen `Mail.Read` und `User.Read.All`, dazu `Calendars.ReadWrite` und `Tasks.ReadWrite.All` für Kalender und Aufgaben. Googles Service-Account ist für die Scopes in `DELEGATED_SCOPES` autorisiert, die auch das Lesen von Verzeichnisbenutzern und Gruppenmitgliedern umfassen.

**Aktualisierung und Aktualität.** Die Probe-Typen dieser Quellen sind `hash` und `none`, und ihr Loader liefert das Änderungstoken:

- **Gmail:** ein Digest der aktuellen History-ID jedes Postfachs, ein Aufruf von `users.getProfile` je Postfach.
- **Microsoft-Mail:** jeder gehaltene Delta-Link, verfolgt für ein Element.
- **Google-Kalender und -Aufgaben:** das Tag und der Zeitpunkt der letzten Änderung jedes Kalenders; das Tag jeder Aufgabenliste sowie ID und Aktualisierungszeit jeder Aufgabe.
- **Microsoft-Kalender und -Aufgaben:** die Änderungszeilen jedes Postfachs über `calendar_check_days` vor und nach heute.

Eine Prüfung, die sich nicht durchführen lässt, fordert ein Lesen an; sie meldet nie „unverändert“. Kalender- und Aufgabentabellen werden bei jeder Aktualisierung vollständig gelesen. Die Mail-Replikate einer Microsoft-365- oder Google-Workspace-Quelle werden an Ort und Stelle gehalten: Jeder Teil eines Postfachs wird geschrieben, sobald er eintrifft, und ist sofort abfragbar. Eine Google-Workspace-Quelle, die mit `mail_search` eingrenzt, wird bei jeder Aktualisierung erneut vollständig gelesen, weil nur Google sagen kann, was eine Suche trifft.

**Schreibvorgänge.** `INSERT`, `UPDATE` und `DELETE` auf `events`, `event_attendees` und `tasks` gehen für beide Typen über einen gemeinsamen Schreibpfad an den Anbieter. Ein `UPDATE` oder `DELETE` benennt seine Zeilen über `provider`, `account` und `id`; ein Teilnehmer wird über `event_id` und `address` benannt. Ein einzelner Termin einer Serie wird über `series_id` und `original_start_at` benannt. Ein Schreibvorgang, der eine Spalte außerhalb der beschreibbaren Menge der Tabelle setzt, wird mit Namen zurückgewiesen. Google schreibt mit `sendUpdates=all`, wenn `calendar_send_updates` an ist, und mit `sendUpdates=none`, wenn es aus ist. Microsoft Graph kann eine solche Änderung nicht stillschweigend vornehmen, daher wird eine Änderung der Teilnehmer eines Meetings zurückgewiesen, mit Nennung der Einstellung.

**Ausschlüsse.** E-Mail wird nur gelesen; keine Anweisung ändert ein Postfach. `calendars`, `task_lists` und jede Mail-Tabelle nehmen keine Schreibvorgänge an. Gmails Spam und Papierkorb bleiben außen vor, solange `mail_include_spam_trash` nicht an ist. Googles Tasks-API behält nur den Tag, an dem eine Aufgabe fällig ist, und kennt keine Erinnerung.

### API-Quellen {: #api-sources }

Registrieren Sie jeden HTTP-Endpunkt als abfragbare Tabelle.

| API-Typ | Discovery | Spaltenableitung |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI-Spec-Parsing | Primitive → nativ, Objekte → JSONB |
| `graphql_remote` | Schema-Introspektion | Primitive → nativ, Objekte → JSONB |
| `grpc_remote` | Server Reflection | Primitive → nativ, Objekte → JSONB |

API-Antworten werden abgerufen, in PostgreSQL gecacht (konfigurierbare TTL) und als GraphQL-Typen offengelegt. Gecachte Tabellen nehmen wie jede andere Quelle an föderierten Abfragen teil.

**JSONB-Regeln**: Komplexe Spalten (Objekte, Arrays), die als JSONB gespeichert werden, sind nicht filterbar. Der Zugriff auf Unterfelder verwendet die `->>`-Extraktion in SQL. Beziehungen werden zwischen Tabellen über skalare FK-Spalten deklariert. JSONB-Blob-Spalten sind keine Join-Ziele. Verwenden Sie JSONB-Promotion, um verschachtelte Felder in native skalare Spalten umzuwandeln, wenn Filtern oder Joinen darauf benötigt wird.

#### GitHub und GitLab {: #github-and-gitlab }

GitHub und GitLab sind Marken-Quellen, die `graphql_remote` trägt: Jede erscheint im Sources-Formular unter ihrem eigenen Namen und registriert sich als Remote-GraphQL-Quelle, die ihre Marke festhält. Das Formular verlangt ein Personal Access Token und sonst nichts. Der Endpunkt (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) und das Schema werden mit Provisa ausgeliefert, daher wird das Schema bei der Registrierung nicht introspiziert. Das Token wird beim Hinzufügen der Quelle geprüft. Das Hinzufügen der Quelle registriert keine Tabellen: Jede Tabelle, die das Schema anbietet, steht unter dem Namensraum der Quelle (`gh`, `gl`) bereit, und der Steward registriert die gewünschten. Ein Feld, das das Token nicht lesen darf, wird beim Registrieren aus einer Tabelle ausgelassen.

#### Stripe {: #stripe }

Stripe ist eine Marken-Quelle, die `openapi` trägt, so gebaut wie GitHub und GitLab über `graphql_remote`. Sie erscheint im Sources-Formular als Stripe unter Enterprise. Sie registriert sich als OpenAPI-Quelle, deren Zeile die Marke in `federation_hints` festhält und deren Spec-Pfad `brand:stripe` lautet.

**Was sie erreicht.** Die Stripe-REST-API, über Stripes veröffentlichte OpenAPI-Spezifikation (`stripe/openapi`, `spec3.json`). Die Spezifikation wird mit Provisa ausgeliefert, gepinnt und gzip-komprimiert, und nicht bei der Registrierung abgerufen. Die ausgelieferte hat die API-Version `2026-09-30.endive` mit 425 Pfaden. Jeder Aufruf trägt `Stripe-Version` mit dieser Version, sodass jede Antwort die Form hat, die die Spezifikation beschreibt, welche Version das Konto auch als Standard hat. Felder, die Stripe zurückgibt und die Spezifikation auslässt, werden aus `stripe.additions.json` ergänzt.

Wie bei jeder OpenAPI-Quelle ist jede `GET`-Operation eine angebotene Tabelle und jede andere Operation ein angebotener Befehl. Das Hinzufügen der Quelle registriert keine davon: Der Steward registriert die gewünschten.

**Auth.** Das Formular verlangt genau eine Angabe, ein Zugriffstoken (`BRAND_AUTH`: `token`). Verwenden Sie einen geheimen oder eingeschränkten Stripe-API-Schlüssel. Das Token wird als Bearer-Token gesendet und vor dem Hinzufügen der Quelle einmal mit `GET /v1/balance` geprüft. Ein abgelehntes Token lässt die Registrierung mit `openapi.credential_rejected` scheitern; ein fehlendes mit `openapi.credential_required`.

**Aktualisierung und Aktualität.** Eine Stripe-Tabelle ist ein Replikat, wie jede OpenAPI-Tabelle. Ihre gecachten Antworten gelten für das `cache_ttl` der Quelle, das eine Registrierungsanfrage auf 300 Sekunden setzt, sofern sie nichts anderes sagt. Es gelten die OpenAPI-Probe-Typen. Ein Preset kann den Parameter benennen, mit dem eine Aktualisierung nur das Geänderte abruft. Die Stripe-Marke benennt keinen, daher liest jede Aktualisierung ihre Tabellen vollständig.

**Ausschlüsse.** Stripes veraltete Token- und Sources-Zahlungsintegration bleibt aus der ausgelieferten Spezifikation draußen. Die Liste steht unter [Ausschlüsse bei einer aus einer Spezifikation gebauten Quelle](#exclusions-on-a-specification-built-source). Die eigenen `exclusions` eines Operators werden denen der Marke hinzugefügt.

`stripe` erscheint auch in `SourceType` unter den gestubbten Supabase-`wrappers`-Typen. Eine über das Formular hinzugefügte Stripe-Quelle ist eine `openapi`-Quelle, nicht dieser Stub.

#### Ausschlüsse bei einer aus einer Spezifikation gebauten Quelle {: #exclusions-on-a-specification-built-source }

Eine veröffentlichte OpenAPI-Spezifikation beschreibt alles, was die entfernte API annimmt, auch das, was ihr Hersteller als veraltet markiert hat und was ein Deployment nie senden darf. Eine OpenAPI-Quelle kann Ausschlüsse tragen: Dinge in der Spezifikation, die die Quelle nicht anbietet.

| Ausgeschlossen | Benannt durch | Wirkung |
|---|---|---|
| Eine Operation | `method` und `path` | Wird nicht zur Registrierung als Tabelle oder als Befehl aufgelistet |
| Ein Anfrageargument | `name`, bei einer Operation (`method` und `path`) oder, wenn keines von beiden, bei jeder Operation, die es hat | Gehört nicht zu den Argumenten der Operation, wird nicht angezeigt, nie gesendet |

Ausschlüsse werden unter `exclusions` im `mapping` der Quelle gesetzt:

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

Sie werden beim Laden auf die Spezifikation angewendet, sodass die Registrierungsliste, die erzeugten Befehle und die gesendete Anfrage alle eine Spezifikation sehen, in der die ausgeschlossenen Dinge nicht existieren. Eine Body-Eigenschaft sowie ein Query-, Header- oder Cookie-Parameter können ausgeschlossen werden; ein Pfadparameter nicht, weil er Teil der Adresse der Operation ist.

Ein Ausschluss, der eine Operation oder ein Argument benennt, das die Spezifikation nicht hat, lässt das Laden der Spezifikation dieser Quelle fehlschlagen und benennt jeden veralteten Eintrag, sodass eine Liste auffällt, die zu einer neueren Spezifikation nicht mehr passt.

Eine Marken-Quelle liefert ihre eigenen Ausschlüsse als Teil ihrer Kuratierung mit, und die eines Operators werden ihnen hinzugefügt. Die Stripe-Quelle schließt die veraltete Token-und-Source-Zahlungsintegration von Stripe aus: das Erstellen einer Charge, die Tokens- und Sources-APIs, das Hinzufügen einer Karte oder eines Bankkontos zu einem Kunden aus einem Token sowie die Argumente `source`, `card`, `bank_account` und `default_card` von Schreibvorgängen für Kunden und Rechnungszahlungen. Zahlungen werden über Payment Intents, Payment Methods und Setup Intents abgewickelt, die die Quelle anbietet. Das Lesen und Löschen von Zahlungsquellen, die ein Konto bereits hat, bleibt im Angebot.

### GovData

Offene Daten der US-Regierung. Der Zugriff ist nach Themengruppierung partitioniert.

Einer Quelle werden ein oder mehrere Themen zugewiesen, und jedes Thema bringt die unten aufgeführten Adapter-Schemas mit. Ein Schema kann zu mehr als einem Thema gehören (`census`). Die Schemas `ref` und `geo` sind Linker-Schemas: Referenzdaten und Geografie, über die die Tabellen jedes Themas joinen, sodass jede Quelle sie bereitstellt, welche Themen sie auch erhält. Die Liste ist einmal auf dem Server festgelegt und wird per Test an die Schemas gebunden, die das gepinnte Adapter-Bundle bereitstellt.

| Thema | Offengelegte Schemas |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| jede Quelle | `ref`, `geo` |

`ALL` ist die Kurzform eines Abonnements für jedes Thema; es ist keine Auswahl im Sources-Formular.

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `id` | Ja | — | Eindeutiger Bezeichner |
| `subject` | Ja | — | Einer der obigen Themenwerte |
| `domain_id` | Ja | — | Domäne, zu der diese Quelle gehört |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

Eine im Admin angelegte AskAmerica-Quelle nimmt eine Anmeldeinformation entgegen, den API-Schlüssel. Provisa legt der API von AskAmerica den Schlüssel vor und erhält die kurzlebigen, schreibgeschützten Speicher-Anmeldeinformationen, mit denen die Daten gelesen werden; der Schlüssel wird dort namentlich abgelehnt, wenn er unbekannt oder widerrufen ist.

Die Quelle ist dann eine Postgres-Wire-Quelle wie die SharePoint-, Salesforce- und Cloud-Inventar-Quellen: Provisa startet dafür den mitgelieferten `pgwire-govdata`-Server, mit dem Schlüssel und diesen Anmeldeinformationen in der Umgebung des Servers, und jede Engine erreicht diesen Server so, wie sie den eines Geschwisters erreicht. DuckDB, Postgres und ClickHouse binden ihn ein und lesen seine Tabellen an Ort und Stelle, sodass eine AskAmerica-Tabelle mit Tabellen anderer Quellen joint, unter Sichten liegt und wie jede andere ausgeliefert wird. Trino hat dafür keinen Connector und liest ein Replikat, das über den Server gelandet wurde: eine Kopie der Tabelle im Materialisierungs-Store, so frisch wie ihr letzter Refresh unter den Cache- und Replikationseinstellungen der Tabelle und nicht zur Abfragezeit gelesen. Eine Tabelle wird aus dem Adapter-Schema gelesen, das sie nennt (`sec`, `econ` und so weiter).

Der Server bindet seine Schemas aus dem Objektspeicher ein, bevor er eine Verbindung annimmt; das dauert beim ersten Start Minuten. Er wird gestartet, wenn die Quelle gespeichert wird; bis er bereit ist, meldet das Register-Table-Formular, dass die Quelle noch startet.

### Wo das Passwort einer Quelle lebt {: #where-a-sources-password-lives }

Das Passwort einer Quelle wird nie neben den übrigen Verbindungseinstellungen gespeichert. Die
`sources`-Zeile der Control Plane trägt eine `password_ref`-Spalte mit dem, was für das Passwort
eingegeben wurde: dem Wert selbst oder einer Referenz wie `${env:PG_PASSWORD}` oder
`${secret:SNOWFLAKE_KEY}`. Sie wird in dem Moment aufgelöst, in dem die Quelle angewählt wird,
innerhalb der Organisation, in deren Namen die Anfrage läuft.

Ein Wert ohne `${…}` wird unverändert verwendet. `${env:VAR}` liest die Prozessumgebung des
Deployments und benötigt keine Bindung. `${secret:NAME}` benennt ein Secret, das einer Organisation
gehört, sodass es nur innerhalb der eigenen Operationen dieser Organisation aufgelöst wird: die
Admin-Introspektions-Nahtstellen und das Query-Terminal, das jede Oberfläche erreicht, stellen diese
Bindung her.

Das Feld hält, was Sie hineinschreiben, gleichgültig wie die Quelle registriert wurde:

- **Aus der Konfiguration.** Die Datei ist der Datensatz: Provisa kopiert den Wert oder die Referenz
  wortwörtlich in `password_ref`.
- **Aus dem Sources-Formular.** Was in das Passwortfeld eingegeben wird, wird wortwörtlich
  gespeichert, ein Wert wie eine Referenz. Provisa schreibt dafür nichts in den Vault der
  Organisation und erfindet keinen Secret-Namen. Um eine Anmeldeinformation aus der Zeile der
  Control Plane herauszuhalten, legen Sie das Secret selbst an (siehe [Secrets](secrets.md)) und
  tippen `${secret:NAME}` in das Feld.

Ein als Wert eingegebener Wert wird so, wie eingegeben, in der Datenbank der Control Plane
gespeichert. Das erneute Eintippen des Passworts ersetzt den Text des Felds. Das Löschen der Quelle
entfernt ihre Zeile und nichts aus dem Vault: Ein Secret, das Sie angelegt haben, gehört Ihnen.

`password_ref` wandert nicht zwischen Umgebungen. Ein Branch oder eine kopierte Umgebung
liefert ihre eigenen Verbindungswerte, und der Vault, den eine Referenz benennt, gehört zu welcher
Umgebung auch immer sie geliefert hat.

### Data-Profiler {: #data-profiler }

Ein Data Profiler ist eine Quelle, die einen Namen, einen Zeitplan und Lauf-Standardwerte hält, sonst nichts. Sie wird nie introspiziert. Tabellen anderer Quellen werden ihr hinzugefügt, und jeder Lauf misst sie und schreibt seine Zeilen in die Control Plane der Organisation, wo sie wie die Tabellen jeder anderen Quelle gelesen werden. Wie Sie einen Profiler anlegen und Tabellen hinzufügen, steht unter [Testdaten](test-data.md).

### Data-Quality-Checker {: #data-quality-checkers }

Ein Data-Quality-Checker ist einfach ein weiterer Quelltyp. Seine Scan-Ausgabe sind Daten: Jedes Prüfergebnis ist eine Beobachtung, landet also über den gewöhnlichen Quellpfad und bekommt alles, was jede andere Quelle bekommt, von Takt, Aktualität, Ereignissen und Lineage bis zu Governance, RLS, Grid und Export.

Zwei Checker werden unterstützt, und die Wahl zwischen ihnen ist ebenso eine Lizenz- wie eine Funktionsfrage.

| Quelltyp | Vertrags-Dialekt | Extra | Lizenz | Gehostete Cloud-Plane |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda-Contract-YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Abgelehnt — siehe unten |
| `great_expectations` | Expectation-Suite-JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Erlaubt |

Die Elastic License 2.0 verbietet, die Software Dritten als gehosteten oder verwalteten Dienst bereitzustellen, und Soda im SaaS-Plane im Auftrag eines Mandanten auszuführen wäre genau das. `config/capabilities.yaml` hält das als `cloud_eligible: false` an der Option `soda` fest, und das gehostete Plane liest dieses Flag. Ein gehostetes Deployment, das Soda will, verweist auf einen Soda-Endpunkt, den der Betreiber selbst betreibt.

Provisa liefert nichts mit und linkt nichts. Der Scan läuft in einem Kind-Interpreter (`python -m provisa.dq.worker`), dem einzigen Ort, an dem `soda_core` oder `great_expectations` importiert wird. Ein Source-Available-Checker erreicht den Serverprozess deshalb nie, und stürzt ein Checker ab, reißt er einen Unterprozess mit und lässt die Event-Loop in Ruhe.

**Die Quelle zeigt auf Provisas eigenen pgwire-Endpunkt.** So kann ein einziger Postgres-Treiber eine Tabelle prüfen, hinter der Snowflake oder Iceberg steht: Der Checker scannt die föderierte Sicht, nie das System darunter. Auf dieser Verbindung gilt die Policy, deshalb deklarieren Sie die Scan-Identität ausdrücklich. Würde sie geerbt, könnte eine gefilterte Zeilenmenge eine Prüfung bestehen lassen, ohne dass es jemand merkt.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**Eine Ergebnistabelle pro Vertrag, und der Vertrag ist die ganze Registrierung.** Die Tabelle trägt `dq_contract`, den Vertragstext Wort für Wort, und sonst nichts über ihre Form. Spalten, Watermark und Promotions werden alle daraus abgeleitet.

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

Aus diesem Text leitet die Registrierung vier Dinge ab:

- **Lineage.** Der Vertrag nennt sein Zieldataset bereits, also parst die Registrierung ihn so, wie `extract_inputs` SQL parst, und löst ihn auf die gesteuerte Tabelle auf. Es gibt eine Definition und keine zweite Kopie, die abweichen könnte. Ein Vertrag, der ein nicht gesteuertes Dataset nennt, scheitert bei der Registrierung lautstark und landet so nie Zeilen, die niemand angefordert hat.
- **Spalten.** Der Ergebnis-Umschlag gehört dem Checker: 16 mitgelieferte Spalten, von `scan_id` bis `diagnostics`. Deklarierte Spalten werden nur auf ihr `visible_to` gelesen, das bei allen gleich sein muss, und dann ersetzt.
- **Watermark.** `scan_time` wird zum Watermark, was das Landen zu einem Append macht. Die Scan-Historie wächst ohne eigenes Historien-Subsystem.
- **Promotions.** `freshness_max_timestamp` und `dataset_rows_tested` werden aus dem `diagnostics`-jsonb als typisierte Spalten hervorgehoben. Weitere fügen Sie hinzu wie bei jeder anderen jsonb-Spalte.

Für das Timing braucht es keine neuen Felder. `change_signal` plus `cache_ttl` legen fest, wie oft abgefragt wird, `mv_debounce_quiet` und `mv_debounce_max_delay` fassen einen Schwall vorgelagerter Änderungen zu einem Scan zusammen, ein Kalenderraster macht ihn periodisch, und `expected_events` hält den Scan zurück, bis seine Eingaben über das Fenster aktuell sind. Die Poll-Schleife ist der Scan-Scheduler.

`outcome` ist einer der Werte `pass`, `fail`, `warn`, `error` oder `skipped`. Keiner davon setzt von sich aus etwas durch. Wenn Sie Durchsetzung wollen, deklarieren Sie sie getrennt: als Preflight oder als MV über die gelandeten Ergebnisse. Eine gelandete Beobachtung muss nicht deterministisch sein, also sind hier Prüfungen in Ordnung, die nie einen Preflight steuern könnten, etwa ein Anomalie-Score, eine Veränderung über ein gleitendes Fenster oder Aktualität gemessen an jetzt.

Sie schreiben den Vertrag in der UI, im Data-Quality-Panel der Bearbeitungsseite der Tabelle, und der rohe Vertragstext dort ist immer die maßgebliche Quelle. Ein Probelauf führt den Vertrag gegen die Live-Tabelle aus und zeigt die Ergebnisse, ohne sie zu landen. Damit fangen Sie einen Vertrag ab, dessen Datasetname unerwartet aufgelöst wurde und der sonst nur bestandene Zeilen landen würde.

---

## Benutzerdefinierte Connectors {: #custom-connectors }

Ein Betreiber kann den nativen Federation-Engines (Postgres, DuckDB und ClickHouse) Zugriff auf einen neuen Quelltyp geben, indem er in `config/custom_connectors.yaml` einen Connector dafür deklariert. Code ist nicht nötig.

Damit bekommen die nativen Engines, die ohne Cluster laufen, dieselbe konfigurationsgesteuerte Erweiterbarkeit, die die Trino-Engine auf ihrer eigenen Ebene hat. Dort wird ein generischer JDBC-Connector pro Quelltyp parametrisiert, jeder Typ erhält einen Katalog-`.properties`-Rumpf, und Provisa ergänzt eigene Trino-Connector-Plugins (Splunk, SharePoint, Calcite).

Die Datei wird leer ausgeliefert. Die eingebauten Connectors decken die Quellen ab, die ohne zusätzliche Einrichtung funktionieren, und alles in dieser Datei schreibt der Betreiber. Für einen anderen Pfad, etwa in Tests, setzen Sie `PROVISA_CUSTOM_CONNECTORS`.

### Deskriptor-Arten {: #descriptor-kinds }

| Engine | Art | Mechanismus | Was der Deskriptor liefert |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (ISO-Standard) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + Scanner-View | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (legt automatisch jede entfernte Tabelle offen) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` pro Tabelle (Spalten aus der Registry) | `ch_engine`, `engine_template` (kann `{table}` tragen) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse leitet das Schema ab | `ch_engine`, `engine_template` |

**Postgres ist generisch.** SQL/MED ist ein ISO-Standard, sodass jeder konforme FDW dieselbe DDL-Form teilt: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, optional `CREATE USER MAPPING`, dann entweder `IMPORT FOREIGN SCHEMA` (wenn `supports_import: true`) oder ein explizites `CREATE FOREIGN TABLE` pro Tabelle (wenn `false`). Ein `pg_fdw`-Deskriptor liefert nur die Pro-FDW-Varianz: Erweiterungsname, Server-Options-Keys, User-Mapping-Keys, Import-Flag, Tabellen-Optionen. Jeder standardkonforme FDW ist daher allein aus der Konfiguration ansteuerbar.

**DuckDB unterstützt zwei Mechanismen.** Eine Erweiterung, die einen Katalog über ATTACH offenlegt, verwendet `duckdb_attach`; eine, die eine lesende Table-Function offenlegt, verwendet `duckdb_scan`. Eine Erweiterung, die zu keinem der beiden Muster passt, wird nicht unterstützt.

**ClickHouse unterstützt drei Mechanismen**, einen pro Integrations-Engine-Form: eine relationale DATABASE-Engine, die automatisch jede entfernte Tabelle offenlegt (`clickhouse_database`, z. B. Redis/MySQL), eine Pro-Tabelle-Engine, deren Spalten die Registry liefert (`clickhouse_table`, z. B. die JDBC-/ODBC-Brücke; der `engine_template` kann einen `{table}`-Platzhalter tragen, den die Laufzeit bindet), und eine Datei-/Lake-/URL-Engine, deren Schema ClickHouse ableitet (`clickhouse_scan`, z. B. HDFS/URL). SQLite (DATABASE-Engine, Datei, kein Server) und Hudi (Lakehouse, Zero-Copy) werden OOTB ausgeliefert.

Ein unbekannter `kind`-Wert scheitert beim Start lautstark, sodass ein Tippfehler in einem Deskriptor einen Quelltyp nie stillschweigend unerreichbar lässt.

### Probe-Gating

Verfügbarkeit wird zur Attach-Zeit gegen den Standard-Discovery-Katalog jeder Engine geprüft:

- **Postgres** — prüft `pg_extension`, dann `pg_available_extensions`.
- **DuckDB** — führt `INSTALL`/`LOAD` aus und prüft `duckdb_functions()` auf das deklarierte `probe_symbol`.
- **ClickHouse** — prüft `system.table_engines` auf die deklarierte `ch_engine`; fehlt sie im Build, schlägt es laut fehl.

Eine deklarierte Erweiterung, die nicht installierbar ist, schlägt laut fehl. Kein stiller Skip, kein Fallback. Ein Connector, dessen Probe fehlschlägt, ist für dieses Deployment schlicht nicht aktiv.

### Template-Variablen {: #template-variables }

Jeder `server_options`-Wert, `user_mapping`-Wert, `attach_template` und `scan_template` kann `{field}`-Platzhalter verwenden. Verfügbare Felder:

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}` sowie jeder Schlüssel aus `federation_hints`. DuckDB-Attach-Templates erhalten außerdem `{alias}`, den internen Katalog-Alias, den Provisa der angebundenen Datenbank gibt.

Ein Template, das ein unbekanntes Feld referenziert, schlägt zur Attach-Zeit laut fehl und macht eine Deskriptor-/Quell-Fehlpassung sichtbar, bevor defektes DDL die Engine erreicht.

### Beispiele {: #examples }

**Postgres — MongoDB über `mongo_fdw` (kein Schema-Import; Spalten pro Tabelle geliefert)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — Excel-Dateien über `read_xlsx` (Scan-Table-Function)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

Sobald einer der beiden Deskriptoren vorhanden ist, routet die Registrierung einer Quelle mit dem deklarierten `source_type` über den benutzerdefinierten Connector, vorbehaltlich einer erfolgreichen Probe. Keine weitere Konfigurationsänderung ist nötig.

---

## Warehouses als benannte Quellen {: #warehouses-as-named-sources }

Snowflake, Databricks und ClickHouse können unabhängig davon, welche Federation-Engine aktiv ist, als benannte Quellen registriert werden.

Nach der Registrierung wird das Warehouse über den DirectDriver der Quelle gelesen, der ein Replikat im Materialisierungs-Store der aktiven Engine landet, und die Abfrage läuft gegen dieses Replikat. Auf dem üblichen direkten Weg (asyncpg, aiomysql) wird die Engine ganz übersprungen. Hier führt die Engine die Abfrage weiterhin aus, aber gegen ein lokales Replikat, statt bei jeder Anfrage über die Leitung zum Warehouse zu gehen.

Reads sind Arrow-nativ, wo das Warehouse es unterstützt: Databricks verwendet Cloud Fetch, Snowflake verwendet `fetch_arrow_table`, und ClickHouse verwendet die native columnar HTTP-Schnittstelle.

Erweiterte Verbindungsparameter, die die Standardfelder `host`/`port`/`username`/`password` nicht tragen können, gehen in `federation_hints`:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

Die Registrierung als benannte Quelle ist unabhängig davon, ob dasselbe Warehouse als Federation-Engine gewählt wird. Eine Snowflake-Quelle auf einer DuckDB-Engine landet ein Replikat in DuckDB, nicht in Snowflake.

Cloud-Objekt- und Lake-Daten (parquet-, csv-, iceberg- und delta_lake-Dateien auf S3, GCS oder R2) sind ein eigener Quelltyp. Sie werden an Ort und Stelle angebunden, wenn die aktive Engine einen ATTACH-Connector für diesen Typ hat; es wird also kein Replikat gelandet, und die Engine scannt den Objektspeicher direkt. Auch die Anmeldedaten dieser Quellen gehören in `federation_hints`:

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Quellkonfigurationsfelder {: #source-configuration-fields }

Alle Quellen teilen eine gemeinsame Menge von Feldern.

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `id` | Ja | — | Eindeutiger Bezeichner; alphanumerisch mit Bindestrichen/Unterstrichen |
| `type` | Ja | — | Quelltyp (siehe obige Tabellen) |
| `host` | Nein | `""` | Hostname oder IP |
| `port` | Nein | `0` | Portnummer |
| `database` | Nein | `""` | Datenbankname |
| `username` | Nein | `""` | Benutzername |
| `password` | Nein | `""` | Passwort; verwenden Sie `${env:VAR}` oder `${secret:NAME}` statt eines Literals (siehe unten) |
| `path` | Nein | `null` | Dateipfad oder Cloud-URI für dateibasierte und Objekt-/Lake-Quellen |
| `base_url` | Nein | `null` | Basis-URL für OpenAPI-Quellen |
| `pool_min` | Nein | `1` | Minimale Connection-Pool-Größe |
| `pool_max` | Nein | `5` | Maximale Connection-Pool-Größe |
| `use_pgbouncer` | Nein | `false` | Verbindungen über PgBouncer routen |
| `pgbouncer_port` | Nein | `6432` | PgBouncer-Port |
| `cache_enabled` | Nein | `true` | API-Response-Caching aktivieren |
| `cache_ttl` | Nein | `null` | Cache-TTL in Sekunden; erbt globalen Standard, wenn null |
| `cache_catalog` | Nein | `null` | Föderierter Katalog für API-Cache; Standard ist der eigene Katalog der Quelle |
| `cache_schema` | Nein | `api_cache` | Schema innerhalb des Cache-Katalogs |
| `naming_convention` | Nein | `null` | Überschreibt die globale Namenskonvention für diese Quelle |
| `federation_hints` | Nein | `{}` | An die Federation-Engine übergebene Session-Eigenschaften und erweiterte Verbindungsparameter für Warehouse-Quellen |
| `mapping` | Nein | `{}` | Typspezifische Connector-Einstellungen für NoSQL- und SaaS-Quellen (z. B. SharePoint `auth_type`, Salesforce `auth_type`, Splunk `use_token`) |
| `allowed_domains` | Nein | `[]` | Quelle auf bestimmte Domänen beschränken; leer = uneingeschränkt |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

---

## Kafka-Quellen {: #kafka-sources }

Kafka-Topics werden separat unter `kafka_sources` konfiguriert, verschlüsselt nach der Quell-`id` einer registrierten `kafka`-Quelle.

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Feld | Beschreibung |
| ------- | ------------- |
| `id` | Muss der `id` einer Quelle mit `type: kafka` entsprechen |
| `topics[].id` | Logischer Name für dieses Topic innerhalb von Provisa |
| `topics[].topic` | Kafka-Topic-Name |
| `topics[].domain_id` | Domäne, zu der dieses Topic gehört |
| `topics[].description` | Menschenlesbare Beschreibung |
| `topics[].default_window` | Standard-Zeitfenster für Windowed-Abfragen (z. B. `1h`) |
| `topics[].columns` | Spaltendefinitionen für das Topic-Schema |

---

## Spaltensichtbarkeit {: #column-visibility }

Das Feld `visible_to` auf jeder Spalte ist eine Liste von Rollen-IDs, die diese Spalte sehen können.

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Spalten, die aus der `visible_to`-Liste einer Rolle ausgelassen sind, erscheinen nicht im GraphQL-Schema dieser Rolle und können nicht abgefragt oder in Filtern referenziert werden.

---

## Beziehungen {: #relationships }

Beziehungen verbinden zwei registrierte Tabellen und erscheinen als verschachtelte Felder in GraphQL.

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `id` | Ja | Eindeutiger Bezeichner für diese Beziehung |
| `source_table_id` | Ja | Tabelle, die den Fremdschlüssel trägt |
| `target_table_id` | Ja | Referenzierte Tabelle; leer bei berechneten Beziehungen |
| `source_column` | Ja | Spalte auf der Quelltabelle |
| `target_column` | Ja | Spalte auf der Zieltabelle; leer bei berechneten Beziehungen |
| `cardinality` | Ja | `many-to-one` oder `one-to-many` |
| `materialize` | Nein | Erstellt automatisch eine materialisierte Sicht für quellenübergreifende Joins. Bei einer junction-gestützten Kante deckt die Sicht die Zwei-Hop-Traversierung ab, nicht einen direkten Join |
| `refresh_interval` | Nein | MV-Refresh-Intervall in Sekunden (Standard: 300) |
| `target_function_name` | Nein | DB-Funktionsname für berechnete Beziehungen |
| `function_arg` | Nein | Welches Funktionsargument den Wert der Quellspalte erhält |
| `alias` | Nein | Menschenlesbarer Beziehungstyp (z. B. `WORKS_FOR`) |
| `graphql_alias` | Nein | Benennt das SDL-Feld, das diese Beziehung auf dem übergeordneten Typ offenlegt. Fehlt es, wird der Name aus dem `field_name` der Zieltabelle und der Beziehungskardinalität abgeleitet. |
| `disable_cypher` | Nein | Bei `true` wird diese Beziehung von den Cypher-Graphkanten ausgeschlossen |
| `source_json_key` | Nein | Extrahiert diesen Key aus der Quellspalte als JSON-Objekt vor dem JOIN |
| `via_table` | Nein | Registrierter Tabellenname der Junction, die diese Kante durchläuft. Das Setzen macht die Kante junction-gestützt; leer lassen macht sie zu einer Fremdschlüsselkante |
| `via_source_column` | Nein | Junction-Spalte, gepaart mit `source_column`. Kommagetrennt und positionsbezogen bei einem zusammengesetzten Schlüssel |
| `via_target_column` | Nein | Junction-Spalte, gepaart mit `target_column` |
| `via_type_column` | Nein | Diskriminator-Spalte, wenn eine Junction mehrere Beziehungstypen trägt |
| `via_type_value` | Nein | Der Diskriminatorwert, auf den diese Kante festgelegt ist |
| `via_label_source` | Nein | Welche Nominierung den Cypher-Typ benennt: `column` (der Diskriminatorwert), `table` (der Tabellenname der Junction) oder `fixed` (der deklarierte Alias). Alle werden in Upper-Snake-Case umgewandelt |

### Junction-gestützte Beziehungen {: #junction-backed-relationships }

Sie können eine assoziative Tabelle als eigenständige Cypher-Beziehung deklarieren statt als Knoten. Ihre Spalten werden dann zu den Attributen der Beziehung:

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

Die Junction ist eine registrierte Tabelle wie jede andere, und sie muss registriert sein, bevor eine Beziehung sie nennen kann. Deklarieren Sie eine Beziehung pro Diskriminatorwert. Drei Zeilen über `pet_companions` ergeben `BONDED_PAIR`, `LITTERMATE` und `SHARES_ENCLOSURE` als drei getrennte Cypher-Typen, jeder mit den übrigen Spalten der Junction-Zeile als Kanteneigenschaften. Die mitgelieferte Demo-Konfiguration deklariert genau das.

Eine Junction-Kante gibt es nur in Cypher. Der GraphQL-Join-Emitter baut seine `ON`-Klausel für ein einziges Spaltenpaar und hat keinen Platz für den zweiten Hop, daher fehlen Junction-Kanten in der generierten SDL und in `pg_constraint`. Die Junction-Tabelle selbst bleibt als eigenes Root-Feld abfragbar. Aus der Knotenseite des Cypher-Graph-Schemas wird sie entfernt, sodass sie nie als Knotenbezeichnung erscheint.

`materialize: true` funktioniert auch auf einer Junction-Kante. Materialisiert wird die ganze Traversierung: Die Sicht hält den Quell-Hop, den Junction-Hop, den Diskriminator und die eigenen Spalten der Junction neben denen des Ziels. Die Junction ist ein drittes Bein des Joins, also wird über alle drei Tabellen beurteilt, ob die Kante quellenübergreifend ist. Eine Junction, die in einer anderen Quelle liegt als die beiden Tabellen, die sie verbindet, wird materialisiert, auch wenn diese beiden in derselben Quelle liegen. Jede Deklaration materialisiert einen Kantentyp, sodass eine für `bonded pair` gebaute Sicht nie eine `littermate`-Traversierung beantwortet.

Kardinalitätswerte:

- `many-to-one`: Jede Quellzeile gehört zu einer Zielzeile (FK zu PK)
- `one-to-many`: Jede Quellzeile gehört zu mehreren Zielzeilen (die Umkehrung)

---

## Sicherheit-auf-Zeilenebene-Regeln {: #row-level-security-rules }

RLS-Regeln injizieren `WHERE`-Klauseln zur Abfragezeit, bezogen auf eine Rolle und optional auf eine Tabelle oder Domäne.

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

Existieren sowohl eine Regel auf Domänenebene als auch auf Tabellenebene für dieselbe Rolle, hat die Regel auf Tabellenebene Vorrang.

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `table_id` | Bedingt | Tabelle, auf die die Regel angewendet wird; schließt sich mit `domain_id` gegenseitig aus |
| `domain_id` | Bedingt | Domäne, auf die die Regel angewendet wird; gilt für alle Tabellen in der Domäne |
| `role_id` | Ja | Rolle, für die diese Regel gilt |
| `filter` | Ja | SQL-Prädikat, injiziert in `WHERE`; kann Session-Variablen referenzieren |
| `description` | Ja | Wofür die Regel da ist, so wie eine Zugriffsprüfung sie liest |

---

## Funktionen und Webhooks {: #functions-and-webhooks }

### DB-Funktionen {: #db-functions }

Tracken Sie eine Datenbankfunktion und legen Sie sie als GraphQL-Abfrage oder -Mutation offen.

Datenbankquellen können ihre gespeicherten Prozeduren und Funktionen auch automatisch aus dem Vendor-Katalog erkennen (`pg_proc`, `information_schema.routines`, oder Vendor-Äquivalente), wodurch die manuelle Registrierung jeder einzelnen entfällt. Discovery liest `prokind` und `provolatile`: unveränderliche/stabile Funktionen registrieren sich als parametrisierte Relationen (Prozedurargumente werden zu Abfrageparametern, dieselbe Form wie OpenAPI-GET-Tabellen), und volatile Prozeduren registrieren sich als Mutations/getrackte Funktionen. Erkannte Routinen durchlaufen die Stage-2-Governance identisch zu handregistrierten.

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `name` | Ja | — | GraphQL-Feldname |
| `source_id` | Ja | — | Quelle, die die Funktion enthält |
| `schema` | Nein | `public` | Datenbankschema |
| `function_name` | Ja | — | Tatsächlicher Datenbankfunktionsname |
| `returns` | Ja | — | Registrierte Tabellen-ID, die die Funktion zurückgibt |
| `arguments` | Nein | `[]` | Liste von `{name, type}`-Argumentdefinitionen |
| `visible_to` | Nein | `[]` | Rollen, denen die Funktion zugewiesen ist (leer: jede Rolle). Eine `mutation` braucht zusätzlich das Recht `write` |
| `domain_id` | Nein | `""` | Domäne, zu der diese Funktion gehört |
| `description` | Nein | `null` | GraphQL-Feldbeschreibung |
| `kind` | Nein | `mutation` | `"query"` oder `"mutation"` |

### Webhooks

Legen Sie einen externen HTTP-Endpunkt als GraphQL-Abfrage oder -Mutation offen.

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `name` | Ja | — | GraphQL-Feldname |
| `url` | Ja | — | Webhook-Endpunkt-URL |
| `method` | Nein | `POST` | HTTP-Methode |
| `timeout_ms` | Nein | `5000` | Anfrage-Timeout in Millisekunden |
| `returns` | Nein | `null` | Registrierte Tabellen-ID, oder null für Inline-Typ |
| `inline_return_type` | Nein | `[]` | Liste von `{name, type}`-Feldern für benutzerdefinierte Rückgabeformen |
| `arguments` | Nein | `[]` | Liste von `{name, type}`-Argumentdefinitionen |
| `visible_to` | Nein | `[]` | Rollen, die diesen Webhook aufrufen können |
| `domain_id` | Nein | `""` | Domäne, zu der dieser Webhook gehört |
| `description` | Nein | `null` | GraphQL-Feldbeschreibung |
| `kind` | Nein | `mutation` | `"query"` oder `"mutation"` |

---

## Authentifizierung {: #authentication }

Auth wird unter dem Key `auth` konfiguriert.

| Provider | Beschreibung |
| ---------- | ------------- |
| `none` | Keine Authentifizierung; alle Anfragen werden als `default_role` behandelt |
| `firebase` | Firebase Authentication; erfordert `project_id` und `service_account_key` |
| `keycloak` | Keycloak OIDC |
| `oauth` | Generisches OAuth 2.0 |
| `simple` | Benutzername/Passwort ohne externen Provider |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` liest Rollenzuweisungen aus JWT-Claims. `assignments_source: provisa` liest sie aus Provisas eigenem Zuweisungs-Store.

---

## Ausführungs-Routing {: #execution-routing }

**Direkte Ausführung.** Eine Abfrage, die nur eine relationale Quelle betrifft, geht direkt an deren eigenen Treiber, in unter 100 ms. Ein Quelltyp nimmt diesen Weg, wenn Provisa sowohl seinen SQL-Dialekt als auch seinen Connector kennt.

**Föderierte Ausführung.** Eine quellenübergreifende Abfrage oder eine Abfrage an eine Quelle ohne direkten Treiber läuft über die Federation-Engine. Provisa bringt eine eingebettete mit; für große Deployments lassen Sie sie auf Ihren eigenen kompatiblen Cluster zeigen.

**Statistiken.** Wenn eine Tabelle registriert wird, führt Provisa `ANALYZE` für jede veröffentlichte Tabelle aus, damit der kostenbasierte Optimierer Zeilenzahlen, Null-Anteile, eindeutige Werte und Min/Max hat. Schlägt das fehl, wird der Fehler protokolliert und die Registrierung läuft weiter.

---

## Graph- & Semantic-Quellen {: #graph-semantic-sources }

### Neo4j

Registrieren Sie eine Neo4j-Graphdatenbank als abfragbare Quelle. Stewards verfassen Cypher-Abfragen, die skalare Werte projizieren; Provisa cacht Ergebnisse und legt sie als GraphQL-Typen offen.

Cypher-Abfragen müssen in der `RETURN`-Klausel Eigenschaftszugriffe verwenden (`RETURN n.id AS id, n.name AS name`). Die Registrierung lehnt eine Abfrage ab, die Knotenobjekte zurückgibt.

#### Konfigurationsdatei-Registrierung {: #config-file-registration }

Deklarieren Sie eine `neo4j`-Quelle und ihre Tabellen in YAML. Jede Tabelle benötigt ein `query_template` (das Cypher, das ihre Zeilen erzeugt) und typisierte Spalten. Der Key `query_template` ist unter jedem anderen Quelltyp ungültig.

Eine Quelle benötigt `host`, `port` und `database`. Zeilen werden abgerufen, indem `{"statements": [{"statement": <cypher>}]}` an `/db/<database>/tx/commit` gePOSTet wird (die Neo4j-HTTP-Transaktions-API). Eine Antwort mit nicht-leerer `errors`-Liste wird als fehlgeschlagene Abfrage behandelt, nicht als leeres Ergebnis.

Jede Spalte benötigt `data_type`. Der Loader bildet Konfigurationstypen auf den zur Abfragezeit verwendeten API-Spaltentyp ab:

| Konfigurations-`data_type` | API-Typ |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` und `decimal(10,2)` werden akzeptiert, und der Basistyp vor der Klammer wird verwendet.

Die Registrierung persistiert eine `api_sources`-Zeile und eine `api_endpoints`-Zeile pro Tabelle, sodass die Tabellen einen Neustart überstehen, ohne die Datei erneut zu lesen. Die Admin-REST-Endpunkte unter `/admin/sources/neo4j` schreiben dieselben Zeilen.

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table in der UI {: #register-table-in-the-ui }

Eine neo4j-Quelle hat keine Tabellen zum Auflisten, daher fragt das Register-Table-Formular nach der Tabelle, statt eine anzubieten.

1. Wählen Sie die neo4j-Quelle und eine Domäne. Die Schema- und Tabellenauswahl, die Discover-Checkbox und die Watermark-Auswahl erscheinen nicht; die Quelle wird nie introspiziert.
2. Geben Sie einen Tabellennamen und das Cypher ein. Das Cypher muss Skalare projizieren (`RETURN a.name AS name`); eine Projektion, die einen Knoten oder eine Liste zurückgibt, wird als Fehler gemeldet.
3. Drücken Sie Preview. Das Formular führt das Cypher mit `LIMIT 5` über die `neo4jPreview`-GraphQL-Abfrage aus und befüllt die Spaltenliste aus den zurückkommenden Zeilen, typisiert als `text`, `integer`, `double`, `boolean` oder `json`. Eine fehlgeschlagene Vorschau lässt das Cypher im Editor und zeigt die Meldung.
4. Passen Sie Sichtbarkeit, Aliase oder Maskierung wie bei jeder Tabelle an und registrieren Sie dann. Das Formular verweigert das Absenden, bis eine Vorschau die Spalten typisiert hat, und der Server lehnt eine neo4j-Tabelle ohne Cypher ab (`schema.neo4j_query_required`).

Das Cypher wird mit der Tabelle als `queryTemplate` gespeichert, erscheint in der Lese-Ansicht der Tabelle und persistiert genau wie eine Konfigurationsdatei-Registrierung: eine `api_sources`-Zeile und eine `api_endpoints`-Zeile, die der nächste Start hydriert. Das Bearbeiten der Tabelle persistiert ein bearbeitetes Cypher erneut.

#### Admin-REST-Registrierung {: #admin-rest-registration }

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

Der Preview-Endpunkt (`POST /admin/sources/neo4j/{id}/preview`) gibt Beispielzeilen zurück und blockiert die Registrierung, wenn das Cypher Knotenobjekte zurückgibt.

### SPARQL

Registrieren Sie jeden SPARQL-1.1-konformen Triple-Store (Apache Jena Fuseki, Virtuoso, Stardog usw.) als abfragbare Quelle.

Abfragen müssen `SELECT`-Abfragen sein. Variablennamen in der `SELECT`-Klausel werden automatisch zu Spaltennamen.

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Beide Connectors verwenden die API-Source-Cache-Pipeline. Ergebnisse werden in PostgreSQL mit konfigurierbarer TTL gespeichert, wodurch sie für quellenübergreifende föderierte JOINs verfügbar werden.

---

#### Konfigurationsdatei- und UI-Registrierung {: #config-file-and-ui-registration }

Der `host` einer `sparql`-Quelle ist ihre SPARQL-Endpunkt-URL (das Sources-Formular speichert es auf dieselbe Weise). Jede Tabelle darunter trägt `query_template`, ein SELECT, dessen Variablen die Spalten sind; jede Bindung ist `text`.

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table funktioniert genauso wie bei Neo4j: Wählen Sie die Quelle, geben Sie einen Tabellennamen und das SELECT ein, drücken Sie Preview (die `sparqlPreview`-Abfrage führt es mit `LIMIT 5` aus und befüllt die Spaltenliste), dann registrieren. Die Registrierung persistiert eine `api_sources`-Zeile und eine `api_endpoints`-Zeile (formularkodiertes POST an den Endpunktpfad, `sparql_bindings`-Normalisierer), dieselben Zeilen, die eine Konfigurationsdatei-Registrierung schreibt, und die native Engine landet die Zeilen über dieselbe Fetch-Kette wie Neo4j.

## Verbindungsbeispiele {: #connection-examples }

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Quellenübergreifende Abfrage {: #cross-source-query }

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Einzelquellen-Anteile routen direkt. Quellenübergreifende JOINs föderieren mit automatischer Typkonvertierung.
