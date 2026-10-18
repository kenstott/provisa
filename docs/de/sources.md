# Quelltypen {: #source-types }

## Ausführungsmodell {: #execution-model }

Jede Abfrage wird letztlich über die Federation-Engine ausgeführt, die Federation über alle Quellen hinweg bereitstellt. Quellen fallen basierend auf ihrer Konnektivität in drei Kategorien. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Kategorie | Hat direkten Treiber | Hat föderierten Connector | Beispiele |
| --- | --- | --- | --- |
| **Direktfähig** | Ja | Ja | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Nur Federation** | Nein | Ja | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-gestützt) |
| **Direktlesen (Replikat)** | Ja | Ja | Snowflake, Databricks, ClickHouse — Treiber liest Daten und landet ein Replikat; Abfragen laufen gegen das Replikat in der aktiven Engine |
| **Direkt (kein quellenübergreifender Connector)** | Ja | Nein | Athena — Einzelquellen-Anweisungen laufen vollständig in Athena; quellenübergreifende Joins landen ein vollständiges Replikat |
| **Materialisieren → Federation** | Nein | Nein | REST/OpenAPI, Remote-GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (Push-Empfänger), GovData, SharePoint, Salesforce, ServiceNow, Cloud-Inventar, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

**Direktfähige** Quellen führen Einzelquellen-Abfragen über ihren nativen Treiber aus (unter 100 ms) und umgehen dabei die Federation-Engine (REQ-027, REQ-229). Sie behalten volle Connector-Unterstützung und nehmen an der Federation teil, wenn sie mit anderen Quellen gejoint werden (REQ-028).

**Nur-Federation**-Quellen werden immer über die Federation-Schicht abgefragt. Es existiert kein direkter Treiber (REQ-229).

**Direktlesen (Replikat)**-Quellen haben einen DirectDriver, der nativ aus dem Warehouse liest (wo verfügbar Arrow-nativ), ein Replikat in den Materialisierungs-Store der aktiven Engine landet, und Abfragen laufen dann gegen dieses Replikat. Siehe [Warehouses als benannte Quellen](#warehouses-as-named-sources).

**Direkt (kein quellenübergreifender Connector)**-Quellen haben einen DirectDriver, der Einzelquellen-Anweisungen vollständig in der Quelle ausführt — jeder Filter, jede Spalte und jedes Limit wird weitergereicht. Verbindet eine Anweisung die Quelle mit einer anderen, liest der Treiber die Daten und landet ein Replikat; der Join läuft gegen dieses Replikat in der aktiven Engine. Für diese Quellen existiert kein Trino-Connector.

**Materialize**-Quellen haben keinen föderierten Connector. Provisa ruft ihre Daten ab (beim Start oder zur Abfragezeit) und cacht sie als Parquet in S3 oder in PostgreSQL, wodurch sie für quellenübergreifende Abfragen durch die Federation-Engine erreichbar werden (REQ-309).

---

## Alle Quellen {: #all-sources }

Provisa registriert **73** Quelltypen, die das Sources-Formular anbietet. Die folgenden Tabellen decken alle 73 ab; der Index ist die Zählung. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, wobei `graphql` und `grpc` für `graphql_remote` und `grpc_remote` stehen; Kaggle, Wikipedia, GitHub, GitLab und Stripe zählen als eigenständige Quellen, obwohl sie intern über die Connectors `files`, `graphql_remote` und `openapi` registrieren]

| # | Gruppe | Quelltypen |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Cloud-Data-Warehouses](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data-Lake / offene Tabellenformate](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Code-Quellen](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Push-Empfänger](#push-receiver) | `ingest` |
| 45–46 | [Graph & Semantic](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Dateibasiert](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observability & Sonstiges](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [Enterprise-SaaS](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [API-Quellen](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Data-Quality-Checker](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 71 | [Data-Profiler](#data-profiler) | `data_profiler` |
| 72 | [Kaggle-Datasets](#kaggle-datasets) | Kaggle (über das Sources-Formular bereitgestellt; registriert als `files`-Quelle — siehe [Kaggle-Datasets](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (registriert als `files`-Quelle, die die benannten Seiten crawlt — siehe [Wikipedia](#wikipedia)) |

Referenz für jeden Quelltyp, den Provisa unterstützt. „Direkter Treiber" bedeutet, dass Einzelquellen-Abfragen nativ gegen die Quelle ausgeführt werden (unter 100 ms) (REQ-027). „Connector-Name" ist der föderierte Connector, der verwendet wird, wenn die Quelle an Mehrquellen-JOINs teilnimmt (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

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

Wire-kompatible Datenbanken verwenden den JDBC-Treiber, den nativen Async-Treiber und den Dialekt eines Basis-Wire-Protokolls wieder — CockroachDB, YugabyteDB und Greenplum reiten auf dem PostgreSQL-Wire; TiDB reitet auf dem MySQL-Wire. Sie benötigen nur Registry-Einträge, keinen neuen Connector-Code. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) und `airport` (Arrow-Flight-Server) sind registrierte Quelltypen, die vor Ort über DuckDB-Community-Erweiterungen erreicht werden, wenn DuckDB die aktive Engine ist — kein direkter Treiber, kein föderierter Connector. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA wird über einen eigenen Treiber gelesen und in den Store der aktiven Engine repliziert, ist also auf jeder Engine erreichbar. Das Formular verlangt Host, Port (Standard 39015), Benutzername, Passwort und Datenbank; bei einem Multi-Tenant-System benennt die Datenbank den Tenant. Register Table listet die Schemas auf, die der Benutzer lesen darf; die Systemschemas von HANA bleiben außen vor. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Ein HiveServer2-Endpunkt, direkt über Thrift gelesen, für ein Hive-Deployment, in dem nur dieser Endpunkt erreichbar ist. Er ist von `hive` und `hive_s3` zu unterscheiden, die die Dateien und den Metastore des Warehouse lesen und nie über HiveServer2 laufen. Zeilen werden über den Endpunkt gelesen und in den Store der aktiven Engine repliziert. Das Formular verlangt Host, Port (Standard 10000), Benutzername, Passwort und Datenbank; `federation_hints.auth_mechanism` wählt die Authentifizierung (Standard `PLAIN`, zum Beispiel `GSSAPI` für Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Cloud-Data-Warehouses

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federiert | Liest über snowflake-connector-python; landet Replikat; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federiert | Kein DirectDriver; erreicht über Federation-Engine oder BigQuery-Engine-ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federiert | Liest über databricks-sql-connector (Cloud Fetch, Arrow); landet Replikat; `http_path` erforderlich in `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federiert | — |
| `athena` | AthenaDriver | — | athena | Partiell (INSERT immer; UPDATE/DELETE/MERGE nur für Iceberg) | Serverless-S3-Abfrage; Einzelquellen-Anweisungen laufen vollständig in Athena; quellenübergreifende Abfragen landen ein Replikat; `region` in `federation_hints` erforderlich (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federiert | Microsoft Fabric Warehouse; T-SQL über TDS, Azure-AD-Auth; landet Replikat (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federiert | Azure Synapse SQL; T-SQL über TDS, Azure-AD-Auth; landet Replikat (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federiert | Entfernter Trino-/Presto-Koordinator, gelesen über den SQLAlchemy-Trino-Dialekt; landet Replikat auf jeder Engine (REQ-994) |

#### `athena`

Amazon Athena ist ein serverloser Abfragedienst für Daten in S3, abgerechnet pro gelesenes Byte. Eine Anweisung ausschließlich auf Athena-Tabellen wird direkt in Athena ausgeführt: Filter, Spaltenlisten und `LIMIT` werden vollständig übergeben, sodass nur die Bytes gescannt und abgerechnet werden, die Athena zur Beantwortung benötigt. Eine Anweisung, die eine Athena-Tabelle mit einer anderen Quelle verbindet, landet zuerst ein vollständiges Replikat aus Athena — die gesamte Tabelle wird gelesen und Athena rechnet sie ab —, dann läuft der Join gegen dieses Replikat. Die Replikations- und Lastschutzeinstellungen der Quelle regeln, wie oft eine Landung stattfindet; der Docs-Hinweis ist die einzige Regel. (REQ-1981)

**Discovery.** Register Table ruft die Metadata-API von Athena auf — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Diese Aufrufe führen keine Abfrage aus, sodass das Auflisten von Datenbanken und Tabellen sowie das Lesen der Spalten einer Tabelle keine Bytes kostet. Die Spalten einer Tabelle werden erst gelesen, wenn die Tabelle ausgewählt wird. Partitionsschlüssel folgen den regulären Spalten. Typen werden über den Eintrag `athena` in `provisa/core/ir_types.py` zugeordnet: `array<…>`, `map<…>` und `struct<…>` werden zu `text`; ein unbekannter Typ wird unter Nennung seines Namens abgelehnt. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns`; `provisa/core/ir_types.py` Eintrag `"athena"`]

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

Wenn sowohl `username` als auch `password` leer sind, wird die Credential-Chain von boto3 verwendet: Instanzrolle, Umgebungsvariablen (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) oder ein benanntes Profil. Eines ohne das andere wird abgelehnt. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`; `provisa/federation/athena.py` `athena_client`]

**Schreiboperationen.** Jede registrierte Tabelle akzeptiert `INSERT`. `UPDATE`, `DELETE` und `MERGE` sind nur für Iceberg-Tabellen verfügbar; das Format wird bei der Registrierung gespeichert (`registered_tables.table_format`). Eine Schreiboperation auf einer Hive-Tabelle, die `UPDATE`, `DELETE` oder `MERGE` anfordert, wird unter Nennung der Tabelle und der Operation abgelehnt. Athena gibt keine geschriebenen Zeilen zurück — eine `RETURNING`-Klausel wird abgelehnt, und GraphQL bietet für diese Tabellen keine Mutation-Felder; `INSERT`, `UPDATE`, `DELETE` und `COPY` sind die Schreibpfade. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` und `TRUNCATE` werden unter Nennung der Form abgelehnt. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops`; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

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

`motherduck` (REQ-1998) ist eine bei MotherDuck gehostete DuckDB-Datenbank. Auf der DuckDB-Engine wird sie direkt angehängt; auf der Postgres-Engine bleibt sie der Connector über den Foreign Data Wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Verbindung.** `database` benennt die MotherDuck-Datenbank oder eine Freigabe über ihre Freigabeadresse. `password` ist das MotherDuck-Token und wird wie jedes Quellen-Secret aufgelöst. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

Die DuckDB-Engine setzt `ATTACH 'md:<database>'` ab; bei einer Freigabe ist `database` die Freigabeadresse (`_share/...`), das Anhängen lautet also `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Token-Behandlung.** Die Engine meldet sich mit `SET GLOBAL motherduck_token = ?` an, wobei das Token als gebundener Wert übergeben wird. Es steht im Text keiner Anweisung, daher enthält es kein Log, kein Trace und kein Fehler. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Kein Token.** Eine Quelle ohne `password` wird unter Nennung des Grundes abgelehnt, bevor etwas angehängt wird (`a MotherDuck source signs in with its token (the password field); none is set`). Ohne diese Prüfung würde die Erweiterung eine interaktive Anmeldung starten und etwa eine Minute warten. Eine Quelle ohne `database` wird ebenfalls abgelehnt. [tool-verified: `details`]
- **Ein Konto pro Engine.** Eine Engine meldet sich bei einem MotherDuck-Konto an. Eine zweite MotherDuck-Quelle mit einem anderen Token wird unter Nennung des Grundes abgelehnt: "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Schreiboperationen** laufen über die Engine, wie auf der Postgres-Engine. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**Hybride (duale) Ausführung** braucht keine Einstellung. Auf der DuckDB-Engine ist jede Quelle an einer Verbindung angehängt, daher läuft eine Anweisung, die eine Tabelle einer DuckDB-Dateiquelle mit einer MotherDuck-Tabelle verknüpft, als eine DuckDB-Anweisung, und MotherDuck entscheidet, wo jeder Teil läuft. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Datenbewegung.** Bei dualer Ausführung kann MotherDuck lokale Zeilen für die Teile, die es ausführt, auf seine Seite verschieben. Eine solche Anweisung kann daher Daten aus der lokalen Datei in das MotherDuck-Konto der Organisation senden. Verknüpfen Sie eine Datei mit Daten, die auf dem Knoten bleiben müssen, nicht mit einer MotherDuck-Quelle, es sei denn, das Senden an dieses Konto ist akzeptabel. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Eine MotherDuck-Quelle wird nur von der DuckDB- und der Postgres-Engine bedient. Keine andere Engine hat einen Connector dafür, und nichts landet ihre Tabellen dort als Replikate. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federiert | Liest über clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` für TLS (REQ-986) |
| `druid` | — | druid | druid | Nein | — |
| `exasol` | — | exasol | exasol | Nein | — |
| `elasticsearch` | HTTP (native Engines) | elasticsearch (Trino) | — | Nein | Auf Trino liest der Connector sie, seine Eigenschaften aus der Mapping-DSL des Typs [tool-verified: `trino_connectors.py:309`]; auf jeder anderen Engine liest Provisa den Index über HTTP (Indizes und Mapping für Register Table, ein Scroll-Read zum Landen) und landet die Zeilen [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | Nein | Trino-`pinot`-Connector; `pinot.controller-urls` = Host:Port des Pinot-Controllers [tool-verified: `trino_connectors.py:199`] |

### Data-Lake / offene Tabellenformate {: #data-lake-open-table-formats }

Diese Quelltypen sind nur-Federation — kein direkter Treiber, kein Dialekt. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Quelltyp | Connector-Name | Time Travel | Hinweise |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Ja (`as_of`-Argument, REQ-372) | — |
| `delta_lake` | delta_lake | Ja (`as_of`-Argument, REQ-372) | — |
| `hive` | hive | Nein | — |
| `hudi` | — (ClickHouse-`Hudi`-Engine, Zero-Copy — REQ-1178) | Nein | Kein föderierter Connector; wird vor Ort erreicht, wenn ClickHouse die aktive Engine ist |
| `hive_s3` | hive | Nein | S3-gestütztes Hive |

### NoSQL

`mongodb`, `cassandra` und `redis` haben Trino-Connectors (`redis` baut seine Eigenschaften aus der Mapping-DSL des Typs). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Quelltyp | Connector-Name | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Nein |
| `cassandra` | cassandra (Trino); CQL-Read über cassandra-driver auf jeder anderen Engine | Nein | Keyspaces sind Schemas; Register Table listet die Tabellen eines Keyspace und typisiert Spalten aus den Schema-Metadaten des Clusters (Partition Keys als Primärschlüssel); das `cassandra`-Extra installiert den Treiber [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino); HTTP-freier redis-py-Read auf jeder anderen Engine | Nein | Ein Key-Präfix `<table>:*` ist eine Tabelle, und ein Hash ist eine Zeile; Register Table listet die vorhandenen Präfixe und typisiert die Spalten eines Präfixes aus dessen Hashes (ein `mapping.tables`-Eintrag überschreibt Muster, Key-Spalte, Werttyp und Spalten) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

**Redis auf den gehosteten Engines.** Redis ist nicht verfügbar, wenn die Engine der Organisation eine der bereitgestellten Engines der gehosteten Plattform ist. Trino liest die Tabellen eines Redis-Katalogs aus Beschreibungsdateien in einem eigenen Verzeichnis des Katalogs, und einer gehosteten Engine kann im laufenden Betrieb keines gegeben werden. Das Speichern oder Anwenden einer Redis-Quelle wird dort mit Namen abgelehnt (HTTP 422, `sources.redis_needs_engine_files`); ein selbst gehostetes Deployment liest Redis wie oben beschrieben. [tool-verified: `provisa/core/repositories/source.py` `refuse_on_engine_shard`] (REQ-2073)

### Qdrant

`qdrant` (REQ-1983, REQ-2000) wird vom eigenen Client von Qdrant gelesen und geschrieben, `qdrant-client`. Qdrant ist **nur live**: eine Eigenschaft des Quelltyps, keine Einstellung. Antworten gibt es mit seinem Vektorindex und seiner Query API, und kein Replikat kann dasselbe beantworten; seine Tabellen werden daher nie repliziert. Für eine Qdrant-Quelle oder ihre Tabellen gibt es keine Replicate-Auswahl; ein `replicate`-Wert oder `row_materialize` darauf wird abgelehnt. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Verbindung.** `host` ist die URL der Instanz mit Port (`https://<cluster>.cloud.qdrant.io:6333` oder ein selbst gehostetes `http://qdrant:6333`); `password` ist ihr API-Schlüssel, eine Secret-Referenz, die beim Lesen der Quelle aufgelöst wird, leer bei einer Instanz ohne Schlüssel. `federation_hints.prefer_grpc: true` liest über den gRPC-Port von Qdrant statt über REST. Die Quelle hat ein Schema, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`; `provisa/qdrant/introspect.py` `SCHEMA`]

**Collection-Tabellen.** Eine Collection ist eine Tabelle. Register Table bietet die Spalten der Collection an: `id` (die Point-ID, ihr Schlüssel, Pfad `@id`), die Felder des Payloads (ein indiziertes Feld nach seinem Index typisiert, jedes andere so, wie die Stichproben-Points es nahelegen; verschachtelte Felder mit ihrem Pfad abgeflacht) und eine `json`-Spalte je Vektor (Pfad `@vector`, bei einem benannten Vektor `@vector.<name>`), nicht ausgewählt angeboten. Eine Vektorspalte nennt das registrierte Embedding-Modell, dessen Embeddings sie enthält, gewählt aus den Modellen, deren Dimensionen der Größe des Vektors entsprechen (in Register Table und im Tab „Typ und Schlüssel“ des Editors); mit der Wahl eines Modells wird der Vektor registriert. [tool-verified: `provisa/qdrant/introspect.py` `columns`; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Anweisungen auf einer Qdrant-Quelle.** Eine Anweisung, die nur eine Collection liest, läuft in Qdrant, wenn die API von Qdrant sie exakt ausdrücken kann:

- Payload-Bedingungen werden zu einem Qdrant-Filter (`=`, `<>`, `IN`, Bereiche auf Zahlen und Datumswerten, `IS NULL`, `AND`, `OR`, `NOT`); die NULL-Semantik von SQL bleibt erhalten, sodass `lang <> 'en'` Points ohne `lang` überspringt;
- eine Bedingung allein auf `id` (`=` oder `IN`) ist ein `retrieve`; jedes andere Lesen ist ein gefilterter `scroll`;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` ist ein `query_points` über einen Vektor mit der Distanz Cosine, der Filter wird vor dem Top-k angewendet. Ein Text als Abfrage wird vom Modell des Vektors eingebettet. Die Antwort trägt eine Warnung, dass das Top-k approximativ ist und über den Index von Qdrant gewählt wird.

Alles andere (ein Aggregat, ein `GROUP BY`, eine berechnete oder maskierte Spalte, eine andere Sortierung als „ähnlichste zuerst“) beantwortet die Engine, wie unten beschrieben. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Anweisungen, die die Engine beantwortet.** Verbindet eine Anweisung eine Collection mit einer anderen Quelle oder rechnet sie über deren Spalten, wird der Teil der Collection für diese Anweisung live aus Qdrant gelesen und der Engine als Zeilen übergeben. Der Teil besteht aus den Spalten der Collection, den Bedingungen der Anweisung allein auf der Collection (darunter ihre Zeilenregeln) und, wenn die Collection in ihrem `SELECT` allein gelesen wird, der Ähnlichkeitssortierung und dem `LIMIT` dieses `SELECT`. Ein Teil, den Qdrant nicht exakt ausdrücken kann, wird abgelehnt, unter Nennung von Tabelle und Grund. Ein Teil ohne Bedingung, ohne Schlüssel und ohne Top-k würde die ganze Collection lesen und wird ebenfalls abgelehnt. Eine Bedingung auf einen Parameter (`$1`) bleibt der Engine überlassen. [tool-verified: `provisa/federation/live_only.py` `cut`, `read`; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Schreibzugriffe.** `INSERT`, `UPDATE` und `DELETE` auf eine Collection laufen in Qdrant:

- `INSERT … VALUES` nennt die Spalte `id` und schreibt neue Points. Eine `id`, die die Collection bereits enthält, wird unter Nennung abgelehnt. Die Prüfung liest zuerst die IDs und ist daher nicht atomar mit dem Schreiben.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` ist ein Upsert. Spalten, die die Anweisung nicht setzt, behalten ihre Werte.
- `UPDATE` und `DELETE` wirken auf die Points, die ihr `WHERE` auswählt, als Qdrant-Filter; eine Bedingung ohne Filterform wird abgelehnt.
- Ein in eine Vektorspalte geschriebener Text wird vom Modell der Spalte eingebettet. Ein dorthin geschriebener Vektor muss die Dimensionen des Modells haben.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Suchtabellen.** Die Vektorsuche ist eine Suchtabelle: Ihr `query_template` enthält eine Query-API-Anfrage.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` ist immer ein Argument; `using`, `filter`, `limit`, `score_threshold` und `model` sind optional. Jeder Wert in `using`, `filter`, `limit` oder `score_threshold`, der genau `"@name"` lautet, ist ebenfalls ein Argument.
- Jedes Argument ist ein Parameter der Tabelle (`query_arg`, REQ-1986), streng in seinen deklarierten Typ umgewandelt und als JSON-Wert in die Anfrage eingesetzt, nie in deren Text. Das Argument von `query` ist `array(double)` und erforderlich.
- Ein Aufrufer übergibt Argumente wie jeden Parameter: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` in SQL, Feldargumente in GraphQL.
- Ein nicht übergebenes optionales Argument filtert nichts: Seine Bedingung entfällt im Filter. Ohne `limit` liefert eine Suche den Standard von Qdrant, 10 Points.
- Der Filter wird von Qdrant angewendet, bevor die nächsten Points gewählt werden, ist also exakt. Jede andere Bedingung der Anweisung schränkt die Points, die die Suche geliefert hat, erst danach ein; sie holt keine weiteren.
- `score` ist die eigene Metrik der Collection für den durchsuchten Vektor: bei einer Cosine-Collection die Kosinus-Ähnlichkeit. Register Table listet die Vektoren der Collection mit Größe und Distanz.
- `model` nennt ein registriertes Embedding-Modell (`vector_models`, Admin → AI Models). Ein für die Abfrage übergebener Text wird dann von diesem Modell eingebettet, und ein übergebener Vektor muss die Dimensionen dieses Modells haben (REQ-429, REQ-430). Ohne `model` muss die Abfrage ein Vektor sein.

Eine Suche wird pro Anweisung unter ihren Argumenten gelesen, ihre Zeilen für die TTL der Tabelle unter diesen Argumenten gehalten. Eine Suchtabelle ist schreibgeschützt. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read`; `provisa/api/data/materialization.py` `_is_native_read`]

### Streaming

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Föderierter Kafka-Connector; Schema über Confluent Schema Registry (Avro, Protobuf, JSON Schema), manuelle Definition oder Stichproben-Inferenz (REQ-147, REQ-150) | Nein; die Live-Ausgabe einer Tabelle veröffentlicht nach Kafka (REQ-565) |
| `websocket` | Externer WebSocket-Feed — verbinden, abonnieren, Ereignisse empfangen; Ergebnisse materialisiert (REQ-338) | Nein |
| `rss` | RSS-2.0-/Atom-Feed — pollen, Watermark nach pubDate/updated; Ergebnisse materialisiert (REQ-342, REQ-343) | Nein |

**Kafka auf den gehosteten Engines.** Auf den bereitgestellten Engines der gehosteten Plattform muss eine Kafka-Quelle eine Schema-Registry nennen (`schema_registry_url`). Eine Quelle, deren Topic-Spalten nur das Modell deklariert, wird beim Speichern oder Anwenden mit Namen abgelehnt (HTTP 422, `sources.kafka_needs_schema_registry`), da ihre Tabellenbeschreibungen ein Verzeichnis bräuchten, das der laufenden Engine nicht gegeben werden kann. SASL-Zugangsdaten erreichen diese Engines im Speicher (siehe [Sicherheit](security.md#the-federation-engine-trino)). [tool-verified: `provisa/core/repositories/kafka_source.py` `refuse_on_engine_shard`] (REQ-2073)

### Push-Empfänger {: #push-receiver }

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | Externe Dienste POSTen JSON-Ereignisse; Ergebnisse materialisiert (REQ-331, REQ-335) | Nein |

### Graph & Semantic

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher über HTTP-API, Ergebnisse in PostgreSQL gecacht (REQ-295) | Nein |
| `sparql` | SPARQL-1.1-POST, Ergebnisse in PostgreSQL gecacht (REQ-297) | Nein |

### Dateibasiert {: #file-based }

Drei Mechanismen decken Dateien ab. Einzeldatei-Quellen und der Datei-Crawler verwenden das Feld `path` statt `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) Der dritte, [Logs](#logs), benennt seine Dateien durch einen Glob in seinem `mapping` und liest nur Dateien auf dem Provisa-Server. [tool-verified: `provisa/logs/settings.py`]

**Dateidatenbank- und Einzeldatei-Quellen** — `sqlite`, `duckdb`, `csv`, `parquet` richten `path` auf eine Datei. (Eine `duckdb`-Quelle hält ihren Speicherort im Feld `database` des Formulars.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Quelltyp | Speicherorte | Mutations |
| --- | --- | --- |
| `sqlite` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` | Ja bei einer lokalen Datei; keine bei einer entfernten |
| `duckdb` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` | Ja bei einer lokalen Datei; keine bei einer entfernten |
| `csv` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` (DuckDB-Engine) | Nein |
| `parquet` | lokaler Pfad, `https://`, `s3://`, `gs://`, `az://` (DuckDB-Engine) | Nein |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Ein Pfad mit einem anderen Schema (etwa `ftp://`) wird abgelehnt: "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Zugangsdaten für einen entfernten Speicherort.** Legen Sie sie in den `federation_hints` der Quelle ab. Jede gilt nur für den Bucket oder Container des eigenen Pfads, daher werden die Zugangsdaten einer Quelle nie für den Speicherort einer anderen Quelle verwendet. Eine Quelle ohne Zugangsdaten wird als öffentlicher Speicherort gelesen. [tool-verified: `object_store_secret_ddl`]

| Speicherort | `federation_hints`-Schlüssel |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; optional `region`, `endpoint` (ein S3-kompatibler Speicher wie R2 oder MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (ein HMAC-Schlüssel) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Entfernte SQLite- und DuckDB-Dateien sind schreibgeschützt.** Die DuckDB-Engine hängt sie direkt mit `READ_ONLY` an, und eine Tabelle darauf bietet kein Insert, Update oder Delete. Auf einer Engine, die SQLite nicht anhängt, wird eine entfernte SQLite-Datei über eine temporäre DuckDB-Verbindung gelesen, die sie schreibgeschützt anhängt, und ihre Zeilen landen wie üblich. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Eine Datei hochladen.** Im Quellformular einer `sqlite`-, `duckdb`-, `csv`- oder `parquet`-Quelle laden Sie die Datei hoch, statt einen Pfad einzugeben. Die Datei wird auf die Platte gestreamt, nie im Speicher gehalten, in den Ordner der Organisation unter `files/<source id>/<file name>`, und der Pfad der Quelle wird darauf gesetzt. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Ersetzen.** Ein erneuter Upload tritt an die Stelle der früheren Datei, gleich welchen Namen sie hat, und die Quelle wird neu bereitgestellt, sodass der nächste Lesevorgang die neue Datei sieht. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Entfernen.** Das Entfernen der Quelle entfernt ihre hochgeladene Datei. Nichts anderes entfernt sie. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Wer.** Nur Rollen mit der Fähigkeit `source_registration` dürfen hochladen. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Größenlimit.** Die Einstellung `sources.max_upload_bytes` (Umgebung `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) begrenzt die Datei, standardmäßig 1 GiB (1073741824 Bytes), Minimum 1, gelesen beim Beginn eines Uploads. Eine größere Datei wird mit HTTP 413 unter Nennung des Limits abgelehnt, und nichts davon wird behalten. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Pfad.** Solange der Pfad der Upload der Quelle ist, ist das Pfadfeld des Formulars schreibgeschützt. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Namen.** Quell-ID und Dateiname dürfen nur Buchstaben, Ziffern, `.`, `_` und `-` enthalten. [tool-verified: `source_uploads._SEGMENT`]
- **Speicherung.** [inferred from the `source_uploads` docstring] Ein Upload wird wie jede Org-Datei gespeichert: im Ordner der Organisation, verschlüsselt, wenn der Ordner der Organisation verschlüsselt ist, und für jeden Knoten auf einem gemeinsam genutzten Mount sichtbar.

**Schreiboperationen auf eine lokale SQLite- oder DuckDB-Datei, und Sperren.** Eine Schreiboperation nimmt die Sperre der Datei und wartet nie darauf. Hält ein anderer Prozess oder Knoten die Datei, wird die Schreiboperation abgelehnt, unter Nennung der Quelle und der Tatsache, dass ihre Datei gesperrt ist. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** Die Schreiboperation setzt `PRAGMA busy_timeout = 0`, sodass eine gesperrte Datei sofort fehlschlägt. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** DuckDB teilt eine Datei nur zwischen schreibgeschützten Öffnungen. Daher halten das Anhängen der Engine und die eigene Verbindung der Quelle die Datei schreibgeschützt, und jeder Knoten liest sie gleichzeitig. Eine Schreiboperation pausiert die Halter dieses Prozesses, öffnet die Datei nur für diese Anweisung lesend und schreibend und schließt sie wieder. Kann das Öffnen die Sperre nicht nehmen, wird die Schreiboperation abgelehnt. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Gemeinsam genutzte Mounts.** In einem verschlüsselten Org-Ordner, dessen Mount von mehreren Knoten gemeinsam genutzt wird (Einstellung `org_files.shared_mount`), wird eine Schreiboperation auf eine SQLite- oder DuckDB-Datei darin grundsätzlich abgelehnt, unter Nennung der Quelle. Die Ordnerverschlüsselungsschicht (gocryptfs) überträgt Dateisperren nicht an die anderen Knoten, sodass nichts zwei Knoten daran hindern würde, gleichzeitig zu schreiben. Lesen funktioniert weiterhin. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Ein ohne Verschlüsselung gemeinsam genutzter Ordner muss mit Sperren gemountet werden, die jeden Knoten erreichen: ein NFS-Mount mit aktiviertem Locking, nicht `nolock` und nicht `local_lock`. Keine Prüfung kontrolliert das; es ist eine Anforderung an das Deployment. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Datei-Crawler (`files`-Quelle)** — richtet `path` auf ein Glob-Muster, durchläuft es rekursiv und registriert das Verzeichnis als föderierten Katalog von Tabellen. Sie liest viele Formate über viele Transporte; die untenstehenden Mengen stammen vom File-Connector (kenstott/calcite-Fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; Format- und Transportlisten vom Calcite-`file`-Adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formate | Transporte |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow und in Tabellen konvertierte Dokumente — HTML, Markdown, DOCX, PPTX | Lokales Dateisystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST und Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Jede `files`-Quelle kann in ihrem Mapping auch ein `crawl` deklarieren, mit denselben Einstellungen, die unter [Wikipedia](#wikipedia) aufgeführt sind: Die Quelle liest dann zuerst Webseiten und landet jede HTML-Datentabelle und jede verlinkte Datendatei in ihrem Verzeichnis als Tabelle. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

Auf der DuckDB-Engine wird `files` nativ gelesen — eine `read_csv_auto`-Scanner-View pro `<table>.csv` unter dem aufgelösten Verzeichnis (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. Auf einer Engine ohne eigenen `files`-Connector landen Zeilen über denselben connector-gebündelten Calcite-pgwire-Server (`pgwire-file`), den auch sharepoint/splunk verwenden (REQ-954) — siehe [Enterprise-SaaS-Connectors](#enterprise-saas-connectors) unten. Die End-to-End-UI-Abdeckung (Sources-Formular → Register Table → SQL-Abfrage) und der pgwire-Landepfad sind in REQ-1694 nachgewiesen.

#### Logs (REQ-1996) {: #logs }

Eine `logs`-Quelle macht Logdateien auf dem Provisa-Server zu einer Tabelle. Sie benennen die Dateien mit einem vollständigen Pfad oder einem Glob und wählen das Format, in dem die Einträge geschrieben sind. Jeder Eintrag wird zu einer Zeile. Ein Eintrag, den das Format nicht lesen kann, bleibt mit seinem Rohtext und dem Grund erhalten und wird nie verworfen. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

Das Mapping kennt vier Schlüssel und weist jeden anderen mit Namen zurück: `glob` und `format` (beide erforderlich) sowie `pattern` und `record_start`, die nur das Format `regex` annimmt. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

Im Sources-Formular erscheint eine Logs-Quelle als „Log files“ unter File. Die Schaltfläche **Preview** zeigt, noch vor dem Speichern, die Dateien, auf die der Glob passt, die Spalten, die das Format liefert, und die ersten 20 Zeilen der neuesten Datei. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**Der Glob.** Er muss ein vollständiger Pfad sein; `~` wird aufgelöst, ein relativer Pfad wird zurückgewiesen („name the files by their full path“). Er wird mit Pythons `glob` im nicht rekursiven Modus ausgewertet, daher verhält sich `**` wie `*` und passt innerhalb einer Verzeichnisebene. Nur reguläre Dateien werden übernommen, in Namensreihenfolge. Eine leere Datei wird übersprungen, bis sie Bytes enthält. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] Der Glob darf eine Umgebungsvariable nennen, etwa `${env:LOG_DIR}/*.log`. Die Quelle speichert den Verweis so, wie er geschrieben wurde, und löst ihn bei jedem Lesen der Dateien auf; eine nicht gesetzte Variable wird mit ihrem Namen abgelehnt. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

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

`_file_id`, `_line_offset` und `_entry` bilden den Schlüssel der Zeile, sodass ein zweimal gelesener Eintrag eine Zeile bleibt. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Formate und Presets.** Die Formatauswahl gruppiert die Formate nach Familien; die letzte Wahl, `regex`, ist Ihr eigenes Muster. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

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

Das sind 56 eingebaute Formate und `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Wie ein Format zu Spalten kommt.** Formate bekommen ihre Spalten auf vier Arten. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Feste Spalten.** Die meisten Formate nennen ihre Spalten und Typen. `rfc3164` liefert zum Beispiel `timestamp`, `facility`, `severity`, `host`, `process`, `pid` und `message`; `facility` und `severity` werden aus dem `<PRI>`-Wert berechnet. Eine BSD-Syslog-Zeile enthält kein Jahr, daher gilt das Jahr der letzten Schreibzeit der Datei, oder das Jahr davor, wenn der Monat der Zeile nach diesem Schreibzeitpunkt liegt.
- **Vom Eintrag benannte Felder.** `jsonl` und `logfmt` übernehmen ihre Spalten aus den Feldern der ersten Zeilen der Dateien (Einstellung `logs.preview_lines`, Standard 1000). Jede Spalte erhält den engsten Typ, in den alle gesehenen Werte passen: boolean, dann integer, float, timestamp und json, sonst text. Ein Feld, das ein späterer Eintrag trägt und für das es keine Spalte gibt, landet in einer json-Spalte `extra`.
- **Von einem Header benannte Felder.** `w3c` und die darauf aufbauenden Presets lesen die `#Fields:`-Zeile jeder Datei; `date` und `time` werden zu einer Spalte `timestamp` verbunden. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` und `zeek_tsv` lesen `#`-Headerzeilen auf dieselbe Weise, und `aws_vpc_flow` liest eine Headerzeile, die mit `version` beginnt. Wo eine Headerzeile gelesen wurde, bleibt bei der Datei, sodass eine später weitergelesene Datei ihre Felder behält.
- **`regex`.** Jede benannte Gruppe, etwa `(?P<level>…)`, ist eine Spalte vom Typ text.

Der Bildschirm Register Table listet diese Spalten mit ihren Typen auf und markiert den Schlüssel. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] Beim Lesen der Zeilen wird jeder Wert in den Typ umgewandelt, den seine Spalte in der registrierten Tabelle hat; ein dort geänderter Typ ist also der angewandte. Ein Wert, den seine Spalte nicht aufnehmen kann, wird zu NULL, und sein Grund steht in `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Ein Eintrag, der länger als `logs.max_line_bytes` (Standard 65536) ist, behält seine ersten Bytes in `_raw` und wird nicht geparst; `_parse_error` nennt seine Länge. Eine Datei, die mit einer UTF-8- oder UTF-16-Byte-Order-Mark beginnt, wird in dieser Kodierung gelesen, jede andere als UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Einträge über mehrere Zeilen.** Ein Format mit Eintragsbeginn-Muster bildet einen Eintrag aus einer Zeile, die darauf passt, plus allen folgenden Zeilen, die nicht passen. Ein Stacktrace bleibt bei seiner Exception, ein Slow-Query-Eintrag bleibt ganz. Zeilen vor dem ersten Treffer bilden einen eigenen Eintrag; das Format weist ihn meist zurück, und die Zeile bleibt erhalten. Die eingebauten Formate mit Eintragsbeginn sind `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` und `azure_monitor`. Bei `regex` setzen Sie `record_start` selbst. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Zwei Formate lesen ihre Einträge selbst statt zeilenweise. `kubernetes_cri` fügt die partiellen Zeilen (`P`), die die Runtime zerlegt hat, zu einem Eintrag zusammen, und `windows_evtx` liest die binäre `.evtx`-Datei. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Eine Datei, in die noch geschrieben wird, wird bis zu ihrer letzten vollständigen Zeile gelesen. Bei einem mehrzeiligen Format wird sie bis zum Beginn ihres letzten Eintrags gelesen, den die nächste Zeile fortsetzen könnte. Sobald die Datei 2 Sekunden ohne Schreibvorgang geblieben ist, gilt dieser letzte Eintrag als vollständig. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Rotation.** Eine Datei wird an einer Prüfsumme ihrer ersten 4096 Bytes, dekomprimiert, erkannt, nicht an ihrem Namen. Wird `system.log` zu `system.log.0` rotiert und dann zu `system.log.0.gz` komprimiert, ist jede Kopie dieselbe Datei. Das Lesen setzt bei dem Byte fort, das der letzte Lesevorgang erreicht hat, sodass keine Zeile doppelt gelesen wird. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- Kompression wird an den ersten Bytes der Datei erkannt, nie an ihrem Namen: gzip und bzip2. Eine komprimierte Datei gilt als vollständig.
- Eine rotierte Kopie und ihr komprimierter Zwilling, die beide passen, werden einmal gelesen, und zwar die komprimierte.
- Eine Datei, die kürzer ist als der bereits gelesene Offset, wurde abgeschnitten: Sie ist eine neue Datei und wird von vorn gelesen. Die Zeilen der alten bleiben.
- Dateien unter 4096 Bytes, die gleich beginnen, werden über die Inode unterschieden und bleiben daher zwei Dateien.
- Eine Datei, auf die der Glob nicht mehr passt, wird vergessen; ihre Zeilen bleiben als Historie.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Aktualisierung und Live-Zustellung.** Eine Logs-Tabelle ist ein Replikat. Nichts überwacht die Dateien. Stattdessen bildet, wenn das Änderungssignal der Quelle `probe` oder `ttl_probe` ist, ihre Probe einen Digest über Pfad, Inode, Größe und letzte Schreibzeit jeder passenden Datei; weicht der Digest ab, wird eine Aktualisierung angefordert. Die Probe-Typen, die eine Logs-Quelle annimmt, sind `hash` und `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Eine Aktualisierung liest nur, was neu ist. Jede Datei wird dort weitergelesen, wo der letzte Durchlauf stand, und die Zeilen werden nach ihrem Schlüssel geschrieben, 5000 auf einmal, bevor die neue Position gespeichert wird. Ein Durchlauf, der zwischen beiden Schritten stoppt, liest diese Einträge erneut, was dieselben Zeilen ergibt. Jeder Durchlauf stempelt seine Zeilen in `_landed_at`, der Spalte, die die Live-Zustellung als Wasserzeichen nutzt. Eine geänderte Tabellendefinition ist ein vollständiger Neuaufbau, der jede Datei von vorn liest. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Erlaubte Wurzeln.** Die Operator-Einstellung `logs.allowed_roots` listet die Verzeichnisse auf, in die ein Glob auflösen darf. Ihr Standard ist `["/var/log"]`; sie ist eine Liste auf der Karte Security unter Admin → Einstellungen, auch über `PROVISA_LOGS_ALLOWED_ROOTS` setzbar, und eine Änderung verlangt eine Bestätigung. Jede Wurzel wird nach der `~`-Auflösung und nach dem Auflösen von Symlinks ausgewertet. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Zwei Prüfungen gelten. Der feste Teil des Globs bis zum ersten Platzhalter muss innerhalb einer Wurzel auflösen; das wird beim Speichern der Quelle und bei jeder Vorschau und jedem Lesen geprüft. Jede passende Datei muss außerdem nach dem Folgen ihrer Symlinks innerhalb einer Wurzel auflösen, sodass ein Symlink aus `/var/log` heraus zurückgewiesen wird. Die Zurückweisung nennt den Pfad, das Auflösungsziel und die erlaubten Wurzeln:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Beim Speichern lautet der Code `schema.logs_path_refused`; bei der Vorschau ist es ein 403 mit `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Zurückweisung im gehosteten Betrieb (SaaS).** Ein Deployment, das viele Organisationen hält, liest keine eigene Logdatei. Jede Pfadprüfung weist zuerst zurück, beim Speichern, bei der Vorschau und beim Lesen, mit „`<path>`: a server that holds many organizations reads no log file of its own“. Die Dateien gehören dem Server, nicht einer einzelnen Organisation, daher darf kein Mandant sie lesen. Ob das Deployment viele Organisationen hält, entnimmt es seinem Multitenancy-Zustand. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Kaggle-Datasets (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle ist eine Datei-Download-Plattform. Ein bereitgestellter Kaggle-Datensatz registriert sich als Quelle vom Typ `files` und wird über denselben pgwire-file-Connector abgefragt, den jede andere `files`-Quelle verwendet — es gibt keinen `kaggle`-SourceType im Enum. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — kein `kaggle`-Literal]

**Einen Datensatz hinzufügen.** Öffnen Sie Sources → Subscriptions → Kaggle. Das Formular durchläuft zwei sequenzielle Schritte, da Kaggles eigener Dataset-Suchendpunkt Auth erfordert — die Auswahl kann nicht erscheinen, bevor das Token validiert ist. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Token** — geben Sie ein Kaggle-API-Token ein und klicken Sie auf „Validate". Die Validierung ruft `POST https://www.kaggle.com/api/v1/datasets/create/new` mit leerem Body auf. Ein `401` bedeutet ungültig; jede andere Antwort bedeutet gültig (Kaggles eigene Payload-Validierung greift, bevor irgendein Datensatz erstellt wird — nichts wird persistiert). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Auswahl** — ein Live-Search-as-you-type-Feld fragt `GET /api/v1/datasets/list` ab. Jedes Ergebnis zeigt Dataset-Titel und -Beschreibung. Wählen Sie eines aus und klicken Sie auf „Add Dataset".

Ein Klick auf „Add Dataset" lädt das Bundle von `GET /api/v1/datasets/download/{owner}/{ref}` herunter, entpackt CSV- und Parquet-Mitglieder nach `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` und erstellt **eine** Quelle vom Typ `files`, deren `path` dieses Wurzelverzeichnis ist. Das Token wird serverseitig nie gespeichert. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Nach dem Hinzufügen der Quelle registrieren Sie deren Tabellen über den normalen Register-Table-Bildschirm. Die rekursive Verzeichniserkennung des pgwire-file-Connectors listet jede Datei als eigene Tabelle — derselbe Mechanismus, den jede andere `files`-Quelle verwendet (REQ-1690). (REQ-1783)

**v1-Einschränkung.** Ein Bundle mit einer `.sqlite`- oder `.db`-Datei wird rundweg mit einer klaren Fehlermeldung abgelehnt. Nur CSV- und Parquet-Dateien werden bereitgestellt. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Tabellenbenennung.** Eine gecrawlte Tabelle heißt `<page>__<name>`: der Titel der Seite, dann die Beschriftung der Tabelle oder, wenn sie keine hat, die letzte Überschrift über der Tabelle, beides in der Spaltennamen-Schreibweise der Quelle. Zum Beispiel ist die Tabelle unter der Überschrift „Tallest buildings in the world" auf der Seite `List_of_tallest_buildings` die Tabelle `list_of_tallest_buildings__tallest_buildings_in_the_world`. Eine verlinkte Datendatei behält ihren eigenen Namen. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Aktualisieren.** Um einen Datensatz erneut abzurufen, nachdem Kaggle eine neue Version veröffentlicht hat, rufen Sie die GraphQL-Mutation `refreshKaggleSource` mit der Quell-ID und einem gültigen Token auf. Dies stellt die Dateien vor Ort erneut bereit und leert den pgwire-file-Endpunkt-Cache, sodass der Connector bei seiner nächsten Abfrage etwaige Schemaänderungen übernimmt. Die zur Erstellungszeit in `federation_hints` gespeicherten `kaggle_owner` und `kaggle_ref` identifizieren, welcher Datensatz erneut abgerufen werden soll. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Kein statischer YAML-Konfigurationspfad.** Kaggle-Quellen werden nur über das Sources-Formular erstellt. Eine nach YAML exportierte Kaggle-Quelle erscheint als `type: files` mit `kaggle_owner` und `kaggle_ref` in `federation_hints`. Ein erneuter Download von Kaggle erfordert den UI-Refresh-Flow oder die Mutation `refreshKaggleSource` — das Ausrichten des YAML-`path` auf ein vorab bereitgestelltes Verzeichnis ist die Alternative für air-gapped Umgebungen.


#### Wikipedia (REQ-1960) {: #wikipedia }

Wikipedia crawlt die Seiten, die Sie benennen. Jede Datentabelle einer Seite und jede verlinkte CSV-, TSV-, Excel-, JSON- oder Parquet-Datei wird zu einer Tabelle der Quelle. Wie Kaggle registriert es sich als `files`-Quelle mit einem `crawl` in ihrem Mapping. Das Hinzufügen registriert nichts: Ein Steward registriert die gewünschten Tabellen über den normalen Register-Table-Bildschirm. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

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

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa kennt die Überschrift „See also" dieser Ausgaben: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Bei jeder anderen Ausgabe fragt das Formular, wenn „See also" aus ist, wie die Ausgabe diesen Abschnitt nennt; geben Sie seinen Titel ein, wie eine Seite ihn zeigt (zum Beispiel `Voir aussi`). Eine Quelle, die ohne ihn gespeichert wird, wird abgelehnt (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Welche Links verfolgt werden.** Ein Link wird danach gewählt, was das Markup der Seite selbst über ihn sagt, nie nach einem Namen in seiner Adresse: Solche Namen unterscheiden sich in jeder Sprachausgabe und kommen auch in gewöhnlichen Titeln vor. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Verfolgt: Links auf andere Artikel, die im Fließtext, in Listen und in Datentabellen des Artikels stehen. Die Markup-Markierung ist `a[rel='mw:WikiLink']`.
- Nicht verfolgt: noch nicht geschriebene Seiten, die Seite selbst, Buchquellen-Nachschläge, Dateiseiten, andere Wikis und externe Sites.
- Gar nicht gelesen (entfernt, bevor Links und Tabellen gesammelt werden): Navigationskästen, Infoboxen, Anmerkungen und Belege, Kategorien, das Inhaltsverzeichnis, Bilder und Wartungshinweise.
- Nicht gelesen, solange Sie die Option nicht einschalten: der Abschnitt „See also".

Nur Tabellen, die Redakteure als Datentabellen markieren (`table.wikitable`), werden zu Tabellen, und eine Tabelle mit weniger als 2 Zeilen wird nicht angeboten. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Erweiterte Crawl-Einstellungen.** Das Formular hält diese unter „Crawl settings" eingeklappt und zeigt jede mit ihrem Standard; lassen Sie ein Feld leer, um den Standard zu behalten. In einem `files`-Mapping stehen dieselben Namen unter `crawl:`, und ein Name außerhalb dieser Liste wird abgelehnt. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

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

**Tabellenbenennung.** Eine gecrawlte Tabelle heißt `<page>__<name>`. Der Name ist die eigene `id` der Tabelle, wenn sie eine hat, sonst ihre Beschriftung, sonst die nächste Überschrift darüber. Zum Beispiel wird eine Tabelle unter der Überschrift „Tallest buildings" auf der Seite `List_of_tallest_buildings` nach der Namensnormalisierung zu `list_of_tallest_buildings__tallest_buildings`. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Aktualisieren.** Ein Crawl wird für die Cache-Dauer aufbewahrt, standardmäßig `1 days` (`html_cache_ttl`). Seiten werden erst nach Ablauf dieser Zeit erneut abgerufen. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]


### Observability & Sonstiges {: #observability-other }

`prometheus` hat einen Trino-Connector (Eigenschaften aus der Mapping-DSL des Typs gebaut). `google_sheets` ist ein registrierter Quelltyp ohne Trino-Connector und materialisiert über die API-Cache-Pipeline. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Quelltyp | Connector-Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialisiert) | Nein |
| `prometheus` | prometheus | Nein | Eine Metrik ist eine Tabelle, und ein Sample ist eine Zeile (`timestamp`, `value`, eine Spalte pro Label); auf jeder Engine ohne Live-Connector liest Provisa die HTTP-API — Metriknamen und Labels für Register Table, `query_range` über den Bereich der Tabelle zum Landen [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689)

### Enterprise-SaaS-Connectors

Google Workspace und Microsoft 365 werden von eigenen Loadern gelesen; siehe [Google Workspace und Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, Cloud-Inventar und Splunk registrieren sich über Apache-Calcite-Connectors (kenstott/calcite-Fork). Keiner hat einen direkten Treiber — Provisa startet den mitgelieferten Calcite-pgwire-Server des Connectors (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`) und erreicht ihn als generischen PostgreSQL-Endpunkt. Auf der DuckDB-Engine wird dieser Endpunkt live über die postgres-Erweiterung angehängt: Register Table listet die Tabellen des Connectors aus dem angehängten Katalog, Abfragen lesen den Connector vor Ort, und Filter und Projektionen werden zu Calcite heruntergeschoben (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Jede andere Engine landet die Zeilen im Materialisierungs-Store für die Federation (REQ-954). Die Bundles werden pro OS/Architektur aus dem gepinnten `kenstott/calcite`-Release abgerufen (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Ein Operator kann stattdessen pro Connector einen Bundle-Ordner benennen, der für diesen Host erstellt wurde, unter Admin → Einstellungen → Connector-Server (`connectors.<connector>.bundle_dir`); siehe [Admin](admin.md).. Alle fünf Connectors aktivieren immer Groß-/Kleinschreibung-unabhängigen Namensabgleich, entsprechend der eigenen case-insensitiven Semantik des jeweiligen Produkts (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint-Listen werden als Schemas aufgezählt und als abfragbare Tabellen offengelegt (REQ-726, REQ-731). Zwei Auth-Methoden: `CLIENT_CREDENTIALS` (Standard) und zertifikatbasiert über ein PFX-Zertifikat (REQ-727). Secret-Werte in `mapping` werden über die Secrets-Engine aufgelöst, bevor sie den Connector erreichen (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` oder `host` | `site-url` | SharePoint-Site-URL |
| `username` | `client-id` | Azure-App-Client-ID |
| `password` | `client-secret` | Azure-App-Client-Secret |
| `database` | `tenant-id` | Azure-Tenant-UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (Standard) oder `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | PFX-Pfad bei `auth_type: CERTIFICATE` — muss ABSOLUT sein |
| `mapping.certificate_password` | `certificate-password` | PFX-Passwort — der Key muss vorhanden sein, leerer String bei einer passwortlosen PFX |

Zertifikats-Auth auf den Nicht-Trino-Engines trägt zwei zusätzliche Regeln, beide beim Aufbau des `model.json`-Operanden des Calcite-pgwire-Servers durchgesetzt (REQ-1693). `certificate_path` muss absolut sein: Der Server läuft mit seinem Bundle-Verzeichnis als Arbeitsverzeichnis, sodass ein relativer Pfad innerhalb des Runtime-Deps-Caches aufgelöst wird und die PFX nicht gefunden wird. `certificate_password` muss in `mapping` vorhanden sein, selbst wenn die PFX kein Passwort hat — in dem Fall ist es der leere String —, denn der Calcite-Adapter lehnt ein Null-Passwort rundweg ab, und ein fehlender Key wird als Konfigurationsfehler behandelt statt stillschweigend als leeres Passwort gelesen. Ein fehlender oder relativer Wert löst `MissingConnectorConfig` mit Nennung des Felds aus. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

Wenn der Connector `information_schema.columns` nicht offenlegt, registrieren Sie die Tabelle mit expliziten Spaltendefinitionen (aus der Microsoft-Graph-API) über die `registerTable`-Mutation (REQ-732).

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

Jedes sObject, das die Anmeldeinformationen abfragen können, ist eine Tabelle, die der Steward registrieren kann (`Account`, `Opportunity`, benutzerdefinierte `*__c`-Objekte); nichts wird automatisch registriert (REQ-1946). Spalten und Typen stammen aus dem Describe des sObjects. Filter, Projektionen, Sortierungen und Limits werden als SOQL an Salesforce weitergegeben. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

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

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einem registrierten sObject sind Mutationen: gesteuert, auditiert und je Umgebung behandelt wie jede andere Mutation. Auf jeder Engine, auch Trino, läuft ein Schreibvorgang auf dem pgwire-Server der Quelle; auf Trino läuft dieser Server nur für Schreibvorgänge. Jede Anweisung wird an Salesforce gesendet und bei der Ausführung festgeschrieben, kann also nicht zurückgerollt werden. Der Server gibt die geschriebenen Zeilen nicht zurück: Eine Anweisung mit `RETURNING`-Klausel wird abgelehnt, und GraphQL bietet für diese Tabellen keine Mutationsfelder an; SQL-`INSERT`, -`UPDATE`, -`DELETE` und -`COPY` sind die Schreibpfade. Ein Schreibvorgang ist eine einzelne Anweisung auf einer Tabelle: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` und `TRUNCATE` werden abgelehnt und das jeweilige Formular benannt. Eine SharePoint-Quelle wird auf dieselbe Weise geschrieben. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Start.** Bevor er Verbindungen annimmt, liest der Server der Quelle die Spalten jedes sObjects, etwa vier Minuten bei einer Organisation mit 1.200 davon. Das fällt einmal an: Die Describe-Ergebnisse liegen im eigenen Zustandsverzeichnis der Quelle unter dem Datenverzeichnis der Instanz. Der Server wird gestartet, wenn die Quelle registriert oder geladen wird. Auf Trino hält der `salesforce`-Katalog seine eigenen Describe-Ergebnisse, ein Verzeichnis pro Quelle unter `/data/trino/cache/salesforce-describe` im Coordinator, sodass der Aufwand auch dort nur einmal anfällt, wenn dieser Pfad ein Volume ist, das den Container überdauert: das Volume `trino_cache` des Compose-Stacks oder das Coordinator-Cache-Volume des Helm-Charts (`trino.coordinator.cache.persistence`). Bis er Verbindungen annimmt, zeigt „Tabelle registrieren“ den Connector als startend an, und ein Schreibvorgang erhält dieselbe Meldung, statt zu warten. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

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

Jede Tabelle der Instanz steht zur Registrierung bereit; nichts wird automatisch registriert (REQ-1954). Auf einer vollen Instanz können es Tausende Tabellen sein; Register Table gruppiert sie daher nach Thema — jedes Thema ist der Anzeigename eines ServiceNow-Pakets oder „(kein Paket)", wenn eine Tabelle keinem Paket angehört. Jede Tabelle wird als `name — label` angezeigt. Die Auswahl einer Tabelle liest deren Spalten aus der Instanz; das bloße Auflisten liest keine. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Eine ServiceNow-Quelle nennt die vollständige `https://…`-URL der Instanz sowie Benutzername und Passwort. Die URL stammt aus `base_url` oder `host`; ein blosser Host oder ein anderes Schema wird abgelehnt und das Feld benannt (`instanceUrl`). Ein fehlender Benutzername oder ein fehlendes Passwort wird abgelehnt und das jeweilige Feld benannt. Jeder Wert kann eine `${secret:…}`- oder `${env:…}`-Referenz sein. Ein als Literal eingegebenes Geheimnis wird im Tresor der Organisation abgelegt, und die Quelle behält ihre Referenz.

| Quellfeld | Adapter-Eigenschaft | Hinweis |
| --- | --- | --- |
| `base_url` oder `host` | `instanceUrl` | Vollständige `https://…`-URL der Instanz (z. B. `https://acme.service-now.com`). Ein blosser Host oder ein anderes Schema wird abgelehnt |
| `username` | `username` | Basic-Auth-Benutzername |
| `password` | `password` | Basic-Auth-Passwort |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Lesen.** Auf Trino wird die Quelle über den `servicenow`-Katalog gelesen, der nur liest. Auf jeder anderen Engine bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle. Nur die ausgewählten Spalten werden von der Instanz angefordert (`sysparm_fields`). Ein Filter wird nur in einer Form an die Instanz gesendet (`sysparm_query`), die der Adapter an einer Live-Instanz verifiziert hat; jeder andere Filter sowie jede Sortierung und jedes Limit werden auf die gelesenen Zeilen angewendet. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Ein Referenzfeld erscheint als zwei Spalten: `<field>` enthält die sys_id des referenzierten Eintrags, und `<field>__display` enthält den Anzeigewert. Ein leerer Feldwert ist NULL. Ein Feld, dessen Typ der Adapter nicht abbilden kann, erzeugt einen Fehler, der Tabelle, Feld und Typ benennt. [inferred from adapter behavior; in dieser Sitzung nicht direkt im Provisa-Quellcode überprüft]

Die Tabellensuche (natürliche Sprache) stimmt auf Name, Label und Thema jeder Tabelle ab. Sobald eine Tabelle registriert ist, nehmen ihre Spalten an der Suche teil. [tool-verified: `provisa/discovery/catalog_cache.py` `COLUMNS_PER_TABLE_KINDS` Docstring]

**Server-Lebenszyklus.** Der Server bedient nur die aus ihm registrierten Tabellen. Das Registrieren oder Entfernen einer Tabelle startet den Server neu und provisioniert die Quelle erneut auf der Engine; eine Anweisung, die während dieses Neustarts eintrifft, schlägt fehl. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einer registrierten Tabelle sind Mutationen: gesteuert, auditiert und je Umgebung behandelt wie jede andere Mutation. Auf jeder Engine, auch Trino, läuft ein Schreibvorgang auf dem pgwire-Server der Quelle. Der Server gibt die Zeile zurück, die die Instanz nach dem Schreibvorgang enthält; eine `RETURNING`-Klausel wird daher akzeptiert, und GraphQL bietet Mutationsfelder für diese Tabellen an. Ein Schreibvorgang ist eine einzelne Anweisung auf einer Tabelle: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` und `TRUNCATE` werden abgelehnt und das jeweilige Formular benannt. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import-Sets.** Wenn eine Tabelle über eine aktive Transform-Map verfügt, bietet Register Table deren Import-Sets an, und der Steward kann die Tabelle an eines davon binden (eine Tabelle mit mehreren Maps benennt jedes einzeln). Einfügungen in eine gebundene Tabelle laufen über die Import-Set-API der Instanz und die Transformation der jeweiligen Map; Einfügungen in eine ungebundene Tabelle sowie alle Updates und Deletes werden direkt in die Tabelle geschrieben. Eine Zeile, die die Transformation ablehnt, wird zurückgewiesen und die betreffende Zeile benannt; bereits geschriebene Zeilen bleiben erhalten. Die Bindung wird beim Speichern der Tabelle geprüft: Sie muss das Import-Set einer aktiven Transform-Map in die Tabelle sein, auf einer ServiceNow-Quelle. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`; `provisa/federation/servicenow.py` `servicenow_settings`; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Eine Jira-Cloud-Site wird über einen eigenen Calcite-Adapter gelesen und geschrieben (REQ-2069). Eine Tabelle einer Jira-Quelle ist ein **Formular**: ein Projekt und ein Vorgangstyp, dazu die Felder, die ihre Spalten lesen. Register Table listet jedes Formular auf, für das das Konto einen Vorgang anlegen kann, nach Projekt gruppiert; die Auswahl eines Formulars liest dessen Felder aus Jira, das bloße Auflisten der Formulare liest sie nicht. Jedes Formular hat die Spalten `key` (sein Primärschlüssel), `id`, `summary`, `status`, `created` und `updated`, danach je eine Spalte pro gewähltem Feld, benannt, wie der Adapter sie benennt: der Feldname in Snake Case (`Story Points` wird zu `story_points`). Ein Feld, dessen Name bereits der Name einer Spalte ist, wird nach seiner Feld-ID benannt (`customfield_10099`), sodass sich jedes Feld auswählen lässt. Ein Benutzerfeld enthält die Konto-ID; sein Anzeigename ist ein eigenes Feld (`assignee.displayName`, Spalte `assignee_name`). Spalten, die Jira selbst schreibt, sind schreibgeschützt: `key`, `id`, `created`, `updated`, der Anzeigename eines Benutzerfelds sowie ein Feld in einer Form, die der Adapter nicht kennt und die er als JSON-Text liefert. Bei der Registrierung wird eine solche Spalte in ihrer Beschreibung mit „Read only: Jira sets it“ gekennzeichnet, sofern sie keine hat, und ein Schreibvorgang, der sie setzt, wird vom Server abgelehnt, wobei die Spalte benannt wird. [tool-verified: `provisa/federation/jira.py` `FIXED_COLUMNS`, `form_columns`; `provisa/federation/pgwire_replica.py` `jira_form_list`, `jira_fields`; `provisa/api/admin/introspect.py` `complete_jira_form`]

Die Tabelle eines Formulars nennt ihr Projekt und ihren Vorgangstyp in ihren `arguments` (`project`, `issue_type`), und jede Spalte liest das Feld, das ihr `path` nennt (eine Feld-ID oder ein Feldname), oder das Feld mit ihrem eigenen Namen. Eine im Admin registrierte Tabelle erhält beides von Jira; eine Konfiguration gibt sie an. Eine Jira-Tabelle, die etwas anderes festlegt oder eines von beiden nicht enthält, wird abgelehnt und benannt. [tool-verified: `provisa/federation/jira.py` `form_of`; `provisa/core/config_loader.py` `_validate_table_arguments`]

| Quellfeld | Adapter-Eigenschaft | Hinweis |
| --- | --- | --- |
| `base_url` oder `host` | `baseUrl` | Vollständige `https://…`-Adresse der Site (z. B. `https://acme.atlassian.net`). Ein blosser Host oder ein anderes Schema wird abgelehnt |
| `username` | `email` | E-Mail-Adresse des Atlassian-Kontos |
| `password` | `apiToken` | Ein API-Token dieses Kontos, erstellt unter id.atlassian.com unter Security, API tokens |

[tool-verified: `provisa/federation/jira.py` `jira_site_url`, `jira_settings`]

**Verknüpfungen.** Die Tabelle `issue_links` der Quelle enthält die Verknüpfungen zwischen den Vorgängen ihrer Formulare: `link_id`, `link_type`, `outward_key` und `inward_key`, jede Verknüpfung einmal. Eine Beziehung zwischen zwei Formularen wird darüber als Junction deklariert (REQ-1586), mit `link_type` als Diskriminator, sodass GraphQL und Cypher vom Vorgang zum verknüpften Vorgang nach Verknüpfungstyp navigieren. [inferred: the adapter's design agreed for REQ-2069]

**Lesen.** Eine Suche nach Schlüssel (`key =`, `key IN`) wird per Schlüssel abgerufen, sodass ein Schlüssel, der keinen Vorgang benennt, keine Zeile ergibt. Ein Vergleich von Datum oder Zahl wird als JQL an Jira gesendet. Jeder andere Filter und jede Sortierung wird auf die gelesenen Zeilen angewendet: Eine Gleichheit auf Text, einer Option, einem Status oder einem Benutzer wird nicht gesendet, da JQL einen Wert ablehnt, der nichts benennt, wo SQL keine Zeilen zurückgibt. Ein Mehrwertfeld (Labels, Mehrfachauswahl, Komponenten, Versionen) ist Text mit einem JSON-Array und wird genauso geschrieben. [inferred: the adapter's behaviour, as the calcite connector reports it for REQ-2069]

Auf DuckDB, PostgreSQL und ClickHouse bindet die Engine den pgwire-Server der Quelle ein und liest sie an Ort und Stelle. Auf Trino hat die Quelle keinen eigenen Katalog und wird über ihren Server geladen. [tool-verified: `provisa/federation/engine.py`; `provisa/federation/strategy.py` `_CONNECTOR_PGWIRE_REPLICA`]

**Server-Lebenszyklus.** Der Server bedient nur die aus ihm registrierten Formulare, daneben seine Formularliste, seine Feldliste und die Verknüpfungen. Das Registrieren, Ändern oder Entfernen eines Formulars startet den Server neu und provisioniert die Quelle erneut auf der Engine. [tool-verified: `provisa/federation/served_tables.py` `SERVES_REGISTERED_TABLES`, `serve_tables`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Schreiben.** `INSERT`, `UPDATE` und `DELETE` auf einem Formular sind Mutationen, auf jeder Engine auf dem pgwire-Server der Quelle geschrieben. Zeilen werden in Stapeln von 50 eingefügt; ein Update oder Delete erfolgt Vorgang für Vorgang, per Schlüssel. Das Setzen von `status` führt den Workflow-Übergang in diesen Status aus und wird abgelehnt, wobei die erreichbaren Status benannt werden, wenn der Workflow des Vorgangs keinen hat. Ein Vorgang mit Teilaufgaben wird nicht gelöscht. Der Server gibt jede geschriebene Zeile per Schlüssel zurück; eine `RETURNING`-Klausel wird daher akzeptiert, und GraphQL bietet Mutationsfelder an. Auf `issue_links` erstellt `INSERT` eine Verknüpfung und `DELETE` entfernt eine; `UPDATE` wird abgelehnt. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`] [inferred: the batching, transitions and subtask refusal are the adapter's, agreed for REQ-2069]

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

Das Cloud-Inventar stellt die Ressourcen von Azure, AWS und GCP als einen Satz Tabellen dar: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` und `compute_security_groups`, das eine Compute-Ressource mit ihren Sicherheitsgruppen verknüpft. Jede Tabelle hat über die drei Clouds hinweg dieselben Spalten, und ihre Spalte `cloud_provider` nennt die Cloud, aus der eine Zeile stammt. Die Quelle liest nur, und ihre Tabellen werden wie alle anderen von einem Steward registriert (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

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

Splunk-Suchergebnisse sind als Tabellen abfragbar (z. B. `internal_server`) (REQ-721). Die Connector-URL stammt aus `base_url` oder wird als `https://{host}:{port}` mit Standardport `8089` konstruiert (REQ-722). Auth: Ist `mapping.use_token` `true` (Standard), wird `password` als API-Token übergeben; ist es `false`, werden `username` und `password` als separate Zugangsdaten übergeben (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, sonst `https://host:port` (Standardport 8089) |
| `password` | `token` oder `password` | Token, wenn `use_token: true` |
| `username` | `user` | nur, wenn `use_token: false` |
| `database` | `app` | auf eine Splunk-App einschränken |
| `mapping.datamodel_filter` | `datamodel-filter` | auf ein Data Model filtern |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | für selbstsignierte Zertifikate (REQ-724) |

Auf dem pgwire-Replica-Pfad (jede Engine außer Trino) werden dieselben vier optionalen Einstellungen zu den Calcite-`model.json`-Operanden-Keys `app`, `token`/`username`+`password`, `datamodelFilter` und `disableSslValidation` — die letzten beiden in den Typen, in die `SplunkSchemaFactory` sie castet, ein String und ein Boolean (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

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

#### Google Workspace und Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` und `microsoft_365` lesen E-Mails, Kalender und Aufgaben der Konten, die eine Quelle benennt, und landen sie als Replikate. Es sind keine Calcite-Connectors: Jeder hat einen eigenen Loader, der die API des Anbieters aufruft, auf der einen Seite Gmail, Google Calendar und Google Tasks, auf der anderen Microsoft Graph. Das Formular führt sie unter Enterprise als „Google Workspace (Gmail)“ und „Microsoft 365 (Outlook, Exchange)“. [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Tabellen.** Beide Typen erzeugen dieselben kanonischen Tabellen: dieselben Namen, Spaltentypen und dieselbe Reihenfolge. Eine Vereinigung beider braucht weder Cast noch Umbenennung; eine Spalte, für die ein Anbieter keinen Wert hat, ist NULL. Die Tabellen stehen im Schema `default`, und jede Zeile beginnt mit dem Konto, aus dem sie gelesen wurde. Eine Quelle bietet die Tabellen der Ressourcen an, die sie liest: [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Ressource | Tabellen | Durch Anweisungen beschrieben |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Nein |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Die Spalten stehen im [kanonischen Mail-Schema](arch/canonical-mail-schema.md). `events` enthält Einzeltermine, Serienmaster mit ihrer Wiederholung als RFC-5545-Zeilen und Ausnahmen; die aufgelösten Termine einer Serie enthält die Tabelle nie. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Registrierung.** Der Administrator einer Organisation trägt zuerst den Client der Organisation für die Plattform unter Admin → Email ein: eine Client-ID und ein Secret, bei Microsoft 365 dazu die Verzeichnis-ID (Tenant-ID) oder die Domain. Wer eine Quelle hinzufügt, sieht und tippt ihn nie. Eine Quelle hat dann eine von zwei Formen: [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Ein Postfach.** Die Quelle nennt eine Adresse in `accounts`. Ihr Inhaber klickt im Formular auf **Connect** und genehmigt den Client der Organisation. Die daraus entstehende Genehmigung liegt im State-Store der Organisation, nicht in den Einstellungen der Quelle. Bei Google Workspace nennt diese Form zusätzlich `sign_in: google_account`.
- **Die Postfächer der Organisation.** Die Quelle nennt `mailboxes` als genau eines von `everyone: true`, `group: <address or id>` oder `list: [addresses]`. Sie hält keine Zugangsdaten, und niemand verbindet sie. Die Organisation liest mit ihrem eigenen Zugriff, und zwar erst, wenn der Administrator „Read the organization's mailboxes“ eingeschaltet hat. Bei Google ist dieser Zugriff ein Service-Account-Schlüssel mit domänenweiter Delegierung, dazu eine Administratoradresse für Verzeichnisaufrufe. Bei Microsoft ist es der Client-Credentials-Grant der App-Registrierung.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

Bei der zweiten Form hält eine Tabelle die Zeilen aller Postfächer. Eine Rolle, die die Tabelle lesen darf, liest jedes Postfach, es sei denn, eine Zeilenregel auf der Spalte `account` schränkt sie ein. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

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

**Einstellungen.** Jedes Modul weist einen Schlüssel, den es nicht liest, mit Namen zurück. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

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

Google weist eine Suche unter `gmail.metadata` zurück, daher wird `mail_content: headers` mit `mail_search` oder `mail_since` zurückgewiesen. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Scopes.** Eine Anmeldung fragt nur die Scopes der angekreuzten Ressourcen an: [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Ressource | Google Workspace | Microsoft 365 (delegiert) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, oder `gmail.metadata` nur für Header | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| jede Quelle | `userinfo.email` | `offline_access`, `User.Read` |

Für die Organisationsform braucht Microsoft die Anwendungsberechtigungen `Mail.Read` und `User.Read.All`, dazu `Calendars.ReadWrite` und `Tasks.ReadWrite.All` für Kalender und Aufgaben. Googles Service-Account ist für die Scopes in `DELEGATED_SCOPES` autorisiert, die auch das Lesen von Verzeichnisbenutzern und Gruppenmitgliedern umfassen. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Aktualisierung und Aktualität.** Die Probe-Typen dieser Quellen sind `hash` und `none`, und ihr Loader liefert das Änderungstoken: [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail:** ein Digest der aktuellen History-ID jedes Postfachs, ein Aufruf von `users.getProfile` je Postfach. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Microsoft-Mail:** jeder gehaltene Delta-Link, verfolgt für ein Element. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Google-Kalender und -Aufgaben:** das Tag und der Zeitpunkt der letzten Änderung jedes Kalenders; das Tag jeder Aufgabenliste sowie ID und Aktualisierungszeit jeder Aufgabe. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Microsoft-Kalender und -Aufgaben:** die Änderungszeilen jedes Postfachs über `calendar_check_days` vor und nach heute. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Eine Prüfung, die sich nicht durchführen lässt, fordert ein Lesen an; sie meldet nie „unverändert“. Kalender- und Aufgabentabellen werden bei jeder Aktualisierung vollständig gelesen. Die Mail-Replikate einer Microsoft-365- oder Google-Workspace-Quelle werden an Ort und Stelle gehalten: Jeder Teil eines Postfachs wird geschrieben, sobald er eintrifft, und ist sofort abfragbar. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Eine Google-Workspace-Quelle, die mit `mail_search` eingrenzt, wird bei jeder Aktualisierung erneut vollständig gelesen, weil nur Google sagen kann, was eine Suche trifft. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Schreibvorgänge.** `INSERT`, `UPDATE` und `DELETE` auf `events`, `event_attendees` und `tasks` gehen für beide Typen über einen gemeinsamen Schreibpfad an den Anbieter. Ein `UPDATE` oder `DELETE` benennt seine Zeilen über `provider`, `account` und `id`; ein Teilnehmer wird über `event_id` und `address` benannt. Ein einzelner Termin einer Serie wird über `series_id` und `original_start_at` benannt. Ein Schreibvorgang, der eine Spalte außerhalb der beschreibbaren Menge der Tabelle setzt, wird mit Namen zurückgewiesen. Google schreibt mit `sendUpdates=all`, wenn `calendar_send_updates` an ist, und mit `sendUpdates=none`, wenn es aus ist. Microsoft Graph kann eine solche Änderung nicht stillschweigend vornehmen, daher wird eine Änderung der Teilnehmer eines Meetings zurückgewiesen, mit Nennung der Einstellung. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Ausschlüsse.** E-Mail wird nur gelesen; keine Anweisung ändert ein Postfach. `calendars`, `task_lists` und jede Mail-Tabelle nehmen keine Schreibvorgänge an. Gmails Spam und Papierkorb bleiben außen vor, solange `mail_include_spam_trash` nicht an ist. Googles Tasks-API behält nur den Tag, an dem eine Aufgabe fällig ist, und kennt keine Erinnerung. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### API-Quellen {: #api-sources }

Registrieren Sie jeden HTTP-Endpunkt als abfragbare Tabelle. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API-Typ | Discovery | Spaltenableitung |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI-Spec-Parsing (REQ-314, REQ-316) | Primitive → nativ, Objekte → JSONB |
| `graphql_remote` | Schema-Introspektion (REQ-307, REQ-308) | Primitive → nativ, Objekte → JSONB |
| `grpc_remote` | Server Reflection (REQ-322, REQ-325) | Primitive → nativ, Objekte → JSONB |

API-Antworten werden abgerufen, in PostgreSQL gecacht (konfigurierbare TTL) und als GraphQL-Typen offengelegt (REQ-309, REQ-318, REQ-327). Gecachte Tabellen nehmen wie jede andere Quelle an föderierten Abfragen teil (REQ-313).

**JSONB-Regeln**: Komplexe Spalten (Objekte, Arrays), die als JSONB gespeichert werden, sind nicht filterbar (REQ-119). Der Zugriff auf Unterfelder verwendet die `->>`-Extraktion in SQL (REQ-151). Beziehungen werden zwischen Tabellen über skalare FK-Spalten deklariert — JSONB-Blob-Spalten sind keine Join-Ziele. Verwenden Sie JSONB-Promotion, um verschachtelte Felder in native skalare Spalten umzuwandeln, wenn Filtern oder Joinen darauf benötigt wird (REQ-119).

#### GitHub und GitLab {: #github-and-gitlab }

GitHub und GitLab sind Marken-Quellen, die `graphql_remote` trägt: Jede erscheint im Sources-Formular unter ihrem eigenen Namen und registriert sich als Remote-GraphQL-Quelle, die ihre Marke festhält. Das Formular verlangt ein Personal Access Token und sonst nichts. Der Endpunkt (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) und das Schema werden mit Provisa ausgeliefert, daher wird das Schema bei der Registrierung nicht introspiziert. Das Token wird beim Hinzufügen der Quelle geprüft. Das Hinzufügen der Quelle registriert keine Tabellen: Jede Tabelle, die das Schema anbietet, steht unter dem Namensraum der Quelle (`gh`, `gl`) bereit, und der Steward registriert die gewünschten. Ein Feld, das das Token nicht lesen darf, wird beim Registrieren aus einer Tabelle ausgelassen. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

Stripe ist eine Marken-Quelle, die `openapi` trägt, so gebaut wie GitHub und GitLab über `graphql_remote`. Sie erscheint im Sources-Formular als Stripe unter Enterprise. Sie registriert sich als OpenAPI-Quelle, deren Zeile die Marke in `federation_hints` festhält und deren Spec-Pfad `brand:stripe` lautet. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**Was sie erreicht.** Die Stripe-REST-API, über Stripes veröffentlichte OpenAPI-Spezifikation (`stripe/openapi`, `spec3.json`). Die Spezifikation wird mit Provisa ausgeliefert, gepinnt und gzip-komprimiert, und nicht bei der Registrierung abgerufen. Die ausgelieferte hat die API-Version `2026-09-30.endive` mit 425 Pfaden. Jeder Aufruf trägt `Stripe-Version` mit dieser Version, sodass jede Antwort die Form hat, die die Spezifikation beschreibt, welche Version das Konto auch als Standard hat. Felder, die Stripe zurückgibt und die Spezifikation auslässt, werden aus `stripe.additions.json` ergänzt. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Wie bei jeder OpenAPI-Quelle ist jede `GET`-Operation eine angebotene Tabelle und jede andere Operation ein angebotener Befehl. Das Hinzufügen der Quelle registriert keine davon: Der Steward registriert die gewünschten. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Auth.** Das Formular verlangt genau eine Angabe, ein Zugriffstoken (`BRAND_AUTH`: `token`). Verwenden Sie einen geheimen oder eingeschränkten Stripe-API-Schlüssel. Das Token wird als Bearer-Token gesendet und vor dem Hinzufügen der Quelle einmal mit `GET /v1/balance` geprüft. Ein abgelehntes Token lässt die Registrierung mit `openapi.credential_rejected` scheitern; ein fehlendes mit `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Aktualisierung und Aktualität.** Eine Stripe-Tabelle ist ein Replikat, wie jede OpenAPI-Tabelle. Ihre gecachten Antworten gelten für das `cache_ttl` der Quelle, das eine Registrierungsanfrage auf 300 Sekunden setzt, sofern sie nichts anderes sagt. Es gelten die OpenAPI-Probe-Typen. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 sieht vor, dass ein Preset den Parameter benennt, mit dem eine Aktualisierung nur das Geänderte abruft. Die Stripe-Marke benennt keinen, daher liest jede Aktualisierung ihre Tabellen vollständig. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Ausschlüsse.** Stripes veraltete Token- und Sources-Zahlungsintegration bleibt aus der ausgelieferten Spezifikation draußen. Die Liste steht unter [Ausschlüsse bei einer aus einer Spezifikation gebauten Quelle](#exclusions-on-a-specification-built-source). Die eigenen `exclusions` eines Operators werden denen der Marke hinzugefügt. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` erscheint auch in `SourceType` unter den gestubbten Supabase-`wrappers`-Typen. Eine über das Formular hinzugefügte Stripe-Quelle ist eine `openapi`-Quelle, nicht dieser Stub. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Ausschlüsse bei einer aus einer Spezifikation gebauten Quelle {: #exclusions-on-a-specification-built-source }

Eine veröffentlichte OpenAPI-Spezifikation beschreibt alles, was die entfernte API annimmt, auch das, was ihr Hersteller als veraltet markiert hat und was ein Deployment nie senden darf. Eine OpenAPI-Quelle kann Ausschlüsse tragen: Dinge in der Spezifikation, die die Quelle nicht anbietet. (REQ-1957)

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

Offene Daten der US-Regierung. Der Zugriff ist nach Themengruppierung partitioniert. [tool-verified: `provisa/core/models.py` lines 543–609]

Einer Quelle werden ein oder mehrere Themen zugewiesen, und jedes Thema bringt die unten aufgeführten Adapter-Schemas mit. Ein Schema kann zu mehr als einem Thema gehören (`census`). Die Schemas `ref` und `geo` sind Linker-Schemas: Referenzdaten und Geografie, über die die Tabellen jedes Themas joinen, sodass jede Quelle sie bereitstellt, welche Themen sie auch erhält. Die Liste ist einmal auf dem Server festgelegt und wird per Test an die Schemas gebunden, die das gepinnte Adapter-Bundle bereitstellt. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

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

Eine im Admin angelegte AskAmerica-Quelle nimmt eine Anmeldeinformation entgegen, den API-Schlüssel. Provisa legt der API von AskAmerica den Schlüssel vor und erhält die kurzlebigen, schreibgeschützten Speicher-Anmeldeinformationen, mit denen die Daten gelesen werden; der Schlüssel wird dort namentlich abgelehnt, wenn er unbekannt oder widerrufen ist. [tool-verified: `provisa/federation/askamerica.py`]

Die Quelle ist dann eine Postgres-Wire-Quelle wie die SharePoint-, Salesforce- und Cloud-Inventar-Quellen: Provisa startet dafür den mitgelieferten `pgwire-govdata`-Server, mit dem Schlüssel und diesen Anmeldeinformationen in der Umgebung des Servers, und jede Engine erreicht diesen Server so, wie sie den eines Geschwisters erreicht. DuckDB, Postgres und ClickHouse binden ihn ein und lesen seine Tabellen an Ort und Stelle, sodass eine AskAmerica-Tabelle mit Tabellen anderer Quellen joint, unter Sichten liegt und wie jede andere ausgeliefert wird. Trino hat dafür keinen Connector und liest ein Replikat, das über den Server gelandet wurde: eine Kopie der Tabelle im Materialisierungs-Store, so frisch wie ihr letzter Refresh unter den Cache- und Replikationseinstellungen der Tabelle und nicht zur Abfragezeit gelesen. Eine Tabelle wird aus dem Adapter-Schema gelesen, das sie nennt (`sec`, `econ` und so weiter). [tool-verified: `provisa/federation/pgwire_replica.py`, `provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

Der Server bindet seine Schemas aus dem Objektspeicher ein, bevor er eine Verbindung annimmt; das dauert beim ersten Start Minuten. Er wird gestartet, wenn die Quelle gespeichert wird; bis er bereit ist, meldet das Register-Table-Formular, dass die Quelle noch startet. [tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Wo das Passwort einer Quelle lebt {: #where-a-sources-password-lives }

Das Passwort einer Quelle wird nie neben den übrigen Verbindungseinstellungen gespeichert. Die
`sources`-Zeile der Control Plane trägt eine `password_ref`-Spalte mit dem, was für das Passwort
eingegeben wurde: dem Wert selbst oder einer Referenz wie `${env:PG_PASSWORD}` oder
`${secret:SNOWFLAKE_KEY}`. Sie wird in dem Moment aufgelöst, in dem die Quelle angewählt wird,
innerhalb der Organisation, in deren Namen die Anfrage läuft. [tool-verified:
`provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

Ein Wert ohne `${…}` wird unverändert verwendet. `${env:VAR}` liest die Prozessumgebung des
Deployments und benötigt keine Bindung. `${secret:NAME}` benennt ein Secret, das einer Organisation
gehört, sodass es nur innerhalb der eigenen Operationen dieser Organisation aufgelöst wird: die
Admin-Introspektions-Nahtstellen und das Query-Terminal, das jede Oberfläche erreicht, stellen diese
Bindung her. [tool-verified: `provisa/core/secrets.py` `resolve_secrets`; `provisa/pgwire/_pipeline.py`
`_execute_plan`]

Das Feld hält, was Sie hineinschreiben, gleichgültig wie die Quelle registriert wurde:

- **Aus der Konfiguration.** Die Datei ist der Datensatz: Provisa kopiert den Wert oder die Referenz
  wortwörtlich in `password_ref`.
- **Aus dem Sources-Formular.** Was in das Passwortfeld eingegeben wird, wird wortwörtlich
  gespeichert, ein Wert wie eine Referenz. Provisa schreibt dafür nichts in den Vault der
  Organisation und erfindet keinen Secret-Namen. Um eine Anmeldeinformation aus der Zeile der
  Control Plane herauszuhalten, legen Sie das Secret selbst an (siehe [Secrets](secrets.md)) und
  tippen `${secret:NAME}` in das Feld. [tool-verified: `provisa/api/admin/schema_mutation.py`
  `create_source`]

Ein als Wert eingegebener Wert wird so, wie eingegeben, in der Datenbank der Control Plane
gespeichert. Das erneute Eintippen des Passworts ersetzt den Text des Felds. Das Löschen der Quelle
entfernt ihre Zeile und nichts aus dem Vault: Ein Secret, das Sie angelegt haben, gehört Ihnen.
[tool-verified: `provisa/api/admin/schema_mutation.py` `delete_source`;
`tests/unit/test_no_product_written_secrets.py`]


`password_ref` wandert nicht zwischen Umgebungen (REQ-1491). Ein Branch oder eine kopierte Umgebung
liefert ihre eigenen Verbindungswerte, und der Vault, den eine Referenz benennt, gehört zu welcher
Umgebung auch immer sie geliefert hat. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Data-Profiler {: #data-profiler }

Ein Data Profiler ist eine Quelle, die einen Namen, einen Zeitplan und Lauf-Standardwerte hält, sonst nichts. Sie wird nie introspiziert. Tabellen anderer Quellen werden ihr hinzugefügt, und jeder Lauf misst sie und schreibt seine Zeilen in die Control Plane der Organisation, wo sie wie die Tabellen jeder anderen Quelle gelesen werden. Wie Sie einen Profiler anlegen und Tabellen hinzufügen, steht unter [Testdaten](test-data.md). [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)

### Data-Quality-Checker (REQ-1443) {: #data-quality-checkers-req-1443 }

Ein Data-Quality-Checker ist ein Quelltyp, kein Subsystem. Seine Scan-Ausgabe sind Daten: Ein Check-Ergebnis ist eine Beobachtung, sodass es über den gewöhnlichen Quellpfad landet und Kadenz, Freshness, Events, Lineage, Governance, RLS, Grid und Export von jeder anderen Quelle erbt. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Zwei werden unterstützt, und die Wahl ist ebenso eine Lizenzfrage wie eine Feature-Frage.

| Quelltyp | Vertrags-Dialekt | Extra | Lizenz | Gehostete Cloud-Plane |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda-Contract-YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Abgelehnt — siehe unten |
| `great_expectations` | Expectation-Suite-JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Erlaubt |

Die Elastic License 2.0 verbietet, die Software Dritten als gehosteten oder verwalteten Dienst bereitzustellen, und Soda innerhalb der SaaS-Plane im Namen eines Mandanten laufen zu lassen ist genau das. `config/capabilities.yaml` trägt die Aufteilung als `cloud_eligible: false` auf der `soda`-Option, und die gehostete Plane liest dieses Flag. Ein gehostetes Deployment, das Soda möchte, erreicht einen vom Betreiber bereitgestellten Soda-Endpunkt, den der Betreiber selbst betreibt. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa bündelt und verlinkt nichts. Der Scan läuft in einem Kind-Interpreter (`python -m provisa.dq.worker`), dem einzigen Ort, an dem `soda_core` oder `great_expectations` importiert wird, sodass ein source-available Checker den Serverprozess nie erreicht und ein Checker-Absturz einen Subprozess tötet statt die Event-Loop. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**Die Quelle zeigt auf Provisas eigenen pgwire-Endpunkt.** Das ist, was es einem Postgres-Treiber erlaubt, eine Snowflake- oder Iceberg-gestützte Tabelle zu prüfen: Der Checker scannt die föderierte Sicht, nicht das darunterliegende System. Weil Richtlinie auf diese Verbindung angewendet wird, wird die Scan-Identität deklariert statt geerbt — eine gefilterte Zeilenmenge darf nie einen stillschweigend bestehenden Check erzeugen.

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

**Eine Ergebnistabelle pro Vertrag, und der Vertrag ist die gesamte Registrierung.** Die Tabelle trägt `dq_contract` — den Vertragstext wortwörtlich — und sonst nichts über ihre Form. Spalten, Watermark und Promotions werden alle abgeleitet. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

Was die Registrierung aus diesem Text ableitet:

- **Lineage.** Der Vertrag benennt bereits sein Zieldataset, sodass die Registrierung ihn so parst, wie `extract_inputs` SQL parst (REQ-939), und ihn auf die geregelte Tabelle auflöst. Eine Definition, keine zweite Kopie, die driften kann. Ein Vertrag, der ein ungeregeltes Dataset benennt, schlägt bei der Registrierung laut fehl, statt Zeilen zu landen, die niemand angefordert hat.
- **Spalten.** Der Ergebnis-Umschlag gehört dem Checker, nicht dem Betreiber — 16 mitgelieferte Spalten von `scan_id` bis `diagnostics`. Deklarierte Spalten werden nur für ihr `visible_to` gelesen, das einstimmig sein muss, und dann ersetzt. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Watermark.** `scan_time` wird zum Watermark, was das Landen zu einem Append macht (REQ-982). Scan-Historie akkumuliert ohne Historien-Subsystem.
- **Promotions.** `freshness_max_timestamp` und `dataset_rows_tested` werden aus dem `diagnostics`-jsonb als typisierte Spalten hochgestuft (REQ-119). Fügen Sie weitere hinzu, wie Sie es bei jeder anderen jsonb-Spalte täten. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

Timing führt keine neuen Felder ein. `change_signal` plus `cache_ttl` geben die Poll-Kadenz vor; `mv_debounce_quiet` und `mv_debounce_max_delay` fassen einen vorgelagerten Burst zu einem Scan zusammen (REQ-963); eine Kalendergranularität macht ihn periodisch (REQ-962); `expected_events` hält den Scan zurück, bis seine Eingaben durch das Fenster frisch sind (REQ-961). Die Poll-Schleife ist der Scan-Scheduler.

`outcome` ist eines von `pass`, `fail`, `warn`, `error`, `skipped`. Keines davon ist ein Urteil — Durchsetzung, falls gewünscht, ist eine separate, spätere Deklaration: ein Preflight oder eine materialisierte Sicht über den gelandeten Ergebnissen. Weil eine gelandete Beobachtung keine Determinismus-Verpflichtung trägt (REQ-964), sind hier nichtdeterministische Checks zulässig, die auf einem Preflight-Gate nie sitzen könnten — Anomalie-Score, Trailing-Window-Änderung, Freshness gegen jetzt.

Der Vertrag wird in der UI verfasst, im Data-Quality-Panel der Tabellen-Bearbeitungsoberfläche, und der rohe Vertragstext dort ist immer die Source of Truth. Ein Trockenlauf führt den Vertrag gegen die Live-Tabelle aus und zeigt die Ergebnisse, ohne sie zu landen — so erwischen Sie einen Vertrag, dessen Datasetname an eine unerwartete Stelle aufgelöst wurde und der sonst nichts als bestehende Zeilen landen würde.

---

## Benutzerdefinierte Connectors (REQ-1177) {: #custom-connectors-req-1177 }

Die nativen Federation-Engines — Postgres, DuckDB und ClickHouse — gewinnen Erreichbarkeit zu einem neuen Quelltyp, wenn ein Betreiber dafür einen Connector in `config/custom_connectors.yaml` deklariert. Kein Code erforderlich. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Connector-Erweiterbarkeit selbst geht dem voraus. Die Trino-Engine ist auf ihrer eigenen Schicht seit Langem erweiterbar — ein generischer JDBC-Connector, parametrisiert pro Quelltyp, ein Katalog-`.properties`-Body pro Typ, und Provisas eigene benutzerdefinierte Trino-Connector-Plugins (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 bringt dieselbe konfigurationsgesteuerte Erweiterbarkeit zu den zwei nativen, clusterlosen Engines, die zuvor eine feste Connector-Menge trugen.

Die Konfiguration wird leer ausgeliefert. Eingebaute Connectors decken die Reichweite von Haus aus ab; alles in dieser Datei ist vom Betreiber verfasst. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Setzen Sie `PROVISA_CUSTOM_CONNECTORS`, um auf einen anderen Pfad zu verweisen (nützlich für Tests).

### Deskriptor-Arten {: #descriptor-kinds }

| Engine | Art | Mechanismus | Was der Deskriptor liefert |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (ISO-Standard) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + Scanner-View | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (legt automatisch jede entfernte Tabelle offen) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` pro Tabelle (Spalten aus der Registry) | `ch_engine`, `engine_template` (kann `{table}` tragen) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse leitet das Schema ab | `ch_engine`, `engine_template` |

**Postgres ist generisch.** SQL/MED ist ein ISO-Standard, sodass jeder konforme FDW dieselbe DDL-Form teilt: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, optional `CREATE USER MAPPING`, dann entweder `IMPORT FOREIGN SCHEMA` (wenn `supports_import: true`) oder ein explizites `CREATE FOREIGN TABLE` pro Tabelle (wenn `false`). Ein `pg_fdw`-Deskriptor liefert nur die Pro-FDW-Varianz — Erweiterungsname, Server-Options-Keys, User-Mapping-Keys, Import-Flag, Tabellen-Optionen. Jeder standardkonforme FDW ist daher allein aus der Konfiguration ansteuerbar. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB unterstützt zwei Mechanismen.** Eine Erweiterung, die einen Katalog über ATTACH offenlegt, verwendet `duckdb_attach`; eine, die eine lesende Table-Function offenlegt, verwendet `duckdb_scan`. Eine Erweiterung, die zu keinem der beiden Muster passt, wird nicht unterstützt. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse unterstützt drei Mechanismen**, einen pro Integrations-Engine-Form: eine relationale DATABASE-Engine, die automatisch jede entfernte Tabelle offenlegt (`clickhouse_database`, z. B. Redis/MySQL), eine Pro-Tabelle-Engine, deren Spalten die Registry liefert (`clickhouse_table`, z. B. die JDBC-/ODBC-Brücke — der `engine_template` kann einen `{table}`-Platzhalter tragen, den die Laufzeit bindet), und eine Datei-/Lake-/URL-Engine, deren Schema ClickHouse ableitet (`clickhouse_scan`, z. B. HDFS/URL). SQLite (DATABASE-Engine, Datei, kein Server) und Hudi (Lakehouse, Zero-Copy) werden OOTB ausgeliefert. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Ein unbekannter `kind`-Wert schlägt beim Start laut fehl — ein Deskriptor-Tippfehler darf einen Quelltyp nicht stillschweigend unerreichbar machen. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Probe-Gating

Verfügbarkeit wird zur Attach-Zeit gegen den Standard-Discovery-Katalog jeder Engine geprüft:

- **Postgres** — prüft `pg_extension`, dann `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — führt `INSTALL`/`LOAD` aus und prüft `duckdb_functions()` auf das deklarierte `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — prüft `system.table_engines` auf die deklarierte `ch_engine`; fehlt sie im Build, schlägt es laut fehl. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Eine deklarierte Erweiterung, die nicht installierbar ist, schlägt laut fehl. Kein stiller Skip, kein Fallback. Ein Connector, dessen Probe fehlschlägt, ist für dieses Deployment schlicht nicht aktiv.

### Template-Variablen {: #template-variables }

Jeder `server_options`-Wert, `user_mapping`-Wert, `attach_template` und `scan_template` kann `{field}`-Platzhalter verwenden. Verfügbare Felder: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus jeder Key aus `federation_hints`. DuckDB-Attach-Templates erhalten außerdem `{alias}` — den internen Katalog-Alias, den Provisa der angehängten Datenbank zuweist.

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

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

Sobald einer der beiden Deskriptoren vorhanden ist, routet die Registrierung einer Quelle mit dem deklarierten `source_type` über den benutzerdefinierten Connector, vorbehaltlich einer erfolgreichen Probe. Keine weitere Konfigurationsänderung ist nötig.

---

## Warehouses als benannte Quellen {: #warehouses-as-named-sources }

Snowflake, Databricks und ClickHouse können unabhängig davon, welche Federation-Engine aktiv ist, als benannte Quellen registriert werden. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Bei Registrierung liest Provisa das Warehouse über den DirectDriver der Quelle und landet ein Replikat im Materialisierungs-Store der aktiven Engine. Die Abfrage läuft dann gegen dieses Replikat. Das unterscheidet sich vom traditionellen direktfähigen Pfad (asyncpg, aiomysql), bei dem die Engine vollständig umgangen wird — hier führt die Engine die Abfrage weiterhin aus, aber gegen ein lokales Replikat statt bei jeder Anfrage über die Leitung zum Warehouse.

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

Cloud-Objekt-/Lake-Daten (Parquet-, CSV-, Iceberg-, Delta-Lake-Dateien auf S3 / GCS / R2) sind ein separater Quelltyp, der vor Ort angehängt wird, wenn die aktive Engine einen ATTACH-Connector für diesen Typ hat. Kein Replikat wird gelandet — die Engine scannt den Objektspeicher direkt. Zugangsdaten für diese Quellen gehen ebenfalls in `federation_hints`:

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

Alle Quellen teilen eine gemeinsame Menge von Feldern. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

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
| `pool_min` | Nein | `1` | Minimale Connection-Pool-Größe (REQ-052) |
| `pool_max` | Nein | `5` | Maximale Connection-Pool-Größe (REQ-052) |
| `use_pgbouncer` | Nein | `false` | Verbindungen über PgBouncer routen (REQ-053) |
| `pgbouncer_port` | Nein | `6432` | PgBouncer-Port (REQ-053) |
| `cache_enabled` | Nein | `true` | API-Response-Caching aktivieren |
| `cache_ttl` | Nein | `null` | Cache-TTL in Sekunden; erbt globalen Standard, wenn null |
| `cache_catalog` | Nein | `null` | Föderierter Katalog für API-Cache; Standard ist der eigene Katalog der Quelle |
| `cache_schema` | Nein | `api_cache` | Schema innerhalb des Cache-Katalogs |
| `naming_convention` | Nein | `null` | Überschreibt die globale Namenskonvention für diese Quelle (REQ-194) |
| `federation_hints` | Nein | `{}` | An die Federation-Engine übergebene Session-Eigenschaften und erweiterte Verbindungsparameter für Warehouse-Quellen (REQ-278, REQ-281) |
| `mapping` | Nein | `{}` | Typspezifische Connector-Einstellungen für NoSQL- und SaaS-Quellen (z. B. SharePoint `auth_type`, Salesforce `auth_type`, Splunk `use_token`) (REQ-251) |
| `allowed_domains` | Nein | `[]` | Quelle auf bestimmte Domänen beschränken; leer = uneingeschränkt |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

---

## Kafka-Quellen {: #kafka-sources }

Kafka-Topics werden separat unter `kafka_sources` konfiguriert, verschlüsselt nach der Quell-`id` einer registrierten `kafka`-Quelle. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `topics[].default_window` | Standard-Zeitfenster für Windowed-Abfragen (z. B. `1h`) (REQ-148) |
| `topics[].columns` | Spaltendefinitionen für das Topic-Schema (REQ-150) |

---

## Spaltensichtbarkeit {: #column-visibility }

Das Feld `visible_to` auf jeder Spalte ist eine Liste von Rollen-IDs, die diese Spalte sehen können. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Spalten, die aus der `visible_to`-Liste einer Rolle ausgelassen sind, erscheinen nicht im GraphQL-Schema dieser Rolle und können nicht abgefragt oder in Filtern referenziert werden (REQ-039).

---

## Beziehungen {: #relationships }

Beziehungen verbinden zwei registrierte Tabellen und erscheinen als verschachtelte Felder in GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

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
| `cardinality` | Ja | `many-to-one` oder `one-to-many` (REQ-019) |
| `materialize` | Nein | Erstellt automatisch eine materialisierte Sicht für quellenübergreifende Joins (REQ-158). Bei einer junction-gestützten Kante deckt die Sicht die Zwei-Hop-Traversierung ab, nicht einen direkten Join (REQ-1586) |
| `refresh_interval` | Nein | MV-Refresh-Intervall in Sekunden (Standard: 300) |
| `target_function_name` | Nein | DB-Funktionsname für berechnete Beziehungen |
| `function_arg` | Nein | Welches Funktionsargument den Wert der Quellspalte erhält |
| `alias` | Nein | Menschenlesbarer Beziehungstyp (z. B. `WORKS_FOR`) |
| `graphql_alias` | Nein | Benennt das SDL-Feld, das diese Beziehung auf dem übergeordneten Typ offenlegt. Fehlt es, wird der Name aus dem `field_name` der Zieltabelle und der Beziehungskardinalität abgeleitet. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Nein | Bei `true` wird diese Beziehung von den Cypher-Graphkanten ausgeschlossen |
| `source_json_key` | Nein | Extrahiert diesen Key aus der Quellspalte als JSON-Objekt vor dem JOIN |
| `via_table` | Nein | Registrierter Tabellenname der Junction, die diese Kante durchläuft. Das Setzen macht die Kante junction-gestützt; leer lassen macht sie zu einer Fremdschlüsselkante (REQ-1586) |
| `via_source_column` | Nein | Junction-Spalte, gepaart mit `source_column`. Kommagetrennt und positionsbezogen bei einem zusammengesetzten Schlüssel |
| `via_target_column` | Nein | Junction-Spalte, gepaart mit `target_column` |
| `via_type_column` | Nein | Diskriminator-Spalte, wenn eine Junction mehrere Beziehungstypen trägt |
| `via_type_value` | Nein | Der Diskriminatorwert, auf den diese Kante festgelegt ist |
| `via_label_source` | Nein | Welche Nominierung den Cypher-Typ benennt: `column` (der Diskriminatorwert), `table` (der Tabellenname der Junction) oder `fixed` (der deklarierte Alias). Alle werden in Upper-Snake-Case umgewandelt |

### Junction-gestützte Beziehungen {: #junction-backed-relationships }

Eine assoziative Tabelle kann als erstklassige Cypher-Beziehung statt als Knoten deklariert werden, sodass ihre
eigenen Spalten zu den Attributen dieser Beziehung werden: (REQ-1586)

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

Die Junction ist eine registrierte Tabelle wie jede andere und muss registriert sein, bevor eine Beziehung sie
benennen kann. Deklarieren Sie sie einmal pro Diskriminatorwert: Drei Zeilen über `pet_companions` erzeugen
`BONDED_PAIR`, `LITTERMATE` und `SHARES_ENCLOSURE` als drei unterschiedliche Cypher-Typen, jeder mit den
verbleibenden Spalten der Junction-Zeile als Kanteneigenschaften. Die mitgelieferte Demo-Konfiguration
deklariert genau dies.

Eine Junction-Kante ist eine Cypher-Beziehung, kein GraphQL-Join-Feld: Der GraphQL-Join-Emitter baut
seine `ON`-Klausel für ein einzelnes Spaltenpaar und hat keinen Platz für den zweiten Hop, sodass Junction-Kanten
von der generierten SDL und von `pg_constraint` ausgeschlossen sind. [tool-verified: `provisa/compiler/schema_gen.py:304`]
Die Junction-Tabelle bleibt als eigenes Root-Feld abfragbar und wird aus der Knotenseite des
Cypher-Graph-Schemas entfernt, sodass sie nie als Knotenbezeichnung erscheint.

`materialize: true` funktioniert auf einer Junction-Kante, und was sie materialisiert, ist die Traversierung statt
eines direkten `pets`-zu-`pets`-Joins: Die Sicht hält den Quell-Hop, den Junction-Hop, den Diskriminator
und die eigenen Spalten der Junction neben denen des Ziels. Da die Junction ein drittes Bein des Joins ist,
wird beurteilt, ob die Kante quellenübergreifend ist, über alle drei Tabellen hinweg — eine Junction in einer anderen Quelle
als die beiden, die sie verbindet, wird materialisiert, selbst wenn diese beiden übereinstimmen. Eine Deklaration materialisiert einen
Kantentyp, sodass eine für `bonded pair` gebaute Sicht nie eine `littermate`-Traversierung beantwortet.

Kardinalitätswerte [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — jede Quellzeile bildet auf eine Zielzeile ab (FK auf PK)
- `one-to-many` — jede Quellzeile bildet auf mehrere Zielzeilen ab (Umkehrung des Obigen)

---

## Sicherheit-auf-Zeilenebene-Regeln {: #row-level-security-rules }

RLS-Regeln injizieren `WHERE`-Klauseln zur Abfragezeit, bezogen auf eine Rolle und optional auf eine Tabelle oder Domäne. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
    description: Analysts read only their own tenant's rows.
```

Existieren sowohl eine Regel auf Domänenebene als auch auf Tabellenebene für dieselbe Rolle, hat die Regel auf Tabellenebene Vorrang (REQ-403).

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `table_id` | Bedingt | Tabelle, auf die die Regel angewendet wird; schließt sich mit `domain_id` gegenseitig aus |
| `domain_id` | Bedingt | Domäne, auf die die Regel angewendet wird; gilt für alle Tabellen in der Domäne (REQ-402) |
| `role_id` | Ja | Rolle, für die diese Regel gilt |
| `filter` | Ja | SQL-Prädikat, injiziert in `WHERE`; kann Session-Variablen referenzieren (REQ-041) |
| `description` | Ja | Wofür die Regel da ist, so wie eine Zugriffsprüfung sie liest (REQ-2081) |

---

## Funktionen und Webhooks {: #functions-and-webhooks }

### DB-Funktionen {: #db-functions }

Tracken Sie eine Datenbankfunktion und legen Sie sie als GraphQL-Abfrage oder -Mutation offen. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Datenbankquellen können ihre gespeicherten Prozeduren und Funktionen auch automatisch aus dem Vendor-Katalog erkennen (`pg_proc`, `information_schema.routines`, oder Vendor-Äquivalente), wodurch die manuelle Registrierung jeder einzelnen entfällt. Discovery liest `prokind` und `provolatile`: unveränderliche/stabile Funktionen registrieren sich als parametrisierte Relationen (Prozedurargumente werden zu Abfrageparametern, dieselbe Form wie OpenAPI-GET-Tabellen), und volatile Prozeduren registrieren sich als Mutations/getrackte Funktionen. Erkannte Routinen durchlaufen die Stage-2-Governance identisch zu handregistrierten. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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
| `returns` | Ja | — | Registrierte Tabellen-ID, die die Funktion zurückgibt (REQ-207) |
| `arguments` | Nein | `[]` | Liste von `{name, type}`-Argumentdefinitionen (REQ-211) |
| `visible_to` | Nein | `[]` | Rollen, denen die Funktion zugewiesen ist (leer: jede Rolle). Eine `mutation` braucht zusätzlich das Recht `write` |
| `domain_id` | Nein | `""` | Domäne, zu der diese Funktion gehört |
| `description` | Nein | `null` | GraphQL-Feldbeschreibung |
| `kind` | Nein | `mutation` | `"query"` oder `"mutation"` (REQ-205) |

### Webhooks

Legen Sie einen externen HTTP-Endpunkt als GraphQL-Abfrage oder -Mutation offen. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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
| `inline_return_type` | Nein | `[]` | Liste von `{name, type}`-Feldern für benutzerdefinierte Rückgabeformen (REQ-210) |
| `arguments` | Nein | `[]` | Liste von `{name, type}`-Argumentdefinitionen |
| `visible_to` | Nein | `[]` | Rollen, die diesen Webhook aufrufen können |
| `domain_id` | Nein | `""` | Domäne, zu der dieser Webhook gehört |
| `description` | Nein | `null` | GraphQL-Feldbeschreibung |
| `kind` | Nein | `mutation` | `"query"` oder `"mutation"` |

---

## Authentifizierung {: #authentication }

Auth wird unter dem Key `auth` konfiguriert. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Provider | Beschreibung |
| ---------- | ------------- |
| `none` | Keine Authentifizierung; alle Anfragen werden als `default_role` behandelt |
| `firebase` | Firebase Authentication; erfordert `project_id` und `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | Generisches OAuth 2.0 (REQ-123) |
| `simple` | Benutzername/Passwort ohne externen Provider (REQ-124) |

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

`assignments_source: claims` liest Rollenzuweisungen aus JWT-Claims. `assignments_source: provisa` liest sie aus Provisas eigenem Zuweisungs-Store. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Ausführungs-Routing {: #execution-routing }

**Direkte Ausführung** — Einzelquellen-RDBMS-Abfragen routen zum nativen Treiber für Latenz unter 100 ms (REQ-027). Quellen benötigen sowohl einen `SOURCE_TO_DIALECT`-Eintrag als auch einen `SOURCE_TO_CONNECTOR`-Eintrag, um diesen Pfad zu unterstützen (REQ-229).

**Föderierte Ausführung** — Mehrquellen-Abfragen und Quellen ohne direkten Treiber routen über die Federation-Engine (REQ-028). Provisa enthält eine eingebettete Federation-Engine; verweisen Sie für großangelegte Deployments auf Ihren eigenen kompatiblen Cluster (REQ-226).

**Statistiken** — Bei der Registrierung führt Provisa `ANALYZE` gegen jede veröffentlichte Tabelle aus, um den kostenbasierten Optimizer zu initialisieren (Zeilenanzahlen, Null-Anteil, eindeutige Werte, Min/Max). Fehler werden protokolliert und blockieren die Registrierung nicht (REQ-275).

---

## Graph- & Semantic-Quellen {: #graph-semantic-sources }

### Neo4j

Registrieren Sie eine Neo4j-Graphdatenbank als abfragbare Quelle. Stewards verfassen Cypher-Abfragen, die skalare Werte projizieren; Provisa cacht Ergebnisse und legt sie als GraphQL-Typen offen (REQ-295).

Cypher-Abfragen müssen Property-Zugriffe in der `RETURN`-Klausel verwenden (`RETURN n.id AS id, n.name AS name`) — die Rückgabe von Knotenobjekten wird bei der Registrierung abgelehnt (REQ-296).

#### Konfigurationsdatei-Registrierung (REQ-1668) {: #config-file-registration-req-1668 }

Deklarieren Sie eine `neo4j`-Quelle und ihre Tabellen in YAML. Jede Tabelle benötigt ein `query_template` (das Cypher, das ihre Zeilen erzeugt) und typisierte Spalten. Der Key `query_template` ist unter jedem anderen Quelltyp ungültig. [tool-verified: `provisa/core/config_loader.py:456-511`]

Eine Quelle benötigt `host`, `port` und `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Zeilen werden abgerufen, indem `{"statements": [{"statement": <cypher>}]}` an `/db/<database>/tx/commit` gePOSTet wird (die Neo4j-HTTP-Transaktions-API). Eine Antwort mit nicht-leerer `errors`-Liste wird als fehlgeschlagene Abfrage behandelt, nicht als leeres Ergebnis. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Jede Spalte benötigt `data_type`. Der Loader bildet Konfigurationstypen auf den zur Abfragezeit verwendeten API-Spaltentyp ab [tool-verified: `provisa/neo4j/persist.py:31-64`]:

| Konfigurations-`data_type` | API-Typ |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` und `decimal(10,2)` werden akzeptiert — der Basistyp vor der Klammer wird verwendet.

Die Registrierung persistiert eine `api_sources`-Zeile und eine `api_endpoints`-Zeile pro Tabelle, sodass die Tabellen einen Neustart überstehen, ohne die Datei erneut zu lesen. Die Admin-REST-Endpunkte unter `/admin/sources/neo4j` schreiben dieselben Zeilen. [tool-verified: `provisa/neo4j/persist.py:77-120`]

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

#### Register Table in der UI (REQ-1670) {: #register-table-in-the-ui-req-1670 }

Eine neo4j-Quelle hat keine Tabellen zum Auflisten, daher fragt das Register-Table-Formular nach der Tabelle, statt eine anzubieten. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Wählen Sie die neo4j-Quelle und eine Domäne. Die Schema- und Tabellenauswahl, die Discover-Checkbox und die Watermark-Auswahl erscheinen nicht; die Quelle wird nie introspiziert.
2. Geben Sie einen Tabellennamen und das Cypher ein. Das Cypher muss Skalare projizieren (`RETURN a.name AS name`); eine Projektion, die einen Knoten oder eine Liste zurückgibt, wird als Fehler gemeldet.
3. Drücken Sie Preview. Das Formular führt das Cypher mit `LIMIT 5` über die `neo4jPreview`-GraphQL-Abfrage aus und befüllt die Spaltenliste aus den zurückkommenden Zeilen, typisiert als `text`, `integer`, `double`, `boolean` oder `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Eine fehlgeschlagene Vorschau lässt das Cypher im Editor und zeigt die Meldung.
4. Passen Sie Sichtbarkeit, Aliase oder Maskierung wie bei jeder Tabelle an und registrieren Sie dann. Das Formular verweigert das Absenden, bis eine Vorschau die Spalten typisiert hat, und der Server lehnt eine neo4j-Tabelle ohne Cypher ab (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

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

Der Preview-Endpunkt (`POST /admin/sources/neo4j/{id}/preview`) gibt Beispielzeilen zurück und blockiert die Registrierung, wenn das Cypher Knotenobjekte zurückgibt (REQ-296).

### SPARQL

Registrieren Sie jeden SPARQL-1.1-konformen Triple-Store (Apache Jena Fuseki, Virtuoso, Stardog usw.) als abfragbare Quelle (REQ-297).

Abfragen müssen `SELECT`-Abfragen sein. Variablennamen in der `SELECT`-Klausel werden automatisch zu Spaltennamen (REQ-297).

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

Beide Connectors verwenden die API-Source-Cache-Pipeline — Ergebnisse werden in PostgreSQL mit konfigurierbarer TTL gespeichert, wodurch sie für quellenübergreifende föderierte JOINs verfügbar werden (REQ-295, REQ-297, REQ-299).

---

#### Konfigurationsdatei- und UI-Registrierung (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

Der `host` einer `sparql`-Quelle ist ihre SPARQL-Endpunkt-URL (das Sources-Formular speichert es auf dieselbe Weise). Jede Tabelle darunter trägt `query_template`, ein SELECT, dessen Variablen die Spalten sind; jede Bindung ist `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

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

Register Table funktioniert genauso wie bei Neo4j: Wählen Sie die Quelle, geben Sie einen Tabellennamen und das SELECT ein, drücken Sie Preview (die `sparqlPreview`-Abfrage führt es mit `LIMIT 5` aus und befüllt die Spaltenliste), dann registrieren. Die Registrierung persistiert eine `api_sources`-Zeile und eine `api_endpoints`-Zeile (formularkodiertes POST an den Endpunktpfad, `sparql_bindings`-Normalisierer), dieselben Zeilen, die eine Konfigurationsdatei-Registrierung schreibt, und die native Engine landet die Zeilen über dieselbe Fetch-Kette wie Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

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

Einzelquellen-Anteile routen direkt (REQ-027). Quellenübergreifende JOINs föderieren mit automatischer Typkonvertierung (REQ-028, REQ-552).
