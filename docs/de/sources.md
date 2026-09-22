# Quelltypen

## Ausführungsmodell

Jede Abfrage wird letztlich über die Federation-Engine ausgeführt, die Federation über alle Quellen hinweg bereitstellt. Quellen fallen basierend auf ihrer Konnektivität in drei Kategorien. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Kategorie | Hat direkten Treiber | Hat föderierten Connector | Beispiele |
| --- | --- | --- | --- |
| **Direktfähig** | Ja | Ja | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Nur Federation** | Nein | Ja | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-gestützt) |
| **Direktlesen (Replikat)** | Ja | Ja | Snowflake, Databricks, ClickHouse — Treiber liest Daten und landet ein Replikat; Abfragen laufen gegen das Replikat in der aktiven Engine |
| **Materialisieren → Federation** | Nein | Nein | REST/OpenAPI, Remote-GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (Push-Empfänger), GovData, SharePoint, Splunk |

**Direktfähige** Quellen führen Einzelquellen-Abfragen über ihren nativen Treiber aus (unter 100 ms) und umgehen dabei die Federation-Engine (REQ-027, REQ-229). Sie behalten volle Connector-Unterstützung und nehmen an der Federation teil, wenn sie mit anderen Quellen gejoint werden (REQ-028).

**Nur-Federation**-Quellen werden immer über die Federation-Schicht abgefragt. Es existiert kein direkter Treiber (REQ-229).

**Direktlesen (Replikat)**-Quellen haben einen DirectDriver, der nativ aus dem Warehouse liest (wo verfügbar Arrow-nativ), ein Replikat in den Materialisierungs-Store der aktiven Engine landet, und Abfragen laufen dann gegen dieses Replikat. Siehe [Warehouses als benannte Quellen](#warehouses-as-named-sources).

**Materialize**-Quellen haben keinen föderierten Connector. Provisa ruft ihre Daten ab (beim Start oder zur Abfragezeit) und cacht sie als Parquet in S3 oder in PostgreSQL, wodurch sie für quellenübergreifende Abfragen durch die Federation-Engine erreichbar werden (REQ-309).

---

## Alle Quellen

Provisa registriert **54** Quelltypen. Die folgenden Tabellen decken alle 54 ab; der Index ist die Zählung. [tool-verified: `provisa/core/models.py` `SourceType`; Kaggle zählt als eigenständige Quelle, obwohl es intern über den `files`-Connector registriert]

| # | Gruppe | Quelltypen |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Cloud-Data-Warehouses](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Data-Lake / offene Tabellenformate](#data-lake-offene-tabellenformate) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Push-Empfänger](#push-empfänger) | `ingest` |
| 38–39 | [Graph & Semantic](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [Dateibasiert](#dateibasiert) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Observability & Sonstiges](#observability-sonstiges) | `google_sheets`, `prometheus` |
| 46–47 | [Enterprise-SaaS](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [API-Quellen](#api-quellen) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Data-Quality-Checker](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 54 | [Kaggle-Datasets](#kaggle-datasets) | Kaggle (über das Sources-Formular bereitgestellt; registriert als `files`-Quelle — siehe [Kaggle-Datasets](#kaggle-datasets)) |

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

Wire-kompatible Datenbanken verwenden den JDBC-Treiber, den nativen Async-Treiber und den Dialekt eines Basis-Wire-Protokolls wieder — CockroachDB, YugabyteDB und Greenplum reiten auf dem PostgreSQL-Wire; TiDB reitet auf dem MySQL-Wire. Sie benötigen nur Registry-Einträge, keinen neuen Connector-Code. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) und `airport` (Arrow-Flight-Server) sind registrierte Quelltypen, die vor Ort über DuckDB-Community-Erweiterungen erreicht werden, wenn DuckDB die aktive Engine ist — kein direkter Treiber, kein föderierter Connector. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Cloud-Data-Warehouses

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federiert | Liest über snowflake-connector-python; landet Replikat; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federiert | Kein DirectDriver; erreicht über Federation-Engine oder BigQuery-Engine-ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federiert | Liest über databricks-sql-connector (Cloud Fetch, Arrow); landet Replikat; `http_path` erforderlich in `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federiert | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federiert | Microsoft Fabric Warehouse; T-SQL über TDS, Azure-AD-Auth; landet Replikat (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federiert | Azure Synapse SQL; T-SQL über TDS, Azure-AD-Auth; landet Replikat (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federiert | Entfernter Trino-/Presto-Koordinator, gelesen über den SQLAlchemy-Trino-Dialekt; landet Replikat auf jeder Engine (REQ-994) |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutations | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federiert | Liest über clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` für TLS (REQ-986) |
| `druid` | — | druid | druid | Nein | — |
| `exasol` | — | exasol | exasol | Nein | — |
| `elasticsearch` | HTTP (native Engines) | elasticsearch (Trino) | — | Nein | Auf Trino liest der Connector sie, seine Eigenschaften aus der Mapping-DSL des Typs [tool-verified: `trino_connectors.py:309`]; auf jeder anderen Engine liest Provisa den Index über HTTP (Indizes und Mapping für Register Table, ein Scroll-Read zum Landen) und landet die Zeilen [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | Nein | Trino-`pinot`-Connector; `pinot.controller-urls` = Host:Port des Pinot-Controllers [tool-verified: `trino_connectors.py:199`] |

### Data-Lake / offene Tabellenformate

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

### Streaming

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Föderierter Kafka-Connector; Schema über Confluent Schema Registry (Avro, Protobuf, JSON Schema), manuelle Definition oder Stichproben-Inferenz (REQ-147, REQ-150) | Nur Sink (REQ-176) |
| `websocket` | Externer WebSocket-Feed — verbinden, abonnieren, Ereignisse empfangen; Ergebnisse materialisiert (REQ-338) | Nein |
| `rss` | RSS-2.0-/Atom-Feed — pollen, Watermark nach pubDate/updated; Ergebnisse materialisiert (REQ-342, REQ-343) | Nein |

### Push-Empfänger {: #push-empfänger }

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | Externe Dienste POSTen JSON-Ereignisse; Ergebnisse materialisiert (REQ-331, REQ-335) | Nein |

### Graph & Semantic

| Quelltyp | Mechanismus | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher über HTTP-API, Ergebnisse in PostgreSQL gecacht (REQ-295) | Nein |
| `sparql` | SPARQL-1.1-POST, Ergebnisse in PostgreSQL gecacht (REQ-297) | Nein |

### Dateibasiert

Zwei Mechanismen decken Dateien ab. Beide verwenden das Feld `path` statt `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Einzeldatei-Quellen** — `sqlite`, `csv`, `parquet` richten `path` auf eine Datei.

| Quelltyp | Transporte | Mutations |
| --- | --- | --- |
| `sqlite` | lokal | Ja |
| `csv` | lokal | Nein |
| `parquet` | lokal, `s3://` | Nein |

Private Buckets benötigen Zugangsdaten (AWS-Region und Keys aus der Umgebung). Für CSV über `s3://` oder `http(s)://`, oder um viele Dateien auf einmal zu registrieren, verwenden Sie die `files`-Quelle. [tool-verified: `provisa/file_source/source.py`]

**`files`-Quelle** — richtet `path` auf ein Glob-Muster, durchläuft es rekursiv und registriert das Verzeichnis als föderierten Katalog von Tabellen. Sie liest viele Formate über viele Transporte; die untenstehenden Mengen stammen vom File-Connector (kenstott/calcite-Fork). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; Format- und Transportlisten vom Calcite-`file`-Adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formate | Transporte |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow und in Tabellen konvertierte Dokumente — HTML, Markdown, DOCX, PPTX | Lokales Dateisystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST und Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Auf der DuckDB-Engine wird `files` nativ gelesen — eine `read_csv_auto`-Scanner-View pro `<table>.csv` unter dem aufgelösten Verzeichnis (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. Auf einer Engine ohne eigenen `files`-Connector landen Zeilen über denselben connector-gebündelten Calcite-pgwire-Server (`pgwire-file`), den auch sharepoint/splunk verwenden (REQ-954) — siehe [Enterprise-SaaS-Connectors](#enterprise-saas-connectors) unten. Die End-to-End-UI-Abdeckung (Sources-Formular → Register Table → SQL-Abfrage) und der pgwire-Landepfad sind in REQ-1694 nachgewiesen.

#### Kaggle-Datasets (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle ist eine Datei-Download-Plattform. Ein bereitgestellter Kaggle-Datensatz registriert sich als Quelle vom Typ `files` und wird über denselben pgwire-file-Connector abgefragt, den jede andere `files`-Quelle verwendet — es gibt keinen `kaggle`-SourceType im Enum. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — kein `kaggle`-Literal]

**Einen Datensatz hinzufügen.** Öffnen Sie Sources → Subscriptions → Kaggle. Das Formular durchläuft zwei sequenzielle Schritte, da Kaggles eigener Dataset-Suchendpunkt Auth erfordert — die Auswahl kann nicht erscheinen, bevor das Token validiert ist. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Token** — geben Sie ein Kaggle-API-Token ein und klicken Sie auf „Validate". Die Validierung ruft `POST https://www.kaggle.com/api/v1/datasets/create/new` mit leerem Body auf. Ein `401` bedeutet ungültig; jede andere Antwort bedeutet gültig (Kaggles eigene Payload-Validierung greift, bevor irgendein Datensatz erstellt wird — nichts wird persistiert). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Auswahl** — ein Live-Search-as-you-type-Feld fragt `GET /api/v1/datasets/list` ab. Jedes Ergebnis zeigt Dataset-Titel und -Beschreibung. Wählen Sie eines aus und klicken Sie auf „Add Dataset".

Ein Klick auf „Add Dataset" lädt das Bundle von `GET /api/v1/datasets/download/{owner}/{ref}` herunter, entpackt CSV- und Parquet-Mitglieder nach `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` und erstellt **eine** Quelle vom Typ `files`, deren `path` dieses Wurzelverzeichnis ist. Das Token wird serverseitig nie gespeichert. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Nach dem Hinzufügen der Quelle registrieren Sie deren Tabellen über den normalen Register-Table-Bildschirm. Die rekursive Verzeichniserkennung des pgwire-file-Connectors listet jede Datei als eigene Tabelle — derselbe Mechanismus, den jede andere `files`-Quelle verwendet (REQ-1690). (REQ-1783)

**v1-Einschränkung.** Ein Bundle mit einer `.sqlite`- oder `.db`-Datei wird rundweg mit einer klaren Fehlermeldung abgelehnt. Nur CSV- und Parquet-Dateien werden bereitgestellt. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Tabellenbenennung.** Jede Datei landet in ihrem eigenen `<file-stem>/`-Unterverzeichnis unter der Dataset-Wurzel. Der pgwire-file-Connector benennt die resultierende Tabelle `<subdir>__<stem>` nach SMART_CASING-Normalisierung. Zum Beispiel: `StatewiseTestingDetails.csv` landet unter `statewise_testing_details/StatewiseTestingDetails.csv` und wird zur Tabelle `statewise_testing_details__statewise_testing_details`. Der verdoppelte Stem ist bei Einzeldatei-Datasets zu erwarten. Ein Multi-Datei-Dataset erzeugt ein Paar pro Datei: `orders__orders`, `customers__customers`. (REQ-471)

**Aktualisieren.** Um einen Datensatz erneut abzurufen, nachdem Kaggle eine neue Version veröffentlicht hat, rufen Sie die GraphQL-Mutation `refreshKaggleSource` mit der Quell-ID und einem gültigen Token auf. Dies stellt die Dateien vor Ort erneut bereit und leert den pgwire-file-Endpunkt-Cache, sodass der Connector bei seiner nächsten Abfrage etwaige Schemaänderungen übernimmt. Die zur Erstellungszeit in `federation_hints` gespeicherten `kaggle_owner` und `kaggle_ref` identifizieren, welcher Datensatz erneut abgerufen werden soll. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Kein statischer YAML-Konfigurationspfad.** Kaggle-Quellen werden nur über das Sources-Formular erstellt. Eine nach YAML exportierte Kaggle-Quelle erscheint als `type: files` mit `kaggle_owner` und `kaggle_ref` in `federation_hints`. Ein erneuter Download von Kaggle erfordert den UI-Refresh-Flow oder die Mutation `refreshKaggleSource` — das Ausrichten des YAML-`path` auf ein vorab bereitgestelltes Verzeichnis ist die Alternative für air-gapped Umgebungen.


### Observability & Sonstiges

`prometheus` hat einen Trino-Connector (Eigenschaften aus der Mapping-DSL des Typs gebaut). `google_sheets` ist ein registrierter Quelltyp ohne Trino-Connector und materialisiert über die API-Cache-Pipeline. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Quelltyp | Connector-Name | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialisiert) | Nein |
| `prometheus` | prometheus | Nein | Eine Metrik ist eine Tabelle, und ein Sample ist eine Zeile (`timestamp`, `value`, eine Spalte pro Label); auf jeder Engine ohne Live-Connector liest Provisa die HTTP-API — Metriknamen und Labels für Register Table, `query_range` über den Bereich der Tabelle zum Landen [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689)

### Enterprise-SaaS-Connectors

SharePoint und Splunk registrieren sich über Apache-Calcite-Connectors (kenstott/calcite-Fork). Keiner hat einen direkten Treiber — Provisa startet den mitgelieferten Calcite-pgwire-Server des Connectors (`pgwire-sharepoint`, `pgwire-splunk`) und erreicht ihn als generischen PostgreSQL-Endpunkt. Auf der DuckDB-Engine wird dieser Endpunkt live über die postgres-Erweiterung angehängt: Register Table listet die Tabellen des Connectors aus dem angehängten Katalog, Abfragen lesen den Connector vor Ort, und Filter und Projektionen werden zu Calcite heruntergeschoben (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Jede andere Engine landet die Zeilen im Materialisierungs-Store für die Federation (REQ-954). Die Bundles werden pro OS/Architektur aus dem gepinnten `kenstott/calcite`-Release abgerufen (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`]. Beide Connectors aktivieren immer Groß-/Kleinschreibung-unabhängigen Namensabgleich, entsprechend der eigenen case-insensitiven Semantik des jeweiligen Produkts (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

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

### API-Quellen

Registrieren Sie jeden HTTP-Endpunkt als abfragbare Tabelle. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| API-Typ | Discovery | Spaltenableitung |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI-Spec-Parsing (REQ-314, REQ-316) | Primitive → nativ, Objekte → JSONB |
| `graphql_remote` | Schema-Introspektion (REQ-307, REQ-308) | Primitive → nativ, Objekte → JSONB |
| `grpc_remote` | Server Reflection (REQ-322, REQ-325) | Primitive → nativ, Objekte → JSONB |

API-Antworten werden abgerufen, in PostgreSQL gecacht (konfigurierbare TTL) und als GraphQL-Typen offengelegt (REQ-309, REQ-318, REQ-327). Gecachte Tabellen nehmen wie jede andere Quelle an föderierten Abfragen teil (REQ-313).

**JSONB-Regeln**: Komplexe Spalten (Objekte, Arrays), die als JSONB gespeichert werden, sind nicht filterbar (REQ-119). Der Zugriff auf Unterfelder verwendet die `->>`-Extraktion in SQL (REQ-151). Beziehungen werden zwischen Tabellen über skalare FK-Spalten deklariert — JSONB-Blob-Spalten sind keine Join-Ziele. Verwenden Sie JSONB-Promotion, um verschachtelte Felder in native skalare Spalten umzuwandeln, wenn Filtern oder Joinen darauf benötigt wird (REQ-119).

### GovData

Offene Daten der US-Regierung. Der Zugriff ist nach Themengruppierung partitioniert. [tool-verified: `provisa/core/models.py` lines 543–609]

Jede `govdata`-Quelle wählt ein Thema. Dieses Thema bestimmt, welche GovData-Schemas offengelegt werden. Die Schemas `ref` und `geo` sind immer als Linker-Schemas enthalten — sie werden nicht pro Thema aufgeführt, sind aber immer vorhanden. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Thema | Offengelegte Schemas |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | Jedes obige Schema |

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

### Wo das Passwort einer Quelle lebt

Das Passwort einer Quelle wird nie neben den übrigen Verbindungseinstellungen gespeichert. Die
`sources`-Zeile der Control Plane trägt eine `password_ref`-Spalte mit einer *Referenz* —
`${env:PG_PASSWORD}`, `${secret:SNOWFLAKE_KEY}` —, die im Moment aufgelöst wird, in dem die Quelle
angewählt wird, innerhalb der Organisation, in deren Namen die Anfrage läuft (REQ-1695). [tool-verified:
`provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

`${env:VAR}` liest die Prozessumgebung des Deployments und benötigt keine Bindung. `${secret:NAME}`
benennt ein Secret, das einer Organisation gehört, sodass es nur innerhalb der eigenen Operationen
dieser Organisation aufgelöst wird: die Admin-Introspektions-Nahtstellen und das Query-Terminal, das
jede Oberfläche erreicht, stellen diese Bindung her. [tool-verified: `provisa/pgwire/_pipeline.py` `_execute_plan`]

Wohin die Referenz zeigt, hängt davon ab, wie die Quelle registriert wurde:

- **Aus der Konfiguration.** Sie schreiben die Referenz selbst. `${env:VAR}` liest die Prozessumgebung
  des Deployments; `${secret:NAME}` liest den Vault der Organisation (siehe [Secrets](secrets.md)). Die
  Datei ist der Datensatz, und Provisa kopiert die Referenz wortwörtlich in `password_ref`.
- **Aus dem Sources-Formular.** Eine in das Passwortfeld eingegebene Referenz wird ebenfalls wortwörtlich
  gespeichert. Ein *literales* Passwort wird unter `source_<id>_password` in den Vault der Organisation
  geschrieben — verschlüsselt gespeichert und nie namentlich zurücklesbar —, und die Zeile behält das
  `${secret:source_<id>_password}`, das es benennt. [tool-verified:
  `provisa/api/admin/schema_common.py` `persist_source_password`]

Das erneute Eintippen des Passworts bei einer bestehenden Quelle rotiert diesen einen Vault-Eintrag,
statt einen zweiten zu erstellen. Das Löschen der Quelle entfernt den Eintrag, den Provisa dafür
geprägt hat, und nur diesen einen: Eine Referenz, die Sie selbst geschrieben haben, benennt ein
Secret, das Ihnen aus eigenen Gründen gehört, also bleibt sie unangetastet.
[tool-verified: `provisa/api/admin/schema_mutation.py` `delete_source`]

`password_ref` wandert nicht zwischen Umgebungen (REQ-1491). Ein Branch oder eine kopierte Umgebung
liefert ihre eigenen Verbindungswerte, und der Vault, den eine Referenz benennt, gehört zu welcher
Umgebung auch immer sie geliefert hat. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

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

## Benutzerdefinierte Connectors (REQ-1177)

Die nativen Federation-Engines — Postgres, DuckDB und ClickHouse — gewinnen Erreichbarkeit zu einem neuen Quelltyp, wenn ein Betreiber dafür einen Connector in `config/custom_connectors.yaml` deklariert. Kein Code erforderlich. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Connector-Erweiterbarkeit selbst geht dem voraus. Die Trino-Engine ist auf ihrer eigenen Schicht seit Langem erweiterbar — ein generischer JDBC-Connector, parametrisiert pro Quelltyp, ein Katalog-`.properties`-Body pro Typ, und Provisas eigene benutzerdefinierte Trino-Connector-Plugins (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 bringt dieselbe konfigurationsgesteuerte Erweiterbarkeit zu den zwei nativen, clusterlosen Engines, die zuvor eine feste Connector-Menge trugen.

Die Konfiguration wird leer ausgeliefert. Eingebaute Connectors decken die Reichweite von Haus aus ab; alles in dieser Datei ist vom Betreiber verfasst. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Setzen Sie `PROVISA_CUSTOM_CONNECTORS`, um auf einen anderen Pfad zu verweisen (nützlich für Tests).

### Deskriptor-Arten

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

### Template-Variablen

Jeder `server_options`-Wert, `user_mapping`-Wert, `attach_template` und `scan_template` kann `{field}`-Platzhalter verwenden. Verfügbare Felder: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus jeder Key aus `federation_hints`. DuckDB-Attach-Templates erhalten außerdem `{alias}` — den internen Katalog-Alias, den Provisa der angehängten Datenbank zuweist.

Ein Template, das ein unbekanntes Feld referenziert, schlägt zur Attach-Zeit laut fehl und macht eine Deskriptor-/Quell-Fehlpassung sichtbar, bevor defektes DDL die Engine erreicht.

### Beispiele

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

## Quellkonfigurationsfelder

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
| `mapping` | Nein | `{}` | Typspezifische Connector-Einstellungen für NoSQL- und SaaS-Quellen (z. B. SharePoint `auth_type`, Splunk `use_token`) (REQ-251) |
| `allowed_domains` | Nein | `[]` | Quelle auf bestimmte Domänen beschränken; leer = uneingeschränkt |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

---

## Kafka-Quellen

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

## Spaltensichtbarkeit

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

## Beziehungen

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

### Junction-gestützte Beziehungen

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

## Sicherheit-auf-Zeilenebene-Regeln

RLS-Regeln injizieren `WHERE`-Klauseln zur Abfragezeit, bezogen auf eine Rolle und optional auf eine Tabelle oder Domäne. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Existieren sowohl eine Regel auf Domänenebene als auch auf Tabellenebene für dieselbe Rolle, hat die Regel auf Tabellenebene Vorrang (REQ-403).

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `table_id` | Bedingt | Tabelle, auf die die Regel angewendet wird; schließt sich mit `domain_id` gegenseitig aus |
| `domain_id` | Bedingt | Domäne, auf die die Regel angewendet wird; gilt für alle Tabellen in der Domäne (REQ-402) |
| `role_id` | Ja | Rolle, für die diese Regel gilt |
| `filter` | Ja | SQL-Prädikat, injiziert in `WHERE`; kann Session-Variablen referenzieren (REQ-041) |

---

## Funktionen und Webhooks

### DB-Funktionen

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
| `visible_to` | Nein | `[]` | Rollen, die diese Funktion aufrufen können |
| `writable_by` | Nein | `[]` | Rollen, die dies als Mutation aufrufen können |
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

## Authentifizierung

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

## Ausführungs-Routing

**Direkte Ausführung** — Einzelquellen-RDBMS-Abfragen routen zum nativen Treiber für Latenz unter 100 ms (REQ-027). Quellen benötigen sowohl einen `SOURCE_TO_DIALECT`-Eintrag als auch einen `SOURCE_TO_CONNECTOR`-Eintrag, um diesen Pfad zu unterstützen (REQ-229).

**Föderierte Ausführung** — Mehrquellen-Abfragen und Quellen ohne direkten Treiber routen über die Federation-Engine (REQ-028). Provisa enthält eine eingebettete Federation-Engine; verweisen Sie für großangelegte Deployments auf Ihren eigenen kompatiblen Cluster (REQ-226).

**Statistiken** — Bei der Registrierung führt Provisa `ANALYZE` gegen jede veröffentlichte Tabelle aus, um den kostenbasierten Optimizer zu initialisieren (Zeilenanzahlen, Null-Anteil, eindeutige Werte, Min/Max). Fehler werden protokolliert und blockieren die Registrierung nicht (REQ-275).

---

## Graph- & Semantic-Quellen

### Neo4j

Registrieren Sie eine Neo4j-Graphdatenbank als abfragbare Quelle. Stewards verfassen Cypher-Abfragen, die skalare Werte projizieren; Provisa cacht Ergebnisse und legt sie als GraphQL-Typen offen (REQ-295).

Cypher-Abfragen müssen Property-Zugriffe in der `RETURN`-Klausel verwenden (`RETURN n.id AS id, n.name AS name`) — die Rückgabe von Knotenobjekten wird bei der Registrierung abgelehnt (REQ-296).

#### Konfigurationsdatei-Registrierung (REQ-1668)

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

#### Register Table in der UI (REQ-1670)

Eine neo4j-Quelle hat keine Tabellen zum Auflisten, daher fragt das Register-Table-Formular nach der Tabelle, statt eine anzubieten. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Wählen Sie die neo4j-Quelle und eine Domäne. Die Schema- und Tabellenauswahl, die Discover-Checkbox und die Watermark-Auswahl erscheinen nicht; die Quelle wird nie introspiziert.
2. Geben Sie einen Tabellennamen und das Cypher ein. Das Cypher muss Skalare projizieren (`RETURN a.name AS name`); eine Projektion, die einen Knoten oder eine Liste zurückgibt, wird als Fehler gemeldet.
3. Drücken Sie Preview. Das Formular führt das Cypher mit `LIMIT 5` über die `neo4jPreview`-GraphQL-Abfrage aus und befüllt die Spaltenliste aus den zurückkommenden Zeilen, typisiert als `text`, `integer`, `double`, `boolean` oder `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Eine fehlgeschlagene Vorschau lässt das Cypher im Editor und zeigt die Meldung.
4. Passen Sie Sichtbarkeit, Aliase oder Maskierung wie bei jeder Tabelle an und registrieren Sie dann. Das Formular verweigert das Absenden, bis eine Vorschau die Spalten typisiert hat, und der Server lehnt eine neo4j-Tabelle ohne Cypher ab (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

Das Cypher wird mit der Tabelle als `queryTemplate` gespeichert, erscheint in der Lese-Ansicht der Tabelle und persistiert genau wie eine Konfigurationsdatei-Registrierung: eine `api_sources`-Zeile und eine `api_endpoints`-Zeile, die der nächste Start hydriert. Das Bearbeiten der Tabelle persistiert ein bearbeitetes Cypher erneut.

#### Admin-REST-Registrierung

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

#### Konfigurationsdatei- und UI-Registrierung (REQ-1683)

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

## Verbindungsbeispiele

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

### Quellenübergreifende Abfrage

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
