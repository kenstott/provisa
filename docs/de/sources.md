# Quellentypen

## Ausführungsmodell

Jede Abfrage wird letztlich über die Föderations-Engine ausgeführt, die die Föderation über alle Quellen hinweg bereitstellt. Quellen fallen anhand ihrer Konnektivität in drei Kategorien. [tool-verified: `provisa/core/models.py` Zeilen 84–132] (REQ-550)

| Kategorie | Hat direkten Treiber | Hat föderierten Connector | Beispiele |
| --- | --- | --- | --- |
| **Direktfähig** | Ja | Ja | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Nur Föderation** | Nein | Ja | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-gestützt) |
| **Direktlesend (Replika)** | Ja | Ja | Snowflake, Databricks, ClickHouse — Treiber liest Daten und legt eine Replika an; Abfragen laufen gegen die Replika in der aktiven Engine |
| **Materialisieren → Föderation** | Nein | Nein | REST/OpenAPI, Remote-GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (Push-Receiver), GovData, SharePoint, Splunk |

**Direktfähige** Quellen führen Einzelquellen-Abfragen über ihren nativen Treiber aus (unter 100 ms) und umgehen die Föderations-Engine (REQ-027, REQ-229). Sie behalten volle Connector-Unterstützung und nehmen an der Föderation teil, wenn sie mit anderen Quellen verbunden werden (REQ-028).

**Nur-Föderation**-Quellen werden immer über die Föderationsschicht abgefragt. Es existiert kein direkter Treiber (REQ-229).

**Direktlesende (Replika)**-Quellen haben einen DirectDriver, der nativ aus dem Warehouse liest (wo verfügbar Arrow-nativ), eine Replika im Materialisierungs-Store der aktiven Engine anlegt, und Abfragen laufen dann gegen diese Replika. Siehe [Warehouses als benannte Quellen](#warehouses-als-benannte-quellen).

**Materialisieren**-Quellen haben keinen föderierten Connector. Provisa ruft ihre Daten ab (beim Start oder zum Abfragezeitpunkt) und cacht sie als Parquet in S3 oder in PostgreSQL, wodurch sie für die Föderations-Engine bei quellenübergreifenden Abfragen erreichbar werden (REQ-309).

---

## Alle Quellen

Referenz für jeden von Provisa unterstützten Quellentyp. „Direkter Treiber" bedeutet, dass Einzelquellen-Abfragen nativ gegen die Quelle ausgeführt werden (unter 100 ms) (REQ-027). „Connector-Name" ist der föderierte Connector, der verwendet wird, wenn die Quelle an mehrquellen-JOINs teilnimmt (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| Quellentyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Ja |
| `mysql` | aiomysql | mysql | mysql | Ja |
| `mariadb` | aiomysql | mariadb | mysql | Ja |
| `singlestore` | — | singlestore | singlestore | Föderiert |
| `sqlserver` | aioodbc | sqlserver | tsql | Ja |
| `oracle` | oracledb | oracle | oracle | Ja |
| `duckdb` | duckdb | memory | duckdb | Ja |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Ja |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Ja |

Wire-kompatible Datenbanken nutzen den JDBC-Treiber, nativen asynchronen Treiber und Dialekt eines Basis-Protokolls wieder — CockroachDB, YugabyteDB und Greenplum nutzen das PostgreSQL-Wire-Protokoll; TiDB nutzt das MySQL-Wire-Protokoll. Sie benötigen nur Registrierungseinträge, keinen neuen Connector-Code. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) und `airport` (Arrow-Flight-Server) sind registrierte Quellentypen, die über DuckDB-Community-Extensions direkt erreicht werden, wenn DuckDB die aktive Engine ist — kein direkter Treiber, kein föderierter Connector. [tool-verified: `provisa/core/models.py` Zeilen 44, 93] (REQ-899)

### Cloud Data Warehouses

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Quellentyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Föderiert | Liest über snowflake-connector-python; legt Replika an; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Föderiert | Kein DirectDriver; erreicht über Föderations-Engine oder BigQuery-Engine-ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Föderiert | Liest über databricks-sql-connector (Cloud Fetch, Arrow); legt Replika an; `http_path` in `federation_hints` erforderlich (REQ-987) |
| `redshift` | — | redshift | redshift | Föderiert | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Föderiert | Microsoft Fabric Warehouse; T-SQL über TDS, Azure-AD-Auth; legt Replika an (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Föderiert | Azure Synapse SQL; T-SQL über TDS, Azure-AD-Auth; legt Replika an (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Föderiert | Remote-Trino/Presto-Coordinator gelesen über den SQLAlchemy-Trino-Dialekt; legt Replika auf jeder Engine an (REQ-994) |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Quellentyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen | Hinweise |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Föderiert | Liest über clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` für TLS (REQ-986) |
| `druid` | — | druid | druid | Nein | — |
| `exasol` | — | exasol | exasol | Nein | — |
| `elasticsearch` | — | elasticsearch | — | Nein | Connector-Eigenschaften stammen aus der Mapping-DSL des Typs [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Nein | Trino-`pinot`-Connector; `pinot.controller-urls` = host:port des Pinot-Controllers [tool-verified: `trino_connectors.py:199`] |

### Data Lake / Offene Tabellenformate

Diese Quellentypen sind Nur-Föderation — kein direkter Treiber, kein Dialekt. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Quellentyp | Connector-Name | Time Travel | Hinweise |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Ja (Argument `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Ja (Argument `as_of`, REQ-372) | — |
| `hive` | hive | Nein | — |
| `hive_s3` | hive | Nein | S3-gestütztes Hive |

### NoSQL

`mongodb`, `cassandra` und `redis` haben Trino-Connectoren (`redis` baut seine Eigenschaften aus der Mapping-DSL des Typs). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Quellentyp | Connector-Name | Mutationen |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Nein |
| `cassandra` | cassandra | Nein |
| `redis` | redis | Nein |

### Streaming

| Quellentyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `kafka` | Föderierter Kafka-Connector; Schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), manuelle Definition oder Sample-Inferenz (REQ-147, REQ-150) | Nur Sink (REQ-176) |
| `websocket` | Externer WebSocket-Feed — verbinden, abonnieren, Ereignisse empfangen; Ergebnisse materialisiert (REQ-338) | Nein |
| `rss` | RSS-2.0-/Atom-Feed — pollen, Watermark nach pubDate/updated; Ergebnisse materialisiert (REQ-342, REQ-343) | Nein |

### Push-Receiver

| Quellentyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `ingest` | Externe Dienste senden JSON-Ereignisse per POST; Ergebnisse materialisiert (REQ-331, REQ-335) | Nein |

### Graph & Semantic

| Quellentyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher über HTTP-API, Ergebnisse in PostgreSQL gecacht (REQ-295) | Nein |
| `sparql` | SPARQL-1.1-POST, Ergebnisse in PostgreSQL gecacht (REQ-297) | Nein |

### Dateibasiert

Zwei Mechanismen decken Dateien ab. Beide verwenden das Feld `path` anstelle von `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Einzeldatei-Quellen** — `sqlite`, `csv`, `parquet` verweisen mit `path` auf eine Datei.

| Quellentyp | Transporte | Mutationen |
| --- | --- | --- |
| `sqlite` | lokal | Ja |
| `csv` | lokal | Nein |
| `parquet` | lokal, `s3://` | Nein |

Private Buckets benötigen Anmeldedaten (AWS-Region und Schlüssel aus der Umgebung). Für CSV über `s3://` oder `http(s)://`, oder um viele Dateien auf einmal zu registrieren, verwenden Sie die Quelle `files`. [tool-verified: `provisa/file_source/source.py`]

**Quelle `files`** — verweist mit `path` auf einen Glob, durchsucht ihn rekursiv und registriert das Verzeichnis als föderierten Katalog von Tabellen. Sie liest viele Formate über viele Transporte; die untenstehenden Mengen stammen vom File-Connector (kenstott/calcite-Fork). [tool-verified: `provisa/core/catalog.py` Zweig `files` und `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; Format- und Transportlisten aus dem calcite-`file`-Adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formate | Transporte |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow und in Tabellen konvertierte Dokumente — HTML, Markdown, DOCX, PPTX | Lokales Dateisystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST und Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observability & Sonstiges

`prometheus` hat einen Trino-Connector (Eigenschaften aus der Mapping-DSL des Typs gebaut). `google_sheets` ist ein registrierter Quellentyp ohne Trino-Connector und materialisiert über die API-Cache-Pipeline. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` Zeilen 87–88]

| Quellentyp | Connector-Name | Mutationen |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialisiert) | Nein |
| `prometheus` | prometheus | Nein |

### SaaS-Connectoren für Unternehmen

SharePoint und Splunk registrieren sich über Apache-Calcite-Connectoren (kenstott/calcite-Fork). Keiner hat einen direkten Treiber — Provisa materialisiert ihre Zeilen, indem der mitgelieferte Calcite-pgwire-Server des Connectors gestartet wird (`pgwire-sharepoint`, `pgwire-splunk`), sich als generischer PostgreSQL-Endpunkt damit verbindet und die Zeilen im Materialisierungs-Store für die Föderation ablegt (REQ-954). Beide Connectoren aktivieren immer den Groß-/Kleinschreibungs-unabhängigen Namensabgleich, entsprechend der eigenen groß-/kleinschreibungsunabhängigen Semantik des jeweiligen Produkts (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` Zeilen 99–100; `provisa/federation/trino_connectors.py` Zeilen 223–286]

#### `sharepoint`

SharePoint-Listen werden als Schemas aufgezählt und als abfragbare Tabellen bereitgestellt (REQ-726, REQ-731). Zwei Auth-Methoden: `CLIENT_CREDENTIALS` (Standard) und zertifikatsbasiert über ein PFX-Zertifikat (REQ-727). Geheimwerte in `mapping` werden über die Secrets-Engine aufgelöst, bevor sie den Connector erreichen (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` Zeilen 230–252]

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` oder `host` | `site-url` | SharePoint-Site-URL |
| `username` | `client-id` | Azure-App-Client-ID |
| `password` | `client-secret` | Azure-App-Client-Secret |
| `database` | `tenant-id` | Azure-Tenant-UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (Standard) oder `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | PFX-Pfad, wenn `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | PFX-Passwort |

Wenn der Connector `information_schema.columns` nicht offenlegt, registrieren Sie die Tabelle mit expliziten Spaltendefinitionen (ermittelt über die Microsoft-Graph-API) über die Mutation `registerTable` (REQ-732).

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

#### `splunk`

Splunk-Suchergebnisse sind als Tabellen abfragbar (z. B. `internal_server`) (REQ-721). Die Connector-URL stammt aus `base_url`, oder wird als `https://{host}:{port}` mit einem Standardport von `8089` konstruiert (REQ-722). Auth: Wenn `mapping.use_token` auf `true` steht (Standard), wird `password` als API-Token übergeben; wenn `false`, werden `username` und `password` als separate Anmeldedaten übergeben (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` Zeilen 262–286]

| Quellfeld | Connector-Eigenschaft | Hinweise |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, sonst `https://host:port` (Port-Standard 8089) |
| `password` | `token` oder `password` | Token, wenn `use_token: true` |
| `username` | `user` | nur, wenn `use_token: false` |
| `database` | `app` | auf eine Splunk-App beschränken |
| `mapping.datamodel_filter` | `datamodel-filter` | auf ein Data Model filtern |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | für selbstsignierte Zertifikate (REQ-724) |

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

Registrieren Sie jeden HTTP-Endpunkt als abfragbare Tabelle. [tool-verified: `provisa/core/models.py` `SourceType`-Enum] (REQ-314, REQ-307, REQ-322)

| API-Typ | Erkennung | Spalten-Inferenz |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI-Spezifikationsanalyse (REQ-314, REQ-316) | Primitive → nativ, Objekte → JSONB |
| `graphql_remote` | Schema-Introspektion (REQ-307, REQ-308) | Primitive → nativ, Objekte → JSONB |
| `grpc_remote` | Server-Reflection (REQ-322, REQ-325) | Primitive → nativ, Objekte → JSONB |

API-Antworten werden abgerufen, in PostgreSQL gecacht (konfigurierbare TTL) und als GraphQL-Typen bereitgestellt (REQ-309, REQ-318, REQ-327). Gecachte Tabellen nehmen an föderierten Abfragen wie jede andere Quelle teil (REQ-313).

**JSONB-Regeln**: Komplexe Spalten (Objekte, Arrays), die als JSONB gespeichert sind, sind nicht filterbar (REQ-119). Der Zugriff auf Unterfelder nutzt `->>`-Extraktion in SQL (REQ-151). Beziehungen werden zwischen Tabellen anhand skalarer FK-Spalten deklariert — JSONB-Blob-Spalten sind keine JOIN-Ziele. Verwenden Sie JSONB-Promotion, um verschachtelte Felder in native skalare Spalten umzuwandeln, wenn Filtern oder Joinen auf ihnen benötigt wird (REQ-119).

### GovData

US-amerikanische offene Regierungsdaten. Der Zugriff ist nach Subject-Gruppierung partitioniert. [tool-verified: `provisa/core/models.py` Zeilen 543–609]

Jede `govdata`-Quelle wählt ein Subject aus. Dieses Subject bestimmt, welche GovData-Schemas offengelegt werden. Die Schemas `ref` und `geo` sind immer als Linker-Schemas enthalten — sie sind nicht pro Subject aufgeführt, aber immer vorhanden. [tool-verified: `provisa/core/models.py` Zeile 562–563 Kommentar]

| Subject | Offengelegte Schemas |
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
| `subject` | Ja | — | Einer der obigen Subject-Werte |
| `domain_id` | Ja | — | Domäne, zu der diese Quelle gehört |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

---

## Benutzerdefinierte Connectoren (REQ-1177)

Die nativen Föderations-Engines — Postgres, DuckDB und ClickHouse — erhalten Erreichbarkeit für einen neuen Quellentyp, wenn ein Operator einen Connector dafür in `config/custom_connectors.yaml` deklariert. Es ist kein Code erforderlich. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Connector-Erweiterbarkeit an sich geht dem voraus. Die Trino-Engine ist auf ihrer eigenen Schicht seit Langem erweiterbar — ein generischer JDBC-Connector, parametrisiert pro Quellentyp, ein Katalog-`.properties`-Body pro Typ, und Provisas eigene benutzerdefinierte Trino-Connector-Plugins (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 bringt dieselbe konfigurationsgetriebene Erweiterbarkeit zu den beiden nativen, clusterlosen Engines, die zuvor einen festen Connector-Satz hatten.

Die Konfiguration wird leer ausgeliefert. Eingebaute Connectoren decken die Erreichbarkeit von Haus aus ab; alles in dieser Datei ist vom Operator verfasst. [tool-verified: `config/custom_connectors.yaml` Zeile 52: `connectors: []`] Setzen Sie `PROVISA_CUSTOM_CONNECTORS`, um auf einen anderen Pfad zu verweisen (nützlich für Tests).

### Descriptor-Arten

| Engine | Art | Mechanismus | Was der Descriptor liefert |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (ISO-Standard) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + Scanner-Sicht | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (legt automatisch jede Remote-Tabelle offen) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | pro-Tabelle `CREATE TABLE ENGINE=…` (Spalten aus der Registry) | `ch_engine`, `engine_template` (kann `{table}` tragen) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse leitet das Schema ab | `ch_engine`, `engine_template` |

**Postgres ist generisch.** SQL/MED ist ein ISO-Standard, daher teilt sich jeder konforme FDW dieselbe DDL-Form: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, optionales `CREATE USER MAPPING`, dann entweder `IMPORT FOREIGN SCHEMA` (wenn `supports_import: true`) oder eine explizite `CREATE FOREIGN TABLE` pro Tabelle (wenn `false`). Ein `pg_fdw`-Descriptor liefert nur die Pro-FDW-Varianz — Extension-Name, Server-Options-Schlüssel, User-Mapping-Schlüssel, Import-Flag, Tabellen-Optionen. Jeder standardkonforme FDW ist daher allein aus der Konfiguration steuerbar. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` Zeilen 98–125]

**DuckDB unterstützt zwei Mechanismen.** Eine Extension, die einen Katalog über ATTACH offenlegt, verwendet `duckdb_attach`; eine, die eine lesende Table-Function offenlegt, verwendet `duckdb_scan`. Eine Extension, die zu keinem Muster passt, wird nicht unterstützt. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse unterstützt drei Mechanismen**, einen pro Integration-Engine-Form: eine relationale DATABASE-Engine, die automatisch jede Remote-Tabelle offenlegt (`clickhouse_database`, z. B. Redis/MySQL), eine Pro-Tabelle-Engine, deren Spalten die Registry liefert (`clickhouse_table`, z. B. die JDBC-/ODBC-Brücke — die `engine_template` kann einen `{table}`-Platzhalter tragen, den die Laufzeit bindet), und eine Datei-/Lake-/URL-Engine, deren Schema ClickHouse ableitet (`clickhouse_scan`, z. B. HDFS/URL). SQLite (DATABASE-Engine, Datei, kein Server) und Hudi (Lakehouse, Zero-Copy) werden von Haus aus ausgeliefert. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Ein unbekannter `kind`-Wert schlägt beim Start laut fehl — ein Tippfehler im Descriptor darf einen Quellentyp nicht still unerreichbar machen. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` Zeilen 178–197]

### Probe-Gating

Die Verfügbarkeit wird beim Attach-Zeitpunkt gegen den Standard-Erkennungskatalog jeder Engine verifiziert:

- **Postgres** — prüft `pg_extension`, dann `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` Zeilen 333–344]
- **DuckDB** — führt `INSTALL`/`LOAD` aus und prüft `duckdb_functions()` auf das deklarierte `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` Zeilen 160–180]
- **ClickHouse** — prüft `system.table_engines` auf die deklarierte `ch_engine`; Fehlen im Build schlägt laut fehl. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Eine deklarierte Extension, die nicht installierbar ist, schlägt laut fehl. Kein stilles Überspringen, kein Fallback. Ein Connector, dessen Probe fehlschlägt, ist für diese Bereitstellung schlicht nicht aktiv.

### Template-Variablen

Jeder `server_options`-Wert, `user_mapping`-Wert, `attach_template` und `scan_template` kann `{field}`-Platzhalter verwenden. Verfügbare Felder: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` Zeilen 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus jeder Schlüssel aus `federation_hints`. DuckDB-Attach-Templates erhalten zusätzlich `{alias}` — den internen Katalog-Alias, den Provisa der angehängten Datenbank zuweist.

Ein Template, das auf ein unbekanntes Feld verweist, schlägt beim Attach-Zeitpunkt laut fehl und deckt eine Descriptor-/Quellen-Diskrepanz auf, bevor fehlerhaftes DDL die Engine erreicht.

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

[tool-verified: `config/custom_connectors.yaml` auskommentierte Beispiele, Zeilen 26–50]

Mit einem der beiden Descriptoren an Ort und Stelle routet die Registrierung einer Quelle mit dem deklarierten `source_type` über den benutzerdefinierten Connector, vorbehaltlich einer erfolgreichen Probe. Keine weitere Konfigurationsänderung ist nötig.

---

## Warehouses als benannte Quellen

Snowflake, Databricks und ClickHouse können als benannte Quellen registriert werden, unabhängig davon, welche Föderations-Engine aktiv ist. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Bei der Registrierung liest Provisa das Warehouse über den DirectDriver der Quelle und legt eine Replika im Materialisierungs-Store der aktiven Engine an. Die Abfrage läuft dann gegen diese Replika. Dies unterscheidet sich vom traditionellen direktfähigen Pfad (asyncpg, aiomysql), bei dem die Engine vollständig umgangen wird — hier führt die Engine die Abfrage weiterhin aus, aber gegen eine lokale Replika statt bei jeder Anfrage über die Leitung zum Warehouse.

Lesevorgänge sind dort Arrow-nativ, wo das Warehouse dies unterstützt: Databricks nutzt Cloud Fetch, Snowflake nutzt `fetch_arrow_table`, und ClickHouse nutzt die native spaltenorientierte HTTP-Schnittstelle.

Erweiterte Verbindungsparameter, die die Standardfelder `host`/`port`/`username`/`password` nicht tragen können, kommen in `federation_hints`:

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

Die Registrierung als benannte Quelle ist unabhängig davon, ob dasselbe Warehouse als Föderations-Engine gewählt wurde. Eine Snowflake-Quelle auf einer DuckDB-Engine legt eine Replika in DuckDB an, nicht in Snowflake.

Cloud-Objekt-/Lake-Daten (Parquet-, CSV-, Iceberg-, Delta-Lake-Dateien auf S3 / GCS / R2) sind ein separater Quellentyp, der direkt angehängt wird, wenn die aktive Engine einen ATTACH-Connector für diesen Typ hat. Es wird keine Replika angelegt — die Engine scannt den Objektspeicher direkt. Anmeldedaten für diese Quellen kommen ebenfalls in `federation_hints`:

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

## Quellenkonfigurationsfelder

Alle Quellen teilen sich einen gemeinsamen Feldsatz. [tool-verified: `provisa/core/models.py` Klasse `Source`, Zeilen 138–204]

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `id` | Ja | — | Eindeutiger Bezeichner; alphanumerisch mit Bindestrichen/Unterstrichen |
| `type` | Ja | — | Quellentyp (siehe obige Tabellen) |
| `host` | Nein | `""` | Hostname oder IP |
| `port` | Nein | `0` | Portnummer |
| `database` | Nein | `""` | Datenbankname |
| `username` | Nein | `""` | Benutzername |
| `password` | Nein | `""` | Passwort; verwenden Sie `${env:VAR}` für Secret-Auflösung |
| `path` | Nein | `null` | Dateipfad oder Cloud-URI für dateibasierte und Objekt-/Lake-Quellen |
| `base_url` | Nein | `null` | Basis-URL für OpenAPI-Quellen |
| `pool_min` | Nein | `1` | Minimale Connection-Pool-Größe (REQ-052) |
| `pool_max` | Nein | `5` | Maximale Connection-Pool-Größe (REQ-052) |
| `use_pgbouncer` | Nein | `false` | Verbindungen über PgBouncer routen (REQ-053) |
| `pgbouncer_port` | Nein | `6432` | PgBouncer-Port (REQ-053) |
| `cache_enabled` | Nein | `true` | API-Antwort-Caching aktivieren |
| `cache_ttl` | Nein | `null` | Cache-TTL in Sekunden; erbt den globalen Standard, wenn null |
| `cache_catalog` | Nein | `null` | Föderierter Katalog für API-Cache; Standard ist der eigene Katalog der Quelle |
| `cache_schema` | Nein | `api_cache` | Schema innerhalb des Cache-Katalogs |
| `naming_convention` | Nein | `null` | Globale Namenskonvention für diese Quelle überschreiben (REQ-194) |
| `federation_hints` | Nein | `{}` | Sitzungseigenschaften, die an die Föderations-Engine übergeben werden, sowie erweiterte Verbindungsparameter für Warehouse-Quellen (REQ-278, REQ-281) |
| `mapping` | Nein | `{}` | Typspezifische Connector-Einstellungen für NoSQL- und SaaS-Quellen (z. B. SharePoint `auth_type`, Splunk `use_token`) (REQ-251) |
| `allowed_domains` | Nein | `[]` | Quelle auf bestimmte Domänen beschränken; leer = uneingeschränkt |
| `description` | Nein | `""` | Menschenlesbare Beschreibung |

---

## Kafka-Quellen

Kafka-Topics werden separat unter `kafka_sources` konfiguriert, verschlüsselt nach der Quellen-`id` einer registrierten `kafka`-Quelle. [tool-verified: `config/provisa.yaml` Zeilen 138–151] (REQ-147)

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
| `topics[].default_window` | Standard-Zeitfenster für fensterbasierte Abfragen (z. B. `1h`) (REQ-148) |
| `topics[].columns` | Spaltendefinitionen für das Topic-Schema (REQ-150) |

---

## Spaltensichtbarkeit

Das Feld `visible_to` an jeder Spalte ist eine Liste von Rollen-IDs, die diese Spalte sehen können. [tool-verified: `provisa/core/models.py` Klasse `Column` Zeile 248; `config/provisa.yaml` Zeilen 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Spalten, die aus der `visible_to`-Liste einer Rolle ausgelassen wurden, erscheinen nicht im GraphQL-Schema dieser Rolle und können nicht abgefragt oder in Filtern referenziert werden (REQ-039).

---

## Beziehungen

Beziehungen verbinden zwei registrierte Tabellen und erscheinen als verschachtelte Felder in GraphQL. [tool-verified: `provisa/core/models.py` Klasse `Relationship` Zeilen 323–343; `config/provisa.yaml` Zeilen 103–110] (REQ-019)

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
| `source_table_id` | Ja | Tabelle, die den Fremdschlüssel hält |
| `target_table_id` | Ja | Referenzierte Tabelle; leer bei berechneten Beziehungen |
| `source_column` | Ja | Spalte auf der Quelltabelle |
| `target_column` | Ja | Spalte auf der Zieltabelle; leer bei berechneten Beziehungen |
| `cardinality` | Ja | `many-to-one` oder `one-to-many` (REQ-019) |
| `materialize` | Nein | Automatisch eine materialisierte Sicht für quellenübergreifende Joins erstellen (REQ-158) |
| `refresh_interval` | Nein | MV-Aktualisierungsintervall in Sekunden (Standard: 300) |
| `target_function_name` | Nein | DB-Funktionsname für berechnete Beziehungen |
| `function_arg` | Nein | Welches Funktionsargument den Quellspaltenwert erhält |
| `alias` | Nein | Menschenlesbarer Beziehungstyp (z. B. `WORKS_FOR`) |
| `graphql_alias` | Nein | Benennt das SDL-Feld, das diese Beziehung auf dem übergeordneten Typ offenlegt. Wenn nicht vorhanden, wird der Name aus dem `field_name` der Zieltabelle und der Beziehungskardinalität abgeleitet. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Nein | Wenn `true`, wird diese Beziehung von Cypher-Graphkanten ausgeschlossen |
| `source_json_key` | Nein | Extrahiert diesen Schlüssel aus der Quellspalte als JSON-Objekt vor dem JOIN |

Kardinalitätswerte [tool-verified: `provisa/core/models.py` `Cardinality`-Enum, Zeilen 79–81]:

- `many-to-one` — jede Quellzeile bildet auf eine Zielzeile ab (FK auf PK)
- `one-to-many` — jede Quellzeile bildet auf mehrere Zielzeilen ab (Umkehrung des Obigen)

---

## Row-Level-Security-Regeln

RLS-Regeln injizieren zum Abfragezeitpunkt `WHERE`-Klauseln, geltend für eine Rolle und optional für eine Tabelle oder Domäne. [tool-verified: `provisa/core/models.py` Klasse `RLSRule` Zeilen 391–395; `config/provisa.yaml` Zeilen 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Wenn sowohl eine Domänen-Ebene- als auch eine Tabellen-Ebene-Regel für dieselbe Rolle existieren, hat die Tabellen-Ebene-Regel Vorrang (REQ-403).

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `table_id` | Bedingt | Tabelle, auf die die Regel angewendet wird; schließt sich gegenseitig mit `domain_id` aus |
| `domain_id` | Bedingt | Domäne, auf die die Regel angewendet wird; gilt für alle Tabellen in der Domäne (REQ-402) |
| `role_id` | Ja | Rolle, für die diese Regel gilt |
| `filter` | Ja | SQL-Prädikat, injiziert in `WHERE`; kann Sitzungsvariablen referenzieren (REQ-041) |

---

## Funktionen und Webhooks

### DB-Funktionen

Verfolgen Sie eine Datenbankfunktion und legen Sie sie als GraphQL-Abfrage oder -Mutation offen. [tool-verified: `provisa/core/models.py` Klasse `Function` Zeilen 423–438; `config/provisa.yaml` Zeilen 152–164] (REQ-205)

Datenbankquellen können auch ihre gespeicherten Prozeduren und Funktionen aus dem Herstellerkatalog automatisch erkennen (`pg_proc`, `information_schema.routines` oder Herstelläquivalente), wodurch die manuelle Registrierung jeder einzelnen entfällt. Die Erkennung liest `prokind` und `provolatile`: Immutable/Stable-Funktionen registrieren sich als parametrisierte Relationen (Prozedurargumente werden zu Abfrageparametern, dieselbe Form wie OpenAPI-GET-Tabellen), und volatile Prozeduren registrieren sich als Mutationen/verfolgte Funktionen. Erkannte Routinen durchlaufen dieselbe Stage-2-Governance-Pipeline wie manuell registrierte. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

Legen Sie einen externen HTTP-Endpunkt als GraphQL-Abfrage oder -Mutation offen. [tool-verified: `provisa/core/models.py` Klasse `Webhook` Zeilen 441–455; `config/provisa.yaml` Zeilen 166–178] (REQ-209)

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

Auth wird unter dem Schlüssel `auth` konfiguriert. [tool-verified: `provisa/core/models.py` Klasse `AuthConfig` Zeilen 467–477] (REQ-120)

| Provider | Beschreibung |
| ---------- | ------------- |
| `none` | Keine Authentifizierung; alle Anfragen werden als `default_role` behandelt |
| `firebase` | Firebase Authentication; erfordert `project_id` und `service_account_key` (REQ-121) |
| `keycloak` | Keycloak-OIDC (REQ-122) |
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

`assignments_source: claims` liest Rollenzuweisungen aus JWT-Claims. `assignments_source: provisa` liest sie aus Provisas eigenem Zuweisungs-Store. [tool-verified: `provisa/core/models.py` Zeile 476] (REQ-551)

---

## Ausführungs-Routing

**Direkte Ausführung** — Einzelquellen-RDBMS-Abfragen routen zum nativen Treiber für Latenz unter 100 ms (REQ-027). Quellen benötigen sowohl einen `SOURCE_TO_DIALECT`-Eintrag als auch einen `SOURCE_TO_CONNECTOR`-Eintrag, um diesen Pfad zu unterstützen (REQ-229).

**Föderierte Ausführung** — Mehrquellen-Abfragen und Quellen ohne direkten Treiber routen über die Föderations-Engine (REQ-028). Provisa enthält eine eingebettete Föderations-Engine; verweisen Sie auf Ihren eigenen kompatiblen Cluster für Deployments in großem Maßstab (REQ-226).

**Statistiken** — Bei der Registrierung führt Provisa `ANALYZE` gegen jede veröffentlichte Tabelle aus, um den kostenbasierten Optimierer vorzubereiten (Zeilenanzahl, Null-Anteil, eindeutige Werte, Min/Max). Fehler werden protokolliert und blockieren die Registrierung nicht (REQ-275).

---

## Graph- und semantische Quellen

### Neo4j

Registrieren Sie eine Neo4j-Graphdatenbank als abfragbare Quelle. Data Stewards verfassen Cypher-Abfragen, die skalare Werte projizieren; Provisa cacht Ergebnisse und legt sie als GraphQL-Typen offen (REQ-295).

Cypher-Abfragen müssen Property-Accessoren in der `RETURN`-Klausel verwenden (`RETURN n.id AS id, n.name AS name`) — die Rückgabe von Node-Objekten wird bei der Registrierung abgelehnt (REQ-296).

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

Der Preview-Endpunkt (`POST /admin/sources/neo4j/{id}/preview`) gibt Beispielzeilen zurück und blockiert die Registrierung, wenn das Cypher Node-Objekte zurückgibt (REQ-296).

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

Beide Connectoren verwenden die API-Quellen-Cache-Pipeline — Ergebnisse werden in PostgreSQL mit konfigurierbarer TTL gespeichert, wodurch sie für quellenübergreifende föderierte JOINs verfügbar werden (REQ-295, REQ-297, REQ-299).

---

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
