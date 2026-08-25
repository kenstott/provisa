# Quelltypen
## Ausführungsmodell
Letztlich läuft jede Abfrage durch die Föderations-Engine, die Föderation über alle Quellen bereitstellt. Quellen fallen basierend auf ihrer Konnektivität in drei Kategorien. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Kategorie | Hat direkten Treiber | Hat föderierten Connector | Beispiele |
| --- | --- | --- | --- |
| **Direktfähig** | Ja | Ja | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Nur Föderation** | Nein | Ja | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (S3-basiert) |
| **Direktlesend (Replika)** | Ja | Ja | Snowflake, Databricks, ClickHouse — Treiber liest Daten und legt eine Replika an; Abfragen laufen gegen die Replika in der aktiven Engine |
| **Materialisieren → Föderation** | Nein | Nein | REST/OpenAPI, entferntes GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (Push-Empfänger), GovData, SharePoint, Splunk |

**Direktfähige** Quellen führen Einzelquellen-Abfragen über ihren nativen Treiber aus (unter 100 ms) und umgehen die Föderations-Engine (REQ-027, REQ-229). Sie behalten vollständige Connector-Unterstützung und nehmen an der Föderation teil, wenn sie mit anderen Quellen gejoint werden (REQ-028).

**Nur-Föderations**-Quellen werden immer durch die Föderationsschicht abgefragt. Es existiert kein direkter Treiber (REQ-229).

**Direktlesende (Replika)**-Quellen haben einen DirectDriver, der nativ aus dem Warehouse liest (wo verfügbar Arrow-nativ), eine Replika im Materialisierungsspeicher der aktiven Engine anlegt, und anschließend laufen Abfragen gegen diese Replika. Siehe [Warehouses als benannte Quellen](#warehouses-als-benannte-quellen).

**Materialisieren**-Quellen haben keinen föderierten Connector. Provisa ruft ihre Daten ab (beim Start oder zur Abfragezeit) und cacht sie als Parquet in S3 oder in PostgreSQL, wodurch sie für quellübergreifende Abfragen durch die Föderations-Engine erreichbar werden (REQ-309).

---

## Alle Quellen
Provisa registriert **53** Quelltypen. Die Tabellen unten decken alle 53 ab; der Index ist die Zählung. [tool-verified: `provisa/core/models.py` `SourceType`]

| # | Gruppe | Quelltypen |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Cloud Data Warehouses](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Data Lake / offene Tabellenformate](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Push-Empfänger](#push-receiver) | `ingest` |
| 38–39 | [Graph & Semantik](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [Dateibasiert](#file-based) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Observability & Sonstige](#observability-other) | `google_sheets`, `prometheus` |
| 46–47 | [Enterprise-SaaS](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [API-Quellen](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Data-Quality-Checker](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |

Referenz für jeden von Provisa unterstützten Quelltyp. „Direkter Treiber" bedeutet, dass Einzelquellen-Abfragen nativ gegen die Quelle ausgeführt werden (unter 100 ms) (REQ-027). „Connector Name" ist der föderierte Connector, der genutzt wird, wenn die Quelle an quellübergreifenden JOINs teilnimmt (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen |
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
| `firebird` | — | — (DuckDB-Extension) | — | Nein |
| `airport` | — | — (DuckDB-Extension) | — | Nein |

Wire-kompatible Datenbanken nutzen den JDBC-Treiber, nativen asynchronen Treiber und Dialekt eines Basis-Wire-Protokolls wieder — CockroachDB, YugabyteDB und Greenplum reiten auf dem PostgreSQL-Wire; TiDB reitet auf dem MySQL-Wire. Sie benötigen nur Registry-Einträge, keinen neuen Connector-Code. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) und `airport` (Arrow-Flight-Server) sind registrierte Quelltypen, die über DuckDB-Community-Extensions in place erreicht werden, wenn DuckDB die aktive Engine ist — kein direkter Treiber, kein föderierter Connector. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Cloud Data Warehouses

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen | Anmerkungen |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Föderiert | Liest via snowflake-connector-python; legt Replika an; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Föderiert | Kein DirectDriver; erreicht über Föderations-Engine oder BigQuery-Engine-ATTACH |
| `databricks` | DatabricksDriver | delta_lake | databricks | Föderiert | Liest via databricks-sql-connector (Cloud Fetch, Arrow); legt Replika an; `http_path` erforderlich in `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Föderiert | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Föderiert | Microsoft Fabric Warehouse; T-SQL über TDS, Azure-AD-Auth; legt Replika an (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Föderiert | Azure Synapse SQL; T-SQL über TDS, Azure-AD-Auth; legt Replika an (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Föderiert | Entfernter Trino/Presto-Koordinator, gelesen über den SQLAlchemy-Trino-Dialekt; legt Replika auf jeder Engine an (REQ-994) |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Quelltyp | Direkter Treiber | Connector-Name | Dialekt | Mutationen | Anmerkungen |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Föderiert | Liest via clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` für TLS (REQ-986) |
| `druid` | — | druid | druid | Nein | — |
| `exasol` | — | exasol | exasol | Nein | — |
| `elasticsearch` | — | elasticsearch | — | Nein | Connector-Eigenschaften stammen aus der Mapping-DSL des Typs [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Nein | Trino-`pinot`-Connector; `pinot.controller-urls` = host:port des Pinot-Controllers [tool-verified: `trino_connectors.py:199`] |

### Data Lake / offene Tabellenformate {#data-lake-open-table-formats}
Diese Quelltypen sind reine Föderationsquellen — kein direkter Treiber, kein Dialekt. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Quelltyp | Connector-Name | Time Travel | Anmerkungen |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Ja (`as_of`-Argument, REQ-372) | — |
| `delta_lake` | delta_lake | Ja (`as_of`-Argument, REQ-372) | — |
| `hive` | hive | Nein | — |
| `hudi` | — (ClickHouse-`Hudi`-Engine, Zero-Copy — REQ-1178) | Nein | Nein | Kein föderierter Connector; wird in place erreicht, wenn ClickHouse die aktive Engine ist |
| `hive_s3` | hive | Nein | S3-basiertes Hive |

### NoSQL

`mongodb`, `cassandra` und `redis` haben Trino-Connectors (`redis` baut seine Eigenschaften aus der Mapping-DSL des Typs). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Quelltyp | Connector-Name | Mutationen |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Nein |
| `cassandra` | cassandra | Nein |
| `redis` | redis | Nein |

### Streaming

| Quelltyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `kafka` | Föderierter Kafka-Connector; Schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), manuelle Definition oder Sample-Inferenz (REQ-147, REQ-150) | Nur Sink (REQ-176) |
| `websocket` | Externer WebSocket-Feed — verbinden, abonnieren, Events empfangen; Ergebnisse materialisiert (REQ-338) | Nein |
| `rss` | RSS-2.0-/Atom-Feed — pollen, Watermark nach pubDate/updated; Ergebnisse materialisiert (REQ-342, REQ-343) | Nein |

### Push-Empfänger {#push-receiver}
| Quelltyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `ingest` | Externe Dienste senden JSON-Events per POST; Ergebnisse materialisiert (REQ-331, REQ-335) | Nein |

### Graph & Semantik {#graph-semantic}
| Quelltyp | Mechanismus | Mutationen |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher über HTTP-API, Ergebnisse in PostgreSQL gecacht (REQ-295) | Nein |
| `sparql` | SPARQL-1.1-POST, Ergebnisse in PostgreSQL gecacht (REQ-297) | Nein |

### Dateibasiert {#file-based}
Zwei Mechanismen decken Dateien ab. Beide verwenden das Feld `path` statt `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Einzeldatei-Quellen** — `sqlite`, `csv`, `parquet` verweisen mit `path` auf eine Datei.

| Quelltyp | Transporte | Mutationen |
| --- | --- | --- |
| `sqlite` | lokal | Ja |
| `csv` | lokal | Nein |
| `parquet` | lokal, `s3://` | Nein |

Private Buckets benötigen Anmeldedaten (AWS-Region und Schlüssel aus der Umgebung). Für CSV über `s3://` oder `http(s)://`, oder um viele Dateien auf einmal zu registrieren, verwenden Sie die Quelle `files`. [tool-verified: `provisa/file_source/source.py`]

**`files`-Quelle** — `path` verweist auf ein Glob, durchläuft es rekursiv und registriert das Verzeichnis als föderierten Katalog von Tabellen. Es liest viele Formate über viele Transporte; die untenstehenden Mengen stammen vom File-Connector (kenstott/calcite-Fork). [tool-verified: `provisa/core/catalog.py` `files`-Zweig und `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; Format- und Transportlisten vom calcite-`file`-Adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formate | Transporte |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow und in Tabellen umgewandelte Dokumente — HTML, Markdown, DOCX, PPTX | Lokales Dateisystem, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST und Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observability & Sonstige {#observability-other}
`prometheus` hat einen Trino-Connector (Eigenschaften aus der Mapping-DSL des Typs gebaut). `google_sheets` ist ein registrierter Quelltyp ohne Trino-Connector und materialisiert über die API-Cache-Pipeline. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Quelltyp | Connector-Name | Mutationen |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materialisiert) | Nein |
| `prometheus` | prometheus | Nein |

### Enterprise-SaaS-Connectors

SharePoint und Splunk registrieren sich über Apache-Calcite-Connectors (kenstott/calcite-Fork). Keiner hat einen direkten Treiber — Provisa materialisiert ihre Zeilen, indem es den mitgelieferten Calcite-pgwire-Server des Connectors startet (`pgwire-sharepoint`, `pgwire-splunk`), sich als generischen PostgreSQL-Endpunkt daran verbindet und die Zeilen im Materialisierungsspeicher für die Föderation anlegt (REQ-954). Beide Connectors aktivieren immer Case-insensitive-Namensabgleich, passend zur eigenen case-insensitiven Semantik des jeweiligen Produkts (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

SharePoint-Listen werden als Schemas aufgezählt und als abfragbare Tabellen exponiert (REQ-726, REQ-731). Zwei Auth-Methoden: `CLIENT_CREDENTIALS` (Standard) und zertifikatsbasiert über ein PFX-Zertifikat (REQ-727). Geheime Werte in `mapping` werden vor Erreichen des Connectors über die Secrets-Engine aufgelöst (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Quellfeld | Connector-Eigenschaft | Anmerkungen |
| --- | --- | --- |
| `base_url` oder `host` | `site-url` | SharePoint-Site-URL |
| `username` | `client-id` | Azure-App-Client-ID |
| `password` | `client-secret` | Azure-App-Client-Secret |
| `database` | `tenant-id` | Azure-Tenant-UUID |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (Standard) oder `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | PFX-Pfad, wenn `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | PFX-Passwort |

Wenn der Connector `information_schema.columns` nicht exponiert, registrieren Sie die Tabelle mit expliziten Spaltendefinitionen (aus der Microsoft-Graph-API bezogen) über die Mutation `registerTable` (REQ-732).

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

Splunk-Suchergebnisse sind als Tabellen abfragbar (z. B. `internal_server`) (REQ-721). Die Connector-URL stammt aus `base_url`, oder wird als `https://{host}:{port}` mit einem Standardport von `8089` konstruiert (REQ-722). Auth: Wenn `mapping.use_token` `true` ist (Standard), wird `password` als API-Token übergeben; wenn `false`, werden `username` und `password` als separate Anmeldedaten übergeben (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Quellfeld | Connector-Eigenschaft | Anmerkungen |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, sonst `https://host:port` (Standardport 8089) |
| `password` | `token` oder `password` | Token, wenn `use_token: true` |
| `username` | `user` | nur wenn `use_token: false` |
| `database` | `app` | auf eine Splunk-App beschränken |
| `mapping.datamodel_filter` | `datamodel-filter` | auf ein Datenmodell filtern |
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

### API-Quellen {#api-sources}
Registrieren Sie jeden HTTP-Endpunkt als abfragbare Tabelle. [tool-verified: `provisa/core/models.py` `SourceType`-Enum] (REQ-314, REQ-307, REQ-322)

| API-Typ | Discovery | Spaltenableitung |
| --------- | ----------- | ----------------- |
| `openapi` | OpenAPI-Spec-Parsing (REQ-314, REQ-316) | Primitive → nativ, Objekte → JSONB |
| `graphql_remote` | Schema-Introspektion (REQ-307, REQ-308) | Primitive → nativ, Objekte → JSONB |
| `grpc_remote` | Server Reflection (REQ-322, REQ-325) | Primitive → nativ, Objekte → JSONB |

API-Antworten werden abgerufen, in PostgreSQL gecacht (konfigurierbare TTL) und als GraphQL-Typen exponiert (REQ-309, REQ-318, REQ-327). Gecachte Tabellen nehmen wie jede andere Quelle an föderierten Abfragen teil (REQ-313).

**JSONB-Regeln**: Komplexe, als JSONB gespeicherte Spalten (Objekte, Arrays) sind nicht filterbar (REQ-119). Der Zugriff auf Unterfelder nutzt `->>`-Extraktion in SQL (REQ-151). Beziehungen werden zwischen Tabellen mittels skalarer FK-Spalten deklariert — JSONB-Blob-Spalten sind keine Join-Ziele. Verwenden Sie JSONB-Promotion, um verschachtelte Felder in native Skalarspalten umzuwandeln, wenn Filtern oder Joinen darauf erforderlich ist (REQ-119).

### GovData

US-amerikanische offene Regierungsdaten. Der Zugriff ist nach Themengruppierung partitioniert. [tool-verified: `provisa/core/models.py` lines 543–609]

Jede `govdata`-Quelle wählt ein Thema. Dieses Thema bestimmt, welche GovData-Schemas exponiert werden. Die Schemas `ref` und `geo` sind immer als Linker-Schemas enthalten — sie werden nicht pro Thema aufgeführt, sind aber immer vorhanden. [tool-verified: `provisa/core/models.py` line 562–563 Kommentar]

| Thema | Exponierte Schemas |
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

### Data-Quality-Checker (REQ-1443) {#data-quality-checkers-req-1443}
Ein Data-Quality-Checker ist ein Quelltyp, kein Subsystem. Seine Scan-Ausgabe ist Daten: Ein Prüfergebnis ist eine Beobachtung, landet also über den gewöhnlichen Quellpfad und erbt Kadenz, Aktualität, Events, Lineage, Governance, RLS, Grid und Export von jeder anderen Quelle. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Zwei werden unterstützt, und die Wahl ist ebenso eine Lizenzfrage wie eine Funktionsfrage.

| Quelltyp | Contract-Dialekt | Extra | Lizenz | Gehostete Cloud-Ebene |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda-Contract-YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Verweigert — siehe unten |
| `great_expectations` | Expectation-Suite-JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Erlaubt |

Die Elastic License 2.0 verbietet es, die Software Dritten als gehosteten oder verwalteten Dienst bereitzustellen, und Soda innerhalb der SaaS-Ebene im Namen eines Mandanten laufen zu lassen ist genau das. `config/capabilities.yaml` trägt die Trennung als `cloud_eligible: false` an der `soda`-Option, und die gehostete Ebene liest dieses Flag. Ein gehostetes Deployment, das Soda möchte, erreicht einen vom Betreiber bereitgestellten Soda-Endpunkt, den der Betreiber selbst betreibt. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa vendort und linkt nichts. Der Scan läuft in einem Child-Interpreter (`python -m provisa.dq.worker`), der einzige Ort, an dem `soda_core` oder `great_expectations` importiert wird, sodass ein source-available Checker nie den Serverprozess erreicht und ein Checker-Crash einen Subprozess statt der Event-Loop tötet. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**Die Quelle verweist auf Provisas eigenen pgwire-Endpunkt.** Das ermöglicht es einem einzigen Postgres-Treiber, eine Snowflake- oder Iceberg-gestützte Tabelle zu prüfen: Der Checker scannt die föderierte Sicht, nicht das darunterliegende System. Weil Richtlinien auf diese Verbindung angewendet werden, wird die Scan-Identität deklariert statt geerbt — eine gefilterte Zeilenmenge darf niemals eine still bestehende Prüfung erzeugen.

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

**Eine Ergebnistabelle pro Contract, und der Contract ist die gesamte Registrierung.** Die Tabelle trägt `dq_contract` — den Contract-Text wortwörtlich — und sonst nichts über ihre Form. Spalten, Watermark und Promotions sind alle abgeleitet. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

- **Lineage.** Der Contract benennt bereits sein Ziel-Dataset, sodass die Registrierung ihn so parst, wie `extract_inputs` SQL parst (REQ-939), und ihn zur regulierten Tabelle auflöst. Eine Definition, keine zweite Kopie, die abdriften kann. Ein Contract, der ein nicht reguliertes Dataset benennt, schlägt laut bei der Registrierung fehl, statt Zeilen zu landen, die niemand angefordert hat.
- **Spalten.** Die Ergebnishülle gehört dem Checker, nicht dem Betreiber — 16 mitgelieferte Spalten von `scan_id` bis `diagnostics`. Deklarierte Spalten werden nur für ihr `visible_to` gelesen, das einstimmig sein muss, und dann ersetzt. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Watermark.** `scan_time` wird zur Watermark, was das Landen zu einem Append macht (REQ-982). Scan-Historie akkumuliert ohne separates Historie-Subsystem.
- **Promotions.** `freshness_max_timestamp` und `dataset_rows_tested` werden aus dem `diagnostics`-jsonb als typisierte Spalten promotet (REQ-119). Weitere hinzuzufügen funktioniert wie bei jeder anderen jsonb-Spalte. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

Timing führt keine neuen Felder ein. `change_signal` plus `cache_ttl` geben die Poll-Kadenz vor; `mv_debounce_quiet` und `mv_debounce_max_delay` fassen einen vorgelagerten Burst zu einem Scan zusammen (REQ-963); eine Kalendergranularität macht ihn periodisch (REQ-962); `expected_events` hält den Scan zurück, bis seine Eingaben über das Fenster hinweg aktuell sind (REQ-961). Die Poll-Schleife ist der Scan-Scheduler.

`outcome` ist eines von `pass`, `fail`, `warn`, `error`, `skipped`. Keines davon ist ein Urteil — Durchsetzung, falls gewünscht, ist eine separate, spätere Deklaration: ein Preflight oder eine MV über den gelandeten Ergebnissen. Weil eine gelandete Beobachtung keine Determinismus-Verpflichtung trägt (REQ-964), sind hier nicht-deterministische Prüfungen zulässig, die auf einem Preflight-Gate nie stehen könnten — Anomalie-Score, Trailing-Window-Änderung, Aktualität gegen jetzt.

Der Contract wird in der UI autorisiert, im Data-Quality-Panel der Tabellenbearbeitungsoberfläche, und der rohe Contract-Text dort ist immer die Source of Truth. Ein Dry Run führt den Contract gegen die Live-Tabelle aus und zeigt die Ergebnisse, ohne sie zu landen — so fangen Sie einen Contract ab, dessen Dataset-Name unerwartet aufgelöst wurde und andernfalls nichts als bestehende Zeilen landen würde.

---

## Benutzerdefinierte Connectors (REQ-1177)
Die nativen Föderations-Engines — Postgres, DuckDB und ClickHouse — erlangen Erreichbarkeit zu einem neuen Quelltyp, wenn ein Betreiber einen Connector dafür in `config/custom_connectors.yaml` deklariert. Kein Code erforderlich. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Connector-Erweiterbarkeit als solche existierte schon vorher. Die Trino-Engine ist auf ihrer eigenen Ebene seit langem erweiterbar — ein generischer JDBC-Connector, parametrisiert pro Quelltyp, ein Katalog-`.properties`-Body pro Typ, sowie Provisas eigene benutzerdefinierte Trino-Connector-Plugins (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 bringt dieselbe konfigurationsgetriebene Erweiterbarkeit zu den zwei nativen, clusterlosen Engines, die zuvor einen festen Connector-Satz trugen.

Die Konfiguration wird leer ausgeliefert. Eingebaute Connectors decken die Reichweite ab Werk ab; alles in dieser Datei ist vom Betreiber autorisiert. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Setzen Sie `PROVISA_CUSTOM_CONNECTORS`, um auf einen anderen Pfad zu verweisen (nützlich für Tests).

### Descriptor-Arten
| Engine | Art | Mechanismus | Was der Descriptor liefert |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (ISO-Standard) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + Scanner-View | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (exponiert automatisch jede entfernte Tabelle) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | pro-Tabelle `CREATE TABLE ENGINE=…` (Spalten aus der Registry) | `ch_engine`, `engine_template` (kann `{table}` tragen) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse leitet das Schema ab | `ch_engine`, `engine_template` |

**Postgres ist generisch.** SQL/MED ist ein ISO-Standard, sodass jeder konforme FDW dieselbe DDL-Form teilt: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, optional `CREATE USER MAPPING`, dann entweder `IMPORT FOREIGN SCHEMA` (wenn `supports_import: true`) oder ein explizites `CREATE FOREIGN TABLE` pro Tabelle (wenn `false`). Ein `pg_fdw`-Descriptor liefert nur die Pro-FDW-Varianz — Extension-Name, Server-Options-Schlüssel, User-Mapping-Schlüssel, Import-Flag, Table-Options. Jeder standardkonforme FDW ist daher allein aus der Konfiguration ansteuerbar. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB unterstützt zwei Mechanismen.** Eine Extension, die einen Katalog via ATTACH exponiert, nutzt `duckdb_attach`; eine, die eine lesende Table-Funktion exponiert, nutzt `duckdb_scan`. Eine Extension, die zu keinem Muster passt, wird nicht unterstützt. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse unterstützt drei Mechanismen**, einen pro Integrations-Engine-Form: eine relationale DATABASE-Engine, die automatisch jede entfernte Tabelle exponiert (`clickhouse_database`, z. B. Redis/MySQL), eine Pro-Tabelle-Engine, deren Spalten die Registry liefert (`clickhouse_table`, z. B. die JDBC/ODBC-Brücke — das `engine_template` kann einen `{table}`-Platzhalter tragen, den die Runtime bindet), und eine Datei-/Lake-/URL-Engine, deren Schema ClickHouse ableitet (`clickhouse_scan`, z. B. HDFS/URL). SQLite (DATABASE-Engine, Datei, kein Server) und Hudi (Lakehouse, Zero-Copy) werden ab Werk ausgeliefert. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Ein unbekannter `kind`-Wert schlägt beim Start laut fehl — ein Tippfehler im Descriptor darf einen Quelltyp nicht still unerreichbar lassen. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Probe-Gating

Verfügbarkeit wird zur Attach-Zeit gegen den Standard-Discovery-Katalog jeder Engine verifiziert:

- **Postgres** — prüft `pg_extension`, dann `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — führt `INSTALL`/`LOAD` aus und prüft `duckdb_functions()` auf das deklarierte `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — prüft `system.table_engines` auf die deklarierte `ch_engine`; fehlt sie im Build, schlägt es laut fehl. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Eine deklarierte Extension, die nicht installierbar ist, schlägt laut fehl. Kein stiller Skip, kein Fallback. Ein Connector, dessen Probe fehlschlägt, ist für dieses Deployment schlicht nicht aktiv.

### Template-Variablen
Jeder `server_options`-Wert, `user_mapping`-Wert, `attach_template` und `scan_template` kann `{field}`-Platzhalter verwenden. Verfügbare Felder: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus jeder Schlüssel aus `federation_hints`. DuckDB-Attach-Templates erhalten zusätzlich `{alias}` — den internen Katalog-Alias, den Provisa der angehängten Datenbank zuweist.

Ein Template, das auf ein unbekanntes Feld verweist, schlägt zur Attach-Zeit laut fehl und deckt eine Descriptor-/Quell-Fehlpassung auf, bevor defektes DDL die Engine erreicht.

### Beispiele
**Postgres — MongoDB via `mongo_fdw` (kein Schema-Import; Spalten pro Tabelle geliefert)**

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

**DuckDB — Excel-Dateien via `read_xlsx` (Scan-Table-Funktion)**

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

Mit einem der beiden Descriptors an Ort und Stelle routet die Registrierung einer Quelle mit dem deklarierten `source_type` durch den benutzerdefinierten Connector, vorbehaltlich einer erfolgreichen Probe. Keine weitere Konfigurationsänderung ist nötig.

---

## Warehouses als benannte Quellen
Snowflake, Databricks und ClickHouse können unabhängig davon, welche Föderations-Engine aktiv ist, als benannte Quellen registriert werden. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Bei Registrierung liest Provisa das Warehouse über den DirectDriver der Quelle und legt eine Replika im Materialisierungsspeicher der aktiven Engine an. Die Abfrage läuft dann gegen diese Replika. Dies unterscheidet sich vom traditionellen direktfähigen Pfad (asyncpg, aiomysql), bei dem die Engine vollständig umgangen wird — hier führt die Engine die Abfrage weiterhin aus, aber gegen eine lokale Replika statt bei jeder Anfrage über das Wire zum Warehouse.

Lesevorgänge sind Arrow-nativ, wo das Warehouse dies unterstützt: Databricks nutzt Cloud Fetch, Snowflake nutzt `fetch_arrow_table`, und ClickHouse nutzt die native Columnar-HTTP-Schnittstelle.

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

Die Registrierung als benannte Quelle ist unabhängig davon, dasselbe Warehouse als Föderations-Engine auszuwählen. Eine Snowflake-Quelle auf einer DuckDB-Engine legt eine Replika in DuckDB an, nicht in Snowflake.

Cloud-Objekt-/Lake-Daten (Parquet-, CSV-, Iceberg-, Delta-Lake-Dateien auf S3 / GCS / R2) sind ein separater Quelltyp, der in place anhängt, wenn die aktive Engine einen ATTACH-Connector für diesen Typ hat. Keine Replika wird angelegt — die Engine scannt den Objektspeicher direkt. Anmeldedaten für diese Quellen kommen ebenfalls in `federation_hints`:

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
Alle Quellen teilen sich eine gemeinsame Menge von Feldern. [tool-verified: `provisa/core/models.py` `Source`-Klasse, lines 138–204]

| Feld | Erforderlich | Standard | Beschreibung |
| ------- | ---------- | --------- | ------------- |
| `id` | Ja | — | Eindeutiger Bezeichner; alphanumerisch mit Bindestrichen/Unterstrichen |
| `type` | Ja | — | Quelltyp (siehe obige Tabellen) |
| `host` | Nein | `""` | Hostname oder IP |
| `port` | Nein | `0` | Portnummer |
| `database` | Nein | `""` | Datenbankname |
| `username` | Nein | `""` | Benutzername |
| `password` | Nein | `""` | Passwort; `${env:VAR}` für Secret-Auflösung verwenden |
| `path` | Nein | `null` | Dateipfad oder Cloud-URI für dateibasierte und Objekt-/Lake-Quellen |
| `base_url` | Nein | `null` | Basis-URL für OpenAPI-Quellen |
| `pool_min` | Nein | `1` | Minimale Connection-Pool-Größe (REQ-052) |
| `pool_max` | Nein | `5` | Maximale Connection-Pool-Größe (REQ-052) |
| `use_pgbouncer` | Nein | `false` | Verbindungen über PgBouncer routen (REQ-053) |
| `pgbouncer_port` | Nein | `6432` | PgBouncer-Port (REQ-053) |
| `cache_enabled` | Nein | `true` | API-Antwort-Caching aktivieren |
| `cache_ttl` | Nein | `null` | Cache-TTL in Sekunden; erbt den globalen Standard, wenn null |
| `cache_catalog` | Nein | `null` | Föderierter Katalog für API-Cache; standardmäßig der eigene Katalog der Quelle |
| `cache_schema` | Nein | `api_cache` | Schema innerhalb des Cache-Katalogs |
| `naming_convention` | Nein | `null` | Globale Namenskonvention für diese Quelle überschreiben (REQ-194) |
| `federation_hints` | Nein | `{}` | Sitzungseigenschaften, die an die Föderations-Engine übergeben werden, und erweiterte Verbindungsparameter für Warehouse-Quellen (REQ-278, REQ-281) |
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
| `topics[].default_window` | Standard-Zeitfenster für gefensterte Abfragen (z. B. `1h`) (REQ-148) |
| `topics[].columns` | Spaltendefinitionen für das Topic-Schema (REQ-150) |

---

## Spaltensichtbarkeit
Das Feld `visible_to` an jeder Spalte ist eine Liste von Rollen-IDs, die diese Spalte sehen können. [tool-verified: `provisa/core/models.py` `Column`-Klasse Zeile 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Spalten, die in der `visible_to`-Liste einer Rolle fehlen, erscheinen nicht im GraphQL-Schema dieser Rolle und können nicht abgefragt oder in Filtern referenziert werden (REQ-039).

---

## Beziehungen
Beziehungen verbinden zwei registrierte Tabellen und erscheinen als verschachtelte Felder in GraphQL. [tool-verified: `provisa/core/models.py` `Relationship`-Klasse lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

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
| `target_table_id` | Ja | Referenzierte Tabelle; leer für berechnete Beziehungen |
| `source_column` | Ja | Spalte auf der Quelltabelle |
| `target_column` | Ja | Spalte auf der Zieltabelle; leer für berechnete Beziehungen |
| `cardinality` | Ja | `many-to-one` oder `one-to-many` (REQ-019) |
| `materialize` | Nein | Automatisch eine materialisierte Sicht für quellübergreifende Joins erzeugen (REQ-158). Auf einer Junction-gestützten Kante deckt die Sicht die Zwei-Sprung-Traversierung ab, nicht einen direkten Join (REQ-1586) |
| `refresh_interval` | Nein | MV-Aktualisierungsintervall in Sekunden (Standard: 300) |
| `target_function_name` | Nein | DB-Funktionsname für berechnete Beziehungen |
| `function_arg` | Nein | Welches Funktionsargument den Quellspaltenwert erhält |
| `alias` | Nein | Menschenlesbarer Beziehungstyp (z. B. `WORKS_FOR`) |
| `graphql_alias` | Nein | Benennt das SDL-Feld, das diese Beziehung am übergeordneten Typ exponiert. Wenn abwesend, wird der Name aus dem `field_name` der Zieltabelle und der Beziehungskardinalität abgeleitet. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Nein | Wenn `true`, diese Beziehung von Cypher-Graph-Kanten ausschließen |
| `source_json_key` | Nein | Diesen Schlüssel aus der Quellspalte als JSON-Objekt extrahieren, vor dem JOIN |
| `via_table` | Nein | Registrierter Tabellenname der Junction, die diese Kante durchläuft. Gesetzt macht es die Kante Junction-gestützt; leer gelassen bleibt sie eine Fremdschlüssel-Kante (REQ-1586) |
| `via_source_column` | Nein | Junction-Spalte, die zu `source_column` gehört. Bei zusammengesetztem Schlüssel kommagetrennt und positionsabhängig |
| `via_target_column` | Nein | Junction-Spalte, die zu `target_column` gehört |
| `via_type_column` | Nein | Diskriminator-Spalte, wenn eine Junction mehrere Beziehungstypen trägt |
| `via_type_value` | Nein | Der Diskriminatorwert, auf den diese Kante festgelegt ist |
| `via_label_source` | Nein | Welche Nominierung den Cypher-Typ benennt: `column` (der Diskriminatorwert), `table` (der Tabellenname der Junction) oder `fixed` (der deklarierte Alias). Alle werden in UPPER_SNAKE_CASE überführt |

### Junction-gestützte Beziehungen

Eine Zuordnungstabelle kann statt als Knoten als vollwertige Cypher-Beziehung deklariert werden, sodass ihre eigenen Spalten zu Attributen dieser Beziehung werden: (REQ-1586)

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

Die Junction ist eine registrierte Tabelle wie jede andere und muss registriert sein, bevor eine Beziehung sie benennen kann. Deklarieren Sie sie einmal pro Diskriminatorwert: Drei Zeilen über `pet_companions` ergeben `BONDED_PAIR`, `LITTERMATE` und `SHARES_ENCLOSURE` als drei verschiedene Cypher-Typen, von denen jeder die übrigen Spalten der Junction-Zeile als Kanten-Eigenschaften trägt. Genau das deklariert die mitgelieferte Demo-Konfiguration.

Eine Junction-Kante ist eine Cypher-Beziehung, kein GraphQL-Join-Feld: Der GraphQL-Join-Emitter baut seine `ON`-Klausel für ein einzelnes Spaltenpaar und hat keinen Platz für den zweiten Sprung, daher sind Junction-Kanten aus dem generierten SDL und aus `pg_constraint` ausgeschlossen. [tool-verified: `provisa/compiler/schema_gen.py:304`] Die Junction-Tabelle bleibt als eigenes Root-Feld abfragbar und fällt auf der Knotenseite des Cypher-Graph-Schemas weg, sodass sie nie als Knoten-Label erscheint.

`materialize: true` funktioniert auf einer Junction-Kante, und materialisiert wird die Traversierung statt eines direkten `pets`-zu-`pets`-Joins: Die Sicht hält den Quellsprung, den Junction-Sprung, den Diskriminator und die eigenen Spalten der Junction neben denen des Ziels. Weil die Junction ein drittes Bein des Joins ist, wird über alle drei Tabellen hinweg beurteilt, ob die Kante Quellen überschreitet — eine Junction in einer anderen Quelle als die beiden verbundenen wird materialisiert, selbst wenn diese beiden übereinstimmen. Eine Deklaration materialisiert einen Kantentyp, eine für `bonded pair` gebaute Sicht beantwortet also nie eine `littermate`-Traversierung.

Kardinalitätswerte [tool-verified: `provisa/core/models.py` `Cardinality`-Enum, lines 79–81]:

- `many-to-one` — jede Quellzeile ordnet sich einer Zielzeile zu (FK zu PK)
- `one-to-many` — jede Quellzeile ordnet sich mehreren Zielzeilen zu (Umkehrung des obigen)

---

## Regeln für Sicherheit auf Zeilenebene
RLS-Regeln injizieren `WHERE`-Klauseln zur Abfragezeit, geltend für eine Rolle und optional für eine Tabelle oder Domäne. [tool-verified: `provisa/core/models.py` `RLSRule`-Klasse lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Wenn sowohl eine domänenweite als auch eine tabellenspezifische Regel für dieselbe Rolle existieren, hat die tabellenspezifische Regel Vorrang (REQ-403).

| Feld | Erforderlich | Beschreibung |
| ------- | ---------- | ------------- |
| `table_id` | Bedingt | Tabelle, auf die die Regel angewendet wird; schließt sich mit `domain_id` gegenseitig aus |
| `domain_id` | Bedingt | Domäne, auf die die Regel angewendet wird; gilt für alle Tabellen in der Domäne (REQ-402) |
| `role_id` | Ja | Rolle, für die diese Regel gilt |
| `filter` | Ja | SQL-Prädikat, injiziert in `WHERE`; kann auf Sitzungsvariablen verweisen (REQ-041) |

---

## Funktionen und Webhooks
### DB-Funktionen
Tracken Sie eine Datenbankfunktion und exponieren Sie sie als GraphQL-Query oder -Mutation. [tool-verified: `provisa/core/models.py` `Function`-Klasse lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Datenbankquellen können auch ihre gespeicherten Prozeduren und Funktionen automatisch aus dem Hersteller-Katalog entdecken (`pg_proc`, `information_schema.routines`, oder Hersteller-Äquivalente), wodurch die Notwendigkeit entfällt, jede einzeln von Hand zu registrieren. Discovery liest `prokind` und `provolatile`: immutable/stable Funktionen registrieren sich als parametrisierte Relationen (Prozedurargumente werden zu Abfrageparametern, dieselbe Form wie OpenAPI-GET-Tabellen), und volatile Prozeduren registrieren sich als Mutationen/getrackte Funktionen. Entdeckte Routinen durchlaufen die Stage-2-Governance identisch zu von Hand registrierten. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

Exponieren Sie einen externen HTTP-Endpunkt als GraphQL-Query oder -Mutation. [tool-verified: `provisa/core/models.py` `Webhook`-Klasse lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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
Auth wird unter dem Schlüssel `auth` konfiguriert. [tool-verified: `provisa/core/models.py` `AuthConfig`-Klasse lines 467–477] (REQ-120)

| Provider | Beschreibung |
| ---------- | ------------- |
| `none` | Keine Authentifizierung; alle Anfragen werden als `default_role` behandelt |
| `firebase` | Firebase Authentication; benötigt `project_id` und `service_account_key` (REQ-121) |
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

`assignments_source: claims` liest Rollenzuweisungen aus JWT-Claims. `assignments_source: provisa` liest sie aus Provisas eigenem Zuweisungsspeicher. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Ausführungsrouting
**Direkte Ausführung** — Einzelquellen-RDBMS-Abfragen routen zum nativen Treiber für Latenz unter 100 ms (REQ-027). Quellen benötigen sowohl einen `SOURCE_TO_DIALECT`-Eintrag als auch einen `SOURCE_TO_CONNECTOR`-Eintrag, um diesen Pfad zu unterstützen (REQ-229).

**Föderierte Ausführung** — Quellübergreifende Abfragen und Quellen ohne direkten Treiber routen durch die Föderations-Engine (REQ-028). Provisa enthält eine eingebettete Föderations-Engine; verweisen Sie für großskalige Deployments auf Ihren eigenen kompatiblen Cluster (REQ-226).

**Statistiken** — Bei der Registrierung führt Provisa `ANALYZE` gegen jede veröffentlichte Tabelle aus, um den kostenbasierten Optimizer zu grundieren (Zeilenanzahlen, Null-Anteil, distinkte Werte, Min/Max). Fehler werden protokolliert und blockieren die Registrierung nicht (REQ-275).

---

## Graph- & Semantik-Quellen
### Neo4j

Registrieren Sie eine Neo4j-Graphdatenbank als abfragbare Quelle. Data Stewards autorisieren Cypher-Abfragen, die skalare Werte projizieren; Provisa cacht Ergebnisse und exponiert sie als GraphQL-Typen (REQ-295).

Cypher-Abfragen müssen Property-Accessoren in der `RETURN`-Klausel verwenden (`RETURN n.id AS id, n.name AS name`) — die Rückgabe von Node-Objekten wird zur Registrierungszeit abgelehnt (REQ-296).

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

Registrieren Sie jeden SPARQL-1.1-konformen Triplestore (Apache Jena Fuseki, Virtuoso, Stardog usw.) als abfragbare Quelle (REQ-297).

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

Beide Connectors nutzen die API-Quellen-Cache-Pipeline — Ergebnisse werden in PostgreSQL mit konfigurierbarer TTL gespeichert, wodurch sie für quellübergreifende föderierte JOINs verfügbar sind (REQ-295, REQ-297, REQ-299).

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

### Quellübergreifende Abfrage
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

Einzelquellen-Anteile routen direkt (REQ-027). Quellübergreifende JOINs föderieren mit automatischer Typkonvertierung (REQ-028, REQ-552).
