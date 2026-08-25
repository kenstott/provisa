# Tipi di origine

## Modello di esecuzione

Ogni query viene infine eseguita attraverso il motore di federazione, che fornisce federazione su tutte le origini. Le origini rientrano in tre categorie in base alla loro connettività. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoria | Ha driver diretto | Ha connettore federato | Esempi |
| --- | --- | --- | --- |
| **Direct-capable** | Sì | Sì | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Solo federazione** | No | Sì | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (basato su S3) |
| **Direct-read (replica)** | Sì | Sì | Snowflake, Databricks, ClickHouse — il driver legge i dati e atterra una replica; le query girano contro la replica nel motore attivo |
| **Materializza → Federazione** | No | No | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (ricevitore push), GovData, SharePoint, Splunk |

Le origini **Direct-capable** eseguono query a singola origine tramite il loro driver nativo (sub-100ms), bypassando il motore di federazione (REQ-027, REQ-229). Mantengono il pieno supporto del connettore e partecipano alla federazione quando unite ad altre origini (REQ-028).

Le origini **Solo federazione** vengono sempre interrogate attraverso il layer di federazione. Non esiste un driver diretto (REQ-229).

Le origini **Direct-read (replica)** hanno un DirectDriver che legge dal warehouse nativamente (Arrow-native dove disponibile), atterra una replica nello store di materializzazione del motore attivo, e poi le query girano contro quella replica. Vedi [Warehouse come origini nominate](#warehouse-come-origini-nominate).

Le origini **Materialize** non hanno un connettore federato. Provisa recupera i loro dati (all'avvio o al momento della query) e li mette in cache come Parquet su S3 o in PostgreSQL, rendendoli raggiungibili dal motore di federazione per query cross-source (REQ-309).

---

## Tutte le origini

Provisa registra **53** tipi di origine. Le tabelle qui sotto le coprono tutte e 53; l'indice è il conteggio. [tool-verified: `provisa/core/models.py` `SourceType`]

| # | Gruppo | Tipi di origine |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Data warehouse cloud](#data-warehouse-cloud) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Data Lake / formati tabella aperti](#data-lake-formati-tabella-aperti) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Ricevitore push](#ricevitore-push) | `ingest` |
| 38–39 | [Grafo e semantico](#grafo-e-semantico) | `neo4j`, `sparql` |
| 40–43 | [Basate su file](#basate-su-file) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Osservabilità e altro](#osservabilita-e-altro) | `google_sheets`, `prometheus` |
| 46–47 | [Connettori SaaS enterprise](#connettori-saas-enterprise) | `sharepoint`, `splunk` |
| 48–50 | [Origini API](#origini-api) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Controlli di qualità dei dati (REQ-1443)](#controlli-di-qualita-dei-dati-req-1443) | `soda`, `great_expectations` |

Riferimento per ogni tipo di origine supportato da Provisa. "Driver diretto" significa che le query a singola origine vengono eseguite nativamente contro l'origine (sub-100ms) (REQ-027). "Nome connettore" è il connettore federato usato quando l'origine partecipa a JOIN multi-source (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| Tipo di origine | Driver diretto | Nome connettore | Dialetto | Mutation |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Sì |
| `mysql` | aiomysql | mysql | mysql | Sì |
| `mariadb` | aiomysql | mariadb | mysql | Sì |
| `singlestore` | — | singlestore | singlestore | Federata |
| `sqlserver` | aioodbc | sqlserver | tsql | Sì |
| `oracle` | oracledb | oracle | oracle | Sì |
| `duckdb` | duckdb | memory | duckdb | Sì |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Sì |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Sì |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Sì |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Sì |
| `firebird` | — | — (estensione DuckDB) | — | No |
| `airport` | — | — (estensione DuckDB) | — | No |

I database wire-compatible riusano il driver JDBC, il driver async nativo e il dialetto di un wire di base — CockroachDB, YugabyteDB e Greenplum usano il wire PostgreSQL; TiDB usa il wire MySQL. Richiedono solo voci di registro, nessun nuovo codice connettore. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) e `airport` (server Arrow Flight) sono tipi di origine registrati, raggiunti in loco tramite estensioni community di DuckDB quando DuckDB è il motore attivo — nessun driver diretto, nessun connettore federato. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Data warehouse cloud

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo di origine | Driver diretto | Nome connettore | Dialetto | Mutation | Note |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federata | Legge via snowflake-connector-python; atterra una replica; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federata | Nessun DirectDriver; raggiunta via motore di federazione o ATTACH del motore BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federata | Legge via databricks-sql-connector (Cloud Fetch, Arrow); atterra una replica; `http_path` richiesto in `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federata | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federata | Microsoft Fabric Warehouse; T-SQL su TDS, auth Azure AD; atterra una replica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federata | Azure Synapse SQL; T-SQL su TDS, auth Azure AD; atterra una replica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federata | Coordinator Trino/Presto remoto letto via il dialetto SQLAlchemy trino; atterra una replica su qualsiasi motore (REQ-994) |

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo di origine | Driver diretto | Nome connettore | Dialetto | Mutation | Note |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federata | Legge via clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` per TLS (REQ-986) |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | — | elasticsearch | — | No | Le proprietà del connettore provengono dal DSL di mapping del tipo [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | No | Connettore Trino `pinot`; `pinot.controller-urls` = host:porta del controller Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / formati tabella aperti

Questi tipi di origine sono solo-federazione — nessun driver diretto, nessun dialetto. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Tipo di origine | Nome connettore | Time Travel | Note |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sì (argomento `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Sì (argomento `as_of`, REQ-372) | — |
| `hive` | hive | No | — |
| `hudi` | — (motore `Hudi` di ClickHouse, zero-copy — REQ-1178) | No | No | Nessun connettore federato; raggiunta sul posto quando ClickHouse è il motore attivo |
| `hive_s3` | hive | No | Hive basato su S3 |

### NoSQL

`mongodb`, `cassandra` e `redis` hanno connettori Trino (`redis` costruisce le sue proprietà dal DSL di mapping del tipo). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Tipo di origine | Nome connettore | Mutation |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra | No |
| `redis` | redis | No |

### Streaming

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `kafka` | Connettore Kafka federato; schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), definizione manuale, o inferenza da campione (REQ-147, REQ-150) | Solo sink (REQ-176) |
| `websocket` | Feed WebSocket esterno — connette, si iscrive, riceve eventi; risultati materializzati (REQ-338) | No |
| `rss` | Feed RSS 2.0 / Atom — polling, watermark per pubDate/updated; risultati materializzati (REQ-342, REQ-343) | No |

### Ricevitore push

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `ingest` | Servizi esterni inviano eventi JSON via POST; risultati materializzati (REQ-331, REQ-335) | No |

### Grafo e semantico

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, risultati messi in cache in PostgreSQL (REQ-295) | No |
| `sparql` | SPARQL 1.1 POST, risultati messi in cache in PostgreSQL (REQ-297) | No |

### Basate su file

Due meccanismi coprono i file. Entrambi usano il campo `path` invece di `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Origini a file singolo** — `sqlite`, `csv`, `parquet` puntano `path` su un singolo file.

| Tipo di origine | Trasporti | Mutation |
| --- | --- | --- |
| `sqlite` | locale | Sì |
| `csv` | locale | No |
| `parquet` | locale, `s3://` | No |

I bucket privati richiedono credenziali (regione AWS e chiavi dall'ambiente). Per CSV su `s3://` o `http(s)://`, o per registrare molti file contemporaneamente, usa l'origine `files`. [tool-verified: `provisa/file_source/source.py`]

**Origine `files`** — punta `path` su un glob, lo esegue in crawling ricorsivamente, e registra la directory come catalogo federato di tabelle. Legge molti formati su molti trasporti; gli insiemi sotto provengono dal connettore file (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch e `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; liste di formati e trasporti dall'adapter calcite `file` — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formati | Trasporti |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, e documenti convertiti in tabelle — HTML, Markdown, DOCX, PPTX | Filesystem locale, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST e Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Osservabilità e altro

`prometheus` ha un connettore Trino (proprietà costruite dal DSL di mapping del tipo). `google_sheets` è un tipo di origine registrato senza connettore Trino e materializza attraverso la pipeline di cache API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo di origine | Nome connettore | Mutation |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializzata) | No |
| `prometheus` | prometheus | No |

### Connettori SaaS enterprise

SharePoint e Splunk si registrano tramite connettori Apache Calcite (fork kenstott/calcite). Nessuno dei due ha un driver diretto — Provisa materializza le loro righe avviando il server pgwire Calcite integrato del connettore (`pgwire-sharepoint`, `pgwire-splunk`), connettendosi a esso come endpoint PostgreSQL generico, e atterrando le righe nello store di materializzazione per la federazione (REQ-954). Entrambi i connettori abilitano sempre il matching dei nomi case-insensitive, corrispondente alla semantica case-insensitive propria di ciascun prodotto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Le liste SharePoint vengono enumerate come schemi ed esposte come tabelle interrogabili (REQ-726, REQ-731). Due metodi di autenticazione: `CLIENT_CREDENTIALS` (default) e basato su certificato tramite un certificato PFX (REQ-727). I valori segreti in `mapping` vengono risolti attraverso il motore dei secret prima di raggiungere il connettore (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo origine | Proprietà connettore | Note |
| --- | --- | --- |
| `base_url` o `host` | `site-url` | URL del sito SharePoint |
| `username` | `client-id` | Client ID dell'app Azure |
| `password` | `client-secret` | Client secret dell'app Azure |
| `database` | `tenant-id` | UUID del tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (default) o `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Percorso PFX quando `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Password PFX |

Quando il connettore non espone `information_schema.columns`, registra la tabella con definizioni di colonna esplicite (ottenute dall'API Microsoft Graph) tramite la mutation `registerTable` (REQ-732).

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

I risultati di ricerca Splunk sono interrogabili come tabelle (es. `internal_server`) (REQ-721). L'URL del connettore proviene da `base_url`, oppure viene costruito come `https://{host}:{port}` con porta di default `8089` (REQ-722). Auth: quando `mapping.use_token` è `true` (default), `password` viene passata come token API; quando `false`, `username` e `password` vengono passate come credenziali separate (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Campo origine | Proprietà connettore | Note |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, altrimenti `https://host:port` (porta default 8089) |
| `password` | `token` o `password` | token quando `use_token: true` |
| `username` | `user` | solo quando `use_token: false` |
| `database` | `app` | restringe a un'app Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra a un data model |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | per certificati self-signed (REQ-724) |

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

### Origini API

Registra qualsiasi endpoint HTTP come tabella interrogabile. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo API | Discovery | Inferenza colonne |
| --------- | ----------- | ----------------- |
| `openapi` | Parsing della spec OpenAPI (REQ-314, REQ-316) | Primitivi → nativo, oggetti → JSONB |
| `graphql_remote` | Introspezione dello schema (REQ-307, REQ-308) | Primitivi → nativo, oggetti → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | Primitivi → nativo, oggetti → JSONB |

Le risposte API vengono recuperate, messe in cache in PostgreSQL (TTL configurabile), ed esposte come tipi GraphQL (REQ-309, REQ-318, REQ-327). Le tabelle in cache partecipano a query federate come qualsiasi altra origine (REQ-313).

**Regole JSONB**: Le colonne complesse (oggetti, array) memorizzate come JSONB non sono filtrabili (REQ-119). L'accesso ai sotto-campi usa l'estrazione `->>` in SQL (REQ-151). Le relazioni vengono dichiarate tra tabelle usando colonne FK scalari — le colonne blob JSONB non sono target di join. Usa la promozione JSONB per convertire campi annidati in colonne scalari native quando è necessario filtrare o unirsi su di essi (REQ-119).

### GovData

Dati aperti del governo statunitense. L'accesso è partizionato per raggruppamento subject. [tool-verified: `provisa/core/models.py` lines 543–609]

Ogni origine `govdata` seleziona un subject. Quel subject determina quali schemi GovData vengono esposti. Gli schemi `ref` e `geo` sono sempre inclusi come schemi linker — non sono elencati per subject ma sono sempre presenti. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Subject | Schemi esposti |
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
| `ALL` | Ogni schema sopra elencato |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Campo | Richiesto | Default | Descrizione |
| ------- | ---------- | --------- | ------------- |
| `id` | Sì | — | Identificatore univoco |
| `subject` | Sì | — | Uno dei valori subject sopra |
| `domain_id` | Sì | — | Dominio a cui appartiene questa origine |
| `description` | No | `""` | Descrizione leggibile |

---

### Controlli di qualità dei dati (REQ-1443)

Un checker di qualità dei dati è un tipo di origine, non un sottosistema. Il suo output di
scansione è dato: un risultato di controllo è un'osservazione, quindi percorre il normale percorso
di origine ed eredita cadenza, freschezza, eventi, derivazione, governance, RLS, griglia ed
esportazione da ogni altra origine. [tool-verified: `provisa/core/models.py` lines 110–116
`SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py`
`make_dq_loader`]

Ne sono supportati due, e la scelta è tanto una scelta di licenza quanto una scelta funzionale.

| Tipo di origine | Dialetto di contratto | Extra | Licenza | Piano cloud ospitato |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Rifiutato — vedi sotto |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Consentito |

La Elastic License 2.0 vieta di fornire il software a terzi come servizio ospitato o gestito, ed
eseguire Soda all'interno del piano SaaS per conto di un tenant è esattamente questo.
`config/capabilities.yaml` porta la distinzione come `cloud_eligible: false` sull'opzione `soda`, e
il piano ospitato legge quel flag. Una distribuzione ospitata che desideri Soda raggiunge un
endpoint Soda fornito dall'operatore, gestito direttamente da quest'ultimo. [tool-verified:
`config/capabilities.yaml` lines 197–203]

Provisa non fa vendoring né linking di nulla. La scansione viene eseguita in un interprete figlio
(`python -m provisa.dq.worker`), l'unico punto in cui vengono importati `soda_core` o
`great_expectations`, così un checker source-available non raggiunge mai il processo del server e
un crash del checker abbatte un sottoprocesso anziché l'event loop. [tool-verified:
`provisa/dq/runner.py` `build_command`, `run_contract`]

**L'origine punta al proprio endpoint pgwire di Provisa.** Questo è ciò che permette a un unico
driver postgres di controllare una tabella supportata da Snowflake o Iceberg: il checker analizza
la vista federata, non il sistema sottostante. Poiché la policy si applica a quella connessione,
l'identità di scansione è dichiarata anziché ereditata — un insieme di righe filtrato non deve mai
produrre un controllo che passa silenziosamente.

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

**Una tabella dei risultati per contratto, e il contratto è l'intera registrazione.** La tabella
porta `dq_contract` — il testo del contratto testuale — e nient'altro riguardo alla propria forma.
Colonne, watermark e promozioni sono tutte derivate. [tool-verified: `provisa/dq/registration.py`
`derive_checker_table`]

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

Ciò che la registrazione deriva da quel testo:

- **Derivazione.** Il contratto nomina già il proprio dataset di destinazione, quindi la
  registrazione lo analizza nello stesso modo in cui `extract_inputs` analizza SQL (REQ-939) e lo
  risolve nella tabella governata. Un'unica definizione, nessuna seconda copia che possa
  divergere. Un contratto che nomina un dataset non governato fallisce rumorosamente alla
  registrazione anziché depositare righe che nessuno ha richiesto.
- **Colonne.** L'involucro del risultato è quello del checker, non dell'operatore — 16 colonne
  fornite di serie da `scan_id` a `diagnostics`. Le colonne dichiarate vengono lette solo per il
  loro `visible_to`, che deve essere unanime, e vengono poi sostituite. [tool-verified:
  `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Watermark.** `scan_time` diventa il watermark, il che rende il deposito un append (REQ-982).
  La cronologia delle scansioni si accumula senza un sottosistema di cronologia.
- **Promozioni.** `freshness_max_timestamp` e `dataset_rows_tested` vengono promossi dal jsonb
  `diagnostics` a colonne tipizzate (REQ-119). Aggiungerne altre nello stesso modo in cui si
  farebbe su qualsiasi altra colonna jsonb. [tool-verified: `provisa/dq/results.py`
  `DQ_PROMOTIONS`]

La temporizzazione non introduce nuovi campi. `change_signal` più `cache_ttl` forniscono la
cadenza di polling; `mv_debounce_quiet` e `mv_debounce_max_delay` collassano un burst a monte in
una sola scansione (REQ-963); un grano di calendario la rende periodica (REQ-962);
`expected_events` trattiene la scansione finché i suoi input non sono aggiornati entro la finestra
(REQ-961). Il ciclo di polling è lo scheduler della scansione.

`outcome` è uno tra `pass`, `fail`, `warn`, `error`, `skipped`. Nessuno di essi è un verdetto —
l'applicazione, se desiderata, è una dichiarazione separata successiva: un preflight o una MV sui
risultati depositati. Poiché un'osservazione depositata non porta alcun obbligo di determinismo
(REQ-964), qui sono ammissibili controlli non deterministici che non potrebbero mai stare su un
gate di preflight — punteggio di anomalia, variazione su finestra mobile, freschezza rispetto
all'istante presente.

Il contratto viene redatto nell'interfaccia utente, nel pannello di qualità dei dati della
superficie di modifica tabella, e il testo del contratto grezzo lì presente è sempre la fonte di
verità. Un dry run esegue il contratto sulla tabella live e mostra gli esiti senza depositarli —
questo è il modo per individuare un contratto il cui nome di dataset si è risolto in un punto
inatteso e che altrimenti non depositerebbe altro che righe che passano.

---

## Connettori personalizzati (REQ-1177)

I motori di federazione nativi — Postgres, DuckDB e ClickHouse — guadagnano raggiungibilità verso un nuovo tipo di origine quando un operatore dichiara un connettore per esso in `config/custom_connectors.yaml`. Non è richiesto codice. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

L'estensibilità dei connettori di per sé precede questo. Il motore Trino è da tempo estensibile al proprio livello — un connettore JDBC generico parametrizzato per tipo di origine, un corpo `.properties` di catalogo per tipo, e i plugin connettore Trino personalizzati propri di Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 porta la stessa estensibilità config-driven ai due motori nativi senza cluster, che in precedenza portavano un insieme fisso di connettori.

La configurazione viene fornita vuota. I connettori integrati coprono la raggiungibilità out-of-the-box; tutto in questo file è scritto dall'operatore. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Imposta `PROVISA_CUSTOM_CONNECTORS` per puntare a un percorso diverso (utile per i test).

### Tipi di descrittore

| Motore | Tipo | Meccanismo | Cosa fornisce il descrittore |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (standard ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + vista scanner | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (espone automaticamente ogni tabella remota) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` per tabella (colonne dal registro) | `ch_engine`, `engine_template` (può portare `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse inferisce lo schema | `ch_engine`, `engine_template` |

**Postgres è generico.** SQL/MED è uno standard ISO, quindi ogni FDW conforme condivide la stessa forma di DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, opzionalmente `CREATE USER MAPPING`, poi o `IMPORT FOREIGN SCHEMA` (quando `supports_import: true`) oppure un `CREATE FOREIGN TABLE` esplicito per tabella (quando `false`). Un descrittore `pg_fdw` fornisce solo la variazione per-FDW — nome dell'estensione, chiavi delle opzioni server, chiavi di user-mapping, flag di import, opzioni tabella. Qualsiasi FDW conforme allo standard è quindi pilotabile solo da configurazione. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB supporta due meccanismi.** Un'estensione che espone un catalogo via ATTACH usa `duckdb_attach`; una che espone una table-function di lettura usa `duckdb_scan`. Un'estensione che non rientra in nessuno dei due schemi non è supportata. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse supporta tre meccanismi**, uno per ciascuna forma di motore di integrazione: un motore DATABASE relazionale che espone automaticamente ogni tabella remota (`clickhouse_database`, es. Redis/MySQL), un motore per tabella le cui colonne sono fornite dal registro (`clickhouse_table`, es. il bridge JDBC/ODBC — l'`engine_template` può portare un placeholder `{table}` che il runtime lega), e un motore file/lake/URL il cui schema ClickHouse inferisce (`clickhouse_scan`, es. HDFS/URL). SQLite (motore DATABASE, file, nessun server) e Hudi (lakehouse, zero-copy) sono forniti out-of-the-box. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Un valore `kind` sconosciuto fallisce in modo esplicito all'avvio — un errore di battitura nel descrittore non deve lasciare silenziosamente un tipo di origine irraggiungibile. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Gating della sonda (probe)

La disponibilità viene verificata al momento dell'attach contro il catalogo di discovery standard di ciascun motore:

- **Postgres** — controlla `pg_extension`, poi `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — esegue `INSTALL`/`LOAD` e controlla `duckdb_functions()` per il `probe_symbol` dichiarato. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — controlla `system.table_engines` per il `ch_engine` dichiarato; l'assenza dal build fallisce in modo esplicito. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Un'estensione dichiarata che non è installabile fallisce in modo esplicito. Nessun salto silenzioso, nessun fallback. Un connettore la cui sonda fallisce semplicemente non è attivo per quel deployment.

### Variabili di template

Ogni valore `server_options`, valore `user_mapping`, `attach_template` e `scan_template` può usare placeholder `{field}`. Campi disponibili: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, più qualsiasi chiave da `federation_hints`. I template attach di DuckDB ricevono anche `{alias}` — l'alias di catalogo interno che Provisa assegna al database attached.

Un template che referenzia un campo sconosciuto fallisce in modo esplicito al momento dell'attach, facendo emergere un mismatch descrittore/origine prima che DDL rotto raggiunga il motore.

### Esempi

**Postgres — MongoDB via `mongo_fdw` (nessun import di schema; colonne fornite per tabella)**

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

**DuckDB — file Excel via `read_xlsx` (scan table-function)**

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

Con l'uno o l'altro descrittore in atto, registrare un'origine con il `source_type` dichiarato viene instradato attraverso il connettore personalizzato, soggetto a una sonda riuscita. Non è necessaria alcuna altra modifica di configurazione.

---

## Warehouse come origini nominate

Snowflake, Databricks e ClickHouse possono essere registrati come origini nominate indipendentemente da quale motore di federazione è attivo. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Una volta registrato, Provisa legge il warehouse tramite il DirectDriver dell'origine e atterra una replica nello store di materializzazione del motore attivo. La query poi gira contro quella replica. Questo differisce dal percorso direct-capable tradizionale (asyncpg, aiomysql) dove il motore viene bypassato completamente — qui il motore esegue comunque la query, ma contro una replica locale piuttosto che sul wire verso il warehouse a ogni richiesta.

Le letture sono Arrow-native dove il warehouse lo supporta: Databricks usa Cloud Fetch, Snowflake usa `fetch_arrow_table`, e ClickHouse usa l'interfaccia HTTP columnar nativa.

I parametri di connessione estesi che i campi standard `host`/`port`/`username`/`password` non possono portare vanno in `federation_hints`:

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

La registrazione come origine nominata è indipendente dalla selezione dello stesso warehouse come motore di federazione. Un'origine Snowflake su un motore DuckDB atterra una replica in DuckDB, non in Snowflake.

I dati cloud object/lake (file parquet, csv, iceberg, delta_lake su S3 / GCS / R2) sono un tipo di origine separato che si attacca in loco quando il motore attivo ha un connettore ATTACH per quel tipo. Nessuna replica viene atterrata — il motore esegue la scansione dell'object storage direttamente. Anche le credenziali per queste origini vanno in `federation_hints`:

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

## Campi di configurazione dell'origine

Tutte le origini condividono un insieme comune di campi. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Campo | Richiesto | Default | Descrizione |
| ------- | ---------- | --------- | ------------- |
| `id` | Sì | — | Identificatore univoco; alfanumerico con trattini/underscore |
| `type` | Sì | — | Tipo di origine (vedi tabelle sopra) |
| `host` | No | `""` | Hostname o IP |
| `port` | No | `0` | Numero di porta |
| `database` | No | `""` | Nome del database |
| `username` | No | `""` | Nome utente |
| `password` | No | `""` | Password; usa `${env:VAR}` per la risoluzione del secret |
| `path` | No | `null` | Percorso file o URI cloud per origini basate su file e object/lake |
| `base_url` | No | `null` | URL base per origini OpenAPI |
| `pool_min` | No | `1` | Dimensione minima del connection pool (REQ-052) |
| `pool_max` | No | `5` | Dimensione massima del connection pool (REQ-052) |
| `use_pgbouncer` | No | `false` | Instrada le connessioni attraverso PgBouncer (REQ-053) |
| `pgbouncer_port` | No | `6432` | Porta PgBouncer (REQ-053) |
| `cache_enabled` | No | `true` | Abilita la cache delle risposte API |
| `cache_ttl` | No | `null` | TTL della cache in secondi; eredita il default globale quando null |
| `cache_catalog` | No | `null` | Catalogo federato per la cache API; di default il catalogo dell'origine stessa |
| `cache_schema` | No | `api_cache` | Schema all'interno del catalogo di cache |
| `naming_convention` | No | `null` | Sovrascrive la convenzione di naming globale per questa origine (REQ-194) |
| `federation_hints` | No | `{}` | Proprietà di sessione passate al motore di federazione, e parametri di connessione estesi per origini warehouse (REQ-278, REQ-281) |
| `mapping` | No | `{}` | Impostazioni di connettore specifiche per tipo per origini NoSQL e SaaS (es. `auth_type` di SharePoint, `use_token` di Splunk) (REQ-251) |
| `allowed_domains` | No | `[]` | Restringe l'origine a domini specifici; vuoto = senza restrizioni |
| `description` | No | `""` | Descrizione leggibile |

---

## Origini Kafka

I topic Kafka vengono configurati separatamente sotto `kafka_sources`, indicizzati dall'`id` di origine di un'origine `kafka` registrata. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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

| Campo | Descrizione |
| ------- | ------------- |
| `id` | Deve corrispondere all'`id` di un'origine con `type: kafka` |
| `topics[].id` | Nome logico per questo topic all'interno di Provisa |
| `topics[].topic` | Nome del topic Kafka |
| `topics[].domain_id` | Dominio a cui appartiene questo topic |
| `topics[].description` | Descrizione leggibile |
| `topics[].default_window` | Finestra temporale di default per query con finestra (es. `1h`) (REQ-148) |
| `topics[].columns` | Definizioni di colonna per lo schema del topic (REQ-150) |

---

## Visibilità delle colonne

Il campo `visible_to` su ogni colonna è una lista di ID di ruolo che possono vedere quella colonna. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Le colonne omesse dalla lista `visible_to` di un ruolo non appaiono nello schema GraphQL di quel ruolo e non possono essere interrogate o referenziate nei filtri (REQ-039).

---

## Relazioni

Le relazioni collegano due tabelle registrate e appaiono come campi annidati in GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Campo | Richiesto | Descrizione |
| ------- | ---------- | ------------- |
| `id` | Sì | Identificatore univoco per questa relazione |
| `source_table_id` | Sì | Tabella che detiene la chiave esterna |
| `target_table_id` | Sì | Tabella referenziata; vuoto per relazioni calcolate |
| `source_column` | Sì | Colonna sulla tabella sorgente |
| `target_column` | Sì | Colonna sulla tabella target; vuoto per relazioni calcolate |
| `cardinality` | Sì | `many-to-one` o `one-to-many` (REQ-019) |
| `materialize` | No | Crea automaticamente una vista materializzata per join cross-source (REQ-158). Su un arco basato su una giunzione la vista copre la traversata a due salti, non un join diretto (REQ-1586) |
| `refresh_interval` | No | Intervallo di refresh della MV in secondi (default: 300) |
| `target_function_name` | No | Nome della funzione DB per relazioni calcolate |
| `function_arg` | No | Quale argomento della funzione riceve il valore della colonna sorgente |
| `alias` | No | Tipo di relazione leggibile (es. `WORKS_FOR`) |
| `graphql_alias` | No | Nomina il campo SDL che questa relazione espone sul tipo padre. Quando assente, il nome viene derivato dal `field_name` della tabella target e dalla cardinalità della relazione. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | No | Quando `true`, esclude questa relazione dagli archi grafo Cypher |
| `source_json_key` | No | Estrae questa chiave dalla colonna sorgente come oggetto JSON prima del JOIN |
| `via_table` | No | Nome della tabella registrata della giunzione che questo arco attraversa. Valorizzarlo rende l'arco basato su una giunzione; lasciarlo vuoto lo lascia un arco su chiave esterna (REQ-1586) |
| `via_source_column` | No | Colonna della giunzione accoppiata a `source_column`. Separata da virgole e posizionale per una chiave composta |
| `via_target_column` | No | Colonna della giunzione accoppiata a `target_column` |
| `via_type_column` | No | Colonna discriminante, quando una stessa giunzione porta più tipi di relazione |
| `via_type_value` | No | Il valore del discriminante a cui questo arco è fissato |
| `via_label_source` | No | Quale designazione dà il nome al tipo Cypher: `column` (il valore del discriminante), `table` (il nome della tabella di giunzione) o `fixed` (l'alias dichiarato). Tutte vengono portate in UPPER_SNAKE_CASE |

### Relazioni basate su una tabella di giunzione

Una tabella associativa può essere dichiarata come relazione Cypher di prima classe anziché come nodo, così che le sue colonne diventino gli attributi di quella relazione: (REQ-1586)

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

La giunzione è una tabella registrata come tutte le altre e deve essere registrata prima che una relazione possa nominarla. Dichiaratela una volta per valore di discriminante: tre righe su `pet_companions` producono `BONDED_PAIR`, `LITTERMATE` e `SHARES_ENCLOSURE` come tre tipi Cypher distinti, ciascuno con le restanti colonne della riga di giunzione come proprietà dell'arco. La configurazione demo inclusa dichiara esattamente questo.

Un arco di giunzione è una relazione Cypher, non un campo di join GraphQL: l'emettitore di join GraphQL costruisce la sua clausola `ON` per una singola coppia di colonne e non ha posto per il secondo salto, quindi gli archi di giunzione sono esclusi dall'SDL generato e da `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`] La tabella di giunzione resta interrogabile come proprio campo radice ed esce dal lato nodi dello schema del grafo Cypher, così da non comparire mai come etichetta di nodo.

`materialize: true` funziona su un arco di giunzione, e ciò che materializza è la traversata anziché un join diretto `pets`-`pets`: la vista contiene il salto di origine, il salto della giunzione, il discriminante e le colonne proprie della giunzione accanto a quelle della destinazione. Poiché la giunzione è una terza gamba del join, se l'arco attraversa origini si giudica su tutte e tre le tabelle — una giunzione in un'origine diversa dalle due che collega viene materializzata anche quando quelle due coincidono. Una dichiarazione materializza un tipo di arco, quindi una vista costruita per `bonded pair` non risponde mai a una traversata `littermate`.

Valori di cardinalità [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — ogni riga sorgente mappa a una riga target (FK verso PK)
- `one-to-many` — ogni riga sorgente mappa a più righe target (inverso di sopra)

---

## Regole di sicurezza a livello di riga

Le regole RLS iniettano clausole `WHERE` al momento della query, ambito a un ruolo e opzionalmente a una tabella o dominio. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Quando esistono sia una regola a livello di dominio che una a livello di tabella per lo stesso ruolo, la regola a livello di tabella ha la precedenza (REQ-403).

| Campo | Richiesto | Descrizione |
| ------- | ---------- | ------------- |
| `table_id` | Condizionale | Tabella a cui applicare la regola; mutuamente esclusivo con `domain_id` |
| `domain_id` | Condizionale | Dominio a cui applicare la regola; si applica a tutte le tabelle nel dominio (REQ-402) |
| `role_id` | Sì | Ruolo a cui si applica questa regola |
| `filter` | Sì | Predicato SQL iniettato in `WHERE`; può referenziare variabili di sessione (REQ-041) |

---

## Funzioni e webhook

### Funzioni DB

Traccia una funzione database ed esponila come query o mutation GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Le origini database possono anche scoprire automaticamente le loro stored procedure e funzioni dal catalogo del vendor (`pg_proc`, `information_schema.routines`, o equivalenti del vendor), eliminando la necessità di registrare ciascuna a mano. La discovery legge `prokind` e `provolatile`: le funzioni immutabili/stabili si registrano come relazioni parametrizzate (gli argomenti della procedura diventano parametri di query, la stessa forma delle tabelle OpenAPI GET), e le procedure volatili si registrano come mutation/funzioni tracciate. Le routine scoperte passano attraverso la governance Stage-2 in modo identico a quelle registrate a mano. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| Campo | Richiesto | Default | Descrizione |
| ------- | ---------- | --------- | ------------- |
| `name` | Sì | — | Nome del campo GraphQL |
| `source_id` | Sì | — | Origine contenente la funzione |
| `schema` | No | `public` | Schema del database |
| `function_name` | Sì | — | Nome effettivo della funzione database |
| `returns` | Sì | — | ID della tabella registrata restituita dalla funzione (REQ-207) |
| `arguments` | No | `[]` | Elenco di definizioni argomento `{name, type}` (REQ-211) |
| `visible_to` | No | `[]` | Ruoli che possono chiamare questa funzione |
| `writable_by` | No | `[]` | Ruoli che possono chiamarla come mutation |
| `domain_id` | No | `""` | Dominio a cui appartiene questa funzione |
| `description` | No | `null` | Descrizione del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` (REQ-205) |

### Webhook

Esponi un endpoint HTTP esterno come query o mutation GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| Campo | Richiesto | Default | Descrizione |
| ------- | ---------- | --------- | ------------- |
| `name` | Sì | — | Nome del campo GraphQL |
| `url` | Sì | — | URL dell'endpoint webhook |
| `method` | No | `POST` | Metodo HTTP |
| `timeout_ms` | No | `5000` | Timeout della richiesta in millisecondi |
| `returns` | No | `null` | ID della tabella registrata, o null per tipo inline |
| `inline_return_type` | No | `[]` | Elenco di campi `{name, type}` per forme di ritorno personalizzate (REQ-210) |
| `arguments` | No | `[]` | Elenco di definizioni argomento `{name, type}` |
| `visible_to` | No | `[]` | Ruoli che possono chiamare questo webhook |
| `domain_id` | No | `""` | Dominio a cui appartiene questo webhook |
| `description` | No | `null` | Descrizione del campo GraphQL |
| `kind` | No | `mutation` | `"query"` o `"mutation"` |

---

## Autenticazione

L'autenticazione viene configurata sotto la chiave `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Provider | Descrizione |
| ---------- | ------------- |
| `none` | Nessuna autenticazione; tutte le richieste trattate come il `default_role` |
| `firebase` | Firebase Authentication; richiede `project_id` e `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | OAuth 2.0 generico (REQ-123) |
| `simple` | Nome utente/password senza un provider esterno (REQ-124) |

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

`assignments_source: claims` legge le assegnazioni di ruolo dalle claim JWT. `assignments_source: provisa` le legge dallo store di assegnazioni proprio di Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Routing di esecuzione

**Esecuzione diretta** — Le query a singola origine RDBMS vengono instradate al driver nativo per latenza sub-100ms (REQ-027). Le origini richiedono sia una voce `SOURCE_TO_DIALECT` che una voce `SOURCE_TO_CONNECTOR` per supportare questo percorso (REQ-229).

**Esecuzione federata** — Le query multi-source e le origini senza driver diretto vengono instradate attraverso il motore di federazione (REQ-028). Provisa include un motore di federazione integrato; punta al tuo cluster compatibile per deployment su larga scala (REQ-226).

**Statistiche** — Alla registrazione, Provisa esegue `ANALYZE` su ogni tabella pubblicata per innescare l'ottimizzatore cost-based (conteggio righe, frazione null, valori distinti, min/max). I fallimenti vengono loggati e non bloccano la registrazione (REQ-275).

---

## Origini grafo e semantiche

### Neo4j

Registra un database a grafo Neo4j come origine interrogabile. Gli steward scrivono query Cypher che proiettano valori scalari; Provisa mette in cache i risultati e li espone come tipi GraphQL (REQ-295).

Le query Cypher devono usare accessori di proprietà nella clausola `RETURN` (`RETURN n.id AS id, n.name AS name`) — restituire oggetti nodo viene rifiutato al momento della registrazione (REQ-296).

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

L'endpoint di preview (`POST /admin/sources/neo4j/{id}/preview`) restituisce righe di esempio e blocca la registrazione se il Cypher restituisce oggetti nodo (REQ-296).

### SPARQL

Registra qualsiasi triplestore conforme a SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, ecc.) come origine interrogabile (REQ-297).

Le query devono essere query `SELECT`. I nomi delle variabili nella clausola `SELECT` diventano automaticamente nomi di colonna (REQ-297).

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

Entrambi i connettori usano la pipeline di cache delle origini API — i risultati vengono memorizzati in PostgreSQL con TTL configurabile, rendendoli disponibili per JOIN federati cross-source (REQ-295, REQ-297, REQ-299).

---

## Esempi di connessione

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

### Query cross-source

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

Le porzioni a singola origine vengono instradate direttamente (REQ-027). I JOIN cross-source federano con coercizione di tipo automatica (REQ-028, REQ-552).
