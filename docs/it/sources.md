# Tipi di origine {: #source-types }

## Modello di esecuzione {: #execution-model }

Ogni query viene infine eseguita attraverso il motore di federazione, che fornisce federazione su tutte le origini. Le origini rientrano in tre categorie in base alla loro connettività. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Categoria | Ha driver diretto | Ha connettore federato | Esempi |
| --- | --- | --- | --- |
| **Direct-capable** | Sì | Sì | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Solo federazione** | No | Sì | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (basato su S3) |
| **Direct-read (replica)** | Sì | Sì | Snowflake, Databricks, ClickHouse — il driver legge i dati e atterra una replica; le query girano contro la replica nel motore attivo |
| **Direct (nessun connettore cross-source)** | Sì | No | Athena — le istruzioni su singola origine vengono eseguite interamente in Athena; i join tra origini diverse atterrano una replica completa |
| **Materializza → Federazione** | No | No | REST/OpenAPI, GraphQL remoto, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (ricevitore push), GovData, SharePoint, Salesforce, ServiceNow, Cloud inventory, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Le origini **Direct-capable** eseguono query a singola origine tramite il loro driver nativo (sub-100ms), bypassando il motore di federazione (REQ-027, REQ-229). Mantengono il pieno supporto del connettore e partecipano alla federazione quando unite ad altre origini (REQ-028).

Le origini **Solo federazione** vengono sempre interrogate attraverso il layer di federazione. Non esiste un driver diretto (REQ-229).

Le origini **Direct-read (replica)** hanno un DirectDriver che legge dal warehouse nativamente (Arrow-native dove disponibile), atterra una replica nello store di materializzazione del motore attivo, e poi le query girano contro quella replica. Vedi [Warehouse come origini nominate](#warehouse-come-origini-nominate).

Le origini **Direct (nessun connettore cross-source)** hanno un DirectDriver che esegue le istruzioni su singola origine interamente nell'origine — ogni filtro, colonna e limite viene spinto fino ad essa. Quando un'istruzione unisce l'origine a qualsiasi altra, il driver legge i dati e atterra una replica; il join viene eseguito contro quella replica nel motore attivo. Per queste origini non esiste alcun connettore Trino.

Le origini **Materialize** non hanno un connettore federato. Provisa recupera i loro dati (all'avvio o al momento della query) e li mette in cache come Parquet su S3 o in PostgreSQL, rendendoli raggiungibili dal motore di federazione per query cross-source (REQ-309).

---

## Tutte le origini {: #all-sources }

Provisa registra **72** tipi di origine, quelli che il modulo Sources offre. Le tabelle qui sotto le coprono tutte e 72; l'indice è il conteggio. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, where `graphql` and `grpc` are `graphql_remote` and `grpc_remote`; Kaggle, Wikipedia, GitHub, GitLab and Stripe are counted as distinct sources even though they register through the `files`, `graphql_remote` and `openapi` connectors internally]

| # | Gruppo | Tipi di origine |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Data warehouse cloud](#data-warehouse-cloud) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytics / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data Lake / formati tabella aperti](#data-lake-formati-tabella-aperti) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Sorgenti di codice](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Streaming](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Ricevitore push](#ricevitore-push) | `ingest` |
| 45–46 | [Grafo e semantico](#grafo-e-semantico) | `neo4j`, `sparql` |
| 47–51 | [Basate su file](#basate-su-file) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Osservabilità e altro](#osservabilita-e-altro) | `google_sheets`, `prometheus` |
| 54–60 | [Connettori SaaS enterprise](#connettori-saas-enterprise) | `sharepoint`, `salesforce`, `servicenow`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 61–66 | [Origini API](#origini-api) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 67 | [GovData](#govdata) | `govdata` |
| 68–69 | [Controlli di qualità dei dati (REQ-1443)](#controlli-di-qualita-dei-dati-req-1443) | `soda`, `great_expectations` |
| 70 | [Data profiler](#data-profiler) | `data_profiler` |
| 71 | [Dataset Kaggle](#kaggle-datasets) | Kaggle (messo in stage tramite il modulo Sources; si registra come origine `files` — vedere [Dataset Kaggle](#kaggle-datasets)) |
| 72 | [Wikipedia](#wikipedia) | Wikipedia (si registra come origine `files` che scansiona le pagine indicate — vedere [Wikipedia](#wikipedia)) |

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
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | No |
| `hiveserver2` | impyla (HiveServer2 over Thrift) | — | hive | No |

I database wire-compatible riusano il driver JDBC, il driver async nativo e il dialetto di un wire di base — CockroachDB, YugabyteDB e Greenplum usano il wire PostgreSQL; TiDB usa il wire MySQL. Richiedono solo voci di registro, nessun nuovo codice connettore. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) e `airport` (server Arrow Flight) sono tipi di origine registrati, raggiunti in loco tramite estensioni community di DuckDB quando DuckDB è il motore attivo — nessun driver diretto, nessun connettore federato. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA viene letto tramite il proprio driver e replicato nello store del motore attivo, quindi è raggiungibile su ogni motore. Il modulo richiede host, porta (predefinita 39015), nome utente, password e database; su un sistema multi-tenant il database indica il tenant. Registra tabella elenca gli schemi che l'utente può leggere, escludendo gli schemi di sistema di HANA. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Un endpoint HiveServer2 letto direttamente su Thrift, per un deployment Hive in cui l'endpoint è l'unica cosa raggiungibile. È distinto da `hive` e `hive_s3`, che leggono i file e il metastore del warehouse e non passano mai da HiveServer2. Le righe vengono lette attraverso l'endpoint e replicate nello store del motore attivo. Il modulo richiede host, porta (predefinita 10000), nome utente, password e database; `federation_hints.auth_mechanism` seleziona l'autenticazione (predefinita `PLAIN`, ad esempio `GSSAPI` per Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Data warehouse cloud {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Tipo di origine | Driver diretto | Nome connettore | Dialetto | Mutation | Note |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Federata | Legge via snowflake-connector-python; atterra una replica; `account`/`warehouse`/`role` in `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Federata | Nessun DirectDriver; raggiunta via motore di federazione o ATTACH del motore BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Federata | Legge via databricks-sql-connector (Cloud Fetch, Arrow); atterra una replica; `http_path` richiesto in `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Federata | — |
| `athena` | AthenaDriver | — | athena | Parziale (INSERT sempre; UPDATE/DELETE/MERGE solo su Iceberg) | Query serverless su S3; le istruzioni su singola sorgente vengono eseguite interamente in Athena; i join tra sorgenti diverse atterrano una replica; `region` obbligatoria in `federation_hints` (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Federata | Microsoft Fabric Warehouse; T-SQL su TDS, auth Azure AD; atterra una replica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Federata | Azure Synapse SQL; T-SQL su TDS, auth Azure AD; atterra una replica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Federata | Coordinator Trino/Presto remoto letto via il dialetto SQLAlchemy trino; atterra una replica su qualsiasi motore (REQ-994) |

#### `athena`

Amazon Athena è un servizio di query serverless su dati in S3, fatturato per byte analizzato. Un'istruzione che opera solo su tabelle Athena viene eseguita direttamente in Athena: filtri, elenchi di colonne e `LIMIT` vengono trasmessi integralmente, quindi vengono analizzati e fatturati solo i byte necessari ad Athena per rispondere a quell'istruzione. Un'istruzione che unisce una tabella Athena con un'altra sorgente prima atterrisce una replica completa da Athena — l'intera tabella viene letta e Athena la fattura — poi il join viene eseguito contro quella replica. Le impostazioni Replicate e di protezione del carico della sorgente regolano la frequenza degli atterraggi; la nota nella documentazione è l'unica regola. (REQ-1981)

**Scoperta.** Register Table chiama l'API dei metadati di Athena — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Queste chiamate non eseguono query, quindi elencare database e tabelle e leggere le colonne di una tabella non ha alcun costo per byte. Le colonne di una tabella vengono lette solo quando quella tabella viene selezionata. Le chiavi di partizione seguono le colonne regolari. I tipi vengono mappati tramite la voce `athena` in `provisa/core/ir_types.py`: `array<…>`, `map<…>` e `struct<…>` diventano `text`; un tipo sconosciuto viene rifiutato nominandolo. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns`; `provisa/core/ir_types.py` voce `"athena"`]

**Campi sorgente.**

| Campo | Dove | Obbligatorio | Predefinito | Note |
| --- | --- | --- | --- | --- |
| `database` | sorgente | Sì | — | Il database Glue interrogato per impostazione predefinita |
| `username` | sorgente | No | — | ID chiave di accesso AWS; può essere `${secret:…}` o `${env:…}` |
| `password` | sorgente | No | — | Chiave di accesso segreta AWS; può essere `${secret:…}` o `${env:…}` |
| `region` | `federation_hints` | Sì | — | Regione AWS, es. `us-east-1` |
| `workgroup` | `federation_hints` | No | `primary` | Gruppo di lavoro Athena |
| `s3_staging_dir` | `federation_hints` | No | predefinito del gruppo | Dove Athena scrive i risultati; obbligatorio se il gruppo di lavoro non ne imposta uno |
| `catalog` | `federation_hints` | No | `AwsDataCatalog` | Catalogo dati Athena |
| `role_arn` | `federation_hints` | No | — | ARN del ruolo IAM assunto per ogni chiamata |

Entrambi `username` e `password` vuoti significa che viene utilizzata la catena di credenziali di boto3: ruolo dell'istanza, variabili d'ambiente (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) o un profilo con nome. Uno senza l'altro viene rifiutato. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`; `provisa/federation/athena.py` `athena_client`]

**Scritture.** Ogni tabella registrata accetta `INSERT`. `UPDATE`, `DELETE` e `MERGE` sono disponibili solo su una tabella Iceberg; il formato viene registrato al momento della registrazione (`registered_tables.table_format`). Una scrittura su una tabella Hive che richiede `UPDATE`, `DELETE` o `MERGE` viene rifiutata nominando la tabella e l'operazione. Athena non restituisce le righe scritte — una clausola `RETURNING` viene rifiutata e GraphQL non offre campi di mutazione per queste tabelle; `INSERT`, `UPDATE`, `DELETE` e `COPY` sono i percorsi di scrittura. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` e `TRUNCATE` vengono rifiutati nominando la forma. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops`; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

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

Con un ruolo assunto per ogni chiamata (catena di credenziali per la sessione base):

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

`motherduck` (REQ-1998) è un database DuckDB ospitato da MotherDuck. Sul motore DuckDB viene collegato sul posto; sul motore Postgres resta il connettore del foreign data wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Connessione.** `database` indica il database MotherDuck, oppure una condivisione tramite il suo indirizzo di condivisione. `password` è il token MotherDuck, risolto come qualsiasi segreto di origine. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

Il motore DuckDB esegue `ATTACH 'md:<database>'`; per una condivisione, `database` è l'indirizzo della condivisione (`_share/...`), quindi il collegamento è `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Gestione del token.** Il motore accede con `SET GLOBAL motherduck_token = ?`, con il token passato come valore associato. Non compare nel testo di alcuna istruzione, quindi nessun log, trace o errore lo contiene. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Nessun token.** Un'origine senza `password` viene rifiutata con il suo nome prima che venga collegato qualcosa (`a MotherDuck source signs in with its token (the password field); none is set`). Senza questo controllo, l'estensione avvierebbe un accesso interattivo e attenderebbe circa un minuto. Anche un'origine senza `database` viene rifiutata. [tool-verified: `details`]
- **Un account per motore.** Un motore accede a un solo account MotherDuck. Una seconda origine MotherDuck con un token diverso viene rifiutata con il suo nome: "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Le scritture** passano dal motore, come sul motore Postgres. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**L'esecuzione ibrida (duale)** non richiede alcuna impostazione. Sul motore DuckDB ogni origine è collegata su un'unica connessione, quindi un'istruzione che unisce una tabella di un'origine file DuckDB con una tabella MotherDuck viene eseguita come un'unica istruzione DuckDB, e MotherDuck decide dove viene eseguita ciascuna parte. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Spostamento dei dati.** Con l'esecuzione duale, MotherDuck può spostare righe locali dalla sua parte per le parti che esegue. Un'istruzione di questo tipo può quindi inviare dati dal file locale all'account MotherDuck dell'organizzazione. Non unire un file con dati che devono restare sul nodo a un'origine MotherDuck, a meno che inviarli a quell'account sia accettabile. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Un'origine MotherDuck è servita solo dai motori DuckDB e Postgres. Nessun altro motore ha un connettore per essa, e nulla vi atterra le sue tabelle come repliche. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analytics / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Tipo di origine | Driver diretto | Nome connettore | Dialetto | Mutation | Note |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Federata | Legge via clickhouse-connect (HTTP); `secure: "true"` in `federation_hints` per TLS (REQ-986) |
| `druid` | — | druid | druid | No | — |
| `exasol` | — | exasol | exasol | No | — |
| `elasticsearch` | HTTP (motori nativi) | elasticsearch (Trino) | — | No | Su Trino il connettore lo legge, le sue proprietà provengono dal DSL di mapping del tipo [tool-verified: `trino_connectors.py:309`]; su ogni altro motore Provisa legge l'indice via HTTP (indici e mapping per Registra tabella, una scroll read per l'atterraggio) e atterra le righe [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | No | Connettore Trino `pinot`; `pinot.controller-urls` = host:porta del controller Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / formati tabella aperti {: #data-lake-open-table-formats }

Questi tipi di origine sono solo-federazione — nessun driver diretto, nessun dialetto. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Tipo di origine | Nome connettore | Time Travel | Note |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Sì (argomento `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Sì (argomento `as_of`, REQ-372) | — |
| `hive` | hive | No | — |
| `hudi` | — (motore `Hudi` di ClickHouse, zero-copy — REQ-1178) | No | Nessun connettore federato; raggiunta sul posto quando ClickHouse è il motore attivo |
| `hive_s3` | hive | No | Hive basato su S3 |

### NoSQL

`mongodb`, `cassandra` e `redis` hanno connettori Trino (`redis` costruisce le sue proprietà dal DSL di mapping del tipo). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Tipo di origine | Nome connettore | Mutation |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | No |
| `cassandra` | cassandra (Trino); lettura CQL tramite cassandra-driver su ogni altro motore | No | I keyspace sono schemi; Registra tabella elenca le tabelle di un keyspace e tipizza le colonne dai metadati di schema del cluster (partition key come chiavi primarie); l'extra `cassandra` installa il driver [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino); lettura redis-py senza HTTP su ogni altro motore | No | Un prefisso di chiave `<table>:*` è una tabella e un hash è una riga; Registra tabella elenca i prefissi presenti e tipizza le colonne di un prefisso dai suoi hash (una voce `mapping.tables` sovrascrive pattern, colonna chiave, tipo valore e colonne) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

### Qdrant

`qdrant` (REQ-1983, REQ-2000) viene letto e scritto dal client proprio di Qdrant, `qdrant-client`. Qdrant è **solo live**: una proprietà del tipo di origine, non un'impostazione. Risponde con il suo indice vettoriale e la sua Query API, e nessuna replica può rispondere allo stesso modo, quindi le sue tabelle non vengono mai replicate. Non esiste la scelta Replicate per un'origine Qdrant né per le sue tabelle; un valore `replicate` o `row_materialize` su una di esse viene rifiutato. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Connessione.** `host` è l'URL dell'istanza con la sua porta (`https://<cluster>.cloud.qdrant.io:6333`, oppure un `http://qdrant:6333` self-hosted); `password` è la sua chiave API, un riferimento a un secret risolto quando l'origine viene letta, vuota per un'istanza senza chiave. `federation_hints.prefer_grpc: true` legge dalla porta gRPC di Qdrant invece che via REST. L'origine ha uno schema, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`; `provisa/qdrant/introspect.py` `SCHEMA`]

**Tabelle di collection.** Una collection è una tabella. Registra tabella offre le colonne della collection: `id` (l'id del punto, la sua chiave, percorso `@id`), i campi del payload (un campo indicizzato tipizzato dal suo indice, ogni altro come suggeriscono i punti campionati; i campi annidati appiattiti con il loro percorso) e una colonna `json` per vettore (percorso `@vector`, o `@vector.<name>` per un vettore con nome), offerta non selezionata. Una colonna vettoriale indica il modello di embedding registrato di cui contiene gli embedding, scelto tra i modelli le cui dimensioni sono la dimensione del vettore (in Registra tabella e nella scheda Tipo e chiavi dell'editor); scegliere un modello registra il vettore. [tool-verified: `provisa/qdrant/introspect.py` `columns`; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Istruzioni su un'origine Qdrant.** Un'istruzione che legge una sola collection viene eseguita in Qdrant quando l'API di Qdrant la esprime esattamente:

- le condizioni sul payload diventano un filtro Qdrant (`=`, `<>`, `IN`, intervalli su numeri e date, `IS NULL`, `AND`, `OR`, `NOT`); la semantica di NULL di SQL è mantenuta, quindi `lang <> 'en'` salta i punti senza `lang`;
- una condizione solo su `id` (`=` o `IN`) è un `retrieve`; ogni altra lettura è uno `scroll` filtrato;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` è una `query_points` su un vettore la cui distanza è Cosine, con il filtro applicato prima del top-k. Un testo di query viene convertito in embedding dal modello del vettore. La risposta porta un avviso che il top-k è approssimato, scelto tramite l'indice di Qdrant.

Tutto il resto (un aggregato, un `GROUP BY`, una colonna calcolata o mascherata, un ordinamento diverso dal più simile per primo) è risolto dal motore, come sotto. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Istruzioni risolte dal motore.** Quando un'istruzione unisce una collection a un'altra origine, o calcola sulle sue colonne, la parte della collection viene letta in diretta da Qdrant per quell'istruzione e consegnata al motore come righe. La parte comprende le colonne della collection, le condizioni dell'istruzione sulla sola collection (comprese le sue regole di riga) e, quando la collection è letta da sola nel suo `SELECT`, l'ordinamento per similarità e il `LIMIT` di quel `SELECT`. Una parte che Qdrant non può esprimere esattamente viene rifiutata, indicando la tabella e il motivo. Una parte senza condizione, senza chiave e senza top-k leggerebbe l'intera collection e viene rifiutata anch'essa. Una condizione su un parametro (`$1`) è lasciata al motore. [tool-verified: `provisa/federation/live_only.py` `cut`, `read`; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Scritture.** `INSERT`, `UPDATE` e `DELETE` su una collection vengono eseguiti in Qdrant:

- `INSERT … VALUES` indica la colonna `id` e scrive nuovi punti. Un `id` che la collection contiene già viene rifiutato per nome. Il controllo legge prima gli id, quindi non è atomico con la scrittura.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` è un upsert. Le colonne che l'istruzione non imposta mantengono i loro valori.
- `UPDATE` e `DELETE` agiscono sui punti selezionati dal loro `WHERE`, come filtro Qdrant; una condizione senza forma di filtro viene rifiutata.
- Un testo scritto in una colonna vettoriale viene convertito in embedding dal modello della colonna. Un vettore scritto in essa deve avere le dimensioni del modello.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Tabelle di ricerca.** La ricerca vettoriale è una tabella di ricerca: il suo `query_template` contiene una richiesta della Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` è sempre un argomento; `using`, `filter`, `limit`, `score_threshold` e `model` sono opzionali. Qualsiasi valore di `using`, `filter`, `limit` o `score_threshold` che sia esattamente `"@name"` è anch'esso un argomento.
- Ogni argomento è un parametro della tabella (`query_arg`, REQ-1986), convertito rigorosamente nel tipo dichiarato e inserito nella richiesta come valore JSON, mai nel suo testo. L'argomento di `query` è `array(double)` ed è obbligatorio.
- Un chiamante passa gli argomenti come qualsiasi parametro: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` in SQL, argomenti di campo in GraphQL.
- Un argomento opzionale non dato non filtra nulla: la sua condizione viene omessa dal filtro. Senza `limit`, una ricerca restituisce il valore predefinito di Qdrant, 10 punti.
- Il filtro è applicato da Qdrant prima di scegliere i suoi punti più vicini, quindi è esatto. Qualsiasi altra condizione dell'istruzione restringe in seguito i punti restituiti dalla ricerca; non ne recupera altri.
- `score` è la metrica propria della collection per il vettore cercato: per una collection Cosine, la similarità del coseno. Registra tabella elenca i vettori della collection con dimensione e distanza.
- `model` nomina un modello di embedding registrato (`vector_models`, admin → AI Models). Un testo dato per la query viene allora incorporato da quel modello, e un vettore dato deve avere le dimensioni di quel modello (REQ-429, REQ-430). Senza `model`, la query deve essere un vettore.

Una ricerca viene letta per istruzione con i suoi argomenti, le sue righe conservate sotto quegli argomenti per il TTL della tabella. Una tabella di ricerca è di sola lettura. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read`; `provisa/api/data/materialization.py` `_is_native_read`]

### Streaming

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `kafka` | Connettore Kafka federato; schema via Confluent Schema Registry (Avro, Protobuf, JSON Schema), definizione manuale, o inferenza da campione (REQ-147, REQ-150) | No; l'output live di una tabella pubblica su Kafka (REQ-565) |
| `websocket` | Feed WebSocket esterno — connette, si iscrive, riceve eventi; risultati materializzati (REQ-338) | No |
| `rss` | Feed RSS 2.0 / Atom — polling, watermark per pubDate/updated; risultati materializzati (REQ-342, REQ-343) | No |

### Ricevitore push {: #push-receiver }

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `ingest` | Servizi esterni inviano eventi JSON via POST; risultati materializzati (REQ-331, REQ-335) | No |

### Grafo e semantico {: #graph-semantic }

| Tipo di origine | Meccanismo | Mutation |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, risultati messi in cache in PostgreSQL (REQ-295) | No |
| `sparql` | SPARQL 1.1 POST, risultati messi in cache in PostgreSQL (REQ-297) | No |

### Basate su file {: #file-based }

Tre meccanismi coprono i file. Le origini a file singolo e il crawler di file usano il campo `path` invece di `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) Il terzo, [Logs](#logs), indica i propri file con un glob nel suo `mapping` e legge solo file presenti sul server Provisa. [tool-verified: `provisa/logs/settings.py`]

**Origini database su file e a file singolo** — `sqlite`, `duckdb`, `csv`, `parquet` puntano `path` su un singolo file. (Un'origine `duckdb` tiene la sua posizione nel campo `database` del modulo.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Tipo di origine | Posizioni | Mutation |
| --- | --- | --- |
| `sqlite` | percorso locale, `https://`, `s3://`, `gs://`, `az://` | Sì su un file locale; nessuna su uno remoto |
| `duckdb` | percorso locale, `https://`, `s3://`, `gs://`, `az://` | Sì su un file locale; nessuna su uno remoto |
| `csv` | percorso locale, `https://`, `s3://`, `gs://`, `az://` (motore DuckDB) | No |
| `parquet` | percorso locale, `https://`, `s3://`, `gs://`, `az://` (motore DuckDB) | No |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Un percorso con qualsiasi altro schema (ad esempio `ftp://`) viene rifiutato: "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Credenziali per una posizione remota.** Inseritele nei `federation_hints` dell'origine. Ciascuna è limitata al bucket o al container del percorso stesso, quindi le credenziali di un'origine non vengono mai usate per la posizione di un'altra origine. Un'origine che non ne fornisce viene letta come posizione pubblica. [tool-verified: `object_store_secret_ddl`]

| Posizione | Chiavi `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; facoltativi `region`, `endpoint` (uno storage compatibile con S3 come R2 o MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (una chiave HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**I file SQLite e DuckDB remoti sono di sola lettura.** Il motore DuckDB li collega sul posto con `READ_ONLY`, e una tabella su uno di essi non offre insert, update o delete. Su un motore che non collega SQLite, un file SQLite remoto viene letto tramite una connessione DuckDB temporanea che lo collega in sola lettura, e le sue righe atterrano come di consueto. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Caricare un file.** Nel modulo di un'origine `sqlite`, `duckdb`, `csv` o `parquet`, caricate il file invece di digitare un percorso. Il file viene scritto su disco in streaming, mai tenuto in memoria, nella cartella dell'organizzazione sotto `files/<source id>/<file name>`, e il percorso dell'origine viene impostato su di esso. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Sostituzione.** Un nuovo caricamento prende il posto del file precedente, qualunque sia il suo nome, e l'origine viene ripristinata così che la lettura successiva veda il nuovo file. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Rimozione.** Rimuovere l'origine rimuove il file caricato. Nient'altro lo rimuove. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Chi.** Solo i ruoli con la capability `source_registration` possono caricare. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Limite di dimensione.** L'impostazione `sources.max_upload_bytes` (ambiente `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) limita il file, 1 GiB (1073741824 byte) per impostazione predefinita, minimo 1, letta all'inizio di un caricamento. Un file più grande viene rifiutato con HTTP 413 indicando il limite, e non ne viene conservato nulla. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Percorso.** Finché il percorso è il file caricato dell'origine, il campo percorso del modulo è di sola lettura. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Nomi.** L'id dell'origine e il nome del file possono contenere solo lettere, cifre, `.`, `_` e `-`. [tool-verified: `source_uploads._SEGMENT`]
- **Archiviazione.** [inferred from the `source_uploads` docstring] Un file caricato viene archiviato come qualsiasi file dell'organizzazione: nella cartella dell'organizzazione, cifrato quando lo è quella cartella, e visibile a ogni nodo su un mount condiviso.

**Scritture su un file SQLite o DuckDB locale, e lock.** Una scrittura prende il lock del file e non lo attende mai. Quando un altro processo o nodo detiene il file, la scrittura viene rifiutata, indicando l'origine e che il suo file è bloccato. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** La scrittura imposta `PRAGMA busy_timeout = 0`, quindi un file bloccato fallisce subito. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** DuckDB condivide un file solo tra aperture in sola lettura. Per questo il collegamento del motore e la connessione propria dell'origine tengono il file in sola lettura, e ogni nodo lo legge contemporaneamente. Una scrittura sospende i detentori di questo processo, apre il file in lettura e scrittura solo per quell'istruzione e lo chiude. Se l'apertura non riesce a prendere il lock, la scrittura viene rifiutata. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Mount condivisi.** In una cartella dell'organizzazione cifrata il cui mount è condiviso da più nodi (impostazione `org_files.shared_mount`), una scrittura su un file SQLite o DuckDB al suo interno viene rifiutata direttamente, indicando l'origine. Lo strato di cifratura delle cartelle (gocryptfs) non porta i lock dei file agli altri nodi, quindi nulla impedirebbe a due nodi di scrivere contemporaneamente. Le letture funzionano comunque. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Una cartella condivisa senza cifratura deve essere montata con lock che raggiungono ogni nodo: un mount NFS con il locking attivo, non `nolock` e non `local_lock`. Nessuna verifica lo controlla; è un requisito di deployment. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Crawler di file (origine `files`)** — punta `path` su un glob, lo esegue in crawling ricorsivamente, e registra la directory come catalogo federato di tabelle. Legge molti formati su molti trasporti; gli insiemi sotto provengono dal connettore file (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch e `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; liste di formati e trasporti dall'adapter calcite `file` — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formati | Trasporti |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, e documenti convertiti in tabelle — HTML, Markdown, DOCX, PPTX | Filesystem locale, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST e Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Qualsiasi origine `files` può anche dichiarare un `crawl` nel proprio mapping, con le stesse impostazioni elencate sotto [Wikipedia](#wikipedia): l'origine legge prima le pagine web e atterra ogni tabella di dati HTML e ogni file di dati collegato nella propria directory come tabella. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

Sul motore DuckDB, `files` viene letta nativamente — una vista scanner `read_csv_auto` per ogni `<table>.csv` sotto la directory risolta (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. Su un motore senza un proprio connettore `files`, le righe atterrano tramite lo stesso server pgwire Calcite integrato nel connettore (`pgwire-file`) usato da sharepoint/splunk (REQ-954) — vedere [Connettori SaaS enterprise](#connettori-saas-enterprise) sotto. La copertura end-to-end della UI (modulo Sources → Registra tabella → query SQL) e il percorso di atterraggio pgwire sono verificati in REQ-1694.

#### Logs (REQ-1996) {: #logs }

Un'origine `logs` trasforma i file di log sul server Provisa in una tabella. I file si indicano con un percorso completo o con un glob, e si sceglie il formato in cui sono scritti i loro record. Ogni record diventa una riga. Un record che il formato non sa leggere viene conservato, con il testo grezzo e il motivo, mai scartato. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

Il mapping accetta quattro chiavi e rifiuta qualsiasi altra per nome: `glob` e `format` (entrambe obbligatorie), e `pattern` e `record_start`, che solo il formato `regex` accetta. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

Nel modulo Sources, un'origine Logs compare come "Log files" sotto File. Il pulsante **Preview** mostra i file che il glob seleziona, le colonne che il formato fornisce e le prime 20 righe del file più recente, prima che venga salvato qualsiasi cosa. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**Il glob.** Deve essere un percorso completo; `~` viene espanso e un percorso relativo viene rifiutato ("name the files by their full path"). La corrispondenza usa `glob` di Python in modalità non ricorsiva, quindi `**` si comporta come `*` e corrisponde entro un solo livello di directory. Si prendono solo i file regolari, in ordine di nome. Un file vuoto viene saltato finché non ha byte. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] Il glob può nominare una variabile d'ambiente, come in `${env:LOG_DIR}/*.log`. La sorgente conserva il riferimento così come è scritto e lo risolve a ogni lettura dei file; una variabile non impostata viene rifiutata per nome. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

**Le colonne proprie di ogni riga.** Vengono prima delle colonne del formato:

| Colonna | Tipo | Contenuto |
| --- | --- | --- |
| `_file_id` | text | L'identità del file: un checksum dei suoi primi 4096 byte, decompressi |
| `_file_name` | text | Il percorso con cui il record è stato letto |
| `_line_offset` | bigint | Offset in byte della prima riga del record nel testo decompresso |
| `_entry` | bigint | Quale voce del record è la riga: 0, tranne per un record batch come `Records` di un file CloudTrail |
| `_raw` | text | Il record come è stato scritto |
| `_parse_error` | text | Perché il record, o uno dei suoi valori, non ha potuto essere letto; NULL quando lo è stato |
| `_landed_at` | timestamp | Quando la riga è atterrata |

`_file_id`, `_line_offset` e `_entry` sono la chiave della riga, quindi un record letto due volte è una sola riga. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Formati e preset.** La selezione del formato li raggruppa per famiglia; l'ultima scelta, `regex`, è il pattern definito da voi. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

| Famiglia (etichetta nel modulo) | Nomi dei formati |
| --- | --- |
| Formati comuni | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Database e runtime | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Container e Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Monitoraggio della sicurezza di rete | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Server web e proxy | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Personalizzato | `regex` |

Sono 56 formati integrati più `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Come un formato produce le colonne.** I formati ottengono le colonne in uno di quattro modi. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Colonne fisse.** La maggior parte dei formati dichiara colonne e tipi. `rfc3164`, per esempio, dà `timestamp`, `facility`, `severity`, `host`, `process`, `pid` e `message`; `facility` e `severity` sono calcolate dal valore `<PRI>`. Una riga syslog BSD non porta l'anno, quindi l'anno è quello dell'ultima scrittura del file, oppure l'anno precedente se il mese della riga è successivo a quello della scrittura.
- **Campi nominati dal record.** `jsonl` e `logfmt` prendono le colonne dai campi visti nelle prime righe dei file (impostazione `logs.preview_lines`, predefinita 1000). Ogni colonna riceve il tipo più stretto in cui rientrano tutti i valori visti: boolean, poi integer, float, timestamp e json, altrimenti text. Un campo che un record successivo porta e che nessuna colonna ha finisce in una colonna json `extra`.
- **Campi nominati da un'intestazione.** `w3c` e i preset costruiti su di esso leggono la riga `#Fields:` di ciascun file; `date` e `time` si uniscono in un'unica colonna `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` e `zeek_tsv` leggono allo stesso modo le righe di intestazione che iniziano con `#`, e `aws_vpc_flow` legge una riga di intestazione che inizia con `version`. Il punto in cui è stata letta una riga di intestazione resta con il file, quindi un file letto più avanti mantiene i suoi campi.
- **`regex`.** Ogni gruppo nominato, come `(?P<level>…)`, è una colonna di tipo text.

La schermata Register Table elenca queste colonne con i loro tipi e segna la chiave. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] Quando le righe vengono lette, ogni valore è convertito nel tipo che la sua colonna ha nella tabella registrata, quindi un tipo cambiato lì è quello applicato. Un valore che la colonna non può contenere diventa NULL e il suo motivo va in `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Un record più lungo di `logs.max_line_bytes` (predefinito 65536) conserva i primi byte in `_raw` e non viene analizzato; `_parse_error` ne indica la lunghezza. Un file che inizia con un byte-order mark UTF-8 o UTF-16 è letto in quella codifica, e qualsiasi altro file come UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Record su più righe.** Un formato con un pattern di inizio record crea un record da una riga che lo soddisfa più tutte le righe successive che non lo soddisfano. Uno stack trace resta con la sua eccezione, e una voce di query lenta resta intera. Le righe prima della prima corrispondenza sono un record a sé; di solito il formato lo rifiuta, e la riga viene conservata. I formati integrati con un inizio record sono `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` e `azure_monitor`. Con `regex`, impostate voi `record_start`. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Due formati leggono i propri record da soli invece che per riga. `kubernetes_cri` unisce in un record le righe parziali (`P`) che il runtime ha spezzato, e `windows_evtx` legge il file binario `.evtx`. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Un file ancora in scrittura è letto fino all'ultima riga completa. Per un formato multiriga è letto fino all'inizio del suo ultimo record, che la riga successiva potrebbe continuare. Quando il file resta 2 secondi senza scritture, quell'ultimo record è considerato intero. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Rotazione.** Un file è riconosciuto da un checksum dei suoi primi 4096 byte, decompressi, non dal suo nome. Quando `system.log` viene ruotato in `system.log.0` e poi compresso in `system.log.0.gz`, ogni copia è lo stesso file. La lettura prosegue dal byte raggiunto dall'ultima lettura, quindi nessuna riga è letta due volte. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- La compressione è riconosciuta dai primi byte del file, mai dal nome: gzip e bzip2. Un file compresso è considerato completo.
- Una copia ruotata e la sua gemella compressa, entrambe selezionate, sono lette una volta sola, dalla compressa.
- Un file trovato più corto dell'offset già letto è stato troncato: è un file nuovo, letto dall'inizio. Le righe del vecchio restano.
- I file più corti di 4096 byte che iniziano allo stesso modo si distinguono per inode, quindi restano due file.
- Un file che il glob non seleziona più viene dimenticato; le sue righe restano come storico.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Aggiornamento e consegna live.** Una tabella Logs è una replica. Nulla osserva i file. Invece, quando il segnale di cambiamento dell'origine è `probe` o `ttl_probe`, la sua sonda calcola un digest di percorso, inode, dimensione e ultima scrittura di ogni file selezionato; quando il digest differisce, viene richiesto un aggiornamento. I tipi di sonda che un'origine Logs accetta sono `hash` e `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Un aggiornamento legge solo ciò che è nuovo. Ogni file è letto da dove è arrivato l'ultimo passaggio, e le righe sono scritte per chiave, 5000 alla volta, prima che la nuova posizione sia conservata. Un passaggio che si ferma tra i due passi rilegge quei record, il che lascia le stesse righe. Ogni passaggio marca le proprie righe in `_landed_at`, la colonna che la consegna live usa come watermark. Una definizione di tabella modificata è una ricostruzione completa che legge ogni file dall'inizio. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Radici consentite.** L'impostazione operatore `logs.allowed_roots` elenca le directory in cui un glob può risolversi. Il valore predefinito è `["/var/log"]`; è un elenco nella scheda Security di Admin → Settings, impostabile anche con `PROVISA_LOGS_ALLOWED_ROOTS`, e una modifica chiede conferma. Ogni radice è risolta dopo l'espansione di `~` e dei symlink. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Si applicano due controlli. La parte fissa del glob, fino al primo carattere jolly, deve risolversi dentro una radice; è controllata quando l'origine viene salvata e a ogni anteprima o lettura. Anche ogni file selezionato deve risolversi dentro una radice una volta seguiti i symlink, quindi un symlink che esce da `/var/log` viene rifiutato. Il rifiuto indica il percorso, ciò in cui si è risolto e le radici consentite:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

Al salvataggio il codice è `schema.logs_path_refused`; in anteprima è un 403 con `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Rifiuto in hosting (SaaS).** Un deployment che ospita molte organizzazioni non legge alcun file di log proprio. Ogni controllo di percorso rifiuta per primo, al salvataggio, in anteprima e in lettura, con "`<path>`: a server that holds many organizations reads no log file of its own". I file sono del server, non di una singola organizzazione, quindi nessun tenant può leggerli. Il deployment sa se ospita molte organizzazioni dal suo stato di multitenancy. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Dataset Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle è una piattaforma di download di file. Un dataset Kaggle messo in stage si registra come origine di tipo `files` e viene interrogato tramite lo stesso connettore pgwire-file usato da qualsiasi altra origine `files` — non esiste un `SourceType` `kaggle` nell'enum. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — nessun letterale `kaggle`]

**Aggiungere un dataset.** Apri Sources → Subscriptions → Kaggle. Il modulo esegue due passaggi sequenziali, perché l'endpoint di ricerca dataset di Kaggle richiede l'autenticazione — il selettore non può apparire prima che il token sia validato. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Token** — inserisci un token API Kaggle e clicca "Validate". La validazione chiama `POST https://www.kaggle.com/api/v1/datasets/create/new` con corpo vuoto. Un `401` significa non valido; qualsiasi altra risposta significa valido (la validazione del payload propria di Kaggle scatta prima che venga creato qualsiasi dataset — nulla viene persistito). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Selettore** — un campo di ricerca live-as-you-type interroga `GET /api/v1/datasets/list`. Ogni risultato mostra titolo e descrizione del dataset. Selezionane uno, poi clicca "Add Dataset".

Cliccare "Add Dataset" scarica il bundle da `GET /api/v1/datasets/download/{owner}/{ref}`, decomprime i membri CSV e Parquet in `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`, e crea **una** origine di tipo `files` il cui `path` è quella directory radice. Il token non viene mai memorizzato lato server. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Dopo aver aggiunto l'origine, registra le sue tabelle tramite la normale schermata Registra tabella. La discovery ricorsiva delle directory del connettore pgwire-file elenca ogni file come propria tabella — lo stesso meccanismo usato da qualsiasi altra origine `files` (REQ-1690). (REQ-1783)

**Limitazione v1.** Un bundle contenente un file `.sqlite` o `.db` viene rifiutato immediatamente con un errore chiaro. Vengono messi in stage solo file CSV e Parquet. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Denominazione delle tabelle.** Una tabella scansionata si chiama `<page>__<name>`: il titolo della pagina, poi la didascalia della tabella, oppure l'ultima intestazione sopra la tabella quando non ha didascalia, entrambi nell'ortografia dei nomi di colonna dell'origine. Per esempio, la tabella sotto l'intestazione "Tallest buildings in the world" nella pagina `List_of_tallest_buildings` è `list_of_tallest_buildings__tallest_buildings_in_the_world`. Un file di dati collegato conserva il proprio nome. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Aggiornamento.** Per riscaricare un dataset dopo che Kaggle pubblica una nuova versione, chiama la mutation GraphQL `refreshKaggleSource` con l'ID dell'origine e un token valido. Questo rimette in stage i file sul posto e invalida la cache dell'endpoint pgwire-file, cosicché il connettore recepisce eventuali modifiche allo schema alla query successiva. `kaggle_owner` e `kaggle_ref` memorizzati in `federation_hints` al momento della creazione identificano quale dataset riscaricare. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Nessun percorso di configurazione YAML statico.** Le origini Kaggle vengono create solo tramite il modulo Sources. Un'origine Kaggle esportata in YAML appare come `type: files` con `kaggle_owner` e `kaggle_ref` in `federation_hints`. Riscaricare da Kaggle richiede il flusso di aggiornamento della UI o la mutation `refreshKaggleSource` — puntare il `path` YAML a una directory già in stage è l'alternativa per ambienti air-gapped.



#### Wikipedia (REQ-1960) {: #wikipedia }

Wikipedia scansiona le pagine che indichi. Ogni tabella di dati in una pagina, e ogni file CSV, TSV, Excel, JSON o Parquet collegato, diventa una tabella dell'origine. Come Kaggle, si registra come origine `files` con un `crawl` nel proprio mapping. Aggiungerla non registra nulla: uno steward registra le tabelle desiderate tramite la normale schermata Registra tabella. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

**Campi di configurazione.**

| Campo | Predefinito | Significato |
| --- | --- | --- |
| Pages to start from | nessuno (obbligatorio) | Uno per riga: il titolo di una pagina, oppure l'indirizzo di una pagina nell'edizione scelta |
| Language edition | `en` | Il codice dell'edizione, ad esempio `en`, `de` o `ja` |
| Link depth | `1` | A quanti link di distanza da una pagina iniziale leggere; `0` legge solo le pagine iniziali |
| Page limit | `25` | Il massimo di pagine che una scansione legge |
| Contact | vuoto | Inviato con ogni richiesta perché Wikipedia possa contattarti, come richiede la sua policy |
| Follow "See also" links | disattivato | Disattivato esclude quella sezione dall'articolo |
| What this edition calls its "See also" section | vuoto | Richiesto solo quando la casella sopra è disattivata e Provisa non sa indicare la sezione per l'edizione scelta. Il titolo della sezione come lo mostra una pagina, ad esempio `Voir aussi` |
| Directory for the crawl's files | vuoto | Dove vengono scritte tabelle e file di dati. Vuoto: `<PROVISA_DATA_DIR>/crawl/<source id>` |

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa conosce l'intestazione "See also" di queste edizioni: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Per qualsiasi altra edizione con "See also" disattivato, il modulo chiede come l'edizione chiami quella sezione; digita il suo titolo come lo mostra una pagina (ad esempio `Voir aussi`). Un'origine salvata senza di esso viene rifiutata (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Quali link vengono seguiti.** Un link è scelto da ciò che il markup della pagina stessa dice che è, mai da un nome nel suo indirizzo: tali nomi differiscono in ogni edizione linguistica e compaiono anche nei titoli ordinari. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Seguiti: link ad altri articoli che si trovano nel testo, nelle liste e nelle tabelle di dati dell'articolo stesso. Il segno di markup è `a[rel='mw:WikiLink']`.
- Non seguiti: pagine non ancora scritte, la pagina stessa, ricerche di libri, pagine di file, altri wiki e siti esterni.
- Non letti affatto (rimossi prima di raccogliere link e tabelle): riquadri di navigazione, riquadri informativi, note e citazioni, categorie, indice, immagini e messaggi di manutenzione.
- Non letta a meno che tu non attivi l'opzione: la sezione "See also".

Solo le tabelle che i redattori contrassegnano come tabelle di dati (`table.wikitable`) diventano tabelle, e una tabella con meno di 2 righe non viene offerta. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Impostazioni avanzate di scansione.** Il modulo le tiene raccolte sotto "Crawl settings" e mostra ciascuna con il suo valore predefinito; lascia un campo vuoto per mantenerlo. In un mapping `files` gli stessi nomi vanno sotto `crawl:`, e un nome fuori da questo elenco viene rifiutato. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

| Impostazione | Predefinito Wikipedia | Significato |
| --- | --- | --- |
| `start_urls` | le pagine che hai indicato | Pagine da cui parte la scansione |
| `max_depth` | `1` | Profondità dei link |
| `max_pages` | `25` | Limite di pagine |
| `request_delay` | `1 seconds` | Pausa tra le richieste |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, con il tuo contatto aggiunto | Il nome con cui la scansione si presenta |
| `content_selector` | `#mw-content-text .mw-parser-output` | Selettore CSS: link e tabelle vengono letti solo al suo interno |
| `remove_selectors` | l'elenco di parti descritto sopra | Selettori CSS per le parti escluse. Indicarlo sostituisce l'intero elenco, regola "See also" compresa |
| `link_selector` | `a[rel='mw:WikiLink']` con esclusioni | Selettore CSS che un link deve soddisfare per essere seguito |
| `link_exclude_patterns` | vuoto | Espressioni regolari; un link il cui indirizzo ne contiene una non viene seguito |
| `follow_external_links` | `false` | Segue i link verso altri siti |
| `allowed_domains` | non impostato | Domini che la scansione può visitare |
| `table_selector` | `table.wikitable` | Selettore CSS che una tabella deve soddisfare |
| `generate_tables_from_html` | non impostato | Se le tabelle HTML diventano tabelle |
| `html_table_min_rows` | `2` | Numero minimo di righe in una tabella |
| `html_table_max_rows` | non impostato | Numero massimo di righe in una tabella |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Tipi di file collegati che diventano tabelle |
| `html_cache_ttl` | `1 days` | Per quanto tempo una scansione viene conservata |
| `max_html_size` | non impostato | La pagina più grande letta, ad esempio `10MB` |
| `max_data_file_size` | non impostato | Il file di dati più grande letto, ad esempio `100MB` |

**Denominazione delle tabelle.** Una tabella scansionata si chiama `<page>__<name>`. Il nome è l'`id` della tabella stessa quando ne ha uno, altrimenti la sua didascalia, altrimenti l'intestazione più vicina sopra di essa. Per esempio, una tabella sotto l'intestazione "Tallest buildings" nella pagina `List_of_tallest_buildings` diventa `list_of_tallest_buildings__tallest_buildings` dopo la normalizzazione del nome. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Aggiornamento.** Una scansione viene conservata per il periodo di cache, `1 days` per impostazione predefinita (`html_cache_ttl`). Le pagine vengono recuperate di nuovo solo dopo che quel tempo è trascorso. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]

### Osservabilità e altro {: #observability-other }

`prometheus` ha un connettore Trino (proprietà costruite dal DSL di mapping del tipo). `google_sheets` è un tipo di origine registrato senza connettore Trino e materializza attraverso la pipeline di cache API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Tipo di origine | Nome connettore | Mutation |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (materializzata) | No |
| `prometheus` | prometheus | No | Una metrica è una tabella e un campione è una riga (`timestamp`, `value`, una colonna per etichetta); su ogni motore privo di connettore live Provisa legge l'API HTTP — nomi delle metriche ed etichette per Registra tabella, `query_range` sull'intervallo della tabella per l'atterraggio [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689) |

### Connettori SaaS enterprise {: #enterprise-saas-connectors }

Google Workspace e Microsoft 365 sono letti da loader propri; vedere [Google Workspace e Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, l'inventario cloud e Splunk si registrano tramite connettori Apache Calcite (fork kenstott/calcite). Nessuno ha un driver diretto — Provisa avvia il server pgwire Calcite integrato del connettore (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-cloudops`, `pgwire-splunk`) e lo raggiunge come endpoint PostgreSQL generico. Sul motore DuckDB quell'endpoint viene collegato live tramite l'estensione postgres: Registra tabella elenca le tabelle del connettore dal catalogo collegato, le query leggono il connettore sul posto, e i filtri e le proiezioni vengono spinti fino a Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Ogni altro motore atterra le righe nello store di materializzazione per la federazione (REQ-954). I bundle vengono scaricati per OS/architettura dalla release fissata di `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Un operatore può invece indicare una cartella bundle costruita per questo host, per ogni connettore, in Admin → Impostazioni → Server dei connettori (`connectors.<connector>.bundle_dir`); vedere [Admin](admin.md). Tutti e cinque i connettori abilitano sempre il matching dei nomi case-insensitive, corrispondente alla semantica case-insensitive propria di ciascun prodotto (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Le liste SharePoint vengono enumerate come schemi ed esposte come tabelle interrogabili (REQ-726, REQ-731). Due metodi di autenticazione: `CLIENT_CREDENTIALS` (default) e basato su certificato tramite un certificato PFX (REQ-727). I valori segreti in `mapping` vengono risolti attraverso il motore dei secret prima di raggiungere il connettore (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Campo origine | Proprietà connettore | Note |
| --- | --- | --- |
| `base_url` o `host` | `site-url` | URL del sito SharePoint |
| `username` | `client-id` | Client ID dell'app Azure |
| `password` | `client-secret` | Client secret dell'app Azure |
| `database` | `tenant-id` | UUID del tenant Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (default) o `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Percorso PFX quando `auth_type: CERTIFICATE` — deve essere ASSOLUTO |
| `mapping.certificate_password` | `certificate-password` | Password PFX — la chiave deve essere presente, stringa vuota per un PFX senza password |

L'autenticazione a certificato sui motori diversi da Trino porta due regole aggiuntive, entrambe applicate quando viene costruito l'operando `model.json` del server pgwire Calcite (REQ-1693). `certificate_path` deve essere assoluto: il server viene eseguito con la directory del proprio bundle come directory di lavoro, quindi un percorso relativo si risolve dentro la cache dei runtime-deps e il PFX non viene trovato. `certificate_password` deve essere presente in `mapping` anche quando il PFX non ha password, nel qual caso è la stringa vuota — l'adapter Calcite rifiuta categoricamente una password null, e una chiave assente viene trattata come errore di configurazione anziché letta silenziosamente come password vuota. Un valore mancante o relativo solleva `MissingConnectorConfig` nominando il campo. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

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

Autenticazione a certificato, con il percorso assoluto e la password sempre presente:

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

Ogni sObject che le credenziali possono interrogare è una tabella che lo steward può registrare (`Account`, `Opportunity`, oggetti personalizzati `*__c`); nulla viene registrato automaticamente (REQ-1946). Colonne e tipi provengono dal describe dell'sObject. Filtri, proiezioni, ordinamenti e limiti vengono delegati a Salesforce come SOQL. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

Una fonte Salesforce indica l'URL di accesso Dominio personale dell'organizzazione e un set completo di credenziali, scelto con `mapping.auth_type`. L'URL di accesso è l'URL `https://…` completo (ad esempio `https://acme.my.salesforce.com`); un host nudo o qualsiasi altro schema viene rifiutato, nominando il campo. Un set incompleto viene rifiutato quando la fonte viene usata, indicando il valore mancante. Ogni segreto può essere un riferimento `${secret:…}` o `${env:…}`. Un segreto digitato come valore letterale viene conservato nel vault dell'organizzazione e la fonte ne mantiene il riferimento.

| Campo origine | Proprietà connettore | Note |
| --- | --- | --- |
| `base_url` o `host` | `login-url` | URL Dominio personale dell'organizzazione (Imposta, poi Dominio personale). `login.salesforce.com` non funziona con il flusso client credentials |
| `username` | `client-id` | Chiave consumatore dell'app connessa |
| `password` | `client-secret` | Segreto consumatore dell'app connessa |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (predefinito), `USERNAME_PASSWORD` o `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | Solo `USERNAME_PASSWORD`, insieme a chiave e segreto consumatore |
| `mapping.security_token` | `security-token` | Solo `USERNAME_PASSWORD`; facoltativo |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | Solo `ACCESS_TOKEN`; nessuna chiave o segreto consumatore |
| `mapping.api_version` | `api-version` | Facoltativo, ad esempio `v61.0` |

Per `CLIENT_CREDENTIALS` l'app connessa richiede *Enable Client Credentials Flow* e un utente *Run As*; ogni lettura e scrittura viene eseguita con le autorizzazioni di quell'utente. Le organizzazioni create da Summer '23 bloccano per impostazione predefinita il flusso nome utente e password.

**Letture.** Su Trino la fonte viene letta tramite il catalogo `salesforce`, che legge soltanto. Su ogni altro motore, il motore collega il server pgwire della fonte e la legge sul posto.

**Scritture.** `INSERT`, `UPDATE` e `DELETE` su un sObject registrato sono mutazioni: governate, sottoposte ad audit e gestite per ambiente come ogni altra mutazione. Su ogni motore, Trino incluso, una scrittura viene eseguita sul server pgwire della fonte; su Trino quel server è in esecuzione solo per le scritture. Ogni istruzione viene inviata a Salesforce e confermata all'esecuzione, quindi non può essere annullata. Il server non restituisce le righe che scrive: un'istruzione con clausola `RETURNING` viene rifiutata e GraphQL non offre campi di mutazione per queste tabelle; `INSERT`, `UPDATE`, `DELETE` e `COPY` in SQL sono i percorsi di scrittura. Una scrittura è una singola istruzione semplice su una tabella: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` vengono rifiutati, nominando la forma. Una fonte SharePoint viene scritta allo stesso modo. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Avvio.** Prima di accettare connessioni, il server della fonte legge le colonne di ogni sObject, circa quattro minuti per un'organizzazione che ne ha 1.200. Il costo si paga una volta: i risultati dei describe sono conservati nella directory di stato propria della fonte, sotto la directory dati dell'istanza. Il server viene avviato quando la fonte viene registrata o caricata. Su Trino il catalogo `salesforce` conserva i propri risultati dei describe, una directory per fonte sotto `/data/trino/cache/salesforce-describe` nel coordinator, quindi il costo si paga una volta anche lì quando quel percorso è un volume che sopravvive al container: il volume `trino_cache` dello stack compose, o il volume della cache del coordinator del chart Helm (`trino.coordinator.cache.persistence`). Finché non accetta connessioni, Registra tabella mostra il connettore come in avvio e una scrittura riceve lo stesso messaggio invece di attendere. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Flusso nome utente e password:

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

Ogni tabella dell'istanza è disponibile per la registrazione; nulla viene registrato automaticamente (REQ-1954). Un'istanza completa può averne migliaia; Register Table le raggruppa quindi per tema — ogni tema è il nome visualizzato di un pacchetto ServiceNow, o "(nessun pacchetto)" quando una tabella non appartiene a nessun pacchetto. Ogni tabella viene mostrata come `name — label`. Selezionare una tabella ne legge le colonne dall'istanza; elencare le tabelle non lo fa. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Una fonte ServiceNow indica l'URL completo `https://…` dell'istanza insieme a nome utente e password. L'URL è preso da `base_url` o `host`; un host nudo o qualsiasi altro schema viene rifiutato, nominando il campo (`instanceUrl`). Un nome utente o una password assenti vengono rifiutati, nominando il campo. Ogni valore può essere un riferimento `${secret:…}` o `${env:…}`.

| Campo fonte | Proprietà dell'adattatore | Note |
| --- | --- | --- |
| `base_url` o `host` | `instanceUrl` | URL completo `https://…` dell'istanza (ad es. `https://acme.service-now.com`). Un host nudo o un altro schema viene rifiutato |
| `username` | `username` | Nome utente per l'autenticazione di base |
| `password` | `password` | Password per l'autenticazione di base |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Letture.** Su Trino la fonte viene letta tramite il catalogo `servicenow`, che legge soltanto. Su ogni altro motore, il motore collega il server pgwire della fonte e la legge sul posto. Solo le colonne selezionate vengono richieste all'istanza (`sysparm_fields`). Un filtro viene inviato all'istanza (`sysparm_query`) solo in una forma che l'adattatore ha verificato su un'istanza reale; ogni altro filtro, e ogni ordinamento e limite, viene applicato alle righe lette. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Un campo di riferimento appare come due colonne: `<field>` contiene il sys_id del record referenziato e `<field>__display` contiene il valore di visualizzazione. Un campo vuoto è NULL. Un campo il cui tipo non può essere mappato dall'adattatore genera un errore che nomina la tabella, il campo e il tipo. [inferred from adapter behavior; non verificato direttamente nel codice sorgente di Provisa in questa sessione]

La ricerca di tabelle (linguaggio naturale) corrisponde a nome, etichetta e tema di ogni tabella. Una volta registrata una tabella, le sue colonne si uniscono alla ricerca. [tool-verified: `provisa/discovery/catalog_cache.py` docstring di `COLUMNS_PER_TABLE_KINDS`]

**Ciclo di vita del server.** Il server serve solo le tabelle registrate da esso. Registrare o rimuovere una tabella riavvia il server e ri-provisiona la fonte sul motore; un'istruzione che arriva durante quel riavvio fallisce. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Scritture.** `INSERT`, `UPDATE` e `DELETE` su una tabella registrata sono mutazioni: governate, sottoposte ad audit e gestite per ambiente come ogni altra mutazione. Su ogni motore, Trino incluso, una scrittura viene eseguita sul server pgwire della fonte. Il server restituisce la riga che l'istanza contiene dopo ogni scrittura; una clausola `RETURNING` è quindi accettata e GraphQL offre campi di mutazione per queste tabelle. Una scrittura è una singola istruzione su una tabella: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` e `TRUNCATE` vengono rifiutati, nominando la forma. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import set.** Quando una tabella ha una transform map attiva verso di essa, Register Table offre i suoi import set, e lo steward può vincolare la tabella a uno (una tabella con più map nomina ciascuno). Le inserzioni in una tabella vincolata passano attraverso l'Import Set API dell'istanza e la trasformazione di quella map; le inserzioni in una tabella non vincolata, e ogni aggiornamento ed eliminazione, vengono scritte direttamente nella tabella. Una riga che la trasformazione rifiuta viene respinta, nominando la riga; le righe già scritte restano scritte. Il vincolo viene verificato quando la tabella viene salvata: deve essere l'import set di una transform map attiva verso la tabella, su una fonte ServiceNow. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`; `provisa/federation/servicenow.py` `servicenow_settings`; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `cloudops`

L'inventario cloud presenta le risorse di Azure, AWS e GCP come un unico insieme di tabelle: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` e `compute_security_groups`, che collega una risorsa di calcolo ai suoi gruppi di sicurezza. Ogni tabella ha le stesse colonne per i tre cloud, e la colonna `cloud_provider` indica il cloud da cui proviene ogni riga. La fonte legge soltanto, e le sue tabelle vengono registrate da uno steward come tutte le altre (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

Una fonte indica uno o più cloud. Ogni cloud è tutto o niente: un cloud con solo una parte dei valori obbligatori viene rifiutato, indicando ciò che manca, così come una fonte che non indica alcun cloud. La fonte legge esattamente i cloud che indica. Tutti i valori si trovano in `mapping`; ogni segreto può essere un riferimento `${secret:…}` o `${env:…}`, Un segreto digitato come valore letterale viene conservato nel vault dell'organizzazione e la fonte ne mantiene il riferimento.

| Cloud | Chiave di `mapping` | Proprietà connettore | Note |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Obbligatorio |
| Azure | `azure_client_id` | `azure.client-id` | Obbligatorio; il client ID della registrazione dell'app |
| Azure | `azure_client_secret` | `azure.client-secret` | Obbligatorio; segreto |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Obbligatorio; separati da virgole |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Obbligatorio |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Obbligatorio; segreto |
| AWS | `aws_region` | `aws.region` | Obbligatorio, ad esempio `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Obbligatorio; separati da virgole |
| AWS | `aws_role_arn` | `aws.role-arn` | Facoltativo; un ruolo da assumere per l'accesso cross-account |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Obbligatorio; percorso ASSOLUTO di un file di chiave JSON di un service account |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Obbligatorio; separati da virgole |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Facoltativo; per quanto tempo un risultato viene riutilizzato prima di interrogare di nuovo i cloud (predefinito dell'adattatore 5) |

Il file della chiave GCP viene aperto da chi legge la fonte. Su Trino è il server Trino, quindi il percorso deve esistere lì. Su ogni altro motore è il server pgwire della fonte sull'host Provisa, la cui directory di lavoro è la propria directory di stato, quindi un percorso relativo viene rifiutato.

Su Trino la fonte viene letta tramite il catalogo `cloudops`. Su ogni altro motore, il motore collega il server pgwire della fonte e la legge sul posto, come una fonte SharePoint.

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

I risultati di ricerca Splunk sono interrogabili come tabelle (es. `internal_server`) (REQ-721). L'URL del connettore proviene da `base_url`, oppure viene costruito come `https://{host}:{port}` con porta di default `8089` (REQ-722). Auth: quando `mapping.use_token` è `true` (default), `password` viene passata come token API; quando `false`, `username` e `password` vengono passate come credenziali separate (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Campo origine | Proprietà connettore | Note |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, altrimenti `https://host:port` (porta default 8089) |
| `password` | `token` o `password` | token quando `use_token: true` |
| `username` | `user` | solo quando `use_token: false` |
| `database` | `app` | restringe a un'app Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtra a un data model |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | per certificati self-signed (REQ-724) |

Sul percorso pgwire-replica (ogni motore tranne Trino) le stesse quattro impostazioni opzionali diventano le chiavi dell'operando Calcite `model.json` `app`, `token`/`username`+`password`, `datamodelFilter` e `disableSslValidation` — le ultime due nei tipi in cui `SplunkSchemaFactory` le converte, una stringa e un booleano (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

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

#### Google Workspace e Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` e `microsoft_365` leggono posta, calendari e attività degli account che un'origine indica, e li atterrano come repliche. Non sono connettori Calcite: ciascuno ha un loader proprio che chiama l'API del provider, Gmail, Google Calendar e Google Tasks da una parte e Microsoft Graph dall'altra. Il modulo li elenca sotto Enterprise come "Google Workspace (Gmail)" e "Microsoft 365 (Outlook, Exchange)". [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Tabelle.** I due tipi producono le stesse tabelle canoniche: stessi nomi, tipi di colonna e ordine. Un'unione dei due non richiede cast né rinomine; una colonna per cui un provider non ha valore è NULL. Le tabelle sono elencate nello schema `default`, e ogni riga inizia con l'account da cui è stata letta. Un'origine offre le tabelle delle risorse che legge: [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Risorsa | Tabelle | Scritte da istruzioni |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | No |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Le colonne sono elencate nello [schema canonico della posta](arch/canonical-mail-schema.md). `events` contiene eventi singoli, eventi master di serie con la ricorrenza come righe RFC 5545, ed eccezioni; non contiene mai le occorrenze espanse di una serie. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Registrazione.** L'amministratore di un'organizzazione inserisce prima il client dell'organizzazione per la piattaforma sotto Admin → Email: un client ID e un secret, più l'ID della directory (tenant) o il dominio per Microsoft 365. Chi aggiunge un'origine non lo vede né lo digita. Un'origine assume poi una di due forme: [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Una casella.** L'origine indica un indirizzo in `accounts`. Il suo titolare fa clic su **Connect** nel modulo e approva il client dell'organizzazione. L'approvazione risultante è conservata nello store di stato dell'organizzazione, non nelle impostazioni dell'origine. In Google Workspace questa forma dichiara anche `sign_in: google_account`.
- **Le caselle dell'organizzazione.** L'origine dichiara `mailboxes` come esattamente uno tra `everyone: true`, `group: <address or id>` o `list: [addresses]`. Non contiene credenziali e nessuno la connette. L'organizzazione legge con un accesso proprio, e solo dopo che l'amministratore ha attivato "Read the organization's mailboxes". Su Google quell'accesso è una chiave di service account con delega a livello di dominio, più un indirizzo amministratore per le chiamate alla directory. Su Microsoft è il grant client-credentials della registrazione dell'app.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

Con la seconda forma, una tabella contiene le righe di ogni casella. Un ruolo che può leggere la tabella legge ogni casella, a meno che una regola di riga sulla colonna `account` lo limiti. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

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

**Impostazioni.** Ogni modulo rifiuta per nome una chiave che non legge. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

| Chiave `mapping` | Tipo | Significato |
| --- | --- | --- |
| `resources` | entrambi | Quali tra `mail`, `calendar`, `tasks` l'origine legge; almeno uno |
| `accounts` | entrambi | L'indirizzo dell'unica casella (forma a una casella) |
| `mailboxes` | entrambi | `everyone`, `group` o `list` (forma organizzazione) |
| `sign_in` | `google_workspace` | `google_account` (forma a una casella) |
| `mail_since` | entrambi | `YYYY-MM-DD`; nulla di ricevuto prima viene conservato |
| `mail_content` | `google_workspace` | `full`, oppure `headers` per soli header ed etichette (scope `gmail.metadata` di Gmail). Obbligatorio quando si legge la posta |
| `mail_search` | `google_workspace` | Una ricerca come si digita nella casella di ricerca di Gmail |
| `mail_labels` | `google_workspace` | Nomi di etichette; si legge solo la posta che le porta tutte |
| `mail_include_spam_trash` | `google_workspace` | `false` per impostazione predefinita |
| `calendar_send_updates` | entrambi | Se una scrittura avvisa i partecipanti di un evento; disattivato per impostazione predefinita |
| `calendar_check_days` | `microsoft_365` | Giorni prima e dopo oggi che il controllo delle modifiche esamina; un numero intero, 1 o più, obbligatorio quando si legge il calendario |

Google rifiuta una ricerca sotto `gmail.metadata`, quindi `mail_content: headers` con `mail_search` o `mail_since` viene rifiutato. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Scope.** Un accesso chiede solo gli scope delle risorse selezionate: [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Risorsa | Google Workspace | Microsoft 365 (delegated) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, oppure `gmail.metadata` per i soli header | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| ogni origine | `userinfo.email` | `offline_access`, `User.Read` |

Per la forma organizzazione, Microsoft richiede i permessi applicativi `Mail.Read` e `User.Read.All`, più `Calendars.ReadWrite` e `Tasks.ReadWrite.All` per calendari e attività. Il service account di Google è autorizzato per gli scope in `DELEGATED_SCOPES`, che includono anche la lettura di utenti e membri di gruppi della directory. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Aggiornamento e freschezza.** I tipi di sonda che queste origini accettano sono `hash` e `none`, e il loro loader fornisce il token di cambiamento: [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail:** un digest dell'history id corrente di ogni casella, una chiamata `users.getProfile` per casella. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Posta Microsoft:** ogni delta link conservato, seguito per un elemento. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Calendari e attività Google:** il tag e l'ora dell'ultima modifica di ogni calendario; il tag di ogni lista di attività, e l'id e l'ora di aggiornamento di ogni attività. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Calendari e attività Microsoft:** le righe di modifica di ogni casella, entro `calendar_check_days` prima e dopo oggi. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Un controllo che non può essere eseguito richiede una lettura; non riporta mai "invariato". Le tabelle di calendari e attività sono lette per intero a ogni aggiornamento. Le repliche della posta di un'origine Microsoft 365 o Google Workspace sono mantenute sul posto: ogni parte di una casella è scritta man mano che arriva e può essere interrogata subito. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Un'origine Google Workspace che restringe con `mail_search` è riletta per intero a ogni aggiornamento, perché solo Google può dire cosa una ricerca seleziona. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Scritture.** `INSERT`, `UPDATE` e `DELETE` su `events`, `event_attendees` e `tasks` vanno al provider attraverso un unico percorso di scrittura per entrambi i tipi. Un `UPDATE` o `DELETE` indica le sue righe per `provider`, `account` e `id`; un partecipante è indicato da `event_id` e `address`. Una singola occorrenza di una serie è indicata da `series_id` e `original_start_at`. Una scrittura che imposta una colonna fuori dall'insieme scrivibile della tabella viene rifiutata per nome. Google scrive con `sendUpdates=all` quando `calendar_send_updates` è attivo e con `sendUpdates=none` quando è disattivo. Microsoft Graph non può fare una simile modifica in silenzio, quindi una modifica ai partecipanti di una riunione viene rifiutata, indicando l'impostazione. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Esclusioni.** La posta è di sola lettura; nessuna istruzione modifica una casella. `calendars`, `task_lists` e ogni tabella di posta non accettano scritture. Spam e Cestino di Gmail sono esclusi a meno che `mail_include_spam_trash` sia attivo. L'API Tasks di Google conserva solo il giorno di scadenza di un'attività, e non ha promemoria. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### Origini API {: #api-sources }

Registra qualsiasi endpoint HTTP come tabella interrogabile. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Tipo API | Discovery | Inferenza colonne |
| --------- | ----------- | ----------------- |
| `openapi` | Parsing della spec OpenAPI (REQ-314, REQ-316) | Primitivi → nativo, oggetti → JSONB |
| `graphql_remote` | Introspezione dello schema (REQ-307, REQ-308) | Primitivi → nativo, oggetti → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | Primitivi → nativo, oggetti → JSONB |

Le risposte API vengono recuperate, messe in cache in PostgreSQL (TTL configurabile), ed esposte come tipi GraphQL (REQ-309, REQ-318, REQ-327). Le tabelle in cache partecipano a query federate come qualsiasi altra origine (REQ-313).

**Regole JSONB**: Le colonne complesse (oggetti, array) memorizzate come JSONB non sono filtrabili (REQ-119). L'accesso ai sotto-campi usa l'estrazione `->>` in SQL (REQ-151). Le relazioni vengono dichiarate tra tabelle usando colonne FK scalari — le colonne blob JSONB non sono target di join. Usa la promozione JSONB per convertire campi annidati in colonne scalari native quando è necessario filtrare o unirsi su di essi (REQ-119).

#### GitHub e GitLab {: #github-and-gitlab }

GitHub e GitLab sono origini con marchio, gestite da `graphql_remote`: ciascuna compare nel modulo Sources con il proprio nome e si registra come origine GraphQL remota che ne registra il marchio. Il modulo richiede un personal access token e nient'altro. L'endpoint (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) e lo schema sono forniti con Provisa, quindi lo schema non viene sottoposto a introspezione alla registrazione. Il token viene verificato quando l'origine viene aggiunta. Aggiungere l'origine non registra tabelle: ogni tabella offerta dallo schema è disponibile sotto il namespace dell'origine (`gh`, `gl`), e lo steward registra quelle desiderate. Un campo che il token non può leggere viene escluso da una tabella quando questa viene registrata. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

Stripe è un'origine con marchio portata da `openapi`, costruita come GitHub e GitLab sono costruiti su `graphql_remote`. Compare nel modulo Sources come Stripe, sotto Enterprise. Si registra come origine OpenAPI la cui riga registra il marchio in `federation_hints` e il cui percorso della specifica è `brand:stripe`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**Cosa raggiunge.** L'API REST di Stripe, tramite la specifica OpenAPI pubblicata da Stripe (`stripe/openapi`, `spec3.json`). La specifica è distribuita con Provisa, fissata a una versione e compressa con gzip, e non viene scaricata alla registrazione. Quella distribuita è la versione API `2026-09-30.endive` con 425 percorsi. Ogni chiamata porta `Stripe-Version` impostato su quella versione, quindi ogni risposta ha la forma che la specifica descrive, qualunque sia la versione predefinita dell'account. I campi che Stripe restituisce ma che la specifica omette sono aggiunti da `stripe.additions.json`. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Come per qualsiasi origine OpenAPI, ogni operazione `GET` è una tabella offerta e ogni altra operazione è un comando offerto. Aggiungere l'origine non ne registra nessuna: lo steward registra quelle volute. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Autenticazione.** Il modulo chiede una sola cosa, un token di accesso (`BRAND_AUTH`: `token`). Usate una chiave API segreta o limitata (restricted) di Stripe. Il token è inviato come bearer token ed è verificato una volta con `GET /v1/balance` prima che l'origine sia aggiunta. Un token rifiutato fa fallire la registrazione con `openapi.credential_rejected`; uno mancante la fa fallire con `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Aggiornamento e freschezza.** Una tabella Stripe è una replica, come ogni tabella OpenAPI. Le sue risposte in cache durano per il `cache_ttl` dell'origine, che una richiesta di registrazione imposta a 300 secondi se non dice altrimenti. Si applicano i tipi di sonda OpenAPI. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 prevede che un preset possa indicare il parametro che un aggiornamento usa per recuperare solo ciò che è cambiato. Il marchio Stripe non ne indica nessuno, quindi ogni aggiornamento legge le sue tabelle per intero. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Esclusioni.** L'integrazione di pagamento deprecata di Stripe basata su token e Sources è lasciata fuori dalla specifica distribuita. L'elenco è sotto [Esclusioni su un'origine costruita da una specifica](#esclusioni-su-unorigine-costruita-da-una-specifica). Le `exclusions` proprie dell'operatore si aggiungono a quelle del marchio. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` compare anche in `SourceType` tra i tipi stub di Supabase `wrappers`. Un'origine Stripe aggiunta tramite il modulo è un'origine `openapi`, non quello stub. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Esclusioni su un'origine costruita da una specifica {: #exclusions-on-a-specification-built-source }

Una specifica OpenAPI pubblicata descrive tutto ciò che l'API remota accetta, compreso ciò che il suo fornitore ha dichiarato obsoleto e ciò che un deployment non deve mai inviare. Un'origine OpenAPI può avere delle esclusioni: elementi della specifica che l'origine non offre. (REQ-1957)

| Escluso | Indicato da | Effetto |
|---|---|---|
| Un'operazione | `method` e `path` | Non elencata per la registrazione come tabella o come comando |
| Un argomento di richiesta | `name`, su una operazione (`method` e `path`) oppure, senza di essi, su ogni operazione che lo ha | Non tra gli argomenti dell'operazione, non mostrato, mai inviato |

Le esclusioni si impostano sotto `exclusions` nel `mapping` dell'origine:

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

Vengono applicate alla specifica quando viene caricata, quindi l'elenco di registrazione, i comandi generati e la richiesta inviata vedono tutti una specifica in cui gli elementi esclusi non esistono. Si può escludere una proprietà del body e un parametro di query, header o cookie; un parametro di percorso (path) no, perché fa parte dell'indirizzo dell'operazione.

Un'esclusione che nomina un'operazione o un argomento che la specifica non ha fa fallire il caricamento della specifica di quell'origine e nomina ogni voce obsoleta, così un elenco che non corrisponde più a una specifica più recente non passa inosservato.

Un'origine con marchio fornisce proprie esclusioni come parte della sua curatela, e quelle dell'operatore vi si aggiungono. L'origine Stripe esclude l'integrazione di pagamento obsoleta di Stripe basata su token e source: la creazione di un addebito (charge), le API Tokens e Sources, l'aggiunta di una carta o di un conto bancario a un cliente a partire da un token, e gli argomenti `source`, `card`, `bank_account` e `default_card` delle scritture di clienti e di pagamenti di fatture. I pagamenti si effettuano tramite Payment Intents, Payment Methods e Setup Intents, che l'origine offre. La lettura e l'eliminazione dei metodi di pagamento che un account ha già restano disponibili.

### GovData

Dati aperti del governo statunitense. L'accesso è partizionato per raggruppamento subject. [tool-verified: `provisa/core/models.py` lines 543–609]

A un'origine vengono assegnati uno o più subject, e ogni subject porta gli schemi dell'adattatore elencati sotto. Uno schema può appartenere a più di un subject (`census`). Gli schemi `ref` e `geo` sono schemi linker: dati di riferimento e geografia attraverso cui si uniscono le tabelle di ogni subject, quindi ogni origine li serve qualunque subject le sia stato assegnato. L'elenco è dichiarato una sola volta sul server ed è vincolato, da un test, agli schemi serviti dal bundle dell'adattatore fissato. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

| Subject | Schemi esposti |
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
| ogni origine | `ref`, `geo` |

`ALL` è la scorciatoia di una sottoscrizione per tutti i subject; non è una scelta nel modulo Sources.

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

Un'origine AskAmerica creata nell'admin richiede una sola credenziale, la chiave API. Provisa presenta la chiave all'API di AskAmerica e riceve le credenziali di storage a breve durata e in sola lettura con cui i dati vengono letti; la chiave viene rifiutata lì, per nome, se è sconosciuta o revocata. [tool-verified: `provisa/federation/askamerica.py`]

L'origine è poi un'origine Postgres-wire come le origini SharePoint, Salesforce e inventario cloud: Provisa avvia per essa il server `pgwire-govdata` integrato, con la chiave e quelle credenziali nell'ambiente del server, e ogni motore raggiunge quel server come raggiunge quello di un'origine sorella. DuckDB, Postgres e ClickHouse lo collegano e leggono le sue tabelle sul posto, quindi una tabella AskAmerica si unisce alle tabelle di altre origini, sta sotto le view ed è consegnata come qualsiasi altra. Trino non ha un connettore per essa e legge una replica atterrata tramite il server: una copia della tabella conservata nello store di materializzazione, aggiornata quanto il suo ultimo refresh secondo le impostazioni di cache e replica della tabella, anziché letta al momento della query. Una tabella viene letta dallo schema dell'adattatore che nomina (`sec`, `econ` e così via). [tool-verified: `provisa/federation/pgwire_replica.py`, `provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

Il server monta i suoi schemi dall'object storage prima di accettare una connessione, il che richiede minuti al primo avvio. Viene avviato quando l'origine viene salvata; finché non è pronto, il modulo Registra tabella segnala che l'origine è ancora in avvio. [tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Dove risiede la password di un'origine {: #where-a-sources-password-lives }

La password di un'origine non viene mai memorizzata insieme al resto delle sue impostazioni di
connessione. La riga `sources` del control plane porta una colonna `password_ref` che contiene ciò
che è stato inserito come password: il valore stesso, oppure un riferimento come
`${env:PG_PASSWORD}` o `${secret:SNOWFLAKE_KEY}`. Viene risolto nel momento in cui l'origine viene
contattata, all'interno dell'organizzazione in cui la richiesta è in esecuzione.
[tool-verified: `provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

Un valore senza `${…}` al suo interno viene usato così com'è. `${env:VAR}` legge l'ambiente di
processo del deployment e non richiede alcun binding. `${secret:NAME}` nomina un secret di
proprietà di un'organizzazione, quindi si risolve solo all'interno delle operazioni proprie di
quell'organizzazione: le seam di introspezione admin e il terminale di query che ogni superficie
raggiunge stabiliscono quel binding. [tool-verified: `provisa/core/secrets.py` `resolve_secrets`;
`provisa/pgwire/_pipeline.py` `_execute_plan`]

Il campo contiene ciò che vi è stato inserito, comunque l'origine sia stata registrata:

- **Da config.** Il file è il record: Provisa copia il valore o il riferimento in `password_ref`
  alla lettera.
- **Dal modulo Sources.** Ciò che viene digitato nel campo password viene memorizzato alla lettera,
  valore o riferimento che sia. Provisa non scrive nulla nel vault dell'organizzazione per esso e non
  inventa alcun nome di secret. Per tenere una credenziale fuori dalla riga del control plane, crea
  tu stesso il secret (vedi [Secrets](secrets.md)) e digita `${secret:NAME}` nel campo.
  [tool-verified: `provisa/api/admin/schema_mutation.py` `create_source`]

Un valore digitato come valore viene memorizzato nel database del control plane così come digitato.
Riscrivere la password sostituisce il testo del campo. Eliminare l'origine rimuove la sua riga e
nulla dal vault: un secret che hai creato è tuo. [tool-verified:
`provisa/api/admin/schema_mutation.py` `delete_source`; `tests/unit/test_no_product_written_secrets.py`]

`password_ref` non viaggia tra ambienti (REQ-1491). Un branch o un ambiente copiato fornisce i
propri valori di connessione, e il vault che un riferimento nomina appartiene a qualunque ambiente
lo abbia fornito. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Data profiler {: #data-profiler }

Un data profiler è un'origine che contiene un nome, una pianificazione e i valori predefiniti di esecuzione, e nient'altro. Non viene mai sottoposta a introspezione. Vi si aggiungono tabelle di altre origini, e ogni esecuzione le misura e scrive le sue righe nel control plane dell'organizzazione, dove vengono lette come le tabelle di qualsiasi altra origine. Vedi [Test data](test-data.md) per creare un profiler e aggiungervi tabelle. [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)

### Controlli di qualità dei dati (REQ-1443) {: #data-quality-checkers-req-1443 }

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

## Connettori personalizzati (REQ-1177) {: #custom-connectors-req-1177 }

I motori di federazione nativi — Postgres, DuckDB e ClickHouse — guadagnano raggiungibilità verso un nuovo tipo di origine quando un operatore dichiara un connettore per esso in `config/custom_connectors.yaml`. Non è richiesto codice. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

L'estensibilità dei connettori di per sé precede questo. Il motore Trino è da tempo estensibile al proprio livello — un connettore JDBC generico parametrizzato per tipo di origine, un corpo `.properties` di catalogo per tipo, e i plugin connettore Trino personalizzati propri di Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 porta la stessa estensibilità config-driven ai due motori nativi senza cluster, che in precedenza portavano un insieme fisso di connettori.

La configurazione viene fornita vuota. I connettori integrati coprono la raggiungibilità out-of-the-box; tutto in questo file è scritto dall'operatore. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Imposta `PROVISA_CUSTOM_CONNECTORS` per puntare a un percorso diverso (utile per i test).

### Tipi di descrittore {: #descriptor-kinds }

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

### Gating della sonda (probe) {: #probe-gating }

La disponibilità viene verificata al momento dell'attach contro il catalogo di discovery standard di ciascun motore:

- **Postgres** — controlla `pg_extension`, poi `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — esegue `INSTALL`/`LOAD` e controlla `duckdb_functions()` per il `probe_symbol` dichiarato. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — controlla `system.table_engines` per il `ch_engine` dichiarato; l'assenza dal build fallisce in modo esplicito. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Un'estensione dichiarata che non è installabile fallisce in modo esplicito. Nessun salto silenzioso, nessun fallback. Un connettore la cui sonda fallisce semplicemente non è attivo per quel deployment.

### Variabili di template {: #template-variables }

Ogni valore `server_options`, valore `user_mapping`, `attach_template` e `scan_template` può usare placeholder `{field}`. Campi disponibili: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, più qualsiasi chiave da `federation_hints`. I template attach di DuckDB ricevono anche `{alias}` — l'alias di catalogo interno che Provisa assegna al database attached.

Un template che referenzia un campo sconosciuto fallisce in modo esplicito al momento dell'attach, facendo emergere un mismatch descrittore/origine prima che DDL rotto raggiunga il motore.

### Esempi {: #examples }

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

## Warehouse come origini nominate {: #warehouses-as-named-sources }

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

## Campi di configurazione dell'origine {: #source-configuration-fields }

Tutte le origini condividono un insieme comune di campi. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Campo | Richiesto | Default | Descrizione |
| ------- | ---------- | --------- | ------------- |
| `id` | Sì | — | Identificatore univoco; alfanumerico con trattini/underscore |
| `type` | Sì | — | Tipo di origine (vedi tabelle sopra) |
| `host` | No | `""` | Hostname o IP |
| `port` | No | `0` | Numero di porta |
| `database` | No | `""` | Nome del database |
| `username` | No | `""` | Nome utente |
| `password` | No | `""` | Password; usa `${env:VAR}` o `${secret:NAME}` invece di un letterale (vedi sotto) |
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
| `mapping` | No | `{}` | Impostazioni di connettore specifiche per tipo per origini NoSQL e SaaS (es. `auth_type` di SharePoint, `auth_type` di Salesforce, `use_token` di Splunk) (REQ-251) |
| `allowed_domains` | No | `[]` | Restringe l'origine a domini specifici; vuoto = senza restrizioni |
| `description` | No | `""` | Descrizione leggibile |

---

## Origini Kafka {: #kafka-sources }

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

## Visibilità delle colonne {: #column-visibility }

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

## Relazioni {: #relationships }

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

### Relazioni basate su una tabella di giunzione {: #junction-backed-relationships }

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

## Regole di sicurezza a livello di riga {: #row-level-security-rules }

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

## Funzioni e webhook {: #functions-and-webhooks }

### Funzioni DB {: #db-functions }

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

### Webhook {: #webhooks }

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

## Autenticazione {: #authentication }

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

## Routing di esecuzione {: #execution-routing }

**Esecuzione diretta** — Le query a singola origine RDBMS vengono instradate al driver nativo per latenza sub-100ms (REQ-027). Le origini richiedono sia una voce `SOURCE_TO_DIALECT` che una voce `SOURCE_TO_CONNECTOR` per supportare questo percorso (REQ-229).

**Esecuzione federata** — Le query multi-source e le origini senza driver diretto vengono instradate attraverso il motore di federazione (REQ-028). Provisa include un motore di federazione integrato; punta al tuo cluster compatibile per deployment su larga scala (REQ-226).

**Statistiche** — Alla registrazione, Provisa esegue `ANALYZE` su ogni tabella pubblicata per innescare l'ottimizzatore cost-based (conteggio righe, frazione null, valori distinti, min/max). I fallimenti vengono loggati e non bloccano la registrazione (REQ-275).

---

## Origini grafo e semantiche {: #graph-semantic-sources }

### Neo4j

Registra un database a grafo Neo4j come origine interrogabile. Gli steward scrivono query Cypher che proiettano valori scalari; Provisa mette in cache i risultati e li espone come tipi GraphQL (REQ-295).

Le query Cypher devono usare accessori di proprietà nella clausola `RETURN` (`RETURN n.id AS id, n.name AS name`) — restituire oggetti nodo viene rifiutato al momento della registrazione (REQ-296).

#### Registrazione da file di configurazione (REQ-1668) {: #config-file-registration-req-1668 }

Dichiara un'origine `neo4j` e le sue tabelle in YAML. Ogni tabella richiede un `query_template` (il Cypher che produce le sue righe) e colonne tipizzate. La chiave `query_template` non è valida sotto nessun altro tipo di origine. [tool-verified: `provisa/core/config_loader.py:456-511`]

Un'origine richiede `host`, `port` e `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Le righe vengono recuperate con un POST di `{"statements": [{"statement": <cypher>}]}` a `/db/<database>/tx/commit` (l'API delle transazioni HTTP di Neo4j). Una risposta con un elenco `errors` non vuoto viene trattata come query fallita, non come risultato vuoto. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Ogni colonna richiede `data_type`. Il loader mappa i tipi di config al tipo di colonna API usato al momento della query [tool-verified: `provisa/neo4j/persist.py:31-64`]:

| `data_type` di config | Tipo API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` e `decimal(10,2)` sono accettati — viene usato il tipo base prima della parentesi.

La registrazione persiste una riga `api_sources` e una riga `api_endpoints` per tabella, cosicché le tabelle sopravvivono a un riavvio senza rileggere il file. Gli endpoint REST admin sotto `/admin/sources/neo4j` scrivono le stesse righe. [tool-verified: `provisa/neo4j/persist.py:77-120`]

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

#### Registra tabella nella UI (REQ-1670) {: #register-table-in-the-ui-req-1670 }

Un'origine neo4j non ha tabelle da elencare, quindi il modulo Registra tabella chiede la tabella invece di offrirne una. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Scegli l'origine neo4j e un dominio. I selettori di schema e tabella, la checkbox di discovery e il selettore del watermark non appaiono; l'origine non viene mai introspezionata.
2. Digita un nome tabella e il Cypher. Il Cypher deve proiettare scalari (`RETURN a.name AS name`); una proiezione che restituisce un nodo o una lista viene segnalata come errore.
3. Premi Anteprima. Il modulo esegue il Cypher con `LIMIT 5` tramite la query GraphQL `neo4jPreview` e compila l'elenco delle colonne dalle righe restituite, tipizzate come `text`, `integer`, `double`, `boolean` o `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Un'anteprima fallita mantiene il Cypher nell'editor e mostra il messaggio.
4. Regola visibilità, alias o mascheramento come per qualsiasi tabella, poi registra. Il modulo rifiuta l'invio finché un'anteprima non ha tipizzato le colonne, e il server rifiuta una tabella neo4j priva di Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

Il Cypher viene memorizzato con la tabella come `queryTemplate`, appare nella vista di lettura della tabella, e persiste esattamente come fa una registrazione da file di configurazione: una riga `api_sources` e una riga `api_endpoints` che il riavvio successivo idrata. Modificare la tabella ripersiste un Cypher modificato.

#### Registrazione REST admin {: #admin-rest-registration }

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

#### Registrazione da file di configurazione e da UI (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

L'`host` di un'origine `sparql` è l'URL del suo endpoint SPARQL (il modulo Sources lo memorizza allo stesso modo). Ogni tabella sotto di essa porta `query_template`, un SELECT le cui variabili sono le colonne; ogni binding è `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

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

Registra tabella funziona allo stesso modo di Neo4j: scegli l'origine, digita un nome tabella e il SELECT, premi Anteprima (la query `sparqlPreview` lo esegue con `LIMIT 5` e compila l'elenco delle colonne), poi registra. La registrazione persiste una riga `api_sources` e una riga `api_endpoints` (POST form-encoded al percorso dell'endpoint, normalizzatore `sparql_bindings`), le stesse righe che scrive una registrazione da file di configurazione, e il motore nativo atterra le righe tramite la stessa catena di fetch di Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

---

## Esempi di connessione {: #connection-examples }

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

### Query cross-source {: #cross-source-query }

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
