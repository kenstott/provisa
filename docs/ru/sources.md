# Типы источников {: #source-types }

## Модель выполнения {: #execution-model }

В конечном счёте каждый запрос выполняется через движок федерации, который обеспечивает федерацию по всем источникам. Источники делятся на три категории в зависимости от их связности. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Категория | Есть прямой драйвер | Есть федеративный коннектор | Примеры |
| --- | --- | --- | --- |
| **Прямая поддержка (direct-capable)** | Да | Да | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Только федерация** | Нет | Да | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (на базе S3) |
| **Прямое чтение (реплика)** | Да | Да | Snowflake, Databricks, ClickHouse — драйвер читает данные и создаёт реплику; запросы выполняются против реплики в активном движке |
| **Прямой (без межисточникового коннектора)** | Да | Нет | Athena — инструкции для одного источника выполняются целиком в Athena; межисточниковые соединения выгружают полную реплику |
| **Материализация → Федерация** | Нет | Нет | REST/OpenAPI, удалённый GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (приёмник push-данных), GovData, SharePoint, Salesforce, ServiceNow, облачный инвентарь, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Источники с **прямой поддержкой** выполняют однопоточные (single-source) запросы через свой нативный драйвер (менее 100 мс), минуя движок федерации (REQ-027, REQ-229). Они сохраняют полную поддержку коннектора и участвуют в федерации при объединении с другими источниками (REQ-028).

Источники **только с федерацией** всегда опрашиваются через слой федерации. Прямого драйвера не существует (REQ-229).

Источники **с прямым чтением (реплика)** имеют DirectDriver, который читает данные из хранилища нативно (в формате Arrow, где это возможно), создаёт реплику в хранилище материализации активного движка, а затем запросы выполняются против этой реплики. См. [Хранилища как именованные источники](#warehouses-as-named-sources).

Источники **прямого типа (без межисточникового коннектора)** имеют DirectDriver, который выполняет инструкции для одного источника целиком в самом источнике — передаются все фильтры, столбцы и лимиты. Когда инструкция объединяет источник с любым другим, драйвер читает данные и выгружает реплику; соединение выполняется против этой реплики в активном движке. Коннектора Trino для таких источников нет.

Источники **материализации** не имеют федеративного коннектора. Provisa получает их данные (при запуске или во время выполнения запроса) и кеширует их как Parquet в S3 или в PostgreSQL, делая их достижимыми для движка федерации в межисточниковых запросах (REQ-309).

---

## Все источники {: #all-sources }

Provisa регистрирует **72** типов источников — те, что предлагает форма Sources. Приведённые ниже таблицы охватывают все 72; индекс — это счётчик. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, где `graphql` и `grpc` — это `graphql_remote` и `grpc_remote`; Kaggle, Wikipedia, GitHub, GitLab и Stripe учитываются как отдельные источники, хотя внутренне регистрируются через коннекторы `files`, `graphql_remote` и `openapi`]

| # | Группа | Типы источников |
| --- | --- | --- |
| 1–15 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Облачные хранилища данных](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Аналитика / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Data lake / открытые табличные форматы](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Источники на основе кода](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Потоковая передача](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Приёмник push-данных](#push-receiver) | `ingest` |
| 45–46 | [Граф и семантика](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [На основе файлов](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Наблюдаемость и прочее](#observability-other) | `google_sheets`, `prometheus` |
| 54–60 | [Корпоративный SaaS](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 61–66 | [API-источники](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 67 | [GovData](#govdata) | `govdata` |
| 68–69 | [Проверки качества данных](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 70 | [Профайлер данных](#data-profiler) | `data_profiler` |
| 71 | [Наборы данных Kaggle](#kaggle-datasets) | Kaggle (загружается через форму Sources; регистрируется как источник `files` — см. [Наборы данных Kaggle](#kaggle-datasets)) |
| 72 | [Wikipedia](#wikipedia) | Wikipedia (регистрируется как источник `files`, который обходит указанные страницы — см. [Wikipedia](#wikipedia)) |

Справочник по каждому типу источника, поддерживаемому Provisa. «Прямой драйвер» означает, что однопоточные запросы выполняются напрямую к источнику (менее 100 мс) (REQ-027). «Имя коннектора» — это федеративный коннектор, используемый, когда источник участвует в многопоточных (multi-source) JOIN (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### RDBMS

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Да |
| `mysql` | aiomysql | mysql | mysql | Да |
| `mariadb` | aiomysql | mariadb | mysql | Да |
| `singlestore` | — | singlestore | singlestore | Федеративные |
| `sqlserver` | aioodbc | sqlserver | tsql | Да |
| `oracle` | oracledb | oracle | oracle | Да |
| `duckdb` | duckdb | memory | duckdb | Да |
| `cockroachdb` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `yugabytedb` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `greenplum` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `tidb` | aiomysql (протокол mysql) | mysql | mysql | Да |
| `firebird` | — | — (расширение DuckDB) | — | Нет |
| `airport` | — | — (расширение DuckDB) | — | Нет |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | Нет |
| `hiveserver2` | impyla (HiveServer2 поверх Thrift) | — | hive | Нет |

Базы данных, совместимые по протоколу, повторно используют JDBC-драйвер, нативный асинхронный драйвер и диалект базового протокола — CockroachDB, YugabyteDB и Greenplum используют протокол PostgreSQL; TiDB использует протокол MySQL. Им требуются только записи в реестре, без нового кода коннектора. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) и `airport` (сервер Arrow Flight) — зарегистрированные типы источников, достигаемые на месте через community-расширения DuckDB, когда DuckDB является активным движком — без прямого драйвера, без федеративного коннектора. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA читается собственным драйвером и реплицируется в хранилище активного движка, поэтому доступна на любом движке. Форма принимает хост, порт (по умолчанию 39015), имя пользователя, пароль и базу данных; в многотенантной системе база данных называет тенанта. Register Table перечисляет схемы, которые пользователь может читать, без системных схем HANA. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Конечная точка HiveServer2, читаемая напрямую поверх Thrift, — для развёртывания Hive, где эта точка единственная доступная. Она отличается от `hive` и `hive_s3`, которые читают файлы и metastore хранилища и никогда не обращаются к HiveServer2. Строки читаются через конечную точку и реплицируются в хранилище активного движка. Форма принимает хост, порт (по умолчанию 10000), имя пользователя, пароль и базу данных; `federation_hints.auth_mechanism` выбирает аутентификацию (по умолчанию `PLAIN`, например `GSSAPI` для Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Облачные хранилища данных {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Федеративные | Чтение через snowflake-connector-python; создаёт реплику; `account`/`warehouse`/`role` в `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Федеративные | Нет DirectDriver; достигается через движок федерации или через ATTACH движка BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Федеративные | Чтение через databricks-sql-connector (Cloud Fetch, Arrow); создаёт реплику; `http_path` обязателен в `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Федеративные | — |
| `athena` | AthenaDriver | — | athena | Частичные (INSERT всегда; UPDATE/DELETE/MERGE только для Iceberg) | Безсерверные запросы к S3; инструкции для одного источника выполняются целиком в Athena; перекрёстные объединения выгружают реплику; `region` обязателен в `federation_hints` (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Федеративные | Microsoft Fabric Warehouse; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Федеративные | Azure Synapse SQL; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Федеративные | Удалённый координатор Trino/Presto, чтение через диалект trino SQLAlchemy; создаёт реплику на любом движке (REQ-994) |

#### `athena`

Amazon Athena — безсерверный сервис запросов к данным в S3, тарифицируемый за каждый отсканированный байт. Инструкция, работающая только с таблицами Athena, направляется напрямую в Athena: фильтры, списки столбцов и `LIMIT` передаются полностью, поэтому сканируются и тарифицируются только те байты, которые нужны Athena для выполнения запроса. Инструкция, объединяющая таблицу Athena с другим источником, сначала выгружает полную реплику из Athena — вся таблица читается и тарифицируется, — а затем объединение выполняется по этой реплике. Настройки репликации и защиты от нагрузки источника определяют частоту выгрузок; примечание в документации — единственное правило. (REQ-1981)

**Обнаружение.** Register Table обращается к API метаданных Athena — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Эти вызовы не выполняют запросов, поэтому перечисление баз данных и таблиц и чтение столбцов таблицы не требует затрат на байты. Столбцы таблицы читаются только при её выборе. Ключи партиций следуют за обычными столбцами. Типы сопоставляются через запись `athena` в `provisa/core/ir_types.py`: `array<…>`, `map<…>` и `struct<…>` преобразуются в `text`; неизвестный тип отклоняется с указанием его имени. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns`; `provisa/core/ir_types.py` запись `"athena"`]

**Поля источника.**

| Поле | Где | Обязательно | По умолчанию | Примечания |
| --- | --- | --- | --- | --- |
| `database` | источник | Да | — | База данных Glue, используемая по умолчанию |
| `username` | источник | Нет | — | ID ключа доступа AWS; может быть `${secret:…}` или `${env:…}` |
| `password` | источник | Нет | — | Секретный ключ доступа AWS; может быть `${secret:…}` или `${env:…}` |
| `region` | `federation_hints` | Да | — | Регион AWS, например `us-east-1` |
| `workgroup` | `federation_hints` | Нет | `primary` | Рабочая группа Athena |
| `s3_staging_dir` | `federation_hints` | Нет | по умолчанию группы | Куда Athena записывает результаты; обязателен, если рабочая группа не задаёт расположение |
| `catalog` | `federation_hints` | Нет | `AwsDataCatalog` | Каталог данных Athena |
| `role_arn` | `federation_hints` | Нет | — | ARN роли IAM, принимаемой для каждого вызова |

Если и `username`, и `password` пусты, используется цепочка учётных данных boto3: роль экземпляра, переменные среды (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) или именованный профиль. Одно без другого отклоняется. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect`; `provisa/federation/athena.py` `athena_client`]

**Запись.** Каждая зарегистрированная таблица принимает `INSERT`. `UPDATE`, `DELETE` и `MERGE` доступны только для таблицы Iceberg; формат записывается при регистрации (`registered_tables.table_format`). Запись в таблицу Hive с запросом `UPDATE`, `DELETE` или `MERGE` отклоняется с указанием таблицы и операции. Athena не возвращает записанные строки — предложение `RETURNING` отклоняется, а GraphQL не предлагает поля мутаций для этих таблиц; `INSERT`, `UPDATE`, `DELETE` и `COPY` — пути записи. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` и `TRUNCATE` отклоняются с указанием формы. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops`; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

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

С ролью, принимаемой для каждого вызова (цепочка учётных данных для базовой сессии):

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

`motherduck` (REQ-1998) — база данных DuckDB, размещённая в MotherDuck. На движке DuckDB она подключается на месте; на движке Postgres она остаётся коннектором через foreign data wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Подключение.** `database` задаёт базу данных MotherDuck или общий ресурс по его адресу. `password` — токен MotherDuck, который разрешается как любой секрет источника. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

Движок DuckDB выполняет `ATTACH 'md:<database>'`; для общего ресурса `database` — это адрес ресурса (`_share/...`), поэтому подключение выглядит как `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Обработка токена.** Движок входит в систему через `SET GLOBAL motherduck_token = ?`, передавая токен как связанное значение. Его нет в тексте ни одной инструкции, поэтому он не попадает ни в лог, ни в трассировку, ни в ошибку. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Нет токена.** Источник без `password` отклоняется с указанием причины до того, как что-либо подключается (`a MotherDuck source signs in with its token (the password field); none is set`). Без этой проверки расширение начало бы интерактивный вход и ждало бы около минуты. Источник без `database` также отклоняется. [tool-verified: `details`]
- **Одна учётная запись на движок.** Один движок входит в одну учётную запись MotherDuck. Второй источник MotherDuck с другим токеном отклоняется с указанием причины: "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Запись** идёт через движок, как и на движке Postgres. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**Гибридное (двойное) выполнение** не требует настройки. На движке DuckDB все источники подключены к одному соединению, поэтому инструкция, соединяющая таблицу файлового источника DuckDB с таблицей MotherDuck, выполняется как одна инструкция DuckDB, а MotherDuck решает, где выполняется каждая часть. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Перемещение данных.** При двойном выполнении MotherDuck может переносить локальные строки на свою сторону для тех частей, которые он выполняет. Поэтому такая инструкция может отправить данные из локального файла в учётную запись MotherDuck организации. Не соединяйте файл с данными, которые должны оставаться на узле, с источником MotherDuck, если отправка их в эту учётную запись недопустима. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Источник MotherDuck обслуживают только движки DuckDB и Postgres. Ни у одного другого движка нет для него коннектора, и ничто не выгружает туда его таблицы как реплики. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Аналитика / OLAP {: #analytics-olap }

[tool-verified: `executor/drivers/clickhouse.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Федеративные | Чтение через clickhouse-connect (HTTP); `secure: "true"` в `federation_hints` для TLS (REQ-986) |
| `druid` | — | druid | druid | Нет | — |
| `exasol` | — | exasol | exasol | Нет | — |
| `elasticsearch` | HTTP (нативные движки) | elasticsearch (Trino) | — | Нет | В Trino коннектор читает его, а свойства берёт из DSL сопоставления (mapping DSL) этого типа [tool-verified: `trino_connectors.py:309`]; на любом другом движке Provisa читает индекс по HTTP (индексы и сопоставление для регистрации таблицы, scroll-чтение для создания реплики) и загружает строки [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | Нет | Коннектор Trino `pinot`; `pinot.controller-urls` = хост:порт контроллера Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / открытые табличные форматы {: #data-lake-open-table-formats }

Эти типы источников работают только через федерацию — без прямого драйвера, без диалекта. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Тип источника | Имя коннектора | Путешествие во времени (Time Travel) | Примечания |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Да (аргумент `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Да (аргумент `as_of`, REQ-372) | — |
| `hive` | hive | Нет | — |
| `hudi` | — (движок ClickHouse `Hudi`, без копирования — REQ-1178) | Нет | Нет федеративного коннектора; достигается на месте, когда активным движком является ClickHouse |
| `hive_s3` | hive | Нет | Hive на базе S3 |

### NoSQL

`mongodb`, `cassandra` и `redis` имеют коннекторы Trino (`redis` строит свои свойства из DSL сопоставления этого типа). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Нет |
| `cassandra` | cassandra (Trino); чтение CQL через cassandra-driver на любом другом движке | Нет | Keyspace являются схемами; регистрация таблицы перечисляет таблицы keyspace и типизирует столбцы из метаданных схемы кластера (ключи партиции — как первичные ключи); экстра `cassandra` устанавливает драйвер [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino); чтение через redis-py без HTTP на любом другом движке | Нет | Префикс ключа `<table>:*` — это таблица, а хеш — это строка; регистрация таблицы перечисляет присутствующие префиксы и типизирует столбцы префикса по его хешам (запись `mapping.tables` переопределяет шаблон, столбец ключа, тип значения и столбцы) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

### Qdrant

`qdrant` (REQ-1983, REQ-2000) читается и записывается собственным клиентом Qdrant, `qdrant-client`. Qdrant — **только live**: это свойство типа источника, а не настройка. Отвечает он своим векторным индексом и Query API, и ни одна реплика не может ответить так же, поэтому его таблицы никогда не реплицируются. Для источника Qdrant и его таблиц нет выбора Replicate; значение `replicate` или `row_materialize` для них отклоняется. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES`; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Подключение.** `host` — URL экземпляра с портом (`https://<cluster>.cloud.qdrant.io:6333` или собственный `http://qdrant:6333`); `password` — его ключ API, ссылка на секрет, разрешаемая при чтении источника; для экземпляра без ключа он пуст. `federation_hints.prefer_grpc: true` читает через порт gRPC Qdrant вместо REST. У источника одна схема, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source`; `provisa/qdrant/introspect.py` `SCHEMA`]

**Таблицы коллекций.** Коллекция — это таблица. Register Table предлагает столбцы коллекции: `id` (идентификатор точки, её ключ, путь `@id`), поля payload (индексированное поле типизируется по индексу, любое другое — как подсказывают выбранные точки-образцы; вложенные поля разворачиваются вместе с путём) и по одному столбцу `json` на вектор (путь `@vector` или `@vector.<name>` для именованного вектора), предлагаемому невыбранным. Столбец вектора называет зарегистрированную модель эмбеддингов, эмбеддинги которой он хранит; модель выбирается из тех, чья размерность равна размеру вектора (в Register Table и на вкладке «Тип и ключи» редактора); выбор модели регистрирует вектор. [tool-verified: `provisa/qdrant/introspect.py` `columns`; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Инструкции к одному источнику Qdrant.** Инструкция, читающая только одну коллекцию, выполняется в Qdrant, если API Qdrant выражает её точно:

- условия по payload превращаются в фильтр Qdrant (`=`, `<>`, `IN`, диапазоны по числам и датам, `IS NULL`, `AND`, `OR`, `NOT`); семантика NULL в SQL сохраняется, поэтому `lang <> 'en'` пропускает точки без `lang`;
- условие только по `id` (`=` или `IN`) — это один `retrieve`; любое другое чтение — отфильтрованный `scroll`;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` — это один `query_points` по вектору с расстоянием Cosine, фильтр применяется до выбора top-k. Текст запроса преобразуется в эмбеддинг моделью вектора. Ответ сопровождается предупреждением, что top-k приблизительный и выбран через индекс Qdrant.

Всё остальное (агрегат, `GROUP BY`, вычисляемый или маскированный столбец, порядок, отличный от «сначала наиболее похожие») обрабатывает движок, как описано ниже. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Инструкции, которые обрабатывает движок.** Когда инструкция соединяет коллекцию с другим источником или вычисляет по её столбцам, часть, относящаяся к коллекции, читается из Qdrant вживую для этой инструкции и передаётся движку как строки. Эта часть — столбцы коллекции, условия инструкции только по коллекции (в том числе её правила строк) и, если коллекция читается одна в своём `SELECT`, упорядочивание по сходству и `LIMIT` этого `SELECT`. Часть, которую Qdrant не может выразить точно, отклоняется с указанием таблицы и причины. Часть без условия, без ключа и без top-k прочитала бы всю коллекцию и тоже отклоняется. Условие по параметру (`$1`) остаётся движку. [tool-verified: `provisa/federation/live_only.py` `cut`, `read`; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Запись.** `INSERT`, `UPDATE` и `DELETE` для коллекции выполняются в Qdrant:

- `INSERT … VALUES` указывает столбец `id` и записывает новые точки. `id`, который в коллекции уже есть, отклоняется с указанием его значения. Проверка сначала читает идентификаторы, поэтому не атомарна с записью.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` — это upsert. Столбцы, которые инструкция не задаёт, сохраняют значения.
- `UPDATE` и `DELETE` действуют на точки, выбранные их `WHERE`, в виде фильтра Qdrant; условие, не имеющее формы фильтра, отклоняется.
- Текст, записанный в столбец вектора, преобразуется в эмбеддинг моделью столбца. Записанный в него вектор должен иметь размерность модели.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Таблицы поиска.** Векторный поиск — это таблица поиска: её `query_template` содержит запрос Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` — всегда аргумент; `using`, `filter`, `limit`, `score_threshold` и `model` необязательны. Любое значение в `using`, `filter`, `limit` или `score_threshold`, равное ровно `"@name"`, тоже является аргументом.
- Каждый аргумент — параметр таблицы (`query_arg`, REQ-1986), строго приводимый к объявленному типу и помещаемый в запрос как значение JSON, но не в его текст. Аргумент `query` имеет тип `array(double)` и обязателен.
- Вызывающий передаёт аргументы как любой параметр: `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` в SQL, аргументы полей в GraphQL.
- Необязательный аргумент, который не передан, ничего не фильтрует: его условие исключается из фильтра. Без `limit` поиск возвращает значение Qdrant по умолчанию — 10 точек.
- Qdrant применяет фильтр до выбора ближайших точек, поэтому он точный. Любое другое условие инструкции сужает уже возвращённые поиском точки; новых оно не добавляет.
- `score` — собственная метрика коллекции для искомого вектора: для коллекции Cosine — косинусное сходство. Register Table перечисляет векторы коллекции с их размерностью и расстоянием.
- `model` называет зарегистрированную модель эмбеддингов (`vector_models`, admin → AI Models). Текст, переданный как запрос, тогда преобразуется этой моделью, а переданный вектор должен иметь размерность этой модели (REQ-429, REQ-430). Без `model` запрос должен быть вектором.

Поиск читается для каждой инструкции с её аргументами, а его строки хранятся под этими аргументами в течение TTL таблицы. Таблица поиска доступна только для чтения. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read`; `provisa/api/data/materialization.py` `_is_native_read`]

### Потоковая передача {: #streaming }

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `kafka` | Федеративный коннектор Kafka; схема через Confluent Schema Registry (Avro, Protobuf, JSON Schema), ручное определение или вывод по образцу (REQ-147, REQ-150) | Нет; живой вывод таблицы публикует в Kafka (REQ-565) |
| `websocket` | Внешний источник WebSocket — подключение, подписка, получение событий; результаты материализуются (REQ-338) | Нет |
| `rss` | Лента RSS 2.0 / Atom — опрос, водяной знак по pubDate/updated; результаты материализуются (REQ-342, REQ-343) | Нет |

### Приёмник push-данных {: #push-receiver }

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `ingest` | Внешние сервисы отправляют события через POST JSON; результаты материализуются (REQ-331, REQ-335) | Нет |

### Граф и семантика {: #graph-semantic }

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher через HTTP API, результаты кешируются в PostgreSQL (REQ-295) | Нет |
| `sparql` | SPARQL 1.1 через POST, результаты кешируются в PostgreSQL (REQ-297) | Нет |

### На основе файлов {: #file-based }

Три механизма охватывают файлы. Источники из одного файла и обходчик файлов используют поле `path` вместо `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) Третий, [Logs](#logs), задаёт свои файлы glob-шаблоном в `mapping` и читает только файлы на сервере Provisa. [tool-verified: `provisa/logs/settings.py`]

**Файловые базы данных и однофайловые источники** — `sqlite`, `duckdb`, `csv`, `parquet` указывают `path` на один файл. (Источник `duckdb` хранит своё расположение в поле `database` формы.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Тип источника | Расположения | Мутации |
| --- | --- | --- |
| `sqlite` | локальный путь, `https://`, `s3://`, `gs://`, `az://` | Да для локального файла; нет для удалённого |
| `duckdb` | локальный путь, `https://`, `s3://`, `gs://`, `az://` | Да для локального файла; нет для удалённого |
| `csv` | локальный путь, `https://`, `s3://`, `gs://`, `az://` (движок DuckDB) | Нет |
| `parquet` | локальный путь, `https://`, `s3://`, `gs://`, `az://` (движок DuckDB) | Нет |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Путь с любой другой схемой (например, `ftp://`) отклоняется: "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Учётные данные для удалённого расположения.** Укажите их в `federation_hints` источника. Каждый набор действует только для бакета или контейнера самого пути, поэтому учётные данные одного источника никогда не используются для расположения другого. Источник без учётных данных читается как публичное расположение. [tool-verified: `object_store_secret_ddl`]

| Расположение | Ключи `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key`; необязательные `region`, `endpoint` (S3-совместимое хранилище, например R2 или MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (ключ HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Удалённые файлы SQLite и DuckDB доступны только для чтения.** Движок DuckDB подключает их на месте с `READ_ONLY`, и таблица на таком файле не поддерживает insert, update или delete. На движке, который не подключает SQLite, удалённый файл SQLite читается через временное соединение DuckDB, подключающее его только для чтения, и его строки выгружаются как обычно. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Загрузка файла.** В форме источника `sqlite`, `duckdb`, `csv` или `parquet` загрузите файл вместо ввода пути. Файл потоком записывается на диск, никогда не удерживаясь в памяти, в папку организации по пути `files/<source id>/<file name>`, и путь источника указывает на него. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Замена.** Повторная загрузка занимает место прежнего файла, каким бы ни было его имя, и источник подготавливается заново, чтобы следующее чтение увидело новый файл. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Удаление.** Удаление источника удаляет загруженный файл. Больше ничто его не удаляет. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Кто.** Загружать могут только роли с возможностью `source_registration`. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Ограничение размера.** Настройка `sources.max_upload_bytes` (переменная окружения `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) ограничивает файл: по умолчанию 1 ГиБ (1073741824 байт), минимум 1, значение читается в начале загрузки. Файл большего размера отклоняется с HTTP 413 с указанием лимита, и ничего из него не сохраняется. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Путь.** Пока путь указывает на загруженный файл источника, поле пути в форме доступно только для чтения. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Имена.** Идентификатор источника и имя файла могут содержать только буквы, цифры, `.`, `_` и `-`. [tool-verified: `source_uploads._SEGMENT`]
- **Хранение.** [inferred from the `source_uploads` docstring] Загруженный файл хранится как любой файл организации: в папке организации, зашифрованным, если зашифрована папка организации, и виден каждому узлу на общем монтировании.

**Запись в локальный файл SQLite или DuckDB и блокировки.** Запись берёт блокировку файла и никогда её не ждёт. Если файл удерживает другой процесс или узел, запись отклоняется с указанием источника и того, что его файл заблокирован. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** Запись устанавливает `PRAGMA busy_timeout = 0`, поэтому заблокированный файл сразу даёт ошибку. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** DuckDB разделяет файл только между открытиями только для чтения. Поэтому подключение движка и собственное соединение источника держат файл только для чтения, и каждый узел читает его одновременно. Запись приостанавливает держателей в этом процессе, открывает файл на чтение и запись только для этой инструкции и закрывает его. Если при открытии не удаётся взять блокировку, запись отклоняется. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Общие монтирования.** В зашифрованной папке организации, монтирование которой разделяют несколько узлов (настройка `org_files.shared_mount`), запись в файл SQLite или DuckDB в этой папке отклоняется сразу, с указанием источника. Слой шифрования папок (gocryptfs) не передаёт блокировки файлов другим узлам, поэтому ничто не помешало бы двум узлам писать одновременно. Чтение по-прежнему работает. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Папка, разделяемая без шифрования, должна быть смонтирована с блокировками, которые доходят до каждого узла: монтирование NFS с включёнными блокировками, не `nolock` и не `local_lock`. Никакая проверка этого не контролирует; это требование к развёртыванию. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Обходчик файлов (источник `files`)** — указывает `path` на glob-шаблон, рекурсивно обходит его и регистрирует директорию как федеративный каталог таблиц. Он читает множество форматов через множество транспортов; наборы ниже приведены из файлового коннектора (форк kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Форматы | Транспорты |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow и документы, преобразуемые в таблицы — HTML, Markdown, DOCX, PPTX | Локальная файловая система, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST и Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Любой источник `files` может также объявить `crawl` в своём mapping с теми же настройками, что перечислены в разделе [Wikipedia](#wikipedia): тогда источник сначала читает веб-страницы и выгружает каждую HTML-таблицу данных и каждый связанный файл данных в своей директории как таблицу. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

На движке DuckDB `files` читается нативно — представление-сканер `read_csv_auto` для каждого `<table>.csv` в разрешённой директории (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. На движке без собственного коннектора `files` строки загружаются через тот же встроенный в коннектор сервер Calcite pgwire (`pgwire-file`), который используют sharepoint/splunk (REQ-954) — см. [Корпоративные SaaS-коннекторы](#enterprise-saas-connectors) ниже. Сквозное покрытие UI (форма Sources → регистрация таблицы → SQL-запрос) и путь загрузки через pgwire подтверждены в REQ-1694.

#### Logs (REQ-1996) {: #logs }

Источник `logs` превращает файлы журналов на сервере Provisa в одну таблицу. Вы задаёте файлы полным путём или glob-шаблоном и выбираете формат, в котором записаны их записи. Каждая запись становится строкой. Запись, которую формат не может прочитать, сохраняется вместе с исходным текстом и причиной и никогда не отбрасывается. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

Mapping принимает четыре ключа и по имени отклоняет любой другой: `glob` и `format` (оба обязательны), а также `pattern` и `record_start`, которые принимает только формат `regex`. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

В форме Sources источник Logs называется «Log files» и находится в группе File. Кнопка **Preview** до сохранения показывает файлы, которым соответствует glob, столбцы, которые даёт формат, и первые 20 строк самого нового файла. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**Glob.** Это должен быть полный путь; `~` раскрывается, а относительный путь отклоняется («name the files by their full path»). Сопоставление идёт функцией `glob` из Python в нерекурсивном режиме, поэтому `**` ведёт себя как `*` и сопоставляет в пределах одного уровня каталога. Берутся только обычные файлы, в порядке имён. Пустой файл пропускается, пока в нём нет байтов. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] Glob может ссылаться на переменную окружения, например `${env:LOG_DIR}/*.log`. Источник хранит ссылку в том виде, в каком она записана, и раскрывает её при каждом чтении файлов; незаданная переменная отклоняется с указанием её имени. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

**Собственные столбцы каждой строки.** Они идут перед столбцами формата:

| Столбец | Тип | Содержит |
| --- | --- | --- |
| `_file_id` | text | Идентичность файла: контрольная сумма его первых 4096 байт после распаковки |
| `_file_name` | text | Путь, по которому прочитана запись |
| `_line_offset` | bigint | Смещение в байтах первой строки записи в распакованном тексте |
| `_entry` | bigint | Каким по счёту элементом записи является строка: 0, кроме пакетных записей, например `Records` в файле CloudTrail |
| `_raw` | text | Запись в том виде, в каком она записана |
| `_parse_error` | text | Почему запись или одно из её значений не удалось прочитать; NULL, если удалось |
| `_landed_at` | timestamp | Когда строка была загружена |

`_file_id`, `_line_offset` и `_entry` образуют ключ строки, поэтому запись, прочитанная дважды, остаётся одной строкой. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Форматы и пресеты.** Выпадающий список форматов группирует их по семействам; последний вариант, `regex`, — ваш собственный шаблон. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

| Семейство (подпись в форме) | Названия форматов |
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

Итого 56 встроенных форматов и `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Как формат создаёт столбцы.** Формат получает столбцы одним из четырёх способов. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Фиксированные столбцы.** Большинство форматов сами задают столбцы и типы. Например, `rfc3164` даёт `timestamp`, `facility`, `severity`, `host`, `process`, `pid` и `message`; `facility` и `severity` вычисляются из значения `<PRI>`. Строка BSD syslog не содержит года, поэтому берётся год последней записи в файл, а если месяц строки позже месяца этой записи — предыдущий год.
- **Поля, названные записью.** `jsonl` и `logfmt` берут столбцы из полей, встреченных в первых строках файлов (настройка `logs.preview_lines`, по умолчанию 1000). Каждый столбец получает самый узкий тип, подходящий для всех увиденных значений: boolean, затем integer, float, timestamp и json, иначе text. Поле, которое несёт более поздняя запись, но для которого нет столбца, попадает в json-столбец `extra`.
- **Поля, названные заголовком.** `w3c` и пресеты на его основе читают строку `#Fields:` каждого файла; `date` и `time` объединяются в один столбец `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` и `zeek_tsv` так же читают строки заголовка, начинающиеся с `#`, а `aws_vpc_flow` читает строку заголовка, начинающуюся с `version`. Место, где прочитана строка заголовка, сохраняется вместе с файлом, поэтому файл, который дочитывают позже, сохраняет свои поля.
- **`regex`.** Каждая именованная группа, например `(?P<level>…)`, — это столбец типа text.

Экран Register Table перечисляет эти столбцы с их типами и отмечает ключ. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] При чтении строк каждое значение приводится к типу, который его столбец имеет в зарегистрированной таблице, поэтому применяется тип, изменённый там. Значение, которое столбец вместить не может, становится NULL, а причина записывается в `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Запись длиннее `logs.max_line_bytes` (по умолчанию 65536) сохраняет в `_raw` свои первые байты и не разбирается; `_parse_error` указывает её длину. Файл, начинающийся с метки порядка байтов UTF-8 или UTF-16, читается в этой кодировке, любой другой — как UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Записи из нескольких строк.** Формат с шаблоном начала записи собирает запись из строки, соответствующей ему, и всех следующих за ней строк, которые ему не соответствуют. Трассировка стека остаётся вместе со своим исключением, а запись о медленном запросе остаётся целой. Строки до первого совпадения образуют отдельную запись; формат обычно её отклоняет, но строка сохраняется. Встроенные форматы с началом записи: `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` и `azure_monitor`. С `regex` задайте `record_start` сами. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Два формата читают свои записи сами, а не построчно. `kubernetes_cri` склеивает в одну запись частичные строки (`P`), на которые их разбила среда выполнения, а `windows_evtx` читает двоичный файл `.evtx`. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Файл, в который ещё пишут, читается до последней полной строки. Для многострочного формата он читается до начала своей последней записи, которую следующая строка может продолжить. Когда в файл не писали 2 секунды, эта последняя запись считается целой. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Ротация.** Файл определяется контрольной суммой его первых 4096 байт после распаковки, а не именем. Когда `system.log` ротируется в `system.log.0`, а затем сжимается в `system.log.0.gz`, все копии — один и тот же файл. Чтение продолжается с байта, до которого дошло прошлое чтение, поэтому ни одна строка не читается дважды. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- Сжатие определяется по первым байтам файла, а не по имени: gzip и bzip2. Сжатый файл считается завершённым.
- Ротированная копия и её сжатый двойник, если соответствуют обе, читаются один раз — из сжатой.
- Файл, оказавшийся короче уже прочитанного смещения, был усечён: это новый файл, он читается с начала. Строки старого остаются.
- Файлы короче 4096 байт, начинающиеся одинаково, различаются по inode, поэтому остаются двумя файлами.
- Файл, которому glob больше не соответствует, забывается; его строки остаются как история.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Обновление и живая доставка.** Таблица Logs — это реплика. За файлами никто не следит. Вместо этого, когда сигнал изменений источника — `probe` или `ttl_probe`, его проба вычисляет дайджест пути, inode, размера и времени последней записи каждого подходящего файла; если дайджест отличается, запрашивается обновление. Типы проб, которые принимает источник Logs, — `hash` и `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Обновление читает только новое. Каждый файл читается с места, до которого дошёл прошлый проход, а строки записываются по ключу, по 5000 за раз, прежде чем сохраняется новая позиция. Проход, прервавшийся между этими двумя шагами, читает те же записи заново, и строки получаются теми же. Каждый проход помечает свои строки в `_landed_at` — столбце, который живая доставка использует как водяной знак. Изменение определения таблицы — это полная перестройка, которая читает каждый файл с начала. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Разрешённые корни.** Настройка оператора `logs.allowed_roots` перечисляет каталоги, в которые может разрешаться glob. Значение по умолчанию — `["/var/log"]`; это список на карточке Security в Admin → Settings, который также задаётся через `PROVISA_LOGS_ALLOWED_ROOTS`, а изменение требует подтверждения. Каждый корень разрешается после раскрытия `~` и символических ссылок. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Действуют две проверки. Фиксированная часть glob до первого подстановочного знака должна разрешаться внутри корня; это проверяется при сохранении источника и при каждом просмотре или чтении. Каждый подходящий файл также должен разрешаться внутри корня после раскрытия его символических ссылок, поэтому ссылка за пределы `/var/log` отклоняется. Отказ называет путь, во что он разрешился, и разрешённые корни:

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

При сохранении код — `schema.logs_path_refused`; при просмотре — 403 с `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Отказ на хостинге (SaaS).** Развёртывание, в котором работает много организаций, не читает никаких собственных файлов журналов. Каждая проверка пути отказывает первой — при сохранении, просмотре и чтении — сообщением «`<path>`: a server that holds many organizations reads no log file of its own». Эти файлы принадлежат серверу, а не какой-либо одной организации, поэтому ни один арендатор не вправе их читать. Развёртывание определяет, обслуживает ли оно много организаций, по состоянию своей мультитенантности. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Наборы данных Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle — это платформа для загрузки файлов. Загруженный набор данных Kaggle регистрируется как источник типа `files` и опрашивается через тот же коннектор pgwire-file, который используют любые другие источники `files` — в перечислении SourceType нет отдельного типа `kaggle`. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — no `kaggle` literal]

**Добавление набора данных.** Откройте Sources → Subscriptions → Kaggle. Форма выполняет два последовательных шага, потому что собственная конечная точка поиска наборов данных Kaggle требует аутентификации — селектор не может появиться до проверки токена. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Токен** — введите API-токен Kaggle и нажмите «Validate». Проверка вызывает `POST https://www.kaggle.com/api/v1/datasets/create/new` с пустым телом. `401` означает недействительность; любой другой ответ означает валидность (собственная валидация полезной нагрузки Kaggle срабатывает до создания какого-либо набора данных — ничего не сохраняется). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Селектор** — поле поиска в реальном времени по мере ввода запрашивает `GET /api/v1/datasets/list`. Каждый результат показывает заголовок и описание набора данных. Выберите один, затем нажмите «Add Dataset».

Нажатие «Add Dataset» загружает пакет из `GET /api/v1/datasets/download/{owner}/{ref}`, распаковывает элементы CSV и Parquet в `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` и создаёт **один** источник типа `files`, чей `path` — эта корневая директория. Токен никогда не сохраняется на стороне сервера. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

После добавления источника зарегистрируйте его таблицы через обычный экран регистрации таблицы. Рекурсивное обнаружение директорий коннектора pgwire-file перечисляет каждый файл как отдельную таблицу — тот же механизм, что используют другие источники `files` (REQ-1690). (REQ-1783)

**Ограничение v1.** Пакет, содержащий файл `.sqlite` или `.db`, отклоняется сразу с чёткой ошибкой. Загружаются только файлы CSV и Parquet. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Именование таблиц.** Обойденная таблица называется `<page>__<name>`: заголовок страницы, затем подпись таблицы или, если подписи нет, последний заголовок над таблицей — оба в написании имён столбцов источника. Например, таблица под заголовком «Tallest buildings in the world» на странице `List_of_tallest_buildings` называется `list_of_tallest_buildings__tallest_buildings_in_the_world`. Связанный файл данных сохраняет собственное имя. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Обновление.** Чтобы повторно получить набор данных после публикации Kaggle новой версии, вызовите GraphQL-мутацию `refreshKaggleSource` с идентификатором источника и действительным токеном. Она заново загружает файлы на месте и очищает кеш конечной точки pgwire-file, чтобы коннектор подхватил любые изменения схемы при следующем запросе. `kaggle_owner` и `kaggle_ref`, сохранённые в `federation_hints` при создании, определяют, какой набор данных обновлять. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Нет статического пути конфигурации YAML.** Источники Kaggle создаются только через форму Sources. Источник Kaggle, экспортированный в YAML, отображается как `type: files` с `kaggle_owner` и `kaggle_ref` в `federation_hints`. Повторная загрузка из Kaggle требует потока обновления через UI или мутации `refreshKaggleSource` — указание `path` в YAML на уже загруженную директорию — альтернатива для сред без доступа к интернету (air-gapped).


#### Wikipedia (REQ-1960) {: #wikipedia }

Wikipedia обходит названные вами страницы. Каждая таблица данных на странице и каждый связанный файл CSV, TSV, Excel, JSON или Parquet становится таблицей источника. Как и Kaggle, он регистрируется как источник `files` с `crawl` в mapping. Добавление источника ничего не регистрирует: стюард регистрирует нужные таблицы через обычный экран Register Table. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

**Поля настройки.**

| Поле | По умолчанию | Значение |
| --- | --- | --- |
| Pages to start from | нет (обязательно) | По одной на строку: название страницы или адрес страницы в выбранной редакции |
| Language edition | `en` | Код редакции, например `en`, `de` или `ja` |
| Link depth | `1` | На сколько ссылок от начальной страницы читать; `0` читает только начальные страницы |
| Page limit | `25` | Наибольшее число страниц, читаемых за один обход |
| Contact | пусто | Отправляется с каждым запросом, чтобы Wikipedia могла с вами связаться, как требует её политика |
| Follow "See also" links | выкл. | Выключено — раздел исключается из статьи |
| What this edition calls its "See also" section | пусто | Запрашивается, только когда флажок выше выключен и Provisa не может назвать раздел для выбранной редакции. Название раздела так, как его показывает страница, например `Voir aussi` |
| Directory for the crawl's files | пусто | Куда записываются таблицы и файлы данных. Пусто: `<PROVISA_DATA_DIR>/crawl/<source id>` |

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa знает заголовок «See also» этих редакций: `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Для любой другой редакции при выключенном «See also» форма спрашивает, как редакция называет этот раздел; введите его название так, как его показывает страница (например, `Voir aussi`). Источник, сохранённый без него, отклоняется (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Какие ссылки отслеживаются.** Ссылка выбирается по тому, чем её называет собственная разметка страницы, а не по имени в её адресе: такие имена различаются в каждой языковой редакции и встречаются также в обычных заголовках. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Отслеживаются: ссылки на другие статьи, расположенные в тексте статьи, в списках и таблицах данных. Метка разметки — `a[rel='mw:WikiLink']`.
- Не отслеживаются: ещё не написанные страницы, сама страница, поиск по книжным источникам, страницы файлов, другие вики и внешние сайты.
- Не читаются вовсе (удаляются до сбора ссылок и таблиц): навигационные блоки, информационные блоки, примечания и цитаты, категории, оглавление, изображения и служебные сообщения.
- Не читаются, пока вы не включите параметр: раздел «See also».

Таблицами становятся только таблицы, которые редакторы помечают как таблицы данных (`table.wikitable`), а таблица менее чем из 2 строк не предлагается. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Расширенные настройки обхода.** Форма держит их свёрнутыми под «Crawl settings» и показывает каждую с её значением по умолчанию; оставьте поле пустым, чтобы сохранить его. В mapping источника `files` те же имена идут под `crawl:`, а имя вне этого списка отклоняется. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

| Настройка | Значение по умолчанию для Wikipedia | Значение |
| --- | --- | --- |
| `start_urls` | названные вами страницы | Страницы, с которых начинается обход |
| `max_depth` | `1` | Глубина ссылок |
| `max_pages` | `25` | Лимит страниц |
| `request_delay` | `1 seconds` | Задержка между запросами |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, с добавленным вашим контактом | Имя, под которым обход представляется |
| `content_selector` | `#mw-content-text .mw-parser-output` | CSS-селектор: ссылки и таблицы читаются только внутри него |
| `remove_selectors` | список частей, описанный выше | CSS-селекторы для исключаемых частей. Указание заменяет весь список, включая правило «See also» |
| `link_selector` | `a[rel='mw:WikiLink']` с исключениями | CSS-селектор, которому должна соответствовать ссылка, чтобы её отслеживать |
| `link_exclude_patterns` | пусто | Регулярные выражения; ссылка, в адресе которой одно из них найдено, не отслеживается |
| `follow_external_links` | `false` | Переходить по ссылкам на другие сайты |
| `allowed_domains` | не задано | Домены, которые обход может посещать |
| `table_selector` | `table.wikitable` | CSS-селектор, которому должна соответствовать таблица |
| `generate_tables_from_html` | не задано | Становятся ли HTML-таблицы таблицами |
| `html_table_min_rows` | `2` | Наименьшее число строк в таблице |
| `html_table_max_rows` | не задано | Наибольшее число строк в таблице |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Типы связанных файлов, которые становятся таблицами |
| `html_cache_ttl` | `1 days` | Как долго хранится обход |
| `max_html_size` | не задано | Наибольшая читаемая страница, например `10MB` |
| `max_data_file_size` | не задано | Наибольший читаемый файл данных, например `100MB` |

**Именование таблиц.** Обойденная таблица называется `<page>__<name>`. Имя — собственный `id` таблицы, если он есть, иначе её подпись, иначе ближайший заголовок над ней. Например, таблица под заголовком «Tallest buildings» на странице `List_of_tallest_buildings` после нормализации имени становится `list_of_tallest_buildings__tallest_buildings`. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Обновление.** Обход хранится в течение периода кеша, по умолчанию `1 days` (`html_cache_ttl`). Страницы загружаются заново только после истечения этого времени. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]


### Наблюдаемость и прочее {: #observability-other }

`prometheus` имеет коннектор Trino (свойства строятся из DSL сопоставления этого типа). `google_sheets` — зарегистрированный тип источника без коннектора Trino, материализующийся через конвейер API-кеша. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (материализуется) | Нет |
| `prometheus` | prometheus | Нет | Метрика — это таблица, а образец — это строка (`timestamp`, `value`, по одному столбцу на метку); на любом движке без живого коннектора Provisa читает HTTP API — имена метрик и метки для регистрации таблицы, `query_range` по диапазону таблицы для создания реплики [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689)

### Корпоративные SaaS-коннекторы {: #enterprise-saas-connectors }

Google Workspace и Microsoft 365 читаются собственными загрузчиками; см. [Google Workspace и Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, облачный инвентарь и Splunk регистрируются через коннекторы Apache Calcite (форк kenstott/calcite). Ни у одного нет прямого драйвера — Provisa запускает встроенный в коннектор сервер Calcite pgwire (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-cloudops`, `pgwire-splunk`) и обращается к нему как к обычной конечной точке PostgreSQL. На движке DuckDB эта конечная точка подключается вживую через расширение postgres: регистрация таблицы перечисляет таблицы коннектора из подключённого каталога, запросы читают коннектор на месте, а фильтры и проекции проталкиваются в Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Любой другой движок загружает строки в хранилище материализации для федерации (REQ-954). Пакеты загружаются под конкретную ОС/архитектуру из закреплённого релиза `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Оператор может вместо этого задать папку с пакетом, собранным для данного хоста, отдельно для каждого коннектора, в разделе Admin → Настройки → Серверы коннекторов (`connectors.<connector>.bundle_dir`); см. [Admin](admin.md).. Все пять коннекторов всегда включают сопоставление имён без учёта регистра, что соответствует собственной регистронезависимой семантике каждого продукта (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Списки SharePoint перечисляются как схемы и представляются в виде запрашиваемых таблиц (REQ-726, REQ-731). Два метода аутентификации: `CLIENT_CREDENTIALS` (по умолчанию) и на основе сертификата через PFX-сертификат (REQ-727). Секретные значения в `mapping` разрешаются через движок секретов до достижения коннектора (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` или `host` | `site-url` | URL сайта SharePoint |
| `username` | `client-id` | ID клиента приложения Azure |
| `password` | `client-secret` | Секрет клиента приложения Azure |
| `database` | `tenant-id` | UUID клиента (тенанта) Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (по умолчанию) или `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Путь к PFX при `auth_type: CERTIFICATE` — должен быть АБСОЛЮТНЫМ |
| `mapping.certificate_password` | `certificate-password` | Пароль PFX — ключ должен присутствовать, пустая строка для PFX без пароля |

Аутентификация по сертификату на движках, отличных от Trino, несёт два дополнительных правила, оба применяются при построении операнда `model.json` сервера Calcite pgwire (REQ-1693). `certificate_path` должен быть абсолютным: сервер работает с директорией своего пакета как рабочей директорией, поэтому относительный путь разрешается внутри кеша runtime-зависимостей, и PFX не находится. `certificate_password` должен присутствовать в `mapping`, даже если у PFX нет пароля — в этом случае это пустая строка; адаптер Calcite полностью отклоняет пустой (null) пароль, а отсутствующий ключ трактуется как ошибка конфигурации, а не молча читается как пустой пароль. Отсутствующее или относительное значение вызывает `MissingConnectorConfig` с именем поля. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

Когда коннектор не предоставляет `information_schema.columns`, зарегистрируйте таблицу с явными определениями столбцов (полученными из Microsoft Graph API) через мутацию `registerTable` (REQ-732).

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

Аутентификация по сертификату, с абсолютным путём и всегда присутствующим паролем:

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

Каждый sObject, который доступен учётным данным для запросов, — это таблица, которую стюард может зарегистрировать (`Account`, `Opportunity`, пользовательские объекты `*__c`); автоматически ничего не регистрируется (REQ-1946). Столбцы и типы берутся из describe объекта. Фильтры, проекции, сортировки и лимиты передаются в Salesforce в виде SOQL. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

Источник Salesforce указывает URL входа «Мой домен» организации и один полный набор учётных данных, выбираемый через `mapping.auth_type`. URL входа — полный URL `https://…` (например, `https://acme.my.salesforce.com`); голый хост или любая другая схема отклоняется с указанием поля. Неполный набор отклоняется при использовании источника с указанием недостающего значения. Каждый секрет может быть ссылкой `${secret:…}` или `${env:…}`. Секрет, введённый как буквальное значение, сохраняется в хранилище организации, а источник хранит ссылку на него.

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` или `host` | `login-url` | URL «Мой домен» организации (Настройка, затем Мой домен). `login.salesforce.com` не работает с потоком client credentials |
| `username` | `client-id` | Ключ клиента подключённого приложения |
| `password` | `client-secret` | Секрет клиента подключённого приложения |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (по умолчанию), `USERNAME_PASSWORD` или `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | Только `USERNAME_PASSWORD`, вместе с ключом и секретом клиента |
| `mapping.security_token` | `security-token` | Только `USERNAME_PASSWORD`; необязательно |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | Только `ACCESS_TOKEN`; без ключа и секрета клиента |
| `mapping.api_version` | `api-version` | Необязательно, например `v61.0` |

Для `CLIENT_CREDENTIALS` подключённому приложению нужны *Enable Client Credentials Flow* и пользователь *Run As*; каждое чтение и каждая запись выполняются с правами этого пользователя. Организации, созданные начиная с Summer '23, по умолчанию блокируют поток «имя пользователя и пароль».

**Чтение.** В Trino источник читается через каталог `salesforce`, который только читает. В любом другом движке движок подключает pgwire-сервер источника и читает его на месте.

**Запись.** `INSERT`, `UPDATE` и `DELETE` над зарегистрированным sObject — это мутации: они управляются, попадают в аудит и обрабатываются по правилам среды, как любая другая мутация. В каждом движке, включая Trino, запись выполняется на pgwire-сервере источника; в Trino этот сервер работает только для записи. Каждая инструкция отправляется в Salesforce и фиксируется при выполнении, поэтому её нельзя откатить. Сервер не возвращает записанные им строки: инструкция с предложением `RETURNING` отклоняется, а GraphQL не предлагает поля мутаций для этих таблиц; SQL `INSERT`, `UPDATE`, `DELETE` и `COPY` — пути записи. Запись — это одна простая инструкция над одной таблицей: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` и `TRUNCATE` отклоняются с указанием формы. Источник SharePoint записывается так же. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Запуск.** Прежде чем принимать подключения, сервер источника читает столбцы каждого sObject — около четырёх минут для организации с 1200 объектами. Это оплачивается один раз: результаты describe хранятся в собственном каталоге состояния источника внутри каталога данных экземпляра. Сервер запускается при регистрации или загрузке источника. В Trino каталог `salesforce` хранит собственные результаты describe, по одному каталогу на источник в `/data/trino/cache/salesforce-describe` внутри координатора, поэтому и там цена оплачивается один раз, если этот путь — том, переживающий контейнер: том `trino_cache` стека compose или том кеша координатора в Helm-чарте (`trino.coordinator.cache.persistence`). Пока сервер не принимает подключения, Register Table показывает, что коннектор запускается, а запись получает то же сообщение, а не ждёт. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Поток «имя пользователя и пароль»:

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

Каждая таблица инстанса доступна для регистрации; ничего не регистрируется автоматически (REQ-1954). Полный инстанс может содержать тысячи таблиц; Register Table группирует их по теме — каждая тема является отображаемым именем пакета ServiceNow, или «(без пакета)», если таблица не принадлежит ни одному пакету. Каждая таблица отображается как `name — label`. При выборе таблицы её столбцы считываются из инстанса; простое перечисление таблиц столбцы не загружает. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Источник ServiceNow указывает полный URL `https://…` инстанса, а также имя пользователя и пароль. URL берётся из `base_url` или `host`; голый хост или любая другая схема отклоняется с указанием поля (`instanceUrl`). Отсутствующее имя пользователя или пароль отклоняется с указанием поля. Каждое значение может быть ссылкой `${secret:…}` или `${env:…}`. Секрет, введённый как буквальное значение, сохраняется в хранилище организации, а источник хранит ссылку на него.

| Поле источника | Свойство адаптера | Примечание |
| --- | --- | --- |
| `base_url` или `host` | `instanceUrl` | Полный URL `https://…` инстанса (например, `https://acme.service-now.com`). Голый хост или другая схема отклоняется |
| `username` | `username` | Имя пользователя для базовой аутентификации |
| `password` | `password` | Пароль для базовой аутентификации |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Чтение.** В Trino источник читается через каталог `servicenow`, который только читает. В любом другом движке движок подключает pgwire-сервер источника и читает его на месте. От инстанса запрашиваются только выбранные столбцы (`sysparm_fields`). Фильтр передаётся в инстанс (`sysparm_query`) только в форме, которую адаптер проверил на реальном инстансе; все прочие фильтры, а также каждая сортировка и лимит применяются к уже прочитанным строкам. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Ссылочное поле представлено двумя столбцами: `<field>` содержит sys_id referenced записи, а `<field>__display` — её отображаемое значение. Пустое поле — NULL. Поле, тип которого адаптер не может отобразить, вызывает ошибку с указанием таблицы, поля и типа. [inferred from adapter behavior; в данной сессии не проверялось непосредственно в исходном коде Provisa]

Поиск таблиц (естественный язык) сопоставляется по имени, метке и теме каждой таблицы. После регистрации таблицы её столбцы присоединяются к поиску. [tool-verified: `provisa/discovery/catalog_cache.py` docstring `COLUMNS_PER_TABLE_KINDS`]

**Жизненный цикл сервера.** Сервер обслуживает только зарегистрированные из него таблицы. Регистрация или удаление таблицы перезапускает сервер и повторно провизионирует источник на движке; запрос, поступивший во время перезапуска, завершается ошибкой. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Запись.** `INSERT`, `UPDATE` и `DELETE` на зарегистрированной таблице — это мутации: управляемые, проходящие аудит и обрабатываемые по правилам среды, как любая другая мутация. В каждом движке, включая Trino, запись выполняется на pgwire-сервере источника. Сервер возвращает строку, которую инстанс содержит после записи; клауза `RETURNING` поэтому принимается, и GraphQL предлагает поля мутаций для этих таблиц. Запись — это одна инструкция над одной таблицей: `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` и `TRUNCATE` отклоняются с указанием формы. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import-сеты.** Если у таблицы есть активная карта преобразования (transform map) в неё, Register Table предлагает её import-сеты, и стюард может привязать таблицу к одному из них (таблица с несколькими картами называет каждую). Вставки в привязанную таблицу проходят через Import Set API инстанса и преобразование той карты; вставки в непривязанную таблицу, а также каждое обновление и удаление записываются в таблицу напрямую. Строка, отклонённая преобразованием, отклоняется с указанием этой строки; уже записанные строки остаются записанными. Привязка проверяется при сохранении таблицы: она должна быть import-сетом активной карты преобразования в эту таблицу, в источнике ServiceNow. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal`; `provisa/federation/servicenow.py` `servicenow_settings`; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `cloudops`

Облачный инвентарь представляет ресурсы Azure, AWS и GCP как один набор таблиц: `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` и `compute_security_groups`, связывающую вычислительный ресурс с его группами безопасности. У каждой таблицы один набор столбцов для всех трёх облаков, а её столбец `cloud_provider` называет облако, из которого пришла строка. Источник только читает, а его таблицы регистрирует стюард, как и любые другие (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

Источник называет одно или несколько облаков. Для каждого облака действует правило «всё или ничего»: облако, у которого заполнена лишь часть обязательных значений, отклоняется с указанием недостающего, как и источник, не называющий ни одного облака. Источник читает ровно те облака, которые называет. Все значения находятся в `mapping`; каждый секрет может быть ссылкой `${secret:…}` или `${env:…}`. Секрет, введённый как буквальное значение, сохраняется в хранилище организации, а источник хранит ссылку на него.

| Облако | Ключ `mapping` | Свойство коннектора | Примечания |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Обязательно |
| Azure | `azure_client_id` | `azure.client-id` | Обязательно; ID клиента регистрации приложения |
| Azure | `azure_client_secret` | `azure.client-secret` | Обязательно; секрет |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Обязательно; через запятую |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Обязательно |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Обязательно; секрет |
| AWS | `aws_region` | `aws.region` | Обязательно, например `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Обязательно; через запятую |
| AWS | `aws_role_arn` | `aws.role-arn` | Необязательно; роль, принимаемая для доступа между аккаунтами |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Обязательно; АБСОЛЮТНЫЙ путь к JSON-файлу ключа сервисного аккаунта |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Обязательно; через запятую |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Необязательно; как долго результат используется повторно, прежде чем облака будут опрошены снова (по умолчанию в адаптере 5) |

Файл ключа GCP открывает тот, кто читает источник. В Trino это сервер Trino, поэтому путь должен существовать на нём. В любом другом движке это pgwire-сервер источника на хосте Provisa, чей рабочий каталог — его собственный каталог состояния, поэтому относительный путь отклоняется.

В Trino источник читается через каталог `cloudops`. В любом другом движке движок подключает pgwire-сервер источника и читает его на месте, как источник SharePoint.

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

Результаты поиска Splunk доступны для запросов как таблицы (например, `internal_server`) (REQ-721). URL коннектора берётся из `base_url` либо формируется как `https://{host}:{port}` со значением порта по умолчанию `8089` (REQ-722). Аутентификация: когда `mapping.use_token` равно `true` (по умолчанию), `password` передаётся как API-токен; когда `false`, `username` и `password` передаются как отдельные учётные данные (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, иначе `https://host:port` (порт по умолчанию 8089) |
| `password` | `token` или `password` | токен, когда `use_token: true` |
| `username` | `user` | только когда `use_token: false` |
| `database` | `app` | ограничить конкретным приложением Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | фильтр по модели данных |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | для самоподписанных сертификатов (REQ-724) |

На пути pgwire-replica (любой движок, кроме Trino) те же четыре опциональных настройки становятся ключами операнда Calcite `model.json`: `app`, `token`/`username`+`password`, `datamodelFilter` и `disableSslValidation` — последние два приводятся к типам, которые ожидает `SplunkSchemaFactory`: строка и булево значение (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

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

#### Google Workspace и Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` и `microsoft_365` читают почту, календари и задачи учётных записей, которые называет источник, и загружают их как реплики. Это не коннекторы Calcite: у каждого свой загрузчик, обращающийся к API провайдера — Gmail, Google Calendar и Google Tasks с одной стороны и Microsoft Graph с другой. Форма перечисляет их в группе Enterprise как «Google Workspace (Gmail)» и «Microsoft 365 (Outlook, Exchange)». [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Таблицы.** Оба типа создают одни и те же канонические таблицы: те же имена, типы столбцов и порядок. Для объединения двух не нужны ни приведение типов, ни переименование; столбец, для которого у одного из провайдеров нет значения, равен NULL. Таблицы перечислены в схеме `default`, и каждая строка начинается с учётной записи, из которой она прочитана. Источник предлагает таблицы тех ресурсов, которые он читает: [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Ресурс | Таблицы | Записываются инструкциями |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Нет |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Столбцы перечислены в [канонической схеме почты](arch/canonical-mail-schema.md). `events` хранит одиночные события, главные события серий с правилом повторения в виде строк RFC 5545 и исключения; развёрнутых вхождений серии она не хранит никогда. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Регистрация.** Сначала администратор организации вводит клиент организации для платформы в Admin → Email: ID клиента и секрет, а для Microsoft 365 ещё и ID каталога (tenant) или домен. Тот, кто добавляет источник, их не видит и не вводит. Затем источник принимает одну из двух форм: [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Один почтовый ящик.** Источник называет один адрес в `accounts`. Его владелец нажимает **Connect** в форме и одобряет клиент организации. Полученное одобрение хранится в хранилище состояния организации, а не в настройках источника. В Google Workspace эта форма дополнительно задаёт `sign_in: google_account`.
- **Почтовые ящики организации.** Источник задаёт `mailboxes` ровно одним из вариантов: `everyone: true`, `group: <address or id>` или `list: [addresses]`. Учётных данных он не хранит, и подключать его никто не должен. Организация читает со своим собственным доступом, и только после того, как администратор включил «Read the organization's mailboxes». В Google этот доступ — ключ сервисного аккаунта с делегированием на уровне домена плюс адрес администратора для обращений к каталогу. В Microsoft это грант client-credentials регистрации приложения.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

При второй форме одна таблица содержит строки всех ящиков. Роль, которой разрешено читать таблицу, читает все ящики, если её не ограничивает правило строк по столбцу `account`. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

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

**Настройки.** Каждый модуль по имени отклоняет ключ, который он не читает. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

| Ключ `mapping` | Тип | Значение |
| --- | --- | --- |
| `resources` | оба | Какие из `mail`, `calendar`, `tasks` читает источник; не менее одного |
| `accounts` | оба | Адрес единственного ящика (форма «один ящик») |
| `mailboxes` | оба | `everyone`, `group` или `list` (форма организации) |
| `sign_in` | `google_workspace` | `google_account` (форма «один ящик») |
| `mail_since` | оба | `YYYY-MM-DD`; ничего, полученное раньше, не хранится |
| `mail_content` | `google_workspace` | `full` или `headers` — только заголовки и метки (область `gmail.metadata` Gmail). Обязателен, если читается почта |
| `mail_search` | `google_workspace` | Поиск в том виде, как его вводят в строку поиска Gmail |
| `mail_labels` | `google_workspace` | Названия меток; читается только почта, несущая каждую из них |
| `mail_include_spam_trash` | `google_workspace` | По умолчанию `false` |
| `calendar_send_updates` | оба | Сообщает ли запись участникам события; по умолчанию выключено |
| `calendar_check_days` | `microsoft_365` | На сколько дней в обе стороны от сегодняшнего дня смотрит проверка изменений; целое число, 1 или больше, обязательно, если читается календарь |

Google отклоняет поиск в области `gmail.metadata`, поэтому `mail_content: headers` вместе с `mail_search` или `mail_since` отклоняется. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Области доступа (scopes).** При входе запрашиваются только области отмеченных ресурсов: [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Ресурс | Google Workspace | Microsoft 365 (делегированные) |
| --- | --- | --- |
| `mail` | `gmail.readonly` или `gmail.metadata` только для заголовков | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| каждый источник | `userinfo.email` | `offline_access`, `User.Read` |

Для формы организации Microsoft требует разрешения приложения `Mail.Read` и `User.Read.All`, а для календарей и задач ещё `Calendars.ReadWrite` и `Tasks.ReadWrite.All`. Сервисный аккаунт Google авторизован на области из `DELEGATED_SCOPES`, которые также включают чтение пользователей каталога и участников групп. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Обновление и актуальность.** Эти источники принимают типы проб `hash` и `none`, а токен изменений предоставляет их загрузчик: [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail:** дайджест текущего history id каждого ящика, один вызов `users.getProfile` на ящик. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Почта Microsoft:** каждая сохранённая delta-ссылка, пройденная на один элемент. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Календари и задачи Google:** тег и время последнего изменения каждого календаря; тег каждого списка задач, а также id и время обновления каждой задачи. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Календари и задачи Microsoft:** строки изменений каждого ящика за `calendar_check_days` дней в обе стороны от сегодняшнего дня. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Проверка, которую выполнить нельзя, запрашивает чтение; она никогда не сообщает «unchanged». Таблицы календарей и задач читаются целиком при каждом обновлении. Реплики почты источника Microsoft 365 или Google Workspace хранятся на месте: каждая часть ящика записывается по мере поступления и сразу доступна для запросов. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Источник Google Workspace, сужающий выборку через `mail_search`, при каждом обновлении читается заново целиком, потому что только Google может сказать, чему соответствует поиск. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Запись.** `INSERT`, `UPDATE` и `DELETE` для `events`, `event_attendees` и `tasks` уходят провайдеру по одному пути записи для обоих типов. `UPDATE` или `DELETE` называет свои строки по `provider`, `account` и `id`; участник называется по `event_id` и `address`. Одно вхождение серии называется по `series_id` и `original_start_at`. Запись, задающая столбец вне набора записываемых столбцов таблицы, отклоняется по имени. Google пишет с `sendUpdates=all`, когда `calendar_send_updates` включена, и с `sendUpdates=none`, когда выключена. Microsoft Graph не может сделать такое изменение незаметно, поэтому изменение состава участников встречи отклоняется с указанием этой настройки. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Исключения.** Почта только для чтения; ни одна инструкция не изменяет почтовый ящик. `calendars`, `task_lists` и все таблицы почты не принимают записи. Папки Spam и Trash в Gmail исключены, пока не включён `mail_include_spam_trash`. Google Tasks API хранит только день, на который назначен срок задачи, и не имеет напоминаний. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### API-источники {: #api-sources }

Зарегистрируйте любую HTTP-конечную точку как запрашиваемую таблицу. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Тип API | Обнаружение | Вывод типов столбцов |
| --------- | ----------- | ----------------- |
| `openapi` | Разбор спецификации OpenAPI (REQ-314, REQ-316) | Примитивы → нативные типы, объекты → JSONB |
| `graphql_remote` | Интроспекция схемы (REQ-307, REQ-308) | Примитивы → нативные типы, объекты → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | Примитивы → нативные типы, объекты → JSONB |

Ответы API получаются, кешируются в PostgreSQL (настраиваемый TTL) и представляются как типы GraphQL (REQ-309, REQ-318, REQ-327). Кешированные таблицы участвуют в федеративных запросах наравне с любым другим источником (REQ-313).

**Правила JSONB**: сложные столбцы (объекты, массивы), хранящиеся как JSONB, не поддерживают фильтрацию (REQ-119). Доступ к вложенным полям использует извлечение через `->>` в SQL (REQ-151). Связи объявляются между таблицами через скалярные столбцы внешнего ключа — столбцы-блобы JSONB не являются целями соединения (join). Используйте продвижение JSONB (JSONB promotion), чтобы преобразовать вложенные поля в нативные скалярные столбцы, когда требуется фильтрация или соединение по ним (REQ-119).

#### GitHub и GitLab {: #github-and-gitlab }

GitHub и GitLab — фирменные источники на основе `graphql_remote`: каждый появляется в форме Sources под своим именем и регистрируется как удалённый источник GraphQL, запоминающий свой бренд. Форма принимает персональный токен доступа и ничего больше. Конечная точка (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) и схема поставляются вместе с Provisa, поэтому схема при регистрации не интроспектируется. Токен проверяется при добавлении источника. Добавление источника не регистрирует таблиц: каждая таблица, которую предлагает схема, доступна в пространстве имён источника (`gh`, `gl`), а стюард регистрирует нужные. Поле, которое токену читать нельзя, при регистрации таблицы исключается. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

Stripe — фирменный источник на основе `openapi`, построенный так же, как GitHub и GitLab поверх `graphql_remote`. В форме Sources он появляется под именем Stripe в группе Enterprise. Он регистрируется как источник OpenAPI, строка которого записывает бренд в `federation_hints`, а путь спецификации — `brand:stripe`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**Что он охватывает.** REST API Stripe через опубликованную Stripe спецификацию OpenAPI (`stripe/openapi`, `spec3.json`). Спецификация поставляется вместе с Provisa, закреплённой по версии и сжатой gzip, и при регистрации не загружается. Поставляемая — версия API `2026-09-30.endive` с 425 путями. Каждый вызов несёт `Stripe-Version`, равный этой версии, поэтому каждый ответ имеет форму, описанную в спецификации, какую бы версию ни использовал аккаунт по умолчанию. Поля, которые Stripe возвращает, но которых нет в спецификации, добавляются из `stripe.additions.json`. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Как и в любом источнике OpenAPI, каждая операция `GET` — это предлагаемая таблица, а каждая другая операция — предлагаемая команда. Добавление источника не регистрирует ни одну из них: управляющий данными регистрирует нужные. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Аутентификация.** Форма просит одно — токен доступа (`BRAND_AUTH`: `token`). Используйте секретный или ограниченный API-ключ Stripe. Токен отправляется как bearer-токен и один раз проверяется запросом `GET /v1/balance` до добавления источника. Отклонённый токен завершает регистрацию ошибкой `openapi.credential_rejected`; отсутствующий — ошибкой `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Обновление и актуальность.** Таблица Stripe — это реплика, как и любая таблица OpenAPI. Её кешированные ответы живут `cache_ttl` источника, который запрос регистрации устанавливает равным 300 секундам, если не указано иное. Применимы типы проб OpenAPI. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 предусматривает, что пресет может назвать параметр, с помощью которого обновление получает только изменившееся. Бренд Stripe такого параметра не называет, поэтому каждое обновление читает свои таблицы целиком. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Исключения.** Устаревшая платёжная интеграция Stripe с токенами и Sources исключена из поставляемой спецификации. Список приведён в разделе [Исключения в источнике, построенном по спецификации](#exclusions-on-a-specification-built-source). Собственные `exclusions` оператора добавляются к исключениям бренда. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` также присутствует в `SourceType` среди заглушек типов Supabase `wrappers`. Источник Stripe, добавленный через форму, — это источник `openapi`, а не эта заглушка. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Исключения в источнике, построенном по спецификации {: #exclusions-on-a-specification-built-source }

Опубликованная спецификация OpenAPI описывает всё, что принимает удалённый API, включая то, что его поставщик объявил устаревшим и что развёртывание ни в коем случае не должно отправлять. Источник OpenAPI может нести исключения: то, что есть в спецификации, но источник не предлагает. (REQ-1957)

| Исключается | Задаётся | Эффект |
|---|---|---|
| Операция | `method` и `path` | Не предлагается для регистрации как таблица или команда |
| Аргумент запроса | `name`, для одной операции (`method` и `path`) или, при отсутствии обоих, для каждой операции, где он есть | Не входит в аргументы операции, не показывается, никогда не отправляется |

Исключения задаются под `exclusions` в `mapping` источника:

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

Они применяются к спецификации при её загрузке, поэтому список для регистрации, сгенерированные команды и отправляемый запрос видят спецификацию, в которой исключённых элементов не существует. Можно исключить свойство тела, а также параметр запроса, заголовка или cookie; параметр пути исключить нельзя, потому что он часть адреса операции.

Исключение, называющее операцию или аргумент, которых нет в спецификации, приводит к сбою загрузки спецификации этого источника и называет каждую устаревшую запись, так что список, переставший соответствовать более новой спецификации, не остаётся незамеченным.

Фирменный источник поставляется со своими исключениями как частью кураторской настройки, а исключения оператора добавляются к ним. Источник Stripe исключает устаревшую интеграцию платежей Stripe через токены и источники: создание списания (charge), API Tokens и Sources, добавление карты или банковского счёта клиенту из токена, а также аргументы `source`, `card`, `bank_account` и `default_card` в операциях записи клиентов и оплаты счетов. Платежи принимаются через Payment Intents, Payment Methods и Setup Intents, которые источник предлагает. Чтение и удаление платёжных источников, которые у аккаунта уже есть, остаются доступными.


### GovData

Открытые данные правительства США. Доступ разделён по тематическим группам. [tool-verified: `provisa/core/models.py` lines 543–609]

Источнику задаются одна или несколько тем, и каждая тема приносит перечисленные ниже схемы адаптера. Схема может принадлежать более чем одной теме (`census`). Схемы `ref` и `geo` — связующие: справочные данные и география, через которые соединяются таблицы каждой темы, поэтому каждый источник обслуживает их независимо от заданных тем. Список задан один раз на сервере, и тест удерживает его в соответствии со схемами, которые обслуживает закреплённый пакет адаптера. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

| Тема | Открываемые схемы |
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
| каждый источник | `ref`, `geo` |

`ALL` — сокращение подписки для всех тем; в форме Sources такого выбора нет.

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор |
| `subject` | Да | — | Одно из значений темы выше |
| `domain_id` | Да | — | Домен, которому принадлежит этот источник |
| `description` | Нет | `""` | Описание для человека |

Источник AskAmerica, созданный в админке, принимает один учётный параметр — ключ API. Provisa
предъявляет ключ API AskAmerica и получает краткоживущие учётные данные хранилища только для
чтения, с которыми читаются данные; неизвестный или отозванный ключ отклоняется там с указанием
имени. [tool-verified: `provisa/federation/askamerica.py`]

Затем источник — это источник с протоколом Postgres, как источники SharePoint, Salesforce и
облачного инвентаря: Provisa запускает для него встроенный сервер `pgwire-govdata`, передавая ключ и
эти учётные данные в окружение сервера, а каждый движок обращается к этому серверу так же, как к
серверу соседнего источника. DuckDB, Postgres и ClickHouse подключают его и читают его таблицы на
месте, поэтому таблица AskAmerica соединяется с таблицами других источников, лежит под
представлениями и доставляется как любая другая. У Trino для него нет коннектора, и он читает
реплику, выгруженную через сервер: копию таблицы, хранящуюся в хранилище материализации, настолько
свежую, насколько свежо её последнее обновление по настройкам кеша и репликации таблицы, а не
прочитанную во время запроса. Таблица читается из схемы адаптера, которую она называет (`sec`,
`econ` и так далее). [tool-verified: `provisa/federation/pgwire_replica.py`,
`provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

Сервер монтирует свои схемы из объектного хранилища, прежде чем принять подключение, — при первом
запуске это занимает минуты. Он запускается при сохранении источника; пока он не готов, форма
Register Table сообщает, что источник ещё запускается. [tool-verified:
`provisa/federation/pgwire_replica.py` `start_when_saved`]

### Где хранится пароль источника {: #where-a-sources-password-lives }

Пароль источника никогда не хранится рядом с остальными настройками подключения. Строка `sources`
в плоскости управления содержит столбец `password_ref`, хранящий то, что было введено как пароль:
само значение или ссылку, например `${env:PG_PASSWORD}` либо `${secret:SNOWFLAKE_KEY}`. Она
разрешается в момент, когда источник подключается, внутри организации, от имени которой выполняется
запрос. [tool-verified: `provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

Значение без `${…}` используется как есть. `${env:VAR}` читает переменную окружения процесса
развёртывания и не требует привязки. `${secret:NAME}` именует секрет, которым владеет организация,
поэтому он разрешается только в рамках собственных операций этой организации: интроспекционные
точки администрирования и терминал запросов, к которым обращается каждая поверхность, устанавливают
эту привязку. [tool-verified: `provisa/core/secrets.py` `resolve_secrets`;
`provisa/pgwire/_pipeline.py` `_execute_plan`]

Поле хранит то, что вы в него ввели, как бы ни был зарегистрирован источник:

- **Из конфигурации.** Файл — это запись: Provisa копирует значение или ссылку в `password_ref`
  дословно.
- **Из формы Sources.** То, что введено в поле пароля, сохраняется дословно — значение или ссылка.
  Provisa ничего не записывает для него в хранилище организации и не придумывает имя секрета. Чтобы
  не держать учётные данные в строке плоскости управления, создайте секрет сами (см.
  [Секреты](secrets.md)) и введите `${secret:NAME}` в поле. [tool-verified:
  `provisa/api/admin/schema_mutation.py` `create_source`]

Значение, введённое как значение, хранится в базе данных плоскости управления так, как введено.
Повторный ввод пароля заменяет текст поля. Удаление источника убирает его строку и ничего не
убирает из хранилища: созданный вами секрет остаётся вашим. [tool-verified:
`provisa/api/admin/schema_mutation.py` `delete_source`;
`tests/unit/test_no_product_written_secrets.py`]

`password_ref` не переносится между окружениями (REQ-1491). Ветка (branch) или скопированное
окружение предоставляет собственные значения подключения, а хранилище, которое именует ссылка,
принадлежит тому окружению, которое её предоставило. [tool-verified:
`provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Профайлер данных {: #data-profiler }

Профайлер данных — это источник, который хранит только имя, расписание и значения запуска по умолчанию. Он никогда не интроспектируется. К нему добавляются таблицы других источников, и каждый запуск измеряет их и записывает свои строки в плоскость управления организации, где они читаются как таблицы любого другого источника. О создании профайлера и добавлении в него таблиц см. [Тестовые данные](test-data.md). [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)

### Проверки качества данных (REQ-1443) {: #data-quality-checkers-req-1443 }

Проверка качества данных — это тип источника, а не подсистема. Результат её сканирования — это данные: результат проверки — это наблюдение, поэтому он проходит через обычный путь источника и наследует периодичность, свежесть, события, происхождение (lineage), управление (governance), RLS, сетку (grid) и экспорт наравне с любым другим источником. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Поддерживаются два варианта, и выбор — это в той же мере выбор лицензии, что и функции.

| Тип источника | Диалект контракта | Дополнение (extra) | Лицензия | Разрешено в облачной плоскости |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | Soda contract YAML | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Отказано — см. ниже |
| `great_expectations` | Expectation suite JSON | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Разрешено |

Elastic License 2.0 запрещает предоставлять программное обеспечение третьим лицам как хостинговую или управляемую услугу, а запуск Soda внутри плоскости SaaS от имени тенанта — это именно оно. `config/capabilities.yaml` содержит это разделение как `cloud_eligible: false` для опции `soda`, и хостинговая плоскость читает этот флаг. Хостинговое развёртывание, которому нужен Soda, обращается к конечной точке Soda, предоставленной оператором и управляемой им самим. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa ничего не поставляет и не линкует. Сканирование выполняется в дочернем интерпретаторе (`python -m provisa.dq.worker`) — единственном месте, куда импортируется `soda_core` или `great_expectations`, поэтому доступный из исходников проверщик никогда не достигает серверного процесса, а падение проверщика убивает подпроцесс, а не цикл событий. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**Источник указывает на собственную конечную точку pgwire Provisa.** Именно это позволяет одному драйверу postgres проверять таблицу с бэкендом Snowflake или Iceberg: проверщик сканирует федеративное представление, а не базовую систему. Поскольку к этому подключению применяется политика, идентичность сканирования объявляется явно, а не наследуется — отфильтрованный набор строк никогда не должен приводить к молчаливому прохождению проверки.

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

**Одна таблица результатов на контракт, и контракт — это вся регистрация.** Таблица содержит `dq_contract` — текст контракта дословно — и больше ничего о своей структуре. Столбцы, водяной знак и продвижения (promotions) выводятся автоматически. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

Что регистрация выводит из этого текста:

- **Происхождение (lineage).** Контракт уже называет свой целевой набор данных, поэтому регистрация разбирает его так же, как `extract_inputs` разбирает SQL (REQ-939), и разрешает его до управляемой (governed) таблицы. Одно определение, никакой второй копии, способной разойтись. Контракт, называющий неуправляемый набор данных, падает сразу при регистрации, а не загружает строки, которые никто не запрашивал.
- **Столбцы.** Конверт результата принадлежит проверщику, а не оператору — 16 поставляемых столбцов от `scan_id` до `diagnostics`. Объявленные столбцы читаются только ради их `visible_to`, которое должно быть единогласным, а затем заменяются. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Водяной знак.** `scan_time` становится водяным знаком, что делает загрузку добавлением (append) (REQ-982). История сканирований накапливается без отдельной подсистемы истории.
- **Продвижения (promotions).** `freshness_max_timestamp` и `dataset_rows_tested` продвигаются из jsonb-поля `diagnostics` в типизированные столбцы (REQ-119). Добавляйте другие так же, как для любого другого jsonb-столбца. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

Синхронизация во времени не вводит новых полей. `change_signal` вместе с `cache_ttl` задают периодичность опроса; `mv_debounce_quiet` и `mv_debounce_max_delay` сворачивают всплеск на входе в одно сканирование (REQ-963); календарный шаг (grain) делает его периодическим (REQ-962); `expected_events` удерживает сканирование до тех пор, пока его входные данные не станут свежими в пределах окна (REQ-961). Цикл опроса и есть планировщик сканирований.

`outcome` — одно из значений `pass`, `fail`, `warn`, `error`, `skipped`. Ни одно из них не является вердиктом — принудительное применение (enforcement), если требуется, — это отдельное объявление позже: preflight-проверка или материализованное представление поверх загруженных результатов. Поскольку загруженное наблюдение не несёт обязательства детерминизма (REQ-964), здесь допустимы недетерминированные проверки, которые никогда не могли бы стоять на preflight-шлюзе — оценка аномалий, изменение в скользящем окне, свежесть относительно текущего момента.

Контракт создаётся в UI, на панели качества данных экрана редактирования таблицы, и текст контракта там всегда является источником истины. Пробный запуск (dry run) выполняет контракт против живой таблицы и показывает результаты без их загрузки — именно так вы обнаруживаете контракт, имя набора данных которого разрешилось в неожиданное место и иначе загрузил бы только проходящие строки.

---

## Пользовательские коннекторы (REQ-1177) {: #custom-connectors-req-1177 }

Нативные движки федерации — Postgres, DuckDB и ClickHouse — получают достижимость до нового типа источника, когда оператор объявляет для него коннектор в `config/custom_connectors.yaml`. Код не требуется. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Расширяемость коннекторов сама по себе появилась раньше. Движок Trino давно расширяем на собственном уровне — один универсальный JDBC-коннектор, параметризуемый под тип источника, тело каталога `.properties` на тип, и собственные плагины Provisa для коннекторов Trino (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 привносит ту же конфигурационно-управляемую расширяемость в два нативных, безкластерных движка, которые ранее имели фиксированный набор коннекторов.

Конфигурация поставляется пустой. Встроенные коннекторы обеспечивают достижимость «из коробки»; всё в этом файле создаётся оператором. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Установите `PROVISA_CUSTOM_CONNECTORS`, чтобы указать на другой путь (полезно для тестов).

### Виды дескрипторов {: #descriptor-kinds }

| Движок | Вид | Механизм | Что предоставляет дескриптор |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (стандарт ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + представление-сканер | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (автоматически раскрывает каждую удалённую таблицу) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` на таблицу (столбцы из реестра) | `ch_engine`, `engine_template` (может содержать `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse сам выводит схему | `ch_engine`, `engine_template` |

**Postgres универсален.** SQL/MED — это стандарт ISO, поэтому каждый соответствующий ему FDW разделяет одну и ту же форму DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, опционально `CREATE USER MAPPING`, затем либо `IMPORT FOREIGN SCHEMA` (когда `supports_import: true`), либо явный `CREATE FOREIGN TABLE` для каждой таблицы (когда `false`). Дескриптор `pg_fdw` предоставляет только специфичную для конкретного FDW часть — имя расширения, ключи опций сервера, ключи сопоставления пользователей, флаг импорта, опции таблицы. Поэтому любой FDW, соответствующий стандарту, можно настроить только через конфигурацию. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB поддерживает два механизма.** Расширение, раскрывающее каталог через ATTACH, использует `duckdb_attach`; раскрывающее табличную функцию для чтения использует `duckdb_scan`. Расширение, не подходящее ни под один из этих шаблонов, не поддерживается. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse поддерживает три механизма**, по одному на форму движка интеграции: реляционный движок DATABASE, который автоматически раскрывает каждую удалённую таблицу (`clickhouse_database`, например Redis/MySQL), потабличный движок, столбцы которого предоставляет реестр (`clickhouse_table`, например мост JDBC/ODBC — `engine_template` может содержать placeholder `{table}`, который среда выполнения подставляет), и движок файлов/lake/URL, схему которого выводит сам ClickHouse (`clickhouse_scan`, например HDFS/URL). SQLite (движок DATABASE, файл, без сервера) и Hudi (lakehouse, без копирования) поставляются «из коробки». [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Неизвестное значение `kind` приводит к падению сразу при старте — опечатка в дескрипторе не должна молча оставлять тип источника недостижимым. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Проверка доступности (probe gating)

Доступность проверяется в момент подключения по стандартному каталогу обнаружения каждого движка:

- **Postgres** — проверяет `pg_extension`, затем `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — выполняет `INSTALL`/`LOAD` и проверяет `duckdb_functions()` на наличие объявленного `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — проверяет `system.table_engines` на наличие объявленного `ch_engine`; отсутствие в сборке приводит к падению сразу. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Объявленное расширение, которое не устанавливается, приводит к падению сразу. Никакого молчаливого пропуска, никакого запасного варианта (fallback). Коннектор, чья проверка не проходит, просто неактивен для этого развёртывания.

### Переменные шаблона {: #template-variables }

Каждое значение `server_options`, значение `user_mapping`, `attach_template` и `scan_template` может использовать placeholder-ы вида `{field}`. Доступные поля: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, а также любой ключ из `federation_hints`. Шаблоны подключения DuckDB также получают `{alias}` — внутренний псевдоним каталога, который Provisa присваивает подключённой базе данных.

Шаблон, ссылающийся на неизвестное поле, приводит к падению сразу в момент подключения, выявляя несоответствие дескриптора и источника до того, как некорректный DDL достигнет движка.

### Примеры {: #examples }

**Postgres — MongoDB через `mongo_fdw` (без импорта схемы; столбцы предоставляются на уровне таблицы)**

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

**DuckDB — файлы Excel через `read_xlsx` (табличная функция-сканер)**

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

Когда любой из дескрипторов на месте, регистрация источника с объявленным `source_type` направляется через пользовательский коннектор при условии успешной проверки доступности. Никаких других изменений конфигурации не требуется.

---

## Хранилища как именованные источники {: #warehouses-as-named-sources }

Snowflake, Databricks и ClickHouse можно зарегистрировать как именованные источники независимо от того, какой движок федерации активен. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

При регистрации Provisa читает хранилище через DirectDriver источника и создаёт реплику в хранилище материализации активного движка. Затем запрос выполняется против этой реплики. Это отличается от традиционного пути с прямой поддержкой (asyncpg, aiomysql), где движок полностью минуется — здесь движок по-прежнему выполняет запрос, но против локальной реплики, а не через сеть к хранилищу при каждом запросе.

Чтение выполняется в формате Arrow там, где хранилище это поддерживает: Databricks использует Cloud Fetch, Snowflake использует `fetch_arrow_table`, а ClickHouse использует нативный колоночный HTTP-интерфейс.

Расширенные параметры подключения, которые не помещаются в стандартные поля `host`/`port`/`username`/`password`, идут в `federation_hints`:

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

Регистрация в качестве именованного источника не зависит от выбора того же хранилища в качестве движка федерации. Источник Snowflake на движке DuckDB создаёт реплику в DuckDB, а не в Snowflake.

Данные объектных/lake-хранилищ в облаке (файлы parquet, csv, iceberg, delta_lake в S3 / GCS / R2) — это отдельный тип источника, который подключается на месте, когда активный движок имеет ATTACH-коннектор для этого типа. Реплика не создаётся — движок сканирует объектное хранилище напрямую. Учётные данные для таких источников также идут в `federation_hints`:

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

## Поля конфигурации источника {: #source-configuration-fields }

Все источники разделяют общий набор полей. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор; буквенно-цифровой, с дефисами/подчёркиваниями |
| `type` | Да | — | Тип источника (см. таблицы выше) |
| `host` | Нет | `""` | Имя хоста или IP |
| `port` | Нет | `0` | Номер порта |
| `database` | Нет | `""` | Имя базы данных |
| `username` | Нет | `""` | Имя пользователя |
| `password` | Нет | `""` | Пароль; используйте `${env:VAR}` или `${secret:NAME}` вместо буквального значения (см. ниже) |
| `path` | Нет | `null` | Путь к файлу или облачный URI для файловых и объектных/lake-источников |
| `base_url` | Нет | `null` | Базовый URL для источников OpenAPI |
| `pool_min` | Нет | `1` | Минимальный размер пула соединений (REQ-052) |
| `pool_max` | Нет | `5` | Максимальный размер пула соединений (REQ-052) |
| `use_pgbouncer` | Нет | `false` | Направлять соединения через PgBouncer (REQ-053) |
| `pgbouncer_port` | Нет | `6432` | Порт PgBouncer (REQ-053) |
| `cache_enabled` | Нет | `true` | Включить кеширование ответов API |
| `cache_ttl` | Нет | `null` | TTL кеша в секундах; наследует глобальное значение по умолчанию, если null |
| `cache_catalog` | Нет | `null` | Федеративный каталог для кеша API; по умолчанию — собственный каталог источника |
| `cache_schema` | Нет | `api_cache` | Схема внутри каталога кеша |
| `naming_convention` | Нет | `null` | Переопределить глобальное соглашение об именовании для этого источника (REQ-194) |
| `federation_hints` | Нет | `{}` | Параметры сессии, передаваемые движку федерации, и расширенные параметры подключения для источников-хранилищ (REQ-278, REQ-281) |
| `mapping` | Нет | `{}` | Настройки коннектора, специфичные для типа, для NoSQL- и SaaS-источников (например, `auth_type` SharePoint, `auth_type` Salesforce, `use_token` Splunk) (REQ-251) |
| `allowed_domains` | Нет | `[]` | Ограничить источник конкретными доменами; пусто = без ограничений |
| `description` | Нет | `""` | Описание для человека |

---

## Источники Kafka {: #kafka-sources }

Топики Kafka настраиваются отдельно под ключом `kafka_sources`, ключом выступает `id` зарегистрированного источника `kafka`. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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

| Поле | Описание |
| ------- | ------------- |
| `id` | Должен совпадать с `id` источника с `type: kafka` |
| `topics[].id` | Логическое имя этого топика внутри Provisa |
| `topics[].topic` | Имя топика Kafka |
| `topics[].domain_id` | Домен, которому принадлежит этот топик |
| `topics[].description` | Описание для человека |
| `topics[].default_window` | Временное окно по умолчанию для оконных запросов (например, `1h`) (REQ-148) |
| `topics[].columns` | Определения столбцов для схемы топика (REQ-150) |

---

## Видимость столбцов {: #column-visibility }

Поле `visible_to` на каждом столбце — это список ID ролей, которым виден этот столбец. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Столбцы, отсутствующие в списке `visible_to` роли, не появляются в GraphQL-схеме этой роли и не могут запрашиваться или упоминаться в фильтрах (REQ-039).

---

## Связи {: #relationships }

Связи соединяют две зарегистрированные таблицы и появляются как вложенные поля в GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Поле | Обязательно | Описание |
| ------- | ---------- | ------------- |
| `id` | Да | Уникальный идентификатор этой связи |
| `source_table_id` | Да | Таблица, содержащая внешний ключ |
| `target_table_id` | Да | Таблица, на которую ссылаются; пусто для вычисляемых связей |
| `source_column` | Да | Столбец в исходной таблице |
| `target_column` | Да | Столбец в целевой таблице; пусто для вычисляемых связей |
| `cardinality` | Да | `many-to-one` или `one-to-many` (REQ-019) |
| `materialize` | Нет | Автоматически создать материализованное представление для межисточниковых соединений (REQ-158). На связи, опирающейся на связующую таблицу (junction), представление охватывает двухшаговый обход, а не прямое соединение (REQ-1586) |
| `refresh_interval` | Нет | Интервал обновления материализованного представления в секундах (по умолчанию: 300) |
| `target_function_name` | Нет | Имя функции БД для вычисляемых связей |
| `function_arg` | Нет | Какой аргумент функции получает значение исходного столбца |
| `alias` | Нет | Читаемое имя типа связи (например, `WORKS_FOR`) |
| `graphql_alias` | Нет | Именует поле SDL, которое эта связь предоставляет в родительском типе. Если отсутствует, имя выводится из `field_name` целевой таблицы и кардинальности связи. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Нет | Если `true`, исключить эту связь из рёбер графа Cypher |
| `source_json_key` | Нет | Извлечь этот ключ из исходного столбца как JSON-объект перед JOIN |
| `via_table` | Нет | Имя зарегистрированной таблицы связующего звена (junction), через которое проходит это ребро. Указание этого поля делает ребро опирающимся на junction; оставление пустым делает его ребром по внешнему ключу (REQ-1586) |
| `via_source_column` | Нет | Столбец junction, сопоставляемый с `source_column`. Через запятую и позиционно для составного ключа |
| `via_target_column` | Нет | Столбец junction, сопоставляемый с `target_column` |
| `via_type_column` | Нет | Столбец-дискриминатор, когда одна junction-таблица несёт несколько типов связей |
| `via_type_value` | Нет | Значение дискриминатора, к которому привязано это ребро |
| `via_label_source` | Нет | Какая номинация именует тип Cypher: `column` (значение дискриминатора), `table` (имя таблицы junction) или `fixed` (объявленный псевдоним). Все приводятся к верхнему регистру со знаком подчёркивания |

### Связи, опирающиеся на junction {: #junction-backed-relationships }

Ассоциативную таблицу можно объявить как полноценную связь Cypher вместо узла, так что её
собственные столбцы становятся атрибутами этой связи: (REQ-1586)

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

Junction-таблица регистрируется как обычная таблица и должна быть зарегистрирована до того, как связь
сможет её назвать. Объявляйте её по одному разу на каждое значение дискриминатора: три строки над
`pet_companions` дают `BONDED_PAIR`, `LITTERMATE` и `SHARES_ENCLOSURE` как три разных типа Cypher,
каждый из которых несёт оставшиеся столбцы строки junction в качестве свойств ребра. Поставляемая
демо-конфигурация объявляет именно это.

Ребро junction — это связь Cypher, а не поле соединения GraphQL: эмиттер соединений GraphQL строит
свою секцию `ON` для одной пары столбцов и не имеет места для второго шага, поэтому junction-рёбра
исключаются из генерируемого SDL и из `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`]
Junction-таблица остаётся запрашиваемой как собственное корневое поле и удаляется со стороны узлов
графовой схемы Cypher, чтобы никогда не появляться как метка узла.

`materialize: true` работает на ребре junction, и то, что материализуется, — это обход, а не
прямое соединение `pets`-к-`pets`: представление содержит исходный шаг, шаг junction,
дискриминатор и собственные столбцы junction рядом со столбцами цели. Поскольку junction — это
третье звено соединения, пересекает ли ребро источники, оценивается по всем трём таблицам —
junction в источнике, отличном от источника двух связываемых им таблиц, материализуется, даже если
эти две таблицы совпадают. Одно объявление материализует один тип ребра, поэтому представление,
построенное для `bonded pair`, никогда не отвечает на обход `littermate`.

Значения кардинальности [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — каждая строка источника сопоставляется с одной строкой цели (FK к PK)
- `one-to-many` — каждая строка источника сопоставляется с несколькими строками цели (обратное предыдущему)

---

## Правила безопасности на уровне строк {: #row-level-security-rules }

Правила RLS вставляют предложения `WHERE` во время выполнения запроса, привязанные к роли и, опционально, к таблице или домену. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Когда для одной и той же роли существуют и правило на уровне домена, и правило на уровне таблицы, правило на уровне таблицы имеет приоритет (REQ-403).

| Поле | Обязательно | Описание |
| ------- | ---------- | ------------- |
| `table_id` | Условно | Таблица, к которой применяется правило; взаимоисключающе с `domain_id` |
| `domain_id` | Условно | Домен, к которому применяется правило; применяется ко всем таблицам домена (REQ-402) |
| `role_id` | Да | Роль, к которой применяется это правило |
| `filter` | Да | SQL-предикат, вставляемый в `WHERE`; может ссылаться на переменные сессии (REQ-041) |

---

## Функции и вебхуки {: #functions-and-webhooks }

### Функции БД {: #db-functions }

Отслеживайте функцию базы данных и представляйте её как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Источники баз данных также могут автоматически обнаруживать свои хранимые процедуры и функции из каталога поставщика (`pg_proc`, `information_schema.routines` или эквивалентов у других поставщиков), устраняя необходимость вручную регистрировать каждую из них. Обнаружение читает `prokind` и `provolatile`: неизменяемые/стабильные (immutable/stable) функции регистрируются как параметризованные отношения (аргументы процедуры становятся параметрами запроса, в той же форме, что и GET-таблицы OpenAPI), а изменчивые (volatile) процедуры регистрируются как мутации/отслеживаемые функции. Обнаруженные процедуры проходят через управление (governance) на Этапе 2 так же, как и зарегистрированные вручную. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `name` | Да | — | Имя поля GraphQL |
| `source_id` | Да | — | Источник, содержащий функцию |
| `schema` | Нет | `public` | Схема базы данных |
| `function_name` | Да | — | Фактическое имя функции базы данных |
| `returns` | Да | — | ID зарегистрированной таблицы, которую возвращает функция (REQ-207) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` (REQ-211) |
| `visible_to` | Нет | `[]` | Роли, которым назначена функция (пусто: каждая роль). `mutation` дополнительно требует права `write` |
| `domain_id` | Нет | `""` | Домен, которому принадлежит эта функция |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` (REQ-205) |

### Вебхуки {: #webhooks }

Представьте внешнюю HTTP-конечную точку как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `name` | Да | — | Имя поля GraphQL |
| `url` | Да | — | URL конечной точки вебхука |
| `method` | Нет | `POST` | HTTP-метод |
| `timeout_ms` | Нет | `5000` | Таймаут запроса в миллисекундах |
| `returns` | Нет | `null` | ID зарегистрированной таблицы, или null для встроенного типа |
| `inline_return_type` | Нет | `[]` | Список полей `{name, type}` для пользовательских форм возвращаемого значения (REQ-210) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` |
| `visible_to` | Нет | `[]` | Роли, которым разрешено вызывать этот вебхук |
| `domain_id` | Нет | `""` | Домен, которому принадлежит этот вебхук |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` |

---

## Аутентификация {: #authentication }

Аутентификация настраивается под ключом `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Провайдер | Описание |
| ---------- | ------------- |
| `none` | Без аутентификации; все запросы обрабатываются как `default_role` |
| `firebase` | Firebase Authentication; требует `project_id` и `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | Универсальный OAuth 2.0 (REQ-123) |
| `simple` | Имя пользователя/пароль без внешнего провайдера (REQ-124) |

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

`assignments_source: claims` читает назначения ролей из утверждений (claims) JWT. `assignments_source: provisa` читает их из собственного хранилища назначений Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Маршрутизация выполнения {: #execution-routing }

**Прямое выполнение** — однопоточные запросы к RDBMS направляются к нативному драйверу для задержки менее 100 мс (REQ-027). Для поддержки этого пути источникам требуются как запись в `SOURCE_TO_DIALECT`, так и запись в `SOURCE_TO_CONNECTOR` (REQ-229).

**Федеративное выполнение** — многопоточные запросы и источники без прямого драйвера направляются через движок федерации (REQ-028). Provisa включает встроенный движок федерации; для крупномасштабных развёртываний укажите на собственный совместимый кластер (REQ-226).

**Статистика** — при регистрации Provisa выполняет `ANALYZE` для каждой опубликованной таблицы, чтобы подготовить оптимизатор на основе стоимости (количество строк, доля null-значений, число различных значений, min/max). Ошибки логируются и не блокируют регистрацию (REQ-275).

---

## Графовые и семантические источники {: #graph-semantic-sources }

### Neo4j

Зарегистрируйте графовую базу данных Neo4j как запрашиваемый источник. Стюарды пишут запросы Cypher, проецирующие скалярные значения; Provisa кеширует результаты и представляет их как типы GraphQL (REQ-295).

Запросы Cypher должны использовать акцессоры свойств в предложении `RETURN` (`RETURN n.id AS id, n.name AS name`) — возврат объектов узлов отклоняется в момент регистрации (REQ-296).

#### Регистрация через файл конфигурации (REQ-1668) {: #config-file-registration-req-1668 }

Объявите источник `neo4j` и его таблицы в YAML. Каждая таблица требует `query_template` (Cypher-запрос, порождающий её строки) и типизированные столбцы. Ключ `query_template` недопустим для любого другого типа источника. [tool-verified: `provisa/core/config_loader.py:456-511`]

Источнику требуются `host`, `port` и `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Строки получаются через POST-запрос `{"statements": [{"statement": <cypher>}]}` к `/db/<database>/tx/commit` (HTTP transaction API Neo4j). Ответ с непустым списком `errors` трактуется как неудавшийся запрос, а не как пустой результат. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Каждый столбец требует `data_type`. Загрузчик сопоставляет типы конфигурации с типом столбца API, используемым во время запроса [tool-verified: `provisa/neo4j/persist.py:31-64`]:

| Тип `data_type` конфигурации | Тип API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` и `decimal(10,2)` принимаются — используется базовый тип до скобки.

Регистрация сохраняет строку `api_sources` и по одной строке `api_endpoints` на таблицу, поэтому таблицы переживают перезапуск без повторного чтения файла. REST-конечные точки администратора под `/admin/sources/neo4j` записывают те же строки. [tool-verified: `provisa/neo4j/persist.py:77-120`]

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

#### Регистрация таблицы в UI (REQ-1670) {: #register-table-in-the-ui-req-1670 }

У источника neo4j нет таблиц для перечисления, поэтому форма регистрации таблицы запрашивает саму таблицу вместо того, чтобы предлагать её на выбор. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Выберите источник neo4j и домен. Селекторы схемы и таблицы, флажок обнаружения и селектор водяного знака не отображаются; источник никогда не интроспектируется.
2. Введите имя таблицы и Cypher-запрос. Cypher должен проецировать скаляры (`RETURN a.name AS name`); проекция, возвращающая узел или список, сообщается как ошибка.
3. Нажмите Preview. Форма выполняет Cypher с `LIMIT 5` через GraphQL-запрос `neo4jPreview` и заполняет список столбцов из полученных строк, типизированных как `text`, `integer`, `double`, `boolean` или `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Неудачный предпросмотр оставляет Cypher в редакторе и показывает сообщение.
4. Настройте видимость, псевдонимы или маскирование как для любой таблицы, затем зарегистрируйте. Форма отказывается отправляться до тех пор, пока предпросмотр не типизировал столбцы, а сервер отказывается регистрировать таблицу neo4j без Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

Cypher хранится вместе с таблицей как `queryTemplate`, отображается в представлении чтения таблицы и сохраняется точно так же, как при регистрации через файл конфигурации: строка `api_sources` и строка `api_endpoints`, которые гидратируются при следующем запуске. Редактирование таблицы заново сохраняет отредактированный Cypher.

#### Регистрация через admin REST {: #admin-rest-registration }

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

Конечная точка предпросмотра (`POST /admin/sources/neo4j/{id}/preview`) возвращает примеры строк и блокирует регистрацию, если Cypher возвращает объекты узлов (REQ-296).

### SPARQL

Зарегистрируйте любое совместимое с SPARQL 1.1 хранилище триплетов (Apache Jena Fuseki, Virtuoso, Stardog и т. д.) как запрашиваемый источник (REQ-297).

Запросы должны быть запросами `SELECT`. Имена переменных в предложении `SELECT` автоматически становятся именами столбцов (REQ-297).

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

Оба коннектора используют конвейер кеша API-источника — результаты хранятся в PostgreSQL с настраиваемым TTL, что делает их доступными для межисточниковых федеративных JOIN (REQ-295, REQ-297, REQ-299).

---

#### Регистрация через файл конфигурации и через UI (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

`host` источника `sparql` — это URL его конечной точки SPARQL (форма Sources хранит его так же). Каждая таблица под ним содержит `query_template` — SELECT-запрос, чьи переменные становятся столбцами; каждая привязка (binding) имеет тип `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

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

Регистрация таблицы работает так же, как для Neo4j: выберите источник, введите имя таблицы и SELECT-запрос, нажмите Preview (запрос `sparqlPreview` выполняет его с `LIMIT 5` и заполняет список столбцов), затем зарегистрируйте. Регистрация сохраняет строку `api_sources` и строку `api_endpoints` (POST в форме form-encoded на путь конечной точки, нормализатор `sparql_bindings`) — те же строки, что записывает регистрация через файл конфигурации, а нативный движок загружает строки через ту же цепочку получения данных, что и Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

## Примеры подключения {: #connection-examples }

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

### Межисточниковый запрос {: #cross-source-query }

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

Однопоточные части маршрутизируются напрямую (REQ-027). Межисточниковые JOIN федерируются с автоматическим приведением типов (REQ-028, REQ-552).
