# Типы источников
## Модель выполнения
Любой запрос в итоге выполняется через федеративный движок, обеспечивающий федерацию по всем источникам. Источники делятся на три категории по способу подключения. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Категория | Есть прямой драйвер | Есть федеративный коннектор | Примеры |
| --- | --- | --- | --- |
| **С прямым доступом** | Да | Да | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Только федерация** | Нет | Да | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (на базе S3) |
| **Прямое чтение (реплика)** | Да | Да | Snowflake, Databricks, ClickHouse — драйвер читает данные и создаёт реплику; запросы выполняются к реплике в активном движке |
| **Материализация → федерация** | Нет | Нет | REST/OpenAPI, удалённый GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (приёмник push-событий), GovData, SharePoint, Splunk |

Источники **с прямым доступом** выполняют односточниковые запросы через собственный драйвер (менее 100 мс), минуя федеративный движок (REQ-027, REQ-229). При этом они сохраняют полную поддержку коннектора и участвуют в федерации при соединении с другими источниками (REQ-028).

Источники **только с федерацией** всегда опрашиваются через уровень федерации. Прямого драйвера не существует (REQ-229).

Источники с **прямым чтением (репликой)** имеют DirectDriver, который читает из хранилища нативно (в формате Arrow, где это доступно), создаёт реплику в хранилище материализации активного движка, и далее запросы выполняются к этой реплике. См. [Хранилища данных как именованные источники](#warehouses-as-named-sources).

Источники с **материализацией** не имеют федеративного коннектора. Provisa получает их данные (при запуске или во время запроса) и кеширует их как Parquet в S3 или в PostgreSQL, делая их доступными федеративному движку для межисточниковых запросов (REQ-309).

---

## Все источники
Provisa регистрирует **53** типа источников. Таблицы ниже охватывают все 53; номер — это порядковый счёт. [tool-verified: `provisa/core/models.py` `SourceType`]

| № | Группа | Типы источников |
| --- | --- | --- |
| 1–13 | [РСУБД](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Облачные хранилища данных](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Аналитика и OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Озеро данных и открытые табличные форматы](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Потоковая передача](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Приёмник push-событий](#push-receiver) | `ingest` |
| 38–39 | [Графовые и семантические](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [Файловые](#file-based) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Наблюдаемость и прочее](#observability-other) | `google_sheets`, `prometheus` |
| 46–47 | [Корпоративные SaaS](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [Источники API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Проверки качества данных](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |

Справочник по всем типам источников, поддерживаемым Provisa. «Прямой драйвер» означает, что односточниковые запросы выполняются к источнику нативно (менее 100 мс) (REQ-027). «Имя коннектора» — федеративный коннектор, используемый, когда источник участвует в межисточниковых JOIN (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### РСУБД {#rdbms}
| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Да |
| `mysql` | aiomysql | mysql | mysql | Да |
| `mariadb` | aiomysql | mariadb | mysql | Да |
| `singlestore` | — | singlestore | singlestore | Федеративно |
| `sqlserver` | aioodbc | sqlserver | tsql | Да |
| `oracle` | oracledb | oracle | oracle | Да |
| `duckdb` | duckdb | memory | duckdb | Да |
| `cockroachdb` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `yugabytedb` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `greenplum` | asyncpg (протокол pg) | postgresql | postgres | Да |
| `tidb` | aiomysql (протокол mysql) | mysql | mysql | Да |
| `firebird` | — | — (расширение DuckDB) | — | Нет |
| `airport` | — | — (расширение DuckDB) | — | Нет |

Базы данных, совместимые по проводному протоколу, переиспользуют JDBC-драйвер, нативный асинхронный драйвер и диалект базового протокола: CockroachDB, YugabyteDB и Greenplum используют протокол PostgreSQL, TiDB — протокол MySQL. Им нужны только записи в реестре, без нового кода коннектора. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) и `airport` (сервер Arrow Flight) — зарегистрированные типы источников, к которым обращаются на месте через расширения сообщества DuckDB, когда активным движком является DuckDB: ни прямого драйвера, ни федеративного коннектора. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Облачные хранилища данных {#cloud-data-warehouses}
[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Федеративно | Читает через snowflake-connector-python; создаёт реплику; `account`/`warehouse`/`role` в `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Федеративно | Нет DirectDriver; доступен через федеративный движок или ATTACH движка BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Федеративно | Читает через databricks-sql-connector (Cloud Fetch, Arrow); создаёт реплику; в `federation_hints` обязателен `http_path` (REQ-987) |
| `redshift` | — | redshift | redshift | Федеративно | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Федеративно | Microsoft Fabric Warehouse; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Федеративно | Azure Synapse SQL; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Федеративно | Чтение с удалённого координатора Trino/Presto через диалект trino для SQLAlchemy; создаёт реплику на любом движке (REQ-994) |

### Аналитика и OLAP {#analytics-olap}
[tool-verified: `executor/drivers/clickhouse.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Федеративно | Читает через clickhouse-connect (HTTP); для TLS — `secure: "true"` в `federation_hints` (REQ-986) |
| `druid` | — | druid | druid | Нет | — |
| `exasol` | — | exasol | exasol | Нет | — |
| `elasticsearch` | — | elasticsearch | — | Нет | Свойства коннектора берутся из DSL-описания сопоставления для этого типа [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Нет | Коннектор Trino `pinot`; `pinot.controller-urls` = host:port контроллера Pinot [tool-verified: `trino_connectors.py:199`] |

### Озеро данных и открытые табличные форматы {#data-lake-open-table-formats}
Эти типы источников доступны только через федерацию — ни прямого драйвера, ни диалекта. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Тип источника | Имя коннектора | Путешествие во времени | Примечания |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Да (аргумент `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Да (аргумент `as_of`, REQ-372) | — |
| `hive` | hive | Нет | — |
| `hudi` | — (движок ClickHouse `Hudi`, без копирования — REQ-1178) | Нет | Федеративного коннектора нет; доступен на месте, когда активным движком является ClickHouse |
| `hive_s3` | hive | Нет | Hive на базе S3 |

### NoSQL

Для `mongodb`, `cassandra` и `redis` есть коннекторы Trino (`redis` строит свои свойства из DSL-описания сопоставления для этого типа). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Нет |
| `cassandra` | cassandra | Нет |
| `redis` | redis | Нет |

### Потоковая передача {#streaming}
| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `kafka` | Федеративный коннектор Kafka; схема через Confluent Schema Registry (Avro, Protobuf, JSON Schema), ручное определение или вывод по образцу (REQ-147, REQ-150) | Только приёмник (REQ-176) |
| `websocket` | Внешний поток WebSocket — подключение, подписка, приём событий; результаты материализуются (REQ-338) | Нет |
| `rss` | Лента RSS 2.0 / Atom — опрос, отметка по pubDate/updated; результаты материализуются (REQ-342, REQ-343) | Нет |

### Приёмник push-событий {#push-receiver}
| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `ingest` | Внешние службы отправляют события JSON методом POST; результаты материализуются (REQ-331, REQ-335) | Нет |

### Графовые и семантические {#graph-semantic}
| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher через HTTP API, результаты кешируются в PostgreSQL (REQ-295) | Нет |
| `sparql` | SPARQL 1.1 методом POST, результаты кешируются в PostgreSQL (REQ-297) | Нет |

### Файловые {#file-based}
Файлы охватываются двумя механизмами. Оба используют поле `path` вместо `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Однофайловые источники** — `sqlite`, `csv`, `parquet` указывают `path` на один файл.

| Тип источника | Транспорты | Мутации |
| --- | --- | --- |
| `sqlite` | локальный | Да |
| `csv` | локальный | Нет |
| `parquet` | локальный, `s3://` | Нет |

Для закрытых бакетов нужны учётные данные (регион и ключи AWS из окружения). Для CSV поверх `s3://` или `http(s)://`, а также чтобы зарегистрировать сразу много файлов, используйте источник `files`. [tool-verified: `provisa/file_source/source.py`]

**Источник `files`** — указывает `path` на шаблон-глоб, рекурсивно обходит его и регистрирует каталог как федеративный каталог таблиц. Он читает множество форматов через множество транспортов; наборы ниже взяты из файлового коннектора (форк kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Форматы | Транспорты |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, а также документы, преобразуемые в таблицы, — HTML, Markdown, DOCX, PPTX | Локальная файловая система, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST и Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Наблюдаемость и прочее {#observability-other}
Для `prometheus` есть коннектор Trino (свойства строятся из DSL-описания сопоставления для этого типа). `google_sheets` — зарегистрированный тип источника без коннектора Trino; он материализуется через конвейер кеша API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (материализуется) | Нет |
| `prometheus` | prometheus | Нет |

### Коннекторы корпоративных SaaS {#enterprise-saas-connectors}
SharePoint и Splunk регистрируются через коннекторы Apache Calcite (форк kenstott/calcite). Прямого драйвера нет ни у одного из них — Provisa материализует их строки, запуская встроенный в коннектор сервер pgwire для Calcite (`pgwire-sharepoint`, `pgwire-splunk`), подключаясь к нему как к обычной конечной точке PostgreSQL и складывая строки в хранилище материализации для федерации (REQ-954). Оба коннектора всегда включают сопоставление имён без учёта регистра, соответствующее собственной регистронезависимой семантике каждого продукта (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Списки SharePoint перечисляются как схемы и выставляются как запрашиваемые таблицы (REQ-726, REQ-731). Два способа аутентификации: `CLIENT_CREDENTIALS` (по умолчанию) и по сертификату через PFX-сертификат (REQ-727). Значения секретов в `mapping` разрешаются через движок секретов до того, как попадут в коннектор (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` или `host` | `site-url` | URL сайта SharePoint |
| `username` | `client-id` | Идентификатор клиента приложения Azure |
| `password` | `client-secret` | Секрет клиента приложения Azure |
| `database` | `tenant-id` | UUID арендатора Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (по умолчанию) или `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Путь к PFX, когда `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Пароль PFX |

Если коннектор не предоставляет `information_schema.columns`, зарегистрируйте таблицу с явными определениями столбцов (полученными из Microsoft Graph API) через мутацию `registerTable` (REQ-732).

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

Результаты поиска Splunk доступны как таблицы (например, `internal_server`) (REQ-721). URL коннектора берётся из `base_url` либо собирается как `https://{host}:{port}` с портом по умолчанию `8089` (REQ-722). Аутентификация: когда `mapping.use_token` равно `true` (по умолчанию), `password` передаётся как API-токен; когда `false`, `username` и `password` передаются как отдельные учётные данные (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, иначе `https://host:port` (порт по умолчанию 8089) |
| `password` | `token` или `password` | токен, когда `use_token: true` |
| `username` | `user` | только когда `use_token: false` |
| `database` | `app` | ограничить приложением Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | фильтр по модели данных |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | для самоподписанных сертификатов (REQ-724) |

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

### Источники API {#api-sources}
Зарегистрируйте любую конечную точку HTTP как запрашиваемую таблицу. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Тип API | Обнаружение | Вывод столбцов |
| --------- | ----------- | ----------------- |
| `openapi` | Разбор спецификации OpenAPI (REQ-314, REQ-316) | Примитивы → нативные, объекты → JSONB |
| `graphql_remote` | Интроспекция схемы (REQ-307, REQ-308) | Примитивы → нативные, объекты → JSONB |
| `grpc_remote` | Рефлексия сервера (REQ-322, REQ-325) | Примитивы → нативные, объекты → JSONB |

Ответы API загружаются, кешируются в PostgreSQL (с настраиваемым TTL) и выставляются как типы GraphQL (REQ-309, REQ-318, REQ-327). Кешированные таблицы участвуют в федеративных запросах наравне с любым другим источником (REQ-313).

**Правила JSONB**: сложные столбцы (объекты, массивы), хранимые как JSONB, не фильтруются (REQ-119). Доступ к вложенным полям выполняется извлечением `->>` в SQL (REQ-151). Связи объявляются между таблицами по скалярным столбцам внешнего ключа — столбцы-блобы JSONB не являются целями соединения. Используйте продвижение JSONB, чтобы превратить вложенные поля в нативные скалярные столбцы, когда по ним нужно фильтровать или соединять (REQ-119).

### GovData

Открытые данные правительства США. Доступ разделён по тематическим группам. [tool-verified: `provisa/core/models.py` lines 543–609]

Каждый источник `govdata` выбирает одну тему. Тема определяет, какие схемы GovData выставляются. Схемы `ref` и `geo` включаются всегда как связующие схемы — они не перечисляются по темам, но присутствуют всегда. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Тема | Выставляемые схемы |
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
| `ALL` | Все схемы выше |

```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Поле | Обязательное | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор |
| `subject` | Да | — | Одно из значений темы выше |
| `domain_id` | Да | — | Домен, которому принадлежит источник |
| `description` | Нет | `""` | Понятное человеку описание |

### Проверки качества данных (REQ-1443) {#data-quality-checkers-req-1443}
Проверка качества данных — это тип источника, а не подсистема. Результат её сканирования — это данные: результат проверки есть наблюдение, поэтому он попадает в систему по обычному пути источника и наследует периодичность, свежесть, события, происхождение, управление, RLS, сетку и экспорт от всех остальных источников. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Поддерживаются две, и выбор здесь не столько функциональный, сколько лицензионный.

| Тип источника | Диалект контракта | Extra | Лицензия | Размещённая облачная плоскость |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML-контракт Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Запрещено — см. ниже |
| `great_expectations` | JSON-набор ожиданий | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Разрешено |

Elastic License 2.0 запрещает предоставлять программное обеспечение третьим лицам как размещённую или управляемую услугу, а запуск Soda внутри SaaS-плоскости от имени арендатора — ровно это. `config/capabilities.yaml` фиксирует это разделение как `cloud_eligible: false` у варианта `soda`, и размещённая плоскость читает этот признак. Размещённое развёртывание, которому нужна Soda, обращается к конечной точке Soda, которую оператор поднимает сам. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa ничего не встраивает и ни с чем не компонуется. Сканирование выполняется в дочернем интерпретаторе (`python -m provisa.dq.worker`), который остаётся единственным местом импорта `soda_core` или `great_expectations`, так что проверка с открытым исходным кодом ограниченной лицензии никогда не попадает в серверный процесс, а её падение убивает подпроцесс, а не цикл событий. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**Источник указывает на собственную конечную точку pgwire в Provisa.** Именно это позволяет одному драйверу postgres проверять таблицу на базе Snowflake или Iceberg: проверка сканирует федеративное представление, а не нижележащую систему. Поскольку к этому соединению применяется политика, идентичность сканирования объявляется явно, а не наследуется, — отфильтрованный набор строк никогда не должен давать молча проходящую проверку.

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

**Одна таблица результатов на контракт, и контракт составляет всю регистрацию.** Таблица несёт `dq_contract` — текст контракта дословно — и ничего больше о своей форме. Столбцы, отметка и продвижения выводятся автоматически. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

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

- **Происхождение данных.** Контракт уже называет свой целевой набор данных, поэтому регистрация разбирает его так же, как `extract_inputs` разбирает SQL (REQ-939), и сопоставляет с управляемой таблицей. Одно определение, без второй копии, которая может разойтись. Контракт, называющий неуправляемый набор данных, громко падает при регистрации, а не складывает строки, которых никто не просил.
- **Столбцы.** Оболочка результата принадлежит проверке, а не оператору, — 16 поставляемых столбцов от `scan_id` до `diagnostics`. Объявленные столбцы читаются только ради их `visible_to`, который должен быть единогласным, после чего они заменяются. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Отметка.** `scan_time` становится отметкой, что делает запись добавлением (REQ-982). История сканирований накапливается без отдельной подсистемы истории.
- **Продвижения.** `freshness_max_timestamp` и `dataset_rows_tested` продвигаются из jsonb-поля `diagnostics` в типизированные столбцы (REQ-119). Добавляйте новые так же, как для любого другого столбца jsonb. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

Тайминг не вводит новых полей. `change_signal` вместе с `cache_ttl` задают частоту опроса; `mv_debounce_quiet` и `mv_debounce_max_delay` сворачивают всплеск изменений выше по потоку в одно сканирование (REQ-963); календарная гранулярность делает его периодическим (REQ-962); `expected_events` удерживает сканирование, пока его входные данные не станут свежими на всём окне (REQ-961). Цикл опроса и есть планировщик сканирований.

`outcome` принимает одно из значений `pass`, `fail`, `warn`, `error`, `skipped`. Ни одно из них не является приговором — принуждение, если оно нужно, объявляется отдельно и позже: предполётной проверкой или материализованным представлением над сложенными результатами. Поскольку сложенное наблюдение не несёт обязательства детерминизма (REQ-964), здесь допустимы недетерминированные проверки, которые никогда не могли бы стоять на предполётном шлюзе, — оценка аномалии, изменение по скользящему окну, свежесть относительно текущего момента.

Контракт создаётся в интерфейсе, на панели качества данных в форме редактирования таблицы, и сырой текст контракта там всегда является источником истины. Пробный прогон выполняет контракт против живой таблицы и показывает результаты, не сохраняя их, — именно так вы поймаете контракт, чьё имя набора данных разрешилось не туда и который иначе сложил бы только проходящие строки.

---

## Собственные коннекторы (REQ-1177)
Нативные федеративные движки — Postgres, DuckDB и ClickHouse — получают доступ к новому типу источника, когда оператор объявляет для него коннектор в `config/custom_connectors.yaml`. Код не требуется. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Сама расширяемость коннекторов появилась раньше. Движок Trino давно расширяем на своём уровне — один универсальный коннектор JDBC, параметризуемый по типу источника, тело каталога `.properties` на каждый тип и собственные подключаемые модули коннекторов Trino от Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 приносит ту же расширяемость через конфигурацию двум нативным бескластерным движкам, у которых прежде был фиксированный набор коннекторов.

Конфигурация поставляется пустой. Встроенные коннекторы покрывают доступность «из коробки»; всё в этом файле пишет оператор. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Задайте `PROVISA_CUSTOM_CONNECTORS`, чтобы указать другой путь (удобно для тестов).

### Виды дескрипторов
| Движок | Вид | Механизм | Что задаёт дескриптор |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (стандарт ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + представление-сканер | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (автоматически выставляет каждую удалённую таблицу) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` на таблицу (столбцы из реестра) | `ch_engine`, `engine_template` (может содержать `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse выводит схему сам | `ch_engine`, `engine_template` |

**Postgres универсален.** SQL/MED — стандарт ISO, поэтому любой соответствующий ему FDW имеет одну и ту же форму DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, необязательный `CREATE USER MAPPING`, а затем либо `IMPORT FOREIGN SCHEMA` (когда `supports_import: true`), либо явный `CREATE FOREIGN TABLE` на каждую таблицу (когда `false`). Дескриптор `pg_fdw` задаёт только то, что различается между FDW, — имя расширения, ключи параметров сервера, ключи сопоставления пользователей, признак импорта, параметры таблицы. Поэтому любым FDW, соответствующим стандарту, можно управлять из одной конфигурации. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB поддерживает два механизма.** Расширение, выставляющее каталог через ATTACH, использует `duckdb_attach`; расширение, выставляющее табличную функцию чтения, — `duckdb_scan`. Расширение, не подходящее ни под один из этих шаблонов, не поддерживается. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse поддерживает три механизма** — по одному на форму интеграционного движка: реляционный движок DATABASE, автоматически выставляющий каждую удалённую таблицу (`clickhouse_database`, например Redis/MySQL), потабличный движок, чьи столбцы берутся из реестра (`clickhouse_table`, например мост JDBC/ODBC — `engine_template` может содержать заполнитель `{table}`, который среда выполнения подставляет), и файловый/озёрный/URL-движок, чью схему ClickHouse выводит сам (`clickhouse_scan`, например HDFS/URL). SQLite (движок DATABASE, файл, без сервера) и Hudi (озеро-хранилище, без копирования) поставляются «из коробки». [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Неизвестное значение `kind` громко падает при запуске — опечатка в дескрипторе не должна молча оставить тип источника недостижимым. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Проверка доступности
Доступность проверяется в момент подключения по стандартному каталогу обнаружения каждого движка:

- **Postgres** — проверяет `pg_extension`, затем `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — выполняет `INSTALL`/`LOAD` и проверяет `duckdb_functions()` на объявленный `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — проверяет `system.table_engines` на объявленный `ch_engine`; отсутствие в сборке громко падает. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Объявленное расширение, которое невозможно установить, громко падает. Ни молчаливого пропуска, ни запасного варианта. Коннектор, чья проверка не прошла, просто не активен в этом развёртывании.

### Переменные шаблона
Любое значение в `server_options`, любое значение в `user_mapping`, `attach_template` и `scan_template` может использовать заполнители `{field}`. Доступные поля: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, а также любой ключ из `federation_hints`. Шаблоны attach для DuckDB получают ещё и `{alias}` — внутренний псевдоним каталога, который Provisa назначает подключённой базе данных.

Шаблон, ссылающийся на неизвестное поле, громко падает в момент подключения, обнаруживая расхождение между дескриптором и источником до того, как некорректный DDL дойдёт до движка.

### Примеры
**Postgres — MongoDB через `mongo_fdw` (без импорта схемы; столбцы задаются на каждую таблицу)**

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

**DuckDB — файлы Excel через `read_xlsx` (табличная функция сканирования)**

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

Когда любой из этих дескрипторов на месте, регистрация источника с объявленным `source_type` направляется через собственный коннектор при условии успешной проверки. Никаких других изменений конфигурации не нужно.

---

## Хранилища данных как именованные источники {#warehouses-as-named-sources}
Snowflake, Databricks и ClickHouse можно зарегистрировать как именованные источники независимо от того, какой федеративный движок активен. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

После регистрации Provisa читает хранилище через DirectDriver этого источника и складывает реплику в хранилище материализации активного движка. Затем запрос выполняется к этой реплике. Это отличается от классического пути с прямым доступом (asyncpg, aiomysql), где движок обходится полностью: здесь движок по-прежнему выполняет запрос, но по локальной реплике, а не по сети к хранилищу на каждый запрос.

Чтения выполняются нативно в формате Arrow там, где хранилище это поддерживает: Databricks использует Cloud Fetch, Snowflake — `fetch_arrow_table`, а ClickHouse — нативный колоночный интерфейс HTTP.

Расширенные параметры подключения, которые не помещаются в стандартные поля `host`/`port`/`username`/`password`, задаются в `federation_hints`:

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

Регистрация в качестве именованного источника не зависит от выбора того же хранилища в роли федеративного движка. Источник Snowflake на движке DuckDB складывает реплику в DuckDB, а не в Snowflake.

Данные в облачных объектных хранилищах и озёрах (файлы parquet, csv, iceberg, delta_lake на S3 / GCS / R2) — это отдельный тип источника, который подключается на месте, когда у активного движка есть коннектор ATTACH для этого типа. Реплика не создаётся — движок сканирует объектное хранилище напрямую. Учётные данные для таких источников тоже задаются в `federation_hints`:

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

## Поля конфигурации источника
У всех источников есть общий набор полей. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Поле | Обязательное | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор; буквы и цифры с дефисами и подчёркиваниями |
| `type` | Да | — | Тип источника (см. таблицы выше) |
| `host` | Нет | `""` | Имя узла или IP |
| `port` | Нет | `0` | Номер порта |
| `database` | Нет | `""` | Имя базы данных |
| `username` | Нет | `""` | Имя пользователя |
| `password` | Нет | `""` | Пароль; используйте `${env:VAR}` для разрешения секретов |
| `path` | Нет | `null` | Путь к файлу или облачный URI для файловых и объектных/озёрных источников |
| `base_url` | Нет | `null` | Базовый URL для источников OpenAPI |
| `pool_min` | Нет | `1` | Минимальный размер пула соединений (REQ-052) |
| `pool_max` | Нет | `5` | Максимальный размер пула соединений (REQ-052) |
| `use_pgbouncer` | Нет | `false` | Направлять соединения через PgBouncer (REQ-053) |
| `pgbouncer_port` | Нет | `6432` | Порт PgBouncer (REQ-053) |
| `cache_enabled` | Нет | `true` | Включить кеширование ответов API |
| `cache_ttl` | Нет | `null` | TTL кеша в секундах; при null наследует глобальное значение по умолчанию |
| `cache_catalog` | Нет | `null` | Федеративный каталог для кеша API; по умолчанию — собственный каталог источника |
| `cache_schema` | Нет | `api_cache` | Схема внутри каталога кеша |
| `naming_convention` | Нет | `null` | Переопределить глобальное соглашение об именовании для этого источника (REQ-194) |
| `federation_hints` | Нет | `{}` | Свойства сессии, передаваемые федеративному движку, и расширенные параметры подключения для источников-хранилищ (REQ-278, REQ-281) |
| `mapping` | Нет | `{}` | Настройки коннектора, зависящие от типа, для источников NoSQL и SaaS (например, `auth_type` у SharePoint, `use_token` у Splunk) (REQ-251) |
| `allowed_domains` | Нет | `[]` | Ограничить источник конкретными доменами; пусто = без ограничений |
| `description` | Нет | `""` | Понятное человеку описание |

---

## Источники Kafka
Топики Kafka настраиваются отдельно в разделе `kafka_sources`, с ключом по `id` зарегистрированного источника типа `kafka`. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `topics[].domain_id` | Домен, которому принадлежит топик |
| `topics[].description` | Понятное человеку описание |
| `topics[].default_window` | Окно времени по умолчанию для оконных запросов (например, `1h`) (REQ-148) |
| `topics[].columns` | Определения столбцов для схемы топика (REQ-150) |

---

## Видимость столбцов
Поле `visible_to` у каждого столбца — список идентификаторов ролей, которые могут видеть этот столбец. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Столбцы, не попавшие в список `visible_to` роли, не появляются в схеме GraphQL для этой роли и не могут быть запрошены или использованы в фильтрах (REQ-039).

---

## Связи
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

| Поле | Обязательное | Описание |
| ------- | ---------- | ------------- |
| `id` | Да | Уникальный идентификатор этой связи |
| `source_table_id` | Да | Таблица, содержащая внешний ключ |
| `target_table_id` | Да | Таблица, на которую ссылаются; пусто для вычисляемых связей |
| `source_column` | Да | Столбец исходной таблицы |
| `target_column` | Да | Столбец целевой таблицы; пусто для вычисляемых связей |
| `cardinality` | Да | `many-to-one` или `one-to-many` (REQ-019) |
| `materialize` | Нет | Автоматически создавать материализованное представление для межисточниковых соединений (REQ-158). У ребра через junction представление покрывает обход в два перехода, а не прямое соединение (REQ-1586) |
| `refresh_interval` | Нет | Интервал обновления материализованного представления в секундах (по умолчанию: 300) |
| `target_function_name` | Нет | Имя функции БД для вычисляемых связей |
| `function_arg` | Нет | Какой аргумент функции получает значение исходного столбца |
| `alias` | Нет | Понятный человеку тип связи (например, `WORKS_FOR`) |
| `graphql_alias` | Нет | Задаёт имя поля SDL, которое эта связь выставляет на родительском типе. Когда не задано, имя выводится из `field_name` целевой таблицы и кардинальности связи. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Нет | Когда `true`, исключить эту связь из рёбер графа Cypher |
| `source_json_key` | Нет | Извлечь этот ключ из исходного столбца как объект JSON перед JOIN |
| `via_table` | Нет | Имя зарегистрированной таблицы junction, через которую проходит это ребро. Заполнение делает ребро junction-ребром; пустое значение оставляет его ребром на внешнем ключе (REQ-1586) |
| `via_source_column` | Нет | Столбец junction, парный к `source_column`. Для составного ключа — через запятую и позиционно |
| `via_target_column` | Нет | Столбец junction, парный к `target_column` |
| `via_type_column` | Нет | Столбец-дискриминатор, когда одна junction несёт несколько типов связей |
| `via_type_value` | Нет | Значение дискриминатора, за которым закреплено это ребро |
| `via_label_source` | Нет | Что именно даёт имя типу Cypher: `column` (значение дискриминатора), `table` (имя junction-таблицы) или `fixed` (объявленный алиас). Все приводятся к UPPER_SNAKE_CASE |

### Связи через junction

Ассоциативную таблицу можно объявить полноценной связью Cypher вместо узла — тогда её собственные столбцы становятся атрибутами этой связи: (REQ-1586)

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

Junction — такая же зарегистрированная таблица, как любая другая, и должна быть зарегистрирована до того, как связь сможет её назвать. Объявляйте её по одному разу на каждое значение дискриминатора: три строки над `pet_companions` дают `BONDED_PAIR`, `LITTERMATE` и `SHARES_ENCLOSURE` как три разных типа Cypher, каждый из которых несёт остальные столбцы junction-строки как свойства ребра. Поставляемая демо-конфигурация объявляет ровно это.

Junction-ребро — это связь Cypher, а не поле соединения GraphQL: генератор соединений GraphQL строит свою `ON`-часть для одной пары столбцов, и места для второго перехода в ней нет, поэтому junction-рёбра исключаются из генерируемого SDL и из `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`] Junction-таблица остаётся доступной для запросов как собственное корневое поле и исчезает с узловой стороны схемы графа Cypher, поэтому никогда не появляется как метка узла.

`materialize: true` работает и на junction-ребре, и материализуется при этом обход, а не прямое соединение `pets`-к-`pets`: представление держит переход от источника, переход через junction, дискриминатор и собственные столбцы junction рядом со столбцами цели. Поскольку junction — третья нога соединения, вопрос о межисточниковости ребра решается по всем трём таблицам: junction в источнике, отличном от двух связываемых, материализуется, даже когда эти две совпадают. Одно объявление материализует один тип ребра, поэтому представление, построенное для `bonded pair`, никогда не отвечает на обход `littermate`.

Значения кардинальности [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — каждая исходная строка сопоставляется одной целевой строке (FK на PK)
- `one-to-many` — каждая исходная строка сопоставляется нескольким целевым строкам (обратное к предыдущему)

---

## Правила безопасности на уровне строк
Правила RLS подставляют условия `WHERE` во время запроса, в пределах роли и, при необходимости, таблицы или домена. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Когда для одной роли есть и правило уровня домена, и правило уровня таблицы, приоритет имеет правило уровня таблицы (REQ-403).

| Поле | Обязательное | Описание |
| ------- | ---------- | ------------- |
| `table_id` | Условно | Таблица, к которой применяется правило; взаимоисключающе с `domain_id` |
| `domain_id` | Условно | Домен, к которому применяется правило; действует на все таблицы домена (REQ-402) |
| `role_id` | Да | Роль, к которой применяется правило |
| `filter` | Да | Предикат SQL, подставляемый в `WHERE`; может ссылаться на переменные сессии (REQ-041) |

---

## Функции и веб-хуки
### Функции БД
Возьмите функцию базы данных под учёт и выставьте её как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Источники-базы данных могут также автоматически обнаруживать свои хранимые процедуры и функции по каталогу поставщика (`pg_proc`, `information_schema.routines` или эквиваленты поставщика), что избавляет от ручной регистрации каждой из них. Обнаружение читает `prokind` и `provolatile`: неизменяемые и стабильные функции регистрируются как параметризованные отношения (аргументы процедуры становятся параметрами запроса — та же форма, что у таблиц GET в OpenAPI), а изменчивые процедуры регистрируются как мутации и учтённые функции. Обнаруженные подпрограммы проходят управление второго этапа так же, как зарегистрированные вручную. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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

| Поле | Обязательное | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `name` | Да | — | Имя поля GraphQL |
| `source_id` | Да | — | Источник, содержащий функцию |
| `schema` | Нет | `public` | Схема базы данных |
| `function_name` | Да | — | Фактическое имя функции в базе данных |
| `returns` | Да | — | Идентификатор зарегистрированной таблицы, которую возвращает функция (REQ-207) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` (REQ-211) |
| `visible_to` | Нет | `[]` | Роли, которые могут вызывать эту функцию |
| `writable_by` | Нет | `[]` | Роли, которые могут вызывать её как мутацию |
| `domain_id` | Нет | `""` | Домен, которому принадлежит функция |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` (REQ-205) |

### Веб-хуки
Выставьте внешнюю конечную точку HTTP как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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

| Поле | Обязательное | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `name` | Да | — | Имя поля GraphQL |
| `url` | Да | — | URL конечной точки веб-хука |
| `method` | Нет | `POST` | Метод HTTP |
| `timeout_ms` | Нет | `5000` | Таймаут запроса в миллисекундах |
| `returns` | Нет | `null` | Идентификатор зарегистрированной таблицы или null для встроенного типа |
| `inline_return_type` | Нет | `[]` | Список полей `{name, type}` для произвольных форм возврата (REQ-210) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` |
| `visible_to` | Нет | `[]` | Роли, которые могут вызывать этот веб-хук |
| `domain_id` | Нет | `""` | Домен, которому принадлежит веб-хук |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` |

---

## Аутентификация
Аутентификация настраивается в разделе `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Провайдер | Описание |
| ---------- | ------------- |
| `none` | Без аутентификации; все запросы считаются выполняемыми ролью `default_role` |
| `firebase` | Firebase Authentication; требуются `project_id` и `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | Обычный OAuth 2.0 (REQ-123) |
| `simple` | Имя пользователя и пароль без внешнего провайдера (REQ-124) |

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

`assignments_source: claims` читает назначения ролей из утверждений JWT. `assignments_source: provisa` читает их из собственного хранилища назначений Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Маршрутизация выполнения
**Прямое выполнение** — односточниковые запросы к РСУБД направляются в нативный драйвер ради задержки менее 100 мс (REQ-027). Для поддержки этого пути источнику нужны и запись `SOURCE_TO_DIALECT`, и запись `SOURCE_TO_CONNECTOR` (REQ-229).

**Федеративное выполнение** — межисточниковые запросы и источники без прямого драйвера направляются через федеративный движок (REQ-028). В состав Provisa входит встроенный федеративный движок; для крупных развёртываний укажите на собственный совместимый кластер (REQ-226).

**Статистика** — при регистрации Provisa выполняет `ANALYZE` для каждой опубликованной таблицы, чтобы наполнить оптимизатор по стоимости (число строк, доля null, число различных значений, min/max). Ошибки записываются в журнал и не блокируют регистрацию (REQ-275).

---

## Графовые и семантические источники
### Neo4j

Зарегистрируйте графовую базу данных Neo4j как запрашиваемый источник. Распорядители пишут запросы Cypher, проецирующие скалярные значения; Provisa кеширует результаты и выставляет их как типы GraphQL (REQ-295).

Запросы Cypher должны использовать доступ к свойствам в предложении `RETURN` (`RETURN n.id AS id, n.name AS name`) — возврат объектов-узлов отклоняется на этапе регистрации (REQ-296).

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

Конечная точка предпросмотра (`POST /admin/sources/neo4j/{id}/preview`) возвращает образцы строк и блокирует регистрацию, если Cypher возвращает объекты-узлы (REQ-296).

### SPARQL

Зарегистрируйте любое хранилище триплетов, совместимое со SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog и др.), как запрашиваемый источник (REQ-297).

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

Оба коннектора используют конвейер кеша источников API — результаты сохраняются в PostgreSQL с настраиваемым TTL, что делает их доступными для межисточниковых федеративных JOIN (REQ-295, REQ-297, REQ-299).

---

## Примеры подключения
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

### Межисточниковый запрос
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

Односточниковые части направляются напрямую (REQ-027). Межисточниковые JOIN федерируются с автоматическим приведением типов (REQ-028, REQ-552).
