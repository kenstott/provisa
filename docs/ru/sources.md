# Типы источников

## Модель выполнения

Каждый запрос в конечном счёте выполняется через движок федерации, который обеспечивает федерацию по всем источникам. Источники делятся на три категории в зависимости от их связности. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Категория | Есть прямой драйвер | Есть федеративный коннектор | Примеры |
| --- | --- | --- | --- |
| **С прямым доступом** | Да | Да | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Только федерация** | Нет | Да | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (на базе S3) |
| **Прямое чтение (реплика)** | Да | Да | Snowflake, Databricks, ClickHouse — драйвер читает данные и загружает реплику; запросы выполняются к реплике в активном движке |
| **Материализация → федерация** | Нет | Нет | REST/OpenAPI, удалённый GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (приёмник push), GovData, SharePoint, Splunk |

Источники **с прямым доступом** выполняют однокорневые запросы через свой нативный драйвер (менее 100 мс), минуя движок федерации (REQ-027, REQ-229). Они сохраняют полную поддержку коннектора и участвуют в федерации при соединении с другими источниками (REQ-028).

Источники **только федерация** всегда запрашиваются через слой федерации. Прямого драйвера не существует (REQ-229).

Источники **прямого чтения (реплика)** имеют DirectDriver, который читает из хранилища нативно (нативно для Arrow там, где доступно), загружает реплику в хранилище материализации активного движка, и затем запросы выполняются к этой реплике. См. [Хранилища как именованные источники](#_13).

Источники **материализации** не имеют федеративного коннектора. Provisa извлекает их данные (при запуске или во время запроса) и кеширует их как Parquet в S3 или в PostgreSQL, делая их доступными для движка федерации для кросс-источниковых запросов (REQ-309).

---

## Все источники

Справочник по всем поддерживаемым Provisa типам источников. «Прямой драйвер» означает, что однокорневые запросы выполняются к источнику нативно (менее 100 мс) (REQ-027). «Имя коннектора» — это федеративный коннектор, используемый, когда источник участвует в многоисточниковых JOIN (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### РСУБД

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Да |
| `mysql` | aiomysql | mysql | mysql | Да |
| `mariadb` | aiomysql | mariadb | mysql | Да |
| `singlestore` | — | singlestore | singlestore | Федеративно |
| `sqlserver` | aioodbc | sqlserver | tsql | Да |
| `oracle` | oracledb | oracle | oracle | Да |
| `duckdb` | duckdb | memory | duckdb | Да |
| `cockroachdb` | asyncpg (pg wire) | postgresql | postgres | Да |
| `yugabytedb` | asyncpg (pg wire) | postgresql | postgres | Да |
| `greenplum` | asyncpg (pg wire) | postgresql | postgres | Да |
| `tidb` | aiomysql (mysql wire) | mysql | mysql | Да |

Совместимые по протоколу базы данных повторно используют JDBC-драйвер, нативный асинхронный драйвер и диалект базового протокола — CockroachDB, YugabyteDB и Greenplum используют протокол PostgreSQL; TiDB — протокол MySQL. Им нужны только записи в реестре, без нового кода коннектора. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) и `airport` (сервер Arrow Flight) — зарегистрированные типы источников, достигаемые на месте через community-расширения DuckDB, когда DuckDB является активным движком — без прямого драйвера, без федеративного коннектора. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Облачные хранилища данных

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Федеративно | Читает через snowflake-connector-python; загружает реплику; `account`/`warehouse`/`role` в `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Федеративно | Нет DirectDriver; достигается через движок федерации или ATTACH движка BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Федеративно | Читает через databricks-sql-connector (Cloud Fetch, Arrow); загружает реплику; `http_path` обязателен в `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Федеративно | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Федеративно | Microsoft Fabric Warehouse; T-SQL по TDS, аутентификация Azure AD; загружает реплику (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Федеративно | Azure Synapse SQL; T-SQL по TDS, аутентификация Azure AD; загружает реплику (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Федеративно | Удалённый координатор Trino/Presto читается через диалект SQLAlchemy trino; загружает реплику в любой движок (REQ-994) |

### Аналитика / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Федеративно | Читает через clickhouse-connect (HTTP); `secure: "true"` в `federation_hints` для TLS (REQ-986) |
| `druid` | — | druid | druid | Нет | — |
| `exasol` | — | exasol | exasol | Нет | — |
| `elasticsearch` | — | elasticsearch | — | Нет | Свойства коннектора берутся из mapping DSL типа [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Нет | Коннектор Trino `pinot`; `pinot.controller-urls` = host:port контроллера Pinot [tool-verified: `trino_connectors.py:199`] |

### Data Lake / открытые табличные форматы

Эти типы источников доступны только через федерацию — нет прямого драйвера, нет диалекта. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Тип источника | Имя коннектора | Путешествие во времени | Примечания |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Да (аргумент `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Да (аргумент `as_of`, REQ-372) | — |
| `hive` | hive | Нет | — |
| `hive_s3` | hive | Нет | Hive на базе S3 |

### NoSQL

`mongodb`, `cassandra` и `redis` имеют коннекторы Trino (`redis` строит свои свойства из mapping DSL типа). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Нет |
| `cassandra` | cassandra | Нет |
| `redis` | redis | Нет |

### Потоковая передача

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `kafka` | Федеративный коннектор Kafka; схема через Confluent Schema Registry (Avro, Protobuf, JSON Schema), ручное определение или вывод по выборке (REQ-147, REQ-150) | Только приёмник (REQ-176) |
| `websocket` | Внешний фид WebSocket — подключение, подписка, получение событий; результаты материализуются (REQ-338) | Нет |
| `rss` | Фид RSS 2.0 / Atom — опрос, водяной знак по pubDate/updated; результаты материализуются (REQ-342, REQ-343) | Нет |

### Приёмник Push

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `ingest` | Внешние сервисы отправляют события JSON через POST; результаты материализуются (REQ-331, REQ-335) | Нет |

### Граф и семантика

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher через HTTP API, результаты кешируются в PostgreSQL (REQ-295) | Нет |
| `sparql` | SPARQL 1.1 POST, результаты кешируются в PostgreSQL (REQ-297) | Нет |

### На основе файлов

Два механизма покрывают файлы. Оба используют поле `path` вместо `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Источники с одним файлом** — `sqlite`, `csv`, `parquet` указывают `path` на один файл.

| Тип источника | Транспорты | Мутации |
| --- | --- | --- |
| `sqlite` | локальный | Да |
| `csv` | локальный | Нет |
| `parquet` | локальный, `s3://` | Нет |

Приватным бакетам требуются учётные данные (регион AWS и ключи из окружения). Для CSV через `s3://` или `http(s)://`, или для регистрации множества файлов сразу, используйте источник `files`. [tool-verified: `provisa/file_source/source.py`]

**Источник `files`** — указывает `path` на glob-шаблон, рекурсивно сканирует его и регистрирует директорию как федеративный каталог таблиц. Он читает множество форматов через множество транспортов; наборы ниже происходят из файлового коннектора (форк kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Форматы | Транспорты |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow и документы, конвертированные в таблицы — HTML, Markdown, DOCX, PPTX | Локальная файловая система, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST и Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Наблюдаемость и прочее

`prometheus` имеет коннектор Trino (свойства строятся из mapping DSL типа). `google_sheets` — зарегистрированный тип источника без коннектора Trino, материализующийся через конвейер API-кеша. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (материализуется) | Нет |
| `prometheus` | prometheus | Нет |

### Корпоративные коннекторы SaaS

SharePoint и Splunk регистрируются через коннекторы Apache Calcite (форк kenstott/calcite). Ни у одного нет прямого драйвера — Provisa материализует их строки, запуская встроенный сервер pgwire коннектора (`pgwire-sharepoint`, `pgwire-splunk`), подключаясь к нему как к обычному эндпоинту PostgreSQL, и загружая строки в хранилище материализации для федерации (REQ-954). Оба коннектора всегда включают сопоставление имён без учёта регистра, соответствуя собственной семантике каждого продукта без учёта регистра (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Списки SharePoint перечисляются как схемы и выставляются как запрашиваемые таблицы (REQ-726, REQ-731). Два метода аутентификации: `CLIENT_CREDENTIALS` (по умолчанию) и на основе сертификата через сертификат PFX (REQ-727). Значения секретов в `mapping` разрешаются через движок секретов до достижения коннектора (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` или `host` | `site-url` | URL сайта SharePoint |
| `username` | `client-id` | ID клиента приложения Azure |
| `password` | `client-secret` | Секрет клиента приложения Azure |
| `database` | `tenant-id` | UUID арендатора Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (по умолчанию) или `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Путь к PFX при `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Пароль PFX |

Когда коннектор не выставляет `information_schema.columns`, зарегистрируйте таблицу с явными определениями столбцов (полученными из Microsoft Graph API) через мутацию `registerTable` (REQ-732).

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

Результаты поиска Splunk запрашиваются как таблицы (например, `internal_server`) (REQ-721). URL коннектора берётся из `base_url` или строится как `https://{host}:{port}` с портом по умолчанию `8089` (REQ-722). Аутентификация: когда `mapping.use_token` равно `true` (по умолчанию), `password` передаётся как API-токен; когда `false`, `username` и `password` передаются как отдельные учётные данные (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Поле источника | Свойство коннектора | Примечания |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, иначе `https://host:port` (порт по умолчанию 8089) |
| `password` | `token` или `password` | токен, когда `use_token: true` |
| `username` | `user` | только когда `use_token: false` |
| `database` | `app` | ограничение приложением Splunk |
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

### Источники API

Зарегистрируйте любой HTTP-эндпоинт как запрашиваемую таблицу. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Тип API | Обнаружение | Вывод столбцов |
| --------- | ----------- | ----------------- |
| `openapi` | Разбор спецификации OpenAPI (REQ-314, REQ-316) | Примитивы → нативные, объекты → JSONB |
| `graphql_remote` | Интроспекция схемы (REQ-307, REQ-308) | Примитивы → нативные, объекты → JSONB |
| `grpc_remote` | Рефлексия сервера (REQ-322, REQ-325) | Примитивы → нативные, объекты → JSONB |

Ответы API извлекаются, кешируются в PostgreSQL (настраиваемый TTL) и выставляются как типы GraphQL (REQ-309, REQ-318, REQ-327). Кешированные таблицы участвуют в федеративных запросах, как и любой другой источник (REQ-313).

**Правила JSONB**: сложные столбцы (объекты, массивы), хранимые как JSONB, не фильтруемы (REQ-119). Доступ к вложенным полям использует извлечение `->>` в SQL (REQ-151). Связи объявляются между таблицами с использованием скалярных столбцов FK — блоб-столбцы JSONB не являются целями соединения. Используйте продвижение JSONB для преобразования вложенных полей в нативные скалярные столбцы, когда фильтрация или соединение по ним необходимы (REQ-119).

### GovData

Открытые данные правительства США. Доступ разделён по группировке предметов. [tool-verified: `provisa/core/models.py` lines 543–609]

Каждый источник `govdata` выбирает один предмет. Этот предмет определяет, какие схемы GovData выставляются. Схемы `ref` и `geo` всегда включены как связующие схемы — они не перечисляются по предмету, но всегда присутствуют. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Предмет | Выставляемые схемы |
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

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор |
| `subject` | Да | — | Одно из значений предмета выше |
| `domain_id` | Да | — | Домен, которому принадлежит этот источник |
| `description` | Нет | `""` | Человекочитаемое описание |

---

## Пользовательские коннекторы (REQ-1177)

Нативные движки федерации — Postgres, DuckDB и ClickHouse — получают доступность к новому типу источника, когда оператор объявляет для него коннектор в `config/custom_connectors.yaml`. Код не требуется. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Расширяемость коннекторов сама по себе существовала и раньше. Движок Trino давно расширяем на своём собственном уровне — один универсальный JDBC-коннектор, параметризуемый для каждого типа источника, тело `.properties` каталога для каждого типа и собственные плагины пользовательских коннекторов Trino Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 привносит ту же управляемую конфигурацией расширяемость в два нативных движка без кластера, которые ранее несли фиксированный набор коннекторов.

Конфигурация поставляется пустой. Встроенные коннекторы покрывают доступность «из коробки»; всё в этом файле создаётся оператором. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Установите `PROVISA_CUSTOM_CONNECTORS`, чтобы указать на другой путь (полезно для тестов).

### Виды дескрипторов

| Движок | Вид | Механизм | Что предоставляет дескриптор |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (стандарт ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + представление-сканер | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (автоматически выставляет каждую удалённую таблицу) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` для каждой таблицы (столбцы из реестра) | `ch_engine`, `engine_template` (может нести `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse выводит схему | `ch_engine`, `engine_template` |

**Postgres универсален.** SQL/MED — стандарт ISO, поэтому каждый соответствующий FDW разделяет одну и ту же форму DDL: `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, опциональный `CREATE USER MAPPING`, затем либо `IMPORT FOREIGN SCHEMA` (когда `supports_import: true`), либо явный `CREATE FOREIGN TABLE` для каждой таблицы (когда `false`). Дескриптор `pg_fdw` предоставляет только вариацию для каждого FDW — имя расширения, ключи опций сервера, ключи сопоставления пользователей, флаг импорта, опции таблицы. Поэтому любой соответствующий стандарту FDW может управляться только конфигурацией. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB поддерживает два механизма.** Расширение, выставляющее каталог через ATTACH, использует `duckdb_attach`; выставляющее табличную функцию для чтения использует `duckdb_scan`. Расширение, не подходящее ни под один шаблон, не поддерживается. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse поддерживает три механизма**, по одному на форму интеграционного движка: реляционный движок DATABASE, автоматически выставляющий каждую удалённую таблицу (`clickhouse_database`, например Redis/MySQL), движок для каждой таблицы, столбцы которого предоставляет реестр (`clickhouse_table`, например мост JDBC/ODBC — `engine_template` может нести заполнитель `{table}`, который связывается во время выполнения), и движок файлов/озера/URL, схему которого ClickHouse выводит (`clickhouse_scan`, например HDFS/URL). SQLite (движок DATABASE, файл, без сервера) и Hudi (lakehouse, без копирования) поставляются «из коробки». [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Неизвестное значение `kind` громко отказывает при запуске — опечатка в дескрипторе не должна незаметно оставлять тип источника недостижимым. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Проверка доступности (probe gating)

Доступность проверяется в момент присоединения по стандартному каталогу обнаружения каждого движка:

- **Postgres** — проверяет `pg_extension`, затем `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — выполняет `INSTALL`/`LOAD` и проверяет `duckdb_functions()` на предмет объявленного `probe_symbol`. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — проверяет `system.table_engines` на предмет объявленного `ch_engine`; отсутствие в сборке громко отказывает. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Объявленное расширение, которое невозможно установить, громко отказывает. Никакого тихого пропуска, никакого запасного варианта. Коннектор, чья проверка не проходит, просто не активен для этого развёртывания.

### Переменные шаблона

Каждое значение `server_options`, значение `user_mapping`, `attach_template` и `scan_template` может использовать заполнители `{field}`. Доступные поля: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, плюс любой ключ из `federation_hints`. Шаблоны присоединения DuckDB также получают `{alias}` — внутренний псевдоним каталога, назначаемый Provisa присоединённой базе данных.

Шаблон, ссылающийся на неизвестное поле, громко отказывает в момент присоединения, выявляя несоответствие дескриптора/источника до того, как сломанный DDL достигнет движка.

### Примеры

**Postgres — MongoDB через `mongo_fdw` (без импорта схемы; столбцы предоставляются для каждой таблицы)**

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

При наличии любого из дескрипторов регистрация источника с объявленным `source_type` маршрутизируется через пользовательский коннектор при условии успешной проверки. Никаких других изменений конфигурации не требуется.

---

## Хранилища как именованные источники

Snowflake, Databricks и ClickHouse могут быть зарегистрированы как именованные источники независимо от того, какой движок федерации активен. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

При регистрации Provisa читает хранилище через DirectDriver источника и загружает реплику в хранилище материализации активного движка. Затем запрос выполняется к этой реплике. Это отличается от традиционного пути с прямым доступом (asyncpg, aiomysql), где движок полностью минуется — здесь движок всё ещё выполняет запрос, но к локальной реплике, а не по проводу к хранилищу при каждом запросе.

Чтения нативны для Arrow там, где хранилище это поддерживает: Databricks использует Cloud Fetch, Snowflake использует `fetch_arrow_table`, а ClickHouse использует нативный колоночный HTTP-интерфейс.

Расширенные параметры соединения, которые стандартные поля `host`/`port`/`username`/`password` не могут нести, идут в `federation_hints`:

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

Регистрация как именованного источника не зависит от выбора того же хранилища в качестве движка федерации. Источник Snowflake на движке DuckDB загружает реплику в DuckDB, а не в Snowflake.

Данные облачного объекта/озера (файлы parquet, csv, iceberg, delta_lake на S3 / GCS / R2) — это отдельный тип источника, который присоединяется на месте, когда у активного движка есть коннектор ATTACH для этого типа. Реплика не загружается — движок сканирует объектное хранилище напрямую. Учётные данные для этих источников также идут в `federation_hints`:

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

Все источники разделяют общий набор полей. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Поле | Обязательно | По умолчанию | Описание |
| ------- | ---------- | --------- | ------------- |
| `id` | Да | — | Уникальный идентификатор; буквенно-цифровой с дефисами/подчёркиваниями |
| `type` | Да | — | Тип источника (см. таблицы выше) |
| `host` | Нет | `""` | Имя хоста или IP |
| `port` | Нет | `0` | Номер порта |
| `database` | Нет | `""` | Имя базы данных |
| `username` | Нет | `""` | Имя пользователя |
| `password` | Нет | `""` | Пароль; используйте `${env:VAR}` для разрешения секрета |
| `path` | Нет | `null` | Путь к файлу или облачный URI для файловых и объектных/озёрных источников |
| `base_url` | Нет | `null` | Базовый URL для источников OpenAPI |
| `pool_min` | Нет | `1` | Минимальный размер пула соединений (REQ-052) |
| `pool_max` | Нет | `5` | Максимальный размер пула соединений (REQ-052) |
| `use_pgbouncer` | Нет | `false` | Маршрутизировать соединения через PgBouncer (REQ-053) |
| `pgbouncer_port` | Нет | `6432` | Порт PgBouncer (REQ-053) |
| `cache_enabled` | Нет | `true` | Включить кеширование ответов API |
| `cache_ttl` | Нет | `null` | TTL кеша в секундах; наследует глобальное значение по умолчанию, если null |
| `cache_catalog` | Нет | `null` | Федеративный каталог для API-кеша; по умолчанию — собственный каталог источника |
| `cache_schema` | Нет | `api_cache` | Схема внутри каталога кеша |
| `naming_convention` | Нет | `null` | Переопределить глобальную конвенцию именования для этого источника (REQ-194) |
| `federation_hints` | Нет | `{}` | Свойства сессии, передаваемые движку федерации, и расширенные параметры соединения для источников-хранилищ (REQ-278, REQ-281) |
| `mapping` | Нет | `{}` | Настройки коннектора, специфичные для типа, для источников NoSQL и SaaS (например, `auth_type` для SharePoint, `use_token` для Splunk) (REQ-251) |
| `allowed_domains` | Нет | `[]` | Ограничить источник конкретными доменами; пусто = без ограничений |
| `description` | Нет | `""` | Человекочитаемое описание |

---

## Источники Kafka

Топики Kafka настраиваются отдельно под `kafka_sources`, ключом является `id` зарегистрированного источника `kafka`. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

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
| `id` | Должно соответствовать `id` источника с `type: kafka` |
| `topics[].id` | Логическое имя для этого топика внутри Provisa |
| `topics[].topic` | Имя топика Kafka |
| `topics[].domain_id` | Домен, которому принадлежит этот топик |
| `topics[].description` | Человекочитаемое описание |
| `topics[].default_window` | Временное окно по умолчанию для оконных запросов (например, `1h`) (REQ-148) |
| `topics[].columns` | Определения столбцов для схемы топика (REQ-150) |

---

## Видимость столбцов

Поле `visible_to` для каждого столбца — это список идентификаторов ролей, которые могут видеть этот столбец. [tool-verified: `provisa/core/models.py` `Column` class line 248; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Столбцы, опущенные из списка `visible_to` роли, не появляются в схеме GraphQL этой роли и не могут запрашиваться или упоминаться в фильтрах (REQ-039).

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

| Поле | Обязательно | Описание |
| ------- | ---------- | ------------- |
| `id` | Да | Уникальный идентификатор для этой связи |
| `source_table_id` | Да | Таблица, содержащая внешний ключ |
| `target_table_id` | Да | Таблица, на которую делается ссылка; пусто для вычисляемых связей |
| `source_column` | Да | Столбец в исходной таблице |
| `target_column` | Да | Столбец в целевой таблице; пусто для вычисляемых связей |
| `cardinality` | Да | `many-to-one` или `one-to-many` (REQ-019) |
| `materialize` | Нет | Автоматически создать материализованное представление для кросс-источниковых соединений (REQ-158) |
| `refresh_interval` | Нет | Интервал обновления MV в секундах (по умолчанию: 300) |
| `target_function_name` | Нет | Имя функции БД для вычисляемых связей |
| `function_arg` | Нет | Какой аргумент функции получает значение исходного столбца |
| `alias` | Нет | Человекочитаемый тип связи (например, `WORKS_FOR`) |
| `graphql_alias` | Нет | Именует поле SDL, которое эта связь выставляет в родительском типе. Если отсутствует, имя выводится из `field_name` целевой таблицы и кардинальности связи. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Нет | Когда `true`, исключить эту связь из рёбер графа Cypher |
| `source_json_key` | Нет | Извлечь этот ключ из исходного столбца как JSON-объект перед JOIN |

Значения кардинальности [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81]:

- `many-to-one` — каждая исходная строка сопоставляется с одной целевой строкой (FK → PK)
- `one-to-many` — каждая исходная строка сопоставляется с несколькими целевыми строками (обратное вышеуказанному)

---

## Правила безопасности на уровне строк

Правила RLS внедряют предложения `WHERE` во время выполнения запроса, ограниченные ролью и опционально таблицей или доменом. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Когда для одной и той же роли существуют и правило уровня домена, и правило уровня таблицы, правило уровня таблицы имеет приоритет (REQ-403).

| Поле | Обязательно | Описание |
| ------- | ---------- | ------------- |
| `table_id` | Условно | Таблица, к которой применяется правило; взаимоисключающе с `domain_id` |
| `domain_id` | Условно | Домен, к которому применяется правило; применяется ко всем таблицам домена (REQ-402) |
| `role_id` | Да | Роль, к которой применяется это правило |
| `filter` | Да | Предикат SQL, внедряемый в `WHERE`; может ссылаться на переменные сессии (REQ-041) |

---

## Функции и вебхуки

### Функции БД

Отслеживайте функцию базы данных и выставляйте её как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438; `config/provisa.yaml` lines 152–164] (REQ-205)

Источники баз данных также могут автоматически обнаруживать свои хранимые процедуры и функции из каталога вендора (`pg_proc`, `information_schema.routines` или эквиваленты вендора), устраняя необходимость вручную регистрировать каждую из них. Обнаружение читает `prokind` и `provolatile`: неизменяемые/стабильные функции регистрируются как параметризованные отношения (аргументы процедуры становятся параметрами запроса, той же формы, что и таблицы GET OpenAPI), а изменчивые процедуры регистрируются как мутации/отслеживаемые функции. Обнаруженные процедуры проходят через governance этапа 2 идентично вручную зарегистрированным. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

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
| `returns` | Да | — | Идентификатор зарегистрированной таблицы, которую возвращает функция (REQ-207) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` (REQ-211) |
| `visible_to` | Нет | `[]` | Роли, которые могут вызывать эту функцию |
| `writable_by` | Нет | `[]` | Роли, которые могут вызывать её как мутацию |
| `domain_id` | Нет | `""` | Домен, которому принадлежит эта функция |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` (REQ-205) |

### Вебхуки

Выставите внешний HTTP-эндпоинт как запрос или мутацию GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455; `config/provisa.yaml` lines 166–178] (REQ-209)

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
| `url` | Да | — | URL эндпоинта вебхука |
| `method` | Нет | `POST` | HTTP-метод |
| `timeout_ms` | Нет | `5000` | Таймаут запроса в миллисекундах |
| `returns` | Нет | `null` | Идентификатор зарегистрированной таблицы или null для инлайн-типа |
| `inline_return_type` | Нет | `[]` | Список полей `{name, type}` для пользовательских форм возврата (REQ-210) |
| `arguments` | Нет | `[]` | Список определений аргументов `{name, type}` |
| `visible_to` | Нет | `[]` | Роли, которые могут вызывать этот вебхук |
| `domain_id` | Нет | `""` | Домен, которому принадлежит этот вебхук |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` |

---

## Аутентификация

Аутентификация настраивается под ключом `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Провайдер | Описание |
| ---------- | ------------- |
| `none` | Без аутентификации; все запросы обрабатываются как `default_role` |
| `firebase` | Firebase Authentication; требует `project_id` и `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | Общий OAuth 2.0 (REQ-123) |
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

`assignments_source: claims` считывает назначения ролей из утверждений JWT. `assignments_source: provisa` считывает их из собственного хранилища назначений Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Маршрутизация выполнения

**Прямое выполнение** — однокорневые запросы РСУБД маршрутизируются к нативному драйверу для задержки менее 100 мс (REQ-027). Источникам требуется как запись `SOURCE_TO_DIALECT`, так и запись `SOURCE_TO_CONNECTOR`, чтобы поддерживать этот путь (REQ-229).

**Федеративное выполнение** — многоисточниковые запросы и источники без прямого драйвера маршрутизируются через движок федерации (REQ-028). Provisa включает встроенный движок федерации; для развёртываний большого масштаба укажите на собственный совместимый кластер (REQ-226).

**Статистика** — при регистрации Provisa выполняет `ANALYZE` для каждой опубликованной таблицы, чтобы подготовить оптимизатор на основе стоимости (количество строк, доля null, уникальные значения, min/max). Сбои регистрируются и не блокируют регистрацию (REQ-275).

---

## Источники графа и семантики

### Neo4j

Зарегистрируйте графовую базу данных Neo4j как запрашиваемый источник. Стюарды пишут запросы Cypher, проецирующие скалярные значения; Provisa кеширует результаты и выставляет их как типы GraphQL (REQ-295).

Запросы Cypher должны использовать аксессоры свойств в предложении `RETURN` (`RETURN n.id AS id, n.name AS name`) — возврат объектов узлов отклоняется во время регистрации (REQ-296).

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

Эндпоинт предпросмотра (`POST /admin/sources/neo4j/{id}/preview`) возвращает примеры строк и блокирует регистрацию, если Cypher возвращает объекты узлов (REQ-296).

### SPARQL

Зарегистрируйте любое совместимое с SPARQL 1.1 тройное хранилище (Apache Jena Fuseki, Virtuoso, Stardog и т. д.) как запрашиваемый источник (REQ-297).

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

Оба коннектора используют конвейер кеша источников API — результаты хранятся в PostgreSQL с настраиваемым TTL, что делает их доступными для кросс-источниковых федеративных JOIN (REQ-295, REQ-297, REQ-299).

---

## Примеры соединений

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

### Кросс-источниковый запрос

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

Однокорневые части маршрутизируются напрямую (REQ-027). Кросс-источниковые JOIN федерируются с автоматическим приведением типов (REQ-028, REQ-552).
