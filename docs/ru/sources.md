# Типы источников

## Модель выполнения

В конечном счёте каждый запрос выполняется через движок федерации, который обеспечивает федерацию по всем источникам. Источники делятся на три категории в зависимости от их связности. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Категория | Есть прямой драйвер | Есть федеративный коннектор | Примеры |
| --- | --- | --- | --- |
| **Прямая поддержка (direct-capable)** | Да | Да | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Только федерация** | Нет | Да | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (на базе S3) |
| **Прямое чтение (реплика)** | Да | Да | Snowflake, Databricks, ClickHouse — драйвер читает данные и создаёт реплику; запросы выполняются против реплики в активном движке |
| **Материализация → Федерация** | Нет | Нет | REST/OpenAPI, удалённый GraphQL, gRPC, Neo4j Cypher, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (приёмник push-данных), GovData, SharePoint, Splunk |

Источники с **прямой поддержкой** выполняют однопоточные (single-source) запросы через свой нативный драйвер (менее 100 мс), минуя движок федерации (REQ-027, REQ-229). Они сохраняют полную поддержку коннектора и участвуют в федерации при объединении с другими источниками (REQ-028).

Источники **только с федерацией** всегда опрашиваются через слой федерации. Прямого драйвера не существует (REQ-229).

Источники **с прямым чтением (реплика)** имеют DirectDriver, который читает данные из хранилища нативно (в формате Arrow, где это возможно), создаёт реплику в хранилище материализации активного движка, а затем запросы выполняются против этой реплики. См. [Хранилища как именованные источники](#warehouses-as-named-sources).

Источники **материализации** не имеют федеративного коннектора. Provisa получает их данные (при запуске или во время выполнения запроса) и кеширует их как Parquet в S3 или в PostgreSQL, делая их достижимыми для движка федерации в межисточниковых запросах (REQ-309).

---

## Все источники

Provisa регистрирует **54** типа источников. Приведённые ниже таблицы охватывают все 54; индекс — это просто счётчик. [tool-verified: `provisa/core/models.py` `SourceType`; Kaggle учитывается как отдельный источник, хотя внутренне регистрируется через коннектор `files`]

| # | Группа | Типы источников |
| --- | --- | --- |
| 1–13 | [RDBMS](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Облачные хранилища данных](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Аналитика / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Data lake / открытые табличные форматы](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Потоковая передача](#streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Приёмник push-данных](#push-receiver) | `ingest` |
| 38–39 | [Граф и семантика](#graph-semantic) | `neo4j`, `sparql` |
| 40–43 | [На основе файлов](#file-based) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Наблюдаемость и прочее](#observability-other) | `google_sheets`, `prometheus` |
| 46–47 | [Корпоративный SaaS](#enterprise-saas-connectors) | `sharepoint`, `splunk` |
| 48–50 | [API-источники](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Проверки качества данных](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 54 | [Наборы данных Kaggle](#kaggle-datasets) | Kaggle (загружается через форму Sources; регистрируется как источник `files` — см. [Наборы данных Kaggle](#kaggle-datasets)) |

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

Базы данных, совместимые по протоколу, повторно используют JDBC-драйвер, нативный асинхронный драйвер и диалект базового протокола — CockroachDB, YugabyteDB и Greenplum используют протокол PostgreSQL; TiDB использует протокол MySQL. Им требуются только записи в реестре, без нового кода коннектора. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) и `airport` (сервер Arrow Flight) — зарегистрированные типы источников, достигаемые на месте через community-расширения DuckDB, когда DuckDB является активным движком — без прямого драйвера, без федеративного коннектора. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Облачные хранилища данных {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Тип источника | Прямой драйвер | Имя коннектора | Диалект | Мутации | Примечания |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Федеративные | Чтение через snowflake-connector-python; создаёт реплику; `account`/`warehouse`/`role` в `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Федеративные | Нет DirectDriver; достигается через движок федерации или через ATTACH движка BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Федеративные | Чтение через databricks-sql-connector (Cloud Fetch, Arrow); создаёт реплику; `http_path` обязателен в `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Федеративные | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Федеративные | Microsoft Fabric Warehouse; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Федеративные | Azure Synapse SQL; T-SQL поверх TDS, аутентификация Azure AD; создаёт реплику (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Федеративные | Удалённый координатор Trino/Presto, чтение через диалект trino SQLAlchemy; создаёт реплику на любом движке (REQ-994) |

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

### Потоковая передача {: #streaming }

| Тип источника | Механизм | Мутации |
| ------------ | ----------- | ----------- |
| `kafka` | Федеративный коннектор Kafka; схема через Confluent Schema Registry (Avro, Protobuf, JSON Schema), ручное определение или вывод по образцу (REQ-147, REQ-150) | Только приёмник (sink) (REQ-176) |
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

Два механизма охватывают файлы. Оба используют поле `path` вместо `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Однофайловые источники** — `sqlite`, `csv`, `parquet` указывают `path` на один файл.

| Тип источника | Транспорты | Мутации |
| --- | --- | --- |
| `sqlite` | локальный | Да |
| `csv` | локальный | Нет |
| `parquet` | локальный, `s3://` | Нет |

Приватным бакетам нужны учётные данные (регион AWS и ключи из окружения). Для CSV через `s3://` или `http(s)://`, или для регистрации многих файлов сразу, используйте источник `files`. [tool-verified: `provisa/file_source/source.py`]

**Источник `files`** — указывает `path` на glob-шаблон, рекурсивно обходит его и регистрирует директорию как федеративный каталог таблиц. Он читает множество форматов через множество транспортов; наборы ниже приведены из файлового коннектора (форк kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Форматы | Транспорты |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow и документы, преобразуемые в таблицы — HTML, Markdown, DOCX, PPTX | Локальная файловая система, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST и Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

На движке DuckDB `files` читается нативно — представление-сканер `read_csv_auto` для каждого `<table>.csv` в разрешённой директории (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. На движке без собственного коннектора `files` строки загружаются через тот же встроенный в коннектор сервер Calcite pgwire (`pgwire-file`), который используют sharepoint/splunk (REQ-954) — см. [Корпоративные SaaS-коннекторы](#enterprise-saas-connectors) ниже. Сквозное покрытие UI (форма Sources → регистрация таблицы → SQL-запрос) и путь загрузки через pgwire подтверждены в REQ-1694.

#### Наборы данных Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle — это платформа для загрузки файлов. Загруженный набор данных Kaggle регистрируется как источник типа `files` и опрашивается через тот же коннектор pgwire-file, который используют любые другие источники `files` — в перечислении SourceType нет отдельного типа `kaggle`. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — no `kaggle` literal]

**Добавление набора данных.** Откройте Sources → Subscriptions → Kaggle. Форма выполняет два последовательных шага, потому что собственная конечная точка поиска наборов данных Kaggle требует аутентификации — селектор не может появиться до проверки токена. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Токен** — введите API-токен Kaggle и нажмите «Validate». Проверка вызывает `POST https://www.kaggle.com/api/v1/datasets/create/new` с пустым телом. `401` означает недействительность; любой другой ответ означает валидность (собственная валидация полезной нагрузки Kaggle срабатывает до создания какого-либо набора данных — ничего не сохраняется). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Селектор** — поле поиска в реальном времени по мере ввода запрашивает `GET /api/v1/datasets/list`. Каждый результат показывает заголовок и описание набора данных. Выберите один, затем нажмите «Add Dataset».

Нажатие «Add Dataset» загружает пакет из `GET /api/v1/datasets/download/{owner}/{ref}`, распаковывает элементы CSV и Parquet в `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>` и создаёт **один** источник типа `files`, чей `path` — эта корневая директория. Токен никогда не сохраняется на стороне сервера. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

После добавления источника зарегистрируйте его таблицы через обычный экран регистрации таблицы. Рекурсивное обнаружение директорий коннектора pgwire-file перечисляет каждый файл как отдельную таблицу — тот же механизм, что используют другие источники `files` (REQ-1690). (REQ-1783)

**Ограничение v1.** Пакет, содержащий файл `.sqlite` или `.db`, отклоняется сразу с чёткой ошибкой. Загружаются только файлы CSV и Parquet. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Именование таблиц.** Каждый файл попадает в собственную поддиректорию `<file-stem>/` под корнем набора данных. Коннектор pgwire-file именует итоговую таблицу как `<subdir>__<stem>` после нормализации SMART_CASING. Например: `StatewiseTestingDetails.csv` попадает в `statewise_testing_details/StatewiseTestingDetails.csv` и становится таблицей `statewise_testing_details__statewise_testing_details`. Удвоение основы ожидаемо для однофайловых наборов данных. Многофайловый набор данных даёт одну пару на файл: `orders__orders`, `customers__customers`. (REQ-471)

**Обновление.** Чтобы повторно получить набор данных после публикации Kaggle новой версии, вызовите GraphQL-мутацию `refreshKaggleSource` с идентификатором источника и действительным токеном. Она заново загружает файлы на месте и очищает кеш конечной точки pgwire-file, чтобы коннектор подхватил любые изменения схемы при следующем запросе. `kaggle_owner` и `kaggle_ref`, сохранённые в `federation_hints` при создании, определяют, какой набор данных обновлять. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Нет статического пути конфигурации YAML.** Источники Kaggle создаются только через форму Sources. Источник Kaggle, экспортированный в YAML, отображается как `type: files` с `kaggle_owner` и `kaggle_ref` в `federation_hints`. Повторная загрузка из Kaggle требует потока обновления через UI или мутации `refreshKaggleSource` — указание `path` в YAML на уже загруженную директорию — альтернатива для сред без доступа к интернету (air-gapped).


### Наблюдаемость и прочее {: #observability-other }

`prometheus` имеет коннектор Trino (свойства строятся из DSL сопоставления этого типа). `google_sheets` — зарегистрированный тип источника без коннектора Trino, материализующийся через конвейер API-кеша. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Тип источника | Имя коннектора | Мутации |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (материализуется) | Нет |
| `prometheus` | prometheus | Нет | Метрика — это таблица, а образец — это строка (`timestamp`, `value`, по одному столбцу на метку); на любом движке без живого коннектора Provisa читает HTTP API — имена метрик и метки для регистрации таблицы, `query_range` по диапазону таблицы для создания реплики [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689)

### Корпоративные SaaS-коннекторы {: #enterprise-saas-connectors }

SharePoint и Splunk регистрируются через коннекторы Apache Calcite (форк kenstott/calcite). Ни у одного нет прямого драйвера — Provisa запускает встроенный в коннектор сервер Calcite pgwire (`pgwire-sharepoint`, `pgwire-splunk`) и обращается к нему как к обычной конечной точке PostgreSQL. На движке DuckDB эта конечная точка подключается вживую через расширение postgres: регистрация таблицы перечисляет таблицы коннектора из подключённого каталога, запросы читают коннектор на месте, а фильтры и проекции проталкиваются в Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Любой другой движок загружает строки в хранилище материализации для федерации (REQ-954). Пакеты загружаются под конкретную ОС/архитектуру из закреплённого релиза `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz`; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`]. Оба коннектора всегда включают сопоставление имён без учёта регистра, что соответствует собственной регистронезависимой семантике каждого продукта (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100; `provisa/federation/trino_connectors.py` lines 223–286]

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

### API-источники {: #api-sources }

Зарегистрируйте любую HTTP-конечную точку как запрашиваемую таблицу. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Тип API | Обнаружение | Вывод типов столбцов |
| --------- | ----------- | ----------------- |
| `openapi` | Разбор спецификации OpenAPI (REQ-314, REQ-316) | Примитивы → нативные типы, объекты → JSONB |
| `graphql_remote` | Интроспекция схемы (REQ-307, REQ-308) | Примитивы → нативные типы, объекты → JSONB |
| `grpc_remote` | Server reflection (REQ-322, REQ-325) | Примитивы → нативные типы, объекты → JSONB |

Ответы API получаются, кешируются в PostgreSQL (настраиваемый TTL) и представляются как типы GraphQL (REQ-309, REQ-318, REQ-327). Кешированные таблицы участвуют в федеративных запросах наравне с любым другим источником (REQ-313).

**Правила JSONB**: сложные столбцы (объекты, массивы), хранящиеся как JSONB, не поддерживают фильтрацию (REQ-119). Доступ к вложенным полям использует извлечение через `->>` в SQL (REQ-151). Связи объявляются между таблицами через скалярные столбцы внешнего ключа — столбцы-блобы JSONB не являются целями соединения (join). Используйте продвижение JSONB (JSONB promotion), чтобы преобразовать вложенные поля в нативные скалярные столбцы, когда требуется фильтрация или соединение по ним (REQ-119).

### GovData

Открытые данные правительства США. Доступ разделён по тематическим группам. [tool-verified: `provisa/core/models.py` lines 543–609]

Каждый источник `govdata` выбирает одну тему. Эта тема определяет, какие схемы GovData доступны. Схемы `ref` и `geo` всегда включены как связующие схемы — они не перечислены по темам, но присутствуют всегда. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Тема | Открываемые схемы |
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
| `subject` | Да | — | Одно из значений темы выше |
| `domain_id` | Да | — | Домен, которому принадлежит этот источник |
| `description` | Нет | `""` | Описание для человека |

### Где хранится пароль источника

Пароль источника никогда не хранится рядом с остальными настройками подключения. Строка `sources`
в плоскости управления содержит столбец `password_ref`, хранящий *ссылку* —
`${env:PG_PASSWORD}`, `${secret:SNOWFLAKE_KEY}` — которая разрешается в момент, когда источник
подключается, внутри организации, от имени которой выполняется запрос (REQ-1695). [tool-verified:
`provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

`${env:VAR}` читает переменную окружения процесса развёртывания и не требует привязки. `${secret:NAME}`
именует секрет, которым владеет организация, поэтому он разрешается только в рамках собственных
операций этой организации: интроспекционные точки администрирования и терминал запросов, к которым
обращается каждая поверхность, устанавливают эту привязку. [tool-verified: `provisa/pgwire/_pipeline.py` `_execute_plan`]

Куда указывает ссылка, зависит от того, как был зарегистрирован источник:

- **Из конфигурации.** Вы сами пишете ссылку. `${env:VAR}` читает переменную окружения процесса
  развёртывания; `${secret:NAME}` читает хранилище секретов организации (см. [Секреты](secrets.md)).
  Файл — это запись, и Provisa копирует ссылку в `password_ref` дословно.
- **Из формы Sources.** Ссылка, введённая в поле пароля, также сохраняется дословно. *Буквальный*
  пароль записывается в хранилище секретов организации под именем
  `source_<id>_password` — зашифрованно, и его нельзя прочитать обратно по имени — а строка сохраняет
  `${secret:source_<id>_password}`, который его именует. [tool-verified:
  `provisa/api/admin/schema_common.py` `persist_source_password`]

Повторный ввод пароля для существующего источника ротирует эту одну запись хранилища, а не создаёт
вторую. Удаление источника убирает запись, созданную для него Provisa, и только её: ссылка, которую
вы написали сами, именует секрет, которым владеете вы по своим собственным причинам, и её никто не
трогает.
[tool-verified: `provisa/api/admin/schema_mutation.py` `delete_source`]

`password_ref` не переносится между окружениями (REQ-1491). Ветка (branch) или скопированное окружение
предоставляет собственные значения подключения, а хранилище секретов, которое именует ссылка, принадлежит
тому окружению, которое её предоставило. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

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

## Пользовательские коннекторы (REQ-1177)

Нативные движки федерации — Postgres, DuckDB и ClickHouse — получают достижимость до нового типа источника, когда оператор объявляет для него коннектор в `config/custom_connectors.yaml`. Код не требуется. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

Расширяемость коннекторов сама по себе появилась раньше. Движок Trino давно расширяем на собственном уровне — один универсальный JDBC-коннектор, параметризуемый под тип источника, тело каталога `.properties` на тип, и собственные плагины Provisa для коннекторов Trino (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 привносит ту же конфигурационно-управляемую расширяемость в два нативных, безкластерных движка, которые ранее имели фиксированный набор коннекторов.

Конфигурация поставляется пустой. Встроенные коннекторы обеспечивают достижимость «из коробки»; всё в этом файле создаётся оператором. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Установите `PROVISA_CUSTOM_CONNECTORS`, чтобы указать на другой путь (полезно для тестов).

### Виды дескрипторов

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

### Переменные шаблона

Каждое значение `server_options`, значение `user_mapping`, `attach_template` и `scan_template` может использовать placeholder-ы вида `{field}`. Доступные поля: [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, а также любой ключ из `federation_hints`. Шаблоны подключения DuckDB также получают `{alias}` — внутренний псевдоним каталога, который Provisa присваивает подключённой базе данных.

Шаблон, ссылающийся на неизвестное поле, приводит к падению сразу в момент подключения, выявляя несоответствие дескриптора и источника до того, как некорректный DDL достигнет движка.

### Примеры

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

## Поля конфигурации источника

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
| `mapping` | Нет | `{}` | Настройки коннектора, специфичные для типа, для NoSQL- и SaaS-источников (например, `auth_type` SharePoint, `use_token` Splunk) (REQ-251) |
| `allowed_domains` | Нет | `[]` | Ограничить источник конкретными доменами; пусто = без ограничений |
| `description` | Нет | `""` | Описание для человека |

---

## Источники Kafka

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

## Видимость столбцов

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

### Связи, опирающиеся на junction

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

## Правила безопасности на уровне строк

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

## Функции и вебхуки

### Функции БД

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
| `visible_to` | Нет | `[]` | Роли, которым разрешено вызывать эту функцию |
| `writable_by` | Нет | `[]` | Роли, которым разрешено вызывать это как мутацию |
| `domain_id` | Нет | `""` | Домен, которому принадлежит эта функция |
| `description` | Нет | `null` | Описание поля GraphQL |
| `kind` | Нет | `mutation` | `"query"` или `"mutation"` (REQ-205) |

### Вебхуки

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

## Аутентификация

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

## Маршрутизация выполнения

**Прямое выполнение** — однопоточные запросы к RDBMS направляются к нативному драйверу для задержки менее 100 мс (REQ-027). Для поддержки этого пути источникам требуются как запись в `SOURCE_TO_DIALECT`, так и запись в `SOURCE_TO_CONNECTOR` (REQ-229).

**Федеративное выполнение** — многопоточные запросы и источники без прямого драйвера направляются через движок федерации (REQ-028). Provisa включает встроенный движок федерации; для крупномасштабных развёртываний укажите на собственный совместимый кластер (REQ-226).

**Статистика** — при регистрации Provisa выполняет `ANALYZE` для каждой опубликованной таблицы, чтобы подготовить оптимизатор на основе стоимости (количество строк, доля null-значений, число различных значений, min/max). Ошибки логируются и не блокируют регистрацию (REQ-275).

---

## Графовые и семантические источники

### Neo4j

Зарегистрируйте графовую базу данных Neo4j как запрашиваемый источник. Стюарды пишут запросы Cypher, проецирующие скалярные значения; Provisa кеширует результаты и представляет их как типы GraphQL (REQ-295).

Запросы Cypher должны использовать акцессоры свойств в предложении `RETURN` (`RETURN n.id AS id, n.name AS name`) — возврат объектов узлов отклоняется в момент регистрации (REQ-296).

#### Регистрация через файл конфигурации (REQ-1668)

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

#### Регистрация таблицы в UI (REQ-1670)

У источника neo4j нет таблиц для перечисления, поэтому форма регистрации таблицы запрашивает саму таблицу вместо того, чтобы предлагать её на выбор. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Выберите источник neo4j и домен. Селекторы схемы и таблицы, флажок обнаружения и селектор водяного знака не отображаются; источник никогда не интроспектируется.
2. Введите имя таблицы и Cypher-запрос. Cypher должен проецировать скаляры (`RETURN a.name AS name`); проекция, возвращающая узел или список, сообщается как ошибка.
3. Нажмите Preview. Форма выполняет Cypher с `LIMIT 5` через GraphQL-запрос `neo4jPreview` и заполняет список столбцов из полученных строк, типизированных как `text`, `integer`, `double`, `boolean` или `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Неудачный предпросмотр оставляет Cypher в редакторе и показывает сообщение.
4. Настройте видимость, псевдонимы или маскирование как для любой таблицы, затем зарегистрируйте. Форма отказывается отправляться до тех пор, пока предпросмотр не типизировал столбцы, а сервер отказывается регистрировать таблицу neo4j без Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

Cypher хранится вместе с таблицей как `queryTemplate`, отображается в представлении чтения таблицы и сохраняется точно так же, как при регистрации через файл конфигурации: строка `api_sources` и строка `api_endpoints`, которые гидратируются при следующем запуске. Редактирование таблицы заново сохраняет отредактированный Cypher.

#### Регистрация через admin REST

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

#### Регистрация через файл конфигурации и через UI (REQ-1683)

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

Однопоточные части маршрутизируются напрямую (REQ-027). Межисточниковые JOIN федерируются с автоматическим приведением типов (REQ-028, REQ-552).
