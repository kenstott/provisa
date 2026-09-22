# Справочник по конфигурации

Provisa настраивается через YAML-файл (по умолчанию: `config/provisa.yaml`). (REQ-528)

## Включения (Includes) (REQ-1669)

Разделите конфигурацию на несколько файлов с помощью `includes:`. Включающий файл перечисляет пути к фрагментам под этим ключом; Provisa объединяет их перед валидацией, получая тот же результат, как если бы всё было написано в одном файле.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Файл-обёртка, содержащий только `includes:`, допустим. `load_control_plane` читает через включения, поэтому раздел `control_plane:` берётся из того включённого файла, который его задаёт. [tool-verified: `provisa/core/config_loader.py:154-166`]

**Правила слияния** [tool-verified: `provisa/core/config_loader.py:104-146`]

- Пути разрешаются относительно включающего файла. Абсолютные пути используются как есть.
- Секции-списки (`sources`, `tables`, `domains`, `relationships`, `roles`, …) дополняются — записи фрагмента следуют за записями включающего файла.
- Скалярный ключ или ключ-словарь, который включающий файл не задаёт, берётся из фрагмента.
- Ключ, заданный обоими файлами с разными значениями, — конфликт; загрузка завершается ошибкой с указанием ключа.
- Одинаковые значения в обоих файлах конфликтом не считаются.
- Включения вкладываются друг в друга. Файл, включающий сам себя — напрямую или через другой фрагмент, — отклоняется.
- `includes` учитывается только при загрузке и никогда не появляется в проверенной конфигурации.


## Источники

```yaml
sources:
  - id: sales-pg           # unique identifier
    type: postgresql
    host: postgres
    port: 5432
    database: provisa
    username: provisa
    password: ${env:PG_PASSWORD}  # secret resolution
    pool_min: 1
    pool_max: 5
    use_pgbouncer: false
    pgbouncer_port: 6432
```

У всех источников общий набор полей. [tool-verified: `provisa/core/models.py:129-212`]

| Поле | По умолчанию | Примечания |
| ------- | --------- | ------- |
| `id` | обязательно | Латинские буквы и цифры, дефисы, подчёркивания |
| `type` | обязательно | См. таблицу ниже |
| `host` | `""` | Имя хоста или IP-адрес |
| `port` | `0` | `0` означает, что каждый коннектор подставляет собственное значение по умолчанию — центральной карты портов по умолчанию нет |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Поддерживает ссылки на учётные данные `${env:VAR}` и `${secret:NAME}` — см. [Секреты](secrets.md) |
| `path` | `null` | Путь к файлу или URI для файловых источников |
| `base_url` | `null` | Базовый URL для API-источников |
| `pool_min` / `pool_max` | `1` / `5` | Границы пула соединений |
| `cache_enabled` | `true` | Включает кэширование для всех таблиц этого источника |
| `cache_ttl` | `null` | Секунды; `null` наследует глобальное значение по умолчанию |
| `federation_hints` | `{}` | Расширенные параметры конкретного коннектора (dict[str,str]); см. справочник по типам ниже. REQ-281 |
| `mapping` | `{}` | DSL сопоставления для redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Ограничивает источник конкретными идентификаторами доменов; пусто = без ограничений |
| `description` | `""` | |

### Поддерживаемые типы источников [tool-verified: `provisa/core/models.py:36-101`]

| Тип | Способ подключения | Примечания |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Пул asyncpg; PgBouncer включается через `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (файл БД) | Расширение DuckDB firebird community (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `greenplum` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `tidb` | host/port | Использует драйвер/диалект MySQL (REQ-950) |
| **Облачное хранилище данных** | | |
| `snowflake` | host/port + `federation_hints` | В hints обязателен `account` |
| `bigquery` | `federation_hints` | Обязателен `project`; аутентификация через `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | В hints обязателен `http_path` |
| `fabric` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `synapse` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Hint `secure` переключает TLS; порт по умолчанию 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | REST-эндпоинт контроллера |
| `druid` | host/port | Avatica-эндпоинт брокера |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI таблицы) | `delta_scan` DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `iceberg` | `path` (URI таблицы) | `iceberg_scan` DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `hudi` | `path` (URI таблицы) | Движок Hudi в ClickHouse, без копирования (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Бэкенд хранилища в `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + ключи S3 в `mapping` | Отдельный тип; всегда хранилище S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Обычные поля подключения; без DSL сопоставления |
| `cassandra` | host/port | Обычные поля подключения; без DSL сопоставления |
| `redis` | host/port + DSL `mapping` | |
| **Потоковая передача** | | |
| `kafka` | только регистрация | Реальная конфигурация в `kafka_sources[]`; см. §Kafka ниже |
| `websocket` | host/port/path + `federation_hints` | Внешний WebSocket-фид |
| `rss` | host/port/path + `federation_hints` | Фид RSS 2.0 / Atom |
| **Граф/Семантика** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Файлы** | | |
| `sqlite` | `path` | Всегда маршрутизируется через движок (без прямого пула) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (каталог) | Обход по маске; представляет CSV/Parquet/XLSX/JSON как таблицы |
| **API/Удалённые источники** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port или `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + опционально `mapping` | Заголовки, forward-client-headers, таймаут в `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (расположение Flight) | Расширение DuckDB airport (REQ-899) |
| `ingest` | приёмник push-событий | Внешние сервисы отправляют JSON-события через POST |
| **SaaS** | | |
| `sharepoint` | `base_url` или `host` + `mapping` | Аутентификация через `mapping.auth_type` |
| `splunk` | `host`/`port` или `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Отдельная модель `GovDataSource`; см. §GovData ниже |
| **Качество данных** | | |
| `soda` | host/port, направленные на pgwire Provisa | Требует extra `soda`; Elastic License 2.0, только self-hosted (REQ-1443) |
| `great_expectations` | host/port, направленные на pgwire Provisa | Требует extra `gx`; Apache 2.0 (REQ-1443) |

### Справочник по типам источников

Для типов, требующих неочевидной настройки, ниже приведена короткая справка. Типы RDBMS (postgresql, mysql и т. д.) используют только общие поля выше — дополнительный раздел не нужен.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Источники `govdata` используют отдельную модель верхнего уровня, `GovDataSource`, а не обычный `Source`. (REQ-540) Доступ разделён по группам subject.

```yaml
sources:
  - id: federal-data
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    api_key: ${env:GOVDATA_API_KEY}   # optional
    start_year: 2020                   # optional year filter
    end_year: 2024                     # optional year filter
```

Каждый subject соответствует одной или нескольким схемам GovData. Настройка источника `govdata` с указанием subject автоматически открывает все схемы этого subject. (REQ-540)

| Subject | Схемы |
| --------- | --------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ`, `econ_reference` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec` |

Схемы `ref` и `geo` всегда включены как связующие схемы — они не настраиваются и не перечислены выше. (REQ-541) Используйте subject `ALL`, чтобы предоставить доступ ко всем схемам. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

Строка `kafka` в `sources:` предназначена только для регистрации. Метод `details()` её коннектора возвращает `{}` — реальная конфигурация находится в блоке `kafka_sources[]` верхнего уровня, а не в записи `sources:`. Kafka всегда является VIRTUAL_SOURCE (маршрутизируется через движок; без прямого пула). [tool-verified: `provisa/transpiler/router.py:44-63`]

```yaml
kafka_sources:
  - id: event-stream
    bootstrap_servers: kafka:9092
    schema_registry_url: http://schema-registry:8081  # optional
    topics:
      - id: order-created
        topic: orders.events
        default_window: 1h          # auto-injected time bound
        schema_source: manual       # manual, registry, or sample
        value_format: json
        discriminator:              # filter shared topic by message type
          field: event_type
          value: OrderCreated
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: amount
            type: double
          - name: metadata
            type: varchar           # raw JSON for complex nested data
      - id: order-shipped
        topic: orders.events        # same physical topic
        default_window: 1h
        discriminator:
          field: event_type
          value: OrderShipped
        columns:
          - name: event_type
            type: varchar
          - name: order_id
            type: integer
          - name: shipped_at
            type: timestamp
```

**Временное окно** — `default_window` ограничивает каждый запрос недавним периодом времени, предотвращая неограниченное чтение из высоконагруженных топиков. (REQ-148) Формат: `1h`, `30m`, `7d`, `60s`. По умолчанию `1h`. Автоматически подставляется как `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Клиенты могут переопределить это собственным фильтром `_timestamp` в аргументе `where` GraphQL.

**Дискриминатор** — Несколько конфигураций топиков могут указывать на один и тот же физический топик Kafka с разными значениями `discriminator`, порождая отдельные типы GraphQL. (REQ-149) Дискриминатор автоматически подставляется как условие WHERE.

**Источник схемы**

| Значение | Поведение |
| ------- | ---------- |
| `registry` | Получить схему из Confluent Schema Registry |
| `manual` | Определить столбцы прямо в конфигурации (Schema Registry не нужен) |
| `sample` | Автоматически определить схему по примерам сообщений |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` в `federation_hints` обязателен. `warehouse`, `role` и `schema` опциональны.

```yaml
sources:
  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    database: MY_DB
    federation_hints:
      account: myorg-myaccount     # required
      warehouse: COMPUTE_WH
      role: PROVISA_ROLE
      schema: PUBLIC               # remote schema override
```

#### Databricks [tool-verified: `provisa/executor/drivers/databricks.py:34-52`]

`http_path` в `federation_hints` обязателен. `password` содержит personal access token. `catalog` опционален (передаётся в SQL/hints, а не в поле `database`).

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required
      catalog: my_unity_catalog              # optional
```

#### BigQuery [tool-verified: `provisa/federation/connector_duckdb.py:238`]

`project` в `federation_hints` обязателен. Аутентификация использует `GOOGLE_APPLICATION_CREDENTIALS` (путь к файлу ключа сервисного аккаунта) или Application Default Credentials в окружении движка.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Оба используют T-SQL поверх TDS с аутентификацией Azure AD. Аутентифицируйтесь через `az login` (для разработки) или управляемое удостоверение (для продакшена) — движок читает учётные данные через `DefaultAzureCredential` из `azure-identity`. Данные подключения берутся из переменных окружения: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) или `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), либо через `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` в `federation_hints` включает TLS на HTTP-интерфейсе. Порт по умолчанию `8123` (без шифрования) или `8443` (при `secure: "true"`). `schema` в `federation_hints` переопределяет удалённую схему. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

```yaml
sources:
  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"    # uses port 8443; omit to use 8123
      schema: analytics
```

#### Delta Lake / Iceberg [tool-verified: `provisa/federation/connector_duckdb.py:291-327`]

`path` — это URI таблицы (S3, GCS, ADLS или локальный). Доступ к объектному хранилищу требует учётных данных в `federation_hints`. Для Cloudflare R2 добавьте `account_id`.

```yaml
sources:
  - id: events-delta
    type: delta_lake
    path: s3://my-bucket/data/events
    federation_hints:
      access_key_id: ${env:S3_ACCESS_KEY}
      secret_access_key: ${env:S3_SECRET}

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

#### Hive / Hive S3 [tool-verified: `provisa/federation/trino_connectors.py:244-363`]

`host` и `port` указывают на Hive Thrift metastore (порт по умолчанию 9083). Для `hive` задайте `mapping["storage"]`, чтобы выбрать бэкенд объектного хранилища. Отсутствующие обязательные ключи приводят к явной ошибке — без запасных значений. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` — отдельный тип, который всегда объявляет хранилище S3 (REQ-229); `mapping.storage` не нужен.

```yaml
sources:
  - id: hive-s3-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: s3
      endpoint: https://s3.us-east-1.amazonaws.com
      access_key_id: ${env:AWS_ACCESS_KEY_ID}
      secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      path_style: true           # required for MinIO and non-AWS S3-compatible endpoints

  - id: hive-adls-lake
    type: hive
    host: metastore.internal
    port: 9083
    mapping:
      storage: adls
      storage_account: mystorageaccount
      access_key: ${env:ADLS_ACCESS_KEY}
      # sas_token: ${env:ADLS_SAS_TOKEN}   # alternative to access_key
```

Допустимые значения `mapping.storage`: `hadoop` (по умолчанию), `hdfs`, `local`, `s3`, `azure`, `adls`. Ключи `mapping` для S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Ключи `mapping` для ADLS: `storage_account`, `access_key` или `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Использует DSL `mapping`. `mongodb` и `cassandra` используют обычные поля подключения и НЕ используют DSL сопоставления.

```yaml
sources:
  - id: my-redis
    type: redis
    host: redis.internal
    port: 6379
    password: ${env:REDIS_PASSWORD}
    mapping:
      tables:
        - name: sessions
          key_pattern: "sessions:*"
          key_column: key           # default "key"
          value_type: hash          # hash | string | zset | list; default hash
          columns:
            - name: user_id
              data_type: VARCHAR
              field: user_id        # Redis hash field name
            - name: expires_at
              data_type: BIGINT
              field: expires_at
```

#### Elasticsearch [tool-verified: `provisa/core/trino_catalog_files.py:78-104`]

```yaml
sources:
  - id: my-es
    type: elasticsearch
    host: es.internal
    port: 9200
    username: elastic
    password: ${env:ES_PASSWORD}
    mapping:
      tls: true
      tables:
        - name: logs
          index: app-logs-*
          discover: false
          columns:
            - name: timestamp
              data_type: TIMESTAMP
              path: "@timestamp"
            - name: level
              data_type: VARCHAR
              path: level
            - name: message
              data_type: VARCHAR
              path: message
```

#### Prometheus [tool-verified: `provisa/core/trino_catalog_files.py:107-124`]

`mapping.url` переопределяет `host:port`, когда заданы оба.

```yaml
sources:
  - id: my-prometheus
    type: prometheus
    mapping:
      url: http://prometheus.internal:9090
      tables:
        - name: http_requests
          metric: http_requests_total
          labels_as_columns: [method, status, handler]
          value_column: value      # default "value"
          default_range: 1h        # default "1h"
```

#### Google Sheets [tool-verified: `provisa/federation/connector_duckdb.py:273-275`]

`spreadsheet_id` в `federation_hints` обязателен. Аутентификация использует SECRET `gsheet` DuckDB, подготавливаемый в момент присоединения.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Файловые источники (csv / parquet / sqlite / files)

`path` обязателен. `files` обходит каталог в поисках файлов CSV, Parquet, XLSX и JSON, представляя каждый как таблицу. Все файловые источники VIRTUAL (маршрутизируются через движок; без прямого пула). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Наборы данных Kaggle** нельзя добавить через этот файл — они требуют действующего токена и выбора набора данных, доступного в форме источников (Sources → Subscriptions → Kaggle). Источник Kaggle, экспортированный в YAML, выглядит как `type: files` с `kaggle_owner` и `kaggle_ref` в `federation_hints`. Для повторной загрузки из Kaggle нужна мутация `refreshKaggleSource` или поток обновления в UI, а не правка YAML. См. [Наборы данных Kaggle](sources.md#kaggle-datasets) в справочнике по типам источников.

#### API / удалённые источники

**openapi** — задайте `base_url` как базовый URL OpenAPI. Обнаружение схемы читает спецификацию OpenAPI при запуске.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — задайте `base_url`. Опциональные ключи `mapping`: `headers` (словарь статических заголовков), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

```yaml
sources:
  - id: orders-gql
    type: graphql_remote
    base_url: https://orders.internal/graphql
    mapping:
      headers:
        X-Api-Key: ${env:ORDERS_API_KEY}
      forward_client_headers: true
      timeout_seconds: 30
```

**airport** — `base_url` — это расположение сервера Arrow Flight. Расширение DuckDB airport (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — используют `host`, `port`, `path` и `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

```yaml
sources:
  - id: market-feed
    type: websocket
    host: feed.example.com
    port: 443
    path: /ws/v1
    federation_hints:
      use_ssl: "true"
      subscribe_payload: '{"action":"subscribe","channels":["ticker"]}'
      event_path: data

  - id: news-rss
    type: rss
    host: feeds.example.com
    port: 443
    path: /rss/latest
    federation_hints:
      use_ssl: "true"
      poll_interval: "300"      # seconds
      # feed_url: https://...  # overrides host/port/path when set
```

**sharepoint** [tool-verified: `provisa/federation/trino_connectors.py:394-423`]

```yaml
sources:
  - id: my-sharepoint
    type: sharepoint
    base_url: https://myorg.sharepoint.com/sites/data
    username: ${env:SP_CLIENT_ID}
    password: ${env:SP_CLIENT_SECRET}
    database: ${env:SP_TENANT_ID}
    mapping:
      auth_type: CLIENT_CREDENTIALS   # default
      # certificate_path: /path/to/cert.pem
      # certificate_password: ${env:CERT_PASSWORD}
```

**splunk** [tool-verified: `provisa/federation/trino_connectors.py:426-457`]

```yaml
sources:
  - id: my-splunk
    type: splunk
    host: splunk.internal
    port: 8089
    password: ${env:SPLUNK_TOKEN}
    database: search           # Splunk app name (optional)
    mapping:
      use_token: true          # default; false = username/password auth
      datamodel_filter: ""     # optional Splunk Data Model filter
      disable_ssl_validation: false
```

#### Проверки качества данных (soda / great_expectations)

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

Источник-проверка указывает на собственный эндпоинт pgwire Provisa, поэтому один драйвер postgres сканирует федеративное представление таблицы, поддерживаемой Snowflake или Iceberg. Личность сканирования объявляется, а не наследуется — политика применяется именно к этому соединению, и отфильтрованный набор строк не должен приводить к незаметно проходящей проверке. Ключи подключения берутся из `mapping`: `host`, `port`, `database`, `user`, `password`.

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

Каждая таблица результатов содержит `dq_contract` — YAML-контракт Soda или JSON-набор Great Expectations, дословно. Столбцы, watermark и продвижения выводятся из него; полное описание вывода см. в разделе [Проверки качества данных](sources.md#data-quality-checkers-req-1443).

**Выбор на этапе установки.** Проверка не подключается статически — сканирование выполняется в дочернем интерпретаторе, и библиотека устанавливается только тогда, когда оператор её называет. Каждый путь установщика (`install.sh`, `packaging/linux/first-launch.sh` и мастер для macOS через `PROVISA_DQ_CHECKER`) записывает выбор в `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` читает этот ключ и экспортирует `PROVISA_EXTRAS`, который `docker-compose.app.yml` передаёт как build-аргумент в `ARG PROVISA_EXTRAS` файла `Dockerfile`: [tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS` (уровень Docker) | Установка в нативный venv |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Установка демо-набора данных повышает `none` до `gx` и сообщает об этом, потому что демо-конфигурация регистрирует набор Great Expectations поверх `pet_store.pets`, и его карточке качества иначе было бы нечего показывать. Указание `soda` оставляет `soda` без изменений.

Доступ к демо через pip, минуя установщик, пропускает этот шаг мастера, поэтому extra `demo` несёт ту же проверку: `pip install 'provisa[embedded,demo]'` — именно это нужно `provisa run --demo` для работы сканирования. Без этого сканирование сообщает `data-quality checker 'great_expectations' is not installed`, называя команду установки.

Любое другое значение останавливает запуск, вместо того чтобы стартовать без проверки, которую запросил оператор. Extra `soda` подтягивает `soda-postgres`; `gx` подтягивает `great-expectations[postgresql]`. Soda Core распространяется под Elastic License 2.0 — `config/capabilities.yaml` помечает этот вариант как `cloud_eligible: false`, и облачная плоскость его отклоняет.

## Домены

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Именование

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Соглашение об именовании

Механизм именования — единственный источник истины для имён, видимых клиенту; физические имена столбцов бэкенда клиентам никогда не раскрываются. (REQ-194) Каждый язык запросов выводит имя столбца из его `column.alias`, если он задан, иначе — из физического имени столбца по настроенному соглашению. (REQ-194)

Соглашение GraphQL — одно из трёх предустановленных перечислений. (REQ-416) Старые произвольные строки (`none`, `snake_case`, `camelCase`, `PascalCase`) объявлены устаревшими. (REQ-416)

| Пресет | По умолчанию | Имена типов | Имена полей | Имена мутаций |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | да | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Соглашение GraphQL по умолчанию — `apollo_graphql`, которое даёт имена полей и мутаций в camelCase. (REQ-194, REQ-416) Соглашение SQL отдельное, по умолчанию `snake_case`, применяется через `apply_sql_name()`; соглашение GraphQL применяется через `apply_gql_name()`, а имя CQL выводится из имени GraphQL. (REQ-194)

`domain_prefix: bool` — независимая опция, действующая вне зависимости от выбранного пресета. (REQ-416)

Явный `column.alias` — каноническое имя: SQL использует его дословно без применения соглашения, GraphQL применяет к нему своё соглашение, а CQL выводится из имени GraphQL. (REQ-194)

Переопределение на уровне источника:

```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Переопределение на уровне таблицы:

```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Префикс домена

Когда `domain_prefix: true`, ко всем именам полей и типов GraphQL добавляется идентификатор домена через разделитель из двойного подчёркивания: (REQ-154)

| Таблица | Домен | Имя поля |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Это предотвращает коллизии имён, когда в разных доменах есть таблицы с одинаковым именем, и делает запросы самодокументируемыми.

### Правила именования

Regex-правила, применяемые к именам таблиц при генерации имён полей GraphQL. Применяются по порядку до разрешения уникальности. (REQ-542)

## Таблицы

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    alias: purchase_orders     # optional: override GraphQL name
    description: "Customer purchase orders"  # optional: GraphQL description
    columns:
      - name: id
        visible_to: [admin, analyst]
        writable_by: []           # read-only (empty = no writes)
      - name: email
        visible_to: [admin, analyst]
        writable_by: [admin]      # only admin can mutate
        unmasked_to: [admin]      # admin sees raw, analyst sees masked
        mask_type: regex
        mask_pattern: "^(.{2}).*(@.*)$"
        mask_replace: "$1***$2"
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        writable_by: [admin]
        unmasked_to: [admin]
        mask_type: constant
        mask_value: "0"
      - name: created_at
        visible_to: [admin, analyst]
        writable_by: []           # nobody can write
        unmasked_to: [admin]
        mask_type: truncate
        mask_precision: month
    column_presets:               # auto-set values on insert/update
      - column: created_by
        source: header            # from request header
        name: X-User-ID
      - column: updated_at
        source: now               # current timestamp
```

### Псевдонимы

Псевдонимы таблиц и столбцов переопределяют имя GraphQL по умолчанию. (REQ-155) Полезно для:

- Переименования непонятных имён в базе данных (например, `tbl_cust_seg` → `customer_segments`)
- Избегания сокращений в слое API
- Создания чистого предметно-ориентированного словаря

### Описания

Описания таблиц и столбцов включаются в сгенерированный SDL GraphQL. (REQ-156) Они отображаются в проводнике документации GraphiQL и в запросах интроспекции. Задавайте их в конфигурационном YAML или через панель администрирования.

### Path (вычисляемое извлечение из JSON)

Столбцы могут извлекать значения из исходного столбца JSON/JSONB с помощью `path` в точечной нотации. (REQ-151) Это полезно для слабоструктурированных данных в сообщениях Kafka, документах MongoDB или столбцах JSONB PostgreSQL.

```yaml
columns:
  - name: payload
    type: varchar
    visible_to: []            # hide the raw JSON column
  - name: order_id
    type: integer
    path: payload.order_id    # extracts from payload column
    visible_to: [admin, analyst]
  - name: customer_name
    type: varchar
    path: payload.customer.name
    visible_to: [admin, analyst]
```

Формат path — `source_column.key1.key2...`. Компилятор генерирует `json_extract_scalar(source_column, '$.key1.key2')` в SQL. (REQ-151)

**Влияние на маршрутизацию:** столбцы с path используют операторы JSON PostgreSQL (`->>`), которые нативно поддерживаются прямой маршрутизацией PG. (REQ-152) Для источников, отличных от PostgreSQL (MySQL, SQL Server и т. д.), запросы со столбцами path автоматически маршрутизируются через движок федерации. (REQ-152) На мутации это не влияет, поскольку столбцы path — вычисляемые поля только для чтения. (REQ-153)

### Типы маскирования

| Тип | Поля | Описание |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (только строковые столбцы) |
| `constant` | `value` | Замена литералом (NULL, 0, MAX, MIN, произвольное значение) |
| `truncate` | `precision` | DATE_TRUNC (только столбцы даты/времени) |

## Связи (Relationships)

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one   # or: one-to-many

  - id: orders-to-reviews
    source_table_id: orders        # sales-pg source
    target_table_id: product_reviews  # reviews-mongo source
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV for this cross-source join
    refresh_interval: 600          # refresh every 10 minutes
```

### Автоматическая материализация

Установите `materialize: true` на связи, чтобы автоматически сгенерировать материализованное представление для межисточниковых JOIN. (REQ-158) Это позволяет избежать дорогостоящих федеративных запросов за счёт предварительного вычисления результата JOIN.

- Материализованные представления генерируются только для межисточниковых связей (JOIN в пределах одного источника уже быстрые) (REQ-159)
- Для связи, поддерживаемой через junction-таблицу, материализованное представление покрывает двухшаговый обход — переход к источнику, переход к junction, дискриминатор и собственные столбцы junction как атрибуты ребра. Junction считается отдельным звеном, поэтому ребро межисточниковое, если любая из трёх таблиц находится в другом источнике (REQ-1586)
- Материализованное представление изначально устаревшее и заполняется фоновым циклом обновления (REQ-160)
- Мутации любой из таблиц-источников помечают материализованное представление как устаревшее для повторного обновления (REQ-543)
- `refresh_interval` по умолчанию 300 секунд (5 минут) (REQ-543)

## Роли

```yaml
roles:
  - id: admin
    capabilities:
      - source_registration
      - table_registration
      - relationship_registration
      - security_config
      - query_development
      - full_results
      - admin
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Роли с `parent_role_id` наследуют возможности, доступ к доменам, права на столбцы и объекты, а также правила безопасности на уровне строк от родителя, при этом собственное правило безопасности на уровне строк дочерней роли для таблицы имеет приоритет. (REQ-215, REQ-1677) Цепочка разворачивается при запуске. (REQ-215)

### Возможности (Capabilities)

| Возможность | Описание |
| ----------- | ------------- |
| `source_registration` | Регистрация источников данных |
| `table_registration` | Регистрация таблиц |
| `relationship_registration` | Определение связей |
| `security_config` | Настройка безопасности на уровне строк, маскирования |
| `query_development` | Выполнение запросов |
| `full_results` | Обход ограничений выборки |
| `admin` | Все возможности |

## Правила безопасности на уровне строк (RLS)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

## Материализованные представления

```yaml
materialized_views:
  - id: mv-orders-customers
    source_tables: [orders, customers]
    join_pattern:
      left_table: orders
      left_column: customer_id
      right_table: customers
      right_column: id
      join_type: left
      # REQ-1586: add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Представления (управляемые вычисляемые наборы данных)

Представления — это вычисляемые наборы данных, определённые на SQL, с полным управлением на уровне столбцов. (REQ-133) Это управляемый механизм добавления агрегаций, преобразований и производных метрик в семантический слой. (REQ-136)

```yaml
views:
  - id: monthly-revenue
    sql: |
      SELECT DATE_TRUNC('month', created_at) AS month,
             region,
             SUM(amount) AS revenue,
             COUNT(*) AS order_count
      FROM orders
      GROUP BY 1, 2
    description: "Monthly revenue by region"
    domain_id: sales-analytics
    materialize: true
    refresh_interval: 3600
    columns:
      - name: month
        visible_to: [admin, analyst]
      - name: region
        visible_to: [admin, analyst]
      - name: revenue
        visible_to: [admin]
      - name: order_count
        visible_to: [admin, analyst]
```

| Поле | Обязательно | Описание |
| ------- | ---------- | ------------- |
| `id` | Да | Уникальный идентификатор представления |
| `sql` | Да | SQL-инструкция SELECT, определяющая представление |
| `domain_id` | Да | Домен для видимости схемы |
| `materialize` | Нет | `true` = периодическое обновление через CTAS, `false` = живое федеративное представление |
| `refresh_interval` | Нет | Секунды между обновлениями (только для материализованных, по умолчанию 300) |
| `description` | Нет | Отображается в SDL GraphQL |
| `alias` | Нет | Переопределяет имя GraphQL |
| `columns` | Да | Определения столбцов с видимостью, маскированием, описаниями |

### Материализованное или живое

- **`materialize: true`**: Provisa создаёт таблицу через CTAS и обновляет её по расписанию. (REQ-135) Быстрее запросы, но данные могут устаревать до `refresh_interval` секунд.
- **`materialize: false`**: Provisa создаёт федеративное представление. (REQ-135) Запросы всегда возвращают актуальные данные, но могут выполняться медленнее для сложных агрегаций.

Представления проходят тот же конвейер управления, что и таблицы, — безопасность на уровне строк, маскирование, выборка и видимость на основе ролей. (REQ-134) Это гарантирует, что новая семантика не может быть добавлена в платформу без надзора со стороны дата-стюарда. (REQ-136)

### Представления только для чтения

И представления с `materialize: true`, и с `materialize: false` предоставляют свой тип GraphQL только для запросов. Для отношений, поддерживаемых `view_sql`, не генерируются мутации insert, upsert, update или delete. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Кеш

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Иерархия кеша

Порядок разрешения TTL (наиболее конкретное значение побеждает): **таблица** > **источник** > **глобальное значение по умолчанию**. (REQ-544) Используется первое ненулевое значение.

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300              # global fallback: 5 minutes

sources:
  - id: sales-pg
    cache_enabled: true          # toggle caching for all tables in this source
    cache_ttl: 600               # source override: 10 minutes

tables:
  - source_id: sales-pg
    table: orders
    cache_ttl: 60                # table override: 1 minute (frequently changing)
  - source_id: sales-pg
    table: customers
    # no cache_ttl → inherits source TTL (600s)
```

Установка `cache_enabled: false` на источнике отключает кеширование для всех таблиц этого источника независимо от TTL на уровне таблицы. (REQ-544) Ключи кеша всегда включают `role_id` и значения контекста безопасности на уровне строк для разделения по соображениям безопасности. (REQ-544)

## Аутентификация

```yaml
auth:
  provider: simple           # none, firebase, keycloak, oauth, simple
  superuser:
    username: admin
    password: ${env:PROVISA_SUPERUSER_PASSWORD}
  simple:
    allow: true
    jwt_secret: ${env:PROVISA_JWT_SECRET}
    users:
      - username: admin
        password_hash: "$2b$12$..."
        roles: [admin]
  role_mapping:
    - claim: groups
      contains: data-analysts
      provisa_role: analyst
    default_role: analyst
```

### Типы провайдеров аутентификации

| Провайдер | Сценарий использования | Проверка токена |
| ---------- | ---------- | ----------------- |
| `simple` | Локальная разработка/тестирование. Пользователи определены в YAML. | JWT, подписанный `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (все методы). | `verify_id_token()` SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Сопоставление тенанта и клиентских ролей. | Проверка JWT на основе JWKS |
| `oauth` | Обобщённый OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS с discovery URL |
| `basic` | Автономные развёртывания. Учётные записи хранятся в собственном хранилище Provisa. | Пароль bcrypt или SCRAM-SHA-256 на pgwire |

Учётные данные суперпользователя (блок `superuser`) работают с любым провайдером и всегда разрешаются в роль admin со всеми возможностями. (REQ-125) Используется для первоначальной настройки до подключения внешней аутентификации.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Заставляет pgwire анонсировать SASL с `SCRAM-SHA-256`, так что пароль доказывается, а не передаётся открытым текстом. (REQ-1394) Это применяется только к провайдеру `basic` — ни один другой провайдер не хранит верификаторы RFC 5802, необходимые для SCRAM, — и привязка канала не предлагается.

Верификаторы нельзя получить из существующих хешей bcrypt. Верификатор записывается всякий раз, когда пароль проходит в открытом виде, поэтому первое SCRAM-соединение каждого пользователя следует за его следующей регистрацией, входом, сменой пароля или сбросом администратором. До этого момента соединения этого пользователя используют резервный обмен открытым текстом по TLS; протокол не раскрывает, кто уже перешёл на SCRAM.

### Ограничение частоты попыток входа (`auth.login_throttle`)

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Включено по умолчанию с указанными значениями; блок только настраивает их. (REQ-1393) Счётчик находится на уровне проверки учётных данных, поэтому неудачи через HTTP, pgwire и Bolt накапливаются для одного и того же субъекта, и блокировка действует на всех поверхностях. Она действует в рамках процесса: несколько воркеров API каждый допускают до `max_attempts`.

### Персональные токены доступа

Персональные токены доступа не требуют блока конфигурации — они всегда принимаются, и хранилище создаётся вместе с остальной схемой плоскости управления. (REQ-1263) Настраивается срок действия, который пользователь может запросить при выпуске: от 1 до 366 дней или "без срока действия" для токена, который не истекает. См. [Модель безопасности](security.md#personal-access-tokens).

### Взаимный TLS (Mutual TLS)

Проверка клиентских сертификатов настраивается через переменную окружения, а не в `provisa.yaml`, наряду с настройками сертификатов TLS, которые она расширяет. (REQ-1228)

| Переменная | По умолчанию | Значение |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | не задано | Пакет PEM с CA, которым разрешено подписывать клиентские сертификаты. Установка этой переменной включает проверку клиентских сертификатов |
| `PROVISA_MTLS_MODE` | `required`, как только задан CA | `required` или `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя (common name) сертификата совпадало с именем пользователя, от имени которого аутентифицируется соединение |

Для каждого протокола можно задать отдельное переопределение по той же схеме именования, что и настройки TLS. Режим, заданный без CA, или режим, не являющийся ни одним из допустимых значений, приводит к отказу от запуска, а не к обслуживанию соединений, которые оператор считает проверенными.

### Адресация организации через TLS

Настраивать ничего не нужно. В развёртывании с несколькими организациями pgwire и Bolt читают организацию из имени хоста, к которому обратился клиент, переданного в TLS ClientHello, точно так же, как HTTP читает его из заголовка `Host`. (REQ-1234) Клиент, подключающийся к `acme.provisa.dev`, запрашивает организацию `acme`; запрос отклоняется, если аутентифицированный субъект не является её участником. Подключение по IP-адресу не запрашивает организацию, что соответствует любому соединению в развёртывании с одной организацией.

### Полный пример конфигурации аутентификации (закомментировано)

```yaml
# auth:
#   provider: firebase
#
#   superuser:
#     username: admin
#     password: ${env:PROVISA_SUPERUSER_PASSWORD}
#
#   firebase:
#     project_id: ${env:FIREBASE_PROJECT_ID}
#     service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT}
#
#   # keycloak:
#   #   server_url: https://keycloak.example.com
#   #   # kc-tenant: set to your Keycloak tenant name (e.g. provisa)
#   #   client_id: provisa-app
#   #   client_secret: ${env:KEYCLOAK_CLIENT_SECRET}
#
#   # oauth:
#   #   discovery_url: https://login.example.com/.well-known/openid-configuration
#   #   client_id: provisa
#   #   client_secret: ${env:OAUTH_CLIENT_SECRET}
#   #   role_claim: groups
#   #   audience: provisa-api
#
#   role_mapping:
#     - claim: custom_claims.role
#       value: admin
#       provisa_role: admin
#     - claim: groups
#       contains: data-analysts
#       provisa_role: analyst
#     default_role: analyst
```

## Мутации Upsert

Для таблиц с первичным ключом Provisa автоматически генерирует поля мутации `upsert_<table>`. (REQ-212) Они компилируются в upsert на целевом диалекте — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` в PostgreSQL, `ON DUPLICATE KEY UPDATE` в MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Столбцы конфликта выводятся из метаданных первичного ключа. (REQ-212) Применяются все правила видимости столбцов и разрешений на запись.

## Distinct On

Аргумент `distinct_on` выбирает первую строку для каждого отдельного значения указанных столбцов. (REQ-213) Доступен для корневых полей запроса.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Компилируется в `SELECT DISTINCT ON (region) ...` в PostgreSQL. (REQ-213) Для диалектов, отличных от PG, используется резервный вариант с оконными функциями. (REQ-213)

## Пресеты столбцов (Column Presets)

Автоматически подставляют значения в столбцы при insert/update. (REQ-214) Определяются для каждой таблицы в конфигурации.

```yaml
tables:
  - source_id: sales-pg
    table: orders
    column_presets:
      - column: created_by
        source: header           # from request header
        name: X-User-ID
      - column: updated_at
        source: now              # current timestamp
      - column: source_system
        source: literal          # constant value
        value: "provisa"
```

| Источник | Поведение |
| -------- | ---------- |
| `header` | Подставляет значение из именованного HTTP-заголовка запроса |
| `now` | Подставляет `NOW()` (текущую метку времени) |
| `literal` | Подставляет константное значение |

Пресетные столбцы подставляются на этапе компиляции мутации, до генерации SQL. (REQ-214) Они не видны во входном типе мутации. (REQ-214)

## Наследуемые роли

Роли могут наследоваться от одной родительской роли через `parent_role_id`. (REQ-215) Цепочка разворачивается при запуске. (REQ-215) Дочерняя роль обладает объединением возможностей и доступа к доменам своих предков; столбец, метрика, функция или вебхук, предоставленные предку, предоставляются и дочерней роли; а правила безопасности на уровне строк предка применяются к дочерней роли для каждой таблицы, начиная с ближайшей роли, при этом собственное правило дочерней роли для таблицы заменяет родительское. (REQ-1677)

```yaml
roles:
  - id: admin
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Поддерживается многоуровневое наследование. (REQ-215) Явные возможности и domain_access дочерней роли объединяются с родительскими. (REQ-215) Родитель должен быть существующей ролью, не может быть самой ролью и не может замыкать цикл; каждое из этих условий проверяется и отклоняется при сохранении. (REQ-1677)

## Запланированные триггеры

Триггеры на основе cron, вызывающие URL вебхука по расписанию. (REQ-216) Использует APScheduler. (REQ-216)

```yaml
scheduled_triggers:
  - name: daily-report
    cron: "0 8 * * *"           # 8:00 AM daily
    webhook_url: https://hooks.example.com/daily-report
    enabled: true
  - name: hourly-sync
    cron: "0 * * * *"           # every hour
    webhook_url: https://hooks.example.com/sync
    enabled: false
```

Запланированные задачи управляются через панель администрирования (переключатель включения/выключения) или административную мутацию `toggle_scheduled_task`. (REQ-216)

## Формат OrderBy

OrderBy использует формат `{column: direction}` с перечислением направления из 6 значений: (REQ-200, REQ-201)

```graphql
{
  orders(order_by: [{created_at: desc_nulls_last}, {amount: asc}]) {
    id
    created_at
    amount
  }
}
```

| Направление | SQL |
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Сортировка по связям поддерживается через вложенные объекты: (REQ-202)

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Наблюдаемость

```yaml
observability:
  endpoint: "http://localhost:4319"   # OTLP collector; env OTEL_EXPORTER_OTLP_ENDPOINT overrides
  service_name: provisa               # env OTEL_SERVICE_NAME overrides
  sample_rate: 1.0                    # 0.0–1.0; TraceIdRatioBased sampler
  log_level: WARNING                  # env OTEL_LOG_LEVEL overrides
  compact_batch_size: 1000
  telemetry_filter:
    redact_sql_literals: false        # strip literal values from db.statement before export
    redact_attributes: []             # attribute keys dropped entirely before export
  # support_endpoint: ""              # env PROVISA_SUPPORT_OTLP_ENDPOINT; off by default
  support_telemetry_filter:
    redact_sql_literals: true         # default on — strip literals before sending to support
    redact_attributes: []             # additional keys dropped before sending to support
```

### Фильтры телеметрии [tool-verified]

Provisa использует два независимых пути экспорта OTLP: ваш внутренний коллектор и опциональный эндпоинт поддержки Provisa. (REQ-545) У каждого пути свой фильтр. Фильтры выполняются внутри оборачивающего `_FilteringExporter` перед тем, как спаны покидают процесс, — исходные объекты спанов никогда не изменяются. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — управляет тем, что попадает в ваш внутренний коллектор.

| Ключ | Тип | По умолчанию | Описание |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

**`support_telemetry_filter`** — управляет тем, что попадает на эндпоинт поддержки Provisa. Удаление SQL-литералов по умолчанию включено (`true`) на этом пути, поскольку данные запросов принадлежат вам. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Ключ | Тип | По умолчанию | Описание |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

Пример редактирования `db.statement` — при `redact_sql_literals: true` этот атрибут спана:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

становится:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Эндпоинт поддержки [tool-verified]

`support_endpoint` (или переменная окружения `PROVISA_SUPPORT_OTLP_ENDPOINT`) пересылает телеметрию в службу поддержки Provisa для диагностики. (REQ-548) Если не задано, никакие данные не покидают вашу инфраструктуру через этот путь. (REQ-548) Фильтр поддержки применяется независимо от внутреннего фильтра — вы можете удалять SQL-литералы из обоих экспортов, при этом всё равно делясь со службой поддержки данными о времени выполнения спанов и ошибках. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Определение протокола эндпоинта [tool-verified]

Provisa выбирает OTLP/HTTP или OTLP/gRPC на основе схемы URL эндпоинта. (REQ-549) URL, начинающиеся с `http://` или `https://`, используют OTLP/HTTP, с автоматическим добавлением `/v1/traces`, `/v1/metrics` и `/v1/logs`. (REQ-549) Любая другая схема использует OTLP/gRPC с `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Движок федерации

Настройка движка федерации опциональна. По умолчанию используется `duckdb` — без конфигурации, в процессе, без необходимости во внешнем сервисе (REQ-989). Выберите другой движок, если вам нужен масштаб MPP или вы хотите переиспользовать существующее хранилище данных.

Приоритет: переменная окружения `PROVISA_ENGINE` → сохранённое поле конфигурации `federation_engine` в панели администрирования → `duckdb`. Изменения вступают в силу при перезапуске сервиса. [tool-verified: `engine.py` `build_engine`]

### Обзор движков [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Ключ движка | Название | Диалект | MPP | Механизм внешней связи | Аутентификация |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Provisa Federation Engine | Trino SQL | Да | Каталоги Trino (широкий набор коннекторов) | Учётные данные JDBC |
| `trino-byo` | Trino | Trino SQL | Да | Так же, как `trino`; неуправляемый координатор | Учётные данные JDBC |
| `pg` | PostgreSQL | PostgreSQL | Нет | FDW / pg_duckdb | Учётные данные PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Нет | Нативный ATTACH расширения | Нет (в процессе) |
| `clickhouse` | ClickHouse (встроенный) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | chdb (в процессе, без аутентификации) |
| `clickhouse-server` | ClickHouse (сервер / облако) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | Учётные данные ClickHouse |
| `snowflake` | Snowflake | Snowflake | Да | Внешний stage + внешняя таблица | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Да | Внешние таблицы Unity Catalog через REST | `PROVISA_ENGINE_URL` (bearer-токен + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Да | Внешние таблицы BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Да | OneLake shortcuts → OPENROWSET | Azure AD (`az login` или управляемое удостоверение) |
| `synapse` | Azure Synapse | T-SQL | Да | ADLS OPENROWSET / внешние таблицы | Azure AD |
| `mysql` | MySQL | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `mariadb` | MariaDB | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `oracle` | Oracle Database | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `mssql` | Microsoft SQL Server | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `db2` | IBM Db2 | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `redshift` | Amazon Redshift | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `greenplum` | Greenplum | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `cockroachdb` | CockroachDB | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `yugabytedb` | YugabyteDB | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `opengauss` | openGauss | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `tidb` | TiDB | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `singlestore` | SingleStore | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `vertica` | Vertica | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `exasol` | Exasol | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `teradata` | Teradata Vantage | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `saphana` | SAP HANA | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `sapase` | SAP ASE (Sybase) | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `sqlanywhere` | SAP SQL Anywhere | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `monetdb` | MonetDB | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `firebird` | Firebird | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |
| `sqlalchemy` | Другая реляционная база данных (по URL подключения) | По диалекту | Нет | Нет (только приземление данных) | Учётные данные по диалекту |

### Справочник по движкам

#### trino / trino-byo

`trino` — управляемый координатор Provisa; `trino-byo` подключается к вашему собственному кластеру Trino. Оба используют Trino SQL и обладают самым широким охватом типов источников.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Выполняет федерацию через расширения postgres_fdw (SQL/MED) и pg_duckdb. Однонодовый режим; без MPP. Лучше всего подходит, если ваши данные уже находятся в PostgreSQL и вы хотите соединять несколько удалённых источников.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### duckdb

В процессе; без внешнего сервиса. Движок по умолчанию (REQ-989). `PROVISA_DATA_DIR` определяет, где находится встроенное хранилище (по умолчанию `~/.provisa`).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Хранилище материализации по умолчанию — `~/.provisa/materialize.duckdb` — единственный движок с хранилищем по умолчанию, отличным от PostgreSQL.

#### clickhouse (встроенный) / clickhouse-server

`clickhouse` использует chdb (в процессе). `clickhouse-server` подключается к внешнему экземпляру ClickHouse или ClickHouse Cloud. Оба читают Delta Lake, Iceberg и Hudi напрямую через нативные табличные движки ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### snowflake

Движок как хранилище данных: Snowflake выполняет запросы; Provisa передаёт данные источников через внешние stage.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### databricks

Внешние таблицы Unity Catalog соединяют источники, управляемые Provisa, с Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### bigquery

Внешние таблицы BigQuery и BigLake. Проект берётся из URL или `GOOGLE_CLOUD_PROJECT`; аутентификация через ключ сервисного аккаунта.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### fabric / synapse

Оба используют T-SQL поверх TDS с аутентификацией Azure AD (`az login` или управляемое удостоверение). Опустите `PROVISA_ENGINE_URL`, чтобы читать данные подключения из переменных окружения.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### Реляционные движки (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) и `sqlalchemy`

Один ключ на каждую реляционную базу данных, доступную по сети, все работают на одной и той же среде выполнения "только приземление" (без федерации к внешним источникам): каждый источник приземляется в хранилище и запрашивается там же. Ключ выбирает базу данных; `PROVISA_ENGINE_URL` несёт DSN в формате, который принимает её диалект. `sqlalchemy` — универсальный вариант для базы данных без собственного ключа. Встроенные в файл хранилища (SQLite, Access) не предлагаются — сервер должен быть доступен по сети.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

### Хранилище материализации

Когда источник не может подключиться напрямую (нет коннектора ATTACH для выбранного движка), он приземляется в хранилище материализации движка. Порядок разрешения: явный `PROVISA_MATERIALIZE_URL` → объявленное значение по умолчанию движка → явная ошибка (без незаметного резервного варианта). [tool-verified: `engine.py` `materialize_store`]

DuckDB объявляет свой встроенный файл (`~/.provisa/materialize.duckdb`) как значение по умолчанию. Все остальные движки по умолчанию используют `TENANT_DATABASE_URL` (PostgreSQL). Переопределить можно для любого движка через `PROVISA_MATERIALIZE_URL`.

### Подсказки федерации для отдельных источников

Расширенные параметры подключения, которые не помещаются в стандартные поля host/port/user/password, задаются в `federation_hints` источника. Ключи подсказок для конкретных типов см. в справочнике по типам источников выше. Сводный пример:

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required for Databricks sources

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount
      warehouse: COMPUTE_WH

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"           # enable TLS on the HTTP interface

  - id: r2-parquet
    type: parquet
    path: s3://my-bucket/data/events.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}   # Cloudflare R2 account (S3-compatible)
```

Для источников Google Cloud задайте `GOOGLE_APPLICATION_CREDENTIALS` — путь к файлу ключа сервисного аккаунта. Для Fabric и Synapse аутентифицируйтесь через `az login` (для разработки) или управляемое удостоверение (для продакшена) — движок читает учётные данные через `DefaultAzureCredential` из `azure-identity`.

## Переменные окружения

| Переменная | По умолчанию | Описание |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI хранилища плоскости управления (SQLAlchemy async); принимает `sqlite+aiosqlite://…` / `duckdb://…` для встроенного desktop-хранилища (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI реестра платформы (каталог тенантов, реестр движков); обязателен при запуске, без резервного варианта (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` использует встроенный fakeredis вместо сервера Redis — без Docker (REQ-829) |
| `PG_HOST` | `localhost` | Хост PostgreSQL |
| `PG_PORT` | `5432` | Порт PostgreSQL |
| `PG_DATABASE` | `provisa` | База данных PostgreSQL |
| `PG_USER` | `provisa` | Пользователь PostgreSQL |
| `PG_PASSWORD` | `provisa` | Пароль PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Ключ движка федерации (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | URL подключения для движков, управляемых через URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Переопределяет DSN хранилища материализации (по умолчанию — объявленное значение движка) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Каталог данных для встроенного хранилища DuckDB (REQ-989) |
| `TRINO_HOST` | `localhost` | Хост координатора Trino |
| `TRINO_PORT` | `8080` | HTTP-порт координатора Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Путь к JSON-ключу сервисного аккаунта GCP (движок/источник BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Проект GCP по умолчанию (BigQuery; переопределяется URL) |
| `FABRIC_SQL_SERVER` | — | SQL-эндпоинт Fabric Warehouse (альтернатива `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Имя базы данных Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Serverless SQL-эндпоинт Synapse |
| `SYNAPSE_DATABASE` | — | Имя базы данных Synapse |
| `REDIS_URL` | — | URL подключения к Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Ограничение выборки по умолчанию |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Ограничение количества строк, когда запрос не задаёт явный `LIMIT` |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Бюджет повторных попыток чтения уровня 1 в секундах; экспоненциальная задержка с полным джиттером (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Порт прокси Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Порт сервера Provisa Arrow Flight |
| `GRPC_PORT` | `50051` | Порт сервера Provisa Protobuf gRPC |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включает серверное перенаправление по порогу |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Пороговое значение количества строк по умолчанию |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Формат перенаправления по умолчанию |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 для перенаправленных результатов |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL S3-совместимого эндпоинта |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Ключ доступа S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Секретный ключ S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL подписанного URL (секунды) |
| `PROVISA_MTLS_CLIENT_CA` | — | Пакет PEM с CA, которым разрешено подписывать клиентские сертификаты; установка этой переменной включает проверку клиентских сертификатов на pgwire, Bolt, gRPC и Flight (REQ-1228) |
| `PROVISA_MTLS_MODE` | `required`, как только задан CA | `required` или `optional`; любое другое значение приводит к отказу от запуска (REQ-1228) |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя (common name) сертификата совпадало с аутентифицирующимся именем пользователя (REQ-1228) |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Список сайтов через запятую, которым разрешено открывать WebSocket-соединение Bolt из браузера; если не задано, отклоняется любое происхождение (origin) браузера (REQ-802) |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras Pyproject, встроенные в образ приложения; `scripts/provisa` выводит их из `dq_checker` в `~/.provisa/config.yaml` (REQ-1443) |
| `PROVISA_DQ_CHECKER` | `none` | Только для установщика: `none`/`soda`/`gx`, считывается `first-launch.sh` в неинтерактивном режиме и записывается в `config.yaml` как `dq_checker` (REQ-1443) |
| `ANTHROPIC_API_KEY` | — | Ключ Claude API (обнаружение) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Переопределяет `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Переопределяет `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Переопределяет `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Переопределяет `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Задержка сброса пакетного процессора спанов |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Переопределяет `observability.support_endpoint` |
