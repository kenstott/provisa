# Справочник по конфигурации

Provisa настраивается через YAML-файл (по умолчанию: `config/provisa.yaml`). (REQ-528)

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
| `firebird` | host + `path` (файл БД) | Расширение сообщества firebird для DuckDB (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Использует драйвер и диалект PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Использует драйвер и диалект PostgreSQL (REQ-950) |
| `greenplum` | host/port | Использует драйвер и диалект PostgreSQL (REQ-950) |
| `tidb` | host/port | Использует драйвер и диалект MySQL (REQ-950) |
| **Cloud DW** | | |
| `snowflake` | host/port + `federation_hints` | В подсказках обязателен `account` |
| `bigquery` | `federation_hints` | Обязателен `project`; аутентификация через `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | В подсказках обязателен `http_path` |
| `fabric` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `synapse` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Подсказка `secure` включает TLS; порт по умолчанию 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | REST-эндпоинт контроллера |
| `druid` | host/port | Эндпоинт Avatica брокера |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI таблицы) | `delta_scan` в DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `iceberg` | `path` (URI таблицы) | `iceberg_scan` в DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `hudi` | `path` (URI таблицы) | Движок Hudi в ClickHouse, без копирования (REQ-1178) |
| `hive` | host/port (метахранилище) + `mapping.storage` | Бэкенд хранения в `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (метахранилище) + ключи S3 в `mapping` | Отдельный тип; хранилище всегда S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Обычные поля подключения; DSL сопоставления не используется |
| `cassandra` | host/port | Обычные поля подключения; DSL сопоставления не используется |
| `redis` | host/port + DSL `mapping` | |
| **Streaming** | | |
| `kafka` | только регистрация | Настоящая конфигурация находится в `kafka_sources[]`; см. §Kafka ниже |
| `websocket` | host/port/path + `federation_hints` | Внешний поток WebSocket |
| `rss` | host/port/path + `federation_hints` | Лента RSS 2.0 / Atom |
| **Graph/Semantic** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **File** | | |
| `sqlite` | `path` | Всегда маршрутизируется через механизм (без прямого пула) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (каталог) | Обходчик по шаблону; выдаёт CSV/Parquet/XLSX/JSON как таблицы |
| **API/Remote** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port или `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + необязательный `mapping` | Заголовки, проброс клиентских заголовков, тайм-аут — в `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (расположение Flight) | Расширение airport для DuckDB (REQ-899) |
| `ingest` | приёмник записи | Внешние сервисы отправляют JSON-события методом POST |
| **SaaS** | | |
| `sharepoint` | `base_url` или `host` + `mapping` | Аутентификация через `mapping.auth_type` |
| `splunk` | `host`/`port` или `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | предмет + `domain_id` | Отдельная модель `GovDataSource`; см. §GovData ниже |
| **Data Quality** | | |
| `soda` | host/port, нацеленные на pgwire Provisa | Требует дополнение `soda`; Elastic License 2.0, только самостоятельное размещение (REQ-1443) |
| `great_expectations` | host/port, нацеленные на pgwire Provisa | Требует дополнение `gx`; Apache 2.0 (REQ-1443) |

### Справочник по типам источников

Для каждого типа с неочевидной конфигурацией ниже есть короткая запись. Типы RDBMS (postgresql, mysql и т. д.) используют только общие поля выше — отдельный раздел им не нужен.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Источники `govdata` используют отдельную модель верхнего уровня, `GovDataSource`, а не общую `Source`. (REQ-540) Доступ разделён по группировке предметов.

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

Каждый предмет соответствует одной или нескольким схемам GovData. Настройка источника `govdata` с предметом автоматически открывает все схемы этого предмета. (REQ-540)

| Предмет | Схемы |
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

Схемы `ref` и `geo` всегда включены как связующие схемы — они не настраиваются и не перечислены выше. (REQ-541) Используйте предмет `ALL`, чтобы дать доступ ко всем схемам. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

Строка `kafka` в `sources:` служит только для регистрации. Метод `details()` её коннектора возвращает `{}` — настоящая конфигурация находится в блоке верхнего уровня `kafka_sources[]`, а не в строке `sources:`. Kafka всегда является VIRTUAL_SOURCE (маршрутизируется через механизм; прямого пула нет). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Временное окно** — `default_window` ограничивает каждый запрос недавним промежутком времени, не давая читать высоконагруженные топики без границ. (REQ-148) Формат: `1h`, `30m`, `7d`, `60s`. По умолчанию `1h`. Подставляется автоматически как `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Клиенты могут переопределить его собственным фильтром по `_timestamp` в аргументе `where` GraphQL.

**Дискриминатор** — Несколько конфигураций топиков могут указывать на один физический топик Kafka с разными значениями `discriminator`, порождая отдельные типы GraphQL. (REQ-149) Дискриминатор автоматически подставляется как условие WHERE.

**Источник схемы**

| Значение | Поведение |
| ------- | ---------- |
| `registry` | Получить схему из Confluent Schema Registry |
| `manual` | Задать столбцы прямо в конфигурации (Schema Registry не нужен) |
| `sample` | Определить автоматически по образцам сообщений |

#### Snowflake [tool-verified: `provisa/executor/drivers/snowflake.py:48-62`]

`account` в `federation_hints` обязателен. `warehouse`, `role` и `schema` необязательны.

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

`http_path` в `federation_hints` обязателен. `password` содержит персональный токен доступа. `catalog` необязателен (передаётся в SQL или подсказках, а не в поле `database`).

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

`project` в `federation_hints` обязателен. Аутентификация использует `GOOGLE_APPLICATION_CREDENTIALS` (путь к файлу ключа служебного аккаунта) или Application Default Credentials в окружении механизма.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse [tool-verified: `provisa/core/models.py:56-57`]

Оба используют T-SQL поверх TDS с аутентификацией Azure AD. Аутентифицируйтесь через `az login` (разработка) или управляемое удостоверение (продуктив) — механизм читает учётные данные через `DefaultAzureCredential` из `azure-identity`. Параметры подключения берутся из переменных окружения: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) либо `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), или из `PROVISA_ENGINE_URL`.

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

`path` — это URI таблицы (S3, GCS, ADLS или локальный). Для доступа к объектному хранилищу нужны учётные данные в `federation_hints`. Для Cloudflare R2 добавьте `account_id`.

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

`host` и `port` указывают на метахранилище Hive Thrift (порт по умолчанию 9083). Для `hive` задайте `mapping["storage"]`, чтобы выбрать бэкенд объектного хранилища. Отсутствие обязательных ключей приводит к явной ошибке — без запасных значений. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` — отдельный тип, всегда объявляющий хранилище S3 (REQ-229); `mapping.storage` не нужен.

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

Допустимые значения `mapping.storage`: `hadoop` (по умолчанию), `hdfs`, `local`, `s3`, `azure`, `adls`. Ключи сопоставления для S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Ключи сопоставления для ADLS: `storage_account`, `access_key` или `sas_token`.

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

`spreadsheet_id` в `federation_hints` обязателен. Аутентификация использует SECRET `gsheet` в DuckDB, создаваемый в момент подключения.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Файловые источники (csv / parquet / sqlite / files)

`path` обязателен. `files` обходит каталог в поисках файлов CSV, Parquet, XLSX и JSON, выдавая каждый как таблицу. Все файловые источники являются VIRTUAL (маршрутизируются через механизм; прямого пула нет). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### Источники API / Remote

**openapi** — задайте в `base_url` базовый URL OpenAPI. Обнаружение схемы читает спецификацию OpenAPI при запуске.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — задайте `base_url`. Необязательные ключи `mapping`: `headers` (словарь статических заголовков), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` — это расположение сервера Arrow Flight. Расширение airport для DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — используйте `host`, `port`, `path` и `federation_hints`. [tool-verified: `provisa/api/data/subscribe.py:85-129`]

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

#### Проверяющие качества данных (soda / great_expectations)

[tool-verified: `provisa/dq/registration.py`, `provisa/events/source_loader.py` `make_dq_loader`]

Источник-проверяющий указывает на собственный эндпоинт pgwire в Provisa, поэтому один драйвер postgres сканирует федеративное представление таблицы, лежащей в Snowflake или Iceberg. Личность сканирования объявляется, а не наследуется — политика применяется к этому соединению, и отфильтрованный набор строк не должен давать молча пройденную проверку. Ключи подключения берутся из `mapping`: `host`, `port`, `database`, `user`, `password`.

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

Каждая таблица результатов несёт `dq_contract` — YAML-контракт Soda или JSON набора Great Expectations, дословно. Столбцы, водяной знак и продвижения выводятся из него; полный вывод описан в разделе [Проверяющие качества данных](sources.md#data-quality-checkers-req-1443).

**Выбор при установке.** Проверяющий не встроен в сборку — сканирование выполняется в дочернем интерпретаторе, и библиотека устанавливается, только когда оператор её назовёт. Каждый путь установки (`install.sh`, `packaging/linux/first-launch.sh` и мастер для macOS через `PROVISA_DQ_CHECKER`) записывает выбор в `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` читает этот ключ и экспортирует `PROVISA_EXTRAS`, который `docker-compose.app.yml` передаёт как аргумент сборки в `ARG PROVISA_EXTRAS` из `Dockerfile`: [tool-verified: `scripts/provisa:69-79`]

| `dq_checker` | `PROVISA_EXTRAS` (уровень Docker) | Установка в нативное venv |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Установка демонстрационного набора данных поднимает `none` до `gx` и сообщает об этом, потому что демонстрационная конфигурация регистрирует набор Great Expectations поверх `pet_store.pets`, и её карточке качества иначе нечего было бы показать. Выбор `soda` сохраняется.

Установка демо через pip, а не через инсталлятор, пропускает этот шаг мастера, поэтому дополнение `demo` несёт тот же проверяющий: `pip install 'provisa[embedded,demo]'` — это то, что нужно `provisa run --demo`, чтобы сканирование запустилось. Без него сканирование сообщает `data-quality checker 'great_expectations' is not installed`, называя команду установки.

Любое другое значение останавливает запуск, а не стартует без проверяющего, которого запросил оператор. Дополнение `soda` подтягивает `soda-postgres`; `gx` подтягивает `great-expectations[postgresql]`. Soda Core распространяется по Elastic License 2.0 — `config/capabilities.yaml` помечает этот вариант как `cloud_eligible: false`, и размещённая плоскость его отклоняет.

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

Служба именования — единственный источник истины для имён, видимых клиенту; физические имена столбцов бэкенда клиентам никогда не раскрываются. (REQ-194) Каждый язык запросов выводит имя столбца из `column.alias`, если он задан, иначе из физического имени столбца по настроенному соглашению. (REQ-194)

Соглашение GraphQL — одно из трёх предустановленных перечислений. (REQ-416) Старые свободные строки (`none`, `snake_case`, `camelCase`, `PascalCase`) устарели. (REQ-416)

| Пресет | По умолчанию | Имена типов | Имена полей | Имена мутаций |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | да | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Соглашение GraphQL по умолчанию — `apollo_graphql`, оно даёт имена полей и мутаций в camelCase. (REQ-194, REQ-416) Соглашение SQL задаётся отдельно, по умолчанию `snake_case`, и применяется через `apply_sql_name()`; соглашение GraphQL применяется через `apply_gql_name()`, а имя CQL выводится из имени GraphQL. (REQ-194)

`domain_prefix: bool` — независимый параметр, действующий при любом выбранном пресете. (REQ-416)

Явный `column.alias` является каноническим именем: SQL использует его дословно без применения соглашения, GraphQL применяет к нему своё соглашение, а CQL выводится из имени GraphQL. (REQ-194)

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

При `domain_prefix: true` все имена полей и типов GraphQL получают префикс идентификатора домена через двойное подчёркивание: (REQ-154)

| Таблица | Домен | Имя поля |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Это предотвращает конфликты имён, когда в разных доменах есть таблицы с одинаковыми именами, и делает запросы самодокументируемыми.

### Правила именования

Правила на регулярных выражениях, применяемые к именам таблиц при формировании имён полей GraphQL. Применяются по порядку до разрешения уникальности. (REQ-542)

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
- Избавления от сокращений на уровне API
- Создания чистого предметного словаря

### Описания

Описания таблиц и столбцов включаются в сформированный SDL GraphQL. (REQ-156) Они появляются в обозревателе документации GraphiQL и в интроспекционных запросах. Задавайте их в YAML-конфигурации или через административный интерфейс.

### Path (вычисляемое извлечение из JSON)

Столбцы могут извлекать значения из исходного столбца JSON/JSONB по точечному пути `path`. (REQ-151) Это полезно для полуструктурированных данных в сообщениях Kafka, документах MongoDB или столбцах JSONB в PostgreSQL.

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

Формат пути — `source_column.key1.key2...`. Компилятор порождает в SQL `json_extract_scalar(source_column, '$.key1.key2')`. (REQ-151)

**Влияние на маршрутизацию:** Столбцы с путями используют операторы JSON PostgreSQL (`->>`), которые изначально поддерживаются прямой маршрутизацией в PG. (REQ-152) Для источников, отличных от PostgreSQL (MySQL, SQL Server и т. д.), запросы со столбцами-путями автоматически направляются через механизм федерации. (REQ-152) Мутации это не затрагивает, поскольку столбцы-пути — вычисляемые поля только для чтения. (REQ-153)

### Типы маскирования

| Тип | Поля | Описание |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (только строковые столбцы) |
| `constant` | `value` | Замена литералом (NULL, 0, MAX, MIN, произвольное) |
| `truncate` | `precision` | DATE_TRUNC (только столбцы date/timestamp) |

## Связи

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

Задайте `materialize: true` на связи, чтобы автоматически создавать материализованное представление для межисточниковых JOIN. (REQ-158) Это избавляет от дорогих федеративных запросов за счёт предварительного вычисления результата JOIN.

- Материализованные представления создаются только для межисточниковых связей (JOIN внутри одного источника и так быстры) (REQ-159)
- Представление начинается устаревшим и наполняется фоновым циклом обновления (REQ-160)
- Мутации в любой из исходных таблиц помечают представление устаревшим для повторного обновления (REQ-543)
- `refresh_interval` по умолчанию равен 300 секундам (5 минут) (REQ-543)

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

Роли с `parent_role_id` наследуют возможности и доступ к доменам от родителя. (REQ-215) Иерархия разворачивается при запуске. (REQ-215)

### Возможности

| Возможность | Описание |
| ----------- | ------------- |
| `source_registration` | Регистрировать источники данных |
| `table_registration` | Регистрировать таблицы |
| `relationship_registration` | Определять связи |
| `security_config` | Настраивать RLS и маскирование |
| `query_development` | Выполнять запросы |
| `full_results` | Обходить ограничения выборки |
| `admin` | Все возможности |

## Правила RLS

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
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Представления (управляемые вычисляемые наборы данных)

Представления — это вычисляемые наборы данных, заданные на SQL, с полным управлением на уровне столбцов. (REQ-133) Это управляемый способ добавлять агрегаты, преобразования и производные метрики в семантический слой. (REQ-136)

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
| `sql` | Да | Оператор SQL SELECT, задающий представление |
| `domain_id` | Да | Домен для видимости схемы |
| `materialize` | Нет | `true` = периодическое обновление через CTAS, `false` = живое федеративное представление |
| `refresh_interval` | Нет | Секунды между обновлениями (только для материализованных, по умолчанию 300) |
| `description` | Нет | Появляется в SDL GraphQL |
| `alias` | Нет | Переопределяет имя GraphQL |
| `columns` | Да | Определения столбцов с видимостью, маскированием и описаниями |

### Материализованное или живое

- **`materialize: true`**: Provisa создаёт таблицу через CTAS и обновляет её по расписанию. (REQ-135) Запросы быстрее, но данные могут отставать на величину до `refresh_interval` секунд.
- **`materialize: false`**: Provisa создаёт федеративное представление. (REQ-135) Запросы всегда возвращают живые данные, но для сложных агрегатов могут работать медленнее.

Представления проходят тот же управляющий конвейер, что и таблицы, — RLS, маскирование, выборку и видимость по ролям. (REQ-134) Это гарантирует, что новая семантика не появится на платформе без надзора распорядителя данных. (REQ-136)

### Представления только для запросов

Представления и с `materialize: true`, и с `materialize: false` открывают свой тип GraphQL только для запросов. Для отношений на основе `view_sql` не порождаются мутации insert, upsert, update и delete. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Кэш

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Иерархия кэша

Порядок разрешения TTL (побеждает самое конкретное): **таблица** > **источник** > **глобальное значение по умолчанию**. (REQ-544) Используется первое ненулевое значение.

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

Установка `cache_enabled: false` на источнике отключает кэширование для всех его таблиц независимо от TTL на уровне таблицы. (REQ-544) Ключи кэша всегда включают `role_id` и значения контекста RLS для разделения по безопасности. (REQ-544)

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

### Типы поставщиков аутентификации

| Поставщик | Сценарий | Проверка токена |
| ---------- | ---------- | ----------------- |
| `simple` | Локальная разработка и тестирование. Пользователи заданы в YAML. | JWT, подписанный `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (все методы). | `verify_id_token()` из SDK `firebase-admin` |
| `keycloak` | OIDC в Keycloak. Сопоставляются роли арендатора и клиента. | Проверка JWT по JWKS |
| `oauth` | Общий OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS из URL обнаружения |
| `basic` | Автономные развёртывания. Учётные записи хранятся в собственном хранилище Provisa. | Пароль bcrypt или SCRAM-SHA-256 на pgwire |

Учётные данные суперпользователя (блок `superuser`) работают с любым поставщиком и всегда дают роль admin со всеми возможностями. (REQ-125) Используются для первоначальной настройки до подключения внешней аутентификации.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Заставляет pgwire объявлять SASL с `SCRAM-SHA-256`, так что пароль доказывается, а не передаётся открытым текстом. (REQ-1394) Работает только с поставщиком `basic` — ни один другой поставщик не хранит верификаторы RFC 5802, нужные SCRAM, — и привязка к каналу не предлагается.

Верификаторы нельзя получить из существующих хешей bcrypt. Верификатор записывается, когда пароль проходит через систему в открытом виде, поэтому первое SCRAM-соединение каждого пользователя наступает после его следующей регистрации, входа, смены пароля или сброса администратором. До этого момента соединения такого пользователя откатываются к обмену открытым текстом поверх TLS; по проводу не видно, кто уже перешёл.

### Ограничение попыток входа (`auth.login_throttle`)

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Включено по умолчанию с показанными значениями; блок лишь настраивает их. (REQ-1393) Счётчик находится на уровне проверки учётных данных, поэтому неудачи по HTTP, pgwire и Bolt накапливаются для одного и того же субъекта, а блокировка действует на всех поверхностях. Работает на уровне процесса: несколько рабочих API каждый допускают до `max_attempts`.

### Персональные токены доступа

Персональным токенам доступа не нужен блок конфигурации — они принимаются всегда, а хранилище создаётся вместе с остальной схемой плоскости управления. (REQ-1263) Настраивается лишь срок действия, который пользователь может запросить при выпуске: от 1 до 366 дней либо бессрочный токен. См. [Модель безопасности](security.md#personal-access-tokens).

### Взаимный TLS

Проверка клиентских сертификатов настраивается переменными окружения, а не в `provisa.yaml`, рядом с настройками TLS-сертификата, которые она расширяет. (REQ-1228)

| Переменная | По умолчанию | Значение |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | не задано | Набор PEM с центрами сертификации, которым разрешено подписывать клиентские сертификаты. Её установка включает проверку клиентских сертификатов |
| `PROVISA_MTLS_MODE` | `required`, когда задан CA | `required` или `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя сертификата совпадало с именем пользователя, под которым аутентифицируется соединение |

Для каждой из них есть переопределение по протоколу с тем же именованием, что и у настроек TLS. Режим, заданный без CA, или режим с иным значением приводит к отказу от запуска, а не к обслуживанию соединений, которые оператор считает проверенными.

### Обращение к организации поверх TLS

Настраивать нечего. В развёртывании с несколькими организациями pgwire и Bolt читают организацию из имени хоста, к которому подключился клиент, — оно передаётся в TLS ClientHello ровно так же, как HTTP читает его из заголовка `Host`. (REQ-1234) Клиент, подключающийся к `acme.provisa.dev`, запрашивает организацию `acme`; запрос отклоняется, если аутентифицированный субъект не является её участником. Подключение по IP-адресу не запрашивает организацию — так происходит с каждым соединением в развёртывании с одной организацией.

### Полный пример конфигурации аутентификации (закомментирован)

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

## Мутации upsert

Для таблиц с первичным ключом Provisa автоматически порождает поля мутации `upsert_<table>`. (REQ-212) Они компилируются в upsert на целевом диалекте — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` в PostgreSQL, `ON DUPLICATE KEY UPDATE` в MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Столбцы конфликта выводятся из метаданных первичного ключа. (REQ-212) Все правила видимости столбцов и прав на запись действуют.

## Distinct On

Аргумент `distinct_on` выбирает первую строку для каждого различного значения указанных столбцов. (REQ-213) Доступен в корневых полях запросов.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Компилируется в `SELECT DISTINCT ON (region) ...` в PostgreSQL. (REQ-213) Для диалектов, отличных от PG, используется запасной вариант на оконной функции. (REQ-213)

## Предустановки столбцов

Автоматически подставляют значения в столбцы при вставке и обновлении. (REQ-214) Задаются для каждой таблицы в конфигурации.

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
| `header` | Подставляет значение из указанного HTTP-заголовка запроса |
| `now` | Подставляет `NOW()` (текущая отметка времени) |
| `literal` | Подставляет константу |

Предустановленные столбцы подставляются при компиляции мутации до генерации SQL. (REQ-214) Во входном типе мутации они не видны. (REQ-214)

## Наследуемые роли

Роли могут наследовать возможности и доступ к доменам от родительской роли через `parent_role_id`. (REQ-215) Иерархия разворачивается при запуске. (REQ-215)

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

Поддерживается многоуровневое наследование. (REQ-215) Явные возможности и domain_access дочерней роли объединяются с родительскими. (REQ-215)

## Триггеры по расписанию

Триггеры на основе cron, вызывающие URL веб-перехватчика по расписанию. (REQ-216) Используется APScheduler. (REQ-216)

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

Запланированными задачами управляют через административный интерфейс (переключатель включения) или административную мутацию `toggle_scheduled_task`. (REQ-216)

## Формат OrderBy

OrderBy использует формат `{column: direction}` с перечислением направлений из шести значений: (REQ-200, REQ-201)

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

Provisa ведёт два независимых пути экспорта OTLP: ваш внутренний сборщик и необязательный эндпоинт поддержки Provisa. (REQ-545) У каждого пути свой фильтр. Фильтры работают внутри обёртки `_FilteringExporter` до того, как спаны покинут процесс, — исходные объекты спанов никогда не изменяются. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — определяет, что попадает в ваш внутренний сборщик.

| Ключ | Тип | По умолчанию | Описание |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

**`support_telemetry_filter`** — определяет, что попадает на эндпоинт поддержки Provisa. На этом пути скрытие литералов SQL по умолчанию включено, поскольку данные запросов принадлежат вам. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Ключ | Тип | По умолчанию | Описание |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

Пример скрытого `db.statement` — при `redact_sql_literals: true` этот атрибут спана:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

становится таким:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Эндпоинт поддержки [tool-verified]

`support_endpoint` (или переменная окружения `PROVISA_SUPPORT_OTLP_ENDPOINT`) пересылает телеметрию в поддержку Provisa для диагностики. (REQ-548) Когда он не задан, по этому пути данные не покидают вашу инфраструктуру. (REQ-548) Фильтр поддержки применяется независимо от внутреннего фильтра — вы можете скрывать литералы SQL в обоих экспортах, продолжая делиться с поддержкой временными характеристиками спанов и данными об ошибках. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Определение протокола эндпоинта [tool-verified]

Provisa выбирает OTLP/HTTP или OTLP/gRPC по схеме URL эндпоинта. (REQ-549) URL, начинающиеся с `http://` или `https://`, используют OTLP/HTTP, при этом `/v1/traces`, `/v1/metrics` и `/v1/logs` добавляются автоматически. (REQ-549) Любая другая схема использует OTLP/gRPC с `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Механизм федерации

Настраивать механизм федерации необязательно. По умолчанию используется `duckdb` — без конфигурации, внутри процесса, без внешнего сервиса (REQ-989). Выбирайте другой механизм, когда нужен масштаб MPP или хочется задействовать существующее хранилище.

Приоритет: переменная окружения `PROVISA_ENGINE` → сохранённое поле конфигурации `federation_engine` из административного интерфейса → `duckdb`. Изменения вступают в силу после перезапуска сервиса. [tool-verified: `engine.py` `build_engine`]

### Обзор механизмов [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Ключ механизма | Название | Диалект | MPP | Механизм внешней связи | Аутентификация |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
| `trino` | Механизм федерации Provisa | Trino SQL | Да | Каталоги Trino (широкий набор коннекторов) | Учётные данные JDBC |
| `trino-byo` | Trino | Trino SQL | Да | То же, что у `trino`; неуправляемый координатор | Учётные данные JDBC |
| `pg` | PostgreSQL | PostgreSQL | Нет | FDW / pg_duckdb | Учётные данные PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Нет | Нативный для расширения ATTACH | Нет (внутри процесса) |
| `clickhouse` | ClickHouse (встроенный) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | chdb (внутри процесса, без аутентификации) |
| `clickhouse-server` | ClickHouse (сервер / облако) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | Учётные данные ClickHouse |
| `snowflake` | Snowflake | Snowflake | Да | Внешняя площадка + внешняя таблица | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Да | Внешние таблицы Unity Catalog через REST | `PROVISA_ENGINE_URL` (bearer-токен + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Да | Внешние таблицы BigQuery / BigLake | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Да | Ярлыки OneLake → OPENROWSET | Azure AD (`az login` или управляемое удостоверение) |
| `synapse` | Azure Synapse | T-SQL | Да | OPENROWSET / внешние таблицы ADLS | Azure AD |
| `mysql` | MySQL | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `mariadb` | MariaDB | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `oracle` | Oracle Database | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `mssql` | Microsoft SQL Server | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `db2` | IBM Db2 | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `redshift` | Amazon Redshift | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `greenplum` | Greenplum | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `cockroachdb` | CockroachDB | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `yugabytedb` | YugabyteDB | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `opengauss` | openGauss | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `tidb` | TiDB | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `singlestore` | SingleStore | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `vertica` | Vertica | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `exasol` | Exasol | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `teradata` | Teradata Vantage | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `saphana` | SAP HANA | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `sapase` | SAP ASE (Sybase) | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `sqlanywhere` | SAP SQL Anywhere | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `monetdb` | MonetDB | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `firebird` | Firebird | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |
| `sqlalchemy` | Другая реляционная база данных (по URL подключения) | По диалекту | Нет | Нет (только приземление) | Учётные данные по диалекту |

### Справочник по механизмам

#### trino / trino-byo

`trino` — управляемый координатор Provisa; `trino-byo` подключается к вашему собственному кластеру Trino. Оба используют Trino SQL и охватывают самый широкий набор типов источников.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Федерация через расширения postgres_fdw (SQL/MED) и pg_duckdb. Один узел; без MPP. Лучший выбор, когда данные уже лежат в PostgreSQL и нужно подключить к ним несколько удалённых источников.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### duckdb

Внутри процесса; без внешнего сервиса. Механизм по умолчанию (REQ-989). `PROVISA_DATA_DIR` задаёт расположение встроенного хранилища (`~/.provisa` по умолчанию).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Хранилище материализации по умолчанию — `~/.provisa/materialize.duckdb`: единственный механизм, у которого хранилище по умолчанию не на PostgreSQL.

#### clickhouse (встроенный) / clickhouse-server

`clickhouse` использует chdb (внутри процесса). `clickhouse-server` подключается к внешнему экземпляру ClickHouse или к ClickHouse Cloud. Оба читают Delta Lake, Iceberg и Hudi напрямую через нативные табличные движки ClickHouse.

```bash
# External server
PROVISA_ENGINE=clickhouse-server
PROVISA_ENGINE_URL="clickhouse://user:pass@host:9000/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### snowflake

Механизм как хранилище: запросы выполняет Snowflake, а Provisa передаёт данные источников через внешние площадки.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### databricks

Внешние таблицы Unity Catalog связывают источники под управлением Provisa с Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### bigquery

Внешние таблицы BigQuery и BigLake. Проект берётся из URL или `GOOGLE_CLOUD_PROJECT`; аутентификация — по ключу служебного аккаунта.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### fabric / synapse

Оба используют T-SQL поверх TDS с аутентификацией Azure AD (`az login` или управляемое удостоверение). Не задавайте `PROVISA_ENGINE_URL`, чтобы читать параметры подключения из переменных окружения.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### Механизмы на реляционных базах данных (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) и `sqlalchemy`

По одному ключу на каждую сетевую реляционную базу данных, и все они работают в одном режиме «только приземление» (без федерации к внешним источникам): каждый источник приземляется в хранилище и запрашивается там. Ключ выбирает базу данных; `PROVISA_ENGINE_URL` несёт DSN в формате её диалекта. `sqlalchemy` — универсальный вариант для базы данных, у которой нет собственного ключа. Файловые встроенные хранилища (SQLite, Access) не предлагаются — сервер должен быть доступен по сети.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

### Хранилище материализации

Когда источник нельзя подключить вживую (для выбранного механизма нет коннектора ATTACH), он приземляется в хранилище материализации этого механизма. Порядок разрешения: явный `PROVISA_MATERIALIZE_URL` → объявленное значение по умолчанию для механизма → явная ошибка (молчаливого запасного варианта нет). [tool-verified: `engine.py` `materialize_store`]

DuckDB объявляет своим значением по умолчанию встроенный файл (`~/.provisa/materialize.duckdb`). Все остальные механизмы по умолчанию используют `TENANT_DATABASE_URL` (PostgreSQL). Любой механизм можно переопределить через `PROVISA_MATERIALIZE_URL`.

### Подсказки федерации на уровне источника

Расширенные параметры подключения, которые не помещаются в стандартные поля host/port/user/password, задаются в `federation_hints` на источнике. Ключи подсказок для каждого типа перечислены в справочнике по типам источников выше. Сводный пример:

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

Для источников в Google Cloud задайте в `GOOGLE_APPLICATION_CREDENTIALS` путь к файлу ключа служебного аккаунта. Для Fabric и Synapse аутентифицируйтесь через `az login` (разработка) или управляемое удостоверение (продуктив) — механизм читает учётные данные через `DefaultAzureCredential` из `azure-identity`.

## Переменные окружения

| Переменная | По умолчанию | Описание |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI хранилища плоскости управления (асинхронный SQLAlchemy); принимает `sqlite+aiosqlite://…` / `duckdb://…` для встроенного настольного хранилища (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI реестра платформы (каталог арендаторов, реестр механизмов); обязателен при запуске, запасного значения нет (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` включает встроенный fakeredis вместо сервера Redis — без Docker (REQ-829) |
| `PG_HOST` | `localhost` | Хост PostgreSQL |
| `PG_PORT` | `5432` | Порт PostgreSQL |
| `PG_DATABASE` | `provisa` | База данных PostgreSQL |
| `PG_USER` | `provisa` | Пользователь PostgreSQL |
| `PG_PASSWORD` | `provisa` | Пароль PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Ключ механизма федерации (REQ-989, REQ-916) |
| `PROVISA_ENGINE_URL` | — | URL подключения для механизмов, управляемых через URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Переопределяет DSN хранилища материализации (по умолчанию берётся объявленное значение механизма) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Каталог данных для встроенного хранилища DuckDB (REQ-989) |
| `TRINO_HOST` | `localhost` | Хост координатора Trino |
| `TRINO_PORT` | `8080` | HTTP-порт координатора Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Путь к JSON-ключу служебного аккаунта GCP (механизм/источник BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Проект GCP по умолчанию (BigQuery; переопределяется URL) |
| `FABRIC_SQL_SERVER` | — | SQL-эндпоинт Fabric Warehouse (альтернатива `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Имя базы данных Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Бессерверный SQL-эндпоинт Synapse |
| `SYNAPSE_DATABASE` | — | Имя базы данных Synapse |
| `REDIS_URL` | — | URL подключения к Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Предел выборки по умолчанию |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Ограничение числа строк, когда запрос не задаёт явный `LIMIT` |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Бюджет повторов чтения уровня 1 в секундах; экспоненциальная задержка с полным разбросом (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Порт прокси Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Порт сервера Arrow Flight в Provisa |
| `GRPC_PORT` | `50051` | Порт сервера Protobuf gRPC в Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включает серверное перенаправление по порогу |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Порог количества строк по умолчанию |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Формат перенаправления по умолчанию |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Бакет S3 для перенаправленных результатов |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL S3-совместимого эндпоинта |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Ключ доступа S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Секретный ключ S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL предподписанного URL (в секундах) |
| `PROVISA_MTLS_CLIENT_CA` | — | Набор PEM с центрами сертификации, которым разрешено подписывать клиентские сертификаты; его установка включает проверку клиентских сертификатов на pgwire, Bolt, gRPC и Flight (REQ-1228) |
| `PROVISA_MTLS_MODE` | `required`, когда задан CA | `required` или `optional`; любое другое значение приводит к отказу от запуска (REQ-1228) |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя сертификата совпадало с именем аутентифицирующегося пользователя (REQ-1228) |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Разделённый запятыми список сайтов, которым разрешено открывать WebSocket Bolt из браузера; если не задано, любой браузерный источник отклоняется (REQ-802) |
| `PROVISA_EXTRAS` | `firebase,vector` | Дополнения pyproject, встроенные в образ приложения; `scripts/provisa` выводит его из `dq_checker` в `~/.provisa/config.yaml` (REQ-1443) |
| `PROVISA_DQ_CHECKER` | `none` | Только для установщика: `none`/`soda`/`gx`, читается `first-launch.sh` в неинтерактивном режиме и записывается в `config.yaml` как `dq_checker` (REQ-1443) |
| `ANTHROPIC_API_KEY` | — | Ключ API Claude (обнаружение) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Переопределяет `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Переопределяет `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Переопределяет `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Переопределяет `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Задержка сброса пакетного обработчика спанов |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Переопределяет `observability.support_endpoint` |
