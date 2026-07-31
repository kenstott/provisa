# Справочник конфигурации

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

Все источники разделяют общий набор полей. [tool-verified: `provisa/core/models.py:129-212`]

| Поле | По умолчанию | Примечания |
|-------|---------|-------|
| `id` | обязательно | Буквы, цифры, дефисы, подчёркивания |
| `type` | обязательно | См. таблицу ниже |
| `host` | `""` | Имя хоста или IP |
| `port` | `0` | `0` означает, что каждый коннектор подставляет собственное значение по умолчанию — единой центральной карты портов по умолчанию нет |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Поддерживает разрешение секретов `${env:VAR}` |
| `path` | `null` | Путь к файлу или URI для файловых источников |
| `base_url` | `null` | Базовый URL для API-источников |
| `pool_min` / `pool_max` | `1` / `5` | Границы пула соединений |
| `cache_enabled` | `true` | Включение/выключение кеширования для всех таблиц этого источника |
| `cache_ttl` | `null` | Секунды; `null` наследует глобальное значение по умолчанию |
| `federation_hints` | `{}` | Расширенные параметры для каждого коннектора (dict[str,str]); см. справочник типов ниже. REQ-281 |
| `mapping` | `{}` | DSL сопоставления для redis, elasticsearch, prometheus. REQ-251 |
| `allowed_domains` | `[]` | Ограничивает этот источник конкретными идентификаторами доменов; пусто = без ограничений |
| `description` | `""` | |

### Поддерживаемые типы источников [tool-verified: `provisa/core/models.py:36-101`]

| Тип | Стиль соединения | Примечания |
|------|-----------------|-------|
| **РСУБД** | | |
| `postgresql` | host/port | Пул asyncpg; PgBouncer опционально через `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (файл БД) | Расширение сообщества DuckDB firebird (REQ-899) |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `yugabytedb` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `greenplum` | host/port | Использует драйвер/диалект PostgreSQL (REQ-950) |
| `tidb` | host/port | Использует драйвер/диалект MySQL (REQ-950) |
| **Облачное хранилище (Cloud DW)** | | |
| `snowflake` | host/port + `federation_hints` | `account` обязателен в hints |
| `bigquery` | `federation_hints` | `project` обязателен; аутентификация через `GOOGLE_APPLICATION_CREDENTIALS` |
| `databricks` | host + `federation_hints` | `http_path` обязателен в hints |
| `fabric` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `synapse` | переменные окружения или `PROVISA_ENGINE_URL` | T-SQL поверх TDS, аутентификация Azure AD |
| `redshift` | host/port | |
| **OLAP** | | |
| `clickhouse` | host/port + `federation_hints` | Подсказка `secure` переключает TLS; порт по умолчанию 8123/8443 |
| `elasticsearch` | host/port + DSL `mapping` | |
| `pinot` | host/port | REST-эндпоинт контроллера |
| `druid` | host/port | Avatica-эндпоинт брокера |
| `exasol` | host/port | |
| **Data Lake** | | |
| `delta_lake` | `path` (URI таблицы) | `delta_scan` DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `iceberg` | `path` (URI таблицы) | `iceberg_scan` DuckDB; доступ к объектному хранилищу через `federation_hints` |
| `hudi` | `path` (URI таблицы) | Движок Hudi ClickHouse, zero-copy (REQ-1178) |
| `hive` | host/port (metastore) + `mapping.storage` | Бэкенд хранения в `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + ключи S3 в `mapping` | Отдельный тип; всегда хранение S3 (REQ-229) |
| **NoSQL** | | |
| `mongodb` | host/port | Обычные поля соединения; без DSL сопоставления |
| `cassandra` | host/port | Обычные поля соединения; без DSL сопоставления |
| `redis` | host/port + DSL `mapping` | |
| **Потоковая передача** | | |
| `kafka` | только регистрация | Реальная конфигурация находится в `kafka_sources[]`; см. §Kafka ниже |
| `websocket` | host/port/path + `federation_hints` | Внешний фид WebSocket |
| `rss` | host/port/path + `federation_hints` | Фид RSS 2.0 / Atom |
| **Граф/Семантика** | | |
| `neo4j` | [UNVERIFIED end-to-end mapping] | |
| `sparql` | [UNVERIFIED end-to-end mapping] | |
| **Файлы** | | |
| `sqlite` | `path` | Всегда маршрутизируется через движок (без прямого пула) |
| `csv` | `path` | |
| `parquet` | `path` | |
| `files` | `path` (каталог) | Обход по glob-маске; выставляет CSV/Parquet/XLSX/JSON как таблицы |
| **API/Удалённые** | | |
| `google_sheets` | `federation_hints.spreadsheet_id` | |
| `prometheus` | host/port или `mapping.url` + DSL `mapping` | |
| `graphql_remote` | `base_url` + опциональный `mapping` | Заголовки, forward-client-headers, таймаут в `mapping` |
| `openapi` | `base_url` | |
| `grpc_remote` | [UNVERIFIED end-to-end mapping] | |
| `airport` | `base_url` (местоположение Flight) | Расширение airport DuckDB (REQ-899) |
| `ingest` | приёмник push | Внешние сервисы отправляют JSON-события через POST |
| **SaaS** | | |
| `sharepoint` | `base_url` или `host` + `mapping` | Аутентификация через `mapping.auth_type` |
| `splunk` | `host`/`port` или `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Отдельная модель `GovDataSource`; см. §GovData ниже |

### Справочник типов источников

Для типов, требующих неочевидной конфигурации, ниже есть краткое описание. Типы РСУБД (postgresql, mysql и т.д.) используют только общие поля выше — дополнительный раздел не нужен.

#### GovData [tool-verified: `provisa/core/models.py:953-983`]

Источники `govdata` используют отдельную модель верхнего уровня, `GovDataSource`, а не общий `Source`. (REQ-540) Доступ разделяется по группировке subject.

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

Каждый subject соответствует одной или нескольким схемам GovData. Настройка источника `govdata` с subject автоматически выставляет все схемы этого subject. (REQ-540)

| Subject | Схемы |
|---------|---------|
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

Схемы `ref` и `geo` всегда включены как связующие схемы — не настраиваются и не перечислены выше. (REQ-541) Используйте subject `ALL`, чтобы предоставить доступ ко всем схемам. [tool-verified: `provisa/core/models.py:961-963`]

#### Kafka [tool-verified: `provisa/federation/trino_connectors.py:497-502`, `provisa/api/app_loaders.py:113-118`]

Строка `kafka` в `sources:` предназначена только для регистрации. Её `details()` коннектора возвращает `{}` — реальная конфигурация находится в блоке верхнего уровня `kafka_sources[]`, а не в строке `sources:`. Kafka всегда является VIRTUAL_SOURCE (маршрутизируется через движок; без прямого пула). [tool-verified: `provisa/transpiler/router.py:44-63`]

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

**Временное окно** — `default_window` ограничивает каждый запрос недавним периодом времени, предотвращая неограниченные чтения из высоконагруженных топиков. (REQ-148) Формат: `1h`, `30m`, `7d`, `60s`. По умолчанию `1h`. Автоматически внедряется как `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Клиенты могут переопределить это собственным фильтром `_timestamp` в аргументе GraphQL `where`.

**Дискриминатор** — Несколько конфигураций топика могут указывать на один и тот же физический топик Kafka с разными значениями `discriminator`, создавая отдельные типы GraphQL. (REQ-149) Дискриминатор автоматически внедряется как предложение WHERE.

**Источник схемы**

| Значение | Поведение |
|-------|----------|
| `registry` | Получить схему из Confluent Schema Registry |
| `manual` | Определить столбцы напрямую в конфигурации (Schema Registry не требуется) |
| `sample` | Автоматически определить по образцам сообщений |

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

`http_path` в `federation_hints` обязателен. `password` несёт токен персонального доступа. `catalog` опционален (передаётся в SQL/hints, а не в поле `database`).

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

Оба используют T-SQL поверх TDS с аутентификацией Azure AD. Аутентифицируйтесь через `az login` (для разработки) или управляемую идентичность (для продакшена) — движок читает учётные данные через `DefaultAzureCredential` из `azure-identity`. Данные соединения берутся из переменных окружения: `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) или `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), либо через `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse [tool-verified: `provisa/executor/drivers/clickhouse.py:49-59`]

`secure` в `federation_hints` включает TLS на HTTP-интерфейсе. Порт по умолчанию — `8123` (обычный) или `8443` (при `secure: "true"`). `schema` в `federation_hints` переопределяет удалённую схему. [tool-verified: `provisa/federation/connector_duckdb.py:378-379`]

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

`host` и `port` указывают на Thrift metastore Hive (порт по умолчанию 9083). Для `hive` установите `mapping["storage"]`, чтобы выбрать бэкенд объектного хранилища. Отсутствующие обязательные ключи приводят к явному сбою — без запасного варианта. [tool-verified: `provisa/federation/trino_connectors.py:328-331`]

`hive_s3` — отдельный тип, который всегда объявляет хранение S3 (REQ-229); `mapping.storage` не требуется.

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

Допустимые значения `mapping.storage`: `hadoop` (по умолчанию), `hdfs`, `local`, `s3`, `azure`, `adls`. Ключи сопоставления S3: `endpoint`, `access_key_id`, `secret_access_key`, `region`, `path_style`. Ключи сопоставления ADLS: `storage_account`, `access_key` или `sas_token`.

#### Redis [tool-verified: `provisa/core/trino_catalog_files.py:54-75`]

Использует DSL `mapping`. `mongodb` и `cassandra` используют обычные поля соединения и НЕ используют DSL сопоставления.

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
          key_pattern: "session:*"
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

`mapping.url` переопределяет `host:port`, когда присутствуют оба.

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

`spreadsheet_id` в `federation_hints` обязателен. Аутентификация использует DuckDB SECRET `gsheet`, выданный во время присоединения (attach).

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Файловые источники (csv / parquet / sqlite / files)

`path` обязателен. `files` обходит каталог на предмет файлов CSV, Parquet, XLSX и JSON, выставляя каждый как таблицу. Все файловые источники VIRTUAL (маршрутизируются через движок; без прямого пула). [tool-verified: `provisa/transpiler/router.py:44-48`]

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

#### API / Удалённые источники

**openapi** — установите `base_url` на базовый URL OpenAPI. Обнаружение схемы читает спецификацию OpenAPI при запуске.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — установите `base_url`. Опциональные ключи `mapping`: `headers` (словарь статических заголовков), `forward_client_headers` (bool), `timeout_seconds` (int). [tool-verified: `provisa/hasura_v2/mapper.py:129-152`]

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

**airport** — `base_url` — это местоположение сервера Arrow Flight. Расширение airport DuckDB (REQ-899). [tool-verified: `provisa/federation/connector_duckdb.py:285-288`]

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

## Домены

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Наименование

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Соглашение об именовании

Орган именования — единственный источник истины для имён, обращённых к клиенту; физические имена столбцов бэкенда никогда не раскрываются клиентам. (REQ-194) Каждый язык запросов выводит имя столбца из его `column.alias`, если он задан, иначе — из физического имени столбца через настроенное соглашение. (REQ-194)

Соглашение GraphQL — одно из трёх заранее заданных перечислений. (REQ-416) Старые произвольные строки (`none`, `snake_case`, `camelCase`, `PascalCase`) устарели. (REQ-416)

| Пресет | По умолчанию | Имена типов | Имена полей | Имена мутаций |
|--------|---------|------------|-------------|----------------|
| `apollo_graphql` | да | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Соглашение GraphQL по умолчанию — `apollo_graphql`, которое производит имена полей и мутаций в camelCase. (REQ-194, REQ-416) Соглашение SQL отдельное, по умолчанию `snake_case`, применяется через `apply_sql_name()`; соглашение GraphQL применяется через `apply_gql_name()`, а имя CQL выводится из имени GraphQL. (REQ-194)

`domain_prefix: bool` — независимая опция, применяемая независимо от выбранного пресета. (REQ-416)

Явный `column.alias` является каноническим именем: SQL использует его дословно без применения соглашения, GraphQL применяет к нему своё соглашение, а CQL выводится из имени GraphQL. (REQ-194)

Переопределение для источника:
```yaml
sources:
  - id: legacy-db
    naming_convention: hasura_graphql  # overrides global for this source
```

Переопределение для таблицы:
```yaml
tables:
  - source_id: legacy-db
    table: orders
    naming_convention: snake  # overrides source for this table
```

### Префикс домена

Когда `domain_prefix: true`, все имена полей и типов GraphQL получают префикс из идентификатора домена с разделителем в виде двойного подчёркивания: (REQ-154)

| Таблица | Домен | Имя поля |
|-------|--------|-----------|
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Это предотвращает коллизии имён, когда разные домены содержат таблицы с одинаковыми именами, и делает запросы самодокументируемыми.

### Правила именования

Правила regex, применяемые к именам таблиц при генерации имён полей GraphQL. Применяются по порядку до разрешения уникальности. (REQ-542)

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

### Алиасы

Алиасы таблиц и столбцов переопределяют имя GraphQL по умолчанию. (REQ-155) Полезно для:
- Переименования непонятных имён из базы данных (например, `tbl_cust_seg` → `customer_segments`)
- Избегания сокращений на уровне API
- Создания чистого, специфичного для домена словаря

### Описания

Описания таблиц и столбцов включаются в сгенерированный GraphQL SDL. (REQ-156) Они отображаются в проводнике документации GraphiQL и в запросах интроспекции. Задайте их в YAML-конфигурации или через админ-интерфейс.

### Путь (вычисляемое извлечение JSON)

Столбцы могут извлекать значения из исходного столбца JSON/JSONB, используя `path` в точечной нотации. (REQ-151) Это полезно для полуструктурированных данных в сообщениях Kafka, документах MongoDB или столбцах JSONB PostgreSQL.

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

Формат пути — `source_column.key1.key2...`. Компилятор генерирует `json_extract_scalar(source_column, '$.key1.key2')` в SQL. (REQ-151)

**Влияние на маршрутизацию:** Столбцы path используют операторы JSON PostgreSQL (`->>`), которые нативно поддерживаются прямой маршрутизацией PG. (REQ-152) Для источников, отличных от PostgreSQL (MySQL, SQL Server и т.д.), запросы со столбцами path автоматически маршрутизируются через движок федерации. (REQ-152) Мутации не затронуты, поскольку столбцы path — это вычисляемые поля только для чтения. (REQ-153)

### Типы маскирования

| Тип | Поля | Описание |
|------|--------|-------------|
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (только для строковых столбцов) |
| `constant` | `value` | Литеральная замена (NULL, 0, MAX, MIN, произвольное значение) |
| `truncate` | `precision` | DATE_TRUNC (только для столбцов date/timestamp) |

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

Установите `materialize: true` на связи, чтобы автоматически сгенерировать материализованное представление для кросс-источниковых JOIN. (REQ-158) Это позволяет избежать дорогостоящих федеративных запросов путём предварительного вычисления результата JOIN.

- MV генерируются только для кросс-источниковых связей (JOIN в пределах одного источника уже быстрые) (REQ-159)
- MV изначально устарело (stale) и заполняется фоновым циклом обновления (REQ-160)
- Мутации любой из исходных таблиц помечают MV как устаревшее для повторного обновления (REQ-543)
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

Роли с `parent_role_id` наследуют возможности и доступ к домену от родителя. (REQ-215) Иерархия сглаживается при запуске. (REQ-215)

### Возможности (Capabilities)

| Возможность | Описание |
|-----------|-------------|
| `source_registration` | Регистрация источников данных |
| `table_registration` | Регистрация таблиц |
| `relationship_registration` | Определение связей |
| `security_config` | Настройка RLS, маскирования |
| `query_development` | Выполнение запросов |
| `full_results` | Обход лимитов выборки |
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

Представления — это вычисляемые наборы данных, определённые в SQL, с полным governance на уровне столбцов. (REQ-133) Это управляемый механизм для добавления агрегаций, трансформаций и производных метрик в семантический слой. (REQ-136)

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
|-------|----------|-------------|
| `id` | Да | Уникальный идентификатор представления |
| `sql` | Да | Оператор SQL SELECT, определяющий представление |
| `domain_id` | Да | Домен для видимости схемы |
| `materialize` | Нет | `true` = периодическое обновление CTAS, `false` = живое федеративное представление |
| `refresh_interval` | Нет | Секунды между обновлениями (только для материализованных, по умолчанию 300) |
| `description` | Нет | Отображается в GraphQL SDL |
| `alias` | Нет | Переопределяет имя GraphQL |
| `columns` | Да | Определения столбцов с видимостью, маскированием, описаниями |

### Материализованные и живые

- **`materialize: true`**: Provisa создаёт таблицу через CTAS и обновляет её по расписанию. (REQ-135) Более быстрые запросы, но данные могут отставать по времени до `refresh_interval` секунд.
- **`materialize: false`**: Provisa создаёт федеративное представление. (REQ-135) Запросы всегда возвращают живые данные, но могут быть медленнее для сложных агрегаций.

Представления проходят через тот же конвейер governance, что и таблицы — RLS, маскирование, выборка и видимость на основе роли. (REQ-134) Это гарантирует, что в платформу нельзя добавить новую семантику без надзора дата-стюарда. (REQ-136)

### Представления только для чтения

Как представления с `materialize: true`, так и с `materialize: false` выставляют свой тип GraphQL только для запросов. Для отношений на основе `view_sql` не генерируются мутации insert, upsert, update или delete. (REQ-1157) [tool-verified: `provisa/compiler/schema_gen.py:184`, `provisa/compiler/schema_types.py:79`]

## Кеш

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Иерархия кеша

Порядок разрешения TTL (наиболее специфичное побеждает): **таблица** > **источник** > **глобальное значение по умолчанию**. (REQ-544) Используется первое ненулевое значение.

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

Установка `cache_enabled: false` на источнике отключает кеширование для всех таблиц этого источника, независимо от TTL на уровне таблицы. (REQ-544) Ключи кеша всегда включают `role_id` + значения контекста RLS для разделения по соображениям безопасности. (REQ-544)

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

| Провайдер | Сценарий использования | Валидация токена |
|----------|----------|-----------------|
| `simple` | Локальная разработка/тестирование. Пользователи определены в YAML. | JWT, подписанный `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (все методы). | SDK `firebase-admin`, `verify_id_token()` |
| `keycloak` | Keycloak OIDC. Сопоставление ролей тенанта и клиента. | Валидация JWT на основе JWKS |
| `oauth` | Общий OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS с discovery URL |

Учётные данные суперпользователя (блок `superuser`) работают с любым провайдером и всегда разрешаются в роль admin со всеми возможностями. (REQ-125) Используются для первоначальной настройки до конфигурации внешней аутентификации.

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

## Мутации Upsert

Для таблиц с первичным ключом Provisa автоматически генерирует поля мутаций `upsert_<table>`. (REQ-212) Они компилируются в upsert на целевом диалекте — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` в PostgreSQL, `ON DUPLICATE KEY UPDATE` в MySQL. (REQ-212)

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Столбцы конфликта выводятся из метаданных PK. (REQ-212) Применяются все правила видимости столбцов и прав записи.

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

Компилируется в `SELECT DISTINCT ON (region) ...` в PostgreSQL. (REQ-213) Для не-PG диалектов используется запасной вариант на основе оконных функций. (REQ-213)

## Пресеты столбцов

Автоматическое внедрение значений в столбцы при вставке/обновлении. (REQ-214) Определяется для каждой таблицы в конфигурации.

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
|--------|----------|
| `header` | Внедряет значение из указанного HTTP-заголовка запроса |
| `now` | Внедряет `NOW()` (текущую метку времени) |
| `literal` | Внедряет константное значение |

Столбцы-пресеты внедряются во время компиляции мутации перед генерацией SQL. (REQ-214) Они не видны во входном типе мутации. (REQ-214)

## Наследуемые роли

Роли могут наследовать возможности и доступ к домену от родительской роли через `parent_role_id`. (REQ-215) Иерархия сглаживается при запуске. (REQ-215)

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

## Плановые триггеры

Триггеры на основе cron, вызывающие URL вебхука по расписанию. (REQ-216) Используется APScheduler. (REQ-216)

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

Плановые задачи управляются через админ-интерфейс (переключатель включения/выключения) или административную мутацию `toggle_scheduled_task`. (REQ-216)

## Формат OrderBy

OrderBy использует формат `{column: direction}` с перечислением из 6 направлений: (REQ-200, REQ-201)

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
|-----------|-----|
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

Provisa использует два независимых пути экспорта OTLP: ваш внутренний коллектор и опциональный эндпоинт поддержки Provisa. (REQ-545) У каждого пути свой фильтр. Фильтры выполняются внутри оборачивающего `_FilteringExporter`, прежде чем спаны покинут процесс — исходные объекты спанов никогда не изменяются. (REQ-546) [tool-verified: `provisa/api/otel_setup.py` lines 156–207]

**`telemetry_filter`** — управляет тем, что попадает в ваш внутренний коллектор.

| Ключ | Тип | По умолчанию | Описание |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `false` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

**`support_telemetry_filter`** — управляет тем, что попадает в эндпоинт поддержки Provisa. Редактирование SQL-литералов по умолчанию включено (`true`) на этом пути, поскольку данные запросов принадлежат вам. (REQ-547) [tool-verified: `provisa/api/otel_setup.py` line 240]

| Ключ | Тип | По умолчанию | Описание |
|-----|------|---------|-------------|
| `redact_sql_literals` | bool | `true` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

Пример редактирования `db.statement` — с `redact_sql_literals: true` этот атрибут спана:

```
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

становится:

```
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Эндпоинт поддержки [tool-verified]

`support_endpoint` (или переменная окружения `PROVISA_SUPPORT_OTLP_ENDPOINT`) пересылает телеметрию в службу поддержки Provisa для диагностики. (REQ-548) Когда не задан, никакие данные не покидают вашу инфраструктуру по этому пути. (REQ-548) Фильтр поддержки применяется независимо от внутреннего фильтра — вы можете редактировать SQL-литералы в обоих экспортах, при этом всё же делясь временем выполнения спанов и данными об ошибках со службой поддержки. (REQ-545) [tool-verified: `provisa/api/otel_setup.py` lines 238–288]

### Определение протокола эндпоинта [tool-verified]

Provisa выбирает OTLP/HTTP или OTLP/gRPC на основе схемы URL эндпоинта. (REQ-549) URL, начинающиеся с `http://` или `https://`, используют OTLP/HTTP, с автоматическим добавлением `/v1/traces`, `/v1/metrics` и `/v1/logs`. (REQ-549) Любая другая схема использует OTLP/gRPC с `insecure=True`. (REQ-549) [tool-verified: `provisa/api/otel_setup.py` lines 60–70]

## Движок федерации

Настройка движка федерации опциональна. По умолчанию используется `duckdb` — без конфигурации, встроен в процесс, внешний сервис не требуется (REQ-989). Выбирайте другой движок, когда вам нужен масштаб MPP или вы хотите переиспользовать существующее хранилище.

Приоритет: переменная окружения `PROVISA_ENGINE` → сохранённое поле конфигурации `federation_engine` из админ-интерфейса → `duckdb`. Изменения вступают в силу после перезапуска сервиса. [tool-verified: `engine.py` `build_engine`]

### Обзор движков [tool-verified: `engine.py` `ENGINE_REGISTRY`, `_ENGINE_BUILDERS`]

| Ключ движка | Метка | Диалект | MPP | Механизм внешней связи | Аутентификация |
|-----------|-------|---------|-----|------------------------|------|
| `trino` | Provisa Federation Engine | Trino SQL | Да | Каталоги Trino (широкий набор коннекторов) | Учётные данные JDBC |
| `trino-byo` | Trino (bring-your-own) | Trino SQL | Да | То же, что и `trino`; неуправляемый координатор | Учётные данные JDBC |
| `pg` | PostgreSQL | PostgreSQL | Нет | FDW / pg_duckdb | Учётные данные PostgreSQL |
| `duckdb` | DuckDB | DuckDB | Нет | Нативный ATTACH расширения | Нет (в процессе) |
| `clickhouse` | ClickHouse (встроенный) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | chdb (в процессе, без аутентификации) |
| `clickhouse-server` | ClickHouse (Server / Cloud) | ClickHouse | Да | Табличные движки S3 / IcebergS3 / DeltaLake | Учётные данные ClickHouse |
| `snowflake` | Snowflake | Snowflake | Да | Внешняя стадия + внешняя таблица | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks | Databricks SQL | Да | Внешние таблицы Unity Catalog через REST | `PROVISA_ENGINE_URL` (bearer-токен + `http_path`) |
| `bigquery` | BigQuery | BigQuery | Да | Внешние / BigLake таблицы BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | Microsoft Fabric | T-SQL | Да | Ярлыки OneLake → OPENROWSET | Azure AD (`az login` или управляемая идентичность) |
| `synapse` | Azure Synapse | T-SQL | Да | ADLS OPENROWSET / внешние таблицы | Azure AD |
| `sqlalchemy` | SQLAlchemy (любая РСУБД) | По диалекту | Нет | Нет (только загрузка) | Учётные данные по диалекту |

### Справочник движков

#### trino / trino-byo

`trino` — управляемый координатор Provisa; `trino-byo` подключается к вашему собственному кластеру Trino. Оба используют Trino SQL и имеют наибольший охват типов источников.

```bash
PROVISA_ENGINE=trino
TRINO_HOST=trino.internal
TRINO_PORT=8080
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL` (PostgreSQL).

#### pg

Федерация через postgres_fdw (SQL/MED) и расширения pg_duckdb. Единый узел; без MPP. Лучше всего, когда ваши данные уже находятся в PostgreSQL и вы хотите присоединить несколько удалённых источников.

```bash
PROVISA_ENGINE=pg
# Connection uses the standard PG_* env vars
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### duckdb

В процессе; внешний сервис не требуется. Движок по умолчанию (REQ-989). `PROVISA_DATA_DIR` определяет, где находится встроенное хранилище (по умолчанию `~/.provisa`).

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

Движок как хранилище: Snowflake выполняет запросы; Provisa проталкивает данные источников через внешние стадии.

```bash
PROVISA_ENGINE=snowflake
PROVISA_ENGINE_URL="snowflake://user:pass@account/db/schema?warehouse=WH"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### databricks

Внешние таблицы Unity Catalog связывают источники, управляемые Provisa, с Databricks SQL.

```bash
PROVISA_ENGINE=databricks
PROVISA_ENGINE_URL="databricks://token:TOKEN@my-workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxxx"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### bigquery

Внешние и BigLake таблицы BigQuery. Проект берётся из URL или `GOOGLE_CLOUD_PROJECT`; аутентификация через ключ сервисного аккаунта.

```bash
PROVISA_ENGINE=bigquery
PROVISA_ENGINE_URL="bigquery://my-project?location=US"
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### fabric / synapse

Оба используют T-SQL поверх TDS с аутентификацией Azure AD (`az login` или управляемая идентичность). Опустите `PROVISA_ENGINE_URL`, чтобы вместо этого читать данные соединения из переменных окружения.

```bash
PROVISA_ENGINE=fabric
# FABRIC_SQL_SERVER=...   FABRIC_DATABASE=...
# or: PROVISA_ENGINE_URL set explicitly

PROVISA_ENGINE=synapse
# SYNAPSE_SQL_SERVER=...  SYNAPSE_DATABASE=...
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

#### sqlalchemy

Общий движок РСУБД только для загрузки (без федерации к внешним источникам). Используйте для развёртываний с единым хранилищем или для тестирования.

```bash
PROVISA_ENGINE=sqlalchemy
PROVISA_ENGINE_URL="postgresql+psycopg2://user:pass@host/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

### Хранилище материализации

Когда источник не может подключиться вживую (нет коннектора ATTACH для выбранного движка), он загружается в хранилище материализации движка. Порядок разрешения: явный `PROVISA_MATERIALIZE_URL` → заявленное значение по умолчанию движка → явная ошибка (без бесшумного запасного варианта). [tool-verified: `engine.py` `materialize_store`]

DuckDB объявляет свой встроенный файл (`~/.provisa/materialize.duckdb`) как значение по умолчанию. Все остальные движки по умолчанию используют `TENANT_DATABASE_URL` (PostgreSQL). Переопределите для любого движка через `PROVISA_MATERIALIZE_URL`.

### Подсказки федерации для отдельного источника

Расширенные параметры соединения, которые не могут нести стандартные поля host/port/user/password, помещаются в `federation_hints` источника. Ключи подсказок для каждого типа см. в справочнике типов источников выше. Сводный пример:

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

Для источников Google Cloud установите `GOOGLE_APPLICATION_CREDENTIALS` на путь к файлу ключа сервисного аккаунта. Для Fabric и Synapse аутентифицируйтесь через `az login` (для разработки) или управляемую идентичность (для продакшена) — движок читает учётные данные через `DefaultAzureCredential` из `azure-identity`.

## Переменные окружения

| Переменная | По умолчанию | Описание |
|----------|---------|-------------|
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации |
| `TENANT_DATABASE_URL` | `postgresql+asyncpg://provisa:provisa@localhost:5432/provisa` | URI хранилища управляющей плоскости (SQLAlchemy async); принимает `sqlite+aiosqlite://…` / `duckdb://…` для встроенного десктопного хранилища (REQ-828, REQ-850) |
| `PLATFORM_DATABASE_URL` | — | URI реестра платформы (каталог тенантов, реестр движков); обязателен при запуске, без запасного варианта (REQ-837) |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` использует встроенный fakeredis вместо сервера Redis — без Docker (REQ-829) |
| `PG_HOST` | `localhost` | Хост PostgreSQL |
| `PG_PORT` | `5432` | Порт PostgreSQL |
| `PG_DATABASE` | `provisa` | База данных PostgreSQL |
| `PG_USER` | `provisa` | Пользователь PostgreSQL |
| `PG_PASSWORD` | `provisa` | Пароль PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Ключ движка федерации (REQ-989) |
| `PROVISA_ENGINE_URL` | — | URL соединения для движков, управляемых URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Переопределяет DSN хранилища материализации (по умолчанию — заявленное значение движка) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Каталог данных для встроенного хранилища DuckDB (REQ-989) |
| `TRINO_HOST` | `localhost` | Хост координатора Trino |
| `TRINO_PORT` | `8080` | HTTP-порт координатора Trino |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Путь к JSON-ключу сервисного аккаунта GCP (движок/источник BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | — | Проект GCP по умолчанию (BigQuery; переопределяется URL) |
| `FABRIC_SQL_SERVER` | — | SQL-эндпоинт Fabric Warehouse (альтернатива `PROVISA_ENGINE_URL`) |
| `FABRIC_DATABASE` | — | Имя базы данных Fabric Warehouse |
| `SYNAPSE_SQL_SERVER` | — | Бессерверный SQL-эндпоинт Synapse |
| `SYNAPSE_DATABASE` | — | Имя базы данных Synapse |
| `REDIS_URL` | — | URL соединения Redis |
| `PROVISA_SAMPLE_SIZE` | `10000` | Лимит выборки по умолчанию |
| `PROVISA_DEFAULT_ROW_LIMIT` | `100` | Ограничение строк, когда запрос не задаёт явный `LIMIT` |
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Бюджет повторных попыток чтения уровня 1 в секундах; экспоненциальная задержка с полным джиттером (REQ-703) |
| `ZAYCHIK_PORT` | `8480` | Порт прокси Zaychik Flight SQL |
| `FLIGHT_PORT` | `8815` | Порт сервера Arrow Flight Provisa |
| `GRPC_PORT` | `50051` | Порт сервера gRPC Protobuf Provisa |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включить пороговое перенаправление на стороне сервера |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Порог количества строк по умолчанию |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Формат перенаправления по умолчанию |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Бакет S3 для перенаправленных результатов |
| `PROVISA_REDIRECT_ENDPOINT` | — | URL S3-совместимого эндпоинта |
| `PROVISA_REDIRECT_ACCESS_KEY` | — | Ключ доступа S3 |
| `PROVISA_REDIRECT_SECRET_KEY` | — | Секретный ключ S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL подписанного URL (секунды) |
| `ANTHROPIC_API_KEY` | — | API-ключ Claude (обнаружение) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Переопределяет `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Переопределяет `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Переопределяет `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Переопределяет `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Задержка сброса пакетного обработчика спанов |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Переопределяет `observability.support_endpoint` |
