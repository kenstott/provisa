# Справочник по конфигурации {: #configuration-reference }

Provisa настраивается через YAML-файл (по умолчанию: `config/provisa.yaml`).

## Включения (Includes) {: #includes }

Разделите конфигурацию на несколько файлов с помощью `includes:`. Включающий файл перечисляет пути к фрагментам под этим ключом; Provisa объединяет их перед валидацией, получая тот же результат, как если бы всё было написано в одном файле.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Файл-обёртка, содержащий только `includes:`, допустим. `load_control_plane` читает через включения, поэтому раздел `control_plane:` берётся из того включённого файла, который его задаёт.

**Правила слияния**

- Пути разрешаются относительно включающего файла. Абсолютные пути используются как есть.
- Секции-списки (`sources`, `tables`, `domains`, `relationships`, `roles`, …) дополняются — записи фрагмента следуют за записями включающего файла.
- Скалярный ключ или ключ-словарь, который включающий файл не задаёт, берётся из фрагмента.
- Ключ, заданный обоими файлами с разными значениями, — конфликт; загрузка завершается ошибкой с указанием ключа.
- Одинаковые значения в обоих файлах конфликтом не считаются.
- Включения вкладываются друг в друга. Файл, включающий сам себя — напрямую или через другой фрагмент, — отклоняется.
- `includes` учитывается только при загрузке и никогда не появляется в проверенной конфигурации.

## Источники {: #sources }

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

У всех источников общий набор полей.

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
| `cache_ttl` | `null` | Секунды; `null` наследует глобальное значение по умолчанию; `-1` не истекает |
| `federation_hints` | `{}` | Расширенные параметры конкретного коннектора (dict[str,str]); см. справочник по типам ниже. |
| `mapping` | `{}` | DSL сопоставления для redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Ограничивает источник конкретными идентификаторами доменов; пусто = без ограничений |
| `description` | `""` | |

### Поддерживаемые типы источников {: #supported-source-types }

| Тип | Способ подключения | Примечания |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Пул asyncpg; PgBouncer включается через `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (файл БД) | Расширение DuckDB firebird community |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Использует драйвер/диалект PostgreSQL |
| `yugabytedb` | host/port | Использует драйвер/диалект PostgreSQL |
| `greenplum` | host/port | Использует драйвер/диалект PostgreSQL |
| `tidb` | host/port | Использует драйвер/диалект MySQL |
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
| `hudi` | `path` (URI таблицы) | Движок Hudi в ClickHouse, без копирования |
| `hive` | host/port (metastore) + `mapping.storage` | Бэкенд хранилища в `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + ключи S3 в `mapping` | Отдельный тип; всегда хранилище S3 |
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
| `airport` | `base_url` (расположение Flight) | Расширение DuckDB airport |
| `ingest` | приёмник push-событий | Внешние сервисы отправляют JSON-события через POST |
| **SaaS** | | |
| `sharepoint` | `base_url` или `host` + `mapping` | Аутентификация через `mapping.auth_type` |
| `splunk` | `host`/`port` или `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Отдельная модель `GovDataSource`; см. §GovData ниже |
| **Качество данных** | | |
| `soda` | host/port, направленные на pgwire Provisa | Требует extra `soda`; Elastic License 2.0, только self-hosted |
| `great_expectations` | host/port, направленные на pgwire Provisa | Требует extra `gx`; Apache 2.0 |

### Справочник по типам источников {: #source-type-reference }

У каждого типа, которому нужна неочевидная конфигурация, ниже есть короткая запись. Типы РСУБД (postgresql, mysql и т. д.) используют только общие поля выше, и отдельный раздел им не нужен.

#### GovData

Источники `govdata` используют собственную модель верхнего уровня, `GovDataSource`, вместо общей `Source`. Доступ разделён по группам тем.

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

Каждый subject соответствует одной или нескольким схемам GovData. Настройка источника `govdata` с указанием subject автоматически открывает все схемы этого subject.

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

Схемы `ref` и `geo` всегда включены как связующие. Их нельзя настроить, и в списке выше их нет. Чтобы дать доступ ко всем схемам, используйте тему `ALL`.

#### Kafka

Строка `kafka` в `sources:` только регистрирует источник. `details()` его коннектора возвращает `{}`; настоящая конфигурация находится в блоке верхнего уровня `kafka_sources[]`. Kafka всегда VIRTUAL_SOURCE: он идёт через движок и не имеет прямого пула.

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

**Временное окно** — `default_window` ограничивает каждый запрос недавним периодом времени, предотвращая неограниченное чтение из высоконагруженных топиков. Формат: `1h`, `30m`, `7d`, `60s`. По умолчанию `1h`. Автоматически подставляется как `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Клиенты могут переопределить это собственным фильтром `_timestamp` в аргументе `where` GraphQL.

**Дискриминатор** — Несколько конфигураций топиков могут указывать на один и тот же физический топик Kafka с разными значениями `discriminator`, порождая отдельные типы GraphQL. Дискриминатор автоматически подставляется как условие WHERE.

**Источник схемы**

| Значение | Поведение |
| ------- | ---------- |
| `registry` | Получить схему из Confluent Schema Registry |
| `manual` | Определить столбцы прямо в конфигурации (Schema Registry не нужен) |
| `sample` | Автоматически определить схему по примерам сообщений |

#### Snowflake

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

#### Databricks

`http_path` в `federation_hints` обязателен. `password` содержит персональный токен доступа. `catalog` необязателен и указывается в SQL или в подсказках; поле `database` его не содержит.

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

#### BigQuery

`project` в `federation_hints` обязателен. Аутентификация использует `GOOGLE_APPLICATION_CREDENTIALS` (путь к файлу ключа сервисного аккаунта) или Application Default Credentials в окружении движка.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Оба используют T-SQL поверх TDS с аутентификацией Azure AD. Входите через `az login` (разработка) или управляемое удостоверение (продакшен); движок читает учётные данные через `DefaultAzureCredential` из `azure-identity`. Параметры подключения берутся из переменных окружения, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) или `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), либо из `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` в `federation_hints` включает TLS на HTTP-интерфейсе. Порт по умолчанию `8123` (без шифрования) или `8443` (при `secure: "true"`). `schema` в `federation_hints` переопределяет удалённую схему.

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

#### Delta Lake / Iceberg

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

#### Hive / Hive S3

`host` и `port` указывают на Thrift-метастор Hive (порт по умолчанию 9083). Для `hive` задайте `mapping["storage"]`, чтобы выбрать бэкенд объектного хранилища. Отсутствие обязательного ключа приводит к явной ошибке, без запасного значения.

`hive_s3` — отдельный тип, который всегда объявляет хранилище S3; `mapping.storage` не нужен.

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

#### Redis

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

#### Elasticsearch

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

#### Prometheus

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

#### Google Sheets

`spreadsheet_id` в `federation_hints` обязателен. Аутентификация использует SECRET `gsheet` DuckDB, подготавливаемый в момент присоединения.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Файловые источники (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` обязателен. `files` обходит каталог в поисках файлов CSV, Parquet, XLSX и JSON, представляя каждый как таблицу. Все файловые источники VIRTUAL (маршрутизируются через движок; без прямого пула).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Наборы данных Kaggle** нельзя добавить через этот файл: им нужны действующий токен и выбор набора данных в форме источников (Sources → Subscriptions → Kaggle). Источник Kaggle, экспортированный в YAML, выглядит как `type: files` с `kaggle_owner` и `kaggle_ref` в `federation_hints`. Повторная загрузка из Kaggle требует мутации `refreshKaggleSource` или обновления в интерфейсе; правка YAML этого не делает. См. [Наборы данных Kaggle](sources.md#kaggle-datasets) в справочнике типов источников.

#### API / удалённые источники {: #api-remote-sources }

**openapi** — задайте `base_url` как базовый URL OpenAPI. Обнаружение схемы читает спецификацию OpenAPI при запуске.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — задайте `base_url`. Опциональные ключи `mapping`: `headers` (словарь статических заголовков), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` — это расположение сервера Arrow Flight. Расширение DuckDB airport.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — используют `host`, `port`, `path` и `federation_hints`.

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

**sharepoint**

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

**splunk**

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

#### Проверки качества данных (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Источник-проверщик указывает на собственный pgwire-endpoint Provisa, поэтому один драйвер postgres сканирует федеративное представление таблицы, за которой стоит Snowflake или Iceberg. Идентичность сканирования вы объявляете; она никогда не наследуется. К этому подключению применяется политика, и отфильтрованный набор строк не должен незаметно пропускать проверку. Ключи подключения берутся из `mapping`: `host`, `port`, `database`, `user`, `password`.

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

Каждая таблица результатов несёт `dq_contract`: YAML контракта Soda или JSON набора Great Expectations дословно. Столбцы, водяной знак и продвижения выводятся из него; как именно, см. в [Проверки качества данных](sources.md#data-quality-checkers).

**Выбор при установке.** Проверщик не встраивается: сканирование идёт в дочернем интерпретаторе, а библиотека устанавливается, только когда её называет оператор. Каждый путь установки (`install.sh`, `packaging/linux/first-launch.sh` и мастер macOS через `PROVISA_DQ_CHECKER`) записывает выбор в `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` читает этот ключ и экспортирует `PROVISA_EXTRAS`, который `docker-compose.app.yml` передаёт как build-аргумент в `ARG PROVISA_EXTRAS` файла `Dockerfile`:

| `dq_checker` | `PROVISA_EXTRAS` (уровень Docker) | Установка в нативный venv |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Установка демо-набора данных повышает `none` до `gx` и сообщает об этом, потому что демо-конфигурация регистрирует набор Great Expectations поверх `pet_store.pets`, и его карточке качества иначе было бы нечего показывать. Указание `soda` оставляет `soda` без изменений.

Если вы приходите к демо через pip, а не через установщик, этот шаг мастера пропускается, поэтому extra `demo` несёт тот же проверщик: `provisa run --demo` нужен `pip install 'provisa[embedded,demo]'`, чтобы сканирование заработало. Без него сканирование сообщает `data-quality checker 'great_expectations' is not installed` и называет команду установки.

Любое другое значение останавливает загрузчик, так что он никогда не стартует без проверщика, который запросил оператор. Extra `soda` подтягивает `soda-postgres`, а `gx` подтягивает `great-expectations[postgresql]`. Soda Core распространяется по Elastic License 2.0: `config/capabilities.yaml` помечает этот вариант как `cloud_eligible: false`, и размещённая плоскость его отклоняет.

## Домены {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Именование {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Соглашение об именовании {: #naming-convention }

Механизм именования — единственный источник истины для имён, видимых клиенту; физические имена столбцов бэкенда клиентам никогда не раскрываются. Каждый язык запросов выводит имя столбца из его `column.alias`, если он задан, иначе — из физического имени столбца по настроенному соглашению.

Соглашение GraphQL — одно из трёх предустановленных перечислений. Старые произвольные строки (`none`, `snake_case`, `camelCase`, `PascalCase`) объявлены устаревшими.

| Пресет | По умолчанию | Имена типов | Имена полей | Имена мутаций |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | да | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

Соглашение GraphQL по умолчанию — `apollo_graphql`, которое даёт имена полей и мутаций в camelCase. Соглашение SQL отдельное, по умолчанию `snake_case`, применяется через `apply_sql_name()`; соглашение GraphQL применяется через `apply_gql_name()`, а имя CQL выводится из имени GraphQL.

`domain_prefix: bool` — независимая опция, действующая вне зависимости от выбранного пресета.

Явный `column.alias` — каноническое имя: SQL использует его дословно без применения соглашения, GraphQL применяет к нему своё соглашение, а CQL выводится из имени GraphQL.

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

### Префикс домена {: #domain-prefix }

Когда `domain_prefix: true`, ко всем именам полей и типов GraphQL добавляется идентификатор домена через разделитель из двойного подчёркивания:

| Таблица | Домен | Имя поля |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Это предотвращает коллизии имён, когда в разных доменах есть таблицы с одинаковым именем, и делает запросы самодокументируемыми.

### Правила именования {: #naming-rules }

Regex-правила, применяемые к именам таблиц при генерации имён полей GraphQL. Применяются по порядку до разрешения уникальности.

## Таблицы {: #tables }

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
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

По умолчанию у таблицы нет ни одного разрешения на запись, поэтому ни одна роль не может в неё писать. Разрешение даёт роли одну операцию (`insert`, `update` или `delete`) над таблицей. В таблице не больше одного разрешения на роль и операцию.

| Поле разрешения | Значение |
| ----------- | ------- |
| `columns` | Колонки, которые `INSERT` может перечислить, а `UPDATE` — задать. Разрешение `delete` их не называет. |
| `rows` | Строки, которых может касаться `UPDATE` или `DELETE`: SQL-предикат над колонками таблицы. У разрешения `insert` его нет. |
| `check` | Условие, которому должна удовлетворять каждая строка, записанная `INSERT` или оставленная `UPDATE`: SQL-предикат. У разрешения `delete` его нет. |
| `presets` | Колонки, которые разрешение задаёт само. Инструкция не может их передать, поэтому колонка с пресетом не входит также в `columns`. |

Если опустить `rows` или `check`, разрешение берёт фильтр строк роли на чтение для этой таблицы: роль пишет те строки, которые читает, если разрешение не говорит иного. `"TRUE"` означает любую строку. Прежние ключи `writable_by` у колонки и `column_presets` у таблицы отклоняются по имени; конфигурация не загрузится, пока их не заменят на `write_grants`.

Каждая запись на любой поверхности допускается в таком порядке: у роли есть право `write`; у роли есть разрешение на операцию над таблицей; каждая колонка, которую пишет инструкция, указана в разрешении; пресеты разрешения добавляются к инструкции; предикат `rows` присоединяется через AND к `WHERE` инструкции `UPDATE` или `DELETE`; а `check` вычисляется на новых строках до того, как что-либо отправлено источнику. Инструкция, которую `check` не может решить, отклоняется. Колонки разрешения также определяют, какие поля мутаций GraphQL предлагает роли: по одному полю на каждую операцию, на которую у неё есть разрешение, ровно с этими колонками.

Некоторым инструкциям нужно более широкое разрешение. `TRUNCATE` требует разрешения `delete` на все строки. `MERGE` требует разрешений `insert` и `update`, оба на все строки. `INSERT ... ON CONFLICT DO UPDATE` требует разрешения `update` на все строки без пресетов. `COPY ... FROM STDIN` требует разрешения `insert` без пресетов.

Разрешения наследуются. Дочерняя роль берёт для каждой таблицы и операции разрешение ближайшей роли в своей цепочке, у которой оно есть; собственное идёт первым. Мета-роль держит свёртку разрешений своих участников: объединение их колонок, OR их предикатов `rows` и `check` и только те пресеты, которые все участники задают одинаково.

В административном интерфейсе раздел **Запись** таблицы перечисляет разрешения и редактирует их вместе.

### Псевдонимы {: #aliases }

Псевдонимы таблиц и столбцов переопределяют имя GraphQL по умолчанию. Полезно для:

- Переименования непонятных имён в базе данных (например, `tbl_cust_seg` → `customer_segments`)
- Избегания сокращений в слое API
- Создания чистого предметно-ориентированного словаря

### Описания {: #descriptions }

Описания таблиц и столбцов включаются в сгенерированный SDL GraphQL. Они отображаются в проводнике документации GraphiQL и в запросах интроспекции. Задавайте их в конфигурационном YAML или через панель администрирования.

### Path (вычисляемое извлечение из JSON) {: #path-computed-json-extraction }

Столбцы могут извлекать значения из исходного столбца JSON/JSONB с помощью `path` в точечной нотации. Это полезно для слабоструктурированных данных в сообщениях Kafka, документах MongoDB или столбцах JSONB PostgreSQL.

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

Формат path — `source_column.key1.key2...`. Компилятор генерирует `json_extract_scalar(source_column, '$.key1.key2')` в SQL.

**Влияние на маршрутизацию:** столбцы с path используют операторы JSON PostgreSQL (`->>`), которые нативно поддерживаются прямой маршрутизацией PG. Для источников, отличных от PostgreSQL (MySQL, SQL Server и т. д.), запросы со столбцами path автоматически маршрутизируются через движок федерации. На мутации это не влияет, поскольку столбцы path — вычисляемые поля только для чтения.

### Типы маскирования {: #masking-types }

| Тип | Поля | Описание |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (только строковые столбцы) |
| `constant` | `value` | Замена литералом (NULL, 0, MAX, MIN, произвольное значение) |
| `truncate` | `precision` | DATE_TRUNC (только столбцы даты/времени) |

## Связи (Relationships) {: #relationships }

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

### Автоматическая материализация {: #auto-materialization }

Установите `materialize: true` на связи, чтобы автоматически сгенерировать материализованное представление для межисточниковых JOIN. Это позволяет избежать дорогостоящих федеративных запросов за счёт предварительного вычисления результата JOIN.

- Материализованные представления генерируются только для межисточниковых связей (JOIN в пределах одного источника уже быстрые)
- Для связи, поддерживаемой через junction-таблицу, материализованное представление покрывает двухшаговый обход — переход к источнику, переход к junction, дискриминатор и собственные столбцы junction как атрибуты ребра. Junction считается отдельным звеном, поэтому ребро межисточниковое, если любая из трёх таблиц находится в другом источнике
- Материализованное представление изначально устаревшее и заполняется фоновым циклом обновления
- Мутации любой из таблиц-источников помечают материализованное представление как устаревшее для повторного обновления
- `refresh_interval` по умолчанию 300 секунд (5 минут)

## Роли {: #roles }

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
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
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Роли с `parent_role_id` наследуют возможности, доступ к доменам, права на столбцы и объекты, а также правила безопасности на уровне строк от родителя, при этом собственное правило безопасности на уровне строк дочерней роли для таблицы имеет приоритет. Цепочка разворачивается при запуске.

### Возможности (Capabilities) {: #capabilities }

| Возможность | Описание |
| ----------- | ------------- |
| `source_registration` | Регистрация источников данных |
| `table_registration` | Регистрация таблиц |
| `relationship_registration` | Определение связей |
| `security_config` | Настройка безопасности на уровне строк, маскирования |
| `query_development` | Выполнение запросов |
| `full_results` | Обход ограничений выборки |
| `admin` | Все возможности |

## Правила безопасности на уровне строк (RLS) {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Материализованные представления {: #materialized-views }

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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Представления (управляемые вычисляемые наборы данных) {: #views-governed-computed-datasets }

Представления — это вычисляемые наборы данных, определённые на SQL, с полным управлением на уровне столбцов. Это управляемый механизм добавления агрегаций, преобразований и производных метрик в модель.

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

### Материализованное или живое {: #materialized-vs-live }

- **`materialize: true`**: Provisa создаёт таблицу через CTAS и обновляет её по расписанию. Быстрее запросы, но данные могут устаревать до `refresh_interval` секунд.
- **`materialize: false`**: Provisa создаёт федеративное представление. Запросы всегда возвращают актуальные данные, но могут выполняться медленнее для сложных агрегаций.

Представления проходят тот же конвейер управления, что и таблицы: RLS, маскирование, выборку и видимость по ролям. Поэтому ничто не может добавить в платформу новую семантику без надзора стюарда.

### Представления с историей (путешествие во времени) {: #views-with-history-time-travel }

Материализованное представление может хранить каждую версию своих строк и читаться на прошлый момент. Оно объявляет, как их хранит и к какому ключу относится версия:

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    bitemporal_mode: delta            # snapshot | delta
    bitemporal_key: [customer_id]     # required
    bitemporal_source_time: updated_at  # optional: the source's change time
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```
| Поле | Обязательное | Описание |
| ------- | ---------- | ------------- |
| `bitemporal_mode` | Да | `snapshot` добавляет весь результат при каждом обновлении; `delta` добавляет только изменившееся |
| `bitemporal_key` | Да | Колонки, к которым относится версия |
| `bitemporal_source_time` | Нет | Колонка, хранящая момент изменения каждой строки в источнике: часы, по которым читает путешествие во времени |

**Выбор часов.** С `bitemporal_source_time` версии упорядочиваются и читаются по времени самого источника: «на прошлый вторник» означает прошлый вторник в источнике. Без него они упорядочиваются и читаются по моменту, когда Provisa загрузил каждое изменение, а это следует за периодичностью обновления, а не за моментом изменения строки в источнике. Оба варианта допустимы. Время загрузки подходит для частых обновлений, для данных, у которых время в источнике не важно, и для чтений, которые должны касаться времени загрузки, например для аудита того, что и когда хранил Provisa.

Версия действует со своего момента до момента следующей версии. У удаления нет времени изменения в источнике, и оно вступает в силу, когда Provisa его записал.

**Чтение.** Обычное чтение возвращает версию каждого ключа, действующую сейчас. Заголовок `X-Provisa-As-Of` читает на момент или за диапазон, заданный как интервал ISO 8601 `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```
Момент возвращает версию каждого ключа, действовавшую тогда. Диапазон возвращает каждую версию, действовавшую в какой-либо момент внутри него, по строке на версию.

**Как было записано.** Исправление, записанное позже, меняет то, что представление говорит о прошлом: версия, действовавшая 30 сентября, становится исправленной, как только исправление записано. Чтобы прочитать представление так, как Provisa записал его на какой-то момент, до любого позднейшего исправления, добавьте `X-Provisa-As-Recorded` — один момент в формате ISO 8601:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Все версии, записанные после этого момента, исключаются, и as-of затем выбирает версию, действовавшую в его собственный момент. С одним `X-Provisa-As-Recorded` чтение возвращает версию каждого ключа, действовавшую по записям на тот момент. Он принимает один момент, а не диапазон; диапазон или некорректное значение отклоняются с HTTP 400 (`data.invalid_as_recorded`).

Оба момента можно передать Provisa и без заголовков. Момент as-of — это аргумент `as_of` у `resolve` (GraphQL, gRPC, MCP), параметр запроса REST `as_of` и опция `as_of` тикета Arrow Flight, а диапазон, переданный в `resolve`, возвращает `versions` строки вместо её `row`. Момент as-recorded — это аргумент `as_recorded` у `resolve`, параметр запроса REST `as_recorded` и опция `as_recorded` тикета Arrow Flight. Запись аудита каждой инструкции хранит оба момента, с которыми она читала, `as_of` и `as_recorded`. Права, маски и правила строк действуют как при любом чтении. Некорректное значение as-of или диапазон, конец которого не позже начала, отклоняются с HTTP 400 (`data.invalid_as_of`).

Каждое представление с историей возвращает ещё три колонки, предоставляемые так же, как его колонка времени источника (иначе первая ключевая колонка):

| Колонка | Значение |
| ------- | ------- |
| `source_from` | Когда эта версия вступила в силу: её время источника или, при его отсутствии, время загрузки |
| `source_to` | Когда вступила в силу следующая версия; пусто, пока действует эта |
| `recorded_at` | Когда Provisa записал эту версию |

**Время источника из захвата изменений данных.** Таблица, загруженная захватом изменений данных, несёт время своего коммита как `source_committed_at`, чтобы представление с историей указало его как время источника:

- Debezium: время коммита базы данных, которое несёт каждое событие изменения (`source.ts_ms`). У строки, скопированной полной сборкой реплики, нет события изменения, поэтому её `source_committed_at` пуст, пока строка снова не изменится, если только источник не PostgreSQL с включёнными метками времени коммита (следующий пункт).
- PostgreSQL с сигналом изменения `native`: время коммита, которое PostgreSQL хранит для каждой строки (`pg_xact_commit_timestamp(xmin)`), читаемое собственным драйвером источника при сборке реплики. PostgreSQL хранит его только при `track_commit_timestamp = on`. Включите его, чтобы получать время источника с нативным сигналом; без него `source_committed_at` пуст. Включение или выключение параметра пересобирает реплику.

Темы Kafka без конверта Debezium не несут времени коммита.

### Представления только для чтения {: #query-only-views }

И представления с `materialize: true`, и с `materialize: false` предоставляют свой тип GraphQL только для запросов. Для отношений, поддерживаемых `view_sql`, не генерируются мутации insert, upsert, update или delete.

## Кеш {: #cache }

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Иерархия кеша {: #cache-hierarchy }

Порядок разрешения TTL (наиболее конкретное значение побеждает): **таблица** > **источник** > **глобальное значение по умолчанию**. Используется первое ненулевое значение.

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

Установка `cache_enabled: false` на источнике отключает кеширование для всех таблиц этого источника независимо от TTL на уровне таблицы. Ключи кеша всегда включают `role_id` и значения контекста безопасности на уровне строк для разделения по соображениям безопасности.

### Включение для отдельного запроса {: #per-request-opt-in }

Кеш ответов выключен для каждого запроса, который его не запрашивает. Запрос включает его так:

| Интерфейс | Включение |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` на операции, при необходимости `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | строка комментария `-- @provisa cache=true` или `-- @provisa cache_ttl=60` в инструкции |
| Cypher (`/data/cypher`, Neo4j Query API, Bolt, Arrow Flight) | строка комментария `// @provisa cache=true` или `// @provisa cache_ttl=60` в запросе |
| gRPC (и HTTP-прокси gRPC) | метаданные вызова (заголовки на прокси) `x-provisa-cache: true` или `x-provisa-cache-ttl: 60` |
| REST, JSON:API и, как альтернатива указанию в тексте: GraphQL по HTTP, `/data/cypher`, Neo4j Query API | заголовок запроса `Cache-Control: max-age=60` (или `max-stale=60`) либо `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

`ttl`, не являющийся целым числом, приводит к ошибке запроса. Запрос, который указывает включение дважды (в тексте и в заголовке или в двух заголовках) с разными значениями, завершается ошибкой `400`; ни одному значению предпочтение не отдаётся. `Cache-Control: no-cache` или `no-store` означает отсутствие кеша, как без заголовка.

Без подсказки запрос не читает кеш и не пишет в него. С ней по-прежнему решают настройки выше: результат кешируется, только если у каждого читаемого источника `cache_enabled: true`, а разрешённый TTL каждой таблицы больше 0. Запись живёт `ttl` запроса, если он указан, иначе — кратчайший разрешённый TTL прочитанных таблиц.

Подсказка лишь соглашается на более старые данные ради скорости. Она никогда не делает чтение более свежим, чем позволяют настройки оператора: свежесть загрузки и реплики (`cache_ttl`, `change_signal`), `row_materialize`, снимки `load_protected` и проверки свежести действуют для запроса с подсказкой точно так же, как без неё. См. [Свежесть и нагрузка: кто решает](#freshness-and-load-who-decides).

`@noCache` и `-- @provisa no_cache=true` больше не существуют: при выключенном по умолчанию кеше обходить нечего.

Запись делает недействительной каждую запись кеша, которую пишущая организация хранит для записанных таблиц; записи другой организации для той же таблицы не затрагиваются. Если инвалидация не удалась, запрос завершается ошибкой, а не оставляет устаревшие записи.

## Актуальность и нагрузка: кто решает {: #freshness-and-load-who-decides }

Три стороны влияют на то, насколько свежим будет результат и какую нагрузку запрос создаст на системы за ним.

- **Исходный источник.** В идеале он сам управляет своим противодавлением: лимиты соединений, тайм-ауты операторов, реплики для чтения.
- **Оператор.** Защищает платформу, то есть сам Provisa, от противодавления. Если источник не может защитить себя, оператор защищает и его. Настройки для этого задаются на источниках и таблицах: `load_protected`, `replicate`, `federation_hints` источника, порог перенаправления больших результатов, кластер Kafka, в который пишут sink-и, и зарегистрированный watermark каждой таблицы.
- **Конечный пользователь.** Выбирает между скоростью и актуальностью, запрос за запросом.

Настройки оператора являются нижней границей. Запрос может сдвинуться выше неё, к более старым данным или меньшей нагрузке; включение кэша ответов — обычный пример. Опуститься ниже он не может никогда. Подсказка в запросе, которая сделала бы это, отклоняется с ошибкой, называющей настройку оператора; её не применяют и не отбрасывают молча.

| Входные данные запроса | Что разрешено | Отклоняется, когда |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Выбрать прямой драйвер для одного источника | таблица, которую читает запрос, имеет `load_protected` или `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Задать свойство сессии движка, не заданное в `federation_hints` источника | меняет свойство, заданное оператором |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Перенаправить результат раньше | значение выше порога оператора |
| `@sink(broker:)`, брокер в `X-Provisa-Sink` | Повторить брокер оператора | указан другой брокер или `KAFKA_BOOTSTRAP_SERVERS` не настроен |
| `@watermark` в подписке | Повторить зарегистрированный watermark таблицы | указан другой столбец или у таблицы его нет |
| `@cached`, `-- @provisa cache=true` | Отдать более старый результат из кэша | никогда; запись кэша не бывает свежее чтения, которое её сохранило |

Отказы возвращают HTTP 403 с кодом `query.operator_floor`, SQLSTATE `42501` через pgwire и `PERMISSION_DENIED` через Flight и gRPC.

Таблица с `load_protected` или `replicate: 0` никогда не читается запросом напрямую ни через один транспорт. Запросы читают её реплику. Источник читают только репликация и её обновления: обновление выполняется, когда этого требует `cache_ttl` или проверка актуальности; без них реплика строится один раз и далее обновляется только через поток изменений или планировщик. Для такой таблицы с `off_peak_window` (у таблицы или её источника, в часовом поясе `off_peak_tz`) обновление выполняется только пока окно открыто: обновление, наступившее или запрошенное вне окна, ждёт его следующего открытия, а окно без `cache_ttl` обновляет таблицу один раз за открытие. Первое построение никогда не откладывается, потому что чтения таблицы ждут его.

### Настройка `replicate` {: #the-replicate-setting }

`replicate` — целое число на таблице. Таблица без значения берёт значение своего источника, а если не задано ни то ни другое, действует глобальный порог. Реплика — это копия таблицы источника, хранящаяся в хранилище движка и обновляемая по настройкам актуальности таблицы; это не непрерывный поток изменений.

| Значение | Название | Смысл |
| --- | --- | --- |
| не задано | По умолчанию | Действует глобальный порог |
| `-1` | Никогда | Читается напрямую везде, где есть прямой путь. Реплицируется только на движке, который не может читать источник на месте |
| `N` больше 0 | Hot-N | Читается напрямую, пока таблица не превысит N проверенных политиками запросов за интервал, затем обслуживается из реплики |
| `0` | Всегда | Чтение идёт из реплики |

Гарантией является только «Всегда». «Никогда» и значения Hot действуют по возможности. `load_protected` вместе с `replicate: -1` отклоняется при загрузке конфигурации и при сохранении. У источника с `replicate: 0` нет прямого подключения ни на одном движке, поэтому каждая его таблица обслуживается из своей реплики.

Нижняя граница оценивается по таблицам, которые читает запрос. Запрос, не читающий ни одной таблицы, обслуживаемой из реплики, сохраняет прямой маршрут, даже если у его источника есть другие реплицируемые таблицы.

Реплика индексируется по объявленному первичному ключу и по столбцам, по которым соединяются объявленные связи; индексы строятся после её загрузки. Где эти индексы ускоряют соединения, где хранить реплики и как выбирать их актуальность — см. [Replicas](replicas.md).

### Hot-репликация {: #hot-replication }

Таблица, у которой `replicate` не задан или равен `N` больше 0, читается напрямую, пока не станет загруженной.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Настройка | Переменная окружения | По умолчанию |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

Все три — настройки оператора: admin UI показывает их в разделе Hot Tables, а сохранённое значение применяется при следующей оценке.

Как считается таблица:

- Проверенный политиками запрос засчитывает каждую читаемую таблицу один раз, сколько бы раз он её ни читал.
- Запрос, обслуженный из кэша ответов, не засчитывается. Он не нагрузил источник.
- Запрос, отклонённый политиками доступа, не засчитывается.

Таблица повышается, когда её счётчик достигает её порога (её собственного `N`, иначе порога её источника, иначе `hot_threshold`), и понижается, когда счётчик опускается ниже половины порога. Повышение запрашивает построение реплики; до его завершения таблица читается напрямую, затем из реплики. Понижение сразу возвращает чтение к источнику.

Таблица не повышается, если в ней больше строк, чем `hot_max_rows`, если её сигнал изменений — `ttl` или `ttl_probe`, а ни у неё, ни у её источника не объявлен `cache_ttl`, или если её уже ведёт горячий уровень Redis. Сводка таблицы в admin UI указывает, какой из случаев имеет место.

Счётчики хранятся в Redis. При общем Redis (`cache.redis_url`) они охватывают все процессы развёртывания. При встроенном Redis они ведутся по процессам, поэтому повышение работает только когда запросы обслуживает один рабочий процесс; при нескольких рабочих процессах и без общего Redis ничего не повышается, о чём говорится в сводке.

## Аутентификация {: #authentication }

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

### Типы провайдеров аутентификации {: #auth-provider-types }

| Провайдер | Сценарий использования | Проверка токена |
| ---------- | ---------- | ----------------- |
| `simple` | Локальная разработка/тестирование. Пользователи определены в YAML. | JWT, подписанный `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (все методы). | `verify_id_token()` SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Сопоставление тенанта и клиентских ролей. | Проверка JWT на основе JWKS |
| `oauth` | Обобщённый OIDC (Okta, Azure AD, Auth0, PingFederate). | JWKS с discovery URL |
| `basic` | Автономные развёртывания. Учётные записи хранятся в собственном хранилище Provisa. | Пароль bcrypt или SCRAM-SHA-256 на pgwire |

Учётные данные суперпользователя (блок `superuser`) работают с любым провайдером и всегда разрешаются в роль admin со всеми возможностями. Используется для первоначальной настройки до подключения внешней аутентификации.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Заставляет pgwire анонсировать SASL с `SCRAM-SHA-256`, так что пароль доказывается, а не передаётся открытым текстом. Это применяется только к провайдеру `basic` — ни один другой провайдер не хранит верификаторы RFC 5802, необходимые для SCRAM, — и привязка канала не предлагается.

Верификаторы нельзя получить из существующих хешей bcrypt. Верификатор записывается всякий раз, когда пароль проходит в открытом виде, поэтому первое SCRAM-соединение каждого пользователя следует за его следующей регистрацией, входом, сменой пароля или сбросом администратором. До этого момента соединения этого пользователя используют резервный обмен открытым текстом по TLS; протокол не раскрывает, кто уже перешёл на SCRAM.

### Ограничение частоты попыток входа (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Включено по умолчанию с указанными значениями; блок только настраивает их. Счётчик находится на уровне проверки учётных данных, поэтому неудачи через HTTP, pgwire и Bolt накапливаются для одного и того же субъекта, и блокировка действует на всех поверхностях. Она действует в рамках процесса: несколько воркеров API каждый допускают до `max_attempts`.

### Персональные токены доступа {: #personal-access-tokens }

Персональные токены доступа не требуют блока конфигурации — они всегда принимаются, и хранилище создаётся вместе с остальной схемой плоскости управления. Настраивается срок действия, который пользователь может запросить при выпуске: от 1 до 366 дней или "без срока действия" для токена, который не истекает. См. [Модель безопасности](security.md#personal-access-tokens).

### Взаимный TLS (Mutual TLS) {: #mutual-tls }

Проверка клиентских сертификатов настраивается через переменную окружения, а не в `provisa.yaml`, наряду с настройками сертификатов TLS, которые она расширяет.

| Переменная | По умолчанию | Значение |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | не задано | Пакет PEM с CA, которым разрешено подписывать клиентские сертификаты. Установка этой переменной включает проверку клиентских сертификатов |
| `PROVISA_MTLS_MODE` | `required`, как только задан CA | `required` или `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя (common name) сертификата совпадало с именем пользователя, от имени которого аутентифицируется соединение |

Для каждого протокола можно задать отдельное переопределение по той же схеме именования, что и настройки TLS. Режим, заданный без CA, или режим, не являющийся ни одним из допустимых значений, приводит к отказу от запуска, а не к обслуживанию соединений, которые оператор считает проверенными.

### Адресация организации через TLS {: #addressing-an-org-over-tls }

Настраивать ничего не нужно. В развёртывании с несколькими организациями pgwire и Bolt читают организацию из имени хоста, к которому обратился клиент, переданного в TLS ClientHello, точно так же, как HTTP читает его из заголовка `Host`. Клиент, подключающийся к `acme.provisa.dev`, запрашивает организацию `acme`; запрос отклоняется, если аутентифицированный субъект не является её участником. Подключение по IP-адресу не запрашивает организацию, что соответствует любому соединению в развёртывании с одной организацией.

### Полный пример конфигурации аутентификации (закомментировано) {: #full-auth-config-example-commented-out }

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

## Мутации Upsert {: #upsert-mutations }

Для таблиц с первичным ключом Provisa автоматически генерирует поля мутации `upsert_<table>`. Они компилируются в upsert на целевом диалекте — `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` в PostgreSQL, `ON DUPLICATE KEY UPDATE` в MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Столбцы конфликта выводятся из метаданных первичного ключа. Применяются все правила видимости столбцов и разрешений на запись.

## Distinct On

Аргумент `distinct_on` выбирает первую строку для каждого отдельного значения указанных столбцов. Доступен для корневых полей запроса.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Компилируется в `SELECT DISTINCT ON (region) ...` в PostgreSQL. Для диалектов, отличных от PG, используется резервный вариант с оконными функциями.

## Пресеты разрешений на запись {: #write-grant-presets }

Разрешение на запись само задаёт колонки при вставке и обновлении. Пресеты объявляются в списке `presets` разрешения, для каждой роли и операции.

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| Источник | Поведение |
| -------- | ---------- |
| `session` | Подставляет значение указанной переменной сеанса (`name`) запроса |
| `now` | Подставляет момент записи |
| `literal` | Подставляет константу (`value`) |

Пресеты добавляются к инструкции при компиляции, до генерации SQL. Инструкция не может передать колонку с пресетом, и колонки с пресетом нет во входном типе мутации.

## Наследуемые роли {: #inherited-roles }

Роли могут наследоваться от одной родительской роли через `parent_role_id`. Цепочка разворачивается при запуске. Дочерняя роль обладает объединением возможностей и доступа к доменам своих предков; столбец, метрика, функция или вебхук, предоставленные предку, предоставляются и дочерней роли; а правила безопасности на уровне строк предка применяются к дочерней роли для каждой таблицы, начиная с ближайшей роли, при этом собственное правило дочерней роли для таблицы заменяет родительское.

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Поддерживается многоуровневое наследование. Явные возможности и domain_access дочерней роли объединяются с родительскими. Родитель должен быть существующей ролью, не может быть самой ролью и не может замыкать цикл; каждое из этих условий проверяется и отклоняется при сохранении.

## Запланированные триггеры {: #scheduled-triggers }

Триггеры на основе cron, вызывающие URL вебхука по расписанию. Использует APScheduler.

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

Запланированные задачи управляются через панель администрирования (переключатель включения/выключения) или административную мутацию `toggle_scheduled_task`.

Триггер может срабатывать по зарегистрированной таблице вместо расписания: `on: {table, event}` называет таблицу в виде `source_id.schema.table` и событие, `change`, `error` или `quarantine`. Триггер `command` запускает зарегистрированную команду от своей роли `role`, через собственную управляемую диспетчеризацию команды, и передаёт ей в аргументах то, что его вызвало. Изменение передаёт `table` и, где сигнал изменений его несёт (Kafka, Debezium), загруженный пакет как `changes`. Событие `error` или `quarantine` передаёт `table`, `event`, `reason` и `error`. Триггер, команда которого объявляет аргумент, не передаваемый триггером, отклоняется при сохранении. Изменение срабатывает один раз на каждую серию, о которой сообщает сигнал изменений, или когда поступает запись, сделанная через Provisa; таблица, обслуживаемая из реплики, срабатывает, когда сборка загружает строки, отличающиеся от прошлых. Собственная запись команды — это изменение, поэтому триггеры выстраиваются в цепочку. Каждый запуск записывается со временем, результатом (`succeeded`, `refused` или `failed`), ошибкой и тем, что его вызвало, и административный запрос `scheduledTasks` его показывает. Срабатывают только триггеры prod, и в исходной базе данных ничего не устанавливается.

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## Формат OrderBy {: #orderby-format }

OrderBy использует формат `{column: direction}` с перечислением направления из 6 значений:

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

Сортировка по связям поддерживается через вложенные объекты:

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Наблюдаемость {: #observability }

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

### Фильтры телеметрии {: #telemetry-filters }

Provisa использует два независимых пути экспорта OTLP: ваш внутренний коллектор и опциональный эндпоинт поддержки Provisa. У каждого пути свой фильтр. Фильтры выполняются внутри оборачивающего `_FilteringExporter` перед тем, как спаны покидают процесс, — исходные объекты спанов никогда не изменяются.

**`telemetry_filter`** — управляет тем, что попадает в ваш внутренний коллектор.

| Ключ | Тип | По умолчанию | Описание |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Заменяет строковые и числовые литералы в `db.statement` на `?` |
| `redact_attributes` | list[str] | `[]` | Ключи атрибутов, полностью удаляемые из каждого спана |

**`support_telemetry_filter`** — управляет тем, что попадает на эндпоинт поддержки Provisa. Удаление SQL-литералов по умолчанию включено (`true`) на этом пути, поскольку данные запросов принадлежат вам.

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

### Эндпоинт поддержки {: #support-endpoint }

`support_endpoint` (или переменная окружения `PROVISA_SUPPORT_OTLP_ENDPOINT`) пересылает телеметрию в службу поддержки Provisa для диагностики. Если не задано, никакие данные не покидают вашу инфраструктуру через этот путь. Фильтр поддержки применяется независимо от внутреннего фильтра — вы можете удалять SQL-литералы из обоих экспортов, при этом всё равно делясь со службой поддержки данными о времени выполнения спанов и ошибках.

### Определение протокола эндпоинта {: #endpoint-protocol-detection }

Provisa выбирает OTLP/HTTP или OTLP/gRPC на основе схемы URL эндпоинта. URL, начинающиеся с `http://` или `https://`, используют OTLP/HTTP, с автоматическим добавлением `/v1/traces`, `/v1/metrics` и `/v1/logs`. Любая другая схема использует OTLP/gRPC с `insecure=True`.

## Движок федерации {: #federation-engine }

Настройка движка федерации опциональна. По умолчанию используется `duckdb` — без конфигурации, в процессе, без необходимости во внешнем сервисе. Выберите другой движок, если вам нужен масштаб MPP или вы хотите переиспользовать существующее хранилище данных.

Приоритет: переменная окружения `PROVISA_ENGINE` → сохранённое поле конфигурации `federation_engine` в панели администрирования → `duckdb`. Изменения вступают в силу при перезапуске сервиса.

### Обзор движков {: #engine-overview }

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

### Справочник по движкам {: #engine-reference }

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

В процессе; без внешнего сервиса. Движок по умолчанию. `PROVISA_DATA_DIR` определяет, где находится встроенное хранилище (по умолчанию `~/.provisa`).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

Хранилище материализации по умолчанию — `~/.provisa/materialize.duckdb` — единственный движок с хранилищем по умолчанию, отличным от PostgreSQL.

#### clickhouse (встроенный) / clickhouse-server {: #clickhouse-embedded-clickhouse-server }

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

#### Реляционные движки (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) и `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Один ключ на каждую реляционную базу данных, доступную по сети, все работают на одной и той же среде выполнения "только приземление" (без федерации к внешним источникам): каждый источник приземляется в хранилище и запрашивается там же. Ключ выбирает базу данных; `PROVISA_ENGINE_URL` несёт DSN в формате, который принимает её диалект. `sqlalchemy` — универсальный вариант для базы данных без собственного ключа. Встроенные в файл хранилища (SQLite, Access) не предлагаются — сервер должен быть доступен по сети.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

Хранилище материализации по умолчанию — `TENANT_DATABASE_URL`.

### Хранилище материализации {: #materialization-store }

Когда источник не может подключиться напрямую (нет коннектора ATTACH для выбранного движка), он приземляется в хранилище материализации движка. Порядок разрешения: явный `PROVISA_MATERIALIZE_URL` → объявленное значение по умолчанию движка → явная ошибка (без незаметного резервного варианта).

DuckDB объявляет свой встроенный файл (`~/.provisa/materialize.duckdb`) как значение по умолчанию. Все остальные движки по умолчанию используют `TENANT_DATABASE_URL` (PostgreSQL). Переопределить можно для любого движка через `PROVISA_MATERIALIZE_URL`.

Хранилище определяет, что индексы реплики дают соединениям: хранилище PostgreSQL индексирует реплики, и движок, выполняющий в нём SQL, читает эти индексы, тогда как DuckDB соединяет реплики своего встроенного файла поверх копии, создаваемой на каждый запрос. См. [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Подсказки федерации для отдельных источников {: #per-source-federation-hints }

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

## Переменные окружения {: #environment-variables }

| Переменная | По умолчанию | Описание |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI хранилища плоскости управления (SQLAlchemy async); принимает `sqlite+pysqlite://…` / `duckdb://…` для встроенного desktop-хранилища |
| `PLATFORM_DATABASE_URL` | — | URI реестра платформы (каталог тенантов, реестр движков); обязателен при запуске, без резервного варианта |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` использует встроенный fakeredis вместо сервера Redis — без Docker |
| `PG_HOST` | `localhost` | Хост PostgreSQL |
| `PG_PORT` | `5432` | Порт PostgreSQL |
| `PG_DATABASE` | `provisa` | База данных PostgreSQL |
| `PG_USER` | `provisa` | Пользователь PostgreSQL |
| `PG_PASSWORD` | `provisa` | Пароль PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Ключ движка федерации |
| `PROVISA_ENGINE_URL` | — | URL подключения для движков, управляемых через URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Переопределяет DSN хранилища материализации (по умолчанию — объявленное значение движка) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Каталог данных для встроенного хранилища DuckDB |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Бюджет повторных попыток чтения уровня 1 в секундах; экспоненциальная задержка с полным джиттером |
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
| `PROVISA_MTLS_CLIENT_CA` | — | Пакет PEM с CA, которым разрешено подписывать клиентские сертификаты; установка этой переменной включает проверку клиентских сертификатов на pgwire, Bolt, gRPC и Flight |
| `PROVISA_MTLS_MODE` | `required`, как только задан CA | `required` или `optional`; любое другое значение приводит к отказу от запуска |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Требовать, чтобы общее имя (common name) сертификата совпадало с аутентифицирующимся именем пользователя |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Список сайтов через запятую, которым разрешено открывать WebSocket-соединение Bolt из браузера; если не задано, отклоняется любое происхождение (origin) браузера |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras Pyproject, встроенные в образ приложения; `scripts/provisa` выводит их из `dq_checker` в `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Только для установщика: `none`/`soda`/`gx`, считывается `first-launch.sh` в неинтерактивном режиме и записывается в `config.yaml` как `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Ключ Claude API (обнаружение) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Переопределяет `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Переопределяет `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Переопределяет `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Переопределяет `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Задержка сброса пакетного процессора спанов |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Переопределяет `observability.support_endpoint` |
