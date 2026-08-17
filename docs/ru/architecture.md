# Архитектура Provisa

## Обзор

Provisa — это платформа виртуализации данных, управляемая конфигурацией, специально спроектированная для питания семантического слоя от небольших команд до крупных предприятий. Она предоставляет единый API поверх разнородных источников данных с governance, безопасностью и оптимизацией производительности. Клиенты выполняют запросы через SQL, GraphQL или Cypher; все три — полноправные интерфейсы с идентично применяемым governance. (REQ-002, REQ-038)

Разграничение семантического слоя важно. Чтобы добавить что-то в семантический слой, нужно создать новые источники данных или агрегаты внутри слоя виртуализации данных. Это создаёт чистое разделение — никакие новые дополнения к семантике не могут быть сделаны за пределами платформы, что обеспечивает подлинный data governance. (REQ-136) Применение происходит на уровне компилятора: каталог утверждённых связей — это источник истины независимо от того, какой язык запросов используется. (REQ-002)

Provisa спроектирована быть высокопроизводительной для операционных нужд и высокомасштабируемой для аналитических нужд предприятия. Одна платформа обслуживает и то, и другое без потери скорости или масштабируемости.

```text
Config YAML → PG Metadata → Federation Catalogs
                               ↓
         Federation engine metadata → Schema Generator → SDL / SQL catalog / Cypher labels / gRPC proto (per role)
                                     ↓
                     Query → Parser → SQL Compiler → Transpiler
                                     ↓
                             Router (Smart Dispatch)
                         /           |            \
                    Federation  Direct PG      Direct MySQL/etc.
                         \           |            /
                              Executor Pool
                                     ↓
                         ┌───── Inline ─────┐     ┌──── Redirect ────┐
                         │  JSON (HTTP)     │     │  CTAS → S3       │
                         │  Arrow (Flight)  │     │  (Parquet, ORC)  │
                         │  Protobuf (gRPC) │     │  Provisa → S3    │
                         └─────────────────-┘     │  (JSON, CSV, …)  │
                                                  └─────────────────-┘
```

## Интерфейсы запросов

Каждый интерфейс — это отдельный транспорт. Все четыре применяют один и тот же конвейер безопасности (RLS, маскирование, сэмплирование, проверка ролей). (REQ-002, REQ-038) Клиенты никогда не обращаются напрямую к движку федерации. (REQ-266) «Язык запросов» (SQL / GraphQL / Cypher) ортогонален транспорту — несколько языков могут поступать по одному и тому же транспорту.

| Порт | Транспорт | Принимаемые языки запросов | Сценарий использования |
| ------ | ----------- | -------------------------- | ---------- |
| 8001 | HTTP | GraphQL, SQL, Cypher | Веб-клиенты, BI-инструменты, curl, потребители REST |
| 8815 | Arrow Flight (gRPC) | SQL (через Arrow Flight SQL) | Инструменты данных (Pandas, DuckDB, Spark, ADBC) |
| 50051 | Protobuf gRPC | Сгенерированные для каждой роли RPC на базе proto | Взаимодействие сервис-сервис с типизированными контрактами |
| настраиваемый¹ | Проводной протокол PostgreSQL (pgwire) | SQL | psql, DBeaver, SQLAlchemy, любой PG-совместимый клиент |

¹ Установите `PROVISA_PGWIRE_PORT` (например, 5433). Отключено, если не задано или равно `0`.

### HTTP (порт 8001)

Несколько конечных точек на одном порту, различаются по пути:

| Путь | Язык | Примечания |
| ------ | ---------- | ------- |
| `POST /data/graphql` | GraphQL | Чтение и мутации; хеш APQ принимается через `extensions.persistedQuery` |
| `POST /data/sql` | SQL | Только чтение; без gate по возможностям (capability) — governance через видимость объектов + RLS + маскирование (REQ-001, REQ-267) |
| `POST /data/query` | Cypher | Только чтение; стандартная роль |
| `GET /data/nl` | Естественный язык | Переводится в SQL/GraphQL/Cypher в зависимости от типа источника |
| `GET /data/subscribe/{table}` | GraphQL | Поток подписки SSE |
| `GET /neo4j/...` | Cypher (совместимость с Neo4j) | Прослойка совместимости с HTTP API Neo4j |
| `POST /admin/graphql` | GraphQL | Admin API (требуется роль superuser/admin) |

Все пути по умолчанию возвращают JSON. `Accept: text/csv`, `application/vnd.apache.parquet`, `application/vnd.apache.arrow.stream` и `application/octet-stream` (сырые бинарные данные) поддерживаются через согласование содержимого (content negotiation). Результаты, превышающие настроенный порог размера, автоматически перенаправляются на подписанный URL S3. (REQ-029, REQ-137)

### Arrow Flight (порт 8815)

Нативный колоночный транспорт Arrow через gRPC. (REQ-045, REQ-143) Клиенты отправляют JSON-тикет:

```json
{"query": "SELECT name, email FROM customers", "role": "analyst"}
```

и получают потоково, лениво, RecordBatches Arrow. Когда доступен прокси Zaychik Arrow Flight SQL, данные текут потоком Arrow record batches от начала до конца: (REQ-144)

```text
Client ←(Arrow batches)← Provisa Flight Server ←(Arrow batches)← Zaychik ←(JDBC)← Federation Engine
```

Полный результат никогда не материализуется в памяти Provisa — батчи пересылаются по мере поступления. (REQ-145) Это делает Arrow Flight неограниченным путём, подходящим для сколь угодно больших результатов.

### Protobuf gRPC (порт 50051)

Автоматически сгенерированный `.proto` из схемы данных, генерируется для каждой роли. (REQ-525) Потоковые запросы (одно сообщение на строку), унарные мутации. Включена рефлексия сервера. (REQ-526) Роль передаётся через ключ метаданных `x-provisa-role`.

### Проводной протокол PostgreSQL / pgwire (настраиваемый порт)

Реализует протокол frontend/backend PostgreSQL с использованием библиотеки `buenavista`. (REQ-527) Любой PostgreSQL-совместимый клиент — `psql`, DBeaver, SQLAlchemy с `psycopg2`, JDBC — может подключиться без изменений. Принимает только SQL. Полный конвейер governance (RLS, маскирование, разрешения на домен) применяется к соединениям pgwire идентично. (REQ-266, REQ-002) Включается установкой `PROVISA_PGWIRE_PORT` на ненулевой порт.

## Конвейер запроса

Принимаются три языка запросов. Все они сходятся на governance после соответствующих этапов разбора/компиляции. (REQ-262, REQ-263) Только GraphQL поддерживает записи. (REQ-037) Для самого запроса нет gate по возможностям (capability) — любая аутентифицированная идентичность может выполнять запросы на любом языке, а данные управляются исключительно видимостью объектов, RLS и маскированием. (REQ-001)

| Интерфейс | Чтение | Запись | Gate запроса |
| --- | --- | --- | --- |
| GraphQL (`/data/graphql`) | Да | Да (мутации) | Нет — только governance на уровне данных |
| SQL (`/data/sql`) | Да | Нет | Нет — только governance на уровне данных (REQ-267) |
| Cypher (`/data/query`) | Да | Нет | Нет — только governance на уровне данных |

```mermaid
flowchart TD
    A[GraphQL Request] --> B[Auth / Role Resolution]
    A2[SQL Request] --> B
    A3[Cypher Request] --> B
    B --> E[APQ Hash Check]
    E --> F[Parse & Validate]
    F --> G[Extract Directives / Hints]
    G --> H{Cache Hit?}
    H -- yes --> R
    H -- no --> I{Input Type}
    I -- GraphQL --> I1[Compile → Semantic SQL]
    I -- SQL --> I2[Parse & Validate SQL\nApply Namespace / Source Binding]
    I -- Cypher --> I3[Translate Cypher → SQL\nResolve Node / Rel Mappings]
    I1 --> J[Governance: RLS + Masking + Visibility + Sampling]
    I2 --> J
    I3 --> J
    J --> K[MV Rewrite]
    K --> L{Route}
    L -- Direct --> M[Transpile → Source Dialect\nExecute via Driver]
    L -- Federation --> N[Transpile → Federation SQL\nInject Session Hints\nExecute via Federation Engine / Flight]
    L -- Materialize --> O[Fetch from REST / GraphQL / gRPC\nMaterialize → S3 Parquet\nPost-filter via Federation Engine]
    L -- Mutation --> P[RLS Injection\nTranspile → Source Dialect\nExecute via Driver\nInvalidate Cache + MV\nEmit Change Event]
    M --> Q{Redirect?}
    N --> Q
    O --> Q
    Q -- yes --> S[Upload to S3\nReturn Signed URL]
    Q -- no --> R[Serialize: JSON / CSV / Parquet / Arrow]
    R --> T[Store in Cache]
    T --> U[Return to Client]
    P --> U
```

**Решения о маршрутизации:**

| Маршрут | Когда |
| --- | --- |
| **Cache** | Попадание в кеш результата — вычисляется первым, отдаёт сохранённый результат без выполнения (REQ-865) |
| **Cheap-count** | Запрос формы `count(*)` над нематериализованным источником, предоставляющим точный нативный счётчик — маршрутизируется на нативный вызов подсчёта вместо материализации ради подсчёта (REQ-875) |
| **Direct** | Один источник + есть нативный драйвер + есть коннектор федерации |
| **Federation** | Федерация нескольких источников, либо источник имеет коннектор, но не имеет драйвера |
| **Materialize** | У источника нет коннектора федерации — сначала выбрать и закешировать в S3/PG |
| **Mutation** | GraphQL-мутация — всегда напрямую, никогда не федерируется |

Маршрутизация использует вывод этапа оптимизации после governance, а не governed SQL до оптимизации. Governance может ДОБАВИТЬ источники (предикаты подзапроса RLS); этап оптимизации может их УБРАТЬ (встраивание горячих таблиц как VALUES-CTE, переписывание API-кеша, отсечение веток union). Федеративный запрос, который после встраивания схлопывается до одного живого источника, поэтому перемаршрутизируется как прямой. (REQ-863)

### Запросы с несколькими корнями

GraphQL-запросы с несколькими корневыми полями (например, `{ orders { id } customers { name } }`) компилируются в отдельные SQL-запросы и выполняются независимо. (REQ-534) Запросы SQL и Cypher по определению однокорневые. Результаты объединяются в единый ответ:

- Поля ниже порога перенаправления возвращаются встроенно в `data`
- Поля выше порога перенаправляются, с записями для каждого поля в `redirects`
- Бинарные форматы (Parquet, Arrow) поддерживаются только для однокорневых запросов

## Пути выполнения федерации

| Путь | Транспорт | Через | Когда используется |
| ------ | ----------- | ----- | ----------- |
| REST | клиент движка федерации (HTTP :8080) | Прямой запрос | По умолчанию, всегда доступен |
| Flight SQL | `adbc-driver-flightsql` (gRPC :8480) | Прокси Zaychik → JDBC | Когда запущен Zaychik |
| CTAS | клиент движка федерации (HTTP :8080) | Прямая запись, Iceberg в S3 | Перенаправление Parquet/ORC |

### Прокси Zaychik Arrow Flight SQL

Движок федерации нативно не поддерживает протокол Arrow Flight SQL. [Zaychik](https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy) — это Java-прокси, реализующий gRPC-интерфейс Arrow Flight SQL, транслирующий запросы в JDBC-запросы и потоково возвращающий результаты как Arrow record batches. (REQ-144)

```text
ADBC client → gRPC :8480 → Zaychik → JDBC :8080 → Federation Engine → results → Arrow batches → client
```

Flight-сервер Provisa (порт 8815) подключается к Zaychik как ADBC-клиент, обеспечивая потоковую передачу Arrow от начала до конца без материализации результатов. (REQ-145)

### Каталог результатов Iceberg

Перенаправление CTAS использует коннектор Iceberg (каталог `results`), опирающийся на JDBC-каталог на существующем экземпляре PostgreSQL. (REQ-169) Iceberg пишет файлы Parquet/ORC напрямую в MinIO/S3 через нативную файловую систему S3 (`fs.native-s3.enabled=true`).

## Движки федерации

Provisa выбирает движок федерации при старте через переменную окружения `PROVISA_ENGINE`, сохранённую конфигурацию admin UI либо значение по умолчанию. Если ничего не задано, по умолчанию используется DuckDB — полностью встроенный, без внешнего сервиса (REQ-989). Подробности выбора см. в [Configuration](configuration.md#_30).

Каждый движок — это экземпляр `FederationEngine`, определённый в `provisa/federation/engine.py`. Экземпляр владеет набором коннекторов, определяющим, какие типы источников движок может читать вживую (ATTACH), а какие сначала должны попасть в хранилище материализации движка. [tool-verified: `engine.py` `_ENGINE_BUILDERS`, `ENGINE_REGISTRY`]

### Классы драйверов (REQ-840) [tool-verified: `engine.py` `DriverClass`]

| Класс | Значение | Примеры |
| ------- | --------- | --------- |
| `BROAD` | Достигает многих внешних типов источников через нативные коннекторы | Trino |
| `PARTIAL` | Достигает подмножества (реляционные, файлы, облачное объектное хранилище/lake), а всё остальное приземляет | DuckDB, PostgreSQL, ClickHouse, Databricks, Snowflake, BigQuery, Fabric, Synapse |
| `SELF_ONLY` | Достигает только собственного хранилища; всё остальное приземляется | SQLAlchemy |

### Доступные движки [tool-verified: `engine.py` `_ENGINE_BUILDERS`]

| Ключ движка | Диалект | MPP | Механизм внешней связи | Аутентификация |
| ----------- | --------- | ----- | ------------------------ | ------ |
| `trino` / `trino-byo` | Trino SQL | Да | Каталоги Trino (широкий набор коннекторов) | Учётные данные JDBC |
| `pg` | PostgreSQL | Нет | FDW / pg_duckdb | Учётные данные PostgreSQL |
| `duckdb` | DuckDB | Нет | Нативное расширение ATTACH | Нет (в процессе) |
| `clickhouse` / `clickhouse-server` | ClickHouse | Да (шарды) | Табличные движки S3 / IcebergS3 / DeltaLake (REQ-986) | Учётные данные ClickHouse |
| `snowflake` | Snowflake | Да | Внешний stage + внешняя таблица (REQ-988) | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks SQL | Да | Внешние таблицы Unity Catalog через REST (REQ-987) | Bearer-токен (`http_path` в `federation_hints`) |
| `bigquery` | BigQuery | Да (Dremel) | Внешние таблицы BigQuery / BigLake | Ключ сервисного аккаунта `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | T-SQL | Да | Ярлыки OneLake → OPENROWSET | Azure AD (`az login` / managed identity) |
| `synapse` | T-SQL | Да | ADLS OPENROWSET / внешние таблицы | Azure AD |
| `sqlalchemy` | Любой диалект SQLAlchemy | Нет | Нет (только приземление) | Учётные данные для каждого диалекта |

### Готовый к работе без конфигурации по умолчанию: DuckDB (REQ-989) [tool-verified: `engine.py` `build_duckdb_engine`, `_embedded_duckdb_materialize_default`]

Когда `PROVISA_ENGINE` не задан, Provisa использует полностью встроенный внутрипроцессный движок DuckDB. Хранилище материализации DuckDB — это встроенный файл DuckDB по пути `$PROVISA_DATA_DIR/materialize.duckdb` (по умолчанию `~/.provisa/materialize.duckdb`). Внешняя база данных или сервис не требуются.

Поскольку DuckDB допускает только одного писателя на файл, `store_connection.py` пишет во встроенное хранилище через собственное соединение движка — никогда через второе независимое соединение. Это единственный случай, когда движок и хранилище материализации намеренно разделяют один файловый дескриптор. [tool-verified: `store_connection.py` module docstring]

### Нативный для Arrow транспорт чтения (REQ-986, REQ-987, REQ-988) [tool-verified: `engine.py` `build_*_engine` `capabilities=`]

ClickHouse, DuckDB, Snowflake, Databricks, BigQuery, Fabric и Synapse — все заявляют `EngineCapability.ARROW` и `EngineCapability.ARROW_STREAM`. Запросы к этим движкам возвращают RecordBatches Arrow напрямую — путь сериализации строк полностью минуется. Flight-сервер передаёт эти батчи клиентам потоково, не материализуя полный результат в памяти процесса Provisa. Для Trino потоковая передача Arrow опирается на прокси Zaychik; для движков хранилищ данных — на собственный Arrow-нативный API движка (Cloud Fetch для Databricks, Storage Read API для BigQuery, `fetch_arrow_table` для DuckDB и Snowflake), питающий поток Flight.

### Внешние связи с данными (ATTACH) [tool-verified: `engine.py` `_warehouse_connectors`]

Каждый движок хранилища данных может сканировать облачные объектные/lake-данные на месте, не приземляя копию. Файлы Parquet, CSV, Iceberg и Delta Lake на S3, GCS или OneLake присоединяются к движку напрямую, как будто это нативные таблицы. Стратегия — ATTACH (сканирование на месте) или LAND (копирование в хранилище) — определяется объявленным `Mechanism` коннектора; в планировщике нет ветвления, специфичного для движка. Коннектор `Mechanism.ATTACH_R` запускает сканирование с нулевым копированием; `Mechanism.DIRECT` или отсутствие коннектора запускает приземление. [tool-verified: `connector_base.py` `Mechanism`, `engine.py` `_warehouse_connectors`]

Attach автоматически подготавливает все предпосылки в момент присоединения:

| Движок | Форматы объектов/lake | Механизм | Автоматическая подготовка [tool-verified] |
| -------- | ------------------- | ---------- | ---------------------------------- |
| Databricks | parquet, csv, iceberg, delta_lake | Внешняя таблица UC (`ATTACH_R`) | REST устанавливает storage credential Unity Catalog + external location, затем `CREATE TABLE … USING <format> LOCATION …` — проверено вживую поверх Cloudflare R2 |
| BigQuery | parquet, csv, json, iceberg, delta_lake | Внешняя таблица BigQuery / BigLake (`ATTACH_R`) | `CREATE OR REPLACE EXTERNAL TABLE … OPTIONS(format=…, uris=[…])` — проверено вживую |
| ClickHouse | csv, parquet, iceberg, delta_lake | Табличный движок S3 / IcebergS3 / DeltaLake (`ATTACH_R`) | Проверочный зонд выполняется в момент присоединения — проверено вживую поверх Cloudflare R2 |
| Fabric | parquet, csv, iceberg, delta_lake | Ярлык OneLake → OPENROWSET (`ATTACH_R`) | REST создаёт соединение `AmazonS3Compatible` + lakehouse + shortcut; возвращает путь `BULK` OneLake — проверено вживую при чтении R2 через Fabric |
| Snowflake | parquet, csv, json, iceberg, delta_lake | Внешний stage + внешняя таблица (`ATTACH_R`) | `CREATE STAGE … URL=… CREDENTIALS=…`, затем `CREATE OR REPLACE EXTERNAL TABLE … LOCATION=@stage FILE_FORMAT=(TYPE=…)` — реализовано; не протестировано вживую (нет доступного аккаунта) |

Учётные данные для облачного хранилища передаются в `federation_hints` источника (см. [Sources](sources.md#_13)). Любой тип источника, который не может выполнить ATTACH, сначала приземляется в хранилище материализации движка.

### Колоночные записи материализации (REQ-990) [tool-verified: `core/database.py:436`, `store_connection.py:99`]

`Connection.bulk_copy` в `provisa/core/database.py` выбирает самый быстрый путь массовой загрузки для каждого диалекта хранилища: бинарный `COPY` (`copy_records_to_table` asyncpg) для хранилищ PostgreSQL и один подготовленный запрос `executemany` для всех остальных реляционных хранилищ. Встроенное хранилище DuckDB приземляется через `land_duckdb_native` в `store_connection.py` — один вызов `executemany` на весь батч, никогда построчный цикл.

## Перенаправление больших результатов

Результаты, превышающие порог по числу строк, перенаправляются в S3-совместимое хранилище (MinIO) вместо возврата встроенно. (REQ-029)

### Режимы перенаправления

| Режим | Как работает | Данные проходят через Provisa? |
| ------ | ------------- | ---------------------- |
| **CTAS** (Parquet, ORC) | Движок федерации пишет напрямую в S3 через `CREATE TABLE AS SELECT` | Нет |
| **Загрузка Provisa** (JSON, NDJSON, CSV, Arrow IPC) | Provisa сериализует и загружает через boto3 | Да |

Для нативных для CTAS форматов Provisa никогда не обрабатывает данные — движок федерации пишет файлы напрямую в MinIO/S3. (REQ-138) Это предпочтительный путь для крупных аналитических экспортов.

### Заголовки перенаправления

| Заголовок | Эффект |
| -------- | -------- |
| `X-Provisa-Redirect-Format: <mime>` | Перенаправить в этом формате (подразумевает принудительность, если не задан порог) |
| `X-Provisa-Redirect-Threshold: N` | Перенаправлять, только если результат превышает N строк |
| `X-Provisa-Redirect: true` | Принудительное перенаправление с использованием формата по умолчанию |

Эти заголовки реализуют перенаправление, управляемое клиентом. (REQ-137)

**Ответ:**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://minio:9000/provisa-results/results/abc.parquet?...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

### Конфигурация сервера

| Переменная окружения | По умолчанию | Назначение |
| --------- | --------- | --------- |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включить перенаправление по порогу на стороне сервера |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Порог по количеству строк по умолчанию |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Формат перенаправления по умолчанию |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Имя бакета S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL S3-совместимой конечной точки |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL подписанного URL (секунды) |

## Дерево решений маршрутизации

```text
Multi-source query? → Federation engine
NoSQL source (MongoDB, Cassandra)? → Federation engine
Uses path columns on non-PG source? → Federation engine
Single RDBMS with driver? → Direct (sub-100ms target)
Single RDBMS without driver? → Federation engine
Steward hint "federated"? → Federation engine (override)
Steward hint "direct"? → Direct (if possible)
Redirect to Parquet/ORC? → Federation engine (CTAS, regardless of source count)
```

(REQ-027, REQ-028, REQ-030, REQ-279)

## Оптимизация федеративных запросов

Provisa автоматически подготавливает оптимизатор движка федерации на основе стоимости, так что межисточниковые планы запросов основаны на реальном распределении данных, а не на жёстко заданных значениях по умолчанию.

### Автоматическая статистика (`ANALYZE`)

При регистрации источника Provisa запускает `ANALYZE catalog.schema.table` для каждой опубликованной таблицы. (REQ-275) Это собирает:

- Количество строк
- Для каждой колонки: долю null-значений, количество различных значений, min/max, гистограммы (зависит от коннектора)

Оптимизатор использует их для оценки селективности отфильтрованных запросов. Без статистики он откатывается к фиксированным значениям по умолчанию (например, 10% селективности для предикатов равенства), что даёт плохие планы join на скошенных или высококардинальных данных. Со статистикой оценки достаточно точны, чтобы правильно выбирать между broadcast- и partitioned-join для большинства нагрузок.

**Покрытие**: поддержка статистики варьируется от коннектора к коннектору. PostgreSQL, MySQL, Hive, Iceberg и Delta Lake полностью поддерживают `ANALYZE`. Коннекторы MongoDB и Cassandra имеют частичную поддержку или не имеют её вовсе. Provisa молча поглощает сбои `ANALYZE` — регистрация никогда не блокируется. (REQ-275)

**Ограничения селективности**: статистика предоставляет оценки для каждой колонки отдельно. Для коррелированных предикатов (`WHERE region = 'US' AND city = 'Seattle'`) оптимизатор предполагает независимость колонок, что может занижать оценку числа строк. Это известное ограничение статистики уровня колонок во всех оптимизаторах на основе стоимости.

**Источники API**: таблицы `api_cache_{table_name}` в PostgreSQL анализируются автоматически после каждого цикла обновления кеша, так что у оптимизатора есть актуальные оценки числа строк при соединении источников на базе API с реляционными источниками. (REQ-280)

### Admin: обновление статистики

Повторный запуск сбора статистики по требованию через admin API: (REQ-276)

```graphql
mutation {
  refreshSourceStatistics(sourceId: "sales-pg") {
    tablesAnalyzed
    failures { table message }
  }
}
```

Полезно, когда источник получил значительный объём новых данных с момента регистрации.

## Материализованные представления

MV прозрачно оптимизируют дорогие запросы, предварительно вычисляя и кешируя результаты.

### Связи как подсказки для MV

Объявление связи — это не только артефакт governance — это также структурное описание формы join. Именно эта форма нужна оптимизатору MV: две таблицы, две колонки, тип join. Это значит, что связь может напрямую управлять материализацией.

Для **межисточниковых связей** это происходит автоматически при старте: каждая утверждённая межисточниковая связь генерирует MV `JoinPattern` (`auto-mv-<rel_id>`). (REQ-158) Отдельная конфигурация MV не требуется. Когда компилятор видит этот join в запросе, переписчик прозрачно подставляет предварительно материализованный результат.

Для связей **в пределах одного источника** стюарды могут явно включить это через `materialize: true`. JOIN в пределах одного источника уже быстры за счёт прямого выполнения, поэтому материализация оправдана только для очень горячих путей join. (REQ-159)

Практическое следствие: стюарды, утверждающие связь, неявно решают, является ли join хорошим кандидатом для материализации. Акт governance и подсказка оптимизации — это одно и то же объявление.

### Режимы

| Режим | Конфигурация | Поведение |
| ------ | -------- | ---------- |
| **Join-pattern** | `join_pattern` в конфигурации MV | Переписывает совпадающие JOIN на чтение из таблицы MV |
| **Custom SQL** | `sql` в конфигурации MV | Произвольный SELECT, опционально раскрываемый в SDL |
| **Автоматически материализованная связь** | межисточниковая связь (автоматически) | Автоматически генерирует join-pattern MV; конфигурация не требуется |
| **Материализованная стюардом связь** | `materialize: true` на связи в пределах одного источника | Явное включение для горячих путей join в пределах одного источника |

### Автоматическая материализация

Межисточниковые JOIN — самые дорогие запросы (всегда федерируются). Межисточниковые связи автоматически генерируют определения MV при старте: (REQ-158)

```yaml
relationships:
  - id: orders-to-reviews
    source_table_id: orders        # sales-pg
    target_table_id: product_reviews  # reviews-mongo
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV
    refresh_interval: 600          # refresh every 10 minutes
```

Только межисточниковые связи генерируют MV (JOIN в пределах одного источника уже быстры за счёт прямого выполнения). (REQ-159) MV начинает в статусе `STALE` и обновляется фоновым циклом обновления, прежде чем его начнёт использовать оптимизатор запросов. (REQ-160)

### Жизненный цикл обновления

```text
STALE → (refresh loop picks up) → REFRESHING → FRESH
  ↑                                                |
  └──── mutation hits source table ────────────────┘
```

Цикл обновления запускается каждые 30 секунд, проверяет `get_due_for_refresh()` и выполняет `CREATE TABLE AS SELECT` (первый запуск) или `DELETE + INSERT` (последующие) над целевой таблицей MV через движок федерации. (REQ-160, REQ-234)

## Карта модулей

| Модуль | Назначение |
| -------- | --------- |
| `api/` | Приложение FastAPI, роутеры, middleware, управление жизненным циклом |
| `api/flight/` | Сервер Arrow Flight (gRPC, порт 8815) |
| `api/admin/` | Admin GraphQL API на Strawberry — конфигурация, обнаружение, представления |
| `api/rest/` | Автоматически сгенерированные REST-конечные точки из зарегистрированных таблиц |
| `api/jsonapi/` | Автоматически сгенерированные конечные точки JSON:API с пагинацией и обработкой ошибок |
| `api/data/subscribe.py` | Подписки SSE — LISTEN/NOTIFY, опрос, Debezium CDC |
| `compiler/` | Парсеры GraphQL/SQL, генератор семантического SQL, RLS, маскирование, сэмплирование, двухэтапный governance (`stage2.py`) |
| `cypher/` | Транслятор Cypher → SQL, парсер, карта меток (REQ-351), транслятор записи для мутаций Cypher |
| `pgwire/` | Сервер проводного протокола PostgreSQL; `catalog.py` перехватывает pg_catalog/information_schema для видимости объектов для каждой роли (REQ-527, REQ-883, REQ-891) |
| `vector/` | Векторный поиск — реестр моделей, провайдеры эмбеддингов (openai/ollama/huggingface), трансляция `cosine_similarity()`, резервный кеш pgvector, декларативная генерация эмбеддингов (REQ-419–431) |
| `compiler/federation.py` | Поддержка субграфа Apollo Federation v2 |
| `transpiler/` | Транспиляция диалектов, логика маршрутизации |
| `executor/` | Федеративное/прямое выполнение, сериализация, форматы вывода |
| `executor/drivers/` | Прямые драйверы источников (PostgreSQL, MySQL, DuckDB, Snowflake, Databricks, ClickHouse, …) |
| `executor/trino_flight.py` | ADBC-клиент Flight SQL для движка федерации |
| `executor/ctas_write.py` | Перенаправление на базе CTAS (движок федерации пишет в S3) |
| `executor/redirect.py` | Логика перенаправления S3, загрузка со стороны Provisa |
| `federation/engine.py` | `FederationEngine`, `DriverClass`, `_ENGINE_BUILDERS`, `ENGINE_REGISTRY`, `build_engine` |
| `federation/connector.py` | Абстракции коннекторов — Trino, ClickHouse; `Mechanism`, `WarehouseNativeConnector` |
| `federation/connector_duckdb.py` | Определения коннекторов DuckDB и PostgreSQL FDW |
| `federation/snowflake_connectors.py` | Коннекторы ATTACH внешнего stage + внешней таблицы Snowflake (REQ-988) |
| `federation/databricks_connectors.py` | Коннекторы ATTACH внешней таблицы Databricks UC (REQ-987) |
| `federation/bigquery_connectors.py` | Коннекторы ATTACH BigQuery external / BigLake |
| `federation/databricks_uc.py` | Автоматическая подготовка credential Unity Catalog + external location |
| `federation/databricks_backend.py` | Бэкенд выполнения Databricks SQL warehouse |
| `federation/snowflake_backend.py` | Бэкенд выполнения Snowflake |
| `federation/bigquery_backend.py` | Бэкенд выполнения BigQuery (Arrow-транспорт Storage Read API) |
| `federation/mssql_warehouse_backend.py` | Бэкенды выполнения Fabric Warehouse + Synapse (T-SQL через ODBC) |
| `federation/mssql_warehouse_connectors.py` | Коннекторы ATTACH OPENROWSET для Fabric / Synapse |
| `federation/fabric_shortcuts.py` | Автоматическая подготовка ярлыков OneLake (соединение → lakehouse → shortcut) |
| `federation/clickhouse_backend.py` | Бэкенд выполнения ClickHouse |
| `federation/duckdb_backend.py` | Внутрипроцессный бэкенд выполнения DuckDB |
| `federation/pg_backend.py` | Бэкенд выполнения PostgreSQL |
| `federation/store_connection.py` | Нативное для DuckDB звено записи хранилища материализации (REQ-989, REQ-990) |
| `registry/` | Реестр сохранённых запросов, governance |
| `security/` | Видимость, права, маскирование колонок |
| `cache/` | Кеширование результатов запросов на базе Redis (горячий уровень) |
| `mv/` | Реестр материализованных представлений, обновление, переписыватель SQL |
| `events/` | События изменения наборов данных и диспетчеризация триггеров |
| `webhooks/` | Исходящее выполнение webhook для мутаций и событий |
| `scheduler/` | Управление фоновыми заданиями на базе APScheduler — cron- и интервальные триггеры, запускающие webhook'и, мутации или публикации в приёмники Kafka |
| `apq/` | Проводной протокол Apollo APQ — кеш хешей запросов на базе Redis; отдельно от кеширования результатов |
| `compiler/cursor.py` | Курсорная пагинация в стиле Relay — аргументы `first`/`after`/`last`/`before` и генерация `pageInfo` для всех запросов списков |
| `compiler/aggregate_gen.py` | Автоматически сгенерированные типы запросов `{table}_aggregate` с подполями `count`, `sum`, `avg`, `min`, `max` и доступом к отфильтрованным `nodes` |
| `compiler/enum_detect.py` | Автоматическое обнаружение enum-типов — нативные enum-типы PostgreSQL (`pg_enum`), раскрываемые как GraphQL enum-типы, а не строковые скаляры |
| `compiler/hints.py` | Подсказки производительности федерации — директивы маршрутизации на уровне запроса, встроенные как SQL-комментарии (`/* @provisa route=federated */`), переопределяющие автоматическую маршрутизацию |
| `compiler/mutation_gen.py` | Компилятор мутаций; пресеты колонок — статические значения или значения из переменных сессии на стороне сервера, применяемые при insert/update, не раскрываемые во входном типе мутации |
| `auth/approval_hook.py` | Хук утверждения ABAC — подключаемая внешняя авторизация, вызываемая перед выполнением запроса; транспорты webhook, gRPC и unix_socket; область действия для таблицы/источника/глобальная; настраиваемая политика отката |
| `subscriptions/` | Состояние и доставка подписок SSE |
| `discovery/` | Обнаружение связей на базе LLM (API Claude) |
| `grpc/` | Генерация proto, сервер gRPC, рефлексия |
| `api_source/` | Источники REST/GraphQL/gRPC API с кешем PG |
| `kafka/` | Источники тем Kafka, приёмник, Schema Registry |
| `auth/` | Подключаемые провайдеры аутентификации, middleware, отображение ролей |
| `core/` | Конфигурация, модели, БД, репозитории, секреты; модель ролей поддерживает `parent_role_id` и `flatten_roles()` для рекурсивного наследования ролей |
| `hasura_v2/` | Конвертер метаданных Hasura v2 → конфигурация Provisa |
| `ddn/` | Конвертер supergraph Hasura DDN → конфигурация Provisa |
| `mongodb/` | Коннектор источника MongoDB |
| `elasticsearch/` | Коннектор источника Elasticsearch |
| `cassandra/` | Коннектор источника Cassandra |
| `prometheus/` | Коннектор источника метрик Prometheus |
| `source_adapters/` | Общий слой адаптеров для соединений с источниками |

## Admin API

Admin GraphQL API на Strawberry монтируется по адресу `/admin/graphql` (HTTP-порт 8001). Он отделён от конечной точки данных GraphQL и требует роли superuser или admin.

| Возможность | Описание |
| ----------- | ------------- |
| Загрузка/выгрузка конфигурации | Экспорт или замена полной YAML-конфигурации Provisa |
| Редактор связей | Создание, обновление, удаление определений связей |
| AI-обнаружение FK | Запуск анализа кандидатов на внешний ключ на базе Claude |
| Интроспекция схемы | Просмотр опубликованных таблиц, колонок и ролей |
| Управление представлениями | Регистрация и управление определениями материализованных представлений |

(REQ-164, REQ-165, REQ-166, REQ-167)

## Конфигурация AI-моделей

`GET /admin/ai-models` и `PUT /admin/ai-models` настраивают конвейер LLM для каждой организации. (REQ-464, REQ-419, REQ-500, REQ-370, REQ-1349)

Настройки **привязаны к организации**: выбор каждой организации ложится поверх конфигурации развёртывания и вступает в силу со следующего запроса — перезапуск не требуется. (REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

**Назначения моделей для каждой операции.** Пять операций NL, каждая с настраиваемым вендором и строкой модели:

| Операция | Что управляет |
| --------- | -------------- |
| `table_description` | Сгенерированные LLM описания таблиц |
| `column_description` | Сгенерированные LLM описания колонок |
| `relationship_inference` | Обнаружение кандидатов на FK |
| `sql_generation` | Генерация NL → SQL |
| `table_selection` | Выбор таблиц для включения в промпт NL |

Поле вендора принимает любой вендор, совместимый с `aisuite` (`anthropic`, `openai`, `groq`, `mistral`, `cohere` и другие), либо локальную конечную точку (`ollama`, `lmstudio`). Пустая строка модели удаляет переопределение организации и возвращает к значению по умолчанию из развёртывания. [tool-verified: `provisa/api/admin/ai_models_router.py:29-35`, `provisa-ui/src/components/admin/AiModelsTab.tsx:43-60`]

**Ограничение частоты запросов NL.** Опциональный предел запросов за период, применяемый для каждой роли. Избыточные запросы возвращают `429` с `Retry-After`. [tool-verified: `provisa-ui/src/components/admin/AiModelsTab.tsx:306-313`]

**Реестр векторных моделей.** Список моделей эмбеддинга (поля: `id`, `provider`, `dimensions`, опционально `api_key_env` и `base_url`, флаг `enabled`). Полная замена списка: у каждой записи должны быть `id`, `provider` и `dimensions`, иначе запись на запись отклоняется с `400`. [tool-verified: `provisa/api/admin/ai_models_router.py:122-131`]

**API-ключи.** Ключи API LLM для каждого вендора хранятся зашифрованными через `provisa.core.org_secrets` (см. ниже). Ответ `GET` сообщает только о том, задан ли ключ для каждого вендора — само значение никогда не возвращается. Отправка пустой строки для вендора очищает этот ключ, возвращая вызовы LLM для этого вендора к учётным данным из переменной окружения развёртывания. (REQ-1395, REQ-1398) [tool-verified: `provisa/api/admin/ai_models_router.py:76-78`, `provisa/api/admin/ai_models_router.py:149-165`]

## Зашифрованные секреты для каждой организации

`provisa/core/org_secrets.py` хранит учётные данные, которые никогда не должны появляться в базе данных как открытый текст. В настоящее время ограничено ключами API вендоров LLM (`{vendor}_api_key`). (REQ-1395, REQ-1398) [tool-verified: `provisa/core/org_secrets.py`]

Значения шифруются через общепроцессный `encryption_service` из `provisa.encryption.runtime` — тот же механизм, что и `api_sources.auth`. [tool-verified: `provisa/core/org_secrets.py:16-17`]

Поддерживаются двенадцать вендоров, совместимых с `aisuite`: `anthropic`, `openai`, `cohere`, `groq`, `mistral`, `xai`, `deepseek`, `together`, `fireworks`, `nebius`, `sambanova` и `inception`. Google, AWS и Azure исключены, поскольку требуют конфигурации сверх простого API-ключа (ID проекта, роли IAM, регион). Вендоры с локальной конечной точкой (`ollama`, `lmstudio`) не имеют ключа и исключены по той же причине. [tool-verified: `provisa/core/org_secrets.py:33-53`]

Передача `value=None` в `write_org_secret` удаляет строку. Вызывающий код, читающий секрет, немедленно его потребляет (например, для создания клиента LLM) и не должен возвращать его эхом ни в одном ответе API. [tool-verified: `provisa/core/org_secrets.py:97-117`]

## Автоматически сгенерированные конечные точки REST и JSON:API

Зарегистрированные таблицы раскрываются как конечные точки REST и JSON:API наряду с интерфейсом GraphQL. (REQ-256, REQ-257)

| Интерфейс | Путь монтирования | Спецификация |
| ----------- | ----------- | ------ |
| REST | `/rest/<table-id>` | Простой GET/POST с параметрами запроса |
| JSON:API | `/jsonapi/<table-id>` | Соответствует [jsonapi.org](https://jsonapi.org) — пагинация, связи, объекты ошибок |

Эти конечные точки применяют тот же конвейер безопасности (RLS, маскирование, проверка ролей), что и конечная точка GraphQL. (REQ-002, REQ-038)

## Подписки

Подписки SSE обслуживаются по адресу `GET /data/subscribe/{table}`. Три режима доставки: (REQ-258)

| Режим | Механизм | Когда используется |
| ------ | ----------- | ----------- |
| **LISTEN/NOTIFY** | `LISTEN` PostgreSQL на канале | Источники PG с активностью мутаций |
| **Опрос (Polling)** | Повторное выполнение запроса с интервалом | Источники не PG или когда CDC недоступен |
| **Debezium CDC** | Тема Kafka от коннектора Debezium | Высокочастотные потоки изменений |

(REQ-258, REQ-260, REQ-261)

Клиент получает `text/event-stream` с одним JSON-событием на изменённую строку или diff.

## Система событий и webhook

Мутации базы данных (INSERT/UPDATE/DELETE) могут запускать исходящие события через модули `events/` и `webhooks/`. (REQ-172, REQ-173, REQ-220)

```text
Mutation executed → EventDispatcher → match event trigger rules
                                          ↓
                               WebhookExecutor → HTTP POST to configured URL
```

Триггеры событий определяются в конфигурации и сопоставляются по таблице, типу операции и опциональному фильтру строк. Полезная нагрузка webhook включает тип операции, изменённую строку и контекст роли.

## Фоновые сервисы

Четыре фоновых цикла запускаются в течение жизненного цикла приложения (`api/app.py`):

| Сервис | Интервал | Назначение |
| --------- | ---------- | --------- |
| Цикл обновления MV | 30 с | Опрашивает `get_due_for_refresh()`, выполняет CTAS или DELETE+INSERT для устаревших MV |
| Менеджер тёплых таблиц | Настраиваемый | Продвигает часто запрашиваемые таблицы в кеш локального SSD Iceberg |
| Загрузчик горячих таблиц | Настраиваемый | Загружает небольшие справочные таблицы в кеш в памяти для доступа с задержкой менее миллисекунды |
| Опросчик источников API | Интервал для каждого источника | Повторно выбирает и перекеширует удалённые источники REST/GraphQL/gRPC |

(REQ-160, REQ-238, REQ-239, REQ-236)

### Уровни кеширования горячих/тёплых таблиц

| Уровень | Хранилище | Критерий продвижения | Задержка доступа |
| ------ | --------- | ------------------- | ---------------- |
| Горячий | Внутрипроцессная память | Количество строк ниже порога, либо таблица — цель связи | <1 мс |
| Тёплый | Iceberg на локальном SSD | Превышен порог частоты запросов | ~5–20 мс |
| Холодный | Удалённый источник | По умолчанию | 50–500 мс |

(REQ-230, REQ-236, REQ-238, REQ-241)

## Импорт метаданных (Hasura v2 / DDN)

Существующие развёртывания Hasura можно преобразовать в конфигурацию Provisa без ручного переписывания. (REQ-182, REQ-183)

| Модуль | Вход | Выход |
| -------- | ------- | -------- |
| `hasura_v2/` | Hasura v2 `metadata.yaml` | Provisa `config.yaml` |
| `ddn/` | Hasura DDN supergraph JSON | Provisa `config.yaml` |

Оба конвертера сопоставляют отслеживаемые таблицы, связи, разрешения и удалённые схемы. Результат — полная конфигурация Provisa, готовая к развёртыванию. (REQ-182, REQ-183)

## Apollo Federation

`compiler/federation.py` раскрывает Provisa как субграф Apollo Federation v2. (REQ-259) SDL субграфа автоматически генерируется из опубликованной схемы с директивами `@key` на колонках первичного ключа и аннотациями `@external`/`@provides` на межсубграфовых связях. Provisa отвечает на запросы `_entities` и `_service`, требуемые шлюзом федерации. (REQ-259)

## Курсорная пагинация

Все запросы списков поддерживают курсорную пагинацию в стиле Relay через `compiler/cursor.py`. (REQ-218) Клиенты передают аргументы `first`/`after` (вперёд) или `last`/`before` (назад). Компилятор кодирует позицию строки как непрозрачный курсор в base64 и внедряет соответствующие предложения `WHERE`/`LIMIT`. Каждый запрос списка возвращает объект `pageInfo`:

| Поле | Тип | Описание |
| ------- | ------ | ------------- |
| `hasNextPage` | Boolean | True, если после этой страницы есть ещё результаты |
| `hasPreviousPage` | Boolean | True, если есть результаты до этой страницы |
| `startCursor` | String | Курсор первого узла на этой странице |
| `endCursor` | String | Курсор последнего узла на этой странице |

## Агрегатные запросы

Каждая зарегистрированная таблица получает автоматически сгенерированное корневое поле `{table}_aggregate` (`compiler/aggregate_gen.py`). (REQ-196) Агрегатный тип раскрывает `count`, `sum`, `avg`, `min`, `max` для каждой числовой колонки и `nodes` для доступа к отфильтрованным строкам с полным выбором полей (те же RLS/маскирование, что и в базовом запросе). (REQ-196, REQ-198) Агрегатные запросы имеют право на маршрутизацию через Aggregate MV — см. `mv/aggregate_catalog.py`. (REQ-198)

## Автоматически сохраняемые запросы (APQ)

`apq/cache.py` реализует проводной протокол Apollo APQ. (REQ-288) Когда клиент отправляет только хеш запроса (`extensions.persistedQuery`), Provisa ищет его в Redis. (REQ-289) При промахе возвращается ошибка `PersistedQueryNotFound`; клиент повторяет запрос с полным телом, которое Provisa сохраняет. (REQ-288) Это отдельно от кеширования результатов (`cache/`).

## Наследуемые роли

Роли в `core/models.py` могут ссылаться на `parent_role_id`. (REQ-215) `flatten_roles()` рекурсивно разрешает цепочку наследования и объединяет предложения WHERE для RLS (через AND), видимость колонок (объединение, побеждает наиболее ограничительное) и политики маскирования (дочерняя переопределяет родительскую для каждой колонки). Это позволяет избежать дублирования наборов разрешений между похожими ролями (например, `analyst`, наследующая от `reader`). (REQ-215)

## Хук утверждения ABAC

`auth/approval_hook.py` — подключаемый хук авторизации, вызываемый перед выполнением запроса, после RLS и маскирования. (REQ-203) Он интегрируется с внешними движками политик (OPA, пользовательские сервисы ABAC).

| Настройка | Описание |
| --------- | ------------- |
| Транспорт | `webhook` (HTTP POST), `grpc` или `unix_socket` |
| Область действия | Для таблицы, для источника или глобальная |
| Политика отката | `allow` или `deny`, когда конечная точка хука недостижима |

(REQ-246, REQ-247, REQ-204)

## Автоматическое обнаружение enum-типов

`compiler/enum_detect.py` интроспектирует нативные enum-типы PostgreSQL (`pg_enum`) во время генерации схемы. (REQ-221) Колонки, использующие пользовательский enum-тип PostgreSQL, повышаются до GraphQL enum-типов — их значения становятся членами enum, а не строковыми скалярами.

## Запланированные триггеры

`scheduler/jobs.py` использует APScheduler для запуска фоновых заданий, определённых как cron- или интервальные триггеры. (REQ-216) Каждое задание может выполнить POST на URL webhook, выполнить мутацию против конечной точки данных или опубликовать результаты запроса в тему Kafka. Триггеры настраиваются через admin API (мутации `scheduledTrigger`) или ключ `scheduled_triggers` в YAML-конфигурации. (REQ-216)

## Подсказки производительности федерации

`compiler/hints.py` разбирает подсказки стюардов, встроенные в запросы как комментарии, с использованием синтаксиса комментариев Provisa. (REQ-279) Формат подсказки зависит от языка запроса:

```graphql
# @provisa route=federated
{ orders { id amount } }
```

```sql
/* @provisa route=federated */
SELECT id, amount FROM orders
```

```cypher
// @provisa route=federated
MATCH (o:Order) RETURN o.id, o.amount
```

| Подсказка | Эффект |
| ------ | -------- |
| `route=federated` | Принудительно федерировать через движок федерации, минуя прямую маршрутизацию через драйвер |
| `route=direct` | Принудительно выполнить напрямую через драйвер |

(REQ-279, REQ-277, REQ-278)

## Пресеты колонок в мутациях

`compiler/mutation_gen.py` поддерживает пресеты для отдельных колонок на стороне сервера, применяемые при `INSERT` или `UPDATE`. (REQ-214) Пресеты не включаются в сгенерированный входной тип GraphQL-мутации — они внедряются компилятором прозрачно. Типы пресетов: `static` (буквальное значение) или `session` (значение из сессии/заголовка запроса, например `x-hasura-user-id`). (REQ-214)

## Обозреватель схемы GraphQL Voyager

Admin UI (`provisa-ui/src/pages/SchemaExplorer.tsx`) встраивает GraphQL Voyager как интерактивный инструмент визуализации схемы. (REQ-248) Он отображает схему в области видимости роли как навигируемую диаграмму «сущность-связь» — таблицы как узлы, связи как рёбра. Показанная схема всегда отфильтрована по текущей выбранной роли.

## Порядок применения безопасности

Для самого запроса нет gate по возможностям (capability) — governance выражается полностью через элементы управления на уровне данных. (REQ-001) Запрос на сыром SQL отклоняет (HTTP 403) любую таблицу вне области видимости объектов роли до того, как запустится governance. (REQ-267)

1. **Видимость объектов**: схема для каждой роли скрывает неавторизованные таблицы/колонки; таблицы вне области видимости в сыром SQL отклоняются (REQ-039, REQ-267)
2. **Применение связей**: обходы должны существовать в утверждённом каталоге связей, если роль не обладает `ignore_relationships` — среди заложенных системных ролей это только `modeler` (REQ-001, REQ-1297). В режиме повышенной безопасности эта возможность игнорируется, и ни один обход не выходит за пределы каталога (REQ-693)
3. **RLS**: внедрение предложения WHERE для каждой таблицы, для каждой роли (REQ-040, REQ-041, REQ-263)
4. **Маскирование колонок**: преобразование данных для каждой колонки, для каждой роли (REQ-263)
5. **Ограничение строк (LIMIT)**: предел числа строк для ролей без `full_results`; случайное статистическое сэмплирование — отдельная пользовательская функция запроса (REQ-263, REQ-478)

Все четыре интерфейса запросов (HTTP, Flight, gRPC, pgwire) применяют один и тот же конвейер governance этапа 2; ни один клиентский путь не может его обойти, не обойдя сервер. (REQ-002, REQ-038, REQ-266)

## Пределы масштабируемости

Provisa — тонкий слой компиляции и маршрутизации — она добавляет к задержке запроса единицы миллисекунд. Однако пути, где Provisa сериализует данные результата, ограничены памятью процесса. Два пути по-настоящему неограничены:

| Путь | Ограничен памятью? | Подходит для |
| ------ | -------------- | ------------- |
| JSON встроенно (HTTP) | Да | Небольшие-средние результаты |
| **Потоковая передача Arrow Flight (gRPC :8815)** | **Нет** | **Неограниченно — потоковая передача через Zaychik или Arrow API хранилища данных** |
| Protobuf gRPC встроенно (:50051) | Да | Средние результаты, взаимодействие сервис-сервис |
| Перенаправление: загрузка Provisa (JSON, CSV, NDJSON, Arrow IPC) | Да | Средние результаты, скачивание файлов |
| **Перенаправление: CTAS (Parquet, ORC)** | **Нет** | **Неограниченно — движок федерации пишет в S3** |

(REQ-145, REQ-138)

### Пороговое зондирование

Для перенаправления на основе порога Provisa внедряет `LIMIT threshold + 1` в запрос в качестве зонда. (REQ-140) Если в результате меньше строк, он возвращается встроенно (полный результат, никакой потраченной впустую работы). Если результат достигает предела, зонд отбрасывается, и полный запрос повторно выполняется через CTAS или загрузку Provisa. Это позволяет избежать `SELECT COUNT(*)` (который некоторые источники не оптимизируют) и работает для любого источника.

Для крупных аналитических нагрузок используйте один из вариантов:

- **Arrow Flight** (порт 8815) для потоковой передачи в инструменты данных — батчи проходят через Provisa без материализации (REQ-145)
- **Перенаправление Parquet/ORC** для экспортов на основе файлов — движок федерации пишет напрямую в S3, Provisa возвращает подписанный URL (REQ-138, REQ-044)

## Инфраструктура

| Сервис | Образ | Порт | Назначение |
| --------- | ------- | ------ | --------- |
| Provisa API | (процесс хоста) | 8001 | Конечная точка HTTP/REST |
| Provisa Flight | (процесс хоста) | 8815 | Сервер Arrow Flight gRPC |
| Provisa gRPC | (процесс хоста) | 50051 | Сервер Protobuf gRPC |
| Federation Engine | `trinodb/trino` (по умолчанию) или внешнее хранилище данных | 8080 / варьируется | Движок федерации запросов — Trino для встроенного стека; Snowflake/Databricks/BigQuery/Fabric/Synapse/DuckDB для целей хранилищ данных |
| Zaychik | `provisa-zaychik` (собирается из исходников) | 8480 | Прокси Arrow Flight SQL для Trino; не требуется для движков хранилищ данных |
| PostgreSQL | `postgres:16` | 5432 | Метаданные конфигурации + каталог Iceberg |
| MongoDB | `mongo:7` | 27017 | Демонстрационный источник данных NoSQL |
| MinIO | `minio/minio` | 9000/9001 | S3-совместимое объектное хранилище |
| Redis | `redis:7-alpine` | 6379 | Кеш результатов запросов |
| PgBouncer | `edoburu/pgbouncer` | 6432 | Пул соединений для PG |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092 | Потоковые источники данных |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.0` | 8081 | Управление схемами Avro/Protobuf |

(REQ-055, REQ-169)
