# Интеграции {: #integrations }

## Выбор пути подключения {: #choosing-a-connection-path }

| Тип клиента | Рекомендуемый путь | Почему |
| ------------- | ----------------- | ----- |
| BI-инструменты (Tableau, Power BI, Looker) | JDBC | Колоночная потоковая передача Arrow Flight по сети; у BI-инструментов есть встроенный мастер JDBC, и они выигрывают от высокопроизводительной колоночной доставки больших результирующих наборов |
| psql, DBeaver, любой PG-совместимый инструмент | pgwire (нативный драйвер PG) | Вариант без трения по умолчанию — не нужен специальный драйвер; используйте то, что уже есть |
| Python-стек данных (pandas, pyarrow) | `provisa-client` или чистый ADBC | Потоковые батчи Arrow; без накладных расходов на сериализацию строк |
| Spark, DuckDB, высокопроизводительные конвейеры | Arrow Flight (ADBC) | Неограниченная колоночная потоковая передача напрямую в память Arrow |
| Взаимодействие сервис–сервис (типизированные контракты) | Protobuf gRPC | Сгенерированный для каждой роли proto; потоковая передача строк; типобезопасность |
| Веб-приложения, скрипты | HTTP (`/data/graphql`, `/data/sql`) | Без драйвера; стандартный HTTP; полный выбор языка запросов |
| REST-клиенты (стандарт JSON:API) | `GET /data/jsonapi/{table}` | Конверт JSON:API v1.0; разреженные наборы полей, пагинация, фильтрация через параметры запроса; без драйвера |

---

## pgwire — нативный драйвер PostgreSQL {: #pgwire-native-postgresql-driver }

Provisa реализует проводной протокол PostgreSQL (версия протокола 3.0). Любой клиент, говорящий на PostgreSQL, подключается без специального драйвера.

Включается установкой `PROVISA_PGWIRE_PORT` (например, `5433`) перед запуском Provisa. Отключено, если переменная не задана или равна `0`.

### Почему pgwire, а не JDBC? {: #why-pgwire-instead-of-jdbc }

Драйвер JDBC использует Arrow Flight как транспорт и требует развёртывания `provisa-jdbc.jar`. Для pgwire не требуется ничего — если у вас уже есть `psql`, DBeaver, SQLAlchemy или PG-драйвер JDBC, всё готово. Это путь с меньшим трением для нагрузок, ограниченных SQL.

JDBC — правильный выбор для BI-инструментов со встроенным мастером подключения JDBC, которые выигрывают от колоночной потоковой передачи Arrow Flight для больших результирующих наборов. pgwire принимает произвольный SQL по всей опубликованной схеме — те же запросы, но с меньшими затратами на настройку.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Имя пользователя / пароль, как настроено в Provisa
4. Загрузка дополнительного драйвера не требуется

### SQLAlchemy (Python)

```python
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://alice:secret@localhost:5433/provisa")
df = pd.read_sql("SELECT * FROM sales.orders", engine)
```

Или с `asyncpg`:

```python
engine = create_engine("postgresql+asyncpg://alice:secret@localhost:5433/provisa")
```

### Аутентификация {: #authentication }

Поле `password` пакета startup несёт учётные данные, а то, чем именно является это значение,
определяет метод: персональный токен доступа, OIDC bearer-токен или пароль относительно
настроенного провайдера. У провайдера `basic` при `auth.scram: true` пароль доказывается через
SCRAM-SHA-256, а не отправляется напрямую. Клиентские сертификаты поддерживаются. В режиме доверия
(`none`) имя пользователя напрямую сопоставляется с ролью, а пароль игнорируется.

Полная таблица «поверхность × метод» приведена в [Security Model](security.md#surfaces-and-credentials). MD5 не поддерживается; включайте TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) при работе через недоверенную сеть.

### Ограничения {: #limitations }

- Только SQL. GraphQL и Cypher через pgwire не принимаются.
- Не только чтение. Строки зарегистрированных таблиц записываются с помощью `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` и `COPY ... FROM STDIN`, каждую из которых допускают право `write` роли, её предоставления колонок и её фильтр строк; см. [Запись данных](pgwire.md#data-writes-insert-update-delete-merge-copy-from-stdin).
- Без определений: `CREATE`, `ALTER` и `DROP` отклоняются с SQLSTATE 0A000, кроме собственной временной таблицы сеанса; см. [Определения](pgwire.md#definitions-create-alter-drop). `COPY ... TO STDOUT` экспортирует в `text`, `csv` и `binary`; `COPY ... FROM STDIN` читает `text` и `csv`.
- Запросы к `information_schema` и `pg_catalog` перехватываются и обслуживаются прослойкой каталога DuckDB — инструменты обнаружения схемы работают корректно.

---

## Драйвер JDBC {: #jdbc-driver }

Драйвер JDBC Provisa использует Arrow Flight в качестве базового транспорта. Это рекомендуемый путь для BI-инструментов с мастером подключения JDBC.

Запросы идут через Arrow Flight, если порт Flight отвечает при открытии соединения, и результаты поступают по одной партии вместе с типами колонок. Портом Flight считается порт рядом с портом HTTP (8815 рядом с 8001); задайте свойство `flight_port`, если это не так. Если порт Flight недоступен при подключении, соединение использует для своих запросов HTTP API и записывает это в журнал на уровне INFO. Когда соединение уже работает через Flight, ошибка сервера — отклонённые учётные данные, роль, которой у пользователя нет, неудавшаяся инструкция — передаётся вызывающему с сообщением сервера; повторно через HTTP она не выполняется. (REQ-293)

Просмотр схемы (`getTables`, `getColumns`, первичные и внешние ключи) читается через тот же порт Flight с учётными данными входа: драйвер перечисляет каталог роли, от имени которой действует, поэтому таблица или колонка, не предоставленная этой роли, не появляется. Ключ, который ссылается на не предоставленную роли таблицу или колонку, тоже не перечисляется. В соединении, у которого порт Flight был недоступен при подключении, драйвер читает тот же каталог по HTTP (`GET /data/catalog`): сервер строит оба перечня одной функцией, поэтому таблицы, имена колонок, типы и ключи не зависят от того, какой порт ответил. (REQ-128)

### Подключение {: #connection }

Скачайте [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (всегда последний релиз) и добавьте его в путь драйверов вашего инструмента.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

Аутентификация использует стандартные свойства JDBC `user` / `password`. Драйвер обменивает их на токен сеанса и отправляет его с каждым запросом; отклонённый вход приводит к ошибке соединения со статусом и причиной сервера. Provisa выводит роль из идентичности, выполнившей вход. Необязательное свойство `role` (или `?role=` в URL) просит действовать от одной роли, которую имеет пользователь, или от набора таких ролей через запятую; роль, которой у пользователя нет, отклоняется. В развёртывании без провайдера аутентификации входа нет, и имя пользователя принимается как запрошенная роль. (REQ-131, REQ-273)

### Настройка BI-инструментов {: #bi-tool-setup }

**Tableau**

1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Введите имя пользователя и пароль при запросе

**DBeaver** (путь JDBC — для пути pgwire см. выше)

1. Database → New Connection → JDBC
2. Driver: добавьте `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Введите имя пользователя и пароль на вкладке Authentication

**Power BI** — используйте шлюз ODBC с мостом Provisa JDBC-ODBC (включён в установщик).

---

## Клиенты Arrow Flight {: #arrow-flight-clients }

Arrow Flight (порт 8815) — рекомендуемый путь для инструментов данных, которые его поддерживают. Результаты передаются потоком как Arrow RecordBatches без материализации в памяти Provisa.

### Python (`provisa-client`)

Рекомендуемый путь для Python — оборачивает и GraphQL, и Arrow Flight:

```bash
pip install provisa-client
```

```python
from provisa_client import ProvisaClient

client = ProvisaClient("http://localhost:8001", username="alice", password="secret")

# Arrow Flight → pyarrow Table (high-throughput, streaming)
table = client.flight("SELECT id, amount FROM sales.orders")

# Arrow Flight → pandas DataFrame
df = client.flight_df("SELECT id, amount FROM sales.orders")

# GraphQL → DataFrame
df = client.query_df("{ orders { id amount } }")
```

Полное описание, включая DB-API 2.0, диалект SQLAlchemy и ADBC, см. в [docs/python-client.md](python-client.md).

### Python (чистый PyArrow) {: #python-raw-pyarrow }

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Flight несёт свои учётные данные в полезной нагрузке JSON, в поле `token` — bearer-токен провайдера
или персональный токен доступа. И рукопожатие (handshake), и каждый ticket принимают его, и оба
проверяют его одинаково, поэтому клиент, аутентифицировавшийся при рукопожатии, всё равно
предъявляет токен при каждом `do_get`. Поле `role` рядом с ним *запрашивает* роль; сервер выводит
разрешённые для идентичности роли и подставляет авторизованное значение, поэтому строка роли в
ticket никогда не является идентичностью. (REQ-1263) См. [Security Model](security.md#surfaces-and-credentials).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
```

Просмотру тоже нужны учётные данные. `list_flights`, `get_flight_info` и `get_schema` не несут тикета, поэтому они читают учётные данные из заголовка вызова `authorization: Bearer <token>`, а запрошенную роль — из `x-provisa-role` (одна роль или набор имеющихся ролей через запятую). При включённой аутентификации вызов просмотра без учётных данных отклоняется, а возвращаемый каталог — это каталог авторизованной роли: таблицы, колонки и команды, предоставленные этой роли, и ничего больше. Тикет каталога (тикет без `query`) обслуживается так же, по его собственным `token` и `role`. Поэтому клиент должен отправить свои учётные данные до просмотра; `provisa-client` делает это в `list_tables()`. В развёртывании без провайдера аутентификации эти вызовы перечисляют весь каталог и не принимают учётных данных. (REQ-1263, REQ-127)

```python
options = flight.FlightCallOptions(headers=[
    (b"authorization", b"Bearer provisa_pat_..."),
    (b"x-provisa-role", b"analyst"),
])
for info in client.list_flights(b"", options):
    print(info.descriptor.path)
```

### ADBC

```python
import adbc_driver_flightsql.dbapi as adbc

conn = adbc.connect("grpc://localhost:8815", db_kwargs={"username": "alice", "password": "secret"})
cursor = conn.cursor()
cursor.execute("SELECT id, amount FROM sales.orders")
table = cursor.fetch_arrow_table()
```

### DuckDB

```python
import duckdb, pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT * FROM sales.orders"}')
arrow_table = client.do_get(ticket).read_all()

conn = duckdb.connect()
result = conn.execute("SELECT region, sum(amount) FROM arrow_table GROUP BY 1").df()
```

### Spark (PySpark)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.arrow:flight-core:14.0.0") \
    .getOrCreate()

# Use ADBC Flight connector or load via pandas → Spark
```

---

## Protobuf gRPC (порт 50051) {: #protobuf-grpc-port-50051 }

Путь «сервис — сервис». Provisa генерирует `.proto` для каждой роли и пересоздаёт его при каждом изменении модели или роли — каждая роль видит только те таблицы и колонки, к которым у неё есть доступ. В сервисе есть RPC для каждой таблицы в корне схемы роли; связанные таблицы достигаются через их связи, как в GraphQL.

Скачайте proto для своей роли:

```bash
curl http://localhost:8001/data/proto/analyst > provisa_analyst.proto
```

Используйте `grpc_server_reflection`, чтобы программно исследовать схему. Рефлексия описывает сервис роли, от имени которой выполняется вызов, — тот же файл, что и скачиваемый выше. При отключённой аутентификации вызов рефлексии указывает свою роль, как любой другой вызов:

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
```

Каждый RPC должен нести учётные данные в ключе метаданных `authorization` — токен провайдера или персональный токен доступа. `x-provisa-role` запрашивает роль из разрешённого набора идентичности; это не учётные данные и никогда ими не было. Клиентские сертификаты поддерживаются. См. [Security Model](security.md#surfaces-and-credentials).

Потоковые запросы выдают по одному сообщению на строку; мутации — унарные.

---

## Вызов команд через протоколы {: #invoking-commands-across-protocols }

**Команда (command)** — это зарегистрированная отслеживаемая функция или webhook: вызываемый
объект, зарегистрированный в модели Provisa, с полем `kind` (`query` или `mutation`) и
`impl_kind`, описывающим, как он выполняется. Каждая поверхность направляет вызовы через единый
управляемый исполнитель (`invoke_tracked_function`), который единообразно применяет назначение команды (`visible_to`), право `write` для мутации
и governance (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`,
`provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`,
`provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Что выполняется | Поля привязки |
| ------------ | ----------- | --------------- |
| `source_procedure` | Хранимая процедура на зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Скрипт на стороне сервера | `script` |
| `http` | Исходящий HTTP-вызов | `url`, `method` |
| `grpc` | Исходящий gRPC-вызов к внешнему серверу | `target`, `method` |
| `python` | Python-вызываемый объект, размещённый в Provisa (REQ-885) | `callable` (напр., `demo.py_functions:random_dataset`) |

Когда команда объявляет `return_schema` (JSON Schema с `type: array, items: object`), она
возвращает набор — каждая поверхность проецирует его как типизированный набор строк. Демонстрационные
команды `random_python_set` (impl_kind `python`) и `random_grpc_set` (impl_kind `grpc`) иллюстрируют
как размещённый вызываемый объект, так и внешний мост gRPC, возвращающий строки со случайными
значениями; обе зарегистрированы в `config/provisa-install.yaml`. [tool-verified:
`config/provisa-install.yaml:809-856`]

### Матрица протоколов {: #protocol-matrix }

| Поверхность | Синтаксис | Пример |
| --------- | -------- | --------- |
| GraphQL | `kind=query` → поле Query; `kind=mutation` → поле Mutation; с префиксом домена при `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` или `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / driver) | `CALL fn(args)` — позиционные аргументы сопоставляются с объявленными именами аргументов | `CALL random_python_set(3, 7)` |
| Provisa gRPC (порт 50051) | Унарный `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Поле `kind` управляет только размещением в GraphQL — поверхности SQL, Cypher, Bolt и gRPC принимают команды `query` и `mutation` одинаково.

---

## Apollo Federation

Provisa может выступать в роли субграфа Federation v2, предоставляя свою опубликованную схему Apollo Router или Apollo Gateway.

### Настройка {: #setup }

Включите федерацию в `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa автоматически генерирует директивы `@key` на колонках первичного ключа и `@external`/`@provides` на межсубграфовых связях.

### Регистрация в Apollo Router {: #register-with-apollo-router }

В вашем `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Запустите `rover supergraph compose --config supergraph.yaml`, чтобы сгенерировать схему supergraph.

### Сущности {: #entities }

Provisa отвечает на запросы `_entities` для межсубграфовых join. Любая таблица с первичным ключом автоматически становится разрешаемой как сущность (entity) Federation.

---

## Импорт Hasura v2 / DDN {: #hasura-v2-ddn-import }

О переходе с Hasura на Provisa см. [docs/import.md](import.md).

---

## Kafka

Настройку тем Kafka как таблиц только для чтения и как приёмников результатов запросов см. в [docs/sources.md](sources.md#kafka-sources).

---

## Проверки качества данных (REQ-1443) {: #data-quality-checkers-req-1443 }

Soda Core и Great Expectations подключаются к Provisa так же, как любой другой postgres-клиент — через pgwire. Это и есть вся интеграция: чекер держит один драйвер postgres и сканирует федеративное представление, поэтому таблица Snowflake, таблица Iceberg и коллекция Mongo проверяются одним и тем же диалектом контрактов без отдельного чекера для каждой системы. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

Сканирование выполняется в дочернем интерпретаторе — `python -m provisa.dq.worker` — единственном месте, где импортируются `soda_core` или `great_expectations`. В процесс сервера ничего не линкуется, и падение чекера обрушивает подпроцесс, а не цикл событий. [tool-verified: `provisa/dq/runner.py` `build_command`]

Результаты сканирования попадают как обычные строки источника, поэтому периодичность, свежесть, события, происхождение, governance, RLS, сетка данных и экспорт применяются без второго механизма. Написание контрактов, конверт результата и производная регистрация описаны в [docs/sources.md](sources.md#data-quality-checkers-req-1443).

### Установка чекера {: #installing-a-checker }

Ни одна из библиотек не поставляется по умолчанию. Установщик спрашивает, какую вы хотите, и ответ становится `dq_checker: none|soda|gx` в `~/.provisa/config.yaml`. На уровне Docker `scripts/provisa` превращает это в аргумент сборки `PROVISA_EXTRAS`; на нативном уровне `first-launch.sh` устанавливает соответствующий extra pyproject в venv. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | Библиотека | Лицензия | Размещённая облачная плоскость |
| -------------- | --------- | --------- | -------------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Отклонено (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Разрешено |

Elastic License 2.0 запрещает предоставление ПО третьим лицам как размещённой (hosted) услуги, а именно этим было бы выполнение Soda внутри плоскости SaaS от имени клиента. Размещённое развёртывание, которому нужен Soda, указывает на конечную точку Soda, которую оператор запускает самостоятельно. Ключи подключения см. в [docs/configuration.md](configuration.md#data-quality-checkers-soda-great_expectations).

---

## Семантический обмен Apache Ossie (REQ-1316) {: #apache-ossie-semantic-interchange-req-1316 }

Provisa обменивается семантическими моделями с Apache Ossie (спецификация 0.2.0.dev0, в инкубации; ранее Open
Semantic Interchange) через пограничный адаптер. Внутренний словарь Provisa никогда не переименовывается
в словарь Ossie — спецификация объявляет несовместимые изменения вероятными, поэтому связь ограничена адаптером.
[tool-verified: `provisa/ossie/convert.py` module docstring; `OSSIE_VERSION = "0.2.0.dev0"`]

Черновая спецификация меняла форму, не меняя строки версии. Адаптер следует
спецификации в том виде, в каком она находится в коммите `642cb894` репозитория `apache/ossie` (2026-10-10): одна модель на документ,
с `version`, `name`, `datasets`, `relationships` и `metrics` в корне документа и
перечисляемым `datatype`. Экспорт проверяется в модульных тестах по
`core-spec/ossie-schema.json` этого коммита.
[tool-verified: `OSSIE_SPEC_COMMIT`, `provisa/ossie/convert.py`;
`test_the_export_is_valid_against_ossies_own_schema`, `tests/unit/test_ossie_convert.py`]

### Экспорт {: #export }

Каноническая поверхность экспорта — живой HTTP endpoint. Он выводит документ Ossie из живого состояния
при каждом запросе — без кеширования и шага генерации.

```http
GET /admin/ossie
```
Ответ — YAML-документ с `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Страница Метрики также предлагает кнопку **Скачать** и копируемый URL endpoint на панели Ossie
Interchange, обе ведут на тот же endpoint.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Что экспортируется {: #what-is-exported }

Адаптер отображает объекты Provisa на объекты Ossie так:

| Объект Provisa | Объект Ossie | Примечания |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; первичные/уникальные ключи из конфигурации колонок и `UniqueConstraint` |
| `Column` | `field` | `expression` = ссылка на колонку (диалект ANSI_SQL); временные колонки получают `dimension.is_time: true` |
| `Relationship` | `relationship` | Псевдоним используется как имя, если задан; вычисляемые связи (с целью-функцией) пропускаются |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype` (отображается как у колонки), `description`, `ai_context` |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Только для обратного пути; другие инструменты могут игнорировать |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py`: "Computed
(function-target) relationships have no dataset target — not representable in Ossie; skipping
is the defined export boundary"]

Связь по нескольким колонкам перечисляет каждую колонку в `from_columns` и `to_columns`.
Модель без таблиц отклоняется с `422`: в документе Ossie есть хотя бы один dataset.
[tool-verified: `test_a_relationship_over_several_columns_lists_each_column`,
`test_a_model_with_no_table_is_refused_by_name`, `tests/unit/test_ossie_convert.py`]

Governance, RLS, lineage и семантика графа не экспортируются. Они могут передаваться в необязательном слоте
`provisa` в custom_extensions для точности обратного пути, но обмен никогда не зависит от того, читают ли его другие
инструменты. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Тип колонки вне словаря Ossie записывается как `Opaque`, с собственным именем типа в расширении
`provisa` поля, — именно это спецификация предписывает для известного типа, для которого у неё нет
члена. Колонка без записанного типа не указывает `datatype`. Адаптер никогда не отображает на тип, которого у
колонки нет. [tool-verified: `_map_datatype`, `_typed`, `provisa/ossie/convert.py`]

#### Отображение типов {: #type-mapping }

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py`]

| Тип Provisa / источника | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `String` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `Integer` |
| `numeric`, `decimal` | `Decimal` |
| `float`, `double`, `real` | `Float` |
| `bool`, `boolean` | `Boolean` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp`, `datetime` | `DateTime` |
| `timestamptz`, `timestamp with time zone` | `DateTimeTz` |
| любой другой | `Opaque`, с именем типа в расширении `provisa` |

`Decimal` в Ossie не несёт точности и масштаба, поэтому `numeric(10,2)` экспортируется как `Decimal`
и возвращается из импорта как `decimal`.
[tool-verified: `test_parse_round_trips_export`, `tests/unit/test_ossie_convert.py`]

### Импорт {: #import }

Импорт принимает документ Ossie (YAML или JSON) и возвращает предложения регистрации. Ничего не
регистрируется автоматически — импортированные определения никогда не обходят шаг проверки.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```
Сервер разбирает документ с помощью `parse_ossie_model`, который проверяет структуру и возвращает
dataclass `OssieImport` с предлагаемыми таблицами, связями и метриками в виде простых словарей.
Любая структурная проблема — это `400` с ошибкой, названной по пути, например
`ossie import: missing $.datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py`:
"Nothing is registered here — imported definitions never bypass registration review"]

Импортёр читает только плоскую форму документа. Три вещи отклоняются по имени:

- Документ, который оборачивает свою модель в `semantic_model` — форму, которую черновая спецификация имела до
  сентября 2026 года. Ошибка говорит, что документ старше плоской формы, и как его перевести.
- `version`, отличная от `0.2.0.dev0`.
- `datatype`, который не является членом словаря Ossie; ошибка называет путь и перечисляет
  члены.

Предлагаемая колонка или метрика получает тип, который представляет её datatype Ossie (`Integer` предлагает
`bigint`, `DateTime` предлагает `timestamp`). `Opaque` предлагает тип, который называет расширение `provisa`,
если документ пришёл из Provisa, и никакого типа иначе. `ai_context`, заданный как
объект, читается по его `instructions`.

Поле предлагается как колонка, только если его выражение — это одна колонка источника. Поле,
выражение которого является вычислением, не предлагается; экран проверки называет его под его таблицей
как неимпортированное. Поле, которое переименовывает свою колонку, предлагается как эта колонка, с именем
поля в качестве псевдонима колонки. `source` у dataset должен состоять из трёх частей,
`source.schema.table`; запрос или более короткое имя отклоняется, и ошибка об этом говорит.
[tool-verified: `parse_ossie_model`, `_parse_dataset`, `_proposed_type`, `_ai_context`,
`provisa/ossie/convert.py`; `tests/unit/test_ossie_convert.py`]

#### Из dbt, Cube, Snowflake и других семантических слоёв {: #coming-from-dbt-cube-snowflake-and-other-semantic-layers }

Проект Apache Ossie поддерживает конвертеры между Ossie и форматами моделей других инструментов. Запустите
конвертер для своего инструмента, затем отправьте его вывод на endpoint импорта или выберите файл на
экране проверки ниже. Provisa читает документ Ossie; файлы другого инструмента он не читает.

| Откуда | Команда конвертера | Что он читает |
|---|---|---|
| dbt (семантический слой) | `ossie-dbt msi-to-ossie -i target/semantic_manifest.json -o semantic_model.yaml` | `semantic_manifest.json`, который dbt пишет для MetricFlow |
| Cube | `ossie-cube import -i model/ -o model.yaml` | Каталог модели Cube или отдельные файлы кубов и представлений |

[tool-verified: `converters/dbt/README.md` and `converters/cube/README.md` in the
`apache/ossie` repository, read 2026-10-10]

В том же репозитории есть конвертеры для Snowflake, Databricks, Microsoft, Salesforce,
ThoughtSpot, Sigma, Omni, Hex, GoodData и других; у каждого свой README.
[tool-verified: directory listing of `converters/` in `apache/ossie`, 2026-10-10]

Два ограничения, о которых стоит знать до конвертации:

- Конвертер dbt читает только манифест семантического слоя. Проекту dbt без семантических
  моделей нечего ему конвертировать. Он отбрасывает метрики конверсии, приватные метрики и
  сущности с естественным ключом и печатает предупреждение для каждой.
  [tool-verified: `converters/dbt/README.md`, the "MSI → Ossie is lossy" table]
- Вывод конвертера следует спецификации в том виде, в каком она есть в собственном коммите этого конвертера. Provisa читает
  плоскую форму коммита `642cb894` репозитория `apache/ossie`; конвертер старше сентября 2026 года
  пишет обёртку `semantic_model`, которую Provisa отклоняет по имени.

Оба пути были выполнены 2026-10-10 с конвертерами, установленными из коммита `642cb894` репозитория `apache/ossie`
(они не публикуются как пакеты; они устанавливаются из каталогов `converters/dbt` и
`converters/cube` репозитория и требуют Python 3.11 или новее). Модель из
двух таблиц, одной связи и одной метрики была экспортирована из Provisa, сконвертирована в семантический манифест dbt
и в модель Cube, сконвертирована обратно в Ossie с помощью `ossie-dbt msi-to-ossie` и
`ossie-cube import` и прочитана `parse_ossie_model`:

| Путь | Результат |
|---|---|
| Модель Cube → Ossie → Provisa | Прочитано. Вернулись таблицы, ключи, типы колонок, связь и метрика; имя связи было именем конвертера (`orders_to_customers`). |
| Семантический манифест dbt → Ossie → Provisa | Прочитано. Вернулись таблицы, первичные ключи и метрика. Типы колонок нет (вывод конвертера их не указывает), а связь вернулась такой, какой конвертер вывел её из сущностей dbt, а это не зарегистрированная. Проверьте связи перед применением. |

[tool-verified: both runs, 2026-10-10, `apache/ossie` commit `642cb894`, `ossie-dbt` and
`ossie-cube` 0.2.0.dev0, metricflow 0.213.0; inputs were files those converters had written
from a Provisa export, not files authored in dbt or Cube]

Конвертер dbt отклоняет модель, в которой у какой-либо таблицы есть ограничение уникальности по нескольким
колонкам («MetricFlow entities cannot represent composite keys losslessly»).
[tool-verified: `ossie-dbt ossie-to-msi` on the same export with such a constraint, 2026-10-10]

#### Экран проверки {: #the-review-screen }

В интерфейсе кнопка **Импорт** (страница Метрики → панель Ossie Interchange) открывает выбор файла.
После отправки и разбора документа открывается окно проверки, в котором каждая предлагаемая таблица,
связь и метрика перечислены как отмеченные элементы. Моделировщик может снять отметку с чего угодно, чтобы исключить это.
Кнопка **Применить** регистрирует отмеченные элементы через существующие мутации регистрации — сначала
таблицы, затем связи (которые ссылаются на таблицы), затем метрики.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Роль моделирования и история, сохранённые в документе Ossie, экспортированном Provisa, корректно проходят обратный путь
при импорте. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

### Файл sources для dbt (REQ-1967) {: #dbt-sources-file-req-1967 }

Для проекта dbt, который не использует семантический слой, Provisa пишет `sources.yml` dbt напрямую.
Он выводится из живой модели при каждом запросе.

```http
GET /admin/dbt/sources?role=<role id>
```
Ответ — YAML с `Content-Disposition: attachment; filename=provisa.sources.yml`.
Вызывающему нужен `org_settings`, а `role` должна быть ролью, которую он имеет; обладатель
`access_config` может назвать любую роль. Панель обмена на странице Метрики предлагает ту же загрузку
для используемой роли.
[tool-verified: `download_dbt_sources`, `provisa/api/admin/ossie_router.py`; `TestTheEndpoint`,
`tests/unit/test_dbt_sources.py`]

Файл перечисляет каждую таблицу, которую обслуживает роль, под source dbt для каждого источника и схемы (с именем
по id источника или `<source>_<schema>`, когда у источника есть таблицы более чем в одной схеме),
с описанием таблицы, описаниями обслуживаемых колонок и такими тестами:

| Модель указывает | Файл пишет |
|---|---|
| Первичный ключ из одной колонки | `unique` и `not_null` на этой колонке |
| Первичный ключ из нескольких колонок | `not_null` на каждой колонке |
| Ограничение уникальности из одной колонки | `unique` на этой колонке |
| Связь между двумя таблицами, которые обе обслуживаются роли | `relationships` на исходной колонке, к `source('<source>', '<table>')` |

Колонка, которая не обслуживается роли, отсутствует, как и таблица без обслуживаемых колонок. У ключа по
нескольким колонкам нет стандартного теста dbt; вступительный комментарий файла называет каждый такой ключ, и
теста для него не пишется. Модель не записывает допустимость NULL, поэтому `not_null` появляется только на ключевых колонках.
Файл не несёт метрик, построчной безопасности, маскировки, ролей или lineage, и его вступительный комментарий
это говорит.
[tool-verified: `build_dbt_sources`, `provisa/dbt/sources.py`; `tests/unit/test_dbt_sources.py`]

Файл, сгенерированный из модели из двух таблиц и одной связи, разобран dbt-core 1.12.5
с dbt-duckdb 1.11.0: два source и пять тестов, без предупреждений. dbt-core 1.9.11 не читает
его тесты `relationships`, которые вкладывают свои аргументы в `arguments`; промежуточные версии
не проверялись. [tool-verified: `dbt parse` and `dbt ls` on both versions, 2026-10-10; `dbt test`
against data was not run]

---

## Метрики по всем протоколам (REQ-1319) {: #metrics-across-protocols-req-1319 }

Определение управляемой метрики — её выражение, описание и `ai_context` — перемещается вместе со
значением на каждую поверхность запроса через одно раскрытие компилятором. Копий не существует.
Компилятор резервирует схему `metrics` для доступа по SQL; каждый протокол затем добавляет свой
собственный канал метаданных.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Обращайтесь к любой метрике как к виртуальному отношению в схеме `metrics`. Выбранные колонки
измерений становятся `GROUP BY`:

```sql
-- Grand total
SELECT value FROM metrics.net_revenue;

-- By region
SELECT region, value FROM metrics.net_revenue GROUP BY region;

-- By region and month, filtered
SELECT region, month, value
FROM metrics.net_revenue
WHERE net_revenue.status = 'completed'
GROUP BY region, month;
```

Компилятор раскрывает форму `metrics.<name>` в реальный сгруппированный агрегат до выполнения
governance. Описания колонок отображаются как записи `pg_description`, поэтому DBeaver и `\d+` в
psql их показывают. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py`
lines 52–70; REQ-1319: "description surfaced via pg_description"]

`SELECT *` отклоняется — указывайте колонки явно.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Метрики проецируются внутри корневого поля `_aggregate` как блок `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Текст определения (`description`, `ai_context`) появляется в документации интроспекции GraphQL,
поэтому инструменты, осведомлённые о схеме, и генераторы кода подхватывают его автоматически.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (AI-агенты) {: #mcp-ai-agents }

Два инструмента предоставляют метрики клиентам MCP:

- **`list_metrics`** — возвращает все управляемые метрики, видимые сессии, с полями `name`,
  `description` и `ai_context`.
- **`query_metric`** — принимает имя метрики и список измерений и вызывает семантический
  SQL-путь компилятора, возвращая результат агрегации.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Агенты, вызывающие `list_metrics` перед построением запроса, выбирают управляемую метрику по имени,
а не пишут агрегирующий SQL вручную. Поле `ai_context` — это место для текста определения,
направляющего правильный выбор.

### Arrow Flight {: #arrow-flight }

Метрики адресуемы как дескрипторы Flight для метрик, возвращающие таблицы Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Используйте ту же форму SQL `metrics.<name>` через стандартный путь тикетов Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Вызовите метрику через процедуру `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Таблицы Fact и Dimension несут метки узлов `:Fact` и `:Dimension` в федеративном графе, поэтому
Bloom автоматически отображает форму звезды.
[inferred: per REQ-1319 and REQ-1320: "federated graph labels nodes :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Запросы на естественном языке {: #natural-language-queries }

Сопоставитель схем NL разрешает словарь метрик в вопросах на естественном языке напрямую в метрику
плюс измерения, затем генерирует семантический SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Таблицы фактов помечаются `[fact]` в подсказке NL; таблицы измерений помечаются `[dimension]`.
Сопоставитель смещает пути join от факта к измерению при разрешении вопросов.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Потоковая передача {: #streaming }

Комбинируйте `view_metrics` с `materialize`, чтобы представление метрик оставалось актуальным при
изменении его входных данных, используя существующий механизм материализации. Новый конвейер не
требуется.
[inferred: per REQ-1319. The table Kafka sink that requirement named is retired (REQ-176); a
table publishes its new rows to Kafka through a live output (`live.outputs`, REQ-565)]

### Наблюдаемость (OTel) {: #observability-otel }

Вычисления метрик трассируются и экспортируются как метрики OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
