# Интеграции

## Выбор пути подключения

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

## pgwire — нативный драйвер PostgreSQL

Provisa реализует проводной протокол PostgreSQL (версия протокола 3.0). Любой клиент, говорящий на PostgreSQL, подключается без специального драйвера.

Включается установкой `PROVISA_PGWIRE_PORT` (например, `5433`) перед запуском Provisa. Отключено, если переменная не задана или равна `0`.

### Почему pgwire, а не JDBC?

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

### Аутентификация

Поле `password` пакета startup несёт учётные данные, а то, чем именно является это значение,
определяет метод: персональный токен доступа, OIDC bearer-токен или пароль относительно
настроенного провайдера. У провайдера `basic` при `auth.scram: true` пароль доказывается через
SCRAM-SHA-256, а не отправляется напрямую. Клиентские сертификаты поддерживаются. В режиме доверия
(`none`) имя пользователя напрямую сопоставляется с ролью, а пароль игнорируется.

Полная таблица «поверхность × метод» приведена в [Security Model](security.md#_15). MD5 не поддерживается; включайте TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) при работе через недоверенную сеть.

### Ограничения

- Только SQL. GraphQL и Cypher через pgwire не принимаются.
- Не только для чтения. `COPY ... FROM STDIN` вставляет строки в источники `postgresql`, `mysql`, `sqlite` и `mariadb`, поддерживается и DDL (см. ниже).
- DDL (`CREATE`, `ALTER`, `DROP`) поддерживается и направляется по пути Trino или прямому пути; новая таблица регистрируется в контексте компиляции и сразу доступна для запросов. `COPY ... TO STDOUT` (экспорт) и `COPY ... FROM STDIN` (импорт) поддерживаются в форматах `text` и `csv`.
- Запросы к `information_schema` и `pg_catalog` перехватываются и обслуживаются прослойкой каталога DuckDB — инструменты обнаружения схемы работают корректно.

---

## Драйвер JDBC

Драйвер JDBC Provisa использует Arrow Flight в качестве базового транспорта. Это рекомендуемый путь для BI-инструментов с мастером подключения JDBC.

### Подключение

Скачайте [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (всегда последний релиз) и добавьте его в путь драйверов вашего инструмента.

URL JDBC:

```yaml
jdbc:provisa://<host>:8815
```

Аутентификация использует стандартные свойства JDBC `user` / `password`. Provisa аутентифицирует учётные данные относительно настроенного провайдера аутентификации и назначает роль — клиент не выбирает роль самостоятельно.

### Настройка BI-инструментов

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

## Клиенты Arrow Flight

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

### Python (чистый PyArrow)

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
ticket никогда не является идентичностью. (REQ-1263) См. [Security Model](security.md#_15).

```python
ticket = flight.Ticket(json.dumps({
    "query": "SELECT id, amount FROM sales.orders",
    "token": "provisa_pat_...",
    "role": "analyst",
}).encode())
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

## Protobuf gRPC (порт 50051)

Путь для взаимодействия сервис–сервис. Provisa генерирует `.proto` для каждой роли при запуске — каждая роль видит только те таблицы и колонки, к которым у неё есть доступ.

Скачайте proto для своей роли:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Используйте `grpc_server_reflection` для программного обнаружения схемы.

Каждый RPC должен нести учётные данные в ключе метаданных `authorization` — токен провайдера или персональный токен доступа. `x-provisa-role` запрашивает роль из разрешённого набора идентичности; это не учётные данные и никогда ими не было. Клиентские сертификаты поддерживаются. См. [Security Model](security.md#_15).

Потоковые запросы выдают по одному сообщению на строку; мутации — унарные.

---

## Вызов команд через протоколы

**Команда (command)** — это зарегистрированная отслеживаемая функция или webhook: вызываемый
объект, зарегистрированный в семантическом слое Provisa, с полем `kind` (`query` или `mutation`) и
`impl_kind`, описывающим, как он выполняется. Каждая поверхность направляет вызовы через единый
управляемый исполнитель (`invoke_tracked_function`), который единообразно применяет `writable_by`
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

### Матрица протоколов

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

### Настройка

Включите федерацию в `config.yaml`:

```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa автоматически генерирует директивы `@key` на колонках первичного ключа и `@external`/`@provides` на межсубграфовых связях.

### Регистрация в Apollo Router

В вашем `supergraph.yaml`:

```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Запустите `rover supergraph compose --config supergraph.yaml`, чтобы сгенерировать схему supergraph.

### Сущности

Provisa отвечает на запросы `_entities` для межсубграфовых join. Любая таблица с первичным ключом автоматически становится разрешаемой как сущность (entity) Federation.

---

## Импорт Hasura v2 / DDN

О переходе с Hasura на Provisa см. [docs/import.md](import.md).

---

## Kafka

Настройку тем Kafka как таблиц только для чтения и как приёмников результатов запросов см. в [docs/sources.md](sources.md#kafka).

---

## Проверки качества данных (REQ-1443)

Soda Core и Great Expectations подключаются к Provisa так же, как любой другой postgres-клиент — через pgwire. Это и есть вся интеграция: чекер держит один драйвер postgres и сканирует федеративное представление, поэтому таблица Snowflake, таблица Iceberg и коллекция Mongo проверяются одним и тем же диалектом контрактов без отдельного чекера для каждой системы. [tool-verified: `provisa/events/source_loader.py` `make_dq_loader`]

Сканирование выполняется в дочернем интерпретаторе — `python -m provisa.dq.worker` — единственном месте, где импортируются `soda_core` или `great_expectations`. В процесс сервера ничего не линкуется, и падение чекера обрушивает подпроцесс, а не цикл событий. [tool-verified: `provisa/dq/runner.py` `build_command`]

Результаты сканирования попадают как обычные строки источника, поэтому периодичность, свежесть, события, происхождение, governance, RLS, сетка данных и экспорт применяются без второго механизма. Написание контрактов, конверт результата и производная регистрация описаны в [docs/sources.md](sources.md#req-1443).

### Установка чекера

Ни одна из библиотек не поставляется по умолчанию. Установщик спрашивает, какую вы хотите, и ответ становится `dq_checker: none|soda|gx` в `~/.provisa/config.yaml`. На уровне Docker `scripts/provisa` превращает это в аргумент сборки `PROVISA_EXTRAS`; на нативном уровне `first-launch.sh` устанавливает соответствующий extra pyproject в venv. [tool-verified: `scripts/provisa:69-79`, `packaging/linux/first-launch.sh` `_native_extras`]

| `dq_checker` | Библиотека | Лицензия | Размещённая облачная плоскость |
| -------------- | --------- | --------- | -------------------------- |
| `soda` | `soda-postgres` | Elastic License 2.0 | Отклонено (`cloud_eligible: false`) |
| `gx` | `great-expectations[postgresql]` | Apache 2.0 | Разрешено |

Elastic License 2.0 запрещает предоставление ПО третьим лицам как размещённой (hosted) услуги, а именно этим было бы выполнение Soda внутри плоскости SaaS от имени клиента. Размещённое развёртывание, которому нужен Soda, указывает на конечную точку Soda, которую оператор запускает самостоятельно. Ключи подключения см. в [docs/configuration.md](configuration.md#soda-great_expectations).

---

## Семантический обмен Apache Ossie (REQ-1316)

Provisa обменивается семантическими моделями с Apache Ossie (спецификация 0.2.0.dev0, incubating;
ранее Open Semantic Interchange) через адаптер границы. Внутренняя терминология Provisa никогда не
переименовывается в терминологию Ossie — спецификация объявляет обратно несовместимые изменения
вероятными, поэтому связанность ограничена адаптером. [tool-verified: `provisa/ossie/convert.py`
docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`, `provisa/ossie/convert.py` line 29]

### Экспорт

Каноническая поверхность экспорта — живая HTTP-конечная точка. Она выводит документ Ossie из живого
состояния при каждом запросе — без кеширования, без шага генерации.

```http
GET /admin/ossie
```

Ответ — YAML-документ с `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Страница Metrics также предлагает кнопку **Download** и копируемый URL конечной точки на панели
Ossie Interchange, оба указывают на ту же конечную точку.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Что экспортируется

Адаптер сопоставляет объекты Provisa с объектами Ossie следующим образом:

| Объект Provisa | Объект Ossie | Примечания |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; первичные/уникальные ключи берутся из конфигурации колонок и `UniqueConstraint` |
| `Column` | `field` | `expression` = ссылка на колонку (диалект ANSI_SQL); колонки времени получают `dimension.is_time: true` |
| `Relationship` | `relationship` | Алиас используется как имя, если задан; вычисляемые связи (с функцией-целью) пропускаются |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — без потерь по замыслу |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Только для round-trip; другие инструменты могут игнорировать |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governance, RLS, происхождение (lineage) и графовая семантика не экспортируются. Они могут
передаваться в необязательном слоте `provisa` custom_extensions для точности round-trip, но обмен
никогда не зависит от того, что другие инструменты его прочитают. [tool-verified:
`provisa/ossie/convert.py` docstring lines 13–15]

Неизвестные типы колонок Provisa проходят как есть; адаптер никогда не сопоставляет их молча с
неверным типом. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown
types pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Сопоставление типов

[tool-verified: `_DATATYPE_MAP`, `provisa/ossie/convert.py` lines 35–65]

| Тип Provisa / источника | `datatype` Ossie |
| --- | --- |
| `varchar`, `text`, `char`, `uuid`, `string` | `string` |
| `int`, `integer`, `bigint`, `smallint`, `int4`, `int8`, `tinyint` | `integer` |
| `numeric`, `decimal`, `float`, `double`, `real` | `number` |
| `bool`, `boolean` | `boolean` |
| `date` | `date` |
| `time` | `time` |
| `timestamp`, `timestamptz`, `datetime` | `timestamp` |
| всё остальное | проходит как есть |

### Импорт

Импорт принимает документ Ossie (YAML или JSON) и возвращает предложения на регистрацию. Ничего не
регистрируется автоматически — импортированные определения никогда не обходят шаг проверки.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Сервер разбирает документ функцией `parse_ossie_model`, которая валидирует структуру и возвращает
dataclass `OssieImport`, содержащий предложенные таблицы, связи и метрики как обычные словари. Любая
структурная проблема — это `400` с ошибкой, именующей путь, например
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### Экран проверки

В UI кнопка **Import** (страница Metrics → панель Ossie Interchange) открывает выбор файла. После
того как документ отправлен и разобран, открывается модальное окно проверки со всеми предложенными
таблицами, связями и метриками, перечисленными как отмеченные элементы. Специалист по моделированию
может снять отметку с любого пункта, чтобы исключить его. Нажатие **Apply** регистрирует отмеченные
элементы через существующие мутации регистрации — сначала таблицы, затем связи (которые ссылаются
на таблицы), затем метрики. [tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen
opens with everything checked; trimming = unchecking"; "Tables first, then relationships... then
metrics — each through the EXISTING registration mutations (REQ-1316)"]

Роль моделирования и история, сохранённые в экспортированном Provisa документе Ossie, корректно
проходят round-trip через импорт. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Метрики по всем протоколам (REQ-1319)

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

### MCP (AI-агенты)

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

### Arrow Flight

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

### Запросы на естественном языке

Сопоставитель схем NL разрешает словарь метрик в вопросах на естественном языке напрямую в метрику
плюс измерения, затем генерирует семантический SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Таблицы фактов помечаются `[fact]` в подсказке NL; таблицы измерений помечаются `[dimension]`.
Сопоставитель смещает пути join от факта к измерению при разрешении вопросов.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Потоковая передача

Комбинируйте `view_metrics` с `materialize` и приёмником Kafka, чтобы получить push-вывод метрик
при изменении, используя существующий механизм материализации. Новый конвейер не требуется.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Наблюдаемость (OTel)

Вычисления метрик трассируются и экспортируются как метрики OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
