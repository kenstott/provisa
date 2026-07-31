# Интеграции

## Выбор пути подключения

| Тип клиента | Рекомендуемый путь | Почему |
|-------------|-----------------|-----|
| BI-инструменты (Tableau, Power BI, Looker) | JDBC | Колоночная потоковая передача Arrow Flight по проводу; BI-инструменты имеют встроенный мастер JDBC и выигрывают от высокопроизводительной колоночной доставки для больших наборов результатов |
| psql, DBeaver, любой PG-совместимый инструмент | pgwire (нативный драйвер PG) | Подключение без трения по умолчанию — не нужен специальный драйвер; используйте то, что у вас уже есть |
| Python data stack (pandas, pyarrow) | `provisa-client` или чистый ADBC | Потоковые пакеты Arrow; отсутствие накладных расходов на сериализацию строк |
| Spark, DuckDB, высокопроизводительные конвейеры | Arrow Flight (ADBC) | Неограниченная колоночная потоковая передача напрямую в память Arrow |
| Взаимодействие сервис-сервис (типизированные контракты) | Protobuf gRPC | Сгенерированный на роль proto; потоковые строки; типобезопасность |
| Веб-приложения, скрипты | HTTP (`/data/graphql`, `/data/sql`) | Без драйвера; стандартный HTTP; полный выбор языка запросов |
| REST-клиенты (стандарт JSON:API) | `GET /data/jsonapi/{table}` | Конверт JSON:API v1.0; разреженные наборы полей, пагинация, фильтрация через параметры запроса; без драйвера |

---

## pgwire — нативный драйвер PostgreSQL

Provisa реализует протокол проводного взаимодействия PostgreSQL (версия протокола 3.0). Любой клиент, говорящий на PostgreSQL, подключается без специального драйвера.

Включается установкой `PROVISA_PGWIRE_PORT` (например, `5433`) перед запуском Provisa. Отключён, если не задан или равен `0`.

### Почему pgwire вместо JDBC?

Драйвер JDBC использует Arrow Flight в качестве транспорта и требует развёртывания `provisa-jdbc.jar`. pgwire не требует ничего — если у вас уже есть `psql`, DBeaver, SQLAlchemy или PG JDBC-драйвер, всё готово. Это путь с меньшим трением для рабочих нагрузок только на SQL.

JDBC — правильный выбор для BI-инструментов со встроенным мастером подключения JDBC, выигрывающих от колоночной потоковой передачи Arrow Flight для больших наборов результатов. pgwire принимает свободный SQL к полной опубликованной схеме — те же запросы, меньшая стоимость настройки.

### psql

```bash
psql -h localhost -p 5433 -U alice
```

### DBeaver

1. New Connection → PostgreSQL
2. Host: `localhost`, Port: `5433`
3. Имя пользователя / пароль как настроено в Provisa
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

pgwire использует аутентификацию по паролю в открытом виде, мостируемую к настроенному провайдеру аутентификации Provisa (`none` или `simple`). В режиме доверия (`none`) имя пользователя напрямую отображается в роль — пароль игнорируется. MD5 не поддерживается; включите TLS (`PROVISA_PGWIRE_CERT` / `PROVISA_PGWIRE_KEY`) при работе через недоверенную сеть.

### Ограничения

- Только SQL. GraphQL и Cypher не принимаются через pgwire.
- Не только для чтения. `COPY ... FROM STDIN` вставляет строки в источники `postgresql`, `mysql`, `sqlite` и `mariadb`, а DDL поддерживается (см. ниже).
- DDL (`CREATE`, `ALTER`, `DROP`) поддерживается и диспетчеризуется в путь Trino или прямой путь; новая таблица регистрируется в контексте компиляции и немедленно доступна для запросов. `COPY ... TO STDOUT` (экспорт) и `COPY ... FROM STDIN` (импорт) поддерживаются в форматах `text` и `csv`.
- Запросы к `information_schema` и `pg_catalog` перехватываются и обрабатываются через прослойку каталога DuckDB — инструменты обнаружения схемы работают корректно.

---

## Драйвер JDBC

Драйвер Provisa JDBC использует Arrow Flight в качестве базового транспорта. Это рекомендуемый путь для BI-инструментов с мастером подключения JDBC.

### Подключение

Скачайте [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (всегда последний релиз) и добавьте его в путь драйвера вашего инструмента.

URL JDBC:
```
jdbc:provisa://<host>:8815
```

Аутентификация использует стандартные свойства JDBC `user` / `password`. Provisa аутентифицирует учётные данные по настроенному провайдеру аутентификации и назначает роль — клиент не выбирает свою роль сам.

### Настройка BI-инструментов

**Tableau**
1. Manage → Drivers → Install Provisa JDBC
2. Connect → Other Databases (JDBC)
3. URL: `jdbc:provisa://localhost:8815`
4. Введите имя пользователя и пароль при запросе

**DBeaver** (путь JDBC — для пути pgwire см. выше)
1. Database → New Connection → JDBC
2. Драйвер: добавьте `provisa-jdbc.jar`
3. URL: `jdbc:provisa://localhost:8815`
4. Введите имя пользователя и пароль на вкладке Authentication

**Power BI** — используйте шлюз ODBC с мостом Provisa JDBC-ODBC (включён в установщик).

---

## Клиенты Arrow Flight

Arrow Flight (порт 8815) — рекомендуемый путь для инструментов данных, которые его поддерживают. Результаты передаются потоком как Arrow RecordBatches без материализации в памяти Provisa.

### Python (`provisa-client`)

Рекомендуемый путь Python — оборачивает как GraphQL, так и Arrow Flight:

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

Полный справочник, включая DB-API 2.0, диалект SQLAlchemy и ADBC, см. в [docs/python-client.md](python-client.md).

### Python (чистый PyArrow)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "SELECT id, amount FROM sales.orders"}')
df = client.do_get(ticket).read_all().to_pandas()
```

Билет не несёт роли. Сервер назначает роль от настроенного провайдера аутентификации. Там, где выбор роли разрешён, передавайте её в метаданных вызова gRPC под ключом `x-provisa-role` (например, `flight.FlightCallOptions(headers=[(b"x-provisa-role", b"analyst")])`), а не в JSON билета.

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

Путь взаимодействия сервис-сервис. Provisa генерирует `.proto` для каждой роли при запуске — каждая роль видит только те таблицы и столбцы, к которым у неё есть доступ.

Скачайте proto для своей роли:

```bash
curl http://localhost:8001/proto/analyst > provisa_analyst.proto
```

Используйте `grpc_server_reflection` для программного обнаружения схемы.

Роль передаётся через ключ метаданных `x-provisa-role` в каждом RPC. Потоковые запросы выдают одно сообщение на строку; мутации унарны.

---

## Вызов команд через протоколы

**Команда** — это зарегистрированная отслеживаемая функция или webhook — вызываемый объект, зарегистрированный в семантическом слое Provisa с `kind` (`query` или `mutation`) и `impl_kind`, описывающим способ выполнения. Каждая поверхность направляет вызовы через единый управляемый исполнитель (`invoke_tracked_function`), который единообразно применяет `writable_by` и governance (REQ-1156). [tool-verified: `provisa/api/data/action_exec.py`, `provisa/bolt/session.py:786-791`, `provisa/grpc/server.py:107-135`, `provisa/pgwire/function_call.py:80-88`, `provisa/api/flight/server.py:542-554`]

| `impl_kind` | Что выполняется | Поля привязки |
|------------|-----------|---------------|
| `source_procedure` | Хранимая процедура на зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Серверный скрипт | `script` |
| `http` | Исходящий вызов HTTP | `url`, `method` |
| `grpc` | Исходящий вызов gRPC к внешнему серверу | `target`, `method` |
| `python` | Python-вызываемый объект, размещённый Provisa (REQ-885) | `callable` (например, `demo.py_functions:random_dataset`) |

Когда команда объявляет `return_schema` (JSON Schema с `type: array, items: object`), она возвращает набор — каждая поверхность проецирует её как типизированный набор строк. Демонстрационные команды `random_python_set` (impl_kind `python`) и `random_grpc_set` (impl_kind `grpc`) иллюстрируют как размещённый вызываемый объект, так и внешний мост gRPC, возвращающий строки со случайными значениями; обе зарегистрированы в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

### Матрица протоколов

| Поверхность | Синтаксис | Пример |
|---------|--------|---------|
| GraphQL | `kind=query` → поле Query; `kind=mutation` → поле Mutation; с доменным префиксом при `domain_prefix: true` | `{ ps__random_python_set(rows: 5, seed: 42) { id region amount } }` |
| pgwire / Arrow Flight / MCP `run_sql` | `SELECT * FROM fn(args)` или `SELECT fn(args)` | `SELECT * FROM random_python_set(5, 42)` |
| Cypher HTTP (`POST /data/cypher`) | `CALL fn(args) YIELD cols` | `CALL random_python_set(5, 42) YIELD id, region, amount` |
| Bolt (Neo4j Browser / драйвер) | `CALL fn(args)` — позиционные аргументы отображаются на объявленные имена аргументов | `CALL random_python_set(3, 7)` |
| Provisa gRPC (порт 50051) | Унарный `CallCommand(CommandRequest{name, args_json})` → `CommandResponse{rows_json}` | `grpcurl -d '{"name":"random_python_set","args_json":"{\"rows\":5}"}' ... ProvisaService/CallCommand` |

Поле `kind` управляет только размещением в GraphQL — поверхности SQL, Cypher, Bolt и gRPC принимают команды `query` и `mutation` одинаково.

---

## Apollo Federation

Provisa может выступать в роли подграфа Federation v2, предоставляя свою опубликованную схему Apollo Router или Apollo Gateway.

### Настройка

Включите федерацию в `config.yaml`:
```yaml
federation:
  enabled: true
  subgraph_name: provisa-data
```

Provisa автоматически генерирует директивы `@key` на столбцах первичного ключа и `@external`/`@provides` на межподграфовых связях.

### Регистрация с Apollo Router

В вашем `supergraph.yaml`:
```yaml
subgraphs:
  provisa-data:
    routing_url: http://provisa:8001/data/graphql
    schema:
      subgraph_url: http://provisa:8001/data/graphql
```

Запустите `rover supergraph compose --config supergraph.yaml` для генерации схемы суперграфа.

### Сущности

Provisa отвечает на запросы `_entities` для межподграфовых соединений. Любая таблица с первичным ключом автоматически разрешима как сущность Federation.

---

## Импорт из Hasura v2 / DDN

Об миграции с Hasura на Provisa см. [docs/import.md](import.md).

---

## Kafka

О конфигурации топиков Kafka как таблиц только для чтения и приёмников результатов запросов см. [docs/sources.md](sources.md#kafka).

---

## Семантический обмен Apache Ossie (REQ-1316)

Provisa обменивается семантическими моделями с Apache Ossie (спецификация 0.2.0.dev0, incubating; ранее Open
Semantic Interchange) через адаптер границы. Внутренний словарь Provisa никогда не переименовывается
под словарь Ossie — спецификация объявляет ломающие изменения вероятными, поэтому связывание ограничено адаптером.
[tool-verified: `provisa/ossie/convert.py` docstring lines 7–16; `OSSIE_VERSION = "0.2.0.dev0"`,
`provisa/ossie/convert.py` line 29]

### Экспорт

Канонической поверхностью экспорта является живой HTTP-эндпоинт. Он выводит документ Ossie из живого состояния
при каждом запросе — без кеширования, без этапа генерации.

```
GET /admin/ossie
```

Ответ представляет собой YAML-документ с `Content-Disposition: attachment; filename=provisa.ossie.yaml`.
[tool-verified: `ossie_router.py` lines 20–33: "THE canonical live Ossie endpoint: the semantic
model derived from live state on every read — no caching, no regeneration step"]

Страница Metrics также предлагает кнопку **Download** и копируемый URL эндпоинта на панели Ossie
Interchange, оба указывают на один и тот же эндпоинт.
[tool-verified: `OssieInterchangePanel.tsx` lines 64–79: `endpointUrl = window.location.origin + OSSIE_ENDPOINT_PATH`]

#### Что экспортируется

Адаптер отображает объекты Provisa на объекты Ossie следующим образом:

| Объект Provisa | Объект Ossie | Примечания |
| --- | --- | --- |
| `Table` | `dataset` | `source` = `catalog.schema.table`; первичные/уникальные ключи из конфигурации столбцов и `UniqueConstraint` |
| `Column` | `field` | `expression` = ссылка на столбец (диалект ANSI_SQL); временные столбцы получают `dimension.is_time: true` |
| `Relationship` | `relationship` | Алиас используется как имя, если задан; вычисляемые связи (с целью-функцией) пропускаются |
| `Metric` | `metric` | `name`, `expression` (ANSI_SQL), `datatype`, `description`, `ai_context` — без потерь по замыслу |
| `modeling_role` / `modeling_history` | `custom_extensions[].vendor_name="provisa"` | Только для round-trip; другие инструменты могут игнорировать |

[tool-verified: `_table_to_dataset`, `build_ossie_model`, `provisa/ossie/convert.py` lines 90–198;
`_table_to_dataset` comment at line 153: "Computed (function-target) relationships have no dataset
target — not representable in Ossie; skipping is the defined export boundary"]

Governance, RLS, происхождение (lineage) и графовая семантика не экспортируются. Они могут перемещаться в опциональном
слоте `provisa` custom_extensions для round-trip-верности, но обмен никогда не полагается на то, что другие
инструменты его читают. [tool-verified: `provisa/ossie/convert.py` docstring lines 13–15]

Неизвестные типы столбцов Provisa проходят без изменений; адаптер никогда не отображает их молча на неверный
тип. [tool-verified: `_map_datatype`, `provisa/ossie/convert.py` lines 70–77: "Unknown types
pass through verbatim — mapping silently to a wrong type would corrupt the model"]

#### Отображение типов

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
| всё остальное | проходит без изменений |

### Импорт

Импорт принимает документ Ossie (YAML или JSON) и возвращает предложения регистрации. Ничего не
регистрируется автоматически — импортированные определения никогда не обходят этап проверки.

```http
POST /admin/ossie/import
Content-Type: text/yaml   (or application/json)

<ossie document>
```

Сервер разбирает документ с помощью `parse_ossie_model`, который проверяет структуру и возвращает
датакласс `OssieImport`, содержащий предлагаемые таблицы, связи и метрики как обычные словари.
Любая структурная проблема — это `400` с ошибкой, именованной по пути, например:
`ossie import: missing semantic_model[0].datasets[1].source`.
[tool-verified: `import_ossie`, `provisa/api/admin/ossie_router.py` lines 36–52:
"Nothing is registered here — imported definitions never bypass registration review"]

#### Экран проверки

В UI кнопка **Import** (страница Metrics → панель Ossie Interchange) открывает выбор файла.
После того как документ отправлен и разобран, открывается модальное окно проверки со всеми предложенными таблицами,
связями и метриками, перечисленными как отмеченные элементы. Разработчик модели может снять отметку с любого элемента, чтобы исключить его.
Нажатие **Apply** регистрирует отмеченные элементы через существующие мутации регистрации — сначала таблицы,
затем связи (которые ссылаются на таблицы), затем метрики.
[tool-verified: `OssieInterchangePanel.tsx` lines 88–165: "Review screen opens with everything
checked; trimming = unchecking"; "Tables first, then relationships... then metrics — each through
the EXISTING registration mutations (REQ-1316)"]

Роль моделирования и история, хранящиеся в экспортированном Provisa документе Ossie, корректно проходят round-trip
через импорт. [tool-verified: `_parse_dataset` custom_extensions handling,
`provisa/ossie/convert.py` lines 287–300: "REQ-1320: round-trip the provisa modeling metadata slot"]

---

## Метрики через протоколы (REQ-1319)

Определение управляемой метрики — её выражение, описание и `ai_context` — перемещается вместе со
значением в каждую поверхность запроса через одно расширение компилятора. Копий не существует. Компилятор
резервирует схему `metrics` для доступа SQL; каждый протокол затем добавляет свой собственный канал метаданных.

[tool-verified: `METRICS_SCHEMA = "metrics"`, `provisa/compiler/metric_expand.py` line 43;
REQ-1319 requirement text: "the definition (description, ai_context) travels with the value
everywhere, with no copies"]

### SQL / pgwire

Обращайтесь к любой метрике как к виртуальному отношению в схеме `metrics`. Столбцы измерений, которые вы выбираете,
становятся GROUP BY:

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

Компилятор расширяет форму `metrics.<name>` до реального сгруппированного агрегата перед запуском governance.
Описания столбцов представлены как записи `pg_description`, поэтому DBeaver и psql `\d+`
показывают их. [tool-verified: `metric_semantic_sql`, `provisa/compiler/metric_expand.py` lines 52–70;
REQ-1319: "description surfaced via pg_description"]

`SELECT *` отклоняется — называйте столбцы явно.
[tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

### GraphQL

Метрики проецируются внутри корневого поля `_aggregate` как блок `metrics`.
[inferred: per REQ-1319; aggregate_gen.py not read in this session]

Текст определения (`description`, `ai_context`) появляется в документации интроспекции GraphQL, поэтому
инструменты, чувствительные к схеме, и генераторы кода подхватывают его автоматически.
[inferred: per REQ-1319: "definition in introspection docs"]

### MCP (AI-агенты)

Два инструмента предоставляют метрики клиентам MCP:

- **`list_metrics`** — возвращает все управляемые метрики, видимые сессии, с `name`,
  `description` и `ai_context`.
- **`query_metric`** — принимает имя метрики плюс список измерений и вызывает путь семантического SQL
  компилятора, возвращая агрегированный результат.

[inferred: per REQ-1319: "MCP: list_metrics and query_metric tools carrying ai_context, so agents
select governed meanings instead of composing aggregation SQL"; `provisa/api/mcp/tools.py` not
read in this session]

Агенты, вызывающие `list_metrics` перед построением запроса, выбирают управляемую метрику по имени,
а не пишут агрегирующий SQL вручную. Поле `ai_context` — это место для текста определения, направляющего
правильный выбор.

### Arrow Flight

Метрики адресуемы как дескрипторы flight метрик, возвращающие таблицы Arrow.
[inferred: per REQ-1319: "Arrow Flight: metric flight descriptors returning Arrow tables";
`provisa/api/flight/catalog.py` not read in this session]

Используйте ту же форму SQL `metrics.<name>` через стандартный путь билета Flight SQL.

### Bolt / Cypher (Neo4j Browser)

Вызывайте метрику с помощью процедуры `provisa.metric()`:

```cypher
CALL provisa.metric('net_revenue', ['region']) YIELD region, value
```

[inferred: per REQ-1319: "Bolt/Cypher: a provisa.metric() procedure"; the procedure signature
is inferred from the REQ text and not verified against provisa/bolt/session.py in this session]

Таблицы Fact и Dimension несут метки узлов `:Fact` и `:Dimension` в федеративном графе, поэтому
Bloom автоматически отображает форму звезды.
[inferred: per REQ-1319 and REQ-1320: "федеративный граф помечает узлы :Fact/:Dimension so Bloom
renders the star"; provisa/cypher/label_map.py not read in this session]

### Запросы на естественном языке

Сопоставитель схемы NL разрешает словарь метрик в вопросах на естественном языке напрямую в метрику
плюс измерения, затем генерирует семантический SQL. [tool-verified: `resolve_metric`,
`provisa/nl/schema_matcher.py` is exercised in `test_nl_metrics.py` lines 76–78:
`sql = matcher.resolve_metric("What is the total revenue by region?")` →
`"SELECT region, value FROM metrics.total_revenue GROUP BY region"`]

Таблицы фактов помечены `[fact]` в подсказке NL; таблицы измерений помечены `[dimension]`.
Сопоставитель смещает пути соединения от факта к измерению при разрешении вопросов.
[tool-verified: `test_format_entities_tags_star_roles`, `tests/unit/test_nl_metrics.py` lines 129–132:
`assert "table: orders [fact]  fields: amount" in block`]

### Потоковая передача

Комбинируйте `view_metrics` с `materialize` и приёмником Kafka для получения выхода метрик push-on-change,
используя существующий механизм материализации. Новый конвейер не требуется.
[inferred: per REQ-1319: "Streaming: view_metrics + materialize + Kafka sink yields push-on-change
metrics from existing machinery"; implementation not verified beyond the requirement text]

### Наблюдаемость (OTel)

Вычисления метрик трассируются и экспортируемы как метрики OpenTelemetry.
[inferred: per REQ-1319: "Observability: metric evaluations traced and exportable as OTel metrics";
OTel integration code not read in this session]
