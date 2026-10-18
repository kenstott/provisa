# Python-клиент (`provisa-client`) {: #python-client-provisa-client }

Python-клиент для Provisa. Предоставляет четыре интерфейса:

| Интерфейс | Сценарий использования |
| ----------- | ---------- |
| `ProvisaClient` | Запросы GraphQL, Arrow Flight, вывод DataFrame |
| DB-API 2.0 (`connect`) | Стандартный интерфейс базы данных Python (PEP 249) (REQ-268) |
| Диалект SQLAlchemy | BI-инструменты, ORM, Pandas `read_sql` (REQ-270) |
| ADBC | Нативная колоночная потоковая передача Arrow через Flight (REQ-271) |

## Установка {: #install }

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Быстрый старт {: #quick-start }

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    token="provisa_pat_...",   # personal access token, or a provider bearer token
    role="analyst",
)
```

`ProvisaClient` принимает учётные данные, а не имя пользователя с паролем: собственного шага входа у него нет. Персональный токен доступа — это те учётные данные, к которым стоит обратиться, когда скрипт должен работать без присмотра: он выпускается из профиля самого пользователя, имеет срок действия и отзывается, не затрагивая учётную запись. (REQ-1263) Bearer-токен провайдера работает точно так же. И тот и другой передаются в `token`, и клиент предъявляет его как на пути HTTP, так и на пути Arrow Flight.

Чтобы обменять пароль на токен, отправьте POST на `/auth/login` и прочитайте `access_token`:

```python
import httpx

body = httpx.post(
    "http://localhost:8001/auth/login",
    json={"username": "alice", "password": "secret"},
).json()
client = ProvisaClient("http://localhost:8001", token=body["access_token"])
```

Точки входа DB-API и ADBC выполняют этот обмен за вас — см. ниже.

### Запросы GraphQL {: #graphql-queries }

```python
# Raw response dict
result = client.query("{ orders { id amount region } }")

# With variables
result = client.query(
    "query Q($region: String!) { orders(region: $region) { id amount } }",
    variables={"region": "west"},
)

# pandas DataFrame (first root field is flattened)
df = client.query_df("{ orders { id amount region } }")
```

### Асинхронность {: #async }

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (высокая пропускная способность, колоночный формат) {: #arrow-flight-high-throughput-columnar }

Используйте Flight для больших наборов результатов — данные передаются потоком как пакеты записей Arrow без материализации на сервере. (REQ-143, REQ-145)

```python
import pyarrow as pa

table: pa.Table = client.flight("{ orders { id amount region } }")
df = client.flight_df("{ orders { id amount region } }")
```

Flight по умолчанию подключается к порту 8815. (REQ-143) Переопределите через `flight_port=`:

```python
client = ProvisaClient("http://prod.example.com", flight_port=8815)
```

### Исследование каталога {: #catalog-exploration }

```python
tables_df = client.list_tables()
```

### Справочник подключения {: #connection-reference }

| Параметр | По умолчанию | Описание |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Базовый URL сервера Provisa |
| `token` | `None` | Bearer-учётные данные — токен провайдера или персональный токен доступа; опустите для аутентификации по паролю (REQ-606, REQ-1263) |
| `role` | `None` | Роль, от имени которой действовать; передаётся как `X-Provisa-Role` и учитывается, только если назначена пользователю; если не указана, запрос выполняется от собственной роли идентичности (REQ-273) |
| `flight_port` | `8815` | Порт gRPC Arrow Flight (REQ-143) |

### Обработка ошибок {: #error-handling }

`query()` выбрасывает `httpx.HTTPStatusError` при ошибках HTTP. (REQ-607)
`query_df()` выбрасывает `RuntimeError`, если ответ содержит ошибки GraphQL. (REQ-607)

---

## Provisa в Jupyter {: #using-provisa-in-jupyter }

Установите пакет в работающее ядро до импорта. Дополнение `pandas` даёт вывод в DataFrame; добавьте `sqlalchemy`, если также нужен `pandas.read_sql` через диалект.

```python
%pip install "provisa-client[pandas]"
```

### Учётные данные {: #credentials }

Блокноты сохраняют вывод ячеек в файл `.ipynb`. Токен, введённый в ячейку, будет сохранён на диск вместе с вашим кодом. Не делайте так.

`ProvisaClient` не читает окружение: передайте ему учётные данные явно, из переменных окружения. (`provisa_client.query()`, ниже, читает их за вас.)

```python
import os
from provisa_client import ProvisaClient

client = ProvisaClient(
    os.environ["PROVISA_URL"],           # choose your own variable name
    token=os.environ["PROVISA_TOKEN"],   # personal access token
    role=os.environ.get("PROVISA_ROLE"), # optional; omit to run as your own role
    org=os.environ.get("PROVISA_ORG"),   # required on a multi-tenant deployment; None otherwise
)
```

Задайте эти переменные в оболочке до запуска Jupyter или загрузите файл `.env` через `python-dotenv`. В любом случае держите их вне блокнота.

### Один вызов: `query()` {: #one-call-query }

`provisa_client.query()` принимает инструкцию SQL, GraphQL или Cypher и возвращает DataFrame pandas. Сервер сам определяет язык. Заранее ничего не создаётся: соединение берётся из окружения или из файла профиля.

```python
import provisa_client

df = provisa_client.query("SELECT region, amount FROM shop.orders")
admin_view = provisa_client.query("SELECT region, amount FROM shop.orders", role="org_admin")
```

`role=` выполняет одно чтение от имени этой роли и отправляется как `X-Provisa-Role`. Сервер принимает его только для роли, которая у вас есть. Чтение идёт через Arrow Flight; задайте `transport` равным `http`, чтобы отправить его на `/data/query`. `await provisa_client.aquery(...)` — асинхронная форма.

Соединение читается из первого места, где названа каждая настройка:

1. ключевые аргументы `provisa_client.sign_in(...)`, который один раз разрешает соединение для всех последующих чтений;
2. переменные окружения: `PROVISA_URL`, `PROVISA_TOKEN`, `PROVISA_USERNAME`, `PROVISA_PASSWORD`, `PROVISA_ROLE`, `PROVISA_ORG`, `PROVISA_FLIGHT_PORT`, `PROVISA_TRANSPORT`;
3. раздел файла `~/.provisa/credentials`: `[default]` или тот, что назван в `PROVISA_PROFILE` (или `sign_in(profile=...)`).

```ini
[default]
url = https://provisa.example.com
token = provisa_pat_...
role = analyst

[staging]
url = https://staging.provisa.example.com
username = ana
```

`token` — это персональный токен доступа или токен сеанса. Укажите вместо него `username` и `password`, и клиент обменяет их на токен сеанса через `POST /auth/login`. Это работает в развёртывании, чей провайдер входа принимает пароль (basic, simple, LDAP). Храните пароль в `PROVISA_PASSWORD`, а не в файле. Отклонённый вход, отсутствующий URL или названный профиль, которого нет в файле, вызывают ошибку с указанием причины. У Provisa нет процесса входа, которым блокнот мог бы управлять через браузер, поэтому в развёртываниях с единым входом используйте персональный токен доступа.

### Магические команды ячеек {: #cell-magics }

```python
%pip install "provisa-client[notebook]"
%load_ext provisa_client.magics
```

```text
%%sql eu --role analyst_eu
SELECT region, customer_email FROM shop.orders
```

`%%sql`, `%%graphql` и `%%cypher` отвечают на ячейку через `query()`. DataFrame — результат ячейки. Имя после магической команды также связывает его в блокноте (`eu` выше); `--role` выполняет ячейку от имени этой роли.

JupySQL (`%load_ext sql`) тоже определяет `%%sql`. Загрузите оба — и `%%sql` достанется расширению, загруженному последним. Расширение Provisa принимает `--role` и читает через Arrow Flight. JupySQL тоже работает, через диалект SQLAlchemy: `%sql provisa+http://user:password@host:port` подключается, `%sql`/`%%sql` выполняют управляемые инструкции, а `%sqlcmd tables -s <domain>` и `%sqlcmd columns -t <table> -s <domain>` выводят каталог.

### В браузере (Pyodide) {: #in-the-browser-pyodide }

В Pyodide `query()` читает с сервера, который отдал страницу, от имени пользователя, вошедшего на странице. Страница-хост хранит этого пользователя в глобальной области ядра:

```javascript
self.provisaAuth = { baseUrl: "", role: "analyst_eu", bearer: "<token>", org: null };
```

`baseUrl` пуст для собственного источника страницы. Клиент читает `provisaAuth` при каждом запросе, поэтому обновлённый токен действует без перезапуска. Результаты приходят в Arrow, когда загружен `pyarrow`; без него — в JSON. Инструкция Cypher всегда возвращается в JSON. `query()` блокирует выполнение, что может Web Worker (где работает ядро блокнота); в основном потоке страницы он вызывает ошибку, и нужно использовать `await provisa_client.aquery(...)`.

### DataFrame из GraphQL, SQL и Cypher {: #dataframes-from-graphql-sql-and-cypher }

`query_df()` принимает все три языка запросов и возвращает DataFrame pandas.

```python
# GraphQL — first root field is extracted automatically
df = client.query_df("{ orders { id amount region } }")

# SQL
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Cypher
df = client.query_df("MATCH (o:Order) RETURN o.id, o.amount, o.region")
```

Ответы GraphQL несут данные под именем корневого поля; `query_df()` извлекает их. Ответы SQL и Cypher содержат явные имена столбцов от сервера и отображаются напрямую.

### Arrow Flight для больших выборок {: #arrow-flight-for-large-pulls }

`flight_df()` передаёт данные потоком пакетов записей Arrow вместо десериализации JSON. Для широких таблиц или результатов более чем в несколько сотен тысяч строк это быстрее и требует меньше памяти.

```python
df = client.flight_df("SELECT * FROM events")
```

Arrow сохраняет типы столбцов: целые остаются целыми, метки времени — метками времени. JSON — нет. Одного этого достаточно, чтобы предпочесть Flight, когда важна схема.

Flight по умолчанию подключается к порту 8815. Передайте `flight_port=` в конструктор `ProvisaClient`, если ваш сервер использует другой порт.

### `pandas.read_sql` через диалект SQLAlchemy {: #pandasread_sql-through-the-sqlalchemy-dialect }

Установите дополнение `sqlalchemy` в ядро:

```python
%pip install "provisa-client[sqlalchemy]"
```

Затем создайте engine с URL `provisa+http://`. Храните учётные данные в переменных окружения, а не в URL.

```python
import os
import pandas as pd
from sqlalchemy import create_engine

host = os.environ["PROVISA_HOST"]       # e.g. "localhost:8001"
user = os.environ["PROVISA_USER"]
pwd  = os.environ["PROVISA_PASSWORD"]

engine = create_engine(f"provisa+http://{user}:{pwd}@{host}")

df = pd.read_sql("SELECT id, amount FROM orders", engine)
```

Диалект отправляет имя пользователя и пароль на `/auth/login` и хранит полученный токен всё время жизни engine — учётные данные не передаются заново при каждом запросе.

---

## DB-API 2.0

Стандартный интерфейс [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Работает с любым инструментом, принимающим подключение DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="analyst",     # optional; omit to run as the role the login returns
)
```

`connect` отправляет имя пользователя и пароль POST-запросом на `/auth/login` и сохраняет полученный `access_token`, поэтому соединение несёт настоящие учётные данные, а не имя. `role` *запрашивает* роль, и сервер удовлетворяет запрос лишь тогда, когда роль назначена этой личности (REQ-273); если параметр опущен, соединение работает под ролью, которую разрешил вход.

### Выполнение запросов {: #executing-queries }

Курсор принимает либо GraphQL, либо SQL — определяется автоматически. (REQ-268, REQ-274)

```python
cur = conn.cursor()

# GraphQL
cur.execute("{ orders { id amount region } }")
rows = cur.fetchall()           # list of tuples
one  = cur.fetchone()           # single tuple or None
many = cur.fetchmany(size=50)   # up to N tuples

# SQL (routed through Stage 2 governance)
cur.execute("SELECT id, amount FROM orders WHERE region = 'west'")
rows = cur.fetchall()
```

### Метаданные столбцов {: #column-metadata }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Именованные параметры {: #named-parameters }

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Контекстные менеджеры {: #context-managers }

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Диалект SQLAlchemy {: #sqlalchemy-dialect }

```bash
pip install "provisa-client[sqlalchemy]"
```

Схема URL: `provisa+http://` или `provisa+https://` (REQ-270)

```python
from sqlalchemy import create_engine, text

engine = create_engine("provisa+http://alice:secret@localhost:8001")

with engine.connect() as conn:
    result = conn.execute(text("{ orders { id amount region } }"))
    for row in result:
        print(row)
```

### С pandas {: #with-pandas }

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Параметры URL {: #url-parameters }

| Параметр | Описание | По умолчанию |
| ----------- | ------------- | --------- |
| `role` | Запрашиваемая роль; проверяется на сервере (REQ-273) | роль, которую разрешает вход |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Интроспекция схемы {: #schema-introspection }

Диалект отражает каталог, который отдаётся вашей роли, читая его из `GET /data/catalog` от имени пользователя, роли и организации соединения. Каждый домен — это схема, и каждая таблица находится в одной из них: `get_schema_names()` выводит домены, `get_table_names(schema="shop")` — таблицы этого домена по их SQL-именам, а `get_columns("orders", schema="shop")` даёт тип, допустимость NULL и описание каждого столбца. Без указанной схемы `get_table_names()` не выводит ни одной. Неизвестная таблица вызывает `NoSuchTableError`; каталог, в котором отказывает сервер, вызывает ошибку. (REQ-363, REQ-270, REQ-2070)

---

## ADBC

Arrow Database Connectivity на базе Arrow Flight. (REQ-271) Возвращает `pyarrow.Table` напрямую — без десериализации JSON. (REQ-271)

```bash
pip install "provisa-client[adbc]"
```

```python
from provisa_client.adbc import adbc_connect

conn = adbc_connect(
    "http://localhost:8001",
    user="alice",
    password="secret",
    role="analyst",   # optional; server validates the requested role
    port=8815,        # Arrow Flight port (REQ-711)
)
```

`adbc_connect` сначала выполняет вход по HTTP и помещает полученный токен в каждый Flight-тикет, поэтому Flight-сервер аутентифицирует соединение так же, как это делает REST-поверхность. (REQ-1263) Аргумент `role` — это запрос, проверяемый на сервере по назначениям личности; он никогда не становится самой личностью. (REQ-273)

### Получение как таблицы Arrow {: #fetch-as-arrow-table }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Получение как кортежей {: #fetch-as-tuples }

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Метаданные столбцов {: #column-metadata_1 }

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Контекстный менеджер {: #context-manager }

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC по умолчанию подключается к серверу Flight на порту 8815. (REQ-143) Передайте `port=`, чтобы обратиться к серверу Flight, привязанному к нестандартному порту. (REQ-711)
