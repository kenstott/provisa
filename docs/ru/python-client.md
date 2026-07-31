# Python-клиент (`provisa-client`)

Python-клиент для Provisa. Предоставляет четыре интерфейса:

| Интерфейс | Сценарий использования |
| ----------- | ---------- |
| `ProvisaClient` | Запросы GraphQL, Arrow Flight, вывод DataFrame |
| DB-API 2.0 (`connect`) | Стандартный интерфейс базы данных Python (PEP 249) (REQ-268) |
| Диалект SQLAlchemy | BI-инструменты, ORM, Pandas `read_sql` (REQ-270) |
| ADBC | Нативная колоночная потоковая передача Arrow через Flight (REQ-271) |

## Установка

```bash
pip install provisa-client                      # core (ProvisaClient + DB-API)
pip install "provisa-client[pandas]"            # adds pandas
pip install "provisa-client[sqlalchemy]"        # adds SQLAlchemy dialect
pip install "provisa-client[adbc]"              # adds ADBC over Arrow Flight
```

---

## ProvisaClient

### Быстрый старт

```python
from provisa_client import ProvisaClient

client = ProvisaClient(
    "http://localhost:8001",
    username="alice",
    password="secret",
)
```

### Запросы GraphQL

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

### Асинхронность

```python
result = await client.aquery("{ orders { id amount } }")
```

### Arrow Flight (высокая пропускная способность, колоночный формат)

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

### Исследование каталога

```python
tables_df = client.list_tables()
```

### Справочник подключения

| Параметр | По умолчанию | Описание |
| ----------- | --------- | ------------- |
| `url` | `http://localhost:8001` | Базовый URL сервера Provisa |
| `token` | `None` | Токен Bearer; опустите для аутентификации по паролю (REQ-606) |
| `role` | `"admin"` | Роль, передаваемая с каждым запросом (REQ-273) |
| `flight_port` | `8815` | Порт gRPC Arrow Flight (REQ-143) |

### Обработка ошибок

`query()` выбрасывает `httpx.HTTPStatusError` при ошибках HTTP. (REQ-607)
`query_df()` выбрасывает `RuntimeError`, если ответ содержит ошибки GraphQL. (REQ-607)

---

## DB-API 2.0

Стандартный интерфейс [PEP 249](https://peps.python.org/pep-0249/). (REQ-268) Работает с любым инструментом, принимающим подключение DB-API.

```python
from provisa_client import connect

conn = connect(
    "http://localhost:8001",
    username="alice",
    password="secret",
    role="admin",       # optional, default "admin"
)
```

### Выполнение запросов

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

### Метаданные столбцов

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
print(cur.rowcount)
```

### Именованные параметры

```python
cur.execute(
    "SELECT * FROM orders WHERE region = :region",
    {"region": "west"},
)
```

### Контекстные менеджеры

```python
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        print(cur.fetchall())
```

---

## Диалект SQLAlchemy

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

### С pandas

```python
import pandas as pd

df = pd.read_sql("{ orders { id amount } }", engine)
```

### Параметры URL

| Параметр | Описание | По умолчанию |
| ----------- | ------------- | --------- |
| `role` | Роль Provisa | `admin` |

```python
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)
```

### Интроспекция схемы

Диалект реализует `get_table_names()`, `get_columns()` и `has_table()` — инструменты каталогизации (DBeaver, SQLAlchemy automap) могут исследовать схему. (REQ-363, REQ-270)

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

### Получение как таблицы Arrow

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount region } }")
    table = cur.fetch_arrow_table()   # pyarrow.Table
    df = table.to_pandas()
```

### Получение как кортежей

```python
with conn.cursor() as cur:
    cur.execute("{ orders { id amount } }")
    rows = cur.fetchall()    # list of tuples
    one  = cur.fetchone()    # single tuple or None
```

### Метаданные столбцов

```python
cur.execute("{ orders { id amount } }")
print(cur.description)
# [('id', None, ...), ('amount', None, ...)]
```

### Контекстный менеджер

```python
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

ADBC по умолчанию подключается к серверу Flight на порту 8815. (REQ-143) Передайте `port=`, чтобы обратиться к серверу Flight, привязанному к нестандартному порту. (REQ-711)
