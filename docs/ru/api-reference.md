# Справочник API

## Обзор

Provisa предоставляет REST-эндпоинты под двумя префиксами: `/data` — для выполнения запросов и интроспекции схемы, `/admin` — для управления конфигурацией. (REQ-043) Большинству эндпоинтов данных требуется идентификатор роли. Административные операции с конфигурацией используют Strawberry GraphQL API по адресу `/admin/graphql`. (REQ-164)

---

## Аутентификация

Когда в `provisa.yaml` настроен `auth.provider`, всем эндпоинтам, кроме `/health` и `/setup/status`, требуется заголовок `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Без настроенной аутентификации сервер работает в режиме разработки. Любой запрос трактуется как личность `anonymous`, которая сопоставляется со всеми настроенными ролями и доступом ко всем доменам по шаблону. (REQ-535)

**Вход (`POST /auth/login`)** предоставляется активным провайдером аутентификации, когда настроено `provider: basic`. (REQ-124) Формат учётных данных и ответа зависит от провайдера.

**Интроспекция личности:**

```http
GET /auth/me
```

Возвращает идентификатор аутентифицированного пользователя, адрес электронной почты, отображаемое имя, членства в организациях и назначения ролей. В режиме разработки возвращает `dev_mode: true` со списком всех идентификаторов ролей. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Возвращает `{"provider": "<name>"}` либо `{"provider": null}`, когда аутентификация не настроена. [tool-verified: `provisa/api/auth_router.py`]

---

## Эндпоинты данных

### `POST /data/graphql`

Выполнить GraphQL-запрос или мутацию. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Тело запроса:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Поле `role` используется только в режиме разработки (без аутентификации). Когда аутентификация активна, используется роль аутентифицированного пользователя, а `role` в теле игнорируется.

Поле `extensions` поддерживает протокол Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Заголовки:**

- `X-Provisa-Role` — переопределение роли (режим разработки)
- `Accept` — формат ответа (см. согласование содержимого)
- `Authorization` — `Bearer <token>`, когда аутентификация включена
- `X-Provisa-Redirect-Format` — MIME-тип для вывода при перенаправлении в S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — число строк, выше которого срабатывает перенаправление (REQ-137)
- `X-Provisa-Redirect` — `true`, чтобы принудительно включить перенаправление (REQ-029)

**Ответ (JSON, встроенный):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Ответ (перенаправление):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Ответ (несколько корневых полей, смесь встроенного и перенаправления):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Запросы с несколькими корневыми полями выполняют каждое корневое поле независимо. Поля ниже порога перенаправления возвращаются встроенно; поля выше — перенаправляются. Ключ `redirects` (во множественном числе) сопоставляет имена полей со сведениями о перенаправлении. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Заголовки кэша:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (при HIT) (REQ-536)

**Требуемые возможности:** `QUERY_DEVELOPMENT` для всех запросов, включая интроспекцию. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Согласование содержимого

| Заголовок Accept | Формат |
| --- | --- |
| `application/json` | JSON (по умолчанию) |
| `application/x-ndjson` | JSON с разделением по строкам |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Перенаправление

Результаты выше настроенного порога числа строк (или при `X-Provisa-Redirect: true`) записываются в S3, и возвращается предподписанный URL. (REQ-029, REQ-044)

| Формат перенаправления | Кем записывается | Память |
| --- | --- | --- |
| `application/vnd.apache.parquet` | федеративный CTAS | Нет — данные не проходят через Provisa |
| `application/x-orc` | федеративный CTAS | Нет — данные не проходят через Provisa |
| `application/json` | Provisa | Ограничено памятью |
| `application/x-ndjson` | Provisa | Ограничено памятью |
| `text/csv` | Provisa | Ограничено памятью |
| `application/vnd.apache.arrow.stream` | Provisa | Ограничено памятью |

Для крупных аналитических выгрузок используйте перенаправление в Parquet или ORC. Механизм федерации пишет в S3 напрямую и параллельно — данные не проходят через Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Выполнить сырой SQL через конвейер управления этапа 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Требуемые возможности:** `QUERY_DEVELOPMENT`.

Нарушения управления на `POST /data/sql` возвращают HTTP 403. (REQ-002, REQ-266)

**Ответ:** тот же формат, что и у `/data/graphql` (по умолчанию строки JSON, формат согласуется через `Accept`).

---

### `POST /data/query`

Единый эндпоинт запросов. Принимает GraphQL, SQL или Cypher — синтаксис определяется автоматически. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Запросы на Cypher можно также отправлять на отдельный эндпоинт `POST /query/cypher`. (REQ-345)

**Тело запроса:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Возвращает `{"data": ...}` для GraphQL и `{"columns": [...], "rows": [...]}` для SQL и Cypher.

---

### `POST /data/sql/explain`

Объяснить или проанализировать SQL-оператор через управляемый конвейер. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

Эндпоинт оборачивает **управляемый** SQL — оператор, который действительно выполняется под ролью вызывающего, после RLS и маскирования — в синтаксис EXPLAIN соответствующего диалекта. План показывает авторизованную версию запроса, а не исходный ввод.

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Установите `analyze: true`, чтобы выполнить EXPLAIN ANALYZE. Запрос выполняется, и план несёт реальные количества строк и тайминги. ANALYZE поддерживают не все диалекты; см. таблицу в разделе [Планы запросов и статистика](engines.md#query-plans-and-statistics).

**Ответ:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, когда диалект не поддерживает EXPLAIN либо когда `analyze: true` запрошен на диалекте, который его не поддерживает (например, SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Вернуть текущее состояние шарда механизма, не пробуждая его. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

Интерфейс опрашивает этот эндпоинт, чтобы показать баннер запуска, пока механизм проходит холодный старт. Он никогда не инициирует пробуждение — опрос безопасен и не считается активностью для сборщика простаивающих шардов.

**Ответ:**

```json
{"state": "ready"}
```

Возможные значения:

| Состояние | Значение |
| --- | --- |
| `always-on` | Настольная версия, самостоятельный хостинг или собственный координатор — управление жизненным циклом отсутствует |
| `ready` | Шард поднят и принимает запросы |
| `starting` | Идёт холодный старт |
| `stopped` | Шард масштабирован до нуля |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Инициировать пробуждение механизма без выполнения запроса. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Немедленно возвращает `202 Accepted`. Пробуждение выполняется в фоне. Используйте это, если механизм должен быть готов до прихода первого запроса — например, из планировщика, который выполнит запросы через несколько минут.

**Ответ:** `202 Accepted`, тело `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Автоматически создаваемый простой REST-эндпоинт для каждой зарегистрированной таблицы. Строка запроса отображается в аргументы GraphQL, а сам запрос компилируется и выполняется через тот же конвейер (RLS, маскирование, маршрутизация), что и GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Параметры запроса:**

- `limit` — максимум строк (≥ 1)
- `offset` — пропустить строки (≥ 0)
- `fields` — имена столбцов через запятую (по умолчанию все скалярные поля)
- `filter` — JSON-массив объектов фильтра `{"field", "comparator", "value"}`
- `orderBy` — JSON-массив объектов сортировки `{"field", "direction"}`

Аутентифицированная роль обязательна; неаутентифицированные запросы возвращают `401`. Спецификация OpenAPI для этих маршрутов отдаётся по `GET /data/rest/openapi.json`, а Swagger UI — по `GET /data/rest/docs`.

#### Обозреватель OpenAPI / Swagger UI

Страница обозревателя OpenAPI (`/app/openapi`) встраивает Swagger UI в изолированный iframe. Спецификация ограничена ролью — видны только таблицы и столбцы, доступные текущей роли — и опционально фильтруется по домену через селектор доменов. Интерфейс автоматически переключается между светлой и тёмной темами. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Страница загружает HTML спецификации через `fetch()`, а не прямым `src` у iframe, поэтому запрос несёт токен-носитель сессии, а собственные относительные запросы Swagger UI корректно разрешаются относительно того же источника. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

При переходе по ссылке «Открыть в OpenAPI» из режима естественного языка страница автоматически разворачивает целевой эндпоинт, заполняет параметры запроса из сформированного URL (например, `aggregate`, `groupBy`) и нажимает Execute — используя опрос DOM, чтобы каждый шаг завершался до начала следующего. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически создаваемый эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен содержать `application/vnd.api+json` (медиатип JSON:API), иначе запрос возвращает `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей, например `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — например, `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — постраничная навигация
- `aggregate` — агрегатные функции через запятую, выполняемые вместо выборки строк: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Используйте `?aggregate=count,sum`, чтобы запросить подмножество. Агрегатные ответы возвращают `data: null`, а результаты — в `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — имена столбцов через запятую; используется вместе с `?aggregate=` для группировки результатов. Допустимы только столбцы из перечисления `DistinctOnColumn` этой таблицы; сервер возвращает `400` для любого столбца, недоступного роли. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, чтобы включить скалярные столбцы базовой таблицы (и скаляры присоединённых измерений, названные в `include=`) в массив `nodes` каждой строки группы. Требуется, когда запрос группировки на естественном языке также запрашивает подробности измерений. (REQ-1405)

Ответы — объекты ресурсов с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

#### Обозреватель JSON:API

Страница обозревателя JSON:API (`/app/jsonapi`) — браузерный интерфейс поверх этих эндпоинтов. Выберите таблицу из списка, сгруппированного по доменам, затем настройте:

- **Поля** — выберите включаемые столбцы (разреженный набор полей); оставьте всё неотмеченным, чтобы запросить каждый столбец
- **Связи** — выберите имена связей, выведенных из внешних ключей, для загрузки через `?include=`
- **Фильтр** — поле, оператор (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) и значение
- **Сортировка** — одно поле, по возрастанию или убыванию
- **Агрегация** — выберите столбцы группировки из проверенного сервером списка, затем отметьте одну или несколько агрегатных функций; когда выбраны столбцы группировки, появляется флажок «Включить узлы», добавляющий скалярные столбцы базовой таблицы к каждой строке
- **Размер страницы** — ресурсов на страницу, с переходами к первой/предыдущей/следующей/последней

Результаты отображаются в отформатированном сводном виде (карточки ресурсов с кликабельными якорями связей) либо на вкладке сырого JSON. Действующий URL запроса показан и может быть скопирован. Выбор таблицы и размер страницы сохраняются между сессиями в `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

При переходе по ссылке «Открыть в JSON:API» из режима естественного языка обозреватель заранее выбирает таблицу, заполняет выбор агрегатов из сформированных параметров запроса и автоматически выполняет запрос. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Отправить вопрос на естественном языке. Сервис запускает асинхронное задание и немедленно возвращает `202 Accepted` с `job_id`. Требуется провайдер LLM, настроенный в разделе конфигурации `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Тело запроса:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Возвращает `{"job_id": "<id>"}`. Превышение лимита частоты запросов на естественном языке для роли возвращает `429` с заголовком `Retry-After`. (REQ-370)

**Получение результата:**

- `GET /query/nl/{job_id}` — опрос. Возвращает документ задания.
- `GET /query/nl/{job_id}/stream` — SSE. По одному событию `branch` на каждую цель генерации по мере её завершения, затем событие `done`. (REQ-357, REQ-358)

Три цикла генерации (Cypher, GraphQL, SQL) выполняются параллельно, каждый проверяется компилятором и уточняется при ошибке. (REQ-355) Подсказка ограничена схемой, видимой роли. (REQ-356) Документ результата содержит ключ для каждой ветви по цели: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Ветвь, исчерпавшая лимит итераций, возвращает `query: null`, `result: null` и строку `error`. Каждый сгенерированный запрос выполняется с правами потребителя и с управлением этапа 2 — сервис никогда не обходит управление. (REQ-359)

#### Группировка на естественном языке с подробностями измерений (REQ-1405)

Когда запрос группировки на естественном языке также проецирует столбцы из присоединённой таблицы измерений — например, «количество обращений по пользователям с именем и электронной почтой пользователя» — исполнитель выводит из спроецированных в SELECT столбцов измерений точечные пути по полям (`dim_paths`). Эти пути заполняют параметр `includeNodes=` в URL, формируемых панелями JSON:API и OpenAPI, так что эти панели запрашивают те же поля присоединённого измерения, которые разрешили ветви SQL и GraphQL. Без этого `includeNodes=true` возвращал бы только собственные скалярные поля базовой агрегируемой таблицы. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

На панели gRPC сформированный `{Type}GroupByRequest` несёт `include_nodes` (bool) и `include` (повторяющаяся строка с именами полей связей). Возвращаемый `{Type}GroupByRow` содержит типизированное поле `nodes` со строками подробностей измерений. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Вернуть GraphQL SDL для схемы роли. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Параметры запроса:**

- `domain` — идентификаторы доменов через запятую. Когда задан, ответ фильтруется по названным доменам и таблицам, достижимым из них.

**Ответ:** GraphQL SDL в виде `text/plain`.

---

### `GET /data/introspection`

Вернуть JSON интроспекции GraphQL, при необходимости отфильтрованный по домену. [tool-verified: `provisa/api/data/sdl.py:200`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:** `domain` — идентификаторы доменов через запятую.

**Ответ:** результат интроспекции в виде `application/json`.

---

### `GET /data/graph-schema`

Вернуть графовое представление схемы роли: метки узлов и типы их связей, для клиентов Cypher и графов. Включает `pk_columns` для каждой метки узла, чтобы вызывающая сторона могла определить столбцы первичного ключа. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Ответ:** `application/json` с `node_labels` (каждый несёт `pk`/`pk_columns`) и `relationship_types`.

Каждый тип связи несёт также `junction_table_name` и `properties` (REQ-1586). У ребра через junction первое называет проходимую ассоциативную таблицу, а второе перечисляет столбцы этой таблицы, читаемые как `r.attr` и фильтруемые в `WHERE`; у ребра на внешнем ключе имя равно `null`, а список свойств пуст — именно так клиент отличает одно от другого. Сама junction-таблица никогда не является меткой узла — она и есть ребро, поэтому у неё нет ни пилюли в графовом клиенте, ни строки в `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Вернуть идентификаторы доменов, доступные запрашивающей роли. [tool-verified: `provisa/api/data/sdl.py:116`]

**Заголовки:** `X-Role: <role_id>` (обязателен)

**Ответ:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Вернуть текущую строку версии схемы. Объединяет разовое значение, создаваемое при загрузке, со счётчиком перестроений. Клиенты используют её для сброса кэшей схемы после перезапуска сервера. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Ответ:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Вернуть автоматически сгенерированный файл `.proto` для роли. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Ответ:** схема protobuf в виде `text/plain`.

Каждая зарегистрированная таблица порождает `message` в proto. Связи порождают вложенные поля сообщений. Отображение типов: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Поток Server-Sent Events с уведомлениями об изменениях таблицы в реальном времени. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Доставка уведомлений использует подключаемый провайдер, выбираемый по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY` (через asyncpg), источники MongoDB — Change Streams (`collection.watch()`), а источники Kafka — группы потребителей. Каждый провайдер реализует общий асинхронный интерфейс наблюдения. Фильтрация RLS и проверка схемы применяются независимо от провайдера. (REQ-258) Источники WebSocket и RSS также поддерживаются. (REQ-338, REQ-342)

**Заголовок `X-Provisa-Sink`:** установите его в цель Kafka (например, `kafka://broker:9092/topic`), чтобы перенаправить события изменений в приёмник Kafka вместо ответа SSE. Сервер запускает потребителя-приёмник и возвращает `202 Accepted` вместо открытого потока. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Административные REST-эндпоинты

### Конфигурация

#### `GET /admin/config`

Скачать текущий `provisa.yaml` как `application/x-yaml` с заголовком `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Загрузить изменённый YAML конфигурации. Сервер записывает резервную копию `.bak`, сохраняет новый файл и перезагружает все схемы, источники и материализованные представления. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Тело запроса:** сырое содержимое YAML.

**Ответ:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

При сбое перезагрузки: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Скачать **текущую действующую конфигурацию** — конфигурацию в том виде, в каком Provisa записала бы её сегодня, отражающую каждую созданную администратором таблицу, связь, домен, роль и правило RLS, накопившиеся с момента запуска. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Файл на диске может отставать от действующего состояния, если изменения вносились через административный API без последующей загрузки. Этот эндпоинт закрывает разрыв: его вывод — это то, что `PUT /admin/config` должен получить, чтобы файл на диске совпал с действующим состоянием.

Возвращает `application/x-yaml` с `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Вернуть обе стороны сравнения конфигурации — `original` (исходное состояние при запуске) и `current` (действующее состояние) — нормализованные одинаково, так что сравнение показывает только подлинные изменения, а не перестановки или расхождения в комментариях. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Ответ:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Сформировать патч в формате унифицированного сравнения от исходного состояния к отправленной конфигурации. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Отправьте изменённый YAML в теле запроса. Ответ — файл `text/x-patch` (`provisa.config.patch`), который `git apply` или `patch` могут применить напрямую, — удобно для фиксации сделанных в интерфейсе изменений конфигурации через конвейер CI/CD.

---

### Настройки

#### `GET /admin/settings`

Вернуть текущие настройки платформы в JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**Ответ:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Обновить настройки платформы во время работы. Все поля необязательны — обновляются только ключи, присутствующие в теле. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Тело запроса (частичный пример):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Обновляемые поля по разделам:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — записывается в файл конфигурации и вызывает перезагрузку схемы (REQ-253)
- `relationships`: `auto_track_fk` — управляет только отслеживанием внешних ключей. Связь через junction объявляется при регистрации таблицы и никогда не выводится автоматически, поэтому эта настройка её не касается. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Ответ:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Модели ИИ

#### `GET /admin/ai-models`

Вернуть назначения моделей ИИ действующей организации, реестр векторных моделей и лимит частоты запросов на естественном языке. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**Ответ:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

Ключи API никогда не возвращаются обратно — `api_keys_set` сообщает лишь о том, настроен ли ключ у каждого поставщика. Изменения вступают в силу со следующего запроса; перезапуск не нужен. (REQ-1349)

#### `PUT /admin/ai-models`

Обновить назначения моделей ИИ организации, реестр векторных моделей или лимит частоты запросов на естественном языке. Вступает в силу со следующего запроса. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Вернуть имена моделей, которые поставщик обслуживает в данный момент, для выбора модели. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

Список читается вживую из собственного API списка моделей поставщика с использованием ключа организации — либо учётных данных развёртывания, когда ключ организации не задан. Модель, выпущенная после сборки этой версии, доступна для выбора в тот же день, когда поставщик начинает её обслуживать.

Возвращает `400`, когда поставщик не публикует API списка моделей (в этом случае введите имя модели напрямую) либо когда ключ недоступен. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Механизм федерации

#### `GET /admin/federation-engine`

Вернуть текущий выбор механизма федерации, его конфигурацию подключения и полный реестр доступных для выбора механизмов. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**Ответ:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

Ключ `current` — механизм, работающий прямо сейчас; `persisted` — то, что записано в файл конфигурации и загрузится при следующем перезапуске. Они расходятся, когда конфигурация изменена, а служба ещё не перезапущена.

#### `PUT /admin/federation-engine`

Сохранить выбор механизма федерации. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Тело запроса:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Выбор записывается в конфигурацию платформы. Он вступает в силу после следующего перезапуска службы — механизм выбирается один раз при загрузке.

---

### Политика доменов

#### `POST /admin/domain-policy`

Изменить политику доменов действующей организации (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Это разрушительная операция в пределах действующей организации. Каждый зарегистрированный источник, таблица, домен и связь очищаются и перестраиваются по новой политике. Используйте её при переводе организации с доменных пространств имён на плоскую модель (или наоборот).

**Тело запроса:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` снимает переопределение организации и возвращает настройку уровня развёртывания. `use_domains: false` требует `default_domain` (единственное имя домена, в который попадают все таблицы). Перестроение каталога синхронно; ответ возвращается, когда схемы готовы.

---

### Наблюдаемость

#### `GET /admin/traces/recent`

Вернуть до N недавних завершённых интервалов из буфера интервалов в памяти. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Параметры запроса:** `limit` (по умолчанию 50, максимум 200)

**Ответ:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Перезагрузить на лету именованный каталог в координаторе механизма федерации через его REST API. Переподключает внутреннее соединение Provisa и заново выполняет DDL для OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Параметры запроса:** `catalog` (по умолчанию `"otel"`)

**Ответ:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Перезапустить контейнер механизма федерации (только одноузловая разработка). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Параметры запроса:** `container` (по умолчанию переменная окружения `QUERY_ENGINE_CONTAINER`, затем `"trino"`)

---

### Обнаружение

#### `POST /admin/discover/relationships`

Запустить обнаружение связей. Всегда выполняет интроспекцию внешних ключей из механизма федерации. (REQ-018) Выполняет вывод средствами LLM, если задан `ANTHROPIC_API_KEY`. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Тело запроса:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` должен быть одним из `"table"`, `"domain"`, `"cross-domain"`. Для области `"table"` обязателен `table_id` (целое число). Для области `"domain"` обязателен `domain_id`.

**Ответ:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Перечислить ожидающие кандидаты в связи. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Принять кандидата и зарегистрировать его как связь. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Тело запроса (необязательно):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Отклонить кандидата. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Тело запроса:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Вернуть количество отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Удалить все отклонённые кандидаты. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Обход источника

#### `POST /admin/sources/crawl`

Обойти источник данных, чтобы провести интроспекцию его схемы и зарегистрировать таблицы. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Поиск таблиц источника

#### `GET /admin/sources/{source_id}/tables/search`

Искать по имени доступные (ещё не зарегистрированные) таблицы в источнике. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Профилирование таблиц

#### `POST /admin/tables/{table_id}/profile`

Выполнить профилирование столбцов зарегистрированной таблицы — мощность множества, минимум/максимум, доли пустых значений. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Описания источников

#### `POST /admin/source-meta/db-description`

Сформировать с помощью LLM описания таблиц и столбцов источника. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Объектное хранилище (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Сообщить объём хранилища, занимаемый действующей организацией, относительно её квоты на платформе, а также зарегистрировала ли организация собственное хранилище. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Когда организация зарегистрировала собственный DSN, её материализации размещаются там и больше не засчитываются в квоту. Сам DSN никогда не возвращается.

#### `PUT /admin/org-storage`

Зарегистрировать (или очистить) собственное хранилище материализаций организации. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Тело запроса:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN проверяется механизмом федерации до принятия — непригодный DSN отказывает при регистрации, а не часами позже во время обновления. Значение шифруется при хранении и никогда не возвращается методом GET.

Отправьте `storage_url: null`, чтобы очистить собственное хранилище организации и вернуть её материализации в хранилище платформы (и в квоту). Среда выполнения организации перестраивается тем же вызовом, поэтому новое хранилище действует немедленно. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Шифрование организации (REQ-1574)

#### `GET /admin/org-encryption`

Вернуть текущее состояние ключа организации: отпечаток, идентификатор и происхождение. Никогда не возвращает материал ключа. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Когда организация не задала ключ, возвращает `{"configured": false}`. Каждая организация начинает в этом состоянии и наследует ключ развёртывания.

#### `PUT /admin/org-encryption`

Задать или сменить ключ шифрования организации при хранении. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Тело запроса:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Опустите `key_b64`, чтобы Provisa сгенерировала ключ, — самый безопасный путь, поскольку ключ не попадает ни в буфер обмена, ни в журнал запросов. Передача `key_b64` означает использование собственного ключа.

Смена добавляет новую активную запись в связку ключей и сохраняет старую, поэтому данные, записанные под предыдущим ключом, остаются читаемыми. Смена — это не перешифрование. Эндпоинта удаления нет: вывод последнего ключа сделал бы каждую защищённую полезную нагрузку нечитаемой. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Действующая связка переподключается тем же вызовом, поэтому следующая зашифрованная запись сразу использует новый ключ.

---

### Импорт Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Преобразовать архив проекта Hasura v2 или DDN в предлагаемую конфигурацию Provisa, ничего не записывая. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` — это `"auto"` (определяется по структуре архива), `"hasura_v2"` или `"ddn"`.

**Ответ:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Ничего не сохраняется. Предпросмотр не кэшируется на сервере; `apply` принимает переданный вами YAML, поэтому применяется ровно то, что было просмотрено (и при необходимости отредактировано).

#### `POST /admin/import/hasura/apply`

Загрузить ранее просмотренную конфигурацию в действующую организацию. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{"config_yaml": "<yaml string>"}
```

Использует тот же путь горячей перезагрузки, что и `PUT /admin/config`. Каталог, схемы и пулы организации перестраиваются до возврата ответа.

---

### Обмен через Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Экспортировать управляемую модель организации как документ YAML в формате Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Документ выводится из действующего состояния при каждом запросе — никогда не кэшируется, — поэтому он не может устареть. Таблицы становятся объектами `dataset`, столбцы — объектами `field`, а связи отображаются в объекты `relationship` Ossie.

Возвращает `text/yaml` с `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Разобрать документ Ossie в формате YAML или JSON и вернуть предлагаемые регистрации таблиц и связей. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Тело запроса:** сырой Ossie в YAML или JSON. Формат определяется автоматически.

**Ответ:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Ничего не регистрируется. Используйте экран проверки в административном интерфейсе, чтобы принять или сократить предложения до того, как сработает любое изменение.

---

### Действия (функции и веб-перехватчики)

Все эндпоинты находятся под префиксом `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Каждый вызов — из GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` и gRPC Provisa — проходит через единый управляемый исполнитель, который единообразно применяет `writable_by` и управление. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Синтаксис вызова для каждого протокола см. в [docs/integrations.md](integrations.md#invoking-commands-across-protocols).

#### `GET /admin/actions`

Вернуть все отслеживаемые функции БД и веб-перехватчики. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**Ответ:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "writableBy": [],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Каждый объект веб-перехватчика несёт булево поле `approved`. Веб-перехватчик считается утверждённым, как только стюард исполнит запрос на его создание (REQ-209); веб-перехватчики, объявленные в конфигурации, утверждаются автоматически. Неутверждённый веб-перехватчик зарегистрирован, но не доступен ни на одной поверхности. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Зарегистрировать отслеживаемую функцию (команду). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Ключевые поля:**

| Поле | Обязательно | Описание |
| --- | --- | --- |
| `name` | Да | Уникальное имя команды |
| `kind` | Да | `"query"` → поле Query в GraphQL; `"mutation"` → поле Mutation |
| `implKind` | Нет | Как выполняется команда — см. таблицу ниже (по умолчанию `source_procedure`) |
| `binding` | Нет | Сведения о подключении, зависящие от `implKind` (объект JSON) |
| `returnSchema` | Нет | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — делает команду возвращающей набор на каждой поверхности |
| `arguments` | Нет | Определения аргументов `[{name, type}]`; порядок важен для вызывающих через SQL и Bolt |
| `visibleTo` | Нет | Идентификаторы ролей, которые могут вызывать команду |
| `writableBy` | Нет | Идентификаторы ролей, которым разрешено вызывать её как мутацию |
| `domainId` | Нет | Домен для размещения в GraphQL и контроля доступа |

**Значения `implKind`:**

| `implKind` | Что выполняется | Поля `binding` |
| --- | --- | --- |
| `source_procedure` | Хранимая процедура в зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Серверный сценарий | `script` |
| `http` | Исходящий HTTP-вызов | `url`, `method` |
| `grpc` | Исходящий вызов gRPC к внешнему серверу | `target`, `method` |
| `python` | Вызываемый объект Python, размещённый в Provisa (REQ-885) | `callable` (например, `"demo.py_functions:random_dataset"`) |

Демонстрационные команды `random_python_set` (`implKind: python`) и `random_grpc_set` (`implKind: grpc`) показывают возвращающие набор команды с `returnSchema` на практике; обе находятся в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Обновить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Удалить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Зарегистрировать отслеживаемый веб-перехватчик. (REQ-209) Регистрация или обновление веб-перехватчика ставит в очередь запрос на утверждение стюардом — веб-перехватчик становится активным на всех поверхностях только после утверждения стюардом. Веб-перехватчики, объявленные в конфигурации, утверждаются автоматически. **Поля тела запроса:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Обновить отслеживаемый веб-перехватчик по имени. Любое изменение возвращает утверждение в состояние ожидания до повторного утверждения. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Удалить отслеживаемый веб-перехватчик по имени. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Проверить действие (функцию или веб-перехватчик) по имени. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Роли

Все эндпоинты находятся под префиксом `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Перечислить все роли |
| `POST` | `/admin/roles/` | Создать роль |
| `PUT` | `/admin/roles/{role_id}` | Обновить роль |
| `DELETE` | `/admin/roles/{role_id}` | Удалить роль |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Пользователи

Все эндпоинты находятся под префиксом `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/users/` | Создать локального пользователя |
| `GET` | `/admin/users/` | Перечислить локальных пользователей |
| `GET` | `/admin/users/{user_id}` | Получить пользователя |
| `PUT` | `/admin/users/{user_id}` | Обновить пользователя |
| `PATCH` | `/admin/users/{user_id}/password` | Сменить пароль |
| `DELETE` | `/admin/users/{user_id}` | Удалить пользователя |
| `GET` | `/admin/users/{user_id}/assignments` | Перечислить назначения ролей |
| `POST` | `/admin/users/{user_id}/assignments` | Добавить назначение роли |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Снять назначение роли |

---

### Организации

Все эндпоинты находятся под `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Перечислить организации |
| `POST` | `/admin/orgs/` | Создать организацию |
| `PUT` | `/admin/orgs/{org_id}` | Обновить организацию |
| `DELETE` | `/admin/orgs/{org_id}` | Удалить организацию |
| `GET` | `/admin/orgs/{org_id}/members` | Перечислить участников |
| `POST` | `/admin/orgs/{org_id}/members` | Добавить участника |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Удалить участника |

---

### Приглашения

Все эндпоинты находятся под `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Создать приглашение |
| `GET` | `/admin/invites/` | Перечислить ожидающие приглашения |
| `DELETE` | `/admin/invites/{token}` | Отозвать приглашение |

---

### Административный GraphQL

#### `POST /admin/graphql`

Эндпоинт Strawberry GraphQL для всех административных операций: CRUD источников и таблиц, управление связями, настройка доменов, правила RLS, управление кэшем, соглашения об именовании, управление запланированными задачами и компиляция запросов. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Ключевые мутации:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Первоначальная настройка

#### `GET /setup/status`

Вернуть состояние первоначальной настройки. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Завершить первоначальную настройку. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Проверка работоспособности

#### `GET /health` или `HEAD /health`

Возвращает `{"status": "ok"}`. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Ответы с ошибками

| Статус | Значение |
| --- | --- |
| 400 | Неверный запрос, ошибка проверки или ошибка разбора SQL |
| 401 | Отсутствует или недействителен токен аутентификации |
| 403 | Недостаточно возможностей; нарушение управления |
| 404 | Роль, ресурс или файл конфигурации не найдены |
| 422 | Отсутствует обязательный заголовок (например, `X-Role`) |
| 503 | База данных или источник не подключены; зависимость недоступна |
| 504 | Истекло время ожидания запроса |

Нарушения управления на `POST /data/sql` возвращают HTTP 403 со структурированным телом: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Все прочие ошибки используют: `{"detail": "<message>"}`.

---

## Эндпоинт Arrow Flight

Порт `8815`. Нативный колоночный транспорт Arrow поверх gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Запросы и обнаружение каталога доступны в рамках одного соединения. Полный конвейер управления (RLS, маскирование, выборка) применяется к каждому запросу. (REQ-130, REQ-143)

**Формат билета** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Использование (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Когда доступен прокси Zaychik Flight SQL (порт 8480), пакеты записей передаются сквозным потоком без полной материализации. (REQ-144) При недоступности Zaychik происходит возврат к материализации через федеративный слой запросов. (REQ-146)

---

## Эндпоинт Protobuf gRPC

Порт `50051` (переопределяется переменной окружения `GRPC_PORT` или параметром конфигурации `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Передавайте роль в ключе метаданных gRPC `x-provisa-role`. При его отсутствии сервер прерывает вызов с `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Скачайте proto для конкретной роли по `GET /data/proto/{role_id}`. В него попадают только таблицы и столбцы, видимые этой роли. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Каждая таблица порождает потоковый RPC `Query{TypeName}`. RPC `Insert{TypeName}` существуют ради симметрии схемы, но прерываются с `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` включён для обнаружения служб без предварительно скомпилированного proto. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Сервер gRPC запускается только тогда, когда при старте удаётся скомпилировать корректный proto. Если построение схемы не удалось, сервер gRPC не запускается. (REQ-529)

#### RPC агрегации и группировки (REQ-1359, REQ-1361, REQ-1405)

Когда у таблицы установлен `enable_aggregates`, сгенерированный proto включает два дополнительных RPC наряду с `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — возвращает агрегатные скаляры таблицы (`count`; `sum`, `avg`, `stddev`, `variance` для каждого числового столбца; `min`, `max` для каждого сравнимого столбца)
- **`Query{TypeName}GroupBy`** — возвращает по одной строке на ключ группы с агрегатными подполями и, при необходимости, скалярами базовой таблицы и строками присоединённых измерений в поле `nodes`

Оба проходят через тот же агрегатный конвейер компилятора, что и корневые поля GraphQL `{field}_aggregate` и `{field}_group_by`, — отдельной реализации агрегатов нет. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Поле `funcs` (REQ-1361).** Сообщение запроса принимает повторяющееся строковое поле `funcs`. Допустимые значения: `count`, `sum`, `avg`, `stddev`, `variance`, `min` и `max`. Когда `funcs` опущено, запрашивается каждая функция, которую схема предоставляет для этой таблицы. Когда задано — появляются только названные функции. Если ни одна из названных функций не применима к типам столбцов таблицы, запрос возвращается к `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Поля `include_nodes` и `include` (REQ-1405).** Запросы `Query{TypeName}GroupBy` могут установить `include_nodes: true`, чтобы включить скалярные столбцы базовой таблицы в поле `nodes` каждой строки. Повторяющееся строковое поле `include` называет поля связей «многие к одному», скалярные столбцы которых также вкладываются в `nodes`. Это соответствует поведению `?includeNodes=` / `?include=` в JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Драйвер JDBC

Драйвер JDBC для Provisa (`provisa-jdbc-0.1.0.jar`) предоставляет семантический каталог инструментам бизнес-аналитики (Tableau, PowerBI, DBeaver). (REQ-126)

**URL подключения:** `jdbc:provisa://host:port` (REQ-131)

Домены отображаются в схемы JDBC. (REQ-127) Таблицы используют свои зарегистрированные псевдонимы. Столбцы используют псевдонимы и предоставляют описания как `REMARKS`. (REQ-128) Стандартные методы метаданных (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) предоставляют семантические связи как метаданные первичных и внешних ключей.

**Поддержка SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Драйвер по умолчанию запрашивает перенаправление в формате Arrow IPC. Результаты передаются пакет за пакетом через `ArrowStreamReader`, с ограничением в один пакет записей в памяти. (REQ-293)

---

## Формат аргумента `orderBy`

Аргумент `order_by` использует объекты `{column: direction}` с перечислением направления из шести значений: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Поддерживаемые направления: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Подписки

Подписки SSE доступны по `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Доставка уведомлений использует подключаемый провайдер, выбираемый по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY`, источники MongoDB — Change Streams, а источники Kafka — группы потребителей. Фильтрация RLS и проверка схемы применяются независимо от провайдера. Источники WebSocket и RSS также поддерживаются через тот же эндпоинт. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Бизнес-глоссарий (REQ-1387)

Бизнес-глоссарий отображает физические имена полей — такими, какие они есть в исходных базах данных, — на общий человеческий словарь. Каждый столбец, зарегистрированный в семантическом слое, автоматически получает термин. Ручной ввод для наполнения глоссария не требуется; кураторы добавляют определения, связи и экспертов поверх того, что система выводит сама.

### Как выводятся термины

Когда Provisa регистрирует или обновляет столбцы таблицы, `normalize_term` (`provisa/core/glossary.py`) выполняется для каждого имени столбца и порождает каноническую фразу. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Нормализация применяет пять правил по порядку:

1. Разбиение по границам camelCase и символам-разделителям (`_`, `-`, `.`, `/`, пробельные символы).
2. Приведение результата к нижнему регистру.
3. Развёртывание фиксированной таблицы сокращений (например, `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Отбрасывание завершающего **замещающего токена** (`identifier`, `code`, `index` или `reference`) — столбец, названный по своему ключу или коду, указывает на лежащее в основе понятие через подставное значение, поэтому термином должно быть само понятие. Последний оставшийся токен никогда не отбрасывается.
5. Уточнение **слишком общей фразы** понятием таблицы. Когда полная нормализованная фраза — это голое слово-атрибут (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` и подобные), термином становится `<понятие таблицы> <фраза>`: `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Один общий термин `name` для несвязанных таблиц слил бы разные значения; уточнение вместо этого связывает каждый столбец с охватывающим его понятием. Понятие таблицы — это бизнес-имя таблицы, нормализованное с главным существительным в единственном числе (`order_lines` → `order line`).

Псевдостолбцы нативных фильтров (с префиксом `_nf_` либо любой столбец, несущий `native_filter_type`) — это механика параметров запроса, а не бизнес-поля, и терминов не порождают.

Поскольку `id`, `key`, `pk` и `sk` все развёртываются в `identifier` до проверки на замещающий токен, три физически разных имени столбца приходят ровно к одному термину:

| Физическое имя | После нормализации |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Первые три схлопываются в один термин. `transaction amount` сохраняет оба токена, поскольку `amount` не является замещающим. Голый столбец `id` — без предшествующих токенов — отброшен быть не может; он нормализуется в `identifier`, так что термин не пуст. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Жизненный цикл

Термины **выводятся из членства в семантическом слое**, а не создаются пользователями по запросу. Репозиторий таблиц — единственный путь записи: `sync_table_refs` выполняется внутри каждого обновления набора столбцов, а `sweep_refless_terms` — после любого пути удаления. [tool-verified: `provisa/core/repositories/glossary.py`]

**Когда столбец добавлен:** Provisa ищет нормализованный термин по имени. Если он уже существует, столбец получает ссылку на него (а если термин был помечен устаревшим, он возвращается к жизни — `deprecated` снова становится `False`). Если термина ещё нет, он создаётся.

**Когда столбец уходит** (изменение схемы или удаление таблицы): его ссылка удаляется, а термин **улаживается** по правилу «удалить или пометить устаревшим». Укоренённый термин без оставшихся ссылок удаляется полностью — вместе со своими рёбрами и назначениями экспертов, — если только его удаление не оставило бы абстрактный термин отсоединённым от всех укоренённых терминов (нет пути через граф терминов). В этом случае термин **помечается устаревшим** (`deprecated=True`), а не удаляется, чтобы якорь абстрактного термина в графе сохранился.

Абстрактные термины никогда не удаляются автоматически; они существуют вне физического жизненного цикла и удаляются только явно через административный API.

**Возвращение к жизни:** если нормализованное имя устаревшего термина появляется снова (столбец зарегистрирован повторно), пометка снимается, и его ссылки продолжают накапливаться.

### Эндпоинты курирования

Все эндпоинты находятся под `/admin/glossary`. Они требуют доступа `org_admin` и настроенной организации. Каждое изменение вызывает публикацию метаданных. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Перечислить термины. Параметры запроса: `q` (поиск по имени/определению), `include_deprecated` (по умолчанию `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Получить сведения о термине: определение, физические ссылки, типизированные рёбра, эксперты |
| `POST` | `/admin/glossary/terms` | Создать абстрактный термин — пользовательский словарь без физических ссылок |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Переименовать, задать определение или переключить исключение из экспорта |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Удалить термин, не имеющий физических ссылок |
| `POST` | `/admin/glossary/refs/move` | Перенести одну физическую ссылку к другому термину (консолидация) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Добавить типизированное ребро связи между двумя терминами |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Удалить ребро (параметры запроса: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Отметить пользователя как эксперта или автора термина |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Снять с пользователя обозначение эксперта или автора |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Составить черновик определения для одного термина моделью ИИ организации — возвращает только текст, до сохранения ничего не записывается |
| `POST` | `/admin/glossary/definitions/generate` | Сформировать и сохранить определения для каждого термина, у которого их нет, — никогда не перезаписывает текст, написанный человеком |
| `POST` | `/admin/glossary/relationships/generate` | Предложить и сохранить типизированные рёбра по всему глоссарию моделью ИИ организации |

**Тело `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Тело `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Допустимые значения `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Тело `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Допустимые значения `kind`: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Тело `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Перенос ссылки улаживает теряющий её термин по правилу «удалить или пометить устаревшим». Используйте это, чтобы объединить два термина, которые нормализация оставила раздельными, — например, после того как источник применил нестандартное сокращение, не попавшее в таблицу развёртывания.

Удаление укоренённого термина (имеющего физические ссылки) возвращает `400 glossary.invalid`. Сначала удалите или перенесите все ссылки.

**`PATCH /admin/glossary/terms/{term_id}` — поле `export_excluded`:**

```json
{"export_excluded": true}
```

Установка `export_excluded` в `true` изымает термин из всех снимков экспорта метаданных, независимо от его физических ссылок или абстрактного статуса. Возврат в `false` восстанавливает термин в снимке при следующей публикации. Данные курирования (определение, рёбра, эксперты) не затрагиваются. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Курирование с помощью ИИ

Настроенная в организации модель ИИ может составлять определения и предлагать рёбра связей по всему глоссарию за одну операцию. Оба массовых действия требуют доступа `org_admin` и настроенной организации.

**`POST /admin/glossary/definitions/generate`**

Перебирает каждый термин в глоссарии, пропускает те, у которых определение уже есть, и вызывает модель ИИ организации, чтобы составить его для каждого оставшегося термина. Черновик сохраняется немедленно — в отличие от эндпоинта черновика для одного термина (`POST /admin/glossary/terms/{term_id}/definition/generate`), шага редактирования нет. Определения, написанные человеком, никогда не перезаписываются: защита — это `if summary["definition"]: continue` перед любым вызовом модели. Одно уведомление о публикации покрывает весь пакет. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Ответ:

```json
{"generated": 12}
```

`generated` — количество терминов, получивших новое определение. Оно равно нулю, когда определение уже есть у каждого термина.

**`POST /admin/glossary/relationships/generate`**

Отправляет полный список терминов модели ИИ организации с подсказкой, которая задаёт десять допустимых типов рёбер (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) и просит только уверенные предложения. Модель отвечает массивом JSON; каждая запись проверяется до любой записи в хранилище: неизвестные имена терминов, петли и типы рёбер вне закрытого перечисления молча отбрасываются. Корректные предложения записываются идемпотентно — повторный запуск действия не дублирует рёбра. Одно уведомление о публикации покрывает пакет. Эндпоинт немедленно возвращает `{"added": 0}`, когда в глоссарии меньше двух неустаревших терминов. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Ответ:

```json
{"added": 5}
```

`added` — количество записанных рёбер. Уже существовавшее ребро тоже засчитывается: запись проходит успешно, но данные ребра не меняются.

### Инструмент MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Ищет по именам и определениям терминов подстрокой без учёта регистра, до `limit` результатов. Каждый результат — полные сведения о термине: `name`, `definition`, `is_abstract`, `deprecated`, физические ссылки (с `source_id`, `schema_name`, `table_name`, `column_name`), типизированные рёбра и назначения экспертов. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Используйте `search_terms` перед написанием SQL, чтобы найти по имени каждое физическое поле, представляющее понятие. Например, поиск `"order date"` вернёт термин и все столбцы `order_dt`, `orderDate`, `ORDER_DATE` по каждой зарегистрированной таблице.

### Экспорт метаданных

Граф терминов глоссария включён в каждый `MetadataSnapshot`, строимый функцией `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Экспорт применяет те же фильтры, что и остальная часть снимка:

- Термин, помеченный `export_excluded`, изымается сразу — независимо от его физических ссылок, абстрактного статуса и того, настроен ли каталог организации. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Укоренённый термин публикуется, только когда хотя бы одна из его физических ссылок принадлежит столбцу, проходящему и фильтр **продукта данных** (флаг таблицы `data_product` должен быть `true`), и **технический** фильтр столбцов (столбцы, помеченные `technical`, изымаются).
- Укоренённый термин, все ссылки которого изъяты этими фильтрами, изымается вместе с ними.
- Абстрактные термины публикуются безусловно — это пользовательский словарь, не привязанный к физическим столбцам.
- Ребро между двумя терминами публикуется, только когда публикуются оба его конечных термина.

Каждый адаптер поставщика публикует граф терминов нативно, в принадлежащий Provisa контейнер глоссария, который он создаёт идемпотентно, — и никогда в существующий глоссарий каталога:

| Поставщик | Контейнер | Термины | Связи | Признание устаревшим |
| --- | --- | --- | --- | --- |
| Apache Atlas | «Provisa Glossary» (API глоссария) | термины глоссария, определение в `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | маркер `[DEPRECATED]` в shortDescription |
| Atlan | Глоссарий Provisa по устойчивому qualifiedName | `longDescription` (никогда не редактируемое человеком `userDescription`) | то же отображение, что у Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | аспект `glossaryTermInfo` на термин | KIND_OF → Inherits, PART_OF → Contains (инвертировано), RELATED_TO/SYNONYM_OF → связанные термины | аспект deprecation; переименования следуют преемственности URN |
| OpenMetadata | Глоссарий Provisa через `/v1/glossaries` | PUT по ключу fqn, переименования через PATCH с перепривязкой по сохранённому UUID | KIND_OF → нативная родительская иерархия, SYNONYM_OF → `synonyms`, остальные → `relatedTerms` | `entityStatus` |
| Collibra | Домен типа «глоссарий» «Provisa Glossary» | активы Business Term через Import API | нативные типы связей Business Term | статус актива |

Привязкой служит владение, а не имя: идентификатор каждого опубликованного термина на стороне поставщика фиксируется в `catalog_bindings` под URN термина (`provisa://<org>/terms/<name>`), и Provisa изменяет или удаляет элемент глоссария на стороне поставщика только тогда, когда владеет этой привязкой (или элемент находится в созданном ею контейнере, принадлежащем Provisa). Элемент глоссария без привязки Provisa возник во внешней системе и никогда не затрагивается; обновления выполняются со слиянием при чтении, поэтому добавленные стюардом поля у собственных терминов Provisa сохраняются; ничего не удаляется, когда термин покидает снимок. Назначения терминов активам, сделанные стюардом, остаются во внешнем владении — ни один адаптер не пишет назначения терминов активам (публикация назначений, созданных Provisa, — явное продолжение). В случае Collibra безопасность при семантике REPLACE у Import API опирается на вложенность: полезная нагрузка упоминает только активы внутри домена глоссария Provisa и экземпляры связей только между терминами Provisa, поэтому глоссарии стюардов и их связи недостижимы. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
