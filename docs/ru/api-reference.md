# Справочник API {: #api-reference }

## Обзор {: #overview }

Provisa предоставляет REST-эндпоинты под двумя префиксами: `/data` для выполнения запросов и интроспекции схемы, и `/admin` для управления конфигурацией. (REQ-043) Большинство эндпоинтов данных требуют идентификатор роли. Операции конфигурации admin используют Strawberry GraphQL API по адресу `/admin/graphql`. (REQ-164)

---

## Аутентификация {: #authentication }

Когда `auth.provider` настроен в `provisa.yaml`, все эндпоинты, кроме `/health` и `/setup/status`, требуют заголовок `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Без настроенной аутентификации сервер работает в режиме разработки. Любой запрос обрабатывается как identity `anonymous`, которая сопоставляется со всеми настроенными ролями с доступом к доменам по маске (wildcard). (REQ-535)

**Вход (`POST /auth/login`)** предоставляется активным провайдером аутентификации, когда настроено `provider: basic`. (REQ-124) Формат учётных данных и ответа зависит от провайдера.

**Интроспекция identity:**

```http
GET /auth/me
```

Возвращает id, email, отображаемое имя, членства в организациях и назначенные роли аутентифицированного пользователя. В режиме разработки возвращает `dev_mode: true` со списком всех id ролей. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Возвращает `{"provider": "<name>"}` или `{"provider": null}`, если аутентификация не настроена. [tool-verified: `provisa/api/auth_router.py`]

---

## Эндпоинты данных {: #data-endpoints }

### `POST /data/graphql`

Выполнить GraphQL-запрос или мутацию. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Тело запроса:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

Запрос выполняется от своей действующей роли: роли аутентифицированного пользователя или, в режиме разработки (без аутентификации), заголовка `X-Provisa-Role`, а при его отсутствии — `org_admin`. Поле `role` в теле не выбирает роль. Если оно есть, оно должно совпадать с действующей ролью; другое значение возвращает `400` с кодом `data.role_mismatch`, называющим обе роли. То же правило действует для `POST /data/sql`, `POST /data/query` и `POST /query/nl`. (REQ-273) [tool-verified: `provisa/api/acting_role.py`]

Поле `extensions` поддерживает протокол Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Заголовки:**

- `X-Provisa-Role` — роль, от которой выполнять (режим разработки); с аутентификацией — запрос роли, который учитывается, только если роль назначена пользователю
- `Accept` — формат ответа (см. раздел «Согласование содержимого»)
- `Authorization` — `Bearer <token>`, когда включена аутентификация
- `X-Provisa-Redirect-Format` — MIME-тип для вывода перенаправления в S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — количество строк, выше которого срабатывает перенаправление (REQ-137)
- `X-Provisa-Redirect` — `true` для безусловного принудительного перенаправления (REQ-029)

**Кеш ответов:** включается для отдельного запроса директивой операции `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` необязателен); та же директива действует для GraphQL, отправленного через Arrow Flight. Cypher включает кеш строкой `// @provisa cache=true` или `// @provisa cache_ttl=N`, gRPC — метаданными вызова `x-provisa-cache: true` или `x-provisa-cache-ttl: N`. Без неё запрос не читает кеш ответов и не пишет в него. Заголовок ответа `X-Provisa-Cache` сообщает `HIT` или `MISS`. См. [Кеш](configuration.md#per-request-opt-in). (REQ-544)

<a id="response-cache-request-headers"></a>

#### Заголовки запроса для кеша ответов {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` и `/data/jsonapi` принимают включение также в виде заголовка запроса. Каждая из форм ниже ведёт себя как `@cached(ttl: N)`: (REQ-544)

| Заголовок | Равносильно |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (без значения) | `@cached` (TTL оператора) |
| `Cache-Control: no-cache` / `no-store` | без кеша, как если бы ничего не было отправлено |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

Запрос без одного из этих заголовков и без указания в тексте не кешируется, хотя обычные HTTP-кеши кешируют по умолчанию. Настройки кеша оператора действуют так же, как для `@cached`: заголовок никогда не делает чтение более свежим, чем они допускают, и никогда не отклоняется из-за запроса более длинного `ttl`. Остальные директивы `Cache-Control` (`no-transform`, `min-fresh`, ...) игнорируются, как того требует RFC 9111 от кеша. [tool-verified: `provisa/compiler/directives.py` `request_cache_hint`]

Запрос может указать включение несколько раз: в тексте и в заголовке или в двух заголовках. Если они совпадают, запрос кешируется один раз, как запрошено. Если расходятся (например, `@cached(ttl: 60)` с `Cache-Control: max-age=5` или `x-provisa-cache-ttl: 10` с `Cache-Control: max-age=20`), запрос завершается ошибкой `400` с кодом `data.invalid_cache_control`, в которой названа каждая отправленная форма. Значение `N`, не являющееся целым числом секунд, приводит к такой же ошибке. Ни одной из форм предпочтение не отдаётся.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**Ответ (JSON inline):**

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

**Ответ (несколько корневых полей со смешанным inline/redirect):**

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

Запросы с несколькими корневыми полями выполняют каждое корневое поле независимо. Поля ниже порога перенаправления возвращаются inline; поля выше — перенаправляются. Ключ `redirects` (во множественном числе) сопоставляет имена полей с информацией о перенаправлении. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Заголовки кеша:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (при HIT) (REQ-536)

**Требуемые возможности:** `QUERY_DEVELOPMENT` для всех запросов, включая интроспекцию. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Согласование содержимого {: #content-negotiation }

| Заголовок Accept | Формат |
| --- | --- |
| `application/json` | JSON (по умолчанию) |
| `application/x-ndjson` | JSON с разделением строками |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Перенаправление {: #redirect }

Результаты, превышающие настроенный порог по количеству строк (или при `X-Provisa-Redirect: true`), записываются в S3, и возвращается предподписанный (presigned) URL. (REQ-029, REQ-044)

| Формат перенаправления | Кем записывается | Память |
| --- | --- | --- |
| `application/vnd.apache.parquet` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/x-orc` | федеративный CTAS | Не используется — данные никогда не проходят через Provisa |
| `application/json` | Provisa | Ограничено памятью |
| `application/x-ndjson` | Provisa | Ограничено памятью |
| `text/csv` | Provisa | Ограничено памятью |
| `application/vnd.apache.arrow.stream` | Provisa | Ограничено памятью |

Для крупных аналитических экспортов используйте перенаправление в Parquet или ORC. Движок федерации пишет напрямую в S3 параллельно — данные не проходят через Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Выполнить необработанный SQL через конвейер управления Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (необязательно) несёт значения заполнителей `$1…$n` инструкции, по порядку, отдельно от её текста:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

Значения связываются при выполнении инструкции — движком или драйвером источника — и никогда не записываются в инструкцию, поэтому значение не может изменить то, чем является инструкция. Они относятся ровно к одной инструкции, заполнители которой нумеруют их точно: пакет инструкций, заполнитель без значения или значение, которое не берёт ни один заполнитель, отклоняются с `400` и кодом `data.sql_parameters_do_not_fit` до регулирования или выполнения инструкции. Инструкция без заполнителей отправляется без `params`, как и раньше. (REQ-1937)

**Требуемые возможности:** `QUERY_DEVELOPMENT`.

Нарушения управления на `POST /data/sql` возвращают HTTP 403. (REQ-002, REQ-266)

**Ответ:** тот же формат, что и `/data/graphql` (строки JSON по умолчанию, согласование содержимого через `Accept`).

---

### `POST /data/query`

Единый эндпоинт запросов. Принимает GraphQL, SQL или Cypher — синтаксис определяется автоматически. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Запросы Cypher также можно отправлять на выделенный эндпоинт `POST /query/cypher`. (REQ-345)

**Тело запроса:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Возвращает `{"data": ...}` для GraphQL, `{"columns": [...], "rows": [...]}` для SQL и Cypher.

---

### `POST /data/sql/explain`

Выполнить explain или analyze для SQL-выражения через управляемый конвейер. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

Эндпоинт оборачивает **управляемый (governed)** SQL — выражение, которое фактически выполняется под ролью вызывающего, после применения RLS и маскирования — в синтаксис EXPLAIN соответствующего диалекта. То, что показывает план, — это авторизованная версия запроса, а не исходный ввод.

**Тело запроса:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Установите `analyze: true`, чтобы выполнить EXPLAIN ANALYZE. Запрос выполняется, и план содержит реальные количества строк и тайминги. Не каждый диалект поддерживает ANALYZE; см. таблицу в разделе [Планы запросов и статистика](engines.md#query-plans-and-statistics).

**Ответ:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400`, если диалект не поддерживает EXPLAIN, или если запрошен `analyze: true` для диалекта, который его не поддерживает (например, SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Возвращает текущее состояние шарда движка без его пробуждения. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

UI опрашивает этот эндпоинт, чтобы показать баннер запуска, пока движок выполняет холодный старт. Он никогда не вызывает пробуждение — опрос безопасен и не считается активностью для «жнеца простоя» (idle reaper).

**Ответ:**

```json
{"state": "ready"}
```

Возможные значения:

| Состояние | Значение |
| --- | --- |
| `always-on` | Настольная версия, self-hosted или собственный (BYO) координатор — без управления жизненным циклом |
| `ready` | Шард запущен и принимает запросы |
| `starting` | Выполняется холодный старт |
| `stopped` | Шард масштабирован до нуля |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Инициировать пробуждение движка без выполнения запроса. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Немедленно возвращает `202 Accepted`. Пробуждение выполняется в фоновом режиме. Используйте это, если хотите, чтобы движок был готов до прихода первого запроса — например, из планировщика, который выполнит запросы через несколько минут.

**Ответ:** `202 Accepted`, тело `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Автоматически сгенерированный обычный REST-эндпоинт для каждой зарегистрированной таблицы. Строка запроса сопоставляется с аргументами GraphQL, и запрос компилируется и выполняется через тот же конвейер (RLS, маскирование, маршрутизация), что и GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Параметры запроса:**

- `limit` — максимальное число строк (≥ 1)
- `offset` — пропустить строки (≥ 0)
- `fields` — имена полей через запятую в написании схемы GraphQL, в camelCase при соглашении об именах по умолчанию, например `?fields=orderId,amount` (по умолчанию — все скалярные поля). Имена полей в `filter` и `orderBy` следуют тому же написанию. JSON:API вместо этого принимает физическое имя колонки (`fields[orders]=order_id`). [tool-verified: `provisa/api/rest/generator.py:547`]
- `filter` — JSON-массив объектов фильтра `{"field", "comparator", "value"}`. Значение читается как тип своего столбца, поэтому `"100"` в числовом столбце сравнивается как 100; значение, которое тип столбца не может вместить, возвращает `400`.
- `orderBy` — JSON-массив объектов сортировки `{"field", "direction"}`
- `params` — параметры таблицы одним JSON-объектом по именам параметров, например `?params={"region":"EU"}`. У таблицы, зарегистрированной поверх API, и у таблицы-запроса есть параметр для каждого входа, который принимает её источник. Чтение без обязательного параметра возвращает `400` с его именем, как и имя, для которого у таблицы нет параметра. Каждое значение проверяется по типу своего параметра. (REQ-1986) [tool-verified: `provisa/api/_query_helpers.py`]

Значения фильтров и параметров передаются как переменные рядом с текстом GraphQL, который строит эндпоинт, поэтому значение с кавычкой сравнивается так, как написано. [tool-verified: `provisa/api/_query_helpers.py`]

Аутентифицированная роль обязательна; неаутентифицированные запросы возвращают `401`. Спецификация OpenAPI для этих маршрутов предоставляется по адресу `GET /data/rest/openapi.json`, со Swagger UI по адресу `GET /data/rest/docs`.

**Кеш ответов:** включается заголовком запроса, `Cache-Control: max-age=N` или `x-provisa-cache: true` (см. [Заголовки запроса для кеша ответов](#response-cache-request-headers)). Каждый ответ содержит `X-Provisa-Cache: HIT|MISS`, а при попадании ещё и `X-Provisa-Cache-Age`. (REQ-544, REQ-536) [tool-verified: `provisa/api/rest/generator.py`]

#### Обозреватель OpenAPI / Swagger UI {: #openapi-swagger-ui-explorer }

Страница обозревателя OpenAPI (`/app/openapi`) встраивает Swagger UI в изолированный (sandboxed) iframe. Спецификация ограничена ролью — отображаются только таблицы и столбцы, видимые текущей роли, — и опционально фильтруется по домену через селектор домена. UI автоматически переключается между светлой и тёмной темами. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

Страница загружает HTML спецификации через `fetch()`, а не напрямую через `src` iframe, поэтому запрос несёт bearer-токен сессии, а собственные относительные запросы Swagger UI корректно разрешаются относительно того же origin. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

При переходе по ссылке NL «Open in OpenAPI» страница автоматически разворачивает целевой эндпоинт, заполняет параметры запроса из URL, сгенерированного NL (например, `aggregate`, `groupBy`), и нажимает Execute — используя опрос DOM, чтобы гарантировать завершение каждого шага перед выполнением следующего. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Автоматически сгенерированный эндпоинт, совместимый с [JSON:API](https://jsonapi.org), для каждой зарегистрированной таблицы. Те же RLS, маскирование и маршрутизация, что и в GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Заголовок `Accept`:** должен включать `application/vnd.api+json` (медиа-тип JSON:API), иначе запрос вернёт `406`.

**Параметры запроса:**

- `fields[<type>]` — разреженные наборы полей, например `?fields[orders]=amount`. Имена — это физические имена колонок, например `fields[orders]=order_id`, так же как в `filter[...]`, `sort` и ключах выдаваемых атрибутов (REQ-1417). REST вместо этого принимает написание GraphQL (`fields=orderId`). [tool-verified: `provisa/api/jsonapi/generator.py:674`]
- `filter[<col>]` / `filter[<col>][<op>]` — например `?filter[region]=US`, `?filter[amount][gt]=100`. Значение читается как тип своего столбца; значение, которое тип не может вместить, возвращает `400`.
- `params` — параметры таблицы одним JSON-объектом по именам параметров, например `?params={"region":"EU"}`, как у REST-эндпоинта. Отсутствующий обязательный параметр возвращает `400` с его именем; неизвестное имя или значение неверного типа возвращает `400` с `source.parameter`, равным `params`. Ссылки пагинации сохраняют параметр. (REQ-1986) [tool-verified: `provisa/api/jsonapi/generator.py`]
- `sort` — через запятую, префикс `-` для убывания, например `?sort=-created_at,amount`
- `page[number]` / `page[size]` — пагинация
- `page[total]` — `true` подсчитывает строки, подходящие под фильтр, и возвращает `meta.total` и `links.last`. Без него ответ несёт только `links.self`, `links.first`, `links.prev` и `links.next`, и база данных не выполняет подсчёт. Любое значение, кроме `true` или `false`, возвращает `400`. Ссылки в ответе сохраняют параметр. (REQ-1197) [tool-verified: `provisa/api/jsonapi/pagination.py:58`]
- `aggregate` — агрегатные функции через запятую, выполняемые вместо получения строк: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Используйте `?aggregate=count,sum`, чтобы запросить подмножество. Ответы агрегации возвращают `data: null` с результатами в `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — имена столбцов через запятую; используется вместе с `?aggregate=` для группировки результатов. Допустимы только столбцы из перечисления `DistinctOnColumn` таблицы; сервер возвращает `400` для любого столбца, который роль не может видеть. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true`, чтобы включить скалярные столбцы базовой таблицы (и скалярные поля присоединённых измерений, названных в `include=`) внутрь массива `nodes` каждой строки группы. Требуется, когда NL-запрос с группировкой также запрашивает детали измерения. (REQ-1405)

Ответы представляют собой объекты-ресурсы с `type`/`id`/`attributes`. Ошибки следуют форме объекта ошибки JSON:API.

**Кеш ответов:** включается заголовком запроса, `Cache-Control: max-age=N` или `x-provisa-cache: true` (см. [Заголовки запроса для кеша ответов](#response-cache-request-headers)). Каждый ответ содержит `X-Provisa-Cache: HIT|MISS`, а при попадании ещё и `X-Provisa-Cache-Age`. Две расходящиеся формы возвращают `400` в виде объекта ошибки JSON:API. (REQ-544, REQ-536) [tool-verified: `provisa/api/jsonapi/generator.py`]

#### Обозреватель JSON:API {: #jsonapi-explorer }

Страница обозревателя JSON:API (`/app/jsonapi`) — это браузерный UI поверх этих эндпоинтов. Выберите таблицу из списка, сгруппированного по доменам, затем настройте:

- **Поля (Fields)** — выберите, какие столбцы включить (разреженный набор полей); оставьте все невыбранными, чтобы запросить каждый столбец
- **Связи (Relationships)** — выберите имена связей, производных от внешних ключей, для подгрузки через `?include=`
- **Фильтр (Filter)** — поле, оператор (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) и значение
- **Сортировка (Sort)** — одно поле, по возрастанию или убыванию
- **Агрегация (Aggregate)** — выберите столбцы группировки из проверенного сервером списка, затем отметьте одну или несколько агрегатных функций; при выбранных столбцах группировки флажок «Include nodes» добавляет скалярные столбцы базовой таблицы к каждой строке
- **Размер страницы (Page size)** — количество ресурсов на страницу, с навигацией первая/предыдущая/следующая/последняя

Результаты отображаются в отформатированном сводном представлении (карточки ресурсов с кликабельными якорями связей) или на вкладке необработанного JSON. Показывается актуальный URL запроса, который можно скопировать. Выбор таблицы и размер страницы сохраняются между сессиями в `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

При переходе по ссылке NL «Open in JSON:API» обозреватель заранее выбирает таблицу и заполняет средство выбора агрегации из параметров запроса, сгенерированных NL, затем автоматически выполняет запрос. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Отправить вопрос на естественном языке. Сервис запускает асинхронное задание и немедленно возвращает `202 Accepted` с `job_id`. Требует настроенного провайдера LLM в разделе конфигурации `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Тело запроса:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Возвращает `{"job_id": "<id>"}`. Превышение ограничения частоты запросов NL для роли возвращает `429` с заголовком `Retry-After`. (REQ-370)

**Получение результата:**

- `GET /query/nl/{job_id}` — опрос. Возвращает документ задания.
- `GET /query/nl/{job_id}/stream` — SSE. Одно событие `branch` на каждую цель генерации по мере завершения, затем событие `done`. (REQ-357, REQ-358)

Три цикла генерации (Cypher, GraphQL, SQL) выполняются параллельно, каждый проверяется компилятором и уточняется при ошибке. (REQ-355) Подсказка (prompt) ограничена видимой схемой роли. (REQ-356) Итоговый документ индексирует каждую ветвь по цели: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Ветвь, исчерпавшая лимит итераций, возвращает `query: null`, `result: null` и строку `error`. Каждый сгенерированный запрос выполняется в рамках прав потребителя с применением управления Stage 2 — сервис никогда не обходит управление. (REQ-359)

#### NL-группировка с деталями измерения (REQ-1405) {: #nl-group-by-with-dimension-details-req-1405 }

Когда NL-запрос с группировкой также проецирует столбцы из присоединённой таблицы измерения — например, «количество обращений по пользователю с именем и email пользователя», — исполнитель выводит пути через точку (`dim_paths`) для каждого поля на основе столбцов измерения, спроецированных в SELECT. Эти пути заполняют параметр `includeNodes=` в сгенерированных URL панелей JSON:API и OpenAPI, так что эти панели запрашивают те же поля присоединённого измерения, которые разрешили ветви SQL и GraphQL. Без этого `includeNodes=true` вернул бы только собственные скалярные поля базовой агрегатной таблицы. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

На панели gRPC сгенерированный `{Type}GroupByRequest` несёт `include_nodes` (bool) и `include` (повторяющаяся строка имён полей связи). Возвращаемый `{Type}GroupByRow` включает типизированное поле `nodes` со строками деталей измерения. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Возвращает GraphQL SDL для схемы роли. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:**

- `domain` — id доменов через запятую. Если задан, ответ фильтруется по указанным доменам и таблицам, достижимым из них.

**Ответ:** GraphQL SDL в формате `text/plain`.

---

### `GET /data/introspection`

Возвращает JSON интроспекции GraphQL, опционально отфильтрованный по домену. [tool-verified: `provisa/api/data/sdl.py:200`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Параметры запроса:** `domain` — id доменов через запятую.

**Ответ:** результат интроспекции в формате `application/json`.

---

### `GET /data/graph-schema`

Возвращает представление схемы роли в виде графа: метки узлов и типы их связей, для клиентов Cypher/графа. Включает `pk_columns` для каждой метки узла, чтобы вызывающие могли определить столбцы первичного ключа. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Ответ:** `application/json` с `node_labels` (каждая несёт `pk`/`pk_columns`) и `relationship_types`.

Каждый тип связи также несёт `junction_table_name` и `properties` (REQ-1586). Для рёбер на основе связующей таблицы (junction) первое поле называет ассоциативную таблицу, через которую проходит обход, а второе перечисляет столбцы этой таблицы, доступные для чтения как `r.attr` и для фильтрации в `WHERE`; для рёбер на основе внешнего ключа имя равно `null`, а список свойств пуст — так клиент отличает одно от другого. Сама связующая таблица никогда не является меткой узла — она представляет ребро, поэтому у неё нет «пилюли» в графовом клиенте и строки в `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Возвращает id доменов, доступных запрашивающей роли. [tool-verified: `provisa/api/data/sdl.py:116`]

**Заголовки:** `X-Provisa-Role: <role_id>` (обязателен)

**Ответ:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Возвращает строку текущей версии схемы. Объединяет одноразовый идентификатор запуска (nonce) со счётчиком пересборок. Клиенты используют это, чтобы инвалидировать кеши схемы после перезапуска сервера. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Ответ:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Возвращает каталог роли, от которой выполняется запрос: таблицы, которые обслуживаются этой роли, в том виде, в каком их даёт перечень Arrow Flight. (REQ-128) [tool-verified: `provisa/api/data/endpoint_dev.py`]

**Ответ:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` — схема Arrow таблицы в кодировке Arrow IPC: каждая колонка под именем, по которому её читает SQL, с её зарегистрированным типом, описанием и метаданными ключей (`primary_key`, `references`) в метаданных поля. Команды и метрики не перечисляются.

Роль — та, что установил уровень аутентификации (`X-Provisa-Role` может запросить имеющуюся роль или набор через запятую). Развёртывание без провайдера аутентификации перечисляет весь каталог, как и перечень Flight. Драйвер JDBC читает этот маршрут, когда порт Flight недоступен.

---

### `GET /data/proto/{role_id}`

Возвращает автоматически сгенерированный файл `.proto` для роли. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Ответ:** схема protobuf в формате `text/plain`.

`{role_id}` — это одна роль, которой обладает вызывающий, или набор таких ролей через запятую (`analyst,org_admin`), который обслуживается как мета-роль набора: объединение того, что предоставлено его участникам. Роль, которой вызывающий не обладает, отклоняется с `403 auth.role_not_assigned`, а идентификатор мета-роли, указанный напрямую, — с `403 auth.meta_role_named`. `?domains=a,b` сужает proto до этих доменов; домен, которого роль не достигает, отклоняется с `403 data.domain_not_accessible`. Роль без поверхности данных получает `404 data.no_proto_for_role`, неизвестная роль — `404 data.no_role`, а ещё не построенная модель — `503 data.schema_cache_not_ready`. (REQ-1620, REQ-273) [tool-verified: `provisa/api/data/endpoint_dev.py`, `provisa/api/acting_role.py`]

Каждая таблица в схеме роли порождает proto-`message`. Связи порождают вложенные поля сообщений. Сервис перечисляет RPC только для таблиц в корне схемы роли — тех же, что в её типе `Query` GraphQL; связанная таблица имеет своё сообщение и достигается через свою связь. [tool-verified: `provisa/grpc/proto_gen.py`]

Добавьте `?domains=a,b`, чтобы ограничить файл этими доменами. Тогда сервис перечисляет RPC для собственных таблиц выбранных доменов; связанные с ними таблицы присутствуют как сообщения. [tool-verified: `provisa/api/data/endpoint_dev.py`]

Файл следует за моделью. Он пересоздаётся вместе со схемой роли при каждом изменении модели или роли, в том числе для мета-роли. Отображение типов: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Поток Server-Sent Events для уведомлений об изменениях в реальном времени из таблицы. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

Доставка уведомлений использует подключаемого (pluggable) провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY` (через asyncpg), источники MongoDB используют Change Streams (`collection.watch()`), а источники Kafka используют группы потребителей. Каждый провайдер реализует общий асинхронный интерфейс наблюдения (watch). Фильтрация RLS и валидация схемы применяются независимо от провайдера. (REQ-258) Источники WebSocket и RSS также поддерживаются. (REQ-338, REQ-342)

**Заголовок — `X-Provisa-Sink`:** установите на цель Kafka (например, `kafka://broker:9092/topic`), чтобы перенаправить события изменений в приёмник (sink) Kafka вместо ответа SSE. Сервер запускает потребителя приёмника и возвращает `202 Accepted` вместо открытого потока. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

### `POST /data/sources/{source_id}/signal`

Сообщает Provisa, что источник изменился, без строк: приёмник сигнала изменения `signal`. Каждая таблица источника, сигнал изменения которой `signal` (собственный или, если его нет, сигнал источника), считается записанной, как при записи через Provisa: её закешированные ответы отбрасываются, материализованные представления над ней устаревают, и запрашивается сборка её реплики. Сборка таблицы, защищённой от нагрузки, ждёт своего внепикового окна. `?table=schema.table` сигнализирует одну таблицу. (REQ-1149) [tool-verified: `provisa/api/data/signal.py`, `provisa/api/data/table_written.py`]

Роли вызывающего нужно право записи, и она должна достигать домена каждой таблицы, которую сигнализирует. Ответ — `202 Accepted` с сигнализированными таблицами:

```json
{"signalled": ["public.orders"]}
```

Несуществующий источник или `table`, которой у него нет, отвечает `404`; источник без таблиц на сигнале изменения `signal` отвечает `409`, потому что обновлять было бы нечего. [tool-verified: `tests/unit/test_signal_webhook.py`]

---

## Административные REST-эндпоинты {: #admin-rest-endpoints }

### Конфигурация {: #config }

#### `GET /admin/config`

Скачать текущий `provisa.yaml` как `application/x-yaml` с заголовком `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Загружает исправленный YAML конфигурации. Сервер пишет резервную копию `.bak`, сохраняет новый файл, перезагружает из него настройки развёртывания и применяет его к хранилищу модели как явное однократное наполнение: то, что объявляет файл, добавляется или обновляется через хранилище модели, и ничего не удаляется. Последующий перезапуск читает настройки файла и никогда больше не применяет его модель; после первого запуска моделью владеет только хранилище модели. (REQ-164, REQ-1919) [tool-verified: `provisa/api/admin/settings_router.py` `upload_config`]

**Тело запроса:** необработанное содержимое YAML.

**Ответ:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

При сбое перезагрузки: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Скачивает модель в текущем виде, записанную как файл конфигурации: настройки файла развёртывания с каждым разделом модели, прочитанным из хранилища модели, — хранилища, регионы, источники, домены, роли, продукты данных, термины глоссария, таблицы и их колонки, связи, метрики, теги и их назначения, фильтры строк, команды, webhook, запланированные триггеры и правила именования. Применённый к пустому хранилищу модели, файл строит ту же модель. То, что развёртывание добавляет в каждое хранилище (встроенные источники и их таблицы, системные домены, `org_admin` и `platform_admin`), не включается. (REQ-164, REQ-1919) [tool-verified: `provisa/api/admin/config_export.py` `build_live_config`, `provisa/core/store_config.py`]

**Параметр запроса:** `sections` (необязательно) — список разделов модели через запятую, например `sources,tables`. Записываются только эти разделы; неизвестное имя даёт ответ 400 `settings.export_unknown_section`.

Endpoint открыт в любом развёртывании для вызывающего с правом на настройки развёртывания; раздел Файл конфигурации на странице администратора предлагает его как **Экспорт модели**.

Возвращает `application/x-yaml` с `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Возвращает обе стороны разницы конфигурации — `original` (базовая версия при запуске) и `current` (действующее состояние) — нормализованные одинаково, так что сравнение показывает только реальные изменения, а не переупорядочивание или дрейф комментариев. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Ответ:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Сгенерировать унифицированный diff-патч от базовой версии к отправленной конфигурации. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Отправьте пересмотренный YAML как тело запроса. Ответ — это файл `text/x-patch` (`provisa.config.patch`), который `git apply` или `patch` могут применить напрямую — полезно для фиксации изменений конфигурации, сделанных через UI, через CI/CD-конвейер.

---

### Настройки {: #settings }

#### `GET /admin/settings`

Возвращает текущие настройки платформы в формате JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Обновить настройки платформы во время выполнения. Все поля опциональны — обновляются только ключи, присутствующие в теле. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`: `domain_prefix`, `convention` — записывает в файл конфигурации и вызывает согласование схемы (REQ-253)
- `relationships`: `auto_track_fk` — управляет только отслеживанием внешних ключей. Связь на основе связующей таблицы (junction) объявляется при регистрации таблицы и никогда не выводится автоматически, поэтому эта настройка на неё не влияет. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Ответ:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Модели ИИ {: #ai-models }

#### `GET /admin/ai-models`

Возвращает назначения ИИ-моделей действующей организации, реестр векторных моделей и ограничение частоты запросов NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

Ключи API никогда не возвращаются обратно — `api_keys_set` сообщает только, настроен ли ключ для каждого вендора. Изменения вступают в силу со следующего запроса; перезапуск не требуется. (REQ-1349)

#### `PUT /admin/ai-models`

Обновить назначения ИИ-моделей организации, реестр векторных моделей или ограничение частоты запросов NL. Вступает в силу со следующего запроса. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Возвращает имена моделей, которые вендор в данный момент предоставляет, для средства выбора модели. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

Список читается напрямую из собственного API list-models вендора с использованием ключа организации — или учётных данных развёртывания, если ключ организации не задан. Модель, выпущенная после сборки этой версии, становится доступной для выбора в тот же день, когда её начинает предоставлять вендор.

Возвращает `400`, если вендор не публикует API list-models (в этом случае введите имя модели напрямую) или если нет доступного ключа. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Движок федерации {: #federation-engine }

#### `GET /admin/federation-engine`

Возвращает текущий выбор движка федерации, его конфигурацию подключения и полный реестр доступных для выбора движков. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

Ключ `current` — это движок, работающий прямо сейчас; `persisted` — то, что записано в файл конфигурации и будет загружено при следующем перезапуске. Они расходятся, если конфигурация была изменена, а сервис ещё не перезапущен.

#### `PUT /admin/federation-engine`

Сохранить выбор движка федерации. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Тело запроса:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

Выбор записывается в конфигурацию платформы. Вступает в силу после следующего перезапуска сервиса — движок выбирается один раз при загрузке.

---

### Политика домена {: #domain-policy }

#### `POST /admin/domain-policy`

Изменить политику доменов действующей организации (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Переключение ничего не удаляет. Оно отклоняется с `409` и кодом `settings.domain_policy_catalog_exists`, пока у действующей организации есть каталог: зарегистрированная таблица, источник или домен, которые развёртывание не добавило само. `params` отказа подсчитывают каждый вид (`tables`, `sources`, `domains`). Сначала удалите эти объекты, каждый своим действием, затем переключайте. Если файл конфигурации объявляет часть этого каталога, код — `settings.domain_policy_catalog_in_config`: следующая загрузка восстановила бы его, поэтому политика задаётся в файле конфигурации. (REQ-1919)

**Тело запроса:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` очищает переопределение организации и возвращается к настройке уровня развёртывания. `use_domains: false` требует `default_domain` (единственное имя домена, в который попадают все таблицы). Пересборка каталога синхронна; ответ возвращается, когда схемы готовы.

---

### Наблюдаемость {: #observability }

#### `GET /admin/traces/recent`

Возвращает до N последних завершённых спанов из буфера спанов в памяти. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Параметры запроса:** `limit` (по умолчанию 50, максимум 200)

**Ответ:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Горячая перезагрузка именованного каталога в координаторе движка федерации через его REST API. Переподключает внутреннее соединение Provisa и заново выполняет DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Параметры запроса:** `catalog` (по умолчанию `"otel"`)

**Ответ:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Перезапустить контейнер движка федерации (только для однонодовой разработки). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Параметры запроса:** `container` (по умолчанию берётся из переменной окружения `QUERY_ENGINE_CONTAINER`, затем `"trino"`)

---

### Обнаружение {: #discovery }

#### `POST /admin/discover/relationships`

Инициировать обнаружение связей. Всегда выполняет интроспекцию внешних ключей из движка федерации. (REQ-018) Выполняет вывод LLM, если задан `ANTHROPIC_API_KEY`. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Тело запроса:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` должен быть одним из `"table"`, `"domain"`, `"cross-domain"`. Для области `"table"` требуется `table_id` (целое число). Для области `"domain"` требуется `domain_id`.

**Ответ:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Список ожидающих кандидатов связей. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Принять кандидата и зарегистрировать его как связь. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Тело запроса (опционально):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Отклонить кандидата. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Тело запроса:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Возвращает количество отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Удалить всех отклонённых кандидатов. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Обход источника (Crawl) {: #source-crawl }

#### `POST /admin/sources/crawl`

Обойти источник данных, чтобы выполнить интроспекцию его схемы и зарегистрировать таблицы. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Поиск таблиц источника {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

Найти доступные (ещё не зарегистрированные) таблицы в источнике по имени. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Профилирование таблиц {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

Выполнить профилирование столбцов зарегистрированной таблицы — кардинальность, мин/макс, доля null-значений. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Описания источников {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

Сгенерировать описания таблиц и столбцов источника с помощью LLM. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Объектное хранилище (REQ-1046, REQ-1048, REQ-1049) {: #object-storage-req-1046-req-1048-req-1049 }

#### `GET /admin/org-storage`

Сообщает объём хранилища действующей организации относительно её платформенной квоты и указывает, зарегистрировала ли организация собственное хранилище. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Когда у организации зарегистрирован собственный DSN, её материализации попадают туда и больше не учитываются в квоте. Сам DSN никогда не возвращается.

#### `PUT /admin/org-storage`

Зарегистрировать (или очистить) собственное хранилище материализаций организации. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Тело запроса:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

DSN проверяется движком федерации перед принятием — непригодный DSN отклоняется при регистрации, а не спустя часы при обновлении. Значение шифруется в состоянии покоя и никогда не возвращается через GET.

Отправьте `storage_url: null`, чтобы очистить собственное хранилище организации и вернуть её материализации в платформенное хранилище (и квоту). Среда выполнения организации пересобирается в том же вызове, так что новое хранилище действует немедленно. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Шифрование организации (REQ-1574) {: #org-encryption-req-1574 }

#### `GET /admin/org-encryption`

Возвращает текущий статус ключа организации: отпечаток, id и происхождение. Никогда не возвращает материал ключа. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Когда организация не задала ключ, возвращает `{"configured": false}`. Каждая организация начинает в этом состоянии и наследует ключ развёртывания.

#### `PUT /admin/org-encryption`

Задать или сменить ключ шифрования организации в состоянии покоя. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Тело запроса:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Опустите `key_b64`, чтобы Provisa сгенерировала ключ — самый безопасный путь, так как ключ никогда не появляется в буфере обмена или журнале запросов. Указание `key_b64` означает использование собственного ключа.

Смена ключа добавляет новую активную запись в кольцо ключей и сохраняет старую, так что данные, записанные под предыдущим ключом, остаются читаемыми. Смена ключа — это не перешифрование. Эндпоинта удаления нет: удаление последнего ключа сделало бы каждую обёрнутую полезную нагрузку нечитаемой. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Действующее кольцо ключей переподключается в том же вызове, так что следующая зашифрованная запись немедленно использует новый ключ.

---

### Импорт Hasura / DDN (REQ-1483) {: #hasura-ddn-import-req-1483 }

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

Ничего не сохраняется. Предпросмотр не кешируется на стороне сервера; `apply` принимает предоставленный вами YAML, так что применяется именно то, что было проверено (и, возможно, отредактировано).

#### `POST /admin/import/hasura/apply`

Загрузить ранее предпросмотренную конфигурацию в действующую организацию. [tool-verified: `provisa/api/admin/import_router.py`]

**Тело запроса:**

```json
{"config_yaml": "<yaml string>"}
```

Импорт — это явное однократное наполнение хранилища модели действующей организации, то же применение, что и `PUT /admin/config`: он добавляет и обновляет через хранилище модели то, что объявляет импортированная конфигурация, с защитами и версиями, и не удаляет ничего, чего не упоминает. Поля `replace` нет. Каталоги движка объявленных источников, пулы организации и её схемы пересобираются до возврата ответа. (REQ-1919) [tool-verified: `provisa/api/app_loaders.py` `apply_configuration`]

**Ответ:** `{"summary": {...}}` — количество того, что объявляет импортированная конфигурация.

---

### Обмен Apache Ossie (REQ-1316, REQ-1321) {: #apache-ossie-interchange-req-1316-req-1321 }

#### `GET /admin/ossie`

Экспортировать управляемую модель организации в виде YAML-документа Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Документ формируется из действующего состояния при каждом запросе — никогда не кешируется, — поэтому не может устареть. Таблицы становятся объектами `dataset`, столбцы становятся объектами `field`, а связи сопоставляются с объектами `relationship` Ossie.

Возвращает `text/yaml` с `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

Скачивает регулируемый каталог как `sources.yml` dbt в том виде, в каком он обслуживается одной роли. (REQ-1967) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Параметр запроса:** `role` — роль, которая есть у вызывающего, или любая роль для обладателя `access_config`.

Возвращает `text/yaml` с `Content-Disposition: attachment; filename=provisa.sources.yml`. Неизвестная роль — это `404` с кодом `dbt_sources.unknown_role`. Что содержит файл, см. в разделе [интеграции](integrations.md#dbt-sources-file-req-1967).

#### `POST /admin/ossie/import`

Разобрать документ Ossie в формате YAML или JSON и вернуть предлагаемые регистрации таблиц и связей. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Тело запроса:** необработанный Ossie YAML или JSON. Формат определяется автоматически.

**Ответ:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Ничего не регистрируется. Используйте экран проверки в admin UI, чтобы принять или урезать предложения до срабатывания любой мутации.

---

### Действия (Функции и вебхуки) {: #actions-functions-and-webhooks }

Все эндпоинты находятся под префиксом `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Каждый вызов — из GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` и Provisa gRPC — проходит через единый управляемый исполнитель, который единообразно применяет назначение команды (`visible_to`), право `write` для мутации и управление. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] См. [docs/integrations.md](integrations.md#invoking-commands-across-protocols) для синтаксиса вызова по каждому протоколу.

#### `GET /admin/actions`

Возвращает все отслеживаемые функции БД и вебхуки. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Каждый объект вебхука несёт булево поле `approved`. Вебхук становится одобренным, когда стюард выполняет его запрос на создание (REQ-209); объявленные в конфигурации вебхуки одобряются автоматически. Неодобренный вебхук зарегистрирован, но не предоставляется ни на одной поверхности. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Зарегистрировать отслеживаемую функцию (команду). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Ключевые поля:**

| Поле | Обязательно | Описание |
| --- | --- | --- |
| `name` | Да | Уникальное имя команды |
| `kind` | Да | `"query"` → поле GraphQL Query; `"mutation"` → поле Mutation |
| `implKind` | Нет | Способ выполнения команды — см. таблицу ниже (по умолчанию `source_procedure`) |
| `binding` | Нет | Детали подключения, специфичные для `implKind` (объект JSON) |
| `returnSchema` | Нет | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — делает команду возвращающей набор строк на каждой поверхности |
| `arguments` | Нет | Определения аргументов `[{name, type}]`; порядок позиций важен для вызывающих через SQL и Bolt |
| `visibleTo` | Нет | ID ролей, которым назначена команда (пусто: каждая роль). `mutation` дополнительно требует права `write` |
| `domainId` | Нет | Домен для размещения в GraphQL и контроля доступа |

**Значения `implKind`:**

| `implKind` | Что выполняется | Поля `binding` |
| --- | --- | --- |
| `source_procedure` | Хранимая процедура в зарегистрированном источнике (по умолчанию) | `sourceId`, `schemaName`, `functionName` |
| `script` | Скрипт на стороне сервера | `script` |
| `http` | Исходящий HTTP-вызов | `url`, `method` |
| `grpc` | Исходящий gRPC-вызов к внешнему серверу | `target`, `method` |
| `python` | Python-функция, размещённая в Provisa (REQ-885) | `callable` (например, `"demo.py_functions:random_dataset"`) |

Демонстрационные команды `random_python_set` (`implKind: python`) и `random_grpc_set` (`implKind: grpc`) показывают на практике команды, возвращающие набор строк, с `returnSchema`; обе находятся в `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Обновить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Удалить отслеживаемую функцию по имени. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Зарегистрировать отслеживаемый вебхук. (REQ-209) Регистрация или обновление вебхука ставит в очередь запрос на одобрение стюардом — вебхук становится активным на всех поверхностях только после одобрения стюардом. Объявленные в конфигурации вебхуки одобряются автоматически. **Поля тела запроса:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Обновить отслеживаемый вебхук по имени. Любое редактирование сбрасывает одобрение в статус ожидания до повторного одобрения. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Удалить отслеживаемый вебхук по имени. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Протестировать действие (функцию или вебхук) по имени. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Роли {: #roles }

Все эндпоинты находятся под префиксом `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Список всех ролей |
| `POST` | `/admin/roles/` | Создать роль |
| `PUT` | `/admin/roles/{role_id}` | Обновить роль |
| `DELETE` | `/admin/roles/{role_id}` | Удалить роль |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Пользователи {: #users }

Все эндпоинты находятся под префиксом `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/users/` | Создать локального пользователя |
| `GET` | `/admin/users/` | Список локальных пользователей |
| `GET` | `/admin/users/{user_id}` | Получить пользователя |
| `PUT` | `/admin/users/{user_id}` | Обновить пользователя |
| `PATCH` | `/admin/users/{user_id}/password` | Сменить пароль |
| `DELETE` | `/admin/users/{user_id}` | Удалить пользователя |
| `GET` | `/admin/users/{user_id}/assignments` | Список назначенных ролей |
| `POST` | `/admin/users/{user_id}/assignments` | Добавить назначение роли |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Удалить назначение роли |

---

### Организации {: #organizations }

Все эндпоинты находятся под `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Список организаций |
| `POST` | `/admin/orgs/` | Создать организацию |
| `PUT` | `/admin/orgs/{org_id}` | Обновить организацию |
| `DELETE` | `/admin/orgs/{org_id}` | Удалить организацию |
| `GET` | `/admin/orgs/{org_id}/members` | Список участников |
| `POST` | `/admin/orgs/{org_id}/members` | Добавить участника |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Удалить участника |

---

### Приглашения {: #invites }

Все эндпоинты находятся под `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Создать приглашение |
| `GET` | `/admin/invites/` | Список ожидающих приглашений |
| `DELETE` | `/admin/invites/{token}` | Отозвать приглашение |

---

### Admin GraphQL

#### `POST /admin/graphql`

Эндпоинт Strawberry GraphQL для всех операций admin: CRUD источников и таблиц, управление связями, конфигурация доменов, правила RLS, контроль кеша, соглашения об именовании, управление запланированными задачами и компиляция запросов. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

Полный справочник схемы — каждое поле Query, поле Mutation и тип ввода/вывода — см. в разделе [Справочник Admin GraphQL API](admin-graphql.md).

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

### Настройка {: #setup }

#### `GET /setup/status`

Возвращает статус первоначальной настройки. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Завершить первоначальную настройку. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Проверка работоспособности {: #health-check }

#### `GET /health` или `HEAD /health` {: #get-health-or-head-health }

Возвращает `{"status": "ok"}`. Всегда без аутентификации. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Ответы об ошибках {: #error-responses }

| Статус | Значение |
| --- | --- |
| 400 | Некорректный запрос, ошибка валидации или ошибка разбора SQL |
| 401 | Отсутствующий или недействительный токен аутентификации |
| 403 | Недостаточно возможностей; нарушение управления |
| 404 | Роль, ресурс или файл конфигурации не найдены |
| 422 | Отсутствует обязательный заголовок (например, `X-Provisa-Role`) |
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

Все остальные ошибки используют: `{"detail": "<message>"}`.

---

## Эндпоинт Arrow Flight {: #arrow-flight-endpoint }

Порт `8815`. Нативный колоночный транспорт Arrow поверх gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Запросы и обнаружение каталога доступны на одном и том же соединении. Полный конвейер управления (RLS, маскирование, сэмплирование) применяется к каждому запросу. (REQ-130, REQ-143)

Обнаружение каталога (`list_flights`, `get_flight_info`, `get_schema` и тикет без `query`) перечисляет то, что предоставлено роли вызывающего. При включённой аутентификации три вызова без тикета берут учётные данные из заголовка вызова `authorization: Bearer`, а роль — из `x-provisa-role`, и без учётных данных отклоняются; см. [Интеграции](integrations.md). (REQ-1263, REQ-127) [tool-verified: `provisa/api/flight/server.py`]

**Формат тикета** (JSON):

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

Когда доступен прокси Zaychik Flight SQL (порт 8480), пакеты записей передаются потоком от начала до конца без полной материализации. (REQ-144) При недоступности Zaychik происходит откат к материализации через слой федеративных запросов. (REQ-146)

**Предупреждения.** То, что ответ сообщает о себе, передаётся в потоке как `app_metadata` пакета без строк: `{"provisa_warnings": [{"code", "params", "message"}]}`. Предупреждения, известные до чтения строк, приходят в пакете без строк перед ними. Предупреждение, известное только после чтения строк, приходит во втором пакете без строк, последнем в потоке: `statement.rows_cut` говорит, что ответ был обрезан на лимите строк и подходят ещё строки (REQ-1949). Читайте `app_metadata` в каждом пакете, включая последний. [tool-verified: `provisa/api/flight/compression.py`]

---

## Эндпоинт Protobuf gRPC {: #protobuf-grpc-endpoint }

Порт `50051` (переопределяется переменной окружения `GRPC_PORT` или конфигурацией `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Передайте роль в ключе метаданных gRPC `x-provisa-role`. Если он отсутствует, сервер прерывает выполнение с `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Скачайте специфичный для роли proto по адресу `GET /data/proto/{role_id}`. Отображаются только таблицы и столбцы, видимые этой роли. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Каждая таблица в корне схемы роли порождает потоковый RPC `Query{TypeName}`. Это те таблицы, которые перечисляет тип `Query` GraphQL этой роли. Таблица, до которой роль добирается только через связь, не имеет собственного RPC, так же как у неё нет корневого поля в GraphQL. RPC `Insert{TypeName}` существуют ради симметрии схемы, но завершаются с `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/proto_gen.py`, `provisa/grpc/server.py`]

**Параметры.** У таблицы с параметрами (таблицы, зарегистрированной поверх API, или таблицы-запроса) есть сообщение `{TypeName}Params` с необязательным полем на каждый параметр, названным без префикса `_nf_`, и `Query{TypeName}Request` несёт его как `params`. Каждое заданное там значение доходит до источника как параметр, связанный в инструкции; имя, не являющееся параметром таблицы, отклоняется с `INVALID_ARGUMENT`, а чтение без обязательного параметра отклоняется с его именем. HTTP-прокси gRPC принимает те же значения в объекте `params` тела (HTTP 400, код `data.invalid_parameter`, для неизвестного имени или значения неверного JSON-типа). (REQ-1986, REQ-1991) [tool-verified: `provisa/grpc/proto_gen.py`, `provisa/grpc/query_ir.py`]

Метод, не входящий в сервис роли вызывающей стороны, отвечает `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` включён для обнаружения сервисов без предварительно скомпилированного proto. (REQ-529) Рефлексия описывает каждому вызывающему сервис той роли, от имени которой он действует: тот же `.proto`, который эта роль скачивает через `GET /data/proto/{role_id}`. Она перечисляет по именам сервис Provisa и сервис рефлексии и описывает файл сервиса Provisa. [tool-verified: `provisa/grpc/server.py`]

Вызов рефлексии идентифицируется так же, как любой другой вызов gRPC. При включённой аутентификации роль берётся из учётных данных вызывающего. При отключённой аутентификации укажите роль в `x-provisa-role`; в развёртывании, обслуживающем более одной организации, укажите также организацию в `x-provisa-org`. Вызов без них отклоняется с `UNAUTHENTICATED`. При отключённой аутентификации рефлексия остаётся выключенной, пока не задан параметр `server.grpc_allow_unsecured_reflection`. [tool-verified: `provisa/grpc/server.py`]

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

Сервер gRPC запускается, когда модель, загруженная при старте, определяет сервис gRPC. (REQ-529) Далее каждый вызов обслуживается по модели в её текущем состоянии: таблица, зарегистрированная или изменённая во время работы сервера, обслуживается и описывается уже следующим вызовом, без перезапуска. [tool-verified: `provisa/api/app_startup.py`, `provisa/grpc/descriptor.py`]

#### RPC агрегации и группировки (REQ-1359, REQ-1361, REQ-1405) {: #aggregate-and-group-by-rpcs-req-1359-req-1361-req-1405 }

Когда у таблицы установлен `enable_aggregates`, сгенерированный proto включает два дополнительных RPC наряду с `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — возвращает агрегатные скаляры для таблицы (`count`; `sum`, `avg`, `stddev`, `variance` для каждого числового столбца; `min`, `max` для каждого сравнимого столбца)
- **`Query{TypeName}GroupBy`** — возвращает одну строку на ключ группы с агрегатными подполями и, опционально, скалярами базовой таблицы и строками присоединённого измерения в поле `nodes`

Оба используют тот же конвейер компилятора для агрегации, что и корневые поля GraphQL `{field}_aggregate` и `{field}_group_by` — отдельной реализации агрегации нет. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Поле `funcs` (REQ-1361).** Сообщение запроса принимает повторяющееся строковое поле `funcs`. Допустимые значения: `count`, `sum`, `avg`, `stddev`, `variance`, `min` и `max`. Когда `funcs` опущено, запрашивается каждая функция, которую схема предоставляет для этой таблицы. Когда оно задано, отображаются только названные функции. Если ни одна из названных функций не применима к типам столбцов таблицы, запрос откатывается к `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Поля `include_nodes` и `include` (REQ-1405).** Запросы `Query{TypeName}GroupBy` могут установить `include_nodes: true`, чтобы включить скалярные столбцы базовой таблицы в поле `nodes` каждой строки. Повторяющееся строковое поле `include` называет поля связей «многие к одному», чьи скалярные столбцы также вкладываются внутрь `nodes`. Это соответствует поведению `?includeNodes=` / `?include=` в JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Драйвер JDBC {: #jdbc-driver }

Драйвер JDBC Provisa (`provisa-jdbc-<release>.jar`) предоставляет семантический каталог инструментам BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL подключения:** `jdbc:provisa://host:port` (REQ-131)

Домены сопоставляются со схемами JDBC. (REQ-127) Таблицы используют свои зарегистрированные псевдонимы. Столбцы используют псевдонимы и отображают описания как `REMARKS`. (REQ-128) Стандартные методы метаданных (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) предоставляют семантические связи как метаданные PK/FK.

**Поддержка SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

По умолчанию драйвер запрашивает перенаправление Arrow IPC. Результаты передаются потоком пакет за пакетом через `ArrowStreamReader`, ограничены одним пакетом записей в памяти. (REQ-293)

---

## Формат аргумента `orderBy` {: #orderby-argument-format }

Аргумент `order_by` использует объекты `{column: direction}` с перечислением направления из 6 значений: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Поддерживаемые направления: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Подписки {: #subscriptions }

Подписки SSE доступны по адресу `GET /data/subscribe/{table}`. (REQ-219, REQ-258) Доставка уведомлений использует подключаемого провайдера, выбираемого по типу источника: источники PostgreSQL используют `LISTEN/NOTIFY`, источники MongoDB используют Change Streams, а источники Kafka используют группы потребителей. Фильтрация RLS и валидация схемы применяются независимо от провайдера. Источники WebSocket и RSS также поддерживаются через тот же эндпоинт. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Бизнес-глоссарий (REQ-1387) {: #business-glossary-req-1387 }

Бизнес-глоссарий сопоставляет физические имена полей — такими, какие они есть в исходных базах данных, — с общим человеко-понятным словарём. Каждый столбец, зарегистрированный в модели, автоматически получает термин. Ручной ввод для заполнения глоссария не требуется; кураторы добавляют определения, связи и экспертов поверх того, что выводит система.

### Как выводятся термины {: #how-terms-are-derived }

Когда Provisa регистрирует или обновляет столбцы таблицы, `normalize_term` (`provisa/core/glossary.py`) выполняется для каждого имени столбца и формирует каноническую фразу. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

Нормализация применяет пять правил последовательно:

1. Разбиение по границам camelCase и символам-разделителям (`_`, `-`, `.`, `/`, пробел).
2. Приведение результата к нижнему регистру.
3. Расширение фиксированной таблицы сокращений (например, `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Удаление конечного **токена-заместителя (proxy token)** (`identifier`, `code`, `index` или `reference`) — столбец, названный по своему ключу или коду, указывает на лежащее в основе понятие через значение-заместитель, поэтому термином должно быть само понятие. Последний оставшийся токен никогда не удаляется.
5. Уточнение **слишком общей фразы** концептом таблицы. Когда полная нормализованная фраза представляет собой голое слово-атрибут (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name` и подобные), термин становится `<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Один общий термин `name` для не связанных между собой таблиц объединил бы разные значения; уточнение вместо этого связывает каждый столбец с его охватывающим концептом. Концепт таблицы — это бизнес-имя таблицы, нормализованное с единственным числом главного существительного (`order_lines` → `order line`).

Псевдостолбцы нативных фильтров (с префиксом `_nf_`, или любой столбец, несущий `native_filter_type`) являются механизмом параметров запроса, а не бизнес-полями, и не порождают термины.

Поскольку `id`, `key`, `pk` и `sk` — все раскрываются в `identifier` до проверки на заместитель, три физически разных имени столбца попадают точно на один и тот же термин:

| Физическое имя | После нормализации |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Первые три схлопываются в один термин. `transaction amount` сохраняет оба токена, потому что `amount` не является заместителем. Голый столбец `id` — без предшествующих токенов — не может быть удалён; он нормализуется в `identifier`, так что термин остаётся непустым. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Жизненный цикл {: #lifecycle }

Термины **выводятся из членства в модели**, а не создаются пользователями по запросу. Репозиторий таблиц — единственный путь записи: `sync_table_refs` выполняется внутри каждого upsert набора столбцов, а `sweep_refless_terms` выполняется после любого пути удаления. [tool-verified: `provisa/core/repositories/glossary.py`]

**Когда добавляется столбец:** Provisa ищет нормализованный термин по имени. Если он уже существует, столбец получает на него ссылку (ref) (и если термин был устаревшим (deprecated), он восстанавливается — `deprecated` сбрасывается обратно в `False`). Если термина ещё не существует, он создаётся.

**Когда столбец уходит** (изменение схемы или удаление таблицы): его ссылка удаляется, а термин **улаживается (settled)** по правилу «удалить или пометить устаревшим». Корневой (rooted) термин без оставшихся ссылок удаляется полностью — вместе со своими рёбрами и назначениями экспертов — если только удаление не оставит абстрактный термин отсоединённым от всех корневых терминов (нет пути через граф терминов). В этом случае термин **помечается устаревшим** (`deprecated=True`), а не удаляется, чтобы якорь абстрактного термина в графе сохранился.

Абстрактные термины никогда не удаляются автоматически; они существуют вне физического жизненного цикла и удаляются только явно через admin API.

**Восстановление:** если нормализованное имя устаревшего термина появляется снова (столбец перерегистрируется), пометка с термина снимается, и его ссылки снова начинают накапливаться.

### Эндпоинты курирования {: #curation-endpoints }

Все эндпоинты находятся под `/admin/glossary`. Они требуют доступа `org_admin` и настроенной организации. Каждая мутация вызывает публикацию метаданных. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Метод | Путь | Описание |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Список терминов. Параметры запроса: `q` (поиск по имени/определению), `include_deprecated` (по умолчанию `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Получить детали термина: определение, физические ссылки, типизированные рёбра, эксперты |
| `POST` | `/admin/glossary/terms` | Создать абстрактный термин — пользовательский словарь без физических ссылок |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Переименовать, задать определение или переключить исключение из экспорта |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Удалить термин без физических ссылок |
| `POST` | `/admin/glossary/refs/move` | Переместить одну физическую ссылку на другой термин (консолидация) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Добавить типизированное ребро связи между двумя терминами |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Удалить ребро (параметры запроса: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Отметить пользователя как эксперта или автора для термина |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Удалить назначение эксперта/автора пользователя |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Составить черновик определения для одного термина с использованием ИИ-модели организации — возвращает только текст, ничего не сохраняется до сохранения |
| `POST` | `/admin/glossary/definitions/generate` | Сгенерировать и сохранить определения для каждого термина, у которого их нет — никогда не перезаписывает текст, написанный человеком |
| `POST` | `/admin/glossary/relationships/generate` | Предложить и сохранить типизированные рёбра по всему глоссарию с использованием ИИ-модели организации |

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

Перемещение ссылки улаживает теряющий термин по правилу «удалить или пометить устаревшим». Используйте это для консолидации двух терминов, которые нормализация оставила раздельными — например, после того как источник использует нестандартное сокращение, выпадающее из таблицы расширений.

Удаление корневого термина (с физическими ссылками) возвращает `400 glossary.invalid`. Сначала удалите или переместите все ссылки.

**Поле `export_excluded` в `PATCH /admin/glossary/terms/{term_id}`:**

```json
{"export_excluded": true}
```

Установка `export_excluded` в `true` исключает термин из всех снимков экспорта метаданных, независимо от его физических ссылок или абстрактного статуса. Возврат в `false` восстанавливает термин в снимке при следующей публикации. Данные курирования (определение, рёбра, эксперты) не затрагиваются. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Курирование с помощью ИИ {: #ai-assisted-curation }

Настроенная ИИ-модель организации может составлять черновики определений и предлагать рёбра связей по всему глоссарию за одну операцию. Оба массовых действия требуют доступа `org_admin` и настроенной организации.

**`POST /admin/glossary/definitions/generate`**

Перебирает каждый термин в глоссарии, пропускает те, у которых уже есть определение, и вызывает ИИ-модель организации, чтобы составить черновик для каждого оставшегося термина. Черновик сохраняется немедленно — в отличие от эндпоинта черновика для одного термина (`POST /admin/glossary/terms/{term_id}/definition/generate`), здесь нет шага редактирования. Определения, написанные человеком, никогда не перезаписываются: защита — это `if summary["definition"]: continue` перед любым вызовом модели. Одно уведомление о публикации покрывает весь пакет. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Ответ:

```json
{"generated": 12}
```

`generated` — это количество терминов, получивших новое определение. Равно нулю, когда у каждого термина уже есть определение.

**`POST /admin/glossary/relationships/generate`**

Отправляет полный список терминов ИИ-модели организации с подсказкой, которая указывает десять допустимых типов рёбер (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) и просит только уверенные предложения. Модель отвечает JSON-массивом; каждая запись проверяется перед любой записью: неизвестные имена терминов, само-рёбра и типы рёбер вне закрытого перечисления молча отбрасываются. Валидные предложения записываются идемпотентно через upsert — повторный запуск действия не дублирует рёбра. Одно уведомление о публикации покрывает пакет. Эндпоинт немедленно возвращает `{"added": 0}`, когда глоссарий содержит менее двух неустаревших терминов. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Ответ:

```json
{"added": 5}
```

`added` — это количество записанных рёбер. Уже существовавшее ребро всё равно засчитывается — upsert проходит успешно, но данные ребра не меняются.

### Инструмент MCP `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

Ищет по именам и определениям терминов с сопоставлением подстроки без учёта регистра, до `limit` результатов. Каждый результат — полные детали термина: `name`, `definition`, `is_abstract`, `deprecated`, физические ссылки (с `source_id`, `schema_name`, `table_name`, `column_name`), типизированные рёбра и назначения экспертов. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Используйте `search_terms` перед написанием SQL, чтобы найти каждое физическое поле, представляющее понятие по имени. Например, поиск `"order date"` возвращает термин и все столбцы `order_dt`, `orderDate`, `ORDER_DATE` во всех зарегистрированных таблицах.

### Экспорт метаданных {: #metadata-export }

Граф терминов глоссария включён в каждый `MetadataSnapshot`, построенный `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

Экспорт применяет те же фильтры, что и остальная часть снимка:

- Термин, помеченный `export_excluded`, исключается полностью — независимо от его физических ссылок, абстрактного статуса или того, настроен ли каталог организации. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Корневой термин публикуется только тогда, когда хотя бы одна из его физических ссылок принадлежит столбцу, проходящему как фильтр **Data Product** (флаг `data_product` таблицы должен быть `true`), так и фильтр **технических** столбцов (столбцы с тегом `technical` исключаются).
- Корневой термин, чьи ссылки все исключены этими фильтрами, исключается вместе с ними.
- Абстрактные термины публикуются безусловно — это пользовательский словарь, не привязанный к физическим столбцам.
- Ребро между двумя терминами публикуется только тогда, когда публикуются оба конечных термина.

Каждый адаптер вендора публикует граф терминов нативно, в собственный контейнер глоссария Provisa, который он создаёт идемпотентно — никогда в существующий глоссарий каталога:

| Провайдер | Контейнер | Термины | Отношения | Устаревание |
| --- | --- | --- | --- | --- |
| Apache Atlas | «Provisa Glossary» (API глоссария) | термины глоссария, определение в `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | маркер shortDescription `[DEPRECATED]` |
| Atlan | глоссарий Provisa по стабильному qualifiedName | `longDescription` (никогда не человеко-редактируемый `userDescription`) | то же сопоставление Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | аспект `glossaryTermInfo` на термин | KIND_OF → Inherits, PART_OF → Contains (инвертировано), RELATED_TO/SYNONYM_OF → related terms | аспект устаревания; переименования следуют преемственности URN |
| OpenMetadata | глоссарий Provisa через `/v1/glossaries` | PUT с ключом fqn, переименования — PATCH-перепривязка по хранимому UUID | KIND_OF → нативная родительская иерархия, SYNONYM_OF → `synonyms`, остальные → `relatedTerms` | `entityStatus` |
| Collibra | домен типа «Глоссарий» «Provisa Glossary» | активы Business Term через Import API | нативные типы отношений Business Term | статус актива |

Владение — это привязка, а не имя: id вендора каждого опубликованного термина фиксируется в `catalog_bindings` под URN термина (`provisa://<org>/terms/<name>`), и Provisa изменяет или удаляет элемент глоссария на стороне вендора только тогда, когда владеет этой привязкой (или элемент находится в собственном контейнере Provisa, который она создала). Элемент глоссария без привязки Provisa возник во внешней системе и никогда не затрагивается; обновления выполняют read-merge, так что поля, добавленные стюардом к собственным терминам Provisa, сохраняются; ничего не удаляется, когда термин покидает снимок. Назначения «термин-актив» стюарда остаются во внешнем владении — ни один адаптер не записывает назначения «термин-актив» (публикация назначений, авторства Provisa, — это явное последующее развитие). Конкретно в Collibra безопасность в рамках семантики REPLACE Import API опирается на изоляцию: полезная нагрузка упоминает только активы внутри домена глоссария Provisa и экземпляры отношений только между терминами Provisa, так что глоссарии стюардов и их отношения никогда не достижимы. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Продукты данных (REQ-1634) {: #data-products-req-1634 }

Продукт данных группирует таблицы, публикуемые вместе для потребления, принадлежащие ровно одному домену. Поля следуют словарю ODPS (Open Data Product Standard) там, где у Provisa уже есть источник истины. Admin UI предоставляет продукты данных в разделе **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Возможности {: #capabilities }

| Возможность | Предоставляет |
| --- | --- |
| `data_product_read` | Доступ на чтение к полю запроса `data_products` и странице admin «Data Products». По умолчанию предоставлена `org_admin`, `analyst`, `developer` и `modeler`. |
| `data_product_rw` | Мутации создания и удаления. Включает элементы управления New / Edit / Delete в UI. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### Admin GraphQL

Все операции с продуктами данных проходят через `POST /admin/graphql`.

**Запрос:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Требует `data_product_read`.

**Создание или обновление:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` выполняет upsert — вызов с существующим `id` обновляет запись. Требует `data_product_rw`.

**Удаление:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Удаление продукта очищает `product_id` у каждой входящей в него таблицы, снимая её членство. Требует `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Схема полей {: #field-schema }

| Поле | Тип | Обязательно | Примечания |
| --- | --- | --- | --- |
| `id` | `String` | Да | Машиночитаемый стабильный идентификатор, например `customer_360` |
| `domain_id` | `String` | Да | Владеющий домен. Входящие таблицы должны иметь тот же `domain_id` — несовпадения отклоняются при сохранении |
| `name` | `String` | Да | Отображаемое имя |
| `owner_role` | `String` | Нет | Роль, ответственная за этот продукт; отличается от стюарда домена |
| `team_role` | `String` | Нет | Роль, чьи носители сопровождают этот продукт день за днём; разрешается до конкретных людей |
| `purpose` | `String` | Нет | Что публикует этот продукт и зачем |
| `limitations` | `String` | Нет | Известные ограничения, оговорки или исключения |
| `usage` | `String` | Нет | Как потреблять этот продукт |
| `version` | `String` | Нет | например, `1.2.0` |
| `status` | `String` | Нет | например, `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Нет | Обязательства по уровню обслуживания; проза — продукт охватывает несколько таблиц, и структурированный SLA не может однозначно назвать, к какому именно участнику он относится |
| `support` | `String` | Нет | Произвольный текст с рекомендациями по поддержке |
| `custom_properties` | `JSON` | Нет | Произвольные метаданные ключ-значение, не покрытые стандартными полями |

На модели существуют ещё два поля, но они не предоставлены в Strawberry `DataProductType` / `DataProductInput` — они специфичны для Snowflake Horizon Catalog (REQ-1635):

| Поле | Примечания |
| --- | --- |
| `support_contact` | Email или URL; обязателен для манифестов листинга организации Horizon Catalog |
| `publish` | `true` для немедленной публикации листингов Horizon; новые листинги по умолчанию имеют статус DRAFT |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Членство таблиц {: #table-membership }

Таблица присоединяется к продукту данных путём установки поля `product_id` в форме редактирования таблицы. Средство выбора ограничено продуктами, чей `domain_id` совпадает с собственным доменом таблицы — таблице в домене `marketing` никогда не предлагается продукт в домене `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Команды в том же домене также могут быть назначены участниками. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Фильтр экспорта метаданных {: #metadata-export-filter }

`build_snapshot` применяет `data_products_only=True` при каждой публикации каталога. Таблицы без `product_id` исключаются из снимка вместе с их рёбрами связей, рёбрами происхождения и тегами управления. Источники и домены публикуются всегда. Термины глоссария публикуются только тогда, когда хотя бы одна из их физических ссылок принадлежит экспортированной (входящей в продукт) таблице. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Продукт без экспортированных участников не формирует запись снимка — листинг без участников искажал бы представление продукта в каталоге. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Поддержка продуктов данных по целевому каталогу {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` достигает каждого адаптера, но только адаптеры, чья платформа имеет нативное понятие продукта данных, публикуют его как полноценную сущность; остальные публикуют входящие таблицы (уже отфильтрованные выше) без группировки по продукту.

| Цель | Представление продукта данных |
| --- | --- |
| Snowflake Horizon | Каждый продукт становится `SHARE` над физическими адресами входящих в него таблиц, обёрнутым во внутренний `CREATE ORGANIZATION LISTING` — нативный продукт данных Horizon Catalog. `publish=true` немедленно публикует листинг вживую; иначе он попадает в статус DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Каждый продукт становится листингом Analytics Hub через `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Каждый продукт становится нативной сущностью `DataProduct` (`/api/v1/dataProducts`) с владением, производным от домена. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Каждый продукт становится нативной сущностью `dataProduct` (`urn:li:dataProduct:...`) с собственными аспектами `dataProductProperties`/владения. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Каждый продукт становится активом типа community `Data Product`, связанным со своими входящими таблицами через отношение `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Публикуется как предполагаемый (guess) пользовательский typedef `DataProduct` — у Atlan нет документированного стабильного имени типа для этого понятия, поэтому сопоставление выполняется по best-effort. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Публикуется как пользовательский typedef `provisa_data_product` с отношением `provisa_data_product_members` — у Atlas нет нативного типа сущности продукта данных. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Не является полноценной сущностью — входящие таблицы несут пользовательскую фасету `provisa_data_product`, называющую владеющий продукт. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
