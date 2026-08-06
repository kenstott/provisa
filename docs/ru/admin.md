# Admin API

Admin API — это эндпоинт Strawberry GraphQL по адресу `POST /admin/graphql` (REQ-533). Он требует роли суперпользователя или администратора (REQ-125, REQ-060) и отделён от эндпоинта данных GraphQL (REQ-533).

## Аутентификация

Передавайте свои учётные данные в заголовке `Authorization`, используя стандартный провайдер аутентификации Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Доступ администратора управляется возможностью (capability) `admin`, назначенной роли (REQ-060, REQ-042).

### Персональные токены доступа

Персональный токен доступа принимается везде, где принимается bearer-токен, включая эту конечную точку. Выпуск и отзыв выполняются самостоятельно — это личные учётные данные владельца, поэтому они находятся в профиле пользователя в административном интерфейсе, а не на административной странице, рядом с выходом из организации и удалением учётной записи. Администратор не выпускает токены от чужого имени. (REQ-1263)

| Маршрут | Действие |
| ------- | -------- |
| `POST /auth/tokens` | Выпускает токен для вызывающего. Тело: `name`, плюс опционально `role_id`, `scopes`, `expires_in_days` (1–366). Ответ — единственное место, где вообще появляется секрет |
| `GET /auth/tokens` | Активные токены вызывающего в этой организации — отображаемый префикс, имя, отметки времени жизненного цикла и хеш, идентифицирующий токен для отзыва. Никогда не рабочие учётные данные |
| `DELETE /auth/tokens/{token_hash}` | Отзывает один из токенов вызывающего. 404, если токен не его или уже отозван |

Если `role_id` опущен, токен разрешается в роль, которой владеет его владелец; указание роли сужает токен ниже владельца. Отзыв происходит и неявно: удаление членства пользователя в организации отзывает его токены для этой организации. О самих учётных данных см. [модель безопасности](security.md#personal-access-tokens).

## Возможности

### Управление конфигурацией

Скачать текущую работающую конфигурацию (REQ-164):

```http
GET /admin/config
```

Возвращает полный `config.yaml` как YAML-файл. Загрузить новую конфигурацию (REQ-164):

```http
PUT /admin/config
```

Provisa проверяет YAML, перезагружает каталоги и регенерирует схемы (REQ-012, REQ-253). Перезапуск не требуется.

### Настройки времени выполнения

Читать и записывать настройки платформы времени выполнения без редактирования файла конфигурации (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Поверхность настроек охватывает перенаправление больших результатов, выборку и лимит строк по умолчанию, TTL кеша ответов, соглашение об именовании, автоотслеживание FK связей, DSN хранилища материализации, память движка федерации (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) и полную поверхность настройки конвейера трассировки OpenTelemetry (REQ-1082). Лимиты обхода удалённого GraphQL и настройки тёплого уровня/кеша чтения также раскрыты (REQ-1081, REQ-1083).

Режим безопасности — `security.mode` (`standard` | `high`) — применяется при перезапуске (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Назначения AI-моделей, реестр моделей эмбеддингов/векторов и лимит скорости NL — применяются при перезапуске (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Вкладка шифрования администратора выводит свой список провайдеров динамически из реестра шифрования; недоступные провайдеры отображаются, но не выбираемы (REQ-1091).

`GET`/`HEAD /health` и `GET /setup/status` всегда без аутентификации — они обходят требование `Authorization: Bearer`, даже когда настроен провайдер аутентификации (REQ-539).

### Редактор связей

Список связей (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Создать связь (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### Обнаружение связей с помощью AI

Запустить анализ FK на базе Claude через REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Возвращает кандидатов FK, ранжированных по уверенности. Принять кандидата:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Интроспекция схемы

Просмотр опубликованных таблиц по всем источникам (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Управление представлениями

Зарегистрировать материализованное представление (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Запустить обновление вручную (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Регистрация графовых источников

Источники Neo4j и SPARQL регистрируются через эндпоинты REST (а не через GraphQL admin API) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

После регистрации таблицы появляются в схеме GraphQL и доступны для запросов, как и любой другой источник (REQ-016).

## GraphiQL

Admin API поставляется с GraphiQL по адресу `GET /admin/graphql` в браузере (REQ-622). Используйте его для интерактивного изучения полной схемы администратора.

## Представления управления домена ops (REQ-1386)

Восемь SQL-представлений засеиваются во встроенный домен `ops` при каждой установке. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Они предоставляют журнал аудита запросов в виде управляемых таблиц — доступных через SQL (pgwire), GraphQL и Cypher, под теми же правилами доступа к домену, RLS и маскирования, что и любая бизнес-таблица.

При засеивании `org_admin` назначается стюардом домена ops, поэтому домен никогда не появляется как пробел в управлении в `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Представление | На что отвечает |
| --- | --- |
| `usage_ranking` | Количество запросов и различных пользователей по каждой зарегистрированной таблице; таблицы без обращений всплывают как кандидаты на вывод из эксплуатации |
| `deprecated_usage` | Каждое обращение к таблице или столбцу с тегом `deprecated` — активные потребители, блокирующие безопасное удаление |
| `pii_access` | Каждое обращение к таблице или столбцу с тегом `pii`: кто выполнял запрос, под какой ролью, через какую поверхность |
| `policy_denials` | Все попытки доступа, отклонённые управлением (HTTP 401/403) |
| `surface_mix` | Ежедневное количество запросов и различных пользователей по каждой протокольной поверхности (SQL, GraphQL, Cypher, gRPC и т. д.) |
| `query_health` | Ежедневное количество ошибок и средняя/максимальная задержка по каждой поверхности |
| `stale_metadata` | Таблицы и столбцы без описаний; домены без стюарда |
| `join_hotspots` | Пары таблиц, чаще всего запрашиваемые вместе, — кандидаты на материализацию или кеширование |

Сегодня действуют два ограничения. Разрешающая способность — на уровне таблицы: журнал аудита хранит `table_ids`, а не отдельные затронутые столбцы. Текст запроса зашифрован (REQ-689) и исключён из всех представлений здесь; он доступен только через авторизованный административный путь расшифровки. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Чтобы эти представления были видимы, роли нужен доступ к домену `ops`. Предоставьте его так же, как предоставляете доступ к любому другому домену.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Те же запросы работают как GraphQL или Cypher поверх любого управляемого транспорта — pgwire, Arrow Flight или Bolt. [inferred from governed-surface design]

## Просмотр отчётов (REQ-1390)

Просмотр отчётов находится по адресу `/admin/reports`. Роли без возможности `observability` не могут туда попасть.

Левая панель перечисляет каждую зарегистрированную таблицу в домене `ops`, отсортированную по псевдониму. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Восемь засеянных представлений управления появляются там автоматически. Щёлкните любой отчёт, чтобы загрузить его в управляемый просмотрщик данных справа.

**Добавление собственного отчёта.** Кнопка «Добавить отчёт» открывает диалог. Укажите имя, необязательное описание и инструкцию SELECT. Сохранение регистрирует представление как управляемую производную таблицу в домене `ops` — каталогизированную, с контролем доступа и доступную для запросов со всех поверхностей наряду с засеянными представлениями. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Удаление.** Значок корзины появляется только для собственных отчётов. Засеянные представления управления нельзя удалить через эту поверхность. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Предпросмотр таблицы (REQ-1392)

Разверните любую строку на странице «Таблицы». Кнопка **Предпросмотр** открывает модальное окно шириной 90 % с живыми управляемыми данными таблицы. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Таблицы на основе API с обязательными параметрами пути блокируют предпросмотр, пока эти значения не будут указаны. Встроенная форма собирает каждый обязательный параметр до выполнения первого запроса; необязательные параметры запроса появляются в той же форме. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Управляемый просмотрщик данных (REQ-1391)

Один и тот же компонент просмотрщика обслуживает модальное окно предпросмотра и просмотр отчётов. Поведение в обоих контекстах одинаково.

**Постраничный вывод на сервере.** Каждая страница — собственный управляемый `SELECT *` с `LIMIT 101 OFFSET n`. Отображается 100 строк на странице; 101-я сигнализирует, есть ли ещё. Полный набор данных никогда не загружается в браузер. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Фильтрация и сортировка передаются источнику.** У каждого заголовка столбца есть поле фильтра. Термины фильтра становятся предикатами `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; щелчки сортировки порождают предложения `ORDER BY`. И то и другое отправляется в базу данных — фильтрация таблицы на миллиард строк сканирует источник, а не те 100 строк, что перед вами. [tool-verified: `nativeParams.ts:53-70`]

**Многоуровневая группировка.** Значок слоёв на каждом заголовке столбца добавляет этот столбец в группировку. Столбцы группировки идут первыми в `ORDER BY`, поэтому члены группы попадают на ту же страницу, что и их заголовок, даже через границы страниц. Столбцы первичного ключа добавляются в конец как устойчивый разрешитель ничьих. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Строки заголовков групп сворачиваются; сворачивание скрывает членов без нового запроса. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Выбор сохраняется.** Настройки фильтров, сортировки и группировки сохраняются в `localStorage` под ключом `provisa.grid.table:<domain>.<table>` и восстанавливаются при следующем посещении. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Экспорт.** Скачайте текущую страницу как CSV или скопируйте её в буфер обмена как текст с разделителями-табуляциями. Экспорт охватывает только видимую страницу. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
