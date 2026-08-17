# Admin API

Admin API — это конечная точка Strawberry GraphQL по адресу `POST /admin/graphql` (REQ-533). Она требует роль superuser или admin (REQ-125, REQ-060) и отделена от конечной точки данных GraphQL (REQ-533).

## Аутентификация

Передавайте учётные данные в заголовке `Authorization`, используя стандартный провайдер аутентификации Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

Доступ к admin регулируется возможностью (capability) `admin`, назначенной роли (REQ-060, REQ-042).

### Персональные токены доступа

Персональный токен доступа принимается везде, где принимается bearer-токен, включая эту конечную точку. Выпуск и отзыв — самообслуживаемая операция: это собственная учётная запись владельца токена, поэтому она находится в профиле пользователя в admin UI, а не на странице администратора, рядом с выходом из организации и удалением аккаунта. Администратор не выпускает токены от чужого имени. (REQ-1263)

| Маршрут | Эффект |
| ------- | -------- |
| `POST /auth/tokens` | Выпустить токен для вызывающей стороны. Тело: `name`, опционально `role_id`, `scopes`, `expires_in_days` (1–366). Ответ — единственное место, где секрет когда-либо появляется |
| `GET /auth/tokens` | Активные токены вызывающей стороны в этой организации — отображаемый префикс, имя, временные метки жизненного цикла и хеш, идентифицирующий токен для отзыва. Никогда не рабочая учётная запись |
| `DELETE /auth/tokens/{token_hash}` | Отозвать один из токенов вызывающей стороны. 404, если токен не принадлежит ей или уже отозван |

Если не указать `role_id`, токен разрешается в ту роль, которой в данный момент обладает его владелец; указание роли сужает токен относительно владельца. Отзыв также происходит неявно: удаление членства пользователя в организации отзывает его токены для этой организации. Сам механизм учётных данных см. в [Security Model](security.md#_16).

## Возможности (Capabilities)

### Управление конфигурацией

Скачать текущую работающую конфигурацию (REQ-164):

```http
GET /admin/config
```

Возвращает полный `config.yaml` как YAML-файл. Загрузить новую конфигурацию (REQ-164):

```http
PUT /admin/config
```

Provisa валидирует YAML, перезагружает каталоги и регенерирует схемы (REQ-012, REQ-253). Перезапуск не требуется.

### Настройки времени выполнения

Читайте и записывайте настройки платформы времени выполнения без редактирования файла конфигурации (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

Поверхность настроек охватывает перенаправление больших результатов, выборку и лимит строк по умолчанию, TTL кеша ответов, соглашение об именовании, автоотслеживание FK для связей, DSN хранилища материализации, память движка федерации (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), а также всю поверхность настройки конвейера трассировки OpenTelemetry (REQ-1082). Также доступны лимиты обхода удалённого GraphQL и настройки тёплого яруса/кеша чтения (REQ-1081, REQ-1083).

Режим безопасности — `security.mode` (`standard` | `high`) — применяется при перезапуске (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Назначения AI-моделей, реестр моделей embedding/vector и лимит частоты запросов NL — применяются при перезапуске (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

Вкладка шифрования в admin получает список провайдеров динамически из реестра шифрования; недоступные провайдеры отображаются, но недоступны для выбора (REQ-1091).

`GET`/`HEAD /health` и `GET /setup/status` всегда не требуют аутентификации — они обходят требование `Authorization: Bearer` даже при настроенном провайдере аутентификации (REQ-539).

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

### AI-обнаружение связей

Запустить анализ внешних ключей на базе Claude через REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Возвращает кандидатов на внешний ключ, ранжированных по уверенности. Принять кандидата:

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

### Проверка зависимостей колонки (REQ-1484)

Перед сохранением правки таблицы, переименовывающей SQL-алиас колонки или удаляющей колонку, узнайте, что ещё на неё ссылается:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Переименование алиаса ломает каждый артефакт, написанный относительно внешнего имени: представления, MV, выражения метрик, предикаты RLS, контракты DQ. Удаление колонки ломает их же плюс артефакты, хранящие физическое `column_name`: связи, привязки глоссария, назначения тегов. `breaksOn` сообщает, что именно. Страница Tables запускает эту проверку при сохранении и показывает результат в виде рекомендательного диалога. Что охватывает этот запрос и чего не может, см. в разделе [Lineage](lineage.md).

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

Запустить ручное обновление (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Регистрация графовых источников

Источники Neo4j и SPARQL регистрируются через REST-конечные точки (не через GraphQL admin API) (REQ-295, REQ-297):

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

Admin API поставляется с GraphiQL по адресу `GET /admin/graphql` в браузере (REQ-622). Используйте его для интерактивного исследования полной схемы admin.

## Управляющие представления домена ops (REQ-1386)

Восемь SQL-представлений засеваются во встроенный домен `ops` при каждой установке. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Они предоставляют журнал аудита запросов как управляемые (governed) таблицы — доступные для запросов через SQL (pgwire), GraphQL и Cypher по тем же правилам доступа к домену, RLS и маскирования, что и любая бизнес-таблица.

`org_admin` назначается стюардом домена ops в момент засева, поэтому этот домен никогда не отображается как пробел в governance в `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Представление | На какой вопрос отвечает |
| --- | --- |
| `usage_ranking` | Количество запросов и число уникальных пользователей по каждой зарегистрированной таблице; таблицы с нулевым обращением проявляются как кандидаты на устаревание |
| `deprecated_usage` | Каждое обращение к таблице или колонке с тегом `deprecated` — активные потребители, блокирующие безопасное удаление |
| `pii_access` | Каждое обращение к таблице или колонке с тегом `pii`: кто запрашивал, под какой ролью, через какую поверхность |
| `policy_denials` | Все попытки доступа, отклонённые governance (HTTP 401/403) |
| `surface_mix` | Ежедневное количество запросов и число уникальных пользователей по протокольной поверхности (SQL, GraphQL, Cypher, gRPC и т. д.) |
| `query_health` | Ежедневное количество ошибок и средняя/максимальная задержка по поверхности |
| `stale_metadata` | Таблицы и колонки без описаний; домены без стюарда |
| `join_hotspots` | Пары таблиц, чаще всего запрашиваемые совместно, — кандидаты на материализацию или кеширование |

Сегодня действуют два ограничения. Гранулярность на уровне таблицы — журнал аудита фиксирует `table_ids`, а не отдельные запрошенные колонки. Текст запроса зашифрован (REQ-689) и исключён из всех представлений здесь; доступ к нему возможен только через авторизованный путь расшифровки admin. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Роли необходим доступ к домену `ops`, чтобы эти представления были видны. Предоставляйте его так же, как доступ к любому другому домену.

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

Те же запросы выполняются как GraphQL или Cypher через любой управляемый транспорт — pgwire, Arrow Flight или Bolt. [inferred from governed-surface design]

## Просмотрщик отчётов (REQ-1390)

Просмотрщик отчётов находится по адресу `/admin/reports`. Роли без возможности `observability` не могут получить к нему доступ.

Левая панель перечисляет каждую зарегистрированную таблицу в домене `ops`, отсортированную по алиасу. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Восемь засеянных управляющих представлений появляются там автоматически. Нажмите на любой отчёт, чтобы загрузить его в управляемый просмотрщик данных справа.

**Добавление пользовательского отчёта.** Кнопка «Add report» открывает диалог. Укажите имя, необязательное описание и SELECT-выражение. Сохранение регистрирует представление как управляемую производную таблицу в домене `ops` — каталогизированную, с контролем доступа и доступную для запросов через каждую поверхность наравне с засеянными представлениями. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Удаление.** Значок корзины появляется только у пользовательских отчётов. Засеянные управляющие представления нельзя удалить из этого интерфейса. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Предпросмотр таблицы (REQ-1392)

Разверните любую строку таблицы на странице Tables. Кнопка **Preview** открывает модальное окно шириной 90% с живыми управляемыми данными таблицы. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Таблицы, опирающиеся на API с обязательными параметрами пути, блокируют предпросмотр до предоставления этих значений. Встроенная форма собирает каждый обязательный параметр до выполнения первого запроса; необязательные параметры запроса появляются в той же форме. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Просмотрщик управляемых данных (REQ-1391)

Один и тот же компонент просмотрщика обслуживает и модальное окно предпросмотра, и просмотрщик отчётов. Его поведение идентично в обоих контекстах.

**Постраничная выборка на сервере.** Каждая страница — это собственный управляемый `SELECT *` с `LIMIT 101 OFFSET n`. На странице отображается 100 строк; 101-я сигнализирует, есть ли ещё данные. Полный набор данных никогда не загружается в браузер. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Проталкиваемые вниз фильтры и сортировки.** У каждого заголовка колонки есть поле фильтра. Условия фильтра становятся предикатами `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; клики по сортировке порождают предложения `ORDER BY`. Оба уходят в базу данных — фильтр по таблице с миллиардом строк сканирует источник, а не 100-строчную страницу перед вами. [tool-verified: `nativeParams.ts:53-70`]

**Многоуровневая группировка.** Значок Layers в заголовке любой колонки переключает её участие в группировке. Колонки группировки идут первыми в `ORDER BY`, поэтому члены группы попадают на ту же страницу, что и их заголовок, независимо от границ страниц. Колонки первичного ключа добавляются как стабильный разделитель равенства. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Строки заголовков групп сворачиваемы; сворачивание скрывает членов без нового запроса. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Сохраняемые настройки.** Настройки фильтра, сортировки и группировки сохраняются в `localStorage` под ключом `provisa.grid.table:<domain>.<table>` и восстанавливаются при следующем посещении. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Экспорт.** Скачайте текущую страницу как CSV или скопируйте её в буфер обмена как текст с разделителями-табуляциями. Экспорт охватывает только видимую страницу. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
