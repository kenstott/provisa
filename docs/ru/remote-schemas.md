# Удалённые схемы (Remote Schemas)

Источник удалённой схемы подключает внешний API — GraphQL (включая GitHub), gRPC или REST (OpenAPI) — к семантическому слою Provisa. Добавление источника не регистрирует ни одной таблицы. Источник предлагает таблицы, а куратор регистрирует каждую нужную таблицу через выбор «Register Table»; эта регистрация и есть шаг курирования. (REQ-308, REQ-316, REQ-322) Зарегистрированная таблица — полноценная таблица Provisa. (REQ-308, REQ-316, REQ-325) Каждое правило governance, интерфейс запросов и слой безопасности применяются автоматически. (REQ-310, REQ-319, REQ-328) Удалённый сервис никогда не видит правил governance Provisa. (REQ-310, REQ-319, REQ-328)

---

## Три типа источников

### Удалённая схема GraphQL (REQ-307–313)

**Как добавить источник.** Отправьте POST на `/admin/sources/graphql-remote` с URL эндпоинта, пространством имён и опциональной аутентификацией. Provisa выполняет стандартный запрос интроспекции `__schema` к удалённому эндпоинту, чтобы подтвердить эндпоинт и учётные данные. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

Добавление источника не регистрирует ни таблицу, ни command. Каждый тип удалённого источника отвечает на добавление и обновление одними и теми же счётчиками: `tables` (зарегистрированные или приведённые в актуальное состояние таблицы; при добавлении 0), `available_tables` (предлагаемые таблицы), `mutations` (всегда 0) и `available_mutations` (предлагаемые commands). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**Регистрация таблиц.** Откройте Tables, затем Register Table, выберите источник и схему `graphql` и отметьте нужные таблицы и столбцы. Через административный GraphQL API: `availableTables(sourceId, schemaName)` перечисляет предлагаемые таблицы, `availableColumns` перечисляет столбцы таблицы, а `registerTable(input: TableInput)` регистрирует таблицу с выбранными столбцами. Зарегистрированная таблица затем подпадает под governance. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

То, как читается зарегистрированная таблица (корневое поле, путь к строкам, обязательные аргументы, аргументы страниц), хранится в `sources.mapping["tables"]`, поэтому перезапущенный процесс читает её, не запрашивая у удалённого сервиса его схему. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Варианты аутентификации: `none`, `bearer` (заголовок Authorization), `basic` (Base64 username:password). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Переопределения полей.** `field_overrides` — это карта `{fieldName: "query" | "mutation"}`, применяемая после интроспекции. Она имеет приоритет над структурной классификацией. Только поля типа query могут быть переклассифицированы как мутации; поля типа mutation не имеют пути переопределения в GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Связи на момент регистрации.** `relationships` объявляет пути соединения FK/PK между таблицами на момент регистрации. Они сохраняются как связи, объявленные вручную (без флага `remote_managed`). При обновлении автоматически обнаруженные связи (те, что с `remote_managed: True`) выполняются заново и могут измениться; вручную объявленные связи не затрагиваются. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**Что предлагает источник.** Каждое поле удалённого типа `Query`, возвращающее объект или список объектов, предлагается как таблица, как и каждое соединение Relay под полем одиночного объекта (см. ниже). Регистрация предложенной таблицы делает её таблицей. Каждое поле удалённого типа `Mutation` — это предлагаемая команда, учитываемая в `available_mutations`; добавление источника не регистрирует ни одной. Нужные регистрируйте как команды; см. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**Именование таблиц.** Таблицы называются `{namespace}__{field_name}`. С пространством имён `petstore` и полем запроса `pets`: имя таблицы — `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Соединения Relay.** Многие API возвращают списки в виде соединений Relay: объекта с `nodes` (или `edges { node }`) рядом с `pageInfo`. Provisa сопоставляет соединение с таблицей его узлов и читает его постранично. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- Корневое поле, возвращающее соединение (`securityAdvisories`), становится одной таблицей его узлов.
- Соединение на одиночном объекте, который возвращает корневое поле, становится отдельной таблицей. Таблица берёт обязательные аргументы корневого поля. Для `repository(owner, name)` и соединения `issues` на `Repository` таблица называется `repositoryIssues`, имя в SQL — `gh__repository_issues` в пространстве имён `gh`. Фильтруйте её по столбцам `_nf_owner` и `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Соединение никогда не бывает столбцом. Иначе строка несла бы чтение, которое удалённый сервис вычисляет для каждой строки, по каждому соединению её типа.
- Соединение является таблицей, только если его поле принимает `first` и `after`, чтобы его можно было читать постранично. Соединение, которому нужен собственный аргумент, таблицей не является. Не является ею и соединение объединения (union), и любое соединение под корневым полем, возвращающим список.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**Отображение типов (REQ-308).** Скалярные поля отображаются на типы Provisa напрямую. Поля OBJECT разделяются на два случая в зависимости от того, управляется ли целевой тип (см. «Управляемые таблицы» ниже). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Тип GraphQL | Тип Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (неуправляемый встроенный тип, например `ContactInfo`) | столбец-блоб `jsonb` |
| OBJECT (управляемый целевой тип) | полностью исключается из SDL и выборки |
| Любой ENUM | `jsonb` |
| Пользовательский скаляр | `text` (запасной вариант) |

**Управляемые таблицы.** Тип GQL управляется, когда он появляется как корневое поле `Query` в удалённой схеме. `_collect_queryable_types` собирает их во время регистрации, предпочитая поля без обязательных аргументов, чтобы их можно было массово выбирать как цели соединений. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Когда столбец типа OBJECT на управляемой таблице указывает на другой управляемый тип, этот столбец подчиняется трём правилам одновременно [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Исключён из выборки GQL** — поле не запрашивается при получении строк родительской таблицы.
2. **Исключён из SDL** — поле не появляется на родительском типе в сгенерированной схеме.
3. **Доступен только через объявленную связь** — куратор должен зарегистрировать JOIN между двумя материализованными управляемыми таблицами. Без этого поле просто отсутствует; запасного блоба нет.

Типы OBJECT, недостижимые как корневые поля Query (встроенные типы, такие как `ContactInfo` или `Address`), следуют другим правилам: они выбираются как столбцы-блобы `jsonb` и появляются в SDL как поля вложенных объектов. Доступ к подполям осуществляется через извлечение `-->>` в SQL.

**Поля, которым нужен аргумент, не являются столбцами.** Поле с обязательным аргументом нельзя выбрать «как есть», поэтому оно исключается из столбцов таблицы и из вложенных выборок. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Обязательные аргументы.** Когда корневое поле запроса имеет не-null аргументы без значения по умолчанию, они становятся столбцами `native_filter_type: query_param` на таблице (с префиксом `_nf_` на момент внедрения). Исполнитель передаёт их как переменные GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Связи, обнаруживаемые автоматически.** Provisa сканирует столбцы типа OBJECT каждой зарегистрированной таблицы. Когда указанный тип GQL также является зарегистрированной таблицей в том же источнике, а столбец, на котором держится связь, входит в число зарегистрированных, связь сохраняется. Таблица, которая ещё не зарегистрирована, связей не получает. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] Связи many-to-one выводят исходный и целевой столбцы из соглашений об именовании (`breedName` в исходном типе → `name` в целевом типе `Breed`). Поля one-to-many (LIST) порождают связи с пустыми ссылками на столбцы — внешний ключ находится на стороне цели. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Мутации.** Поле мутации регистрируется по одному как команда вида `source_operation`. Её аргументы имеют тип `json` каждый и передаются удалённому сервису как типизированные переменные; ответ — это JSON, который возвращает удалённый сервис, без `return_schema`. См. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**Обновление.** Отправьте POST на `/admin/sources/graphql-remote/{id}/refresh`. Повторно интроспектирует удалённую схему и приводит уже зарегистрированные таблицы в соответствие с ней. Она не добавляет ни таблицу, ни столбец: таблица или столбец, появившиеся в схеме, остаются в предложении, а столбец, исчезнувший из схемы, удаляется. Существующие правила governance (RLS, маскирование) сохраняются. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**Ограничения.**

- Скалярные и ENUM корневые поля запроса (тип возврата не OBJECT) становятся отслеживаемыми функциями, а не виртуальными таблицами. Их `return_schema` — это единственный столбец `value` отображённого скалярного типа. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- Вложенность объектов разрешается на момент регистрации до `graphql_remote.max_object_depth` (по умолчанию: 5). И выборка при удалённой загрузке, и метаданные подполей строятся до этой глубины; поля за пределом не загружаются и недоступны для извлечения в SQL. Тип входит в обход только один раз вдоль любого пути: поле, тип которого уже находится на пути вниз, исключается, так что схема, типы которой ссылаются друг на друга, обходится один раз на тип, а не один раз на уровень глубины. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- Вложенные поля OBJECT типа LIST (например, `breed.awards: [Award]`) включаются в выборку при загрузке до `graphql_remote.max_list_depth` уровней вложенности (по умолчанию: 2). В этом пределе список загружается как массив `jsonb` в родительском столбце. Когда поле списка объявляет аргумент `first` (Relay, PostGraphile, pg_graphql) или аргумент `limit` (Hasura), выборка передаёт его как `first: N` или `limit: N`, где N — `graphql_remote.max_list_items` (по умолчанию: 100). Поле списка, не объявляющее ни того, ни другого, аргумента не получает, потому что удалённый сервис отклоняет аргумент, который поле не объявляет. За пределом `max_list_depth` поле LIST полностью исключается, чтобы предотвратить неограниченное разрастание данных. В SQL массив доступен через `json_array_elements(column_name)` или извлечение по индексу с `->>`. Если у типа элементов списка есть собственный корневой запрос, зарегистрируйте его как отдельную таблицу и создайте связь — путь через join эффективнее и обходит блоб. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- Для SQL-запросов неуправляемые столбцы типа OBJECT выбираются полностью из удалённого источника (все подполя до настроенной глубины) и кешируются как `jsonb`. Доступ к подполям в SQL обрабатывается через извлечение `->>` из блоба; удалённый запрос не сужается только до полей, которые выбирает SQL-запрос. Когда тип элемента LIST не имеет корневого запроса, а представление блоба недостаточно, пишите запрос напрямую в GraphQL SDL — Provisa точно воспроизводит выборку полей GQL, так что удалённая сторона видит ровно запрошенные поля. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Если удалённый сервер отклоняет поле типа OBJECT, потому что оно требует выбора подполей (что не должно происходить, когда доступна `gql_selection`), исполнитель повторяет попытку один раз с удалением этих полей, чтобы скалярные столбцы всё же вернулись. Это относится к таблицам, читаемым с корневого поля. Таблица соединения этот путь не проходит. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Постраничное чтение.** Таблица соединения читается по курсору. Каждая страница запрашивает `first: N, after: $pageCursor` с `pageInfo { hasNextPage endCursor }`, и чтение следует за `endCursor`, пока удалённый сервис не сообщит, что следующей страницы нет. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Параметр | По умолчанию | Действие |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Строк на страницу. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | Наибольшее число строк, которое берёт одно чтение таблицы соединения. Чтение, достигшее его, останавливается и записывает предупреждение в журнал. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Два ответа заставляют исполнитель повторять запрос:

- **Страница слишком тяжёлая.** Когда удалённый сервис отвечает 502 или 504, та же страница запрашивается снова вполовину меньшего размера, вплоть до одной строки. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Ограничение частоты с временем ожидания.** Когда удалённый сервис отвечает 403 или 429 с `Retry-After` не более 120 секунд, исполнитель ждёт это время и отправляет запрос снова, до трёх попыток. Отказ без `Retry-After` или с требованием более долгого ожидания вызывается как ошибка. Это относится к каждому чтению, соединение это или нет. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Любая другая ошибка в ответе приводит к сбою чтения, если тип источника не определяет иного (см. GitHub ниже). У соединения, родитель которого вернулся null, нет строк. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub — обычный тип источника. Его API — GraphQL, поэтому его таблицы ведут себя так, как описано выше, включая таблицы соединений, такие как `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Добавление источника.**

1. Откройте Sources и добавьте источник типа **GitHub**.
2. Введите токен доступа GitHub. При желании введите пространство имён — префикс имён таблиц; по умолчанию `gh`.
3. Сохраните. Provisa проверяет токен в GitHub. Токен, который GitHub отклоняет, приводит к сбою добавления с сообщением GitHub.

Добавление источника не регистрирует таблиц. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Регистрация таблиц.** Откройте Tables, затем Register Table. Выберите источник GitHub, выберите схему `graphql`, затем выберите нужные таблицы. Перечисляется каждая таблица, которую предлагает GitHub; регистрация — это ваш выбор того, что открыть. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Области действия (scopes) токена.** Когда вы регистрируете таблицу, Provisa один раз проверяет её в GitHub с вашим токеном.

- Поле, которое области действия токена не покрывают, исключается из таблицы. Результат называет каждое исключённое поле: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- Таблица, которую токен не может прочитать вовсе, отклоняется с указанием причины от GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Строки, которые токен не может видеть.** GitHub отвечает `FORBIDDEN` для поля, которое токен не может видеть в конкретной строке, например для соавторов репозитория без права записи, и `NOT_ORG_OWNED_REPO` для поля, существующего только в репозиториях, принадлежащих организации. Это поле в данной строке равно null, остальное чтение продолжается, а Provisa записывает предупреждение в журнал. Ошибка, относящаяся к самой таблице, приводит к сбою чтения. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Тяжёлые страницы.** Когда GitHub отвечает `RESOURCE_LIMITS_EXCEEDED`, потому что вычисление страницы обходится слишком дорого, страница запрашивается снова вполовину меньшего размера. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Вложенные объекты.** Таблицы GitHub используют собственную глубину вложенности 0 (`max_object_depth=0` для этого типа источника), а не `graphql_remote.max_object_depth`. Столбец вложенного объекта выбирается только с его собственными скалярными полями; объекты внутри него отображаются как `__typename`. [tool-verified: `brands.py`]

**Хранение токена.** Токен помещается в хранилище секретов, а строка источника хранит ссылку, поэтому после перезапуска источник читается снова без повторного ввода токена. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**Как это работает (для операторов).** Схема GitHub поставляется с Provisa, поэтому добавление источника не выполняет вызова интроспекции, а большая схема ничего не стоит при регистрации. Таблицы отображаются из неё по одной по мере регистрации. Эндпоинт refresh отклоняет этот тип источника; новая схема GitHub приходит с выпуском Provisa. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab — обычный тип источника, и он добавляется и регистрируется так же, как GitHub: добавьте источник типа **GitLab** с токеном доступа, затем зарегистрируйте нужные таблицы из схемы `graphql`. Префикс имён таблиц по умолчанию — `gl`. Источник обращается к `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Выбор столбцов.** GitLab оценивает каждый запрос и отклоняет слишком дорогой: 200 баллов для анонимного вызывающего, 250 с токеном. Широкая таблица со всеми выбранными столбцами превышает эту цену, поэтому регистрируйте таблицу GitLab с нужными вам столбцами. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- Когда вы регистрируете таблицу, Provisa один раз спрашивает GitLab, обслужит ли он выборку при размере страницы, который используют чтения. Если GitLab отвечает, что запрос слишком сложный или слишком большой, таблица не регистрируется, а результат содержит сообщение GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- Цена столбца зависит от его вида. Простое значение стоит около одного балла; столбец вложенного объекта стоит во много раз больше. Отказ от столбцов вложенных объектов освобождает больше всего. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- Размер страницы входит в цену. Это `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Проверка токена.** GitLab отвечает на нераспознанный токен пустым результатом, а не ошибкой. Provisa считает это отклонённым токеном и не добавляет источник. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### Удалённая схема gRPC (REQ-322–329)

**Как добавить источник.** Отправьте POST на `/admin/grpc-remote/register` с адресом сервера, путём или URL к файлу `.proto` и необязательной конфигурацией TLS. Добавление источника не регистрирует таблиц.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa получает proto, разбирает его чисто текстовым парсером (без внешних зависимостей proto на этапе разбора), компилирует Python-заглушки через `grpc_tools.protoc` и открывает постоянный `grpc.aio.Channel`. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Файлы proto также могут быть локальными путями. Пути импорта для хорошо известных типов (`google/protobuf/timestamp.proto`) сохраняются на момент регистрации и переиспользуются при обновлении. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**Что предлагает источник.** Каждый метод `rpc` в proto классифицируется как query или mutation по трём сигналам в порядке приоритета: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** в полезной нагрузке регистрации — `{"MethodName": "query"}` или `{"MethodName": "mutation"}` переопределяет всё остальное.
2. **`server_streaming: true`** — сервер отправляет поток сообщений; всегда виртуальная таблица (если только вывод не скаляр).
3. **Выходное сообщение имеет повторяющееся поле типа message** — например, `ListOrdersResponse { repeated Order items; }` рассматривается как обёртка списка и становится виртуальной таблицей. Повторяющиеся скалярные поля (например, `repeated string tags`) это не вызывают — они являются свойствами-массивами одной сущности, а не источниками строк.

Методы, не соответствующие ни одному из этих сигналов (унарный RPC, возвращающий одно сообщение-сущность, или любой скалярный вывод), становятся отслеживаемыми функциями.

**Регистрация таблиц.** Каждый метод query предлагается как одна таблица с именем `{namespace}__{Service}__{Method}` в схеме выбора `grpc_remote`. Зарегистрируйте нужные через выбор Register Table (`availableTables`, `availableColumns`, `registerTable`, как для источников GraphQL), выбрав столбцы ответа. Поля запроса становятся столбцами нативного фильтра `_nf_*`, и они включаются всегда. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Методы мутации — это предлагаемые команды, учитываемые в `available_mutations`; добавление источника не регистрирует ни одной. Мутация gRPC регистрируется на странице «Команды»: выбираются источник, затем метод с именем `Service.Method`; вид команды — `source_operation`. См. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**Именование таблиц.** Имя по умолчанию — `{namespace}__{ServiceName}__{MethodName}`. Без пространства имён имена сервиса и метода соединяются напрямую. Любой зарегистрированной таблице можно задать `alias`; когда он установлен, алиас — это имя, используемое повсюду (запросы, SDL, связи). Автоматически сгенерированное имя — это ключ регистрации, который никогда не меняется. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Отображение типов (REQ-324).** Скалярные типы proto отображаются на типы SQL следующим образом. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Тип Proto | Тип SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Вложенное message | `jsonb` |
| Enum | `text` |

**Связи на момент регистрации.** `relationships` работает идентично адаптеру GQL — объявляет пути соединения FK/PK, сохраняемые как связи, объявленные вручную (без флага `remote_managed`). При обновлении они остаются без изменений. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Методы Query (REQ-325).** Поля выходного сообщения становятся столбцами таблицы. Поля входного сообщения одновременно становятся аргументами GraphQL, передаваемыми в удалённый вызов, *и* регистрируются как столбцы с префиксом `_nf_` и `native_filter_type: "grpc_input"` — тот же механизм, что используют GQL и OpenAPI для внедрения нативных фильтров. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Подполя вложенного message.** Для методов query неповторяющиеся поля типа message на глубине 0 (прямые выходные столбцы) имеют свои подполя, разрешённые на один уровень вглубь и сохранённые как `object_fields` на `ColumnDef`. Эти метаданные используются для извлечения подполей `jsonb` в SQL и для документации схемы. Поля, вложенные глубже уровня 1, не раскрываются рекурсивно. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Методы с серверным стримингом собирают все переданные потоком сообщения в список перед возвратом строк. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Методы мутации (REQ-326).** Зарегистрированный метод мутации — это команда, аргументы которой — поля входного сообщения, каждое типа `json` и передаётся без изменений. Ответ удалённого сервиса возвращается как строки; отклонённый вызов — это 422, `functions.remote_refused`. См. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**Управление каналами.** Один `grpc.aio.Channel` на зарегистрированный источник хранится в состоянии приложения и переиспользуется между запросами. Старый канал закрывается до открытия нового при обновлении. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Обновление.** Отправьте POST на `/admin/grpc-remote/refresh/{source_id}`. Заново загружает proto по сохранённому пути, перекомпилирует заглушки и приводит уже зарегистрированные таблицы в соответствие с proto, с теми столбцами, с которыми каждая была зарегистрирована. Она не регистрирует новых таблиц; метод query, добавленный в proto, остаётся в предложении. Либо отправьте PUT на `/admin/grpc-remote/{source_id}/proto` с новым `proto_text`, чтобы обновить proto встроенно. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**Ограничения.**

- Извлечение подполей объекта — на один уровень вглубь. Поля вложенного message глубже уровня 1 не раскрываются рекурсивно. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Как добавить источник.** Отправьте POST на `/admin/openapi/register` с ID источника и спецификацией, загруженной из локального файла или по URL. Спецификация разбирается и хранится вместе с источником; ни таблица, ни command не регистрируются. Ответ сообщает `tables: 0` и `mutations: 0`, а предлагаемые количества указаны в `available_tables` и `available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**Регистрация таблиц.** Зарегистрируйте каждую нужную операцию GET через выбор Register Table (`availableTables`, `availableColumns`, `registerTable`), выбрав столбцы. Каждую операцию, отличную от GET, регистрируйте по отдельности как команду на странице «Команды»; они перечисляются `availableFunctions`; см. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). `PUT /admin/openapi/spec/{source_id}` сохраняет спецификацию, отредактированную вручную, ничего не регистрирует и возвращает `available_tables` и `available_mutations`. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

**Полезная нагрузка регистрации.** Эндпоинт `/admin/openapi/register` принимает два дополнительных поля наряду с `source_id`, `spec_path` и т. д.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**Что предлагает источник.** Каждая операция GET в спецификации предлагается как таблица, если только её схема ответа не является скалярным типом (`string`, `number`, `boolean`, `integer`) — операции GET, возвращающие скаляр, вместо этого являются функциями с единственным столбцом `value`. Каждая операция, отличная от GET (POST, PUT, PATCH, DELETE), предлагается как команда с именем её `operationId`. После регистрации она принимает параметры пути операции и аргумент `body` для тела запроса, каждый типа `json`; любой другой аргумент идёт в строку запроса. См. [Операция записи удалённого источника](commands.md#a-remote-sources-write-operation-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

Приоритет классификации: `operation_overrides` (полезная нагрузка) переопределяет `x-provisa-kind` (расширение спецификации), которое переопределяет эвристику GET. `operation_overrides` — рекомендуемый путь переопределения; `x-provisa-kind` — для случаев, когда сама спецификация должна нести классификацию. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Связи на момент регистрации.** `relationships` работает идентично другим адаптерам — сохраняется как связи, объявленные вручную, и сохраняется при обновлении. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Именование таблиц.** Таблицы используют `operationId` операции. Если `operationId` не определён, Provisa образует слаг `{method}_{path}`. Алиас выводится удалением ведущего сегмента глагола и приведением существительного к единственному числу (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Отображение типов.** Типы JSON Schema отображаются на типы Provisa следующим образом. [tool-verified: `provisa/openapi/register.py:59–70`]

| Тип JSON Schema | Тип Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Параметры как столбцы нативного фильтра.** Параметры пути и запроса, ещё не являющиеся полями ответа, становятся столбцами с `native_filter_type`, установленным в `path_param` или `query_param`, с префиксом `_nf_`. Когда имя параметра совпадает с именем поля ответа, метаданные параметра объединяются в существующую запись столбца, а не создают дубликат. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Разрешение схемы ответа.** Маппер проверяет `responses.200`, затем `responses.2xx`, затем `responses.default`. Ответы типа массив разворачиваются в схему их элемента. Ссылки `$ref` разрешаются на один уровень вглубь. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Подполя объекта.** Свойства ответа с `type: object` и собственными `properties` сохраняются как `object_fields` на столбце. Эти подполя видны в SDL и используются для извлечения `jsonb` в запросах. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Кеширование ответов (REQ-318).** Результаты операций GET кешируются в PostgreSQL через `pg_cache.py`. Каждая комбинация параметров запроса получает собственную группу `_params_hash`. Строки для данного хеша заменяются при истечении TTL. Эндпоинты с параметрами пути (`/pets/{id}`) пропускают первоначальную массовую выборку — таблица кеша создаётся пустой для интроспекции схемы, затем заполняется по PK по мере поступления запросов. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Обновление (REQ-321).** Отправьте POST на `/admin/openapi/refresh/{source_id}`. Заново разбирает спецификацию через `_load_and_register`, который ничего не регистрирует: он не добавляет ни таблицу, ни столбец. Существующие правила governance сохраняются. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] Зарегистрированная таблица сохраняет свои столбцы; она читается через операцию обновлённой спецификации. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**Ограничения.**

- Извлечение подполей объекта — на один уровень вглубь. Свойства, вложенные внутри `object_fields`, не раскрываются рекурсивно. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Параметры заголовков и cookie игнорируются; регистрируются только параметры `path` и `query`. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- Разрешение `$ref` на уровне спецификации — на один уровень вглубь для схем свойств; глубоко вложенные ссылки на компоненты могут не разрешиться. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Влияние регистрации удалённой таблицы

Таблица, зарегистрированная из любого источника удалённой схемы, — это полноценная таблица Provisa. Во время выполнения она ничем не отличается от локально подключённой реляционной таблицы. (REQ-308, REQ-313)

**Интерфейсы запросов.** Таблица немедленно доступна для запросов через GraphQL, SQL (pgwire или прямой), Cypher (GQL), JSON:API и Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) Генерация схемы синтезирует `ColumnMetadata` для удалённых таблиц, поскольку у них нет каталога — отображение типов применяется на этапе построения схемы. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Модель безопасности.** Применяются все пять слоёв governance:

1. Контроль доступа к домену — `domain_id` таблицы ограничивает, какие роли могут её видеть. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Безопасность на уровне строк (RLS) — фильтры строк, настроенные на таблице, внедряются в каждый запрос независимо от интерфейса. (REQ-040, REQ-041)
3. Видимость столбцов — список `visible_to` на каждом столбце управляет раскрытием поля по ролям.  (REQ-039)
4. Маскирование столбцов — правила маскирования применяются на Этапе 2 конвейера governance. (REQ-040, REQ-263)
5. Защита предикатов — замаскированные столбцы отклоняются в предложениях WHERE и HAVING. (REQ-603)

Произвольные запросы к удалённым таблицам разрешены исключительно в рамках прав пользователя — доступ единообразно основан на правах (права на таблицу/столбец + одобренные связи), без режима governance для каждой таблицы. (REQ-001, REQ-003)

**Governance связей (V002).** Условия JOIN к удалённым таблицам — при запросе через SQL или Cypher — должны соответствовать зарегистрированной, одобренной связи. (REQ-604) Проверка V002 пропускается для запросов GraphQL, потому что связи, определённые в SDL, изначально одобрены по конструкции. См. [docs/security.md](security.md#relationship-governance-v002).

**Столбцы типа OBJECT.** Когда столбец отображается на неуправляемый встроенный GQL OBJECT или объектный тип OpenAPI, его тип Provisa — `jsonb`. Столбец хранит полный вложенный блоб JSON. Когда подполя объявлены (`gql_object_fields` или `object_fields`), карта `gql_object_columns` заполняется на этапе построения схемы. Генератор SQL использует эту карту для выдачи выражений извлечения `->>` для подполей, когда запрос их выбирает. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Обязательные аргументы как параметры нативного фильтра.** Корневые поля запроса с не-null аргументами без значения по умолчанию внедряют дополнительные столбцы в зарегистрированную таблицу. Эти столбцы несут `native_filter_type: query_param`. Транслятор Cypher переписывает `WHERE n.id = $val` в `WHERE n._nf_id = $val`, а исполнитель GraphQL подбирает их как переменные для передачи удалённому эндпоинту. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Влияние создания покрывающей связи

Когда куратор регистрирует связь между двумя удалёнными таблицами (или между удалённой таблицей и локальной таблицей), связь становится путём соединения, используемым во время выполнения запроса.

**Как побеждает соединение.** На этапе компиляции запроса Provisa разрешает путь соединения через зарегистрированную связь. `source_column` и `target_column` на связи становятся условием соединения в сгенерированном SQL. Соединение заменяет любой удалённый вызов на таблицу, который иначе потребовался бы для связанного типа.

**Сырой блоб никогда не раскрывается в SQL.** Столбец `breed` на `petstore__pets` не выбираем как сырое значение jsonb в SQL-запросах. Когда между `petstore__pets` и `petstore__breeds` зарегистрирована связь, SQL-запросы проходят через соединение — `SELECT breed.name FROM petstore__pets` разрешается через соединение по FK, а не блоб. Когда связь не зарегистрирована, но столбец имеет объявленные подполя (`gql_object_fields`), ссылки на подполя в SQL переписываются в извлечение `->>` из хранимого блоба. Этот путь доступен только для неуправляемых встроенных типов — поля, ссылающиеся на управляемые типы, полностью исключены из SDL, и у них нет блоба для извлечения. Сам сырой блоб никогда не выдаётся как значение голого столбца. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

В SDL GraphQL неуправляемое встроенное поле OBJECT типизировано как вложенный тип объекта. Обслуживается ли оно соединением или извлечением блоба во время выполнения — это деталь реализации; форма SDL идентична в обоих случаях. Когда дочерний тип зарегистрирован как собственная таблица (и становится управляемым), все пять слоёв governance применяются к нему независимо: его собственные правила RLS, видимость столбцов, правила маскирования, защита предикатов и контроль доступа к домену. (REQ-039, REQ-040, REQ-041, REQ-263) Извлечение блоба это обходит — дочерние данные прибывают предварительно встроенными в родительскую строку и управляются только правилами родительской таблицы. Регистрация дочернего элемента как таблицы и создание связи — это путь к точному governance на дочернем типе.

**`graphql_alias` на связи.** Поле `graphql_alias` называет поле SDL, которое связь раскрывает на родительском типе. Когда оно отсутствует, имя выводится из `field_name` целевой таблицы и кардинальности связи через `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 на пути соединения.** Запросы SQL и Cypher, проходящие через связь, подчиняются governance связей V002. Связь должна быть зарегистрирована и одобрена, чтобы соединение было разрешено. (REQ-604) Обход через поле связи SDL в GraphQL всегда предварительно одобрен. [tool-verified: `docs/security.md:41–54`]

**Флаг remote-managed.** Связи, автоматически обнаруженные во время регистрации удалённой схемы GraphQL, сохраняются с `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Это метаданный-маркер; он не изменяет поведение governance.

---

## Поведение только-определение-типа (type-def-only)

Не каждый тип в удалённой схеме должен быть таблицей, доступной для запросов.

Когда на `SchemaInput` установлен `root_table_ids`, таблицы, чьи ID отсутствуют в этом наборе, исключаются из корневых полей запроса в сгенерированном SDL. Они остаются присутствующими как типы GraphQL и достижимы через поля связей на таблицах, которые действительно имеют корневые записи. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

Тот же механизм применяется к сборкам схемы, отфильтрованным по домену: таблицы в доменах, к которым роль не имеет доступа, являются только определениями типов — их определение типа существует в SDL для обхода связей, но корневое поле запроса для них не генерируется. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Таблица только-определение-типа:

- Не имеет корневого поля запроса — клиенты не могут запросить её напрямую по имени.
- Достижима через поля связей на таблицах, которые имеют корневые записи.
- По-прежнему появляется в интроспекции схемы как именованный тип.
- По-прежнему имеет все правила governance, применяемые при доступе к данным через связь. (REQ-039, REQ-040)

Полное удаление из схемы — включая определение типа — происходит только когда регистрация таблицы удаляется полностью. Пометка таблицы как только-определение-типа (удалением её ID из `root_table_ids` или фильтрацией по доступу к домену) не удаляет тип.

Этот дизайн позволяет кураторам предоставлять доступ к навигируемым графам объектов, где некоторые типы достижимы только через обход, а не через независимый запрос.
