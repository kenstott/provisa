# Справочник Admin GraphQL API

Административный GraphQL API — это плоскость конфигурации Provisa. Это API, который вызывает административное веб-приложение для каждой операции управления — создания источников, регистрации таблиц, определения связей, настройки правил RLS и всего остального, что формирует модель.

**Точка подключения:** `POST /admin/graphql`

Это не тот же API, что и плоскость данных на `/data/graphql`. Плоскость данных обслуживает запросы конечных пользователей по зарегистрированным доменам и описывается SDL по адресу `/data/sdl`. Административный API настраивает то, как выглядит эта схема и кто что может видеть.

---

## Как интерфейс взаимодействует с этим API

Административное веб-приложение использует Apollo Client, направленный на `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Каждый запрос несёт bearer-токен (получаемый заново от провайдера авторизации при каждом вызове), заголовок `X-Org-Id` при многоарендности и заголовок `X-Env`, когда обслуживается окружение-ветка. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

Схема собирается из двух классов `@strawberry.type` — `Query` из `schema_query.py` и `Mutation` из `schema_mutation.py` — и оборачивается в `ModelCommitExtension`, который записывает каждую мутацию против текущей ветки окружения (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Авторизация

**Режим разработки:** когда авторизация не настроена и каждый запрос приходит как анонимный принципал, все проверки возможностей пропускаются. Это сохраняет функциональность локальной установки без настройки авторизации. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Шлюзы возможностей:** производственные развёртывания применяют именованные возможности. Конкретное право, требуемое каждым полем, отмечено inline. Вызов мутации без требуемой возможности вызывает `PermissionError`. Роль администратора платформы обходит все проверки возможностей (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Доменные шлюзы:** несколько мутаций также проверяют домен, которому принадлежит объект. Вызывающий, ограниченный доменом `sales`, не может зарегистрировать таблицу в `finance`, поставить в очередь правило RLS для неё или создать связь, чья исходная таблица находится в домене, которым он не владеет (REQ-1530, REQ-1531). Представления ограничены дополнительно: каждая таблица, которую читает SQL представления, должна находиться в пределах доменов вызывающего, потому что иначе произвольный SQL даёт участнику доступ к данным вне его области. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Наследование ролей:** возможности родительской роли наследуются дочерними ролями (REQ-1677). `createRole` и `deleteRole` отклоняют циклы и не позволяют удалить роль, у которой есть наследники.

---

## Общий тип возврата

Большинство мутаций возвращают `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Когда мутация завершается неудачей, `success` равен `false`, а `message` несёт причину на английском языке. `code` — это стабильный идентификатор, который интерфейс использует для отображения локализованного сообщения.

---

## Запросы (Queries)

### Источники

#### `sources → [SourceType!]!`

Все зарегистрированные источники данных. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` — это ссылка `${secret:NAME}` в хранилище секретов организации — никогда не буквальные учётные данные. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Один источник по ID. Возвращает `null`, если не найден. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Схемы, видимые в источнике, отфильтрованные для исключения внутренних для Provisa. Сначала использует нативную интроспекцию; при отсутствии прямого пула у типа источника переходит на каталог движка. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Таблицы в одной схеме источника вместе с их комментариями. Для источников OpenAPI возвращает GET-операции, чей ответ представляет собой массив или обёртку пагинации. Для источников GraphQL возвращает поля запросов, возвращающие список. Для gRPC возвращает серверные потоковые RPC. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Имена столбцов таблицы в каталоге движка. Для источников govdata использует отдельный резолвер. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Имена столбцов с типами данных, комментариями, типами нативных фильтров и флагами первичного ключа. Для источников OpenAPI выводит форму из схемы ответа и параметров операции. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Операции, отличные от GET, для источника OpenAPI (POST, PUT, PATCH, DELETE). Возвращает пустой список для источников, отличных от OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Предпросмотр того, что обнаружил бы обход файлового коннектора — файлы, таблицы и столбцы — до создания источника. Настройки, специфичные для HTTP (`simpleLinks`, `sameDomain`, `excludePattern`), игнорируются для локальных, S3, FTP и SFTP корней. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Возвращает псевдоним для использования при регистрации `tableName` в `domainId` из `sourceId`. Возвращает обычный псевдоним в snake-case, если конфликта нет, или псевдоним с префиксом источника (`sqlite_b_orders`), если эффективное имя уже занято другим источником в том же домене. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Таблицы

#### `tables → [RegisteredTableType!]!`

Все зарегистрированные таблицы, каждая с полным списком столбцов. Видимость столбцов в ответе учитывает возможность вызывающего `table_registration` — `canDeployToDb` зависит от того, обладает ли вызывающий этим правом. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Каждый `RegisteredTableType` предоставляет вычисляемые вложенные поля:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — эффективная политика обновления/обслуживания в виде обычного текста, выведенная на сервере из того же разрешения планировщика, которое использует движок. Возвращает `null` во время запуска. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — имя поля, которое эта таблица имеет в скомпилированной схеме плоскости данных, чтобы панель продукта данных могла построить работающий пример без дублирования алгоритма именования. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — эта таблица как набор данных контракта качества данных, в форме, которую сканирует проверяющий. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — продукт данных, которому принадлежит эта таблица. Таблица-проверка DQ наследует продукт таблицы, которую сканирует её контракт. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Предпросмотр эффективной сводки обновления/обслуживания для *черновых* (несохранённых) настроек таблицы, чтобы сводка вверху формы обновлялась по мере изменения полей без сохранения чего-либо. Тот же вывод, что и `refreshPolicySummary` выше. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Аргументы: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Артефакты, которые сломало бы ожидающее переименование псевдонима или удаление столбца. Рекомендательный характер — административный интерфейс показывает это перед сохранением, а решение принимает администратор. Должен вызываться *до* сохранения, потому что зависимые объекты были созданы против имени столбца, которое он несёт в данный момент. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Связи

#### `relationships → [RelationshipType!]!`

Все связи, определённые пользователем (исключает автоматически сгенерированные записи `gql_auto__` и синтетические записи `meta:%`, используемые ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

То же, что и `relationships`, но включает синтетические записи `meta:%`. Используется графовой ERD, которой нужно показать каждое ребро, включая неявные связи `HAS_TABLE` между таблицами данных и реестром метаданных. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Каждый `RelationshipType` предоставляет:

- **`autoSuggested → Boolean`** — была ли связь предложена анализом внешних ключей (`id` начинается с `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — имя связи на плоскостях SQL и gRPC (параметр `?include=`). Выводится на сервере; клиенты не должны транслитерировать псевдоним GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Домены, роли и пользователи

#### `domains → [DomainType!]!`

Все домены в базе данных тенанта активной организации. База данных тенанта изолирована на уровне схемы, поэтому список доменов администратора организации содержит только строки его организации. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Роли, видимые вызывающему. Администратор видит каждую роль; не-администратор видит только роли без `org_id` или роли, принадлежащие его организации. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Разрешает идентификаторы ролей или пользователей в отдельных пользователей. Используется для раскрытия `DataProduct.ownerRole`, `Domain.steward` и `Column.visibleTo` в человекочитаемый список. Неизвестные ссылки возвращаются как есть, поэтому интерфейс показывает исходный ID, а не ничего. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### Правила RLS

#### `rlsRules → [RLSRuleType!]!`

Все правила безопасности на уровне строк. Базовый репозиторий расшифровывает `filterExpr` на границе. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Продукты данных

#### `dataProducts → [DataProductType!]!`

Все продукты данных. Требует возможность `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Теги

#### `tags → [TagType!]!`

Все определения тегов, включая допустимые значения параметров для каждого тега. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Все назначения тегов по источникам, таблицам, столбцам и связям. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Материализованные представления

#### `mvList → [MVType!]!`

Все материализованные представления с их состоянием во время выполнения: включено/выключено, метка времени последнего обновления, количество строк и последняя ошибка. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Кеш

#### `cacheStats → CacheStatsType`

Статистика кеша. Возвращает `storeType: "redis"` с полными операционными метриками, когда настроен Redis, `storeType: "memory"` для встроенного хранилища fakeredis и `storeType: "noop"`, когда кеш не настроен. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Количество кешированных записей на таблицу. Пусто, когда хранилище кеша не настроено. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Таблицы, копию которых Provisa хранит, в двух уровнях: `hot` (отражена в хранилище ответов для встраивания в JOIN) и `warm` (приземлена как копия Iceberg). Таблица находится не более чем в одном уровне (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Идентификация устойчивого хранилища материализации: имя движка, ссылка DSN хранилища, количество MV и является ли хранилище локальным для экземпляра (локальный файл, такой как DuckDB или SQLite, что означает, что каждый экземпляр за балансировщиком нагрузки хранит собственную копию). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Состояние системы

#### `systemHealth → SystemHealthType`

Состояние подключения движка, количество рабочих процессов, состояние пула БД метаданных, режим кеша и активность каждого слушателя протокола (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Запланированные задачи

#### `scheduledTasks → [ScheduledTaskType!]!`

Запланированные триггеры из конфигурации вместе с состоянием во время выполнения. Каждая запись несёт своё cron-выражение, `kind` (`webhook` или `sql`), включена ли она в данный момент, метку времени последнего запуска (всегда `null` в этом релизе — отслеживается планировщиком) и время следующего запланированного запуска от APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Качество данных

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Разбор необработанного текста контракта в редактируемые строки панели построителя. Вызывается при каждом редактировании; сбой разбора возвращается как `error`, а не как ошибка GraphQL, потому что наполовину написанный текст — обычное явление, пока оператор печатает. (REQ-1443, пункт 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Проверки, которые предлагает `checker`, ограниченные столбцами `dataset`. Набор данных — это наблюдаемая цель контракта, разрешаемая тем же способом, каким её разрешает сканер, — поэтому предлагаемые проверки соответствуют столбцам, которые проверяющий реально увидит. (REQ-1443, пункт 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Текст одной проверки из редакторов панели. На стороне сервера, потому что у диалекта одна реализация; проверка, созданная построителем, и введённая вручную должны быть неотличимы. (REQ-1443, пункт 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Сериализация отредактированных строк проверок обратно в текст контракта. Обратная операция для `dqContractParse`. На стороне сервера по той же причине: панель не может выдать текст, который проверяющий отклонит. (REQ-1443, пункт 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Предпросмотры источников

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Предпросмотр проекции Cypher на источнике Neo4j: до пяти строк и типы столбцов, которые понесёт регистрация. Сбои возвращаются как `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Предпросмотр SPARQL SELECT на источнике SPARQL: до пяти строк, все столбцы как текст. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Живая проверка против API Kaggle. Возвращает `true`, только когда токен аутентифицируется. Обеспечивает шаг проверки токена в форме источника Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Поиск по полному публичному каталогу наборов данных Kaggle. Доступ по токену. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Календари

#### `calendars → [CalendarType!]!`

Все зарегистрированные версии календарей границ снимков. Питает выбор конфигурации расписания снимков и подтверждает, какие календари может использовать периодическое MV. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Метрики

#### `metrics → [MetricType!]!`

Все управляемые определения метрик. Метрики, выведенные из фактов, несут `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Версия схемы

#### `schemaVersion → String!`

Хеш SHA-256 текущего состояния схемы (домены, идентификаторы таблиц, идентификаторы связей). Клиент Apollo читает это из заголовка ответа `X-Schema-Version` и повторно выполняет все активные запросы, когда версия продвигается. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### Помощники ИИ

#### `generateTableDescription(tableId: String!) → String!`

Использовать настроенную LLM для генерации описания из одного-двух предложений для зарегистрированной таблицы. Сначала сохраните таблицу; вызов этого для несохранённой таблицы возвращает инструктивное сообщение. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Использовать настроенную LLM для генерации описания из одного предложения для отдельного столбца. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Запросы на создание

#### `creationRequests → [CreationRequestType!]!`

Ожидающие запросы на создание, видимые вызывающим, обладающим соответствующей возможностью создания. Используется, когда участник без `create_relationship` или `create_view` отправляет запрос, который должен утвердить обладатель прав. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Мутации

### Источники

#### `createSource(input: SourceInput!) → MutationResult`

Зарегистрировать новый источник данных. Проверяет подключение перед сохранением — отклонённый источник не оставляет записи в хранилище секретов. Хранит учётные данные в хранилище секретов организации и записывает ссылку; открытый текст никогда не попадает в базу данных. (REQ-012, REQ-013) Требует возможность `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Обновить детали подключения, описание и конфигурацию существующего источника. Разбирает и повторно подключает конечную точку pgwire для источников файлов/SharePoint, чтобы изменение пути вступило в силу немедленно. Требует `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Удалить источник и его запись в хранилище секретов. Удаляет каталог движка и перестраивает схемы. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Переименовать идентификатор источника. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Включить или отключить кеширование результатов запросов для источника и задать TTL в секундах. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Принудительно включить (или отключить) материализованную федерацию для всех таблиц источника. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Пометить источник как защищённый от нагрузки (только по расписанию обновления). Требует хотя бы один шлюз — окно вне пиковых часов, периодичность TTL кеша или зондирующий сигнал изменения — иначе вызов завершается неудачей. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Задать соглашение об именовании GraphQL для отдельного источника. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Задать, какие домены могут использовать источник (пустой список = без ограничений). Требует `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Скачать и распаковать набор данных Kaggle на локальный диск. Возвращает путь к подготовленному каталогу; затем вызывающая сторона создаёт один источник типа `files`, указывающий на него. Пакеты, несущие SQLite, отклоняются целиком. Требует `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Повторно получить набор данных источника, производного от Kaggle, на месте. Пропускает загрузку, если у Kaggle нет ничего новее того, что уже на диске. Требует `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Выполнить `ANALYZE` для всех зарегистрированных таблиц источника. Улучшает решения о порядке соединений и широковещательной передаче для федеративных запросов. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Таблицы

#### `registerTable(input: TableInput!) → MutationResult`

Зарегистрировать новую таблицу (или представление) в домене. Требует возможность `table_registration` и членство в целевом домене. Вызывающий без `create_relationship`, отправляющий представление, ставится в очередь как запрос на создание для утверждения обладателем прав. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Обновить псевдоним, описание, метаданные столбцов, настройки MV и конфигурацию живой доставки существующей таблицы. (REQ-016, REQ-020) Требует `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Удалить зарегистрированную таблицу. Ищет домен таблицы для доменного шлюза перед удалением. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Переопределить TTL кеша для одной таблицы. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Переопределить материализованную федерацию для одной таблицы. `null` = наследовать значение источника по умолчанию. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Переопределить защиту от нагрузки для одной таблицы. `null` для `loadProtected` наследует значение источника по умолчанию. Проверяет эффективную комбинацию шлюзов (таблица → источник). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Задать соглашение об именовании GraphQL для отдельной таблицы. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Продвинуть виртуальное представление Provisa до реального представления базы данных на его базовом нативном источнике. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Пересчитать приземлённые строки таблицы по требованию, минуя обычный шлюз изменений. `reason` — обязательная аннотация для аудита. Отклоняется для живо-федеративных таблиц (нет приземлённых строк). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Принудительно синхронизировать таблицу файлового коннектора SQLite с диска при следующем обращении. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Синтаксический сахар для регистрации сущности измерения/хаба. Сводится к MV (битемпоральному, при историзации) и вызывает `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Синтаксический сахар для регистрации факта звёздной схемы. Сводится к агрегатному MV, создаёт связи с измерениями и автоматически регистрирует меры факта как управляемые метрики. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Связи

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Создать или обновить связь. Шлюз проверяет домен исходной таблицы (не целевой). Междоменное ребро сохраняется с `needsReview: true`. Вызывающий без `create_relationship` ставится в очередь как запрос на создание. Связующие (многие-ко-многим) рёбра требуют совпадения длин списков ключей. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Удалить связь по ID и перестроить схемы. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Домены

#### `createDomain(input: DomainInput!) → MutationResult`

Создать домен. Зарезервированные слова сегментов (`tables`, `relationships` и другие сегменты пути URI) отклоняются, как и литерал подстановочного знака `*`. Требует возможность `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Удалить домен. Требует `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Задать глобальное соглашение об именовании GraphQL и перестроить схемы для всех ролей. Принимаются только распознанные имена соглашений. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Роли

#### `createRole(input: RoleInput!) → MutationResult`

Создать или заменить роль с возможностями, доступом к доменам, необязательными ограничениями частоты запросов и необязательной родительской ролью. Проверяет, что родитель существует и что родительская цепочка не содержит циклов. Требует `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Удалить роль. Завершается неудачей, если от неё наследуют другие роли — сначала переподчините их. Требует `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### Правила RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Создать или обновить правило безопасности на уровне строк. Выражение фильтра проверяется во время сохранения по столбцам целевой таблицы или домена, поэтому правило, которое администратор не может запросить, отклоняется с указанием причины, а не молча отказывает во время выполнения запроса. Требует `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Цели взаимоисключающие: задайте `tableId` для правила уровня таблицы, `domainId` для правила уровня домена или `actionName` для отслеживаемой функции/вебхука. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Удалить правило RLS. Требует `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Продукты данных

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Создать или заменить продукт данных. Требует возможность `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Удалить продукт данных. Требует `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Теги

#### `upsertTag(input: TagInput!) → MutationResult`

Создать или обновить определение тега. Системные теги и производные теги не могут быть переопределены. `appliesTo` должен быть непустым подмножеством `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Удалить тег. Отклоняет системные и производные теги. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Назначить тег источнику, таблице, столбцу, связи или команде. Применяет политики полей тега (`reason_policy`, `expires_policy`) и — для параметризованных тегов — проверяет значение параметра по допустимому списку тега. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Удалить назначение тега. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Добавить или переописать допустимое значение параметра для параметризованного тега. Список допустимых значений закрыт: каждое назначение должно называть значение из него. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Удалить допустимое значение. Отклоняется, пока хотя бы одно назначение всё ещё несёт его, потому что эти назначения называли бы тип, который список больше не допускает. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Метрики

#### `upsertMetric(input: MetricInput!) → MutationResult`

Создать или заменить определение управляемой метрики. Выражение должно разбираться под sqlglot и содержать хотя бы одну агрегатную функцию. Регенерирует все представления, составленные из метрик, которые ссылаются на эту метрику. Требует `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Удалить управляемую метрику. Перестраивает схемы. Требует `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Календари

#### `createCalendar(input: CalendarInput!) → MutationResult`

Создать или заменить версионированный календарь границ снимков. Проверяется путём построения объекта `Calendar` в памяти перед сохранением — завершается неудачей при неизвестной базовой системе, неверном часовом поясе или неверном фискальном якоре. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Удалить календарь (все версии). Отклоняется, если на него ссылается какое-либо материализованное представление. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Материализованные представления

#### `refreshMv(mvId: String!) → MutationResult`

Инициировать ручное обновление материализованного представления. Координирует по всему парку экземпляров, когда режим согласованности MV — `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Включить или отключить материализованное представление. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Кеш

#### `purgeCache → MutationResult`

Очистить все кешированные результаты запросов. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Очистить кешированные результаты для одной таблицы. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Запланированные задачи

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Создать запланированный триггер — либо вызов вебхука, либо оператор SQL — и зарегистрировать его вживую в APScheduler. `kind` — это `"webhook"` или `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Удалить запланированный триггер из конфигурации и работающего планировщика. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Включить или отключить запланированную задачу в файле конфигурации. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Качество данных

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Выполнить контракт против живой таблицы и вернуть результаты, ничего не приземляя. Мутация, а не запрос, потому что это стоит реального сканирования. Она доказывает, разрешается ли идентификатор набора данных в управляемую таблицу, которую имеет в виду оператор. (REQ-1443, пункт 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Немедленно запустить задачу опроса таблицы-проверки. Приземляет строки обычным способом, поэтому результаты сохраняются, и история DQ показывает новое сканирование. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Обслуживание схемы

#### `rebuildSchemas → MutationResult`

Перестроить схему в памяти из состояния базы данных. Полезно после внешних изменений базы данных. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Запросы на создание

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Обладатель прав выполняет поставленный в очередь запрос на создание — связь, представление или вебхук. Требует возможность, которую ожидает запрос. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Отклонить поставленный в очередь запрос с действенной причиной. `reason` обязателен. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Компиляция запросов

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Скомпилировать запрос GraphQL плоскости данных по схеме роли и вернуть полное решение маршрутизации: семантический SQL, SQL движка, прямой SQL, маршрут, метаданные применения (применённые фильтры RLS, исключённые столбцы, применённое маскирование) и скомпилированный Cypher. Возвращает один результат на каждое корневое поле в запросе. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Поля `CompileQueryInput`:

| Поле | Тип | Описание |
|-------|------|-------------|
| `query` | `String!` | Запрос GraphQL плоскости данных для компиляции |
| `role` | `String!` | Роль, по схеме которой компилировать |
| `variables` | `JSON` | Привязки переменных |
| `flatSql` | `Boolean` | Вернуть одну сведённую строку SQL вместо пары семантический/движковый |
| `flatCypher` | `Boolean` | Свести вывод Cypher |
| `nodeOnlyCypher` | `Boolean` | Выдать Cypher только с узлами (без паттернов рёбер) |

---

## Ключевые входные типы

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| Поле | Тип | Примечания |
|-------|------|-------|
| `id` | `String!` | Идентификатор источника |
| `type` | `String!` | Тип коннектора (например, `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Открытый текст или ссылка `${secret:NAME}` |
| `path` | `String` | Путь в файловой системе для файловых/CSV-источников |
| `federationHintsJson` | `String` | Объект JSON для дополнительных параметров хранилища (Snowflake warehouse/role, Databricks http_path) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Только по расписанию обновления (REQ-1141) |
| `offPeakWindow` | `String` | Окно обслуживания `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Часовой пояс IANA |
| `cdc` | `SourceCdcConfigInput` | Конфигурация транспорта Kafka CDC (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

Основной входной тип регистрации таблицы. Ключевые поля сверх базовых:

| Поле | Примечания |
|-------|-------|
| `materialize` | Приземлить копию в хранилище материализации |
| `mvRefreshInterval` | Секунды между обновлениями |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Инкрементальное обслуживание (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` для битемпоральных таблиц (REQ-1162) |
| `mvCalendar` | Имя календаря снимков (REQ-962) |
| `mvGrain` | Гранулярность снимка: `daily`, `weekly`, `monthly`, `annual` или пользовательская `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL для производного представления |
| `viewMetrics` | Декларативная спецификация представления, составленного из метрик — взаимоисключающая с `viewSql` (REQ-1318) |
| `dqContract` | Текст контракта качества данных в YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher для таблицы Neo4j (REQ-1670) |
| `live` | Конфигурация живой доставки для push SSE/Kafka (REQ-565, REQ-813) |
| `discover` | Выводить столбцы из живого источника во время регистрации (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| Поле | Примечания |
|-------|-------|
| `id` | Идентификатор связи |
| `sourceTableId` | Имя виртуальной таблицы (псевдоним, если задан, иначе имя таблицы) |
| `targetTableId` | Имя виртуальной таблицы; пусто для вычисляемых связей |
| `sourceColumn` | Столбец соединения на исходной стороне |
| `targetColumn` | Столбец соединения на целевой стороне |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Метка ребра Cypher (например, `WORKS_FOR`) |
| `graphqlAlias` | Имя поля GraphQL на исходном типе |
| `viaTable` | Имя связующей таблицы для рёбер многие-ко-многим (REQ-1586) |
| `recordCandidate` | Также записать строку `relationship_candidates` со статусом `accepted` |

---

## Пример: регистрация таблицы

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Пример: создание правила RLS

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
