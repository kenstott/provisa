# Справочник Admin GraphQL API {: #admin-graphql-api-reference }

Административный GraphQL API — это плоскость конфигурации Provisa. Это API, который вызывает административное веб-приложение для каждой операции управления — создания источников, регистрации таблиц, определения связей, настройки правил RLS и всего остального, что формирует модель.

**Точка подключения:** `POST /admin/graphql`

Это не тот же API, что и плоскость данных на `/data/graphql`. Плоскость данных обслуживает запросы конечных пользователей по зарегистрированным доменам и описывается SDL по адресу `/data/sdl`. Административный API настраивает то, как выглядит эта схема и кто что может видеть.

---

## Как интерфейс взаимодействует с этим API {: #how-the-ui-talks-to-this-api }

Административное веб-приложение использует Apollo Client, направленный на `${API_BASE}/admin/graphql`.

Каждый запрос несёт bearer-токен (получаемый заново от провайдера авторизации при каждом вызове), заголовок `X-Org-Id` при многоарендности и заголовок `X-Env`, когда обслуживается окружение-ветка.

Схема собирается из двух классов `@strawberry.type` — `Query` из `schema_query.py` и `Mutation` из `schema_mutation.py` — и оборачивается в `ModelCommitExtension`, который записывает каждую мутацию против текущей ветки окружения.

---

## Авторизация {: #authorization }

**Режим разработки:** когда авторизация не настроена и каждый запрос приходит как анонимный принципал, все проверки возможностей пропускаются. Это сохраняет функциональность локальной установки без настройки авторизации.

**Шлюзы возможностей:** производственные развёртывания применяют именованные возможности. Конкретное право, требуемое каждым полем, отмечено inline. Вызов мутации без требуемой возможности вызывает `PermissionError`. Роль администратора платформы обходит все проверки возможностей.

**Доменные шлюзы:** несколько мутаций также проверяют домен, которому принадлежит объект. Вызывающий, ограниченный доменом `sales`, не может зарегистрировать таблицу в `finance`, поставить в очередь правило RLS для неё или создать связь, чья исходная таблица находится в домене, которым он не владеет. Представления ограничены дополнительно: каждая таблица, которую читает SQL представления, должна находиться в пределах доменов вызывающего, потому что иначе произвольный SQL даёт участнику доступ к данным вне его области.

**Наследование ролей:** возможности родительской роли наследуются дочерними ролями. `createRole` и `deleteRole` отклоняют циклы и не позволяют удалить роль, у которой есть наследники.

---

## Общий тип возврата {: #common-return-type }

Большинство мутаций возвращают `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Когда мутация завершается неудачей, `success` равен `false`, а `message` несёт причину на английском языке. `code` — это стабильный идентификатор, который интерфейс использует для отображения локализованного сообщения.

---

## Запросы (Queries)

### Источники {: #sources }

#### `sources → [SourceType!]!`

Все зарегистрированные источники данных.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` — это ссылка `${secret:NAME}` в хранилище секретов организации — никогда не буквальные учётные данные.

#### `source(id: String!) → SourceType`

Один источник по ID. Возвращает `null`, если не найден.

#### `availableSchemas(sourceId: String!) → [String!]!`

Схемы, видимые в источнике, отфильтрованные для исключения внутренних для Provisa. Сначала использует нативную интроспекцию; при отсутствии прямого пула у типа источника переходит на каталог движка.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Таблицы в одной схеме источника вместе с их комментариями. Для источников OpenAPI возвращает GET-операции, чей ответ представляет собой массив или обёртку пагинации. Для источников GraphQL возвращает поля запросов, возвращающие список. Для gRPC возвращает серверные потоковые RPC.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Имена столбцов таблицы в каталоге движка. Для источников govdata использует отдельный резолвер.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Имена столбцов с типами данных, комментариями, типами нативных фильтров и флагами первичного ключа. Для источников OpenAPI выводит форму из схемы ответа и параметров операции.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Операции, отличные от GET, для источника OpenAPI (POST, PUT, PATCH, DELETE). Возвращает пустой список для источников, отличных от OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Предпросмотр того, что обнаружил бы обход файлового коннектора — файлы, таблицы и столбцы — до создания источника. Настройки, специфичные для HTTP (`simpleLinks`, `sameDomain`, `excludePattern`), игнорируются для локальных, S3, FTP и SFTP корней.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Возвращает псевдоним для использования при регистрации `tableName` в `domainId` из `sourceId`. Возвращает обычный псевдоним в snake-case, если конфликта нет, или псевдоним с префиксом источника (`sqlite_b_orders`), если эффективное имя уже занято другим источником в том же домене.

---

### Таблицы {: #tables }

#### `tables → [RegisteredTableType!]!`

Все зарегистрированные таблицы, каждая с полным списком столбцов. Видимость столбцов в ответе учитывает возможность вызывающего `table_registration` — `canDeployToDb` зависит от того, обладает ли вызывающий этим правом.

Каждый `RegisteredTableType` предоставляет вычисляемые вложенные поля:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — эффективная политика обновления/обслуживания в виде обычного текста, выведенная на сервере из того же разрешения планировщика, которое использует движок. Возвращает `null` во время запуска.
- **`graphqlFieldName → String`** — имя поля, которое эта таблица имеет в скомпилированной схеме плоскости данных, чтобы панель продукта данных могла построить работающий пример без дублирования алгоритма именования.
- **`dqDataset → String`** — эта таблица как набор данных контракта качества данных, в форме, которую сканирует проверяющий.
- **`productId → String`** — продукт данных, которому принадлежит эта таблица. Таблица-проверка DQ наследует продукт таблицы, которую сканирует её контракт.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Предпросмотр эффективной сводки обновления/обслуживания для *черновых* (несохранённых) настроек таблицы, чтобы сводка вверху формы обновлялась по мере изменения полей без сохранения чего-либо. Тот же вывод, что и `refreshPolicySummary` выше.

Аргументы: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Артефакты, которые сломало бы ожидающее переименование псевдонима или удаление столбца. Рекомендательный характер — административный интерфейс показывает это перед сохранением, а решение принимает администратор. Должен вызываться *до* сохранения, потому что зависимые объекты были созданы против имени столбца, которое он несёт в данный момент.

---

### Связи {: #relationships }

#### `relationships → [RelationshipType!]!`

Все связи, определённые пользователем (исключает автоматически сгенерированные записи `gql_auto__` и синтетические записи `meta:%`, используемые ERD).

#### `allRelationships → [RelationshipType!]!`

То же, что и `relationships`, но включает синтетические записи `meta:%`. Используется графовой ERD, которой нужно показать каждое ребро, включая неявные связи `HAS_TABLE` между таблицами данных и реестром метаданных.

Каждый `RelationshipType` предоставляет:

- **`autoSuggested → Boolean`** — была ли связь предложена анализом внешних ключей (`id` начинается с `fk__`).
- **`physicalName → String`** — имя связи на плоскостях SQL и gRPC (параметр `?include=`). Выводится на сервере; клиенты не должны транслитерировать псевдоним GraphQL.

---

### Домены, роли и пользователи {: #domains-roles-users }

#### `domains → [DomainType!]!`

Все домены в базе данных тенанта активной организации. База данных тенанта изолирована на уровне схемы, поэтому список доменов администратора организации содержит только строки его организации.

#### `roles → [RoleType!]!`

Роли, видимые вызывающему. Администратор видит каждую роль; не-администратор видит только роли без `org_id` или роли, принадлежащие его организации.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Разрешает идентификаторы ролей или пользователей в отдельных пользователей. Используется для раскрытия `DataProduct.ownerRole`, `Domain.steward` и `Column.visibleTo` в человекочитаемый список. Неизвестные ссылки возвращаются как есть, поэтому интерфейс показывает исходный ID, а не ничего.

---

### Правила RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Все правила безопасности на уровне строк. Базовый репозиторий расшифровывает `filterExpr` на границе.

---

### Продукты данных {: #data-products }

#### `dataProducts → [DataProductType!]!`

Все продукты данных. Требует возможность `data_product_read`.

---

### Теги {: #tags }

#### `tags → [TagType!]!`

Все определения тегов, включая допустимые значения параметров для каждого тега.

#### `tagAssignments → [TagAssignmentType!]!`

Все назначения тегов по источникам, таблицам, столбцам и связям.

---

### Материализованные представления {: #materialized-views }

#### `mvList → [MVType!]!`

Все материализованные представления с их состоянием во время выполнения: включено/выключено, метка времени последнего обновления, количество строк и последняя ошибка.

---

### Кеш {: #cache }

#### `cacheStats → CacheStatsType`

Статистика кеша. Возвращает `storeType: "redis"` с полными операционными метриками, когда настроен Redis, `storeType: "memory"` для встроенного хранилища fakeredis и `storeType: "noop"`, когда кеш не настроен.

#### `cacheTableStats → [CacheTableStatType!]!`

Количество кешированных записей на таблицу. Пусто, когда хранилище кеша не настроено.

#### `hotTables → [HotTableStatType!]!`

Таблицы, копию которых Provisa хранит, в двух уровнях: `hot` (отражена в хранилище ответов для встраивания в JOIN) и `warm` (приземлена как копия Iceberg). Таблица находится не более чем в одном уровне.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Идентификация устойчивого хранилища материализации: имя движка, ссылка DSN хранилища, количество MV и является ли хранилище локальным для экземпляра (локальный файл, такой как DuckDB или SQLite, что означает, что каждый экземпляр за балансировщиком нагрузки хранит собственную копию).

---

### Состояние системы {: #system-health }

#### `systemHealth → SystemHealthType`

Состояние подключения движка, количество рабочих процессов, состояние пула БД метаданных, режим кеша и активность каждого слушателя протокола (pgwire, gRPC, Arrow Flight, Bolt).

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

### Запланированные задачи {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Запланированные триггеры из конфигурации вместе с состоянием во время выполнения. Каждая запись несёт своё cron-выражение, `kind` (`webhook` или `sql`), включена ли она в данный момент, метку времени последнего запуска (всегда `null`; его отслеживает планировщик) и время следующего запланированного запуска от APScheduler.

---

### Качество данных {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Разбор необработанного текста контракта в редактируемые строки панели построителя. Вызывается при каждом редактировании; сбой разбора возвращается как `error`, а не как ошибка GraphQL, потому что наполовину написанный текст — обычное явление, пока оператор печатает. (пункт 7)

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Проверки, которые предлагает `checker`, ограниченные столбцами `dataset`. Набор данных — это наблюдаемая цель контракта, разрешаемая тем же способом, каким её разрешает сканер, — поэтому предлагаемые проверки соответствуют столбцам, которые проверяющий реально увидит. (пункт 7)

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Текст одной проверки из редакторов панели. На стороне сервера, потому что у диалекта одна реализация; проверка, созданная построителем, и введённая вручную должны быть неотличимы. (пункт 7)

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Сериализация отредактированных строк проверок обратно в текст контракта. Обратная операция для `dqContractParse`. На стороне сервера по той же причине: панель не может выдать текст, который проверяющий отклонит. (пункт 7)

---

### Предпросмотры источников {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Предпросмотр проекции Cypher на источнике Neo4j: до пяти строк и типы столбцов, которые понесёт регистрация. Сбои возвращаются как `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Предпросмотр SPARQL SELECT на источнике SPARQL: до пяти строк, все столбцы как текст.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Живая проверка против API Kaggle. Возвращает `true`, только когда токен аутентифицируется. Обеспечивает шаг проверки токена в форме источника Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Поиск по полному публичному каталогу наборов данных Kaggle. Доступ по токену.

---

### Календари {: #calendars }

#### `calendars → [CalendarType!]!`

Все зарегистрированные версии календарей границ снимков. Питает выбор конфигурации расписания снимков и подтверждает, какие календари может использовать периодическое MV.

---

### Метрики {: #metrics }

#### `metrics → [MetricType!]!`

Все управляемые определения метрик. Метрики, выведенные из фактов, несут `fromFact`.

---

### Версия схемы {: #schema-version }

#### `schemaVersion → String!`

Хеш SHA-256 текущего состояния схемы (домены, идентификаторы таблиц, идентификаторы связей). Клиент Apollo читает это из заголовка ответа `X-Schema-Version` и повторно выполняет все активные запросы, когда версия продвигается.

---

### Помощники ИИ {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Использовать настроенную LLM для генерации описания из одного-двух предложений для зарегистрированной таблицы. Сначала сохраните таблицу; вызов этого для несохранённой таблицы возвращает инструктивное сообщение.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Использовать настроенную LLM для генерации описания из одного предложения для отдельного столбца.

---

### Запросы на создание {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Ожидающие запросы на создание, видимые вызывающим, у которых есть соответствующая возможность создания. Используется, когда участник без `create_relationship` или `create_view` отправляет запрос, который должен одобрить обладатель права. Запрос на связь показывается пользователям, чьё право `create_relationship` достигает домена её исходной или целевой таблицы, и пользователю, который его сделал.

---

## Мутации {: #mutations }

### Источники {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

Регистрирует новый источник данных. Уже существующий id отклоняется (`schema.source_id_taken`) после проверок определения и до того, как что-либо подготовлено или записано; источник изменяется через `updateSource`. Проверяет подключение перед сохранением — отклонённый источник не оставляет записи в хранилище. Хранит учётные данные в хранилище организации и записывает ссылку; открытый текст никогда не попадает в базу данных. Требует возможности `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Обновляет параметры подключения, описание и конфигурацию существующего источника, затем заново подготавливает его на привязанном движке, пересобирает схемы и согласует его загруженные таблицы, так что источник, сохранённый при одном движке, обслуживается движком, на который переключились позже. Отключает и заново подключает endpoint pgwire для источников файлов/SharePoint, чтобы изменение пути вступило в силу сразу. Требует `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Удалить источник и его запись в хранилище секретов. Удаляет каталог движка и перестраивает схемы.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Переименовывает id источника. Новый id, которым владеет другой источник, отклоняется (`schema.source_id_taken`), и ничего не записывается; переименование источника в его собственный id ничего не меняет.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Включить или отключить кеширование результатов запросов для источника и задать TTL в секундах.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Задаёт, когда таблицы источника обслуживаются из их реплик: `null` Default, `-1` Never, `N` больше 0 — как только таблица превысит N регулируемых инструкций за интервал, `0` Always. Таблица без собственного значения наследует его. Отклоняется, если сочетало бы `load_protected` с `-1` или оставило бы без `cache_ttl` таблицу `ttl`, которую реплицирует. Требует `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Пометить источник как защищённый от нагрузки (только по расписанию обновления). Требует хотя бы один шлюз — окно вне пиковых часов, периодичность TTL кеша или зондирующий сигнал изменения — иначе вызов завершается неудачей.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Задать соглашение об именовании GraphQL для отдельного источника.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Задать, какие домены могут использовать источник (пустой список = без ограничений). Требует `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Скачать и распаковать набор данных Kaggle на локальный диск. Возвращает путь к подготовленному каталогу; затем вызывающая сторона создаёт один источник типа `files`, указывающий на него. Пакеты, несущие SQLite, отклоняются целиком. Требует `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Повторно получить набор данных источника, производного от Kaggle, на месте. Пропускает загрузку, если у Kaggle нет ничего новее того, что уже на диске. Требует `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Выполнить `ANALYZE` для всех зарегистрированных таблиц источника. Улучшает решения о порядке соединений и широковещательной передаче для федеративных запросов.

---

### Таблицы {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

Зарегистрировать новую таблицу (или представление) в домене. Требует возможность `table_registration` и членство в целевом домене. Вызывающий без `create_relationship`, отправляющий представление, ставится в очередь как запрос на создание для утверждения обладателем прав.

#### `updateTable(input: TableInput!) → MutationResult`

Обновить псевдоним, описание, метаданные столбцов, настройки MV и конфигурацию живой доставки существующей таблицы. Требует `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Удалить зарегистрированную таблицу. Ищет домен таблицы для доменного шлюза перед удалением.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Переопределить TTL кеша для одной таблицы.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Задаёт, когда одна таблица обслуживается из своей реплики. `null` = наследовать значение источника. Те же значения и отказы, что у `updateSourceReplicate`. Требует `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Переопределить защиту от нагрузки для одной таблицы. `null` для `loadProtected` наследует значение источника по умолчанию. Проверяет эффективную комбинацию шлюзов (таблица → источник).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Задать соглашение об именовании GraphQL для отдельной таблицы.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Продвинуть виртуальное представление Provisa до реального представления базы данных на его базовом нативном источнике.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Пересчитать приземлённые строки таблицы по требованию, минуя обычный шлюз изменений. `reason` — обязательная аннотация для аудита. Отклоняется для живо-федеративных таблиц (нет приземлённых строк).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Принудительно синхронизировать таблицу файлового коннектора SQLite с диска при следующем обращении.

#### `registerEntity(input: EntityInput!) → MutationResult`

Синтаксический сахар для регистрации сущности измерения/хаба. Сводится к MV (битемпоральному, при историзации) и вызывает `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Синтаксический сахар для регистрации факта звёздной схемы. Сводится к агрегатному MV, создаёт связи с измерениями и автоматически регистрирует меры факта как управляемые метрики.

---

### Связи {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Создаёт связь. Проверка смотрит на домен исходной таблицы (не целевой). Междоменное ребро сохраняется с `needsReview: true`. Вызывающий без `create_relationship` ставится в очередь как запрос на создание. Рёбра соединения (многие-ко-многим) требуют списков ключей одинаковой длины. Отклоняется по имени до какой-либо записи: уже существующий id (`schema.relationship_id_taken`); имя таблицы, которое не разрешается ни в одну зарегистрированную таблицу или разрешается в несколько (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); псевдоним Cypher или GraphQL, который уже занят другой связью той же исходной таблицы (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Таблица называется своим виртуальным именем: своим псевдонимом, если он есть.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Изменяет существующую связь с теми же проверками, что у `createRelationship`. Id, которым не владеет ни одна связь, отклоняется (`schema.relationship_not_found`). Изменению нужно само право; оно никогда не ставится в очередь как запрос.

#### `deleteRelationship(id: String!) → MutationResult`

Удалить связь по ID и перестроить схемы.

---

### Домены {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Создаёт домен. Уже существующий id отклоняется (`schema.domain_id_taken`). Зарезервированные слова сегментов (`tables`, `relationships` и другие сегменты пути URI) отклоняются, как и литерал подстановки `*`. Требует возможности `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Удалить домен. Требует `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Задать глобальное соглашение об именовании GraphQL и перестроить схемы для всех ролей. Принимаются только распознанные имена соглашений.

---

### Роли {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

Создаёт роль с возможностями, доступом к доменам, необязательными лимитами запросов и необязательной родительской ролью. Проверяет, что родитель существует и цепочка родителей не содержит циклов. После проверок определения уже существующий id отклоняется (`schema.role_id_taken`). Требует `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Изменяет существующую роль с теми же проверками, что у `createRole`. Id, которым не владеет ни одна роль, отклоняется (`schema.role_not_found`). Переопределение роли — это действие в каждом домене, которого она достигает.

#### `deleteRole(id: String!) → MutationResult`

Удалить роль. Завершается неудачей, если от неё наследуют другие роли — сначала переподчините их. Требует `user_management`.

---

### Правила RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Создать или обновить правило безопасности на уровне строк. Выражение фильтра проверяется во время сохранения по столбцам целевой таблицы или домена, поэтому правило, которое администратор не может запросить, отклоняется с указанием причины, а не молча отказывает во время выполнения запроса. Требует `masking_config`.

Цели взаимоисключающие: задайте `tableId` для правила уровня таблицы, `domainId` для правила уровня домена или `actionName` для отслеживаемой функции/вебхука.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Удалить правило RLS. Требует `masking_config`.

---

### Продукты данных {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Создаёт продукт данных. Уже существующий id отклоняется (`schema.data_product_id_taken`). Требует возможности `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Изменяет существующий продукт данных. Id, которым не владеет ни один продукт, отклоняется (`schema.data_product_not_found`). Требует `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Удалить продукт данных. Требует `data_product_rw`.

---

### Теги {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

Создаёт определение тега. Уже существующий id отклоняется (`schema.tag_id_taken`). Системные и производные теги нельзя переопределить. `appliesTo` должен быть непустым подмножеством `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Изменяет существующее определение тега с теми же проверками, что у `createTag`. Id, которым не владеет ни один тег, отклоняется (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Удалить тег. Отклоняет системные и производные теги.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Назначить тег источнику, таблице, столбцу, связи или команде. Применяет политики полей тега (`reason_policy`, `expires_policy`) и — для параметризованных тегов — проверяет значение параметра по допустимому списку тега.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Удалить назначение тега.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Добавить или переописать допустимое значение параметра для параметризованного тега. Список допустимых значений закрыт: каждое назначение должно называть значение из него.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Удалить допустимое значение. Отклоняется, пока хотя бы одно назначение всё ещё несёт его, потому что эти назначения называли бы тип, который список больше не допускает.

---

### Метрики {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Создаёт определение регулируемой метрики. Уже существующее имя отклоняется (`schema.metric_name_taken`). Выражение должно разбираться sqlglot и содержать хотя бы одну агрегатную функцию. Пересоздаёт все представления, составленные из метрик, которые ссылаются на эту метрику. Требует `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Заменяет определение существующей метрики с теми же проверками, что у `createMetric`. Имя, которым не владеет ни одна метрика, отклоняется (`schema.metric_not_found`). Для сохранения нужны домены нового выражения и заменяемого определения.

#### `deleteMetric(name: String!) → MutationResult`

Удалить управляемую метрику. Перестраивает схемы. Требует `table_registration`.

---

### Календари {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Создать или заменить версионированный календарь границ снимков. Проверяется путём построения объекта `Calendar` в памяти перед сохранением — завершается неудачей при неизвестной базовой системе, неверном часовом поясе или неверном фискальном якоре.

#### `deleteCalendar(name: String!) → MutationResult`

Удалить календарь (все версии). Отклоняется, если на него ссылается какое-либо материализованное представление.

---

### Материализованные представления {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Инициировать ручное обновление материализованного представления. Координирует по всему парку экземпляров, когда режим согласованности MV — `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Включить или отключить материализованное представление.

---

### Кеш {: #cache_1 }

#### `purgeCache → MutationResult`

Очистить все кешированные результаты запросов.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Очистить кешированные результаты для одной таблицы.

---

### Запланированные задачи {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Создать запланированный триггер — либо вызов вебхука, либо оператор SQL — и зарегистрировать его вживую в APScheduler. `kind` — это `"webhook"` или `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Удалить запланированный триггер из конфигурации и работающего планировщика.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Включить или отключить запланированную задачу в файле конфигурации.

---

### Качество данных {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Выполнить контракт против живой таблицы и вернуть результаты, ничего не приземляя. Мутация, а не запрос, потому что это стоит реального сканирования. Она доказывает, разрешается ли идентификатор набора данных в управляемую таблицу, которую имеет в виду оператор. (пункт 7)

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Немедленно запустить задачу опроса таблицы-проверки. Приземляет строки обычным способом, поэтому результаты сохраняются, и история DQ показывает новое сканирование.

---

### Обслуживание схемы {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Перестроить схему в памяти из состояния базы данных. Полезно после внешних изменений базы данных.

---

### Запросы на создание {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Одобряет запрос на создание: то же правило, те же отказы и те же записи журнала администратора, что у `POST /admin/creation-requests/{id}/approve`. Автор запроса не может одобрить собственный запрос, и одобрение одного пользователя засчитывается один раз. Одобрение, которое завершает счёт запроса, создаёт то, что он просит; `params.status` равен `executed`, если это сделало оно, иначе `pending`, а `params.waitingOn` перечисляет домены, которых запрос на связь ещё ждёт.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Выполняет запрос на создание, получивший свои одобрения — связь, представление, таблицу, источник или webhook, — через тот же код, что использует прямая мутация. Отклоняется с `requests.approvals_incomplete`, пока требуемые одобрения запроса (одно, или два для связи) не получены от пользователей, отличных от автора; запрос на связь также отклоняется с `requests.waiting_on_domains`, пока у затронутого им домена нет одобряющего. Одобрения даются через `POST /admin/creation-requests/{id}/approve`, и одобрение, которое завершает счёт, выполняет создание, поэтому эта мутация служит для повторной попытки неудавшегося создания. Неудавшееся создание оставляет запрос ожидающим и отвечает собственным кодом ошибки.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Отклоняет запрос из очереди с практичной причиной. `reason` обязателен. Запрос на связь может отклонить любой пользователь, который мог бы его одобрить. Автор запроса любого другого типа может его отозвать; это записывается в журнал администратора как отзыв.

---

### Компиляция запросов {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Скомпилировать запрос GraphQL плоскости данных по схеме роли и вернуть полное решение маршрутизации: семантический SQL, SQL движка, прямой SQL, маршрут, метаданные применения (применённые фильтры RLS, исключённые столбцы, применённое маскирование) и скомпилированный Cypher. Возвращает один результат на каждое корневое поле в запросе.

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

## Ключевые входные типы {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Только по расписанию обновления |
| `offPeakWindow` | `String` | Окно обслуживания `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Часовой пояс IANA |
| `cdc` | `SourceCdcConfigInput` | Конфигурация транспорта Kafka CDC, включая способ обращения к его schema registry (см. [Аутентификация schema registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` и `schemaRegistryToken` возвращаются как ссылки хранилища, никогда как учётные данные. |

### `TableInput`

Основной входной тип регистрации таблицы. Ключевые поля сверх базовых:

| Поле | Примечания |
|-------|-------|
| `materialize` | Приземлить копию в хранилище материализации |
| `mvRefreshInterval` | Секунды между обновлениями |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Инкрементальное обслуживание |
| `mvBitemporalMode` | `snapshot` \| `delta` для битемпоральных таблиц |
| `mvCalendar` | Имя календаря снимков |
| `mvGrain` | Гранулярность снимка: `daily`, `weekly`, `monthly`, `annual` или пользовательская `3WE` / `LFR` |
| `viewSql` | SQL для производного представления |
| `viewMetrics` | Декларативная спецификация представления, составленного из метрик — взаимоисключающая с `viewSql` |
| `dqContract` | Текст контракта качества данных в YAML/JSON |
| `queryTemplate` | Cypher для таблицы Neo4j |
| `live` | Конфигурация живой доставки для push SSE/Kafka |
| `discover` | Выводить столбцы из живого источника во время регистрации |

### `RelationshipInput`

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
| `viaTable` | Имя связующей таблицы для рёбер многие-ко-многим |
| `recordCandidate` | Также записать строку `relationship_candidates` со статусом `accepted` |

---

## Пример: регистрация таблицы {: #example-registering-a-table }

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

## Пример: создание правила RLS {: #example-creating-an-rls-rule }

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
