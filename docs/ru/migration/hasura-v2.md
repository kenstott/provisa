# Миграция с Hasura v2 на Provisa

## Предварительные требования

1. Работающий экземпляр Hasura v2 (v2.x) с экспортированными метаданными.
2. Экспортируйте метаданные с помощью Hasura CLI:
   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```
   Это создаёт каталог `metadata/`, содержащий `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` и т. д.
3. Python 3.11+ с установленным пакетом `provisa`.

## Использование CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Аргументы

| Аргумент | Обязателен | Описание |
|----------|----------|-------------|
| `metadata_dir` | Да | Путь к экспортированному каталогу метаданных Hasura v2 |

### Опции

| Опция | По умолчанию | Описание |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Путь к выходному файлу YAML |
| `--source-overrides FILE` | None | Файл YAML с переопределениями подключения по источникам |
| `--domain-map KEY=VAL ...` | None | Отображения схема-в-домен (например, `public=core hr=people`) |
| `--auth-env-file FILE` | None | Путь к файлу `.env` с конфигурацией авторизации JWT/admin-secret |
| `--dry-run` | off | Разбор и валидация без записи вывода |

### Файл переопределений источников

Файл YAML, ключами которого являются имена источников, со свойствами подключения для переопределения:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Файл окружения авторизации

Файл в стиле `.env`, содержащий конфигурацию авторизации Hasura для конвертации. Конвертер
отображает:

- JWT с `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Авторизация через webhook -> выдаётся предупреждение (нет эквивалента в Provisa).

## Матрица паритета функций

| Функция Hasura v2 | Эквивалент Provisa | Примечания |
|---|---|---|
| **Sources** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Тип (Kind) отображается: pg/postgres -> postgresql, mssql -> sqlserver. URL подключения разбирается на host/port/database/username/password. Настройки пула сохраняются. |
| **Tables** (отслеживаемые таблицы) | `tables[]` | Имя схемы + таблицы сохраняется. `source_id` связывает с источником. |
| **Пользовательские имена таблиц** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Первое ненулевое значение из `select`, `select_by_pk`, `custom_name`. |
| **Пользовательские имена столбцов** | `columns[].alias` | Отображает словарь `custom_column_names` в алиасы столбцов. |
| **Разрешения Select** (столбцы, фильтр) | `columns[].visible_to[]`, `rls_rules[]` | Списки столбцов становятся `visible_to`. Поддерживаются столбцы-подстановки (`*`). Фильтры преобразуются в SQL через `bool_expr_to_sql`. |
| **Разрешения Insert/Update** (столбцы) | `columns[].writable_by[]` | Списки столбцов становятся `writable_by`. Роли повышаются до возможности `write`. |
| **Разрешения Delete** | Повышение возможности роли | Роль получает возможность `write`. Отображения delete на уровне таблицы нет. |
| **Object-связи** | `relationships[]` с `cardinality: many-to-one` | Отображение столбцов сохраняется. |
| **Array-связи** | `relationships[]` с `cardinality: one-to-many` | Отображение столбцов сохраняется. |
| **Вычисляемые поля (computed fields)** | `functions[]` | Отображаются в Function с `returns`, указывающим на ID родительской таблицы. |
| **Отслеживаемые функции** | `functions[]` | `exposed_as` по умолчанию — mutation. Схема сохраняется. |
| **Actions** (обработчик хранимой процедуры) | `functions[]` | Преобразуется в конфигурацию Function, когда поддерживается хранимой процедурой. |
| **Actions** (обработчик webhook) | Не конвертируется | Выдаётся предупреждение, включая URL обработчика. |
| **Cron-триггеры** | Не конвертируются | Выдаётся предупреждение. (Триггеры по расписанию во время выполнения существуют, но конвертер их не отображает.) |
| **Триггеры событий (event triggers)** | Не конвертируются | Выдаётся предупреждение. (Триггеры событий во время выполнения существуют, но конвертер их не отображает.) |
| **Наследуемые роли** | `roles[].parent_role_id` | Первая роль в `role_set` становится родительской. Создаются все дочерние роли. |
| **Удалённые схемы (remote schemas)** | `sources[]` (`graphql_remote`) | Регистрируется как источник `graphql_remote`. Имя, URL, заголовки и конфигурация аутентификации сохраняются. |
| **Enum-таблицы** | Создаётся таблица | Флаг `is_enum` не переносится (нет эквивалента в Provisa). |
| **Allow lists** | Пропускаются | Отсутствуют в модели метаданных. |

## Шаги после конвертации

1. **Проверьте выходной YAML.** Проверьте, что источники, таблицы и роли выглядят корректно.
2. **Настройте подключения источников.** Конвертер разбирает URL подключения, но по умолчанию использует
   `localhost` при неудаче разбора. Используйте `--source-overrides` или отредактируйте вывод напрямую.
3. **Проверьте назначения доменов.** Без `--domain-map` все таблицы попадают в `default`.
   Назначьте схемы доменам с помощью `--domain-map public=core analytics=reporting`.
4. **Проверьте правила RLS.** Фильтры преобразуются в SQL-приближения. Сложные булевы
   выражения (вложенные `_and`/`_or`/`_exists`) следует проверить вручную.
5. **Просмотрите предупреждения.** Конвертер выводит сводку предупреждений в stderr для функций,
   которые конвертер не отображает (триггеры событий, cron-триггеры, действия на основе webhook).
6. **Настройте авторизацию.** Если ваш экземпляр Hasura использует авторизацию JWT/webhook, создайте файл
   окружения авторизации и перезапустите с `--auth-env-file`.
7. **Протестируйте.** Запустите сервер Provisa и проверьте запросы к вашим источникам данных.

## Распространённые проблемы и устранение неполадок

### URL подключения не разбирается

Если `database_url` источника — это ссылка на переменную окружения (`{"from_env": "PG_URL"}`),
конвертер не может разрешить её на момент конвертации. Источник будет иметь значения-заполнители
(`host: localhost`, `database: default`). Исправьте с помощью `--source-overrides`.

### Столбцы-подстановки

Когда разрешение предоставляет `columns: "*"`, конвертер создаёт единую запись столбца-подстановки.
После конвертации вы можете захотеть заменить её явными списками столбцов, проверив
фактическую схему базы данных.

### Точность триггеров событий

Триггеры событий конвертируются с `operations` и `webhook_url`, но специфичные для Hasura
гарантии доставки (ровно один раз, повторная доставка) не имеют прямых эквивалентов в Provisa.
Просмотрите раздел `event_triggers` и настройте свою инфраструктуру webhook соответствующим образом.

### Отсутствующие роли

Роли собираются только из записей разрешений. Если роль существует в Hasura, но не имеет
разрешений ни на одну таблицу или action, она не появится в выводе.

### Пользовательские корневые поля

Для алиаса таблицы используются только корневые поля `select` и `select_by_pk`. Другие
пользовательские корневые поля (`select_aggregate`, `insert`, `update`, `delete`) не отображаются.

## Пример

Конвертация типичного проекта Hasura v2 с двумя схемами, отображёнными на домены:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Структура вывода:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
