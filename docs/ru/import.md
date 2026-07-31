# Импорт из Hasura

Provisa может преобразовать существующие метаданные Hasura в `config.yaml` Provisa, сохраняя отслеживаемые таблицы, связи, разрешения и удалённые схемы.

## Hasura v2

### Экспорт метаданных

Из консоли или CLI Hasura:
```bash
hasura metadata export --output metadata.yaml
```

Или используйте API Hasura:
```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Преобразование

Конвертер v2 читает **директорию** метаданных Hasura (структуру, создаваемую `hasura metadata export`, или плоскую структуру `tables.yaml` / `actions.yaml`) и записывает конфигурацию Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Опустите `-o`, чтобы вывести конфигурацию в stdout.

Флаги:

| Флаг | Назначение |
|------|---------|
| `-o`, `--output` | Путь вывода YAML (по умолчанию: stdout) |
| `--source-overrides` | Файл YAML с переопределениями подключения для каждого источника (хост, порт, учётные данные) |
| `--domain-map` | Сопоставления схема-домен как пары `SCHEMA=DOMAIN` |
| `--auth-env-file` | Файл `.env` с конфигурацией аутентификации; преобразует JWT/JWK, admin secret и карту claim'ов |
| `--dry-run` | Разбор и проверка без записи вывода |

### Что преобразуется

| Концепция Hasura | Эквивалент Provisa |
|---------------|-------------------|
| Отслеживаемая таблица | `tables[]` с `publish: true` |
| Object relationship | `relationships[]` с `cardinality: many-to-one` |
| Array relationship | `relationships[]` с `cardinality: one-to-many` |
| Select permission | Видимость роли + фильтр RLS |
| Column permission | `visible_to` / `writable_by` |
| Insert/update/delete permission | Мутация `writable_by` + RLS |
| Remote schema | Регистрация источника `graphql_remote` |
| Computed field | Запись `functions[]` с `kind: query` |

### Ограничения

- **Actions** преобразуются автоматически: HTTP-обработчики actions становятся мутациями `webhooks[]`; actions с обработчиком, отличным от HTTP (базы данных), становятся заполнителем `functions[]` и выдают предупреждение о необходимости проверить обработчик
- **Event triggers** преобразуются в конфигурацию `event_triggers` для каждой таблицы (операции, URL webhook, политика повтора) и выдают предупреждение об ограниченной точности
- **Remote schemas** преобразуются в записи источника `graphql_remote`
- **Пользовательские SQL-функции** требуют проверки — простые случаи преобразуются в записи `functions[]`, сложные требуют ручной работы
- **Cron triggers** преобразуются в записи конфигурации `scheduler`, сохраняя выражение cron и флаг enabled

---

## Hasura DDN (v3)

### Найдите проект HML

Конвертер DDN читает **директорию** проекта DDN с файлами `.hml` напрямую — этап сборки суперграфа не требуется. Первый компонент директории под корнем проекта берётся как имя подграфа; файлы под `globals/` назначаются подграфу `globals`.

### Преобразование

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Опустите `-o`, чтобы вывести конфигурацию в stdout.

Флаги:

| Флаг | Назначение |
|------|---------|
| `-o`, `--output` | Путь вывода YAML (по умолчанию: stdout) |
| `--source-overrides` | Файл YAML с переопределениями подключения для каждого источника |
| `--domain-map` | Сопоставления подграф-домен как пары `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Путь вывода для сопутствующего файла агрегатных выражений (по умолчанию: `<output>-aggregates.yaml`) |
| `--dry-run` | Разбор и проверка без записи вывода |

Метаданные `AggregateExpression` сохраняются в сопутствующем файле `*-aggregates.yaml`.

### Что преобразуется

| Концепция DDN | Эквивалент Provisa |
|------------|-------------------|
| Subgraph model | `tables[]` под источником |
| Relationship | `relationships[]` |
| Permission rule | Фильтр RLS |
| Command | Мутация webhook или представление |
| Connector | Запись источника с деталями подключения |

### Ограничения

- **Lambda-коннекторы** (функции TypeScript/Python) требуют ручной настройки webhook
- **Lifecycle plugins** не имеют прямого эквивалента
- **Режимы аутентификации DDN** отображаются на провайдеры аутентификации Provisa, но пути claim'ов JWT могут потребовать корректировки

---

## После импорта

1. Просмотрите сгенерированный `config.yaml` — обратите внимание на `warnings` от конвертера
2. Проверьте учётные данные подключения (конвертер использует значения-заполнители)
3. Запустите Provisa и убедитесь, что таблицы появляются в Explorer
4. Запустите ваши существующие запросы GraphQL — схема совместима для распространённых паттернов
5. Отправьте запросы на утверждение через Admin API или UI перед включением производственного governance
