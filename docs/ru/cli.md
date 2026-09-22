# Справочник CLI

Команда `provisa` — единая точка входа для встраиваемого уровня, устанавливаемого через pip (REQ-1128).
Она запускает среду выполнения, управляет лицензиями, инициирует публикацию метаданных, разворачивает модели и
управляет баннером обслуживания — без Docker, Node или каких-либо внешних сервисов.

Установите её с дополнительным пакетом `embedded`, который также подтягивает офлайн-расширения
DuckDB и встроенную плоскость управления PostgreSQL:

```bash
pip install 'provisa[embedded]'
```

**Требования к платформе.** `provisa run` требует Python 3.12 и платформу с колесом (wheel) pgserver:
linux x86_64, macOS или Windows x86_64. У Linux aarch64 нет ни колеса pgserver, ни исходного
дистрибутива, поэтому встраиваемый уровень там не работает. Используйте контейнерный уровень на aarch64.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Общие параметры {: #shared-options }

Несколько подкоманд вызывают HTTP API Provisa. Они разделяют три флага и две переменные
окружения. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Флаг | По умолчанию | Резервная переменная окружения |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(отсутствует)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 для `maintenance`) | _(отсутствует)_ |

`--api` — это базовый URL работающего экземпляра Provisa. В условиях многоарендности имя хоста
называет организацию — `https://acme.provisa.org` направляет к тенанту acme. `--token` — это
Bearer-токен; когда он пуст, заголовок `Authorization` не отправляется, что верно для
неавторизованных развёртываний. [tool-verified: cli.py:314-316, 357-365]

Задайте обе переменные в вашем окружении CI, чтобы не повторять их при каждом вызове:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Подкоманды, принимающие эти флаги: `metadata export`, `env deploy`, `env fetch`,
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Запустить встраиваемую систему Provisa — API-сервер и статический/прокси-сервер интерфейса — в одном
процессе. Без Docker, без Node, без внешних сервисов. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Флаги

| Флаг | По умолчанию | Примечания |
| --- | --- | --- |
| `--demo` | выкл | Загрузить встроенную демонстрацию — примеры доменов pet-store и shelter поверх встроенного SQLite (REQ-414) |
| `--host` | `127.0.0.1` | Адрес привязки для обоих серверов |
| `--api-port` | `8000` | Порт API-сервера |
| `--ui-port` | `3000` | Порт статического/прокси-сервера интерфейса |
| `--no-browser` | выкл | Пропустить открытие браузера при готовности интерфейса; URL всё равно выводится |
| `--reset` | выкл | Удалить и пересоздать хранилище встроенной плоскости управления перед запуском; используйте после обновления Provisa, если запуск сообщает о несоответствии схемы |
| `--data-dir` | `~/.provisa/native` | Каталог, содержащий встроенный кластер PostgreSQL и кеш расширений DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### Переменные окружения

`provisa run` читает несколько дополнительных переменных перед запуском HTTP-серверов.
Задайте их, чтобы переопределить значения по умолчанию, которые иначе применил бы `load_profile("native", ...)`.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Переменная | Эффект |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Заменяет встроенный движок DuckDB на предоставленный клиентом координатор Trino (REQ-1129) |
| `PROVISA_ENGINE_URL` | Альтернативный способ указать на внешний федеративный движок |
| `PROVISA_CONFIG` | Файл конфигурации для загрузки; `--demo` устанавливает его во встроенную демонстрационную конфигурацию (REQ-1127) |
| `PROVISA_DEMO` | Устанавливается в `1` флагом `--demo`; помечает сессию как демонстрационный запуск |
| `PROVISA_DEMO_DIR` | Путь к каталогу примеров данных для демонстрации; устанавливается флагом `--demo` |
| `PROVISA_CONFIG_REPLACE` | Устанавливается в `true` флагом `--demo`, чтобы разрешить демонстрационной конфигурации перезаписать любую существующую |
| `PROVISA_DUCKDB_EXT_DIR` | Заранее подготовленный каталог расширений DuckDB; устанавливается автоматически из пакета `provisa-duckdb-ext`, если он присутствует; отсутствие означает, что DuckDB загрузится из сети при первом использовании |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Последовательность запуска

1. Проверка платформы — прерывает работу с понятным сообщением при неподдерживаемом Python или отсутствующем pgserver.
2. `--reset` (если запрошен) — удаляет встроенный кластер PostgreSQL; он пересоздаётся на следующем шаге.
3. Демонстрационная конфигурация (если `--demo`) — устанавливает `PROVISA_CONFIG` и `PROVISA_DEMO_DIR`.
4. Встроенное окружение — запускает плоскость управления PostgreSQL, разрешает её URL сокета и
   подготавливает офлайн-расширения DuckDB, если установлен `provisa-duckdb-ext`.
5. Проверка расхождения схемы — сканирует действующую плоскость управления на предмет отсутствующих столбцов. Если такие найдены,
   выводится подсказка о `--reset`, и работа завершается с кодом 1. В V1 нет миграций; столбец, добавленный в
   более новом релизе, требует сброса. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Оба сервера запускаются одновременно. Индикатор готовности опрашивает `GET /ready` (не `/health` — конечная точка
   `/ready` подтверждает, что хранилище подключено и движок прогрет) и открывает браузер,
   когда получает 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Коды завершения

| Код | Значение |
| --- | --- |
| 0 | Чистое завершение (Ctrl-C) |
| 1 | Ошибка запуска (не пройдена проверка платформы, отсутствует демонстрационная конфигурация, обнаружено расхождение схемы) |

### Пример

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Проверить и установить файл лицензии офлайн (REQ-1139). Файл — это `license.json`, выданный
provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Аргумент | Примечания |
| --- | --- |
| `file` | Путь к файлу лицензии; применяется раскрытие `~` |

Код завершения 0 означает, что лицензия действительна и установлена. Код завершения 1 означает, что она была отклонена;
причина выводится в stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Показать идентификатор машины, состояние пробного периода, прошедшие дни и действительность лицензии (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Без флагов. Выводит четыре строки — идентификатор машины, дату первого обнаружения, прошедшие дни, состояние пробного периода
и состояние лицензии — и завершается с кодом 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Инициировать публикацию метаданных по требованию на работающем сервере (REQ-1072/REQ-1074). Отправляет POST на
`POST /admin/metadata-export/publish` — ту же конечную точку, которую вызывает кнопка **Publish now**
на вкладке Admin, поэтому оба пути отправляют один и тот же полный снимок. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Флаг | По умолчанию | Примечания |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, затем `http://127.0.0.1:8000` | В условиях многоарендности имя хоста называет организацию |
| `--token` | `$PROVISA_API_TOKEN` | Bearer-токен для идентификации, обладающей `org_settings`; опустите на неавторизованных развёртываниях |
| `--timeout` | `300` | Секунды до отмены HTTP-вызова |

[tool-verified: cli.py:654-669]

| Код завершения | Значение |
| --- | --- |
| 0 | Все активы опубликованы |
| 1 | Частичная публикация или сбой подключения; ошибки по каждому активу выводятся в stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

Полный справочник конфигурации — провайдеры, учётные данные, `reconcile_cron` и содержимое
снимка — в разделе [Экспорт метаданных](metadata-export.md#from-the-command-line).

---

## provisa env deploy

Развернуть модель по git-ссылке (ref) в окружении, сделав это дерево текущей моделью окружения (REQ-1496). Это команда,
которую выполняет пайплайн развёртывания; правило заключается в том, что развёртывание — это всегда вызов, несущий
идентификацию, против именованной плоскости управления. [tool-verified:
`_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Флаг | Обязателен | Примечания |
| --- | --- | --- |
| `--org` | да | Организация, владеющая окружением |
| `--env` | да | Окружение, которое будет содержать развёрнутую модель |
| `--ref` | да | Ветка или SHA коммита в репозитории организации |
| `--dry-run` | нет | Сообщить, что изменится; ничего не применять |
| `--seed` | нет | Также применить классы только для создания (роли); корректно только когда это развёртывание создаёт окружение впервые |
| `--message` | нет | Примечание, переносимое на запрос утверждения, когда целевое окружение защищено |
| `--api` | нет | См. [Общие параметры](#shared-options) |
| `--token` | нет | См. [Общие параметры](#shared-options) |
| `--timeout` | нет | По умолчанию 300 с |

[tool-verified: cli.py:677-711]

| Код завершения | Значение |
| --- | --- |
| 0 | Развёртывание применено, либо `--dry-run` завершён |
| 2 | Окружение защищено; развёртывание было только предложено, но не применено |

Код завершения 2 задуман намеренно. Пайплайн, который принял бы ожидающее утверждение за выпущенное развёртывание,
был бы неправ. [tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

Полное объяснение классов окружений, правил защиты, отчётов о слиянии и
жизненного цикла утверждения см. в разделе [Окружения](environments.md#the-env-cli-commands).

---

## provisa env fetch

Получить удалённые ветки организации в её репозиторий Provisa (REQ-1541). Выполните это перед
развёртыванием, когда хотите назвать `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at
cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Флаг | Обязателен | Примечания |
| --- | --- | --- |
| `--org` | да | Организация, чей удалённый репозиторий получается |
| `--api` | нет | См. [Общие параметры](#shared-options) |
| `--token` | нет | Bearer-токен для администратора организации |
| `--timeout` | нет | По умолчанию 300 с |

[tool-verified: cli.py:716-733]

Выводит одну строку на каждую полученную ветку — `origin/<name>  <sha12>`. Завершается с кодом 0 при успехе; вызывает
`SystemExit` с сообщением об ошибке при сбое HTTP или подключения.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

---

## provisa maintenance on

Поднять баннер запланированного обслуживания на развёртывании (REQ-1466). Выполните это перед плановыми
работами, которые останавливают плоскость данных — например, перед изменением
`var.engine_cluster_mode`, что заменяет кластер движка и каждый его шард (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Флаг | Примечания |
| --- | --- |
| `--message` | Переопределить стандартную формулировку развёртывания; по умолчанию — стандартное сообщение сервера |
| `--ends-at` | Момент в формате ISO-8601, к которому ожидается завершение работ, например `2026-08-14T22:30:00Z`; по умолчанию оценка отсутствует |
| `--api` | См. [Общие параметры](#shared-options) |
| `--token` | Bearer-токен для идентификации, обладающей `platform_settings` |
| `--timeout` | По умолчанию 30 с |

[tool-verified: cli.py:743-773]

Выводит итоговое состояние баннера и завершается с кодом 0. Вызывает `SystemExit` при сбое HTTP или подключения.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Очистить баннер обслуживания по завершении работ (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Выводит итоговое состояние баннера (active: false) и завершается с кодом 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Показать текущее состояние баннера обслуживания, не изменяя его (REQ-1466).

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Выводит состояние баннера и завершается с кодом 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Краткий справочник

| Команда | REQ | Что делает |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Запустить встроенные API + интерфейс |
| `provisa run --demo` | REQ-414 | Запустить с примерами данных pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Пересобрать плоскость управления перед запуском |
| `provisa license apply <file>` | REQ-1139 | Установить файл лицензии офлайн |
| `provisa license status` | REQ-1139 | Показать идентификатор машины и состояние пробного периода / лицензии |
| `provisa metadata export` | REQ-1072 | Опубликовать снимок метаданных по требованию |
| `provisa env fetch --org ORG` | REQ-1541 | Получить удалённые ветки в репозиторий Provisa |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Развернуть ссылку в окружении |
| `provisa maintenance on` | REQ-1466 | Поднять баннер обслуживания |
| `provisa maintenance off` | REQ-1466 | Очистить баннер обслуживания |
| `provisa maintenance status` | REQ-1466 | Показать текущее состояние баннера |

## См. также

- [Окружения](environments.md) — модель окружений, защищённые окружения, жизненный цикл утверждения развёртывания
- [Экспорт метаданных](metadata-export.md) — провайдеры каталога, конфигурация и содержимое снимка
- [Развёртывание](deployment.md) — контейнерный уровень и облачное развёртывание
