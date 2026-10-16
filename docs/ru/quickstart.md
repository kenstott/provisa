# Быстрый старт для разработчиков {: #developer-quick-start }

Чтобы оценить Provisa без сборки из исходников, см. [Быстрый старт](index.md) — скачайте установщик для macOS, Windows или Linux и запустите `provisa start`. (REQ-223, REQ-224, REQ-227)

Это руководство описывает запуск Provisa **из репозитория** — для активной разработки, отладки или внесения вклада в проект.

---

## Предварительные требования {: #prerequisites }

- **Docker Desktop** (запущен)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Клонирование и настройка {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` создаёт `.venv/`, устанавливает все зависимости Python через `pip install -e ".[dev]"` и настраивает git-хуки в `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Запуск всего окружения {: #2-start-everything }

```bash
./start-ui.sh
```

Когда запуск завершится, вы увидите:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Что запускается:** [tool-verified: start-ui.sh]

- Основные сервисы Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Оверлей для разработки Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- Backend API на порту 8001 (горячая перезагрузка при изменениях в `provisa/` и `config/`) (REQ-618)
- Dev-сервер Vite UI на порту 3000 (HMR)
- Трассировка OpenTelemetry и Grafana по адресу `http://localhost:3100`. Стек наблюдаемости — это опциональный профиль docker-compose `observability` (OTel Collector, Prometheus, Tempo, Grafana), не включённый по умолчанию на уровне платформы; `start-ui.sh` включает его как удобство dev-скрипта, если вы не передадите `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** останавливает всё — backend, UI и все сервисы Docker — и отменяет любые патчи конфигурации. (REQ-619)

**Ctrl+R** перезапускает только backend (полезно после изменения конфигурации, которое не подхватывается горячей перезагрузкой). (REQ-619)

### Опции {: #options }

`--no-observability` — Отключает распределённую трассировку. По умолчанию `start-ui.sh` скачивает Java-агент OpenTelemetry, если он ещё не установлен, патчит `jvm.config` Trino для его загрузки и запускает OTel collector, Prometheus, Tempo и Grafana. Передайте `--no-observability`, чтобы пропустить всё это. Патч `jvm.config` отменяется по Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Заполняет Kafka демонстрационными данными после того, как сервисы Docker становятся здоровыми. По умолчанию не выполняется. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Оставляет сервисы Docker Compose запущенными после Ctrl+C вместо вызова `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Стирает все тома Docker и перезапускает с чистого состояния. Полезно для восстановления после сбоя Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Запускает дополнительные демонстрационные источники данных (схема PostgreSQL pet-store, мок OpenAPI petstore, SQLite и удалённый GraphQL). Автоматически заполняет пользователей и заказы petstore. [tool-verified: start-ui.sh lines 17, 55–171]

`--source=<name>` (только `start-ui-install.sh`, можно повторять) — Поднимает необязательный источник данных вместе с `--demo`. Каждое имя соответствует `demo/sources/<name>/`. Запуск вызывает `demo/sources/provision.py up`, который поднимает `compose.yml` источника как отдельный проект Docker Compose (`provisa-demo-<name>`), ждёт его проверки работоспособности и запускает `prime.py`, если он есть у источника, чтобы наполнить данные. Затем запуск пишет обёрточную конфигурацию в `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml`, которая включает базовую конфигурацию плюс `fragment.yaml` каждого источника, и загружается из неё. [tool-verified: `start-ui-install.sh` (search `SOURCES`), `demo/sources/provision.py`] (REQ-1669)

Тот же `provision.py` вызывает сквозной набор тестов интерфейса, чтобы поднять эти источники (под префиксом проекта `provisa-e2e-<name>` на собственных портах), поэтому данные, которые показывает демо, и строки, которые проверяет набор, определяются один раз. [tool-verified: `provisa-ui/e2e/demo-source-containers.ts`] (REQ-1671)

При запуске в Docker (без `--demo`/`--native`) координатор — контейнер, поэтому каждый источник подключается к сети основного стека и регистрируется как `<name>:<container port>`; при нативном запуске он регистрируется как `localhost:<published port>`. Источник, у которого файл `demo/sources/<name>/engine` называет движок, не запускаемый этим стартом, отклоняется.

Поставляемые источники:

| Имя | Порт(ы) | Примечания |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | Две таблицы Cypher (`adopter`, `adopter_referral`); граф наполняется `seed.cypher`; таблицы регистрируются из фрагмента |
| `mongodb` | 27117 | Источник зарегистрирован; коллекция `product_reviews` наполняется `db/mongo-init.js`; таблицы регистрируются вручную через Register Table |
| `redis` | 26379 | Источник зарегистрирован; хеши `support_agent:*` и `agent_status:*` наполняются `prime.py`; каждый префикс регистрируется как таблица через Register Table (REQ-1675) |
| `cassandra` | 29042 | Источник зарегистрирован; `shelter_ops.intake_events` наполняется `prime.py` (нужно дополнение `cassandra`); keyspace регистрируется как схема через Register Table (REQ-1676) |
| `sparql` | 23030 | Apache Jena Fuseki; источник и одна таблица на основе запроса (`volunteer`) регистрируются из фрагмента, граф наполняется `prime.py`; другие таблицы — через Register Table (запрос + Preview) (REQ-1683) |
| `prometheus` | 29090 | Источник зарегистрирован; сервер опрашивает сам себя, поэтому `up` и метрики `prometheus_*` регистрируются как таблицы через Register Table (REQ-1689) |
| `elasticsearch` | 29200 | Источник и отображение индекса зарегистрированы; индекс `support_tickets` наполняется `prime.py`; читается по HTTP нативным движком (REQ-1672), через коннектор в Trino |
| `splunk` | mgmt 8089, HEC 8088 | Источник зарегистрирован с аутентификацией по токену и `disable_ssl_validation` (сертификат контейнера самоподписан); индекс, семь событий-оповещений приюта и модель данных `shelter_alerts` наполняются `prime.py`, который также создаёт API-токен, читаемый фрагментом как `PROVISA_DEMO_SPLUNK_TOKEN`. Модели данных регистрируются как таблицы через Register Table — в Trino через каталог `splunk`, в любом другом движке через встроенный сервер Calcite pgwire, который подключает движок (REQ-1694) |
| `chinook` | 25433 | Postgres с подмножеством Chinook в snake_case, которое отслеживает пример метаданных Hasura, наполняемый `prime.py` из `tests/fixtures/hasura_v2_t1_seed.sql`; источник регистрируется из фрагмента, и именно на него попадает импорт Hasura v2 из `tests/fixtures/hasura_v2_t1_metadata.json` (REQ-1687) |

`--idp=basic|firebase` — Включает провайдера идентификации для аутентификации. Без этого флага backend работает без провайдера аутентификации, и все запросы обрабатываются как `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Подключение источника данных {: #3-connect-a-data-source }

Provisa читает конфигурацию из `config/`. Добавьте файл источника — например, `config/sources/my-db.yaml`:

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

Установите переменную окружения, и backend подхватит её при следующей перезагрузке:

```bash
export MY_DB_PASSWORD=secret
```

Полный справочник YAML и все поддерживаемые типы источников см. в [docs/configuration.md](configuration.md).

---

## 4. Выполнение первого запроса {: #4-run-your-first-query }

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

Аутентификация не требуется, если в `config/provisa.yaml` отсутствует секция `auth` (значение по умолчанию для разработки). Роль по умолчанию — `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Открытие UI {: #5-open-the-ui }

Откройте `http://localhost:3000` в браузере.

Панель навигации содержит четыре меню верхнего уровня: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Проводник схемы (`/schema`), редактор GraphQL (`/query`), редактор Cypher (`/graph`), редактор SQL (`/sql`)
- **Model** — Представления и команды
- **Security** — Политики безопасности на уровне строк и маскирования столбцов (REQ-038, REQ-041)
- **Admin** — Обзор, домены, кеш, плановые задачи, состояние системы, наблюдаемость, пользователи, организации, роли

Admin GraphQL API находится по адресу `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Устранение неполадок {: #troubleshooting }

**Backend не запускается** — проверьте `.logs/server.log`. Самая частая причина — отсутствующая переменная окружения или конфликт портов на 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Сервисы Docker не в здоровом состоянии** — выполните `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps`, чтобы увидеть, какой сервис завис. Движок федерации занимает ~30 секунд при первом запуске. (REQ-055)

**Конфликт портов на 3000 или 8001** — `start-ui.sh` завершает устаревшие процессы на этих портах перед запуском. Если порт занят чем-то другим, сначала остановите это вручную. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Чистый запуск** — остановите скрипт, затем выполните `./start-ui.sh --reset-volumes`, чтобы стереть все тома и перезапустить. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Следующие шаги {: #next-steps }

| Цель | Документ |
| ------ | ----- |
| Полный справочник конфигурации YAML | [configuration.md](configuration.md) |
| Безопасность на уровне строк, маскирование столбцов, аутентификация | [security.md](security.md) |
| Все поддерживаемые типы источников | [sources.md](sources.md) |
| Подписки в реальном времени | [subscriptions.md](subscriptions.md) |
| JDBC, BI-инструменты, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Python-клиент | [python-client.md](python-client.md) |
| Развёртывание в продакшене | [deployment.md](deployment.md) |
