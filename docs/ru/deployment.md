# Развёртывание

## Выбор пути развёртывания

Provisa поддерживает шесть путей развёртывания. Выбирайте, исходя из вашей аудитории и операционного контекста:

| Путь | Артефакт / Скрипт | Лучше всего для |
| ------ | ------------------- | ---------- |
| **Разработка** | `start-ui.sh` | Разработка из исходного кода, оценка с полными демо-данными |
| **Установщик macOS** | `Provisa-<version>-macOS.dmg` | Рабочие станции разработчиков, оценка |
| **Установщик Windows** | `Provisa-<version>-windows-x64.exe` | Рабочие станции разработчиков, оценка |
| **Linux AppImage** | `Provisa.AppImage` | Серверы on-prem, облачные ВМ, изолированные (air-gapped) среды |
| **Облачные ВМ (AWS)** | `terraform/deploy.sh` | Многоузловое облачное развёртывание с балансировщиками нагрузки |
| **Kubernetes** | `helm/provisa/` | Команды, уже эксплуатирующие K8s |

### ВМ против Kubernetes

Оба варианта промышленного уровня. Путь ВМ/AppImage проще: не нужно готовить кластер, настраивать политики CNI или RBAC, а AppImage полностью самодостаточен (REQ-223). Он естественно вписывается в существующий инструментарий управления серверами (Ansible, Puppet, агенты Datadog, форвардеры Splunk и т. д.).

Выбирайте Kubernetes только если ваша команда уже эксплуатирует кластер K8s и хочет, чтобы Provisa участвовала в этой операционной модели (rolling deploys, HPA, единая наблюдаемость) (REQ-056). Возможности эквивалентны — Kubernetes добавляет операционные накладные расходы, а не возможности.

### Получение образов и сканирование безопасности

Все производственные пути требуют получения артефактов Provisa до того, как может выполниться какое-либо развёртывание. «Air-gapped» относится к тому, что происходит во время установки на целевой машине — артефакты должны быть получены заранее.

**Установщики macOS и Windows:** Скачайте со [страницы релизов GitHub](https://github.com/provisa/provisa/releases). Полностью самодостаточны; интернет после скачивания не требуется (REQ-227). Предназначены для разработки/оценки, не для продакшена — шлюз сканирования образов не ожидается.

**Путь AppImage:** Скачайте со [страницы релизов GitHub](https://github.com/provisa/provisa/releases) и перенесите на целевую машину. AppImage упаковывает все образы компонентов как tar-архивы внутри файловой системы squashfs (REQ-294) — большинство сканеров реестров не могут инспектировать их на месте. Обратитесь к вашей учётной команде Provisa за дайджестами образов компонентов, чтобы сверить их со своим сканером независимо.

**Путь Terraform:** AppImage должен быть загружен в S3 перед запуском `terraform/deploy.sh`. Узлы EC2 скачивают его при загрузке через роль IAM — им требуется исходящий доступ к S3 (прямой или через шлюзовую конечную точку VPC). Применяйте ту же политику сканирования, что и для пути AppImage.

**Путь Helm / Kubernetes:** Отдельные образы должны быть отправлены в реестр, доступный кластеру. Этот путь наиболее совместим со сканированием на основе реестра (Prisma Cloud, Aqua, Trivy, AWS Inspector) — образы являются объектами первого класса, которые сканеры понимают нативно. Для изолированных кластеров зеркалируйте образы во внутренний реестр и переопределяйте ссылки в `values.yaml` (REQ-294).

---

## Разработка (из исходного кода)

### Рекомендуется: `start-ui.sh`

Самый простой способ запустить Provisa из исходного кода. Запускает всю инфраструктуру, backend API и dev-сервер UI одной командой (REQ-055). Ctrl+C аккуратно останавливает всё.

**Предварительные требования:** Docker Desktop, Node.js, виртуальное окружение Python в `.venv/`

```bash
./start-ui.sh
```

Что он делает:

- Запускает `docker-compose.core.yml` + `docker-compose.dev.yml` (все базовые + демо-сервисы) и ждёт готовности (REQ-055)
- Заполняет Kafka демо-данными
- Синхронизирует зависимости Python из `.venv/`
- Запускает backend API на порту 8001 (логи в `.logs/server.log`) (REQ-558)
- Запускает dev-сервер UI Vite на порту 3000 (REQ-559)
- Печатает URL и ждёт; Ctrl+C останавливает всё и разбирает compose

```
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Опции:**

`--reset-volumes` — Запускает `docker compose down -v` перед стартом, уничтожая все тома Docker (данные PostgreSQL, объекты MinIO, состояние Redis и т. д.) (REQ-170). Используйте, когда нужен полностью чистый старт — после изменения схемы во время разработки или когда Docker завершился аварийно и оставил повреждённые тома. **Все данные будут потеряны.**

`--observability` — Добавляет полную инструментацию трассировки и метрик. Скачивает Java-агент OpenTelemetry и патчит `jvm.config` Trino для его загрузки, инструментирует backend Provisa экспортом OTLP и запускает коллектор OTel, Prometheus, Tempo и Grafana (`http://localhost:3100`) (REQ-330). Патч `jvm.config` автоматически откатывается при Ctrl+C.

### Ручные шаги (только backend, без UI)

Если вам нужен только API:

1. Установите [Docker Desktop](https://docs.docker.com/get-docker/)
2. Запустите базовые сервисы:
   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```
3. Запустите API:
   ```bash
   uvicorn main:app --reload --port 8001
   ```
4. Проверьте: `curl http://localhost:8001/health`

### Полный стек (Provisa в контейнере)

Чтобы запустить API как контейнер вместо запуска на хосте:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Сервисы

**Базовые (`docker-compose.core.yml`) — требуются всегда:**

| Сервис | Порт | Назначение |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Метаданные конфигурации + каталог Iceberg (REQ-169) |
| PgBouncer | 6432 | Пулинг соединений (REQ-053) |
| Движок федерации | 8080 | Федерация запросов (REQ-028) |
| Redis | 6379 | Кеш результатов запросов (REQ-371) |
| MinIO | 9000/9001 | S3-совместимое объектное хранилище (REQ-029, REQ-171) |

**Демо (`docker-compose.dev.yml`) — опционально, включено в `start-ui.sh`:**

| Сервис | Порт | Назначение |
| --------- | ------ | --------- |
| MongoDB | 27017 | Демо-источник NoSQL |
| Kafka | 9092 | Демо потоковый источник |
| Schema Registry | 8081 | Демо-управление схемами Avro/Protobuf |
| Debezium | — | Демо-коннектор CDC |
| Elasticsearch | 9200 | Демо-источник поиска |
| Neo4j | 7474/7687 | Демо графовый источник |
| Fuseki | 3030 | Демо-хранилище триплетов SPARQL |
| OpenTelemetry Collector | — | Сбор трассировок (с `--observability`) (REQ-302) |
| Prometheus | 9090 | Метрики (с `--observability`) (REQ-330) |
| Tempo | — | Хранение трассировок (с `--observability`) (REQ-330) |
| Grafana | 3100 | Дашборды (с `--observability`) (REQ-330) |

### Бэкенд телеметрии (`otlp2sql`)

Стек `--observability` выше (Collector → Tempo/Prometheus/Grafana) — один
путь телеметрии. Другой — `otlp2sql` (`provisa.observability.otlp2sql`): приёмник
OTLP/HTTP, который записывает трассировки, метрики и логи в базу данных SQL,
выбранную через URL SQLAlchemy, извлекая атрибуты диапазонов `provisa.*` при приёме,
так что отдельная задача уплотнения не выполняется. Записи пакетируются
(`OTLP2SQL_BATCH_MAX_ROWS`, по умолчанию 1000; `OTLP2SQL_BATCH_MAX_SECS`, по умолчанию 2с).

Телеметрия получает собственное хранилище, отдельное от базы данных плоскости управления. Выберите
бэкенд с помощью `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Бэкенд | Примечания |
|---|---|---|
| *(не задан)* | выделенная DuckDB под `~/.provisa/telemetry/` | по умолчанию; без сервера, без Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | приём с высокой скоростью с автоматическими фоновыми слияниями |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | умеренный объём |
| `trino://user@host:8080/otel` | Trino / Iceberg | технически работает, **не рекомендуется** — см. ниже |

**О `trino://`:** диалект SQLAlchemy для Trino выдаёт корректный Trino DDL и
`INSERT`-запросы, поэтому технически он подходит как бэкенд `otlp2sql`. Не
рекомендуется ни для чего, кроме низкой скорости приёма. Каждый сброс пакета становится
распределённым `INSERT` Trino плюс снапшот Iceberg, поэтому высокоскоростная телеметрия
производит множество мелких файлов и снапшотов и всё равно требует периодического
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` — которые `otlp2sql` не
запускает. Это также помещает движок запросов в горячий путь приёма.

Для высокообъёмной телеметрии в Trino/Iceberg используйте вместо этого `otlp2parquet`: он
записывает parquet в объектное хранилище, минуя Trino, а запланированное уплотнение
Trino перекатывает сырые файлы в живые таблицы Iceberg. Для единого
движка, обрабатывающего и высокоскоростной приём, и уплотнение, предпочтите ClickHouse.

Направьте экспортёры OTLP приложения и Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) на
эндпоинт `otlp2sql` и зарегистрируйте домен ops против того же
`PROVISA_OPS_DB_URL`, чтобы он читал то, что записал приёмник.

---

## Установщик macOS

Для рабочих станций разработчиков и оценки. Полностью изолирован (air-gapped) — интернет после скачивания не требуется (REQ-227).

Базовый установщик — это **нативная установка**: движок федерации DuckDB + плоскость управления SQLite + кеш в памяти (fakeredis), без Docker, ВМ, Trino, Redis или MinIO (REQ-972, REQ-979). Движок федерации — выбор мастера установки — DuckDB (нативный, по умолчанию), Trino-в-Docker или внешний движок (REQ-973). Наблюдаемость всегда включена как самотелеметрия, просматриваемая в Admin; стек Docker collector/Prometheus/Grafana — опциональная внешняя демонстрация, а не переключатель вкл/выкл (REQ-975). Пакет демо-данных опционален и отключён по умолчанию (REQ-978). Trino, стек наблюдаемости Docker и демо — тяжёлые дополнения, разрешаемые сначала локально (директория рядом с установщиком, смонтированные тома, `~/Downloads`, затем релиз GitHub), поэтому предприятия могут заранее подготовить tar-архивы для изолированных установок (REQ-977).

### Шаги

1. Скачайте `Provisa-<version>-macOS.dmg` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)
2. Откройте DMG и перетащите **Provisa.app** в `/Applications`
3. Дважды щёлкните **Provisa.app** — настройка при первом запуске выполняется один раз; мастер предлагает выбор движка, наблюдаемости и демо выше (REQ-1007)
4. Откройте терминал:
   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Устойчивость данных

Все данные хранятся в `~/.provisa/` (REQ-224). Чтобы удалить всё: `provisa uninstall`.

---

## Установщик Windows

Для рабочих станций разработчиков и оценки. Полностью изолирован (air-gapped) — интернет после скачивания не требуется (REQ-227).

Как и на macOS, базовый установщик Windows — это **нативный уровень**: автономная среда выполнения Python + колесо provisa + DuckDB/pg_duckdb + плоскость управления SQLite, без поставки Docker, ВМ и образов контейнеров (REQ-979). Движок федерации (Trino), стек наблюдаемости и пакет демо-данных добавляются позже через отдельные слоистые установщики, по порядку: установщик Container (`Provisa-Container-<version>.exe`, добавляющий WSL2 + containerd + Trino), затем установщик Obs (требует уровня контейнера), затем установщик Demo (требует Core + Obs). Руководство при первом запуске объясняет, как инициализировать движок федерации, запустив установщик Container (REQ-1005).

### Шаги

1. Скачайте `Provisa-<version>-windows-x64.exe` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)
2. Запустите установщик — права администратора не требуются; устанавливает в `%LOCALAPPDATA%\Programs\Provisa\`
3. Откройте **Provisa First Launch** из меню Пуск — нативная настройка выполняется один раз и печатает руководство по следующим шагам для слоистых дополнений (REQ-1005)
4. Откройте новый терминал:
   ```
   provisa status
   provisa open
   ```

   (REQ-224)

### Устойчивость данных

Все данные хранятся в `%USERPROFILE%\.provisa\`.

---

## Linux AppImage — одноузловая или многоузловая ВМ

### Что это такое

`Provisa.AppImage` — это единый самодостаточный исполняемый файл, упаковывающий (REQ-223, REQ-228):

- Rootless-демон Docker (`dockerd-rootless.sh` + `rootlesskit`) — системный Docker или root не требуются
- Все tar-архивы образов контейнеров (PostgreSQL, PgBouncer, MinIO, Redis, движок федерации, Provisa API) (REQ-294)
- Обёртку CLI Provisa и скрипт настройки первого запуска

Образ Provisa собирается заранее на этапе упаковки — исходный код Python никогда не включается.

### Когда использовать

- On-premises "голое железо" или ВМ (один узел или несколько узлов)
- Облачные ВМ без кластера K8s
- Изолированные (air-gapped) среды (REQ-294)
- Когда вы хотите более простую эксплуатацию, чем Kubernetes

---

### Шаги — один узел

1. Скачайте `Provisa.AppImage` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases) и перенесите на целевую машину
2. Сделайте его исполняемым:
   ```bash
   chmod +x Provisa.AppImage
   ```
3. Запустите настройку первого запуска:
   ```bash
   ./Provisa.AppImage
   ```
4. Мастер настройки спрашивает:
   - **Роль** → выберите `primary`
   - **Бюджет RAM** → объём RAM для выделения (0 = вся доступная); определяет количество воркеров Trino
   - **Имя хоста** → адрес этого узла для объявления
   - **Порт API** → по умолчанию `8000` (REQ-560)
5. Настройка загружает все образы контейнеров (~2–5 минут), записывает конфигурацию и запускает сервисы
6. Проверьте:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Шаги — несколько узлов (первичный)

Сначала выполните эти шаги на первичном узле. Вторичные узлы должны быть настроены после того, как первичный запущен.

1. Скачайте и перенесите `Provisa.AppImage` на первичную машину
2. Откройте необходимые порты брандмауэра (вторичные узлы будут подключаться на них входящими соединениями):

   | Порт | Сервис |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Координатор движка федерации |
   | 8000 | Provisa API |

3. Сделайте исполняемым и запустите:
   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```
4. Мастер настройки спрашивает:
   - **Роль** → выберите `primary`
   - **Бюджет RAM**, **имя хоста**, **порт API** → отвечайте как для одного узла
5. После завершения настройки запишите **приватный IP** этой машины — он понадобится вторичным узлам
6. Мастер печатает блок upstream nginx — сохраните его для конфигурации вашего балансировщика нагрузки
7. Проверьте:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Шаги — несколько узлов (каждый вторичный)

Повторите эти шаги на каждом дополнительном узле после того, как первичный узел запущен и доступен.

1. Скачайте и перенесите `Provisa.AppImage` на вторичную машину
2. Убедитесь, что вторичный узел может достичь первичного:
   ```bash
   curl http://<primary-ip>:8000/health
   ```
3. Сделайте исполняемым и запустите:
   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```
4. Мастер настройки спрашивает:
   - **Роль** → выберите `secondary`
   - **IP первичного узла** → введите IP первичного узла (связность проверяется вживую)
   - **Бюджет RAM**, **имя хоста**, **порт API** → отвечайте как выше
5. Настройка загружает уменьшенный набор образов (без PostgreSQL, PgBouncer, MinIO, Redis — они работают только на первичном) (REQ-561), запускает Provisa API и воркер движка федерации
6. Проверьте:
   ```bash
   provisa status
   curl http://localhost:8000/health
   ```
7. Добавьте этот узел в upstream вашего балансировщика нагрузки

---

### Топология первичный / вторичный

**Первичный узел** запускает все синглтон-сервисы:

| Сервис | Почему синглтон |
| --------- | --------------- |
| PostgreSQL | Общая схема, конфигурация приложения, семантическая модель |
| Redis | Общий кеш результатов запросов и состояние подписок (REQ-371) |
| MinIO | Общее объектное хранилище для перенаправленных результатов и снапшотов MV (REQ-029) |
| Координатор движка федерации | Все воркеры (первичный + вторичные) регистрируются здесь (REQ-028) |

**Вторичные узлы** запускают только:

- Provisa API — без состояния; читает всю конфигурацию из PostgreSQL на первичном узле при запуске (REQ-057, REQ-562)
- Воркер движка федерации — самостоятельно регистрируется у координатора на первичном узле (REQ-028)

Всё состояние приложения проходит через PostgreSQL первичного узла. Ручная синхронизация не требуется. (REQ-562)

---

### Неинтерактивный (автоматизированный) первый запуск

Для Terraform, cloud-init или Ansible — передавайте флаги вместо ответов на приглашения:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Неинтерактивный режим устанавливает юнит systemd (`/etc/systemd/system/provisa.service`) для запуска при загрузке. (REQ-563)

| Флаг | Описание |
| ------ | ------------- |
| `--non-interactive` | Пропустить все приглашения; установить юнит systemd |
| `--role primary\|secondary` | Роль узла |
| `--primary-ip <ip>` | IP первичного узла (обязателен для вторичного) |
| `--ram-gb <n>` | Выделяемая RAM (0 = вся доступная) |

---

## Развёртывание в облачной ВМ — Terraform (AWS)

Разворачивает полный многоузловой кластер Provisa на AWS — VPC, группы безопасности, инстансы EC2, ALB, NLB — одной интерактивной командой. (REQ-564)

### Файлы

| Файл | Назначение |
| ------ | --------- |
| `terraform/deploy.sh` | Интерактивная обёртка — собирает параметры, проверяет учётные данные, записывает `terraform.tfvars`, запускает apply |
| `terraform/aws/variables.tf` | Все определения переменных со значениями по умолчанию |
| `terraform/aws/main.tf` | VPC, подсети, группы безопасности, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL эндпоинтов и IP узлов |

### Шаги

1. Скачайте `Provisa.AppImage` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)

2. Загрузите его в бакет S3 в вашей учётной записи AWS:
   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Убедитесь, что учётные данные AWS доступны в вашей оболочке (любой из способов):
   - Переменные окружения: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Именованный профиль: `export AWS_PROFILE=my-profile`
   - Активная сессия SSO: `aws sso login`

4. (Опционально) Если вам нужен доступ по SSH к узлам, создайте пару ключей EC2 в целевом регионе и запомните имя пары ключей

5. Запустите обёртку развёртывания:
   ```bash
   bash terraform/deploy.sh
   ```

6. Ответьте на вопросы мастера (см. справочную таблицу ниже). Скрипт проверяет, что AppImage существует в S3, прежде чем продолжить, и прерывается, если это не так

7. Просмотрите сводку развёртывания и подтвердите

8. Terraform разворачивает всю инфраструктуру (~5–10 минут). После apply скрипт печатает:
   ```
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Опционально) Направьте записи DNS на имена DNS ALB и NLB

10. Проверьте:
    ```bash
    curl http://<api_endpoint>/health
    ```

### Вопросы мастера

| Вопрос | По умолчанию | Примечания |
| ---------- | --------- | ------- |
| Облачный провайдер | — | Сегодня только AWS |
| Учётные данные AWS | — | Сначала проверяется активная сессия |
| Регион | `us-east-1` | |
| Количество узлов | `2` | 1 = только первичный, без LB; 2+ = первичный + вторичные + ALB/NLB |
| Тип инстанса | `m7i.2xlarge` | См. руководство по подбору размера ниже |
| Размер корневого тома | `100 GB` | На узел |
| Бюджет RAM | `0` (вся RAM) | Определяет количество воркеров Trino на узел |
| Бакет S3 | — | Проверяется вживую перед продолжением |
| Ключ S3 | `releases/Provisa.AppImage` | |
| Доступ SSH | Нет | Требует имени существующей пары ключей + CIDR администратора |
| CIDR VPC | `10.0.0.0/16` | |

### Руководство по подбору размера инстанса

| Тип | vCPU | RAM | Воркеров Trino/узел | Сценарий использования |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Разработка / небольшие наборы данных |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Небольшой продакшен |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Средний продакшен |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Крупный продакшен |

Все узлы предоставляют воркеров одному координатору на первичном узле (REQ-028). Кластер из 3 узлов `m7i.4xlarge` даёт в сумме 6 воркеров Trino.

### Что разворачивается

- VPC с двумя публичными подсетями в двух зонах доступности (REQ-564)
- Группы безопасности: группа LB (публичный входящий трафик на 8000/8815), группа узлов (LB → узлы, внутри кластера, опциональный SSH)
- Роль IAM + профиль инстанса с S3 GetObject на бакете AppImage
- Первичный инстанс EC2 — запускает первый запуск в режиме `--non-interactive --role primary`
- Вторичные инстансы EC2 (node_count − 1) — запускают первый запуск в режиме `--non-interactive --role secondary --primary-ip <primary private IP>`; зависят от завершения настройки первичного узла
- ALB на порту 8000 — HTTP API, проверки состояния `/health` (REQ-560)
- NLB на порту 8815 — Arrow Flight / gRPC (REQ-143)
- Оба LB подключены ко всем узлам

### Контрольный список предварительных требований

- [ ] Разрешения IAM: EC2 full, ELB full, VPC full, создание роли IAM, S3 GetObject на бакете AppImage
- [ ] `Provisa.AppImage` загружен в S3
- [ ] Узлы EC2 имеют исходящий доступ к S3 (прямой интернет или шлюзовая конечная точка VPC S3)
- [ ] Пара ключей EC2 существует в целевом регионе (если нужен SSH)
- [ ] Terraform ≥ 1.5 установлен локально
- [ ] Запланированы записи DNS для ALB / NLB (опционально, но рекомендуется)
- [ ] Сертификат ACM готов, если требуется HTTPS (не включён в базовый Terraform)

### Секреты

В Terraform не встроены секреты. AppImage генерирует учётные данные во время первого запуска и записывает их в `~/.provisa/config.yaml` на каждом узле (REQ-563). Для продакшена получите токен администратора с первичного узла после развёртывания:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Когда использовать

Ваша команда уже эксплуатирует кластер Kubernetes и хочет, чтобы Provisa участвовала в этой операционной модели (REQ-056). Если вы оцениваете Provisa или разворачиваетесь on-premises без существующего кластера, путь AppImage проще.

Примечание: AppImage Provisa не может выполняться внутри пода Kubernetes — ему требуется FUSE и rootless-демон Docker, которые недоступны в стандартных профилях безопасности подов.

### Шаги

1. Подтвердите доступ к кластеру:
   ```bash
   kubectl cluster-info
   ```

2. Скачайте и зеркалируйте образы в ваш внутренний реестр (требуется для изолированных или сканируемых сред; пропустите, если тянете напрямую из публичных реестров) (REQ-294):

   | Образ | Используется для |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa API |
   | `trinodb/trino:480` | Координатор + воркеры движка федерации (REQ-169) |
   | `postgres:16` | PostgreSQL в кластере (если `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer в кластере (если `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis в кластере (если `redis.enabled` и не задан `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO в кластере (если `minio.enabled`) (REQ-029) |

   Для сред со сканированием реестра:
   - Отправьте каждый образ в ваш стейджинг-реестр
   - Запустите ваш сканер (Prisma Cloud, Aqua, Trivy, AWS Inspector) и получите одобрение
   - Продвиньте в ваш производственный внутренний реестр

3. Решите перед установкой:
   - **PostgreSQL** — в кластере (`postgresql.enabled: true`) или внешний управляемый (`postgresql.host`)? Для продакшена рекомендуется внешний
   - **Redis** — в кластере или внешний (`redis.host`)? Смените пароль по умолчанию (`redis.password`)
   - **MinIO / S3** — MinIO в кластере или нативный S3? Для AWS используйте S3 с ролью IAM
   - **Секреты** — передавайте через `--set` для оценки; используйте External Secrets или Vault Agent для продакшена

4. Установите чарт:
   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   При использовании внутреннего реестра добавьте переопределения образов:
   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Проверьте, что поды запущены:
   ```bash
   kubectl get pods -n provisa
   ```

6. Проверьте API:
   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Опционально) Включите ingress для внешнего доступа — установите `ingress.enabled: true` и настройте ваш ingress-контроллер

### Контрольный список предварительных требований

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Класс хранилища, поддерживающий PVC `ReadWriteOnce` (для сервисов с состоянием в кластере)
- [ ] Образы доступны кластеру (публичный или внутренний реестр)
- [ ] Эндпоинт PostgreSQL + учётные данные (если внешний)
- [ ] Эндпоинт Redis + учётные данные (если внешний)
- [ ] Бакет S3 + учётные данные или роль IAM
- [ ] Выбран токен администратора
- [ ] Настроен ingress-контроллер (если нужен внешний доступ)

### Ключевые значения

| Значение | По умолчанию | Описание |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Реплики Provisa API (без состояния) (REQ-057) |
| `config.pgHost` | `postgres` | Хост PostgreSQL |
| `config.pgPassword` | | Пароль PostgreSQL |
| `config.adminToken` | | Токен Bearer admin API |
| `redis.enabled` | `true` | Развернуть StatefulSet Redis в кластере (REQ-371) |
| `redis.host` | `""` | Установите, чтобы использовать внешний Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Смените это значение |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Развернуть движок федерации (REQ-028) |
| `trino.workers` | `2` | Реплики воркеров движка федерации (REQ-056) |
| `postgresql.enabled` | `true` | Развернуть PostgreSQL в кластере (REQ-169) |
| `postgresql.host` | `""` | Установите, чтобы использовать внешний PostgreSQL |
| `minio.enabled` | `true` | Развернуть MinIO в кластере (REQ-029) |
| `s3.endpoint` | | URL S3-совместимого эндпоинта |
| `s3.bucket` | `provisa-results` | Бакет для перенаправления больших результатов (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Включить ingress |

### Масштабирование

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Воркеры движка федерации масштабируются независимо — больше воркеров увеличивает пропускную способность и ёмкость параллельных запросов (REQ-056). (REQ-057)

### Обновление конфигурации

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Высокая доступность и восстановление

Provisa применяет двухуровневую модель восстановления во всех режимах развёртывания (REQ-703):

- **Уровень 1 — временные ошибки.** Операции чтения повторяются до 30 секунд при временных ошибках, используя экспоненциальную задержку с полным джиттером. Настройте бюджет через `PROVISA_RETRY_BUDGET_SECS`. Операции записи никогда не повторяются внутренне, а ошибки памяти никогда не подлежат повтору.
- **Уровень 2 — сбой компонента.** Внутренний наблюдатель движка обнаруживает и перезапускает отказавшие программные компоненты в течение 2–3 минут.

Сбои на уровне машины и кластера остаются ответственностью оператора — предоставьте резервные узлы и балансировщик нагрузки (пути Terraform и Helm выше) для устойчивости к потере узлов.

## Зависимости движка федерации

Движки федерации хранилищ данных требуют пакетов Python и системных компонентов сверх стандартной установки Provisa. Все перечисленные здесь пакеты Python объявлены в `pyproject.toml` и устанавливаются как часть стандартного `pip install provisa` или `pip install -e .` [tool-verified: `pyproject.toml` lines 44–52].

Пакеты Python поставляются со стандартной установкой Provisa — дополнительные extras не требуются ни для одного движка хранилища данных. Системные компоненты (драйвер ODBC, облачные CLI, ключи сервисных учётных записей) должны устанавливаться отдельно.

### Пакеты Python (уже в базовых зависимостях)

[tool-verified: `pyproject.toml` lines 41–52]

| Пакет | Движок | Назначение |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Подключение к SQL warehouse; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Подключение + нативный для Arrow `fetch_arrow_table` (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Выполнение запросов |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API для нативного чтения Arrow |
| `google-cloud-storage` | BigQuery | Стейджинг GCS для ссылок на внешние таблицы |
| `pyodbc` | Fabric, Synapse | Подключение ODBC к эндпоинтам T-SQL |
| `azure-identity` | Fabric, Synapse | Токен Azure AD через `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | HTTP-чтение в колоночном формате |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Закрепление совместимости — `google-cloud-*` и OTel используют общую среду выполнения protobuf; `<7` держит их согласованными |
| `grpcio-status<1.82` | gRPC | Согласуется с закреплением `protobuf<7` |

### Требования на уровне системы

Это не пакеты Python — они должны быть установлены на хосте или в контейнере, где выполняется Provisa.

**Microsoft Fabric и Azure Synapse (ODBC)**

`pyodbc` подключается через драйвер Microsoft ODBC Driver для SQL Server (`msodbcsql18`). Драйвер должен быть установлен на хосте — не через pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS:

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux (Ubuntu/Debian):

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa подхватывает драйвер автоматически. Чтобы переопределить имя драйвера (для нестандартных установок), задайте:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Аутентификация Azure AD (Fabric и Synapse)**

Оба движка аутентифицируются через `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` проверяет источники учётных данных по порядку: переменные окружения, workload identity, managed identity, VS Code, `az login` и другие.

Для локальной разработки `az login` — самый простой путь:

```bash
az login
```

Для продакшена используйте managed identity (на ВМ Azure или AKS) — управление учётными данными не требуется. Для аутентификации через service principal задайте:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (сервисная учётная запись)**

`google-cloud-bigquery` использует Application Default Credentials. Для локальной разработки укажите путь к файлу ключа сервисной учётной записи:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Для продакшена на GCP (Cloud Run, GKE с Workload Identity, Compute Engine) библиотека автоматически подхватывает подключённую сервисную учётную запись — переменная окружения не нужна.

Сервисной учётной записи требуются:

- `roles/bigquery.dataViewer` — чтение данных
- `roles/bigquery.jobUser` — выполнение запросов
- `roles/bigquery.dataEditor` — создание внешних таблиц (для ATTACH)
- `roles/storage.objectViewer` — чтение объектов GCS для внешних таблиц

**Databricks (сертификат CA в средах dev-прокси)**

Если Provisa работает за перехватывающим TLS прокси (Charles, mitmproxy, корпоративные прокси), коннектор Databricks SQL может отклонить сертификат прокси. Передайте собственный набор CA:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Коннектор Databricks наследует это от `requests` — специфичная для Databricks переменная окружения не нужна.

### Контрольный список по движкам

**Databricks** (REQ-987)

- [ ] Установлен `databricks-sql-connector` (по умолчанию)
- [ ] URL движка с `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Персональный токен доступа или токен service principal
- [ ] `REQUESTS_CA_BUNDLE` задан, если за перехватывающим TLS прокси

**Snowflake** (REQ-988)

- [ ] Установлен `snowflake-connector-python[pandas]` (по умолчанию)
- [ ] URL движка: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` в `PROVISA_ENGINE_URL` или `federation_hints`

**BigQuery** (REQ-989)

- [ ] Установлены `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` (по умолчанию)
- [ ] Задан `GOOGLE_APPLICATION_CREDENTIALS` (dev) или настроена workload identity (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` задан, если проект нельзя определить из сервисной учётной записи
- [ ] У сервисной учётной записи есть роли BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] Установлены `pyodbc` + `azure-identity` (по умолчанию)
- [ ] Установлен системный драйвер `msodbcsql18`
- [ ] Заданы `FABRIC_SQL_SERVER` и `FABRIC_DATABASE`
- [ ] Аутентификация Azure AD: `az login` (dev) или managed identity / service principal (prod)
- [ ] `FABRIC_WORKSPACE_ID` задан при использовании ссылок на внешнее объектное хранилище

**Azure Synapse** (REQ-989)

- [ ] Те же требования Python + системные, что и для Fabric
- [ ] Заданы `SYNAPSE_SQL_SERVER` и `SYNAPSE_DATABASE`
- [ ] Та же настройка аутентификации Azure AD, что и для Fabric

**ClickHouse** (REQ-986)

- [ ] Установлен `clickhouse-connect` (по умолчанию)
- [ ] URL движка: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` в `federation_hints` для TLS (порт 8443)

---

## Переменные окружения

| Переменная | По умолчанию | Назначение |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Пароль PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включить перенаправление больших результатов в S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Порог числа строк для перенаправления (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Бакет S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL S3-совместимого эндпоинта (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL предподписанного URL (секунды) (REQ-141) |
| `REDIS_HOST` | `localhost` | Хост Redis |
| `REDIS_PORT` | `6379` | Порт Redis |
| `REDIS_PASSWORD` | | Пароль Redis |
| `REDIS_TLS` | `false` | Включить TLS для Redis |
| `TRINO_HOST` | `localhost` | Хост координатора движка федерации Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | HTTP-порт координатора движка федерации Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Ключ активного движка федерации (REQ-989); переопределяет сохранённую конфигурацию |
| `PROVISA_ENGINE_URL` | | URL подключения для движков, управляемых URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Переопределение URL хранилища материализации; по умолчанию — собственное хранилище движка |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Имя драйвера ODBC для Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Путь к JSON-ключу сервисной учётной записи GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | ID проекта GCP (BigQuery; определяется из сервисной учётной записи, если не задан) |
| `FABRIC_SQL_SERVER` | | Имя хоста эндпоинта SQL-аналитики Microsoft Fabric |
| `FABRIC_DATABASE` | | Имя базы данных Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID рабочей области Fabric (требуется для сокращений внешнего объектного хранилища) |
| `SYNAPSE_SQL_SERVER` | | Хост выделенного SQL-пула Azure Synapse или бессерверного эндпоинта |
| `SYNAPSE_DATABASE` | | Имя базы данных Synapse |
| `AZURE_TENANT_ID` | | Арендатор Azure AD (аутентификация service-principal для Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | ID клиента приложения Azure AD |
| `AZURE_CLIENT_SECRET` | | Секрет клиента приложения Azure AD |
| `REQUESTS_CA_BUNDLE` | | Путь к собственному набору CA (коннектор Databricks, dev TLS-прокси) |

---

## Команды CLI

```bash
provisa start              # Start all services
provisa stop               # Stop all services
provisa restart            # Restart
provisa status             # Show service health
provisa open               # Open the UI in the browser
provisa logs               # Tail service logs
provisa export             # Print current config as YAML to stdout
provisa export FILE        # Write current config as YAML to FILE
provisa import FILE        # Replace running config with YAML from FILE
```

(REQ-224, REQ-164)

### Рабочий процесс продвижения конфигурации (dev → test → prod)

Все настройки, специфичные для среды (строки подключения, секреты, порты), принадлежат переменным окружения или менеджерам секретов — не экспортированной конфигурации. Экспортированный YAML фиксирует вашу семантическую модель: источники, домены, роли, представления. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```
