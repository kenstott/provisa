# Развёртывание

## Выбор способа развёртывания

Provisa поддерживает шесть способов развёртывания. Выбирайте, исходя из аудитории и эксплуатационного контекста:

| Способ | Артефакт или скрипт | Лучше всего подходит для |
| ------ | ------------------- | ---------- |
| **Разработка** | `start-ui.sh` | Разработка из исходников, оценка с полным набором демонстрационных данных |
| **Установщик для macOS** | `Provisa-<version>-macOS.dmg` | Рабочие станции разработчиков, оценка |
| **Установщик для Windows** | `Provisa-<version>-windows-x64.exe` | Рабочие станции разработчиков, оценка |
| **Linux AppImage** | `Provisa.AppImage` | Локальные серверы, облачные виртуальные машины, изолированные окружения |
| **Облачные ВМ (AWS)** | `terraform/deploy.sh` | Многоузловое облачное развёртывание с балансировщиками нагрузки |
| **Kubernetes** | `helm/provisa/` | Команды, уже эксплуатирующие K8s |

### Виртуальные машины или Kubernetes

Оба варианта корпоративного уровня. Путь с ВМ и AppImage проще: не нужно готовить кластер, не нужно настраивать политики CNI или RBAC, а AppImage полностью самодостаточен (REQ-223). Он естественно вписывается в существующие средства управления серверами (Ansible, Puppet, агенты Datadog, форвардеры Splunk и т. д.).

Выбирайте Kubernetes, только если ваша команда уже эксплуатирует кластер K8s и хочет, чтобы Provisa участвовала в этой эксплуатационной модели (плавающие развёртывания, HPA, единая наблюдаемость) (REQ-056). Возможности эквивалентны — Kubernetes добавляет эксплуатационные накладные расходы, а не функциональность.

### Получение образов и сканирование безопасности

Все производственные пути требуют получить артефакты Provisa до того, как начнётся развёртывание. «Изоляция от сети» относится к тому, что происходит во время установки на целевой машине, — артефакты нужно получить заранее.

**Установщики для macOS и Windows.** Скачайте со [страницы релизов GitHub](https://github.com/provisa/provisa/releases). Полностью самодостаточны; после скачивания интернет не требуется (REQ-227). Предназначены для разработки и оценки, не для продуктива, — шлюз сканирования образов не предполагается.

**Путь AppImage.** Скачайте со [страницы релизов GitHub](https://github.com/provisa/provisa/releases) и перенесите на целевую машину. AppImage упаковывает все образы компонентов в виде tar-архивов внутри файловой системы squashfs (REQ-294) — большинство сканеров реестров не может проверить их на месте. Обратитесь к своей команде по работе с клиентами Provisa за дайджестами образов компонентов, чтобы проверить их своим сканером самостоятельно.

**Путь Terraform.** AppImage нужно загрузить в S3 до запуска `terraform/deploy.sh`. Узлы EC2 скачивают его при загрузке через роль IAM — им требуется исходящий доступ к S3 (напрямую или через шлюзовую конечную точку VPC). Применяйте ту же политику сканирования, что и для пути AppImage.

**Путь Helm и Kubernetes.** Отдельные образы нужно поместить в реестр, доступный кластеру. Этот путь лучше всего совместим со сканированием на уровне реестра (Prisma Cloud, Aqua, Trivy, AWS Inspector) — образы являются полноценными объектами, которые сканеры понимают напрямую. Для изолированных кластеров зеркалируйте образы во внутренний реестр и переопределите ссылки в `values.yaml` (REQ-294).

---

## Разработка (из исходников)

### Рекомендуется: `start-ui.sh`

Самый простой способ запустить Provisa из исходников. Одной командой поднимает всю инфраструктуру, серверный API и сервер разработки для интерфейса (REQ-055). Ctrl+C корректно всё останавливает.

**Предварительные требования:** Docker Desktop, Node.js, виртуальное окружение Python в `.venv/`

```bash
./start-ui.sh
```

Что он делает:

- Запускает `docker-compose.core.yml` + `docker-compose.dev.yml` (все основные и демонстрационные службы) и ждёт, пока они станут работоспособными (REQ-055)
- Наполняет Kafka демонстрационными данными
- Синхронизирует зависимости Python из `.venv/`
- Запускает серверный API на порту 8001 (журналы в `.logs/server.log`) (REQ-558)
- Запускает сервер разработки Vite для интерфейса на порту 3000 (REQ-559)
- Печатает адреса и ждёт; Ctrl+C останавливает всё и разбирает compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Параметры:**

`--reset-volumes` — выполняет `docker compose down -v` перед запуском, уничтожая все тома Docker (данные PostgreSQL, объекты MinIO, состояние Redis и т. д.) (REQ-170). Используйте, когда нужно начать полностью с нуля — после изменения схемы во время разработки или когда Docker упал и оставил тома повреждёнными. **Все данные будут потеряны.**

`--observability` — добавляет полную инструментацию трассировки и метрик. Скачивает Java-агент OpenTelemetry и правит `jvm.config` Trino, чтобы тот его загрузил, инструментирует серверную часть Provisa экспортом OTLP и запускает коллектор OTel, Prometheus, Tempo и Grafana (`http://localhost:3100`) (REQ-330). Правка `jvm.config` автоматически откатывается по Ctrl+C.

### Ручные шаги (только серверная часть, без интерфейса)

Если вам нужен только API:

1. Установите [Docker Desktop](https://docs.docker.com/get-docker/)
2. Запустите основные службы:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Запустите API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Проверьте: `curl http://localhost:8001/health`

### Полный стек (Provisa в контейнере)

Чтобы запустить API как контейнер, а не на хосте:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Службы

**Основные (`docker-compose.core.yml`) — требуются всегда:**

| Служба | Порт | Назначение |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Метаданные конфигурации и каталог Iceberg (REQ-169) |
| PgBouncer | 6432 | Пул соединений (REQ-053) |
| Федеративный движок | 8080 | Федерация запросов (REQ-028) |
| Redis | 6379 | Кеш результатов запросов (REQ-371) |
| MinIO | 9000/9001 | S3-совместимое объектное хранилище (REQ-029, REQ-171) |

**Демонстрационные (`docker-compose.dev.yml`) — необязательные, включаются `start-ui.sh`:**

| Служба | Порт | Назначение |
| --------- | ------ | --------- |
| MongoDB | 27017 | Демонстрационный источник NoSQL |
| Kafka | 9092 | Демонстрационный потоковый источник |
| Schema Registry | 8081 | Демонстрационное управление схемами Avro и Protobuf |
| Debezium | — | Демонстрационный коннектор CDC |
| Elasticsearch | 9200 | Демонстрационный поисковый источник |
| Neo4j | 7474/7687 | Демонстрационный графовый источник |
| Fuseki | 3030 | Демонстрационное хранилище триплетов SPARQL |
| OpenTelemetry Collector | — | Сбор трассировок (с `--observability`) (REQ-302) |
| Prometheus | 9090 | Метрики (с `--observability`) (REQ-330) |
| Tempo | — | Хранение трассировок (с `--observability`) (REQ-330) |
| Grafana | 3100 | Дашборды (с `--observability`) (REQ-330) |

### Бэкенд телеметрии (`otlp2sql`)

Стек `--observability`, описанный выше (Collector → Tempo/Prometheus/Grafana), — это один
путь телеметрии. Другой — `otlp2sql` (`provisa.observability.otlp2sql`):
приёмник OTLP/HTTP, который пишет трассировки, метрики и журналы в базу данных SQL,
выбранную URL-адресом SQLAlchemy, извлекая атрибуты спанов `provisa.*` при приёме,
так что отдельное задание уплотнения не требуется. Записи выполняются пакетами
(`OTLP2SQL_BATCH_MAX_ROWS`, по умолчанию 1000; `OTLP2SQL_BATCH_MAX_SECS`, по умолчанию 2 с).

Телеметрия получает собственное хранилище, отдельное от базы данных плоскости управления. Выберите
бэкенд через `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Бэкенд | Примечания |
| --- | --- | --- |
| *(не задано)* | выделенная DuckDB в `~/.provisa/telemetry/` | по умолчанию; ни сервера, ни Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | приём с высокой частотой и автоматическими фоновыми слияниями |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | умеренный объём |
| `trino://user@host:8080/otel` | Trino / Iceberg | технически работает, **не рекомендуется** — см. ниже |

**О `trino://`:** диалект Trino для SQLAlchemy выдаёт корректные DDL и
`INSERT` для Trino, поэтому технически он пригоден как бэкенд `otlp2sql`. Он не
рекомендуется ни для чего, кроме низкой частоты приёма. Каждый сброс пакета превращается в
распределённый `INSERT` в Trino плюс снимок Iceberg, поэтому телеметрия с высокой частотой
порождает множество мелких файлов и снимков и всё равно требует периодических
`ALTER TABLE ... EXECUTE optimize` и `expire_snapshots` — которых `otlp2sql`
не выполняет. Вдобавок это ставит движок запросов на горячий путь приёма.

Для больших объёмов телеметрии в Trino и Iceberg используйте вместо этого `otlp2parquet`: он
пишет parquet в объектное хранилище, минуя Trino, а запланированное уплотнение
в Trino сворачивает сырые файлы в живые таблицы Iceberg. Если нужен один
движок, справляющийся и с высокой частотой приёма, и с уплотнением, предпочтите ClickHouse.

Направьте экспортёры OTLP приложения и Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) на
конечную точку `otlp2sql` и зарегистрируйте домен ops для того же
`PROVISA_OPS_DB_URL`, чтобы он читал то, что записал приёмник.

---

## Установщик для macOS

Для рабочих станций разработчиков и оценки. Полностью автономен — после скачивания интернет не требуется (REQ-227).

Базовый установщик выполняет **нативную установку**: федеративный движок DuckDB + плоскость управления на SQLite + кеш в памяти (fakeredis), без Docker, виртуальных машин, Trino, Redis и MinIO (REQ-972, REQ-979). Федеративный движок выбирается в мастере — DuckDB (нативный, по умолчанию), Trino в Docker или внешний движок (REQ-973). Наблюдаемость — это всегда включённая самотелеметрия, доступная в административном разделе; стек с коллектором, Prometheus и Grafana в Docker — необязательная внешняя демонстрация, а не переключатель (REQ-975). Набор демонстрационных данных необязателен и по умолчанию выключен (REQ-978). Trino, стек наблюдаемости в Docker и демонстрация — тяжёлые дополнения, которые разрешаются сначала локально (каталог рядом с установщиком, смонтированные тома, `~/Downloads`, затем релиз на GitHub), чтобы предприятия могли заранее подготовить архивы для изолированных установок (REQ-977).

### Шаги

1. Скачайте `Provisa-<version>-macOS.dmg` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)
2. Откройте DMG и перетащите **Provisa.app** в `/Applications`
3. Дважды щёлкните **Provisa.app** — настройка при первом запуске выполняется один раз; мастер предлагает описанный выше выбор движка, наблюдаемости и демонстрации (REQ-1007)
4. Откройте терминал:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Сохранность данных

Все данные хранятся в `~/.provisa/` (REQ-224). Чтобы удалить всё: `provisa uninstall`.

---

## Установщик для Windows

Для рабочих станций разработчиков и оценки. Полностью автономен — после скачивания интернет не требуется (REQ-227).

Как и в macOS, базовый установщик для Windows — это **нативный уровень**: автономная среда выполнения Python + wheel provisa + DuckDB/pg_duckdb + плоскость управления на SQLite, без Docker, без виртуальных машин и без образов контейнеров (REQ-979). Федеративный движок (Trino), стек наблюдаемости и набор демонстрационных данных добавляются позже отдельными послойными установщиками, по порядку: установщик Container (`Provisa-Container-<version>.exe`, добавляющий WSL2 + containerd + Trino), затем установщик Obs (требует уровня контейнеров) и затем установщик Demo (требует Core + Obs). Подсказки при первом запуске объясняют, как инициализировать федеративный движок, запустив установщик Container (REQ-1005).

### Шаги

1. Скачайте `Provisa-<version>-windows-x64.exe` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)
2. Запустите установщик — права администратора не нужны; установка выполняется в `%LOCALAPPDATA%\Programs\Provisa\`
3. Откройте **Provisa First Launch** из меню «Пуск» — нативная настройка выполняется один раз и печатает подсказки по дальнейшим шагам для послойных дополнений (REQ-1005)
4. Откройте новый терминал:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Сохранность данных

Все данные хранятся в `%USERPROFILE%\.provisa\`.

---

## Linux AppImage — одноузловая или многоузловая ВМ

### Что это

`Provisa.AppImage` — один самодостаточный исполняемый файл, включающий (REQ-223, REQ-228):

- Демон Docker без root-прав (`dockerd-rootless.sh` + `rootlesskit`) — системный Docker и root не нужны
- Все tar-архивы образов контейнеров (PostgreSQL, PgBouncer, MinIO, Redis, федеративный движок, API Provisa) (REQ-294)
- Обёртку CLI Provisa и скрипт настройки при первом запуске

Образ Provisa собирается заранее, во время упаковки, — исходный код Python в него никогда не входит.

### Когда использовать

- Локальное «железо» или ВМ (один или несколько узлов)
- Облачные ВМ без кластера K8s
- Изолированные окружения (REQ-294)
- Когда нужна эксплуатация проще, чем в Kubernetes

---

### Шаги — один узел

1. Скачайте `Provisa.AppImage` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases) и перенесите на целевую машину
2. Сделайте файл исполняемым:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Запустите настройку первого запуска:

   ```bash
   ./Provisa.AppImage
   ```

4. Мастер настройки спрашивает:
   - **Роль** → выберите `primary`
   - **Бюджет ОЗУ** → сколько оперативной памяти выделить (0 = вся доступная); определяет число рабочих узлов Trino
   - **Имя узла** → объявляемый адрес этого узла
   - **Порт API** → по умолчанию `8000` (REQ-560)
5. Настройка загружает все образы контейнеров (~2–5 минут), записывает конфигурацию и запускает службы
6. Проверьте:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Шаги — несколько узлов (главный)

Сначала выполните эти шаги на главном узле. Вторичные узлы настраиваются после того, как главный уже работает.

1. Скачайте `Provisa.AppImage` и перенесите на главную машину
2. Откройте нужные порты в межсетевом экране (вторичные узлы будут подключаться к ним входящими соединениями):

   | Порт | Служба |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Координатор федеративного движка |
   | 8000 | API Provisa |

3. Сделайте файл исполняемым и запустите:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Мастер настройки спрашивает:
   - **Роль** → выберите `primary`
   - **Бюджет ОЗУ**, **имя узла**, **порт API** → отвечайте так же, как для одного узла
5. После завершения настройки запишите **частный IP-адрес** этой машины — он нужен вторичным узлам
6. Мастер печатает блок upstream для nginx — сохраните его для конфигурации балансировщика нагрузки
7. Проверьте:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Шаги — несколько узлов (каждый вторичный)

Повторите эти шаги на каждом дополнительном узле после того, как главный узел работает и доступен.

1. Скачайте `Provisa.AppImage` и перенесите на вторичную машину
2. Убедитесь, что вторичный узел достаёт до главного:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Сделайте файл исполняемым и запустите:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Мастер настройки спрашивает:
   - **Роль** → выберите `secondary`
   - **IP главного узла** → введите IP-адрес главного узла (связность проверяется вживую)
   - **Бюджет ОЗУ**, **имя узла**, **порт API** → отвечайте как выше
5. Настройка загружает сокращённый набор образов (без PostgreSQL, PgBouncer, MinIO и Redis — они работают только на главном узле) (REQ-561), запускает API Provisa и рабочий узел федеративного движка
6. Проверьте:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Добавьте этот узел в upstream балансировщика нагрузки

---

### Топология «главный — вторичные»

**Главный узел** выполняет все службы в единственном экземпляре:

| Служба | Почему в единственном экземпляре |
| --------- | --------------- |
| PostgreSQL | Общая схема, конфигурация приложения, семантическая модель |
| Redis | Общий кеш результатов запросов и состояние подписок (REQ-371) |
| MinIO | Общее объектное хранилище для перенаправленных результатов и снимков материализованных представлений (REQ-029) |
| Координатор федеративного движка | Здесь регистрируются все рабочие узлы (главный и вторичные) (REQ-028) |

**Вторичные узлы** выполняют только:

- API Provisa — без состояния; при запуске читает всю конфигурацию из PostgreSQL на главном узле (REQ-057, REQ-562)
- Рабочий узел федеративного движка — сам регистрируется у координатора на главном узле (REQ-028)

Всё состояние приложения проходит через PostgreSQL главного узла. Ручная синхронизация не требуется. (REQ-562)

---

### Неинтерактивный (автоматизированный) первый запуск

Для Terraform, cloud-init или Ansible — передайте флаги вместо ответов на вопросы:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Неинтерактивный режим устанавливает юнит systemd (`/etc/systemd/system/provisa.service`) для запуска при загрузке. (REQ-563)

| Флаг | Описание |
| ------ | ------------- |
| `--non-interactive` | Пропустить все вопросы; установить юнит systemd |
| `--role primary\|secondary` | Роль узла |
| `--primary-ip <ip>` | IP-адрес главного узла (обязателен для вторичного) |
| `--ram-gb <n>` | Сколько ОЗУ выделить (0 = вся доступная) |

---

## Развёртывание на облачных ВМ — Terraform (AWS)

Разворачивает полноценный многоузловой кластер Provisa в AWS — VPC, группы безопасности, экземпляры EC2, ALB, NLB — одной интерактивной командой. (REQ-564)

### Файлы

| Файл | Назначение |
| ------ | --------- |
| `terraform/deploy.sh` | Интерактивная обёртка — собирает параметры, проверяет учётные данные, пишет `terraform.tfvars`, выполняет apply |
| `terraform/aws/variables.tf` | Все определения переменных со значениями по умолчанию |
| `terraform/aws/main.tf` | VPC, подсети, группы безопасности, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | Адреса конечных точек и IP-адреса узлов |

### Шаги

1. Скачайте `Provisa.AppImage` со [страницы релизов GitHub](https://github.com/provisa/provisa/releases)

2. Загрузите его в бакет S3 в своей учётной записи AWS:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Убедитесь, что учётные данные AWS доступны в вашей оболочке (любым способом):
   - Переменные окружения: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Именованный профиль: `export AWS_PROFILE=my-profile`
   - Активная сессия SSO: `aws sso login`

4. (Необязательно) Если нужен доступ к узлам по SSH, создайте пару ключей EC2 в целевом регионе и запишите её имя

5. Запустите обёртку развёртывания:

   ```bash
   bash terraform/deploy.sh
   ```

6. Ответьте на вопросы мастера (см. справочную таблицу ниже). Скрипт проверяет наличие AppImage в S3, прежде чем продолжить, и прерывается, если файла нет

7. Просмотрите сводку развёртывания и подтвердите

8. Terraform разворачивает всю инфраструктуру (~5–10 минут). После apply скрипт печатает:

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Необязательно) Направьте записи DNS на DNS-имена ALB и NLB

10. Проверьте:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Вопросы мастера

| Вопрос | По умолчанию | Примечания |
| ---------- | --------- | ------- |
| Облачный провайдер | — | Сегодня только AWS |
| Учётные данные AWS | — | Сначала проверяет активную сессию |
| Регион | `us-east-1` | |
| Количество узлов | `2` | 1 = только главный, без балансировщика; 2 и более = главный + вторичные + ALB/NLB |
| Тип экземпляра | `m7i.2xlarge` | См. руководство по подбору размера ниже |
| Размер корневого тома | `100 GB` | На узел |
| Бюджет ОЗУ | `0` (вся ОЗУ) | Определяет число рабочих узлов Trino на узел |
| Бакет S3 | — | Проверяется вживую перед продолжением |
| Ключ S3 | `releases/Provisa.AppImage` | |
| Доступ по SSH | Нет | Требует имени существующей пары ключей и административного CIDR |
| CIDR для VPC | `10.0.0.0/16` | |

### Руководство по подбору размера экземпляра

| Тип | vCPU | ОЗУ | Рабочих узлов Trino на узел | Сценарий использования |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 ГБ | 0 | Разработка и небольшие наборы данных |
| `m7i.2xlarge` | 8 | 32 ГБ | 1 | Небольшой продуктив |
| `m7i.4xlarge` | 16 | 64 ГБ | 2 | Средний продуктив |
| `m7i.8xlarge` | 32 | 128 ГБ | 4 | Крупный продуктив |

Все узлы отдают рабочие процессы одному координатору на главном узле (REQ-028). Кластер из трёх узлов `m7i.4xlarge` даёт в сумме 6 рабочих узлов Trino.

### Что разворачивается

- VPC с двумя публичными подсетями в двух зонах доступности (REQ-564)
- Группы безопасности: группа балансировщиков (публичный входящий трафик на 8000/8815), группа узлов (балансировщик → узлы, внутрикластерный трафик, необязательный SSH)
- Роль IAM и профиль экземпляра с правом S3 GetObject на бакет с AppImage
- Главный экземпляр EC2 — выполняет первый запуск в режиме `--non-interactive --role primary`
- Вторичные экземпляры EC2 (node_count − 1) — выполняют первый запуск в режиме `--non-interactive --role secondary --primary-ip <primary private IP>`; зависят от того, что главный узел завершил настройку первым
- ALB на порту 8000 — HTTP API, проверка работоспособности по `/health` (REQ-560)
- NLB на порту 8815 — Arrow Flight и gRPC (REQ-143)
- Оба балансировщика подключаются ко всем узлам

### Контрольный список предварительных требований

- [ ] Права IAM: полный доступ к EC2, ELB и VPC, создание ролей IAM, S3 GetObject на бакет с AppImage
- [ ] `Provisa.AppImage` загружен в S3
- [ ] У узлов EC2 есть исходящий доступ к S3 (прямой интернет или шлюзовая конечная точка S3 в VPC)
- [ ] Пара ключей EC2 существует в целевом регионе (если нужен SSH)
- [ ] Terraform ≥ 1.5 установлен локально
- [ ] Записи DNS для ALB и NLB спланированы (необязательно, но рекомендуется)
- [ ] Сертификат ACM готов, если требуется HTTPS (в базовый Terraform не входит)

### Секреты

В Terraform не встроено никаких секретов. AppImage генерирует учётные данные при первом запуске и записывает их в `~/.provisa/config.yaml` на каждом узле (REQ-563). Для продуктива получите административный токен с главного узла после развёртывания:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes и Helm

### Когда использовать

Ваша команда уже эксплуатирует кластер Kubernetes и хочет, чтобы Provisa участвовала в этой эксплуатационной модели (REQ-056). Если вы оцениваете Provisa или разворачиваете её локально без готового кластера, путь AppImage проще.

Обратите внимание: AppImage Provisa не может работать внутри пода Kubernetes — ему нужны FUSE и демон Docker без root-прав, недоступные в стандартных профилях безопасности подов.

### Шаги

1. Проверьте доступ к кластеру:

   ```bash
   kubectl cluster-info
   ```

2. Скачайте и зеркалируйте образы в свой внутренний реестр (требуется для изолированных или сканируемых окружений; пропустите, если тянете напрямую из публичных реестров) (REQ-294):

   | Образ | Для чего используется |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Координатор и рабочие узлы федеративного движка (REQ-169) |
   | `postgres:16` | PostgreSQL внутри кластера (если `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer внутри кластера (если `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis внутри кластера (если `redis.enabled` и не задан `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO внутри кластера (если `minio.enabled`) (REQ-029) |

   Для окружений со сканированием реестра:
   - Отправьте каждый образ в промежуточный реестр
   - Запустите свой сканер (Prisma Cloud, Aqua, Trivy, AWS Inspector) и получите утверждение
   - Продвиньте образы в производственный внутренний реестр

3. Решите до установки:
   - **PostgreSQL** — внутри кластера (`postgresql.enabled: true`) или внешняя управляемая (`postgresql.host`)? Для продуктива рекомендуется внешняя
   - **Redis** — внутри кластера или внешний (`redis.host`)? Смените пароль по умолчанию (`redis.password`)
   - **MinIO или S3** — MinIO внутри кластера или нативный S3? Для AWS используйте S3 с ролью IAM
   - **Секреты** — для оценки передавайте через `--set`; для продуктива используйте External Secrets или Vault Agent

4. Установите чарт:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Если используется внутренний реестр, добавьте переопределения образов:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Убедитесь, что поды запущены:

   ```bash
   kubectl get pods -n provisa
   ```

6. Проверьте API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Необязательно) Включите ingress для внешнего доступа — задайте `ingress.enabled: true` и настройте свой контроллер ingress

### Контрольный список предварительных требований

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Класс хранилища, поддерживающий PVC с `ReadWriteOnce` (для служб с состоянием внутри кластера)
- [ ] Образы доступны кластеру (публичный или внутренний реестр)
- [ ] Конечная точка PostgreSQL и учётные данные (если внешняя)
- [ ] Конечная точка Redis и учётные данные (если внешний)
- [ ] Бакет S3 и учётные данные или роль IAM
- [ ] Выбран административный токен
- [ ] Настроен контроллер ingress (если нужен внешний доступ)

### Основные значения

| Значение | По умолчанию | Описание |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Реплики API Provisa (без состояния) (REQ-057) |
| `config.pgHost` | `postgres` | Хост PostgreSQL |
| `config.pgPassword` | | Пароль PostgreSQL |
| `config.adminToken` | | Bearer-токен административного API |
| `redis.enabled` | `true` | Развернуть StatefulSet Redis внутри кластера (REQ-371) |
| `redis.host` | `""` | Задайте, чтобы использовать внешний Redis |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Смените это значение |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Развернуть федеративный движок (REQ-028) |
| `trino.workers` | `2` | Реплики рабочих узлов федеративного движка (REQ-056) |
| `postgresql.enabled` | `true` | Развернуть PostgreSQL внутри кластера (REQ-169) |
| `postgresql.host` | `""` | Задайте, чтобы использовать внешнюю PostgreSQL |
| `minio.enabled` | `true` | Развернуть MinIO внутри кластера (REQ-029) |
| `s3.endpoint` | | URL S3-совместимой конечной точки |
| `s3.bucket` | `provisa-results` | Бакет для перенаправления больших результатов (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Включить ingress |

### Масштабирование

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Рабочие узлы федеративного движка масштабируются независимо — больше рабочих узлов повышают пропускную способность и число одновременных запросов (REQ-056). (REQ-057)

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

- **Уровень 1 — временные ошибки.** Операции чтения повторяются до 30 секунд при временных ошибках, с экспоненциальной задержкой и полным разбросом. Бюджет настраивается через `PROVISA_RETRY_BUDGET_SECS`. Операции записи никогда не повторяются внутренне, а ошибки памяти никогда не считаются повторяемыми.
- **Уровень 2 — отказ компонента.** Внутренний наблюдатель за движком обнаруживает и перезапускает отказавшие программные компоненты в течение 2–3 минут.

Отказы на уровне машины и кластера остаются зоной ответственности оператора — выделите избыточные узлы и балансировщик нагрузки (пути Terraform и Helm выше) для устойчивости к потере узла.

## Зависимости федеративных движков

Федеративные движки хранилищ данных требуют пакетов Python и системных компонентов сверх установки Provisa по умолчанию. Все перечисленные здесь пакеты Python объявлены в `pyproject.toml` и устанавливаются в рамках обычного `pip install provisa` или `pip install -e .` [tool-verified: `pyproject.toml` lines 44–52].

Пакеты Python поставляются с установкой Provisa по умолчанию — никаких дополнительных extras ни для одного движка хранилища не требуется. Системные компоненты (драйвер ODBC, облачные CLI, ключи сервисных учётных записей) нужно устанавливать отдельно.

### Пакеты Python (уже в основных зависимостях)

[tool-verified: `pyproject.toml` lines 41–52]

| Пакет | Движок | Назначение |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Подключение к SQL-хранилищу; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Подключение и нативный для Arrow `fetch_arrow_table` (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Выполнение запросов |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API для нативных для Arrow чтений |
| `google-cloud-storage` | BigQuery | Промежуточное хранение в GCS для ссылок на внешние таблицы |
| `pyodbc` | Fabric, Synapse | Подключение по ODBC к конечным точкам T-SQL |
| `azure-identity` | Fabric, Synapse | Токен Azure AD через `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Колоночные чтения по HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Закрепление совместимости — `google-cloud-*` и OTel используют общую среду выполнения protobuf; `<7` удерживает их согласованными |
| `grpcio-status<1.82` | gRPC | Согласовано с закреплением `protobuf<7` |

### Системные требования

Это не пакеты Python — их нужно установить на хосте или в контейнере, где работает Provisa.

**Microsoft Fabric и Azure Synapse (ODBC)**

`pyodbc` подключается через Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Драйвер нужно установить на хосте — не через pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

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

Для локальной разработки проще всего `az login`:

```bash
az login
```

Для продуктива используйте managed identity (на ВМ Azure или в AKS) — управлять учётными данными не нужно. Для аутентификации по субъекту-службе задайте:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (сервисная учётная запись)**

`google-cloud-bigquery` использует Application Default Credentials. Для локальной разработки укажите файл ключа сервисной учётной записи:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Для продуктива в GCP (Cloud Run, GKE с Workload Identity, Compute Engine) библиотека автоматически подхватывает привязанную сервисную учётную запись — переменная окружения не нужна.

Сервисной учётной записи требуются:

- `roles/bigquery.dataViewer` — чтение данных
- `roles/bigquery.jobUser` — выполнение запросов
- `roles/bigquery.dataEditor` — создание внешних таблиц (для ATTACH)
- `roles/storage.objectViewer` — чтение объектов GCS для внешних таблиц

**Databricks (сертификат CA в окружениях с прокси для разработки)**

Если Provisa работает за прокси, перехватывающим TLS (Charles, mitmproxy, корпоративные прокси), коннектор Databricks SQL может отклонить сертификат прокси. Передайте собственный набор CA:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Коннектор Databricks наследует это от `requests` — отдельной переменной окружения для Databricks не требуется.

### Контрольный список по каждому движку

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` установлен (по умолчанию)
- [ ] URL движка с `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Персональный токен доступа или токен субъекта-службы
- [ ] `REQUESTS_CA_BUNDLE` задан, если используется прокси с перехватом TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` установлен (по умолчанию)
- [ ] URL движка: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` в `PROVISA_ENGINE_URL` или в `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` установлены (по умолчанию)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` задан (разработка) или настроен workload identity (продуктив)
- [ ] `GOOGLE_CLOUD_PROJECT` задан, если проект нельзя вывести из сервисной учётной записи
- [ ] У сервисной учётной записи есть роли BigQuery Data Viewer и Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` и `azure-identity` установлены (по умолчанию)
- [ ] Системный драйвер `msodbcsql18` установлен
- [ ] `FABRIC_SQL_SERVER` и `FABRIC_DATABASE` заданы
- [ ] Аутентификация Azure AD: `az login` (разработка) или managed identity либо субъект-служба (продуктив)
- [ ] `FABRIC_WORKSPACE_ID` задан, если используются ссылки на внешнее объектное хранилище

**Azure Synapse** (REQ-989)

- [ ] Те же требования к Python и системе, что и для Fabric
- [ ] `SYNAPSE_SQL_SERVER` и `SYNAPSE_DATABASE` заданы
- [ ] Та же настройка аутентификации Azure AD, что и для Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` установлен (по умолчанию)
- [ ] URL движка: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` в `federation_hints` для TLS (порт 8443)

---

## Переменные окружения

| Переменная | По умолчанию | Назначение |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Пароль PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Путь к файлу конфигурации (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Включить перенаправление больших результатов в S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Порог по числу строк для перенаправления (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Бакет S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL S3-совместимой конечной точки (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL предподписанного URL (в секундах) (REQ-141) |
| `REDIS_HOST` | `localhost` | Хост Redis |
| `REDIS_PORT` | `6379` | Порт Redis |
| `REDIS_PASSWORD` | | Пароль Redis |
| `REDIS_TLS` | `false` | Включить TLS для Redis |
| `TRINO_HOST` | `localhost` | Хост координатора федеративного движка Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | HTTP-порт координатора федеративного движка Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Ключ активного федеративного движка (REQ-989); переопределяет сохранённую конфигурацию |
| `PROVISA_ENGINE_URL` | | URL подключения для движков, управляемых через URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Переопределение URL хранилища материализации; по умолчанию используется собственное хранилище движка |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Имя драйвера ODBC для Fabric и Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Путь к JSON-ключу сервисной учётной записи GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | Идентификатор проекта GCP (BigQuery; выводится из сервисной учётной записи, если не задан) |
| `FABRIC_SQL_SERVER` | | Имя узла конечной точки SQL-аналитики Microsoft Fabric |
| `FABRIC_DATABASE` | | Имя базы данных Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID рабочей области Fabric (обязателен для ярлыков на внешнее объектное хранилище) |
| `SYNAPSE_SQL_SERVER` | | Имя узла выделенного пула SQL или бессерверного пула Azure Synapse |
| `SYNAPSE_DATABASE` | | Имя базы данных Synapse |
| `AZURE_TENANT_ID` | | Арендатор Azure AD (аутентификация субъектом-службой для Fabric и Synapse) |
| `AZURE_CLIENT_ID` | | Идентификатор клиента приложения Azure AD |
| `AZURE_CLIENT_SECRET` | | Секрет клиента приложения Azure AD |
| `REQUESTS_CA_BUNDLE` | | Путь к собственному набору CA (коннектор Databricks, TLS-прокси для разработки) |

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

### Процесс продвижения конфигурации (dev → test → prod)

Все настройки, зависящие от окружения (строки подключения, секреты, порты), должны храниться в переменных окружения или менеджерах секретов, а не в экспортированной конфигурации. Экспортированный YAML фиксирует вашу семантическую модель: источники, домены, роли, представления. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


См. также: [Окружения](environments.md) — о том, как управлять именованными копиями вашей управляемой модели, изолированными по схемам.
