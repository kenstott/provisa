# Выпуск релизов {: #releasing }

Релизы запускаются push'ем git-тега. Имя тега определяет канал.

## Соглашения о тегах {: #tag-conventions }

| Формат тега | Канал | Тип релиза GitHub |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Создание релиза {: #creating-a-release }

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

Рабочий процесс CI (`build-dmg.yml`, названный "Build Provisa Packages") запускается на любой тег `v*` и выполняет следующие задачи, большинство параллельно:

1. **Определение метаданных релиза** — определяет канал по суффиксу тега, выводит версию PEP 440 и имена артефактов
2. **Скачивание / упаковка плагинов Trino** — тянет коннекторы Calcite Trino и упаковывает tar-архив
3. **Скачивание базовых / obs / demo образов Docker** — сохраняет tar-архивы образов сервисов (arm64, плюс amd64 core для уровня контейнера Windows)
4. **Сборка macOS Core / Obs / Demo DMG** — выполняется на `macos-14` (Apple Silicon), изолированно
5. **Сборка Linux AppImage** — core, изолированно
6. **Сборка установщика Windows Core** — нативный, встроенный Python, без Docker
7. **Сборка установщика уровня Windows Container** — WSL2 + Trino, скачивает образы по требованию (без VirtualBox/OVA)
8. **Сборка драйвера JDBC** — shaded JAR через Maven
9. **Сборка и тестирование Python-клиента**, затем **публикация в PyPI**
10. **Публикация GitHub Release** — загружает все артефакты, устанавливает флаг pre-release для alpha/beta/rc

## Артефакты релиза {: #release-assets }

Каждый релиз публикует следующие артефакты, все прикреплённые к GitHub Release (колесо (wheel) также публикуется в PyPI):

| Артефакт | Платформа / Использование |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, изолированно) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Нативная среда выполнения Python для macOS (монтировать вместе с Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | Расширение наблюдаемости для macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | Демо-расширение для macOS (требует Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Linux x86_64 core (изолированно) |
| `Provisa-<tag>-windows-x64.exe` | Нативный установщик Windows x64 (встроенный Python, без Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Обновление уровня контейнера Windows x64 (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | Драйвер JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Python-клиент (также в PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Tar-архивы образов Core Services (arm64, изолированно) |
| `provisa-core-images-amd64-<tag>.zip` | Образы Core Services (amd64, уровень контейнера Windows / изоляция) |
| `provisa-obs-images-<tag>.tar.gz` | Образы стека наблюдаемости (опционально) |
| `provisa-demo-images-<tag>.tar.gz` | Образы пакета демо-данных (опционально) |
| `provisa-trino-plugins-<tag>.tar.gz` | Коннекторы Coordination Engine (SharePoint, Splunk, File) |

Версия Python-клиента автоматически преобразуется в формат PEP 440:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## Настройка публикации в PyPI (однократно) {: #pypi-publishing-setup-one-time }

Задачи `publish-pypi` (`provisa-client`) и `publish-provisa-pypi` (`provisa`) в `.github/workflows/build-dmg.yml` публикуют через доверенную публикацию PyPI (trusted publishing): задачу аутентифицирует её токен GitHub OIDC (`id-token: write`), и API-токен не читается. Зарегистрируйте этот репозиторий и `build-dmg.yml` как доверенного издателя для каждого проекта на pypi.org (**Your projects → Manage → Publishing**). После этого публикации выполняются автоматически на каждый тег.

## Подпись и цепочка поставок {: #signing-and-supply-chain }

Что несёт каждый артефакт релиза в том виде, в каком его собирают рабочие процессы релиза:

| Артефакт | Подпись |
| -------- | --------- |
| `provisa` и `provisa-client` на PyPI | Подписанная через Sigstore аттестация PEP 740 для каждого файла (`attestations: true`), проверяемая на pypi.org |
| `provisa-pg-ext`, `provisa-duckdb-ext` на PyPI | Подписанная через Sigstore аттестация PEP 740 для каждого файла |
| Драйвер JDBC `.jar` | Аттестация происхождения сборки Sigstore (`actions/attest-build-provenance`) |
| Установщики macOS `.dmg` | Подпись кода Developer ID и нотаризация Apple |
| Установщики Windows `.exe` | Не подписаны |
| Linux `.AppImage` | Не подписан |

Подписанная установка на любой ОС — `pip install provisa`: пакет PyPI с аттестацией Sigstore. Рецензент, которому нужен подписанный артефакт на Linux или Windows, устанавливает через pip. Сборка Windows подписывается через Authenticode, когда настроен сертификат подписи кода (`WINDOWS_CERT_PFX_BASE64`, `WINDOWS_CERT_PFX_PASSWORD`, `WINDOWS_CERT_TIMESTAMP_URL`, читаются `packaging/windows/build-sfx.ps1`); сейчас он не настроен, поэтому сборка поставляется без подписи.

Зависимости и шлюзы CVE:

- Зависимости Python зафиксированы с хешами в `uv.lock`; CI устанавливает их командой `uv sync --frozen`.
- Рабочий процесс `CVE Audit` (`.github/workflows/cve-audit.yml`) запускает `pip-audit` по зафиксированному набору при каждом push в `main`, при каждом pull request и еженедельно.
- При релизе сборки `provisa` и `provisa-client` каждая создают SBOM CycloneDX и запускают `pip-audit` по тому, что поставляется. Драйвер JDBC сканируется командой `grype --fail-on medium`. Найденная проблема приводит к сбою задачи, и релиз ничего не публикует: загрузки в PyPI и релиз GitHub ожидают эти задачи. SBOM и отчёты о CVE прикладываются к каждому релизу.
- Установщики для настольных систем собирают wheel `provisa` из исходников на раннере релиза и разрешают его зависимости через `pip` во время сборки, а не из `uv.lock`; шлюз CVE релиза сканирует зафиксированный набор, а не зависимости, включённые в установщики.
- Одно документированное исключение игнорируется: PYSEC-2026-4114 / CVE-2026-49265 (PKCE oauthlib на стороне сервера; Provisa использует oauthlib только на стороне клиента).
- `packaging/pypi/requirements-embedded.lock` — это разрешение `provisa[embedded]` с фиксацией по хешам для зеркал в изолированных сетях (см. `packaging/pypi/MIRROR-CHECKLIST.md`). Он перегенерируется вручную, а не на каждый релиз.

## Обязательные секреты репозитория {: #required-repository-secrets }

Настройте их в **Settings → Secrets → Actions**:

| Секрет | Требуется для | Описание |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | Подписанные сборки | Файл сертификата `.p12` в кодировке Base64 (см. ниже) |
| `APPLE_CERT_P12_PASSWORD` | Подписанные сборки | Пароль, установленный при экспорте `.p12` из Keychain Access |
| `APPLE_DEVELOPER_ID` | Подписанные сборки | Полное имя сертификата: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Нотаризованные сборки | Email Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Нотаризованные сборки | Пароль для конкретного приложения с appleid.apple.com (не ваш пароль входа) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Нотаризованные сборки | 10-символьный Apple Team ID |

Сборки без этих секретов завершаются успешно, но производят неподписанный/ненотаризованный DMG (пользователи увидят предупреждение Gatekeeper).

## Экспорт сертификата .p12 {: #exporting-the-p12-certificate }

1. Откройте **Keychain Access** → login keychain → **My Certificates**
2. Найдите **Developer ID Application: Your Name (TEAMID)** — разверните, чтобы убедиться, что закрытый ключ вложен под ним
3. Выберите и сертификат, и его закрытый ключ → правая кнопка мыши → **Export 2 Items** → сохраните как `.p12` → задайте надёжный пароль
4. Закодируйте в Base64 и скопируйте в буфер обмена:

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. Вставьте как значение `APPLE_CERT_P12_BASE64`; установите `APPLE_CERT_P12_PASSWORD` в пароль из шага 3

## Поиск имени вашего сертификата {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Скопируйте полную строку в кавычках — это значение для `APPLE_DEVELOPER_ID`.

## Удаление неверного тега {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Затем удалите соответствующий GitHub Release в интерфейсе перед повторным тегированием.
