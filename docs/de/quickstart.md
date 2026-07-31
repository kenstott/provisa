# Schnellstart für Entwickler

Um Provisa zu evaluieren, ohne aus dem Quellcode zu kompilieren, siehe [Schnellstart](index.md) — laden Sie den Installer für macOS, Windows oder Linux herunter und führen Sie `provisa start` aus. (REQ-223, REQ-224, REQ-227)

Dieser Leitfaden richtet sich an das Ausführen von Provisa **aus dem Repository** — aktive Entwicklung, Debugging oder Beiträge.

---

## Voraussetzungen

- **Docker Desktop** (läuft)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Klonen und einrichten

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` erstellt `.venv/`, installiert alle Python-Abhängigkeiten über `pip install -e ".[dev]"` und konfiguriert die Git-Hooks in `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Alles starten

```bash
./start-ui.sh
```

Nach Abschluss des Starts sehen Sie:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Was gestartet wird:** [tool-verified: start-ui.sh]

- Docker-Compose-Kerndienste (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Docker-Compose-Dev-Overlay (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- Backend-API auf Port 8001 (Hot-Reload bei Änderungen an `provisa/` und `config/`) (REQ-618)
- Vite-UI-Dev-Server auf Port 3000 (HMR)
- OpenTelemetry-Tracing und Grafana unter `http://localhost:3100`. Der Observability-Stack ist ein optionales docker-compose-Profil `observability` (OTel Collector, Prometheus, Tempo, Grafana), das auf Plattformebene standardmäßig nicht aktiv ist; `start-ui.sh` aktiviert es als Dev-Skript-Komfortfunktion, sofern Sie nicht `--no-observability` übergeben. (REQ-302, REQ-303, REQ-330)

**Strg+C** stoppt alles — Backend, UI und alle Docker-Dienste — und macht alle Konfigurationspatches rückgängig. (REQ-619)

**Strg+R** startet nur das Backend neu (nützlich nach einer Konfigurationsänderung, die vom Hot-Reload nicht erfasst wird). (REQ-619)

### Optionen

`--no-observability` — Deaktiviert das verteilte Tracing. Standardmäßig lädt `start-ui.sh` den OpenTelemetry-Java-Agent herunter, falls noch nicht vorhanden, patcht Trinos `jvm.config`, um ihn zu laden, und startet den OTel Collector, Prometheus, Tempo und Grafana. Übergeben Sie `--no-observability`, um all dies zu überspringen. Der `jvm.config`-Patch wird bei Strg+C rückgängig gemacht. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Befüllt Kafka mit Demo-Daten, nachdem die Docker-Dienste fehlerfrei laufen. Standardmäßig nicht aktiv. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Lässt die Docker-Compose-Dienste nach Strg+C weiterlaufen, anstatt `docker compose down` aufzurufen. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Löscht alle Docker-Volumes und startet mit einem sauberen Zustand neu. Nützlich zur Wiederherstellung nach einem Docker-Absturz. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Startet zusätzliche Demo-Datenquellen (PostgreSQL-Pet-Store-Schema, OpenAPI-Petstore-Mock, SQLite und ein GraphQL-Remote). Befüllt automatisch Petstore-Benutzer und -Bestellungen. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Aktiviert einen Identity Provider für die Authentifizierung. Ohne dieses Flag läuft das Backend ohne Authentifizierungsanbieter, und alle Anfragen werden als `admin` behandelt. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Eine Datenquelle verbinden

Provisa liest die Konfiguration aus `config/`. Fügen Sie eine Quelldatei hinzu — zum Beispiel `config/sources/my-db.yaml`:

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

Setzen Sie die Umgebungsvariable, und das Backend übernimmt sie beim nächsten Reload:

```bash
export MY_DB_PASSWORD=secret
```

Die vollständige YAML-Referenz und alle unterstützten Quelltypen finden Sie unter [docs/configuration.md](configuration.md).

---

## 4. Ihre erste Abfrage ausführen

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

Es ist keine Authentifizierung erforderlich, wenn in `config/provisa.yaml` kein Abschnitt `auth` vorhanden ist (Standard in der Entwicklung). Die Standardrolle ist `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Die UI öffnen

Öffnen Sie `http://localhost:3000` in einem Browser.

Die Navigationsleiste hat vier Menüs der obersten Ebene: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Schema Explorer (`/schema`), GraphQL-Editor (`/query`), Cypher-Editor (`/graph`), SQL-Editor (`/sql`)
- **Model** — Sichten und Commands
- **Security** — Sicherheit auf Zeilenebene und Spaltenmaskierungsrichtlinien (REQ-038, REQ-041)
- **Admin** — Übersicht, Domänen, Cache, geplante Aufgaben, Systemzustand, Observability, Benutzer, Organisationen, Rollen

Die Admin-GraphQL-API befindet sich unter `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Fehlerbehebung

**Backend startet nicht** — prüfen Sie `.logs/server.log`. Häufigste Ursache ist eine fehlende Umgebungsvariable oder ein Portkonflikt auf 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Docker-Dienste nicht fehlerfrei** — führen Sie `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` aus, um zu sehen, welcher Dienst hängt. Die Federation Engine benötigt beim ersten Start ca. 30 Sekunden. (REQ-055)

**Portkonflikt auf 3000 oder 8001** — `start-ui.sh` beendet veraltete Prozesse auf diesen Ports vor dem Start. Wenn etwas anderes den Port belegt, beenden Sie es zuerst manuell. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Neustart von Grund auf** — stoppen Sie das Skript und führen Sie dann `./start-ui.sh --reset-volumes` aus, um alle Volumes zu löschen und neu zu starten. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Nächste Schritte

| Ziel | Dokument |
| ------ | ----- |
| Vollständige YAML-Konfigurationsreferenz | [configuration.md](configuration.md) |
| Sicherheit auf Zeilenebene, Spaltenmaskierung, Authentifizierung | [security.md](security.md) |
| Alle unterstützten Quelltypen | [sources.md](sources.md) |
| Echtzeit-Subscriptions | [subscriptions.md](subscriptions.md) |
| JDBC, BI-Tools, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Python-Client | [python-client.md](python-client.md) |
| Produktionsbereitstellung | [deployment.md](deployment.md) |
