# Schnellstart für Entwickler {: #developer-quick-start }

Um Provisa zu evaluieren, ohne aus dem Quellcode zu kompilieren, siehe [Schnellstart](index.md) — laden Sie den Installer für macOS, Windows oder Linux herunter und führen Sie `provisa start` aus.

Dieser Leitfaden richtet sich an das Ausführen von Provisa **aus dem Repository** — aktive Entwicklung, Debugging oder Beiträge.

---

## Voraussetzungen {: #prerequisites }

- **Docker Desktop** (läuft)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Klonen und einrichten {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` erstellt `.venv/`, installiert alle Python-Abhängigkeiten über `pip install -e ".[dev]"` und konfiguriert die Git-Hooks in `.githooks/`.

---

## 2. Alles starten {: #2-start-everything }

```bash
./start-ui.sh
```

Nach Abschluss des Starts sehen Sie:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Was gestartet wird:**

- Docker-Compose-Kerndienste (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis
- Docker-Compose-Dev-Overlay (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry
- Backend-API auf Port 8001 (Hot-Reload bei Änderungen an `provisa/` und `config/`)
- Vite-UI-Dev-Server auf Port 3000 (HMR)
- OpenTelemetry-Tracing und Grafana unter `http://localhost:3100`. Der Observability-Stack ist ein optionales docker-compose-Profil `observability` (OTel Collector, Prometheus, Tempo, Grafana), das auf Plattformebene standardmäßig nicht aktiv ist; `start-ui.sh` aktiviert es als Dev-Skript-Komfortfunktion, sofern Sie nicht `--no-observability` übergeben.

**Strg+C** stoppt alles — Backend, UI und alle Docker-Dienste — und macht alle Konfigurationspatches rückgängig.

**Strg+R** startet nur das Backend neu (nützlich nach einer Konfigurationsänderung, die vom Hot-Reload nicht erfasst wird).

### Optionen {: #options }

`--no-observability` — Deaktiviert das verteilte Tracing. Standardmäßig lädt `start-ui.sh` den OpenTelemetry-Java-Agent herunter, falls noch nicht vorhanden, patcht Trinos `jvm.config`, um ihn zu laden, und startet den OTel Collector, Prometheus, Tempo und Grafana. Übergeben Sie `--no-observability`, um all dies zu überspringen. Der `jvm.config`-Patch wird bei Strg+C rückgängig gemacht.

`--seed-data` — Befüllt Kafka mit Demo-Daten, nachdem die Docker-Dienste fehlerfrei laufen. Standardmäßig nicht aktiv.

`--keep-docker` — Lässt die Docker-Compose-Dienste nach Strg+C weiterlaufen, anstatt `docker compose down` aufzurufen.

`--reset-volumes` — Löscht alle Docker-Volumes und startet mit einem sauberen Zustand neu. Nützlich zur Wiederherstellung nach einem Docker-Absturz.

`--demo` — Startet zusätzliche Demo-Datenquellen (PostgreSQL-Pet-Store-Schema, OpenAPI-Petstore-Mock, SQLite und ein GraphQL-Remote). Befüllt automatisch Petstore-Benutzer und -Bestellungen.

`--source=<name>` (nur `start-ui-install.sh`, wiederholbar) — Stellt eine optionale Datenquelle zusätzlich zu `--demo` bereit. Jeder Name entspricht `demo/sources/<name>/`. Der Start ruft `demo/sources/provision.py up` auf, das die `compose.yml` der Quelle als eigenes Docker-Compose-Projekt (`provisa-demo-<name>`) startet, auf dessen Health-Check wartet und `prime.py` ausführt, sofern die Quelle eines hat, um Daten einzusäen. Der Start schreibt anschließend eine Wrapper-Konfiguration unter `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml`, die die Basiskonfiguration sowie das `fragment.yaml` jeder Quelle einbindet, und bootet daraus.

Dasselbe `provision.py` ruft auch die End-to-End-Testsuite der UI auf, um diese Quellen bereitzustellen (unter dem Projektpräfix `provisa-e2e-<name>` auf eigenen Ports), sodass die in der Demo gezeigten Seed-Daten und die von der Suite geprüften Datensätze einmalig definiert werden.

Bei einem Docker-Start (ohne `--demo`/`--native`) ist der Koordinator ein Container, sodass jede Quelle dem Netzwerk des Kern-Stacks beitritt und unter `<name>:<container port>` registriert wird; bei einem nativen Start erfolgt die Registrierung unter `localhost:<published port>`. Eine Quelle, deren Datei `demo/sources/<name>/engine` eine Engine benennt, die der Start nicht ausführt, wird abgelehnt.

Mitgelieferte Quellen:

| Name | Port(s) | Hinweise |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | Zwei Cypher-Tabellen (`adopter`, `adopter_referral`); Graph wird durch `seed.cypher` eingesät; Tabellen werden aus dem Fragment registriert |
| `mongodb` | 27117 | Quelle registriert; Collection `product_reviews` wird durch `db/mongo-init.js` eingesät; Tabellen manuell über Register Table registrieren |
| `redis` | 26379 | Quelle registriert; Hashes `support_agent:*` und `agent_status:*` werden durch `prime.py` eingesät; jedes Präfix wird über Register Table als Tabelle registriert |
| `cassandra` | 29042 | Quelle registriert; `shelter_ops.intake_events` wird durch `prime.py` eingesät (benötigt das `cassandra`-Extra); der Keyspace wird über Register Table als Schema registriert |
| `sparql` | 23030 | Apache Jena Fuseki; Quelle und eine abfragebasierte Tabelle (`volunteer`) werden aus dem Fragment registriert, Graph wird durch `prime.py` eingesät; weitere Tabellen über Register Table (Query + Preview) |
| `prometheus` | 29090 | Quelle registriert; der Server scraped sich selbst, sodass `up` und die `prometheus_*`-Metriken über Register Table als Tabellen registriert werden |
| `elasticsearch` | 29200 | Quelle und Index-Mapping registriert; Index `support_tickets` wird durch `prime.py` eingesät; wird von der nativen Engine über HTTP gelesen, über den Connector auf Trino |
| `splunk` | mgmt 8089, HEC 8088 | Quelle mit Token-Auth und `disable_ssl_validation` registriert (das Zertifikat des Containers ist selbstsigniert); ein Index, sieben Shelter-Alert-Ereignisse und das Data Model `shelter_alerts` werden durch `prime.py` eingesät, das auch das API-Token erzeugt, das das Fragment als `PROVISA_DEMO_SPLUNK_TOKEN` liest. Data Models werden über Register Table als Tabellen registriert: auf Trino über den `splunk`-Katalog, auf allen anderen Engines über den pgwire-Endpunkt des Splunk-Treibers, den die Engine anbindet |
| `chinook` | 25433 | Postgres mit der snake_case-Chinook-Teilmenge, die Hasuras Metadaten-Beispiel abbildet, eingesät durch `prime.py` aus `tests/fixtures/hasura_v2_t1_seed.sql`; Quelle aus dem Fragment registriert und die Quelle, auf der ein Hasura-v2-Import von `tests/fixtures/hasura_v2_t1_metadata.json` landet |

`--idp=basic|firebase` — Aktiviert einen Identity Provider für die Authentifizierung. Ohne dieses Flag läuft das Backend ohne Authentifizierungsanbieter, und alle Anfragen werden als `admin` behandelt.

---

## 3. Eine Datenquelle verbinden {: #3-connect-a-data-source }

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

## 4. Ihre erste Abfrage ausführen {: #4-run-your-first-query }

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

Es ist keine Authentifizierung erforderlich, wenn in `config/provisa.yaml` kein Abschnitt `auth` vorhanden ist (Standard in der Entwicklung). Die Standardrolle ist `admin`.

---

## 5. Die UI öffnen {: #5-open-the-ui }

Öffnen Sie `http://localhost:3000` in einem Browser.

Die Navigationsleiste hat vier Menüs der obersten Ebene:

- **Explore** — Schema Explorer (`/schema`), GraphQL-Editor (`/query`), Cypher-Editor (`/graph`), SQL-Editor (`/sql`)
- **Model** — Sichten und Commands
- **Security** — Sicherheit auf Zeilenebene und Spaltenmaskierungsrichtlinien
- **Admin** — Übersicht, Domänen, Cache, geplante Aufgaben, Systemzustand, Observability, Benutzer, Organisationen, Rollen

Die Admin-GraphQL-API befindet sich unter `http://localhost:8001/admin/graphql`.

---

## Fehlerbehebung {: #troubleshooting }

**Backend startet nicht** — prüfen Sie `.logs/server.log`. Häufigste Ursache ist eine fehlende Umgebungsvariable oder ein Portkonflikt auf 8001.

**Docker-Dienste nicht fehlerfrei** — führen Sie `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` aus, um zu sehen, welcher Dienst hängt. Die Federation Engine benötigt beim ersten Start ca. 30 Sekunden.

**Portkonflikt auf 3000 oder 8001** — `start-ui.sh` beendet veraltete Prozesse auf diesen Ports vor dem Start. Wenn etwas anderes den Port belegt, beenden Sie es zuerst manuell.

**Neustart von Grund auf** — stoppen Sie das Skript und führen Sie dann `./start-ui.sh --reset-volumes` aus, um alle Volumes zu löschen und neu zu starten.

---

## Nächste Schritte {: #next-steps }

| Ziel | Dokument |
| ------ | ----- |
| Vollständige YAML-Konfigurationsreferenz | [configuration.md](configuration.md) |
| Sicherheit auf Zeilenebene, Spaltenmaskierung, Authentifizierung | [security.md](security.md) |
| Alle unterstützten Quelltypen | [sources.md](sources.md) |
| Echtzeit-Subscriptions | [subscriptions.md](subscriptions.md) |
| JDBC, BI-Tools, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Python-Client | [python-client.md](python-client.md) |
| Produktionsbereitstellung | [deployment.md](deployment.md) |
