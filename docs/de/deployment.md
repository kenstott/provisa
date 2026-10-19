# Deployment

## Den richtigen Deployment-Pfad wählen {: #choosing-a-deployment-path }

Provisa unterstützt sechs Deployment-Pfade. Wählen Sie je nach Zielgruppe und operativem Kontext:

| Pfad | Artefakt / Skript | Am besten geeignet für |
| ------ | ------------------- | ---------- |
| **Entwicklung** | `start-ui.sh` | Entwicklung aus dem Quellcode, Evaluierung mit vollständigen Demo-Daten |
| **macOS-Installer** | `Provisa-<version>-macOS.dmg` | Entwickler-Workstations, Evaluierung |
| **Windows-Installer** | `Provisa-<version>-windows-x64.exe` | Entwickler-Workstations, Evaluierung |
| **Linux-AppImage** | `Provisa.AppImage` | On-Prem-Server, Cloud-VMs, Air-Gapped-Umgebungen |
| **Cloud-VMs (AWS)** | `terraform/deploy.sh` | Multi-Node-Cloud-Deployment mit Load Balancern |
| **Kubernetes** | `helm/provisa/` | Teams, die bereits K8s betreiben |

### VM vs. Kubernetes

Beide sind Enterprise-tauglich. Der VM/AppImage-Pfad ist einfacher: kein Cluster zu provisionieren, keine CNI- oder RBAC-Richtlinien zu konfigurieren, und das AppImage ist vollständig eigenständig. Es fügt sich nahtlos in bestehende Server-Management-Tools ein (Ansible, Puppet, Datadog-Agents, Splunk-Forwarder usw.).

Wählen Sie Kubernetes nur, wenn Ihr Team bereits einen K8s-Cluster betreibt und Provisa in diese Betriebsweise einfügen will (Rolling Deploys, HPA, einheitliche Observability). Beide bieten dieselben Fähigkeiten; Kubernetes bringt zusätzlichen Betriebsaufwand.

### Beschaffung der Images und Sicherheitsscans {: #image-acquisition-and-security-scanning }

Jeder Produktionsweg braucht die Provisa-Artefakte, bevor ein Deployment laufen kann. „Air-gapped“ beschreibt, was zur Installationszeit auf der Zielmaschine passiert; die Artefakte besorgen Sie also vorher.

**Installationsprogramme für macOS und Windows:** Laden Sie sie von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter. Sie sind vollständig gebündelt und brauchen nach dem Download kein Internet. Sie sind für Entwicklung und Evaluierung gedacht, daher wird keine Image-Scan-Schranke erwartet.

**AppImage-Weg:** Laden Sie es von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter und kopieren Sie es auf die Zielmaschine. Das AppImage bündelt jedes Komponenten-Image als Tarball in einem squashfs-Dateisystem, das die meisten Registry-Scanner nicht an Ort und Stelle prüfen können. Fragen Sie Ihr Provisa-Account-Team nach den Digests der Komponenten-Images, um sie selbst mit Ihrem Scanner abzugleichen.

**Terraform-Weg:** Laden Sie das AppImage nach S3 hoch, bevor Sie `terraform/deploy.sh` ausführen. Die EC2-Knoten laden es beim Start über ihre IAM-Rolle herunter und brauchen daher ausgehenden Zugriff auf S3, direkt oder über einen VPC-Gateway-Endpunkt. Wenden Sie dieselbe Scan-Richtlinie wie beim AppImage-Weg an.

**Helm-/Kubernetes-Weg:** Pushen Sie die einzelnen Images in eine Registry, die der Cluster erreicht. Dieser Weg passt am besten zu Registry-basiertem Scannen (Prisma Cloud, Aqua, Trivy, AWS Inspector), weil Scanner Images von Haus aus verstehen. Für einen abgeschotteten Cluster spiegeln Sie die Images in eine interne Registry und überschreiben die Verweise in `values.yaml`.

---

## Entwicklung (aus dem Quellcode) {: #development-from-source }

### Empfohlen: `start-ui.sh` {: #recommended-start-uish }

Der einfachste Weg, Provisa aus dem Quellcode heraus auszuführen. Startet die gesamte Infrastruktur, die Backend-API und den UI-Dev-Server mit einem einzigen Befehl. Strg+C fährt alles sauber herunter.

**Voraussetzungen:** Docker Desktop, Node.js, Python-Virtualenv unter `.venv/`

```bash
./start-ui.sh
```

Was es tut:

- Startet `docker-compose.core.yml` + `docker-compose.dev.yml` (alle Core- + Demo-Services) und wartet, bis sie healthy sind
- Befüllt Kafka mit Demo-Daten
- Synchronisiert die Python-Abhängigkeiten aus `.venv/`
- Startet die Backend-API auf Port 8001 (protokolliert nach `.logs/server.log`)
- Startet den Vite-UI-Dev-Server auf Port 3000
- Gibt URLs aus und wartet; Strg+C stoppt alles und fährt Compose herunter

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Optionen:**

`--reset-volumes` — Führt `docker compose down -v` vor dem Start aus und zerstört dabei alle Docker-Volumes (PostgreSQL-Daten, MinIO-Objekte, Redis-Zustand usw.). Verwenden Sie dies, wenn Sie einen vollständig sauberen Zustand wünschen: nach einer Schemaänderung während der Entwicklung oder wenn Docker abgestürzt ist und beschädigte Volumes hinterlassen hat. **Alle Daten gehen verloren.**

`--observability` — Fügt vollständige Tracing- und Metrics-Instrumentierung hinzu. Lädt den OpenTelemetry-Java-Agent herunter und patcht Trinos `jvm.config`, um ihn zu laden, instrumentiert das Provisa-Backend mit OTLP-Export und startet den OTel-Collector, Prometheus, Tempo und Grafana (`http://localhost:3100`). Der `jvm.config`-Patch wird bei Strg+C automatisch rückgängig gemacht.

### Manuelle Schritte (nur Backend, ohne UI) {: #manual-steps-backend-only-no-ui }

Wenn Sie nur die API benötigen:

1. Installieren Sie [Docker Desktop](https://docs.docker.com/get-docker/)
2. Starten Sie die Core-Services:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Starten Sie die API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Überprüfen: `curl http://localhost:8001/health`

### Vollständiger Stack (Provisa im Container) {: #full-stack-provisa-in-container }

Um die API als Container statt auf dem Host auszuführen:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Services

**Core (`docker-compose.core.yml`) — immer erforderlich:**

| Service | Port | Zweck |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Konfigurationsmetadaten + Iceberg-Katalog |
| PgBouncer | 6432 | Connection Pooling |
| Föderations-Engine | 8080 | Query-Föderation |
| Redis | 6379 | Cache für Abfrageergebnisse |
| MinIO | 9000/9001 | S3-kompatibler Objektspeicher |

**Demo (`docker-compose.dev.yml`) — optional, von `start-ui.sh` eingeschlossen:**

| Service | Port | Zweck |
| --------- | ------ | --------- |
| MongoDB | 27017 | Demo-NoSQL-Quelle |
| Kafka | 9092 | Demo-Streaming-Quelle |
| Schema Registry | 8081 | Demo-Verwaltung von Avro-/Protobuf-Schemas |
| Debezium | — | Demo-CDC-Connector |
| Elasticsearch | 9200 | Demo-Suchquelle |
| Neo4j | 7474/7687 | Demo-Graphquelle |
| Fuseki | 3030 | Demo-SPARQL-Triplestore |
| OpenTelemetry Collector | — | Trace-Erfassung (mit `--observability`) |
| Prometheus | 9090 | Metriken (mit `--observability`) |
| Tempo | — | Trace-Speicherung (mit `--observability`) |
| Grafana | 3100 | Dashboards (mit `--observability`) |

Die Stores, die Föderations-Engine und diese Telemetrie-Dienste veröffentlichen auf `PROVISA_STORE_BIND_IP` (`127.0.0.1`, sofern nicht gesetzt), mit für das Deployment generierten Passwörtern. Siehe [Deployment-Härtung](security.md#deployment-hardening).

### Telemetrie-Backend (`otlp2sql`) {: #telemetry-backend-otlp2sql }

Der oben beschriebene `--observability`-Stack (Collector → Tempo/Prometheus/Grafana) ist ein
Telemetrie-Pfad. Der andere ist `otlp2sql` (`provisa.observability.otlp2sql`): ein
OTLP/HTTP-Receiver, der Traces, Metriken und Protokolle in eine SQL-Datenbank schreibt,
die über eine SQLAlchemy-URL ausgewählt wird, wobei die `provisa.*`-Span-Attribute bereits bei
der Ingestion extrahiert werden, sodass kein separater Kompaktierungsjob läuft. Schreibvorgänge werden gebatcht
(`OTLP2SQL_BATCH_MAX_ROWS`, Standard 1000; `OTLP2SQL_BATCH_MAX_SECS`, Standard 2s).

Die Telemetrie erhält einen eigenen Speicher, getrennt von der Control-Plane-Datenbank. Wählen Sie
das Backend mit `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Backend | Hinweise |
| --- | --- | --- |
| *(nicht gesetzt)* | dediziertes DuckDB unter `~/.provisa/telemetry/` | Standard; kein Server, kein Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | hochfrequente Ingestion mit automatischen Hintergrund-Merges |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | moderates Volumen |
| `trino://user@host:8080/otel` | Trino / Iceberg | technisch funktionsfähig, **nicht empfohlen** — siehe unten |

**Mit `trino://`:** Der SQLAlchemy-Trino-Dialekt erzeugt gültiges Trino-DDL und `INSERT`s, kann also als `otlp2sql`-Backend dienen, wir empfehlen ihn aber nur für niedrige Aufnahmeraten. Jedes Leeren eines Stapels wird zu einem verteilten Trino-`INSERT` plus einem Iceberg-Snapshot, sodass Telemetrie mit hoher Rate viele kleine Dateien und Snapshots erzeugt und trotzdem ein regelmäßiges `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` braucht, das `otlp2sql` nicht ausführt. Außerdem liegt die Abfrage-Engine dann im heißen Pfad der Aufnahme.

Für hochvolumige Telemetrie nach Trino/Iceberg verwenden Sie stattdessen `otlp2parquet`: Es
schreibt Parquet in den Objektspeicher, ohne über Trino zu laufen, und eine geplante
Trino-Kompaktierung rollt die Rohdateien in die aktiven Iceberg-Tabellen ein. Für eine einzelne
Engine, die sowohl hochfrequente Ingestion als auch Kompaktierung übernimmt, bevorzugen Sie ClickHouse.

Richten Sie die OTLP-Exporter der App und von Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) auf den
`otlp2sql`-Endpunkt aus und registrieren Sie die Ops-Domain gegen dieselbe
`PROVISA_OPS_DB_URL`, damit sie liest, was der Receiver geschrieben hat.

---

## macOS-Installer

Für Entwickler-Arbeitsplätze und Evaluierung. Vollständig abgeschottet; nach dem Download ist kein Internet nötig.

Das Basis-Installationsprogramm ist eine **native Installation**: die DuckDB-Federation-Engine, eine eingebettete PostgreSQL-Steuerungsebene (pgserver) und ein In-Memory-Cache (fakeredis), ohne Docker, VM, Trino, Redis oder MinIO. Die Federation-Engine wählen Sie im Assistenten: DuckDB (nativ, Standard), Trino auf Docker oder eine externe Engine. Observability ist immer aktive Selbsttelemetrie, einsehbar unter Admin; der Docker-Stack aus Collector, Prometheus und Grafana ist eine getrennte, optionale Demonstration und schaltet sie weder ein noch aus. Das Demo-Datenpaket ist optional und standardmäßig aus. Trino, der Docker-Observability-Stack und die Demo sind schwere Zusatzpakete. Sie werden zuerst lokal gesucht (das Verzeichnis neben dem Installationsprogramm, eingebundene Volumes, `~/Downloads`) und erst danach aus dem GitHub-Release geholt, sodass Unternehmen die Tarballs für abgeschottete Installationen vorab bereitstellen können.

### Schritte {: #steps }

1. Laden Sie `Provisa-<version>-macOS.dmg` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter
2. Öffnen Sie das DMG und ziehen Sie **Provisa.app** nach `/Applications`
3. Doppelklicken Sie auf **Provisa.app**. Die Erstlaunch-Einrichtung läuft einmalig, und der Assistent bietet die oben genannten Wahlmöglichkeiten für Engine, Observability und Demo an
4. Öffnen Sie das Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

### Datenpersistenz {: #data-persistence }

Alle Daten werden in `~/.provisa/` gespeichert. Um alles zu entfernen: `provisa uninstall`.

---

## Windows-Installer

Für Entwickler-Arbeitsplätze und Evaluierung. Vollständig abgeschottet; nach dem Download ist kein Internet nötig.

Wie bei macOS ist der Basis-Windows-Installer eine **native Stufe**: eine eigenständige Python-Laufzeitumgebung + Provisa-Wheel + DuckDB/pg_duckdb + eingebettete PostgreSQL-Control-Plane (pgserver), ohne Docker, VM oder Container-Images. Die Föderations-Engine (Trino), der Observability-Stack und das Demo-Datenpaket werden später über separate, geschichtete Installer hinzugefügt, in dieser Reihenfolge: der Container-Installer (`Provisa-Container-<version>.exe`, der WSL2 + containerd + Trino hinzufügt), dann der Obs-Installer (erfordert die Container-Stufe), dann der Demo-Installer (erfordert Core + Obs). Die Erstlaunch-Anleitung erklärt, wie die Föderations-Engine durch Ausführen des Container-Installers initialisiert wird.

### Schritte {: #steps_1 }

1. Laden Sie `Provisa-<version>-windows-x64.exe` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter
2. Führen Sie den Installer aus. Er braucht keine Administratorrechte und installiert nach `%LOCALAPPDATA%\Programs\Provisa\`
3. Öffnen Sie **Provisa First Launch** über das Startmenü. Die native Einrichtung läuft einmalig und gibt die Anleitung für die nächsten Schritte zu den geschichteten Add-ons aus
4. Öffnen Sie ein neues Terminal:

   ```text
   provisa status
   provisa open
   ```

### Datenpersistenz {: #data-persistence_1 }

Alle Daten werden in `%USERPROFILE%\.provisa\` gespeichert.

---

## Linux-AppImage — Single- oder Multi-Node-VM {: #linux-appimage-single-or-multi-node-vm }

### Was es ist {: #what-it-is }

`Provisa.AppImage` ist eine einzelne eigenständige ausführbare Datei, die Folgendes bündelt:

- Einen rootlosen Docker-Daemon (`dockerd-rootless.sh` + `rootlesskit`), sodass weder System-Docker noch Root nötig sind
- Alle Container-Image-Tarballs (PostgreSQL, PgBouncer, MinIO, Redis, Föderations-Engine, Provisa-API)
- Den Provisa-CLI-Wrapper und das Erstlaunch-Einrichtungsskript

Das Provisa-Image wird beim Paketieren gebaut, und Python-Quellcode ist nie enthalten.

### Wann verwenden {: #when-to-use }

- On-Premises-Bare-Metal oder VM (Single-Node oder Multi-Node)
- Cloud-VMs ohne K8s-Cluster
- Air-Gapped-Umgebungen
- Wenn Sie einfacheren Betrieb als mit Kubernetes wünschen

---

### Schritte — Single Node {: #steps-single-node }

1. Laden Sie `Provisa.AppImage` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter und übertragen Sie es auf die Zielmaschine
2. Machen Sie es ausführbar:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Führen Sie die Erstlaunch-Einrichtung aus:

   ```bash
   ./Provisa.AppImage
   ```

4. Der Einrichtungsassistent fragt:
   - **Rolle** → wählen Sie `primary`
   - **RAM-Budget** → Menge des zuzuweisenden RAM (0 = gesamter verfügbarer); bestimmt die Anzahl der Trino-Worker
   - **Hostname** → die anzukündigende Adresse dieses Nodes
   - **API-Port** → Standard `8000`
5. Die Einrichtung lädt alle Container-Images (~2–5 Minuten), schreibt die Konfiguration und startet die Services
6. Überprüfen:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Schritte — Multi-Node (Primary) {: #steps-multi-node-primary }

Führen Sie diese Schritte zuerst auf dem primären Node aus. Sekundäre Nodes müssen eingerichtet werden, nachdem der primäre Node läuft.

1. Laden Sie `Provisa.AppImage` herunter und übertragen Sie es auf die primäre Maschine
2. Öffnen Sie die erforderlichen Firewall-Ports nur für die sekundären Nodes (sie verbinden sich eingehend auf diesen):

   | Port | Service |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Föderations-Engine-Koordinator |
   | 8000 | Provisa-API |

3. Machen Sie es ausführbar und führen Sie es mit den Stores aus, die auf der privaten Adresse des Nodes veröffentlicht werden, wo die sekundären Nodes sie ansprechen. Ein Node ohne sekundäre Nodes veröffentlicht sie nur auf `127.0.0.1`:

   ```bash
   chmod +x Provisa.AppImage
   PROVISA_STORE_BIND_IP=<this node's private IP> ./Provisa.AppImage
   ```

4. Der Einrichtungsassistent fragt:
   - **Rolle** → wählen Sie `primary`
   - **RAM-Budget**, **Hostname**, **API-Port** → antworten Sie wie bei Single Node
5. Notieren Sie sich nach Abschluss der Einrichtung die **private IP** dieser Maschine. Sekundäre Nodes benötigen sie sowie die Store-Anmeldedaten, die der primäre Node generiert hat: `~/.provisa/store-credentials.env` (`PROVISA_PG_PASSWORD`, `PROVISA_REDIS_PASSWORD`, `PROVISA_MINIO_ROOT_PASSWORD`, `PROVISA_GRAFANA_ADMIN_PASSWORD`, `PROVISA_TRINO_SHARED_SECRET`). Kopieren Sie diese Datei über einen sicheren Kanal auf jeden sekundären Node
6. Der Assistent gibt einen nginx-Upstream-Block aus; speichern Sie ihn für Ihre Load-Balancer-Konfiguration
7. Überprüfen:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Schritte — Multi-Node (jeder Secondary) {: #steps-multi-node-each-secondary }

Wiederholen Sie diese Schritte auf jedem zusätzlichen Node, nachdem der primäre Node läuft und erreichbar ist.

1. Laden Sie `Provisa.AppImage` herunter und übertragen Sie es auf die sekundäre Maschine
2. Bestätigen Sie, dass der sekundäre Node den primären Node erreichen kann:

   ```bash
   curl -k https://<primary-ip>:8000/health
   ```

3. Machen Sie es ausführbar und führen Sie es mit den Store-Anmeldedaten des primären Nodes in seiner Umgebung aus (die vom primären Node kopierte Datei). Ein sekundärer Node ohne diese startet nicht:

   ```bash
   chmod +x Provisa.AppImage
   set -a; . ./store-credentials.env; set +a
   ./Provisa.AppImage
   ```

4. Der Einrichtungsassistent fragt:
   - **Rolle** → wählen Sie `secondary`
   - **Primary-IP** → geben Sie die IP des primären Nodes ein (die Konnektivität wird live überprüft)
   - **RAM-Budget**, **Hostname**, **API-Port** → antworten Sie wie oben
5. Die Einrichtung lädt einen reduzierten Image-Satz (ohne PostgreSQL, PgBouncer, MinIO und Redis, die nur auf dem primären Node laufen), startet die Provisa-API und einen Föderations-Engine-Worker
6. Überprüfen:

   ```bash
   provisa status
   curl -k https://localhost:8000/health
   ```

7. Fügen Sie diesen Node zum Upstream Ihres Load Balancers hinzu

---

### Primary-/Secondary-Topologie {: #primary-secondary-topology }

**Der primäre Node** führt alle Singleton-Services aus:

| Service | Warum Singleton |
| --------- | --------------- |
| PostgreSQL | Gemeinsames Schema, App-Konfiguration, semantisches Modell |
| Redis | Gemeinsamer Cache für Abfrageergebnisse und Subscription-Zustand |
| MinIO | Gemeinsamer Objektspeicher für Redirect-Ergebnisse und Snapshots materialisierter Sichten |
| Föderations-Engine-Koordinator | Alle Worker (primär + sekundär) registrieren sich hier |

**Die sekundären Nodes** führen nur aus:

- Provisa-API — zustandslos; liest bei jedem Start die gesamte Konfiguration von PostgreSQL auf dem primären Node
- Föderations-Engine-Worker — registriert sich selbst beim Koordinator auf dem primären Node

Der gesamte Anwendungszustand fließt über das PostgreSQL des primären Nodes. Keine manuelle Synchronisierung erforderlich.

---

### Nicht-interaktiver (automatisierter) Erstlaunch {: #non-interactive-automated-first-launch }

Für Terraform, Cloud-Init oder Ansible übergeben Sie Flags, anstatt Eingabeaufforderungen zu beantworten:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Der nicht-interaktive Modus installiert eine systemd-Unit (`/etc/systemd/system/provisa.service`) für den Start beim Booten.

| Flag | Beschreibung |
| ------ | ------------- |
| `--non-interactive` | Überspringt alle Eingabeaufforderungen; installiert die systemd-Unit |
| `--role primary\|secondary` | Node-Rolle |
| `--primary-ip <ip>` | IP des primären Nodes (erforderlich für Secondary) |
| `--ram-gb <n>` | Zuzuweisender RAM (0 = gesamter verfügbarer) |

---

## Cloud-VM-Deployment — Terraform (AWS)

Ein interaktiver Befehl stellt einen vollständigen Provisa-Cluster mit mehreren Knoten auf AWS bereit: VPC, Sicherheitsgruppen, EC2-Instanzen, ALB und NLB.

### Dateien {: #files }

| Datei | Zweck |
| ------ | --------- |
| `terraform/deploy.sh` | Interaktiver Wrapper — sammelt Parameter, validiert Anmeldedaten, schreibt `terraform.tfvars`, führt Apply aus |
| `terraform/aws/variables.tf` | Alle Variablendefinitionen mit Standardwerten |
| `terraform/aws/main.tf` | VPC, Subnetze, Security Groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | Endpunkt-URLs und Node-IPs |

### Schritte {: #steps_2 }

1. Laden Sie `Provisa.AppImage` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter

2. Laden Sie es in einen S3-Bucket in Ihrem AWS-Konto hoch:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Stellen Sie sicher, dass AWS-Anmeldedaten in Ihrer Shell verfügbar sind (eine der folgenden Optionen):
   - Umgebungsvariablen: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Benanntes Profil: `export AWS_PROFILE=my-profile`
   - Aktive SSO-Sitzung: `aws sso login`

4. (Optional) Wenn Sie SSH-Zugriff auf die Nodes wünschen, erstellen Sie ein EC2-Schlüsselpaar in Ihrer Zielregion und notieren Sie sich den Namen des Schlüsselpaars

5. Führen Sie den Deploy-Wrapper aus:

   ```bash
   bash terraform/deploy.sh
   ```

6. Beantworten Sie die Fragen des Assistenten (siehe Referenztabelle unten). Das Skript überprüft, ob das AppImage in S3 vorhanden ist, bevor es fortfährt, und bricht ab, wenn dies nicht der Fall ist

7. Überprüfen Sie die Deployment-Zusammenfassung und bestätigen Sie

8. Terraform provisioniert die gesamte Infrastruktur (~5–10 Minuten). Nach dem Apply gibt das Skript aus:

   ```text
   api_endpoint      = "https://<alb-dns>:8000"
   protocol_endpoints = { flight = "<alb-dns>:8815", ... }
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

9. (Optional) Richten Sie DNS-Einträge auf den ALB- und NLB-DNS-Namen aus

10. Überprüfen:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Fragen des Assistenten {: #wizard-questions }

| Frage | Standard | Hinweise |
| ---------- | --------- | ------- |
| Cloud-Anbieter | — | Nur AWS |
| AWS-Anmeldedaten | — | Prüft zuerst auf eine aktive Sitzung |
| Region | `us-east-1` | |
| Node-Anzahl | `2` | 1 = nur primär, kein LB; 2+ = primär + sekundär + ALB/NLB |
| Instance-Typ | `m7i.2xlarge` | Siehe Sizing-Leitfaden unten |
| Größe des Root-Volumes | `100 GB` | Pro Node |
| RAM-Budget | `0` (gesamter RAM) | Bestimmt die Anzahl der Trino-Worker pro Node |
| S3-Bucket | — | Wird live überprüft, bevor fortgefahren wird |
| S3-Key | `releases/Provisa.AppImage` | |
| SSH-Zugriff | Nein | Erfordert einen vorhandenen Schlüsselpaar-Namen + Admin-CIDR |
| VPC-CIDR | `10.0.0.0/16` | |

### Sizing-Leitfaden für Instanzen {: #instance-sizing-guide }

| Typ | vCPU | RAM | Trino-Worker/Node | Anwendungsfall |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Entwicklung / kleine Datensätze |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Kleine Produktion |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Mittlere Produktion |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Große Produktion |

Alle Nodes tragen Worker zu einem einzigen Koordinator auf dem primären Node bei. Ein 3-Node-Cluster mit `m7i.4xlarge` liefert insgesamt 6 Trino-Worker.

### Was provisioniert wird {: #what-gets-provisioned }

- VPC mit zwei öffentlichen Subnetzen über zwei Availability Zones
- Security Groups: LB-Gruppe (öffentlicher Ingress auf 8000/8815), Nodes-Gruppe (LB → Nodes, Intra-Cluster, optionales SSH)
- IAM-Rolle + Instance-Profil mit S3-GetObject auf den AppImage-Bucket
- Primäre EC2-Instanz — führt den Erstlaunch im Modus `--non-interactive --role primary` aus
- Sekundäre EC2-Instanzen (node_count − 1) — führen den Erstlaunch im Modus `--non-interactive --role secondary --primary-ip <primary private IP>` aus; hängen davon ab, dass der primäre Node zuerst fertig wird
- ALB auf Port 8000 — HTTP-API, Health-Check `/health`
- NLB auf Port 8815 — Arrow Flight / gRPC
- Beide LBs sind an alle Nodes angeschlossen

### Checkliste der Voraussetzungen {: #prerequisites-checklist }

- [ ] IAM-Berechtigungen: EC2 vollständig, ELB vollständig, VPC vollständig, IAM-Rollenerstellung, S3-GetObject auf den AppImage-Bucket
- [ ] `Provisa.AppImage` nach S3 hochgeladen
- [ ] EC2-Nodes haben ausgehenden S3-Zugriff (direktes Internet oder S3-VPC-Gateway-Endpoint)
- [ ] EC2-Schlüsselpaar existiert in der Zielregion (falls SSH benötigt wird)
- [ ] Terraform ≥ 1.5 lokal installiert
- [ ] DNS-Einträge für ALB / NLB geplant (optional, aber empfohlen)
- [ ] ACM-Zertifikat bereit, falls HTTPS erforderlich ist (nicht im Basis-Terraform enthalten)

### Secrets

Die Passwörter der gebündelten Stores (PostgreSQL, Redis, MinIO, Admin von Grafana) und das gemeinsame Secret des Trino-Clusters werden von Terraform generiert (`random_password`) und an jeden Node gegeben, der sie in seiner systemd-Umgebungsdatei mit Modus 600 hält. Sie liegen im Terraform-State, schützen Sie den State daher wie die Passwörter. Ohne Terraform generiert der Launcher jedes Nodes sie in `~/.provisa/store-credentials.env`. Das AppImage generiert die übrigen Anmeldedaten während des Erstlaunchs und schreibt sie auf jedem Node in `~/.provisa/config.yaml`. Für die Produktion rufen Sie das Admin-Token nach dem Deployment vom primären Node ab:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Wann verwenden {: #when-to-use_1 }

Ihr Team betreibt bereits einen Kubernetes-Cluster und möchte, dass Provisa an diesem operativen Modell teilnimmt. Wenn Sie Provisa evaluieren oder on-premises ohne bestehenden Cluster deployen, ist der AppImage-Pfad einfacher.

Hinweis: Das Provisa-AppImage läuft nicht in einem Kubernetes-Pod. Es braucht FUSE und einen rootlosen Docker-Daemon, und die üblichen Pod-Sicherheitsprofile erlauben beides nicht.

### Schritte {: #steps_3 }

1. Bestätigen Sie den Cluster-Zugriff:

   ```bash
   kubectl cluster-info
   ```

2. Ziehen Sie die Images und spiegeln Sie sie in Ihre interne Registry (erforderlich für Air-Gapped- oder gescannte Umgebungen; überspringen, wenn direkt aus öffentlichen Registries gezogen wird):

   | Image | Verwendung für |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa-API |
   | `trinodb/trino:480` | Föderations-Engine-Koordinator + Worker |
   | `postgres:16` | PostgreSQL im Cluster (falls `postgresql.enabled`) |
   | `edoburu/pgbouncer:latest` | PgBouncer im Cluster (falls `pgbouncer.enabled`) |
   | `redis:7.2` | Redis im Cluster (falls `redis.enabled` und kein `redis.host`) |
   | `minio/minio:latest` | MinIO im Cluster (falls `minio.enabled`) |

   Für Umgebungen mit Registry-Scanning:
   - Pushen Sie jedes Image in Ihre Staging-Registry
   - Führen Sie Ihren Scanner aus (Prisma Cloud, Aqua, Trivy, AWS Inspector) und holen Sie die Freigabe ein
   - Befördern Sie es in Ihre interne Produktions-Registry

3. Entscheiden Sie vor der Installation:
   - **PostgreSQL** — im Cluster (`postgresql.enabled: true`) oder extern verwaltet (`postgresql.host`)? Für Produktion wird extern empfohlen
   - **Redis** — im Cluster oder extern (`redis.host`)?
   - **Store-Anmeldedaten** — erstellen Sie das Secret, das `credentials.existingSecret` benennt, mit den Schlüsseln `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` und `trino-shared-secret`. Das Chart setzt kein eigenes Passwort und wird ohne dieses nicht gerendert
   - **TLS** — standardmäßig an (`tls.enabled: true`): erstellen Sie das Secret `kubernetes.io/tls`, das `tls.secretName` benennt, oder setzen Sie `tls.enabled: false`
   - **MinIO / S3** — MinIO im Cluster oder natives S3? Für AWS verwenden Sie S3 mit einer IAM-Rolle
   - **Secrets** — für die Evaluierung über `--set` übergeben; für Produktion External Secrets oder Vault Agent verwenden
   - **Trino-Coordinator-Cache** — ein 1Gi-Volume, standardmäßig angefordert und unter `/data/trino/cache` eingehängt (`trino.coordinator.cache.persistence`: `enabled`, `size`, `storageClass`). Dort werden die Describe-Ergebnisse einer Salesforce-Quelle gehalten, damit ein neu gestarteter Coordinator nicht erneut das API-Kontingent der Org für deren Beschreibung verbraucht. Schalten Sie ihn nur aus, wenn keine Quelle ihn braucht

4. Geben Sie dem Deployment seinen Master-Key. Der Key verschlüsselt den Secrets-Vault; jeder API-Pod muss
   denselben halten, und er muss jeden Pod überdauern. Ohne ihn rendert das Chart nicht.

   ```bash
   kubectl create namespace provisa
   kubectl create secret generic provisa-master-key --namespace provisa \
     --from-literal=master-key="$(openssl rand -base64 32)"
   ```

   Bewahren Sie eine Kopie des Keys außerhalb des Clusters auf. Ein Vault, der unter einem später verlorenen Key
   geschrieben wurde, lässt sich nicht öffnen. Die Alternative zu einem Secret ist ein persistentes Datenvolume
   (`encryption.dataVolume.enabled=true`), auf dem das Produkt den Key hält, den es erzeugt; siehe
   `encryption` in `values.yaml`.

5. Installieren Sie das Chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set encryption.existingSecret=provisa-master-key \
     --set auth.provider=oidc \
     --set auth.oidc.issuerUrl=https://login.example.com/realms/main \
     --set auth.oidc.clientId=provisa \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Das Chart rendert erst, wenn `auth.provider` gewählt ist: `oidc` (ein OpenID-Connect-Issuer des Unternehmens),
   `saml` (`auth.saml.idpMetadataUrl` und `auth.saml.publicUrl`), `ldap` (`auth.ldap.*`),
   `local` (nur das Break-Glass-Konto, `auth.breakGlass.*`) oder `none` (keine Authentifizierung,
   nur für die Entwicklung). Jede Anmeldedate kommt aus einem Kubernetes-Secret; `ldap`, `saml` und `local`
   brauchen außerdem `auth.sessionSecret.existingSecret`, den Key, der Browser-Sitzungen signiert. Siehe `auth`
   in `values.yaml`.

   Der Trino-Coordinator führt die Anweisungen jeder Org in deren eigener Ressourcengruppe aus,
   `global.tenant-<org>`, aus der Ressourcengruppen-Datei, die das Chart mitliefert. Anweisungen über den
   Arrow-Flight-Proxy teilen sich eine Gruppe, `global.flight`: Der Proxy öffnet seine eigene Trino-Verbindung
   und kann die Org nicht mitführen.

   Bei Verwendung einer internen Registry fügen Sie Image-Overrides hinzu:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

6. Überprüfen Sie, ob die Pods laufen:

   ```bash
   kubectl get pods -n provisa
   ```

7. Überprüfen Sie die API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

8. (Optional) Aktivieren Sie Ingress für externen Zugriff: Setzen Sie `ingress.enabled: true` und konfigurieren Sie Ihren Ingress-Controller

### Checkliste der Voraussetzungen {: #prerequisites-checklist_1 }

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Storage Class, die `ReadWriteOnce`-PVCs unterstützt (für zustandsbehaftete Services im Cluster)
- [ ] Images für den Cluster verfügbar (öffentliche oder interne Registry)
- [ ] PostgreSQL-Endpunkt + Anmeldedaten (falls extern)
- [ ] Redis-Endpunkt + Anmeldedaten (falls extern)
- [ ] S3-Bucket + Anmeldedaten oder IAM-Rolle
- [ ] Admin-Token gewählt
- [ ] Ingress-Controller konfiguriert (falls externer Zugriff benötigt wird)

### Wichtige Werte {: #key-values }

| Wert | Standard | Beschreibung |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Replikate der Provisa-API (zustandslos) |
| `config.pgHost` | `postgres` | PostgreSQL-Host |
| `config.pgPassword` | | PostgreSQL-Passwort |
| `config.adminToken` | | Bearer-Token für die Admin-API |
| `redis.enabled` | `true` | Deployt ein Redis-StatefulSet im Cluster |
| `redis.host` | `""` | Setzen, um externes Redis zu verwenden |
| `redis.port` | `6379` | |
| `credentials.existingSecret` | | Secret mit den Passwörtern der Stores (erforderlich) |
| `tls.enabled` | `true` | Jeden Endpunkt per TLS aus `tls.secretName` ausliefern |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Deployt die Föderations-Engine |
| `trino.workers` | `2` | Replikate der Föderations-Engine-Worker |
| `trino.internalTls` | `false` | Koordinator-Worker-Datenverkehr mit einem vom Chart erzeugten Zertifikat verschlüsseln; verlangsamt Shuffle-lastige Abfragen um 10 % bis über 100 % |
| `postgresql.enabled` | `true` | Deployt PostgreSQL im Cluster |
| `postgresql.host` | `""` | Setzen, um externes PostgreSQL zu verwenden |
| `minio.enabled` | `true` | Deployt MinIO im Cluster |
| `s3.endpoint` | | S3-kompatible Endpunkt-URL |
| `s3.bucket` | `provisa-results` | Bucket für Redirect großer Ergebnisse |
| `ingress.enabled` | `false` | Aktiviert Ingress |

### Skalierung {: #scaling }

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Die Worker der Federation-Engine skalieren unabhängig, und mehr Worker bedeuten mehr Durchsatz und mehr gleichzeitige Abfragen.

### Konfiguration aktualisieren {: #updating-config }

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Hochverfügbarkeit & Recovery {: #high-availability-recovery }

Provisa wendet über alle Deployment-Modi hinweg ein zweistufiges Recovery-Modell an:

- **Stufe 1 — transiente Fehler.** Leseoperationen werden bei transienten Fehlern bis zu 30 Sekunden lang mit exponentiellem Backoff und vollem Jitter wiederholt. Passen Sie das Budget mit `PROVISA_RETRY_BUDGET_SECS` an. Schreiboperationen werden intern niemals wiederholt, und Speicherfehler sind niemals wiederholbar.
- **Stufe 2 — Komponentenausfall.** Ein interner Engine-Watcher erkennt fehlgeschlagene Softwarekomponenten und startet sie innerhalb von 2–3 Minuten neu.

Ausfälle von Maschinen und Clustern bleiben in der Verantwortung des Betreibers. Damit der Verlust eines Knotens verkraftet wird, stellen Sie redundante Knoten und einen Load Balancer bereit (die Wege Terraform und Helm oben).

### Telemetrietabelle, deren Metadaten fehlen {: #telemetry-table-whose-metadata-is-missing }

Auf der Trino-Engine sind die Telemetrietabellen (`otel.signals.traces`, `metrics`, `logs`) Iceberg-Tabellen. Die Registrierung jeder Tabelle ist eine Zeile in der Datenbank der Steuerungsebene, und ihre Metadaten und Daten sind Dateien im Telemetrie-Bucket. Wenn der Bucket ohne diese Dateien bereitgestellt wird, während die Datenbank die Registrierungen noch hält, schlägt jede Anweisung auf der Tabelle mit `ICEBERG_MISSING_METADATA` fehl. Das geschieht, wenn die Festplatte des Buckets beim Start des Objektspeichers noch nicht eingehängt war oder wenn der Bucket geleert oder ersetzt wurde.

Provisa ändert in diesem Fall nichts. Jeder Kompaktierungslauf protokolliert einen `ERROR`, der die Tabelle und den Metadatenort nennt, den Trino nicht lesen konnte (`otel.signals.traces is registered but its metadata cannot be read …`), überspringt dieses Signal und behält dessen Rohdateien im Bucket für einen späteren Lauf. Die anderen Signale werden wie üblich kompaktiert. Der Start lässt die Tabelle ebenso aus seinen Schemaprüfungen heraus.

Stellen Sie sie von Hand wieder her, in dieser Reihenfolge:

1. **Ermitteln Sie, ob die Metadaten noch existieren.** Der `ERROR` nennt den Ort. Prüfen Sie auf einem Terraform-Koordinator (GCP), dass die Festplatte des Objektspeichers eingehängt ist (`findmnt /var/lib/provisa-object-store`) und dass der MinIO-Container nach dem Einhängen gestartet wurde. Ein zuerst gestartetes MinIO stellt das leere Verzeichnis unter dem Einhängepunkt bereit; starten Sie es neu, sobald die Festplatte eingehängt ist, und die Tabelle ist mit ihrer Historie wieder lesbar.
2. **Wenn die Metadatendatei existiert, die Registrierung aber woandershin zeigt**, registrieren Sie die Tabelle erneut an ihrem Ort:

   ```sql
   CALL otel.system.register_table(schema_name => 'signals', table_name => 'traces', table_location => 's3://<bucket>/warehouse/signals/<table directory>')
   ```

   Heben Sie zuerst die veraltete Registrierung auf, wenn der Name vergeben ist (`CALL otel.system.unregister_table(schema_name => 'signals', table_name => 'traces')`).
3. **Wenn die Dateien des Buckets tatsächlich verloren sind**, heben Sie die Registrierung der Tabelle auf. Der nächste Kompaktierungslauf legt sie leer neu an. Die Signale, die sie hielt, können nicht wieder gelesen werden.

Führen Sie `remove_orphan_files` nicht auf einer Tabelle aus, die wiederhergestellt wird. Es löscht Dateien, auf die die aktuellen Metadaten nicht verweisen, und das kann die Historie sein, die Sie wiederherstellen.

## Abhängigkeiten der Föderations-Engine {: #federation-engine-dependencies }

Die Warehouse-Föderations-Engines brauchen Python-Pakete und Komponenten auf Systemebene. Die Python-Pakete sind in `pyproject.toml` deklariert und kommen mit dem normalen `pip install provisa` oder `pip install -e .`, also braucht keine Warehouse-Engine ein optionales Extra.

Die Dinge auf Systemebene (der ODBC-Treiber, Cloud-CLIs, Dienstkonto-Schlüssel) müssen Sie getrennt installieren.

### Python-Pakete (bereits in den Core-Abhängigkeiten) {: #python-packages-already-in-core-dependencies }

| Paket | Engine | Zweck |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | SQL-Warehouse-Verbindung; Arrow Cloud Fetch |
| `snowflake-connector-python[pandas]` | Snowflake | Verbindung + Arrow-natives `fetch_arrow_table` |
| `google-cloud-bigquery` | BigQuery | Query-Ausführung |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API für Arrow-native Reads |
| `google-cloud-storage` | BigQuery | GCS-Staging für externe Tabellenverknüpfungen |
| `pyodbc` | Fabric, Synapse | ODBC-Verbindung zu T-SQL-Endpunkten |
| `azure-identity` | Fabric, Synapse | Azure-AD-Token über `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | HTTP-Columnar-Reads |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Kompatibilitäts-Pin — `google-cloud-*` und OTel teilen sich eine Protobuf-Laufzeitumgebung; `<7` hält sie aufeinander abgestimmt |
| `grpcio-status<1.82` | gRPC | Passt sich an den `protobuf<7`-Pin an |

### Anforderungen auf Systemebene {: #system-level-requirements }

Das sind keine Python-Pakete. Installieren Sie sie auf dem Host oder Container, auf dem Provisa läuft.

**Microsoft Fabric und Azure Synapse (ODBC)**

`pyodbc` verbindet sich über den Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Installieren Sie den Treiber direkt auf dem Host; pip kann ihn nicht installieren.

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

Provisa erkennt den Treiber automatisch. Um den Treibernamen zu überschreiben (bei nicht standardmäßigen Installationen), setzen Sie:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Azure-AD-Authentifizierung (Fabric und Synapse)**

Beide Engines authentifizieren sich über `azure.identity.DefaultAzureCredential`. `DefaultAzureCredential` prüft die Anmeldedatenquellen in dieser Reihenfolge: Umgebungsvariablen, Workload-Identität, Managed Identity, VS Code, `az login` und weitere.

Für die lokale Entwicklung ist `az login` der einfachste Weg:

```bash
az login
```

In der Produktion nutzen Sie eine verwaltete Identität (auf Azure-VMs oder AKS), die keine Verwaltung von Anmeldedaten braucht. Für die Anmeldung per Dienstprinzipal setzen Sie:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (Service Account)**

`google-cloud-bigquery` verwendet Application Default Credentials. Für die lokale Entwicklung verweisen Sie auf eine Service-Account-Schlüsseldatei:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

In der Produktion auf GCP (Cloud Run, GKE mit Workload Identity, Compute Engine) übernimmt die Bibliothek das angehängte Dienstkonto selbst, ohne Umgebungsvariable.

Das Service Account benötigt:

- `roles/bigquery.dataViewer` — Daten lesen
- `roles/bigquery.jobUser` — Abfragen ausführen
- `roles/bigquery.dataEditor` — externe Tabellen erstellen (für ATTACH)
- `roles/storage.objectViewer` — GCS-Objekte für externe Tabellen lesen

**Databricks (CA-Zertifikat in Dev-Proxy-Umgebungen)**

Wenn Provisa hinter einem TLS-abfangenden Proxy läuft (Charles, mitmproxy, Unternehmensproxys), kann der Databricks-SQL-Connector das Zertifikat des Proxys ablehnen. Übergeben Sie ein benutzerdefiniertes CA-Bundle:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Der Databricks-Connector übernimmt das von `requests` und braucht daher keine Databricks-spezifische Umgebungsvariable.

### Checkliste pro Engine {: #per-engine-checklist }

**Databricks**

- [ ] `databricks-sql-connector` installiert (Standard)
- [ ] Engine-URL mit `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Personal Access Token oder Service-Principal-Token
- [ ] `REQUESTS_CA_BUNDLE` gesetzt, falls hinter einem TLS-abfangenden Proxy

**Snowflake**

- [ ] `snowflake-connector-python[pandas]` installiert (Standard)
- [ ] Engine-URL: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` in `PROVISA_ENGINE_URL` oder `federation_hints`

**BigQuery**

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installiert (Standard)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` gesetzt (Dev) oder Workload Identity konfiguriert (Prod)
- [ ] `GOOGLE_CLOUD_PROJECT` gesetzt, falls das Projekt nicht aus dem Service Account abgeleitet werden kann
- [ ] Service Account hat die Rollen BigQuery Data Viewer + Job User

**Microsoft Fabric**

- [ ] `pyodbc` + `azure-identity` installiert (Standard)
- [ ] Systemtreiber `msodbcsql18` installiert
- [ ] `FABRIC_SQL_SERVER` und `FABRIC_DATABASE` gesetzt
- [ ] Azure-AD-Authentifizierung: `az login` (Dev) oder Managed Identity / Service Principal (Prod)
- [ ] `FABRIC_WORKSPACE_ID` gesetzt, falls externe Objektspeicher-Verknüpfungen verwendet werden

**Azure Synapse**

- [ ] Gleiche Python- und Systemanforderungen wie Fabric
- [ ] `SYNAPSE_SQL_SERVER` und `SYNAPSE_DATABASE` gesetzt
- [ ] Gleiche Azure-AD-Authentifizierungseinrichtung wie Fabric

**ClickHouse**

- [ ] `clickhouse-connect` installiert (Standard)
- [ ] Engine-URL: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` in `federation_hints` für TLS (Port 8443)

---

## Umgebungsvariablen {: #environment-variables }

| Variable | Standard | Zweck |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | PostgreSQL-Passwort |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Pfad zur Konfigurationsdatei |
| `PROVISA_REDIRECT_ENABLED` | `false` | Aktiviert das Redirect großer Ergebnisse nach S3 |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Schwellenwert für die Zeilenanzahl beim Redirect |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3-Bucket |
| `PROVISA_REDIRECT_ENDPOINT` | | S3-kompatible Endpunkt-URL |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL der Presigned-URL (Sekunden) |
| `REDIS_HOST` | `localhost` | Redis-Host |
| `REDIS_PORT` | `6379` | Redis-Port |
| `REDIS_PASSWORD` | | Redis-Passwort |
| `REDIS_TLS` | `false` | Aktiviert TLS für Redis |
| `TRINO_HOST` | `localhost` | Host des Trino-Föderations-Engine-Koordinators |
| `TRINO_PORT` | `8080` | HTTP-Port des Trino-Föderations-Engine-Koordinators |
| `PROVISA_ENGINE` | `duckdb` | Schlüssel der aktiven Föderations-Engine; überschreibt die persistierte Konfiguration |
| `PROVISA_ENGINE_URL` | | Verbindungs-URL für URL-gesteuerte Engines (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Override der Materialisierungs-Store-URL; standardmäßig der eigene Store der Engine |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC-Treibername für Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Pfad zum GCP-Service-Account-Schlüssel-JSON (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | GCP-Projekt-ID (BigQuery; wird aus dem Service Account abgeleitet, falls nicht gesetzt) |
| `FABRIC_SQL_SERVER` | | Hostname des SQL-Analytics-Endpunkts von Microsoft Fabric |
| `FABRIC_DATABASE` | | Name der Fabric-Datenbank |
| `FABRIC_WORKSPACE_ID` | | GUID des Fabric-Workspace (erforderlich für externe Objektspeicher-Shortcuts) |
| `SYNAPSE_SQL_SERVER` | | Hostname des dedizierten SQL-Pools oder Serverless-Pools von Azure Synapse |
| `SYNAPSE_DATABASE` | | Name der Synapse-Datenbank |
| `AZURE_TENANT_ID` | | Azure-AD-Tenant (Service-Principal-Authentifizierung für Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | Client-ID der Azure-AD-Anwendung |
| `AZURE_CLIENT_SECRET` | | Client-Secret der Azure-AD-Anwendung |
| `REQUESTS_CA_BUNDLE` | | Pfad zum benutzerdefinierten CA-Bundle (Databricks-Connector, Dev-TLS-Proxy) |

---

## CLI-Befehle {: #cli-commands }

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

### Workflow zur Konfigurations-Promotion (Dev → Test → Prod) {: #config-promotion-workflow-dev-test-prod }

Jede umgebungsspezifische Einstellung (Verbindungszeichenfolgen, Secrets, Ports) gehört in Umgebungsvariablen oder einen Secret Manager, außerhalb der exportierten Konfiguration. Das exportierte YAML erfasst Ihr semantisches Modell: Quellen, Domänen, Rollen und Sichten.

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```

</content>

Siehe auch: [Umgebungen](environments.md) erklärt, wie Sie benannte, schemaisolierte Kopien Ihres regierten Modells verwalten.
