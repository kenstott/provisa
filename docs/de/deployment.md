# Deployment

## Den richtigen Deployment-Pfad wählen

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

Beide sind Enterprise-tauglich. Der VM/AppImage-Pfad ist einfacher: kein Cluster zu provisionieren, keine CNI- oder RBAC-Richtlinien zu konfigurieren, und das AppImage ist vollständig eigenständig (REQ-223). Es fügt sich nahtlos in bestehende Server-Management-Tools ein (Ansible, Puppet, Datadog-Agents, Splunk-Forwarder usw.).

Wählen Sie Kubernetes nur, wenn Ihr Team bereits einen K8s-Cluster betreibt und Provisa in dieses operative Modell einbinden möchte (Rolling Deployments, HPA, einheitliche Observability) (REQ-056). Die Fähigkeiten sind gleichwertig — Kubernetes bringt zusätzlichen operativen Aufwand, nicht zusätzliche Fähigkeiten.

### Beschaffung der Images und Sicherheitsscans

Alle Produktionspfade erfordern die Beschaffung der Provisa-Artefakte, bevor ein Deployment ausgeführt werden kann. „Air-Gapped" bezieht sich auf das, was zum Installationszeitpunkt auf der Zielmaschine geschieht — die Artefakte müssen zuvor beschafft werden.

**macOS- und Windows-Installer:** Herunterladen von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases). Vollständig gebündelt; nach dem Download ist kein Internet erforderlich (REQ-227). Gedacht für Entwicklung/Evaluierung, nicht für Produktion — kein Image-Scan-Gate zu erwarten.

**AppImage-Pfad:** Herunterladen von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) und auf die Zielmaschine übertragen. Das AppImage bündelt alle Komponenten-Images als Tarballs innerhalb eines Squashfs-Dateisystems (REQ-294) — die meisten Registry-Scanner können diese nicht in-place untersuchen. Wenden Sie sich an Ihr Provisa-Account-Team, um Digests der Komponenten-Images zu erhalten und diese unabhängig gegen Ihren Scanner zu verifizieren.

**Terraform-Pfad:** Das AppImage muss vor der Ausführung von `terraform/deploy.sh` nach S3 hochgeladen werden. EC2-Nodes laden es beim Boot über eine IAM-Rolle herunter — sie benötigen ausgehenden S3-Zugriff (direkt oder über einen VPC-Gateway-Endpoint). Wenden Sie dieselbe Scan-Richtlinie wie beim AppImage-Pfad an.

**Helm-/Kubernetes-Pfad:** Einzelne Images müssen in eine Registry gepusht werden, die der Cluster erreichen kann. Dieser Pfad ist am besten mit registry-basiertem Scanning kompatibel (Prisma Cloud, Aqua, Trivy, AWS Inspector) — Images sind erstklassige Objekte, die Scanner nativ verstehen. Für Air-Gapped-Cluster spiegeln Sie die Images in eine interne Registry und überschreiben die Referenzen in `values.yaml` (REQ-294).

---

## Entwicklung (aus dem Quellcode)

### Empfohlen: `start-ui.sh`

Der einfachste Weg, Provisa aus dem Quellcode heraus auszuführen. Startet die gesamte Infrastruktur, die Backend-API und den UI-Dev-Server mit einem einzigen Befehl (REQ-055). Strg+C fährt alles sauber herunter.

**Voraussetzungen:** Docker Desktop, Node.js, Python-Virtualenv unter `.venv/`

```bash
./start-ui.sh
```

Was es tut:

- Startet `docker-compose.core.yml` + `docker-compose.dev.yml` (alle Core- + Demo-Services) und wartet, bis sie healthy sind (REQ-055)
- Befüllt Kafka mit Demo-Daten
- Synchronisiert die Python-Abhängigkeiten aus `.venv/`
- Startet die Backend-API auf Port 8001 (protokolliert nach `.logs/server.log`) (REQ-558)
- Startet den Vite-UI-Dev-Server auf Port 3000 (REQ-559)
- Gibt URLs aus und wartet; Strg+C stoppt alles und fährt Compose herunter

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Optionen:**

`--reset-volumes` — Führt `docker compose down -v` vor dem Start aus und zerstört dabei alle Docker-Volumes (PostgreSQL-Daten, MinIO-Objekte, Redis-Zustand usw.) (REQ-170). Verwenden Sie dies, wenn Sie einen vollständig sauberen Zustand wünschen — nach einer Schemaänderung während der Entwicklung oder wenn Docker abgestürzt ist und beschädigte Volumes hinterlassen hat. **Alle Daten gehen verloren.**

`--observability` — Fügt vollständige Tracing- und Metrics-Instrumentierung hinzu. Lädt den OpenTelemetry-Java-Agent herunter und patcht Trinos `jvm.config`, um ihn zu laden, instrumentiert das Provisa-Backend mit OTLP-Export und startet den OTel-Collector, Prometheus, Tempo und Grafana (`http://localhost:3100`) (REQ-330). Der `jvm.config`-Patch wird bei Strg+C automatisch rückgängig gemacht.

### Manuelle Schritte (nur Backend, ohne UI)

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

### Vollständiger Stack (Provisa im Container)

Um die API als Container statt auf dem Host auszuführen:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Services

**Core (`docker-compose.core.yml`) — immer erforderlich:**

| Service | Port | Zweck |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Konfigurationsmetadaten + Iceberg-Katalog (REQ-169) |
| PgBouncer | 6432 | Connection Pooling (REQ-053) |
| Föderations-Engine | 8080 | Query-Föderation (REQ-028) |
| Redis | 6379 | Cache für Abfrageergebnisse (REQ-371) |
| MinIO | 9000/9001 | S3-kompatibler Objektspeicher (REQ-029, REQ-171) |

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
| OpenTelemetry Collector | — | Trace-Erfassung (mit `--observability`) (REQ-302) |
| Prometheus | 9090 | Metriken (mit `--observability`) (REQ-330) |
| Tempo | — | Trace-Speicherung (mit `--observability`) (REQ-330) |
| Grafana | 3100 | Dashboards (mit `--observability`) (REQ-330) |

### Telemetrie-Backend (`otlp2sql`)

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

**Zu `trino://`:** Der SQLAlchemy-Trino-Dialekt erzeugt gültiges Trino-DDL und
`INSERT`-Anweisungen, daher ist es als `otlp2sql`-Backend technisch machbar. Es wird
außer für niedrige Ingestionsraten nicht empfohlen. Jeder Batch-Flush wird zu einem
verteilten Trino-`INSERT` plus einem Iceberg-Snapshot, sodass hochfrequente Telemetrie
viele kleine Dateien und Snapshots erzeugt und weiterhin periodische
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots`-Aufrufe benötigt — die `otlp2sql`
nicht ausführt. Zudem gerät die Query-Engine dadurch in den Ingestion-Hotpath.

Für hochvolumige Telemetrie nach Trino/Iceberg verwenden Sie stattdessen `otlp2parquet`: Es
schreibt Parquet in den Objektspeicher, ohne über Trino zu laufen, und eine geplante
Trino-Kompaktierung rollt die Rohdateien in die aktiven Iceberg-Tabellen ein. Für eine einzelne
Engine, die sowohl hochfrequente Ingestion als auch Kompaktierung übernimmt, bevorzugen Sie ClickHouse.

Richten Sie die OTLP-Exporter der App und von Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) auf den
`otlp2sql`-Endpunkt aus und registrieren Sie die Ops-Domain gegen dieselbe
`PROVISA_OPS_DB_URL`, damit sie liest, was der Receiver geschrieben hat.

---

## macOS-Installer

Für Entwickler-Workstations und Evaluierung. Vollständig air-gapped — nach dem Download ist kein Internet erforderlich (REQ-227).

Der Basis-Installer ist eine **native Installation**: DuckDB-Föderations-Engine + SQLite-Control-Plane + In-Memory-Cache (fakeredis), ohne Docker, VM, Trino, Redis oder MinIO (REQ-972, REQ-979). Die Föderations-Engine ist eine Assistentenwahl — DuckDB (nativ, Standard), Trino-auf-Docker oder eine externe Engine (REQ-973). Observability ist immer aktivierte Selbst-Telemetrie, sichtbar unter Admin; der Docker-Stack aus Collector/Prometheus/Grafana ist eine optionale externe Demonstration, kein Ein-/Aus-Schalter (REQ-975). Das Demo-Datenpaket ist optional und standardmäßig deaktiviert (REQ-978). Trino, der Docker-Observability-Stack und die Demo sind schwergewichtige Add-ons, die lokal-first aufgelöst werden (installer-nahes Verzeichnis, gemountete Volumes, `~/Downloads`, dann GitHub-Release), sodass Unternehmen Tarballs für Air-Gapped-Installationen vorab bereitstellen können (REQ-977).

### Schritte

1. Laden Sie `Provisa-<version>-macOS.dmg` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter
2. Öffnen Sie das DMG und ziehen Sie **Provisa.app** nach `/Applications`
3. Doppelklicken Sie auf **Provisa.app** — die Erstlaunch-Einrichtung läuft einmalig; der Assistent bietet die oben genannten Wahlmöglichkeiten für Engine, Observability und Demo an (REQ-1007)
4. Öffnen Sie das Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Datenpersistenz

Alle Daten werden in `~/.provisa/` gespeichert (REQ-224). Um alles zu entfernen: `provisa uninstall`.

---

## Windows-Installer

Für Entwickler-Workstations und Evaluierung. Vollständig air-gapped — nach dem Download ist kein Internet erforderlich (REQ-227).

Wie bei macOS ist der Basis-Windows-Installer eine **native Stufe**: eine eigenständige Python-Laufzeitumgebung + Provisa-Wheel + DuckDB/pg_duckdb + SQLite-Control-Plane, ohne Docker, VM oder Container-Images (REQ-979). Die Föderations-Engine (Trino), der Observability-Stack und das Demo-Datenpaket werden später über separate, geschichtete Installer hinzugefügt, in dieser Reihenfolge: der Container-Installer (`Provisa-Container-<version>.exe`, der WSL2 + containerd + Trino hinzufügt), dann der Obs-Installer (erfordert die Container-Stufe), dann der Demo-Installer (erfordert Core + Obs). Die Erstlaunch-Anleitung erklärt, wie die Föderations-Engine durch Ausführen des Container-Installers initialisiert wird (REQ-1005).

### Schritte

1. Laden Sie `Provisa-<version>-windows-x64.exe` von der [GitHub-Releases-Seite](https://github.com/provisa/provisa/releases) herunter
2. Führen Sie den Installer aus — keine Administratorrechte erforderlich; installiert nach `%LOCALAPPDATA%\Programs\Provisa\`
3. Öffnen Sie **Provisa First Launch** über das Startmenü — die native Einrichtung läuft einmalig und gibt die Anleitung für die nächsten Schritte zu den geschichteten Add-ons aus (REQ-1005)
4. Öffnen Sie ein neues Terminal:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Datenpersistenz

Alle Daten werden in `%USERPROFILE%\.provisa\` gespeichert.

---

## Linux-AppImage — Single- oder Multi-Node-VM

### Was es ist

`Provisa.AppImage` ist eine einzelne eigenständige ausführbare Datei, die Folgendes bündelt (REQ-223, REQ-228):

- Einen rootlosen Docker-Daemon (`dockerd-rootless.sh` + `rootlesskit`) — kein System-Docker oder Root erforderlich
- Alle Container-Image-Tarballs (PostgreSQL, PgBouncer, MinIO, Redis, Föderations-Engine, Provisa-API) (REQ-294)
- Den Provisa-CLI-Wrapper und das Erstlaunch-Einrichtungsskript

Das Provisa-Image wird zur Paketierungszeit vorab erstellt — der Python-Quellcode ist nie enthalten.

### Wann verwenden

- On-Premises-Bare-Metal oder VM (Single-Node oder Multi-Node)
- Cloud-VMs ohne K8s-Cluster
- Air-Gapped-Umgebungen (REQ-294)
- Wenn Sie einfacheren Betrieb als mit Kubernetes wünschen

---

### Schritte — Single Node

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
   - **API-Port** → Standard `8000` (REQ-560)
5. Die Einrichtung lädt alle Container-Images (~2–5 Minuten), schreibt die Konfiguration und startet die Services
6. Überprüfen:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Schritte — Multi-Node (Primary)

Führen Sie diese Schritte zuerst auf dem primären Node aus. Sekundäre Nodes müssen eingerichtet werden, nachdem der primäre Node läuft.

1. Laden Sie `Provisa.AppImage` herunter und übertragen Sie es auf die primäre Maschine
2. Öffnen Sie die erforderlichen Firewall-Ports (sekundäre Nodes verbinden sich eingehend auf diesen):

   | Port | Service |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Föderations-Engine-Koordinator |
   | 8000 | Provisa-API |

3. Machen Sie es ausführbar und führen Sie es aus:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Der Einrichtungsassistent fragt:
   - **Rolle** → wählen Sie `primary`
   - **RAM-Budget**, **Hostname**, **API-Port** → antworten Sie wie bei Single Node
5. Notieren Sie sich nach Abschluss der Einrichtung die **private IP** dieser Maschine — sekundäre Nodes benötigen sie
6. Der Assistent gibt einen nginx-Upstream-Block aus — speichern Sie ihn für Ihre Load-Balancer-Konfiguration
7. Überprüfen:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Schritte — Multi-Node (jeder Secondary)

Wiederholen Sie diese Schritte auf jedem zusätzlichen Node, nachdem der primäre Node läuft und erreichbar ist.

1. Laden Sie `Provisa.AppImage` herunter und übertragen Sie es auf die sekundäre Maschine
2. Bestätigen Sie, dass der sekundäre Node den primären Node erreichen kann:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Machen Sie es ausführbar und führen Sie es aus:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Der Einrichtungsassistent fragt:
   - **Rolle** → wählen Sie `secondary`
   - **Primary-IP** → geben Sie die IP des primären Nodes ein (die Konnektivität wird live überprüft)
   - **RAM-Budget**, **Hostname**, **API-Port** → antworten Sie wie oben
5. Die Einrichtung lädt einen reduzierten Image-Satz (ohne PostgreSQL, PgBouncer, MinIO, Redis — diese laufen nur auf dem primären Node) (REQ-561), startet die Provisa-API und einen Föderations-Engine-Worker
6. Überprüfen:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Fügen Sie diesen Node zum Upstream Ihres Load Balancers hinzu

---

### Primary-/Secondary-Topologie

**Der primäre Node** führt alle Singleton-Services aus:

| Service | Warum Singleton |
| --------- | --------------- |
| PostgreSQL | Gemeinsames Schema, App-Konfiguration, semantisches Modell |
| Redis | Gemeinsamer Cache für Abfrageergebnisse und Subscription-Zustand (REQ-371) |
| MinIO | Gemeinsamer Objektspeicher für Redirect-Ergebnisse und Snapshots materialisierter Sichten (REQ-029) |
| Föderations-Engine-Koordinator | Alle Worker (primär + sekundär) registrieren sich hier (REQ-028) |

**Die sekundären Nodes** führen nur aus:

- Provisa-API — zustandslos; liest bei jedem Start die gesamte Konfiguration von PostgreSQL auf dem primären Node (REQ-057, REQ-562)
- Föderations-Engine-Worker — registriert sich selbst beim Koordinator auf dem primären Node (REQ-028)

Der gesamte Anwendungszustand fließt über das PostgreSQL des primären Nodes. Keine manuelle Synchronisierung erforderlich. (REQ-562)

---

### Nicht-interaktiver (automatisierter) Erstlaunch

Für Terraform, Cloud-Init oder Ansible — übergeben Sie Flags anstatt Eingabeaufforderungen zu beantworten:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Der nicht-interaktive Modus installiert eine systemd-Unit (`/etc/systemd/system/provisa.service`) für den Start beim Booten. (REQ-563)

| Flag | Beschreibung |
| ------ | ------------- |
| `--non-interactive` | Überspringt alle Eingabeaufforderungen; installiert die systemd-Unit |
| `--role primary\|secondary` | Node-Rolle |
| `--primary-ip <ip>` | IP des primären Nodes (erforderlich für Secondary) |
| `--ram-gb <n>` | Zuzuweisender RAM (0 = gesamter verfügbarer) |

---

## Cloud-VM-Deployment — Terraform (AWS)

Provisioniert einen vollständigen Multi-Node-Provisa-Cluster auf AWS — VPC, Security Groups, EC2-Instanzen, ALB, NLB — mit einem einzigen interaktiven Befehl. (REQ-564)

### Dateien

| Datei | Zweck |
| ------ | --------- |
| `terraform/deploy.sh` | Interaktiver Wrapper — sammelt Parameter, validiert Anmeldedaten, schreibt `terraform.tfvars`, führt Apply aus |
| `terraform/aws/variables.tf` | Alle Variablendefinitionen mit Standardwerten |
| `terraform/aws/main.tf` | VPC, Subnetze, Security Groups, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | Endpunkt-URLs und Node-IPs |

### Schritte

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
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Optional) Richten Sie DNS-Einträge auf den ALB- und NLB-DNS-Namen aus

10. Überprüfen:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Fragen des Assistenten

| Frage | Standard | Hinweise |
| ---------- | --------- | ------- |
| Cloud-Anbieter | — | Heute nur AWS |
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

### Sizing-Leitfaden für Instanzen

| Typ | vCPU | RAM | Trino-Worker/Node | Anwendungsfall |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Entwicklung / kleine Datensätze |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Kleine Produktion |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Mittlere Produktion |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Große Produktion |

Alle Nodes tragen Worker zu einem einzigen Koordinator auf dem primären Node bei (REQ-028). Ein 3-Node-Cluster mit `m7i.4xlarge` liefert insgesamt 6 Trino-Worker.

### Was provisioniert wird

- VPC mit zwei öffentlichen Subnetzen über zwei Availability Zones (REQ-564)
- Security Groups: LB-Gruppe (öffentlicher Ingress auf 8000/8815), Nodes-Gruppe (LB → Nodes, Intra-Cluster, optionales SSH)
- IAM-Rolle + Instance-Profil mit S3-GetObject auf den AppImage-Bucket
- Primäre EC2-Instanz — führt den Erstlaunch im Modus `--non-interactive --role primary` aus
- Sekundäre EC2-Instanzen (node_count − 1) — führen den Erstlaunch im Modus `--non-interactive --role secondary --primary-ip <primary private IP>` aus; hängen davon ab, dass der primäre Node zuerst fertig wird
- ALB auf Port 8000 — HTTP-API, Health-Check `/health` (REQ-560)
- NLB auf Port 8815 — Arrow Flight / gRPC (REQ-143)
- Beide LBs sind an alle Nodes angeschlossen

### Checkliste der Voraussetzungen

- [ ] IAM-Berechtigungen: EC2 vollständig, ELB vollständig, VPC vollständig, IAM-Rollenerstellung, S3-GetObject auf den AppImage-Bucket
- [ ] `Provisa.AppImage` nach S3 hochgeladen
- [ ] EC2-Nodes haben ausgehenden S3-Zugriff (direktes Internet oder S3-VPC-Gateway-Endpoint)
- [ ] EC2-Schlüsselpaar existiert in der Zielregion (falls SSH benötigt wird)
- [ ] Terraform ≥ 1.5 lokal installiert
- [ ] DNS-Einträge für ALB / NLB geplant (optional, aber empfohlen)
- [ ] ACM-Zertifikat bereit, falls HTTPS erforderlich ist (nicht im Basis-Terraform enthalten)

### Secrets

Es sind keine Secrets in Terraform eingebettet. Das AppImage generiert Anmeldedaten während des Erstlaunchs und schreibt sie auf jedem Node in `~/.provisa/config.yaml` (REQ-563). Für die Produktion rufen Sie das Admin-Token nach dem Deployment vom primären Node ab:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Wann verwenden

Ihr Team betreibt bereits einen Kubernetes-Cluster und möchte, dass Provisa an diesem operativen Modell teilnimmt (REQ-056). Wenn Sie Provisa evaluieren oder on-premises ohne bestehenden Cluster deployen, ist der AppImage-Pfad einfacher.

Hinweis: Das Provisa-AppImage kann nicht innerhalb eines Kubernetes-Pods ausgeführt werden — es benötigt FUSE und einen rootlosen Docker-Daemon, die in Standard-Pod-Sicherheitsprofilen nicht verfügbar sind.

### Schritte

1. Bestätigen Sie den Cluster-Zugriff:

   ```bash
   kubectl cluster-info
   ```

2. Ziehen Sie die Images und spiegeln Sie sie in Ihre interne Registry (erforderlich für Air-Gapped- oder gescannte Umgebungen; überspringen, wenn direkt aus öffentlichen Registries gezogen wird) (REQ-294):

   | Image | Verwendung für |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | Provisa-API |
   | `trinodb/trino:480` | Föderations-Engine-Koordinator + Worker (REQ-169) |
   | `postgres:16` | PostgreSQL im Cluster (falls `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer im Cluster (falls `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis im Cluster (falls `redis.enabled` und kein `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO im Cluster (falls `minio.enabled`) (REQ-029) |

   Für Umgebungen mit Registry-Scanning:
   - Pushen Sie jedes Image in Ihre Staging-Registry
   - Führen Sie Ihren Scanner aus (Prisma Cloud, Aqua, Trivy, AWS Inspector) und holen Sie die Freigabe ein
   - Befördern Sie es in Ihre interne Produktions-Registry

3. Entscheiden Sie vor der Installation:
   - **PostgreSQL** — im Cluster (`postgresql.enabled: true`) oder extern verwaltet (`postgresql.host`)? Für Produktion wird extern empfohlen
   - **Redis** — im Cluster oder extern (`redis.host`)? Ändern Sie das Standardpasswort (`redis.password`)
   - **MinIO / S3** — MinIO im Cluster oder natives S3? Für AWS verwenden Sie S3 mit einer IAM-Rolle
   - **Secrets** — für die Evaluierung über `--set` übergeben; für Produktion External Secrets oder Vault Agent verwenden

4. Installieren Sie das Chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Bei Verwendung einer internen Registry fügen Sie Image-Overrides hinzu:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Überprüfen Sie, ob die Pods laufen:

   ```bash
   kubectl get pods -n provisa
   ```

6. Überprüfen Sie die API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Optional) Aktivieren Sie Ingress für externen Zugriff — setzen Sie `ingress.enabled: true` und konfigurieren Sie Ihren Ingress-Controller

### Checkliste der Voraussetzungen

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Storage Class, die `ReadWriteOnce`-PVCs unterstützt (für zustandsbehaftete Services im Cluster)
- [ ] Images für den Cluster verfügbar (öffentliche oder interne Registry)
- [ ] PostgreSQL-Endpunkt + Anmeldedaten (falls extern)
- [ ] Redis-Endpunkt + Anmeldedaten (falls extern)
- [ ] S3-Bucket + Anmeldedaten oder IAM-Rolle
- [ ] Admin-Token gewählt
- [ ] Ingress-Controller konfiguriert (falls externer Zugriff benötigt wird)

### Wichtige Werte

| Wert | Standard | Beschreibung |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Replikate der Provisa-API (zustandslos) (REQ-057) |
| `config.pgHost` | `postgres` | PostgreSQL-Host |
| `config.pgPassword` | | PostgreSQL-Passwort |
| `config.adminToken` | | Bearer-Token für die Admin-API |
| `redis.enabled` | `true` | Deployt ein Redis-StatefulSet im Cluster (REQ-371) |
| `redis.host` | `""` | Setzen, um externes Redis zu verwenden |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Dies ändern |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Deployt die Föderations-Engine (REQ-028) |
| `trino.workers` | `2` | Replikate der Föderations-Engine-Worker (REQ-056) |
| `postgresql.enabled` | `true` | Deployt PostgreSQL im Cluster (REQ-169) |
| `postgresql.host` | `""` | Setzen, um externes PostgreSQL zu verwenden |
| `minio.enabled` | `true` | Deployt MinIO im Cluster (REQ-029) |
| `s3.endpoint` | | S3-kompatible Endpunkt-URL |
| `s3.bucket` | `provisa-results` | Bucket für Redirect großer Ergebnisse (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Aktiviert Ingress |

### Skalierung

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Föderations-Engine-Worker skalieren unabhängig — mehr Worker erhöhen den Durchsatz und die Kapazität für gleichzeitige Abfragen (REQ-056). (REQ-057)

### Konfiguration aktualisieren

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Hochverfügbarkeit & Recovery

Provisa wendet über alle Deployment-Modi hinweg ein zweistufiges Recovery-Modell an (REQ-703):

- **Stufe 1 — transiente Fehler.** Leseoperationen werden bei transienten Fehlern bis zu 30 Sekunden lang mit exponentiellem Backoff und vollem Jitter wiederholt. Passen Sie das Budget mit `PROVISA_RETRY_BUDGET_SECS` an. Schreiboperationen werden intern niemals wiederholt, und Speicherfehler sind niemals wiederholbar.
- **Stufe 2 — Komponentenausfall.** Ein interner Engine-Watcher erkennt fehlgeschlagene Softwarekomponenten und startet sie innerhalb von 2–3 Minuten neu.

Ausfälle auf Maschinen- und Cluster-Ebene bleiben in der Verantwortung des Betreibers — provisionieren Sie redundante Nodes und einen Load Balancer (Terraform- und Helm-Pfade oben), um Node-Ausfälle zu tolerieren.

## Abhängigkeiten der Föderations-Engine

Die Warehouse-Föderations-Engines erfordern Python-Pakete und Komponenten auf Systemebene, die über die Standardinstallation von Provisa hinausgehen. Alle hier aufgeführten Python-Pakete sind in `pyproject.toml` deklariert und werden im Rahmen der Standardinstallation von `pip install provisa` oder `pip install -e .` installiert [tool-verified: `pyproject.toml` lines 44–52].

Die Python-Pakete sind Teil der Standardinstallation von Provisa — für keine Warehouse-Engine sind optionale Extras erforderlich. Die Komponenten auf Systemebene (ODBC-Treiber, Cloud-CLIs, Service-Account-Schlüssel) müssen separat installiert werden.

### Python-Pakete (bereits in den Core-Abhängigkeiten)

[tool-verified: `pyproject.toml` lines 41–52]

| Paket | Engine | Zweck |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | SQL-Warehouse-Verbindung; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Verbindung + Arrow-natives `fetch_arrow_table` (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Query-Ausführung |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API für Arrow-native Reads |
| `google-cloud-storage` | BigQuery | GCS-Staging für externe Tabellenverknüpfungen |
| `pyodbc` | Fabric, Synapse | ODBC-Verbindung zu T-SQL-Endpunkten |
| `azure-identity` | Fabric, Synapse | Azure-AD-Token über `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | HTTP-Columnar-Reads |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Kompatibilitäts-Pin — `google-cloud-*` und OTel teilen sich eine Protobuf-Laufzeitumgebung; `<7` hält sie aufeinander abgestimmt |
| `grpcio-status<1.82` | gRPC | Passt sich an den `protobuf<7`-Pin an |

### Anforderungen auf Systemebene

Dies sind keine Python-Pakete — sie müssen auf dem Host oder Container installiert werden, auf dem Provisa läuft.

**Microsoft Fabric und Azure Synapse (ODBC)**

`pyodbc` verbindet sich über den Microsoft-ODBC-Treiber für SQL Server (`msodbcsql18`). Der Treiber muss auf dem Host installiert werden — nicht über pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

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

Beide Engines authentifizieren sich über `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` prüft die Anmeldedatenquellen in dieser Reihenfolge: Umgebungsvariablen, Workload-Identität, Managed Identity, VS Code, `az login` und weitere.

Für die lokale Entwicklung ist `az login` der einfachste Weg:

```bash
az login
```

Für die Produktion verwenden Sie Managed Identity (auf Azure-VMs oder AKS) — keine Anmeldedatenverwaltung erforderlich. Für die Authentifizierung mit Service Principal setzen Sie:

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

Für die Produktion auf GCP (Cloud Run, GKE mit Workload Identity, Compute Engine) erkennt die Bibliothek das angehängte Service Account automatisch — keine Umgebungsvariable erforderlich.

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

Der Databricks-Connector übernimmt dies von `requests` — es ist keine Databricks-spezifische Umgebungsvariable erforderlich.

### Checkliste pro Engine

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` installiert (Standard)
- [ ] Engine-URL mit `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Personal Access Token oder Service-Principal-Token
- [ ] `REQUESTS_CA_BUNDLE` gesetzt, falls hinter einem TLS-abfangenden Proxy

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` installiert (Standard)
- [ ] Engine-URL: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` in `PROVISA_ENGINE_URL` oder `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installiert (Standard)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` gesetzt (Dev) oder Workload Identity konfiguriert (Prod)
- [ ] `GOOGLE_CLOUD_PROJECT` gesetzt, falls das Projekt nicht aus dem Service Account abgeleitet werden kann
- [ ] Service Account hat die Rollen BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` installiert (Standard)
- [ ] Systemtreiber `msodbcsql18` installiert
- [ ] `FABRIC_SQL_SERVER` und `FABRIC_DATABASE` gesetzt
- [ ] Azure-AD-Authentifizierung: `az login` (Dev) oder Managed Identity / Service Principal (Prod)
- [ ] `FABRIC_WORKSPACE_ID` gesetzt, falls externe Objektspeicher-Verknüpfungen verwendet werden

**Azure Synapse** (REQ-989)

- [ ] Gleiche Python- und Systemanforderungen wie Fabric
- [ ] `SYNAPSE_SQL_SERVER` und `SYNAPSE_DATABASE` gesetzt
- [ ] Gleiche Azure-AD-Authentifizierungseinrichtung wie Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` installiert (Standard)
- [ ] Engine-URL: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` in `federation_hints` für TLS (Port 8443)

---

## Umgebungsvariablen

| Variable | Standard | Zweck |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | PostgreSQL-Passwort |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Pfad zur Konfigurationsdatei (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Aktiviert das Redirect großer Ergebnisse nach S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Schwellenwert für die Zeilenanzahl beim Redirect (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | S3-Bucket (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | S3-kompatible Endpunkt-URL (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL der Presigned-URL (Sekunden) (REQ-141) |
| `REDIS_HOST` | `localhost` | Redis-Host |
| `REDIS_PORT` | `6379` | Redis-Port |
| `REDIS_PASSWORD` | | Redis-Passwort |
| `REDIS_TLS` | `false` | Aktiviert TLS für Redis |
| `TRINO_HOST` | `localhost` | Host des Trino-Föderations-Engine-Koordinators (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | HTTP-Port des Trino-Föderations-Engine-Koordinators (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Schlüssel der aktiven Föderations-Engine (REQ-989); überschreibt die persistierte Konfiguration |
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

## CLI-Befehle

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

### Workflow zur Konfigurations-Promotion (Dev → Test → Prod)

Alle umgebungsspezifischen Einstellungen (Connection Strings, Secrets, Ports) gehören in Umgebungsvariablen oder Secret-Manager — nicht in die exportierte Konfiguration. Das exportierte YAML erfasst Ihr semantisches Modell: Quellen, Domains, Rollen, Sichten. (REQ-164)

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
