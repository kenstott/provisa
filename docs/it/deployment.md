# Deployment

## Scegliere un percorso di deployment

Provisa supporta sei percorsi di deployment. Scegli in base al tuo pubblico e contesto operativo:

| Percorso | Artefatto / Script | Ideale per |
| ------ | ------------------- | ---------- |
| **Sviluppo** | `start-ui.sh` | Sviluppo da sorgente, valutazione con dati demo completi |
| **Installer macOS** | `Provisa-<version>-macOS.dmg` | Workstation sviluppatori, valutazione |
| **Installer Windows** | `Provisa-<version>-windows-x64.exe` | Workstation sviluppatori, valutazione |
| **Linux AppImage** | `Provisa.AppImage` | Server on-prem, VM cloud, ambienti air-gapped |
| **VM Cloud (AWS)** | `terraform/deploy.sh` | Deployment cloud multi-nodo con load balancer |
| **Kubernetes** | `helm/provisa/` | Team che già operano K8s |

### VM vs Kubernetes

Entrambi sono enterprise-grade. Il percorso VM/AppImage è più semplice: nessun cluster da provisionare, nessuna policy CNI o RBAC da configurare, e l'AppImage è interamente self-contained (REQ-223). Si integra naturalmente negli strumenti di gestione server esistenti (Ansible, Puppet, agenti Datadog, forwarder Splunk, ecc.).

Scegli Kubernetes solo se il tuo team già opera un cluster K8s e vuole che Provisa partecipi a quel modello operativo (rolling deploy, HPA, osservabilità unificata) (REQ-056). Le capacità sono equivalenti — Kubernetes aggiunge overhead operativo, non capacità.

### Acquisizione immagini e scansione di sicurezza

Tutti i percorsi di produzione richiedono di ottenere gli artefatti Provisa prima che qualsiasi deployment possa essere eseguito. "Air-gapped" si riferisce a ciò che accade al momento dell'installazione sulla macchina target — gli artefatti devono essere acquisiti prima.

**Installer macOS e Windows:** Scarica dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases). Completamente bundlati; nessun internet richiesto dopo il download (REQ-227). Pensati per dev/valutazione, non produzione — nessun gate di scansione immagini previsto.

**Percorso AppImage:** Scarica dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases) e trasferisci sulla macchina target. L'AppImage impacchetta tutte le immagini dei componenti come tarball dentro un filesystem squashfs (REQ-294) — la maggior parte degli scanner di registry non può ispezionarle in loco. Contatta il tuo account team Provisa per i digest delle immagini dei componenti per verificarli indipendentemente con il tuo scanner.

**Percorso Terraform:** L'AppImage deve essere caricato su S3 prima di eseguire `terraform/deploy.sh`. I nodi EC2 lo scaricano all'avvio tramite ruolo IAM — richiedono accesso S3 in uscita (diretto o via VPC gateway endpoint). Applica la stessa policy di scansione del percorso AppImage.

**Percorso Helm / Kubernetes:** Le singole immagini devono essere pushate su un registry raggiungibile dal cluster. Questo percorso è il più compatibile con la scansione basata su registry (Prisma Cloud, Aqua, Trivy, AWS Inspector) — le immagini sono oggetti di prima classe che gli scanner comprendono nativamente. Per cluster air-gapped, esegui il mirroring delle immagini su un registry interno e sovrascrivi i riferimenti in `values.yaml` (REQ-294).

---

## Sviluppo (da sorgente)

### Consigliato: `start-ui.sh`

Il modo più semplice per eseguire Provisa da sorgente. Avvia tutta l'infrastruttura, l'API backend, e il server dev della UI in un unico comando (REQ-055). Ctrl+C spegne tutto in modo pulito.

**Prerequisiti:** Docker Desktop, Node.js, virtualenv Python in `.venv/`

```bash
./start-ui.sh
```

Cosa fa:

- Avvia `docker-compose.core.yml` + `docker-compose.dev.yml` (tutti i servizi core + demo) e attende che siano healthy (REQ-055)
- Semina Kafka con dati demo
- Sincronizza le dipendenze Python da `.venv/`
- Avvia l'API backend sulla porta 8001 (log in `.logs/server.log`) (REQ-558)
- Avvia il server dev Vite della UI sulla porta 3000 (REQ-559)
- Stampa gli URL e attende; Ctrl+C ferma tutto e smonta compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Opzioni:**

`--reset-volumes` — Esegue `docker compose down -v` prima di avviare, distruggendo tutti i volumi Docker (dati PostgreSQL, oggetti MinIO, stato Redis, ecc.) (REQ-170). Usalo quando vuoi uno stato completamente pulito — dopo una modifica dello schema durante lo sviluppo, o quando Docker è andato in crash e ha lasciato volumi corrotti. **Tutti i dati andranno persi.**

`--observability` — Aggiunge strumentazione completa di tracing e metriche. Scarica l'OpenTelemetry Java agent e applica una patch a `jvm.config` di Trino per caricarlo, strumenta il backend Provisa con export OTLP, e avvia l'OTel collector, Prometheus, Tempo, e Grafana (`http://localhost:3100`) (REQ-330). La patch a `jvm.config` viene automaticamente ripristinata su Ctrl+C.

### Passi manuali (solo backend, senza UI)

Se ti serve solo l'API:

1. Installa [Docker Desktop](https://docs.docker.com/get-docker/)
2. Avvia i servizi core:

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Avvia l'API:

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Verifica: `curl http://localhost:8001/health`

### Stack completo (Provisa in container)

Per eseguire l'API come container invece che sull'host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Servizi

**Core (`docker-compose.core.yml`) — sempre richiesto:**

| Servizio | Porta | Scopo |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Metadati di configurazione + catalogo Iceberg (REQ-169) |
| PgBouncer | 6432 | Connection pooling (REQ-053) |
| Motore di federazione | 8080 | Federazione query (REQ-028) |
| Redis | 6379 | Cache dei risultati query (REQ-371) |
| MinIO | 9000/9001 | Object storage compatibile S3 (REQ-029, REQ-171) |

**Demo (`docker-compose.dev.yml`) — opzionale, incluso da `start-ui.sh`:**

| Servizio | Porta | Scopo |
| --------- | ------ | --------- |
| MongoDB | 27017 | Origine NoSQL demo |
| Kafka | 9092 | Origine streaming demo |
| Schema Registry | 8081 | Gestione schema Avro/Protobuf demo |
| Debezium | — | Connettore CDC demo |
| Elasticsearch | 9200 | Origine di ricerca demo |
| Neo4j | 7474/7687 | Origine grafo demo |
| Fuseki | 3030 | Triplestore SPARQL demo |
| OpenTelemetry Collector | — | Raccolta trace (con `--observability`) (REQ-302) |
| Prometheus | 9090 | Metriche (con `--observability`) (REQ-330) |
| Tempo | — | Storage trace (con `--observability`) (REQ-330) |
| Grafana | 3100 | Dashboard (con `--observability`) (REQ-330) |

### Backend di telemetria (`otlp2sql`)

Lo stack `--observability` sopra (Collector → Tempo/Prometheus/Grafana) è un
percorso di telemetria. L'altro è `otlp2sql` (`provisa.observability.otlp2sql`): un
receiver OTLP/HTTP che scrive trace, metriche, e log su un database SQL
scelto tramite URL SQLAlchemy, estraendo gli attributi span `provisa.*` all'ingest
così nessun job di compattazione separato viene eseguito. Le scritture sono in batch
(`OTLP2SQL_BATCH_MAX_ROWS`, default 1000; `OTLP2SQL_BATCH_MAX_SECS`, default 2s).

La telemetria ha il proprio store, separato dal database control-plane. Seleziona
il backend con `PROVISA_OPS_DB_URL`:

| `PROVISA_OPS_DB_URL` | Backend | Note |
| --- | --- | --- |
| *(non impostato)* | DuckDB dedicato sotto `~/.provisa/telemetry/` | default; nessun server, nessun Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | ingest ad alto rate con merge in background automatici |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | volume moderato |
| `trino://user@host:8080/otel` | Trino / Iceberg | funziona tecnicamente, **non raccomandato** — vedi sotto |

**Su `trino://`:** il dialetto SQLAlchemy Trino emette DDL Trino valido e
`INSERT`, quindi è tecnicamente fattibile come backend `otlp2sql`. Non è
raccomandato se non per rate di ingest bassi. Ogni flush di batch diventa un
`INSERT` Trino distribuito più uno snapshot Iceberg, quindi la telemetria ad alto rate
produce molti file e snapshot piccoli e necessita ancora di
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` periodici — che `otlp2sql`
non esegue. Inoltre mette il motore di query nel percorso caldo di ingest.

Per telemetria ad alto volume verso Trino/Iceberg, usa `otlp2parquet` invece: esso
scrive parquet su object storage senza passare per Trino, e una compattazione
Trino pianificata unisce i file raw nelle tabelle Iceberg live. Per un unico
motore che gestisce sia ingest ad alto rate che compattazione, preferisci ClickHouse.

Punta gli exporter OTLP dell'app e di Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) verso
l'endpoint `otlp2sql`, e registra il dominio ops contro lo stesso
`PROVISA_OPS_DB_URL` così che legga ciò che il receiver ha scritto.

---

## Installer macOS

Per workstation sviluppatori e valutazione. Completamente air-gapped — nessun internet richiesto dopo il download (REQ-227).

L'installer base è una **installazione nativa**: motore di federazione DuckDB + control plane SQLite + cache in-memory (fakeredis), senza Docker, VM, Trino, Redis, o MinIO (REQ-972, REQ-979). Il motore di federazione è una scelta del wizard — DuckDB (nativo, default), Trino-su-Docker, o un motore esterno (REQ-973). L'osservabilità è sempre attiva come auto-telemetria visualizzabile in Admin; lo stack Docker collector/Prometheus/Grafana è una dimostrazione esterna opzionale, non un interruttore on/off (REQ-975). Il pacchetto dati demo è opzionale e disattivato per default (REQ-978). Trino, lo stack di osservabilità Docker, e la demo sono add-on pesanti risolti local-first (directory adiacente all'installer, volumi montati, `~/Downloads`, poi GitHub release), così le aziende possono pre-caricare tarball per installazioni air-gapped (REQ-977).

### Passi

1. Scarica `Provisa-<version>-macOS.dmg` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases)
2. Apri il DMG e trascina **Provisa.app** su `/Applications`
3. Fai doppio clic su **Provisa.app** — la configurazione al primo avvio viene eseguita una volta; il wizard offre le scelte di motore, osservabilità, e demo di cui sopra (REQ-1007)
4. Apri Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Persistenza dei dati

Tutti i dati sono memorizzati in `~/.provisa/` (REQ-224). Per rimuovere tutto: `provisa uninstall`.

---

## Installer Windows

Per workstation sviluppatori e valutazione. Completamente air-gapped — nessun internet richiesto dopo il download (REQ-227).

Come macOS, l'installer Windows base è un **tier nativo**: un runtime Python standalone + wheel provisa + DuckDB/pg_duckdb + control plane SQLite, senza spedire Docker, VM, o immagini container (REQ-979). Il motore di federazione (Trino), lo stack di osservabilità, e il pacchetto dati demo vengono aggiunti in seguito tramite installer separati a strati, in ordine: l'installer Container (`Provisa-Container-<version>.exe`, che aggiunge WSL2 + containerd + Trino), poi l'installer Obs (richiede il tier container), poi l'installer Demo (richiede Core + Obs). La guida al primo avvio spiega come inizializzare il motore di federazione eseguendo l'installer Container (REQ-1005).

### Passi

1. Scarica `Provisa-<version>-windows-x64.exe` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases)
2. Esegui l'installer — nessun diritto admin richiesto; installa in `%LOCALAPPDATA%\Programs\Provisa\`
3. Apri **Provisa First Launch** dal menu Start — la configurazione nativa viene eseguita una volta e stampa la guida ai passi successivi per gli add-on a strati (REQ-1005)
4. Apri un nuovo terminale:

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Persistenza dei dati

Tutti i dati sono memorizzati in `%USERPROFILE%\.provisa\`.

---

## Linux AppImage — VM a nodo singolo o multi-nodo

### Cos'è

`Provisa.AppImage` è un unico eseguibile self-contained che impacchetta (REQ-223, REQ-228):

- Un daemon Docker rootless (`dockerd-rootless.sh` + `rootlesskit`) — nessun Docker di sistema o root richiesto
- Tutte le tarball delle immagini container (PostgreSQL, PgBouncer, MinIO, Redis, Motore di federazione, API Provisa) (REQ-294)
- Il wrapper CLI Provisa e lo script di configurazione al primo avvio

L'immagine Provisa è pre-costruita al momento del packaging — il sorgente Python non è mai incluso.

### Quando usarlo

- Bare metal on-premises o VM (nodo singolo o multi-nodo)
- VM cloud senza un cluster K8s
- Ambienti air-gapped (REQ-294)
- Quando vuoi operazioni più semplici di Kubernetes

---

### Passi — Nodo singolo

1. Scarica `Provisa.AppImage` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases) e trasferiscila sulla macchina target
2. Rendila eseguibile:

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Esegui la configurazione al primo avvio:

   ```bash
   ./Provisa.AppImage
   ```

4. Il wizard di setup chiede:
   - **Ruolo** → seleziona `primary`
   - **Budget RAM** → quantità di RAM da allocare (0 = tutta la disponibile); determina il numero di worker Trino
   - **Hostname** → l'indirizzo pubblicizzato di questo nodo
   - **Porta API** → default `8000` (REQ-560)
5. La configurazione carica tutte le immagini container (~2–5 minuti), scrive la config, e avvia i servizi
6. Verifica:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Passi — Multi-nodo (Primary)

Esegui questi passi prima sul nodo primary. I secondary devono essere configurati dopo che il primary è in esecuzione.

1. Scarica e trasferisci `Provisa.AppImage` sulla macchina primary
2. Apri le porte firewall richieste (i secondary si connetteranno in ingresso su queste):

   | Porta | Servizio |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordinator motore di federazione |
   | 8000 | API Provisa |

3. Rendi eseguibile ed esegui:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Il wizard di setup chiede:
   - **Ruolo** → seleziona `primary`
   - **Budget RAM**, **hostname**, **porta API** → rispondi come per il nodo singolo
5. Dopo il completamento della configurazione, annota il **IP privato** di questa macchina — i secondary ne hanno bisogno
6. Il wizard stampa un blocco upstream nginx — salvalo per la configurazione del tuo load balancer
7. Verifica:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Passi — Multi-nodo (ogni Secondary)

Ripeti questi passi su ogni nodo aggiuntivo dopo che il primary è in esecuzione e raggiungibile.

1. Scarica e trasferisci `Provisa.AppImage` sulla macchina secondary
2. Conferma che il secondary possa raggiungere il primary:

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Rendi eseguibile ed esegui:

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. Il wizard di setup chiede:
   - **Ruolo** → seleziona `secondary`
   - **IP primary** → inserisci l'IP del nodo primary (la connettività viene verificata live)
   - **Budget RAM**, **hostname**, **porta API** → rispondi come sopra
5. La configurazione carica un set ridotto di immagini (nessun PostgreSQL, PgBouncer, MinIO, Redis — quelli girano solo sul primary) (REQ-561), avvia l'API Provisa e un worker del motore di federazione
6. Verifica:

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Aggiungi questo nodo all'upstream del tuo load balancer

---

### Topologia primary / secondary

Il **nodo primary** esegue tutti i servizi singleton:

| Servizio | Perché singleton |
| --------- | --------------- |
| PostgreSQL | Schema condiviso, config app, modello semantico |
| Redis | Cache condivisa dei risultati query e stato subscription (REQ-371) |
| MinIO | Store oggetti condiviso per risultati redirect e snapshot MV (REQ-029) |
| Coordinator motore di federazione | Tutti i worker (primary + secondary) si registrano qui (REQ-028) |

I **nodi secondary** eseguono solo:

- API Provisa — stateless; legge tutta la config da PostgreSQL sul primary all'avvio (REQ-057, REQ-562)
- Worker del motore di federazione — si auto-registra con il coordinator sul primary (REQ-028)

Tutto lo stato applicativo passa attraverso il PostgreSQL del primary. Nessuna sincronizzazione manuale richiesta. (REQ-562)

---

### Primo avvio non interattivo (automatizzato)

Per Terraform, cloud-init, o Ansible — passa flag invece di rispondere ai prompt:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

La modalità non interattiva installa una unit systemd (`/etc/systemd/system/provisa.service`) per l'avvio al boot. (REQ-563)

| Flag | Descrizione |
| ------ | ------------- |
| `--non-interactive` | Salta tutti i prompt; installa la unit systemd |
| `--role primary\|secondary` | Ruolo del nodo |
| `--primary-ip <ip>` | IP del nodo primary (richiesto per secondary) |
| `--ram-gb <n>` | RAM da allocare (0 = tutta la disponibile) |

---

## Deployment VM Cloud — Terraform (AWS)

Provisiona un intero cluster Provisa multi-nodo su AWS — VPC, security group, istanze EC2, ALB, NLB — in un unico comando interattivo. (REQ-564)

### File

| File | Scopo |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interattivo — raccoglie parametri, valida credenziali, scrive `terraform.tfvars`, esegue apply |
| `terraform/aws/variables.tf` | Tutte le definizioni di variabile con default |
| `terraform/aws/main.tf` | VPC, subnet, security group, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL endpoint e IP dei nodi |

### Passi

1. Scarica `Provisa.AppImage` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases)

2. Caricala su un bucket S3 nel tuo account AWS:

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Assicurati che le credenziali AWS siano disponibili nella tua shell (una qualsiasi tra):
   - Variabili d'ambiente: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Profilo nominato: `export AWS_PROFILE=my-profile`
   - Sessione SSO attiva: `aws sso login`

4. (Opzionale) Se vuoi accesso SSH ai nodi, crea una coppia di chiavi EC2 nella tua regione target e annota il nome della coppia di chiavi

5. Esegui il wrapper di deploy:

   ```bash
   bash terraform/deploy.sh
   ```

6. Rispondi alle domande del wizard (vedi tabella di riferimento sotto). Lo script verifica che l'AppImage esista in S3 prima di procedere e abortisce se non esiste

7. Rivedi il riepilogo del deployment e conferma

8. Terraform provisiona tutta l'infrastruttura (~5–10 minuti). Dopo l'apply, lo script stampa:

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Opzionale) Punta i record DNS verso i nomi DNS di ALB e NLB

10. Verifica:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Domande del wizard

| Domanda | Default | Note |
| ---------- | --------- | ------- |
| Provider cloud | — | Solo AWS oggi |
| Credenziali AWS | — | Controlla prima una sessione attiva |
| Regione | `us-east-1` | |
| Numero nodi | `2` | 1 = solo primary, nessun LB; 2+ = primary + secondary + ALB/NLB |
| Tipo istanza | `m7i.2xlarge` | Vedi guida al sizing sotto |
| Dimensione volume root | `100 GB` | Per nodo |
| Budget RAM | `0` (tutta la RAM) | Determina il numero di worker Trino per nodo |
| Bucket S3 | — | Verificato live prima di procedere |
| Chiave S3 | `releases/Provisa.AppImage` | |
| Accesso SSH | No | Richiede nome coppia di chiavi esistente + CIDR admin |
| CIDR VPC | `10.0.0.0/16` | |

### Guida al sizing delle istanze

| Tipo | vCPU | RAM | Worker Trino/nodo | Caso d'uso |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / dataset piccoli |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Produzione piccola |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Produzione media |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Produzione grande |

Tutti i nodi contribuiscono worker a un unico coordinator sul primary (REQ-028). Un cluster a 3 nodi `m7i.4xlarge` produce 6 worker Trino totali.

### Cosa viene provisionato

- VPC con due subnet pubbliche su due availability zone (REQ-564)
- Security group: gruppo LB (ingresso pubblico su 8000/8815), gruppo nodi (LB → nodi, intra-cluster, SSH opzionale)
- Ruolo IAM + instance profile con S3 GetObject sul bucket AppImage
- Istanza EC2 primary — esegue il primo avvio in modalità `--non-interactive --role primary`
- Istanze EC2 secondary (node_count − 1) — eseguono il primo avvio in modalità `--non-interactive --role secondary --primary-ip <primary private IP>`; dipendono dal completamento del primary
- ALB sulla porta 8000 — API HTTP, health-check `/health` (REQ-560)
- NLB sulla porta 8815 — Arrow Flight / gRPC (REQ-143)
- Entrambi i LB si collegano a tutti i nodi

### Checklist prerequisiti

- [ ] Permessi IAM: EC2 full, ELB full, VPC full, creazione ruolo IAM, S3 GetObject sul bucket AppImage
- [ ] `Provisa.AppImage` caricato su S3
- [ ] I nodi EC2 hanno accesso S3 in uscita (internet diretto o S3 VPC gateway endpoint)
- [ ] Coppia di chiavi EC2 esistente nella regione target (se serve SSH)
- [ ] Terraform ≥ 1.5 installato in locale
- [ ] Record DNS pianificati per ALB / NLB (opzionale ma consigliato)
- [ ] Certificato ACM pronto se è richiesto HTTPS (non incluso nel Terraform base)

### Segreti

Nessun segreto è incorporato in Terraform. L'AppImage genera credenziali durante il primo avvio e le scrive in `~/.provisa/config.yaml` su ogni nodo (REQ-563). Per la produzione, recupera il token admin dal nodo primary dopo il deployment:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quando usarlo

Il tuo team già opera un cluster Kubernetes e vuole che Provisa partecipi a quel modello operativo (REQ-056). Se stai valutando Provisa o eseguendo il deployment on-premises senza un cluster esistente, il percorso AppImage è più semplice.

Nota: l'AppImage Provisa non può essere eseguita dentro un pod Kubernetes — richiede FUSE e un daemon Docker rootless, non disponibili nei profili di sicurezza pod standard.

### Passi

1. Conferma l'accesso al cluster:

   ```bash
   kubectl cluster-info
   ```

2. Effettua il pull e il mirroring delle immagini sul tuo registry interno (richiesto per ambienti air-gapped o soggetti a scansione; salta se effettui il pull direttamente da registry pubblici) (REQ-294):

   | Immagine | Usata per |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Coordinator + worker del motore di federazione (REQ-169) |
   | `postgres:16` | PostgreSQL in-cluster (se `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer in-cluster (se `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis in-cluster (se `redis.enabled` e nessun `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO in-cluster (se `minio.enabled`) (REQ-029) |

   Per ambienti con scansione di registry:
   - Push di ogni immagine al tuo registry di staging
   - Esegui il tuo scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) e ottieni l'approvazione
   - Promuovi al tuo registry interno di produzione

3. Decidi prima di installare:
   - **PostgreSQL** — in-cluster (`postgresql.enabled: true`) o gestito esterno (`postgresql.host`)? Consigliato esterno per la produzione
   - **Redis** — in-cluster o esterno (`redis.host`)? Cambia la password di default (`redis.password`)
   - **MinIO / S3** — MinIO in-cluster o S3 nativo? Per AWS, usa S3 con un ruolo IAM
   - **Segreti** — passa via `--set` per la valutazione; usa External Secrets o Vault Agent per la produzione

4. Installa il chart:

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Se usi un registry interno, aggiungi override immagine:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Verifica che i pod siano in esecuzione:

   ```bash
   kubectl get pods -n provisa
   ```

6. Controlla l'API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Opzionale) Abilita ingress per accesso esterno — imposta `ingress.enabled: true` e configura il tuo ingress controller

### Checklist prerequisiti

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Storage class che supporta PVC `ReadWriteOnce` (per servizi stateful in-cluster)
- [ ] Immagini disponibili per il cluster (registry pubblico o interno)
- [ ] Endpoint + credenziali PostgreSQL (se esterno)
- [ ] Endpoint + credenziali Redis (se esterno)
- [ ] Bucket S3 + credenziali o ruolo IAM
- [ ] Token admin scelto
- [ ] Ingress controller configurato (se serve accesso esterno)

### Valori chiave

| Valore | Default | Descrizione |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Repliche API Provisa (stateless) (REQ-057) |
| `config.pgHost` | `postgres` | Host PostgreSQL |
| `config.pgPassword` | | Password PostgreSQL |
| `config.adminToken` | | Bearer token API admin |
| `redis.enabled` | `true` | Deploy StatefulSet Redis in-cluster (REQ-371) |
| `redis.host` | `""` | Imposta per usare Redis esterno |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | Cambiala |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Deploy motore di federazione (REQ-028) |
| `trino.workers` | `2` | Repliche worker del motore di federazione (REQ-056) |
| `postgresql.enabled` | `true` | Deploy PostgreSQL in-cluster (REQ-169) |
| `postgresql.host` | `""` | Imposta per usare PostgreSQL esterno |
| `minio.enabled` | `true` | Deploy MinIO in-cluster (REQ-029) |
| `s3.endpoint` | | URL endpoint compatibile S3 |
| `s3.bucket` | `provisa-results` | Bucket per il redirect di risultati grandi (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Abilita ingress |

### Scaling

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

I worker del motore di federazione scalano indipendentemente — più worker aumentano throughput e capacità di query concorrenti (REQ-056). (REQ-057)

### Aggiornamento della config

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Alta disponibilità e recupero

Provisa applica un modello di recupero a due livelli su tutte le modalità di deployment (REQ-703):

- **Livello 1 — errori transitori.** Le operazioni di lettura ritentano fino a 30 secondi su errori transitori usando backoff esponenziale con full jitter. Regola il budget con `PROVISA_RETRY_BUDGET_SECS`. Le operazioni di scrittura non vengono mai ritentate internamente, e gli errori di memoria non sono mai ritentabili.
- **Livello 2 — fallimento di componente.** Un watcher interno del motore rileva e riavvia i componenti software falliti entro 2–3 minuti.

I fallimenti a livello di macchina e cluster restano responsabilità dell'operatore — provisiona nodi ridondanti e un load balancer (percorsi Terraform e Helm sopra) per la tolleranza alla perdita di nodi.

## Dipendenze del motore di federazione

I motori di federazione warehouse richiedono pacchetti Python e componenti a livello di sistema oltre all'installazione di default di Provisa. Tutti i pacchetti Python elencati qui sono dichiarati in `pyproject.toml` e installati come parte della `pip install provisa` o `pip install -e .` standard [tool-verified: `pyproject.toml` lines 44–52].

I pacchetti Python vengono spediti con l'installazione di default di Provisa — nessun extra opzionale richiesto per alcun motore warehouse. Gli elementi a livello di sistema (driver ODBC, CLI cloud, chiavi service-account) devono essere installati separatamente.

### Pacchetti Python (già nelle dipendenze core)

[tool-verified: `pyproject.toml` lines 41–52]

| Pacchetto | Motore | Scopo |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Connessione SQL warehouse; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Connessione + `fetch_arrow_table` Arrow-native (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Esecuzione query |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API per letture Arrow-native |
| `google-cloud-storage` | BigQuery | Staging GCS per link a tabelle esterne |
| `pyodbc` | Fabric, Synapse | Connessione ODBC a endpoint T-SQL |
| `azure-identity` | Fabric, Synapse | Token Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Letture columnar HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Pin di compatibilità — `google-cloud-*` e OTel condividono un runtime protobuf; `<7` li mantiene allineati |
| `grpcio-status<1.82` | gRPC | Allineato con il pin `protobuf<7` |

### Requisiti a livello di sistema

Questi non sono pacchetti Python — devono essere installati sull'host o container che esegue Provisa.

**Microsoft Fabric e Azure Synapse (ODBC)**

`pyodbc` si connette attraverso il Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Il driver deve essere installato sull'host — non via pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

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

Provisa rileva il driver automaticamente. Per sovrascrivere il nome del driver (per installazioni non standard), imposta:

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Autenticazione Azure AD (Fabric e Synapse)**

Entrambi i motori si autenticano via `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` controlla le fonti di credenziali in ordine: variabili d'ambiente, workload identity, managed identity, VS Code, `az login`, e altre.

Per lo sviluppo locale, `az login` è il percorso più semplice:

```bash
az login
```

Per la produzione, usa managed identity (su VM Azure o AKS) — nessuna gestione di credenziali necessaria. Per l'autenticazione con service-principal, imposta:

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (service account)**

`google-cloud-bigquery` usa Application Default Credentials. Per lo sviluppo locale, punta a un file chiave service-account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Per la produzione su GCP (Cloud Run, GKE con Workload Identity, Compute Engine), la libreria rileva automaticamente il service account collegato — nessuna variabile d'ambiente necessaria.

Il service account necessita di:

- `roles/bigquery.dataViewer` — leggere dati
- `roles/bigquery.jobUser` — eseguire query
- `roles/bigquery.dataEditor` — creare tabelle esterne (per ATTACH)
- `roles/storage.objectViewer` — leggere oggetti GCS per tabelle esterne

**Databricks (certificato CA in ambienti proxy di sviluppo)**

Se Provisa gira dietro un proxy che intercetta TLS (Charles, mitmproxy, proxy aziendali), il connettore SQL Databricks potrebbe rifiutare il certificato del proxy. Passa un bundle CA personalizzato:

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Il connettore Databricks eredita questo da `requests` — nessuna variabile d'ambiente specifica per Databricks è necessaria.

### Checklist per motore

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` installato (default)
- [ ] URL motore con `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Token di accesso personale o token service principal
- [ ] `REQUESTS_CA_BUNDLE` impostato se dietro proxy che intercetta TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` installato (default)
- [ ] URL motore: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` in `PROVISA_ENGINE_URL` o `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installati (default)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` impostato (dev) o workload identity configurata (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` impostato se il progetto non può essere inferito dal service account
- [ ] Il service account ha i ruoli BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` installati (default)
- [ ] Driver di sistema `msodbcsql18` installato
- [ ] `FABRIC_SQL_SERVER` e `FABRIC_DATABASE` impostati
- [ ] Autenticazione Azure AD: `az login` (dev) o managed identity / service principal (prod)
- [ ] `FABRIC_WORKSPACE_ID` impostato se si usano link esterni a object-storage

**Azure Synapse** (REQ-989)

- [ ] Gli stessi requisiti Python + sistema di Fabric
- [ ] `SYNAPSE_SQL_SERVER` e `SYNAPSE_DATABASE` impostati
- [ ] Stessa configurazione di autenticazione Azure AD di Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` installato (default)
- [ ] URL motore: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` in `federation_hints` per TLS (porta 8443)

---

## Variabili d'ambiente

| Variabile | Default | Scopo |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Password PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Percorso del file di config (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Abilita il redirect verso S3 per risultati grandi (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Soglia di conteggio righe per il redirect (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL endpoint compatibile S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL URL presigned (secondi) (REQ-141) |
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |
| `REDIS_PASSWORD` | | Password Redis |
| `REDIS_TLS` | `false` | Abilita TLS per Redis |
| `TRINO_HOST` | `localhost` | Host coordinator del motore di federazione Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | Porta HTTP coordinator del motore di federazione Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Chiave del motore di federazione attivo (REQ-989); sovrascrive la config persistita |
| `PROVISA_ENGINE_URL` | | URL di connessione per motori URL-driven (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Override dell'URL dello store di materializzazione; di default lo store proprio del motore |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Nome del driver ODBC per Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Percorso del file JSON chiave service-account GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | ID progetto GCP (BigQuery; inferito dal service account se non impostato) |
| `FABRIC_SQL_SERVER` | | Hostname endpoint analytics SQL di Microsoft Fabric |
| `FABRIC_DATABASE` | | Nome database Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID workspace Fabric (richiesto per shortcut object-storage esterni) |
| `SYNAPSE_SQL_SERVER` | | Hostname pool SQL dedicato o serverless di Azure Synapse |
| `SYNAPSE_DATABASE` | | Nome database Synapse |
| `AZURE_TENANT_ID` | | Tenant Azure AD (autenticazione service-principal per Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | ID client applicazione Azure AD |
| `AZURE_CLIENT_SECRET` | | Secret client applicazione Azure AD |
| `REQUESTS_CA_BUNDLE` | | Percorso bundle CA personalizzato (connettore Databricks, proxy TLS dev) |

---

## Comandi CLI

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

### Workflow di promozione config (dev → test → prod)

Tutte le impostazioni specifiche per ambiente (stringhe di connessione, segreti, porte) appartengono a variabili d'ambiente o secret manager — non alla config esportata. Lo YAML esportato cattura il tuo modello semantico: origini, domini, ruoli, viste. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


Vedere anche: [Ambienti](environments.md) spiega come gestire copie nominate e isolate per schema del modello governato.
