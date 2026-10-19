# Deployment

## Scegliere un percorso di deployment {: #choosing-a-deployment-path }

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

Entrambi sono enterprise-grade. Il percorso VM/AppImage è più semplice: nessun cluster da provisionare, nessuna policy CNI o RBAC da configurare, e l'AppImage è interamente self-contained. Si integra naturalmente negli strumenti di gestione server esistenti (Ansible, Puppet, agenti Datadog, forwarder Splunk, ecc.).

Scegliete Kubernetes solo se il vostro team gestisce già un cluster K8s e vuole che Provisa si inserisca in quel modo di operare (rolling deploy, HPA, osservabilità unificata). Entrambi offrono le stesse capacità; Kubernetes aggiunge carico operativo.

### Acquisizione immagini e scansione di sicurezza {: #image-acquisition-and-security-scanning }

Ogni percorso di produzione ha bisogno degli artefatti Provisa prima che qualsiasi deployment possa partire. "Air-gapped" descrive ciò che accade al momento dell'installazione sulla macchina di destinazione, quindi gli artefatti si procurano prima.

**Programmi di installazione per macOS e Windows:** Scaricateli dalla [pagina delle release su GitHub](https://github.com/provisa/provisa/releases). Sono completi e non serve internet dopo il download. Sono pensati per sviluppo e valutazione, quindi non è previsto un controllo di scansione delle immagini.

**Percorso AppImage:** Scaricatelo dalla [pagina delle release su GitHub](https://github.com/provisa/provisa/releases) e copiatelo sulla macchina di destinazione. L'AppImage include ogni immagine di componente come tarball dentro un filesystem squashfs, che la maggior parte degli scanner di registry non può ispezionare sul posto. Chiedete al vostro team commerciale Provisa i digest delle immagini dei componenti per verificarli voi stessi con il vostro scanner.

**Percorso Terraform:** Caricate l'AppImage su S3 prima di eseguire `terraform/deploy.sh`. I nodi EC2 lo scaricano all'avvio tramite il loro ruolo IAM, quindi hanno bisogno di accesso in uscita a S3, diretto o tramite un endpoint gateway VPC. Applicate la stessa policy di scansione del percorso AppImage.

**Percorso Helm / Kubernetes:** Caricate le singole immagini in un registry raggiungibile dal cluster. Questo percorso è il più compatibile con la scansione basata su registry (Prisma Cloud, Aqua, Trivy, AWS Inspector), perché le immagini sono oggetti che gli scanner capiscono già. Per un cluster isolato, replicate le immagini in un registry interno e sovrascrivete i riferimenti in `values.yaml`.

---

## Sviluppo (da sorgente) {: #development-from-source }

### Consigliato: `start-ui.sh` {: #recommended-start-uish }

Il modo più semplice per eseguire Provisa da sorgente. Avvia tutta l'infrastruttura, l'API backend, e il server dev della UI in un unico comando. Ctrl+C spegne tutto in modo pulito.

**Prerequisiti:** Docker Desktop, Node.js, virtualenv Python in `.venv/`

```bash
./start-ui.sh
```

Cosa fa:

- Avvia `docker-compose.core.yml` + `docker-compose.dev.yml` (tutti i servizi core + demo) e attende che siano healthy
- Semina Kafka con dati demo
- Sincronizza le dipendenze Python da `.venv/`
- Avvia l'API backend sulla porta 8001 (log in `.logs/server.log`)
- Avvia il server dev Vite della UI sulla porta 3000
- Stampa gli URL e attende; Ctrl+C ferma tutto e smonta compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Opzioni:**

`--reset-volumes` — Esegue `docker compose down -v` prima di avviare, distruggendo tutti i volumi Docker (dati PostgreSQL, oggetti MinIO, stato Redis, ecc.). Usalo quando vuoi uno stato completamente pulito: dopo una modifica dello schema durante lo sviluppo, o quando Docker è andato in crash e ha lasciato volumi corrotti. **Tutti i dati andranno persi.**

`--observability` — Aggiunge strumentazione completa di tracing e metriche. Scarica l'OpenTelemetry Java agent e applica una patch a `jvm.config` di Trino per caricarlo, strumenta il backend Provisa con export OTLP, e avvia l'OTel collector, Prometheus, Tempo, e Grafana (`http://localhost:3100`). La patch a `jvm.config` viene automaticamente ripristinata su Ctrl+C.

### Passi manuali (solo backend, senza UI) {: #manual-steps-backend-only-no-ui }

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

### Stack completo (Provisa in container) {: #full-stack-provisa-in-container }

Per eseguire l'API come container invece che sull'host:

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Servizi {: #services }

**Core (`docker-compose.core.yml`) — sempre richiesto:**

| Servizio | Porta | Scopo |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Metadati di configurazione + catalogo Iceberg |
| PgBouncer | 6432 | Connection pooling |
| Motore di federazione | 8080 | Federazione query |
| Redis | 6379 | Cache dei risultati query |
| MinIO | 9000/9001 | Object storage compatibile S3 |

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
| OpenTelemetry Collector | — | Raccolta trace (con `--observability`) |
| Prometheus | 9090 | Metriche (con `--observability`) |
| Tempo | — | Storage trace (con `--observability`) |
| Grafana | 3100 | Dashboard (con `--observability`) |

Gli store, il motore di federazione e questi servizi di telemetria pubblicano su `PROVISA_STORE_BIND_IP` (`127.0.0.1` se non impostato), con password generate per il deployment. Vedi [Hardening del deployment](security.md#deployment-hardening).

### Backend di telemetria (`otlp2sql`) {: #telemetry-backend-otlp2sql }

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

**Con `trino://`:** il dialetto Trino di SQLAlchemy emette DDL Trino e `INSERT` validi, quindi può fare da backend `otlp2sql`, ma lo consigliamo solo per tassi di ingestione bassi. Ogni svuotamento di batch diventa un `INSERT` Trino distribuito più uno snapshot Iceberg, quindi la telemetria ad alta frequenza produce molti piccoli file e snapshot e richiede comunque un `ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` periodico, che `otlp2sql` non esegue. Inoltre mette il motore di query nel percorso critico dell'ingestione.

Per telemetria ad alto volume verso Trino/Iceberg, usa `otlp2parquet` invece: esso
scrive parquet su object storage senza passare per Trino, e una compattazione
Trino pianificata unisce i file raw nelle tabelle Iceberg live. Per un unico
motore che gestisce sia ingest ad alto rate che compattazione, preferisci ClickHouse.

Punta gli exporter OTLP dell'app e di Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) verso
l'endpoint `otlp2sql`, e registra il dominio ops contro lo stesso
`PROVISA_OPS_DB_URL` così che legga ciò che il receiver ha scritto.

---

## Installer macOS {: #macos-installer }

Per postazioni di sviluppo e valutazione. Completamente isolato; dopo il download non serve internet.

Il programma di installazione di base è un'**installazione nativa**: il motore di federazione DuckDB, un piano di controllo PostgreSQL incorporato (pgserver) e una cache in memoria (fakeredis), senza Docker, VM, Trino, Redis né MinIO. Il motore di federazione si sceglie nella procedura guidata: DuckDB (nativo, predefinito), Trino su Docker o un motore esterno. L'osservabilità è un'autotelemetria sempre attiva, visibile in Admin; lo stack Docker con collector, Prometheus e Grafana è una dimostrazione facoltativa separata e non la accende né la spegne. Il pacchetto di dati demo è facoltativo e spento per impostazione predefinita. Trino, lo stack di osservabilità Docker e la demo sono componenti aggiuntivi pesanti. Vengono cercati prima in locale (la directory accanto al programma di installazione, i volumi montati, `~/Downloads`) e solo dopo scaricati dalla release su GitHub, così le aziende possono preparare in anticipo i tarball per installazioni isolate.

### Passi {: #steps }

1. Scarica `Provisa-<version>-macOS.dmg` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases)
2. Apri il DMG e trascina **Provisa.app** su `/Applications`
3. Fai doppio clic su **Provisa.app**. La configurazione al primo avvio viene eseguita una volta, e il wizard offre le scelte di motore, osservabilità, e demo di cui sopra
4. Apri Terminal:

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

### Persistenza dei dati {: #data-persistence }

Tutti i dati sono memorizzati in `~/.provisa/`. Per rimuovere tutto: `provisa uninstall`.

---

## Installer Windows {: #windows-installer }

Per postazioni di sviluppo e valutazione. Completamente isolato; dopo il download non serve internet.

Come macOS, l'installer Windows base è un **tier nativo**: un runtime Python standalone + wheel provisa + DuckDB/pg_duckdb + control plane PostgreSQL embedded (pgserver), senza spedire Docker, VM, o immagini container. Il motore di federazione (Trino), lo stack di osservabilità, e il pacchetto dati demo vengono aggiunti in seguito tramite installer separati a strati, in ordine: l'installer Container (`Provisa-Container-<version>.exe`, che aggiunge WSL2 + containerd + Trino), poi l'installer Obs (richiede il tier container), poi l'installer Demo (richiede Core + Obs). La guida al primo avvio spiega come inizializzare il motore di federazione eseguendo l'installer Container.

### Passi {: #steps_1 }

1. Scarica `Provisa-<version>-windows-x64.exe` dalla [pagina GitHub releases](https://github.com/provisa/provisa/releases)
2. Esegui l'installer. Non richiede diritti admin e installa in `%LOCALAPPDATA%\Programs\Provisa\`
3. Apri **Provisa First Launch** dal menu Start. La configurazione nativa viene eseguita una volta e stampa la guida ai passi successivi per gli add-on a strati
4. Apri un nuovo terminale:

   ```text
   provisa status
   provisa open
   ```

### Persistenza dei dati {: #data-persistence_1 }

Tutti i dati sono memorizzati in `%USERPROFILE%\.provisa\`.

---

## Linux AppImage — VM a nodo singolo o multi-nodo {: #linux-appimage-single-or-multi-node-vm }

### Cos'è {: #what-it-is }

`Provisa.AppImage` è un unico eseguibile self-contained che impacchetta:

- Un daemon Docker rootless (`dockerd-rootless.sh` + `rootlesskit`), quindi non servono Docker di sistema né root
- Tutte le tarball delle immagini container (PostgreSQL, PgBouncer, MinIO, Redis, Motore di federazione, API Provisa)
- Il wrapper CLI Provisa e lo script di configurazione al primo avvio

L'immagine Provisa viene costruita al momento del packaging, e il codice sorgente Python non è mai incluso.

### Quando usarlo {: #when-to-use }

- Bare metal on-premises o VM (nodo singolo o multi-nodo)
- VM cloud senza un cluster K8s
- Ambienti air-gapped
- Quando vuoi operazioni più semplici di Kubernetes

---

### Passi — Nodo singolo {: #steps-single-node }

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
   - **Porta API** → default `8000`
5. La configurazione carica tutte le immagini container (~2–5 minuti), scrive la config, e avvia i servizi
6. Verifica:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Passi — Multi-nodo (Primary) {: #steps-multi-node-primary }

Esegui questi passi prima sul nodo primary. I secondary devono essere configurati dopo che il primary è in esecuzione.

1. Scarica e trasferisci `Provisa.AppImage` sulla macchina primary
2. Apri le porte firewall richieste solo ai secondary (si connettono in ingresso su queste):

   | Porta | Servizio |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordinator motore di federazione |
   | 8000 | API Provisa |

3. Rendi eseguibile ed esegui con gli store pubblicati sull'indirizzo privato del nodo, dove i secondary li raggiungono. Un nodo senza secondary li pubblica solo su `127.0.0.1`:

   ```bash
   chmod +x Provisa.AppImage
   PROVISA_STORE_BIND_IP=<this node's private IP> ./Provisa.AppImage
   ```

4. Il wizard di setup chiede:
   - **Ruolo** → seleziona `primary`
   - **Budget RAM**, **hostname**, **porta API** → rispondi come per il nodo singolo
5. Dopo il completamento della configurazione, annota il **IP privato** di questa macchina. I secondary ne hanno bisogno, insieme alle credenziali degli store generate dal primary: `~/.provisa/store-credentials.env` (`PROVISA_PG_PASSWORD`, `PROVISA_REDIS_PASSWORD`, `PROVISA_MINIO_ROOT_PASSWORD`, `PROVISA_GRAFANA_ADMIN_PASSWORD`, `PROVISA_TRINO_SHARED_SECRET`). Copia quel file su ogni secondary tramite un canale sicuro
6. Il wizard stampa un blocco upstream nginx; salvalo per la configurazione del tuo load balancer
7. Verifica:

   ```bash
   provisa status
   curl -k https://localhost:8000/health   # -k: the node generates a self-signed certificate unless PROVISA_TLS_CERT/KEY supply one
   ```

---

### Passi — Multi-nodo (ogni Secondary) {: #steps-multi-node-each-secondary }

Ripeti questi passi su ogni nodo aggiuntivo dopo che il primary è in esecuzione e raggiungibile.

1. Scarica e trasferisci `Provisa.AppImage` sulla macchina secondary
2. Conferma che il secondary possa raggiungere il primary:

   ```bash
   curl -k https://<primary-ip>:8000/health
   ```

3. Rendi eseguibile ed esegui con le credenziali degli store del primary nel suo ambiente (il file copiato dal primary). Un secondary che non ne riceve non si avvia:

   ```bash
   chmod +x Provisa.AppImage
   set -a; . ./store-credentials.env; set +a
   ./Provisa.AppImage
   ```

4. Il wizard di setup chiede:
   - **Ruolo** → seleziona `secondary`
   - **IP primary** → inserisci l'IP del nodo primary (la connettività viene verificata live)
   - **Budget RAM**, **hostname**, **porta API** → rispondi come sopra
5. La configurazione carica un set ridotto di immagini (senza PostgreSQL, PgBouncer, MinIO e Redis, che girano solo sul primary), avvia l'API Provisa e un worker del motore di federazione
6. Verifica:

   ```bash
   provisa status
   curl -k https://localhost:8000/health
   ```

7. Aggiungi questo nodo all'upstream del tuo load balancer

---

### Topologia primary / secondary {: #primary-secondary-topology }

Il **nodo primary** esegue tutti i servizi singleton:

| Servizio | Perché singleton |
| --------- | --------------- |
| PostgreSQL | Schema condiviso, config app, modello semantico |
| Redis | Cache condivisa dei risultati query e stato subscription |
| MinIO | Store oggetti condiviso per risultati redirect e snapshot MV |
| Coordinator motore di federazione | Tutti i worker (primary + secondary) si registrano qui |

I **nodi secondary** eseguono solo:

- API Provisa — stateless; legge tutta la config da PostgreSQL sul primary all'avvio
- Worker del motore di federazione — si auto-registra con il coordinator sul primary

Tutto lo stato applicativo passa attraverso il PostgreSQL del primary. Nessuna sincronizzazione manuale richiesta.

---

### Primo avvio non interattivo (automatizzato) {: #non-interactive-automated-first-launch }

Per Terraform, cloud-init, o Ansible, passa flag invece di rispondere ai prompt:

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

La modalità non interattiva installa una unit systemd (`/etc/systemd/system/provisa.service`) per l'avvio al boot.

| Flag | Descrizione |
| ------ | ------------- |
| `--non-interactive` | Salta tutti i prompt; installa la unit systemd |
| `--role primary\|secondary` | Ruolo del nodo |
| `--primary-ip <ip>` | IP del nodo primary (richiesto per secondary) |
| `--ram-gb <n>` | RAM da allocare (0 = tutta la disponibile) |

---

## Deployment VM Cloud — Terraform (AWS) {: #cloud-vm-deployment-terraform-aws }

Un solo comando interattivo crea un cluster Provisa completo multi-nodo su AWS: VPC, security group, istanze EC2, ALB e NLB.

### File {: #files }

| File | Scopo |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interattivo — raccoglie parametri, valida credenziali, scrive `terraform.tfvars`, esegue apply |
| `terraform/aws/variables.tf` | Tutte le definizioni di variabile con default |
| `terraform/aws/main.tf` | VPC, subnet, security group, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL endpoint e IP dei nodi |

### Passi {: #steps_2 }

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
   api_endpoint      = "https://<alb-dns>:8000"
   protocol_endpoints = { flight = "<alb-dns>:8815", ... }
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

9. (Opzionale) Punta i record DNS verso i nomi DNS di ALB e NLB

10. Verifica:

    ```bash
    curl http://<api_endpoint>/health
    ```

### Domande del wizard {: #wizard-questions }

| Domanda | Default | Note |
| ---------- | --------- | ------- |
| Provider cloud | — | Solo AWS |
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

### Guida al sizing delle istanze {: #instance-sizing-guide }

| Tipo | vCPU | RAM | Worker Trino/nodo | Caso d'uso |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / dataset piccoli |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Produzione piccola |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Produzione media |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Produzione grande |

Tutti i nodi contribuiscono worker a un unico coordinator sul primary. Un cluster a 3 nodi `m7i.4xlarge` produce 6 worker Trino totali.

### Cosa viene provisionato {: #what-gets-provisioned }

- VPC con due subnet pubbliche su due availability zone
- Security group: gruppo LB (ingresso pubblico su 8000/8815), gruppo nodi (LB → nodi, intra-cluster, SSH opzionale)
- Ruolo IAM + instance profile con S3 GetObject sul bucket AppImage
- Istanza EC2 primary — esegue il primo avvio in modalità `--non-interactive --role primary`
- Istanze EC2 secondary (node_count − 1) — eseguono il primo avvio in modalità `--non-interactive --role secondary --primary-ip <primary private IP>`; dipendono dal completamento del primary
- ALB sulla porta 8000 — API HTTP, health-check `/health`
- NLB sulla porta 8815 — Arrow Flight / gRPC
- Entrambi i LB si collegano a tutti i nodi

### Checklist prerequisiti {: #prerequisites-checklist }

- [ ] Permessi IAM: EC2 full, ELB full, VPC full, creazione ruolo IAM, S3 GetObject sul bucket AppImage
- [ ] `Provisa.AppImage` caricato su S3
- [ ] I nodi EC2 hanno accesso S3 in uscita (internet diretto o S3 VPC gateway endpoint)
- [ ] Coppia di chiavi EC2 esistente nella regione target (se serve SSH)
- [ ] Terraform ≥ 1.5 installato in locale
- [ ] Record DNS pianificati per ALB / NLB (opzionale ma consigliato)
- [ ] Certificato ACM pronto se è richiesto HTTPS (non incluso nel Terraform base)

### Segreti {: #secrets }

Le password degli store inclusi (PostgreSQL, Redis, MinIO, l'admin di Grafana) e il segreto condiviso del cluster Trino sono generati da Terraform (`random_password`) e consegnate a ogni nodo, che le conserva nel proprio file di ambiente systemd, modalità 600. Sono contenute nello stato di Terraform, quindi proteggi lo stato come proteggeresti le password. Senza Terraform, il launcher di ogni nodo le genera in `~/.provisa/store-credentials.env`. L'AppImage genera le restanti credenziali durante il primo avvio e le scrive in `~/.provisa/config.yaml` su ogni nodo. Per la produzione, recupera il token admin dal nodo primary dopo il deployment:

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quando usarlo {: #when-to-use_1 }

Il tuo team già opera un cluster Kubernetes e vuole che Provisa partecipi a quel modello operativo. Se stai valutando Provisa o eseguendo il deployment on-premises senza un cluster esistente, il percorso AppImage è più semplice.

Nota: l'AppImage di Provisa non può girare dentro un pod Kubernetes. Richiede FUSE e un daemon Docker rootless, e i profili di sicurezza standard dei pod non consentono né l'uno né l'altro.

### Passi {: #steps_3 }

1. Conferma l'accesso al cluster:

   ```bash
   kubectl cluster-info
   ```

2. Effettua il pull e il mirroring delle immagini sul tuo registry interno (richiesto per ambienti air-gapped o soggetti a scansione; salta se effettui il pull direttamente da registry pubblici):

   | Immagine | Usata per |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Coordinator + worker del motore di federazione |
   | `postgres:16` | PostgreSQL in-cluster (se `postgresql.enabled`) |
   | `edoburu/pgbouncer:latest` | PgBouncer in-cluster (se `pgbouncer.enabled`) |
   | `redis:7.2` | Redis in-cluster (se `redis.enabled` e nessun `redis.host`) |
   | `minio/minio:latest` | MinIO in-cluster (se `minio.enabled`) |

   Per ambienti con scansione di registry:
   - Push di ogni immagine al tuo registry di staging
   - Esegui il tuo scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) e ottieni l'approvazione
   - Promuovi al tuo registry interno di produzione

3. Decidi prima di installare:
   - **PostgreSQL** — in-cluster (`postgresql.enabled: true`) o gestito esterno (`postgresql.host`)? Consigliato esterno per la produzione
   - **Redis** — in-cluster o esterno (`redis.host`)?
   - **Credenziali degli store** — crea il Secret nominato da `credentials.existingSecret`, con le chiavi `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` e `trino-shared-secret`. Il chart non imposta alcuna password propria e senza di esso non viene renderizzato
   - **TLS** — attivo per impostazione predefinita (`tls.enabled: true`): crea il Secret `kubernetes.io/tls` nominato da `tls.secretName`, oppure imposta `tls.enabled: false`
   - **MinIO / S3** — MinIO in-cluster o S3 nativo? Per AWS, usa S3 con un ruolo IAM
   - **Segreti** — passa via `--set` per la valutazione; usa External Secrets o Vault Agent per la produzione
   - **Cache del coordinator Trino** — un volume da 1Gi richiesto per impostazione predefinita e montato su `/data/trino/cache` (`trino.coordinator.cache.persistence`: `enabled`, `size`, `storageClass`). Vi vengono conservati i risultati di describe di una sorgente Salesforce, così che un coordinator riavviato non spenda di nuovo la quota API dell'organizzazione per descriverla. Disattivatela solo quando nessuna sorgente ne ha bisogno

4. Fornite al deployment la sua chiave master. La chiave cifra il vault dei segreti; ogni pod dell'API deve
   avere la stessa, e deve sopravvivere a qualsiasi pod. Il chart non viene renderizzato senza di essa.

   ```bash
   kubectl create namespace provisa
   kubectl create secret generic provisa-master-key --namespace provisa \
     --from-literal=master-key="$(openssl rand -base64 32)"
   ```

   Conservate una copia della chiave fuori dal cluster. Un vault scritto con una chiave poi perduta
   non può essere aperto. L'alternativa a un Secret è un volume di dati persistente
   (`encryption.dataVolume.enabled=true`), dove il prodotto conserva la chiave che genera; vedere
   `encryption` in `values.yaml`.

5. Installa il chart:

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

   Il chart non viene renderizzato finché non si sceglie `auth.provider`: `oidc` (un emittente OpenID Connect
   aziendale), `saml` (`auth.saml.idpMetadataUrl` e `auth.saml.publicUrl`), `ldap` (`auth.ldap.*`),
   `local` (solo l'account di emergenza, `auth.breakGlass.*`) o `none` (nessuna autenticazione,
   solo sviluppo). Ogni credenziale proviene da un Secret Kubernetes; `ldap`, `saml` e `local`
   richiedono anche `auth.sessionSecret.existingSecret`, la chiave che firma le sessioni del browser. Vedere `auth`
   in `values.yaml`.

   Il coordinator Trino esegue le istruzioni di ogni organizzazione nel suo gruppo di risorse,
   `global.tenant-<org>`, dal file dei gruppi di risorse fornito con il chart. Le istruzioni che passano per il
   proxy Arrow Flight condividono un gruppo, `global.flight`: il proxy apre la propria connessione Trino
   e non può portare l'organizzazione.

   Se usi un registry interno, aggiungi override immagine:

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

6. Verifica che i pod siano in esecuzione:

   ```bash
   kubectl get pods -n provisa
   ```

7. Controlla l'API:

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

8. (Opzionale) Abilita ingress per accesso esterno: imposta `ingress.enabled: true` e configura il tuo ingress controller

### Checklist prerequisiti {: #prerequisites-checklist_1 }

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Storage class che supporta PVC `ReadWriteOnce` (per servizi stateful in-cluster)
- [ ] Immagini disponibili per il cluster (registry pubblico o interno)
- [ ] Endpoint + credenziali PostgreSQL (se esterno)
- [ ] Endpoint + credenziali Redis (se esterno)
- [ ] Bucket S3 + credenziali o ruolo IAM
- [ ] Token admin scelto
- [ ] Ingress controller configurato (se serve accesso esterno)

### Valori chiave {: #key-values }

| Valore | Default | Descrizione |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Repliche API Provisa (stateless) |
| `config.pgHost` | `postgres` | Host PostgreSQL |
| `config.pgPassword` | | Password PostgreSQL |
| `config.adminToken` | | Bearer token API admin |
| `redis.enabled` | `true` | Deploy StatefulSet Redis in-cluster |
| `redis.host` | `""` | Imposta per usare Redis esterno |
| `redis.port` | `6379` | |
| `credentials.existingSecret` | | Secret che contiene le password degli store (obbligatorio) |
| `tls.enabled` | `true` | Serve ogni endpoint su TLS da `tls.secretName` |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Deploy motore di federazione |
| `trino.workers` | `2` | Repliche worker del motore di federazione |
| `trino.internalTls` | `false` | Cifra il traffico coordinator-worker con un certificato generato dal chart; rallenta le query ad alto shuffle dal 10% a oltre il 100% |
| `postgresql.enabled` | `true` | Deploy PostgreSQL in-cluster |
| `postgresql.host` | `""` | Imposta per usare PostgreSQL esterno |
| `minio.enabled` | `true` | Deploy MinIO in-cluster |
| `s3.endpoint` | | URL endpoint compatibile S3 |
| `s3.bucket` | `provisa-results` | Bucket per il redirect di risultati grandi |
| `ingress.enabled` | `false` | Abilita ingress |

### Scaling

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

I worker del motore di federazione scalano per conto loro, e più worker significano più throughput e più query contemporanee.

### Aggiornamento della config {: #updating-config }

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Alta disponibilità e recupero {: #high-availability-recovery }

Provisa applica un modello di recupero a due livelli su tutte le modalità di deployment:

- **Livello 1 — errori transitori.** Le operazioni di lettura ritentano fino a 30 secondi su errori transitori usando backoff esponenziale con full jitter. Regola il budget con `PROVISA_RETRY_BUDGET_SECS`. Le operazioni di scrittura non vengono mai ritentate internamente, e gli errori di memoria non sono mai ritentabili.
- **Livello 2 — fallimento di componente.** Un watcher interno del motore rileva e riavvia i componenti software falliti entro 2–3 minuti.

I guasti di macchina e di cluster restano responsabilità dell'operatore. Per tollerare la perdita di un nodo, predisponete nodi ridondanti e un load balancer (i percorsi Terraform e Helm sopra).

### Tabella di telemetria i cui metadati mancano {: #telemetry-table-whose-metadata-is-missing }

Sul motore Trino, le tabelle di telemetria (`otel.signals.traces`, `metrics`, `logs`) sono tabelle Iceberg. La registrazione di ciascuna è una riga nel database del piano di controllo, e i suoi metadati e dati sono file nel bucket di telemetria. Quando il bucket è servito senza quei file mentre il database conserva ancora le registrazioni, ogni istruzione sulla tabella fallisce con `ICEBERG_MISSING_METADATA`. Succede quando il disco del bucket non era ancora montato all'avvio dell'object store, o quando il bucket è stato svuotato o sostituito.

In questo caso Provisa non cambia nulla. Ogni esecuzione di compattazione registra un `ERROR` che nomina la tabella e la posizione dei metadati che Trino non è riuscito a leggere (`otel.signals.traces is registered but its metadata cannot be read …`), salta quel segnale e conserva i suoi file grezzi nel bucket per un'esecuzione successiva. Gli altri segnali vengono compattati come di consueto. L'avvio lascia la tabella fuori dai suoi controlli dello schema allo stesso modo.

Recuperala a mano, in quest'ordine:

1. **Scopri se i metadati esistono ancora.** L'`ERROR` nomina la posizione. Su un coordinator Terraform (GCP), verifica che il disco dell'object store sia montato (`findmnt /var/lib/provisa-object-store`) e che il container MinIO sia partito dopo il montaggio. Un MinIO partito prima serve la directory vuota sotto il punto di montaggio; riavvialo una volta montato il disco e la tabella torna leggibile con la sua cronologia intatta.
2. **Se il file dei metadati esiste ma la registrazione punta altrove**, registra di nuovo la tabella nella sua posizione:

   ```sql
   CALL otel.system.register_table(schema_name => 'signals', table_name => 'traces', table_location => 's3://<bucket>/warehouse/signals/<table directory>')
   ```

   Annulla prima la registrazione della riga obsoleta se il nome è occupato (`CALL otel.system.unregister_table(schema_name => 'signals', table_name => 'traces')`).
3. **Se i file del bucket sono davvero persi**, annulla la registrazione della tabella. La successiva esecuzione di compattazione la ricrea, vuota. I segnali che conteneva non possono più essere letti.

Non eseguire `remove_orphan_files` su una tabella in fase di recupero. Elimina i file a cui i metadati correnti non fanno riferimento, e quelli possono essere la cronologia che stai ripristinando.

## Dipendenze del motore di federazione {: #federation-engine-dependencies }

I motori di federazione dei warehouse hanno bisogno di pacchetti Python e di componenti di sistema. I pacchetti Python sono dichiarati in `pyproject.toml` e arrivano con il normale `pip install provisa` o `pip install -e .`, quindi nessun motore di warehouse richiede un extra opzionale.

Gli elementi di sistema (il driver ODBC, le CLI cloud, le chiavi dei service account) vanno installati a parte.

### Pacchetti Python (già nelle dipendenze core) {: #python-packages-already-in-core-dependencies }

| Pacchetto | Motore | Scopo |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Connessione SQL warehouse; Arrow Cloud Fetch |
| `snowflake-connector-python[pandas]` | Snowflake | Connessione + `fetch_arrow_table` Arrow-native |
| `google-cloud-bigquery` | BigQuery | Esecuzione query |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API per letture Arrow-native |
| `google-cloud-storage` | BigQuery | Staging GCS per link a tabelle esterne |
| `pyodbc` | Fabric, Synapse | Connessione ODBC a endpoint T-SQL |
| `azure-identity` | Fabric, Synapse | Token Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Letture columnar HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Pin di compatibilità — `google-cloud-*` e OTel condividono un runtime protobuf; `<7` li mantiene allineati |
| `grpcio-status<1.82` | gRPC | Allineato con il pin `protobuf<7` |

### Requisiti a livello di sistema {: #system-level-requirements }

Non sono pacchetti Python. Installateli sull'host o sul container che esegue Provisa.

**Microsoft Fabric e Azure Synapse (ODBC)**

`pyodbc` si connette tramite il Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Installate il driver sull'host stesso; pip non può installarlo.

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

Entrambi i motori si autenticano via `azure.identity.DefaultAzureCredential`. `DefaultAzureCredential` controlla le fonti di credenziali in ordine: variabili d'ambiente, workload identity, managed identity, VS Code, `az login`, e altre.

Per lo sviluppo locale, `az login` è il percorso più semplice:

```bash
az login
```

In produzione, usate un'identità gestita (su VM Azure o AKS), che non richiede di gestire credenziali. Per l'autenticazione con service principal, impostate:

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

In produzione su GCP (Cloud Run, GKE con Workload Identity, Compute Engine), la libreria usa da sola l'account di servizio collegato, senza variabili d'ambiente.

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

Il connettore Databricks lo eredita da `requests`, quindi non serve alcuna variabile d'ambiente specifica di Databricks.

### Checklist per motore {: #per-engine-checklist }

**Databricks**

- [ ] `databricks-sql-connector` installato (default)
- [ ] URL motore con `http_path`: `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Token di accesso personale o token service principal
- [ ] `REQUESTS_CA_BUNDLE` impostato se dietro proxy che intercetta TLS

**Snowflake**

- [ ] `snowflake-connector-python[pandas]` installato (default)
- [ ] URL motore: `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` in `PROVISA_ENGINE_URL` o `federation_hints`

**BigQuery**

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installati (default)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` impostato (dev) o workload identity configurata (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` impostato se il progetto non può essere inferito dal service account
- [ ] Il service account ha i ruoli BigQuery Data Viewer + Job User

**Microsoft Fabric**

- [ ] `pyodbc` + `azure-identity` installati (default)
- [ ] Driver di sistema `msodbcsql18` installato
- [ ] `FABRIC_SQL_SERVER` e `FABRIC_DATABASE` impostati
- [ ] Autenticazione Azure AD: `az login` (dev) o managed identity / service principal (prod)
- [ ] `FABRIC_WORKSPACE_ID` impostato se si usano link esterni a object-storage

**Azure Synapse**

- [ ] Gli stessi requisiti Python + sistema di Fabric
- [ ] `SYNAPSE_SQL_SERVER` e `SYNAPSE_DATABASE` impostati
- [ ] Stessa configurazione di autenticazione Azure AD di Fabric

**ClickHouse**

- [ ] `clickhouse-connect` installato (default)
- [ ] URL motore: `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` in `federation_hints` per TLS (porta 8443)

---

## Variabili d'ambiente {: #environment-variables }

| Variabile | Default | Scopo |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Password PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Percorso del file di config |
| `PROVISA_REDIRECT_ENABLED` | `false` | Abilita il redirect verso S3 per risultati grandi |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Soglia di conteggio righe per il redirect |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL endpoint compatibile S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL URL presigned (secondi) |
| `REDIS_HOST` | `localhost` | Host Redis |
| `REDIS_PORT` | `6379` | Porta Redis |
| `REDIS_PASSWORD` | | Password Redis |
| `REDIS_TLS` | `false` | Abilita TLS per Redis |
| `TRINO_HOST` | `localhost` | Host coordinator del motore di federazione Trino |
| `TRINO_PORT` | `8080` | Porta HTTP coordinator del motore di federazione Trino |
| `PROVISA_ENGINE` | `duckdb` | Chiave del motore di federazione attivo; sovrascrive la config persistita |
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

## Comandi CLI {: #cli-commands }

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

### Workflow di promozione config (dev → test → prod) {: #config-promotion-workflow-dev-test-prod }

Ogni impostazione specifica di un ambiente (stringhe di connessione, segreti, porte) va in variabili d'ambiente o in un secret manager, fuori dalla configurazione esportata. Lo YAML esportato cattura il vostro modello semantico: origini, domini, ruoli e viste.

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
