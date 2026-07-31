# Guida rapida per sviluppatori

Per valutare Provisa senza compilare dal codice sorgente, consultare la [Guida rapida](index.md) — scaricare il programma di installazione per macOS, Windows o Linux ed eseguire `provisa start`. (REQ-223, REQ-224, REQ-227)

Questa guida è pensata per eseguire Provisa **dal repository** — sviluppo attivo, debug o contributi.

---

## Prerequisiti

- **Docker Desktop** (in esecuzione)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Clonare e configurare

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` crea `.venv/`, installa tutte le dipendenze Python tramite `pip install -e ".[dev]"` e configura i git hook in `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Avviare tutto

```bash
./start-ui.sh
```

Al termine dell'avvio verrà visualizzato:

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Cosa viene avviato:** [tool-verified: start-ui.sh]

- Servizi principali Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Overlay di sviluppo Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- API backend sulla porta 8001 (hot-reload alle modifiche di `provisa/` e `config/`) (REQ-618)
- Server di sviluppo Vite dell'UI sulla porta 3000 (HMR)
- Tracciamento OpenTelemetry e Grafana su `http://localhost:3100`. Lo stack di osservabilità è un profilo docker-compose opzionale `observability` (OTel Collector, Prometheus, Tempo, Grafana), non attivo per impostazione predefinita a livello di piattaforma; `start-ui.sh` lo abilita come comodità dello script di sviluppo, a meno che non venga passato `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** arresta tutto — backend, UI e tutti i servizi Docker — e ripristina eventuali patch di configurazione. (REQ-619)

**Ctrl+R** riavvia solo il backend (utile dopo una modifica di configurazione non rilevata dall'hot-reload). (REQ-619)

### Opzioni

`--no-observability` — Disabilita il tracciamento distribuito. Per impostazione predefinita, `start-ui.sh` scarica l'agente Java OpenTelemetry se non già presente, applica una patch al `jvm.config` di Trino per caricarlo e avvia l'OTel collector, Prometheus, Tempo e Grafana. Passare `--no-observability` per ignorare tutto ciò. La patch a `jvm.config` viene ripristinata con Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Popola Kafka con dati demo dopo che i servizi Docker sono in stato integro. Non eseguito per impostazione predefinita. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Lascia i servizi Docker Compose in esecuzione dopo Ctrl+C invece di chiamare `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Cancella tutti i volumi Docker e riavvia con uno stato pulito. Utile per il ripristino da un crash di Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Avvia origini dati demo aggiuntive (schema PostgreSQL pet-store, mock OpenAPI petstore, SQLite e un GraphQL remoto). Popola automaticamente utenti e ordini petstore. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Abilita un provider di identità per l'autenticazione. Senza questo flag, il backend viene eseguito senza provider di autenticazione e tutte le richieste vengono trattate come `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Connettere un'origine dati

Provisa legge la configurazione da `config/`. Aggiungere un file di origine — ad esempio `config/sources/my-db.yaml`:

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

Impostare la variabile d'ambiente e il backend la rileverà al successivo reload:

```bash
export MY_DB_PASSWORD=secret
```

Per il riferimento YAML completo e tutti i tipi di origine supportati, vedere [docs/configuration.md](configuration.md).

---

## 4. Eseguire la prima query

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

Non è richiesta alcuna autenticazione quando non è presente una sezione `auth` in `config/provisa.yaml` (impostazione predefinita in sviluppo). Il ruolo predefinito è `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Aprire l'UI

Aprire `http://localhost:3000` in un browser.

La barra di navigazione ha quattro menu di primo livello: [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Schema Explorer (`/schema`), editor GraphQL (`/query`), editor Cypher (`/graph`), editor SQL (`/sql`)
- **Model** — Viste e Command
- **Security** — Sicurezza a livello di riga e criteri di mascheramento delle colonne (REQ-038, REQ-041)
- **Admin** — Panoramica, domini, cache, attività pianificate, stato del sistema, osservabilità, utenti, organizzazioni, ruoli

L'API GraphQL di amministrazione si trova su `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Risoluzione dei problemi

**Il backend non si avvia** — controllare `.logs/server.log`. La causa più comune è una variabile d'ambiente mancante o un conflitto di porta sulla 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Servizi Docker non integri** — eseguire `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` per vedere quale servizio è bloccato. Il motore di federazione impiega circa 30 secondi al primo avvio. (REQ-055)

**Conflitto di porta sulla 3000 o sulla 8001** — `start-ui.sh` termina i processi obsoleti su quelle porte prima dell'avvio. Se qualcos'altro occupa la porta, arrestarlo manualmente prima. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Avvio pulito** — arrestare lo script, quindi eseguire `./start-ui.sh --reset-volumes` per cancellare tutti i volumi e riavviare. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Prossimi passi

| Obiettivo | Documento |
| ------ | ----- |
| Riferimento completo alla configurazione YAML | [configuration.md](configuration.md) |
| Sicurezza a livello di riga, mascheramento delle colonne, autenticazione | [security.md](security.md) |
| Tutti i tipi di origine supportati | [sources.md](sources.md) |
| Sottoscrizioni in tempo reale | [subscriptions.md](subscriptions.md) |
| JDBC, strumenti BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Client Python | [python-client.md](python-client.md) |
| Distribuzione in produzione | [deployment.md](deployment.md) |
