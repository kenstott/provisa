# Riferimento CLI {: #cli-reference }

Il comando `provisa` è l'unico punto di ingresso per il livello embedded installato via pip (REQ-1128).
Avvia il runtime, gestisce le licenze, attiva la pubblicazione dei metadati, distribuisce i modelli e
controlla il banner di manutenzione — senza Docker, Node o alcun servizio esterno.

Installalo con l'extra `embedded`, che include anche le estensioni DuckDB offline
e il control plane PostgreSQL embedded:

```bash
pip install 'provisa[embedded]'
```

**Requisiti di piattaforma.** `provisa run` richiede Python 3.12 e una piattaforma con una wheel
pgserver: linux x86_64, macOS o Windows x86_64. Linux aarch64 non ha una wheel pgserver né una
distribuzione sorgente, quindi il livello embedded non funziona lì. Usa il livello container su aarch64.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Opzioni condivise {: #shared-options }

Diversi sottocomandi chiamano l'API HTTP di Provisa. Condividono tre flag e due variabili
d'ambiente. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Flag | Default | Fallback variabile d'ambiente |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(nessuno)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 per `maintenance`) | _(nessuna)_ |

`--api` è l'URL di base di un'istanza Provisa in esecuzione. In multitenancy l'host identifica
l'organizzazione — `https://acme.provisa.org` instrada al tenant di acme. `--token` è un token Bearer;
quando è vuoto non viene inviato alcun header `Authorization`, il che è corretto per deployment
non autenticati. [tool-verified: cli.py:314-316, 357-365]

Imposta entrambe le variabili nel tuo ambiente CI per evitare di ripeterle a ogni chiamata:

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Sottocomandi che accettano questi flag: `metadata export`, ogni sottocomando `env` (`list`, `show`,
`create`, `delete`, `diff`, `merge`, `deploy`, `revert`, `redo`, `fetch`, `push`, `pull`),
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Avvia il sistema Provisa embedded — server API e server statico/proxy della UI — in un unico
processo. Nessun Docker, nessun Node, nessun servizio esterno. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Flag {: #flags }

| Flag | Default | Note |
| --- | --- | --- |
| `--demo` | disattivato | Carica la demo inclusa — domini di esempio pet-store e shelter su SQLite embedded (REQ-414) |
| `--host` | `127.0.0.1` | Indirizzo di bind per entrambi i server |
| `--api-port` | `8000` | Porta del server API |
| `--ui-port` | `3000` | Porta del server statico/proxy della UI |
| `--no-browser` | disattivato | Salta l'apertura di un browser quando la UI è pronta; stampa comunque l'URL |
| `--reset` | disattivato | Elimina e ricostruisce lo store del control plane embedded prima di avviarsi; usalo dopo un aggiornamento di Provisa se l'avvio segnala una discrepanza di schema |
| `--data-dir` | `~/.provisa/native` | Directory che contiene il cluster PostgreSQL embedded e la cache delle estensioni DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### Variabili d'ambiente {: #environment-variables }

`provisa run` legge diverse variabili aggiuntive prima che i server HTTP si avviino.
Impostale per sovrascrivere i default che `load_profile("native", ...)` altrimenti applicherebbe.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Variabile | Effetto |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Sostituisce il motore DuckDB embedded con un coordinator Trino fornito dal cliente (REQ-1129) |
| `PROVISA_ENGINE_URL` | Modo alternativo per puntare a un motore di federazione esterno |
| `PROVISA_CONFIG` | File di configurazione da caricare; `--demo` lo imposta alla configurazione demo inclusa (REQ-1127) |
| `PROVISA_DEMO` | Impostata a `1` da `--demo`; contrassegna la sessione come esecuzione demo |
| `PROVISA_DEMO_DIR` | Percorso della directory dei dati di esempio della demo; impostata da `--demo` |
| `PROVISA_DUCKDB_EXT_DIR` | Directory delle estensioni DuckDB pre-staged; impostata automaticamente dal pacchetto `provisa-duckdb-ext` se presente; se assente DuckDB scarica dalla rete al primo utilizzo |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Sequenza di avvio {: #startup-sequence }

1. Controllo piattaforma — interrompe con un messaggio chiaro su Python non supportato o pgserver mancante.
2. `--reset` (se richiesto) — elimina il cluster PostgreSQL embedded; viene ricostruito al passo successivo.
3. Config demo (se `--demo`) — imposta `PROVISA_CONFIG` e `PROVISA_DEMO_DIR`.
4. Ambiente embedded — avvia il control plane PostgreSQL, risolve il suo URL socket, e
   mette in stage le estensioni DuckDB offline se `provisa-duckdb-ext` è installato.
5. Controllo di drift dello schema — scansiona il control plane live per colonne mancanti. Se ne vengono trovate,
   stampa un suggerimento per `--reset` ed esce con codice 1. V1 non ha migrazioni; una colonna aggiunta in una
   release più recente richiede un reset. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Entrambi i server si avviano contemporaneamente. Il ready announcer esegue il polling di `GET /ready` (non `/health` — l'endpoint
   `/ready` conferma che lo store è collegato e il motore è pronto) e apre il browser
   quando restituisce 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Codici di uscita {: #exit-codes }

| Codice | Significato |
| --- | --- |
| 0 | Arresto pulito (Ctrl-C) |
| 1 | Errore di avvio (controllo piattaforma fallito, config demo mancante, drift dello schema rilevato) |

### Esempio {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Verifica e installa un file di licenza offline (REQ-1139). Il file è il `license.json` emesso da
provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Argomento | Note |
| --- | --- |
| `file` | Percorso del file di licenza; viene applicata l'espansione `~` |

Il codice di uscita 0 significa che la licenza è valida e installata. Il codice di uscita 1 significa che è stata
rifiutata; il motivo viene stampato su stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Mostra l'ID macchina, lo stato del trial, i giorni trascorsi e la validità della licenza (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Nessun flag. Stampa quattro righe — ID macchina, data di primo avvistamento, giorni trascorsi, stato del trial, e
stato della licenza — ed esce con 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Attiva la pubblicazione dei metadati on-demand del server in esecuzione (REQ-1072/REQ-1074). Esegue un POST a
`POST /admin/metadata-export/publish` — lo stesso endpoint chiamato dal pulsante **Publish now**
della scheda Admin, quindi entrambi i percorsi inviano lo stesso snapshot completo. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Default | Note |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, poi `http://127.0.0.1:8000` | In multitenancy l'host identifica l'org |
| `--token` | `$PROVISA_API_TOKEN` | Token Bearer per un'identità con `org_settings`; ometti su deployment non autenticati |
| `--timeout` | `300` | Secondi prima che la chiamata HTTP venga abbandonata |

[tool-verified: cli.py:654-669]

| Codice di uscita | Significato |
| --- | --- |
| 0 | Ogni asset pubblicato |
| 1 | Pubblicazione parziale o errore di connessione; gli errori per asset vengono stampati su stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

Il riferimento completo di configurazione — provider, credenziali, `reconcile_cron`, e cosa lo
snapshot contiene — è in [Esportazione dei metadati](metadata-export.md#from-the-command-line).

---

## provisa env

Esegue il ciclo di vita degli ambienti su un Provisa in esecuzione (REQ-1498). Ogni sottocomando è una chiamata
all'API di amministrazione che chiama l'interfaccia; la CLI non decide nulla. L'elenco di ciò che una copia porta, le regole di
merge e di rimozione, il binding, la protezione e i diritti del chiamante appartengono al server, quindi valgono
comunque venga invocata l'operazione. [tool-verified: cli_envs.py:11-28]

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```
### Opzioni accettate da ogni sottocomando {: #flags-every-subcommand-takes }

| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--org` | sì | Organizzazione che contiene l'ambiente |
| `--json` | no | Stampa la risposta dell'API come JSON, per uno script |
| `--api` | no | Vedere [Opzioni comuni](#shared-options) |
| `--token` | no | Token bearer di un'identità che può eseguire l'operazione |
| `--timeout` | no | Secondi; predefinito 300 |

I sottocomandi che agiscono su un ambiente accettano anche `--env ENV` (obbligatorio). `list`, `diff`,
`merge` e `fetch` no; `diff` e `merge` nominano i loro ambienti con `--from` e `--into`.
[tool-verified: cli_envs.py:349-374, `_command()`]

### Codici di uscita {: #exit-codes_1 }

| Codice | Significato |
| --- | --- |
| 0 | Fatto, o una prova che non ha applicato nulla |
| 1 | Rifiutato. Il codice della regola che rifiuta viene stampato su stderr come `refused (<code>): <detail>`; con `--json` è `{"refused": {...}}` su stdout |
| 2 | Proposto, non applicato: l'ambiente di destinazione è protetto e la modifica attende un'approvazione |

Il codice di uscita 2 è voluto. Una pipeline che trattasse un'approvazione in sospeso come un deploy rilasciato
sbaglierebbe. [tool-verified: cli_envs.py:21-28, 40-41, 64-78, 120-135]

### provisa env list

Elenca gli ambienti dell'organizzazione e la build in cui si trova ciascuno. Nessuna opzione oltre a quelle comuni.
[tool-verified: cli_envs.py:141-149]

```
provisa env list --org ORG [--json]
```
### provisa env show

Mostra un ambiente e la sua build attuale. Esce con 1 e `environments.unknown` quando
l'ambiente non esiste. [tool-verified: cli_envs.py:163-183]

```
provisa env show --org ORG --env ENV [--json]
```
### provisa env create

Crea un ambiente che contiene il modello di un altro, con i dati scelti alla creazione (REQ-1942). L'API
decide quali scelte può fare il chiamante. [tool-verified: cli_envs.py:197-216]

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```
| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--env` | sì | Nome del nuovo ambiente |
| `--from` | no | Ambiente da cui partire; predefinito `prod` |
| `--data-mode` | sì | `inherit`, `unbound`, `test_fake` o `test_synthetic`: le connessioni della sorgente per riferimento, nessuna finché non associa le proprie, o dati di test |
| `--mutation-handling` | no | `refused`, `reversible` o `direct`; predefinito `refused` |
| `--expires-at` | no | Istante ISO-8601 dopo il quale l'ambiente viene eliminato |

[tool-verified: cli_envs.py:391-416]

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```
### provisa env delete

Ritira un ambiente. I suoi branch vengono rimossi solo se nominati. [tool-verified: cli_envs.py:219-232]

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```
| Opzione | Note |
| --- | --- |
| `--delete-branch` | Elimina anche il suo branch |
| `--delete-remote-branch` | Elimina anche il suo branch sul remote |

[tool-verified: cli_envs.py:418-424]

### provisa env diff

Riporta cosa farebbe il merge di un ambiente in un altro. È un GET di `merge-preview`, quindi
non può scrivere; affidatelo a un runner che non deve cambiare nulla. [tool-verified:
cli_envs.py:186-191]

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```
| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--from` | sì | Ambiente da cui si fa il merge |
| `--into` | sì | Ambiente in cui si fa il merge |
| `--removals` | no | Rimuove anche dalla destinazione ciò che la sorgente non ha più |
| `--select PATH` | no | Limita il merge a questo oggetto, tramite il suo percorso nel modello (`metrics/revenue.yaml`); ripetere per altri. Ciò che richiede e che manca alla destinazione viene portato con esso ed elencato (REQ-1506) |

[tool-verified: cli_envs.py:426-439]

### provisa env merge

Esegue il merge di un ambiente in un altro, per identità (REQ-1490). [tool-verified: cli_envs.py:235-250]

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```
| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--from` | sì | Ambiente da cui si fa il merge |
| `--into` | sì | Ambiente in cui si fa il merge |
| `--removals` | no | Rimuove anche dalla destinazione ciò che la sorgente non ha più |
| `--select PATH` | no | Limita il merge a questo oggetto, tramite il suo percorso nel modello (`metrics/revenue.yaml`); ripetere per altri. Ciò che richiede e che manca alla destinazione viene portato con esso ed elencato (REQ-1506) |
| `--dry-run` | no | Stampa il rapporto di merge senza applicare nulla |
| `--message` | no | Cosa fa il merge; obbligatorio per applicare |
| `--retire-source` | no | Elimina la sorgente una volta arrivato il suo lavoro |
| `--retire-remote` | no | Con `--retire-source`, elimina anche il branch della sorgente sul remote |

[tool-verified: cli_envs.py:429-451]

### provisa env deploy

Distribuisce il modello di una ref git in un ambiente, rendendo quell'albero il modello attuale dell'ambiente
(REQ-1496). È il comando che esegue una pipeline di deployment; un deploy è sempre
un'invocazione che porta un'identità verso un control plane nominato. [tool-verified:
cli_envs.py:253-267]

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```
| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--env` | sì | Ambiente che conterrà il modello distribuito |
| `--ref` | sì | Branch o commit del repository dell'organizzazione |
| `--dry-run` | no | Riporta cosa farebbe il deploy; non applica nulla |
| `--message` | no | Nota riportata su una richiesta di approvazione quando la destinazione è protetta |

[tool-verified: cli_envs.py:453-468]

Un caricamento in un ambiente esistente non applica mai ruoli. `--seed` non esiste più: l'API rifiuta un seed
con `environments.seed_only_at_creation`.

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```
### provisa env revert

Riporta un ambiente a una build precedente (REQ-1543). Senza `--to` torna indietro di un commit
nella cronologia propria dell'ambiente. Con `--to` è il normale deploy di quella build.
[tool-verified: cli_envs.py:270-284]

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```
| Opzione | Obbligatoria | Note |
| --- | --- | --- |
| `--env` | sì | Ambiente da ripristinare |
| `--to` | no | Build da distribuire; predefinita quella precedente alla build attuale |
| `--message` | no | Nota riportata su una richiesta di approvazione |

[tool-verified: cli_envs.py:470-480]

### provisa env redo

Va di nuovo avanti verso la build da cui un revert era partito. [tool-verified:
cli_envs.py:287-301]

```
provisa env redo --org ORG --env ENV
```
### provisa env fetch

Recupera i branch remoti dell'organizzazione nel suo repository Provisa (REQ-1541). Eseguitelo prima di un deploy
quando volete nominare `origin/<branch>`. Provisa non lo esegue mai a tempo. [tool-verified:
cli_envs.py:307-320]

```
provisa env fetch --org ORG
```
Stampa una riga per ogni branch recuperato, `origin/<name>  <sha12>`.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```
### provisa env push

Invia il branch di un ambiente al remote dell'organizzazione (REQ-1546). Stampa `nothing to send` quando
non c'è nulla da inviare. [tool-verified: cli_envs.py:323-331]

```
provisa env push --org ORG --env ENV
```
### provisa env pull

Applica ciò che il remote contiene per un ambiente (REQ-1547). Stampa
`<env> already holds what the remote does` quando non c'è nulla da applicare. Il codice di uscita 2 vale come
per deploy. [tool-verified: cli_envs.py:334-343]

```
provisa env pull --org ORG --env ENV
```
Per la spiegazione completa delle classi di ambiente, delle regole di protezione, dei rapporti di merge e del
ciclo di vita delle approvazioni, vedere [Ambienti](environments.md#the-env-cli-commands).

---

## provisa maintenance on

Attiva il banner di manutenzione pianificata sul deployment (REQ-1466). Eseguilo prima di un lavoro
pianificato che porta giù il data plane — per esempio, prima di cambiare
`var.engine_cluster_mode`, che sostituisce il cluster del motore e ogni shard su di esso (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Flag | Note |
| --- | --- |
| `--message` | Sovrascrive il testo standard del deployment; il default è il messaggio standard del server |
| `--ends-at` | Istante ISO-8601 in cui il lavoro dovrebbe terminare, es. `2026-08-14T22:30:00Z`; il default è nessuna stima |
| `--api` | Vedi [Opzioni condivise](#opzioni-condivise) |
| `--token` | Token Bearer per un'identità con `platform_settings` |
| `--timeout` | Default 30 s |

[tool-verified: cli.py:743-773]

Stampa lo stato risultante del banner ed esce con 0. Solleva `SystemExit` in caso di fallimento HTTP o di connessione.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Cancella il banner di manutenzione una volta terminato il lavoro (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Stampa lo stato risultante del banner (active: false) ed esce con 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Mostra lo stato attuale del banner di manutenzione senza modificarlo (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Stampa lo stato del banner ed esce con 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Riferimento rapido {: #quick-reference }

| Comando | REQ | Cosa fa |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Avvia l'API + UI embedded |
| `provisa run --demo` | REQ-414 | Avvia con dati di esempio pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Ricostruisce il control plane prima di avviarsi |
| `provisa license apply <file>` | REQ-1139 | Installa un file di licenza offline |
| `provisa license status` | REQ-1139 | Mostra l'ID macchina e lo stato del trial / della licenza |
| `provisa metadata export` | REQ-1072 | Pubblica lo snapshot dei metadati on demand |
| `provisa env list --org ORG` | REQ-1498 | Elenca gli ambienti dell'organizzazione e le loro build |
| `provisa env show --org ORG --env ENV` | REQ-1498 | Mostra un ambiente |
| `provisa env create --org ORG --env ENV --data-mode MODE` | REQ-1942 | Crea un ambiente da un altro |
| `provisa env delete --org ORG --env ENV` | REQ-1498 | Ritira un ambiente |
| `provisa env diff --org ORG --from A --into B` | REQ-1490 | Anteprima di un merge (sola lettura) |
| `provisa env merge --org ORG --from A --into B` | REQ-1490 | Esegue il merge di un ambiente in un altro |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Distribuisce una ref in un ambiente |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | REQ-1543 | Torna indietro di un commit, o distribuisce una build |
| `provisa env redo --org ORG --env ENV` | REQ-1543 | Va di nuovo avanti |
| `provisa env fetch --org ORG` | REQ-1541 | Recupera i branch remoti nel repository Provisa |
| `provisa env push --org ORG --env ENV` | REQ-1546 | Invia il branch di un ambiente al remote |
| `provisa env pull --org ORG --env ENV` | REQ-1547 | Applica ciò che contiene il remote |
| `provisa maintenance on` | REQ-1466 | Attiva il banner di manutenzione |
| `provisa maintenance off` | REQ-1466 | Cancella il banner di manutenzione |
| `provisa maintenance status` | REQ-1466 | Mostra lo stato attuale del banner |

## Vedi anche {: #see-also }

- [Ambienti](environments.md) — modello degli ambienti, ambienti protetti, ciclo di vita di approvazione del deploy
- [Esportazione dei metadati](metadata-export.md) — provider del catalogo, configurazione, e cosa contiene lo snapshot
- [Deployment](deployment.md) — livello container e deployment cloud
