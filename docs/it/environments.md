# Ambienti

Un ambiente è una copia nominata del modello governato di un'organizzazione. La copia è fisicamente
uno schema PostgreSQL separato — non una colonna discriminante, non un prefisso, uno schema vero e
proprio — così ogni query esistente del repository resta corretta dentro un ambiente senza riscrivere
nulla, e le righe di un ambiente non possono finire nella lettura di un altro per via di un
predicato dimenticato (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

Ogni organizzazione nasce con un ambiente chiamato `prod`. Non può essere eliminato né rinominato.
Una richiesta che non nomina alcun ambiente viene servita da `prod`; una richiesta che nomina un
ambiente inesistente viene rifiutata. [tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

Gli ambienti sono disponibili per le organizzazioni con un piano a pagamento. [inferred: REQ-1507]

## Nomi degli ambienti

Un nome deve corrispondere a `[a-z][a-z0-9_]{1,31}` — da due a trentadue caratteri tra lettere
minuscole, cifre e trattini bassi, con lettera iniziale. `prod` e i nomi che iniziano con `pg_`
vengono rifiutati. La lunghezza massima per una singola org dipende dall'id dell'org stessa:
PostgreSQL tronca silenziosamente un identificatore che superi i 63 byte, e il nome di schema più
lungo che un ambiente deriva è ciò da cui il limite protegge. [tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## Che cosa porta con sé una copia

Ogni tabella nello schema dell'org rientra in esattamente una classe (REQ-1489). La classificazione
è un elenco di ammissione, non di esclusione: una tabella aggiunta in seguito non viaggia finché
qualcuno non ne dichiara qui la classe, quindi il modo in cui fallisce una tabella dimenticata è un
test rosso. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| Classe | Tabelle | Che cosa succede nella copia |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, termini del glossario, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiate per intero |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Viaggiano i campi di identità e di governance; i valori di connessione restano indietro (vedere Binding) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiate solo alla prima creazione di un ambiente; i merge successivi le lasciano stare |
| PARTIAL | org_settings | Copiate per chiave: le impostazioni di governance viaggiano, le chiavi che nominano una destinazione esterna o il runtime specifico dell'ambiente restano indietro |
| NEVER_SENSITIVE | org_secrets, user_directory | Mai copiate |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log e altre | Mai copiate |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` esiste per risolvere un problema preciso. Un nuovo ambiente ha bisogno di ruoli
e assegnazioni, altrimenti si apre senza che nessuno possa agire. Ma un merge successivo che
portasse con sé la riga `developer` di `prod` sovrascriverebbe la versione ristretta di cui un
branch ristretto potrebbe avere bisogno, trasformando il percorso di revisione nella via
dell'escalation. Perciò ruoli e assegnazioni viaggiano una sola volta, alla creazione, e da lì in
poi sono la risposta propria di ciascun ambiente. [tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## Binding

I binding sono le colonne che dicono dove punta davvero un'origine — `host`, `port`, `database`,
`username` e le altre. Non viaggiano mai in nessuna copia. Un ambiente che non è stato associato
viene contrassegnato come `unbound` anziché lasciato vuoto: un host vuoto non è un host assente, e
il costruttore della connessione lo leggerebbe come `localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

Le origini di un ambiente si risolvono in uno di due modi.

**Base** — l'ambiente porta con sé le proprie credenziali. Un org_admin crea una base e poi associa
esplicitamente ciascuna origine. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**Branch** — l'ambiente eredita per riferimento le credenziali della base. Non viene copiato nulla.
Quando una query ha bisogno di una connessione, la risoluzione risale la catena `branched_from` e si
ferma al primo ambiente la cui riga è associata. La rotazione di una credenziale sulla base si
propaga a ogni branch che ne discende senza alcun intervento. Revocarla la revoca per tutti in una
sola volta. Nessun segreto viene mai materializzato in un punto da cui un branch, un'esportazione o
un repository potrebbero portarselo via.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

Per creare un branch, attivare **Inherit connections** nel pannello Ambienti. Per impostazione
predefinita è disattivato. [tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## La proiezione git

Ogni scrittura sul modello ne registra il risultato in un commit sul branch git dell'ambiente. Il
repository è una proiezione del modello, mai la sua autorità: Provisa legge e scrive il control
plane; il repository è il registro, non la fonte. Il deployment di un albero richiede una chiamata
esplicita — una pull request già unita sull'host git non si distribuisce da sola (REQ-1524,
REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Ogni entità ottiene un file. Il percorso è l'URI di REQ-1385 privato dello schema e dell'org:
`provisa://acme/sales/tables/Order` diventa `sales/tables/Order.yaml`. Le origini finiscono in
`sources/`, i comandi in `commands/`, le metriche in `metrics/`. Le righe figlie che discendono in
cascata da un genitore — colonne, relazioni, regole RLS — vengono scritte dentro il file del
genitore, non come file a sé stanti.
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

I comandi e le loro assegnazioni di tag sopravvivono al viaggio di andata e ritorno. Un tag su un
comando viene instradato verso il file del comando stesso (`commands/<name>.yaml`); un tag che non
appartiene ad alcun file scompare dalla proiezione e verrebbe eliminato al deployment successivo di
quell'albero. [tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

Nessuna chiave surrogata raggiunge un file. `registered_tables.id` è un intero autoincrementale — lo
stesso modello in due ambienti ottiene interi diversi, quindi un dump ingenuo produce differenze
rispetto a se stesso. Ogni surrogato viene scartato e ogni riferimento a uno di essi viene scritto
come percorso della destinazione.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

La serializzazione è deterministica. Le chiavi vengono emesse in ordine alfabetico, le collezioni
figlie ordinate per indirizzo, e lo stile YAML è fisso. Due ambienti che contengono lo stesso modello
producono alberi identici byte per byte. [tool-verified: `dump()` at env_files.py lines 131-143]

## Merge

Unire il modello di un ambiente in un altro aggiorna per identità: ogni oggetto presente
nell'origine viene creato o aggiornato nella destinazione. Gli oggetti che l'origine non ha più
vengono rimossi solo quando chi chiama richiede esplicitamente le rimozioni. Un merge che fallisce a
metà lascia la destinazione com'era — una sola transazione. [tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

Prima di applicare, chiamare l'endpoint di anteprima (`GET /{name}/merge-preview`) oppure passare
`dry_run: true`. L'anteprima percorre lo stesso codice usato dal merge; è un endpoint `GET` proprio
perché uno script CI che sbagli il flag non possa applicare per errore il merge che intendeva solo
ispezionare. [tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

Un merge lascia i binding, i ruoli e i segreti della destinazione esattamente com'erano. Un ambiente
di sviluppo non perde le proprie connessioni al database perché ha preso un modello più recente da
prod. Prod non acquisisce le concessioni di dev. [tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### Che cosa nomina il report

Il report del merge elenca, per percorso, che cosa è stato aggiunto, modificato, rimosso e lasciato
invariato. Nomina anche gli eventuali **conflitti** — oggetti che entrambe le parti hanno modificato
dall'ultimo commit condiviso. Un conflitto viene segnalato e non risolto: vince l'origine, che è
proprio ciò che significa un merge verso una destinazione. Provisa non offre risoluzione dei
conflitti, né marcatori di merge, né scelte oggetto per oggetto. Il valore dell'elenco dei conflitti
è il segnale — due persone stavano modificando lo stesso oggetto senza saperlo (REQ-1555).
[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

Un oggetto che entrambe le parti hanno modificato ottenendo lo stesso valore è un accordo, non un
conflitto. Quando i due ambienti non condividono alcun antenato, la base nel report è `None` e
l'elenco vuoto dei conflitti significa che non è stato confrontato nulla, non che nulla ha colliso.
[tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

Il merge atterra come un unico commit compattato sul branch della destinazione. Il messaggio di
commit è obbligatorio e non può essere vuoto — è l'unico resoconto dell'intervallo di lavoro che il
commit compattato rappresenta. I commit dell'origine restano dove sono e rimangono distribuibili per
SHA anche dopo.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Il pull prende ciò che il remote contiene per un ambiente e ne fa il modello. Non fa avanzare
direttamente il branch locale in fast-forward; applica l'albero recuperato attraverso il normale
percorso di deployment, così le stesse validazioni e lo stesso audit che governano un deployment
manuale governano anche un pull.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Come un merge, un pull riporta che cosa ha sovrascritto — gli oggetti modificati dall'albero in
arrivo che anche l'ambiente locale aveva modificato dall'ultimo commit condiviso tra le due linee.
Una modifica locale non ancora registrata in un commit è un ambiente andato alla deriva (vedere
Cronologia più avanti); un pull la nomina nel report come una modifica qualsiasi.
[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

Un pull viene rifiutato quando le due linee sono **divergenti** — entrambe contengono commit che
l'altra non ha. Il rifiuto porta con sé l'elenco degli oggetti toccati da entrambe le parti, così
chi ora deve decidere quale lavoro sopravvive sa quali oggetti guardare. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## Cronologia

Ogni deployment sposta in avanti il cursore dell'ambiente sulla sua linea di commit. Un annullamento
torna indietro di un commit; una ripetizione avanza di nuovo verso la posizione da cui
l'annullamento era partito. Nessuna delle due operazioni rimuove un commit — tornare indietro
aggiunge una posizione, non riscrive la cronologia.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

Un branch nasce sulla punta dell'ambiente da cui è stato creato, quindi un annullamento si ferma a
quel punto di innesco e non prosegue sui commit dell'ambiente genitore. [tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

I flag `can_undo` e `can_redo` viaggiano con la risposta dell'elenco degli ambienti. Entrambi
riportano `false` quando la proiezione non contiene il commit nominato dal control plane — uno stato
che il progetto ammette, chiamato **drifted**. Un nodo il cui archivio del repository non ha mai
ricevuto un determinato commit elenca comunque i propri ambienti; cambiano solo le risposte sulla
cronologia (REQ-1561). [tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## Autorizzazione

Gli ambienti sono governati da due diritti. Nessuno dei due appartiene a un analista per
impostazione predefinita (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| Diritto | Chi lo detiene (all'inizializzazione) | Che cosa governa |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Creare ed eliminare ambienti |
| `environment_switch` | org_admin, developer | Essere serviti da un ambiente diverso da prod |

`prod` non richiede alcun diritto — è ciò da cui viene servita una richiesta che non nomina nulla, e
rifiutarlo significherebbe rifiutare ogni richiesta.

L'applicazione avviene nel punto di selezione, prima che venga raggiunta qualsiasi route. A un
membro privo di `environment_switch` viene rifiutato l'accesso su tutte le superfici in una sola
volta — HTTP, GraphQL, SQL e i protocolli di trasporto — perché l'ambiente viene legato nel
middleware, non nei singoli handler.
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

Un analista che non detiene alcun diritto sugli ambienti può interrogare `prod` e non vede il
selettore degli ambienti. Un collaboratore esterno a cui è stato concesso il ruolo di analista non
vede alcuna superficie degli ambienti e non può creare né passare ad alcun ambiente diverso dalla
produzione. [tool-verified: REQ-1573 use_case and scenario]

### L'autorità del proprietario di un ambiente

Creare un ambiente è l'unico percorso attraverso cui un membro in sola lettura acquisisce diritti di
modifica del modello (REQ-1528). Dentro l'ambiente che ha creato, chi lo ha creato detiene le
capability del ruolo `developer` — meno i diritti sui dati (`write`, `full_results`, `usage`).
Diritti di costruzione del modello, non diritti sui dati. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

La concessione è derivata da `environments.created_by` al momento dell'autorizzazione, mai scritta
in una tabella di concessioni. Eliminare l'ambiente la rimuove nello stesso atto.
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

L'appartenenza ai domini continua a limitare ciò che il proprietario può modificare. Creare un
branch cambia ciò che un membro può fare; non cambia mai su quali domini può farlo (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Ambienti protetti (REQ-1504)

Un ambiente può essere protetto. Un merge o un deployment verso un ambiente protetto non viene
applicato al momento della richiesta; viene proposto, e qualcuno diverso da chi lo ha richiesto deve
approvarlo.

`prod` diventa protetto automaticamente non appena l'org ha più di un membro. Un'org con un solo
membro non può soddisfare la condizione «qualcuno diverso da chi ha richiesto», quindi lì la regola
non si applica — renderebbe `prod` impossibile da unire. Qualsiasi ambiente può essere
contrassegnato come protetto da un org_admin.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

Una richiesta di merge è una riga, non una finestra di conferma. Chi approva è per definizione una
persona diversa da chi ha richiesto e non è presente nel momento della richiesta; una conferma
effimera costringerebbe l'approvazione dentro la sessione di chi richiede, che è esattamente
l'assetto vietato dal requisito. [tool-verified: env_approvals.py module docstring lines 11-17]

La riga della richiesta porta con sé il report del merge accanto al messaggio di chi ha richiesto.
L'obsolescenza è derivata al momento della lettura, mai memorizzata: ripianificare al momento della
lettura e confrontare con il report memorizzato è l'unica versione che non può sbagliare. Una
richiesta obsoleta deve essere ripresentata. Chi richiede non può approvare la propria richiesta.
[tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

Stati del ciclo di vita di una richiesta: `requested` → `approved`/`rejected` → `applied`. `stale` è
derivato. [tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

La stessa porta gestisce i deployment da un ref del repository: la richiesta fissa lo SHA al momento
della proposta. Se il ref si sposta tra la proposta e la decisione, chi approva legge il report del
commit fissato, non di quello nuovo. [tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    La UI delle richieste di merge si trova nella scheda **Merge requests** del pannello Ambienti.
    La colonna **Report** mostra per conteggio che cosa cambierebbe; la riga si espande per mostrare
    il dettaglio oggetto per oggetto. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## I comandi CLI `env`

`provisa env deploy` invia in un ambiente il modello presente a un dato ref. Esce con 0 quando il
deployment è stato applicato o era una prova a vuoto, e con 2 quando l'ambiente è protetto e il
deployment è stato solo proposto — una pipeline che trattasse un'approvazione in attesa come un
deployment rilasciato sbaglierebbe, e il codice di uscita lo dice.
[tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` porta i branch remoti dell'org nel repository locale. Un deployment può poi
nominare `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Entrambi i comandi accettano `--api` (l'URL dell'API di Provisa) e `--token` (un bearer token).
Impostare `PROVISA_API_URL` e `PROVISA_API_TOKEN` nell'ambiente per non doverli passare a ogni
chiamata. [inferred: shared `_api_call()` helper]

La tipica pipeline CI per un flusso di lavoro basato su repository:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Vedi anche

- [Deployment](deployment.md) — come predisporre il control plane a cui gli ambienti si collegano
- [Comandi](commands.md) — funzioni tracciate e webhook che compaiono nell'albero di ogni ambiente
