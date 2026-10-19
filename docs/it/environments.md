# Ambienti {: #environments }

Un ambiente è una copia nominata del modello governato di un'organizzazione. La copia è fisicamente
uno schema PostgreSQL separato — non una colonna discriminante, non un prefisso, uno schema vero e
proprio — così ogni query esistente del repository resta corretta dentro un ambiente senza riscrivere
nulla, e le righe di un ambiente non possono finire nella lettura di un altro per via di un
predicato dimenticato.

Ogni organizzazione nasce con un ambiente chiamato `prod`. Non può essere eliminato né rinominato.
Una richiesta che non nomina alcun ambiente viene servita da `prod`; una richiesta che nomina un
ambiente inesistente viene rifiutata.

Gli ambienti sono disponibili per le organizzazioni con un piano a pagamento.

Per riempire un ambiente non di produzione con dati sicuri da usare, vedere [Creare ambienti di test](test-data.md).

## Nomi degli ambienti {: #environment-names }

Un nome deve corrispondere a `[a-z][a-z0-9_]{1,31}` — da due a trentadue caratteri tra lettere
minuscole, cifre e trattini bassi, con lettera iniziale. `prod` e i nomi che iniziano con `pg_`
vengono rifiutati. La lunghezza massima per una singola org dipende dall'id dell'org stessa:
PostgreSQL tronca silenziosamente un identificatore che superi i 63 byte, e il nome di schema più
lungo che un ambiente deriva è ciò da cui il limite protegge.

## Che cosa porta con sé una copia {: #what-a-copy-carries }

Ogni tabella nello schema dell'org rientra in esattamente una classe. La classificazione
è un elenco di ammissione, non di esclusione: una tabella aggiunta in seguito non viaggia finché
qualcuno non ne dichiara qui la classe, quindi il modo in cui fallisce una tabella dimenticata è un
test rosso.

| Classe | Tabelle | Che cosa succede nella copia |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, termini del glossario, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiate per intero |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Viaggiano i campi di identità e di governance; i valori di connessione restano indietro (vedere Binding) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiate solo alla prima creazione di un ambiente; i merge successivi le lasciano stare |
| PARTIAL | org_settings | Copiate per chiave: le impostazioni di governance viaggiano, le chiavi che nominano una destinazione esterna o il runtime specifico dell'ambiente restano indietro |
| NEVER_SENSITIVE | org_secrets, user_directory | Mai copiate |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log e altre | Mai copiate |

`SEEDED_AT_CREATION` esiste per risolvere un problema preciso. Un nuovo ambiente ha bisogno di ruoli
e assegnazioni, altrimenti si apre senza che nessuno possa agire. Ma un merge successivo che
portasse con sé la riga `developer` di `prod` sovrascriverebbe la versione ristretta di cui un
branch ristretto potrebbe avere bisogno, trasformando il percorso di revisione nella via
dell'escalation. Perciò ruoli e assegnazioni viaggiano una sola volta, alla creazione, e da lì in
poi sono la risposta propria di ciascun ambiente.

## Binding {: #bindings }

I binding sono le colonne che dicono dove punta davvero un'origine — `host`, `port`, `database`,
`username` e le altre. Non viaggiano mai in nessuna copia. Un ambiente che non è stato associato
viene contrassegnato come `unbound` anziché lasciato vuoto: un host vuoto non è un host assente, e
il costruttore della connessione lo leggerebbe come `localhost:5432`.

Un ambiente creato in qualsiasi modo tranne Unbound copia la connessione di ogni sorgente dal suo padre,
esattamente come il padre l'ha scritta: un riferimento `${secret:...}` o `${env:...}` viene copiato come quel
riferimento, mai risolto. Dopo la creazione la copia è la riga propria dell'ambiente, modificabile nella pagina
Sorgenti, e nulla viene risolto attraverso il padre quando si costruisce il runtime dell'ambiente.
Il campo `binding` di ogni sorgente dice da dove viene la sua connessione: `copied`, `own` (data in questo
ambiente) o `unbound`. Modificare una connessione copiata la rende `own`. **Ricopia dal padre**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) copia di nuovo le connessioni del padre.
Un merge o un deploy non porta connessioni, quindi una sorgente che introduce arriva `unbound`.

## La proiezione git {: #the-git-projection }

Ogni scrittura nel modello esegue il commit del risultato nel branch git dell'ambiente. Il deploy di un albero
richiede una chiamata esplicita — una pull request unita sull'host git non si distribuisce da sola.

Ogni entità ottiene un file. Il percorso è l'indirizzo `provisa://` dell'entità privato dello schema e dell'org:
`provisa://acme/sales/tables/Order` diventa `sales/tables/Order.yaml`. Le origini finiscono in
`sources/`, i comandi in `commands/`, le metriche in `metrics/`. Le righe figlie che discendono in
cascata da un genitore — colonne, relazioni, regole RLS — vengono scritte dentro il file del
genitore, non come file a sé stanti.

I comandi e le loro assegnazioni di tag sopravvivono al viaggio di andata e ritorno. Un tag su un
comando viene instradato verso il file del comando stesso (`commands/<name>.yaml`); un tag che non
appartiene ad alcun file scompare dalla proiezione e verrebbe eliminato al deployment successivo di
quell'albero.

Nessuna chiave surrogata raggiunge un file. `registered_tables.id` è un intero autoincrementale — lo
stesso modello in due ambienti ottiene interi diversi, quindi un dump ingenuo produce differenze
rispetto a se stesso. Ogni surrogato viene scartato e ogni riferimento a uno di essi viene scritto
come percorso della destinazione.

La serializzazione è deterministica. Le chiavi vengono emesse in ordine alfabetico, le collezioni
figlie ordinate per indirizzo, e lo stile YAML è fisso. Due ambienti che contengono lo stesso modello
producono alberi identici byte per byte.

## Merge

Unire il modello di un ambiente in un altro aggiorna per identità: ogni oggetto presente
nell'origine viene creato o aggiornato nella destinazione. Gli oggetti che l'origine non ha più
vengono rimossi solo quando chi chiama richiede esplicitamente le rimozioni. Un merge che fallisce a
metà lascia la destinazione com'era — una sola transazione.

Prima di applicare, chiamare l'endpoint di anteprima (`GET /{name}/merge-preview`) oppure passare
`dry_run: true`. L'anteprima percorre lo stesso codice usato dal merge; è un endpoint `GET` proprio
perché uno script CI che sbagli il flag non possa applicare per errore il merge che intendeva solo
ispezionare.

Un merge lascia i binding, i ruoli e i segreti della destinazione esattamente com'erano. Un ambiente
di sviluppo non perde le proprie connessioni al database perché ha preso un modello più recente da
prod. Prod non acquisisce le concessioni di dev.

### Che cosa nomina il report {: #what-the-report-names }

Il report del merge elenca, per percorso, che cosa è stato aggiunto, modificato, rimosso e lasciato
invariato. Nomina anche gli eventuali **conflitti** — oggetti che entrambe le parti hanno modificato
dall'ultimo commit condiviso. Un conflitto viene segnalato e non risolto: vince l'origine, che è
proprio ciò che significa un merge verso una destinazione. Provisa non offre risoluzione dei
conflitti, né marcatori di merge, né scelte oggetto per oggetto. Il valore dell'elenco dei conflitti
è il segnale — due persone stavano modificando lo stesso oggetto senza saperlo.

Un oggetto che entrambe le parti hanno modificato ottenendo lo stesso valore è un accordo, non un
conflitto. Quando i due ambienti non condividono alcun antenato, la base nel report è `None` e
l'elenco vuoto dei conflitti significa che non è stato confrontato nulla, non che nulla ha colliso.

Il merge atterra come un unico commit compattato sul branch della destinazione. Il messaggio di
commit è obbligatorio e non può essere vuoto — è l'unico resoconto dell'intervallo di lavoro che il
commit compattato rappresenta. I commit dell'origine restano dove sono e rimangono distribuibili per
SHA anche dopo.

## Pull

Il pull prende ciò che il remote contiene per un ambiente e ne fa il modello. Non fa avanzare
direttamente il branch locale in fast-forward; applica l'albero recuperato attraverso il normale
percorso di deployment, così le stesse validazioni e lo stesso audit che governano un deployment
manuale governano anche un pull.

Come un merge, un pull riporta che cosa ha sovrascritto — gli oggetti modificati dall'albero in
arrivo che anche l'ambiente locale aveva modificato dall'ultimo commit condiviso tra le due linee.
Una modifica locale non ancora registrata in un commit è un ambiente andato alla deriva (vedere
Cronologia più avanti); un pull la nomina nel report come una modifica qualsiasi.

Un pull viene rifiutato quando le due linee sono **divergenti** — entrambe contengono commit che
l'altra non ha. Il rifiuto porta con sé l'elenco degli oggetti toccati da entrambe le parti, così
chi ora deve decidere quale lavoro sopravvive sa quali oggetti guardare.

## Cronologia {: #history }

Ogni deployment sposta in avanti il cursore dell'ambiente sulla sua linea di commit. Un annullamento
torna indietro di un commit; una ripetizione avanza di nuovo verso la posizione da cui
l'annullamento era partito. Nessuna delle due operazioni rimuove un commit — tornare indietro
aggiunge una posizione, non riscrive la cronologia.

Un branch nasce sulla punta dell'ambiente da cui è stato creato, quindi un annullamento si ferma a
quel punto di innesco e non prosegue sui commit dell'ambiente genitore.

I flag `can_undo` e `can_redo` viaggiano con la risposta dell'elenco degli ambienti. Entrambi
riportano `false` quando la proiezione non contiene il commit nominato dal control plane — uno stato
che il progetto ammette, chiamato **drifted**. Un nodo il cui archivio del repository non ha mai
ricevuto un determinato commit elenca comunque i propri ambienti; cambiano solo le risposte sulla
cronologia.

## Autorizzazione {: #authorization }

Gli ambienti sono governati da due diritti. Nessuno dei due appartiene a un analista per
impostazione predefinita.

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

Un analista che non detiene alcun diritto sugli ambienti può interrogare `prod` e non vede il
selettore degli ambienti. Un collaboratore esterno a cui è stato concesso il ruolo di analista non
vede alcuna superficie degli ambienti e non può creare né passare ad alcun ambiente diverso dalla
produzione.

### L'autorità del proprietario di un ambiente {: #environment-owner-authority }

Creare un ambiente è l'unico percorso attraverso cui un membro in sola lettura acquisisce diritti di
modifica del modello. Dentro l'ambiente che ha creato, chi lo ha creato detiene le
capability del ruolo `developer` — meno i diritti sui dati (`write`, `full_results`, `usage`).
Diritti di costruzione del modello, non diritti sui dati.

La concessione è derivata da `environments.created_by` al momento dell'autorizzazione, mai scritta
in una tabella di concessioni. Eliminare l'ambiente la rimuove nello stesso atto.

L'appartenenza ai domini continua a limitare ciò che il proprietario può modificare. Creare un
branch cambia ciò che un membro può fare; non cambia mai su quali domini può farlo.

## Ambienti protetti {: #protected-environments }

Un ambiente può essere protetto. Un merge o un deployment verso un ambiente protetto non viene
applicato al momento della richiesta; viene proposto, e qualcuno diverso da chi lo ha richiesto deve
approvarlo.

`prod` diventa protetto automaticamente non appena l'org ha più di un membro. Un'org con un solo
membro non può soddisfare la condizione «qualcuno diverso da chi ha richiesto», quindi lì la regola
non si applica — renderebbe `prod` impossibile da unire. Qualsiasi ambiente può essere
contrassegnato come protetto da un org_admin.

Una richiesta di merge è una riga, non una finestra di conferma. Chi approva è per definizione una
persona diversa da chi ha richiesto e non è presente nel momento della richiesta; una conferma
effimera costringerebbe l'approvazione dentro la sessione di chi richiede, che è esattamente
l'assetto vietato dal requisito.

La riga della richiesta porta con sé il report del merge accanto al messaggio di chi ha richiesto.
L'obsolescenza è derivata al momento della lettura, mai memorizzata: ripianificare al momento della
lettura e confrontare con il report memorizzato è l'unica versione che non può sbagliare. Una
richiesta obsoleta deve essere ripresentata. Chi richiede non può approvare la propria richiesta.

Stati del ciclo di vita di una richiesta: `requested` → `approved`/`rejected` → `applied`. `stale` è
derivato.

La stessa porta gestisce i deployment da un ref del repository: la richiesta fissa lo SHA al momento
della proposta. Se il ref si sposta tra la proposta e la decisione, chi approva legge il report del
commit fissato, non di quello nuovo.

!!! note
    La UI delle richieste di merge si trova nella scheda **Merge requests** del pannello Ambienti.
    La colonna **Report** mostra per conteggio che cosa cambierebbe; la riga si espande per mostrare
    il dettaglio oggetto per oggetto.

## I comandi CLI `env` {: #the-env-cli-commands }

`provisa env deploy` invia in un ambiente il modello presente a un dato ref. Esce con 0 quando il
deployment è stato applicato o era una prova a vuoto, e con 2 quando l'ambiente è protetto e il
deployment è stato solo proposto — una pipeline che trattasse un'approvazione in attesa come un
deployment rilasciato sbaglierebbe, e il codice di uscita lo dice.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` porta i branch remoti dell'org nel repository locale. Un deployment può poi
nominare `origin/<branch>`.

```
provisa env fetch --org acme --api <url> --token <token>
```

Entrambi i comandi accettano `--api` (l'URL dell'API di Provisa) e `--token` (un bearer token).
Impostare `PROVISA_API_URL` e `PROVISA_API_TOKEN` nell'ambiente per non doverli passare a ogni
chiamata.

La tipica pipeline CI per un flusso di lavoro basato su repository:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Vedi anche {: #see-also }

- [Deployment](deployment.md) — come predisporre il control plane a cui gli ambienti si collegano
- [Comandi](commands.md) — funzioni tracciate e webhook che compaiono nell'albero di ogni ambiente
- [Creare ambienti di test](test-data.md) — profilare la produzione, dichiarare dati fittizi, generare dati sintetici per un ambiente non di produzione
- [Metodi dei dati fittizi](fake-methods.md) — ogni tipo di dato fittizio e ogni metodo che una colonna può dichiarare
