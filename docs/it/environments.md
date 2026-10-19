# Ambienti {: #environments }

Un ambiente è una copia con nome del modello governato di un'organizzazione. Fisicamente la copia è uno schema PostgreSQL separato: uno schema vero, senza colonna discriminante né prefisso. Così ogni query di repository esistente è corretta dentro un ambiente senza essere riscritta, e un predicato dimenticato non può rendere le righe di un ambiente leggibili da un altro.

Ogni organizzazione nasce con un ambiente chiamato `prod`. Non può essere eliminato né rinominato.
Una richiesta che non nomina alcun ambiente viene servita da `prod`; una richiesta che nomina un
ambiente inesistente viene rifiutata.

Gli ambienti sono disponibili per le organizzazioni con un piano a pagamento.

Per riempire un ambiente non di produzione con dati sicuri da usare, vedere [Creare ambienti di test](test-data.md).

## Nomi degli ambienti {: #environment-names }

Un nome deve corrispondere a `[a-z][a-z0-9_]{1,31}`: da due a trentadue lettere minuscole, cifre e trattini bassi, iniziando con una lettera. `prod` e i nomi che iniziano con `pg_` vengono rifiutati. La lunghezza massima per un'organizzazione dipende dal suo id. PostgreSQL tronca in silenzio un identificatore oltre i 63 byte, e il limite protegge dal nome di schema più lungo che un ambiente ricava.

## Che cosa porta con sé una copia {: #what-a-copy-carries }

Ogni tabella dello schema dell'organizzazione rientra in esattamente una classe. La classificazione è una lista di inclusione: una tabella aggiunta in seguito non viaggia finché qualcuno non ne nomina qui la classe, così una tabella dimenticata si vede come un test rosso.

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

I binding sono le colonne che dicono dove punta davvero un'origine: `host`, `port`, `database`, `username` e le altre. Non viaggiano mai in una copia. Un ambiente non ancora collegato viene marcato `unbound` invece di essere lasciato vuoto, perché un host vuoto non equivale a nessun host, e il costruttore della connessione lo leggerebbe come `localhost:5432`.

Un ambiente creato in qualsiasi modo tranne Unbound copia la connessione di ogni sorgente dal suo padre,
esattamente come il padre l'ha scritta: un riferimento `${secret:...}` o `${env:...}` viene copiato come quel
riferimento, mai risolto. Dopo la creazione la copia è la riga propria dell'ambiente, modificabile nella pagina
Sorgenti, e nulla viene risolto attraverso il padre quando si costruisce il runtime dell'ambiente.
Il campo `binding` di ogni sorgente dice da dove viene la sua connessione: `copied`, `own` (data in questo
ambiente) o `unbound`. Modificare una connessione copiata la rende `own`. **Ricopia dal padre**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) copia di nuovo le connessioni del padre.
Un merge o un deploy non porta connessioni, quindi una sorgente che introduce arriva `unbound`.

## La proiezione git {: #the-git-projection }

Ogni scrittura nel modello fa il commit del risultato sul branch git dell'ambiente. Fare il deploy di un albero richiede una chiamata esplicita: una pull request unita sull'host git non si distribuisce da sola.

Ogni entità ha un file. Il percorso è l'indirizzo `provisa://` dell'entità senza schema e organizzazione: `provisa://acme/sales/tables/Order` diventa `sales/tables/Order.yaml`. Le origini finiscono in `sources/`, i command in `commands/` e le metriche in `metrics/`. Le righe figlie che dipendono da un padre (colonne, relazioni, regole RLS) vengono scritte dentro il file del padre, senza file propri.

I comandi e le loro assegnazioni di tag sopravvivono al viaggio di andata e ritorno. Un tag su un
comando viene instradato verso il file del comando stesso (`commands/<name>.yaml`); un tag che non
appartiene ad alcun file scompare dalla proiezione e verrebbe eliminato al deployment successivo di
quell'albero.

Nessuna chiave surrogata arriva in un file. `registered_tables.id` è un intero autoincrementale, quindi lo stesso modello riceve interi diversi in due ambienti, e un dump ingenuo risulterebbe diverso da sé stesso. Ogni chiave surrogata viene omessa, e ogni riferimento a una di esse viene scritto come percorso della destinazione.

La serializzazione è deterministica. Le chiavi vengono emesse in ordine alfabetico, le collezioni
figlie ordinate per indirizzo, e lo stile YAML è fisso. Due ambienti che contengono lo stesso modello
producono alberi identici byte per byte.

## Merge

Unire il modello di un ambiente in un altro aggiorna per identità: ogni oggetto che l'origine possiede viene creato o aggiornato nella destinazione. Gli oggetti che l'origine non ha più vengono rimossi solo se chi chiama chiede esplicitamente le rimozioni. Un merge è una sola transazione, quindi se fallisce a metà la destinazione resta com'era.

Prima di applicare, chiamare l'endpoint di anteprima (`GET /{name}/merge-preview`) oppure passare
`dry_run: true`. L'anteprima percorre lo stesso codice usato dal merge; è un endpoint `GET` proprio
perché uno script CI che sbagli il flag non possa applicare per errore il merge che intendeva solo
ispezionare.

Un merge lascia i binding, i ruoli e i segreti della destinazione esattamente com'erano. Un ambiente
di sviluppo non perde le proprie connessioni al database perché ha preso un modello più recente da
prod. Prod non acquisisce le concessioni di dev.

### Che cosa nomina il report {: #what-the-report-names }

Il report del merge elenca, per percorso, cosa è stato aggiunto, cambiato, rimosso e lasciato com'era. Nomina anche i **conflitti**: oggetti che entrambi i lati hanno cambiato dall'ultimo commit in comune. Un conflitto viene segnalato, e vince l'origine, perché è questo che significa un merge in una destinazione. Provisa non offre risoluzione dei conflitti, né marcatori di merge, né scelta per oggetto. La lista dei conflitti vale come segnale: due persone stavano modificando lo stesso oggetto senza saperlo.

Quando entrambi i lati hanno portato un oggetto allo stesso valore, sono d'accordo e non c'è conflitto. Quando i due ambienti non hanno alcun antenato comune, nel report la base è `None`, e una lista di conflitti vuota significa che non è stato confrontato nulla, non che nulla si sia scontrato.

Il merge arriva come un unico commit squash sul branch della destinazione. Il messaggio di commit è obbligatorio e non può essere vuoto, perché è l'unico racconto del lavoro che lo squash rappresenta. I commit dell'origine restano dove sono e si possono ancora distribuire per SHA in seguito.

## Pull

Il pull prende ciò che il remote contiene per un ambiente e ne fa il modello. Non fa avanzare
direttamente il branch locale in fast-forward; applica l'albero recuperato attraverso il normale
percorso di deployment, così le stesse validazioni e lo stesso audit che governano un deployment
manuale governano anche un pull.

Come un merge, un pull riporta cosa ha sovrascritto: gli oggetti che l'albero in arrivo ha cambiato e che anche l'ambiente locale aveva cambiato dall'ultimo commit in comune delle due linee. Una modifica locale mai committata rende l'ambiente disallineato (vedi Cronologia sotto), e un pull la nomina nel report come una modifica normale.

Un pull viene rifiutato quando le due linee sono **divergenti**, cioè ciascuna ha commit che l'altra non ha. Il rifiuto elenca gli oggetti toccati da entrambi i lati, così chi deve decidere quale lavoro sopravvive sa quali oggetti guardare.

## Cronologia {: #history }

Ogni deploy sposta in avanti il cursore dell'ambiente lungo la sua linea di commit. Un annulla torna indietro di un commit, e un ripristina avanza di nuovo verso il punto da cui era partito l'annulla. Nessuno dei due rimuove un commit: tornare indietro aggiunge una posizione e lascia la cronologia com'è.

Un branch nasce sulla punta dell'ambiente da cui è stato creato, quindi un annullamento si ferma a
quel punto di innesco e non prosegue sui commit dell'ambiente genitore.

I flag `can_undo` e `can_redo` arrivano con la risposta dell'elenco degli ambienti. Entrambi valgono `false` quando la proiezione non contiene il commit che il piano di controllo nomina, uno stato che il design ammette e chiama **disallineato**. Un nodo il cui archivio del repository non ha mai ricevuto un certo commit elenca comunque i suoi ambienti; cambiano solo le risposte sulla cronologia.

## Autorizzazione {: #authorization }

Gli ambienti sono governati da due diritti. Nessuno dei due appartiene a un analista per
impostazione predefinita.

| Diritto | Chi lo detiene (all'inizializzazione) | Che cosa governa |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Creare ed eliminare ambienti |
| `environment_switch` | org_admin, developer | Essere serviti da un ambiente diverso da prod |

`prod` non richiede alcun diritto. È l'ambiente che serve una richiesta che non ne nomina nessuno, e rifiutarlo significherebbe rifiutare ogni richiesta.

Il controllo avviene dove si sceglie l'ambiente, prima di raggiungere qualsiasi route. Un membro senza `environment_switch` viene rifiutato su tutte le superfici insieme (HTTP, GraphQL, SQL e i protocolli wire), perché l'ambiente viene legato nel middleware e non nei singoli handler.

Un analista che non detiene alcun diritto sugli ambienti può interrogare `prod` e non vede il
selettore degli ambienti. Un collaboratore esterno a cui è stato concesso il ruolo di analista non
vede alcuna superficie degli ambienti e non può creare né passare ad alcun ambiente diverso dalla
produzione.

### L'autorità del proprietario di un ambiente {: #environment-owner-authority }

Creare un ambiente è l'unico modo in cui un membro in sola lettura ottiene diritti di modifica del modello. Dentro un ambiente che ha creato, il creatore ha le capacità del ruolo `developer`, meno i diritti sui dati (`write`, `full_results`, `usage`): diritti per costruire il modello, senza diritti sui dati.

La concessione è derivata da `environments.created_by` al momento dell'autorizzazione, mai scritta
in una tabella di concessioni. Eliminare l'ambiente la rimuove nello stesso atto.

L'appartenenza ai domini continua a limitare ciò che il proprietario può modificare. Creare un
branch cambia ciò che un membro può fare; non cambia mai su quali domini può farlo.

## Ambienti protetti {: #protected-environments }

Un ambiente può essere protetto. Un merge o un deployment verso un ambiente protetto non viene
applicato al momento della richiesta; viene proposto, e qualcuno diverso da chi lo ha richiesto deve
approvarlo.

`prod` diventa protetto automaticamente appena l'organizzazione ha più di un membro. Un'organizzazione con un solo membro non può soddisfare "qualcuno diverso dal richiedente", quindi lì la regola non si applica; renderebbe impossibile fare merge in `prod`. Un org_admin può marcare protetto qualsiasi ambiente.

Una richiesta di merge è una riga salvata, e nessuna finestra di conferma la sostituisce. Chi approva è per definizione una persona diversa dal richiedente e non è presente al momento della richiesta. Una conferma effimera costringerebbe ad approvare dentro la sessione del richiedente, che è proprio ciò che la regola vieta.

La riga della richiesta porta con sé il report del merge accanto al messaggio di chi ha richiesto.
L'obsolescenza è derivata al momento della lettura, mai memorizzata: ripianificare al momento della
lettura e confrontare con il report memorizzato è l'unica versione che non può sbagliare. Una
richiesta obsoleta deve essere ripresentata. Chi richiede non può approvare la propria richiesta.

Stati del ciclo di vita di una richiesta: `requested` → `approved`/`rejected` → `applied`. `stale` è
derivato.

I deploy da un ref del repository passano dalla stessa porta, e la richiesta fissa lo SHA al momento della proposta. Se il ref si sposta tra la proposta e la decisione, chi approva legge il report del commit fissato, non quello nuovo.

!!! note
    La UI delle richieste di merge si trova nella scheda **Merge requests** del pannello Ambienti.
    La colonna **Report** mostra per conteggio che cosa cambierebbe; la riga si espande per mostrare
    il dettaglio oggetto per oggetto.

## I comandi CLI `env` {: #the-env-cli-commands }

`provisa env deploy` invia il modello di un ref in un ambiente. Esce con 0 quando il deploy è stato applicato o era una prova, e con 2 quando l'ambiente è protetto e il deploy è stato solo proposto. Una pipeline che trattasse un'approvazione in attesa come un deploy rilasciato sbaglierebbe, e il codice di uscita distingue i due casi.

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
