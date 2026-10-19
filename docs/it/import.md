# Importare da Hasura {: #importing-from-hasura }

Provisa può convertire i metadati Hasura esistenti in un `config.yaml` Provisa, preservando tabelle tracciate, relazioni, permessi, e schemi remoti.

## Importazione interattiva (Admin → Importa configurazione Hasura) {: #interactive-import-admin-import-hasura-config }

La superficie di amministrazione esegue gli stessi convertitori, quindi un'importazione non richiede accesso alla shell né un giro di andata e ritorno del file di config. Richiede la capability `org_settings`; l'importazione viene applicata all'organizzazione in cui opera la sessione.

1. **Caricamento.** Scegli una directory di metadati Hasura v2 compressa in zip, un progetto DDN compresso in zip, un export di metadati consolidato (`.yaml`/`.json`, incluso l'involucro `{resource_version, metadata}` restituito dall'API dei metadati), o un singolo `.hml`. Lascia il formato su *Rileva automaticamente* a meno che il caricamento non sia ambiguo.
2. **Mappa i domini** (opzionale). Ogni coppia mappa uno schema v2 o un subgraph DDN a un dominio Provisa; ciò che non viene mappato mantiene il nome originale.
3. **Converti e anteprima.** Il server converte e restituisce i conteggi, il rapporto di conversione (vedi [Il rapporto](#the-report)) e la configurazione generata. In questo passaggio non viene scritto nulla.
4. **Rivedi e modifica.** La configurazione è modificabile sul posto: dettagli di connessione, nomi dei domini, nomi dei ruoli. Ciò che applichi è ciò che viene mostrato.
5. **Applica.** *Applica a questa organizzazione* carica la configurazione e ricostruisce gli schemi dell'organizzazione. Un'importazione si unisce sempre a ciò che l'organizzazione già possiede.

Endpoint: `POST /admin/import/hasura/preview` e `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Esportazione dei metadati {: #export-metadata }

Entrambi i layout di esportazione vengono convertiti.

**La directory di metadati della CLI.** `hasura metadata export` scrive una directory. `databases/databases.yaml` elenca ogni sorgente con il suo tipo e la sua connessione, e rimanda alle sue tabelle e funzioni tramite indici `!include`:

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

Anche il layout piatto (`tables.yaml` e `actions.yaml` nella radice) viene convertito.

**Il documento unico dell'API dei metadati.** L'API dei metadati restituisce l'intera esportazione come un solo oggetto, con la `configuration` per sorgente che il layout a directory non sa dove mettere:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

Sono accettati sia l'oggetto semplice sia la busta `{resource_version, metadata}`. L'importazione di amministrazione accetta questa forma (vedi [Importazione interattiva](#interactive-import-admin-import-hasura-config)); il convertitore da riga di comando legge solo directory.

### Conversione {: #convert }

Il convertitore da riga di comando legge una **directory** di metadati Hasura (il layout prodotto da `hasura metadata export`, o il layout piatto `tables.yaml` / `actions.yaml`) e scrive una configurazione Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omettere `-o` per scrivere la configurazione su stdout. Quando resta qualcosa da rifare a mano, il rapporto viene stampato su stderr.

Opzioni:

| Opzione | Scopo |
| ------ | --------- |
| `-o`, `--output` | Percorso del YAML di output (predefinito: stdout) |
| `--source-overrides` | File YAML con override di connessione per sorgente (host, porta, credenziali), indicizzato per nome della sorgente |
| `--domain-map` | Mappature schema-dominio come coppie `SCHEMA=DOMAIN` |
| `--auth-env-file` | File `.env` con la configurazione di autenticazione; converte JWT/JWK, il secret di amministrazione e la mappa dei claim |
| `--dry-run` | Analizza e valida senza scrivere l'output |

### Il rapporto {: #the-report }

Ogni conversione ordina ciò che trova in tre parti. Leggetele in quest'ordine.

1. **Riportato così com'è.** L'oggetto ha una controparte diretta in Provisa e si converte senza cambiare significato: sorgenti, tabelle, relazioni approvate, autorizzazioni di select, cron trigger.
2. **Mappato su una funzionalità migliore di Provisa.** Provisa ha una funzionalità che svolge il compito meglio di quella di Hasura, quindi l'oggetto diventa quella funzionalità. Ogni voce dice perché. Qui finiscono gli event trigger, le action, i remote schema, i nomi di campo radice aggiuntivi e i ruoli ereditati da più ruoli.
3. **Da rifare a mano.** L'oggetto non ha un equivalente in Provisa, oppure all'esportazione manca ciò che serve alla conversione. Ogni voce dice cosa fare. Sono gli avvisi.

L'anteprima restituisce le tre parti come elenchi. Il convertitore da riga di comando le stampa su stderr come `Carried as`, `Mapped to` e `Redo by hand`, ciascuna con un conteggio. Una conversione senza voci nella terza parte è completa.

### Cosa viene convertito {: #what-gets-converted }

| Concetto Hasura | Parte | Risultato in Provisa |
| --------------- | ---- | -------------- |
| Sorgente (`kind`, `connection_info`) | riportato | `sources[]`. Una connessione scritta per esteso nell'esportazione diventa host, porta, database e credenziali. Una letta da una variabile d'ambiente non viene indovinata (vedi [Connessioni e domini](#connections-and-domains-on-the-import-tab)). |
| Tabella tracciata | riportato | Una tabella registrata. Il suo nome personalizzato (o il suo campo radice `select`) diventa l'alias della tabella. |
| Nomi di colonna personalizzati (`column_config`, o il precedente `custom_column_names`) | riportato | `alias` della colonna. |
| Nomi di campo radice oltre `select` (`select_by_pk`, `select_aggregate`, `insert`, ...) | mappato | Segnalati per nome. Provisa serve una tabella come un solo campo: una lettura per chiave è un filtro su di esso, un aggregato il suo campo `_aggregate`, una scrittura la sua mutazione. Verificate che le query dell'app abbiano quella forma. |
| Relationship di oggetto e di array | riportato | Una relazione approvata (`many-to-one` per oggetto, `one-to-many` per array) il cui campo GraphQL è il nome della relationship Hasura. I casi che differiscono sono sotto. |
| Autorizzazione di select | riportato | Autorizzazioni di colonna (`visible_to`) e una regola di righe. Un termine con variabile di sessione (`X-Hasura-User-Id`) diventa `current_setting('provisa.hasura_user_id')`, che la richiesta lega al momento della query ai claim dell'identità o agli attributi del membro. Una variabile di sessione Hasura mantiene il proprio nome; `provisa.user_id` resta l'identità autenticata. |
| Regola di righe attraverso una relationship | riportato | Una sottoquery `EXISTS` sulla tabella correlata. |
| Autorizzazione di insert, update e delete | riportato | Autorizzazioni di scrittura sulla tabella: una per ruolo e operazione. `columns` diventano le colonne dell'autorizzazione, `filter` il suo `rows`, `check` il suo `check` e `set` i suoi preset. |
| Ruolo ereditato da un ruolo | riportato | Quel ruolo come `parent_role_id`. |
| Ruolo ereditato da più ruoli | mappato | Un ruolo che detiene l'unione delle loro autorizzazioni, scritta esplicitamente: un ruolo Provisa ha un solo padre. |
| Computed field | da rifare | Una colonna di una vista sulla tabella. |
| Funzione tracciata | riportato | Un comando; vedi sotto. Altrimenti, da rifare. |
| Action | mappato | Un comando che chiama l'handler dell'action. |
| Cron trigger | riportato | Un trigger pianificato. |
| Event trigger | mappato | Un trigger sulle modifiche della tabella. |
| Remote schema | mappato | Una sorgente `graphql_remote` i cui campi radice approdano come tabelle governate. |

Casi delle relationship:

- **Collisione.** Quando la tabella ha già un campo con il nome della relationship, la relationship prende il nome che le dà Provisa, e il rapporto avvisa. Rinominatela nella pagina Relationships se l'app ha bisogno di un altro nome.
- **Lo stesso join due volte.** Una seconda relationship che dichiara un join già dichiarato si fonde con la prima. Il rapporto lo indica tra le voci riportate.
- **Più coppie di colonne.** Una relazione Provisa unisce su una coppia di colonne. Una relationship che unisce su più coppie viene segnalata con un avviso e non viene creata, ma le regole di righe che la attraversano sono riportate.
- **Solo colonna di chiave esterna.** Una relationship di oggetto dichiarata con `foreign_key_constraint_on: artist_id` non nomina alcuna destinazione nell'esportazione. Il convertitore la risolve tramite la relationship di array inversa e avvisa quando non ce n'è nessuna.

Dettagli delle autorizzazioni di scrittura:

- Un `filter` o `check` vuoto significa ogni riga, scritto `TRUE`.
- Un valore di `set` che è una variabile di sessione (`X-Hasura-User-Id`) diventa un preset `session` della variabile di sessione corrispondente (`X-Hasura-User-Id` diventa `hasura_user_id`). Qualsiasi altro valore diventa un preset `literal`. Una colonna con preset non compare anche nelle colonne dell'autorizzazione.
- L'admin implicito di Hasura detiene ogni autorizzazione. Diventa il ruolo `org_admin`, che detiene un'autorizzazione di insert, update e delete su ogni colonna e ogni riga, ed è nominato su ogni colonna importata.
- Ogni ruolo con un'autorizzazione di insert, update o delete riceve inoltre il diritto `write`.

Dettagli dei comandi:

- **Funzione tracciata.** L'esportazione nomina la funzione e i ruoli che la chiamano, non i suoi argomenti né ciò che restituisce. L'anteprima legge entrambi dal catalogo del database e registra un comando con la firma trovata. Quando la sorgente non viene raggiunta, non è PostgreSQL, oppure la funzione manca, è sovraccarica o restituisce righe di una tabella che l'importazione non registra, la funzione passa a da rifare. Il convertitore da riga di comando non raggiunge mai un database, quindi ogni funzione tracciata che trova passa a da rifare.
- **Action.** Il comando chiama l'handler dell'action con un `POST`. Un `{{VAR}}` nell'URL dell'handler si scrive `${env:VAR}` e viene risolto quando il comando viene chiamato. Il comando è visibile ai ruoli che le autorizzazioni dell'action nominano, oppure al solo `org_admin` quando non ne nominano nessuno. Un'action asincrona viene segnalata con un avviso: il suo comando attende la risposta dell'handler. Un tipo di ritorno i cui campi l'esportazione non porta viene segnalato con un avviso.
- **Event trigger.** Il trigger scatta a ogni modifica della tabella, una volta per lotto di modifiche, e invia la tabella e le righe modificate. Non scatta per riga con valori vecchi e nuovi. Un trigger Hasura su meno di tutte e tre le operazioni viene segnalato, perché il trigger Provisa scatta su tutte. I tentativi ripetuti non sono riportati: un trigger Hasura con `num_retries` viene segnalato, perché un trigger Provisa chiama una volta. Ripetete nell'handler se una chiamata mancata conta.
- **Cron trigger.** L'espressione cron, l'URL, il payload e il flag enabled sono riportati. I tentativi ripetuti non sono riportati e vengono segnalati allo stesso modo.

Un documento consolidato contiene anche sezioni per cui il convertitore non ha destinazione: `query_collections`, `allowlist`, `rest_endpoints`, `network` e `opentelemetry`. Quando una è presente, il rapporto la elenca tra le voci da rifare. Il lettore di directory non apre mai i loro file.

### Connessioni e domini nella scheda di importazione {: #connections-and-domains-on-the-import-tab }

Una connessione che l'esportazione scrive per esteso viene convertita così com'è. Quando l'esportazione legge un URL di database da una variabile d'ambiente (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), il valore della variabile non è nell'esportazione, quindi la conversione non ne indovina nessuno. La sorgente viene registrata con i campi di connessione vuoti, e il rapporto dice di rifarla: inserite host, porta, database, utente e password della sorgente nelle impostazioni delle sorgenti dell'importazione e convertite di nuovo. Viaggiano solo i campi che modificate, come override della sorgente. Le righe di dominio coprono ogni schema, sottografo e remote schema che il caricamento porta; ciascuna è un selettore sui domini esistenti dell'organizzazione che accetta anche un nome digitato, contrassegnato come «nuovo dominio» quando non corrisponde ad alcuno. Applica si fonde con ciò che l'organizzazione ha già.

### I tipi provengono dall'origine in anteprima {: #types-come-from-the-source-at-preview }

Un export Hasura nomina le colonne senza tipi, e una tabella tracciata senza permessi non nomina colonne. L'anteprima viene eseguita con le connessioni origine fornite, quindi legge lo `information_schema.columns` di ogni origine SQL raggiungibile: ogni colonna non tipizzata riceve il tipo dell'origine mappato al vocabolario IR, e una tabella senza colonne prende tutte le colonne dell'origine, visibili solo a `org_admin`, poiché Hasura non le esponeva ad alcun altro ruolo. Un'origine che l'anteprima non riesce a raggiungere viene segnalata con un avviso `[sources]` e le sue colonne restano non tipizzate, da completare prima dell'applicazione.

### Limitazioni {: #limitations }

- **I computed field** sono da rifare a mano: l'esportazione nomina la funzione del database ma non la definisce. Aggiungetela come colonna di una vista sulla tabella, oppure calcolatela nell'app.
- **I remote schema** approdano come tabelle dagli SDL delle autorizzazioni di ruolo. Un remote schema senza autorizzazioni non porta nulla, perché l'esportazione non porta altra dichiarazione della sua forma.
- **Le funzioni tracciate** richiedono il database in anteprima per leggere la loro firma; offline, sono da rifare nella pagina Commands.
- **I tentativi ripetuti degli event e cron trigger** non sono riportati.

---

## Hasura DDN (v3)

### Localizzare il progetto HML {: #locate-the-hml-project }

Il convertitore DDN legge direttamente la **directory** di progetto DDN dei file `.hml`, senza alcun passo di build del supergraph. Il primo componente di directory sotto la root del progetto viene preso come nome del subgraph; i file sotto `globals/` vengono assegnati al subgraph `globals`.

### Conversione {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Ometti `-o` per scrivere la config su stdout.

Flag:

| Flag | Scopo |
| ------ | --------- |
| `-o`, `--output` | Percorso YAML di output (default: stdout) |
| `--source-overrides` | File YAML con override di connessione per origine |
| `--domain-map` | Mappature subgraph-a-dominio come coppie `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Percorso di output per il sidecar aggregate-expressions (default: `<output>-aggregates.yaml`) |
| `--dry-run` | Analizza e valida senza scrivere output |

I metadati `AggregateExpression` vengono preservati in un file sidecar `*-aggregates.yaml`.

### Cosa viene convertito {: #what-gets-converted_1 }

| Concetto DDN | Equivalente Provisa |
| ------------ | ------------------- |
| Modello subgraph | `tables[]` sotto un'origine |
| Relazione | `relationships[]` |
| Regola di permesso | Filtro RLS |
| Command | Mutation webhook o vista |
| Connettore | Voce origine con dettagli di connessione |

### Limitazioni {: #limitations_1 }

- I **connettori Lambda** (funzioni TypeScript/Python) richiedono setup manuale del webhook
- I **plugin di lifecycle** non hanno un equivalente diretto
- Le **modalità di autenticazione DDN** mappano ai provider di autenticazione Provisa ma i percorsi delle claim JWT potrebbero richiedere aggiustamenti

---

## Dopo l'import {: #after-import }

1. Esamina il `config.yaml` generato e il rapporto. Parti dalle voci di `Redo by hand`
2. Verifica le credenziali di connessione. Una connessione che l'esportazione leggeva da una variabile d'ambiente arriva vuota; il rapporto la nomina
3. Avvia Provisa e conferma che le tabelle appaiono nell'Explorer
4. Esegui le tue query GraphQL esistenti; lo schema è compatibile per i pattern comuni
5. Invia le query per l'approvazione tramite l'API Admin o la UI prima di abilitare la governance di produzione
