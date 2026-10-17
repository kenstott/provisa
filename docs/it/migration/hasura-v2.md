# Migrazione da Hasura v2 a Provisa {: #migrating-from-hasura-v2-to-provisa }

## Prerequisiti {: #prerequisites }

1. Un'istanza Hasura v2 (v2.x) in esecuzione con i metadati esportati.
2. Esportare i metadati in uno dei due layout:

   **La directory di metadati della CLI:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Questo crea una directory `metadata/`. `databases/databases.yaml` elenca ogni sorgente con il suo tipo
   e la sua connessione e rimanda alle sue tabelle e funzioni tramite indici `!include`;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` e gli
   altri file stanno accanto. Anche il vecchio layout piatto (`tables.yaml` nella radice)
   viene convertito.

   **Il documento unico dell'API dei metadati:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   Funzionano sia l'oggetto semplice sia la busta `{resource_version, metadata}`. L'importazione
   di amministrazione (Admin, Import Hasura Config) accetta questa forma; il convertitore da riga di
   comando qui sotto legge directory.
3. Python 3.11+ con il pacchetto `provisa` installato.

## Utilizzo della CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argomenti {: #arguments }

| Argomento | Obbligatorio | Descrizione |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sì | Percorso della directory dei metadati Hasura v2 esportata |

### Opzioni {: #options }

| Opzione | Predefinito | Descrizione |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Percorso del file YAML di output |
| `--source-overrides FILE` | Nessuno | File YAML con override di connessione per singola origine |
| `--domain-map KEY=VAL ...` | Nessuno | Mappature schema-dominio (ad esempio, `public=core hr=people`) |
| `--auth-env-file FILE` | Nessuno | Percorso del file `.env` con la configurazione di autenticazione JWT/admin-secret |
| `--dry-run` | disattivato | Analizza e valida senza scrivere l'output |

### File di override delle origini {: #source-overrides-file }

Un file YAML indicizzato per nome dell'origine, con le proprietà di connessione da sovrascrivere:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### File dell'ambiente di autenticazione {: #auth-env-file }

Un file in stile `.env` che contiene la configurazione di autenticazione di Hasura da
convertire. Il convertitore effettua le seguenti mappature:

- JWT con `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Autenticazione tramite webhook -> viene emesso un avviso (nessun equivalente in Provisa).

## Il rapporto {: #the-report }

La conversione termina con un rapporto in tre parti:

1. **Riportato così com'è.** L'oggetto ha una controparte in Provisa e si converte senza cambiare significato.
2. **Mappato su una funzionalità migliore di Provisa.** Ogni voce dice perché la funzionalità di Provisa svolge meglio il compito.
3. **Da rifare a mano.** Ogni voce dice cosa fare. Sono gli avvisi.

Il convertitore da riga di comando stampa il rapporto su stderr come `Carried as`, `Mapped to` e `Redo by hand`, ciascuna con un conteggio, quando resta qualcosa da rifare. L'importazione di amministrazione restituisce le tre liste con l'anteprima.

## Matrice di parità delle funzionalità {: #feature-parity-matrix }

| Funzionalità Hasura v2 | Parte | Equivalente Provisa | Note |
| --- | --- | --- | --- |
| **Sorgenti** (postgres, mysql, mssql, bigquery, citus) | riportato | `sources[]` | Tipo mappato: pg/postgres -> postgresql, mssql -> sqlserver. Un URL di connessione scritto per esteso viene scomposto in host/porta/database/utente/password. Dimensione del pool conservata. Un URL letto da una variabile d'ambiente non viene indovinato (vedi sotto). |
| **Tabelle** (tabelle tracciate) | riportato | `tables[]` | Schema e nome della tabella conservati. `source_id` rimanda alla sorgente. |
| **Nomi di tabella personalizzati** (`custom_name`, `custom_root_fields.select`) | riportato | `tables[].alias` | Il primo valore non nullo tra `select`, `select_by_pk`, `custom_name`. |
| **Nomi di colonna personalizzati** (`column_config`, precedente `custom_column_names`) | riportato | `columns[].alias` | Il nome personalizzato di ogni colonna diventa il suo alias. |
| **Altri nomi di campo radice** (`select_by_pk`, `select_aggregate`, `insert`, ...) | mappato | Il campo unico della tabella | Segnalati per nome. Una lettura per chiave è un filtro sulla tabella, un aggregato il suo campo `_aggregate`, una scrittura la sua mutazione. |
| **Autorizzazioni di select** (colonne, filtro) | riportato | `columns[].visible_to[]`, `rls_rules[]` | Gli elenchi di colonne diventano `visible_to`; `*` è supportato. I filtri diventano SQL; le variabili di sessione diventano `current_setting('provisa.<name>')`. |
| **Filtri attraverso relationship** | riportato | Sottoquery `EXISTS` in `rls_rules[]` | La regola segue la relationship fino alla sua tabella. |
| **Autorizzazioni di insert, update e delete** | riportato | `tables[].write_grants[]` | Un'autorizzazione per ruolo e operazione: colonne da `columns`, `rows` da `filter`, `check` da `check`, preset da `set`. I ruoli ottengono il diritto `write`. |
| **Ruolo admin** | riportato | `org_admin` | Detiene ogni autorizzazione su ogni colonna e riga, ed è nominato su ogni colonna importata. |
| **Relationship di oggetto** | riportato | `relationships[]` con `cardinality: many-to-one` | Approvate. Il campo GraphQL è il nome della relationship Hasura. |
| **Relationship di array** | riportato | `relationships[]` con `cardinality: one-to-many` | Approvate. Il campo GraphQL è il nome della relationship Hasura. |
| **Ruoli ereditati** (un ruolo) | riportato | `roles[].parent_role_id` | Il ruolo diventa il padre. |
| **Ruoli ereditati** (più ruoli) | mappato | Un ruolo che detiene l'unione delle loro autorizzazioni | I ruoli Provisa hanno un solo padre, quindi l'unione viene scritta esplicitamente sul ruolo. |
| **Computed field** | da rifare | Una colonna di vista | L'esportazione nomina la funzione, non il suo corpo. Il rapporto indica il `SELECT` da aggiungere. |
| **Funzioni tracciate** | riportato, altrimenti da rifare | Comandi `functions[]` | L'anteprima legge la firma dal database. Senza, aggiungete la funzione nella pagina Commands. |
| **Action** | mappato | Comandi che chiamano l'handler | `{{VAR}}` nell'URL dell'handler si scrive `${env:VAR}` e viene risolto quando il comando viene chiamato. |
| **Cron trigger** | riportato | `scheduled_triggers[]` | Espressione cron, URL, payload e flag enabled. I tentativi ripetuti non sono riportati (avviso). |
| **Event trigger** | mappato | `scheduled_triggers[]` sulla modifica della tabella | Scatta per lotto di modifiche a ogni modifica. I tentativi ripetuti non sono riportati (avviso). |
| **Remote schema** | mappato | `sources[]` (`graphql_remote`) approdato come tabelle | Nome, URL, header e autenticazione conservati; i campi radice diventano tabelle governate. |
| **Tabelle enum** | riportato | Tabella creata | Il flag `is_enum` non viene riportato (nessun equivalente Provisa). |
| **Allow list, query collection, endpoint REST** | da rifare | Nessuno | Segnalati quando presenti in un'esportazione consolidata; non convertiti. |

### Relationship {: #relationships }

- Il campo GraphQL di ogni relationship è il nome Hasura. Quando la tabella ha già un campo con quel nome, la relationship prende il nome che le dà Provisa e il rapporto avvisa.
- Due relationship che dichiarano lo stesso join si fondono in una. Il rapporto indica quale si è fusa in quale.
- Una relationship che unisce su più coppie di colonne viene segnalata con un avviso e non viene creata, perché una relazione Provisa unisce su una coppia. Le regole di righe che la attraversano vengono comunque convertite.
- Una relationship di oggetto dichiarata solo dalla colonna di chiave esterna viene risolta tramite la relationship di array inversa; senza una, viene segnalata.

### Autorizzazioni di scrittura {: #write-grants }

Un'autorizzazione indica che cosa un ruolo può scrivere in una tabella con un'operazione. Un `filter` o `check` vuoto significa ogni riga (`TRUE`). Un valore di `set` `X-Hasura-User-Id` diventa un preset `session` di `user_id`; qualsiasi altro valore diventa un preset `literal`. Una colonna con preset non compare anche nelle colonne dell'autorizzazione.

### Action, funzioni tracciate e trigger {: #actions-tracked-functions-and-triggers }

- Una **action** diventa un comando che chiama il suo handler, visibile ai ruoli che le sue autorizzazioni nominano (al solo `org_admin` quando non ne nominano nessuno). Un'action asincrona viene segnalata: il comando attende la risposta dell'handler.
- Una **funzione tracciata** diventa un comando quando l'anteprima può leggere i suoi argomenti e la sua tabella di ritorno da una sorgente PostgreSQL. Offline, e per qualsiasi funzione mancante, sovraccarica o che restituisce una tabella che l'importazione non registra, passa a da rifare.
- Un **event trigger** scatta a ogni modifica della sua tabella, una volta per lotto, e invia la tabella e le righe modificate anziché un evento per riga con valori vecchi e nuovi. Un trigger che ascoltava meno di tutte e tre le operazioni viene segnalato.

## Passaggi dopo la conversione {: #post-conversion-steps }

1. **Esamina il rapporto.** Parti da `Redo by hand`.
2. **Configura le connessioni delle sorgenti.** Una connessione che l'esportazione scrive per esteso viene convertita così com'è. Una letta da una variabile d'ambiente arriva vuota. Usa `--source-overrides` o modifica direttamente l'output.
3. **Verifica le assegnazioni dei domini.** Senza `--domain-map`, tutte le tabelle finiscono in `default`.
   Assegna gli schemi ai domini con `--domain-map public=core analytics=reporting`.
4. **Controlla le regole di righe.** I filtri vengono convertiti in SQL. Esamina le espressioni booleane
   complesse (`_and`/`_or`/`_exists` annidati) e i filtri attraverso le relationship.
5. **Rifai ciò che è elencato.** Computed field, funzioni tracciate che l'anteprima non ha potuto leggere,
   tentativi ripetuti dei trigger e autenticazione tramite webhook non hanno una conversione automatica.
6. **Configura l'autenticazione.** Se la tua istanza Hasura usa l'autenticazione JWT/webhook, crea un file
   di ambiente di autenticazione ed esegui di nuovo con `--auth-env-file`.
7. **Prova.** Avvia il server Provisa e verifica le query sulle tue sorgenti dati.

## Problemi comuni e risoluzione dei problemi {: #common-issues-and-troubleshooting }

### La connessione viene da una variabile d'ambiente {: #connection-comes-from-an-environment-variable }

Se il `database_url` della sorgente è un riferimento a una variabile d'ambiente (`{"from_env": "PG_URL"}`),
il suo valore non è nell'esportazione. Il convertitore non ne indovina nessuno: la sorgente ha host,
database, utente e password vuoti, e il rapporto la elenca in `Redo by hand`.
Inserisci la connessione nelle impostazioni delle sorgenti dell'importazione (o con `--source-overrides`) e converti
di nuovo.

### Colonne con carattere jolly {: #wildcard-columns }

Quando un'autorizzazione concede `columns: "*"`, il convertitore crea una singola voce di
colonna con carattere jolly. Dopo la conversione, potrebbe essere opportuno sostituirla con elenchi
di colonne espliciti ispezionando lo schema effettivo del database.

### Fedeltà degli event trigger {: #event-trigger-fidelity }

Un event trigger diventa un trigger sulle modifiche della sua tabella, che chiama lo stesso webhook. Scatta
per lotto di modifiche a ogni modifica, sia che la modifica sia passata da Provisa sia direttamente dal database, e invia la tabella e le
righe modificate. I valori vecchi e nuovi per riga, il filtraggio per operazione e i tentativi ripetuti di Hasura non sono riportati;
il rapporto avvisa di ciascuno che si applica.
Fai agire l'handler sulle righe a cui è destinato, e ripeti al suo interno se una chiamata mancata conta.

### Ruoli mancanti {: #missing-roles }

I ruoli vengono raccolti dalle autorizzazioni di tabella, dalle autorizzazioni delle action e dai ruoli ereditati. Se un ruolo esiste in Hasura
ma non compare in nessuno di questi, non comparirà nell'output.

### Campi radice personalizzati {: #custom-root-fields }

I nomi di campo radice `select` e `select_by_pk` danno l'alias della tabella. Gli altri campi radice personalizzati
(`select_aggregate`, `insert`, `update`, `delete`) vengono segnalati per nome: Provisa serve
una tabella come un solo campo, con aggregati e scritture come suo campo `_aggregate` e sue mutazioni.

## Esempio {: #example }

Conversione di un tipico progetto Hasura v2 con due schemi mappati su domini:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Struttura dell'output (abbreviata):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
