# Migrazione da Hasura v2 a Provisa

## Prerequisiti

1. Un'istanza Hasura v2 (v2.x) in esecuzione con i metadati esportati.
2. Esportare i metadati con la CLI di Hasura:

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Questo crea una directory `metadata/` contenente `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, ecc.
3. Python 3.11+ con il pacchetto `provisa` installato.

## Utilizzo della CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argomenti

| Argomento | Obbligatorio | Descrizione |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sì | Percorso della directory dei metadati Hasura v2 esportata |

### Opzioni

| Opzione | Predefinito | Descrizione |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Percorso del file YAML di output |
| `--source-overrides FILE` | Nessuno | File YAML con override di connessione per singola origine |
| `--domain-map KEY=VAL ...` | Nessuno | Mappature schema-dominio (ad esempio, `public=core hr=people`) |
| `--auth-env-file FILE` | Nessuno | Percorso del file `.env` con la configurazione di autenticazione JWT/admin-secret |
| `--dry-run` | disattivato | Analizza e valida senza scrivere l'output |

### File di override delle origini

Un file YAML indicizzato per nome dell'origine, con le proprietà di connessione da sovrascrivere:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### File dell'ambiente di autenticazione

Un file in stile `.env` che contiene la configurazione di autenticazione di Hasura da
convertire. Il convertitore effettua le seguenti mappature:

- JWT con `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Autenticazione tramite webhook -> viene emesso un avviso (nessun equivalente in Provisa).

## Matrice di parità delle funzionalità

| Funzionalità di Hasura v2 | Equivalente in Provisa | Note |
| --- | --- | --- |
| **Origini** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Tipo mappato: pg/postgres -> postgresql, mssql -> sqlserver. L'URL di connessione viene analizzato in host/port/database/username/password. Le impostazioni del pool vengono preservate. |
| **Tabelle** (tabelle monitorate) | `tables[]` | Schema e nome tabella vengono preservati. `source_id` collega all'origine. |
| **Nomi tabella personalizzati** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Primo valore non nullo tra `select`, `select_by_pk`, `custom_name`. |
| **Nomi colonna personalizzati** | `columns[].alias` | Mappa il dizionario `custom_column_names` sugli alias delle colonne. |
| **Autorizzazioni di selezione** (colonne, filtro) | `columns[].visible_to[]`, `rls_rules[]` | Gli elenchi di colonne diventano `visible_to`. Sono supportate colonne con carattere jolly (`*`). I filtri vengono convertiti in SQL tramite `bool_expr_to_sql`. |
| **Autorizzazioni di inserimento/aggiornamento** (colonne) | `columns[].writable_by[]` | Gli elenchi di colonne diventano `writable_by`. I ruoli vengono aggiornati con la capability `write`. |
| **Autorizzazioni di eliminazione** | Aggiornamento della capability del ruolo | Il ruolo ottiene la capability `write`. Nessuna mappatura di eliminazione per singola tabella. |
| **Relazioni di oggetto** | `relationships[]` con `cardinality: many-to-one` | La mappatura delle colonne viene preservata. |
| **Relazioni di array** | `relationships[]` con `cardinality: one-to-many` | La mappatura delle colonne viene preservata. |
| **Campi calcolati** | `functions[]` | Mappati su una Function con `returns` che punta all'ID della tabella padre. |
| **Funzioni monitorate** | `functions[]` | `exposed_as` è impostato su mutation per impostazione predefinita. Lo schema viene preservato. |
| **Actions** (handler di stored procedure) | `functions[]` | Convertite in una configurazione Function quando supportate da una stored procedure. |
| **Actions** (handler webhook) | Non convertite | Viene emesso un avviso, inclusa l'URL dell'handler. |
| **Trigger cron** | Non convertiti | Viene emesso un avviso. (Esistono trigger pianificati a runtime, ma il convertitore non li mappa.) |
| **Trigger di evento** | Non convertiti | Viene emesso un avviso. (Esistono trigger di evento a runtime, ma il convertitore non li mappa.) |
| **Ruoli ereditati** | `roles[].parent_role_id` | Il primo ruolo in `role_set` diventa il ruolo padre. Vengono creati tutti i ruoli figli. |
| **Schemi remoti** | `sources[]` (`graphql_remote`) | Registrati come origine `graphql_remote`. Nome, URL, intestazioni e configurazione di autenticazione vengono preservati. |
| **Tabelle enum** | Tabella creata | Il flag `is_enum` non viene riportato (nessun equivalente in Provisa). |
| **Allow list** | Ignorate | Non presenti nel modello dei metadati. |

## Passaggi post-conversione

1. **Rivedere il YAML di output.** Verificare che origini, tabelle e ruoli siano corretti.
2. **Configurare le connessioni delle origini.** Il convertitore analizza gli URL di connessione, ma
   ricorre a `localhost` in caso di errore di analisi. Utilizzare `--source-overrides` o modificare direttamente l'output.
3. **Verificare le assegnazioni di dominio.** Senza `--domain-map`, tutte le tabelle finiscono in `default`.
   Assegnare gli schemi ai domini con `--domain-map public=core analytics=reporting`.
4. **Controllare le regole RLS.** I filtri vengono convertiti in approssimazioni SQL. Le espressioni
   booleane complesse (`_and`/`_or`/`_exists` annidate) devono essere riviste manualmente.
5. **Esaminare gli avvisi.** Il convertitore stampa su stderr un riepilogo degli avvisi per le
   funzionalità che non riesce a mappare (trigger di evento, trigger cron, actions basate su webhook).
6. **Configurare l'autenticazione.** Se la propria istanza Hasura utilizza autenticazione JWT/webhook, creare
   un file di ambiente di autenticazione e rieseguire con `--auth-env-file`.
7. **Testare.** Avviare il server Provisa e verificare le query sulle proprie origini dati.

## Problemi comuni e risoluzione dei problemi

### L'URL di connessione non viene analizzato

Se `database_url` dell'origine è un riferimento a una variabile d'ambiente (`{"from_env": "PG_URL"}`),
il convertitore non può risolverlo al momento della conversione. L'origine avrà valori
segnaposto (`host: localhost`, `database: default`). Correggere con `--source-overrides`.

### Colonne con carattere jolly

Quando un'autorizzazione concede `columns: "*"`, il convertitore crea una singola voce di
colonna con carattere jolly. Dopo la conversione, potrebbe essere opportuno sostituirla con elenchi
di colonne espliciti ispezionando lo schema effettivo del database.

### Fedeltà dei trigger di evento

I trigger di evento vengono convertiti con `operations` e `webhook_url`, ma le garanzie di
consegna specifiche di Hasura (esattamente una volta, riconsegna) non hanno equivalenti diretti
in Provisa. Rivedere la sezione `event_triggers` e configurare di conseguenza l'infrastruttura webhook.

### Ruoli mancanti

I ruoli vengono raccolti solo dalle voci di autorizzazione. Se un ruolo esiste in Hasura ma
non ha autorizzazioni su alcuna tabella o action, non comparirà nell'output.

### Campi root personalizzati

Solo i campi root `select` e `select_by_pk` vengono utilizzati per l'alias della tabella. Altri
campi root personalizzati (`select_aggregate`, `insert`, `update`, `delete`) non vengono mappati.

## Esempio

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

Struttura dell'output:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
