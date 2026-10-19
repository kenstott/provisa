# API Admin {: #admin-api }

L'API Admin è un endpoint GraphQL Strawberry esposto su `POST /admin/graphql`. Richiede un ruolo superuser o admin ed è separata dall'endpoint GraphQL dei dati.

## Autenticazione {: #authentication }

Passare le proprie credenziali nell'header `Authorization` usando il provider di autenticazione standard di Provisa:

```yaml
Authorization: Bearer <token>
```

L'accesso admin è governato dalla capability `admin` assegnata a un ruolo.

### Token di accesso personali {: #personal-access-tokens }

Un token di accesso personale è accettato ovunque lo sia un bearer token, questo endpoint compreso. Emetterne e revocarne uno è un'operazione self-service — è la credenziale di chi lo detiene, quindi risiede nel profilo utente della UI Admin anziché sotto una pagina di amministrazione, accanto all'uscita da un'organizzazione e all'eliminazione dell'account. Un amministratore non conia token per conto di qualcun altro.

| Route | Effetto |
| ------- | -------- |
| `POST /auth/tokens` | Conia un token per chi chiama. Corpo: `name`, opzionalmente `role_id`, `scopes`, `expires_in_days` (1–366). La risposta è l'unico punto in cui il segreto compare |
| `GET /auth/tokens` | I token attivi di chi chiama in questa organizzazione — prefisso visualizzato, nome, marche temporali del ciclo di vita e l'hash che identifica un token per la revoca. Mai una credenziale funzionante |
| `DELETE /auth/tokens/{token_hash}` | Revoca uno dei token di chi chiama. 404 quando non è suo oppure è già stato revocato |

Omettere `role_id` lascia che il token si risolva sul ruolo detenuto dal proprietario; nominarne uno restringe il token al di sotto del proprietario. La revoca avviene anche implicitamente: rimuovere l'appartenenza di un utente a un'organizzazione revoca i suoi token per quell'organizzazione. Vedere [Modello di sicurezza](security.md#personal-access-tokens) per la credenziale in sé.

## Capability {: #capabilities }

### Gestione della configurazione {: #config-management }

Scaricare la configurazione attualmente in esecuzione:

```http
GET /admin/config
```

Restituisce il `config.yaml` completo come file YAML. Caricare una nuova configurazione:

```http
PUT /admin/config
```

Provisa valida lo YAML, ricarica i cataloghi e rigenera gli schemi. Nessun riavvio necessario.

### Impostazioni di runtime {: #runtime-settings }

Leggere e scrivere le impostazioni di piattaforma a runtime senza modificare il file di configurazione:

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie delle impostazioni copre il redirect dei risultati voluminosi, il campionamento e il limite di righe predefiniti, il TTL della cache delle risposte, la convenzione di denominazione, il tracciamento automatico delle FK per le relazioni, il DSN dello store di materializzazione, la memoria del motore di federazione (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e l'intera superficie di tuning della pipeline di tracing OpenTelemetry. Sono esposti anche i limiti di traversata del GraphQL remoto e le impostazioni del warm tier e della cache di lettura.

La scheda **Server dei connettori** indica, per ogni connettore con un server incluso (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), una cartella contenente un bundle server compilato per questo host (`connectors.<connector>.bundle_dir`, variabile d'ambiente `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Vuota, il server del connettore viene scaricato dalla release fissata di `kenstott/calcite`. Impostata, la cartella viene usata così com'è: il suo launcher deve trovarsi a `bin/pgwire-<connector>`, e una cartella senza di esso impedisce l'avvio del server del connettore con un errore che nomina l'impostazione; la release non viene mai scaricata al suo posto. L'impostazione viene letta ogni volta che il server di un connettore viene avviato.

Postura di sicurezza — `security.mode` (`standard` | `high`) — applicata al riavvio:

```http
GET  /admin/security
PUT  /admin/security
```

Le assegnazioni dei modelli AI, il registro dei modelli di embedding/vettoriali e il rate limit NL — hanno effetto alla richiesta successiva, senza riavvio:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La scheda di crittografia della UI Admin deriva il proprio elenco di provider in tempo reale dal registro di crittografia; i provider non disponibili compaiono ma non sono selezionabili.

`GET`/`HEAD /health` e `GET /setup/status` sono sempre non autenticati — aggirano il requisito `Authorization: Bearer` anche quando è configurato un provider di autenticazione.

### Motore di federazione {: #federation-engine }

Leggere o cambiare il motore usato dal deployment:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` restituisce la chiave del motore attivo e i campi di configurazione di cui ha bisogno. `PUT` accetta un corpo con `engine` (la chiave) e gli eventuali campi specifici del motore; la scelta viene persistita nella configurazione di piattaforma e si lega al riavvio successivo del servizio.

### Editor delle relazioni {: #relationship-editor }

Elencare le relazioni:

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Creare una relazione. Un id già esistente viene rifiutato; una relazione si modifica con `updateRelationship`, che accetta lo stesso input:

```graphql
mutation {
  createRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

Dichiarare una relazione basata su una tabella di giunzione:

```graphql
mutation {
  createRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

Una tabella associativa si dichiara come arco, non viene mai scoperta. `viaTable` nomina una tabella registrata; le sue due colonne chiave portano l'arco, e ogni colonna rimanente diventa un attributo della relazione, filtrabile come qualunque altro campo. `viaTypeColumn` / `viaTypeValue` suddividono una stessa tabella di giunzione in più tipi di arco — tre righe di `pet_companions` con `companion_type` pari a `bonded pair`, `littermate` e `shares enclosure` sono tre relazioni distinte sulla stessa coppia di tabelle.

`viaLabelSource` indica da dove proviene il nome esposto, e tutte e tre le forme vengono portate in UPPER_SNAKE_CASE per Cypher: `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa il nome della tabella di giunzione stessa (`PET_COMPANIONS`), `fixed` usa l'`alias` dichiarato. Una tabella di giunzione dichiarata così è un arco e non un'entità — esce dalle etichette dei nodi, quindi non compare mai come pillola di nodo nell'interfaccia del grafo.

### Scoperta delle relazioni con l'AI {: #ai-relationship-discovery }

Avviare l'analisi delle FK basata su Claude via REST:

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Restituisce le FK candidate ordinate per confidenza. Accettare una candidata:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspezione dello schema {: #schema-introspection }

Sfogliare le tabelle pubblicate su tutte le origini:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Verifica delle dipendenze di colonna {: #column-dependency-check }

Prima di salvare una modifica a una tabella che rinomina l'alias SQL di una colonna o elimina una
colonna, chiedere che cos'altro vi fa riferimento:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Rinominare un alias rompe ogni artefatto scritto sul nome esposto — viste, MV, espressioni delle
metriche, predicati RLS, contratti DQ. Eliminare una colonna rompe quelli più gli artefatti che
memorizzano il `column_name` fisico: relazioni, associazioni del glossario, assegnazioni di tag.
`breaksOn` dice quale. La pagina Tables esegue questa query al salvataggio e ne mostra il risultato
in una finestra informativa. Vedere [Derivazione](lineage.md) per che cosa la query copre e che cosa
non può coprire.

### Gestione delle viste {: #view-management }

Registrare una vista materializzata:

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Avviare un aggiornamento manuale:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registrazione delle origini a grafo {: #graph-source-registration }

Le origini Neo4j e SPARQL si registrano tramite endpoint REST (non tramite l'API Admin GraphQL):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Una volta registrate, le tabelle compaiono nello schema GraphQL e sono interrogabili come quelle di qualsiasi altra origine.

### Importazione Hasura / DDN {: #hasura-ddn-import }

Converte un progetto Hasura v2 o Hasura DDN esistente in configurazione Provisa attraverso la UI Admin o l'API, senza che nulla venga applicato prima dell'approvazione.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**L'anteprima** converte l'archivio caricato e restituisce il `config_yaml` proposto, un elenco di avvisi e un riepilogo di ciò che è stato trovato (conteggi di origini, domini, tabelle, colonne, ruoli, relazioni e regole RLS). Nulla viene scritto nel database del tenant. Corpo della richiesta:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` è `"auto"` (rilevato dalla struttura dell'archivio), `"hasura_v2"` oppure `"ddn"`.

**L'applicazione** prende lo YAML revisionato (ed eventualmente modificato) e lo carica nell'organizzazione attiva — lo stesso percorso di hot-reload di `PUT /admin/config`. Corpo della richiesta: `{"config_yaml": "<yaml string>"}`.

L'anteprima non mette mai in cache lato server lo YAML convertito; l'applicazione prende lo YAML fornito, quindi ciò che viene applicato è esattamente ciò che è stato revisionato.

### Interscambio con Apache Ossie {: #apache-ossie-interchange }

Provisa interopera con Apache Ossie (incubating) come confine di importazione ed esportazione.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) deriva il documento YAML Ossie dal modello governato attivo a ogni richiesta — non viene mai messo in cache, quindi non può essere obsoleto. La risposta è `text/yaml` con un'intestazione `Content-Disposition: attachment`. Il documento è un modello con `version`, `name`, `datasets`, `relationships` e `metrics` alla radice: le tabelle diventano oggetti `dataset`, le colonne oggetti `field`, e le relazioni corrispondono a oggetti `relationship` di Ossie. Un modello senza tabelle viene rifiutato con `422`.

**Import** (`POST /admin/ossie/import`) accetta un documento Ossie in YAML o JSON (il formato viene rilevato automaticamente). Analizza il documento e restituisce le registrazioni proposte di tabelle, relazioni e metriche come oggetto JSON — non viene registrato nulla. Legge solo la forma piatta del documento; un documento che avvolge il suo modello in `semantic_model` viene rifiutato per nome. La schermata di revisione dell'interfaccia di amministrazione consente di accettare o ridurre le proposte prima che venga eseguita qualsiasi mutazione.

### Object storage

Leggere o configurare lo storage di materializzazione dell'organizzazione:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` riporta quanta parte della quota di storage di piattaforma l'organizzazione sta usando. `PUT` registra il DSN di storage dell'organizzazione stessa (cifrato a riposo; mai restituito da GET). Una volta impostato, le materializzazioni dell'organizzazione atterrano nel suo bucket e non vengono più conteggiate sulla quota di piattaforma. Inviare `storage_url: null` lo azzera e riporta l'organizzazione sullo store di piattaforma.

### Crittografia dell'organizzazione {: #org-encryption }

Impostare o ruotare la chiave di crittografia a riposo dell'organizzazione:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` restituisce fingerprint, id e provenienza della chiave — mai il materiale della chiave. `PUT` imposta o ruota la chiave. Fornire `key_b64` (32 byte grezzi, codificati in base64) per portare la propria chiave, oppure ometterlo perché sia Provisa a generarne una. Non esiste un'eliminazione: ritirare l'ultima chiave renderebbe illeggibile ogni payload da essa protetto.

## GraphiQL

L'API Admin include GraphiQL su `GET /admin/graphql` nel browser. Serve a esplorare in modo interattivo l'intero schema admin.

## Viste di gestione del dominio ops {: #ops-domain-management-views }

Otto viste SQL vengono seminate nel dominio integrato `ops` a ogni installazione. Espongono il log di audit delle query come tabelle governate — interrogabili via SQL (pgwire), GraphQL e Cypher sotto le stesse regole di accesso al dominio, RLS e mascheramento di qualsiasi tabella di business.

`org_admin` è designato steward del dominio ops al momento della semina, così il dominio non compare mai come lacuna di governance in `stale_metadata`.

| Vista | A che cosa risponde |
| --- | --- |
| `usage_ranking` | Conteggio delle query e utenti distinti per tabella registrata; le tabelle a zero accessi emergono come candidate alla deprecazione |
| `deprecated_usage` | Ogni accesso a una tabella o colonna che porta il tag `deprecated` — i consumatori attivi che bloccano una rimozione sicura |
| `pii_access` | Ogni accesso a una tabella o colonna che porta il tag `pii`: chi l'ha interrogata, sotto quale ruolo, su quale superficie |
| `policy_denials` | Tutti i tentativi di accesso che la governance ha respinto (HTTP 401/403) |
| `surface_mix` | Conteggio giornaliero delle query e utenti distinti per superficie di protocollo (SQL, GraphQL, Cypher, gRPC, ecc.) |
| `query_health` | Conteggio giornaliero degli errori e latenza media/massima per superficie |
| `stale_metadata` | Tabelle e colonne prive di descrizione; domini privi di steward |
| `join_hotspots` | Coppie di tabelle interrogate insieme più spesso — candidate alla materializzazione o alla cache |

Valgono due limiti. La granularità è a livello di tabella — il log di audit registra `table_ids`, non le singole colonne accedute. Il testo delle query è cifrato ed escluso da ognuna di queste viste; è accessibile solo attraverso il percorso di decifratura admin autorizzato.

Un ruolo ha bisogno dell'accesso al dominio `ops` perché queste viste siano visibili. Si concede allo stesso modo dell'accesso a qualsiasi altro dominio.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Le stesse query funzionano in GraphQL o Cypher su qualsiasi protocollo di trasporto governato — pgwire, Arrow Flight o Bolt.

## Visualizzatore dei report {: #reports-viewer }

Il visualizzatore dei report si trova su `/admin/reports`. I ruoli privi della capability `observability` non possono raggiungerlo.

Il pannello di sinistra elenca ogni tabella registrata nel dominio `ops`, ordinata per alias. Le otto viste di gestione seminate vi compaiono automaticamente. Un clic su un report qualsiasi lo carica nel visualizzatore dei dati governati a destra.

**Aggiungere un report personalizzato.** Il pulsante "Add report" apre una finestra di dialogo. Vanno forniti un nome, una descrizione facoltativa e un'istruzione SELECT. Il salvataggio registra la vista come tabella derivata governata nel dominio `ops` — catalogata, sottoposta a controllo degli accessi e interrogabile attraverso ogni superficie accanto alle viste seminate.

**Eliminare.** L'icona del cestino compare solo per i report personalizzati. Le viste di gestione seminate non possono essere eliminate da questa interfaccia.

## Anteprima delle tabelle {: #table-preview }

Espandere una riga qualsiasi nella pagina Tables. Il pulsante **Preview** apre una finestra modale larga il 90% con i dati governati in tempo reale della tabella.

Le tabelle basate su API con parametri di percorso obbligatori bloccano l'anteprima finché quei valori non vengono forniti. Un modulo inline raccoglie ogni parametro obbligatorio prima che parta la prima query; gli eventuali parametri di query facoltativi compaiono nello stesso modulo.

## Visualizzatore dei dati governati {: #governed-data-viewer }

Lo stesso componente alimenta la finestra di anteprima e il visualizzatore dei report. Il comportamento è identico nei due contesti.

**Paginazione lato server.** Ogni pagina è una `SELECT *` governata a sé con `LIMIT 101 OFFSET n`. Per pagina compaiono 100 righe; la centounesima segnala se ne esistono altre. L'intero dataset non viene mai caricato nel browser.

**Filtri e ordinamenti spinti in basso.** Ogni intestazione di colonna ha un campo di filtro. I termini di filtro diventano predicati `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; i clic di ordinamento producono clausole `ORDER BY`. Entrambi arrivano al database — un filtro su una tabella da un miliardo di righe percorre l'origine, non le 100 righe che si hanno davanti.

**Raggruppamento a più livelli.** L'icona Layers in una qualsiasi intestazione di colonna include quella colonna nel raggruppamento. Le colonne di raggruppamento aprono l'`ORDER BY`, così i membri di un gruppo atterrano nella stessa pagina della loro intestazione anche a cavallo delle pagine. Le colonne di chiave primaria vengono aggiunte in coda come discriminante stabile. Le righe di intestazione di gruppo sono comprimibili; comprimerle nasconde i membri senza emettere una nuova query.

**Scelte persistenti.** Le impostazioni di filtro, ordinamento e raggruppamento vengono persistite in `localStorage` sotto `provisa.grid.table:<domain>.<table>` e ripristinate alla visita successiva.

**Esportazione.** Scaricare la pagina corrente come CSV, oppure copiarla negli appunti come testo separato da tabulazioni. L'esportazione copre solo la pagina visibile.
