# API Admin {: #admin-api }

L'API Admin è un endpoint GraphQL Strawberry esposto su `POST /admin/graphql`. Richiede un ruolo superuser o admin ed è separata dall'endpoint GraphQL dei dati.

## Autenticazione {: #authentication }

Passare le proprie credenziali nell'header `Authorization` usando il provider di autenticazione standard di Provisa:

```yaml
Authorization: Bearer <token>
```

L'accesso admin è governato dalla capability `admin` assegnata a un ruolo.

### Token di accesso personali {: #personal-access-tokens }

Un token di accesso personale è accettato ovunque lo sia un token bearer, questo endpoint compreso. Emettete e revocate i vostri, perché un token è la credenziale del suo titolare. Per questo sta nel vostro profilo nella UI di amministrazione, accanto all'uscita da un'organizzazione e all'eliminazione dell'account, e non sotto una pagina di amministrazione. Un amministratore non emette mai un token per conto di un altro.

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

La postura di sicurezza, `security.mode` (`standard` | `high`), si applica al riavvio:

```http
GET  /admin/security
PUT  /admin/security
```

Le assegnazioni dei modelli di IA, il registro dei modelli di embedding e vettoriali e il limite di frequenza NL hanno effetto dalla richiesta successiva, senza riavvio:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La scheda di crittografia della UI Admin deriva il proprio elenco di provider in tempo reale dal registro di crittografia; i provider non disponibili compaiono ma non sono selezionabili.

`GET`/`HEAD /health` e `GET /setup/status` non richiedono mai l'autenticazione. Saltano il requisito `Authorization: Bearer` anche quando è configurato un provider di autenticazione.

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

Una tabella associativa si dichiara come arco; Provisa non ne scopre mai una. `viaTable` nomina una tabella registrata. Le sue due colonne chiave portano l'arco, e ogni altra colonna diventa un attributo della relazione, filtrabile come qualsiasi campo. `viaTypeColumn` / `viaTypeValue` dividono una tabella ponte in più tipi di arco. Per esempio, le righe di `pet_companions` con `companion_type` pari a `bonded pair`, `littermate` o `shares enclosure` danno tre relazioni distinte sulla stessa coppia di tabelle.

`viaLabelSource` indica da dove viene il nome esposto, e Cypher riceve ogni forma in maiuscolo con trattini bassi. `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa il nome della tabella ponte (`PET_COMPANIONS`) e `fixed` usa l'`alias` dichiarato. Una tabella ponte dichiarata così è un arco. Provisa la toglie dalle etichette dei nodi, quindi non compare mai come pillola di nodo nella UI del grafo.

### Scoperta delle relazioni con l'AI {: #ai-relationship-discovery }

Chiedere a Claude le FK candidate via REST:

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

Rinominare un alias rompe ogni artefatto scritto contro il nome esposto: viste, MV, espressioni di metriche, predicati RLS e contratti DQ. Eliminare una colonna rompe anche quelli, più gli artefatti che salvano il `column_name` fisico, cioè relazioni, associazioni del glossario e assegnazioni di tag. `breaksOn` dice quale. La pagina Tabelle esegue questo controllo al salvataggio e mostra il risultato in una finestra di avviso. [Derivazione](lineage.md) spiega cosa copre la query e cosa no.

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

**L'applicazione** carica lo YAML che avete rivisto, e modificato se lo avete voluto, nell'org che agisce. Usa lo stesso percorso di ricarica a caldo di `PUT /admin/config`. Body della richiesta: `{"config_yaml": "<yaml string>"}`.

L'anteprima non mette mai in cache lato server lo YAML convertito; l'applicazione prende lo YAML fornito, quindi ciò che viene applicato è esattamente ciò che è stato revisionato.

### Interscambio con Apache Ossie {: #apache-ossie-interchange }

Provisa interopera con Apache Ossie (incubating) come confine di importazione ed esportazione.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) costruisce il documento YAML di Ossie dal modello governato attivo a ogni richiesta. Nulla viene messo in cache, quindi il documento non è mai obsoleto. La risposta è `text/yaml` con un'intestazione `Content-Disposition: attachment`. Il documento è un modello con `version`, `name`, `datasets`, `relationships` e `metrics` alla radice. Le tabelle diventano oggetti `dataset`, le colonne oggetti `field`, e le relazioni corrispondono a oggetti `relationship` di Ossie. Un modello senza tabelle viene rifiutato con un `422`.

**Import** (`POST /admin/ossie/import`) accetta un documento Ossie in YAML o JSON e rileva il formato da solo. Analizza il documento e restituisce come oggetto JSON le registrazioni proposte di tabelle, relazioni e metriche. Non viene registrato nulla. Legge solo la forma piatta del documento, e un documento che avvolge il suo modello in `semantic_model` viene rifiutato per nome. Nella UI di amministrazione, la schermata di revisione vi permette di accettare o ridurre le proposte prima che cambi qualcosa.

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

`GET` restituisce impronta, id e provenienza della chiave, mai materiale della chiave. `PUT` imposta o ruota la chiave. Fornite `key_b64` (32 byte grezzi, codificati in base64) per portare la vostra chiave, oppure omettetelo e Provisa ne genera una. Non esiste l'eliminazione, perché ritirare l'ultima chiave renderebbe illeggibile ogni payload che ha avvolto.

## GraphiQL

L'API Admin include GraphiQL su `GET /admin/graphql` nel browser. Serve a consultare l'intero schema admin e a provare le chiamate.

## Viste di gestione del dominio ops {: #ops-domain-management-views }

Ogni installazione crea otto viste SQL nel dominio integrato `ops`. Espongono il log di audit delle query come tabelle governate. Le interrogate via SQL (pgwire), GraphQL e Cypher, con le stesse regole di accesso al dominio, RLS e mascheramento di qualsiasi tabella di business.

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

La granularità si ferma alla tabella. Il log di audit registra `table_ids`, non le singole colonne lette da una query. Il testo delle query è cifrato ed escluso da tutte queste viste; solo il percorso autorizzato di decifratura dell'amministrazione può leggerlo.

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

Poiché le viste sono tabelle governate, potete scrivere le stesse query in GraphQL o Cypher.

## Visualizzatore dei report {: #reports-viewer }

Il visualizzatore dei report si trova su `/admin/reports`. I ruoli privi della capability `observability` non possono raggiungerlo.

Il pannello di sinistra elenca ogni tabella registrata nel dominio `ops`, ordinata per alias. Le otto viste di gestione seminate vi compaiono automaticamente. Un clic su un report qualsiasi lo carica nel visualizzatore dei dati governati a destra.

**Aggiungere un report personalizzato.** Il pulsante "Add report" apre una finestra in cui indicate un nome, una descrizione facoltativa e un'istruzione SELECT. Il salvataggio registra la vista come tabella derivata governata nel dominio `ops`. È catalogata, con controllo degli accessi e interrogabile da ogni superficie, accanto alle viste predefinite.

**Eliminare.** L'icona del cestino compare solo per i report personalizzati. Le viste di gestione seminate non possono essere eliminate da questa interfaccia.

## Anteprima delle tabelle {: #table-preview }

Espandere una riga qualsiasi nella pagina Tables. Il pulsante **Preview** apre una finestra modale larga il 90% con i dati governati in tempo reale della tabella.

Le tabelle basate su API con parametri di percorso obbligatori bloccano l'anteprima finché quei valori non vengono forniti. Un modulo inline raccoglie ogni parametro obbligatorio prima che parta la prima query; gli eventuali parametri di query facoltativi compaiono nello stesso modulo.

## Visualizzatore dei dati governati {: #governed-data-viewer }

Lo stesso componente alimenta la finestra di anteprima e il visualizzatore dei report. Il comportamento è identico nei due contesti.

**Paginazione lato server.** Ogni pagina è una `SELECT *` governata a sé con `LIMIT 101 OFFSET n`. Per pagina compaiono 100 righe; la centounesima segnala se ne esistono altre. L'intero dataset non viene mai caricato nel browser.

**Filtri e ordinamenti spinti in basso.** Ogni intestazione di colonna ha un campo filtro. Un termine di filtro diventa un predicato `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, e un clic di ordinamento produce una clausola `ORDER BY`. Entrambi vengono eseguiti nel database. Un filtro su una tabella da un miliardo di righe scandisce la sorgente, e la sua portata non si limita mai alla pagina da 100 righe che avete davanti.

**Raggruppamento a più livelli.** L'icona Layers in una qualsiasi intestazione di colonna include quella colonna nel raggruppamento. Le colonne di raggruppamento aprono l'`ORDER BY`, così i membri di un gruppo atterrano nella stessa pagina della loro intestazione anche a cavallo delle pagine. Le colonne di chiave primaria vengono aggiunte in coda come discriminante stabile. Le righe di intestazione di gruppo sono comprimibili; comprimerle nasconde i membri senza emettere una nuova query.

**Scelte persistenti.** Le impostazioni di filtro, ordinamento e raggruppamento vengono persistite in `localStorage` sotto `provisa.grid.table:<domain>.<table>` e ripristinate alla visita successiva.

**Esportazione.** Scaricare la pagina corrente come CSV, oppure copiarla negli appunti come testo separato da tabulazioni. L'esportazione copre solo la pagina visibile.
