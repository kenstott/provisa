# API Admin

L'API Admin è un endpoint GraphQL Strawberry esposto su `POST /admin/graphql` (REQ-533). Richiede un ruolo superuser o admin (REQ-125, REQ-060) ed è separata dall'endpoint GraphQL dei dati (REQ-533).

## Autenticazione

Passare le proprie credenziali nell'header `Authorization` usando il provider di autenticazione standard di Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

L'accesso admin è governato dalla capability `admin` assegnata a un ruolo (REQ-060, REQ-042).

### Token di accesso personali

Un token di accesso personale è accettato ovunque lo sia un bearer token, questo endpoint compreso. Emetterne e revocarne uno è un'operazione self-service — è la credenziale di chi lo detiene, quindi risiede nel profilo utente della UI Admin anziché sotto una pagina di amministrazione, accanto all'uscita da un'organizzazione e all'eliminazione dell'account. Un amministratore non conia token per conto di qualcun altro. (REQ-1263)

| Route | Effetto |
| ------- | -------- |
| `POST /auth/tokens` | Conia un token per chi chiama. Corpo: `name`, opzionalmente `role_id`, `scopes`, `expires_in_days` (1–366). La risposta è l'unico punto in cui il segreto compare |
| `GET /auth/tokens` | I token attivi di chi chiama in questa organizzazione — prefisso visualizzato, nome, marche temporali del ciclo di vita e l'hash che identifica un token per la revoca. Mai una credenziale funzionante |
| `DELETE /auth/tokens/{token_hash}` | Revoca uno dei token di chi chiama. 404 quando non è suo oppure è già stato revocato |

Omettere `role_id` lascia che il token si risolva sul ruolo detenuto dal proprietario; nominarne uno restringe il token al di sotto del proprietario. La revoca avviene anche implicitamente: rimuovere l'appartenenza di un utente a un'organizzazione revoca i suoi token per quell'organizzazione. Vedere [Modello di sicurezza](security.md#token-di-accesso-personali) per la credenziale in sé.

## Capability

### Gestione della configurazione

Scaricare la configurazione attualmente in esecuzione (REQ-164):

```http
GET /admin/config
```

Restituisce il `config.yaml` completo come file YAML. Caricare una nuova configurazione (REQ-164):

```http
PUT /admin/config
```

Provisa valida lo YAML, ricarica i cataloghi e rigenera gli schemi (REQ-012, REQ-253). Nessun riavvio necessario.

### Impostazioni di runtime

Leggere e scrivere le impostazioni di piattaforma a runtime senza modificare il file di configurazione (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie delle impostazioni copre il redirect dei risultati voluminosi, il campionamento e il limite di righe predefiniti, il TTL della cache delle risposte, la convenzione di denominazione, il tracciamento automatico delle FK per le relazioni, il DSN dello store di materializzazione, la memoria del motore di federazione (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e l'intera superficie di tuning della pipeline di tracing OpenTelemetry (REQ-1082). Sono esposti anche i limiti di traversata del GraphQL remoto e le impostazioni del warm tier e della cache di lettura (REQ-1081, REQ-1083).

Postura di sicurezza — `security.mode` (`standard` | `high`) — applicata al riavvio (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Le assegnazioni dei modelli AI, il registro dei modelli di embedding/vettoriali e il rate limit NL — hanno effetto alla richiesta successiva, senza riavvio (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La scheda di crittografia della UI Admin deriva il proprio elenco di provider in tempo reale dal registro di crittografia; i provider non disponibili compaiono ma non sono selezionabili (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` sono sempre non autenticati — aggirano il requisito `Authorization: Bearer` anche quando è configurato un provider di autenticazione (REQ-539).

### Motore di federazione

Leggere o cambiare il motore usato dal deployment (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` restituisce la chiave del motore attivo e i campi di configurazione di cui ha bisogno. `PUT` accetta un corpo con `engine` (la chiave) e gli eventuali campi specifici del motore; la scelta viene persistita nella configurazione di piattaforma e si lega al riavvio successivo del servizio. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

### Editor delle relazioni

Elencare le relazioni (REQ-166):

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

Creare una relazione (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
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

### Scoperta delle relazioni con l'AI

Avviare l'analisi delle FK basata su Claude via REST (REQ-167, REQ-018):

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

### Introspezione dello schema

Sfogliare le tabelle pubblicate su tutte le origini (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Verifica delle dipendenze di colonna (REQ-1484)

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

### Gestione delle viste

Registrare una vista materializzata (REQ-133, REQ-135):

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

Avviare un aggiornamento manuale (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registrazione delle origini a grafo

Le origini Neo4j e SPARQL si registrano tramite endpoint REST (non tramite l'API Admin GraphQL) (REQ-295, REQ-297):

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

Una volta registrate, le tabelle compaiono nello schema GraphQL e sono interrogabili come quelle di qualsiasi altra origine (REQ-016).

### Importazione Hasura / DDN (REQ-1483)

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

L'anteprima non mette mai in cache lato server lo YAML convertito; l'applicazione prende lo YAML fornito, quindi ciò che viene applicato è esattamente ciò che è stato revisionato. [tool-verified: `provisa/api/admin/import_router.py`]

### Interscambio con Apache Ossie (REQ-1316, REQ-1321)

Provisa interopera con Apache Ossie (incubating) come confine di importazione ed esportazione.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**L'esportazione** (`GET /admin/ossie`) deriva il documento YAML Ossie dal modello governato attivo a ogni richiesta — non viene mai messo in cache, quindi non può essere obsoleto. La risposta è `text/yaml` con un header `Content-Disposition: attachment`. Le tabelle diventano oggetti `dataset`, le colonne diventano oggetti `field` e le relazioni si mappano su oggetti `relationship` di Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**L'importazione** (`POST /admin/ossie/import`) accetta un documento Ossie YAML o JSON (il formato si rileva da sé). Analizza il documento e restituisce le registrazioni proposte di tabelle e relazioni come oggetto JSON — nulla viene registrato. La schermata di revisione nella UI Admin consente di accettare o ridurre le proposte prima che scatti qualsiasi mutazione. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Object storage (REQ-1046, REQ-1048, REQ-1049)

Leggere o configurare lo storage di materializzazione dell'organizzazione:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` riporta quanta parte della quota di storage di piattaforma l'organizzazione sta usando. `PUT` registra il DSN di storage dell'organizzazione stessa (cifrato a riposo; mai restituito da GET). Una volta impostato, le materializzazioni dell'organizzazione atterrano nel suo bucket e non vengono più conteggiate sulla quota di piattaforma. Inviare `storage_url: null` lo azzera e riporta l'organizzazione sullo store di piattaforma. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Crittografia dell'organizzazione (REQ-1574)

Impostare o ruotare la chiave di crittografia a riposo dell'organizzazione:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` restituisce fingerprint, id e provenienza della chiave — mai il materiale della chiave. `PUT` imposta o ruota la chiave. Fornire `key_b64` (32 byte grezzi, codificati in base64) per portare la propria chiave, oppure ometterlo perché sia Provisa a generarne una. Non esiste un'eliminazione: ritirare l'ultima chiave renderebbe illeggibile ogni payload da essa protetto. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

L'API Admin include GraphiQL su `GET /admin/graphql` nel browser (REQ-622). Serve a esplorare in modo interattivo l'intero schema admin.

## Viste di gestione del dominio ops (REQ-1386)

Otto viste SQL vengono seminate nel dominio integrato `ops` a ogni installazione. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Espongono il log di audit delle query come tabelle governate — interrogabili via SQL (pgwire), GraphQL e Cypher sotto le stesse regole di accesso al dominio, RLS e mascheramento di qualsiasi tabella di business.

`org_admin` è designato steward del dominio ops al momento della semina, così il dominio non compare mai come lacuna di governance in `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

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

Oggi valgono due limiti. La granularità è a livello di tabella — il log di audit registra `table_ids`, non le singole colonne accedute. Il testo delle query è cifrato (REQ-689) ed escluso da ognuna di queste viste; è accessibile solo attraverso il percorso di decifratura admin autorizzato. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

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

Le stesse query funzionano in GraphQL o Cypher su qualsiasi protocollo di trasporto governato — pgwire, Arrow Flight o Bolt. [inferred from governed-surface design]

## Visualizzatore dei report (REQ-1390)

Il visualizzatore dei report si trova su `/admin/reports`. I ruoli privi della capability `observability` non possono raggiungerlo.

Il pannello di sinistra elenca ogni tabella registrata nel dominio `ops`, ordinata per alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Le otto viste di gestione seminate vi compaiono automaticamente. Un clic su un report qualsiasi lo carica nel visualizzatore dei dati governati a destra.

**Aggiungere un report personalizzato.** Il pulsante "Add report" apre una finestra di dialogo. Vanno forniti un nome, una descrizione facoltativa e un'istruzione SELECT. Il salvataggio registra la vista come tabella derivata governata nel dominio `ops` — catalogata, sottoposta a controllo degli accessi e interrogabile attraverso ogni superficie accanto alle viste seminate. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Eliminare.** L'icona del cestino compare solo per i report personalizzati. Le viste di gestione seminate non possono essere eliminate da questa interfaccia. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Anteprima delle tabelle (REQ-1392)

Espandere una riga qualsiasi nella pagina Tables. Il pulsante **Preview** apre una finestra modale larga il 90% con i dati governati in tempo reale della tabella. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Le tabelle basate su API con parametri di percorso obbligatori bloccano l'anteprima finché quei valori non vengono forniti. Un modulo inline raccoglie ogni parametro obbligatorio prima che parta la prima query; gli eventuali parametri di query facoltativi compaiono nello stesso modulo. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visualizzatore dei dati governati (REQ-1391)

Lo stesso componente alimenta la finestra di anteprima e il visualizzatore dei report. Il comportamento è identico nei due contesti.

**Paginazione lato server.** Ogni pagina è una `SELECT *` governata a sé con `LIMIT 101 OFFSET n`. Per pagina compaiono 100 righe; la centounesima segnala se ne esistono altre. L'intero dataset non viene mai caricato nel browser. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtri e ordinamenti spinti in basso.** Ogni intestazione di colonna ha un campo di filtro. I termini di filtro diventano predicati `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; i clic di ordinamento producono clausole `ORDER BY`. Entrambi arrivano al database — un filtro su una tabella da un miliardo di righe percorre l'origine, non le 100 righe che si hanno davanti. [tool-verified: `nativeParams.ts:53-70`]

**Raggruppamento a più livelli.** L'icona Layers in una qualsiasi intestazione di colonna include quella colonna nel raggruppamento. Le colonne di raggruppamento aprono l'`ORDER BY`, così i membri di un gruppo atterrano nella stessa pagina della loro intestazione anche a cavallo delle pagine. Le colonne di chiave primaria vengono aggiunte in coda come discriminante stabile. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Le righe di intestazione di gruppo sono comprimibili; comprimerle nasconde i membri senza emettere una nuova query. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Scelte persistenti.** Le impostazioni di filtro, ordinamento e raggruppamento vengono persistite in `localStorage` sotto `provisa.grid.table:<domain>.<table>` e ripristinate alla visita successiva. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Esportazione.** Scaricare la pagina corrente come CSV, oppure copiarla negli appunti come testo separato da tabulazioni. L'esportazione copre solo la pagina visibile. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
