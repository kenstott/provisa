# Admin API

L'Admin API è un endpoint Strawberry GraphQL disponibile su `POST /admin/graphql` (REQ-533). Richiede un ruolo superuser o admin (REQ-125, REQ-060) ed è distinta dall'endpoint GraphQL dei dati (REQ-533).

## Autenticazione

Passare le credenziali nell'header `Authorization` utilizzando il provider di autenticazione standard di Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

L'accesso admin è governato dalla capacità `admin` assegnata a un ruolo (REQ-060, REQ-042).

### Token di accesso personali

Un token di accesso personale è accettato ovunque sia accettato un token bearer, incluso questo endpoint. L'emissione e la revoca sono self-service — è la credenziale privata di chi lo detiene, quindi risiede nel profilo utente della UI di amministrazione anziché in una pagina da amministratore, accanto all'abbandono di un'organizzazione e all'eliminazione dell'account. Un amministratore non emette token per conto di altri. (REQ-1263)

| Rotta | Effetto |
| ------- | -------- |
| `POST /auth/tokens` | Emette un token per il chiamante. Corpo: `name`, più facoltativamente `role_id`, `scopes`, `expires_in_days` (1–366). La risposta è l'unico punto in cui il segreto compare |
| `GET /auth/tokens` | I token attivi del chiamante in questa organizzazione — prefisso di visualizzazione, nome, marche temporali del ciclo di vita e l'hash che identifica un token per la revoca. Mai una credenziale utilizzabile |
| `DELETE /auth/tokens/{token_hash}` | Revoca uno dei token del chiamante. 404 quando non è suo o è già revocato |

Omettere `role_id` lascia che il token si risolva nel ruolo posseduto dal proprietario; indicare un ruolo restringe il token al di sotto del proprietario. La revoca avviene anche implicitamente: rimuovere l'appartenenza di un utente a un'organizzazione revoca i suoi token per quell'organizzazione. Per la credenziale in sé vedere il [modello di sicurezza](security.md#token-di-accesso-personali).

## Capacità

### Gestione della configurazione

Scaricare la configurazione attualmente in esecuzione (REQ-164):

```http
GET /admin/config
```

Restituisce il file `config.yaml` completo in formato YAML. Caricare una nuova configurazione (REQ-164):

```http
PUT /admin/config
```

Provisa convalida lo YAML, ricarica i cataloghi e rigenera gli schemi (REQ-012, REQ-253). Non è richiesto alcun riavvio.

### Impostazioni di runtime

Leggere e scrivere le impostazioni della piattaforma a runtime senza modificare il file di configurazione (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

La superficie delle impostazioni copre il reindirizzamento dei risultati di grandi dimensioni, il campionamento predefinito e il limite di righe, il TTL della cache delle risposte, la convenzione di denominazione, il tracciamento automatico delle chiavi esterne delle relazioni, il DSN del datastore di materializzazione, la memoria del motore di federazione (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e l'intera superficie di ottimizzazione della pipeline di tracciamento OpenTelemetry (REQ-1082). Sono inoltre esposti i limiti di attraversamento GraphQL remoto e le impostazioni di warm-tier/cache di lettura (REQ-1081, REQ-1083).

Postura di sicurezza — `security.mode` (`standard` | `high`) — applicata al riavvio (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Assegnazioni dei modelli IA, registro dei modelli di embedding/vettoriali e limite di frequenza NL — applicati al riavvio (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

La scheda di crittografia dell'admin deriva il proprio elenco di provider in tempo reale dal registro di crittografia; i provider non disponibili vengono visualizzati ma non sono selezionabili (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` non richiedono mai autenticazione — bypassano il requisito `Authorization: Bearer` anche quando è configurato un provider di autenticazione (REQ-539).

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

### Individuazione delle relazioni con IA

Avviare l'analisi delle chiavi esterne basata su Claude tramite REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Restituisce i candidati chiave esterna classificati per livello di confidenza. Accettare un candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspezione dello schema

Sfogliare le tabelle pubblicate in tutte le origini (REQ-008):

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

### Controllo delle dipendenze di colonna (REQ-1484)

Prima di salvare una modifica a una tabella che rinomina l'alias SQL di una colonna o elimina una
colonna, chiedi cos'altro la referenzia:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Rinominare un alias rompe ogni artefatto scritto in riferimento al nome esposto — viste, MV,
espressioni di metrica, predicati RLS, contratti DQ. Eliminare una colonna rompe questi più gli
artefatti che memorizzano il `column_name` fisico: relazioni, associazioni al glossario,
assegnazioni di tag. `breaksOn` indica quale dei due. La pagina Tables esegue questo controllo al
salvataggio e mostra il risultato come finestra di dialogo consultiva. Vedere
[Lineage](lineage.md) per cosa copre la query e cosa non può coprire.

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

### Registrazione di origini a grafo

Le origini Neo4j e SPARQL vengono registrate tramite endpoint REST (non l'Admin API GraphQL) (REQ-295, REQ-297):

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

Una volta registrate, le tabelle appaiono nello schema GraphQL e sono interrogabili come qualsiasi altra origine (REQ-016).

## GraphiQL

L'Admin API include GraphiQL su `GET /admin/graphql` nel browser (REQ-622). Utilizzarlo per esplorare in modo interattivo l'intero schema admin.

## Viste di gestione del dominio ops (REQ-1386)

Otto viste SQL vengono inizializzate nel dominio integrato `ops` in ogni installazione. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Espongono il registro di audit delle query come tabelle governate — interrogabili tramite SQL (pgwire), GraphQL e Cypher, sotto le stesse regole di accesso al dominio, RLS e mascheramento di qualsiasi tabella di business.

`org_admin` viene impostato come steward del dominio ops al momento dell'inizializzazione, così il dominio non compare mai come lacuna di governance in `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Vista | A cosa risponde |
| --- | --- |
| `usage_ranking` | Conteggio query e utenti distinti per tabella registrata; le tabelle senza accessi emergono come candidate al ritiro |
| `deprecated_usage` | Ogni accesso a una tabella o colonna con tag `deprecated` — i consumatori attivi che bloccano una rimozione sicura |
| `pii_access` | Ogni accesso a una tabella o colonna con tag `pii`: chi ha interrogato, sotto quale ruolo, attraverso quale superficie |
| `policy_denials` | Ogni tentativo di accesso rifiutato dalla governance (HTTP 401/403) |
| `surface_mix` | Conteggio query giornaliero e utenti distinti per superficie di protocollo (SQL, GraphQL, Cypher, gRPC, ecc.) |
| `query_health` | Conteggio errori giornaliero e latenza media/massima per superficie |
| `stale_metadata` | Tabelle e colonne prive di descrizione; domini privi di steward |
| `join_hotspots` | Le coppie di tabelle interrogate insieme più di frequente — candidate alla materializzazione o alla cache |

Oggi valgono due limiti. La risoluzione è a livello di tabella — il registro di audit memorizza `table_ids`, non le singole colonne consultate. Il testo delle query è cifrato (REQ-689) ed escluso da ogni vista qui presente; è accessibile solo tramite il percorso amministrativo autorizzato di decifratura. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Un ruolo ha bisogno dell'accesso al dominio `ops` perché queste viste siano visibili. Concedilo come concederesti l'accesso a qualsiasi altro dominio.

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

Le stesse query funzionano come GraphQL o Cypher su qualsiasi trasporto governato — pgwire, Arrow Flight o Bolt. [inferred from governed-surface design]

## Visualizzatore di report (REQ-1390)

Il visualizzatore di report si trova su `/admin/reports`. I ruoli privi della capacità `observability` non possono raggiungerlo.

Il pannello sinistro elenca ogni tabella registrata nel dominio `ops`, ordinata per alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Le otto viste di gestione inizializzate compaiono lì automaticamente. Clicca un report qualsiasi per caricarlo nel visualizzatore di dati governato a destra.

**Aggiungere un report personalizzato.** Il pulsante "Aggiungi report" apre una finestra di dialogo. Fornisci un nome, una descrizione facoltativa e un'istruzione SELECT. Il salvataggio registra la vista come tabella derivata governata nel dominio `ops` — catalogata, sottoposta a controllo degli accessi e interrogabile da ogni superficie insieme alle viste inizializzate. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Eliminazione.** L'icona del cestino compare solo per i report personalizzati. Le viste di gestione inizializzate non sono eliminabili da questa superficie. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Anteprima tabella (REQ-1392)

Espandi una riga qualsiasi nella pagina Tabelle. Il pulsante **Anteprima** apre una finestra modale larga il 90% con i dati governati in tempo reale della tabella. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Le tabelle basate su API con parametri di percorso obbligatori bloccano l'anteprima finché quei valori non vengono forniti. Un modulo in linea raccoglie ogni parametro obbligatorio prima che venga eseguita la prima query; i parametri di query facoltativi compaiono nello stesso modulo. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visualizzatore di dati governato (REQ-1391)

Lo stesso componente visualizzatore alimenta la finestra di anteprima e il visualizzatore di report. Il comportamento è identico nei due contesti.

**Paginazione lato server.** Ogni pagina è un proprio `SELECT *` governato con `LIMIT 101 OFFSET n`. Vengono mostrate 100 righe per pagina; la 101ª segnala se ne esistono altre. L'insieme completo dei dati non viene mai caricato nel browser. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtri e ordinamenti spinti verso l'origine.** Ogni intestazione di colonna ha un campo di filtro. I termini di filtro diventano predicati `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; i clic di ordinamento producono clausole `ORDER BY`. Entrambi vengono inviati al database — filtrare una tabella da un miliardo di righe analizza l'origine, non le 100 righe che hai davanti. [tool-verified: `nativeParams.ts:53-70`]

**Raggruppamento multilivello.** L'icona a strati su ogni intestazione di colonna inserisce quella colonna nel raggruppamento. Le colonne di raggruppamento guidano l'`ORDER BY`, così i membri di un gruppo finiscono nella stessa pagina della loro intestazione anche a cavallo delle pagine. Le colonne di chiave primaria vengono aggiunte in coda come criterio di spareggio stabile. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Le righe di intestazione di gruppo sono comprimibili; comprimere nasconde i membri senza emettere una nuova query. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Le scelte persistono.** Le configurazioni di filtro, ordinamento e raggruppamento vengono salvate in `localStorage` sotto `provisa.grid.table:<domain>.<table>` e ripristinate alla visita successiva. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Esportazione.** Scarica la pagina corrente come CSV o copiala negli appunti come testo separato da tabulazioni. L'esportazione copre solo la pagina visibile. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
