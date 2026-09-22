# Riferimento API Admin GraphQL

L'API GraphQL admin è il piano di configurazione di Provisa. È l'API che l'app web di amministrazione chiama per ogni operazione di gestione — creare origini, registrare tabelle, definire relazioni, configurare regole RLS, e tutto il resto che modella il modello.

**Punto di mount:** `POST /admin/graphql`

Questa non è la stessa API del data plane su `/data/graphql`. Il data plane serve le query degli utenti finali sui domini registrati ed è descritto dall'SDL su `/data/sdl`. L'API admin configura come appare quello schema e chi può vedere cosa.

---

## Come la UI parla con questa API

L'app web di amministrazione usa Apollo Client, puntato su `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Ogni richiesta porta un bearer token (recuperato fresco dal provider di autenticazione a ogni chiamata), un header `X-Org-Id` in multi-tenancy, e un header `X-Env` quando serve un ambiente branch. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

Lo schema è assemblato da due classi `@strawberry.type` — `Query` da `schema_query.py` e `Mutation` da `schema_mutation.py` — e avvolto in una `ModelCommitExtension` che registra ogni mutation contro il branch dell'ambiente corrente (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Autorizzazione

**Modalità dev:** quando non è configurata alcuna autenticazione e ogni richiesta arriva come principal anonimo, tutti i controlli di capability vengono saltati. Questo mantiene un'installazione locale funzionante senza configurazione di autenticazione. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Gate di capability:** i deployment di produzione applicano capability nominate. Il diritto specifico richiesto da ogni campo è annotato inline. Chiamare una mutation senza la capability richiesta solleva un `PermissionError`. Il ruolo di amministratore della piattaforma bypassa tutti i controlli di capability (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Gate di dominio:** diverse mutation controllano anche il dominio a cui appartiene l'oggetto. Un chiamante ambitoato a `sales` non può registrare una tabella in `finance`, accodare una regola RLS per essa, o creare una relazione la cui tabella origine vive in un dominio che non possiede (REQ-1530, REQ-1531). Le viste sono ulteriormente vincolate: ogni tabella letta dall'SQL della vista deve essere all'interno dei domini del chiamante, perché altrimenti l'SQL a mano libera darebbe a un membro accesso a dati fuori dal proprio ambito. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Ereditarietà dei ruoli:** le capability di un ruolo padre vengono ereditate dai ruoli figli (REQ-1677). `createRole` e `deleteRole` rifiutano i cicli e impediscono di eliminare un ruolo che ha eredi.

---

## Tipo di ritorno comune

La maggior parte delle mutation restituisce `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Quando una mutation fallisce, `success` è `false` e `message` porta la ragione in inglese. `code` è un identificatore stabile che la UI usa per rendere un messaggio localizzato.

---

## Query

### Origini

#### `sources → [SourceType!]!`

Tutte le origini dati registrate. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` è un riferimento `${secret:NAME}` nel vault dell'org — mai la credenziale letterale. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Una singola origine per ID. Restituisce `null` quando non trovata. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Schemi visibili in un'origine, filtrati per escludere quelli interni a Provisa. Usa prima l'introspezione nativa; ricade sul catalogo del motore quando il tipo di origine non ha un pool diretto. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabelle in uno schema di un'origine, con i loro commenti. Per le origini OpenAPI, restituisce operazioni GET la cui risposta è un array o un wrapper di paginazione. Per le origini GraphQL, restituisce i campi query che restituiscono una lista. Per gRPC, restituisce le RPC server-streaming. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nomi delle colonne per una tabella nel catalogo del motore. Per le origini govdata, usa un resolver separato. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nomi delle colonne con tipi di dato, commenti, tipi di filtro nativo, e flag di chiave primaria. Per le origini OpenAPI, deriva la forma dallo schema di risposta dell'operazione e dai parametri. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operazioni non-GET per un'origine OpenAPI (POST, PUT, PATCH, DELETE). Restituisce una lista vuota per origini non-OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Anteprima di cosa scoprirebbe un crawl del connettore file — file, tabelle e colonne — prima che un'origine venga creata. Le impostazioni solo-HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) vengono ignorate per radici locali, S3, FTP e SFTP. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Restituisce l'alias da usare quando si registra `tableName` in `domainId` da `sourceId`. Restituisce un semplice alias snake-case quando non esiste conflitto, o un alias con prefisso origine (`sqlite_b_orders`) quando il nome effettivo è già preso da un'origine diversa nello stesso dominio. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tabelle

#### `tables → [RegisteredTableType!]!`

Tutte le tabelle registrate, ciascuna con l'elenco completo delle colonne. La visibilità delle colonne nella risposta rispetta la capability `table_registration` del chiamante — `canDeployToDb` è vincolato al fatto che il chiamante possieda quel diritto. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Ogni `RegisteredTableType` espone sotto-campi calcolati:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la policy effettiva di refresh/serving come testo semplice, derivata lato server dalla stessa risoluzione del planner usata dal motore. Restituisce `null` durante l'avvio. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — il nome del campo che questa tabella ha nello schema data-plane compilato, cosicché il pannello Data Product possa costruire un esempio eseguibile senza replicare l'algoritmo di naming. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — questa tabella come dataset contratto di qualità dei dati, nella forma che il checker scansiona. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — il prodotto dati a cui appartiene questa tabella. Una tabella DQ-checker eredita il prodotto della tabella scansionata dal suo contratto. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Anteprima del riepilogo effettivo di refresh/serving per manopole di tabella *in bozza* (non salvate), cosicché il riepilogo in cima al modulo si aggiorni man mano che i campi cambiano senza persistere nulla. Stessa derivazione di `refreshPolicySummary` sopra. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Argomenti: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefatti che una ridenominazione di alias o rimozione di colonna in sospeso romperebbe. Consultivo — la UI admin lo mostra prima di salvare e l'amministratore decide. Deve essere chiamato *prima* di salvare, perché i dipendenti sono stati scritti contro il nome esposto che la colonna porta attualmente. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Relazioni

#### `relationships → [RelationshipType!]!`

Tutte le relazioni definite dall'utente (esclude le voci auto-generate `gql_auto__` e le voci sintetiche `meta:%` usate dall'ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Come `relationships`, ma include le voci sintetiche `meta:%`. Usata dall'ERD del grafo, che deve mostrare ogni arco inclusi i link impliciti `HAS_TABLE` tra le tabelle dati e il registro dei metadati. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Ogni `RelationshipType` espone:

- **`autoSuggested → Boolean`** — se la relazione è stata suggerita dall'analisi FK (l'`id` inizia con `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — il nome della relazione sui piani SQL e gRPC (il parametro `?include=`). Derivato lato server; i client non devono traslitterare l'alias GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Domini, ruoli e utenti

#### `domains → [DomainType!]!`

Tutti i domini nel database del tenant dell'org attiva. Il database del tenant è isolato a livello di schema, quindi l'elenco domini di un org-admin contiene solo le righe della propria org. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Ruoli visibili al chiamante. Un admin vede ogni ruolo; un non-admin vede solo ruoli senza `org_id` o ruoli appartenenti alla propria org. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Risolve ID di ruolo o ID utente in singoli utenti. Usato per espandere `DataProduct.ownerRole`, `Domain.steward`, e `Column.visibleTo` in un elenco leggibile dall'uomo. I riferimenti sconosciuti vengono restituiti così come sono, cosicché la UI mostri l'ID grezzo invece di nulla. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### Regole RLS

#### `rlsRules → [RLSRuleType!]!`

Tutte le regole di sicurezza a livello di riga. Il repository sottostante decifra `filterExpr` al confine. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Prodotti dati

#### `dataProducts → [DataProductType!]!`

Tutti i prodotti dati. Richiede la capability `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Tag

#### `tags → [TagType!]!`

Tutte le definizioni di tag, inclusi i valori di parametro permessi per ogni tag. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Tutte le assegnazioni di tag su origini, tabelle, colonne e relazioni. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Viste materializzate

#### `mvList → [MVType!]!`

Tutte le viste materializzate con il loro stato a runtime: abilitata/disabilitata, timestamp dell'ultimo refresh, conteggio righe, e ultimo errore. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Cache

#### `cacheStats → CacheStatsType`

Statistiche della cache. Restituisce `storeType: "redis"` con metriche operative complete quando Redis è configurato, `storeType: "memory"` per lo store fakeredis integrato, e `storeType: "noop"` quando non è configurata alcuna cache. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Conteggi di voci in cache per tabella. Vuoto quando non è configurato alcuno store di cache. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tabelle di cui Provisa mantiene una copia, su due livelli: `hot` (specchiata nello store di risposta per l'inlining dei JOIN) e `warm` (atterrata come copia Iceberg). Una tabella è al massimo in un livello (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identità dello store di materializzazione durevole: nome del motore, riferimento DSN dello store, conteggio MV, e se lo store è locale all'istanza (un file locale come DuckDB o SQLite, il che significa che ogni istanza dietro un load balancer mantiene la propria copia). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Salute del sistema

#### `systemHealth → SystemHealthType`

Stato della connessione al motore, conteggi del worker pool, stato del pool DB dei metadati, modalità cache, e liveness di ogni listener di protocollo (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Task pianificati

#### `scheduledTasks → [ScheduledTaskType!]!`

Trigger pianificati da config con stato a runtime. Ogni voce porta la propria espressione cron, `kind` (`webhook` o `sql`), se è attualmente abilitata, il timestamp dell'ultima esecuzione (sempre `null` in questa release — tracciato dallo scheduler), e il prossimo orario di esecuzione pianificato da APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Qualità dei dati

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analizza il testo grezzo del contratto nelle righe modificabili del pannello builder. Chiamata a ogni modifica; un fallimento di parsing torna come `error` invece che come errore GraphQL, perché testo a metà scrittura è normale mentre l'operatore sta digitando. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

I controlli che `checker` offre, ambitoati alle colonne di `dataset`. Il dataset è il target osservato del contratto, risolto nello stesso modo in cui lo scanner lo risolve — cosicché i controlli offerti corrispondano alle colonne che il checker vedrà davvero. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Il testo di un controllo dagli editor del pannello. Lato server perché il dialetto ha un'unica implementazione; un controllo creato dal builder e uno digitato a mano devono essere indistinguibili. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializza le righe di controllo modificate in testo di contratto. L'inverso di `dqContractParse`. Lato server per lo stesso motivo: il pannello non può emettere testo che il checker rifiuterebbe. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Anteprime origine

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Anteprima di una proiezione Cypher su un'origine Neo4j: fino a cinque righe e i tipi di colonna che la registrazione porterà. I fallimenti tornano come `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Anteprima di un SELECT SPARQL su un'origine SPARQL: fino a cinque righe, tutte le colonne come testo. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Controllo live contro l'API Kaggle. Restituisce `true` solo quando il token si autentica. Sostiene il passo di gate del token nel modulo dell'origine Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Ricerca nel catalogo pubblico completo dei dataset Kaggle. Vincolata dal token. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Calendari

#### `calendars → [CalendarType!]!`

Tutte le versioni registrate di calendario dei confini di snapshot. Alimenta il selettore di configurazione dello snapshot-schedule e conferma quali calendari una MV periodica può referenziare. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Metriche

#### `metrics → [MetricType!]!`

Tutte le definizioni di metriche governate. Le metriche derivate da fact portano `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Versione schema

#### `schemaVersion → String!`

Hash SHA-256 dello stato corrente dello schema (domini, ID tabella, ID relazione). Il client Apollo legge questo dall'header di risposta `X-Schema-Version` e ri-recupera tutte le query attive quando avanza. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### Helper IA

#### `generateTableDescription(tableId: String!) → String!`

Usa l'LLM configurato per generare una descrizione di una o due frasi per una tabella registrata. Salva prima la tabella; chiamare questo su una tabella non salvata restituisce un messaggio istruttivo. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa l'LLM configurato per generare una descrizione di una frase per una singola colonna. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Richieste di creazione

#### `creationRequests → [CreationRequestType!]!`

Richieste di creazione in sospeso, visibili ai chiamanti che possiedono la relativa capability di creazione. Usata quando un membro senza `create_relationship` o `create_view` invia una richiesta che un titolare di diritti deve approvare. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutation

### Origini

#### `createSource(input: SourceInput!) → MutationResult`

Registra una nuova origine dati. Valida la connessione prima di persistere — un'origine rifiutata non lascia alcuna voce nel vault. Memorizza le credenziali nel vault dell'org e registra il riferimento; il testo in chiaro non atterra mai nel database. (REQ-012, REQ-013) Richiede la capability `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Aggiorna i dettagli di connessione, la descrizione e la config di un'origine esistente. Smonta e ricollega l'endpoint pgwire per le origini file/SharePoint, cosicché un cambio di percorso abbia effetto immediato. Richiede `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Rimuove un'origine e la sua voce nel vault. Elimina il catalogo del motore e ricostruisce gli schemi. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Rinomina un ID origine. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Abilita o disabilita il caching dei risultati delle query per un'origine, e imposta il TTL in secondi. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Forza (o rilascia) la federazione materializzata per tutte le tabelle di un'origine. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Contrassegna un'origine come protetta dal carico (solo-refresh-pianificato). Richiede almeno un gate — finestra off-peak, cadenza TTL della cache, o un segnale di cambiamento a probing — altrimenti la chiamata fallisce. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Imposta la convenzione di naming GraphQL per singola origine. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Imposta quali domini possono usare un'origine (lista vuota = senza restrizioni). Richiede `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Scarica e decomprime un dataset Kaggle su disco locale. Restituisce il percorso della directory in stage; il chiamante crea poi un'origine di tipo `files` che punta ad essa. I bundle contenenti SQLite vengono rifiutati per intero. Richiede `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Riscarica il dataset di un'origine derivata da Kaggle sul posto. Salta il download se Kaggle non ha nulla di più recente di quanto sia già su disco. Richiede `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Esegue `ANALYZE` su tutte le tabelle registrate per un'origine. Migliora le decisioni di ordine di join e broadcast per le query federate. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tabelle

#### `registerTable(input: TableInput!) → MutationResult`

Registra una nuova tabella (o vista) in un dominio. Richiede la capability `table_registration` e l'appartenenza al dominio di destinazione. Un chiamante privo di `create_relationship` che invia una vista viene accodato come richiesta di creazione per l'approvazione di un titolare di diritti. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Aggiorna l'alias, la descrizione, i metadati delle colonne, le impostazioni MV e la config di live-delivery di una tabella esistente. (REQ-016, REQ-020) Richiede `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Elimina una tabella registrata. Cerca il dominio della tabella per il gate di dominio prima di eliminare. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Sovrascrive il TTL della cache per una tabella. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Sovrascrive la federazione materializzata per una tabella. `null` = eredita il default dell'origine. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Sovrascrive la protezione dal carico per una tabella. `null` per `loadProtected` eredita il default dell'origine. Valida la combinazione effettiva di gate (tabella → origine). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Imposta la convenzione di naming GraphQL per singola tabella. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promuove una vista virtuale Provisa a una vista di database reale sulla sua origine nativa sottostante. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Ricalcola le righe atterrate di una tabella on demand, bypassando il normale gate di cambiamento. `reason` è un'annotazione di audit obbligatoria. Rifiutata per tabelle federate live (nessuna riga atterrata). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Forza il prossimo accesso di una tabella connettore-file SQLite a ri-sincronizzare da disco. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Zucchero sintattico per registrare un'entità dimensione/hub. Si riduce a una MV (bitemporale, quando storicizzata) e chiama `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Zucchero sintattico per registrare un fact a schema a stella. Si riduce a una MV aggregata, crea relazioni con le dimensioni, e registra automaticamente le misure fact come metriche governate. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Relazioni

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Crea o aggiorna una relazione. Il gate controlla il dominio della tabella origine (non quella di destinazione). Un arco cross-dominio viene memorizzato con `needsReview: true`. Un chiamante privo di `create_relationship` viene accodato come richiesta di creazione. Gli archi junction (molti-a-molti) richiedono lunghezze corrispondenti delle liste di chiavi. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Elimina una relazione per ID e ricostruisce gli schemi. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Domini

#### `createDomain(input: DomainInput!) → MutationResult`

Crea un dominio. Le parole di segmento riservate (`tables`, `relationships`, e altri segmenti di percorso URI) vengono rifiutate, così come il letterale wildcard `*`. Richiede la capability `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Elimina un dominio. Richiede `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Imposta la convenzione di naming GraphQL globale e ricostruisce gli schemi per tutti i ruoli. Vengono accettati solo nomi di convenzione riconosciuti. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Ruoli

#### `createRole(input: RoleInput!) → MutationResult`

Crea o sostituisce un ruolo con capability, accesso ai domini, rate limit opzionali, e un ruolo padre opzionale. Valida che il padre esista e che la catena dei padri sia priva di cicli. Richiede `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Elimina un ruolo. Fallisce se altri ruoli ereditano da esso — ri-assegnali prima a un nuovo padre. Richiede `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### Regole RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Crea o aggiorna una regola di sicurezza a livello di riga. L'espressione di filtro viene validata al momento del salvataggio contro le colonne della tabella o del dominio di destinazione, cosicché una regola che l'admin non può interrogare venga rifiutata con la ragione invece di fallire silenziosamente al momento della query. Richiede `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

I target sono mutuamente esclusivi: imposta `tableId` per una regola a livello di tabella, `domainId` per una regola a livello di dominio, o `actionName` per una funzione/webhook tracciato. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Elimina una regola RLS. Richiede `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Prodotti dati

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Crea o sostituisce un prodotto dati. Richiede la capability `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Elimina un prodotto dati. Richiede `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Tag

#### `upsertTag(input: TagInput!) → MutationResult`

Crea o aggiorna una definizione di tag. I tag di sistema e i tag derivati non possono essere ridefiniti. `appliesTo` deve essere un sottoinsieme non vuoto di `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Elimina un tag. Rifiuta i tag di sistema e derivati. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Assegna un tag a un'origine, tabella, colonna, relazione o comando. Applica le policy di campo del tag (`reason_policy`, `expires_policy`) e — per i tag parametrizzati — valida il valore del parametro contro la lista permessa del tag. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Rimuove un'assegnazione di tag. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Aggiunge o ridescrive un valore di parametro permesso per un tag parametrizzato. La lista dei valori permessi è chiusa: ogni assegnazione deve nominare un valore da essa. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Rimuove un valore permesso. Rifiutato mentre qualche assegnazione lo porta ancora, perché quelle assegnazioni nominerebbero un tipo che la lista non ammette più. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Metriche

#### `upsertMetric(input: MetricInput!) → MutationResult`

Crea o sostituisce una definizione di metrica governata. L'espressione deve essere analizzabile da sqlglot e contenere almeno una funzione di aggregazione. Rigenera tutte le viste composte da metriche che referenziano questa metrica. Richiede `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Elimina una metrica governata. Ricostruisce gli schemi. Richiede `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Calendari

#### `createCalendar(input: CalendarInput!) → MutationResult`

Crea o sostituisce un calendario versionato dei confini di snapshot. Validato costruendo il `Calendar` in memoria prima di persistere — fallisce su un sistema base sconosciuto, un fuso orario errato, o un'ancora fiscale errata. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Elimina un calendario (tutte le versioni). Rifiutato quando qualche vista materializzata lo referenzia. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Viste materializzate

#### `refreshMv(mvId: String!) → MutationResult`

Attiva un refresh manuale di una vista materializzata. Coordina tra la flotta quando la modalità di consistenza MV è `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Abilita o disabilita una vista materializzata. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Cache

#### `purgeCache → MutationResult`

Svuota tutti i risultati di query in cache. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Svuota i risultati in cache per una tabella. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Task pianificati

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Crea un trigger pianificato — una chiamata webhook o un'istruzione SQL — e lo registra live in APScheduler. `kind` è `"webhook"` o `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Rimuove un trigger pianificato dalla config e dallo scheduler live. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Abilita o disabilita un task pianificato nel file di config. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Qualità dei dati

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Esegue un contratto contro la tabella live e restituisce gli esiti senza atterrare nulla. Una mutation piuttosto che una query perché costa una scansione reale. Ciò che dimostra è se l'identificatore del dataset si risolve nella tabella governata che l'operatore intende. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Attiva immediatamente il job di polling di una tabella checker. Atterra le righe nel modo normale, cosicché i risultati persistano e la cronologia DQ mostri la nuova scansione. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Manutenzione dello schema

#### `rebuildSchemas → MutationResult`

Ricostruisce lo schema in memoria dallo stato del database. Utile dopo modifiche esterne al database. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Richieste di creazione

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Un titolare di diritti esegue una richiesta di creazione accodata — relazione, vista o webhook. Richiede la capability su cui la richiesta è in attesa. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rifiuta una richiesta accodata con una ragione azionabile. `reason` è obbligatorio. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Compilazione query

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila una query GraphQL data-plane contro lo schema di un ruolo e restituisce la decisione di routing completa: SQL semantico, SQL motore, SQL diretto, route, metadati di enforcement (filtri RLS applicati, colonne escluse, mascheramento applicato), e Cypher compilato. Restituisce un risultato per ogni campo root nella query. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Campi di `CompileQueryInput`:

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `query` | `String!` | Query GraphQL data-plane da compilare |
| `role` | `String!` | Ruolo contro il cui schema compilare |
| `variables` | `JSON` | Binding delle variabili |
| `flatSql` | `Boolean` | Restituisce un'unica stringa SQL appiattita invece di una coppia semantica/motore |
| `flatCypher` | `Boolean` | Appiattisce l'output Cypher |
| `nodeOnlyCypher` | `Boolean` | Emette Cypher solo-nodi (nessun pattern di arco) |

---

## Tipi di input chiave

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| Campo | Tipo | Note |
|-------|------|-------|
| `id` | `String!` | Identificatore dell'origine |
| `type` | `String!` | Tipo di connettore (es. `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Testo in chiaro o riferimento `${secret:NAME}` |
| `path` | `String` | Percorso filesystem per origini file/CSV |
| `federationHintsJson` | `String` | Oggetto JSON per gli extra warehouse (warehouse/role di Snowflake, http_path di Databricks) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Solo-refresh-pianificato (REQ-1141) |
| `offPeakWindow` | `String` | Finestra di manutenzione `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuso orario IANA |
| `cdc` | `SourceCdcConfigInput` | Config di trasporto CDC Kafka (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

L'input principale di registrazione tabella. Campi chiave oltre alle basi:

| Campo | Note |
|-------|------|
| `materialize` | Atterra una copia nello store di materializzazione |
| `mvRefreshInterval` | Secondi tra i refresh |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Manutenzione incrementale (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` per tabelle bitemporali (REQ-1162) |
| `mvCalendar` | Nome del calendario snapshot (REQ-962) |
| `mvGrain` | Grana dello snapshot: `daily`, `weekly`, `monthly`, `annual`, o custom `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL per una vista derivata |
| `viewMetrics` | Spec dichiarativa di vista composta da metriche — mutuamente esclusiva con `viewSql` (REQ-1318) |
| `dqContract` | Testo di contratto di qualità dei dati YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher per una tabella Neo4j (REQ-1670) |
| `live` | Config di live-delivery per push SSE/Kafka (REQ-565, REQ-813) |
| `discover` | Inferisce le colonne dall'origine live al momento della registrazione (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| Campo | Note |
|-------|------|
| `id` | Identificatore della relazione |
| `sourceTableId` | Nome tabella virtuale (alias se impostato, altrimenti nome tabella) |
| `targetTableId` | Nome tabella virtuale; vuoto per relazioni calcolate |
| `sourceColumn` | Colonna di join sul lato origine |
| `targetColumn` | Colonna di join sul lato destinazione |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Etichetta arco Cypher (es. `WORKS_FOR`) |
| `graphqlAlias` | Nome campo GraphQL sul tipo origine |
| `viaTable` | Nome tabella junction per archi molti-a-molti (REQ-1586) |
| `recordCandidate` | Scrive anche una riga relationship_candidates `accepted` |

---

## Esempio: registrare una tabella

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Esempio: creare una regola RLS

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
