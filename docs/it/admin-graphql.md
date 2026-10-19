# Riferimento API Admin GraphQL {: #admin-graphql-api-reference }

L'API GraphQL admin è il piano di configurazione di Provisa. È l'API che l'app web di amministrazione chiama per ogni operazione di gestione — creare origini, registrare tabelle, definire relazioni, configurare regole RLS, e tutto il resto che modella il modello.

**Punto di mount:** `POST /admin/graphql`

Questa non è la stessa API del data plane su `/data/graphql`. Il data plane serve le query degli utenti finali sui domini registrati ed è descritto dall'SDL su `/data/sdl`. L'API admin configura come appare quello schema e chi può vedere cosa.

---

## Come la UI parla con questa API {: #how-the-ui-talks-to-this-api }

L'app web di amministrazione usa Apollo Client, puntato su `${API_BASE}/admin/graphql`.

Ogni richiesta porta un bearer token (recuperato fresco dal provider di autenticazione a ogni chiamata), un header `X-Org-Id` in multi-tenancy, e un header `X-Env` quando serve un ambiente branch.

Lo schema è assemblato da due classi `@strawberry.type` — `Query` da `schema_query.py` e `Mutation` da `schema_mutation.py` — e avvolto in una `ModelCommitExtension` che registra ogni mutation contro il branch dell'ambiente corrente.

---

## Autorizzazione {: #authorization }

**Modalità dev:** quando non è configurata alcuna autenticazione e ogni richiesta arriva come principal anonimo, tutti i controlli di capability vengono saltati. Questo mantiene un'installazione locale funzionante senza configurazione di autenticazione.

**Gate di capability:** i deployment di produzione applicano capability nominate. Il diritto specifico richiesto da ogni campo è annotato inline. Chiamare una mutation senza la capability richiesta solleva un `PermissionError`. Il ruolo di amministratore della piattaforma bypassa tutti i controlli di capability.

**Gate di dominio:** diverse mutation controllano anche il dominio a cui appartiene l'oggetto. Un chiamante ambitoato a `sales` non può registrare una tabella in `finance`, accodare una regola RLS per essa, o creare una relazione la cui tabella origine vive in un dominio che non possiede. Le viste sono ulteriormente vincolate: ogni tabella letta dall'SQL della vista deve essere all'interno dei domini del chiamante, perché altrimenti l'SQL a mano libera darebbe a un membro accesso a dati fuori dal proprio ambito.

**Ereditarietà dei ruoli:** le capability di un ruolo padre vengono ereditate dai ruoli figli. `createRole` e `deleteRole` rifiutano i cicli e impediscono di eliminare un ruolo che ha eredi.

---

## Tipo di ritorno comune {: #common-return-type }

La maggior parte delle mutation restituisce `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Quando una mutation fallisce, `success` è `false` e `message` porta la ragione in inglese. `code` è un identificatore stabile che la UI usa per rendere un messaggio localizzato.

---

## Query {: #queries }

### Origini {: #sources }

#### `sources → [SourceType!]!`

Tutte le origini dati registrate.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` è un riferimento `${secret:NAME}` nel vault dell'org — mai la credenziale letterale.

#### `source(id: String!) → SourceType`

Una singola origine per ID. Restituisce `null` quando non trovata.

#### `availableSchemas(sourceId: String!) → [String!]!`

Schemi visibili in un'origine, filtrati per escludere quelli interni a Provisa. Usa prima l'introspezione nativa; ricade sul catalogo del motore quando il tipo di origine non ha un pool diretto.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabelle in uno schema di un'origine, con i loro commenti. Per le origini OpenAPI, restituisce operazioni GET la cui risposta è un array o un wrapper di paginazione. Per le origini GraphQL, restituisce i campi query che restituiscono una lista. Per gRPC, restituisce le RPC server-streaming.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nomi delle colonne per una tabella nel catalogo del motore. Per le origini govdata, usa un resolver separato.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nomi delle colonne con tipi di dato, commenti, tipi di filtro nativo, e flag di chiave primaria. Per le origini OpenAPI, deriva la forma dallo schema di risposta dell'operazione e dai parametri.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operazioni non-GET per un'origine OpenAPI (POST, PUT, PATCH, DELETE). Restituisce una lista vuota per origini non-OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Anteprima di cosa scoprirebbe un crawl del connettore file — file, tabelle e colonne — prima che un'origine venga creata. Le impostazioni solo-HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) vengono ignorate per radici locali, S3, FTP e SFTP.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Restituisce l'alias da usare quando si registra `tableName` in `domainId` da `sourceId`. Restituisce un semplice alias snake-case quando non esiste conflitto, o un alias con prefisso origine (`sqlite_b_orders`) quando il nome effettivo è già preso da un'origine diversa nello stesso dominio.

---

### Tabelle {: #tables }

#### `tables → [RegisteredTableType!]!`

Tutte le tabelle registrate, ciascuna con l'elenco completo delle colonne. La visibilità delle colonne nella risposta rispetta la capability `table_registration` del chiamante — `canDeployToDb` è vincolato al fatto che il chiamante possieda quel diritto.

Ogni `RegisteredTableType` espone sotto-campi calcolati:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la policy effettiva di refresh/serving come testo semplice, derivata lato server dalla stessa risoluzione del planner usata dal motore. Restituisce `null` durante l'avvio.
- **`graphqlFieldName → String`** — il nome del campo che questa tabella ha nello schema data-plane compilato, cosicché il pannello Data Product possa costruire un esempio eseguibile senza replicare l'algoritmo di naming.
- **`dqDataset → String`** — questa tabella come dataset contratto di qualità dei dati, nella forma che il checker scansiona.
- **`productId → String`** — il prodotto dati a cui appartiene questa tabella. Una tabella DQ-checker eredita il prodotto della tabella scansionata dal suo contratto.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Anteprima del riepilogo effettivo di refresh/serving per manopole di tabella *in bozza* (non salvate), cosicché il riepilogo in cima al modulo si aggiorni man mano che i campi cambiano senza persistere nulla. Stessa derivazione di `refreshPolicySummary` sopra.

Argomenti: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefatti che una ridenominazione di alias o rimozione di colonna in sospeso romperebbe. Consultivo — la UI admin lo mostra prima di salvare e l'amministratore decide. Deve essere chiamato *prima* di salvare, perché i dipendenti sono stati scritti contro il nome esposto che la colonna porta attualmente.

---

### Relazioni {: #relationships }

#### `relationships → [RelationshipType!]!`

Tutte le relazioni definite dall'utente (esclude le voci auto-generate `gql_auto__` e le voci sintetiche `meta:%` usate dall'ERD).

#### `allRelationships → [RelationshipType!]!`

Come `relationships`, ma include le voci sintetiche `meta:%`. Usata dall'ERD del grafo, che deve mostrare ogni arco inclusi i link impliciti `HAS_TABLE` tra le tabelle dati e il registro dei metadati.

Ogni `RelationshipType` espone:

- **`autoSuggested → Boolean`** — se la relazione è stata suggerita dall'analisi FK (l'`id` inizia con `fk__`).
- **`physicalName → String`** — il nome della relazione sui piani SQL e gRPC (il parametro `?include=`). Derivato lato server; i client non devono traslitterare l'alias GraphQL.

---

### Domini, ruoli e utenti {: #domains-roles-users }

#### `domains → [DomainType!]!`

Tutti i domini nel database del tenant dell'org attiva. Il database del tenant è isolato a livello di schema, quindi l'elenco domini di un org-admin contiene solo le righe della propria org.

#### `roles → [RoleType!]!`

Ruoli visibili al chiamante. Un admin vede ogni ruolo; un non-admin vede solo ruoli senza `org_id` o ruoli appartenenti alla propria org.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Risolve ID di ruolo o ID utente in singoli utenti. Usato per espandere `DataProduct.ownerRole`, `Domain.steward`, e `Column.visibleTo` in un elenco leggibile dall'uomo. I riferimenti sconosciuti vengono restituiti così come sono, cosicché la UI mostri l'ID grezzo invece di nulla.

---

### Regole RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Tutte le regole di sicurezza a livello di riga. Il repository sottostante decifra `filterExpr` al confine.

---

### Prodotti dati {: #data-products }

#### `dataProducts → [DataProductType!]!`

Tutti i prodotti dati. Richiede la capability `data_product_read`.

---

### Tag {: #tags }

#### `tags → [TagType!]!`

Tutte le definizioni di tag, inclusi i valori di parametro permessi per ogni tag.

#### `tagAssignments → [TagAssignmentType!]!`

Tutte le assegnazioni di tag su origini, tabelle, colonne e relazioni.

---

### Viste materializzate {: #materialized-views }

#### `mvList → [MVType!]!`

Tutte le viste materializzate con il loro stato a runtime: abilitata/disabilitata, timestamp dell'ultimo refresh, conteggio righe, e ultimo errore.

---

### Cache

#### `cacheStats → CacheStatsType`

Statistiche della cache. Restituisce `storeType: "redis"` con metriche operative complete quando Redis è configurato, `storeType: "memory"` per lo store fakeredis integrato, e `storeType: "noop"` quando non è configurata alcuna cache.

#### `cacheTableStats → [CacheTableStatType!]!`

Conteggi di voci in cache per tabella. Vuoto quando non è configurato alcuno store di cache.

#### `hotTables → [HotTableStatType!]!`

Tabelle di cui Provisa mantiene una copia, su due livelli: `hot` (specchiata nello store di risposta per l'inlining dei JOIN) e `warm` (atterrata come copia Iceberg). Una tabella è al massimo in un livello.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identità dello store di materializzazione durevole: nome del motore, riferimento DSN dello store, conteggio MV, e se lo store è locale all'istanza (un file locale come DuckDB o SQLite, il che significa che ogni istanza dietro un load balancer mantiene la propria copia).

---

### Salute del sistema {: #system-health }

#### `systemHealth → SystemHealthType`

Stato della connessione al motore, conteggi del worker pool, stato del pool DB dei metadati, modalità cache, e liveness di ogni listener di protocollo (pgwire, gRPC, Arrow Flight, Bolt).

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

### Task pianificati {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Trigger pianificati da config con stato a runtime. Ogni voce porta la propria espressione cron, `kind` (`webhook` o `sql`), se è attualmente abilitata, il timestamp dell'ultima esecuzione (sempre `null` in questa release — tracciato dallo scheduler), e il prossimo orario di esecuzione pianificato da APScheduler.

---

### Qualità dei dati {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analizza il testo grezzo del contratto nelle righe modificabili del pannello builder. Chiamata a ogni modifica; un fallimento di parsing torna come `error` invece che come errore GraphQL, perché testo a metà scrittura è normale mentre l'operatore sta digitando.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

I controlli che `checker` offre, ambitoati alle colonne di `dataset`. Il dataset è il target osservato del contratto, risolto nello stesso modo in cui lo scanner lo risolve — cosicché i controlli offerti corrispondano alle colonne che il checker vedrà davvero.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Il testo di un controllo dagli editor del pannello. Lato server perché il dialetto ha un'unica implementazione; un controllo creato dal builder e uno digitato a mano devono essere indistinguibili.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializza le righe di controllo modificate in testo di contratto. L'inverso di `dqContractParse`. Lato server per lo stesso motivo: il pannello non può emettere testo che il checker rifiuterebbe.

---

### Anteprime origine {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Anteprima di una proiezione Cypher su un'origine Neo4j: fino a cinque righe e i tipi di colonna che la registrazione porterà. I fallimenti tornano come `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Anteprima di un SELECT SPARQL su un'origine SPARQL: fino a cinque righe, tutte le colonne come testo.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Controllo live contro l'API Kaggle. Restituisce `true` solo quando il token si autentica. Sostiene il passo di gate del token nel modulo dell'origine Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Ricerca nel catalogo pubblico completo dei dataset Kaggle. Vincolata dal token.

---

### Calendari {: #calendars }

#### `calendars → [CalendarType!]!`

Tutte le versioni registrate di calendario dei confini di snapshot. Alimenta il selettore di configurazione dello snapshot-schedule e conferma quali calendari una MV periodica può referenziare.

---

### Metriche {: #metrics }

#### `metrics → [MetricType!]!`

Tutte le definizioni di metriche governate. Le metriche derivate da fact portano `fromFact`.

---

### Versione schema {: #schema-version }

#### `schemaVersion → String!`

Hash SHA-256 dello stato corrente dello schema (domini, ID tabella, ID relazione). Il client Apollo legge questo dall'header di risposta `X-Schema-Version` e ri-recupera tutte le query attive quando avanza.

---

### Helper IA {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Usa l'LLM configurato per generare una descrizione di una o due frasi per una tabella registrata. Salva prima la tabella; chiamare questo su una tabella non salvata restituisce un messaggio istruttivo.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa l'LLM configurato per generare una descrizione di una frase per una singola colonna.

---

### Richieste di creazione {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Richieste di creazione in sospeso, visibili ai chiamanti che hanno la capacità di creazione pertinente. Usato quando un membro senza `create_relationship` o `create_view` invia una richiesta che un titolare del diritto deve approvare. Una richiesta di relazione è elencata per gli utenti il cui diritto `create_relationship` raggiunge il dominio della sua tabella di origine o di destinazione, e per l'utente che l'ha fatta.

---

## Mutation {: #mutations }

### Origini {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

Registra una nuova sorgente di dati. Un id già esistente viene rifiutato (`schema.source_id_taken`) dopo i controlli della definizione e prima che qualcosa venga preparato o scritto; una sorgente si modifica con `updateSource`. Convalida la connessione prima di persistere — una sorgente rifiutata non lascia alcuna voce nel vault. Memorizza le credenziali nel vault dell'organizzazione e registra il riferimento; il testo in chiaro non arriva mai al database. Richiede la capacità `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Aggiorna i dettagli di connessione, la descrizione e la configurazione di una sorgente esistente, poi la riprovvisiona sul motore associato, ricostruisce gli schemi e riconcilia le sue tabelle atterrate, così che una sorgente salvata con un motore sia servita da quello a cui si è passati in seguito. Smonta e ricollega l'endpoint pgwire delle sorgenti file/SharePoint perché un cambio di percorso abbia effetto subito. Richiede `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Rimuove un'origine e la sua voce nel vault. Elimina il catalogo del motore e ricostruisce gli schemi.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Rinomina l'id di una sorgente. Un nuovo id già posseduto da un'altra sorgente viene rifiutato (`schema.source_id_taken`) e non viene scritto nulla; rinominare una sorgente con il suo stesso id non cambia nulla.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Abilita o disabilita il caching dei risultati delle query per un'origine, e imposta il TTL in secondi.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Stabilisce quando le tabelle di una sorgente vengono servite dalle loro repliche: `null` Default, `-1` Never, `N` maggiore di 0 non appena una tabella supera N istruzioni governate per intervallo, `0` Always. Una tabella senza un valore proprio lo eredita. Rifiutato quando abbinerebbe `load_protected` a `-1`, o lascerebbe senza `cache_ttl` una tabella `ttl` che replica. Richiede `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Contrassegna un'origine come protetta dal carico (solo-refresh-pianificato). Richiede almeno un gate — finestra off-peak, cadenza TTL della cache, o un segnale di cambiamento a probing — altrimenti la chiamata fallisce.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Imposta la convenzione di naming GraphQL per singola origine.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Imposta quali domini possono usare un'origine (lista vuota = senza restrizioni). Richiede `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Scarica e decomprime un dataset Kaggle su disco locale. Restituisce il percorso della directory in stage; il chiamante crea poi un'origine di tipo `files` che punta ad essa. I bundle contenenti SQLite vengono rifiutati per intero. Richiede `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Riscarica il dataset di un'origine derivata da Kaggle sul posto. Salta il download se Kaggle non ha nulla di più recente di quanto sia già su disco. Richiede `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Esegue `ANALYZE` su tutte le tabelle registrate per un'origine. Migliora le decisioni di ordine di join e broadcast per le query federate.

---

### Tabelle {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

Registra una nuova tabella (o vista) in un dominio. Richiede la capability `table_registration` e l'appartenenza al dominio di destinazione. Un chiamante privo di `create_relationship` che invia una vista viene accodato come richiesta di creazione per l'approvazione di un titolare di diritti.

#### `updateTable(input: TableInput!) → MutationResult`

Aggiorna l'alias, la descrizione, i metadati delle colonne, le impostazioni MV e la config di live-delivery di una tabella esistente. Richiede `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Elimina una tabella registrata. Cerca il dominio della tabella per il gate di dominio prima di eliminare.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Sovrascrive il TTL della cache per una tabella.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Stabilisce quando una tabella viene servita dalla sua replica. `null` = ereditare il valore della sorgente. Stessi valori e rifiuti di `updateSourceReplicate`. Richiede `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Sovrascrive la protezione dal carico per una tabella. `null` per `loadProtected` eredita il default dell'origine. Valida la combinazione effettiva di gate (tabella → origine).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Imposta la convenzione di naming GraphQL per singola tabella.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promuove una vista virtuale Provisa a una vista di database reale sulla sua origine nativa sottostante.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Ricalcola le righe atterrate di una tabella on demand, bypassando il normale gate di cambiamento. `reason` è un'annotazione di audit obbligatoria. Rifiutata per tabelle federate live (nessuna riga atterrata).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Forza il prossimo accesso di una tabella connettore-file SQLite a ri-sincronizzare da disco.

#### `registerEntity(input: EntityInput!) → MutationResult`

Zucchero sintattico per registrare un'entità dimensione/hub. Si riduce a una MV (bitemporale, quando storicizzata) e chiama `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Zucchero sintattico per registrare un fact a schema a stella. Si riduce a una MV aggregata, crea relazioni con le dimensioni, e registra automaticamente le misure fact come metriche governate.

---

### Relazioni {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Crea una relazione. Il controllo verifica il dominio della tabella di origine (non quello della destinazione). Un arco tra domini viene memorizzato con `needsReview: true`. Un chiamante senza `create_relationship` viene messo in coda come richiesta di creazione. Gli archi di giunzione (molti-a-molti) richiedono liste di chiavi della stessa lunghezza. Rifiutato per nome, prima di scrivere qualsiasi cosa: un id già esistente (`schema.relationship_id_taken`); un nome di tabella che non si risolve in nessuna tabella registrata o in più di una (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); un alias Cypher o un alias GraphQL già posseduto da un'altra relazione della stessa tabella di origine (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Una tabella è indicata con il suo nome virtuale: il suo alias quando ne ha uno.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Modifica una relazione esistente, con gli stessi controlli di `createRelationship`. Un id che nessuna relazione possiede viene rifiutato (`schema.relationship_not_found`). Una modifica richiede il diritto stesso; non viene mai messa in coda come richiesta.

#### `deleteRelationship(id: String!) → MutationResult`

Elimina una relazione per ID e ricostruisce gli schemi.

---

### Domini {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Crea un dominio. Un id già esistente viene rifiutato (`schema.domain_id_taken`). Le parole di segmento riservate (`tables`, `relationships` e altri segmenti di percorso URI) vengono rifiutate, così come il letterale jolly `*`. Richiede la capacità `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Elimina un dominio. Richiede `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Imposta la convenzione di naming GraphQL globale e ricostruisce gli schemi per tutti i ruoli. Vengono accettati solo nomi di convenzione riconosciuti.

---

### Ruoli {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

Crea un ruolo con capacità, accesso ai domini, limiti di frequenza facoltativi e un ruolo padre facoltativo. Verifica che il padre esista e che la catena dei padri sia priva di cicli. Dopo i controlli della definizione, un id già esistente viene rifiutato (`schema.role_id_taken`). Richiede `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Modifica un ruolo esistente, con gli stessi controlli di `createRole`. Un id che nessun ruolo possiede viene rifiutato (`schema.role_not_found`). Ridefinire un ruolo è un atto in ogni dominio che raggiunge.

#### `deleteRole(id: String!) → MutationResult`

Elimina un ruolo. Fallisce se altri ruoli ereditano da esso — ri-assegnali prima a un nuovo padre. Richiede `user_management`.

---

### Regole RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Crea o aggiorna una regola di sicurezza a livello di riga. L'espressione di filtro viene validata al momento del salvataggio contro le colonne della tabella o del dominio di destinazione, cosicché una regola che l'admin non può interrogare venga rifiutata con la ragione invece di fallire silenziosamente al momento della query. Richiede `masking_config`.

I target sono mutuamente esclusivi: imposta `tableId` per una regola a livello di tabella, `domainId` per una regola a livello di dominio, o `actionName` per una funzione/webhook tracciato.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Elimina una regola RLS. Richiede `masking_config`.

---

### Prodotti dati {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Crea un prodotto dati. Un id già esistente viene rifiutato (`schema.data_product_id_taken`). Richiede la capacità `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Modifica un prodotto dati esistente. Un id che nessun prodotto possiede viene rifiutato (`schema.data_product_not_found`). Richiede `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Elimina un prodotto dati. Richiede `data_product_rw`.

---

### Tag {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

Crea una definizione di tag. Un id già esistente viene rifiutato (`schema.tag_id_taken`). I tag di sistema e i tag derivati non possono essere ridefiniti. `appliesTo` deve essere un sottoinsieme non vuoto di `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Modifica una definizione di tag esistente, con gli stessi controlli di `createTag`. Un id che nessun tag possiede viene rifiutato (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Elimina un tag. Rifiuta i tag di sistema e derivati.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Assegna un tag a un'origine, tabella, colonna, relazione o comando. Applica le policy di campo del tag (`reason_policy`, `expires_policy`) e — per i tag parametrizzati — valida il valore del parametro contro la lista permessa del tag.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Rimuove un'assegnazione di tag.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Aggiunge o ridescrive un valore di parametro permesso per un tag parametrizzato. La lista dei valori permessi è chiusa: ogni assegnazione deve nominare un valore da essa.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Rimuove un valore permesso. Rifiutato mentre qualche assegnazione lo porta ancora, perché quelle assegnazioni nominerebbero un tipo che la lista non ammette più.

---

### Metriche {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Crea una definizione di metrica governata. Un nome già esistente viene rifiutato (`schema.metric_name_taken`). L'espressione deve essere analizzabile da sqlglot e contenere almeno una funzione di aggregazione. Rigenera tutte le viste composte da metriche che fanno riferimento a questa metrica. Richiede `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Sostituisce la definizione di una metrica esistente, con gli stessi controlli di `createMetric`. Un nome che nessuna metrica possiede viene rifiutato (`schema.metric_not_found`). Salvarla richiede i domini della nuova espressione e della definizione che sostituisce.

#### `deleteMetric(name: String!) → MutationResult`

Elimina una metrica governata. Ricostruisce gli schemi. Richiede `table_registration`.

---

### Calendari {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Crea o sostituisce un calendario versionato dei confini di snapshot. Validato costruendo il `Calendar` in memoria prima di persistere — fallisce su un sistema base sconosciuto, un fuso orario errato, o un'ancora fiscale errata.

#### `deleteCalendar(name: String!) → MutationResult`

Elimina un calendario (tutte le versioni). Rifiutato quando qualche vista materializzata lo referenzia.

---

### Viste materializzate {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Attiva un refresh manuale di una vista materializzata. Coordina tra la flotta quando la modalità di consistenza MV è `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Abilita o disabilita una vista materializzata.

---

### Cache

#### `purgeCache → MutationResult`

Svuota tutti i risultati di query in cache.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Svuota i risultati in cache per una tabella.

---

### Task pianificati {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Crea un trigger pianificato — una chiamata webhook o un'istruzione SQL — e lo registra live in APScheduler. `kind` è `"webhook"` o `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Rimuove un trigger pianificato dalla config e dallo scheduler live.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Abilita o disabilita un task pianificato nel file di config.

---

### Qualità dei dati {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Esegue un contratto contro la tabella live e restituisce gli esiti senza atterrare nulla. Una mutation piuttosto che una query perché costa una scansione reale. Ciò che dimostra è se l'identificatore del dataset si risolve nella tabella governata che l'operatore intende.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Attiva immediatamente il job di polling di una tabella checker. Atterra le righe nel modo normale, cosicché i risultati persistano e la cronologia DQ mostri la nuova scansione.

---

### Manutenzione dello schema {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Ricostruisce lo schema in memoria dallo stato del database. Utile dopo modifiche esterne al database.

---

### Richieste di creazione {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Approva una richiesta di creazione: la stessa regola, gli stessi rifiuti e le stesse voci del registro di amministrazione di `POST /admin/creation-requests/{id}/approve`. Il richiedente non può approvare la propria richiesta e l'approvazione di un utente conta una volta. L'approvazione che completa il conteggio della richiesta crea ciò che chiede; `params.status` è `executed` quando è stata questa, altrimenti `pending`, e `params.waitingOn` elenca i domini che una richiesta di relazione attende ancora.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Esegue una richiesta di creazione che ha ottenuto le sue approvazioni — relazione, vista, tabella, sorgente o webhook — tramite il codice che usa la mutazione diretta. Rifiutata con `requests.approvals_incomplete` finché le approvazioni richieste (una, o due per una relazione) non sono raggiunte da utenti diversi dal richiedente; una richiesta di relazione è rifiutata anche con `requests.waiting_on_domains` finché un dominio che tocca non ha un approvatore. Le approvazioni si danno tramite `POST /admin/creation-requests/{id}/approve`, e l'approvazione che completa il conteggio esegue la creazione, quindi questa mutazione serve a ritentare una creazione fallita. Una creazione fallita lascia la richiesta in sospeso e risponde con il codice proprio del fallimento.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rifiuta una richiesta in coda con un motivo utilizzabile. `reason` è obbligatorio. Una richiesta di relazione può essere rifiutata da qualsiasi utente che potrebbe approvarla. L'autore di una richiesta di qualsiasi altro tipo può ritirarla; ciò viene registrato nel registro di amministrazione come ritiro.

---

### Compilazione query {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila una query GraphQL data-plane contro lo schema di un ruolo e restituisce la decisione di routing completa: SQL semantico, SQL motore, SQL diretto, route, metadati di enforcement (filtri RLS applicati, colonne escluse, mascheramento applicato), e Cypher compilato. Restituisce un risultato per ogni campo root nella query.

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

## Tipi di input chiave {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Solo-refresh-pianificato |
| `offPeakWindow` | `String` | Finestra di manutenzione `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuso orario IANA |
| `cdc` | `SourceCdcConfigInput` | Configurazione del trasporto Kafka CDC, incluso il modo di raggiungere il suo schema registry (vedere [Autenticazione dello schema registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` e `schemaRegistryToken` vengono restituiti come riferimenti del vault, mai come credenziali. |

### `TableInput`

L'input principale di registrazione tabella. Campi chiave oltre alle basi:

| Campo | Note |
|-------|------|
| `materialize` | Atterra una copia nello store di materializzazione |
| `mvRefreshInterval` | Secondi tra i refresh |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Manutenzione incrementale |
| `mvBitemporalMode` | `snapshot` \| `delta` per tabelle bitemporali |
| `mvCalendar` | Nome del calendario snapshot |
| `mvGrain` | Grana dello snapshot: `daily`, `weekly`, `monthly`, `annual`, o custom `3WE` / `LFR` |
| `viewSql` | SQL per una vista derivata |
| `viewMetrics` | Spec dichiarativa di vista composta da metriche — mutuamente esclusiva con `viewSql` |
| `dqContract` | Testo di contratto di qualità dei dati YAML/JSON |
| `queryTemplate` | Cypher per una tabella Neo4j |
| `live` | Config di live-delivery per push SSE/Kafka |
| `discover` | Inferisce le colonne dall'origine live al momento della registrazione |

### `RelationshipInput`

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
| `viaTable` | Nome tabella junction per archi molti-a-molti |
| `recordCandidate` | Scrive anche una riga relationship_candidates `accepted` |

---

## Esempio: registrare una tabella {: #example-registering-a-table }

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

## Esempio: creare una regola RLS {: #example-creating-an-rls-rule }

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
