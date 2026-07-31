# Schemi remoti

Un'origine di schema remoto collega un'API esterna — GraphQL, gRPC o REST (OpenAPI) — al livello semantico di Provisa. Una volta registrate, le operazioni dell'API esterna diventano tabelle e funzioni Provisa a tutti gli effetti. (REQ-308, REQ-316, REQ-325) Ogni regola di governance, interfaccia di query e livello di sicurezza si applica automaticamente. (REQ-310, REQ-319, REQ-328) Il servizio remoto non vede mai le regole di governance di Provisa. (REQ-310, REQ-319, REQ-328)

---

## Tre tipi di origine

### Schema remoto GraphQL (REQ-307–313)

**Come registrarlo.** Inviare una richiesta POST a `/admin/sources/graphql-remote` con l'URL dell'endpoint, un namespace e un'autenticazione facoltativa. Provisa avvia una query di introspezione standard `__schema` verso l'endpoint remoto. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Opzioni di autenticazione: `none`, `bearer` (header Authorization), `basic` (nome utente:password in Base64). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Override dei campi.** `field_overrides` è una mappa `{fieldName: "query" | "mutation"}` applicata dopo l'introspezione. Ha priorità sulla classificazione strutturale. Solo i campi di tipo query possono essere riclassificati come mutation; i campi di tipo mutation non hanno un percorso di override in GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relazioni al momento della registrazione.** `relationships` dichiara percorsi di join FK/PK tra tabelle al momento della registrazione. Vengono memorizzate come relazioni dichiarate manualmente (senza il flag `remote_managed`). Al successivo aggiornamento, le relazioni rilevate automaticamente (quelle con `remote_managed: True`) vengono rieseguite e possono cambiare; le relazioni dichiarate manualmente non vengono modificate. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**Cosa viene rilevato automaticamente.** Ogni campo del tipo `Query` remoto che restituisce un OBJECT diventa una tabella virtuale. Ogni campo del tipo `Mutation` remoto diventa una funzione tracciata. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Denominazione delle tabelle.** Le tabelle vengono denominate `{namespace}__{field_name}`. Con il namespace `petstore` e un campo di query `pets`: il nome della tabella è `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Mappatura dei tipi (REQ-308).** I campi scalari vengono mappati direttamente sui tipi Provisa. I campi OBJECT si suddividono in due casi a seconda che il tipo di destinazione sia governato o meno (vedere "Tabelle governate" più avanti). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Tipo GraphQL | Tipo Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (tipo inline non governato, ad es. `ContactInfo`) | colonna blob `jsonb` |
| OBJECT (tipo di destinazione governato) | escluso interamente da SDL e dal recupero |
| Qualsiasi ENUM | `jsonb` |
| Scalare personalizzato | `text` (valore di fallback) |

**Tabelle governate.** Un tipo GQL è governato quando compare come campo radice di `Query` nello schema remoto. `_collect_queryable_types` raccoglie questi tipi durante la registrazione, privilegiando i campi senza argomenti obbligatori in modo che possano essere recuperati in blocco come destinazioni di join. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Quando una colonna di tipo OBJECT in una tabella governata punta a un altro tipo governato, tale colonna è soggetta a tre regole contemporaneamente [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Esclusa dal recupero GQL** — il campo non viene richiesto durante il recupero delle righe della tabella padre.
2. **Esclusa dalla SDL** — il campo non compare sul tipo padre nello schema generato.
3. **Accessibile solo tramite una relazione dichiarata** — un data steward deve registrare un JOIN tra le due tabelle governate materializzate. In assenza di tale relazione, il campo è semplicemente assente; non esiste un fallback in forma di blob.

I tipi OBJECT NON raggiungibili come campi radice di Query (tipi inline come `ContactInfo` o `Address`) seguono regole diverse: vengono recuperati come colonne blob `jsonb` e compaiono nella SDL come campi di oggetto annidato. I sottocampi sono accessibili tramite estrazione `-->>` in SQL.

**Argomenti obbligatori.** Quando un campo di query radice presenta argomenti non nulli privi di valore predefinito, questi diventano colonne `native_filter_type: query_param` sulla tabella (con prefisso `_nf_` al momento dell'iniezione). L'executor le passa come variabili GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relazioni rilevate automaticamente.** Provisa esamina le colonne di tipo OBJECT di ciascuna tabella. Quando il tipo GQL referenziato è anch'esso registrato come tabella nella stessa origine, viene generata una relazione. Le relazioni molti-a-uno deducono le colonne di origine e destinazione dalle convenzioni di denominazione (`breedName` sul tipo di origine → `name` sul tipo di destinazione `Breed`). I campi uno-a-molti (LIST) generano relazioni con riferimenti di colonna vuoti — la chiave esterna risiede sul lato destinazione. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutation.** I campi mutation producono funzioni tracciate con tipi di argomento mappati dagli argomenti della mutation e uno `return_schema` derivato dal tipo di ritorno della mutation. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Aggiornamento.** Inviare una richiesta POST a `/admin/sources/graphql-remote/{id}/refresh`. Esegue nuovamente l'introspezione dello schema remoto e aggiorna le registrazioni di tabelle e funzioni. Le regole di governance esistenti (RLS, mascheramento) vengono preservate. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**Limitazioni.**

- I campi di query radice di tipo scalare ed ENUM (il cui tipo di ritorno non è OBJECT) diventano funzioni tracciate, non tabelle virtuali. Il loro `return_schema` è costituito da un'unica colonna `value` del tipo scalare mappato. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- L'annidamento degli oggetti viene risolto al momento della registrazione fino a `graphql_remote.max_object_depth` (predefinito: 5). Sia la selezione per il recupero remoto sia i metadati dei sottocampi vengono costruiti fino a tale profondità; i campi oltre il limite non vengono recuperati e non sono disponibili per l'estrazione SQL. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- I campi OBJECT annidati di tipo LIST (ad es. `breed.awards: [Award]`) sono inclusi nella selezione di recupero fino a `graphql_remote.max_list_depth` livelli di annidamento (predefinito: 2). Entro tale limite, la lista viene recuperata come array `jsonb` nella colonna padre, e la selezione GQL inietta `first: N`, dove N corrisponde a `graphql_remote.max_list_items` (predefinito: 100), per limitare la dimensione dell'array. Oltre `max_list_depth`, il campo LIST viene escluso interamente per evitare un'espansione illimitata dei dati. In SQL, l'array è accessibile tramite `json_array_elements(column_name)` o estrazione per indice con `->>`. Se il tipo di elemento della lista dispone di una propria query radice, è preferibile registrarlo come tabella separata e creare una relazione — il percorso di join è più efficiente e bypassa il blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- Per le query SQL, le colonne di tipo OBJECT non governate vengono recuperate integralmente dall'origine remota (tutti i sottocampi fino alla profondità configurata) e memorizzate nella cache come `jsonb`. L'accesso ai sottocampi in SQL avviene tramite estrazione `->>` sul blob; la richiesta remota non viene ristretta ai soli campi selezionati dalla query SQL. Quando il tipo di elemento della lista non dispone di una query radice e la rappresentazione in blob risulta insufficiente, è opportuno scrivere la query direttamente in SDL GraphQL — Provisa riproduce fedelmente la selezione di campi GQL, in modo che l'origine remota veda esattamente i campi richiesti. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Se il server remoto rifiuta un campo di tipo OBJECT perché richiede la selezione di sottocampi (situazione che non dovrebbe verificarsi quando `gql_selection` è disponibile), l'executor riprova una volta rimuovendo tali campi, in modo che le colonne scalari vengano comunque restituite. [tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### Schema remoto gRPC (REQ-322–329)

**Come registrarlo.** Inviare una richiesta POST a `/admin/grpc-remote/register` con l'indirizzo del server, un percorso o un URL a un file `.proto`, e una configurazione TLS facoltativa.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa recupera il proto, lo analizza con un parser di solo testo (senza dipendenze proto esterne al momento dell'analisi), compila gli stub Python tramite `grpc_tools.protoc` e apre un `grpc.aio.Channel` persistente. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

I file proto possono anche essere percorsi locali. I percorsi di importazione per i tipi ben noti (`google/protobuf/timestamp.proto`) vengono memorizzati al momento della registrazione e riutilizzati in fase di aggiornamento. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**Cosa viene rilevato automaticamente.** Ogni metodo `rpc` nel proto viene classificato come query o mutation usando tre segnali in ordine di priorità: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** nel payload di registrazione — `{"MethodName": "query"}` oppure `{"MethodName": "mutation"}` ha priorità su tutto il resto.
2. **`server_streaming: true`** — il server invia un flusso di messaggi; è sempre una tabella virtuale (a meno che l'output non sia uno scalare).
3. **Il messaggio di output presenta un campo ripetuto di tipo messaggio** — ad es. `ListOrdersResponse { repeated Order items; }` viene trattato come un wrapper di lista e diventa una tabella virtuale. I campi scalari ripetuti (ad es. `repeated string tags`) non attivano questa regola — sono proprietà di tipo array di una singola entità, non origini di righe.

I metodi che non corrispondono a nessuno di questi segnali (RPC unario che restituisce un singolo messaggio di entità, o qualsiasi output scalare) diventano funzioni tracciate.

**Denominazione delle tabelle.** Il nome predefinito è `{namespace}__{ServiceName}__{MethodName}`. In assenza di namespace, i nomi di servizio e metodo vengono uniti direttamente. A qualsiasi tabella registrata può essere assegnato un `alias`; quando impostato, l'alias è il nome utilizzato ovunque (query, SDL, relazioni). Il nome generato automaticamente è la chiave di registrazione e non cambia mai. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Mappatura dei tipi (REQ-324).** I tipi scalari proto vengono mappati sui tipi SQL come segue. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Tipo Proto | Tipo SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Messaggio annidato | `jsonb` |
| Enum | `text` |

**Relazioni al momento della registrazione.** `relationships` funziona in modo identico all'adattatore GQL — dichiara percorsi di join FK/PK memorizzati come relazioni dichiarate manualmente (senza il flag `remote_managed`). Al successivo aggiornamento, queste vengono preservate senza modifiche. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Metodi query (REQ-325).** I campi del messaggio di output diventano colonne della tabella. I campi del messaggio di input diventano sia argomenti GraphQL passati alla chiamata remota *sia* colonne registrate con il prefisso `_nf_` e `native_filter_type: "grpc_input"` — lo stesso meccanismo utilizzato da GQL e OpenAPI per l'iniezione di filtri nativi. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Sottocampi di messaggi annidati.** Per i metodi query, i campi di tipo messaggio non ripetuti alla profondità 0 (colonne di output dirette) hanno i propri sottocampi risolti un livello più in profondità e memorizzati come `object_fields` nel `ColumnDef`. Questi metadati vengono utilizzati per l'estrazione di sottocampi `jsonb` in SQL e per la documentazione dello schema. I campi annidati oltre la profondità 1 non vengono espansi ricorsivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

I metodi server-streaming raccolgono tutti i messaggi trasmessi in un elenco prima di restituire le righe. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Metodi mutation (REQ-326).** I campi del messaggio di input diventano argomenti di input della mutation. Lo schema del messaggio di output diventa il `return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**Gestione dei canali.** Un `grpc.aio.Channel` per ogni origine registrata viene memorizzato nello stato dell'applicazione e riutilizzato per le richieste successive. Il canale precedente viene chiuso prima che se ne apra uno nuovo al momento dell'aggiornamento. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Aggiornamento.** Inviare una richiesta POST a `/admin/grpc-remote/refresh/{source_id}`. Ricarica il proto dal percorso memorizzato, ricompila gli stub e riregistra tabelle e funzioni. In alternativa, inviare una richiesta PUT a `/admin/grpc-remote/{source_id}/proto` con un nuovo `proto_text` per aggiornare il proto in linea. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**Limitazioni.**

- L'estrazione dei sottocampi degli oggetti è limitata a un livello di profondità. I campi messaggio annidati oltre la profondità 1 non vengono espansi ricorsivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Come registrarlo.** Chiamare `auto_register_openapi_source` con un ID di origine, una specifica analizzata e metadati di connessione. La specifica viene caricata da un file locale o da un URL. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

**Payload di registrazione.** L'endpoint `/admin/openapi/register` accetta due campi aggiuntivi oltre a `source_id`, `spec_path`, ecc.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**Cosa viene rilevato automaticamente.** Ogni operazione GET nella specifica diventa una tabella virtuale, a meno che il suo schema di risposta non sia un tipo scalare (`string`, `number`, `boolean`, `integer`) — le GET che restituiscono valori scalari diventano funzioni tracciate con un'unica colonna `value`. Ogni operazione diversa da GET (POST, PUT, PATCH, DELETE) diventa una funzione tracciata. (REQ-316, REQ-317)

Priorità di classificazione: `operation_overrides` (payload) ha priorità su `x-provisa-kind` (estensione della specifica), che a sua volta ha priorità sull'euristica GET. `operation_overrides` è il percorso di override consigliato; `x-provisa-kind` è pensato per i casi in cui la specifica stessa debba veicolare la classificazione. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relazioni al momento della registrazione.** `relationships` funziona in modo identico agli altri adattatori — memorizzata come relazioni dichiarate manualmente, preservata in fase di aggiornamento. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Denominazione delle tabelle.** Le tabelle utilizzano l'`operationId` dell'operazione. Se non è definito alcun `operationId`, Provisa genera uno slug `{method}_{path}`. Un alias viene derivato eliminando il segmento verbale iniziale e mettendo al singolare il sostantivo (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Mappatura dei tipi.** I tipi JSON Schema vengono mappati sui tipi Provisa come segue. [tool-verified: `provisa/openapi/register.py:59–70`]

| Tipo JSON Schema | Tipo Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parametri come colonne di filtro nativo.** I parametri di percorso e di query che non sono già campi di risposta diventano colonne con `native_filter_type` impostato su `path_param` o `query_param`, con prefisso `_nf_`. Quando il nome di un parametro corrisponde al nome di un campo di risposta, i metadati del parametro vengono uniti alla voce di colonna esistente anziché creare un duplicato. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Risoluzione dello schema di risposta.** Il mapper verifica `responses.200`, poi `responses.2xx`, poi `responses.default`. Le risposte di tipo array vengono estese al proprio schema di elemento. I riferimenti `$ref` vengono risolti fino a un livello di profondità. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Sottocampi di oggetto.** Le proprietà di risposta con `type: object` e proprie `properties` vengono memorizzate come `object_fields` sulla colonna. Questi sottocampi sono visibili nella SDL e vengono utilizzati per l'estrazione `jsonb` nelle query. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Cache delle risposte (REQ-318).** I risultati delle operazioni GET vengono memorizzati nella cache in PostgreSQL da `pg_cache.py`. Ogni combinazione di parametri della richiesta ottiene un proprio gruppo `_params_hash`. Le righe di un determinato hash vengono sostituite alla scadenza del TTL. Gli endpoint con parametro di percorso (`/pets/{id}`) saltano il recupero massivo iniziale — la tabella cache viene creata vuota per l'introspezione dello schema, quindi popolata per chiave primaria man mano che arrivano le richieste. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Aggiornamento (REQ-321).** Analizzare nuovamente la specifica e richiamare `auto_register_openapi_source`. Le regole di governance esistenti vengono preservate; le registrazioni vengono aggiornate tramite upsert ON CONFLICT. [tool-verified: `provisa/openapi/register.py:249–264`]

**Limitazioni.**

- L'estrazione dei sottocampi degli oggetti è limitata a un livello di profondità. Le proprietà annidate all'interno di `object_fields` non vengono espanse ricorsivamente. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- I parametri header e cookie vengono ignorati; vengono registrati solo i parametri `path` e `query`. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- La risoluzione dei `$ref` a livello di specifica è limitata a un livello di profondità per gli schemi di proprietà; i riferimenti a componenti annidati in profondità potrebbero non risolversi. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Impatto della registrazione di una tabella remota

Una tabella registrata da qualsiasi origine di schema remoto è una tabella Provisa a tutti gli effetti. Nulla al suo interno viene trattato in modo diverso rispetto a una tabella relazionale connessa localmente in fase di esecuzione. (REQ-308, REQ-313)

**Interfacce di query.** La tabella è immediatamente interrogabile tramite GraphQL, SQL (pgwire o diretto), Cypher (GQL), JSON:API e Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) La generazione dello schema sintetizza `ColumnMetadata` per le tabelle remote, poiché non dispongono di un catalogo — la mappatura dei tipi viene applicata al momento della costruzione dello schema. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Modello di sicurezza.** Si applicano tutti e cinque i livelli di governance:

1. Controllo di accesso per dominio — il `domain_id` della tabella determina quali ruoli possono vederla. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Sicurezza a livello di riga (RLS) — i filtri di riga configurati sulla tabella vengono iniettati in ogni query, indipendentemente dall'interfaccia. (REQ-040, REQ-041)
3. Visibilità delle colonne — l'elenco `visible_to` di ciascuna colonna controlla l'esposizione del campo per ruolo. (REQ-039)
4. Mascheramento delle colonne — le regole di mascheramento si applicano nella Fase 2 della pipeline di governance. (REQ-040, REQ-263)
5. Protezione dei predicati — le colonne mascherate vengono rifiutate nelle clausole WHERE e HAVING. (REQ-603)

Le query ad hoc contro tabelle remote sono consentite esclusivamente in base ai diritti dell'utente — l'accesso è uniformemente basato sui diritti (diritti su tabella/colonna + relazioni approvate), senza una modalità di governance specifica per tabella. (REQ-001, REQ-003)

**Governance delle relazioni (V002).** Le condizioni JOIN contro tabelle remote — quando interrogate tramite SQL o Cypher — devono corrispondere a una relazione registrata e approvata. (REQ-604) Il controllo V002 viene ignorato per le query GraphQL, poiché le relazioni definite nella SDL sono preapprovate per progettazione. Vedere [docs/security.md](security.md#governance-delle-relazioni-v002).

**Colonne di tipo OBJECT.** Quando una colonna corrisponde a un OBJECT GQL inline non governato o a un tipo oggetto OpenAPI, il suo tipo Provisa è `jsonb`. La colonna memorizza l'intero blob JSON annidato. Quando sono dichiarati sottocampi (`gql_object_fields` oppure `object_fields`), la mappa `gql_object_columns` viene popolata al momento della costruzione dello schema. Il generatore SQL utilizza questa mappa per emettere espressioni di estrazione `->>` per i sottocampi quando una query li seleziona. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Argomenti obbligatori come parametri di filtro nativo.** I campi di query radice con argomenti non nulli e privi di valore predefinito iniettano colonne aggiuntive sulla tabella registrata. Queste colonne portano `native_filter_type: query_param`. Il traduttore Cypher riscrive `WHERE n.id = $val` in `WHERE n._nf_id = $val`, e l'executor GraphQL le recupera come variabili da passare all'endpoint remoto. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impatto della creazione di una relazione di copertura

Quando un data steward registra una relazione tra due tabelle remote (o tra una tabella remota e una tabella locale), tale relazione diventa il percorso di join utilizzato in fase di query.

**Come prevale il join.** Durante la compilazione della query, Provisa risolve il percorso di join attraverso la relazione registrata. `source_column` e `target_column` della relazione diventano la condizione di join nell'SQL generato. Il join sostituisce qualsiasi chiamata remota per tabella che altrimenti sarebbe necessaria per il tipo connesso.

**Il blob grezzo non viene mai esposto in SQL.** La colonna `breed` su `petstore__pets` non è selezionabile come valore jsonb grezzo nelle query SQL. Quando è registrata una relazione tra `petstore__pets` e `petstore__breeds`, le query SQL attraversano il join — `SELECT breed.name FROM petstore__pets` viene risolta tramite il join FK, non tramite un blob. Quando non è registrata alcuna relazione ma la colonna presenta sottocampi dichiarati (`gql_object_fields`), i riferimenti ai sottocampi in SQL vengono riscritti come estrazione `->>` sul blob memorizzato. Questo percorso è disponibile solo per i tipi inline non governati — i campi con tipo di destinazione governato sono esclusi interamente dalla SDL e non dispongono di alcun blob da cui estrarre. Il blob grezzo in sé non viene mai emesso come valore di colonna semplice. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

Nella SDL GraphQL, un campo OBJECT inline non governato viene tipizzato come il tipo di oggetto annidato. Che venga servito tramite join o tramite estrazione da blob in fase di esecuzione è un dettaglio implementativo — la forma della SDL è identica in entrambi i casi. Quando il tipo figlio viene registrato come tabella propria (diventando così governato), tutti e cinque i livelli di governance si applicano ad esso in modo indipendente: proprie regole RLS, visibilità delle colonne, regole di mascheramento, protezione dei predicati e controllo di accesso per dominio. (REQ-039, REQ-040, REQ-041, REQ-263) L'estrazione da blob elude questo meccanismo — i dati del figlio arrivano pre-incorporati nella riga padre e sono governati solo dalle regole della tabella padre. Registrare il figlio come tabella e creare una relazione è la via per ottenere una governance granulare sul tipo figlio.

**`graphql_alias` sulla relazione.** Il campo `graphql_alias` denomina il campo SDL che la relazione espone sul tipo padre. In sua assenza, il nome viene derivato dal `field_name` della tabella di destinazione e dalla cardinalità della relazione tramite `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 sul percorso di join.** Le query SQL e Cypher che attraversano la relazione sono soggette alla governance delle relazioni V002. La relazione deve essere registrata e approvata affinché il join sia consentito. (REQ-604) L'attraversamento GraphQL tramite il campo di relazione della SDL è sempre preapprovato. [tool-verified: `docs/security.md:41–54`]

**Flag remote-managed.** Le relazioni rilevate automaticamente durante la registrazione di uno schema remoto GraphQL vengono memorizzate con `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Si tratta di un marcatore di metadati; non altera il comportamento di governance.

---

## Comportamento delle sole definizioni di tipo

Non tutti i tipi in uno schema remoto devono essere una tabella interrogabile.

Quando `root_table_ids` è impostato su uno `SchemaInput`, le tabelle il cui ID è assente da tale insieme vengono escluse dai campi di query radice nella SDL generata. Rimangono presenti come tipi GraphQL e sono raggiungibili tramite campi di relazione su tabelle che dispongono di voci radice. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

Lo stesso meccanismo si applica alle build di schema filtrate per dominio: le tabelle nei domini a cui il ruolo non può accedere sono solo definizioni di tipo — la loro definizione di tipo esiste nella SDL per l'attraversamento delle relazioni, ma per esse non viene generato alcun campo di query radice. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Una tabella di sola definizione di tipo:

- Non ha alcun campo di query radice — i client non possono interrogarla direttamente per nome.
- È raggiungibile tramite campi di relazione su tabelle che dispongono di voci radice.
- Continua a comparire nell'introspezione dello schema come tipo con nome.
- Mantiene l'applicazione di tutte le regole di governance quando si accede ai dati tramite una relazione. (REQ-039, REQ-040)

La rimozione completa dallo schema — inclusa la definizione di tipo — avviene solo quando la registrazione della tabella viene eliminata interamente. Contrassegnare una tabella come sola definizione di tipo (rimuovendo il suo ID da `root_table_ids` oppure filtrando in base all'accesso al dominio) non rimuove il tipo.

Questo design consente ai data steward di esporre grafi di oggetti navigabili in cui alcuni tipi sono raggiungibili solo tramite attraversamento, non tramite query indipendente.
