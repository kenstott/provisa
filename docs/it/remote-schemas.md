# Schemi remoti {: #remote-schemas }

Un'origine di schema remoto collega un'API esterna, che sia GraphQL (GitHub compreso), gRPC o REST (OpenAPI), al modello Provisa. Aggiungere l'origine non registra alcuna tabella. L'origine offre tabelle, e uno steward registra ciascuna di quelle che vuole nel selettore Register Table; quella registrazione è il passo di curatela. Una tabella registrata è una tabella Provisa a tutti gli effetti, quindi ogni regola di governance, interfaccia di query e livello di sicurezza le si applica automaticamente. Il servizio remoto non vede mai le regole di governance di Provisa.

---

## Tre tipi di origine {: #three-source-types }

### Schema remoto GraphQL {: #graphql-remote-schema }

**Come aggiungere l'origine.** Inviare una richiesta POST a `/admin/sources/graphql-remote` con l'URL dell'endpoint, un namespace e un'autenticazione facoltativa. Provisa avvia una query di introspezione standard `__schema` verso l'endpoint remoto per confermare l'endpoint e le credenziali.

Aggiungere l'origine non registra alcuna tabella né alcun command. Ogni tipo di origine remota risponde all'aggiunta e all'aggiornamento con gli stessi contatori: `tables` (tabelle registrate o aggiornate; 0 all'aggiunta), `available_tables` (tabelle offerte), `mutations` (sempre 0) e `available_mutations` (command offerti).

**Registrare le tabelle.** Aprire Tables, poi Register Table, scegliere l'origine e lo schema `graphql`, quindi selezionare le tabelle e le colonne desiderate. Tramite l'API GraphQL di amministrazione: `availableTables(sourceId, schemaName)` elenca le tabelle offerte, `availableColumns` elenca le colonne di una tabella e `registerTable(input: TableInput)` ne registra una con le colonne scelte. Una tabella registrata è quindi governata.

Il modo in cui viene letta una tabella registrata (campo radice, percorso delle righe, argomenti obbligatori, argomenti di paginazione) è memorizzato in `sources.mapping["tables"]`, così un processo riavviato la legge senza chiedere al remoto il suo schema.

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

Opzioni di autenticazione: `none`, `bearer` (header Authorization), `basic` (nome utente:password in Base64).

**Override dei campi.** `field_overrides` è una mappa `{fieldName: "query" | "mutation"}` applicata dopo l'introspezione. Ha priorità sulla classificazione strutturale. Solo i campi di tipo query possono essere riclassificati come mutation; i campi di tipo mutation non hanno un percorso di override in GraphQL.

**Relazioni al momento della registrazione.** `relationships` dichiara percorsi di join FK/PK tra tabelle al momento della registrazione. Vengono memorizzate come relazioni dichiarate manualmente (senza il flag `remote_managed`). Al successivo aggiornamento, le relazioni rilevate automaticamente (quelle con `remote_managed: True`) vengono rieseguite e possono cambiare; le relazioni dichiarate manualmente non vengono modificate.

**Cosa offre l'origine.** Ogni campo del tipo `Query` remoto che restituisce un oggetto o una lista di oggetti viene offerto come tabella, così come ogni connessione Relay sotto un campo a oggetto singolo (vedi sotto). Registrare una tabella offerta la rende una tabella. Ogni campo del tipo `Mutation` remoto è un comando offerto, contato in `available_mutations`; aggiungere l'origine non ne registra nessuno. Registrare quelli desiderati come comandi; vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation).

**Denominazione delle tabelle.** Le tabelle vengono denominate `{namespace}__{field_name}`. Con il namespace `petstore` e un campo di query `pets`: il nome della tabella è `petstore__pets`.

**Connessioni Relay.** Molte API restituiscono gli elenchi come connessioni Relay: un oggetto con `nodes` (o `edges { node }`) accanto a `pageInfo`. Provisa mappa una connessione su una tabella dei suoi nodi e la legge pagina per pagina.

- Un campo radice che restituisce una connessione (`securityAdvisories`) diventa una tabella dei suoi nodi.
- Una connessione sull'oggetto singolo restituito da un campo radice diventa una tabella a sé. La tabella assume gli argomenti obbligatori del campo radice. Con `repository(owner, name)` e una connessione `issues` su `Repository`, la tabella è `repositoryIssues`, con nome SQL `gh__repository_issues` nel namespace `gh`. La si filtra tramite le colonne `_nf_owner` e `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Una connessione non è mai una colonna. Altrimenti una riga porterebbe una lettura che il remoto calcola per riga, per ogni connessione che il suo tipo possiede.
- Una connessione è una tabella solo se il suo campo accetta `first` e `after`, così da poter essere letta pagina per pagina. Una connessione che richiede un argomento proprio non è una tabella. Non lo è nemmeno una connessione di un'unione, né alcuna connessione sotto un campo radice che restituisce un elenco.

**Mappatura dei tipi.** I campi scalari vengono mappati direttamente sui tipi Provisa. I campi OBJECT si suddividono in due casi a seconda che il tipo di destinazione sia governato o meno (vedere "Tabelle governate" più avanti).

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

**Tabelle governate.** Un tipo GQL è governato quando compare come campo radice di `Query` nello schema remoto. `_collect_queryable_types` raccoglie questi tipi durante la registrazione, privilegiando i campi senza argomenti obbligatori in modo che possano essere recuperati in blocco come destinazioni di join.

Quando una colonna di tipo OBJECT in una tabella governata punta a un altro tipo governato, tale colonna è soggetta a tre regole contemporaneamente:

1. **Esclusa dal recupero GQL** — il campo non viene richiesto durante il recupero delle righe della tabella padre.
2. **Esclusa dalla SDL** — il campo non compare sul tipo padre nello schema generato.
3. **Accessibile solo tramite una relazione dichiarata** — un data steward deve registrare un JOIN tra le due tabelle governate materializzate. In assenza di tale relazione, il campo è semplicemente assente; non esiste un fallback in forma di blob.

I tipi OBJECT NON raggiungibili come campi radice di Query (tipi inline come `ContactInfo` o `Address`) seguono regole diverse: vengono recuperati come colonne blob `jsonb` e compaiono nella SDL come campi di oggetto annidato. I sottocampi sono accessibili tramite estrazione `-->>` in SQL.

**I campi che richiedono un argomento non sono colonne.** Un campo con un argomento obbligatorio non può essere selezionato così com'è, quindi viene lasciato fuori dalle colonne della tabella e dalle selezioni annidate.

**Argomenti obbligatori.** Quando un campo di query radice presenta argomenti non nulli privi di valore predefinito, questi diventano colonne `native_filter_type: query_param` sulla tabella (con prefisso `_nf_` al momento dell'iniezione). L'executor le passa come variabili GraphQL.

**Relazioni rilevate automaticamente.** Provisa esamina le colonne di tipo OBJECT di ogni tabella registrata. Quando il tipo GQL a cui una colonna rimanda è anch'esso una tabella registrata della stessa origine, e la colonna su cui poggia la relazione è tra quelle registrate, la relazione viene salvata. Una tabella non ancora registrata non ne riceve. Le relazioni many-to-one ricavano le colonne di origine e di destinazione dalle convenzioni di denominazione (`breedName` sul tipo di origine → `name` sul tipo di destinazione `Breed`). I campi one-to-many (LIST) producono relazioni con riferimenti di colonna vuoti, perché la FK sta dal lato della destinazione.

**Mutazioni.** Un campo di mutazione viene registrato, uno alla volta, come comando di tipo `source_operation`. I suoi argomenti sono ciascuno tipizzati `json` e passati al servizio remoto come variabili tipizzate; la risposta è il JSON che il servizio remoto restituisce, senza `return_schema`. Vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation).

**Aggiornamento.** Inviare una richiesta POST a `/admin/sources/graphql-remote/{id}/refresh`. Esegue nuovamente l'introspezione dello schema remoto e porta allo stato di tale schema le tabelle già registrate. Non aggiunge alcuna tabella né colonna: una tabella o una colonna che lo schema ha guadagnato resta offerta, e una colonna che lo schema ha perso viene eliminata. Le regole di governance esistenti (RLS, mascheramento) vengono preservate.

**Limitazioni.**

- I campi di query radice di tipo scalare ed ENUM (il cui tipo di ritorno non è OBJECT) diventano funzioni tracciate, non tabelle virtuali. Il loro `return_schema` è costituito da un'unica colonna `value` del tipo scalare mappato.
- L'annidamento degli oggetti viene risolto al momento della registrazione fino a `graphql_remote.max_object_depth` (predefinito: 5). Sia la selezione del recupero remoto sia i metadati dei sottocampi vengono costruiti fino a tale profondità; i campi oltre il limite non vengono recuperati e non sono disponibili per l'estrazione SQL. Un tipo viene attraversato una sola volta lungo un singolo percorso: un campo il cui tipo è già sul cammino verso il basso viene lasciato fuori, così uno schema i cui tipi si riferiscono l'un l'altro viene percorso una volta per tipo, non una volta per livello di profondità.
- I campi OBJECT annidati di tipo LIST (ad es. `breed.awards: [Award]`) sono inclusi nella selezione di recupero fino a `graphql_remote.max_list_depth` livelli di annidamento (predefinito: 2). Entro tale limite, l'elenco viene recuperato come array `jsonb` nella colonna padre. Quando il campo elenco dichiara un argomento `first` (Relay, PostGraphile, pg_graphql) o un argomento `limit` (Hasura), la selezione lo passa come `first: N` o `limit: N`, dove N è `graphql_remote.max_list_items` (predefinito: 100). Un campo elenco che non dichiara né l'uno né l'altro non riceve alcun argomento, perché un remoto rifiuta un argomento che il campo non dichiara. Oltre `max_list_depth`, il campo LIST viene escluso del tutto per evitare un'espansione illimitata dei dati. In SQL, l'array è accessibile tramite `json_array_elements(column_name)` o l'estrazione per indice con `->>`. Se il tipo di elemento dell'elenco dispone di una propria query radice, registrarlo invece come tabella separata e creare una relazione — il percorso di join è più efficiente e aggira il blob.
- Per le query SQL, le colonne di tipo OBJECT non governate vengono recuperate integralmente dall'origine remota (tutti i sottocampi fino alla profondità configurata) e memorizzate nella cache come `jsonb`. L'accesso ai sottocampi in SQL avviene tramite estrazione `->>` sul blob; la richiesta remota non viene ristretta ai soli campi selezionati dalla query SQL. Quando il tipo di elemento della lista non dispone di una query radice e la rappresentazione in blob risulta insufficiente, è opportuno scrivere la query direttamente in SDL GraphQL — Provisa riproduce fedelmente la selezione di campi GQL, in modo che l'origine remota veda esattamente i campi richiesti.
- Se il server remoto rifiuta un campo di tipo OBJECT perché richiede la selezione di sottocampi (situazione che non dovrebbe verificarsi quando `gql_selection` è disponibile), l'executor riprova una volta rimuovendo tali campi, in modo che le colonne scalari vengano comunque restituite. Ciò vale per le tabelle lette da un campo radice. Una tabella di connessione non segue questo percorso.

**Letture paginate.** Una tabella di connessione viene letta tramite cursore. Ogni pagina richiede `first: N, after: $pageCursor` con `pageInfo { hasNextPage endCursor }`, e la lettura segue `endCursor` finché il remoto non segnala che non c'è una pagina successiva.

| Impostazione | Predefinito | Effetto |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Righe per pagina. |
| `graphql_remote.max_rows` | `10000` | Il massimo di righe che una lettura di una tabella di connessione acquisisce. Una lettura che lo raggiunge si ferma e registra un avviso. |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Due risposte fanno riprovare l'executor:

- **Pagina troppo pesante.** Quando il remoto risponde 502 o 504, la stessa pagina viene richiesta di nuovo a metà dimensione, fino a una riga.
- **Limite di frequenza con tempo di attesa.** Quando il remoto risponde 403 o 429 con un `Retry-After` di 120 secondi o meno, l'executor attende quel tempo e invia di nuovo la richiesta, fino a tre tentativi. Un rifiuto senza `Retry-After`, o che chiede un'attesa più lunga, viene sollevato come errore. Ciò vale per ogni lettura, di connessione o meno.

Qualsiasi altro errore nella risposta fa fallire la lettura, a meno che il tipo di origine non disponga diversamente (vedi GitHub sotto). Una connessione il cui padre è tornato nullo non ha righe.

---

### GitHub

GitHub è un tipo di origine ordinario. La sua API è GraphQL, quindi le sue tabelle si comportano come descritto sopra, comprese le tabelle di connessione come `gh__repository_issues`.

**Aggiungere l'origine.**

1. Aprire Sources e aggiungere un'origine di tipo **GitHub**.
2. Inserire un token di accesso GitHub. Facoltativamente inserire un namespace, il prefisso dei nomi di tabella; il valore predefinito è `gh`.
3. Salvare. Provisa verifica il token presso GitHub. Un token che GitHub rifiuta fa fallire l'aggiunta con il messaggio di GitHub.

Aggiungere l'origine non registra alcuna tabella.

**Registrare le tabelle.** Aprire Tables, poi Register Table. Scegliere l'origine GitHub, scegliere lo schema `graphql`, quindi scegliere le tabelle desiderate. Ogni tabella che GitHub offre viene elencata; la registrazione è la vostra scelta di cosa esporre.

**Ambiti (scope) del token.** Quando registrate una tabella, Provisa la verifica una volta presso GitHub con il vostro token.

- Un campo che gli ambiti del token non coprono viene lasciato fuori dalla tabella. Il risultato nomina ogni campo escluso: `Left out, because the source's credential may not read them: projectsV2`.
- Una tabella che il token non può leggere affatto viene rifiutata, con il motivo di GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`.

**Righe che il token non può vedere.** GitHub risponde `FORBIDDEN` per un campo che il token non può vedere in una particolare riga, come i collaboratori di un repository senza accesso in scrittura, e `NOT_ORG_OWNED_REPO` per un campo che esiste solo nei repository di proprietà di un'organizzazione. Quel campo è nullo in quella riga, il resto della lettura prosegue e Provisa registra un avviso. Un errore a carico della tabella stessa fa fallire la lettura.

**Pagine pesanti.** Quando GitHub risponde `RESOURCE_LIMITS_EXCEEDED` perché calcolare una pagina costa troppo, la pagina viene richiesta di nuovo a metà dimensione.

**Oggetti annidati.** Le tabelle GitHub usano una propria profondità di annidamento, 0 (`max_object_depth=0` per questo tipo di origine), invece di `graphql_remote.max_object_depth`. Una colonna di oggetto annidato viene selezionata solo con i propri campi scalari, e gli oggetti al suo interno compaiono come `__typename`.

**Archiviazione del token.** Il token va nel vault dei segreti e la riga dell'origine conserva un riferimento, così un riavvio rilegge l'origine senza dover reinserire il token.

**Come funziona (operatori).** Lo schema di GitHub è distribuito con Provisa, quindi aggiungere l'origine non effettua alcuna chiamata di introspezione e uno schema grande non costa nulla alla registrazione. Le tabelle vengono mappate da esso una alla volta man mano che le registrate. L'endpoint refresh rifiuta questo tipo di origine; un nuovo schema GitHub arriva con una release di Provisa.

---

### GitLab

GitLab è un tipo di origine ordinario e viene aggiunto e registrato come GitHub: aggiungere un'origine di tipo **GitLab** con un token di accesso, quindi registrare le tabelle desiderate dallo schema `graphql`. Il prefisso predefinito dei nomi di tabella è `gl`. L'origine raggiunge `gitlab.com`.

**Scegliere le colonne.** GitLab assegna un costo a ogni query e rifiuta quella che costa troppo: 200 punti per un chiamante anonimo, 250 con un token. Una tabella larga con tutte le colonne selezionate supera tale costo, quindi registrate una tabella GitLab con le colonne che desiderate.

- Quando registrate una tabella, Provisa chiede una volta a GitLab se servirà la selezione con la dimensione di pagina usata dalle letture. Se GitLab risponde che la query è troppo complessa o troppo grande, la tabella non viene registrata e il risultato riporta il messaggio di GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- Il costo di una colonna dipende dal suo tipo. Un valore semplice costa circa un punto; una colonna di oggetto annidato costa molte volte tanto. Eliminare le colonne di oggetto annidato libera di più.
- La dimensione di pagina fa parte del costo. È `graphql_remote.max_list_items`.

**Controllo del token.** GitLab risponde a un token non riconosciuto con un risultato vuoto invece che con un errore. Provisa lo considera un token rifiutato e non aggiunge l'origine.

---

### Schema remoto gRPC {: #grpc-remote-schema }

**Come aggiungere l'origine.** Inviare una richiesta POST a `/admin/grpc-remote/register` con l'indirizzo del server, un percorso o un URL a un file `.proto` e una configurazione TLS facoltativa. Aggiungere l'origine non registra alcuna tabella.

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

Provisa recupera il proto, lo analizza con un parser di solo testo (senza dipendenze proto esterne al momento dell'analisi), compila gli stub Python tramite `grpc_tools.protoc` e apre un `grpc.aio.Channel` persistente.

I file proto possono anche essere percorsi locali. I percorsi di importazione per i tipi ben noti (`google/protobuf/timestamp.proto`) vengono memorizzati al momento della registrazione e riutilizzati in fase di aggiornamento.

**Cosa offre l'origine.** Ogni metodo `rpc` del proto viene classificato come query o mutation mediante tre segnali, in ordine di priorità:

1. **`method_overrides`** nel payload di registrazione — `{"MethodName": "query"}` oppure `{"MethodName": "mutation"}` ha priorità su tutto il resto.
2. **`server_streaming: true`** — il server invia un flusso di messaggi; è sempre una tabella virtuale (a meno che l'output non sia uno scalare).
3. **Il messaggio di output presenta un campo ripetuto di tipo messaggio** — ad es. `ListOrdersResponse { repeated Order items; }` viene trattato come un wrapper di lista e diventa una tabella virtuale. I campi scalari ripetuti (ad es. `repeated string tags`) non attivano questa regola — sono proprietà di tipo array di una singola entità, non origini di righe.

I metodi che non corrispondono a nessuno di questi segnali (RPC unario che restituisce un singolo messaggio di entità, o qualsiasi output scalare) diventano funzioni tracciate.

**Registrare le tabelle.** Ogni metodo query è offerto come una tabella, denominata `{namespace}__{Service}__{Method}`, sotto lo schema di selettore `grpc_remote`. Registrare quelli desiderati con il selettore Register Table (`availableTables`, `availableColumns`, `registerTable`, come per le origini GraphQL), scegliendo le colonne di risposta. I campi della richiesta diventano colonne di filtro nativo `_nf_*`, e queste sono sempre incluse.

I metodi di mutazione sono comandi offerti, contati in `available_mutations`; aggiungere l'origine non ne registra nessuno. Una mutazione gRPC si registra nella pagina Comandi scegliendo l'origine e poi il metodo, chiamato `Service.Method`; il tipo del comando è `source_operation`. Vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation).

**Denominazione delle tabelle.** Il nome predefinito è `{namespace}__{ServiceName}__{MethodName}`. In assenza di namespace, i nomi di servizio e metodo vengono uniti direttamente. A qualsiasi tabella registrata può essere assegnato un `alias`; quando impostato, l'alias è il nome utilizzato ovunque (query, SDL, relazioni). Il nome generato automaticamente è la chiave di registrazione e non cambia mai.

**Mappatura dei tipi.** I tipi scalari proto vengono mappati sui tipi SQL come segue.

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

**Relazioni alla registrazione.** `relationships` funziona come nell'adattatore GQL: dichiara cammini di join FK/PK, salvati come relazioni dichiarate a mano (senza flag `remote_managed`). Un aggiornamento le lascia come sono.

**Metodi query.** I campi del messaggio di output diventano colonne della tabella. I campi del messaggio di input diventano sia argomenti GraphQL passati alla chiamata remota *sia* colonne registrate con il prefisso `_nf_` e `native_filter_type: "grpc_input"` — lo stesso meccanismo utilizzato da GQL e OpenAPI per l'iniezione di filtri nativi.

**Sottocampi di messaggi annidati.** Per i metodi query, i campi di tipo messaggio non ripetuti alla profondità 0 (colonne di output dirette) hanno i propri sottocampi risolti un livello più in profondità e memorizzati come `object_fields` nel `ColumnDef`. Questi metadati vengono utilizzati per l'estrazione di sottocampi `jsonb` in SQL e per la documentazione dello schema. I campi annidati oltre la profondità 1 non vengono espansi ricorsivamente.

I metodi server-streaming raccolgono tutti i messaggi trasmessi in un elenco prima di restituire le righe.

**Metodi di mutazione.** Un metodo di mutazione registrato è un comando i cui argomenti sono i campi del messaggio di input, ciascuno tipizzato `json` e passato invariato. La risposta del servizio remoto torna come righe; una chiamata rifiutata è un 422, `functions.remote_refused`. Vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation).

**Gestione dei canali.** Un `grpc.aio.Channel` per ogni origine registrata viene memorizzato nello stato dell'applicazione e riutilizzato per le richieste successive. Il canale precedente viene chiuso prima che se ne apra uno nuovo al momento dell'aggiornamento.

**Aggiornamento.** Inviare una richiesta POST a `/admin/grpc-remote/refresh/{source_id}`. Ricarica il proto dal percorso memorizzato, ricompila gli stub e porta allo stato del proto le tabelle già registrate, con le colonne con cui ciascuna è stata registrata. Non registra alcuna nuova tabella; un metodo query aggiunto al proto resta offerto. In alternativa, inviare una richiesta PUT a `/admin/grpc-remote/{source_id}/proto` con un nuovo `proto_text` per aggiornare il proto in linea.

**Limitazioni.**

- L'estrazione dei sottocampi degli oggetti è limitata a un livello di profondità. I campi messaggio annidati oltre la profondità 1 non vengono espansi ricorsivamente.

---

### OpenAPI / REST

**Come aggiungere l'origine.** Inviare una richiesta POST a `/admin/openapi/register` con un ID origine e una specifica, caricata da un file locale o da un URL. La specifica viene analizzata e conservata con l'origine; non viene registrata alcuna tabella né alcun command. La risposta riporta `tables: 0` e `mutations: 0`, con i contatori offerti in `available_tables` e `available_mutations`.

**Registrare le tabelle.** Registrare ogni operazione GET desiderata tramite il selettore Register Table (`availableTables`, `availableColumns`, `registerTable`), scegliendo le colonne. Registrare ogni operazione non GET singolarmente come comando nella pagina Comandi, elencate da `availableFunctions`; vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation). `PUT /admin/openapi/spec/{source_id}` memorizza una specifica modificata a mano, non registra nulla e restituisce `available_tables` e `available_mutations`.

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

**Cosa offre l'origine.** Ogni operazione GET della specifica viene offerta come tabella, a meno che il suo schema di risposta sia un tipo scalare (`string`, `number`, `boolean`, `integer`); un GET che restituisce uno scalare è una funzione con un'unica colonna `value`. Ogni altra operazione (POST, PUT, PATCH, DELETE) viene offerta come command, con il nome del suo `operationId`. Una volta registrata, riceve i parametri di percorso dell'operazione e un argomento `body` per il corpo della richiesta, ciascuno di tipo `json`, e ogni altro argomento va nella query string. Vedi [Operazione di scrittura di un'origine remota](commands.md#a-remote-sources-write-operation).

Priorità di classificazione: `operation_overrides` (payload) ha priorità su `x-provisa-kind` (estensione della specifica), che a sua volta ha priorità sull'euristica GET. `operation_overrides` è il percorso di override consigliato; `x-provisa-kind` è pensato per i casi in cui la specifica stessa debba veicolare la classificazione.

**Relazioni alla registrazione.** `relationships` funziona come negli altri adattatori: salvate come relazioni dichiarate a mano e conservate all'aggiornamento.

**Denominazione delle tabelle.** Le tabelle utilizzano l'`operationId` dell'operazione. Se non è definito alcun `operationId`, Provisa genera uno slug `{method}_{path}`. Un alias viene derivato eliminando il segmento verbale iniziale e mettendo al singolare il sostantivo (`findPetsByStatus` → `pet_by_status`).

**Mappatura dei tipi.** I tipi JSON Schema vengono mappati sui tipi Provisa come segue.

| Tipo JSON Schema | Tipo Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parametri come colonne di filtro nativo.** I parametri di percorso e di query che non sono già campi della risposta diventano colonne con `native_filter_type` uguale a `path_param` o `query_param`, con prefisso `_nf_`. Quando un parametro ha lo stesso nome di un campo della risposta, i suoi metadati vengono uniti alla voce di quella colonna, senza creare duplicati.

**Risoluzione dello schema di risposta.** Il mapper verifica `responses.200`, poi `responses.2xx`, poi `responses.default`. Le risposte di tipo array vengono estese al proprio schema di elemento. I riferimenti `$ref` vengono risolti fino a un livello di profondità.

**Sottocampi di oggetto.** Le proprietà di risposta con `type: object` e proprie `properties` vengono memorizzate come `object_fields` sulla colonna. Questi sottocampi sono visibili nella SDL e vengono utilizzati per l'estrazione `jsonb` nelle query.

**Cache delle risposte.** I risultati delle operazioni GET vengono memorizzati nella cache in PostgreSQL da `pg_cache.py`. Ogni combinazione di parametri della richiesta ottiene un proprio gruppo `_params_hash`. Le righe di un determinato hash vengono sostituite alla scadenza del TTL. Gli endpoint con parametro di percorso (`/pets/{id}`) saltano il recupero massivo iniziale — la tabella cache viene creata vuota per l'introspezione dello schema, quindi popolata per chiave primaria man mano che arrivano le richieste.

**Aggiornamento.** Inviare una richiesta POST a `/admin/openapi/refresh/{source_id}`. Analizza nuovamente la specifica tramite `_load_and_register`, che non registra nulla: non aggiunge alcuna tabella né colonna. Le regole di governance esistenti vengono preservate. Una tabella registrata mantiene le sue colonne; viene letta tramite l'operazione della spec aggiornata.

**Limitazioni.**

- L'estrazione dei sottocampi degli oggetti è limitata a un livello di profondità. Le proprietà annidate all'interno di `object_fields` non vengono espanse ricorsivamente.
- I parametri header e cookie vengono ignorati; vengono registrati solo i parametri `path` e `query`.
- La risoluzione dei `$ref` a livello di specifica è limitata a un livello di profondità per gli schemi di proprietà; i riferimenti a componenti annidati in profondità potrebbero non risolversi.

---

## Impatto della registrazione di una tabella remota {: #impact-of-registering-a-remote-table }

Una tabella registrata da qualsiasi origine di schema remoto è una tabella Provisa a tutti gli effetti. Nulla al suo interno viene trattato in modo diverso rispetto a una tabella relazionale connessa localmente in fase di esecuzione.

**Interfacce di query.** La tabella è immediatamente interrogabile tramite GraphQL, SQL (pgwire o diretto), Cypher (GQL), JSON:API e Arrow Flight. La generazione dello schema sintetizza `ColumnMetadata` per le tabelle remote, poiché non dispongono di un catalogo — la mappatura dei tipi viene applicata al momento della costruzione dello schema.

**Modello di sicurezza.** Si applicano tutti e cinque i livelli di governance:

1. Controllo di accesso per dominio — il `domain_id` della tabella determina quali ruoli possono vederla.
2. Sicurezza a livello di riga (RLS) — i filtri di riga configurati sulla tabella vengono iniettati in ogni query, indipendentemente dall'interfaccia.
3. Visibilità delle colonne — l'elenco `visible_to` di ciascuna colonna controlla l'esposizione del campo per ruolo.
4. Mascheramento delle colonne — le regole di mascheramento si applicano nella Fase 2 della pipeline di governance.
5. Protezione dei predicati — le colonne mascherate vengono rifiutate nelle clausole WHERE e HAVING.

Le query ad hoc sulle tabelle remote sono consentite sui soli diritti dell'utente. L'accesso segue ovunque lo stesso modello basato sui diritti (diritti di tabella e di colonna più relazioni approvate), senza una modalità di governance per tabella.

**Governance delle relazioni (V002).** Una condizione JOIN su tabelle remote, in una query SQL o Cypher, deve corrispondere a una relazione registrata e approvata. Le query GraphQL saltano il controllo V002, perché le relazioni definite nell'SDL sono approvate per costruzione. Vedi [docs/security.md](security.md#relationship-governance-v002).

**Colonne di tipo OBJECT.** Quando una colonna corrisponde a un OBJECT GQL inline non governato o a un tipo oggetto OpenAPI, il suo tipo Provisa è `jsonb`. La colonna memorizza l'intero blob JSON annidato. Quando sono dichiarati sottocampi (`gql_object_fields` oppure `object_fields`), la mappa `gql_object_columns` viene popolata al momento della costruzione dello schema. Il generatore SQL utilizza questa mappa per emettere espressioni di estrazione `->>` per i sottocampi quando una query li seleziona.

**Argomenti obbligatori come parametri di filtro nativo.** I campi di query radice con argomenti non nulli e privi di valore predefinito iniettano colonne aggiuntive sulla tabella registrata. Queste colonne portano `native_filter_type: query_param`. Il traduttore Cypher riscrive `WHERE n.id = $val` in `WHERE n._nf_id = $val`, e l'executor GraphQL le recupera come variabili da passare all'endpoint remoto.

---

## Impatto della creazione di una relazione di copertura {: #impact-of-creating-a-covering-relationship }

Quando un data steward registra una relazione tra due tabelle remote (o tra una tabella remota e una tabella locale), tale relazione diventa il percorso di join utilizzato in fase di query.

**Come prevale il join.** Durante la compilazione della query, Provisa risolve il percorso di join attraverso la relazione registrata. `source_column` e `target_column` della relazione diventano la condizione di join nell'SQL generato. Il join sostituisce qualsiasi chiamata remota per tabella che altrimenti sarebbe necessaria per il tipo connesso.

**Il blob grezzo non è mai esposto in SQL.** La colonna `breed` di `petstore__pets` non si può selezionare come valore jsonb grezzo in una query SQL. Appena viene registrata una relazione tra `petstore__pets` e `petstore__breeds`, le query SQL percorrono il join: `SELECT breed.name FROM petstore__pets` si risolve tramite il join FK. Se non c'è una relazione registrata ma la colonna ha sottocampi dichiarati (`gql_object_fields`), i riferimenti SQL ai sottocampi vengono riscritti come estrazione `->>` dal blob salvato. Quel percorso esiste solo per i tipi inline non governati; i campi con destinazione governata mancano del tutto dall'SDL e non hanno un blob da cui estrarre. Il blob grezzo stesso non viene mai restituito come semplice valore di colonna.

Nell'SDL GraphQL, un campo OBJECT inline non governato ha come tipo il tipo di oggetto annidato. Che a runtime lo serva un join o un'estrazione dal blob non cambia la forma dell'SDL. Appena il tipo figlio viene registrato come tabella a sé, e quindi governato, gli si applicano indipendentemente tutti e cinque i livelli di governance: le proprie regole RLS, la visibilità delle colonne, le regole di mascheramento, i guard sui predicati e il controllo di accesso per dominio. L'estrazione dal blob aggira tutto questo, perché i dati figli arrivano incorporati nella riga padre e valgono solo le regole della tabella padre. Per governare nel dettaglio il tipo figlio, registratelo come tabella e create una relazione.

**`graphql_alias` sulla relazione.** Il campo `graphql_alias` denomina il campo SDL che la relazione espone sul tipo padre. In sua assenza, il nome viene derivato dal `field_name` della tabella di destinazione e dalla cardinalità della relazione tramite `rel_field_name(target.field_name, cardinality)`.

**V002 sul percorso di join.** Le query SQL e Cypher che attraversano la relazione sono soggette alla governance delle relazioni V002. La relazione deve essere registrata e approvata affinché il join sia consentito. L'attraversamento GraphQL tramite il campo di relazione della SDL è sempre preapprovato.

**Flag remote-managed.** Le relazioni rilevate automaticamente durante la registrazione di uno schema remoto GraphQL vengono memorizzate con `remote_managed: True`. Si tratta di un marcatore di metadati; non altera il comportamento di governance.

---

## Comportamento delle sole definizioni di tipo {: #type-def-only-behavior }

Non tutti i tipi in uno schema remoto devono essere una tabella interrogabile.

Quando `root_table_ids` è impostato su uno `SchemaInput`, le tabelle il cui ID è assente da tale insieme vengono escluse dai campi di query radice nella SDL generata. Rimangono presenti come tipi GraphQL e sono raggiungibili tramite campi di relazione su tabelle che dispongono di voci radice.

Lo stesso meccanismo vale quando lo schema viene costruito filtrato per dominio. Le tabelle dei domini a cui il ruolo non ha accesso sono solo definizioni di tipo: la loro definizione di tipo esiste nell'SDL per attraversare le relazioni, ma per loro non viene generato alcun campo di query radice.

Una tabella di sola definizione di tipo:

- Non ha alcun campo di query radice — i client non possono interrogarla direttamente per nome.
- È raggiungibile tramite campi di relazione su tabelle che dispongono di voci radice.
- Continua a comparire nell'introspezione dello schema come tipo con nome.
- Mantiene l'applicazione di tutte le regole di governance quando si accede ai dati tramite una relazione.

Una tabella, definizione di tipo inclusa, esce del tutto dallo schema solo quando la sua registrazione viene eliminata. Rendere una tabella solo definizione di tipo (togliendo il suo ID da `root_table_ids`, o tramite il filtro per accesso ai domini) non rimuove il tipo.

Così gli steward possono esporre grafi di oggetti navigabili in cui alcuni tipi si raggiungono solo attraversando e non si possono interrogare da soli.
