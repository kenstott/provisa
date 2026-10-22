# Comandi {: #commands }

Un comando è una funzione registrata e governata che porta il calcolo esterno sotto la governance, l'audit e la derivazione di Provisa. Il motore di federazione gestisce SQL in modo nativo. Un comando copre il calcolo che SQL non può esprimere, come un microservizio di arricchimento, un modello Python, uno script shell o una stored procedure nativa del database. Lo registrate una volta, e ogni superficie client (GraphQL, SQL via pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher) può invocarlo con una governance identica.

I comandi sono uno dei modi di scrivere la logica di business in Provisa. Vedi [La logica di business in Provisa](business-logic.md).

Un comando è un **RPC governato**. I suoi input e output sono dichiarati, tipizzati, validati, tracciati e collegati alla derivazione. Un passo ETL improvvisato, una chiamata curl non governata o un sottoprocesso non hanno nulla di tutto ciò.

## Tipi di implementazione {: #implementation-kinds }

Sono supportati sei valori `impl_kind`:

| `impl_kind` | Trasporto |
| --- | --- |
| `source_procedure` | Stored procedure nativa su un'origine registrata |
| `source_operation` | Un'operazione di scrittura di un'origine OpenAPI, GraphQL remota o gRPC, passata così com'è (vedi [Operazione di scrittura di un'origine remota](#a-remote-sources-write-operation)) |
| `script` | Subprocess locale alimentato con JSON su stdin, legge JSON da stdout |
| `http` | Endpoint HTTP/S; corpo richiesta JSON, risposta JSON |
| `grpc` | gRPC unary; bridge JSON senza proto |
| `python` | Callable Python in-process (`module:attr`) |

L'indirizzamento (il `name` del catalogo e `function_name`) è disaccoppiato dal `binding` (trasporto
e posizione). Sostituendo il binding, la governance, la derivazione e i contratti verso il chiamante
del comando rimangono invariati.

## Tipi di argomento {: #argument-kinds }

Ogni argomento dichiara un `arg_kind`:

| `arg_kind` | Comportamento |
| --- | --- |
| `column_value` | Scalare; passato direttamente nel payload della richiesta |
| `table_ref` | Lazy; Provisa passa il riferimento di relazione così com'è; il servizio recupera i dati |
| `result_set` | Eager; Provisa materializza la relazione referenziata e invia le sue righe |

I comandi `http` e `grpc` **devono** dichiarare almeno un argomento `table_ref` o `result_set`.
Un comando esterno che riceve solo argomenti scalari verrebbe invocato una volta per riga, il che
vanifica il batching. Il dispatcher rifiuta questa configurazione al momento della chiamata (422).

Un comando che restituisce un set (dichiarato tramite `output_columns` e `return_schema`) è una
funzione table-valued. Si usa in una clausola `FROM` o in un `JOIN`.

## Il contratto del dataset {: #the-dataset-contract }

Ogni argomento `table_ref` o `result_set` può dichiarare un **contratto di colonne di input**: un
elenco ordinato, tipizzato IR, di colonne in `FunctionArgument.columns`. Il comando stesso dichiara
un **contratto di colonne di output** in `Function.output_columns`.

Entrambi i contratti vengono validati fail-loud a ogni invocazione:

- **Input (solo result_set):** dopo la materializzazione, Provisa valida le righe contro le
  colonne dichiarate. Campi extra, campi mancanti e tipi errati sollevano tutti HTTP 422.

- **Output:** le righe restituite dal comando vengono validate contro `output_columns` prima di
  raggiungere il chiamante.
- **Proiezione ristretta:** quando è dichiarato un contratto di input, la query di
  materializzazione proietta **solo quelle colonne** (`SELECT "id", "region" FROM ...`) anziché
  `SELECT *`.

### Il vocabolario di tipi IR {: #the-ir-type-vocabulary }

I tipi di colonna dei contratti usano i nomi di tipo IR canonici:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Gli alias più comuni si risolvono automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` →
`json`, ecc.).

`return_schema` è la **proiezione GraphQL** di `output_columns`. La fonte di verità è `output_columns`, quindi dichiaratelo per la validazione e la derivazione, e aggiungete `return_schema` per generare i tipi GraphQL.

## Creare un comando {: #authoring-a-command }

### File di config {: #config-file }

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

La variante gRPC (`enrich_grpc_set`) segue lo stesso schema ma indica `impl_kind: grpc` e un
`binding` con le chiavi `target` e `method` anziché `callable`:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

### UI Admin {: #admin-ui }

Il modulo del comando in **Impostazioni → Comandi** include un editor delle colonne di input per
dataset (una riga per colonna dichiarata, con un selettore di tipo IR) e un editor delle colonne di
output. Salvando il modulo si registra o si aggiorna il comando senza ricaricare la configurazione.

## Composizione inline {: #inline-composition }

I comandi possono comparire **dentro** un'istruzione SQL più ampia, dove li unite in join, li usate in sottoquery o li proiettate; `SELECT * FROM fn(args)` è solo la forma più semplice. L'eccezione è l'operazione di scrittura di una sorgente remota, che si chiama da sola (vedi [Perché non può essere composta](#why-it-cannot-be-composed)).

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Prima che siano eseguite la governance, la validazione o l'instradamento, la pipeline rileva le
chiamate ai comandi registrati, esegue ciascuna attraverso l'esecutore governato condiviso (così il
contratto di I/O e il modello di identità si applicano esattamente come per una chiamata diretta) e
riscrive il punto di chiamata come una relazione locale tipizzata.

La sostituzione si adatta alla dimensione: fino a 1.000 righe il risultato viene inserito inline
come elenco `VALUES` tipizzato; oltre quella soglia viene registrato come relazione locale nominata
nel motore.

Un'istruzione localizzata viene instradata normalmente. Le query su una sola origine restano
sull'origine; solo le query realmente cross-origine passano al motore di federazione.

## Operazione di scrittura di un'origine remota {: #a-remote-sources-write-operation }

Un'origine OpenAPI, GraphQL remota o gRPC offre operazioni di scrittura. Registrarne una come comando la rende richiamabile, governata e sottoposta ad audit da ogni superficie. Aggiungere l'origine non ne registra nessuna; si registrano quelle desiderate, una alla volta, come si registrano le tabelle. La registrazione è la curation.

Cosa offre un'origine:

| Tipo di origine | Operazioni offerte | Nome dell'operazione |
| --- | --- | --- |
| `openapi` | Ogni operazione non GET della specifica | L'`operationId` |
| `graphql_remote` | Ogni campo del tipo `Mutation` remoto | Il nome del campo |
| `grpc_remote` | Ogni metodo classificato come mutazione | `Service.Method` |

### Registrarne una {: #register-one }

1. Aprire **Modello → Comandi** e aggiungere un comando.
2. Scegliere l'origine remota. Il modulo passa a un selettore di operazioni che elenca ciò che l'origine offre.
3. Scegliere l'operazione, un dominio e i ruoli che possono chiamarla.
4. Facoltativamente attivare **Richiede approvazione** e compilare il campo **Scrive nella tabella**.

Tramite l'API GraphQL di amministrazione, `availableFunctions(sourceId, schemaName)` elenca le operazioni. Il nome dello schema è `openapi`, `graphql` o `grpc_remote`, in base al tipo di origine.

Tutto il resto deriva dall'operazione, qualunque cosa invii il modulo. Provisa sovrascrive questi campi:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Ogni argomento è tipizzato `json`. Gli argomenti dell'operazione sono i suoi parametri di percorso OpenAPI (più `body` quando l'operazione accetta un corpo della richiesta), i suoi argomenti di mutazione GraphQL o i suoi campi di richiesta gRPC. Un'operazione che l'origine non offre viene rifiutata con 422, `functions.operation_not_offered`.

### Chiamarla {: #calling-it }

Provisa non dà forma, non tipizza e non controlla l'input. Ogni argomento va al servizio remoto invariato, con la credenziale dell'origine, e la risposta del servizio remoto torna invariata. Provisa governa chi può chiamare, in quale dominio, se serve l'approvazione, e registra la chiamata.

Come il servizio remoto riceve gli argomenti:

- **OpenAPI.** I parametri di percorso riempiono il modello del percorso. `body` è il corpo JSON della richiesta. Ogni altro argomento va nella query string.
- **GraphQL.** Gli argomenti vengono inviati come variabili tipizzate, ciascuna dichiarata con il tipo che le assegna lo schema remoto. La mutazione richiede indietro i campi scalari e di enumerazione della risposta, e quelli degli oggetti al suo interno fino a due livelli.
- **gRPC.** Gli argomenti diventano il messaggio di richiesta di `Service.Method`.

La risposta sono le righe del comando: un oggetto è una riga, una lista di oggetti sono le sue righe, qualsiasi altra cosa è una riga `{"result": ...}`.

In GraphQL il comando è un campo di mutazione e la sua risposta è lo scalare JSON.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

Sulle superfici SQL (pgwire e le altre che passano SQL) scrivere ogni argomento come letterale JSON in una stringa. `'{"title": "x"}'` è un oggetto, `'"text"'` una stringa, `'3'` un numero. Un letterale che non è JSON valido fallisce con 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

In REST, inviare con POST un oggetto JSON di argomenti a `/data/rest/{domain}/commands/{command}`. Un argomento `json` è documentato nella specifica generata come un valore qualsiasi.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Rifiuti {: #refusals }

Il rifiuto del servizio remoto torna come l'ha formulato il servizio.

| Servizio remoto | Provisa risponde |
| --- | --- |
| Rifiuta la chiamata (HTTP 4xx, `errors` di GraphQL, una chiamata gRPC rifiutata) | 422, `functions.remote_refused`, con `remote_status` e `answer` |
| Fallisce (HTTP 5xx) | 502, `functions.remote_refused` |

Se la credenziale dell'origine possa eseguire l'operazione lo decide il servizio remoto, quando l'operazione viene chiamata. Provisa non può provare una scrittura alla registrazione senza eseguirla.

### Approvazione {: #approval }

Attivando **Richiede approvazione**, ogni chiamata viene sottoposta all'hook di approvazione del deployment prima di essere eseguita. Viene eseguita solo se l'hook approva.

- Nessun hook configurato: 403, `functions.approval_unavailable`.
- L'hook nega: 403, `functions.approval_denied`, con il motivo dell'hook.

L'hook riceve il chiamante, il ruolo, il nome del comando e i suoi argomenti. Vedi [Hook di approvazione ABAC](security.md#abac-approval-hook). Il flag è memorizzato come `Function.requires_approval`, e il controllo si applica a qualsiasi comando che lo imposti.

### Scrive nella tabella {: #writes-table }

Indicare nel campo **Scrive nella tabella** la tabella in cui scrive l'operazione, come `schema.table`. Deve essere una tabella registrata dell'origine propria del comando, altrimenti il salvataggio viene rifiutato con 422, `actions.written_table_not_registered`. L'impostazione è facoltativa.

Dopo ogni chiamata che il servizio remoto accetta, Provisa la tratta come una scrittura su quella tabella. Scarta le risposte in cache della tabella, segna come obsolete le viste materializzate costruite su di essa, emette l'evento di modifica, esegue i comandi che vengono eseguiti quando cambia e ricarica la tabella quando è tenuta hot.

Quando la tabella viene letta dalla sua replica completa (le impostazioni dell'operatore la collocano lì, o il motore non può leggere la sua sorgente sul posto), dopo la chiamata viene richiesta una costruzione della replica, con il motivo `write`. I lettori mantengono la vecchia replica finché la nuova non la sostituisce. Una tabella replicata riga per riga, o con una colonna parametro, non ha una replica completa da ricostruire.

### Perché non può essere composta {: #why-it-cannot-be-composed }

Un'operazione di scrittura è un'azione. Non trasforma dati, e una vista o vista materializzata che ne contenesse una eseguirebbe la scrittura a ogni lettura o aggiornamento. Per questo la chiamata sta da sola. `SELECT * FROM create_issue(...)` da sola la esegue, e la stessa chiamata dentro un'istruzione più ampia viene rifiutata ovunque si trovi, in un join, in una sottoquery o in una proiezione.

Una vista o vista materializzata la cui definizione ne chiama una viene rifiutata al salvataggio. Anche una definizione che non si riesce a interpretare viene rifiutata finché è registrata una qualsiasi operazione di scrittura, perché non si può dimostrare che non ne chiami nessuna.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

Neanche un'operazione di sorgente è un nodo di derivazione. La derivazione si legge dall'SQL di viste e query, dove un comando compare come nodo, e nessuna definizione salvata può chiamare un'operazione di sorgente.

## Comandi e derivazione {: #commands-and-lineage }

Poiché ogni comando dichiara le proprie colonne di input e di output, la derivazione a livello di
colonna **si chiude attraverso il confine opaco del comando**. Il motore di derivazione applica una
chiusura per contaminazione: ogni colonna di output dichiarata deriva da ogni colonna di input
dichiarata.

**La conseguenza pratica:** l'ampiezza del vostro contratto di input fissa la precisione di quella chiusura. Un input stretto, con solo le colonne che servono al comando, produce un cono di derivazione compatto e leggibile. Dichiarare ogni colonna della relazione sorgente allarga il ventaglio su ogni output. Resta corretto, perché nessuna derivazione va persa, ma offusca la tracciabilità.

**Regola pratica:** passate la proiezione minima che serve al comando e restituite solo colonne derivate. Gli input restituiti invariati allargano il cono di contaminazione, quindi lasciateli fuori.

Vedere [Derivazione](lineage.md) per come i nodi comando appaiono nel DAG e come leggerli.

## Allowlist di egress {: #egress-allowlist }

I comandi `http` e `grpc` chiamano endpoint esterni. Ogni host di destinazione deve comparire nella `udf_egress_allowlist` del deployment. Il loopback (`localhost`, `127.0.0.1`, `::1`) è sempre consentito. Senza allowlist, ogni uscita esterna viene negata con HTTP 403, e non c'è alcun default silenzioso.

## Tracciamento delle invocazioni {: #invocation-tracing }

Ogni invocazione emette una traccia, qualunque sia l'esito. La traccia contiene il nome del comando, il tipo di trasporto, il modello di identità (DEFINER o INVOKER), i riferimenti alle relazioni di input, l'id del ruolo e la cardinalità dell'output. La emette il dispatcher, quindi nessun `impl_kind` può saltarla.

## CLI: provisa metadata export

`provisa metadata export` è un job del livello shell e gira fuori dal percorso RPC governato. Avvia la pubblicazione su richiesta dei metadati del server in esecuzione inviando a `/admin/metadata-export/publish`, l'endpoint che chiama il pulsante **Pubblica ora** della scheda Admin.

Si usa per pilotare esportazioni programmate da cron o CI quando la pianificazione
`reconcile_cron` configurata non è abbastanza granulare:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Uscita 0 = pubblicazione completa. Uscita 1 = pubblicazione parziale o errore di connessione.

Per il riferimento completo dei flag, le opzioni di autenticazione, la denominazione degli host in
multitenancy e un esempio di cron, vedere
[Esportazione dei metadati — Dalla riga di comando](metadata-export.md#from-the-command-line).

I comandi compaiono nella proiezione git di ogni ambiente. Vedere [Ambienti](environments.md) per
come un comando e le sue assegnazioni di tag sopravvivono a un merge e a un pull.
