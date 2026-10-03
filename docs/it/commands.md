# Comandi

Un comando è una funzione registrata e governata che porta il calcolo esterno sotto il sistema di
governance, audit e derivazione di Provisa. Dove il motore di federazione gestisce SQL nativamente,
un comando è la giunzione per il calcolo che non può esprimere: un microservizio di arricchimento,
un modello Python, uno script shell, una stored procedure nativa del database. Lo si registra una
volta; ogni superficie client — GraphQL, SQL pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher — può
invocarlo con una governance identica (REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

La distinzione chiave: un comando è un **RPC governato**, non un ETL ad-hoc. I suoi input e output
sono dichiarati, tipizzati, validati, tracciati e collegati alla derivazione. Una chiamata curl non
governata o un subprocess non sono niente di tutto questo.

## Tipi di implementazione

Sono supportati sei valori `impl_kind` [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]:

| `impl_kind` | Trasporto |
| --- | --- |
| `source_procedure` | Stored procedure nativa su un'origine registrata |
| `source_operation` | Un'operazione di scrittura di un'origine OpenAPI, GraphQL remota o gRPC, passata così com'è (vedi [Operazione di scrittura di un'origine remota](#operazione-di-scrittura-di-unorigine-remota-req-1924)) |
| `script` | Subprocess locale alimentato con JSON su stdin, legge JSON da stdout |
| `http` | Endpoint HTTP/S; corpo richiesta JSON, risposta JSON |
| `grpc` | gRPC unary; bridge JSON senza proto |
| `python` | Callable Python in-process (`module:attr`) |

L'indirizzamento (il `name` del catalogo e `function_name`) è disaccoppiato dal `binding` (trasporto
e posizione). Sostituendo il binding, la governance, la derivazione e i contratti verso il chiamante
del comando rimangono invariati. [tool-verified: Function model in models.py:710-750]

## Tipi di argomento

Ogni argomento dichiara un `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Comportamento |
| --- | --- |
| `column_value` | Scalare; passato direttamente nel payload della richiesta |
| `table_ref` | Lazy; Provisa passa il riferimento di relazione così com'è; il servizio recupera i dati |
| `result_set` | Eager; Provisa materializza la relazione referenziata e invia le sue righe |

I comandi `http` e `grpc` **devono** dichiarare almeno un argomento `table_ref` o `result_set`.
Un comando esterno che riceve solo argomenti scalari verrebbe invocato una volta per riga, il che
vanifica il batching. Il dispatcher rifiuta questa configurazione al momento della chiamata (422).
[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

Un comando che restituisce un set (dichiarato tramite `output_columns` e `return_schema`) è una
funzione table-valued. Si usa in una clausola `FROM` o in un `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## Il contratto del dataset (REQ-1159)

Ogni argomento `table_ref` o `result_set` può dichiarare un **contratto di colonne di input**: un
elenco ordinato, tipizzato IR, di colonne in `FunctionArgument.columns`. Il comando stesso dichiara
un **contratto di colonne di output** in `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Entrambi i contratti vengono validati fail-loud a ogni invocazione:

- **Input (solo result_set):** dopo la materializzazione, Provisa valida le righe contro le
  colonne dichiarate. Campi extra, campi mancanti e tipi errati sollevano tutti HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Output:** le righe restituite dal comando vengono validate contro `output_columns` prima di
  raggiungere il chiamante. [tool-verified: function_dispatch.py:488-490]
- **Proiezione ristretta:** quando è dichiarato un contratto di input, la query di
  materializzazione proietta **solo quelle colonne** (`SELECT "id", "region" FROM ...`) anziché
  `SELECT *`. [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### Il vocabolario di tipi IR

I tipi delle colonne di contratto usano il sistema di tipi IR canonico (REQ-846), non gli scalari
GraphQL né le grafie native delle origini. I nomi validi sono [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Gli alias più comuni si risolvono automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` →
`json`, ecc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` è la **proiezione GraphQL** di `output_columns`, non la fonte di verità. Dichiarare
`output_columns` per la validazione e la derivazione; aggiungere `return_schema` per la generazione
dei tipi GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## Creare un comando

### File di config

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

[tool-verified: sample_config.yaml enrich_orders block]

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

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### UI Admin

Il modulo del comando in **Impostazioni → Comandi** include un editor delle colonne di input per
dataset (una riga per colonna dichiarata, con un selettore di tipo IR) e un editor delle colonne di
output. Salvando il modulo si registra o si aggiorna il comando senza ricaricare la configurazione.
[inferred from CommandFormFields.tsx]

## Composizione inline (REQ-1159)

I comandi possono comparire **dentro** un'istruzione SQL più ampia — in join, in sotto-query o in
proiezione. Non ci si limita a `SELECT * FROM fn(args)`. L'eccezione è l'operazione di scrittura di un'origine remota, che viene chiamata da sola (vedi [Perché non può essere composta](#perche-non-puo-essere-composta)).

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
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

La sostituzione si adatta alla dimensione: fino a 1.000 righe il risultato viene inserito inline
come elenco `VALUES` tipizzato; oltre quella soglia viene registrato come relazione locale nominata
nel motore. [tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Un'istruzione localizzata viene instradata normalmente. Le query su una sola origine restano
sull'origine; solo le query realmente cross-origine passano al motore di federazione.
[tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Operazione di scrittura di un'origine remota (REQ-1924)

Un'origine OpenAPI, GraphQL remota o gRPC offre operazioni di scrittura. Registrarne una come comando la rende richiamabile, governata e sottoposta ad audit da ogni superficie. Aggiungere l'origine non ne registra nessuna; si registrano quelle desiderate, una alla volta, come si registrano le tabelle. La registrazione è la curation. [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

Cosa offre un'origine [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]:

| Tipo di origine | Operazioni offerte | Nome dell'operazione |
| --- | --- | --- |
| `openapi` | Ogni operazione non GET della specifica | L'`operationId` |
| `graphql_remote` | Ogni campo del tipo `Mutation` remoto | Il nome del campo |
| `grpc_remote` | Ogni metodo classificato come mutazione | `Service.Method` |

### Registrarne una

1. Aprire **Modello → Comandi** e aggiungere un comando.
2. Scegliere l'origine remota. Il modulo passa a un selettore di operazioni che elenca ciò che l'origine offre.
3. Scegliere l'operazione, un dominio e i ruoli che possono chiamarla.
4. Facoltativamente attivare **Richiede approvazione** e compilare il campo **Scrive nella tabella**.

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

Tramite l'API GraphQL di amministrazione, `availableFunctions(sourceId, schemaName)` elenca le operazioni. Il nome dello schema è `openapi`, `graphql` o `grpc_remote`, in base al tipo di origine. [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

Tutto il resto deriva dall'operazione, non da ciò che il modulo invia. `_as_source_operation` sovrascrive questi campi:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

Ogni argomento è tipizzato `json`. Gli argomenti dell'operazione sono i suoi parametri di percorso OpenAPI (più `body` quando l'operazione accetta un corpo della richiesta), i suoi argomenti di mutazione GraphQL o i suoi campi di richiesta gRPC. [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] Un'operazione che l'origine non offre viene rifiutata con 422, `functions.operation_not_offered`. [tool-verified: `offered_operation`]

### Chiamarla

Provisa non dà forma, non tipizza e non controlla l'input. Ogni argomento va al servizio remoto invariato, con la credenziale dell'origine, e la risposta del servizio remoto torna invariata. Provisa governa chi può chiamare, in quale dominio, se serve l'approvazione, e registra la chiamata. [tool-verified: `source_operation.py` module docstring]

Come il servizio remoto riceve gli argomenti:

- **OpenAPI.** I parametri di percorso riempiono il modello del percorso. `body` è il corpo JSON della richiesta. Ogni altro argomento va nella query string. [tool-verified: `_call_openapi`]
- **GraphQL.** Gli argomenti vengono inviati come variabili tipizzate, ciascuna dichiarata con il tipo che le assegna lo schema remoto. La mutazione richiede indietro i campi scalari e di enumerazione della risposta, e quelli degli oggetti al suo interno fino a due livelli. [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** Gli argomenti diventano il messaggio di richiesta di `Service.Method`. [tool-verified: `_call_grpc`]

La risposta sono le righe del comando: un oggetto è una riga, una lista di oggetti sono le sue righe, qualsiasi altra cosa è una riga `{"result": ...}`. [tool-verified: `_rows`]

In GraphQL il comando è un campo di mutazione e la sua risposta è lo scalare JSON. [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

Sulle superfici SQL (pgwire e le altre che passano SQL) scrivere ogni argomento come letterale JSON in una stringa. `'{"title": "x"}'` è un oggetto, `'"text"'` una stringa, `'3'` un numero. Un letterale che non è JSON valido fallisce con 422, `functions.json_argument_invalid`. [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

In REST, inviare con POST un oggetto JSON di argomenti a `/data/rest/{domain}/commands/{command}`. Un argomento `json` è documentato nella specifica generata come un valore qualsiasi. [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### Rifiuti

Il rifiuto del servizio remoto torna come l'ha formulato il servizio. [tool-verified: `_refused` in `source_operation.py`]

| Servizio remoto | Provisa risponde |
| --- | --- |
| Rifiuta la chiamata (HTTP 4xx, `errors` di GraphQL, una chiamata gRPC rifiutata) | 422, `functions.remote_refused`, con `remote_status` e `answer` |
| Fallisce (HTTP 5xx) | 502, `functions.remote_refused` |

Se la credenziale dell'origine possa eseguire l'operazione lo decide il servizio remoto, quando l'operazione viene chiamata. Provisa non può provare una scrittura alla registrazione senza eseguirla. [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### Approvazione

Attivando **Richiede approvazione**, ogni chiamata viene sottoposta all'hook di approvazione del deployment prima di essere eseguita. Viene eseguita solo se l'hook approva. [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- Nessun hook configurato: 403, `functions.approval_unavailable`.
- L'hook nega: 403, `functions.approval_denied`, con il motivo dell'hook.

L'hook riceve il chiamante, il ruolo, il nome del comando e i suoi argomenti. Vedi [Hook di approvazione ABAC](security.md#hook-di-approvazione-abac). Il flag è memorizzato come `Function.requires_approval`, e il controllo si applica a qualsiasi comando che lo imposti. [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### Scrive nella tabella

Indicare nel campo **Scrive nella tabella** la tabella in cui scrive l'operazione, come `schema.table`. Deve essere una tabella registrata dell'origine propria del comando, altrimenti il salvataggio viene rifiutato con 422, `actions.written_table_not_registered`. L'impostazione è facoltativa. [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

Dopo ogni chiamata che il servizio remoto accetta, Provisa la tratta come una scrittura su quella tabella. Scarta le risposte in cache della tabella, segna come obsolete le viste materializzate costruite su di essa, emette l'evento di modifica, esegue i sink della tabella e ricarica la tabella quando è tenuta hot. [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

Le repliche non vengono aggiornate dalla chiamata. Ciò attende un modo per richiedere l'aggiornamento di una replica, che non è costruito; fino ad allora una replica si aggiorna secondo la propria pianificazione. [tool-verified: REQ-1924 WRITTEN TABLE amendment; no replica call in `after_table_written`]

### Perché non può essere composta

Un'operazione di scrittura è un'azione, non una trasformazione di dati. Una vista o vista materializzata che ne contenesse una eseguirebbe la scrittura a ogni lettura o aggiornamento. La chiamata sta quindi da sola: `SELECT * FROM create_issue(...)` da sola la esegue, e la stessa chiamata dentro un'istruzione più grande viene rifiutata, ovunque si trovi -- in un join, in una sottoquery o in una proiezione. [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

Una vista o vista materializzata la cui definizione ne chiama una viene rifiutata al salvataggio. Anche una definizione che non si riesce a interpretare viene rifiutata finché è registrata una qualsiasi operazione di scrittura, perché non si può dimostrare che non ne chiami nessuna. [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

Un'operazione di origine non è nemmeno un nodo di derivazione: la derivazione si legge dal SQL di viste e query, dove un comando compare come nodo, e nessuna definizione salvata può chiamare un'operazione di origine. [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## Comandi e derivazione

Poiché ogni comando dichiara le proprie colonne di input e di output, la derivazione a livello di
colonna **si chiude attraverso il confine opaco del comando**. Il motore di derivazione applica una
chiusura per contaminazione: ogni colonna di output dichiarata deriva da ogni colonna di input
dichiarata. [tool-verified: `_splice_commands` in graph.py:223-242]

**La conseguenza pratica:** l'ampiezza del contratto di input determina la precisione di quella
chiusura. Un input ristretto — solo le colonne di cui il comando ha davvero bisogno — produce un
cono di derivazione stretto e leggibile. Dichiarare ogni colonna della relazione di origine allarga
la convergenza su ogni output: resta corretto (nessuna derivazione va perduta) ma offusca la
tracciabilità.

**Regola pratica:** passare la proiezione minima di cui il comando ha bisogno e restituire solo
colonne derivate (non input rimandati indietro invariati). Così il cono di contaminazione resta
accurato. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

Vedere [Derivazione](lineage.md) per come i nodi comando appaiono nel DAG e come leggerli.

## Allowlist di egress

I comandi `http` e `grpc` chiamano endpoint esterni. Ogni host di destinazione deve comparire nella
`udf_egress_allowlist` del deployment. Il loopback (`localhost`, `127.0.0.1`, `::1`) è sempre
consentito. Un'allowlist assente nega tutto l'egress esterno con HTTP 403 — non esiste un valore
predefinito silenzioso. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Tracciamento delle invocazioni (REQ-886)

Ogni invocazione emette una traccia, quale che sia l'esito. La traccia include il nome del comando,
il tipo di trasporto, il modello di identità (DEFINER o INVOKER), i riferimenti alle relazioni di
input, l'id del ruolo e la cardinalità dell'output. La traccia viene emessa dal dispatcher — nessun
`impl_kind` può aggirarla. [tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` è un job di livello shell, non un RPC governato. Attiva la pubblicazione
su richiesta dei metadati del server in esecuzione (REQ-1072/REQ-1074) inviando una POST a
`/admin/metadata-export/publish` — lo stesso endpoint chiamato dal pulsante **Pubblica ora** della
scheda Admin. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

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
