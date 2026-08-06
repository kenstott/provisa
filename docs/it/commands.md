# Comandi

Un comando è una funzione registrata e governata che porta il calcolo esterno sotto il sistema di
governance, audit, e lineage di Provisa. Dove il motore di federazione gestisce SQL nativamente, un
comando è la giunzione per il calcolo che non può esprimere: un microservizio di enrichment, un
modello Python, uno script shell, una stored procedure nativa del database. Registralo una volta;
ogni superficie client — GraphQL, SQL pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher — può invocarlo
con governance identica (REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

Distinzione chiave: un comando è un **RPC governato**, non ETL ad-hoc. I suoi input e output sono
dichiarati, tipizzati, validati, tracciati, e collegati al lineage. Una chiamata curl non governata
o un subprocess non sono niente di tutto questo.

## Tipi di implementazione

Sono supportati cinque valori `impl_kind` [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | Trasporto |
| --- | --- |
| `source_procedure` | Stored procedure nativa su un'origine registrata |
| `script` | Subprocess locale alimentato con JSON su stdin, legge JSON da stdout |
| `http` | Endpoint HTTP/S; corpo richiesta JSON, risposta JSON |
| `grpc` | gRPC unary; bridge JSON senza proto |
| `python` | Callable Python in-process (`module:attr`) |

L'indirizzamento (il `name` del catalogo e `function_name`) è disaccoppiato dal `binding` (trasporto
e posizione). Scambia il binding e la governance, il lineage, e i contratti caller del comando
rimangono invariati. [tool-verified: Function model in models.py:710-750]

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

Un comando che restituisce un set (dichiarato via `output_columns` e `return_schema`) è una
funzione table-valued. Usalo in una clausola `FROM` o in un `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## Il contratto del dataset (REQ-1159)

Ogni argomento `table_ref` o `result_set` può dichiarare un **contratto di colonne di input**: un
elenco ordinato, tipizzato IR, di colonne in `FunctionArgument.columns`. Il comando stesso dichiara
un **contratto di colonne di output** in `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Entrambi i contratti vengono validati fail-loud a ogni invocazione:

- **Input (solo result_set):** dopo la materializzazione, Provisa valida le righe contro le
  colonne dichiarate. Campi extra, campi mancanti, e tipi errati sollevano tutti HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Output:** le righe restituite dal comando vengono validate contro `output_columns` prima di
  raggiungere il chiamante. [tool-verified: function_dispatch.py:488-490]
- **Proiezione ristretta:** quando è dichiarato un contratto di input, la query di materializzazione
  proietta **solo quelle colonne** (`SELECT "id", "region" FROM ...`) invece di `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### Il vocabolario di tipi IR

I tipi di colonna del contratto usano il sistema di tipi IR canonico (REQ-846), non gli scalari
GraphQL o le grafie native dell'origine. I nomi validi sono [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Gli alias comuni si risolvono automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
ecc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` è la **proiezione GraphQL** di `output_columns`, non la fonte di verità.
Dichiara `output_columns` per validazione e lineage; aggiungi `return_schema` per la generazione
del tipo GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

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

La variante gRPC (`enrich_grpc_set`) segue lo stesso pattern ma specifica `impl_kind: grpc`
e un `binding` con le chiavi `target` e `method` invece di `callable`:

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

Il form comando in **Impostazioni → Comandi** include un editor per-dataset delle colonne di input
(una riga per colonna dichiarata, con un selettore di tipo IR) e un editor delle colonne di output.
Salva il form per registrare o aggiornare il comando senza un reload della config. [inferred from CommandFormFields.tsx]

## Composizione inline (REQ-1159)

I comandi possono apparire **dentro** un'istruzione SQL più ampia — uniti, in sotto-query, o
proiettati. Non sei limitato a `SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Prima che governance, validazione, o routing vengano eseguiti, la pipeline rileva le chiamate a
comandi registrati, esegue ciascuna attraverso l'executor governato condiviso (così che il
contratto I/O e il modello di identità si applichino esattamente come per una chiamata diretta), e
riscrive il call site in una relazione locale tipizzata.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

La sostituzione è adattiva alla dimensione: fino a 1.000 righe il risultato viene incorporato
inline come lista `VALUES` tipizzata; sopra quella soglia viene registrato come relazione locale
nominata nel motore.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Un'istruzione localizzata viene instradata normalmente. Le query a singola origine restano
sull'origine; solo le query genuinamente cross-source vanno al motore di federazione. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Comandi e lineage

Poiché ogni comando dichiara le proprie colonne di input e output, il lineage a livello di colonna
**si chiude attraverso il confine opaco del comando**. Il motore di lineage applica una chiusura
di taint: ogni colonna di output dichiarata deriva da ogni colonna di input dichiarata. [tool-verified: `_splice_commands` in graph.py:223-242]

**La conseguenza pratica:** l'ampiezza del tuo contratto di input determina la precisione di quella
chiusura. Un input ristretto — solo le colonne di cui il comando ha effettivamente bisogno —
produce un cono di lineage stretto e leggibile. Dichiarare ogni colonna della relazione sorgente
si propaga ampiamente su ogni output, il che resta comunque corretto (nessun lineage viene perso)
ma offusca la tracciabilità.

**Regola pratica:** passa la proiezione minima di cui il comando ha bisogno, e restituisci solo
colonne derivate (non input semplicemente ripassati invariati). Questo mantiene il cono di taint
accurato. [inferred from _splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

Vedi [Lineage](lineage.md) per come i nodi comando appaiono nel DAG e come leggerli.

## Allowlist di egress

I comandi `http` e `grpc` chiamano endpoint esterni. Ogni host target deve comparire nella
`udf_egress_allowlist` del deployment. Il loopback (`localhost`, `127.0.0.1`, `::1`) è sempre
permesso. Un'allowlist assente nega tutto l'egress esterno con HTTP 403 — non c'è alcun default
silenzioso. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Tracciamento delle invocazioni (REQ-886)

Ogni invocazione emette una trace indipendentemente dall'esito. La trace include il nome del
comando, il tipo di trasporto, il modello di identità (DEFINER o INVOKER), i riferimenti alla
relazione di input, l'id del ruolo, e la cardinalità dell'output. Il dispatcher emette la trace —
nessun `impl_kind` può bypassarla.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` è un job di livello shell, non una RPC governata. Attiva la pubblicazione
on-demand dei metadati del server in esecuzione (REQ-1072/REQ-1074) inviando una POST a
`/admin/metadata-export/publish` — lo stesso endpoint richiamato dal pulsante **Pubblica ora** della
scheda di amministrazione. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Usalo per pilotare esportazioni pianificate da cron o CI quando la pianificazione configurata in
`reconcile_cron` non è abbastanza granulare:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Uscita 0 = pubblicazione completa. Uscita 1 = pubblicazione parziale o errore di connessione.

Per il riferimento completo dei flag, le opzioni di autenticazione, la denominazione degli host in
multitenancy e un esempio di cron, vedi [Esportazione dei metadati — Dalla riga di comando](metadata-export.md#from-the-command-line).
