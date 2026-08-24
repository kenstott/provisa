# Derivazione a livello di colonna

Provisa traccia la derivazione dei dati a livello di colonna in modo statico — calcolata dalle
definizioni SQL e dai contratti dei comandi, senza bisogno di alcuna esecuzione. Sono disponibili
due viste: un DAG per singola istruzione e un grafo di provenienza esteso a tutta la federazione,
che copre tutte le viste e le viste materializzate (MV) registrate.

## L'esploratore della derivazione

Aprire **Lineage** nella UI (`/lineage`). Incollare un'istruzione SQL e fare clic su **Build
statement graph** per vederne il DAG a livello di colonna. Fare clic su **Federation graph** per
caricare il grafo di provenienza su ogni MV del registro. [tool-verified: LineagePage.tsx:28-119]

## DAG a livello di istruzione (REQ-1160)

Ogni colonna di output nominata nel SQL diventa un nodo. Il costruttore la ripercorre a ritroso
attraverso ogni CTE, sotto-query, join e chiamata inline a comando fino alle sue colonne di origine,
costruendo un grafo orientato dagli input di origine agli output finali.

### Esempio svolto

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Questa istruzione produce tre colonne di output. Il grafo di `geo_u` si presenta così:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` e `orders.geo` sono nodi **source** (il contratto di input ristretto
  di `enrich_grpc_set` dichiara `id` e `region`; la chiusura per contaminazione completa collega
  tutti gli input dichiarati a tutti gli output). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` ed `e.geo` sono nodi **command** — il confine di `enrich_grpc_set`.
- `geo_u` è un nodo **derived** prodotto dalla funzione SQL `UPPER`.

Il confine del comando **non è opaco**. Poiché `enrich_grpc_set` dichiara le proprie colonne di
input (`id`, `region`) e di output (`id`, `embedding`, `geo`), il motore di derivazione salda la
chiusura per contaminazione senza interruzioni, dalle colonne dichiarate della relazione di origine
fino a ciascun output. [tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Tipi di nodo e segnali visivi

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Tipo di nodo | Colore | Significato |
| --- | --- | --- |
| `source` | Verde | Una colonna di tabella di base |
| `derived` | Blu | Prodotta da un'espressione SQL (funzione, operatore, CTE) |
| `command` | Viola | Una colonna di output di un comando registrato |

Anelli aggiuntivi su un nodo:

- **Anello arancione** — una colonna di output finale dell'istruzione.
- **Bordo doppio** — la relazione della colonna è una vista materializzata (snapshot MV/CTAS).
- **Anello rosso** — membro di un ciclo classificato come errore.
- **Anello giallo** — membro di un ciclo classificato come anello di retroazione.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Trasformazioni nominate sugli archi

Ogni arco porta con sé l'espressione SQL grezza che produce la colonna di destinazione, più un
elenco di operazioni nominate: funzioni SQL (`sql_function`), operatori aritmetici e logici
(`operator`), comandi registrati (`command`), riferimenti diretti a colonna (`identity`) e letterali
(`constant`). [tool-verified: TransformOp and name_transform in graph.py:36-145]

Un arco che nasce da una chiamata a comando viene disegnato nella UI come una linea viola
tratteggiata. [tool-verified: LineageDag.tsx:122-124]

## Grafo esteso alla federazione (REQ-1161)

Il grafo di federazione unisce la derivazione per istruzione di ogni MV registrata in un unico grafo
di provenienza. L'identità di un nodo è `relation.column` — la colonna di output di una vista e il
riferimento in input di un'altra vista alla stessa colonna collassano in un unico nodo. Il risultato
è un solo DAG che va dalle colonne di origine di base a ogni dataset derivato della piattaforma.
[tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Usare `focus`, `direction` e `depth` per delimitare la vista su scala di federazione senza
ricalcolare il grafo. [tool-verified: `slice_graph` in merge.py:160-189]

## Cicli (REQ-1161)

I cicli vengono descritti, non rifiutati. Il motore di derivazione rileva ogni ciclo orientato e lo
**classifica**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classificazione | Colore del bordo | Significato |
| --- | --- | --- |
| `feedback` | Giallo | Il ciclo attraversa un nodo materializzato — un anello di retroazione legittimo e sfasato nel tempo. Lo snapshot della MV è il confine di versione che lo rende ben definito. |
| `error` | Rosso | Nessun confine di materializzazione sull'anello — una definizione circolare senza un ordine di valutazione stabile. Probabilmente un errore di progettazione. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Un ciclo `feedback` non è un guasto. Una MV di arricchimento che riporta una colonna derivata nella
propria relazione di origine è uno schema valido finché almeno un nodo dell'anello è materializzato
— lo snapshot isola temporalmente le due metà. Un ciclo `error` richiede il giudizio di chi opera:
di solito significa che due viste si riferiscono l'una all'altra senza alcuno snapshot in mezzo.

## API

Entrambi gli endpoint sono **statici** — leggono definizioni e contratti, non dati.

### POST /admin/lineage/graph

Restituisce il DAG a livello di colonna di una singola istruzione SQL.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

Forma della risposta [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

Restituisce HTTP 422 quando il SQL non può essere analizzato.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Restituisce il grafo di provenienza unito su tutte le MV del registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Parametri di query [tool-verified: function signature at lineage_router.py:73-76]:

| Parametro | Valori | Default | Effetto |
| --- | --- | --- | --- |
| `focus` | Un id di nodo | — | Delimita la risposta al sotto-grafo attorno a questo nodo |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | In quale direzione percorrere il grafo a partire da `focus` |
| `depth` | intero | illimitato | Distanza massima in salti da `focus` |

La risposta ha la stessa forma del grafo di istruzione, con l'aggiunta di un campo `cycles`
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## Che cosa romperebbe la ridenominazione o la rimozione di una colonna (REQ-1484)

Una colonna porta due nomi, e ciascuno è memorizzato da un insieme diverso di artefatti.

Il **nome esposto** è quello che mostrano le superfici SQL e GraphQL: `table_columns.alias`, con
ripiego sul valore predefinito in snake_case quando non è impostato alcun alias [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]. Viste, viste materializzate, espressioni di metrica, predicati RLS,
contratti DQ, granularità delle viste di metrica e chiavi di riga delle MV sono tutti scritti su
quel nome, quindi **rinominare un alias li rompe con la stessa certezza con cui li romperebbe
eliminare la colonna**.

Il **nome fisico** è `table_columns.column_name`, l'identità che sopravvive alla sostituzione in
blocco delle colonne durante l'upsert della tabella. Relazioni, collegamenti del
[glossario](glossary.md), assegnazioni di tag, la colonna di watermark e i preset di colonna
memorizzano questo, quindi si rompono solo quando la colonna viene **rimossa**.

`columnDependents` riporta entrambi. Le viste e le MV a valle provengono dal ritaglio del grafo di
federazione sul nome esposto della colonna; gli artefatti che quel grafo non copre provengono da una
scansione diretta del registro [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` vale `rename` per un riferimento al nome esposto e `remove` per uno al nome fisico, così
chi chiama può capire a quale metà della modifica sta reagendo ciascun artefatto.

Questa domanda va posta **prima** del salvataggio. Una colonna rinominata viene individuata tramite
il nome esposto che porta ancora nel registro; una volta che l'alias è stato applicato, il vecchio
nome non c'è più e la query non trova nulla.

La pagina Tabelle esegue la query automaticamente quando una modifica in sospeso cambia un alias o
riduce l'insieme delle colonne, ed elenca ciò che trova [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. L'avviso è consultivo:
nomina gli artefatti interessati e la decisione spetta all'amministratore. Non blocca il
salvataggio, perché non è possibile raggiungere tutti i consumatori del patrimonio informativo — una
dashboard esterna o un'applicazione client che interroga la colonna per nome è fuori dalla
conoscenza del registro. Per lo stesso motivo, le scansioni sul testo SQL libero riconoscono la
colonna come token identificatore anziché risolverne l'ambito, e questo può nominare un artefatto
che poi non usa affatto la colonna. Per un avviso, l'eccesso di segnalazioni è la direzione sicura.

## Usare la derivazione per governare i contratti dei comandi

Poiché la chiusura per contaminazione collega ogni colonna di input dichiarata a ogni colonna di
output dichiarata, l'ampiezza di quella chiusura dipende interamente da ciò che si dichiara.

Si consideri un comando che prende un'intera tabella orders (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) e restituisce un `embedding`. Se il contratto di input
elenca tutte quelle colonne, ogni colonna a valle che usa l'embedding mostrerà una derivazione da
tutte quante. È accurato ma non utile — diventa difficile capire che cosa abbia contato davvero.

Dichiarando solo `id` e `text` (le colonne che il modello di embedding legge realmente), il cono di
derivazione si restringe a quelle due colonne di origine. La derivazione risulta insieme corretta e
precisa.

Vedere [Comandi](commands.md) per la meccanica della dichiarazione di un contratto di input
ristretto.
