# Derivazione a livello di colonna

Provisa tiene traccia della derivazione dei dati a livello di colonna in modo statico — calcolata a
partire dalle definizioni SQL e dai contratti di comando, senza necessità di esecuzione. Sono
disponibili due viste: un DAG per singola istruzione e un grafo di provenienza a livello di
federazione che copre tutte le viste e le viste materializzate (MV) registrate.

## L'esploratore di derivazione

Accedere a **Lineage** nell'interfaccia utente (`/lineage`). Incollare un'istruzione SQL e fare clic
su **Build statement graph** per visualizzarne il DAG a livello di colonna. Fare clic su
**Federation graph** per caricare il grafo di provenienza su ogni MV nel registro.
[tool-verified: LineagePage.tsx:28-119]

## DAG a livello di istruzione (REQ-1160)

Ogni colonna di output denominata nel proprio SQL diventa un nodo. Il generatore la ripercorre
attraverso ogni CTE, sottoquery, join e chiamata di comando inline fino alle colonne di origine,
costruendo un grafo diretto dagli input di origine agli output finali.

### Esempio svolto

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Questa istruzione produce tre colonne di output. Il grafo per `geo_u` è il seguente:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` e `orders.geo` sono nodi **source** (il contratto di input ristretto
  di `enrich_grpc_set` dichiara `id` e `region`; la taint closure completa collega tutti gli input
  dichiarati a tutti gli output). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` ed `e.geo` sono nodi **command** — il confine di `enrich_grpc_set`.
- `geo_u` è un nodo **derived** prodotto dalla funzione SQL `UPPER`.

Il confine del comando **non è opaco**. Poiché `enrich_grpc_set` dichiara le proprie colonne di
input (`id`, `region`) e di output (`id`, `embedding`, `geo`), il motore di derivazione collega la
taint closure in modo continuo dalle colonne dichiarate della relazione di origine fino a ciascun
output. [tool-verified: `_splice_commands` e `_input_relation` in graph.py:245-271]

### Tipi di nodo e indizi visivi

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Tipo di nodo | Colore | Significato |
| --- | --- | --- |
| `source` | Verde | Una colonna di una tabella di base |
| `derived` | Blu | Prodotta da un'espressione SQL (funzione, operatore, CTE) |
| `command` | Viola | Una colonna di output di un comando registrato |

Anelli aggiuntivi su un nodo:

- **Anello arancione** — una colonna di output finale dell'istruzione.
- **Bordo doppio** — la relazione della colonna è una vista materializzata (snapshot MV/CTAS).
- **Anello rosso** — membro di un ciclo classificato come errore.
- **Anello giallo** — membro di un ciclo classificato come ciclo di retroazione (feedback loop).

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Trasformazioni denominate sugli archi

Ogni arco riporta l'espressione SQL grezza che produce la colonna di destinazione, oltre a un
elenco di operazioni denominate: funzioni SQL (`sql_function`), operatori aritmetici/logici
(`operator`), comandi registrati (`command`), riferimenti di colonna semplici (`identity`) e
letterali (`constant`). [tool-verified: TransformOp and name_transform in graph.py:36-145]

Un arco proveniente da una chiamata di comando viene rappresentato come una linea viola
tratteggiata nell'interfaccia utente. [tool-verified: LineageDag.tsx:122-124]

## Grafo a livello di federazione (REQ-1161)

Il grafo di federazione unisce la derivazione per singola istruzione di ogni MV registrata in un
unico grafo di provenienza. L'identità del nodo è `relation.column` — la colonna di output di una
vista e il riferimento di input di un'altra vista alla stessa colonna collassano in un unico nodo.
Il risultato è un unico DAG dalle colonne di origine di base fino a ogni dataset derivato nella
piattaforma. [tool-verified: `build_federation_graph` in merge.py:205-229 e `qualify_outputs` in
graph.py:275-299]

Utilizzare `focus`, `direction` e `depth` per delimitare la vista su scala di federazione senza
ricalcolare il grafo. [tool-verified: `slice_graph` in merge.py:160-189]

## Cicli (REQ-1161)

I cicli vengono descritti, non rifiutati. Il motore di derivazione rileva ogni ciclo diretto e lo
**classifica**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classificazione | Colore del bordo | Significato |
| --- | --- | --- |
| `feedback` | Giallo | Il ciclo attraversa un nodo materializzato — un ciclo di retroazione legittimo e sfasato nel tempo. Lo snapshot della MV costituisce il confine di versione che lo rende ben definito. |
| `error` | Rosso | Nessun confine di materializzazione sul ciclo — una definizione circolare senza un ordine di valutazione stabile. Probabilmente un errore di progettazione. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Un ciclo `feedback` non è un errore. Una MV di arricchimento che reinserisce una colonna derivata
nella propria relazione di origine è un pattern valido, purché un nodo del ciclo sia materializzato
— lo snapshot isola temporalmente le due metà. Un ciclo `error` richiede il giudizio di un
operatore: di norma significa che due viste si fanno riferimento reciprocamente senza uno snapshot
intermedio.

## API

Entrambi gli endpoint sono **statici** — leggono definizioni e contratti, non dati.

### POST /admin/lineage/graph

Restituisce il DAG a livello di colonna per una singola istruzione SQL.

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

Restituisce HTTP 422 quando l'SQL non può essere analizzato.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Restituisce il grafo di provenienza unificato su tutte le MV nel registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Parametri di query [tool-verified: function signature at lineage_router.py:73-76]:

| Parametro | Valori | Predefinito | Effetto |
| --- | --- | --- | --- |
| `focus` | Un id di nodo | — | Delimita la risposta al sottografo attorno a questo nodo |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Direzione di attraversamento a partire da `focus` |
| `depth` | intero | illimitato | Distanza massima in hop da `focus` |

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

## Cosa romperebbe la rinomina o l'eliminazione di una colonna (REQ-1484)

Una colonna porta due nomi, e ciascuno è memorizzato da un insieme diverso di artefatti.

Il **nome esposto** è ciò che le superfici SQL e GraphQL mostrano: `table_columns.alias`, con
fallback al valore predefinito snake_case quando non è impostato alcun alias [tool-verified:
`computed_sql_alias` at `schema_helpers.py:317`]. Viste, viste materializzate, espressioni di
metrica, predicati RLS, contratti DQ, grani delle metric-view e chiavi di riga delle MV sono tutti
scritti in riferimento a quel nome, quindi **rinominare un alias li rompe con la stessa certezza
con cui li romperebbe eliminare la colonna**.

Il **nome fisico** è `table_columns.column_name`, l'identità che sopravvive alla sostituzione
integrale delle colonne durante l'upsert della tabella. Relazioni, associazioni al glossario,
assegnazioni di tag, la colonna watermark e i preset di colonna memorizzano questo nome, quindi si
rompono solo quando la colonna viene **rimossa**.

`columnDependents` riporta entrambi. Le viste e le MV a valle provengono dal sezionamento del
grafo di federazione in corrispondenza del nome esposto della colonna; gli artefatti che quel
grafo non copre provengono da una scansione diretta del registro [tool-verified:
`graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` vale `rename` per un riferimento al nome esposto e `remove` per uno al nome fisico,
così un chiamante può capire a quale metà della modifica reagisce ciascun artefatto.

Effettuare questa richiesta **prima** del salvataggio. Una colonna rinominata viene individuata
tramite il nome esposto che ancora porta nel registro; una volta che l'alias è stato applicato, il
vecchio nome scompare e la query non trova nulla.

La pagina Tables esegue automaticamente la query quando una modifica in sospeso cambia un alias o
riduce l'insieme di colonne, ed elenca ciò che trova [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. L'avviso è consultivo:
indica gli artefatti interessati e la decisione spetta all'amministratore. Non blocca il
salvataggio, perché non tutti i consumer dell'estate sono raggiungibili — una dashboard esterna o
un'applicazione client che interroga la colonna per nome è al di fuori della conoscenza del
registro. Per lo stesso motivo, le scansioni su testo SQL libero fanno corrispondere la colonna
come token identificatore anziché risolvere lo scope, il che può indicare un artefatto che in
realtà non utilizza la colonna. Nella direzione della sicurezza, un avviso preferisce segnalare
troppo piuttosto che troppo poco.

## Utilizzare la derivazione per governare i contratti di comando

Poiché la taint closure collega ogni colonna di input dichiarata a ogni colonna di output
dichiarata, l'ampiezza di tale closure dipende interamente da ciò che si dichiara.

Si consideri un comando che riceve una tabella `orders` completa (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) e restituisce un `embedding`. Se il contratto di input
elenca tutte queste colonne, ogni colonna a valle che utilizza l'embedding mostrerà la derivazione
da tutte quante. Ciò è corretto ma poco utile — è difficile capire cosa abbia effettivamente
contato.

Dichiarando solo `id` e `text` (le colonne che il modello di embedding legge realmente), il cono di
derivazione si restringe a queste due colonne di origine. La derivazione risulta così al tempo
stesso corretta e precisa.

Vedere [Commands](commands.md) per la meccanica di dichiarazione di un contratto di input
ristretto.
