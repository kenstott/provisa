# Derivazione a livello di colonna {: #column-level-lineage }

Provisa traccia la derivazione dei dati a livello di colonna in modo statico. La calcola dalle definizioni SQL e dai contratti dei comandi, quindi non serve eseguire nulla. Avete due viste: un DAG per ogni istruzione e un grafo di provenienza a livello di federazione che copre ogni vista e vista materializzata (MV) registrata.

## L'esploratore della derivazione {: #the-lineage-explorer }

Aprire **Lineage** nella UI (`/lineage`). Incollare un'istruzione SQL e fare clic su **Build
statement graph** per vederne il DAG a livello di colonna. Fare clic su **Federation graph** per
caricare il grafo di provenienza su ogni MV del registro.

## DAG a livello di istruzione {: #statement-level-dag }

Ogni colonna di output nominata nel SQL diventa un nodo. Il costruttore la ripercorre a ritroso
attraverso ogni CTE, sotto-query, join e chiamata inline a comando fino alle sue colonne di origine,
costruendo un grafo orientato dagli input di origine agli output finali.

### Esempio svolto {: #worked-example }

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
  tutti gli input dichiarati a tutti gli output).
- `e.embedding` ed `e.geo` sono nodi **command** al confine di `enrich_grpc_set`.
- `geo_u` è un nodo **derived** prodotto dalla funzione SQL `UPPER`.

Il confine del comando **non è opaco**. Poiché `enrich_grpc_set` dichiara le proprie colonne di
input (`id`, `region`) e di output (`id`, `embedding`, `geo`), il motore di derivazione salda la
chiusura per contaminazione senza interruzioni, dalle colonne dichiarate della relazione di origine
fino a ciascun output.

### Tipi di nodo e segnali visivi {: #node-kinds-and-visual-cues }

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

### Trasformazioni nominate sugli archi {: #named-transforms-on-edges }

Ogni arco porta con sé l'espressione SQL grezza che produce la colonna di destinazione, più un
elenco di operazioni nominate: funzioni SQL (`sql_function`), operatori aritmetici e logici
(`operator`), comandi registrati (`command`), riferimenti diretti a colonna (`identity`) e letterali
(`constant`).

Un arco che nasce da una chiamata a comando viene disegnato nella UI come una linea viola
tratteggiata.

## Grafo esteso alla federazione {: #federation-wide-graph }

Il grafo della federazione unisce la derivazione per istruzione di ogni MV registrata in un unico grafo di provenienza. L'identità di un nodo è `relation.column`, quindi la colonna di output di una vista e il riferimento di input di un'altra vista a quella colonna si fondono in un nodo. Il risultato è un unico DAG dalle colonne delle sorgenti di base a ogni dataset derivato della piattaforma.

Usare `focus`, `direction` e `depth` per delimitare la vista su scala di federazione senza
ricalcolare il grafo.

## Cicli {: #cycles }

Il motore di derivazione rileva ogni ciclo orientato e lo **classifica**. Un ciclo viene descritto, mai rifiutato.

| Classificazione | Colore del bordo | Significato |
| --- | --- | --- |
| `feedback` | Giallo | Il ciclo attraversa un nodo materializzato — un anello di retroazione legittimo e sfasato nel tempo. Lo snapshot della MV è il confine di versione che lo rende ben definito. |
| `error` | Rosso | Nessun confine di materializzazione sull'anello — una definizione circolare senza un ordine di valutazione stabile. Probabilmente un errore di progettazione. |

Un ciclo `feedback` è un pattern valido. Una MV di arricchimento che riporta una colonna derivata nella propria relazione sorgente funziona finché un nodo del ciclo è materializzato, perché lo snapshot separa le due metà nel tempo. Un ciclo `error` richiede il giudizio di un operatore. Di solito significa che due viste si riferiscono l'una all'altra senza uno snapshot in mezzo.

## API

Entrambi gli endpoint sono **statici**. Leggono definizioni e contratti, mai dati.

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

Forma della risposta:

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

### GET /admin/lineage/federation

Restituisce il grafo di provenienza unito su tutte le MV del registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Parametri di query:

| Parametro | Valori | Default | Effetto |
| --- | --- | --- | --- |
| `focus` | Un id di nodo | — | Delimita la risposta al sotto-grafo attorno a questo nodo |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | In quale direzione percorrere il grafo a partire da `focus` |
| `depth` | intero | illimitato | Distanza massima in salti da `focus` |

La risposta ha la stessa forma del grafo di istruzione, con l'aggiunta di un campo `cycles`
:

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

## Da cosa è stato costruito ogni refresh {: #what-each-refresh-was-built-from }

Ogni refresh di una vista materializzata registra da cosa è stato costruito: la versione della definizione della vista, la versione dei suoi input e come è stata stabilita, e le colonne sorgente e l'espressione di ogni colonna di output. La vista operativa `"ops"."mv_refresh_lineage"` restituisce una riga per refresh:

| Colonna | Contiene |
|---|---|
| `mv_id` | La vista materializzata aggiornata |
| `target_table` | La tabella scritta dal refresh |
| `definition_version` | La versione della definizione della vista |
| `input_version`, `input_version_kind` | La versione dei suoi input: uno snapshot Iceberg, un watermark, un token di freschezza o l'ora del refresh |
| `trace_id` | Il trace del refresh in `"ops"."traces"` |
| `refreshed_at` | Quando è stato eseguito |
| `column_derivations` | Ogni colonna di output con le sue colonne sorgente e la sua espressione, in JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

La vista legge i record di refresh in `"ops"."traces"` e risponde su ogni motore che li conserva. Su Trino sono le tabelle Iceberg del catalogo `otel`. Su DuckDB il server scrive i propri record di trace come file Parquet nella sua cartella di telemetria (`PROVISA_TELEMETRY_DIR`, `~/.provisa/telemetry` per impostazione predefinita), un file per batch di esportazione in una cartella per giorno. La compattazione pianificata della telemetria (`otel.compact_cron`) unisce i file piccoli di ogni giorno in file fino a 64 MB. Ogni organizzazione legge solo i propri refresh, e i record sono conservati a tempo indeterminato. Gli span vengono registrati solo mentre l'SDK OpenTelemetry è abilitato (`OTEL_SDK_DISABLED` non impostato o false).

## Una vista com'era, e com'era stata registrata {: #a-view-as-it-stood-and-as-it-was-recorded }

Una vista dichiarata bitemporale conserva ogni versione che ha avuto: ogni refresh aggiunge, e nessuna versione viene riscritta, su qualsiasi archivio. Una lettura semplice risponde con lo stato attuale. Due header della richiesta ne leggono il passato:

- `X-Provisa-As-Of` legge la vista a una data, o su un intervallo, secondo il suo orologio: l'ora di modifica propria della sorgente dove la vista dichiara `bitemporal_source_time`, l'ora di caricamento di Provisa dove non la dichiara.
- `X-Provisa-As-Recorded` la legge come Provisa l'aveva registrata a una data. Le versioni registrate dopo restano escluse, quindi una correzione registrata in seguito non cambia la riproduzione del passato.

Al 30 settembre e registrata il 1° ottobre dà il dato originale; registrata il 5 ottobre dà la correzione. Il record di audit di ogni istruzione conserva entrambe le date (`as_of`, `as_recorded`), così un report si può riprodurre com'era stato prodotto la prima volta. [Viste con cronologia](configuration.md#views-with-history-time-travel) descrive come dichiarare una vista bitemporale e ogni superficie che accetta i due valori.

## Che cosa romperebbe la ridenominazione o la rimozione di una colonna {: #what-a-column-rename-or-drop-would-break }

Una colonna porta due nomi, e ciascuno è memorizzato da un insieme diverso di artefatti.

Il **nome esposto** è quello che mostrano le superfici SQL e GraphQL: `table_columns.alias`, con
ripiego sul valore predefinito in snake_case quando non è impostato alcun alias. Viste, viste materializzate, espressioni di metrica, predicati RLS,
contratti DQ, granularità delle viste di metrica e chiavi di riga delle MV sono tutti scritti su
quel nome, quindi **rinominare un alias li rompe con la stessa certezza con cui li romperebbe
eliminare la colonna**.

Il **nome fisico** è `table_columns.column_name`, l'identità che sopravvive alla sostituzione in
blocco delle colonne durante l'upsert della tabella. Relazioni, collegamenti del
[glossario](glossary.md), assegnazioni di tag, la colonna di watermark e i preset di colonna
memorizzano questo, quindi si rompono solo quando la colonna viene **rimossa**.

`columnDependents` riporta entrambi. Le viste e le MV a valle provengono dal ritaglio del grafo di
federazione sul nome esposto della colonna; gli artefatti che quel grafo non copre provengono da una
scansione diretta del registro.

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

La pagina Tabelle esegue la query automaticamente quando una modifica in sospeso cambia un alias o riduce l'insieme delle colonne, ed elenca ciò che trova. L'avviso è consultivo. Nomina gli artefatti coinvolti, e decide l'amministratore. Non blocca mai il salvataggio, perché non si possono raggiungere tutti i consumatori del patrimonio: una dashboard esterna o un'applicazione client che interroga la colonna per nome sfugge alla conoscenza del registro. Per lo stesso motivo, le scansioni sul testo SQL libero riconoscono la colonna come token identificatore senza risolvere l'ambito, quindi possono nominare un artefatto che alla fine non usa la colonna. Per un avviso, segnalare troppo è la direzione sicura.

## Usare la derivazione per governare i contratti dei comandi {: #using-lineage-to-govern-command-contracts }

Poiché la chiusura per contaminazione collega ogni colonna di input dichiarata a ogni colonna di
output dichiarata, l'ampiezza di quella chiusura dipende interamente da ciò che si dichiara.

Considerate un comando che riceve un'intera tabella degli ordini (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) e restituisce un `embedding`. Se il contratto di input elenca tutte quelle colonne, ogni colonna a valle che usa l'embedding mostra derivazione da tutte. È esatto, ma non aiuta, perché non si capisce cosa abbia contato davvero.

Dichiarando solo `id` e `text` (le colonne che il modello di embedding legge realmente), il cono di
derivazione si restringe a quelle due colonne di origine. La derivazione risulta insieme corretta e
precisa.

Vedere [Comandi](commands.md) per la meccanica della dichiarazione di un contratto di input
ristretto.
