<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modellazione dei dati (Entities e Facts)

Provisa offre due primitive dichiarative — `entity` e `fact` — che coprono i blocchi costitutivi
con cui viene assemblato ogni star schema e ogni Data Vault. Dichiarare la specifica; Provisa la
riduce esattamente alle definizioni di vista materializzata, bitemporali e di relazione che
altrimenti si dovrebbero scrivere a mano (REQ-1164). [tool-verified: modeling.py module
docstring lines 11-28]

## Cosa sono le entity e i fact

Una **entity** è una proiezione con chiave, deduplicata e opzionalmente storicizzata, di una
relazione di origine. Si assegna un nome, la si punta a un'origine, si dichiara la chiave
dell'entity e gli attributi da portare, e si sceglie una modalità di storicizzazione (history).
Provisa scrive l'SQL della vista e registra la MV (vista materializzata). Quando la history è
abilitata, la MV è bitemporale. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

Un **fact** è una join verso chiavi di entity, ridotta a un grain dichiarato, con misure
aggregate. Provisa scrive una query di MV aggregata (`GROUP BY` sul grain + colonne FK) e
registra una relazione per ciascun collegamento a dimensione dichiarato. Un fact senza misure è
un puro insieme di chiavi — il pattern link del Data Vault. [tool-verified: `Fact` dataclass,
modeling.py lines 91-102; `fact_registration` function, modeling.py lines 123-141; comment at
line 130 "a measureless fact is a pure key-set (DV link)"]

Entrambi i costrutti sono IR (rappresentazione intermedia). Le definizioni generate si
ridirigono tra motori diversi — materializzate in Oracle, Databricks, oppure lasciate virtuali
sopra un motore MPP — senza rimodellare. [tool-verified: modeling.py docstring lines 25-28]

## Modalità di history

Su un'entity sono disponibili tre modalità [tool-verified: `_HISTORY` constant at modeling.py
line 38, `_HISTORY_MODE` dict at modeling.py line 40]:

| Modalità | Significato | Modalità bitemporale |
| --- | --- | --- |
| `none` | Solo stato corrente. Nessuna history. | — |
| `scd2` | Traccia ogni modifica. Aggiunge solo le righe modificate (delta), con chiave sulla chiave dell'entity. | `delta` |
| `snapshot` | Traccia ogni aggiornamento. Aggiunge l'intero set di risultati a ogni aggiornamento, con timestamp di sistema. | `snapshot` |

`scd2` richiede una chiave dell'entity per calcolare il delta. `snapshot` funziona su qualsiasi
motore, ma lo storage cresce di una copia completa a ogni aggiornamento. Scegliere `scd2` per
origini grandi e a lenta variazione; scegliere `snapshot` quando serve la history completa e
l'origine non può fornire una chiave.

I fact non hanno modalità di history — la copertura temporale deriva dalla history dell'entity
sottostante.

## Misure e aggregazioni

Le misure si dichiarano come coppie `column:agg`. Aggregazioni supportate [tool-verified:
`_AGGS` at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

L'aggregazione predefinita è `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Esempio pratico: entity Customer + fact Sales

### Le tabelle di origine

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Registrare l'entity Customer

=== "Admin UI"

    1. Aprire **Tables** e fare clic su **+ Model**.
    2. Scegliere **Entity (dimension)**.
    3. Compilare il modulo:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(il proprio dominio)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Fare clic su **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

Provisa genera e registra questa MV bitemporale [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Registrare il fact Sales

=== "Admin UI"

    1. Fare di nuovo clic su **+ Model**.
    2. Scegliere **Fact**.
    3. Compilare il modulo:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(il proprio dominio)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Fare clic su **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

Provisa genera e registra [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

In più, viene registrata una relazione: `Sales.customer_id → Customer` (cardinalità:
many-to-one). [tool-verified: `fact_table_input` in modeling_register.py lines 89-98,
cardinality at line 95]

## Il modulo Model (Admin UI)

Il pulsante **+ Model** compare nella pagina **Tables** (tooltip: "Model an entity or fact
(star schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines
441-450]

Un controllo segmentato in cima alla finestra modale alterna tra **Entity (dimension)** e
**Fact**. [tool-verified: ModelingForm.tsx lines 102-110]

### Campi Entity

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| Name | sì | Il nome della MV nel catalogo |
| Source relation | sì | Relazione in notazione puntata, es. `raw.customers` |
| Domain | sì | Dominio a cui appartiene la MV |
| Entity key | sì | Colonna/e chiave separate da virgole, es. `id` |
| Attributes | no | Colonne di attributi separate da virgole, es. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; il valore predefinito è `none` |

### Campi Fact

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| Name | sì | Il nome della MV nel catalogo |
| Source relation | sì | Relazione in notazione puntata, es. `raw.orders` |
| Domain | sì | Dominio a cui appartiene la MV |
| Grain | sì | Colonna/e di grain separate da virgole, es. `order_id` |
| Measures | no | Coppie `col:agg` separate da virgole, es. `amount:sum, quantity:sum` |
| Dimensions | no | Coppie `Entity:fk_column` separate da virgole, es. `Customer:customer_id` |

Quando `agg` viene omesso in una misura (`amount` invece di `amount:sum`), il valore
predefinito è `sum`. [tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## L'API GraphQL

Entrambe le mutation risiedono nello schema di amministrazione. [tool-verified:
schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campi di `EntityInput` [tool-verified: types.py lines 449-456]:

| Campo | Tipo | Predefinito | Descrizione |
| --- | --- | --- | --- |
| `name` | String | — | Nome di catalogo per la MV dell'entity |
| `source` | String | — | Relazione di origine (`schema.table` o tra virgolette) |
| `domainId` | String | — | Id del dominio |
| `key` | [String] | — | Colonna/e chiave dell'entity |
| `attributes` | [String] | `[]` | Colonne di attributi da proiettare |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Elenco di visibilità per ruolo |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Campi di `FactInput` [tool-verified: types.py lines 472-479]:

| Campo | Tipo | Predefinito | Descrizione |
| --- | --- | --- | --- |
| `name` | String | — | Nome di catalogo per la MV del fact |
| `source` | String | — | Relazione di origine |
| `domainId` | String | — | Id del dominio |
| `grain` | [String] | — | Colonna/e di grain per la GROUP BY |
| `measures` | [MeasureInput] | `[]` | Coppie `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Coppie `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Elenco di visibilità per ruolo |

`MeasureInput`: `{ column: String, agg: String }` — agg è `"sum"` per impostazione predefinita.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` è il nome dell'entity referenziata;
`via` è la colonna FK sull'origine del fact.
[tool-verified: types.py lines 465-468]

In caso di successo, `registerFact` restituisce un messaggio nella forma:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Star schema di Kimball e Data Vault

Nessuno dei due pattern richiede strumenti separati. Le stesse due primitive si compongono per
formare entrambi.

### Star schema di Kimball

Questo percorso costruisce una stella a tre dimensioni. Due tabelle di origine sono nuove:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for
  this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this
  example]

Anche `raw.orders` acquisisce qui le colonne `product_id` e `order_date`. [inferred]

#### Scegliere il tipo di SCD

La modalità di history è l'unico parametro che distingue SCD Type 1 da Type 2:

| Tipo SCD | Modalità di history | Effetto |
| --- | --- | --- |
| Type 1 (solo corrente) | `none` | La MV viene ricostruita a ogni aggiornamento; nessuna history delle righe |
| Type 2 (versionato) | `scd2` | MV delta bitemporale; ogni modifica aggiunge una nuova riga, con chiave sulla chiave dell'entity |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Usare `scd2` quando le query a valle devono unire una dimensione così com'era al momento della
transazione — il tier di un cliente al momento dell'acquisto, non il tier attuale. Usare `none`
per lookup stabili. Un date spine non cambia mai. Un catalogo prodotti in cui serve solo il
prezzo attuale può essere ricostruito a ogni aggiornamento.

#### Decisione del grain

Il grain è il livello di dettaglio più basso a cui risponde il fact. `order_id` restituisce una
riga per ordine, preservando la capacità di contare ordini distinti e unire con qualsiasi
dimensione alla granularità dell'ordine. Un grain più grossolano — ad esempio
`["customer_id", "order_date"]` — preaggrega tra ordini e scarta quel dettaglio in modo
permanente. Dichiarare il grain più fine di cui l'attività ha bisogno; i rollup più grossolani
sono economici da derivare in seguito.

#### Registrare le dimensioni

**Customer** (SCD Type 2 — le modifiche del tier devono essere preservate):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Genera una MV delta bitemporale con chiave su `id` [tool-verified: entity_registration
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — catalogo corrente, nessuna history delle versioni necessaria):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

Genera una MV ordinaria ricostruita a ogni aggiornamento [tool-verified: entity_registration
modeling.py lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line
115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (nessuna history — una data è immutabile):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

Genera:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Registrare il fact Sales su tre dimensioni

Grain: `order_id`. Tre riferimenti a dimensioni — una colonna FK ciascuno. Entrambe le misure
sono somme additive.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

Provisa calcola `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` e genera
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Vengono registrate automaticamente tre relazioni [tool-verified: modeling_register.py lines
89-98, cardinality `"many_to_one"` at line 95]:

| Relazione | Cardinalità |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensioni conformi

Una dimensione conforme viene registrata una sola volta e referenziata per nome da un numero
qualsiasi di fact. Supponiamo che `raw.returns` contenga `return_id`, `customer_id`,
`product_id` e `amount`. Il fact Returns riutilizza Customer e Product senza registrarli di
nuovo:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

Sia `Sales` sia `Returns` puntano alle stesse entity `Customer` e `Product`. I percorsi di join
di Provisa garantiscono che le query attraverso l'uno o l'altro fact attraversino la stessa
definizione di dimensione [tool-verified: fact_registration uses entity name as `target_table`
at modeling.py lines 138-140; fact_table_input wires `target_table_id` from that name at
modeling_register.py lines 91-93].

---

### Data Vault

Le stesse primitive si mappano direttamente sul vocabolario Data Vault:

| Artefatto DV | Primitiva | History |
| --- | --- | --- |
| Hub | `entity` | `none` — solo chiavi dell'entity |
| Satellite | `entity` | `scd2` o `snapshot` — history degli attributi accanto alla chiave dell'hub |
| Link | `fact` senza misure | — |
| Bridge / link aggregato | `fact` con misure | — |

L'esempio costruisce un vault minimo su `raw.customers` e `raw.orders`.

#### Hub

Un hub contiene la chiave dell'entity e nient'altro. `attributes: []` con `history: "none"`
produce un insieme di chiavi correnti deduplicato; la history degli attributi vive interamente
nel satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Genera [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Genera:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Il satellite si colloca accanto alla chiave dell'hub e porta la history completa degli
attributi. Usare `scd2` per aggiungere solo le righe modificate; usare `snapshot` per marcare
ogni aggiornamento completo.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Genera [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` e `CustomerHub` hanno entrambi chiave su `id`. L'hub è il target di join
stabile; il satellite fornisce accesso agli attributi in un punto nel tempo tramite il livello
bitemporale.

#### Link (fact senza misure)

Un link registra quali chiavi dell'hub sono coesistite — solo chiavi, nessuna misura. Provisa
omette la `GROUP BY` quando `measures` è vuoto [tool-verified: modeling.py lines 130-131:
`if f.measures: view_sql += " GROUP BY ..."`].

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]`. Nessuna misura, quindi nessuna `GROUP BY`. Genera [tool-verified:
fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Vengono registrate due relazioni: `OrderCustomerLink.customer_id → CustomerHub` e
`OrderCustomerLink.order_id → OrderHub`, entrambe many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / link aggregato

Aggiungere misure al link e Provisa emette la `GROUP BY`, producendo un bridge preaggregato. Al
grain `order_id` con un cliente per ordine, il risultato è una riga aggregata per ordine:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]` (l'`order_id` duplicato dell'elenco delle dimensioni viene scartato da
`_dedup`). Genera [tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Il modello non decide la metodologia. Grain, conformità, scelta dello SCD e suddivisione
hub/satellite restano decisioni del modellatore. Provisa le esegue. [tool-verified: modeling.py
docstring lines 25-26]

## Metriche (REQ-1317, REQ-1318, REQ-1320)

Una **metrica** (metric) è una definizione di aggregato con nome e governata, senza un proprio
grain. Il grain — le dimensioni secondo cui l'aggregato viene scomposto — viene associato al
momento della query dal chiamante, non al momento della definizione. È questo che distingue una
metrica da una vista: una vista fissa il grain alla creazione; una metrica resta aperta finché
non viene interrogata. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### L'oggetto Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| `name` | sì | snake_case, es. `net_revenue`. Validato: `[a-z][a-z0-9_]*` |
| `expression` | sì | ANSI-SQL aggregato; deve includere almeno una funzione di aggregazione |
| `datatype` | no | Suggerimento del tipo di risultato, es. `number`, `integer` |
| `description` | no | Definizione di business leggibile da un essere umano |
| `ai_context` | no | Testo per i consumatori IA — proiettato su strumenti MCP, pg_description, documentazione GraphQL ed export Ossie |
| `visible_to` | no | Elenco di ruoli; il valore predefinito è `["*"]` (tutti i ruoli) |
| `from_fact` | — | Impostato automaticamente quando la metrica è stata generata da una misura di un fact |

I riferimenti a colonne all'interno dell'espressione devono essere qualificati con la tabella
(`orders.amount`, non `amount`). Una colonna non qualificata è un errore bloccante al momento
dell'espansione, non un avviso.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

Il repository delle metriche valida l'espressione a ogni scrittura. Un'espressione che non
viene analizzata correttamente o che non contiene alcuna funzione di aggregazione viene
rifiutata; non viene mai memorizzata.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Esempio di voce di configurazione:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Interrogare una metrica

Il compilatore riserva lo schema `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Ogni metrica è indirizzabile come relazione
virtuale all'interno di quello schema. Interrogarla come una tabella — le colonne selezionate
diventano il set di dimensioni e la GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Il compilatore riscrive questo in un vero aggregato raggruppato sulle tabelle semantiche
sottostanti prima che venga eseguita la governance, in modo che RLS e mascheramento si
applichino alle colonne reali.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` contro una relazione di metrica viene rifiutato — indicare esplicitamente le colonne
di dimensione e `value`. [tool-verified: `expand_metric_query`,
`provisa/compiler/metric_expand.py` lines 302–306]

Quando l'espressione di una metrica si estende su più tabelle, il compilatore le unisce
attraverso le relazioni registrate. Una dimensione che è una colonna di una tabella
referenziata direttamente si risolve verso quella tabella. Una dimensione a un salto di
relazione di distanza viene unita automaticamente. Due salti o una dimensione ambigua sono un
errore bloccante che nomina il responsabile.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### Metriche da specifiche di fact (REQ-1320)

Quando si registra un fact, ogni misura dichiarata registra automaticamente un oggetto Metric
corrispondente. Il campo `from_fact` della metrica registra il nome della tabella di fact di
origine, e le dimensioni di raggruppamento valide sono gli attributi dell'entity raggiungibili
attraverso le relazioni FK del fact.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Le metriche registrate automaticamente compaiono nella pagina Metrics con un badge **fact**.
Possono essere modificate come qualsiasi altra metrica. [tool-verified: `MetricsPage.tsx` lines
405–408: `{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### Viste composte da metriche (view_metrics, REQ-1318)

Una vista `view_metrics` fissa il grain di una metrica al momento della definizione.
Dichiarare i nomi delle metriche, le colonne di dimensione e i filtri opzionali; il compilatore
genera il SELECT.

[tool-verified: `ViewMetricsSpec`, `provisa/core/models.py` lines 479–492]

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

Il compilatore genera (per questo esempio):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` e `view_sql` si escludono a vicenda sulla stessa tabella.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Rigenerazione automatica al cambio di una metrica.** Quando l'espressione di una metrica
viene aggiornata, ogni vista `view_metrics` che la referenzia viene ricompilata e il nuovo SQL
viene persistito immediatamente. La vista non può divergere dalla definizione della metrica per
costruzione.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Chiamate `metric()` inline in SQL di vista scritto a mano.** Un `view_sql` scritto a mano può
anche referenziare metriche tramite `metric('name')`. Il compilatore sostituisce ogni chiamata
con l'espressione della metrica e registra un edge di lineage. Questo dà alle viste scritte a
mano la stessa proprietà di ricompilazione al cambiamento quando referenziano una metrica
invece di copiarne la formula.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

Nota: le viste del percorso di configurazione che usano chiamate `metric()` inline si
rigenerano al ricaricamento della configurazione, non all'upsert della metrica. [tool-verified:
`regenerate_metric_views` docstring, `_metric_views.py` lines 84–86: "Free-hand view_sql born
from inline metric() calls carries no stored provenance, so it is not regenerated here
(config-path views regenerate on config reload)"]

### La pagina di amministrazione Metrics (REQ-1323, REQ-1324)

Aprire la voce di navigazione **Metrics** per gestire le metriche governate. Fare clic su una
riga per espandere un pannello di dettaglio in sola lettura; fare clic su **Edit** al suo
interno per passare alla modifica inline (nessuna finestra modale). **New Metric** apre una
scheda di creazione inline sopra la tabella. La conferma di eliminazione è l'unica finestra
modale della pagina.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

Il modulo di creazione/modifica offre un builder a tre selettori per le metriche originate da
fact: scegliere la tabella di fact di origine (filtrata su `modelingRole=fact`), una colonna di
misura e una funzione di aggregazione (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Il datatype viene
derivato automaticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → il tipo della
colonna di misura`. L'area di testo dell'espressione resta la via di fuga per espressioni
arbitrarie.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## Il vantaggio dell'IR

Ogni chiamata di registrazione percorre lo stesso percorso di una MV scritta a mano. La
specifica entity/fact è una rappresentazione intermedia — non un template, non una macro. Il
warehouse a cui è destinata è una proprietà del deployment, non del modello. Cambiare il motore
di destinazione, e le stesse dichiarazioni `entity` / `fact` si materializzano lì, perché l'SQL
generato e le modalità bitemporali sono neutrali rispetto al motore per costruzione.
[tool-verified: modeling.py docstring lines 25-28; modeling_register.py lines 56-66, 80-88]
