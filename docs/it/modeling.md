<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modellazione dei dati (Entities e Facts) {: #data-modeling-entities-facts }

Provisa vi dà due primitive dichiarative, `entity` e `fact`, che coprono i mattoni di cui è fatto ogni schema a stella e ogni Data Vault. Dichiarate la specifica, e Provisa la traduce esattamente nelle definizioni di viste materializzate, bitemporali e di relazioni che altrimenti scrivereste a mano.

## Cosa sono le entity e i fact {: #what-entities-and-facts-are }

Una **entity** è una proiezione con chiave, deduplicata e opzionalmente storicizzata, di una
relazione di origine. Si assegna un nome, la si punta a un'origine, si dichiara la chiave
dell'entity e gli attributi da portare, e si sceglie una modalità di storicizzazione (history).
Provisa scrive l'SQL della vista e registra la MV (vista materializzata). Quando la history è
abilitata, la MV è bitemporale.

Un **fatto** è un join sulle chiavi delle entità, ridotto a una granularità dichiarata, con misure aggregate. Provisa scrive una query di MV aggregata (`GROUP BY` granularità + colonne FK) e registra una relazione per ogni collegamento di dimensione dichiarato. Un fatto senza misure è un puro insieme di chiavi, il pattern link di Data Vault.

Entrambi i costrutti sono IR. Le definizioni generate passano da un motore all'altro senza rimodellare: materializzate in Oracle o Databricks, oppure lasciate virtuali su un motore MPP.

## Modalità di history {: #history-modes }

Su un'entity sono disponibili tre modalità:

| Modalità | Significato | Modalità bitemporale |
| --- | --- | --- |
| `none` | Solo stato corrente. Nessuna history. | — |
| `scd2` | Traccia ogni modifica. Aggiunge solo le righe modificate (delta), con chiave sulla chiave dell'entity. | `delta` |
| `snapshot` | Traccia ogni aggiornamento. Aggiunge l'intero set di risultati a ogni aggiornamento, con timestamp di sistema. | `snapshot` |

`scd2` richiede una chiave dell'entity per calcolare il delta. `snapshot` funziona su qualsiasi
motore, ma lo storage cresce di una copia completa a ogni aggiornamento. Scegliere `scd2` per
origini grandi e a lenta variazione; scegliere `snapshot` quando serve la history completa e
l'origine non può fornire una chiave.

I fatti non hanno una propria modalità di storico; la loro copertura nel tempo viene dallo storico delle entità sottostanti.

## Misure e aggregazioni {: #measures-and-aggregations }

Le misure si dichiarano come coppie `column:agg`. Aggregazioni supportate:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

L'aggregazione predefinita è `sum`.

## Esempio pratico: entity Customer + fact Sales {: #worked-example-customer-entity-sales-fact }

### Le tabelle di origine {: #the-source-tables }

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Registrare l'entity Customer {: #register-the-customer-entity }

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

Provisa genera e registra questa MV bitemporale:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Registrare il fact Sales {: #register-the-sales-fact }

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

Provisa genera e registra:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

In più, viene registrata una relazione: `Sales.customer_id → Customer` (cardinalità:
many-to-one).

## Il modulo Model (Admin UI) {: #the-model-form-admin-ui }

Il pulsante **+ Model** compare nella pagina **Tables** (tooltip: "Model an entity or fact
(star schema / Data Vault)").

Un controllo segmentato in cima alla finestra modale alterna tra **Entity (dimension)** e
**Fact**.

### Campi Entity {: #entity-fields }

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| Name | sì | Il nome della MV nel catalogo |
| Source relation | sì | Relazione in notazione puntata, es. `raw.customers` |
| Domain | sì | Dominio a cui appartiene la MV |
| Entity key | sì | Colonna/e chiave separate da virgole, es. `id` |
| Attributes | no | Colonne di attributi separate da virgole, es. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; il valore predefinito è `none` |

### Campi Fact {: #fact-fields }

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| Name | sì | Il nome della MV nel catalogo |
| Source relation | sì | Relazione in notazione puntata, es. `raw.orders` |
| Domain | sì | Dominio a cui appartiene la MV |
| Grain | sì | Colonna/e di grain separate da virgole, es. `order_id` |
| Measures | no | Coppie `col:agg` separate da virgole, es. `amount:sum, quantity:sum` |
| Dimensions | no | Coppie `Entity:fk_column` separate da virgole, es. `Customer:customer_id` |

Quando `agg` viene omesso in una misura (`amount` invece di `amount:sum`), il valore
predefinito è `sum`.

## L'API GraphQL {: #the-graphql-api }

Entrambe le mutation risiedono nello schema di amministrazione.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campi di `EntityInput`:

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

Campi di `FactInput`:

| Campo | Tipo | Predefinito | Descrizione |
| --- | --- | --- | --- |
| `name` | String | — | Nome di catalogo per la MV del fact |
| `source` | String | — | Relazione di origine |
| `domainId` | String | — | Id del dominio |
| `grain` | [String] | — | Colonna/e di grain per la GROUP BY |
| `measures` | [MeasureInput] | `[]` | Coppie `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Coppie `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Elenco di visibilità per ruolo |

`MeasureInput`: `{ column: String, agg: String }`; agg è `"sum"` per impostazione predefinita.

`DimRefInput`: `{ entity: String, via: String }`; `entity` è il nome dell'entity referenziata; `via` è la colonna FK sull'origine del fact.

In caso di successo, `registerFact` restituisce un messaggio nella forma:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Star schema di Kimball e Data Vault {: #kimball-star-schema-and-data-vault }

Nessuno dei due pattern richiede strumenti separati. Le stesse due primitive si compongono per
formare entrambi.

### Star schema di Kimball {: #kimball-star-schema }

Questo percorso costruisce una stella a tre dimensioni. Due tabelle di origine sono nuove:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

Anche `raw.orders` acquisisce qui le colonne `product_id` e `order_date`.

#### Scegliere il tipo di SCD {: #choosing-scd-type }

La modalità di history è l'unico parametro che distingue SCD Type 1 da Type 2:

| Tipo SCD | Modalità di history | Effetto |
| --- | --- | --- |
| Type 1 (solo corrente) | `none` | La MV viene ricostruita a ogni aggiornamento; nessuna history delle righe |
| Type 2 (versionato) | `scd2` | MV delta bitemporale; ogni modifica aggiunge una nuova riga, con chiave sulla chiave dell'entity |

Usate `scd2` quando le query a valle devono unire una dimensione com'era al momento della transazione, come il livello di un cliente al momento dell'acquisto invece di quello attuale. Usate `none` per tabelle di lookup stabili. Un asse delle date non cambia mai, e un catalogo prodotti in cui conta solo il prezzo attuale può essere ricostruito a ogni aggiornamento.

#### Decisione del grain {: #grain-decision }

La granularità è il livello di dettaglio più basso a cui il fatto risponde. `order_id` dà una riga per ordine e conserva la possibilità di contare gli ordini distinti e di unire qualsiasi dimensione a livello di ordine. Una granularità più grossa, per esempio `["customer_id", "order_date"]`, preaggrega tra ordini e perde quel dettaglio per sempre. Dichiarate la granularità più fine che serve al business; le aggregazioni più grossolane sono economiche da ricavare dopo.

#### Registrare le dimensioni {: #register-the-dimensions }

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

Genera una MV delta bitemporale con chiave su `id`:

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

Genera una MV ordinaria ricostruita a ogni aggiornamento:

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

#### Registrare il fact Sales su tre dimensioni {: #register-the-sales-fact-across-three-dimensions }

Granularità: `order_id`. Tre riferimenti di dimensione, con una colonna FK ciascuno. Entrambe le misure sono somme additive.

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
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Vengono registrate automaticamente tre relazioni:

| Relazione | Cardinalità |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensioni conformi {: #conformed-dimensions }

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
definizione di dimensione.

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

#### Hub {: #hubs }

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

Genera:

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

Genera:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` e `CustomerHub` hanno entrambi chiave su `id`. L'hub è il target di join
stabile; il satellite fornisce accesso agli attributi in un punto nel tempo tramite il livello
bitemporale.

#### Link (fact senza misure) {: #link-measureless-fact }

Un link registra quali chiavi di hub sono comparse insieme. Contiene solo chiavi e nessuna misura, e Provisa omette il `GROUP BY` quando `measures` è vuoto.

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
Nessuna misura, quindi nessuna `GROUP BY`. Genera:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Vengono registrate due relazioni: `OrderCustomerLink.customer_id → CustomerHub` e
`OrderCustomerLink.order_id → OrderHub`, entrambe many-to-one
.

#### Bridge / link aggregato {: #bridge-aggregate-link }

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(l'`order_id` duplicato dell'elenco delle dimensioni viene scartato da
`_dedup`). Genera:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Il modello non decide la metodologia. Grain, conformità, scelta dello SCD e suddivisione
hub/satellite restano decisioni del modellatore. Provisa le esegue.

## Metriche {: #metrics }

Una **metrica** è una definizione di aggregato con nome e governata, senza una granularità propria. La granularità, cioè le dimensioni secondo cui l'aggregato viene scomposto, la fissa chi interroga al momento della query e non alla definizione. È questo che distingue una metrica da una vista: una vista fissa la granularità alla creazione, una metrica resta aperta finché non viene interrogata.

### L'oggetto Metric {: #the-metric-object }

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| `name` | sì | snake_case, es. `net_revenue`. Validato: `[a-z][a-z0-9_]*` |
| `expression` | sì | ANSI-SQL aggregato; deve includere almeno una funzione di aggregazione |
| `datatype` | no | Suggerimento del tipo di risultato, es. `number`, `integer` |
| `description` | no | Definizione di business leggibile da un essere umano |
| `ai_context` | no | Testo per i consumatori IA — proiettato su strumenti MCP, pg_description, documentazione GraphQL ed export Ossie |
| `visible_to` | no | Elenco di ruoli; il valore predefinito è `["*"]` (tutti i ruoli) |
| `from_fact` | — | Impostato automaticamente quando la metrica è stata generata da una misura di un fact |

I riferimenti alle colonne nell'espressione devono essere qualificati con la loro tabella (`orders.amount`, non solo `amount`). Una colonna non qualificata è un errore bloccante quando la metrica viene espansa, e niente la lascia passare con un semplice avviso.

Il repository delle metriche valida l'espressione a ogni scrittura. Un'espressione che non
viene analizzata correttamente o che non contiene alcuna funzione di aggregazione viene
rifiutata; non viene mai memorizzata.

Esempio di voce di configurazione:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Interrogare una metrica {: #querying-a-metric }

Il compilatore riserva lo schema `metrics`, e ogni metrica è indirizzabile come relazione virtuale al suo interno. Interrogatela come una tabella, e le colonne selezionate diventano l'insieme delle dimensioni e il GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Il compilatore riscrive questo in un vero aggregato raggruppato sulle tabelle semantiche
sottostanti prima che venga eseguita la governance, in modo che RLS e mascheramento si
applichino alle colonne reali.

`SELECT *` su una relazione di metrica viene rifiutato. Nominate esplicitamente le colonne di dimensione e `value`.

Quando l'espressione di una metrica si estende su più tabelle, il compilatore le unisce
attraverso le relazioni registrate. Una dimensione che è una colonna di una tabella
referenziata direttamente si risolve verso quella tabella. Una dimensione a un salto di
relazione di distanza viene unita automaticamente. Due salti o una dimensione ambigua sono un
errore bloccante che nomina il responsabile.

### Metriche da specifiche di fact {: #metrics-from-fact-specs }

Quando si registra un fact, ogni misura dichiarata registra automaticamente un oggetto Metric
corrispondente. Il campo `from_fact` della metrica registra il nome della tabella di fact di
origine, e le dimensioni di raggruppamento valide sono gli attributi dell'entity raggiungibili
attraverso le relazioni FK del fact.

Le metriche registrate automaticamente compaiono nella pagina Metrics con un badge **fact**.
Possono essere modificate come qualsiasi altra metrica.

### Viste composte da metriche (view_metrics) {: #metric-composed-views-view_metrics }

Una vista `view_metrics` fissa il grain di una metrica al momento della definizione.
Dichiarare i nomi delle metriche, le colonne di dimensione e i filtri opzionali; il compilatore
genera il SELECT.

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

**Rigenerazione automatica al cambio di una metrica.** Quando l'espressione di una metrica
viene aggiornata, ogni vista `view_metrics` che la referenzia viene ricompilata e il nuovo SQL
viene persistito immediatamente. La vista non può divergere dalla definizione della metrica per
costruzione.

**Chiamate `metric()` inline in SQL di vista scritto a mano.** Un `view_sql` scritto a mano può
anche referenziare metriche tramite `metric('name')`. Il compilatore sostituisce ogni chiamata
con l'espressione della metrica e registra un edge di lineage. Questo dà alle viste scritte a
mano la stessa proprietà di ricompilazione al cambiamento quando referenziano una metrica
invece di copiarne la formula.

Nota: le viste del file di configurazione che usano chiamate `metric()` inline si rigenerano quando la configurazione viene ricaricata, e un upsert della metrica non le rigenera.

### La pagina di amministrazione Metrics {: #the-metrics-admin-page }

Aprire la voce di navigazione **Metrics** per gestire le metriche governate. Fare clic su una
riga per espandere un pannello di dettaglio in sola lettura; fare clic su **Edit** al suo
interno per passare alla modifica inline (nessuna finestra modale). **New Metric** apre una
scheda di creazione inline sopra la tabella. La conferma di eliminazione è l'unica finestra
modale della pagina.

Il modulo di creazione/modifica offre un builder a tre selettori per le metriche originate da
fact: scegliere la tabella di fact di origine (filtrata su `modelingRole=fact`), una colonna di
misura e una funzione di aggregazione (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Il datatype viene
derivato automaticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type` (il tipo della colonna di misura). L'area di testo dell'espressione resta la via di fuga per espressioni
arbitrarie.

## Il vantaggio dell'IR {: #the-ir-payoff }

Ogni chiamata di registrazione segue lo stesso percorso di una MV scritta a mano. La specifica di entity e fact è una rappresentazione intermedia; non è un template né una macro. Il warehouse di destinazione è una proprietà del deployment, e il modello non lo nomina. Cambiate il motore di destinazione, e le stesse dichiarazioni `entity` / `fact` si materializzano lì, perché l'SQL generato e le modalità bitemporali sono neutrali rispetto al motore per costruzione.
