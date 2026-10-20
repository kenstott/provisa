<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Datenmodellierung (Entities & Facts) {: #data-modeling-entities-facts }

Provisa gibt Ihnen zwei deklarative Grundbausteine, `entity` und `fact`, die alles abdecken, woraus jedes Sternschema und jedes Data Vault zusammengesetzt ist. Deklarieren Sie die Spezifikation, und Provisa übersetzt sie in genau die Definitionen für Materialized Views, Bitemporalität und Beziehungen, die Sie sonst von Hand schreiben würden.

## Was Entities und Facts sind {: #what-entities-and-facts-are }

Eine **entity** ist eine schlüsselbasierte, deduplizierte, optional historisierte Projektion
einer Quellrelation. Sie vergeben einen Namen, verweisen auf eine Quelle, deklarieren den
Entity-Schlüssel und die gewünschten Attribute und wählen einen History-Modus. Provisa schreibt
das SQL der Sicht und registriert die MV (materialisierte Sicht). Ist History aktiviert, ist die
MV bitemporal.
Sie deklariert dann den Entity-Schlüssel als `business_key` zusammen mit den Spalten für Gültigkeits- und Aufzeichnungszeit, die Sie angeben.

Ein **Fakt** ist ein Join auf Entitätsschlüssel, reduziert auf eine deklarierte Granularität, mit aggregierten Kennzahlen. Provisa schreibt eine Aggregat-MV-Abfrage (`GROUP BY` Granularität + FK-Spalten) und registriert für jede deklarierte Dimensionsverknüpfung eine Beziehung. Ein Fakt ohne Kennzahlen ist eine reine Schlüsselmenge, das Link-Muster von Data Vault.

Beide Konstrukte sind IR. Die erzeugten Definitionen wechseln ohne Neumodellierung zwischen Engines: materialisiert in Oracle oder Databricks oder virtuell über einer MPP-Engine.

## History-Modi {: #history-modes }

Für eine Entity stehen drei Modi zur Verfügung:

| Modus | Bedeutung |
| --- | --- |
| `none` | Nur aktueller Stand. Keine History. |
| `scd2` | Erfasst jede Änderung, mit dem Entity-Schlüssel als Schlüssel. |
| `snapshot` | Erfasst jede Aktualisierung, mit dem Entity-Schlüssel als Schlüssel. |

`scd2` und `snapshot` behalten beide jede Version, und Provisa wählt, wie sie gespeichert werden. Beide brauchen eine Spalte für die Gültigkeitszeit und eine für die Aufzeichnungszeit (`validTime`, `recordedTime`): das Datum, das Ihr Unternehmen dem Datensatz gibt, und wann diese Version erfasst oder geändert wurde. [Sichten mit Historie](configuration.md#views-with-history-time-travel) erklärt die beiden Zeiten.

Fakten haben keinen eigenen Historienmodus; ihre zeitliche Abdeckung kommt aus der Historie der darunterliegenden Entitäten.

## Measures und Aggregationen {: #measures-and-aggregations }

Measures werden als `column:agg`-Paare deklariert. Unterstützte Aggregationen:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

Die Standardaggregation ist `sum`.

## Durchgerechnetes Beispiel: Entity Customer + Fact Sales {: #worked-example-customer-entity-sales-fact }

### Die Quelltabellen {: #the-source-tables }

- `raw.customers` — id, name, region, tier, effective_date, updated_at
- `raw.orders` — order_id, customer_id, amount, quantity

### Die Entity Customer registrieren {: #register-the-customer-entity }

=== "Admin UI"

    1. Öffnen Sie **Tables** und klicken Sie auf **+ Model**.
    2. Wählen Sie **Entity (dimension)**.
    3. Füllen Sie das Formular aus:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(Ihre Domain)*
       - **Business key:** `id`
       - **Attributes:** `name, region, tier`
       - **Keep history:** `Bitemporal`
       - **Valid time:** `effective_date`
       - **Recorded time:** `updated_at`
    4. Klicken Sie auf **Create**.

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
        validTime: "effective_date"
        recordedTime: "updated_at"
      }) {
        success
        message
      }
    }
    ```

Provisa generiert und registriert diese bitemporale MV:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- registered as a bitemporal MV: business_key ["id"], valid_time effective_date,
-- recorded_time updated_at
```

### Den Fact Sales registrieren {: #register-the-sales-fact }

=== "Admin UI"

    1. Klicken Sie erneut auf **+ Model**.
    2. Wählen Sie **Fact**.
    3. Füllen Sie das Formular aus:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(Ihre Domain)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Klicken Sie auf **Create**.

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

Provisa generiert und registriert:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Zusätzlich wird eine Beziehung registriert: `Sales.customer_id → Customer` (Kardinalität:
many-to-one).

## Das Model-Formular (Admin UI) {: #the-model-form-admin-ui }

Die Schaltfläche **+ Model** erscheint auf der Seite **Tables** (Tooltip: "Model an entity or
fact (star schema / Data Vault)").

Ein Segmented Control oben im Dialog wechselt zwischen **Entity (dimension)** und **Fact**.

### Entity-Felder {: #entity-fields }

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| Name | ja | Der MV-Name im Katalog |
| Source relation | ja | Punktnotierte Relation, z. B. `raw.customers` |
| Domain | ja | Domain, zu der die MV gehört |
| Entity key | ja | Komma-getrennte Schlüsselspalte(n), z. B. `id` |
| Attributes | nein | Komma-getrennte Attributspalten, z. B. `name, region, tier` |
| History | nein | `none` / `scd2` / `snapshot`; Standard ist `none` |
| Valid time | mit History | Die Spalte mit dem Datum, das Ihr Unternehmen dem Datensatz gibt |
| Recorded time | mit History | Die Spalte mit dem Zeitpunkt, zu dem diese Version erfasst oder geändert wurde |

### Fact-Felder {: #fact-fields }

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| Name | ja | Der MV-Name im Katalog |
| Source relation | ja | Punktnotierte Relation, z. B. `raw.orders` |
| Domain | ja | Domain, zu der die MV gehört |
| Grain | ja | Komma-getrennte Grain-Spalte(n), z. B. `order_id` |
| Measures | nein | Komma-getrennte `col:agg`-Paare, z. B. `amount:sum, quantity:sum` |
| Dimensions | nein | Komma-getrennte `Entity:fk_column`-Paare, z. B. `Customer:customer_id` |

Wird `agg` in einem Measure weggelassen (`amount` statt `amount:sum`), gilt standardmäßig
`sum`.

## Die GraphQL API {: #the-graphql-api }

Beide Mutations befinden sich im Admin-Schema.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Felder von `EntityInput`:

| Feld | Typ | Standard | Beschreibung |
| --- | --- | --- | --- |
| `name` | String | — | Katalogname für die Entity-MV |
| `source` | String | — | Quellrelation (`schema.table` oder in Anführungszeichen) |
| `domainId` | String | — | Domain-Id |
| `key` | [String] | — | Entity-Schlüsselspalte(n) |
| `attributes` | [String] | `[]` | Zu projizierende Attributspalten |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `validTime` | String | `null` | Spalte der Gültigkeitszeit; erforderlich, wenn `history` nicht `"none"` ist |
| `recordedTime` | String | `null` | Spalte der Aufzeichnungszeit; erforderlich, wenn `history` nicht `"none"` ist |
| `visibleTo` | [String] | `["public"]` | Liste der Rollen-Sichtbarkeit |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Felder von `FactInput`:

| Feld | Typ | Standard | Beschreibung |
| --- | --- | --- | --- |
| `name` | String | — | Katalogname für die Fact-MV |
| `source` | String | — | Quellrelation |
| `domainId` | String | — | Domain-Id |
| `grain` | [String] | — | Grain-Spalte(n) für die GROUP BY |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }`-Paare |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }`-Paare |
| `visibleTo` | [String] | `["public"]` | Liste der Rollen-Sichtbarkeit |

`MeasureInput`: `{ column: String, agg: String }`; agg ist standardmäßig `"sum"`.

`DimRefInput`: `{ entity: String, via: String }`; `entity` ist der Name der referenzierten Entity; `via` ist die FK-Spalte in der Fact-Quelle.

Bei Erfolg liefert `registerFact` eine Meldung der Form:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Kimball Star Schema und Data Vault {: #kimball-star-schema-and-data-vault }

Kein Muster erfordert separates Tooling. Dieselben zwei Primitive setzen sich zu beiden
zusammen.

### Kimball Star Schema

Dieser Durchlauf baut einen Stern mit drei Dimensionen. Zwei Quelltabellen sind neu:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` erhält hier zusätzlich die Spalten `product_id` und `order_date`.

#### Den SCD-Typ wählen {: #choosing-scd-type }

Der History-Modus ist der einzige Regler zwischen SCD Type 1 und Type 2:

| SCD-Typ | History-Modus | Effekt |
| --- | --- | --- |
| Type 1 (nur aktuell) | `none` | MV wird bei jeder Aktualisierung neu aufgebaut; keine Zeilen-History |
| Type 2 (versioniert) | `scd2` | Bitemporale MV; jede Änderung fügt eine neue Version mit dem Entity-Schlüssel als Schlüssel hinzu |

Nehmen Sie `scd2`, wenn nachgelagerte Abfragen eine Dimension so joinen müssen, wie sie zur Zeit der Transaktion war, etwa die Stufe eines Kunden im Moment des Kaufs statt seiner aktuellen Stufe. Nehmen Sie `none` für stabile Nachschlagetabellen. Eine Datumsachse ändert sich nie, und ein Produktkatalog, bei dem nur der aktuelle Preis zählt, kann bei jeder Aktualisierung neu aufgebaut werden.

#### Grain-Entscheidung {: #grain-decision }

Die Granularität ist die feinste Detailebene, die der Fakt beantwortet. `order_id` ergibt eine Zeile pro Bestellung und erhält die Möglichkeit, verschiedene Bestellungen zu zählen und auf Bestellebene mit jeder Dimension zu joinen. Eine gröbere Granularität, etwa `["customer_id", "order_date"]`, aggregiert über Bestellungen vor und verwirft dieses Detail endgültig. Deklarieren Sie die feinste Granularität, die das Geschäft braucht; gröbere Zusammenfassungen lassen sich später billig ableiten.

#### Die Dimensionen registrieren {: #register-the-dimensions }

**Customer** (SCD Type 2 — Tier-Änderungen müssen erhalten bleiben):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

Generiert eine bitemporale MV mit `id` als Schlüssel:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
```

**Product** (SCD Type 1 — aktueller Katalog, keine Versions-History erforderlich):

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

Generiert eine gewöhnliche MV, die bei jeder Aktualisierung neu aufgebaut wird
:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (keine History — ein Datum ist unveränderlich):

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

Generiert:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Den Fact Sales über drei Dimensionen registrieren {: #register-the-sales-fact-across-three-dimensions }

Granularität: `order_id`. Drei Dimensionsverweise mit je einer FK-Spalte. Beide Kennzahlen sind additive Summen.

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

Provisa berechnet `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` und generiert
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Drei Beziehungen werden automatisch registriert:

| Beziehung | Kardinalität |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Konforme Dimensionen {: #conformed-dimensions }

Eine konforme Dimension wird einmal registriert und von beliebig vielen Facts über ihren Namen
referenziert. Angenommen, `raw.returns` enthält `return_id`, `customer_id`, `product_id` und
`amount`. Der Fact Returns nutzt Customer und Product weiter, ohne sie neu zu registrieren:

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

Sowohl `Sales` als auch `Returns` verweisen auf dieselben Entities `Customer` und `Product`.
Die Join-Pfade von Provisa stellen sicher, dass Abfragen über beide Facts hinweg dieselbe
Dimensionsdefinition durchlaufen.

---

### Data Vault

Dieselben Primitive lassen sich direkt auf das Data-Vault-Vokabular abbilden:

| DV-Artefakt | Primitiv | History |
| --- | --- | --- |
| Hub | `entity` | `none` — nur Entity-Schlüssel |
| Satellite | `entity` | `scd2` oder `snapshot` — Attribut-History neben dem Hub-Schlüssel |
| Link | `fact` ohne Measures | — |
| Bridge / aggregierter Link | `fact` mit Measures | — |

Das Beispiel baut einen minimalen Vault über `raw.customers` und `raw.orders` auf.

#### Hubs

Ein Hub enthält den Entity-Schlüssel und sonst nichts. `attributes: []` mit `history: "none"`
erzeugt eine deduplizierte, aktuelle Schlüsselmenge; die Attribut-History liegt vollständig im
Satellite.

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

Generiert:

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

Generiert:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Das Satellite sitzt neben dem Hub-Schlüssel und trägt die vollständige Attribut-History. Verwenden Sie `scd2`, um jede Änderung zu erfassen, oder `snapshot`, um jede Aktualisierung zu erfassen.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
    validTime: "effective_date"
    recordedTime: "updated_at"
  }) { success message }
}
```

Generiert:

```sql
SELECT "id", "name", "region", "tier", "effective_date", "updated_at" FROM "raw"."customers"
-- bitemporal MV: business_key ["id"], valid_time effective_date, recorded_time updated_at
```

`CustomerSat` und `CustomerHub` haben beide `id` als Schlüssel. Der Hub ist das stabile
Join-Ziel; das Satellite liefert punktgenauen Attributzugriff über die bitemporale Schicht.

#### Link (Fact ohne Measures) {: #link-measureless-fact }

Ein Link hält fest, welche Hub-Schlüssel gemeinsam auftraten. Er enthält nur Schlüssel und keine Kennzahlen, und Provisa lässt das `GROUP BY` weg, wenn `measures` leer ist.

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
Keine Measures, also keine `GROUP BY`. Generiert:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Zwei Beziehungen werden registriert: `OrderCustomerLink.customer_id → CustomerHub` und
`OrderCustomerLink.order_id → OrderHub`, beide many-to-one
.

#### Bridge / aggregierter Link {: #bridge-aggregate-link }

Fügen Sie dem Link Measures hinzu, und Provisa gibt die `GROUP BY` aus, wodurch eine
vorab aggregierte Bridge entsteht. Beim Grain `order_id` mit einem Kunden pro Bestellung ist
das Ergebnis eine aggregierte Zeile pro Bestellung:

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
(das doppelte `order_id` aus der Dimensionsliste wird von `_dedup` entfernt).
Generiert:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Das Modell entscheidet nicht über die Methodik. Grain, Konformität, SCD-Wahl und die
Hub/Satellite-Aufteilung bleiben Entscheidungen des Modellierers. Provisa führt sie aus.

## Metriken {: #metrics }

Eine **Metrik** ist eine benannte, gesteuerte Aggregatdefinition ohne eigene Granularität. Die Granularität, also die Dimensionen, nach denen das Aggregat aufgeschlüsselt wird, legt der Aufrufer zur Abfragezeit fest und nicht bei der Definition. Das unterscheidet eine Metrik von einer Sicht: Eine Sicht legt die Granularität beim Erstellen fest, eine Metrik bleibt offen, bis sie abgefragt wird.

### Das Metric-Objekt {: #the-metric-object }

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| `name` | ja | snake_case, z. B. `net_revenue`. Validiert: `[a-z][a-z0-9_]*` |
| `expression` | ja | Aggregierendes ANSI-SQL; muss mindestens eine Aggregatfunktion enthalten |
| `datatype` | nein | Hinweis auf den Ergebnistyp, z. B. `number`, `integer` |
| `description` | nein | Menschenlesbare Geschäftsdefinition |
| `ai_context` | nein | Text für KI-Konsumenten — projiziert auf MCP-Tools, pg_description, GraphQL-Dokumentation und Ossie-Export |
| `visible_to` | nein | Rollenliste; Standard ist `["*"]` (alle Rollen) |
| `from_fact` | — | Wird automatisch gesetzt, wenn die Metrik aus einem Fact-Measure generiert wurde |

Spaltenverweise im Ausdruck müssen mit ihrer Tabelle qualifiziert sein (`orders.amount`, nicht nur `amount`). Eine unqualifizierte Spalte ist beim Expandieren der Metrik ein harter Fehler, und nichts lässt sie mit einer Warnung durch.

Das Metrik-Repository validiert den Ausdruck bei jedem Schreibvorgang. Ein Ausdruck, der nicht
geparst werden kann oder keine Aggregatfunktion enthält, wird abgelehnt; er wird niemals
gespeichert.

Beispiel für einen Konfigurationseintrag:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Eine Metrik abfragen {: #querying-a-metric }

Der Compiler reserviert das Schema `metrics`, und jede Metrik ist darin als virtuelle Relation adressierbar. Fragen Sie sie wie eine Tabelle ab; die ausgewählten Spalten werden zur Dimensionsmenge und zum GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Der Compiler schreibt dies in ein echtes gruppiertes Aggregat über die zugrunde liegenden
semantischen Tabellen um, bevor die Governance läuft, sodass RLS und Maskierung auf die
tatsächlichen Spalten angewendet werden.

`SELECT *` auf eine Metrik-Relation wird abgelehnt. Nennen Sie die Dimensionsspalten und `value` ausdrücklich.

Erstreckt sich der Ausdruck einer Metrik über mehrere Tabellen, joint der Compiler diese über
registrierte Beziehungen. Eine Dimension, die eine Spalte einer direkt referenzierten Tabelle
ist, wird auf diese Tabelle aufgelöst. Eine Dimension einen Beziehungssprung entfernt wird
automatisch gejoint. Zwei Sprünge oder eine mehrdeutige Dimension sind ein harter Fehler, der
den Verursacher benennt.

### Metriken aus Fact-Spezifikationen {: #metrics-from-fact-specs }

Wenn Sie einen Fact registrieren, registriert jedes deklarierte Measure automatisch ein
entsprechendes Metric-Objekt. Das Feld `from_fact` der Metrik erfasst den Namen der
Quell-Fact-Tabelle, und die gültigen Gruppierungsdimensionen sind die Entity-Attribute, die
über die FK-Beziehungen des Facts erreichbar sind.

Automatisch registrierte Metriken erscheinen auf der Metrics-Seite mit einem **fact**-Badge.
Sie können wie jede andere Metrik bearbeitet werden.

### Aus Metriken zusammengesetzte Sichten (view_metrics) {: #metric-composed-views-view_metrics }

Eine `view_metrics`-Sicht legt den Grain einer Metrik zum Definitionszeitpunkt fest.
Deklarieren Sie die Metriknamen, Dimensionsspalten und optionale Filter; der Compiler generiert
das SELECT.

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

Der Compiler generiert (für dieses Beispiel):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` und `view_sql` schließen sich auf derselben Tabelle gegenseitig aus.

**Automatische Neugenerierung bei Metrikänderung.** Wenn der Ausdruck einer Metrik aktualisiert
wird, wird jede `view_metrics`-Sicht, die sie referenziert, neu kompiliert, und das neue SQL
wird sofort persistiert. Die Sicht kann von der Metrikdefinition konstruktionsbedingt nicht
abweichen.

**Inline-`metric()`-Aufrufe in frei geschriebenem View-SQL.** Handgeschriebenes `view_sql` kann
Metriken auch über `metric('name')` referenzieren. Der Compiler ersetzt jeden Aufruf durch den
Ausdruck der Metrik und erfasst eine Lineage-Kante. Dadurch erhalten frei geschriebene Sichten
dieselbe Eigenschaft der automatischen Neukompilierung bei Änderung, wenn sie eine Metrik
referenzieren, statt deren Formel zu kopieren.

Hinweis: Sichten aus der Konfigurationsdatei, die Inline-Aufrufe von `metric()` nutzen, werden beim Neuladen der Konfiguration neu erzeugt; ein Upsert der Metrik erzeugt sie nicht neu.

### Die Metrics-Admin-Seite {: #the-metrics-admin-page }

Öffnen Sie den Navigationseintrag **Metrics**, um governte Metriken zu verwalten. Klicken Sie
auf eine Zeile, um ein schreibgeschütztes Detailpanel aufzuklappen; klicken Sie darin auf
**Edit**, um zur Inline-Bearbeitung zu wechseln (kein Dialog). **New Metric** öffnet eine
Inline-Erstellungskarte oberhalb der Tabelle. Die Löschbestätigung ist der einzige Dialog auf
der Seite.

Das Erstellungs-/Bearbeitungsformular bietet einen Drei-Wähler-Builder für Fact-basierte
Metriken: Wählen Sie die Quell-Fact-Tabelle (gefiltert auf `modelingRole=fact`), eine
Measure-Spalte und eine Aggregatfunktion (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Der Datentyp
wird automatisch abgeleitet: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type`
(der Typ der Measure-Spalte). Das Ausdrucks-Textfeld bleibt das Hintertürchen für beliebige Ausdrücke.

## Der Nutzen der IR {: #the-ir-payoff }

Jeder Registrierungsaufruf geht denselben Weg wie eine von Hand geschriebene MV. Die Spezifikation von Entity und Fact ist eine Zwischendarstellung; sie ist weder Vorlage noch Makro. Das Ziel-Warehouse ist eine Eigenschaft des Deployments, und das Modell nennt es nicht. Wechseln Sie die Ziel-Engine, und dieselben Deklarationen `entity` / `fact` materialisieren dort, weil das erzeugte SQL und die bitemporale History von Grund auf engine-neutral sind.
