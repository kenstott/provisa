<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Datenmodellierung (Entities & Facts)

Provisa bietet zwei deklarative Primitive — `entity` und `fact` — die alle Bausteine abdecken,
aus denen jedes Star Schema und jedes Data Vault zusammengesetzt ist. Deklarieren Sie die
Spezifikation; Provisa übersetzt sie exakt in die materialisierten Sichten, bitemporalen und
Beziehungsdefinitionen, die Sie andernfalls von Hand schreiben müssten (REQ-1164).
[tool-verified: modeling.py module docstring lines 11-28]

## Was Entities und Facts sind

Eine **entity** ist eine schlüsselbasierte, deduplizierte, optional historisierte Projektion
einer Quellrelation. Sie vergeben einen Namen, verweisen auf eine Quelle, deklarieren den
Entity-Schlüssel und die gewünschten Attribute und wählen einen History-Modus. Provisa schreibt
das SQL der Sicht und registriert die MV (materialisierte Sicht). Ist History aktiviert, ist die
MV bitemporal. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

Ein **fact** ist ein Join auf Entity-Schlüssel, reduziert auf einen deklarierten Grain, mit
aggregierten Measures. Provisa schreibt eine aggregierende MV-Abfrage (`GROUP BY` über Grain +
FK-Spalten) und registriert für jeden deklarierten Dimensionslink eine Beziehung. Ein Fact ohne
Measures ist eine reine Schlüsselmenge — das Data-Vault-Link-Muster. [tool-verified: `Fact`
dataclass, modeling.py lines 91-102; `fact_registration` function, modeling.py lines 123-141;
comment at line 130 "a measureless fact is a pure key-set (DV link)"]

Beide Konstrukte sind IR (Intermediate Representation). Die generierten Definitionen lassen
sich engineübergreifend neu ausrichten — materialisiert in Oracle, Databricks oder virtuell über
einer MPP-Engine belassen — ohne Neumodellierung. [tool-verified: modeling.py docstring lines
25-28]

## History-Modi

Für eine Entity stehen drei Modi zur Verfügung [tool-verified: `_HISTORY` constant at
modeling.py line 38, `_HISTORY_MODE` dict at modeling.py line 40]:

| Modus | Bedeutung | Bitemporaler Modus |
| --- | --- | --- |
| `none` | Nur aktueller Stand. Keine History. | — |
| `scd2` | Erfasst jede Änderung. Hängt nur geänderte Zeilen (Delta) an, mit dem Entity-Schlüssel als Schlüssel. | `delta` |
| `snapshot` | Erfasst jede Aktualisierung. Hängt bei jeder Aktualisierung die vollständige Ergebnismenge an, mit Systemzeit-Stempel. | `snapshot` |

`scd2` benötigt einen Entity-Schlüssel, um das Delta zu berechnen. `snapshot` funktioniert auf
jeder Engine, aber der Speicherbedarf wächst pro Aktualisierung um eine vollständige Kopie.
Wählen Sie `scd2` für große, sich langsam ändernde Quellen; wählen Sie `snapshot`, wenn Sie
vollständige History benötigen und die Quelle keinen Schlüssel liefern kann.

Facts haben keinen History-Modus — die zeitliche Abdeckung ergibt sich aus der History der
zugrunde liegenden Entity.

## Measures und Aggregationen

Measures werden als `column:agg`-Paare deklariert. Unterstützte Aggregationen [tool-verified:
`_AGGS` at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

Die Standardaggregation ist `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Durchgerechnetes Beispiel: Entity Customer + Fact Sales

### Die Quelltabellen

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Die Entity Customer registrieren

=== "Admin UI"

    1. Öffnen Sie **Tables** und klicken Sie auf **+ Model**.
    2. Wählen Sie **Entity (dimension)**.
    3. Füllen Sie das Formular aus:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(Ihre Domain)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
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
      }) {
        success
        message
      }
    }
    ```

Provisa generiert und registriert diese bitemporale MV [tool-verified: `entity_registration`
in modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Den Fact Sales registrieren

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

Provisa generiert und registriert [tool-verified: `fact_registration` in modeling.py lines
123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Zusätzlich wird eine Beziehung registriert: `Sales.customer_id → Customer` (Kardinalität:
many-to-one). [tool-verified: `fact_table_input` in modeling_register.py lines 89-98,
cardinality at line 95]

## Das Model-Formular (Admin UI)

Die Schaltfläche **+ Model** erscheint auf der Seite **Tables** (Tooltip: "Model an entity or
fact (star schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx
lines 441-450]

Ein Segmented Control oben im Dialog wechselt zwischen **Entity (dimension)** und **Fact**.
[tool-verified: ModelingForm.tsx lines 102-110]

### Entity-Felder

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| Name | ja | Der MV-Name im Katalog |
| Source relation | ja | Punktnotierte Relation, z. B. `raw.customers` |
| Domain | ja | Domain, zu der die MV gehört |
| Entity key | ja | Komma-getrennte Schlüsselspalte(n), z. B. `id` |
| Attributes | nein | Komma-getrennte Attributspalten, z. B. `name, region, tier` |
| History | nein | `none` / `scd2` / `snapshot`; Standard ist `none` |

### Fact-Felder

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| Name | ja | Der MV-Name im Katalog |
| Source relation | ja | Punktnotierte Relation, z. B. `raw.orders` |
| Domain | ja | Domain, zu der die MV gehört |
| Grain | ja | Komma-getrennte Grain-Spalte(n), z. B. `order_id` |
| Measures | nein | Komma-getrennte `col:agg`-Paare, z. B. `amount:sum, quantity:sum` |
| Dimensions | nein | Komma-getrennte `Entity:fk_column`-Paare, z. B. `Customer:customer_id` |

Wird `agg` in einem Measure weggelassen (`amount` statt `amount:sum`), gilt standardmäßig
`sum`. [tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## Die GraphQL API

Beide Mutations befinden sich im Admin-Schema. [tool-verified: schema_mutation.py lines
449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Felder von `EntityInput` [tool-verified: types.py lines 449-456]:

| Feld | Typ | Standard | Beschreibung |
| --- | --- | --- | --- |
| `name` | String | — | Katalogname für die Entity-MV |
| `source` | String | — | Quellrelation (`schema.table` oder in Anführungszeichen) |
| `domainId` | String | — | Domain-Id |
| `key` | [String] | — | Entity-Schlüsselspalte(n) |
| `attributes` | [String] | `[]` | Zu projizierende Attributspalten |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
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

Felder von `FactInput` [tool-verified: types.py lines 472-479]:

| Feld | Typ | Standard | Beschreibung |
| --- | --- | --- | --- |
| `name` | String | — | Katalogname für die Fact-MV |
| `source` | String | — | Quellrelation |
| `domainId` | String | — | Domain-Id |
| `grain` | [String] | — | Grain-Spalte(n) für die GROUP BY |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }`-Paare |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }`-Paare |
| `visibleTo` | [String] | `["public"]` | Liste der Rollen-Sichtbarkeit |

`MeasureInput`: `{ column: String, agg: String }` — agg ist standardmäßig `"sum"`.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` ist der Name der referenzierten
Entity; `via` ist die FK-Spalte in der Fact-Quelle.
[tool-verified: types.py lines 465-468]

Bei Erfolg liefert `registerFact` eine Meldung der Form:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Kimball Star Schema und Data Vault

Kein Muster erfordert separates Tooling. Dieselben zwei Primitive setzen sich zu beiden
zusammen.

### Kimball Star Schema

Dieser Durchlauf baut einen Stern mit drei Dimensionen. Zwei Quelltabellen sind neu:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for
  this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this
  example]

`raw.orders` erhält hier zusätzlich die Spalten `product_id` und `order_date`. [inferred]

#### Den SCD-Typ wählen

Der History-Modus ist der einzige Regler zwischen SCD Type 1 und Type 2:

| SCD-Typ | History-Modus | Effekt |
| --- | --- | --- |
| Type 1 (nur aktuell) | `none` | MV wird bei jeder Aktualisierung neu aufgebaut; keine Zeilen-History |
| Type 2 (versioniert) | `scd2` | Bitemporale Delta-MV; jede Änderung hängt eine neue Zeile an, mit dem Entity-Schlüssel als Schlüssel |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Verwenden Sie `scd2`, wenn nachgelagerte Abfragen eine Dimension so joinen müssen, wie sie zum
Transaktionszeitpunkt bestand — den Tier eines Kunden zum Kaufzeitpunkt, nicht den aktuellen
Tier. Verwenden Sie `none` für stabile Lookups. Eine Date Spine ändert sich nie. Ein
Produktkatalog, bei dem nur der aktuelle Preis benötigt wird, kann bei jeder Aktualisierung
neu aufgebaut werden.

#### Grain-Entscheidung

Der Grain ist die niedrigste Detailebene, die der Fact beantwortet. `order_id` liefert eine
Zeile pro Bestellung und erhält die Fähigkeit, eindeutige Bestellungen zu zählen und mit jeder
Dimension auf Bestellgranularität zu joinen. Ein gröberer Grain — etwa
`["customer_id", "order_date"]` — aggregiert über Bestellungen hinweg vor und verwirft dieses
Detail dauerhaft. Deklarieren Sie den schmalsten Grain, den das Geschäft benötigt; gröbere
Rollups lassen sich danach günstig ableiten.

#### Die Dimensionen registrieren

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
  }) { success message }
}
```

Generiert eine bitemporale Delta-MV mit `id` als Schlüssel [tool-verified: entity_registration
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
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
[tool-verified: entity_registration modeling.py lines 105-114; `mv_bitemporal_mode` is only
added when `history != "none"`, line 115]:

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

#### Den Fact Sales über drei Dimensionen registrieren

Grain: `order_id`. Drei Dimensionsreferenzen — je eine FK-Spalte. Beide Measures sind additive
Summen.

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
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Drei Beziehungen werden automatisch registriert [tool-verified: modeling_register.py lines
89-98, cardinality `"many_to_one"` at line 95]:

| Beziehung | Kardinalität |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Konforme Dimensionen

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
Dimensionsdefinition durchlaufen [tool-verified: fact_registration uses entity name as
`target_table` at modeling.py lines 138-140; fact_table_input wires `target_table_id` from
that name at modeling_register.py lines 91-93].

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

Generiert [tool-verified: entity_registration modeling.py lines 107-108;
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

Generiert:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Das Satellite sitzt neben dem Hub-Schlüssel und trägt die vollständige Attribut-History.
Verwenden Sie `scd2`, um nur geänderte Zeilen anzuhängen; verwenden Sie `snapshot`, um jede
vollständige Aktualisierung zu stempeln.

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

Generiert [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` und `CustomerHub` haben beide `id` als Schlüssel. Der Hub ist das stabile
Join-Ziel; das Satellite liefert punktgenauen Attributzugriff über die bitemporale Schicht.

#### Link (Fact ohne Measures)

Ein Link erfasst, welche Hub-Schlüssel gemeinsam aufgetreten sind — nur Schlüssel, keine
Measures. Provisa lässt die `GROUP BY` weg, wenn `measures` leer ist [tool-verified:
modeling.py lines 130-131: `if f.measures: view_sql += " GROUP BY ..."`].

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
"customer_id"]`. Keine Measures, also keine `GROUP BY`. Generiert [tool-verified:
fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Zwei Beziehungen werden registriert: `OrderCustomerLink.customer_id → CustomerHub` und
`OrderCustomerLink.order_id → OrderHub`, beide many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / aggregierter Link

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id",
"customer_id"]` (das doppelte `order_id` aus der Dimensionsliste wird von `_dedup` entfernt).
Generiert [tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Das Modell entscheidet nicht über die Methodik. Grain, Konformität, SCD-Wahl und die
Hub/Satellite-Aufteilung bleiben Entscheidungen des Modellierers. Provisa führt sie aus.
[tool-verified: modeling.py docstring lines 25-26]

## Metriken (REQ-1317, REQ-1318, REQ-1320)

Eine **Metrik** (metric) ist eine benannte, governte Aggregatdefinition ohne eigenen Grain. Der
Grain — die Dimensionen, nach denen das Aggregat aufgeschlüsselt wird — wird zum Abfragezeitpunkt
vom Aufrufer gebunden, nicht zum Definitionszeitpunkt. Das unterscheidet eine Metrik von einer
Sicht: Eine Sicht legt den Grain bei der Erstellung fest; eine Metrik bleibt offen, bis sie
abgefragt wird. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### Das Metric-Objekt

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Feld | Erforderlich | Hinweise |
| --- | --- | --- |
| `name` | ja | snake_case, z. B. `net_revenue`. Validiert: `[a-z][a-z0-9_]*` |
| `expression` | ja | Aggregierendes ANSI-SQL; muss mindestens eine Aggregatfunktion enthalten |
| `datatype` | nein | Hinweis auf den Ergebnistyp, z. B. `number`, `integer` |
| `description` | nein | Menschenlesbare Geschäftsdefinition |
| `ai_context` | nein | Text für KI-Konsumenten — projiziert auf MCP-Tools, pg_description, GraphQL-Dokumentation und Ossie-Export |
| `visible_to` | nein | Rollenliste; Standard ist `["*"]` (alle Rollen) |
| `from_fact` | — | Wird automatisch gesetzt, wenn die Metrik aus einem Fact-Measure generiert wurde |

Spaltenreferenzen innerhalb des Ausdrucks müssen tabellenqualifiziert sein (`orders.amount`,
nicht `amount`). Eine nicht qualifizierte Spalte ist zum Zeitpunkt der Expansion ein harter
Fehler, keine Warnung.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

Das Metrik-Repository validiert den Ausdruck bei jedem Schreibvorgang. Ein Ausdruck, der nicht
geparst werden kann oder keine Aggregatfunktion enthält, wird abgelehnt; er wird niemals
gespeichert.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Beispiel für einen Konfigurationseintrag:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Eine Metrik abfragen

Der Compiler reserviert das Schema `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Jede Metrik ist als virtuelle Relation innerhalb
dieses Schemas adressierbar. Fragen Sie sie wie eine Tabelle ab — die ausgewählten Spalten
werden zum Dimensionssatz und zur GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Der Compiler schreibt dies in ein echtes gruppiertes Aggregat über die zugrunde liegenden
semantischen Tabellen um, bevor die Governance läuft, sodass RLS und Maskierung auf die
tatsächlichen Spalten angewendet werden.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` gegen eine Metrik-Relation wird abgelehnt — benennen Sie die Dimensionsspalten und
`value` explizit. [tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py`
lines 302–306]

Erstreckt sich der Ausdruck einer Metrik über mehrere Tabellen, joint der Compiler diese über
registrierte Beziehungen. Eine Dimension, die eine Spalte einer direkt referenzierten Tabelle
ist, wird auf diese Tabelle aufgelöst. Eine Dimension einen Beziehungssprung entfernt wird
automatisch gejoint. Zwei Sprünge oder eine mehrdeutige Dimension sind ein harter Fehler, der
den Verursacher benennt.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### Metriken aus Fact-Spezifikationen (REQ-1320)

Wenn Sie einen Fact registrieren, registriert jedes deklarierte Measure automatisch ein
entsprechendes Metric-Objekt. Das Feld `from_fact` der Metrik erfasst den Namen der
Quell-Fact-Tabelle, und die gültigen Gruppierungsdimensionen sind die Entity-Attribute, die
über die FK-Beziehungen des Facts erreichbar sind.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Automatisch registrierte Metriken erscheinen auf der Metrics-Seite mit einem **fact**-Badge.
Sie können wie jede andere Metrik bearbeitet werden. [tool-verified: `MetricsPage.tsx` lines
405–408: `{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### Aus Metriken zusammengesetzte Sichten (view_metrics, REQ-1318)

Eine `view_metrics`-Sicht legt den Grain einer Metrik zum Definitionszeitpunkt fest.
Deklarieren Sie die Metriknamen, Dimensionsspalten und optionale Filter; der Compiler generiert
das SELECT.

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

Der Compiler generiert (für dieses Beispiel):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` und `view_sql` schließen sich auf derselben Tabelle gegenseitig aus.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Automatische Neugenerierung bei Metrikänderung.** Wenn der Ausdruck einer Metrik aktualisiert
wird, wird jede `view_metrics`-Sicht, die sie referenziert, neu kompiliert, und das neue SQL
wird sofort persistiert. Die Sicht kann von der Metrikdefinition konstruktionsbedingt nicht
abweichen.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Inline-`metric()`-Aufrufe in frei geschriebenem View-SQL.** Handgeschriebenes `view_sql` kann
Metriken auch über `metric('name')` referenzieren. Der Compiler ersetzt jeden Aufruf durch den
Ausdruck der Metrik und erfasst eine Lineage-Kante. Dadurch erhalten frei geschriebene Sichten
dieselbe Eigenschaft der automatischen Neukompilierung bei Änderung, wenn sie eine Metrik
referenzieren, statt deren Formel zu kopieren.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

Hinweis: Sichten aus dem Konfigurationspfad, die Inline-`metric()`-Aufrufe verwenden, werden
beim Konfigurations-Reload neu generiert, nicht beim Upsert der Metrik. [tool-verified:
`regenerate_metric_views` docstring, `_metric_views.py` lines 84–86: "Free-hand view_sql born
from inline metric() calls carries no stored provenance, so it is not regenerated here
(config-path views regenerate on config reload)"]

### Die Metrics-Admin-Seite (REQ-1323, REQ-1324)

Öffnen Sie den Navigationseintrag **Metrics**, um governte Metriken zu verwalten. Klicken Sie
auf eine Zeile, um ein schreibgeschütztes Detailpanel aufzuklappen; klicken Sie darin auf
**Edit**, um zur Inline-Bearbeitung zu wechseln (kein Dialog). **New Metric** öffnet eine
Inline-Erstellungskarte oberhalb der Tabelle. Die Löschbestätigung ist der einzige Dialog auf
der Seite.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

Das Erstellungs-/Bearbeitungsformular bietet einen Drei-Wähler-Builder für Fact-basierte
Metriken: Wählen Sie die Quell-Fact-Tabelle (gefiltert auf `modelingRole=fact`), eine
Measure-Spalte und eine Aggregatfunktion (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). Der Datentyp
wird automatisch abgeleitet: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → der Typ der
Measure-Spalte`. Das Ausdrucks-Textfeld bleibt das Hintertürchen für beliebige Ausdrücke.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## Der Nutzen der IR

Jeder Registrierungsaufruf durchläuft denselben Pfad wie eine handgeschriebene MV. Die
Entity-/Fact-Spezifikation ist eine Zwischendarstellung — kein Template, kein Makro. Das
Warehouse, auf das sie zielt, ist eine Eigenschaft der Bereitstellung, nicht des Modells.
Ändern Sie die Ziel-Engine, und dieselben `entity`-/`fact`-Deklarationen materialisieren dort,
weil das generierte SQL und die bitemporalen Modi konstruktionsbedingt engine-neutral sind.
[tool-verified: modeling.py docstring lines 25-28; modeling_register.py lines 56-66, 80-88]
