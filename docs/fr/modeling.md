<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modélisation des données (Entities et Facts)

Provisa propose deux primitives déclaratives — `entity` et `fact` — qui couvrent les blocs de
construction avec lesquels tout star schema et tout Data Vault sont assemblés. Déclarez la
spécification ; Provisa la ramène exactement aux définitions de vue matérialisée, bitemporelles
et de relation que vous auriez sinon dû écrire vous-même (REQ-1164). [tool-verified: modeling.py
module docstring lines 11-28]

## Ce que sont les entities et les facts

Une **entity** est une projection à clé, dédupliquée et éventuellement historisée d'une relation
source. Vous lui donnez un nom, la pointez vers une source, déclarez la clé de l'entity et les
attributs à conserver, puis choisissez un mode d'historique. Provisa écrit le SQL de la vue et
enregistre la MV. Lorsque l'historique est activé, la MV est bitemporelle. [tool-verified:
`Entity` dataclass, modeling.py lines 53-69; `entity_registration` function, modeling.py lines
105-120]

Un **fact** est une jointure vers des clés d'entity, réduite à un grain déclaré, avec des
mesures agrégées. Provisa écrit une requête de MV agrégée (`GROUP BY` du grain + colonnes FK) et
enregistre une relation pour chaque lien de dimension déclaré. Un fact sans mesure est un pur
ensemble de clés — le motif de link du Data Vault. [tool-verified: `Fact` dataclass, modeling.py
lines 91-102; `fact_registration` function, modeling.py lines 123-141; comment at line 130
"a measureless fact is a pure key-set (DV link)"]

Les deux constructions sont de l'IR (représentation intermédiaire). Les définitions générées se
retargent d'un moteur à l'autre — matérialisées dans Oracle, Databricks, ou laissées virtuelles
au-dessus d'un moteur MPP — sans remodélisation. [tool-verified: modeling.py docstring lines
25-28]

## Modes d'historique

Trois modes sont disponibles sur une entity [tool-verified: `_HISTORY` constant at modeling.py
line 38, `_HISTORY_MODE` dict at modeling.py line 40] :

| Mode | Signification | Mode bitemporel |
| --- | --- | --- |
| `none` | État courant uniquement. Aucun historique. | — |
| `scd2` | Trace chaque changement. Ajoute uniquement les lignes modifiées (delta), avec pour clé la clé de l'entity. | `delta` |
| `snapshot` | Trace chaque rafraîchissement. Ajoute l'ensemble complet des résultats à chaque rafraîchissement, horodaté par le système. | `snapshot` |

`scd2` nécessite une clé d'entity pour calculer le delta. `snapshot` fonctionne sur n'importe
quel moteur, mais le stockage croît d'une copie complète à chaque rafraîchissement. Choisissez
`scd2` pour les sources volumineuses à évolution lente ; choisissez `snapshot` lorsque vous avez
besoin de l'historique complet et que la source ne peut pas fournir de clé.

Les facts n'ont pas de mode d'historique — la couverture temporelle provient de l'historique de
l'entity sous-jacente.

## Mesures et agrégations

Les mesures sont déclarées sous forme de paires `column:agg`. Agrégations prises en charge
[tool-verified: `_AGGS` at modeling.py line 41] :

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

L'agrégation par défaut est `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Exemple pratique : entity Customer + fact Sales

### Les tables source

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Enregistrer l'entity Customer

=== "Admin UI"

    1. Ouvrez **Tables** et cliquez sur **+ Model**.
    2. Choisissez **Entity (dimension)**.
    3. Remplissez le formulaire :
       - **Name :** `Customer`
       - **Source relation :** `raw.customers`
       - **Domain :** *(votre domaine)*
       - **Entity key :** `id`
       - **Attributes :** `name, region, tier`
       - **History :** `SCD2 (track changes — delta bitemporal)`
    4. Cliquez sur **Create**.

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

Provisa génère et enregistre cette MV bitemporelle [tool-verified: `entity_registration` in
modeling.py lines 105-120] :

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Enregistrer le fact Sales

=== "Admin UI"

    1. Cliquez à nouveau sur **+ Model**.
    2. Choisissez **Fact**.
    3. Remplissez le formulaire :
       - **Name :** `Sales`
       - **Source relation :** `raw.orders`
       - **Domain :** *(votre domaine)*
       - **Grain :** `order_id`
       - **Measures :** `amount:sum, quantity:sum`
       - **Dimensions :** `Customer:customer_id`
    4. Cliquez sur **Create**.

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

Provisa génère et enregistre [tool-verified: `fact_registration` in modeling.py lines 123-141] :

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Plus une relation enregistrée : `Sales.customer_id → Customer` (cardinalité : many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## Le formulaire Model (interface d'administration)

Le bouton **+ Model** apparaît sur la page **Tables** (infobulle : "Model an entity or fact
(star schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines
441-450]

Un contrôle segmenté en haut de la fenêtre modale bascule entre **Entity (dimension)** et
**Fact**. [tool-verified: ModelingForm.tsx lines 102-110]

### Champs Entity

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| Name | oui | Le nom de la MV dans le catalogue |
| Source relation | oui | Relation en notation pointée, p. ex. `raw.customers` |
| Domain | oui | Domaine auquel appartient la MV |
| Entity key | oui | Colonne(s) clé séparée(s) par des virgules, p. ex. `id` |
| Attributes | non | Colonnes d'attributs séparées par des virgules, p. ex. `name, region, tier` |
| History | non | `none` / `scd2` / `snapshot` ; la valeur par défaut est `none` |

### Champs Fact

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| Name | oui | Le nom de la MV dans le catalogue |
| Source relation | oui | Relation en notation pointée, p. ex. `raw.orders` |
| Domain | oui | Domaine auquel appartient la MV |
| Grain | oui | Colonne(s) de grain séparée(s) par des virgules, p. ex. `order_id` |
| Measures | non | Paires `col:agg` séparées par des virgules, p. ex. `amount:sum, quantity:sum` |
| Dimensions | non | Paires `Entity:fk_column` séparées par des virgules, p. ex. `Customer:customer_id` |

Lorsque `agg` est omis dans une mesure (`amount` au lieu de `amount:sum`), la valeur par défaut
est `sum`. [tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## L'API GraphQL

Les deux mutations résident dans le schéma d'administration. [tool-verified: schema_mutation.py
lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Champs de `EntityInput` [tool-verified: types.py lines 449-456] :

| Champ | Type | Par défaut | Description |
| --- | --- | --- | --- |
| `name` | String | — | Nom de catalogue pour la MV de l'entity |
| `source` | String | — | Relation source (`schema.table` ou entre guillemets) |
| `domainId` | String | — | Id du domaine |
| `key` | [String] | — | Colonne(s) clé de l'entity |
| `attributes` | [String] | `[]` | Colonnes d'attributs à projeter |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Liste de visibilité par rôle |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Champs de `FactInput` [tool-verified: types.py lines 472-479] :

| Champ | Type | Par défaut | Description |
| --- | --- | --- | --- |
| `name` | String | — | Nom de catalogue pour la MV du fact |
| `source` | String | — | Relation source |
| `domainId` | String | — | Id du domaine |
| `grain` | [String] | — | Colonne(s) de grain pour le GROUP BY |
| `measures` | [MeasureInput] | `[]` | Paires `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Paires `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Liste de visibilité par rôle |

`MeasureInput` : `{ column: String, agg: String }` — agg vaut `"sum"` par défaut.
[tool-verified: types.py lines 460-462]

`DimRefInput` : `{ entity: String, via: String }` — `entity` est le nom de l'entity référencée ;
`via` est la colonne FK sur la source du fact.
[tool-verified: types.py lines 465-468]

En cas de succès, `registerFact` renvoie un message de la forme :
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Star schema de Kimball et Data Vault

Aucun des deux modèles ne nécessite d'outillage distinct. Les deux mêmes primitives se
composent pour former l'un comme l'autre.

### Star schema de Kimball

Ce parcours construit une étoile à trois dimensions. Deux tables source sont nouvelles :

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for
  this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this
  example]

`raw.orders` gagne ici aussi les colonnes `product_id` et `order_date`. [inferred]

#### Choisir le type de SCD

Le mode d'historique est le seul paramètre qui distingue SCD Type 1 de Type 2 :

| Type SCD | Mode d'historique | Effet |
| --- | --- | --- |
| Type 1 (état courant uniquement) | `none` | La MV est reconstruite à chaque rafraîchissement ; aucun historique de lignes |
| Type 2 (versionné) | `scd2` | MV delta bitemporelle ; chaque changement ajoute une nouvelle ligne, avec pour clé la clé de l'entity |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Utilisez `scd2` lorsque les requêtes en aval doivent joindre une dimension telle qu'elle
existait au moment de la transaction — le tier d'un client au moment de l'achat, et non son
tier actuel. Utilisez `none` pour les tables de correspondance stables. Un date spine ne change
jamais. Un catalogue de produits pour lequel seul le prix actuel est nécessaire peut être
reconstruit à chaque rafraîchissement.

#### Décision du grain

Le grain est le niveau de détail le plus fin auquel le fact répond. `order_id` donne une ligne
par commande, préservant la capacité de compter des commandes distinctes et de joindre
n'importe quelle dimension au niveau de granularité de la commande. Un grain plus grossier —
par exemple `["customer_id", "order_date"]` — préagrège les commandes et perd ce détail de
manière définitive. Déclarez le grain le plus fin dont l'activité a besoin ; des agrégats plus
grossiers restent peu coûteux à dériver ensuite.

#### Enregistrer les dimensions

**Customer** (SCD Type 2 — les changements de tier doivent être conservés) :

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

Génère une MV delta bitemporelle avec pour clé `id` [tool-verified: entity_registration
modeling.py lines 105-120] :

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — catalogue courant, aucun historique de versions nécessaire) :

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

Génère une MV ordinaire reconstruite à chaque rafraîchissement [tool-verified:
entity_registration modeling.py lines 105-114; `mv_bitemporal_mode` is only added when
`history != "none"`, line 115] :

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (aucun historique — une date est immuable) :

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

Génère :

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Enregistrer le fact Sales sur trois dimensions

Grain : `order_id`. Trois références de dimension — une colonne FK chacune. Les deux mesures
sont des sommes additives.

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

Provisa calcule `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` et génère
[tool-verified: fact_registration modeling.py lines 125-131] :

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Trois relations sont enregistrées automatiquement [tool-verified: modeling_register.py lines
89-98, cardinality `"many_to_one"` at line 95] :

| Relation | Cardinalité |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensions conformes

Une dimension conforme est enregistrée une seule fois et référencée par nom depuis un nombre
quelconque de facts. Supposons que `raw.returns` contienne `return_id`, `customer_id`,
`product_id` et `amount`. Le fact Returns réutilise Customer et Product sans les réenregistrer :

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

`Sales` et `Returns` pointent tous deux vers les mêmes entities `Customer` et `Product`. Les
chemins de jointure de Provisa garantissent que les requêtes passant par l'un ou l'autre fact
traversent la même définition de dimension [tool-verified: fact_registration uses entity name
as `target_table` at modeling.py lines 138-140; fact_table_input wires `target_table_id` from
that name at modeling_register.py lines 91-93].

---

### Data Vault

Les mêmes primitives se mappent directement sur le vocabulaire Data Vault :

| Artefact DV | Primitive | Historique |
| --- | --- | --- |
| Hub | `entity` | `none` — clés d'entity uniquement |
| Satellite | `entity` | `scd2` ou `snapshot` — historique des attributs aux côtés de la clé du hub |
| Link | `fact` sans mesure | — |
| Bridge / link agrégé | `fact` avec mesures | — |

L'exemple construit un vault minimal sur `raw.customers` et `raw.orders`.

#### Hubs

Un hub ne contient que la clé de l'entity, rien d'autre. `attributes: []` avec
`history: "none"` produit un ensemble de clés courantes dédupliqué ; l'historique des attributs
vit entièrement dans le satellite.

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

Génère [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`] :

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

Génère :

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

Le satellite se place aux côtés de la clé du hub et porte l'historique complet des attributs.
Utilisez `scd2` pour n'ajouter que les lignes modifiées ; utilisez `snapshot` pour horodater
chaque rafraîchissement complet.

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

Génère [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40] :

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` et `CustomerHub` ont tous deux pour clé `id`. Le hub est la cible de jointure
stable ; le satellite fournit un accès aux attributs à un instant donné via la couche
bitemporelle.

#### Link (fact sans mesure)

Un link enregistre quelles clés de hub ont coexisté — uniquement des clés, aucune mesure.
Provisa omet le `GROUP BY` lorsque `measures` est vide [tool-verified: modeling.py lines
130-131: `if f.measures: view_sql += " GROUP BY ..."`].

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
"customer_id"]`. Aucune mesure, donc pas de `GROUP BY`. Génère [tool-verified:
fact_registration modeling.py lines 125-131] :

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Deux relations sont enregistrées : `OrderCustomerLink.customer_id → CustomerHub` et
`OrderCustomerLink.order_id → OrderHub`, toutes deux many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / link agrégé

Ajoutez des mesures au link et Provisa émet le `GROUP BY`, produisant un bridge préagrégé. Au
grain `order_id`, avec un client par commande, le résultat est une ligne agrégée par commande :

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
"customer_id"]` (le `order_id` dupliqué de la liste de dimensions est supprimé par `_dedup`).
Génère [tool-verified: fact_registration modeling.py lines 125-131] :

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Le modèle ne décide pas de la méthodologie. Le grain, la conformité, le choix du SCD et la
répartition hub/satellite restent des décisions du modélisateur. Provisa les exécute.
[tool-verified: modeling.py docstring lines 25-26]

## Métriques (REQ-1317, REQ-1318, REQ-1320)

Une **métrique** (metric) est une définition d'agrégat nommée et gouvernée, sans grain propre.
Le grain — les dimensions selon lesquelles l'agrégat est ventilé — est lié au moment de la
requête par l'appelant, et non au moment de la définition. C'est ce qui distingue une métrique
d'une vue : une vue fixe le grain à la création ; une métrique reste ouverte jusqu'à ce qu'elle
soit interrogée. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### L'objet Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| `name` | oui | snake_case, p. ex. `net_revenue`. Validé : `[a-z][a-z0-9_]*` |
| `expression` | oui | ANSI-SQL agrégatif ; doit inclure au moins une fonction d'agrégation |
| `datatype` | non | Indication du type de résultat, p. ex. `number`, `integer` |
| `description` | non | Définition métier lisible par un humain |
| `ai_context` | non | Texte destiné aux consommateurs IA — projeté vers les outils MCP, pg_description, la documentation GraphQL et l'export Ossie |
| `visible_to` | non | Liste de rôles ; la valeur par défaut est `["*"]` (tous les rôles) |
| `from_fact` | — | Défini automatiquement lorsque la métrique a été générée à partir d'une mesure de fact |

Les références de colonnes dans l'expression doivent être qualifiées par la table
(`orders.amount`, et non `amount`). Une colonne non qualifiée est une erreur bloquante au
moment de l'expansion, et non un simple avertissement.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

Le référentiel de métriques valide l'expression à chaque écriture. Une expression qui ne
s'analyse pas correctement ou qui ne contient aucune fonction d'agrégation est rejetée ; elle
n'est jamais stockée.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Exemple d'entrée de configuration :

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Interroger une métrique

Le compilateur réserve le schéma `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Chaque métrique est adressable comme une relation
virtuelle dans ce schéma. Interrogez-la comme une table — les colonnes sélectionnées
deviennent l'ensemble de dimensions et le GROUP BY :

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Le compilateur réécrit ceci en un véritable agrégat groupé sur les tables sémantiques
sous-jacentes avant l'exécution de la gouvernance, de sorte que la RLS et le masquage
s'appliquent aux colonnes réelles.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines
263–276: "BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` sur une relation de métrique est rejeté — nommez explicitement les colonnes de
dimension et `value`. [tool-verified: `expand_metric_query`,
`provisa/compiler/metric_expand.py` lines 302–306]

Lorsque l'expression d'une métrique s'étend sur plusieurs tables, le compilateur les joint via
les relations enregistrées. Une dimension qui est une colonne d'une table directement
référencée se résout vers cette table. Une dimension à un saut de relation est jointe
automatiquement. Deux sauts, ou une dimension ambiguë, constituent une erreur bloquante qui
nomme le responsable.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines
190–228]

### Métriques issues des spécifications de fact (REQ-1320)

Lors de l'enregistrement d'un fact, chaque mesure déclarée enregistre automatiquement un objet
Metric correspondant. Le champ `from_fact` de la métrique enregistre le nom de la table de
fact source, et les dimensions de regroupement valides sont les attributs d'entity accessibles
via les relations FK du fact.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Les métriques enregistrées automatiquement apparaissent sur la page Metrics avec un badge
**fact**. Elles peuvent être modifiées comme n'importe quelle autre métrique. [tool-verified:
`MetricsPage.tsx` lines 405–408: `{m.fromFact && <Badge ...
data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### Vues composées de métriques (view_metrics, REQ-1318)

Une vue `view_metrics` fixe le grain d'une métrique au moment de la définition. Déclarez les
noms des métriques, les colonnes de dimension et les filtres optionnels ; le compilateur
génère le SELECT.

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

Le compilateur génère (pour cet exemple) :

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` et `view_sql` s'excluent mutuellement sur une même table.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Régénération automatique lors du changement d'une métrique.** Lorsque l'expression d'une
métrique est mise à jour, chaque vue `view_metrics` qui la référence est recompilée et le
nouveau SQL est persisté immédiatement. La vue ne peut pas dériver de la définition de la
métrique, par construction.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Appels `metric()` en ligne dans du SQL de vue écrit à la main.** Un `view_sql` écrit à la
main peut aussi référencer des métriques via `metric('name')`. Le compilateur remplace chaque
appel par l'expression de la métrique et enregistre un lien de traçabilité. Cela donne aux
vues écrites à la main la même propriété de recompilation automatique lorsqu'elles référencent
une métrique plutôt que d'en copier la formule.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines
393–429]

Remarque : les vues issues de la configuration utilisant des appels `metric()` en ligne se
régénèrent au rechargement de la configuration, et non lors de l'upsert de la métrique.
[tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines 84–86: "Free-hand
view_sql born from inline metric() calls carries no stored provenance, so it is not
regenerated here (config-path views regenerate on config reload)"]

### La page d'administration Metrics (REQ-1323, REQ-1324)

Ouvrez l'élément de navigation **Metrics** pour gérer les métriques gouvernées. Cliquez sur une
ligne pour développer un panneau de détail en lecture seule ; cliquez sur **Edit** à
l'intérieur pour passer en édition en ligne (sans fenêtre modale). **New Metric** ouvre une
carte de création en ligne au-dessus du tableau. La confirmation de suppression est la seule
fenêtre modale de la page.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics
management page (list / create / edit / delete). REQ-1323: detail-then-edit"]

Le formulaire de création/modification propose un constructeur à trois sélecteurs pour les
métriques issues de facts : choisissez la table de fact source (filtrée sur
`modelingRole=fact`), une colonne de mesure et une fonction d'agrégation (`SUM`, `AVG`,
`COUNT`, `MIN`, `MAX`). Le datatype est dérivé automatiquement : `COUNT → bigint`,
`AVG → numeric`, `SUM/MIN/MAX → le type de la colonne de mesure`. La zone de texte
d'expression reste l'échappatoire pour les expressions arbitraires.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## L'apport de l'IR

Chaque appel d'enregistrement emprunte le même chemin qu'une MV écrite à la main. La
spécification entity/fact est une représentation intermédiaire — ni un template, ni une macro.
L'entrepôt qu'elle cible est une propriété du déploiement, pas du modèle. Changez le moteur
cible et les mêmes déclarations `entity` / `fact` s'y matérialisent, car le SQL généré et les
modes bitemporels sont neutres vis-à-vis du moteur, par construction. [tool-verified:
modeling.py docstring lines 25-28; modeling_register.py lines 56-66, 80-88]
