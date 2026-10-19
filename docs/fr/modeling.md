<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modélisation des données (Entities et Facts) {: #data-modeling-entities-facts }

Provisa vous donne deux primitives déclaratives, `entity` et `fact`, qui couvrent les briques dont tout schéma en étoile et tout Data Vault sont faits. Déclarez la spécification, et Provisa la traduit exactement en définitions de vues matérialisées, bitemporelles et de relations que vous écririez sinon à la main.

## Ce que sont les entities et les facts {: #what-entities-and-facts-are }

Une **entity** est une projection à clé, dédupliquée et éventuellement historisée d'une relation
source. Vous lui donnez un nom, la pointez vers une source, déclarez la clé de l'entity et les
attributs à conserver, puis choisissez un mode d'historique. Provisa écrit le SQL de la vue et
enregistre la MV. Lorsque l'historique est activé, la MV est bitemporelle.

Un **fait** est une jointure vers des clés d'entité, réduite à un grain déclaré, avec des mesures agrégées. Provisa écrit une requête de MV d'agrégat (`GROUP BY` grain + colonnes FK) et enregistre une relation pour chaque lien de dimension déclaré. Un fait sans mesure est un pur ensemble de clés, le motif link de Data Vault.

Les deux constructions sont de l'IR. Les définitions générées passent d'un moteur à l'autre sans remodélisation : matérialisées dans Oracle ou Databricks, ou laissées virtuelles sur un moteur MPP.

## Modes d'historique {: #history-modes }

Trois modes sont disponibles sur une entity :

| Mode | Signification | Mode bitemporel |
| --- | --- | --- |
| `none` | État courant uniquement. Aucun historique. | — |
| `scd2` | Trace chaque changement. Ajoute uniquement les lignes modifiées (delta), avec pour clé la clé de l'entity. | `delta` |
| `snapshot` | Trace chaque rafraîchissement. Ajoute l'ensemble complet des résultats à chaque rafraîchissement, horodaté par le système. | `snapshot` |

`scd2` nécessite une clé d'entity pour calculer le delta. `snapshot` fonctionne sur n'importe
quel moteur, mais le stockage croît d'une copie complète à chaque rafraîchissement. Choisissez
`scd2` pour les sources volumineuses à évolution lente ; choisissez `snapshot` lorsque vous avez
besoin de l'historique complet et que la source ne peut pas fournir de clé.

Les faits n'ont pas de mode d'historique propre ; leur couverture dans le temps vient de l'historique des entités sous-jacentes.

## Mesures et agrégations {: #measures-and-aggregations }

Les mesures sont déclarées sous forme de paires `column:agg`. Agrégations prises en charge
 :

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

L'agrégation par défaut est `sum`.

## Exemple pratique : entity Customer + fact Sales {: #worked-example-customer-entity-sales-fact }

### Les tables source {: #the-source-tables }

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Enregistrer l'entity Customer {: #register-the-customer-entity }

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

Provisa génère et enregistre cette MV bitemporelle :

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Enregistrer le fact Sales {: #register-the-sales-fact }

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

Provisa génère et enregistre :

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Plus une relation enregistrée : `Sales.customer_id → Customer` (cardinalité : many-to-one).

## Le formulaire Model (interface d'administration) {: #the-model-form-admin-ui }

Le bouton **+ Model** apparaît sur la page **Tables** (infobulle : "Model an entity or fact
(star schema / Data Vault)").

Un contrôle segmenté en haut de la fenêtre modale bascule entre **Entity (dimension)** et
**Fact**.

### Champs Entity {: #entity-fields }

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| Name | oui | Le nom de la MV dans le catalogue |
| Source relation | oui | Relation en notation pointée, p. ex. `raw.customers` |
| Domain | oui | Domaine auquel appartient la MV |
| Entity key | oui | Colonne(s) clé séparée(s) par des virgules, p. ex. `id` |
| Attributes | non | Colonnes d'attributs séparées par des virgules, p. ex. `name, region, tier` |
| History | non | `none` / `scd2` / `snapshot` ; la valeur par défaut est `none` |

### Champs Fact {: #fact-fields }

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| Name | oui | Le nom de la MV dans le catalogue |
| Source relation | oui | Relation en notation pointée, p. ex. `raw.orders` |
| Domain | oui | Domaine auquel appartient la MV |
| Grain | oui | Colonne(s) de grain séparée(s) par des virgules, p. ex. `order_id` |
| Measures | non | Paires `col:agg` séparées par des virgules, p. ex. `amount:sum, quantity:sum` |
| Dimensions | non | Paires `Entity:fk_column` séparées par des virgules, p. ex. `Customer:customer_id` |

Lorsque `agg` est omis dans une mesure (`amount` au lieu de `amount:sum`), la valeur par défaut
est `sum`.

## L'API GraphQL {: #the-graphql-api }

Les deux mutations résident dans le schéma d'administration.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Champs de `EntityInput` :

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

Champs de `FactInput` :

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

`DimRefInput` : `{ entity: String, via: String }` — `entity` est le nom de l'entity référencée ;
`via` est la colonne FK sur la source du fact.

En cas de succès, `registerFact` renvoie un message de la forme :
`Fact 'Sales' registered with 1 dimension link(s)`.

## Star schema de Kimball et Data Vault {: #kimball-star-schema-and-data-vault }

Aucun des deux modèles ne nécessite d'outillage distinct. Les deux mêmes primitives se
composent pour former l'un comme l'autre.

### Star schema de Kimball {: #kimball-star-schema }

Ce parcours construit une étoile à trois dimensions. Deux tables source sont nouvelles :

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` gagne ici aussi les colonnes `product_id` et `order_date`.

#### Choisir le type de SCD {: #choosing-scd-type }

Le mode d'historique est le seul paramètre qui distingue SCD Type 1 de Type 2 :

| Type SCD | Mode d'historique | Effet |
| --- | --- | --- |
| Type 1 (état courant uniquement) | `none` | La MV est reconstruite à chaque rafraîchissement ; aucun historique de lignes |
| Type 2 (versionné) | `scd2` | MV delta bitemporelle ; chaque changement ajoute une nouvelle ligne, avec pour clé la clé de l'entity |

Utilisez `scd2` quand les requêtes en aval doivent joindre une dimension telle qu'elle était au moment de la transaction, comme le niveau d'un client au moment de l'achat plutôt que son niveau actuel. Utilisez `none` pour des tables de référence stables. Un axe de dates ne change jamais, et un catalogue de produits dont seul le prix actuel compte peut être reconstruit à chaque rafraîchissement.

#### Décision du grain {: #grain-decision }

Le grain est le niveau de détail le plus fin auquel répond le fait. `order_id` donne une ligne par commande et garde la possibilité de compter les commandes distinctes et de joindre n'importe quelle dimension au niveau de la commande. Un grain plus grossier, par exemple `["customer_id", "order_date"]`, pré-agrège entre commandes et perd ce détail définitivement. Déclarez le grain le plus fin dont le métier a besoin ; les agrégats plus grossiers se dérivent facilement ensuite.

#### Enregistrer les dimensions {: #register-the-dimensions }

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

Génère une MV delta bitemporelle avec pour clé `id` :

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

Génère une MV ordinaire reconstruite à chaque rafraîchissement :

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

#### Enregistrer le fact Sales sur trois dimensions {: #register-the-sales-fact-across-three-dimensions }

Grain : `order_id`. Trois références de dimension, avec une colonne FK chacune. Les deux mesures sont des sommes additives.

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
 :

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Trois relations sont enregistrées automatiquement :

| Relation | Cardinalité |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensions conformes {: #conformed-dimensions }

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
traversent la même définition de dimension.

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

Génère :

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

Génère :

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` et `CustomerHub` ont tous deux pour clé `id`. Le hub est la cible de jointure
stable ; le satellite fournit un accès aux attributs à un instant donné via la couche
bitemporelle.

#### Link (fact sans mesure) {: #link-measureless-fact }

Un link enregistre quelles clés de hub sont apparues ensemble. Il ne contient que des clés, sans mesure, et Provisa omet le `GROUP BY` quand `measures` est vide.

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
Aucune mesure, donc pas de `GROUP BY`. Génère :

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Deux relations sont enregistrées : `OrderCustomerLink.customer_id → CustomerHub` et
`OrderCustomerLink.order_id → OrderHub`, toutes deux many-to-one
.

#### Bridge / link agrégé {: #bridge-aggregate-link }

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

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(le `order_id` dupliqué de la liste de dimensions est supprimé par `_dedup`).
Génère :

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Le modèle ne décide pas de la méthodologie. Le grain, la conformité, le choix du SCD et la
répartition hub/satellite restent des décisions du modélisateur. Provisa les exécute.

## Métriques {: #metrics }

Une **métrique** est une définition d'agrégat nommée et gouvernée, sans grain propre. Le grain, c'est-à-dire les dimensions selon lesquelles l'agrégat est ventilé, est fixé par l'appelant au moment de la requête et non à la définition. C'est ce qui distingue une métrique d'une vue : une vue fige le grain à sa création, une métrique reste ouverte jusqu'à ce qu'on l'interroge.

### L'objet Metric {: #the-metric-object }

| Champ | Obligatoire | Remarques |
| --- | --- | --- |
| `name` | oui | snake_case, p. ex. `net_revenue`. Validé : `[a-z][a-z0-9_]*` |
| `expression` | oui | ANSI-SQL agrégatif ; doit inclure au moins une fonction d'agrégation |
| `datatype` | non | Indication du type de résultat, p. ex. `number`, `integer` |
| `description` | non | Définition métier lisible par un humain |
| `ai_context` | non | Texte destiné aux consommateurs IA — projeté vers les outils MCP, pg_description, la documentation GraphQL et l'export Ossie |
| `visible_to` | non | Liste de rôles ; la valeur par défaut est `["*"]` (tous les rôles) |
| `from_fact` | — | Défini automatiquement lorsque la métrique a été générée à partir d'une mesure de fact |

Les références de colonnes dans l'expression doivent être qualifiées par leur table (`orders.amount`, pas seulement `amount`). Une colonne non qualifiée est une erreur bloquante à l'expansion de la métrique, et rien ne la laisse passer avec un simple avertissement.

Le référentiel de métriques valide l'expression à chaque écriture. Une expression qui ne
s'analyse pas correctement ou qui ne contient aucune fonction d'agrégation est rejetée ; elle
n'est jamais stockée.

Exemple d'entrée de configuration :

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Interroger une métrique {: #querying-a-metric }

Le compilateur réserve le schéma `metrics`, et chaque métrique y est adressable comme relation virtuelle. Interrogez-la comme une table : les colonnes sélectionnées deviennent l'ensemble de dimensions et le GROUP BY :

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

Le compilateur réécrit ceci en un véritable agrégat groupé sur les tables sémantiques
sous-jacentes avant l'exécution de la gouvernance, de sorte que la RLS et le masquage
s'appliquent aux colonnes réelles.

`SELECT *` sur une relation de métrique est refusé. Nommez explicitement les colonnes de dimension et `value`.

Lorsque l'expression d'une métrique s'étend sur plusieurs tables, le compilateur les joint via
les relations enregistrées. Une dimension qui est une colonne d'une table directement
référencée se résout vers cette table. Une dimension à un saut de relation est jointe
automatiquement. Deux sauts, ou une dimension ambiguë, constituent une erreur bloquante qui
nomme le responsable.

### Métriques issues des spécifications de fact {: #metrics-from-fact-specs }

Lors de l'enregistrement d'un fact, chaque mesure déclarée enregistre automatiquement un objet
Metric correspondant. Le champ `from_fact` de la métrique enregistre le nom de la table de
fact source, et les dimensions de regroupement valides sont les attributs d'entity accessibles
via les relations FK du fact.

Les métriques enregistrées automatiquement apparaissent sur la page Metrics avec un badge
**fact**. Elles peuvent être modifiées comme n'importe quelle autre métrique.

### Vues composées de métriques (view_metrics) {: #metric-composed-views-view_metrics }

Une vue `view_metrics` fixe le grain d'une métrique au moment de la définition. Déclarez les
noms des métriques, les colonnes de dimension et les filtres optionnels ; le compilateur
génère le SELECT.

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

**Régénération automatique lors du changement d'une métrique.** Lorsque l'expression d'une
métrique est mise à jour, chaque vue `view_metrics` qui la référence est recompilée et le
nouveau SQL est persisté immédiatement. La vue ne peut pas dériver de la définition de la
métrique, par construction.

**Appels `metric()` en ligne dans du SQL de vue écrit à la main.** Un `view_sql` écrit à la
main peut aussi référencer des métriques via `metric('name')`. Le compilateur remplace chaque
appel par l'expression de la métrique et enregistre un lien de traçabilité. Cela donne aux
vues écrites à la main la même propriété de recompilation automatique lorsqu'elles référencent
une métrique plutôt que d'en copier la formule.

Remarque : les vues du fichier de configuration qui utilisent des appels `metric()` en ligne se régénèrent au rechargement de la configuration, et un upsert de la métrique ne les régénère pas.

### La page d'administration Metrics {: #the-metrics-admin-page }

Ouvrez l'élément de navigation **Metrics** pour gérer les métriques gouvernées. Cliquez sur une
ligne pour développer un panneau de détail en lecture seule ; cliquez sur **Edit** à
l'intérieur pour passer en édition en ligne (sans fenêtre modale). **New Metric** ouvre une
carte de création en ligne au-dessus du tableau. La confirmation de suppression est la seule
fenêtre modale de la page.

Le formulaire de création/modification propose un constructeur à trois sélecteurs pour les
métriques issues de facts : choisissez la table de fact source (filtrée sur
`modelingRole=fact`), une colonne de mesure et une fonction d'agrégation (`SUM`, `AVG`,
`COUNT`, `MIN`, `MAX`). Le datatype est dérivé automatiquement : `COUNT → bigint`,
`AVG → numeric`, `SUM/MIN/MAX → the measure column's type` (le type de la colonne de mesure). La zone de texte
d'expression reste l'échappatoire pour les expressions arbitraires.

## L'apport de l'IR {: #the-ir-payoff }

Chaque appel d'enregistrement suit le même chemin qu'une MV écrite à la main. La spécification entity/fact est une représentation intermédiaire ; ce n'est ni un modèle ni une macro. L'entrepôt cible est une propriété du déploiement, et le modèle ne le nomme pas. Changez le moteur cible, et les mêmes déclarations `entity` / `fact` se matérialisent là-bas, car le SQL généré et les modes bitemporels sont neutres vis-à-vis du moteur par conception.
