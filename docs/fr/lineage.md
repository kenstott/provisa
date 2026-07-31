# Traçabilité au niveau des colonnes

Provisa assure le suivi de la traçabilité des données au niveau des colonnes de façon statique — calculée à partir des définitions SQL et des contrats de commande, sans exécution requise. Deux vues sont disponibles : un DAG par instruction et un graphe de provenance à l'échelle de la fédération couvrant toutes les vues et vues matérialisées (MV) enregistrées.

## L'explorateur de traçabilité

Accédez à **Lineage** dans l'interface (`/lineage`). Collez une instruction SQL et cliquez sur **Build statement graph** pour voir son DAG au niveau des colonnes. Cliquez sur **Federation graph** pour charger le graphe de provenance sur chaque MV du registre. [tool-verified: LineagePage.tsx:28-119]

## DAG au niveau de l'instruction (REQ-1160)

Chaque colonne de sortie nommée dans votre SQL devient un nœud. Le générateur la retrace à travers chaque CTE, sous-requête, jointure et appel de commande en ligne jusqu'à ses colonnes source, en construisant un graphe orienté depuis les entrées source jusqu'aux sorties finales.

### Exemple détaillé

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Cette instruction produit trois colonnes de sortie. Le graphe pour `geo_u` se présente ainsi :

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` et `orders.geo` sont des nœuds **source** (le contrat d'entrée étroit de `enrich_grpc_set` déclare `id` et `region` ; la clôture de propagation (taint closure) complète relie toutes les entrées déclarées à toutes les sorties). [tool-verified: `_splice_commands` en graph.py:223-242]
- `e.embedding` et `e.geo` sont des nœuds **command** — la frontière de `enrich_grpc_set`.
- `geo_u` est un nœud **derived** produit par la fonction SQL `UPPER`.

La frontière de la commande n'est **pas opaque**. Comme `enrich_grpc_set` déclare ses colonnes d'entrée (`id`, `region`) et ses colonnes de sortie (`id`, `embedding`, `geo`), le moteur de traçabilité relie la clôture de propagation de façon continue depuis les colonnes déclarées de la relation source jusqu'à chaque sortie. [tool-verified: `_splice_commands` et `_input_relation` en graph.py:245-271]

### Types de nœuds et repères visuels

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Type de nœud | Couleur | Signification |
| --- | --- | --- |
| `source` | Vert | Une colonne d'une table de base |
| `derived` | Bleu | Produite par une expression SQL (fonction, opérateur, CTE) |
| `command` | Violet | Une colonne de sortie d'une commande enregistrée |

Anneaux supplémentaires sur un nœud :

- **Anneau orange** — une colonne de sortie finale de l'instruction.
- **Bordure double** — la relation de la colonne est une vue matérialisée (instantané MV/CTAS).
- **Anneau rouge** — membre d'un cycle classé comme erreur.
- **Anneau jaune** — membre d'un cycle classé comme boucle de rétroaction.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformations nommées sur les arêtes

Chaque arête porte l'expression SQL brute qui produit la colonne cible, ainsi qu'une liste d'opérations nommées : fonctions SQL (`sql_function`), opérateurs arithmétiques/logiques (`operator`), commandes enregistrées (`command`), références de colonne simples (`identity`) et littéraux (`constant`). [tool-verified: TransformOp and name_transform en graph.py:36-145]

Une arête issue d'un appel de commande est représentée par une ligne violette en pointillés dans l'interface. [tool-verified: LineageDag.tsx:122-124]

## Graphe à l'échelle de la fédération (REQ-1161)

Le graphe de fédération fusionne la traçabilité par instruction de chaque MV enregistrée en un seul graphe de provenance. L'identité du nœud est `relation.column` — la colonne de sortie d'une vue et la référence d'entrée d'une autre vue vers la même colonne fusionnent en un seul nœud. Le résultat est un DAG unique allant des colonnes source de base jusqu'à chaque jeu de données dérivé de la plateforme. [tool-verified: `build_federation_graph` en merge.py:205-229 et `qualify_outputs` en graph.py:275-299]

Utilisez `focus`, `direction` et `depth` pour restreindre la vue à l'échelle de la fédération sans recalculer le graphe. [tool-verified: `slice_graph` en merge.py:160-189]

## Cycles (REQ-1161)

Les cycles sont décrits, non rejetés. Le moteur de traçabilité détecte chaque cycle orienté et le **classe**. [tool-verified: `Cycle.classification` property en merge.py:43-46]

| Classification | Couleur de bordure | Signification |
| --- | --- | --- |
| `feedback` | Jaune | Le cycle traverse un nœud matérialisé — une boucle de rétroaction légitime et décalée dans le temps. L'instantané de la MV constitue la limite de version qui la rend bien définie. |
| `error` | Rouge | Aucune limite de matérialisation sur la boucle — une définition circulaire sans ordre d'évaluation stable. Probablement une erreur de conception. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Un cycle `feedback` n'est pas un échec. Une MV d'enrichissement qui réinjecte une colonne dérivée dans sa propre relation source est un schéma valide, à condition qu'un nœud de la boucle soit matérialisé — l'instantané isole les deux moitiés dans le temps. Un cycle `error` exige le jugement d'un opérateur : cela signifie généralement que deux vues se référencent mutuellement sans instantané intermédiaire.

## API

Les deux points de terminaison sont **statiques** — ils lisent des définitions et des contrats, pas des données.

### POST /admin/lineage/graph

Renvoie le DAG au niveau des colonnes pour une seule instruction SQL.

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

Forme de la réponse [tool-verified: `LineageGraph.to_dict` en graph.py:82-105] :

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

Renvoie HTTP 422 lorsque le SQL ne peut pas être analysé (parsé).
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Renvoie le graphe de provenance fusionné sur toutes les MV du registre.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Paramètres de requête [tool-verified: function signature at lineage_router.py:73-76] :

| Paramètre | Valeurs | Par défaut | Effet |
| --- | --- | --- | --- |
| `focus` | Un id de nœud | — | Restreint la réponse au sous-graphe autour de ce nœud |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Direction de parcours à partir de `focus` |
| `depth` | entier | illimité | Distance maximale de sauts depuis `focus` |

La réponse a la même forme que le graphe d'instruction, avec un champ `cycles` ajouté
[tool-verified: `MergedGraph.to_dict` en merge.py:60-64] :

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

## Utiliser la traçabilité pour gouverner les contrats de commande

Comme la clôture de propagation relie chaque colonne d'entrée déclarée à chaque colonne de sortie déclarée, l'ampleur de cette clôture dépend entièrement de ce que vous déclarez.

Prenons une commande qui reçoit une table `orders` complète (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) et renvoie un `embedding`. Si le contrat d'entrée énumère toutes ces colonnes, chaque colonne en aval qui utilise l'embedding affichera une traçabilité issue de toutes ces colonnes. C'est exact mais peu utile — il devient difficile de déterminer ce qui a réellement compté.

En déclarant uniquement `id` et `text` (les colonnes que le modèle d'embedding lit réellement), le cône de traçabilité se resserre sur ces deux colonnes source. La dérivation est alors à la fois rigoureuse et précise.

Consultez [Commands](commands.md) pour connaître la mécanique de déclaration d'un contrat d'entrée étroit.
