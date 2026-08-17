# Traçabilité au niveau des colonnes

Provisa suit la traçabilité des données au niveau des colonnes de manière statique — calculée à
partir des définitions SQL et des contrats de commande, sans exécution requise. Deux vues sont
disponibles : un DAG par instruction et un graphe de provenance à l'échelle de la fédération
couvrant toutes les vues et vues matérialisées (MV) enregistrées.

## L'explorateur de traçabilité

Naviguez vers **Lineage** dans l'UI (`/lineage`). Collez une instruction SQL et cliquez sur **Build
statement graph** pour voir son DAG au niveau des colonnes. Cliquez sur **Federation graph** pour
charger le graphe de provenance sur chaque MV du registre. [tool-verified: LineagePage.tsx:28-119]

## DAG au niveau de l'instruction (REQ-1160)

Chaque colonne de sortie nommée dans votre SQL devient un nœud. Le générateur la retrace à travers
chaque CTE, sous-requête, jointure et appel de commande en ligne jusqu'à ses colonnes source, en
construisant un graphe orienté depuis les entrées source jusqu'aux sorties finales.

### Exemple traité

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Cette instruction produit trois colonnes de sortie. Le graphe pour `geo_u` ressemble à :

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region`, et `orders.geo` sont des nœuds **source** (le contrat d'entrée
  étroit de `enrich_grpc_set` déclare `id` et `region` ; la fermeture de propagation (taint
  closure) complète relie toutes les entrées déclarées à toutes les sorties).
  [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` et `e.geo` sont des nœuds **command** — la frontière de `enrich_grpc_set`.
- `geo_u` est un nœud **derived** produit par la fonction SQL `UPPER`.

La frontière de commande n'est **pas opaque**. Parce que `enrich_grpc_set` déclare ses colonnes
d'entrée (`id`, `region`) et ses colonnes de sortie (`id`, `embedding`, `geo`), le moteur de
traçabilité relie la fermeture de propagation de façon continue depuis les colonnes déclarées de
la relation source jusqu'à chaque sortie.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Types de nœuds et indices visuels

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Type de nœud | Couleur | Signification |
| --- | --- | --- |
| `source` | Vert | Une colonne de table de base |
| `derived` | Bleu | Produite par une expression SQL (fonction, opérateur, CTE) |
| `command` | Violet | Une colonne de sortie issue d'une commande enregistrée |

Anneaux additionnels sur un nœud :

- **Anneau orange** — une colonne de sortie finale de l'instruction.
- **Bordure double** — la relation de la colonne est une vue matérialisée (instantané MV/CTAS).
- **Anneau rouge** — membre d'un cycle classé comme erreur.
- **Anneau jaune** — membre d'un cycle classé comme boucle de rétroaction.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformations nommées sur les arêtes

Chaque arête porte l'expression SQL brute qui produit la colonne cible, plus une liste
d'opérations nommées : fonctions SQL (`sql_function`), opérateurs arithmétiques/logiques
(`operator`), commandes enregistrées (`command`), références de colonnes nues (`identity`), et
littéraux (`constant`). [tool-verified: TransformOp and name_transform in graph.py:36-145]

Une arête issue d'un appel de commande est rendue comme une ligne violette en pointillés dans
l'UI. [tool-verified: LineageDag.tsx:122-124]

## Graphe à l'échelle de la fédération (REQ-1161)

Le graphe de fédération fusionne la traçabilité par instruction de chaque MV enregistrée en un
seul graphe de provenance. L'identité d'un nœud est `relation.colonne` — la colonne de sortie
d'une vue et la référence d'entrée d'une autre vue vers la même colonne fusionnent en un seul
nœud. Le résultat est un DAG unique depuis les colonnes source de base jusqu'à chaque jeu de
données dérivé de la plateforme. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Utilisez `focus`, `direction`, et `depth` pour délimiter la vue à l'échelle de la fédération sans
recalculer le graphe. [tool-verified: `slice_graph` in merge.py:160-189]

## Cycles (REQ-1161)

Les cycles sont décrits, pas rejetés. Le moteur de traçabilité détecte chaque cycle orienté et le
**classe**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classification | Couleur de bordure | Signification |
| --- | --- | --- |
| `feedback` | Jaune | Le cycle traverse un nœud matérialisé — une boucle de rétroaction légale et décalée dans le temps. L'instantané de la MV est la frontière de version qui la rend bien définie. |
| `error` | Rouge | Aucune frontière de matérialisation sur la boucle — une définition circulaire sans ordre d'évaluation stable. Probablement une erreur de conception. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Un cycle `feedback` n'est pas un échec. Une MV d'enrichissement qui réinjecte une colonne dérivée
dans sa propre relation source est un motif valide tant qu'un nœud de la boucle est matérialisé —
l'instantané isole les deux moitiés dans le temps. Un cycle `error` nécessite un jugement de
l'opérateur : cela signifie généralement que deux vues se référencent mutuellement sans
instantané entre les deux.

## API

Les deux endpoints sont **statiques** — ils lisent des définitions et des contrats, pas des
données.

### POST /admin/lineage/graph

Retourne le DAG au niveau des colonnes pour une seule instruction SQL.

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

Forme de la réponse [tool-verified: `LineageGraph.to_dict` in graph.py:82-105] :

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

Retourne HTTP 422 quand le SQL ne peut pas être analysé.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Retourne le graphe de provenance fusionné sur toutes les MV du registre.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Paramètres de requête [tool-verified: function signature at lineage_router.py:73-76] :

| Paramètre | Valeurs | Par défaut | Effet |
| --- | --- | --- | --- |
| `focus` | Un id de nœud | — | Délimite la réponse au sous-graphe autour de ce nœud |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Direction de parcours depuis `focus` |
| `depth` | entier | illimité | Distance de saut maximale depuis `focus` |

La réponse a la même forme que le graphe d'instruction, avec un champ `cycles` ajouté
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64] :

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

## Ce qu'un renommage ou une suppression de colonne casserait (REQ-1484)

Une colonne porte deux noms, et chacun est stocké par un ensemble différent d'artefacts.

Le **nom exposé** est ce que montrent les surfaces SQL et GraphQL : `table_columns.alias`, avec
repli sur la valeur par défaut en snake_case quand aucun alias n'est défini
[tool-verified: `computed_sql_alias` at `schema_helpers.py:317`]. Les vues, vues matérialisées,
expressions de métriques, prédicats RLS, contrats DQ, granularités de vue de métriques et clés de
ligne de MV sont tous rédigés par rapport à ce nom, donc **renommer un alias les casse aussi
sûrement que la suppression de la colonne**.

Le **nom physique** est `table_columns.column_name`, l'identité qui survit au remplacement complet
des colonnes lors de l'upsert de table. Les relations, liaisons de glossaire, assignations de
tags, la colonne de filigrane (watermark) et les préréglages de colonnes stockent celui-ci, donc
ils ne cassent que quand la colonne est **supprimée**.

`columnDependents` rapporte les deux. Les vues et MV en aval proviennent du découpage du graphe de
fédération au nom exposé de la colonne ; les artefacts que ce graphe ne couvre pas proviennent
d'un balayage direct du registre [tool-verified: `graph_dependents` in
`provisa/lineage/dependents.py`, registry scans in `provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` vaut `rename` pour une référence au nom exposé et `remove` pour une référence au nom
physique, ce qui permet à l'appelant de savoir à quelle moitié de la modification chaque artefact
réagit.

Posez cette question **avant** l'enregistrement. Une colonne renommée est localisée par le nom
exposé qu'elle porte encore dans le registre ; une fois l'alias enregistré, l'ancien nom a
disparu et la requête ne trouve plus rien.

La page Tables exécute la requête automatiquement quand une modification en attente change un
alias ou réduit l'ensemble de colonnes, et liste ce qu'elle trouve [tool-verified:
`diffEditedColumns` in `provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`].
L'avertissement est consultatif : il nomme les artefacts affectés et l'administrateur décide. Il
ne bloque pas l'enregistrement, car tous les consommateurs de l'estate ne peuvent pas être
atteints — un tableau de bord externe ou une application cliente qui interroge la colonne par
nom échappe à la connaissance du registre. Pour la même raison, les balayages de texte SQL libre
font correspondre la colonne comme un jeton identifiant plutôt que de résoudre la portée, ce qui
peut nommer un artefact qui s'avère ne pas utiliser la colonne. Le sur-signalement est la
direction sûre pour un avertissement.

## Utiliser la traçabilité pour gouverner les contrats de commande

Parce que la fermeture de propagation relie chaque colonne d'entrée déclarée à chaque colonne de
sortie déclarée, l'étendue de cette fermeture dépend entièrement de ce que vous déclarez.

Considérez une commande qui prend une table orders complète (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) et retourne un `embedding`. Si le contrat d'entrée liste
toutes ces colonnes, chaque colonne en aval qui utilise l'embedding affichera une traçabilité
depuis toutes ces colonnes. C'est exact mais peu utile — il est difficile de dire ce qui a
réellement compté.

Ne déclarez que `id` et `text` (les colonnes que le modèle d'embedding lit réellement), et le
cône de traçabilité se resserre sur ces deux colonnes source. La dérivation est à la fois valide
et précise.

Voir [Commands](commands.md) pour la mécanique de déclaration d'un contrat d'entrée étroit.
