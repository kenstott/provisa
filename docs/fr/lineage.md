# Traçabilité au niveau des colonnes

Provisa suit la traçabilité des données au niveau des colonnes de manière statique — calculée à
partir des définitions SQL et des contrats de commandes, sans aucune exécution. Deux vues sont
disponibles : un DAG par instruction et un graphe de provenance à l'échelle de la fédération,
couvrant toutes les vues et vues matérialisées (MV) enregistrées.

## L'explorateur de traçabilité

Rendez-vous sur **Traçabilité** dans l'interface (`/lineage`). Collez une instruction SQL et
cliquez sur **Construire le graphe de l'instruction** pour voir son DAG au niveau des colonnes.
Cliquez sur **Graphe de fédération** pour charger le graphe de provenance couvrant toutes les MV du
registre. [tool-verified: LineagePage.tsx:28-119]

## DAG au niveau de l'instruction (REQ-1160)

Chaque colonne de sortie nommée de votre SQL devient un nœud. Le constructeur la retrace à travers
chaque CTE, sous-requête, jointure et appel de commande en ligne jusqu'à ses colonnes sources, en
bâtissant un graphe orienté des entrées sources vers les sorties finales.

### Exemple détaillé

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Cette instruction produit trois colonnes de sortie. Le graphe de `geo_u` ressemble à ceci :

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` et `orders.geo` sont des nœuds **source** (le contrat d'entrée
  étroit d'`enrich_grpc_set` déclare `id` et `region` ; la clôture de contamination complète relie
  toutes les entrées déclarées à toutes les sorties). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` et `e.geo` sont des nœuds **command** — la frontière d'`enrich_grpc_set`.
- `geo_u` est un nœud **derived** produit par la fonction SQL `UPPER`.

La frontière de la commande n'est **pas opaque**. Parce qu'`enrich_grpc_set` déclare ses colonnes
d'entrée (`id`, `region`) et ses colonnes de sortie (`id`, `embedding`, `geo`), le moteur de
traçabilité épisse la clôture de contamination sans rupture, des colonnes déclarées de la relation
source jusqu'à chaque sortie.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Genres de nœuds et repères visuels

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants ; LineagePage.tsx:21-26 LEGEND]

| Genre de nœud | Couleur | Signification |
| --- | --- | --- |
| `source` | Vert | Une colonne de table de base |
| `derived` | Bleu | Produite par une expression SQL (fonction, opérateur, CTE) |
| `command` | Violet | Une colonne de sortie d'une commande enregistrée |

Anneaux supplémentaires sur un nœud :

- **Anneau orange** — une colonne de sortie finale de l'instruction.
- **Bordure double** — la relation de la colonne est une vue matérialisée (instantané MV/CTAS).
- **Anneau rouge** — membre d'un cycle classé comme erreur.
- **Anneau jaune** — membre d'un cycle classé comme boucle de rétroaction.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformations nommées sur les arêtes

Chaque arête porte l'expression SQL brute qui produit la colonne cible, ainsi qu'une liste
d'opérations nommées : fonctions SQL (`sql_function`), opérateurs arithmétiques et logiques
(`operator`), commandes enregistrées (`command`), références de colonnes nues (`identity`) et
littéraux (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

Une arête issue d'un appel de commande est rendue dans l'interface par une ligne violette en
pointillés.
[tool-verified: LineageDag.tsx:122-124]

## Graphe à l'échelle de la fédération (REQ-1161)

Le graphe de fédération fusionne la traçabilité par instruction de chaque MV enregistrée en un seul
graphe de provenance. L'identité d'un nœud est `relation.column` — la colonne de sortie d'une vue
et la référence d'entrée d'une autre vue vers la même colonne se rabattent sur un seul nœud. Le
résultat est un DAG unique allant des colonnes sources de base à chaque jeu de données dérivé de la
plateforme. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Servez-vous de `focus`, `direction` et `depth` pour cadrer la vue à l'échelle de la fédération sans
recalculer le graphe. [tool-verified: `slice_graph` in merge.py:160-189]

## Cycles (REQ-1161)

Les cycles sont décrits, non rejetés. Le moteur de traçabilité détecte chaque cycle orienté et le
**classe**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classement | Couleur de bordure | Signification |
| --- | --- | --- |
| `feedback` | Jaune | Le cycle traverse un nœud matérialisé — une boucle de rétroaction licite, décalée dans le temps. L'instantané de la MV est la frontière de version qui la rend bien définie. |
| `error` | Rouge | Aucune frontière de matérialisation sur la boucle — une définition circulaire sans ordre d'évaluation stable. Probablement une erreur de conception. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering ; merge.py:38-48]

Un cycle `feedback` n'est pas un échec. Une MV d'enrichissement qui réinjecte une colonne dérivée
dans sa propre relation source est un patron valide tant qu'un nœud de la boucle est matérialisé —
l'instantané isole temporellement les deux moitiés. Un cycle `error` demande le jugement d'un
exploitant : il signifie généralement que deux vues se référencent l'une l'autre sans instantané
entre elles.

## API

Les deux endpoints sont **statiques** — ils lisent des définitions et des contrats, pas des
données.

### POST /admin/lineage/graph

Renvoie le DAG au niveau des colonnes d'une seule instruction SQL.

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

Forme de la réponse [tool-verified: `LineageGraph.to_dict` in graph.py:82-105] :

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

Renvoie un HTTP 422 lorsque le SQL ne peut pas être analysé.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Renvoie le graphe de provenance fusionné couvrant toutes les MV du registre.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Paramètres de requête [tool-verified: function signature at lineage_router.py:73-76] :

| Paramètre | Valeurs | Défaut | Effet |
| --- | --- | --- | --- |
| `focus` | Un identifiant de nœud | — | Cadre la réponse sur le sous-graphe autour de ce nœud |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Dans quelle direction parcourir depuis `focus` |
| `depth` | entier | non borné | Distance maximale en sauts depuis `focus` |

La réponse a la même forme que le graphe d'instruction, avec un champ `cycles` en plus
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64] :

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

## Ce que renommer ou supprimer une colonne casserait (REQ-1484)

Une colonne porte deux noms, et chacun est stocké par un ensemble d'artefacts différent.

Le **nom exposé** est celui que montrent les surfaces SQL et GraphQL : `table_columns.alias`, à
défaut la valeur snake_case par défaut lorsqu'aucun alias n'est posé [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]. Les vues, vues matérialisées, expressions de métriques, prédicats RLS,
contrats de qualité des données, grains de vues de métriques et clés de ligne des MV sont tous
écrits contre ce nom : **renommer un alias les casse donc aussi sûrement que supprimer la colonne**.

Le **nom physique** est `table_columns.column_name`, l'identité qui survit au remplacement en bloc
des colonnes lors de l'upsert d'une table. Les relations, les liaisons du [glossaire](glossary.md), les affectations
d'étiquettes, la colonne de filigrane et les préréglages de colonnes stockent celui-là : ils ne
cassent donc que lorsque la colonne est **supprimée**.

`columnDependents` rapporte les deux. Les vues et MV en aval proviennent d'une coupe du graphe de
fédération sur le nom exposé de la colonne ; les artefacts que ce graphe ne couvre pas proviennent
d'un balayage direct du registre [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` vaut `rename` pour une référence au nom exposé et `remove` pour une référence au nom
physique : un appelant peut donc savoir à quelle moitié de la modification chaque artefact réagit.

Posez la question **avant** l'enregistrement. Une colonne renommée est localisée par le nom exposé
qu'elle porte encore dans le registre ; une fois l'alias posé, l'ancien nom a disparu et la requête
ne trouve rien.

La page Tables exécute automatiquement la requête lorsqu'une modification en attente change un
alias ou réduit l'ensemble des colonnes, et liste ce qu'elle trouve [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. L'avertissement est
consultatif : il nomme les artefacts concernés et c'est l'administrateur qui décide. Il ne bloque
pas l'enregistrement, car les consommateurs du parc ne peuvent pas tous être atteints — un tableau
de bord extérieur ou une application cliente qui interroge la colonne par son nom échappe à la
connaissance du registre. Pour la même raison, les balayages de texte SQL libre repèrent la colonne
comme jeton identifiant plutôt que de résoudre la portée, ce qui peut nommer un artefact qui, en
fin de compte, n'utilise pas la colonne. Sur-rapporter est la direction sûre pour un avertissement.

## Se servir de la traçabilité pour gouverner les contrats de commandes

Parce que la clôture de contamination relie chaque colonne d'entrée déclarée à chaque colonne de
sortie déclarée, l'ampleur de cette clôture dépend entièrement de ce que vous déclarez.

Prenez une commande qui reçoit une table orders complète (`id`, `region`, `amount`, `customer_id`,
`discount`, `notes`, ...) et renvoie un `embedding`. Si le contrat d'entrée liste toutes ces
colonnes, chaque colonne en aval qui se sert de l'embedding montrera une traçabilité issue de
toutes. C'est exact, mais inutilisable — il devient difficile de dire ce qui comptait vraiment.

Ne déclarez que `id` et `text` (les colonnes que le modèle d'embedding lit réellement), et le cône
de traçabilité se resserre sur ces deux colonnes sources. La dérivation est à la fois correcte et
précise.

Voir [Commandes](commands.md) pour la mécanique de déclaration d'un contrat d'entrée étroit.
