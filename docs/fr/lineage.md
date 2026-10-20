# Traçabilité au niveau des colonnes {: #column-level-lineage }

Provisa suit la traçabilité des données au niveau des colonnes de façon statique. Il la calcule à partir des définitions SQL et des contrats de commandes, donc rien n'a besoin de s'exécuter. Vous disposez de deux vues : un DAG par instruction et un graphe de provenance à l'échelle de la fédération, qui couvre chaque vue et vue matérialisée (MV) enregistrée.

## L'explorateur de traçabilité {: #the-lineage-explorer }

Rendez-vous sur **Traçabilité** dans l'interface (`/lineage`). Collez une instruction SQL et
cliquez sur **Construire le graphe de l'instruction** pour voir son DAG au niveau des colonnes.
Cliquez sur **Graphe de fédération** pour charger le graphe de provenance couvrant toutes les MV du
registre.

## DAG au niveau de l'instruction {: #statement-level-dag }

Chaque colonne de sortie nommée de votre SQL devient un nœud. Le constructeur la retrace à travers
chaque CTE, sous-requête, jointure et appel de commande en ligne jusqu'à ses colonnes sources, en
bâtissant un graphe orienté des entrées sources vers les sorties finales.

### Exemple détaillé {: #worked-example }

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
  toutes les entrées déclarées à toutes les sorties).
- `e.embedding` et `e.geo` sont des nœuds **command** à la frontière d'`enrich_grpc_set`.
- `geo_u` est un nœud **derived** produit par la fonction SQL `UPPER`.

La frontière de la commande n'est **pas opaque**. Parce qu'`enrich_grpc_set` déclare ses colonnes
d'entrée (`id`, `region`) et ses colonnes de sortie (`id`, `embedding`, `geo`), le moteur de
traçabilité épisse la clôture de contamination sans rupture, des colonnes déclarées de la relation
source jusqu'à chaque sortie.

### Genres de nœuds et repères visuels {: #node-kinds-and-visual-cues }

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

### Transformations nommées sur les arêtes {: #named-transforms-on-edges }

Chaque arête porte l'expression SQL brute qui produit la colonne cible, ainsi qu'une liste
d'opérations nommées : fonctions SQL (`sql_function`), opérateurs arithmétiques et logiques
(`operator`), commandes enregistrées (`command`), références de colonnes nues (`identity`) et
littéraux (`constant`).

Une arête issue d'un appel de commande est rendue dans l'interface par une ligne violette en
pointillés.

## Graphe à l'échelle de la fédération {: #federation-wide-graph }

Le graphe de la fédération fusionne la traçabilité par instruction de chaque MV enregistrée en un seul graphe de provenance. L'identité d'un nœud est `relation.column` : la colonne de sortie d'une vue et la référence d'entrée d'une autre vue à cette colonne se confondent en un seul nœud. Le résultat est un DAG unique, des colonnes des sources de base jusqu'à chaque jeu de données dérivé de la plateforme.

Servez-vous de `focus`, `direction` et `depth` pour cadrer la vue à l'échelle de la fédération sans
recalculer le graphe.

## Cycles

Le moteur de traçabilité détecte chaque cycle orienté et le **classe**. Un cycle est décrit, jamais rejeté.

| Classement | Couleur de bordure | Signification |
| --- | --- | --- |
| `feedback` | Jaune | Le cycle traverse un nœud matérialisé — une boucle de rétroaction licite, décalée dans le temps. L'instantané de la MV est la frontière de version qui la rend bien définie. |
| `error` | Rouge | Aucune frontière de matérialisation sur la boucle — une définition circulaire sans ordre d'évaluation stable. Probablement une erreur de conception. |

Un cycle `feedback` est un motif valide. Une MV d'enrichissement qui réinjecte une colonne dérivée dans sa propre relation source fonctionne tant qu'un nœud de la boucle est matérialisé, car l'instantané sépare les deux moitiés dans le temps. Un cycle `error` demande le jugement d'un opérateur. Il signifie le plus souvent que deux vues se référencent mutuellement sans instantané entre elles.

## API

Les deux points de terminaison sont **statiques**. Ils lisent des définitions et des contrats, jamais des données.

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

Forme de la réponse :

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

### GET /admin/lineage/federation

Renvoie le graphe de provenance fusionné couvrant toutes les MV du registre.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Paramètres de requête :

| Paramètre | Valeurs | Défaut | Effet |
| --- | --- | --- | --- |
| `focus` | Un identifiant de nœud | — | Cadre la réponse sur le sous-graphe autour de ce nœud |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Dans quelle direction parcourir depuis `focus` |
| `depth` | entier | non borné | Distance maximale en sauts depuis `focus` |

La réponse a la même forme que le graphe d'instruction, avec un champ `cycles` en plus
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

## À partir de quoi chaque rafraîchissement a été construit {: #what-each-refresh-was-built-from }

Chaque rafraîchissement d'une vue matérialisée enregistre à partir de quoi il a été construit : la version de la définition de la vue, la version de ses entrées et la manière dont cette version a été établie, ainsi que les colonnes sources et l'expression de chaque colonne de sortie. La vue opérationnelle `"ops"."mv_refresh_lineage"` renvoie une ligne par rafraîchissement :

| Colonne | Contient |
|---|---|
| `mv_id` | La vue matérialisée rafraîchie |
| `target_table` | La table que le rafraîchissement a écrite |
| `definition_version` | La version de la définition de la vue |
| `input_version`, `input_version_kind` | La version de ses entrées : un snapshot Iceberg, un filigrane, un jeton de fraîcheur ou l'heure du rafraîchissement |
| `trace_id` | La trace du rafraîchissement dans `"ops"."traces"` |
| `refreshed_at` | Quand il a eu lieu |
| `column_derivations` | Chaque colonne de sortie avec ses colonnes sources et son expression, en JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

La vue lit les enregistrements de rafraîchissement dans `"ops"."traces"` et répond sur tout moteur qui les conserve. Sur Trino, ce sont les tables Iceberg du catalogue `otel`. Sur DuckDB, le serveur écrit ses propres enregistrements de traces en fichiers Parquet dans son dossier de télémétrie (`PROVISA_TELEMETRY_DIR`, `~/.provisa/telemetry` par défaut), un fichier par lot d'export dans un dossier par jour. La compaction planifiée de la télémétrie (`otel.compact_cron`) fusionne les petits fichiers de chaque jour en fichiers d'au plus 64 Mo. Chaque organisation ne lit que ses propres rafraîchissements, et les enregistrements sont conservés indéfiniment. Les spans ne sont enregistrés que tant que le SDK OpenTelemetry est activé (`OTEL_SDK_DISABLED` non défini ou false).

## Une vue telle qu'elle était, et telle qu'elle a été enregistrée {: #a-view-as-it-stood-and-as-it-was-recorded }

Une vue qui associe les colonnes de sa source comme `business_key`, `valid_time` et `recorded_time` conserve chaque version qu'elle a connue : chaque rafraîchissement ajoute, et aucune version n'est réécrite, sur aucun stockage. Une lecture simple renvoie l'état actuel. Deux en-têtes de requête lisent son passé :

- `X-Provisa-As-Of` lit la vue à une date ou sur une plage.
- `X-Provisa-As-Recorded` la lit telle que Provisa l'avait enregistrée à une date. Les versions enregistrées après cette date sont écartées, si bien qu'une correction enregistrée plus tard ne change pas la relecture du passé.

Au 30 septembre et enregistrée le 1er octobre donne le chiffre d'origine ; enregistrée le 5 octobre donne la correction. L'enregistrement d'audit de chaque instruction conserve les deux dates (`as_of`, `as_recorded`), de sorte qu'un rapport peut être reproduit tel qu'il a été produit la première fois. [Vues avec historique](configuration.md#views-with-history-time-travel) décrit les trois déclarations, avec des exemples, et chaque surface qui accepte les deux valeurs.

## Ce que renommer ou supprimer une colonne casserait {: #what-a-column-rename-or-drop-would-break }

Une colonne porte deux noms, et chacun est stocké par un ensemble d'artefacts différent.

Le **nom exposé** est celui que montrent les surfaces SQL et GraphQL : `table_columns.alias`, à
défaut la valeur snake_case par défaut lorsqu'aucun alias n'est posé. Les vues, vues matérialisées, expressions de métriques, prédicats RLS,
contrats de qualité des données, grains de vues de métriques et clés de ligne des MV sont tous
écrits contre ce nom : **renommer un alias les casse donc aussi sûrement que supprimer la colonne**.

Le **nom physique** est `table_columns.column_name`, l'identité qui survit au remplacement en bloc
des colonnes lors de l'upsert d'une table. Les relations, les liaisons du [glossaire](glossary.md), les affectations
d'étiquettes, la colonne de filigrane et les préréglages de colonnes stockent celui-là : ils ne
cassent donc que lorsque la colonne est **supprimée**.

`columnDependents` rapporte les deux. Les vues et MV en aval proviennent d'une coupe du graphe de
fédération sur le nom exposé de la colonne ; les artefacts que ce graphe ne couvre pas proviennent
d'un balayage direct du registre.

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

La page Tables lance la requête automatiquement quand une modification en attente change un alias ou réduit l'ensemble des colonnes, et liste ce qu'elle trouve. L'avertissement est consultatif. Il nomme les artefacts touchés, et l'administrateur décide. Il ne bloque jamais l'enregistrement, car on ne peut pas atteindre tous les consommateurs du patrimoine : un tableau de bord externe ou une application cliente qui interroge la colonne par son nom échappe à la connaissance du registre. Pour la même raison, les analyses du texte SQL libre repèrent la colonne comme jeton d'identifiant sans résoudre la portée ; elles peuvent donc nommer un artefact qui, au final, n'utilise pas la colonne. Pour un avertissement, en signaler trop est la direction sûre.

## Se servir de la traçabilité pour gouverner les contrats de commandes {: #using-lineage-to-govern-command-contracts }

Parce que la clôture de contamination relie chaque colonne d'entrée déclarée à chaque colonne de
sortie déclarée, l'ampleur de cette clôture dépend entièrement de ce que vous déclarez.

Prenez une commande qui reçoit une table de commandes complète (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) et renvoie un `embedding`. Si le contrat d'entrée liste toutes ces colonnes, chaque colonne en aval qui utilise l'embedding montre une traçabilité venant de toutes. C'est exact, mais cela n'aide pas, car on ne voit pas ce qui a réellement compté.

Ne déclarez que `id` et `text` (les colonnes que le modèle d'embedding lit réellement), et le cône
de traçabilité se resserre sur ces deux colonnes sources. La dérivation est à la fois correcte et
précise.

Voir [Commandes](commands.md) pour la mécanique de déclaration d'un contrat d'entrée étroit.
