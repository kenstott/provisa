# Commands

Une commande est une fonction enregistrée et gouvernée qui place un calcul externe sous le système
de gouvernance, d'audit et de traçabilité de Provisa. Là où le moteur de fédération gère le SQL
nativement, une commande est le point de passage pour les calculs qu'il ne peut pas exprimer : un
microservice d'enrichissement, un modèle Python, un script shell, une procédure stockée native
d'une base de données. Enregistrez-la une fois ; chaque surface cliente — GraphQL, SQL pgwire,
REST, Arrow Flight, gRPC, Bolt/Cypher — peut l'invoquer avec une gouvernance identique (REQ-885,
REQ-1156). [tool-verified: docstring du module function_dispatch.py + REQ-885 en requirements.md]

La distinction clé : une commande est un **RPC gouverné**, pas de l'ETL ad hoc. Ses entrées et
sorties sont déclarées, typées, validées, tracées, et raccordées à la traçabilité. Un appel curl
ou un sous-processus non gouverné n'est rien de tout cela.

## Types d'implémentation

Cinq valeurs `impl_kind` sont prises en charge [tool-verified: dictionnaire `_EXECUTORS` en
function_dispatch.py:420-426] :

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Procédure stockée native sur une source enregistrée |
| `script` | Sous-processus local alimenté en JSON sur stdin, lit du JSON depuis stdout |
| `http` | Endpoint HTTP/S ; corps de requête JSON, réponse JSON |
| `grpc` | gRPC unaire ; pont JSON sans proto |
| `python` | Callable Python en processus (`module:attr`) |

L'adressage (le `name` du catalogue et `function_name`) est découplé du `binding` (transport et
emplacement). Changez le binding, et la gouvernance, la traçabilité et les contrats d'appel de la
commande restent inchangés. [tool-verified: modèle Function en models.py:710-750]

## Types d'arguments

Chaque argument déclare un `arg_kind` [tool-verified: `FunctionArgument.arg_kind` en
models.py:691-700] :

| `arg_kind` | Comportement |
| --- | --- |
| `column_value` | Scalaire ; transmis directement dans la charge utile de la requête |
| `table_ref` | Paresseux ; Provisa transmet la référence de relation telle quelle ; le service récupère les données |
| `result_set` | Immédiat ; Provisa matérialise la relation référencée et envoie ses lignes |

Les commandes `http` et `grpc` **doivent** déclarer au moins un argument `table_ref` ou
`result_set`. Une commande externe ne recevant que des arguments scalaires serait invoquée une
fois par ligne, ce qui annule tout regroupement (batching). Le dispatcher rejette cette
configuration au moment de l'appel (422). [tool-verified: `_reject_rowwise_external` en
function_dispatch.py:322-344]

Une commande qui retourne un ensemble (déclaré via `output_columns` et `return_schema`) est une
fonction table (table-valued). Utilisez-la dans une clause `FROM` ou un `JOIN`. [inferred from
models.py:744-748 and command_localize.py:52-63]

## Le contrat de jeu de données (REQ-1159)

Chaque argument `table_ref` ou `result_set` peut déclarer un **contrat de colonnes d'entrée** :
une liste ordonnée et typée IR de colonnes dans `FunctionArgument.columns`. La commande elle-même
déclare un **contrat de colonnes de sortie** dans `Function.output_columns`. [tool-verified: modèle
DatasetColumn en models.py:675-683, `Function.output_columns` en models.py:748]

Les deux contrats sont validés de façon stricte (fail-loud) à chaque invocation :

- **Entrée (result_set uniquement) :** après matérialisation, Provisa valide les lignes par
  rapport aux colonnes déclarées. Les champs en trop, les champs manquants et les types incorrects
  déclenchent tous une erreur HTTP 422. [tool-verified: `_validate_against` appelé dans
  `_prepare_args` en function_dispatch.py:243-248]
- **Sortie :** les lignes retournées par la commande sont validées par rapport à `output_columns`
  avant d'atteindre l'appelant. [tool-verified: function_dispatch.py:488-490]
- **Projection étroite :** lorsqu'un contrat d'entrée est déclaré, la requête de matérialisation
  ne projette **que ces colonnes** (`SELECT "id", "region" FROM ...`) plutôt que `SELECT *`.
  [tool-verified: `_materialize_relation` en function_dispatch.py:155-177, col_names transmis
  à la projection à la ligne 171]

### Le vocabulaire de types IR

Les types de colonnes du contrat utilisent le système de types IR canonique (REQ-846), pas les
scalaires GraphQL ni les orthographes natives de la source. Les noms valides sont [tool-verified:
clés `_IR_TO_SA` en ir_types.py:45-63] :

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Les alias courants se résolvent automatiquement (`varchar` → `text`, `int4` → `integer`, `jsonb` →
`json`, etc.). [tool-verified: dictionnaire `_ALIASES` en ir_types.py:67-90]

`return_schema` est la **projection GraphQL** de `output_columns`, pas la source de vérité.
Déclarez `output_columns` pour la validation et la traçabilité ; ajoutez `return_schema` pour la
génération de types GraphQL. [tool-verified: models.py:744-748, commentaire "return_schema is its
GraphQL projection"]

## Créer une commande

### Fichier de configuration

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

[tool-verified: bloc enrich_orders de sample_config.yaml]

La variante gRPC (`enrich_grpc_set`) suit le même modèle mais spécifie `impl_kind: grpc` et un
`binding` avec des clés `target` et `method` au lieu de `callable` :

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

[tool-verified: bloc enrich_grpc_set de config/provisa.yaml]

### Interface d'administration

Le formulaire de commande dans **Settings → Commands** inclut un éditeur de colonnes d'entrée par
jeu de données (une ligne par colonne déclarée, avec un sélecteur de type IR) et un éditeur de
colonnes de sortie. Enregistrez le formulaire pour créer ou mettre à jour la commande sans
rechargement de configuration. [inferred from CommandFormFields.tsx]

## Composition en ligne (REQ-1159)

Les commandes peuvent apparaître **à l'intérieur** d'une instruction SQL plus large — jointes,
en sous-requête, ou projetées. Vous n'êtes pas limité à `SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Avant que la gouvernance, la validation ou le routage ne s'exécutent, le pipeline détecte les
appels de commandes enregistrées, exécute chacun via l'exécuteur gouverné partagé (de sorte que le
contrat d'E/S et le modèle d'identité s'appliquent exactement comme pour un appel direct), et
réécrit le site d'appel en une relation locale typée. [tool-verified: `_localize_inline_commands`
en _pipeline.py:145-163 et localize_commands en command_localize.py:178-222]

La substitution s'adapte à la taille : jusqu'à 1 000 lignes, le résultat s'intègre sous forme de
liste `VALUES` typée ; au-delà de ce seuil, il s'enregistre comme une relation locale nommée dans
le moteur. [tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` en command_localize.py:49, chemin aux
lignes 211-216]

Une instruction localisée est routée normalement. Les requêtes mono-source restent sur la source ;
seules les requêtes véritablement multi-sources vont au moteur de fédération. [tool-verified:
_pipeline.py:304 commentaire "REQ-1159: a localized statement carries an inline local relation..."]

## Commandes et traçabilité

Comme chaque commande déclare ses colonnes d'entrée et de sortie, la traçabilité au niveau des
colonnes **se referme à travers la frontière opaque de la commande**. Le moteur de traçabilité
applique une clôture de propagation (taint closure) : chaque colonne de sortie déclarée dérive de
chaque colonne d'entrée déclarée. [tool-verified: `_splice_commands` en graph.py:223-242]

**La conséquence concrète :** la largeur de votre contrat d'entrée détermine la précision de cette
clôture. Une entrée étroite — seulement les colonnes dont la commande a réellement besoin —
produit un cône de traçabilité resserré et lisible. Déclarer toutes les colonnes de la relation
source fait converger largement chaque sortie, ce qui reste correct (aucune traçabilité n'est
perdue) mais brouille la traçabilité.

**Règle empirique :** transmettez la projection minimale dont la commande a besoin, et ne
retournez que les colonnes dérivées (pas les entrées simplement répercutées sans changement). Cela
garde le cône de propagation précis. [inferred from _splice_commands behavior in graph.py and
_materialize_relation narrow-projection in function_dispatch.py:161]

Voir [Lineage](lineage.md) pour savoir comment les nœuds de commande apparaissent dans le DAG et
comment les lire.

## Liste blanche de sortie (egress allowlist)

Les commandes `http` et `grpc` appellent des endpoints externes. Chaque hôte cible doit figurer
dans le `udf_egress_allowlist` du déploiement. Le loopback (`localhost`, `127.0.0.1`, `::1`) est
toujours autorisé. Une liste blanche absente refuse toute sortie externe avec HTTP 403 — il n'y a
pas de valeur par défaut silencieuse. [tool-verified: `_check_egress` en function_dispatch.py:292-311]

## Traçage des invocations (REQ-886)

Chaque invocation émet une trace, quel que soit le résultat. La trace inclut le nom de la
commande, le type de transport, le modèle d'identité (DEFINER ou INVOKER), les références de
relation en entrée, l'id du rôle, et la cardinalité de sortie. Le dispatcher émet la trace — aucun
`impl_kind` ne peut la contourner. [tool-verified: contexte `udf_invocation_trace` dans
dispatch_function:475-492]

## CLI : provisa metadata export

`provisa metadata export` est une tâche de niveau shell, pas un RPC gouverné. Elle déclenche la
publication de métadonnées à la demande du serveur en cours d'exécution (REQ-1072/REQ-1074) en
postant sur `/admin/metadata-export/publish` — le même endpoint qu'appelle le bouton **Publier
maintenant** de l'onglet d'administration. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Utilisez-la pour piloter des exports planifiés depuis cron ou la CI lorsque la planification
`reconcile_cron` configurée n'est pas assez fine :

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Code de sortie 0 = publication complète. Code 1 = publication partielle ou échec de connexion.

Pour la référence complète des options, les modes d'authentification, le nommage des hôtes en
multitenancy et un exemple cron, voir [Export de métadonnées — Depuis la ligne de commande](metadata-export.md#from-the-command-line).
