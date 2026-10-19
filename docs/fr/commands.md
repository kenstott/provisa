# Commandes {: #commands }

Une commande est une fonction enregistrée et gouvernée qui place un calcul externe sous la gouvernance, l'audit et la traçabilité de Provisa. Le moteur de fédération traite SQL nativement. Une commande couvre le calcul que SQL ne peut pas exprimer, comme un microservice d'enrichissement, un modèle Python, un script shell ou une procédure stockée native de la base. Vous l'enregistrez une fois, et chaque surface cliente (GraphQL, SQL via pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher) peut l'appeler sous une gouvernance identique.

Une commande est un **RPC gouverné**. Ses entrées et sorties sont déclarées, typées, validées, tracées et reliées à la traçabilité. Une étape ETL improvisée, un appel curl non gouverné ou un sous-processus n'ont rien de tout cela.

## Genres d'implémentation {: #implementation-kinds }

Six valeurs d'`impl_kind` sont prises en charge :

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Procédure stockée native sur une source enregistrée |
| `source_operation` | Une opération d'écriture d'une source OpenAPI, GraphQL distante ou gRPC, transmise telle quelle (voir [Opération d'écriture d'une source distante](#a-remote-sources-write-operation)) |
| `script` | Sous-processus local alimenté en JSON sur stdin, lisant du JSON sur stdout |
| `http` | Endpoint HTTP/S ; corps de requête JSON, réponse JSON |
| `grpc` | gRPC unaire ; passerelle JSON sans proto |
| `python` | Appelable Python en processus (`module:attr`) |

L'adressage (le `name` du catalogue et `function_name`) est découplé du `binding` (transport et
emplacement). Changez le binding et la gouvernance, la traçabilité et les contrats d'appel de la
commande restent inchangés.

## Genres d'arguments {: #argument-kinds }

Chaque argument déclare un `arg_kind` :

| `arg_kind` | Comportement |
| --- | --- |
| `column_value` | Scalaire ; transmis directement dans la charge utile de la requête |
| `table_ref` | Paresseux ; Provisa transmet la référence de relation telle quelle ; le service va chercher les données |
| `result_set` | Empressé ; Provisa matérialise la relation référencée et envoie ses lignes |

Les commandes `http` et `grpc` **doivent** déclarer au moins un argument `table_ref` ou
`result_set`. Une commande externe ne recevant que des arguments scalaires serait invoquée une fois
par ligne, ce qui ruine le traitement par lots. Le répartiteur rejette cette configuration au
moment de l'appel (422).

Une commande qui renvoie un ensemble (déclaré via `output_columns` et `return_schema`) est une
fonction table. Utilisez-la dans une clause `FROM` ou une `JOIN`.

## Le contrat de jeu de données {: #the-dataset-contract }

Chaque argument `table_ref` ou `result_set` peut déclarer un **contrat de colonnes d'entrée** : une
liste ordonnée de colonnes typées en IR dans `FunctionArgument.columns`. La commande elle-même
déclare un **contrat de colonnes de sortie** dans `Function.output_columns`.

Les deux contrats sont validés en mode « échec bruyant » à chaque invocation :

- **Entrée (result_set uniquement) :** après matérialisation, Provisa valide les lignes contre les
  colonnes déclarées. Champs en trop, champs manquants et types erronés lèvent tous un HTTP 422.

- **Sortie :** les lignes renvoyées par la commande sont validées contre `output_columns` avant
  d'atteindre l'appelant.
- **Projection étroite :** lorsqu'un contrat d'entrée est déclaré, la requête de matérialisation ne
  projette **que ces colonnes** (`SELECT "id", "region" FROM ...`) plutôt que `SELECT *`.

### Le vocabulaire de types de l'IR {: #the-ir-type-vocabulary }

Les types de colonne des contrats utilisent les noms de types IR canoniques :

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Les alias courants se résolvent automatiquement (`varchar` → `text`, `int4` → `integer`, `jsonb` →
`json`, etc.).

`return_schema` est la **projection GraphQL** de `output_columns`. La source de vérité est `output_columns` : déclarez-le pour la validation et la traçabilité, et ajoutez `return_schema` pour générer les types GraphQL.

## Écrire une commande {: #authoring-a-command }

### Fichier de configuration {: #config-file }

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

La variante gRPC (`enrich_grpc_set`) suit le même patron mais précise `impl_kind: grpc` et un
`binding` portant les clés `target` et `method` au lieu de `callable` :

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

### Interface d'administration {: #admin-ui }

Le formulaire de commande dans **Paramètres → Commandes** comprend un éditeur de colonnes d'entrée
par jeu de données (une ligne par colonne déclarée, avec un sélecteur de type IR) et un éditeur de
colonnes de sortie. Enregistrez le formulaire pour enregistrer ou mettre à jour la commande sans
recharger la configuration.

## Composition en ligne {: #inline-composition }

Les commandes peuvent figurer **à l'intérieur** d'une instruction SQL plus large, où vous les joignez, les placez en sous-requête ou les projetez ; `SELECT * FROM fn(args)` n'en est que la forme la plus simple. L'exception est l'opération d'écriture d'une source distante, appelée seule (voir [Pourquoi elle ne peut pas être composée](#why-it-cannot-be-composed)).

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Avant que la gouvernance, la validation ou le routage ne s'exécutent, le pipeline détecte les
appels de commandes enregistrées, exécute chacun d'eux via l'exécuteur gouverné partagé (de sorte
que le contrat d'E/S et le modèle d'identité s'appliquent exactement comme pour un appel direct),
puis réécrit le site d'appel en une relation locale typée.

La substitution s'adapte à la taille : jusqu'à 1 000 lignes, le résultat est inséré en ligne sous
forme de liste `VALUES` typée ; au-delà de ce seuil, il est enregistré comme relation locale nommée
dans le moteur.

Une instruction localisée est routée normalement. Les requêtes mono-source restent sur la source ;
seules les requêtes véritablement multi-sources partent vers le moteur de fédération.

## Opération d'écriture d'une source distante {: #a-remote-sources-write-operation }

Une source OpenAPI, GraphQL distante ou gRPC propose des opérations d'écriture. Enregistrer l'une d'elles comme commande la rend appelable, gouvernée et auditée depuis toutes les surfaces. Ajouter la source n'en enregistre aucune ; vous enregistrez celles que vous voulez, une à une, comme vous enregistrez des tables. L'enregistrement est la curation.

Ce qu'une source propose :

| Type de source | Opérations proposées | Nom de l'opération |
| --- | --- | --- |
| `openapi` | Toute opération non-GET de la spécification | L'`operationId` |
| `graphql_remote` | Tout champ du type `Mutation` distant | Le nom du champ |
| `grpc_remote` | Toute méthode classée comme mutation | `Service.Method` |

### En enregistrer une {: #register-one }

1. Ouvrez **Modèle → Commandes** et ajoutez une commande.
2. Choisissez la source distante. Le formulaire bascule vers un sélecteur d'opération qui liste ce que la source propose.
3. Choisissez l'opération, un domaine et les rôles qui peuvent l'appeler.
4. Activez éventuellement **Nécessite une approbation** et renseignez le champ **Écrit dans la table**.

Via l'API GraphQL d'administration, `availableFunctions(sourceId, schemaName)` liste les opérations. Le nom de schéma est `openapi`, `graphql` ou `grpc_remote`, selon le type de source.

Tout le reste découle de l'opération, quoi qu'envoie le formulaire. Provisa écrase ces champs :

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Chaque argument est typé `json`. Les arguments de l'opération sont ses paramètres de chemin OpenAPI (plus `body` lorsque l'opération prend un corps de requête), ses arguments de mutation GraphQL ou ses champs de requête gRPC. Une opération que la source ne propose pas est refusée avec 422, `functions.operation_not_offered`.

### L'appeler {: #calling-it }

Provisa ne met pas en forme, ne type pas et ne vérifie pas l'entrée. Chaque argument part vers le service distant sans changement, avec l'identifiant de la source, et la réponse du service distant revient sans changement. Provisa gouverne qui peut appeler, dans quel domaine, si une approbation est requise, et enregistre l'appel.

Comment le service distant reçoit les arguments :

- **OpenAPI.** Les paramètres de chemin remplissent le gabarit du chemin. `body` est le corps JSON de la requête. Tout autre argument va dans la chaîne de requête.
- **GraphQL.** Les arguments sont envoyés comme variables typées, chacune déclarée avec le type que lui donne le schéma distant. La mutation redemande les champs scalaires et d'énumération de la réponse, et ceux des objets qu'elle contient, jusqu'à deux niveaux de profondeur.
- **gRPC.** Les arguments deviennent le message de requête de `Service.Method`.

La réponse constitue les lignes de la commande : un objet est une ligne, une liste d'objets forme ses lignes, tout le reste est une ligne `{"result": ...}`.

Avec GraphQL, la commande est un champ de mutation et sa réponse est le scalaire JSON.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

Sur les surfaces SQL (pgwire et les autres qui transmettent du SQL), écrivez chaque argument comme un littéral JSON dans une chaîne. `'{"title": "x"}'` est un objet, `'"text"'` une chaîne, `'3'` un nombre. Un littéral qui n'est pas du JSON valide échoue avec 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

Avec REST, envoyez par POST un objet JSON d'arguments à `/data/rest/{domain}/commands/{command}`. Un argument `json` est documenté dans la spécification générée comme une valeur quelconque.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Refus {: #refusals }

Le refus du service distant revient tel que le service l'a formulé.

| Service distant | Provisa répond |
| --- | --- |
| Refuse l'appel (HTTP 4xx, `errors` GraphQL, un appel gRPC refusé) | 422, `functions.remote_refused`, avec `remote_status` et `answer` |
| Échoue (HTTP 5xx) | 502, `functions.remote_refused` |

C'est au service distant de dire, lors de l'appel de l'opération, si l'identifiant de la source peut l'exécuter. Provisa ne peut pas essayer une écriture à l'enregistrement sans l'effectuer.

### Approbation {: #approval }

Activez **Nécessite une approbation** et chaque appel est soumis au point d'ancrage d'approbation du déploiement avant de s'exécuter. Il ne s'exécute que si le point d'ancrage approuve.

- Aucun point d'ancrage configuré : 403, `functions.approval_unavailable`.
- Le point d'ancrage refuse : 403, `functions.approval_denied`, avec le motif du point d'ancrage.

Le point d'ancrage reçoit l'appelant, le rôle, le nom de la commande et ses arguments. Voir [Point d'ancrage d'approbation ABAC](security.md#abac-approval-hook). Le drapeau est stocké sous `Function.requires_approval`, et la vérification s'applique à toute commande qui l'active.

### Écrit dans la table {: #writes-table }

Indiquez dans le champ **Écrit dans la table** la table dans laquelle l'opération écrit, sous la forme `schema.table`. Ce doit être une table enregistrée de la source propre à la commande, faute de quoi l'enregistrement est refusé avec 422, `actions.written_table_not_registered`. Le paramètre est facultatif.

Après chaque appel que le service distant accepte, Provisa le traite comme une écriture dans cette table. Il supprime les réponses en cache de la table, marque comme périmées les vues matérialisées qui s'appuient sur elle, émet l'événement de changement, exécute les commandes qui s'exécutent quand elle change et recharge la table lorsqu'elle est conservée à chaud.

Lorsque la table est lue depuis son réplica de table entière (les paramètres de l'opérateur l'y placent, ou le moteur ne peut pas lire sa source sur place), une construction du réplica est demandée après l'appel, avec le motif `write`. Les lecteurs conservent l'ancien réplica jusqu'à ce que le nouveau le remplace. Une table répliquée ligne par ligne, ou dotée d'une colonne de paramètre, n'a pas de réplica entier à reconstruire.

### Pourquoi elle ne peut pas être composée {: #why-it-cannot-be-composed }

Une opération d'écriture est une action. Elle ne transforme pas de données, et une vue ou vue matérialisée qui en contiendrait une effectuerait l'écriture à chaque lecture ou rafraîchissement. L'appel reste donc seul. `SELECT * FROM create_issue(...)` seul l'exécute, et le même appel dans une instruction plus large est refusé où qu'il se trouve, dans une jointure, une sous-requête ou une projection.

Une vue ou vue matérialisée dont la définition en appelle une est refusée à l'enregistrement. Une définition qui ne s'analyse pas est également refusée tant qu'une opération d'écriture est enregistrée, car on ne peut pas montrer qu'elle n'en appelle aucune.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

Une opération de source n'est pas non plus un nœud de traçabilité. La traçabilité se lit dans le SQL des vues et des requêtes, où une commande apparaît comme un nœud, et aucune définition enregistrée ne peut appeler une opération de source.

## Commandes et traçabilité {: #commands-and-lineage }

Parce que chaque commande déclare ses colonnes d'entrée et de sortie, la traçabilité au niveau des
colonnes **se referme par-dessus la frontière opaque de la commande**. Le moteur de traçabilité
applique une clôture de contamination : chaque colonne de sortie déclarée dérive de chaque colonne
d'entrée déclarée.

**La conséquence pratique :** la largeur de votre contrat d'entrée fixe la précision de cette fermeture. Une entrée étroite, limitée aux colonnes dont la commande a besoin, donne un cône de traçabilité serré et lisible. Déclarer chaque colonne de la relation source ouvre largement l'éventail sur chaque sortie. Cela reste correct, aucune traçabilité n'est perdue, mais la lisibilité en souffre.

**Règle empirique :** passez la projection minimale dont la commande a besoin et ne renvoyez que des colonnes dérivées. Les entrées renvoyées telles quelles élargissent le cône de contamination ; laissez-les de côté.

Voir [Traçabilité](lineage.md) pour savoir comment les nœuds de commande apparaissent dans le DAG et comment les lire.

## Liste d'autorisation de sortie {: #egress-allowlist }

Les commandes `http` et `grpc` appellent des points de terminaison externes. Chaque hôte cible doit figurer dans la `udf_egress_allowlist` du déploiement. Le loopback (`localhost`, `127.0.0.1`, `::1`) est toujours autorisé. Sans liste d'autorisation, toute sortie externe est refusée avec HTTP 403, sans valeur par défaut silencieuse.

## Traçage des invocations {: #invocation-tracing }

Chaque invocation émet une trace, quel que soit le résultat. La trace contient le nom de la commande, le type de transport, le modèle d'identité (DEFINER ou INVOKER), les références aux relations d'entrée, l'id du rôle et la cardinalité de sortie. C'est le répartiteur qui l'émet ; aucun `impl_kind` ne peut donc y échapper.

## CLI : provisa metadata export

`provisa metadata export` est une tâche du niveau shell, exécutée hors du chemin RPC gouverné. Elle déclenche la publication des métadonnées à la demande du serveur en cours d'exécution en envoyant à `/admin/metadata-export/publish`, le point de terminaison qu'appelle le bouton **Publier maintenant** de l'onglet Admin.

Utilisez-la pour piloter des exports minutés depuis cron ou la CI lorsque la planification
`reconcile_cron` configurée n'est pas assez fine :

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Sortie 0 = publication complète. Sortie 1 = publication partielle ou échec de connexion.

Pour la référence complète des options, les modes d'authentification, le nommage des hôtes en
multilocataire et un exemple cron, voir
[Export de métadonnées — Depuis la ligne de commande](metadata-export.md#from-the-command-line).

Les commandes apparaissent dans la projection git de chaque environnement. Voir [Environnements](environments.md) pour savoir comment une commande et ses affectations d'étiquettes survivent à une fusion et à un pull.
