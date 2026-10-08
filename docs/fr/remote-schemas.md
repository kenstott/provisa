# Schémas distants

Une source de schéma distant connecte une API externe — GraphQL (y compris GitHub), gRPC ou REST (OpenAPI) — à le modèle de Provisa. Ajouter une source n'enregistre aucune table. La source propose des tables, et un data steward enregistre chaque table souhaitée via le sélecteur « Register Table » ; cet enregistrement est l'étape de curation. (REQ-308, REQ-316, REQ-322) Une table enregistrée est une table Provisa de première classe. (REQ-308, REQ-316, REQ-325) Chaque règle de gouvernance, chaque interface de requête et chaque couche de sécurité s'applique automatiquement. (REQ-310, REQ-319, REQ-328) Le service distant ne voit jamais les règles de gouvernance de Provisa. (REQ-310, REQ-319, REQ-328)

---

## Trois types de source

### Schéma distant GraphQL (REQ-307–313)

**Comment ajouter la source.** Envoyer une requête POST à `/admin/sources/graphql-remote` avec l'URL du endpoint, un namespace et une authentification facultative. Provisa déclenche une requête d'introspection `__schema` standard sur le endpoint distant afin de confirmer le endpoint et l'identifiant. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

Ajouter la source n'enregistre ni table ni command. Tout type de source distante répond à l'ajout et à l'actualisation avec les mêmes compteurs : `tables` (tables enregistrées ou mises à jour ; 0 à l'ajout), `available_tables` (tables proposées), `mutations` (toujours 0) et `available_mutations` (commands proposées). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**Enregistrer des tables.** Ouvrir Tables, puis Register Table, choisir la source et le schéma `graphql`, puis sélectionner les tables et colonnes souhaitées. Via l'API GraphQL d'administration : `availableTables(sourceId, schemaName)` liste les tables proposées, `availableColumns` liste les colonnes d'une table et `registerTable(input: TableInput)` en enregistre une avec les colonnes choisies. Une table enregistrée est alors gouvernée. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

La manière dont une table enregistrée est lue (champ racine, chemin des lignes, arguments obligatoires, arguments de pagination) est stockée dans `sources.mapping["tables"]`, de sorte qu'un processus redémarré la lit sans interroger le distant sur son schéma. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Options d'authentification : `none`, `bearer` (en-tête Authorization), `basic` (nom d'utilisateur:mot de passe en Base64). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Surcharges de champ.** `field_overrides` est une correspondance `{fieldName: "query" | "mutation"}` appliquée après l'introspection. Elle est prioritaire sur la classification structurelle. Seuls les champs de type query peuvent être reclassés en mutation ; les champs de type mutation n'ont pas de chemin de surcharge en GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relations au moment de l'enregistrement.** `relationships` déclare des chemins de jointure clé étrangère/clé primaire entre tables au moment de l'enregistrement. Elles sont stockées comme des relations déclarées manuellement (sans indicateur `remote_managed`). Lors d'une actualisation, les relations détectées automatiquement (celles avec `remote_managed: True`) sont réexécutées et peuvent changer ; les relations déclarées manuellement ne sont pas modifiées. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**Ce que la source propose.** Tout champ du type `Query` distant qui renvoie un objet ou une liste d'objets est proposé comme table, de même que chaque connexion Relay sous un champ à objet unique (voir ci-dessous). Enregistrer une table proposée en fait une table. Chaque champ du type `Mutation` distant est une commande proposée, comptée dans `available_mutations` ; ajouter la source n'en enregistre aucune. Enregistrez celles que vous voulez comme commandes ; voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**Nommage des tables.** Les tables sont nommées `{namespace}__{field_name}`. Avec le namespace `petstore` et un champ de requête `pets` : le nom de la table est `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Connexions Relay.** De nombreuses API renvoient les listes sous forme de connexions Relay : un objet avec `nodes` (ou `edges { node }`) à côté de `pageInfo`. Provisa fait correspondre une connexion à une table de ses nœuds et la lit page par page. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- Un champ racine qui renvoie une connexion (`securityAdvisories`) devient une table de ses nœuds.
- Une connexion sur l'objet unique renvoyé par un champ racine devient sa propre table. La table reprend les arguments obligatoires du champ racine. Avec `repository(owner, name)` et une connexion `issues` sur `Repository`, la table est `repositoryIssues`, de nom SQL `gh__repository_issues` sous le namespace `gh`. Filtrez-la via les colonnes `_nf_owner` et `_nf_name` : `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Une connexion n'est jamais une colonne. Sinon, une ligne porterait une lecture que le distant calcule par ligne, pour chaque connexion que son type possède.
- Une connexion n'est une table que si son champ accepte `first` et `after`, afin de pouvoir être lue page par page. Une connexion qui exige un argument qui lui est propre n'est pas une table. Il en va de même d'une connexion d'une union, ou de toute connexion sous un champ racine qui renvoie une liste.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**Correspondance des types (REQ-308).** Les champs scalaires sont directement mis en correspondance avec les types Provisa. Les champs OBJECT se répartissent en deux cas selon que le type cible est gouverné ou non (voir « Tables gouvernées » ci-dessous). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Type GraphQL | Type Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (type inline non gouverné, p. ex. `ContactInfo`) | colonne blob `jsonb` |
| OBJECT (type cible gouverné) | entièrement exclu du SDL et de la récupération |
| Tout ENUM | `jsonb` |
| Scalaire personnalisé | `text` (valeur de repli) |

**Tables gouvernées.** Un type GQL est gouverné lorsqu'il apparaît comme champ racine de `Query` dans le schéma distant. `_collect_queryable_types` recense ces types lors de l'enregistrement, en privilégiant les champs sans argument obligatoire afin qu'ils puissent être récupérés en masse en tant que cibles de jointure. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Lorsqu'une colonne de type OBJECT sur une table gouvernée pointe vers un autre type gouverné, cette colonne est soumise à trois règles simultanément [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`] :

1. **Exclue de la récupération GQL** — le champ n'est pas demandé lors de la récupération des lignes de la table parente.
2. **Exclue du SDL** — le champ n'apparaît pas sur le type parent dans le schéma généré.
3. **Accessible uniquement via une relation déclarée** — un data steward doit enregistrer une jointure entre les deux tables gouvernées matérialisées. En l'absence de cette relation, le champ est simplement absent ; il n'y a pas de repli sous forme de blob.

Les types OBJECT qui ne sont PAS accessibles en tant que champs racine de Query (types inline tels que `ContactInfo` ou `Address`) suivent des règles différentes : ils sont récupérés sous forme de colonnes blob `jsonb` et apparaissent dans le SDL comme des champs d'objet imbriqué. Les sous-champs sont accessibles via une extraction `-->>` en SQL.

**Les champs qui exigent un argument ne sont pas des colonnes.** Un champ avec un argument obligatoire ne peut pas être sélectionné tel quel ; il est donc écarté des colonnes de la table et des sélections imbriquées. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Arguments obligatoires.** Lorsqu'un champ de requête racine possède des arguments non nuls sans valeur par défaut, ceux-ci deviennent des colonnes `native_filter_type: query_param` sur la table (préfixées `_nf_` au moment de l'injection). L'exécuteur les transmet en tant que variables GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relations détectées automatiquement.** Provisa analyse les colonnes de type OBJECT de chaque table enregistrée. Lorsque le type GQL référencé est lui aussi une table enregistrée dans la même source, et que la colonne sur laquelle repose la relation figure parmi les colonnes enregistrées, la relation est stockée. Une table non encore enregistrée n'en reçoit aucune. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] Les relations many-to-one déduisent les colonnes source et cible à partir de conventions de nommage (`breedName` sur le type source → `name` sur le type cible `Breed`). Les champs one-to-many (LIST) émettent des relations avec des références de colonnes vides — la clé étrangère se trouve du côté cible. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** Un champ de mutation est enregistré, un à un, comme commande de genre `source_operation`. Ses arguments sont chacun typés `json` et transmis au service distant comme variables typées ; la réponse est le JSON que renvoie le service distant, sans `return_schema`. Voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**Actualisation.** Envoyer une requête POST à `/admin/sources/graphql-remote/{id}/refresh`. Ré-introspecte le schéma distant et met à jour, par rapport à lui, les tables déjà enregistrées. Elle n'ajoute ni table ni colonne : une table ou une colonne que le schéma a gagnée reste proposée, et une colonne que le schéma a perdue est supprimée. Les règles de gouvernance existantes (RLS, masquage) sont préservées. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**Limitations.**

- Les champs de requête racine de type scalaire et ENUM (dont le type de retour n'est pas OBJECT) deviennent des fonctions suivies, et non des tables virtuelles. Leur `return_schema` est une colonne unique `value` du type scalaire correspondant. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- L'imbrication d'objets est résolue au moment de l'enregistrement jusqu'à `graphql_remote.max_object_depth` (par défaut : 5). La sélection de la récupération distante et les métadonnées des sous-champs sont toutes deux construites jusqu'à cette profondeur ; les champs au-delà de la limite ne sont pas récupérés et ne sont pas disponibles pour l'extraction SQL. Un type n'est visité qu'une fois le long d'un même chemin : un champ dont le type figure déjà sur le chemin descendant est écarté, de sorte qu'un schéma dont les types se réfèrent les uns aux autres est parcouru une fois par type, et non une fois par niveau de profondeur. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- Les champs OBJECT imbriqués de type LIST (p. ex. `breed.awards: [Award]`) sont inclus dans la sélection de récupération jusqu'à `graphql_remote.max_list_depth` niveaux d'imbrication (par défaut : 2). Dans cette limite, la liste est récupérée sous forme de tableau `jsonb` dans la colonne parente. Lorsque le champ de liste déclare un argument `first` (Relay, PostGraphile, pg_graphql) ou un argument `limit` (Hasura), la sélection le transmet sous la forme `first: N` ou `limit: N`, où N vaut `graphql_remote.max_list_items` (par défaut : 100). Un champ de liste qui ne déclare ni l'un ni l'autre ne reçoit aucun argument, car un distant rejette un argument que le champ ne déclare pas. Au-delà de `max_list_depth`, le champ LIST est entièrement exclu afin d'éviter une expansion illimitée des données. En SQL, le tableau est accessible via `json_array_elements(column_name)` ou par extraction d'index avec `->>`. Si le type d'élément de la liste possède sa propre requête racine, enregistrez-le plutôt comme table distincte et créez une relation — le chemin de jointure est plus efficace et contourne le blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- Pour les requêtes SQL, les colonnes de type OBJECT non gouvernées sont récupérées intégralement depuis la source distante (tous les sous-champs jusqu'à la profondeur configurée) et mises en cache sous forme de `jsonb`. L'accès aux sous-champs en SQL est géré via une extraction `->>` sur le blob ; la requête distante n'est pas restreinte aux seuls champs sélectionnés par la requête SQL. Lorsque le type d'élément de la liste n'a pas de requête racine et que la représentation en blob est insuffisante, il convient d'écrire directement la requête en SDL GraphQL — Provisa reproduit fidèlement la sélection de champs GQL, de sorte que la source distante reçoit exactement les champs demandés. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Si le serveur distant rejette un champ de type OBJECT parce qu'il exige une sélection de sous-champs (ce qui ne devrait pas se produire lorsque `gql_selection` est disponible), l'exécuteur retente une fois en retirant ces champs afin que les colonnes scalaires soient tout de même renvoyées. Cela s'applique aux tables lues depuis un champ racine. Une table de connexion n'emprunte pas ce chemin. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Lectures paginées.** Une table de connexion est lue par curseur. Chaque page demande `first: N, after: $pageCursor` avec `pageInfo { hasNextPage endCursor }`, et la lecture suit `endCursor` jusqu'à ce que le distant indique qu'il n'y a plus de page suivante. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Paramètre | Valeur par défaut | Effet |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Lignes par page. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | Le plus grand nombre de lignes qu'une lecture d'une table de connexion prend. Une lecture qui l'atteint s'arrête et consigne un avertissement. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Deux réponses amènent l'exécuteur à réessayer :

- **Page trop lourde.** Lorsque le distant répond 502 ou 504, la même page est redemandée à la moitié de sa taille, jusqu'à une ligne. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Limite de débit avec délai d'attente.** Lorsque le distant répond 403 ou 429 avec un `Retry-After` de 120 secondes ou moins, l'exécuteur attend ce délai puis renvoie la requête, jusqu'à trois tentatives. Un refus sans `Retry-After`, ou demandant une attente plus longue, est levé comme une erreur. Cela s'applique à toute lecture, de connexion ou non. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Toute autre erreur dans la réponse fait échouer la lecture, sauf si le type de source en décide autrement (voir GitHub ci-dessous). Une connexion dont le parent est revenu nul n'a aucune ligne. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub est un type de source ordinaire. Son API est GraphQL ; ses tables se comportent donc comme décrit ci-dessus, y compris les tables de connexion telles que `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Ajouter la source.**

1. Ouvrir Sources et ajouter une source de type **GitHub**.
2. Saisir un jeton d'accès GitHub. Saisir éventuellement un namespace, le préfixe des noms de table ; la valeur par défaut est `gh`.
3. Enregistrer. Provisa vérifie le jeton auprès de GitHub. Un jeton que GitHub rejette fait échouer l'ajout avec le message de GitHub.

Ajouter la source n'enregistre aucune table. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Enregistrer des tables.** Ouvrir Tables, puis Register Table. Choisir la source GitHub, choisir le schéma `graphql`, puis choisir les tables souhaitées. Toute table que GitHub propose est listée ; l'enregistrement est votre choix de ce qu'il faut exposer. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Portées (scopes) du jeton.** Lorsque vous enregistrez une table, Provisa la vérifie une fois auprès de GitHub avec votre jeton.

- Un champ que les portées du jeton ne couvrent pas est écarté de la table. Le résultat nomme chaque champ écarté : `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- Une table que le jeton ne peut pas lire du tout est refusée, avec la raison de GitHub : `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Lignes que le jeton ne peut pas voir.** GitHub répond `FORBIDDEN` pour un champ que le jeton ne peut pas voir sur une ligne particulière, comme les collaborateurs d'un dépôt sans accès en écriture, et `NOT_ORG_OWNED_REPO` pour un champ qui n'existe que sur les dépôts appartenant à une organisation. Ce champ est nul dans cette ligne, le reste de la lecture se poursuit et Provisa consigne un avertissement. Une erreur portant sur la table elle-même fait échouer la lecture. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Pages lourdes.** Lorsque GitHub répond `RESOURCE_LIMITS_EXCEEDED` parce que le calcul d'une page coûte trop cher, la page est redemandée à la moitié de sa taille. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Objets imbriqués.** Les tables GitHub utilisent leur propre profondeur d'imbrication de 0 (`max_object_depth=0` pour ce type de source), et non `graphql_remote.max_object_depth`. Une colonne d'objet imbriqué est sélectionnée avec ses seuls champs scalaires ; les objets qu'elle contient apparaissent sous la forme `__typename`. [tool-verified: `brands.py`]

**Stockage du jeton.** Le jeton est placé dans le coffre de secrets et la ligne de la source conserve une référence, de sorte qu'un redémarrage relit la source sans qu'il faille ressaisir le jeton. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**Fonctionnement (opérateurs).** Le schéma de GitHub est livré avec Provisa ; ajouter la source ne déclenche donc aucun appel d'introspection, et un grand schéma ne coûte rien à l'enregistrement. Les tables en sont mappées une à une au fur et à mesure de leur enregistrement. Le endpoint refresh refuse ce type de source ; un nouveau schéma GitHub arrive avec une version de Provisa. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab est un type de source ordinaire, ajouté et enregistré comme GitHub : ajouter une source de type **GitLab** avec un jeton d'accès, puis enregistrer les tables souhaitées du schéma `graphql`. Le préfixe par défaut des noms de table est `gl`. La source atteint `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Choisir les colonnes.** GitLab attribue un coût à chaque requête et refuse celle qui coûte trop cher : 200 points pour un appelant anonyme, 250 avec un jeton. Une table large avec toutes les colonnes sélectionnées dépasse ce coût ; enregistrez donc une table GitLab avec les colonnes que vous voulez. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- Lorsque vous enregistrez une table, Provisa demande une fois à GitLab s'il traitera la sélection à la taille de page qu'utilisent les lectures. Si GitLab répond que la requête est trop complexe ou trop volumineuse, la table n'est pas enregistrée et le résultat porte le message de GitLab : `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- Le coût d'une colonne dépend de sa nature. Une valeur simple coûte environ un point ; une colonne d'objet imbriqué coûte plusieurs fois plus. Retirer les colonnes d'objet imbriqué libère le plus. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- La taille de page fait partie du coût. Il s'agit de `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Vérification du jeton.** GitLab répond à un jeton non reconnu par un résultat vide, et non par une erreur. Provisa y voit un jeton rejeté et n'ajoute pas la source. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### Schéma distant gRPC (REQ-322–329)

**Comment ajouter la source.** Envoyer une requête POST à `/admin/grpc-remote/register` avec l'adresse du serveur, un chemin ou une URL vers un fichier `.proto` et une configuration TLS facultative. Ajouter la source n'enregistre aucune table.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa récupère le proto, l'analyse avec un analyseur texte pur (sans dépendance proto externe au moment de l'analyse), compile les stubs Python via `grpc_tools.protoc`, et ouvre un `grpc.aio.Channel` persistant. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Les fichiers proto peuvent également être des chemins locaux. Les chemins d'importation pour les types bien connus (`google/protobuf/timestamp.proto`) sont stockés au moment de l'enregistrement et réutilisés lors de l'actualisation. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**Ce que la source propose.** Chaque méthode `rpc` du proto est classée comme query ou mutation selon trois signaux, par ordre de priorité : (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** dans le payload d'enregistrement — `{"MethodName": "query"}` ou `{"MethodName": "mutation"}` est prioritaire sur tout le reste.
2. **`server_streaming: true`** — le serveur envoie un flux de messages ; c'est toujours une table virtuelle (sauf si la sortie est un scalaire).
3. **Le message de sortie possède un champ répété de type message** — p. ex. `ListOrdersResponse { repeated Order items; }` est traité comme un enveloppant de liste et devient une table virtuelle. Les champs scalaires répétés (p. ex. `repeated string tags`) ne déclenchent pas cette règle — ce sont des propriétés de type tableau d'une entité unique, et non des sources de lignes.

Les méthodes qui ne correspondent à aucun de ces signaux (RPC unaire renvoyant un message d'entité unique, ou toute sortie scalaire) deviennent des fonctions suivies.

**Enregistrer des tables.** Chaque méthode de query est proposée comme une table, nommée `{namespace}__{Service}__{Method}`, sous le schéma de sélecteur `grpc_remote`. Enregistrez celles que vous voulez avec le sélecteur Register Table (`availableTables`, `availableColumns`, `registerTable`, comme pour les sources GraphQL), en choisissant les colonnes de réponse. Les champs de la requête deviennent des colonnes de filtre natif `_nf_*`, et celles-ci sont toujours incluses. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Les méthodes de mutation sont des commandes proposées, comptées dans `available_mutations` ; ajouter la source n'en enregistre aucune. Une mutation gRPC s'enregistre sur la page Commandes en choisissant la source puis la méthode, nommée `Service.Method` ; le genre de la commande est `source_operation`. Voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**Nommage des tables.** Le nom par défaut est `{namespace}__{ServiceName}__{MethodName}`. En l'absence de namespace, les noms du service et de la méthode sont assemblés directement. Toute table enregistrée peut recevoir un `alias` ; lorsqu'il est défini, l'alias devient le nom utilisé partout (requêtes, SDL, relations). Le nom généré automatiquement constitue la clé d'enregistrement et ne change jamais. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Correspondance des types (REQ-324).** Les types scalaires proto sont mis en correspondance avec les types SQL comme suit. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Type Proto | Type SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Message imbriqué | `jsonb` |
| Enum | `text` |

**Relations au moment de l'enregistrement.** `relationships` fonctionne de manière identique à l'adaptateur GQL — elle déclare des chemins de jointure clé étrangère/clé primaire stockés comme des relations déclarées manuellement (sans indicateur `remote_managed`). Lors d'une actualisation, ces relations sont préservées sans modification. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Méthodes de requête (REQ-325).** Les champs du message de sortie deviennent des colonnes de table. Les champs du message d'entrée deviennent à la fois des arguments GraphQL transmis à l'appel distant *et* des colonnes enregistrées avec le préfixe `_nf_` et `native_filter_type: "grpc_input"` — le même mécanisme utilisé par GQL et OpenAPI pour l'injection de filtres natifs. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Sous-champs de messages imbriqués.** Pour les méthodes de requête, les champs de type message non répétés à la profondeur 0 (colonnes de sortie directes) voient leurs sous-champs résolus un niveau plus loin et stockés comme `object_fields` dans le `ColumnDef`. Ces métadonnées sont utilisées pour l'extraction de sous-champs `jsonb` en SQL et pour la documentation du schéma. Les champs imbriqués au-delà de la profondeur 1 ne sont pas développés récursivement. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Les méthodes en streaming côté serveur regroupent tous les messages diffusés en une liste avant de renvoyer les lignes. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Méthodes de mutation (REQ-326).** Une méthode de mutation enregistrée est une commande dont les arguments sont les champs du message d'entrée, chacun typé `json` et transmis sans changement. La réponse du service distant revient sous forme de lignes ; un appel refusé est un 422, `functions.remote_refused`. Voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**Gestion des canaux.** Un `grpc.aio.Channel` par source enregistrée est stocké dans l'état de l'application et réutilisé pour les requêtes suivantes. L'ancien canal est fermé avant qu'un nouveau ne s'ouvre lors de l'actualisation. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Actualisation.** Envoyer une requête POST à `/admin/grpc-remote/refresh/{source_id}`. Recharge le proto depuis le chemin stocké, recompile les stubs et met à jour les tables déjà enregistrées par rapport au proto, avec les colonnes avec lesquelles chacune a été enregistrée. Elle n'enregistre aucune nouvelle table ; une méthode de query ajoutée au proto reste proposée. Il est également possible d'envoyer une requête PUT à `/admin/grpc-remote/{source_id}/proto` avec un nouveau `proto_text` pour mettre à jour le proto en ligne. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**Limitations.**

- L'extraction de sous-champs d'objet se limite à un niveau de profondeur. Les champs de message imbriqués au-delà de la profondeur 1 ne sont pas développés récursivement. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Comment ajouter la source.** Envoyer une requête POST à `/admin/openapi/register` avec un ID de source et une spécification, chargée depuis un fichier local ou une URL. La spécification est analysée et conservée avec la source ; aucune table ni command n'est enregistrée. La réponse indique `tables: 0` et `mutations: 0`, les compteurs proposés figurant dans `available_tables` et `available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**Enregistrer des tables.** Enregistrez chaque opération GET souhaitée via le sélecteur Register Table (`availableTables`, `availableColumns`, `registerTable`), en choisissant les colonnes. Enregistrez chaque opération non-GET individuellement comme commande sur la page Commandes, listées par `availableFunctions` ; voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). `PUT /admin/openapi/spec/{source_id}` stocke une spécification modifiée à la main, n'enregistre rien et renvoie `available_tables` et `available_mutations`. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

**Payload d'enregistrement.** Le endpoint `/admin/openapi/register` accepte deux champs supplémentaires en plus de `source_id`, `spec_path`, etc. :

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**Ce que la source propose.** Toute opération GET de la spécification est proposée comme table, sauf si son schéma de réponse est un type scalaire (`string`, `number`, `boolean`, `integer`) — les opérations GET qui renvoient un scalaire sont des fonctions à une seule colonne `value`. Toute opération non-GET (POST, PUT, PATCH, DELETE) est proposée comme commande, nommée par son `operationId`. Une fois enregistrée, elle prend les paramètres de chemin de l'opération et un argument `body` pour le corps de la requête, chacun typé `json` ; tout autre argument va dans la chaîne de requête. Voir [Opération d'écriture d'une source distante](commands.md#operation-decriture-dune-source-distante-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

Ordre de priorité de la classification : `operation_overrides` (payload) est prioritaire sur `x-provisa-kind` (extension de la spécification), elle-même prioritaire sur l'heuristique GET. `operation_overrides` est le chemin de surcharge recommandé ; `x-provisa-kind` est destiné aux cas où la spécification elle-même doit porter la classification. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relations au moment de l'enregistrement.** `relationships` fonctionne de manière identique aux autres adaptateurs — stockée comme des relations déclarées manuellement, préservées lors de l'actualisation. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Nommage des tables.** Les tables utilisent l'`operationId` de l'opération. En l'absence d'`operationId` défini, Provisa génère un slug `{method}_{path}`. Un alias est dérivé en supprimant le segment verbal initial et en mettant le nom au singulier (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Correspondance des types.** Les types JSON Schema sont mis en correspondance avec les types Provisa comme suit. [tool-verified: `provisa/openapi/register.py:59–70`]

| Type JSON Schema | Type Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Paramètres en tant que colonnes de filtre natif.** Les paramètres de chemin et de requête qui ne sont pas déjà des champs de réponse deviennent des colonnes dont `native_filter_type` est défini sur `path_param` ou `query_param`, préfixées `_nf_`. Lorsque le nom d'un paramètre correspond au nom d'un champ de réponse, les métadonnées du paramètre sont fusionnées dans l'entrée de colonne existante plutôt que de créer un doublon. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Résolution du schéma de réponse.** Le mapper vérifie `responses.200`, puis `responses.2xx`, puis `responses.default`. Les réponses de type tableau sont déballées vers leur schéma d'élément. Les références `$ref` sont résolues sur un niveau de profondeur. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Sous-champs d'objet.** Les propriétés de réponse de `type: object` possédant leurs propres `properties` sont stockées comme `object_fields` sur la colonne. Ces sous-champs sont visibles dans le SDL et utilisés pour l'extraction `jsonb` dans les requêtes. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Mise en cache des réponses (REQ-318).** Les résultats des opérations GET sont mis en cache dans PostgreSQL par `pg_cache.py`. Chaque combinaison de paramètres de requête obtient son propre groupe `_params_hash`. Les lignes d'un hash donné sont remplacées à l'expiration du TTL. Les endpoints à paramètre de chemin (`/pets/{id}`) omettent la récupération initiale en masse — la table de cache est créée vide pour l'introspection de schéma, puis peuplée par clé primaire au fur et à mesure des requêtes. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Actualisation (REQ-321).** Envoyer une requête POST à `/admin/openapi/refresh/{source_id}`. Ré-analyse la spécification via `_load_and_register`, qui n'enregistre rien : elle n'ajoute ni table ni colonne. Les règles de gouvernance existantes sont préservées. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] Une table enregistrée conserve ses colonnes ; elle est lue via l'opération de la spec actualisée. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**Limitations.**

- L'extraction de sous-champs d'objet se limite à un niveau de profondeur. Les propriétés imbriquées dans `object_fields` ne sont pas développées récursivement. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Les paramètres d'en-tête et de cookie sont ignorés ; seuls les paramètres `path` et `query` sont enregistrés. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- La résolution des `$ref` au niveau de la spécification se limite à un niveau de profondeur pour les schémas de propriétés ; les références de composants profondément imbriquées peuvent ne pas se résoudre. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Impact de l'enregistrement d'une table distante

Une table enregistrée depuis n'importe quelle source de schéma distant est une table Provisa de première classe. Rien ne la distingue, au moment de l'exécution, d'une table relationnelle connectée localement. (REQ-308, REQ-313)

**Interfaces de requête.** La table est immédiatement interrogeable via GraphQL, SQL (pgwire ou direct), Cypher (GQL), JSON:API et Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) La génération de schéma synthétise `ColumnMetadata` pour les tables distantes, puisqu'elles n'ont pas de catalogue — la correspondance des types est appliquée au moment de la construction du schéma. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Modèle de sécurité.** Les cinq couches de gouvernance s'appliquent :

1. Contrôle d'accès par domaine — le `domain_id` de la table détermine quels rôles peuvent la voir. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Sécurité au niveau des lignes (RLS) — les filtres de ligne configurés sur la table sont injectés dans chaque requête, quelle que soit l'interface. (REQ-040, REQ-041)
3. Visibilité des colonnes — la liste `visible_to` de chaque colonne contrôle l'exposition des champs par rôle. (REQ-039)
4. Masquage des colonnes — les règles de masquage s'appliquent à l'étape 2 du pipeline de gouvernance. (REQ-040, REQ-263)
5. Protection des prédicats — les colonnes masquées sont rejetées des clauses WHERE et HAVING. (REQ-603)

Les requêtes ad hoc sur les tables distantes sont autorisées sous les seuls droits de l'utilisateur — l'accès repose uniformément sur les droits (droits de table/colonne + relations approuvées), sans mode de gouvernance propre à chaque table. (REQ-001, REQ-003)

**Gouvernance des relations (V002).** Les conditions de jointure sur des tables distantes — lorsqu'elles sont interrogées via SQL ou Cypher — doivent correspondre à une relation enregistrée et approuvée. (REQ-604) La vérification V002 est ignorée pour les requêtes GraphQL car les relations définies dans le SDL sont préapprouvées par conception. Voir [docs/security.md](security.md#gouvernance-des-relations-v002).

**Colonnes de type OBJECT.** Lorsqu'une colonne correspond à un OBJECT GQL inline non gouverné ou à un type d'objet OpenAPI, son type Provisa est `jsonb`. La colonne stocke le blob JSON imbriqué intégral. Lorsque des sous-champs sont déclarés (`gql_object_fields` ou `object_fields`), la correspondance `gql_object_columns` est peuplée au moment de la construction du schéma. Le générateur SQL utilise cette correspondance pour émettre des expressions d'extraction `->>` pour les sous-champs lorsqu'une requête les sélectionne. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Arguments obligatoires en tant que paramètres de filtre natif.** Les champs de requête racine dotés d'arguments non nuls et sans valeur par défaut injectent des colonnes supplémentaires sur la table enregistrée. Ces colonnes portent `native_filter_type: query_param`. Le traducteur Cypher réécrit `WHERE n.id = $val` en `WHERE n._nf_id = $val`, et l'exécuteur GraphQL les récupère comme variables à transmettre au endpoint distant. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impact de la création d'une relation de couverture

Lorsqu'un data steward enregistre une relation entre deux tables distantes (ou entre une table distante et une table locale), cette relation devient le chemin de jointure utilisé au moment de la requête.

**Comment la jointure l'emporte.** Lors de la compilation de la requête, Provisa résout le chemin de jointure via la relation enregistrée. `source_column` et `target_column` de la relation deviennent la condition de jointure dans le SQL généré. La jointure remplace tout appel distant par table qui serait autrement nécessaire pour le type connecté.

**Le blob brut n'est jamais exposé en SQL.** La colonne `breed` de `petstore__pets` n'est pas sélectionnable comme valeur jsonb brute dans les requêtes SQL. Lorsqu'une relation est enregistrée entre `petstore__pets` et `petstore__breeds`, les requêtes SQL parcourent la jointure — `SELECT breed.name FROM petstore__pets` se résout via la jointure clé étrangère, et non via un blob. En l'absence de relation enregistrée mais lorsque la colonne possède des sous-champs déclarés (`gql_object_fields`), les références de sous-champs en SQL sont réécrites en extraction `->>` sur le blob stocké. Ce chemin n'est disponible que pour les types inline non gouvernés — les champs de cible gouvernée sont entièrement exclus du SDL et n'ont aucun blob dont extraire des données. Le blob brut lui-même n'est jamais émis comme valeur de colonne nue. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

Dans le SDL GraphQL, un champ OBJECT inline non gouverné est typé comme le type d'objet imbriqué. Qu'il soit servi par une jointure ou par une extraction de blob au moment de l'exécution relève d'un détail d'implémentation — la forme du SDL est identique dans les deux cas. Lorsque le type enfant est enregistré comme sa propre table (et devient ainsi gouverné), les cinq couches de gouvernance s'y appliquent indépendamment : ses propres règles RLS, la visibilité des colonnes, les règles de masquage, la protection des prédicats et le contrôle d'accès par domaine. (REQ-039, REQ-040, REQ-041, REQ-263) L'extraction de blob contourne cela — les données de l'enfant arrivent préintégrées dans la ligne parente et ne sont gouvernées que par les règles de la table parente. Enregistrer l'enfant comme table et créer une relation constitue la voie vers une gouvernance à grain fin sur le type enfant.

**`graphql_alias` sur la relation.** Le champ `graphql_alias` nomme le champ du SDL que la relation expose sur le type parent. En son absence, le nom est dérivé du `field_name` de la table cible et de la cardinalité de la relation via `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 sur le chemin de jointure.** Les requêtes SQL et Cypher qui parcourent la relation sont soumises à la gouvernance des relations V002. La relation doit être enregistrée et approuvée pour que la jointure soit autorisée. (REQ-604) Le parcours GraphQL via le champ de relation du SDL est toujours préapprouvé. [tool-verified: `docs/security.md:41–54`]

**Indicateur remote-managed.** Les relations détectées automatiquement lors de l'enregistrement d'un schéma distant GraphQL sont stockées avec `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Il s'agit d'un marqueur de métadonnées ; il ne modifie pas le comportement de gouvernance.

---

## Comportement de définition de type seule

Tous les types d'un schéma distant n'ont pas besoin d'être une table interrogeable.

Lorsque `root_table_ids` est défini sur un `SchemaInput`, les tables dont l'identifiant est absent de cet ensemble sont exclues des champs de requête racine dans le SDL généré. Elles restent présentes en tant que types GraphQL et sont accessibles via des champs de relation sur les tables disposant d'entrées racine. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

Le même mécanisme s'applique aux constructions de schéma filtrées par domaine : les tables des domaines auxquels le rôle n'a pas accès sont de simples définitions de type — leur définition de type existe dans le SDL pour le parcours de relations, mais aucun champ de requête racine n'est généré pour elles. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Une table en définition de type seule :

- N'a pas de champ de requête racine — les clients ne peuvent pas l'interroger directement par nom.
- Est accessible via des champs de relation sur les tables disposant d'entrées racine.
- Apparaît toujours dans l'introspection de schéma comme un type nommé.
- Conserve l'application de toutes les règles de gouvernance lorsque les données sont consultées via une relation. (REQ-039, REQ-040)

La suppression complète du schéma — y compris la définition de type — ne se produit que lorsque l'enregistrement de la table est entièrement supprimé. Marquer une table en définition de type seule (en retirant son identifiant de `root_table_ids` ou en filtrant selon l'accès au domaine) ne supprime pas le type.

Cette conception permet aux data stewards d'exposer des graphes d'objets navigables où certains types ne sont accessibles que par parcours, et non par requête indépendante.
